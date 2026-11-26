use std::sync::Arc;
use rtdb_types::AnomalyKind;

use arrow_flight::{Action, Result as ActionResult};
use arrow_array::RecordBatch;
use arrow_ipc::{
    MessageHeader,
    writer::{CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions, write_message},
};
use arrow_schema::{DataType, Schema, SchemaRef};
use bytes::{Buf, Bytes};
use prost::Message;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::ipc::RecordSpans;
use rtdb_storage::{IoRegion, audience_work, sealing::{PlaintextDelivery, PlaintextView}};
use tonic::{Response, Status, metadata::MetadataValue};

use crate::{
    config::{AUDIENCE_READ_LIMITS, AUDIENCE_REPLY_BYTES, AUDIENCE_REQUEST_BYTES},
    error::{arrow, cancelled, invalid, status},
    owned_actions::{HELD_OWNED_POINT_V2, CompactIdentity, HeldRangeFormat, OwnedReplyFormat, PointTime, ValidHistoryMode, WorldOrder},
    service::{Call, Output, Service},
    wire,
};

pub(crate) const OWNED_IPC_HEADER: &str = "x-rtdb-owned-ipc";
pub(super) const READ_POINTS: &str = "rtdb.owned.ReadPoints.v1";
pub(super) const READ_SELECTION: &str = "rtdb.owned.ReadSelection.v1";
pub(super) const READ_AUDIENCE_POINTS: &str = "rtdb.owned.ReadAudiencePoints.v1";
pub(super) const READ_RANGES: &str = "rtdb.owned.ReadRanges.v1";
const MAX_REQUEST_BYTES: usize = 2 << 20;
const MAX_POINTS: u16 = 256;
const MAX_RANGES: u16 = 32;
const MAX_AUDIENCE_POINTS: u16 = 4;
const DEFAULT_IPC_ALIGNMENT: usize = 64;
const IPC_EOS: [u8; 8] = [255, 255, 255, 255, 0, 0, 0, 0];

#[derive(Clone, Copy)]
pub(super) enum BatchKind {
    Point,
    Selection,
    AudiencePoints,
    Range,
}

impl BatchKind {
    pub(super) fn for_action(action: &str) -> Option<Self> {
        match action {
            READ_POINTS => Some(Self::Point),
            READ_SELECTION => Some(Self::Selection),
            READ_AUDIENCE_POINTS => Some(Self::AudiencePoints),
            READ_RANGES => Some(Self::Range),
            _ => None,
        }
    }

    fn header(self) -> &'static str {
        match self {
            Self::Point => "x-rtdb-owned-points",
            Self::Selection => "x-rtdb-owned-selection",
            Self::AudiencePoints => "x-rtdb-audience-points",
            Self::Range => "x-rtdb-owned-ranges",
        }
    }

    pub(super) fn audience(self) -> bool {
        matches!(self, Self::AudiencePoints)
    }

    fn request(self, body: Bytes) -> Result<Bytes, Status> {
        let (count, mut remaining) = body.split_first_chunk::<2>()
            .ok_or_else(|| invalid("Missing held read batch count"))?;
        let max = match self {
            Self::Point | Self::Selection => MAX_POINTS,
            Self::AudiencePoints => MAX_AUDIENCE_POINTS,
            Self::Range => MAX_RANGES,
        };
        let bytes = if self.audience() { AUDIENCE_REQUEST_BYTES } else { MAX_REQUEST_BYTES };
        if !(2..=max).contains(&u16::from_be_bytes(*count)) || body.len() > bytes {
            return Err(invalid("Held read batch exceeds its request bound"));
        }
        let max_selector = match self {
            Self::Point | Self::Range => 7,
            Self::Selection => 11,
            Self::AudiencePoints => 0,
        };
        for _ in 0..u16::from_be_bytes(*count) {
            let (selector, request, rest) = entry(remaining)?;
            if selector > max_selector || request.is_empty() {
                return Err(invalid("Invalid held read batch entry"));
            }
            remaining = rest;
        }
        if !remaining.is_empty() {
            return Err(invalid("Trailing held read batch bytes"));
        }
        Ok(body.slice(2..))
    }
}

fn entry(input: &[u8]) -> Result<(u8, &[u8], &[u8]), Status> {
    let (header, tail) = input.split_first_chunk::<5>()
        .ok_or_else(|| invalid("Truncated held read batch entry"))?;
    let size = u32::from_be_bytes(header[1..].try_into().expect("four length bytes")) as usize;
    let (request, rest) = tail.split_at_checked(size)
        .ok_or_else(|| invalid("Truncated held read batch request"))?;
    Ok((header[0], request, rest))
}

impl Service {
    pub(super) async fn held_read_batch(
        &self, call: Call, kind: BatchKind, body: Bytes,
    ) -> Result<Response<Output<ActionResult>>, Status> {
        let mut body = kind.request(body).map_err(|error| call.error(error))?;
        let mut selection = SelectionEncoder::default();
        let first = self.read_batch_entry(&call, kind, &mut body, &mut selection).await;
        let (mut headers, cutoff) = match &first {
            Ok((_, grant)) => call.delivery_headers(grant, !kind.audience())?,
            Err(_) => (call.response(())?.metadata().clone(), None),
        };
        headers.insert(kind.header(), MetadataValue::from_static("v1"));

        let service = self.clone();
        let stream: Output<ActionResult> = Box::pin(async_stream::try_stream! {
            let mut next = first;
            loop {
                let (reply, grant) = next.map_err(|error| call.error(error))?;
                if call.cancellation.is_cancelled() {
                    Err(call.error(cancelled()))?;
                }
                call.handoff_action(grant, cutoff).map_err(|error| call.error(error))?;
                service.publish();
                yield reply;
                if body.is_empty() {
                    break;
                }
                next = service.read_batch_entry(&call, kind, &mut body, &mut selection).await;
            }
        });
        let mut response = Response::new(stream);
        *response.metadata_mut() = headers;
        Ok(response)
    }

    async fn read_batch_entry(
        &self, call: &Call, kind: BatchKind, body: &mut Bytes, selection: &mut SelectionEncoder,
    ) -> Result<(ActionResult, PlaintextDelivery), Status> {
        let (selector, request, rest) = entry(body)?;
        let consumed = body.len() - rest.len();
        let result = call.cancellation.run_until_cancelled(async {
            let plaintext = PlaintextView::new(Arc::clone(call.memory()));
            let reply = plaintext.reading(async {
                if kind.audience() {
                    let owner = call.audience_io_owner().ok_or_else(|| invalid("Missing audience I/O owner"))?;
                    let turn = self.audience_read_work(call, HELD_OWNED_POINT_V2, request)?;
                    turn.admit_action().map_err(|error| Status::resource_exhausted(error.to_string()))?;
                    let original = Action {
                        r#type: HELD_OWNED_POINT_V2.to_owned(),
                        body: body.slice_ref(request),
                    };
                    let work = async {
                        audience_work::decode_bytes(original.encoded_len())
                            .map_err(|error| Status::resource_exhausted(error.to_string()))?;
                        self.confirm_audience_observation(call).await?;
                        self.held_owned_point(call, &original.body, PointTime::Optional, None, OwnedReplyFormat::Selection(selection))
                            .await.map(OwnedIpcReply::into_unary)
                    };
                    let reply = IoRegion::retaining(owner, audience_work::within_turn(
                        Some(AUDIENCE_READ_LIMITS), Some(turn), work,
                    )).await?;
                    if reply.encoded_len() > AUDIENCE_REPLY_BYTES {
                        return Err(Status::resource_exhausted("The audience reply exceeds its transport allowance"));
                    }
                    Ok(reply)
                } else {
                    self.batch_read(call, kind, selector, &body.slice_ref(request), selection).await
                }
            }).await?;
            let grant = plaintext.grant().await.map_err(status)?;
            Ok((reply, grant))
        }).await.unwrap_or_else(|| Err(cancelled()))?;
        body.advance(consumed);
        Ok(result)
    }

    async fn batch_read(
        &self, call: &Call, kind: BatchKind, selector: u8, request: &Bytes,
        selection: &mut SelectionEncoder,
    ) -> Result<ActionResult, Status> {
        match kind {
            BatchKind::AudiencePoints => unreachable!("audience points use the reserved action allowance"),
            BatchKind::Selection if matches!(selector, 8 | 11) => {
                let identity = if selector == 11 { CompactIdentity::Binary } else { CompactIdentity::Text };
                self.held_owned_valid_during(call, request, ValidHistoryMode::Timeline, Some(identity), false)
                    .await.map(OwnedIpcReply::into_unary)
            },
            BatchKind::Point | BatchKind::Selection => {
                let compact = matches!(kind, BatchKind::Selection).then_some(if matches!(selector, 9 | 10) {
                    CompactIdentity::Binary
                } else {
                    CompactIdentity::Text
                });
                if call.session.fresh_replica && matches!(selector, 4..=6) {
                    return Err(Status::permission_denied("The fresh Flight route only admits Root point reads"));
                }
                let format = if compact.is_some() { OwnedReplyFormat::Selection(selection) } else { OwnedReplyFormat::Unary };
                match selector {
                    0 | 9 => self.held_owned_point(call, request, PointTime::Optional, compact, format).await,
                    1 | 10 => self.held_owned_point(call, request, PointTime::Required, compact, format).await,
                    2 => self.held_owned_historical_point(call, request, compact, format).await,
                    3 => self.held_owned_position_point(call, request, compact, format).await,
                    4 => self.held_world_point(call, request, compact, format).await,
                    5 => self.held_world_point_at(call, request, false, compact, format).await,
                    6 => self.held_world_point_at(call, request, true, compact, format).await,
                    7 if compact.is_none() => self.held_root_cut_point_action(call, request, format).await,
                    _ => Err(invalid("Unsupported held selection entry")),
                }.map(OwnedIpcReply::into_unary)
            }
            BatchKind::Range => {
                if selector <= 3 {
                    let version = match selector {
                        0 => HeldRangeFormat::Leading,
                        1 => HeldRangeFormat::Full,
                        2 => HeldRangeFormat::FixedValid,
                        _ => HeldRangeFormat::Historical,
                    };
                    return self.held_owned_range_action(call, request, version, OwnedReplyFormat::Unary)
                        .await.map(OwnedIpcReply::into_unary);
                }
                if call.session.fresh_replica {
                    return Err(Status::permission_denied("The fresh Flight route only admits Root range reads"));
                }
                let order = match selector {
                    4 => WorldOrder::ParentRange,
                    5 => WorldOrder::OverlayRange,
                    6 => WorldOrder::ParentRangeAt,
                    7 => WorldOrder::OverlayRangeAt,
                    _ => unreachable!("validated range selector"),
                };
                self.held_world_ordered_action(call, request, order, OwnedReplyFormat::Unary)
                    .await.map(OwnedIpcReply::into_unary)
            }
        }
    }
}

/// A reply retains its own decision metadata. Consecutive compatible points
/// share the field schema, even when their original revisions differ.
#[derive(Default)]
pub(crate) struct SelectionEncoder {
    schema: Option<(SchemaRef, Arc<MemoryReservation>)>,
}

impl SelectionEncoder {
    pub(crate) fn point(
        &mut self, memory: &Arc<MemoryBudget>, schema: &SchemaRef,
        batch: Option<&RecordBatch>, maximum_message_bytes: usize,
    ) -> Result<Option<ActionResult>, Status> {
        if schema.metadata().len() > 32 || schema.fields().iter().any(|field| !matches!(field.data_type(),
            DataType::Null | DataType::Boolean | DataType::Int64 | DataType::UInt64
            | DataType::Float64 | DataType::Utf8 | DataType::Binary | DataType::Timestamp(..)))
        {
            return Ok(None);
        }
        let array_bytes = batch.map_or(0, RecordBatch::get_array_memory_size);
        let bound = wire::add(wire::schema_bound(schema)?, wire::mul(array_bytes, 8)?)?;
        // Near the scalar message limit, let the original encoder make the
        // exact size decision using the original envelope.
        if wire::add(bound, 1024)? > maximum_message_bytes {
            return Ok(None);
        }
        let _encoding = memory.reserve(bound).map_err(status)?;
        let changed = self.schema.as_ref().is_none_or(|(prior, _)| prior.fields() != schema.fields());
        let mut response = vec![b'P', u8::from(changed)];
        response.extend_from_slice(&u32::try_from(schema.metadata().len()).map_err(arrow)?.to_be_bytes());
        for (key, value) in schema.metadata() {
            for text in [key, value] {
                response.extend_from_slice(&u32::try_from(text.len()).map_err(arrow)?.to_be_bytes());
                response.extend_from_slice(text.as_bytes());
            }
        }
        let options = IpcWriteOptions::default();
        let fields = if changed {
            let fields = Arc::new(Schema::new(schema.fields().clone()));
            let frame = rtdb_format::ipc::write_schema_message_with_options(&fields, &options).map_err(status)?;
            response.extend_from_slice(&u32::try_from(frame.len()).map_err(arrow)?.to_be_bytes());
            response.extend_from_slice(&frame);
            Some(fields)
        } else {
            None
        };
        if let Some(batch) = batch
            && (batch.schema_ref().fields() != schema.fields()
                || !append_exact_record(&mut response, batch, memory)?)
        {
            let (dictionaries, data) = IpcDataGenerator::default().encode(
                batch, &mut DictionaryTracker::new(false), &options, &mut CompressionContext::default(),
            ).map_err(arrow)?;
            if !dictionaries.is_empty() {
                return Err(invalid("A held point selection unexpectedly contains a dictionary"));
            }
            write_message(&mut response, data, &options).map_err(arrow)?;
        }
        if wire::add(response.len(), 1024)? > maximum_message_bytes {
            return Ok(None);
        }
        if let Some(fields) = fields {
            let Ok(retained) = memory.reserve(wire::schema_bound(&fields)?) else { return Ok(None) };
            self.schema = Some((fields, retained));
        }
        let retained = memory.reserve(response.len()).map_err(status)?;
        Ok(Some(ActionResult { body: wire::retain(response, retained) }))
    }
}

fn append_exact_record(
    frame: &mut Vec<u8>, batch: &RecordBatch, memory: &Arc<MemoryBudget>,
) -> Result<bool, Status> {
    let spans = match RecordSpans::exact(batch, DEFAULT_IPC_ALIGNMENT, memory) {
        Ok(Some(spans)) => spans,
        Ok(None) => return Ok(false),
        // The caller already holds the original encoder's grant for the fallback.
        Err(error) if error.is(AnomalyKind::ResourceExhausted) => return Ok(false),
        Err(error) => return Err(status(error)),
    };
    let (metadata, body_len) = spans.framed_metadata().map_err(status)?;
    frame.try_reserve_exact(wire::add(metadata.len(), body_len)?).map_err(arrow)?;
    frame.extend_from_slice(&metadata);
    spans.write_body(frame).map_err(arrow)?;
    Ok(true)
}

pub(super) fn is_ipc_stream(body: &[u8]) -> bool {
    body.ends_with(&IPC_EOS)
        && ipc_message(body).is_ok_and(|(_, kind)| kind == MessageHeader::Schema)
}

fn ipc_message(body: &[u8]) -> Result<(usize, MessageHeader), Status> {
    let (prefix, tail) = body.split_first_chunk::<8>()
        .ok_or_else(|| invalid("Truncated owned IPC prefix"))?;
    if prefix[..4] != [255; 4] {
        return Err(invalid("Owned IPC requires its continuation prefix"));
    }
    let length = u32::from_le_bytes(prefix[4..].try_into().expect("four length bytes")) as usize;
    if length == 0 {
        return if tail.is_empty() {
            Ok((IPC_EOS.len(), MessageHeader::NONE))
        } else {
            Err(invalid("Trailing bytes after owned IPC terminator"))
        };
    }
    let (metadata, following) = tail.split_at_checked(length)
        .ok_or_else(|| invalid("Truncated owned IPC metadata"))?;
    let message = arrow_ipc::root_as_message(metadata).map_err(arrow)?;
    let body_size = usize::try_from(message.bodyLength()).map_err(arrow)?;
    if body_size > following.len() {
        return Err(invalid("Truncated owned IPC message body"));
    }
    Ok((8 + length + body_size, message.header_type()))
}

pub(crate) struct IpcFrames(pub Bytes);

impl Iterator for IpcFrames {
    type Item = Result<ActionResult, Status>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }
        let (length, _) = match ipc_message(&self.0) {
            Ok(message) => message,
            Err(error) => {
                self.0 = Bytes::new();
                return Some(Err(error));
            }
        };
        Some(Ok(ActionResult { body: self.0.split_to(length) }))
    }
}

pub(crate) enum OwnedIpcReply {
    Unary(ActionResult),
    Frames(Vec<ActionResult>),
}

impl OwnedIpcReply {
    pub(crate) fn encode(
        memory: &Arc<MemoryBudget>,
        schema: &Schema,
        pages: &[RecordBatch],
        stream: bool,
        maximum: usize,
    ) -> Result<Self, Status> {
        let bytes = pages.iter().try_fold(0, |bytes, batch| {
            wire::add(bytes, batch.get_array_memory_size())
        })?;
        let bound = wire::add(wire::schema_bound(schema)?, wire::mul(bytes, 8)?)?;
        let _encoding = memory.reserve(bound).map_err(status)?;
        if stream && pages.iter().all(|page| page.schema_ref().as_ref() == schema)
            && let Ok(frames) = encode_ipc_frames(schema, pages, memory)
        {
            return retain_ipc_frames(memory, frames, maximum);
        }
        // The original writer determines the status and precedence of codec refusals.
        let bytes = rtdb_format::ipc::write_arrow_stream(schema, pages, IpcWriteOptions::default()).map_err(status)?;
        let reply = ActionResult { body: bytes.into() };
        if reply.encoded_len() > maximum {
            return Err(Status::resource_exhausted("Owned action response exceeds the Flight message limit"));
        }
        let retained = memory.reserve(reply.body.len()).map_err(status)?;
        Ok(Self::Unary(ActionResult { body: wire::retain(reply.body, retained) }))
    }

    pub(crate) fn into_unary(self) -> ActionResult {
        match self {
            Self::Unary(reply) => reply,
            Self::Frames(_) => unreachable!("unary reply encoding was requested"),
        }
    }

    pub(crate) fn prepend_frame(self, prefix: ActionResult, maximum: usize) -> Result<Self, Status> {
        let mut frames = match self {
            Self::Frames(frames) => frames,
            Self::Unary(reply) => IpcFrames(reply.body).collect::<Result<Vec<_>, _>>()?,
        };
        let encoded = frames.iter().try_fold(prefix.encoded_len(), |bytes, frame| {
            wire::add(bytes, frame.encoded_len())
        })?;
        if encoded > maximum {
            return Err(Status::resource_exhausted("Owned action response exceeds the Flight message limit"));
        }
        frames.insert(0, prefix);
        Ok(Self::Frames(frames))
    }
}

fn encode_ipc_frames(
    schema: &Schema, batches: &[RecordBatch], memory: &Arc<MemoryBudget>,
) -> Result<Vec<Bytes>, Status> {
    let options = IpcWriteOptions::default();
    let schema_frame = rtdb_format::ipc::write_schema_message_with_options(schema, &options).map_err(status)?;
    let message = arrow_ipc::root_as_message(&schema_frame[8..]).map_err(arrow)?;
    let header = message.header_as_schema().ok_or_else(|| invalid("Missing owned IPC schema"))?;
    let mut tracker = DictionaryTracker::new(false);
    if let Some(fields) = header.fields() {
        for field in fields {
            register_dictionaries(field, &mut tracker);
        }
    }
    let dictionary_free = tracker.dict_id().is_empty();

    let mut frames = vec![schema_frame.into()];
    let generator = IpcDataGenerator::default();
    let mut compression = CompressionContext::default();
    for batch in batches {
        let mut frame = Vec::new();
        if dictionary_free && append_exact_record(&mut frame, batch, memory)? {
            frames.push(frame.into());
            continue;
        }
        let (dictionaries, record) = generator.encode(batch, &mut tracker, &options, &mut compression).map_err(arrow)?;
        for message in dictionaries.into_iter().chain(std::iter::once(record)) {
            let mut frame = Vec::new();
            write_message(&mut frame, message, &options).map_err(arrow)?;
            frames.push(frame.into());
        }
    }
    frames.push(Bytes::from_static(&IPC_EOS));
    Ok(frames)
}

fn register_dictionaries(field: arrow_ipc::Field<'_>, tracker: &mut DictionaryTracker) {
    if let Some(children) = field.children() {
        for child in children {
            register_dictionaries(child, tracker);
        }
    }
    if field.dictionary().is_some() {
        tracker.next_dict_id();
    }
}

fn retain_ipc_frames(
    memory: &Arc<MemoryBudget>, frames: Vec<Bytes>, maximum: usize,
) -> Result<OwnedIpcReply, Status> {
    let bytes = frames.iter().try_fold(0, |size, frame| wire::add(size, frame.len()))?;
    // The original cap includes one protobuf tag and length prefix for the complete stream.
    let encoded_len = wire::add(bytes, 1 + prost::length_delimiter_len(bytes))?;
    if encoded_len > maximum {
        return Err(Status::resource_exhausted("Owned action response exceeds the Flight message limit"));
    }
    let retained = memory.reserve(bytes).map_err(status)?;
    Ok(OwnedIpcReply::Frames(frames.into_iter().map(|frame| ActionResult {
        body: wire::retain(frame, Arc::clone(&retained)),
    }).collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_point_fields_are_sent_once_with_individual_metadata() {
        use arrow_array::{ArrayRef, StringArray};

        let memory = MemoryBudget::new(1 << 20);
        let data = RecordBatch::try_from_iter([
            ("_id", Arc::new(StringArray::from(vec!["before", "row", "after"])) as ArrayRef),
        ]).unwrap().slice(1, 1);
        assert!(RecordSpans::exact(&data, DEFAULT_IPC_ALIGNMENT, &memory).unwrap().is_some());
        let original = rtdb_format::ipc::write_arrow_stream(
            data.schema_ref(), std::slice::from_ref(&data), IpcWriteOptions::default(),
        ).unwrap();
        let mut original = IpcFrames(original.into());
        let schema_frame = original.next().unwrap().unwrap().body;
        let record = original.next().unwrap().unwrap().body;
        let mut encoder = SelectionEncoder::default();
        let encode = |encoder: &mut SelectionEncoder, revision: &str| {
            let schema = Arc::new(data.schema_ref().as_ref().clone()
                .with_metadata([("revision".to_owned(), revision.to_owned())].into()));
            encoder.point(&memory, &schema, Some(&data), 1 << 20).unwrap().unwrap().body
        };
        let first = encode(&mut encoder, "2");
        let next = encode(&mut encoder, "3");
        assert_eq!(&first[..2], &[b'P', 1]);
        assert_eq!(&next[..2], &[b'P', 0]);
        assert_eq!(&first[22..23], b"2");
        assert_eq!(&next[22..23], b"3");
        let schema_len = u32::from_be_bytes(first[23..27].try_into().unwrap()) as usize;
        assert_eq!(&first[27..27 + schema_len], schema_frame.as_ref());
        assert_eq!(&first[27 + schema_len..], record.as_ref());
        assert_eq!(&next[23..], record.as_ref());
    }

    #[test]
    fn selected_windows_and_reserved_points_have_separate_bounds() {
        let selection = Bytes::from_static(&[0, 2, 8, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 2]);
        assert!(BatchKind::Selection.request(selection.clone()).is_ok());
        assert!(BatchKind::Point.request(selection.clone()).is_err());
        assert!(BatchKind::AudiencePoints.request(selection).is_err());
        assert!(BatchKind::AudiencePoints.request(Bytes::from_static(&[0, 5])).is_err());
    }

    #[test]
    fn held_read_batch_has_exact_framing_and_selectors() {
        let bytes = Bytes::from_static(&[0, 2, 0, 0, 0, 0, 2, 1, 2, 7, 0, 0, 0, 1, 3]);
        let body = BatchKind::Point.request(bytes.clone()).unwrap();
        let (selector, request, rest) = entry(&body).unwrap();
        assert_eq!((selector, request), (0, &[1, 2][..]));
        assert_eq!(entry(rest).unwrap(), (7, &[3][..], &[][..]));
        assert!(BatchKind::Range.request(bytes.clone()).is_ok());
        let invalid_selector = Bytes::from_static(&[0, 2, 0, 0, 0, 0, 1, 1, 8, 0, 0, 0, 1, 2]);
        assert!(BatchKind::Range.request(invalid_selector).is_err());
        assert!(BatchKind::Point.request(bytes.slice(..bytes.len() - 1)).is_err());
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(BatchKind::Point.request(trailing.into()).is_err());
        assert!(BatchKind::Range.request(Bytes::from_static(&[0, 33])).is_err());
    }

    #[test]
    fn held_read_batch_ipc_frames_keep_arrow_stream_bytes_and_boundaries() {
        let input = rtdb_format::ipc::write_arrow_stream(
            &arrow_schema::Schema::empty(), &[], arrow_ipc::writer::IpcWriteOptions::default(),
        ).unwrap();
        assert!(is_ipc_stream(&input));
        let frames = IpcFrames(Bytes::copy_from_slice(&input)).collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[1].body.as_ref(), IPC_EOS);
        assert_eq!([frames[0].body.as_ref(), frames[1].body.as_ref()].concat(), input);
    }

    #[test]
    fn detached_ipc_matches_original_for_spans_and_generic_records() {
        use arrow_array::{ArrayRef, BooleanArray, Int64Array, StringArray};

        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("name", Arc::new(StringArray::from(vec!["skip", "longer", "z"])) as ArrayRef),
            ("flag", Arc::new(BooleanArray::from(vec![true, true, false])) as ArrayRef),
        ]).unwrap();
        let pages = [batch.slice(1, 2), batch.slice(0, 1)];
        let schema = batch.schema_ref();
        let memory = MemoryBudget::new(1 << 20);
        assert!(RecordSpans::exact(&pages[0], DEFAULT_IPC_ALIGNMENT, &memory).unwrap().is_some());
        assert!(RecordSpans::exact(&pages[1], DEFAULT_IPC_ALIGNMENT, &memory).unwrap().is_none());

        let original = OwnedIpcReply::encode(&memory, schema, &pages, false, 1 << 20).unwrap().into_unary();
        let maximum = original.encoded_len();
        let expected = IpcFrames(original.body.clone()).collect::<Result<Vec<_>, _>>().unwrap();
        let bytes = pages.iter().map(RecordBatch::get_array_memory_size).sum::<usize>();
        let bound = wire::schema_bound(schema).unwrap() + 8 * bytes;
        let fallback = MemoryBudget::new(bound + original.body.len());
        for budget in [&memory, &fallback] {
            let before = budget.used_bytes();
            let OwnedIpcReply::Frames(frames) = OwnedIpcReply::encode(budget, schema, &pages, true, maximum).unwrap() else {
                panic!("expected one Flight result per IPC message");
            };
            assert_eq!(frames, expected);
            assert_eq!(budget.used_bytes() - before, original.body.len());
            drop(frames);
            assert_eq!(budget.used_bytes(), before);
        }
    }

    #[test]
    fn detached_ipc_matches_sliced_stream_for_nested_dictionaries_and_full_width_decimals() {
        use arrow_array::{Array, ArrayRef, Decimal128Array, StructArray, builder::StringDictionaryBuilder, types::Int32Type};
        use arrow_schema::Field;
        use rtdb_columnar::{DecimalParameters, with_decimal_parameters};

        let dictionary = |value| -> ArrayRef {
            let mut builder = StringDictionaryBuilder::<Int32Type>::new();
            builder.append(value).unwrap();
            Arc::new(builder.finish())
        };
        let label = dictionary("nested");
        let nested = StructArray::from(vec![
            (Arc::new(Field::new("label", label.data_type().clone(), false)), label),
        ]);
        let kind = dictionary("outer");
        let amount = Decimal128Array::from(vec![123]).with_precision_and_scale(20, 1).unwrap();
        let amount_field = with_decimal_parameters(
            Field::new("amount", amount.data_type().clone(), false),
            DecimalParameters { precision: 20, scale: 200, bit_width: 128 },
        ).unwrap();
        let schema = Arc::new(Schema::new_with_metadata(vec![
            Field::new("nested", nested.data_type().clone(), false),
            Field::new("kind", kind.data_type().clone(), false),
            amount_field,
        ], [("decision".into(), "held".into())].into()));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(nested), kind, Arc::new(amount)]).unwrap();
        let mut changed = batch.columns().to_vec();
        changed[1] = dictionary("replacement");
        let changed = RecordBatch::try_new(Arc::clone(&schema), changed).unwrap();
        let pages = [batch.clone(), batch, changed];
        let memory = MemoryBudget::new(1 << 20);
        for batches in [&pages[..0], &pages[..1], &pages[..]] {
            let unary = OwnedIpcReply::encode(&memory, &schema, batches, false, 1 << 20).unwrap().into_unary();
            let maximum = unary.encoded_len();
            let expected = IpcFrames(unary.body).collect::<Result<Vec<_>, _>>().unwrap();
            let OwnedIpcReply::Frames(frames) = OwnedIpcReply::encode(&memory, &schema, batches, true, maximum).unwrap() else {
                panic!("expected one Flight result per IPC message");
            };
            assert_eq!(frames, expected);
        }
    }

    #[test]
    fn negotiated_ipc_preserves_the_unary_schema_refusal() {
        let memory = MemoryBudget::new(1 << 20);
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let schema = Schema::empty().with_metadata([("present".into(), "1".into())].into());
        let pages = [batch];
        let Err(unary) = OwnedIpcReply::encode(&memory, &schema, &pages, false, 1 << 20) else {
            panic!("expected the original writer to reject a mismatched schema");
        };
        let Err(stream) = OwnedIpcReply::encode(&memory, &schema, &pages, true, 1 << 20) else {
            panic!("expected negotiated framing to reject a mismatched schema");
        };
        assert_eq!(stream.code(), unary.code());
        assert_eq!(stream.message(), unary.message());
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn detached_ipc_charges_the_original_aggregate_until_the_last_frame_is_dropped() {
        let frames = encode_ipc_frames(&Schema::empty(), &[], &MemoryBudget::new(1 << 20)).unwrap();
        let original = ActionResult { body: frames.concat().into() };
        let cap = original.encoded_len();
        assert!(frames.iter().all(|body| ActionResult { body: body.clone() }.encoded_len() < cap - 1));
        let memory = MemoryBudget::new(original.body.len());
        let Err(error) = retain_ipc_frames(&memory, frames.clone(), cap - 1) else {
            panic!("aggregate reply exceeded the original message cap");
        };
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(memory.used_bytes(), 0);

        let OwnedIpcReply::Frames(mut replies) = retain_ipc_frames(&memory, frames, cap).unwrap() else {
            panic!("expected detached IPC messages");
        };
        let held = replies.pop().unwrap().body.clone();
        drop(replies);
        assert_eq!(held.as_ref(), IPC_EOS);
        assert_eq!(memory.used_bytes(), original.body.len());
        drop(held);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn ranked_reply_prefix_counts_toward_the_limit_and_reuses_ipc_frames() {
        let memory = MemoryBudget::new(1 << 20);
        let prefix = ActionResult { body: Bytes::from_static(b"inventory") };
        let frames = encode_ipc_frames(&Schema::empty(), &[], &memory).unwrap();
        let retained_schema = frames[0].clone();
        let maximum = frames.iter().fold(prefix.encoded_len(), |size, body| {
            size + ActionResult { body: body.clone() }.encoded_len()
        });
        let make_reply = || retain_ipc_frames(&memory, frames.clone(), maximum).unwrap();
        let Err(error) = make_reply().prepend_frame(prefix.clone(), maximum - 1) else {
            panic!("ranked reply exceeded its aggregate message limit");
        };
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(memory.used_bytes(), 0);

        let OwnedIpcReply::Frames(replies) = make_reply().prepend_frame(prefix.clone(), maximum).unwrap() else {
            panic!("expected ranked IPC messages");
        };
        assert_eq!(replies[0], prefix);
        assert_eq!(replies[1].body.as_ptr(), retained_schema.as_ptr());
        assert_eq!(replies[2].body.as_ref(), IPC_EOS);
    }
}
