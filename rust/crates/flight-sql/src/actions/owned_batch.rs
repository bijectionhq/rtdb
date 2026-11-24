use std::sync::Arc;

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
use rtdb_storage::{IoRegion, audience_work, sealing::{PlaintextDelivery, PlaintextView}};
use tonic::{Response, Status, metadata::MetadataValue};

use crate::{
    config::{AUDIENCE_READ_LIMITS, AUDIENCE_REPLY_BYTES, AUDIENCE_REQUEST_BYTES},
    error::{arrow, cancelled, invalid, status},
    owned_actions::{HELD_OWNED_POINT_V2, HeldRangeFormat, PointTime, ValidHistoryMode, WorldOrder},
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
            Self::Selection => 8,
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
                        self.held_owned_point(call, request, PointTime::Optional, false, Some(selection)).await
                    };
                    let reply = IoRegion::retaining(owner, audience_work::within_turn(
                        Some(AUDIENCE_READ_LIMITS), Some(turn), work,
                    )).await?;
                    if reply.encoded_len() > AUDIENCE_REPLY_BYTES {
                        return Err(Status::resource_exhausted("The audience reply exceeds its transport allowance"));
                    }
                    Ok(reply)
                } else {
                    self.batch_read(call, kind, selector, request, selection).await
                }
            }).await?;
            let grant = plaintext.grant().await.map_err(status)?;
            Ok((reply, grant))
        }).await.unwrap_or_else(|| Err(cancelled()))?;
        body.advance(consumed);
        Ok(result)
    }

    async fn batch_read(
        &self, call: &Call, kind: BatchKind, selector: u8, request: &[u8],
        selection: &mut SelectionEncoder,
    ) -> Result<ActionResult, Status> {
        match kind {
            BatchKind::AudiencePoints => unreachable!("audience points use the reserved action allowance"),
            BatchKind::Selection if selector == 8 => {
                self.held_owned_valid_during(call, request, ValidHistoryMode::Timeline, true).await
            },
            BatchKind::Point | BatchKind::Selection => {
                let compact = matches!(kind, BatchKind::Selection);
                if call.session.fresh_replica && matches!(selector, 4..=6) {
                    return Err(Status::permission_denied("The fresh Flight route only admits Root point reads"));
                }
                match selector {
                    0 => self.held_owned_point(call, request, PointTime::Optional, compact, compact.then_some(selection)).await,
                    1 => self.held_owned_point(call, request, PointTime::Required, compact, compact.then_some(selection)).await,
                    2 => self.held_owned_historical_point(call, request, compact.then_some(selection)).await,
                    3 => self.held_owned_position_point(call, request, compact.then_some(selection)).await,
                    4 => self.held_world_point(call, request, compact.then_some(selection)).await,
                    5 => self.held_world_point_at(call, request, false, compact.then_some(selection)).await,
                    6 => self.held_world_point_at(call, request, true, compact.then_some(selection)).await,
                    7 if !compact => self.held_root_cut_point_action(call, request).await,
                    _ => Err(invalid("Unsupported held selection entry")),
                }
            }
            BatchKind::Range => {
                if selector <= 3 {
                    let version = match selector {
                        0 => HeldRangeFormat::Leading,
                        1 => HeldRangeFormat::Full,
                        2 => HeldRangeFormat::FixedValid,
                        _ => HeldRangeFormat::Historical,
                    };
                    return self.held_owned_range_action(call, request, version).await;
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
                self.held_world_ordered_action(call, request, order).await
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
        if schema.fields().iter().any(|field| !matches!(field.data_type(),
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
        let fields = Arc::new(Schema::new(schema.fields().clone()));
        if changed {
            let frame = rtdb_format::ipc::write_schema_message_with_options(&fields, &options).map_err(status)?;
            response.extend_from_slice(&u32::try_from(frame.len()).map_err(arrow)?.to_be_bytes());
            response.extend_from_slice(&frame);
        }
        if let Some(batch) = batch {
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
        if changed {
            let Ok(retained) = memory.reserve(wire::schema_bound(&fields)?) else { return Ok(None) };
            self.schema = Some((fields, retained));
        }
        let retained = memory.reserve(response.len()).map_err(status)?;
        Ok(Some(ActionResult { body: wire::retain(response, retained) }))
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_point_fields_are_sent_once_with_individual_metadata() {
        use arrow_array::{ArrayRef, StringArray};

        let memory = MemoryBudget::new(1 << 20);
        let data = RecordBatch::try_from_iter([
            ("_id", Arc::new(StringArray::from(vec!["row"])) as ArrayRef),
        ]).unwrap();
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
        assert!(next.len() < first.len());
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
}
