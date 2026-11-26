use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::sync::Arc;
use arrow_array::RecordBatch;
use arrow_ipc::writer::{CompressionContext, DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions};
use arrow_ipc::MetadataVersion;
use bytes::Bytes;
use rtdb_columnar::{DeclaredSchema, MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_node::{AccessSettings, CommitResult, NativeSession, Node, TransactionSettings, TransactionStatus};
use rtdb_pg_codec::Format;
use rtdb_plan::{CopyFormat, CopyOutFormat, Identifier};
use rtdb_tx::{ArrowDocuments, TxOp};
use rtdb_types::{Anomaly, Result, TableRef};
use rtdb_types::AnomalyKind;
use tokio_util::sync::CancellationToken;
use crate::error::{cancelled, protocol};
use crate::wire::{Frame, Writer};

mod history_labels;
#[cfg(unix)]
mod spool;
mod stream_headers;
mod transit;

use stream_headers::StreamHeaders;

const COPY_CHUNK_BYTES: usize = 64 * 1024;
const COPY_IPC_ALIGNMENT: usize = 64;
const COPY_ARROW_END: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];
const COPY_INPUT_SCRATCH_BYTES: usize = 8192;
const COPY_RETAINED_STREAM_BYTES: usize = 256 * 1024;

struct CopyArrowMessage {
    data: EncodedData,
    prefix: [u8; 8],
    padding: usize,
}

impl CopyArrowMessage {
    fn new(data: EncodedData) -> Result<Self> {
        if !data.arrow_data.len().is_multiple_of(COPY_IPC_ALIGNMENT) {
            return Err(arrow_output_error(arrow_schema::ArrowError::MemoryError("Arrow data not aligned".into())));
        }

        // Arrow's V5 padding includes the continuation marker and length field.
        let mut prefix = COPY_ARROW_END;
        let unpadded = data.ipc_message.len().checked_add(prefix.len()).ok_or_else(copy_output_size)?;
        let aligned = unpadded.checked_next_multiple_of(COPY_IPC_ALIGNMENT).ok_or_else(copy_output_size)?;
        let metadata_len = i32::try_from(aligned - prefix.len()).map_err(|_| copy_output_size())?;
        prefix[4..].copy_from_slice(&metadata_len.to_le_bytes());
        Ok(Self { data, prefix, padding: aligned - unpadded })
    }

    fn parts(&self) -> [&[u8]; 4] {
        const PADDING: [u8; COPY_IPC_ALIGNMENT] = [0; COPY_IPC_ALIGNMENT];
        [&self.prefix, &self.data.ipc_message, &PADDING[..self.padding], &self.data.arrow_data]
    }
}

struct CopyArrowPage {
    messages: Vec<CopyArrowMessage>,
    _reservation: Arc<MemoryReservation>,
}

async fn send_copy_arrow<W: tokio::io::AsyncWrite + Unpin>(
    wire: &mut Writer<W>, messages: &[CopyArrowMessage], chunk_bytes: usize,
) -> Result<()> {
    let parts = || messages.iter().flat_map(CopyArrowMessage::parts);
    let mut remaining = parts().try_fold(0usize, |total, part| {
        total.checked_add(part.len()).ok_or_else(copy_output_size)
    })?;
    let mut parts = parts().filter(|part| !part.is_empty());
    let mut part: &[u8] = &[];
    while remaining > 0 {
        let length = remaining.min(chunk_bytes);
        let mut frame_remaining = length;
        let frame = std::iter::from_fn(|| {
            if frame_remaining == 0 { return None; }
            if part.is_empty() {
                part = parts.next().expect("parts cover the measured COPY payload");
            }
            let (prefix, tail) = part.split_at(part.len().min(frame_remaining));
            part = tail;
            frame_remaining -= prefix.len();
            Some(prefix)
        });
        wire.copy_data_parts(length, frame).await?;
        remaining -= length;
    }
    Ok(())
}

struct CopyArrowEncoder {
    options: IpcWriteOptions,
    tracker: DictionaryTracker,
}

impl CopyArrowEncoder {
    fn new(schema: &arrow_schema::Schema) -> Result<(Self, CopyArrowMessage)> {
        let options = IpcWriteOptions::try_new(COPY_IPC_ALIGNMENT, false, MetadataVersion::V5)
            .map_err(arrow_output_error)?;
        let mut tracker = DictionaryTracker::new(false);
        let message = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(schema, &mut tracker, &options);
        Ok((Self { options, tracker }, CopyArrowMessage::new(message)?))
    }

    fn write(
        &mut self, batch: &RecordBatch, replace_dictionaries: bool, mut reservation: Arc<MemoryReservation>,
    ) -> Result<CopyArrowPage> {
        let mut page_tracker;
        let tracker = if replace_dictionaries {
            // Keep the stream's dictionary IDs without carrying spellings across delivery grants.
            page_tracker = DictionaryTracker::new(false);
            for &id in self.tracker.dict_id() {
                let page_id = page_tracker.next_dict_id();
                debug_assert_eq!(id, page_id);
            }
            &mut page_tracker
        } else {
            &mut self.tracker
        };
        let (dictionaries, record) = IpcDataGenerator::default()
            .encode(batch, tracker, &self.options, &mut CompressionContext::default())
            .map_err(arrow_output_error)?;
        let mut messages = Vec::with_capacity(dictionaries.len() + 1);
        for data in dictionaries.into_iter().chain([record]) {
            messages.push(CopyArrowMessage::new(data)?);
        }
        let descriptor_bytes = messages.capacity() * size_of::<CopyArrowMessage>()
            + size_of::<MemoryReservation>() + 2 * size_of::<usize>();
        let retained = messages.iter().try_fold(descriptor_bytes, |bytes, message| {
            bytes.checked_add(message.data.ipc_message.capacity())
                .and_then(|bytes| bytes.checked_add(message.data.arrow_data.capacity()))
                .ok_or_else(copy_output_size)
        })?;
        Arc::get_mut(&mut reservation).expect("COPY page encoding owns its reservation").shrink_to(retained)?;
        Ok(CopyArrowPage { messages, _reservation: reservation })
    }
}

/// Encodes each batch while its source buffers are in scope and releases
/// them before waiting on the connection.
pub(crate) async fn write_arrow<W: tokio::io::AsyncWrite + Unpin>(
    mut cursor: rtdb_query::QueryCursor, format: CopyOutFormat, memory: &Arc<MemoryBudget>, wire: &mut Writer<W>,
) -> Result<()> {
    let schema = cursor.runtime_schema();
    let bound = schema.fields().iter().try_fold(8192usize, |bytes, field| {
        let field_bytes = field.data_type.retained_bytes()?.saturating_add(field.name.len());
        bytes.checked_add(field_bytes.saturating_mul(64).saturating_add(4096))
            .ok_or_else(|| protocol("COPY TO schema is too large"))
    })?;
    let schema_charge = memory.reserve(bound)?;
    let fields = schema.fields().iter().map(|field| rtdb_columnar::to_field(&field.name, &field.data_type))
        .collect::<Result<Vec<_>>>()?;
    let schema = DeclaredSchema::with_reservation(arrow_schema::Schema::new(fields), schema_charge);
    let labels = match format {
        CopyOutFormat::ArrowStream => None,
        CopyOutFormat::HistoryLabelDictionaryV1 => history_labels::HistoryLabels::for_schema(schema.schema()),
    };
    let wire_schema = labels.as_ref().map_or(schema.schema(), |labels| &labels.schema);
    let (mut encoder, header) = CopyArrowEncoder::new(wire_schema)?;
    wire.send(b'H', &[1, 0, 1, 0, 1]).await?;
    send_copy_arrow(wire, std::slice::from_ref(&header), usize::MAX).await?;
    drop(header);
    wire.flush().await?;
    let mut count = 0u64;
    loop {
        let encoded = cursor.encode_next_batch(|batch| -> Result<_> {
            let batch = schema.conform(batch.clone(), memory)?;
            let (dictionary, _dictionary_charge);
            let batch = if let Some(labels) = &labels {
                (dictionary, _dictionary_charge) = labels.encode(batch.batch(), memory)?;
                &dictionary
            } else {
                batch.batch()
            };
            let bytes = batch.get_array_memory_size().checked_mul(6)
                .and_then(|bytes| bytes.checked_add(bound)).ok_or_else(copy_output_size)?;
            let reservation = memory.reserve(bytes)?;
            let page = encoder.write(batch, labels.is_some(), reservation)?;
            Ok((batch.num_rows() as u64, page))
        }).await?;
        let Some(encoded) = encoded else { break };
        let (rows, page) = encoded.inspect_err(|_| cursor.record_consumer_error())?;
        count = count.checked_add(rows).ok_or_else(|| protocol("COPY TO row count overflow"))?;
        let grant = cursor.delivery_grant().await?;
        grant.deliver(async {
            send_copy_arrow(wire, &page.messages, COPY_CHUNK_BYTES).await?;
            drop(page);
            wire.flush().await
        }).await.map_err(crate::wire::delivery_error)?;
    }
    let grant = cursor.delivery_grant().await?;
    grant.deliver(async {
        wire.send(b'd', &COPY_ARROW_END).await?;
        wire.empty(b'c').await?;
        wire.string(b'C', &format!("COPY {count}")).await?;
        if grant.is_bounded() { wire.flush().await?; }
        Ok(())
    }).await.map_err(crate::wire::delivery_error)?;
    grant.observe_answer();
    Ok(())
}

fn copy_output_size() -> Anomaly { protocol("COPY TO batch is too large") }

fn arrow_output_error(error: arrow_schema::ArrowError) -> Anomaly {
    Anomaly::incorrect("xtdb.pgwire/copy-output", error.to_string())
}

type ArrowCopyReader = Box<dyn Iterator<Item = Result<RecordBatch>> + Send>;

struct ArrowCopy {
    table: TableRef,
    batches: ArrowCopyReader,
    _decoder: Arc<MemoryReservation>,
    _input: Arc<MemoryReservation>,
}

struct RetainedInput {
    bytes: Vec<u8>,
    reservation: Arc<MemoryReservation>,
    limit: usize,
}

impl RetainedInput {
    fn append(&mut self, bytes: &[u8]) -> bool {
        let Some(length) = self.bytes.len().checked_add(bytes.len()).filter(|length| *length <= self.limit) else {
            return false;
        };
        if length > self.bytes.capacity() {
            if length > COPY_INPUT_SCRATCH_BYTES {
                let Ok(reservation) = self.reservation.budget().reserve(self.limit) else { return false; };
                let mut retained = Vec::new();
                if retained.try_reserve_exact(self.limit).is_err() { return false; }
                retained.extend_from_slice(&self.bytes);
                self.bytes = retained;
                self.reservation = reservation;
            } else if self.bytes.try_reserve_exact(COPY_INPUT_SCRATCH_BYTES - self.bytes.len()).is_err()
            {
                return false;
            }
        }
        self.bytes.extend_from_slice(bytes);
        true
    }

    fn is_extended(&self) -> bool { self.bytes.capacity() > COPY_INPUT_SCRATCH_BYTES }
}

impl AsRef<[u8]> for RetainedInput {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}

enum CopyStorage {
    Empty,
    Retained(RetainedInput),
    Spool(File),
}

pub(crate) struct CopyInput {
    storage: CopyStorage,
    format: CopyFormat,
    table: TableRef,
    length: u64,
    limit: u64,
    _reservation: Arc<MemoryReservation>,
}

impl CopyInput {
    pub fn new(table: Vec<Identifier>, format: Option<CopyFormat>, memory: &Arc<MemoryBudget>, limit: u64, autocommit: bool) -> Result<Self> {
        let format = format.ok_or_else(|| Anomaly::incorrect("xtdb.pgwire/invalid-copy-format", "COPY IN requires a valid format: 'arrow-file', 'arrow-stream', 'transit-json', 'transit-msgpack'").with_context("format", serde_json::Value::Null))?;
        let table = match table.as_slice() { [table] => TableRef::new("public", table.normalized()), [schema, table] => TableRef::new(schema.normalized(), table.normalized()), _ => return Err(protocol("COPY table requires a table or schema.table")) };
        let reservation = memory.reserve(table.schema.len() + table.table.len() + COPY_INPUT_SCRATCH_BYTES)?;
        let retention_limit = match format {
            CopyFormat::ArrowStream if autocommit => COPY_RETAINED_STREAM_BYTES,
            CopyFormat::ArrowStream => COPY_INPUT_SCRATCH_BYTES,
            CopyFormat::ArrowFile if autocommit => COPY_INPUT_SCRATCH_BYTES,
            _ => 0,
        };
        let storage = if retention_limit == 0 { CopyStorage::Empty } else {
            CopyStorage::Retained(RetainedInput {
                bytes: Vec::new(),
                reservation: Arc::clone(&reservation),
                limit: limit.min(retention_limit as u64) as usize,
            })
        };
        Ok(Self {
            storage,
            format,
            table,
            length: 0,
            limit,
            _reservation: reservation,
        })
    }
    pub fn wire_format(&self) -> Format { if self.format == CopyFormat::TransitJson { Format::Text } else { Format::Binary } }
    pub fn append(&mut self, Frame { body, mut charge, .. }: Frame) -> Result<()> {
        let length = self.checked_length(body.len())?;
        if body.is_empty() { return Ok(()); }
        if let CopyStorage::Retained(input) = &mut self.storage
            && input.bytes.is_empty() && body.capacity() <= input.limit
            && let Some(frame_charge) = Arc::get_mut(&mut charge)
            && frame_charge.bytes() >= body.capacity()
        {
            if body.capacity() > COPY_INPUT_SCRATCH_BYTES {
                frame_charge.shrink_to(body.capacity())?;
                input.reservation = charge;
            }
            input.bytes = body;
            self.length = length;
            return Ok(());
        }
        self.append_bytes(&body, length)
    }

    fn checked_length(&self, size: usize) -> Result<u64> {
        self.length.checked_add(size as u64).filter(|length| *length <= self.limit)
            .ok_or_else(|| Anomaly::new(rtdb_types::AnomalyCategory::Busy, "xtdb.pgwire/copy-limit", "COPY input exceeds its disk limit"))
    }

    fn append_bytes(&mut self, bytes: &[u8], length: u64) -> Result<()> {
        match &mut self.storage {
            CopyStorage::Retained(input) if self.format == CopyFormat::ArrowStream => {
                if !input.append(bytes) {
                    self.storage = CopyStorage::Spool(spill(input.as_ref(), bytes)?);
                }
            }
            CopyStorage::Retained(input) => self.storage = CopyStorage::Spool(spill(input.as_ref(), bytes)?),
            CopyStorage::Empty => self.storage = CopyStorage::Spool(spill(&[], bytes)?),
            CopyStorage::Spool(file) => file.write_all(bytes).map_err(io_error)?,
        }
        self.length = length;
        Ok(())
    }
    pub async fn finish(mut self, session: &mut NativeSession, node: &Node, cancellation: &CancellationToken) -> Result<u64> {
        if let CopyStorage::Spool(file) = &mut self.storage { file.flush().map_err(io_error)?; }
        if session.state().is_read_only() { return Err(Anomaly::incorrect("xtdb/copy-in-read-only-tx", "COPY is not allowed in a READ ONLY transaction")); }
        match self.format {
            CopyFormat::TransitJson | CopyFormat::TransitMsgpack => {
                let size = usize::try_from(self.length).map_err(io_error)?;
                let _scratch = node.memory().reserve(size.checked_mul(256).and_then(|n| n.checked_add(65_536)).ok_or_else(|| protocol("COPY allocation overflow"))?)?;
                let owns_transaction = session.state().status() == TransactionStatus::Idle;
                let mut file = match self.storage {
                    CopyStorage::Spool(file) => file,
                    _ => tempfile::tempfile().map_err(io_error)?,
                };
                let documents = transit::read(&mut file, self.format, size, owns_transaction, node.memory())?;
                let count = documents.row_count() as u64;
                Self::submit(|| documents.into_operation(self.table.clone(), node.memory()), session, node, cancellation).await?;
                Ok(count)
            }
            CopyFormat::ArrowFile | CopyFormat::ArrowStream => {
                let autocommit = session.state().status() == TransactionStatus::Idle;
                let ArrowCopy { table, batches, _decoder, _input } = self.arrow_reader(node.memory(), autocommit)?;
                let mut count = 0u64;
                for batch in batches {
                    if cancellation.is_cancelled() { return Err(cancelled()); }
                    let batch = batch?;
                    count = count.checked_add(batch.num_rows() as u64).ok_or_else(|| protocol("COPY row count overflow"))?;
                    // An explicit transaction can hold this batch after COPY releases its decoder reservation.
                    let batch = OwnedBatch::new(batch, node.memory())?;
                    if let Some(documents) = ArrowDocuments::from_ingest_batch(&batch, node.memory())? {
                        let operation = TxOp::PutArrowDocs { table: table.clone(), documents };
                        Self::submit(|| Ok(operation), session, node, cancellation).await?;
                    }
                }
                Ok(count)
            }
        }
    }

    fn arrow_reader(self, memory: &Arc<MemoryBudget>, autocommit: bool) -> Result<ArrowCopy> {
        let Self { storage, format, table, length, _reservation: input, .. } = self;
        let is_file = format == CopyFormat::ArrowFile;
        let mut file = match storage {
            CopyStorage::Retained(retained) => {
                let extended = retained.is_extended();
                if autocommit || !extended {
                    let mut cursor = Cursor::new(retained.as_ref());
                    let admission = if is_file {
                        arrow_file_allocation(&mut cursor, length, memory)
                    } else {
                        arrow_stream_allocation(&mut cursor, length, memory, None)
                    }.and_then(|bound| memory.reserve(bound));
                    match admission {
                        Ok(decoder) => {
                            // Explicit transactions can retain decoded batches after COPY releases its input scratch.
                            let batches: ArrowCopyReader = match (is_file, autocommit) {
                                (true, true) => {
                                    let bytes = Bytes::from_owner(retained);
                                    let reader = Cursor::new(bytes.clone());
                                    let retain = move |range: std::ops::Range<u64>| {
                                        let range = range.start as usize..range.end as usize;
                                        (range.end <= bytes.len()).then(|| bytes.slice(range))
                                    };
                                    // SAFETY: the cursor and callback share this immutable input allocation.
                                    Box::new(unsafe { rtdb_format::ipc::ArrowFile::from_reader_with_retained_ranges(reader, None, retain) }?)
                                }
                                (true, false) => Box::new(rtdb_format::ipc::ArrowFile::from_reader(Cursor::new(retained.bytes), None)?),
                                (false, true) => Box::new(rtdb_format::ipc::ArrowStream::from_bytes(Bytes::from_owner(retained), None)?),
                                (false, false) => Box::new(rtdb_format::ipc::ArrowStream::from_reader(Cursor::new(retained.bytes), None)?),
                            };
                            return Ok(ArrowCopy { table, batches, _decoder: decoder, _input: input });
                        }
                        Err(error) if !extended || !error.is(AnomalyKind::ResourceExhausted) => return Err(error),
                        Err(_) => {}
                    }
                }
                let mut file = spill(retained.as_ref(), &[])?;
                file.flush().map_err(io_error)?;
                file
            }
            CopyStorage::Spool(file) => file,
            CopyStorage::Empty => tempfile::tempfile().map_err(io_error)?,
        };
        let mut headers = StreamHeaders::default();
        let spooled_length = file.metadata().map_err(io_error)?.len();
        let bound = if is_file {
            arrow_file_allocation(&mut file, spooled_length, memory)?
        } else {
            arrow_stream_allocation(&mut file, spooled_length, memory, Some(&mut headers))?
        };
        let decoder = memory.reserve(bound)?;
        file.seek(SeekFrom::Start(0)).map_err(io_error)?;
        #[cfg(unix)]
        let ranges = if autocommit {
            // SAFETY: this method consumed the only writer and will use its descriptor only for reads and seeks.
            unsafe { spool::retained_ranges(&file, length, &input, &decoder) }
        } else { None };
        #[cfg(unix)]
        if let Some(retain) = ranges {
            let batches: ArrowCopyReader = if is_file {
                // SAFETY: both views come from the same completed, immutable spool descriptor.
                Box::new(unsafe { rtdb_format::ipc::ArrowFile::from_reader_with_retained_ranges(file, None, retain) }?)
            } else {
                Box::new(rtdb_format::ipc::ArrowStream::from_reader_with_retained_ranges(headers.replay(file), None, retain)?)
            };
            return Ok(ArrowCopy { table, batches, _decoder: decoder, _input: input });
        }
        let batches: ArrowCopyReader = if is_file {
            Box::new(rtdb_format::ipc::ArrowFile::from_reader(file, None)?)
        } else {
            Box::new(rtdb_format::ipc::ArrowStream::from_reader(headers.replay(file), None)?)
        };
        Ok(ArrowCopy { table, batches, _decoder: decoder, _input: input })
    }

    async fn submit(operation: impl FnOnce() -> Result<TxOp>, session: &mut NativeSession, node: &Node, cancellation: &CancellationToken) -> Result<()> {
        let owns_transaction = session.state().status() == TransactionStatus::Idle;
        if owns_transaction { session.state_mut().begin_with(TransactionSettings { access: Some(AccessSettings::ReadWrite(Default::default())), ..Default::default() })?; }
        let work = async {
            session.state_mut().execute_operation(operation()?).await?;
            if owns_transaction { let _sample = rtdb_plan::ExecutionSample::new(node.execution_observer(), rtdb_plan::ExecutionTimer::PgwireTransaction); if let CommitResult::Executed(tx) = session.state_mut().commit_tx(None).await? { if let Some(error) = tx.error() { return Err(error.clone()); } } }
            Ok(())
        };
        let result = cancellation.run_until_cancelled(work).await.unwrap_or_else(|| Err(cancelled()));
        if result.is_err() && owns_transaction { session.state_mut().rollback_tx()?; }
        result
    }
}

fn spill(prefix: &[u8], bytes: &[u8]) -> Result<File> {
    let mut file = tempfile::tempfile().map_err(io_error)?;
    file.write_all(prefix).map_err(io_error)?;
    file.write_all(bytes).map_err(io_error)?;
    Ok(file)
}

fn arrow_file_allocation(file: &mut (impl Read + Seek), length: u64, memory: &Arc<MemoryBudget>) -> Result<usize> {
    let mut permanent = 65_536usize; let mut record = 0usize;
    if length < 10 { return Err(protocol("Arrow file has no footer")); }
    file.seek(SeekFrom::End(-10)).map_err(io_error)?; let mut tail = [0; 10]; file.read_exact(&mut tail).map_err(io_error)?;
    let size = arrow_ipc::reader::read_footer_length(tail).map_err(io_error)?;
    let start = length.checked_sub(size as u64 + 10).ok_or_else(|| protocol("Arrow footer exceeds file"))?;
    let _footer = memory.reserve(size.checked_mul(64).and_then(|n| n.checked_add(65_536)).ok_or_else(|| protocol("Arrow footer allocation overflow"))?)?;
    let mut bytes = vec![0; size]; file.seek(SeekFrom::Start(start)).map_err(io_error)?; file.read_exact(&mut bytes).map_err(io_error)?;
    let footer = arrow_ipc::root_as_footer(&bytes).map_err(io_error)?; permanent = add(permanent, size.checked_mul(64).ok_or_else(|| protocol("Arrow footer overflow"))?)?;
    rtdb_format::ipc::validate_decimal_metadata(footer.schema().ok_or_else(|| protocol("Arrow footer has no schema"))?)?;
    for (dictionary, blocks) in [(true, footer.dictionaries()), (false, footer.recordBatches())] {
        if let Some(blocks) = blocks {
            for block in blocks {
                let offset = u64::try_from(block.offset()).map_err(io_error)?;
                file.seek(SeekFrom::Start(offset)).map_err(io_error)?;
                let (bound, end, _) = message_allocation(file, start, memory, None)?
                    .ok_or_else(|| protocol("Missing Arrow file message"))?;
                if end > start { return Err(protocol("Arrow block exceeds footer")); }
                if dictionary { permanent = add(permanent, bound)?; } else { record = record.max(bound); }
            }
        }
    }
    add(permanent, record)
}

fn arrow_stream_allocation(
    reader: &mut (impl Read + Seek), length: u64, memory: &Arc<MemoryBudget>, mut headers: Option<&mut StreamHeaders>,
) -> Result<usize> {
    let mut permanent = 65_536usize;
    let mut record = 0usize;
    reader.seek(SeekFrom::Start(0)).map_err(io_error)?;
    while let Some((bound, _, dictionary)) = message_allocation(reader, length, memory, headers.as_deref_mut())? {
        if dictionary { permanent = add(permanent, bound)?; } else { record = record.max(bound); }
    }
    add(permanent, record)
}

fn message_allocation(
    reader: &mut (impl Read + Seek), end: u64, memory: &Arc<MemoryBudget>, headers: Option<&mut StreamHeaders>,
) -> Result<Option<(usize, u64, bool)>> {
    let offset = reader.stream_position().map_err(io_error)?;
    if offset == end { return Ok(None); }
    let mut prefix = [0; 8];
    reader.read_exact(&mut prefix[..4]).map_err(io_error)?;
    let mut length = i32::from_le_bytes(prefix[..4].try_into().unwrap());
    let prefix = if length == -1 {
        reader.read_exact(&mut prefix[4..]).map_err(io_error)?;
        length = i32::from_le_bytes(prefix[4..].try_into().unwrap());
        &prefix[..]
    } else {
        &prefix[..4]
    };
    if length == 0 {
        if let Some(headers) = headers { headers.retain(offset, prefix, &[]); }
        return Ok(None);
    }
    let length = usize::try_from(length).map_err(io_error)?;
    let _metadata = memory.reserve(length.checked_mul(64).and_then(|n| n.checked_add(8192)).ok_or_else(|| protocol("IPC metadata allocation overflow"))?)?;
    let mut metadata = vec![0; length]; reader.read_exact(&mut metadata).map_err(io_error)?;
    let message = arrow_ipc::root_as_message(&metadata).map_err(io_error)?;
    if let Some(schema) = message.header_as_schema() { rtdb_format::ipc::validate_decimal_metadata(schema)?; }
    let body = reader.stream_position().map_err(io_error)?; let body_length = u64::try_from(message.bodyLength()).map_err(io_error)?; let next = body.checked_add(body_length).filter(|next| *next <= end).ok_or_else(|| protocol("IPC body exceeds file"))?;
    let dictionary = message.header_as_record_batch().is_none();
    let batch = message.header_as_record_batch().or_else(|| message.header_as_dictionary_batch().and_then(|dictionary| dictionary.data()));
    let mut bound = add(length.checked_mul(64).ok_or_else(|| protocol("IPC metadata allocation overflow"))?, usize::try_from(body_length).map_err(io_error)?.checked_mul(4).ok_or_else(|| protocol("IPC body allocation overflow"))?)?;
    if let Some(batch) = batch {
        if let Some(nodes) = batch.nodes() { for node in nodes { let rows = usize::try_from(node.length()).map_err(io_error)?; bound = add(bound, rows.checked_add(1).and_then(|n| n.checked_mul(4096)).ok_or_else(|| protocol("IPC node allocation overflow"))?)?; } }
        if let Some(buffers) = batch.buffers() { for buffer in buffers {
            let start = u64::try_from(buffer.offset()).map_err(io_error)?; let size = u64::try_from(buffer.length()).map_err(io_error)?;
            if start.checked_add(size).is_none_or(|end| end > body_length) { return Err(protocol("Arrow buffer exceeds IPC body")); }
            let decoded = if batch.compression().is_some() && size != 0 { if size < 8 { return Err(protocol("Compressed Arrow buffer has no length")); } reader.seek(SeekFrom::Start(body + start)).map_err(io_error)?; let mut value = [0; 8]; reader.read_exact(&mut value).map_err(io_error)?; let value = i64::from_le_bytes(value); if value == -1 { size - 8 } else { u64::try_from(value).map_err(io_error)? } } else { size };
            bound = add(bound, usize::try_from(decoded).map_err(io_error)?.checked_mul(4).and_then(|n| n.checked_add(512)).ok_or_else(|| protocol("Arrow decompression allocation overflow"))?)?;
        } }
    }
    reader.seek(SeekFrom::Start(next)).map_err(io_error)?;
    if let Some(headers) = headers { headers.retain(offset, prefix, &metadata); }
    Ok(Some((bound, next, dictionary)))
}
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(|| protocol("COPY allocation overflow")) }
fn io_error(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("xtdb.pgwire/copy-input", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::{
        COPY_ARROW_END, COPY_CHUNK_BYTES, COPY_INPUT_SCRATCH_BYTES, COPY_RETAINED_STREAM_BYTES,
        CopyArrowEncoder, CopyFormat, CopyInput, CopyStorage, Cursor, Frame, Identifier, MemoryBudget,
        OwnedBatch, Read, RecordBatch, Seek, SeekFrom, StreamHeaders, Write, Writer,
        arrow_file_allocation, arrow_stream_allocation, send_copy_arrow,
    };
    use rtdb_types::Value;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    fn append(copy: &mut CopyInput, bytes: &[u8]) -> rtdb_types::Result<()> {
        copy.append_bytes(bytes, copy.checked_length(bytes.len())?)
    }

    fn spool_bytes(copy: &mut CopyInput) -> Option<Vec<u8>> {
        let CopyStorage::Spool(file) = &mut copy.storage else { return None; };
        let position = file.stream_position().unwrap();
        file.rewind().unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        file.seek(SeekFrom::Start(position)).unwrap();
        Some(bytes)
    }

    #[test]
    fn missing_and_unrecognized_copy_formats_fail_before_table_or_file_admission() {
        for options in ["", "FORMAT 'unknown'", "FORMAT 'ARROW-STREAM'", "FORMAT 'unknown', FORMAT 'arrow-stream'"] {
            let source = format!("COPY docs FROM STDIN WITH ({options})");
            let Some(rtdb_plan::ControlStatement::CopyIn { format, .. }) = rtdb_sql::parse_control(&source, &rtdb_sql::PrepareOptions::default()).unwrap() else { panic!() };
            let memory = MemoryBudget::new(0);
            let error = match CopyInput::new(Vec::new(), format, &memory, 0, true) { Err(error) => error, Ok(_) => panic!() };
            assert_eq!(error.category, rtdb_types::AnomalyCategory::Incorrect);
            assert_eq!(error.code, "xtdb.pgwire/invalid-copy-format");
            assert_eq!(error.context.get("format"), Some(&serde_json::Value::Null));
            assert_eq!(serde_json::to_value(&error).unwrap()["message"], "COPY IN requires a valid format: 'arrow-file', 'arrow-stream', 'transit-json', 'transit-msgpack'");
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn copy_checks_disk_limit_before_spooling_and_releases_memory_on_drop() {
        for format in [CopyFormat::TransitJson, CopyFormat::ArrowStream] {
            let memory = MemoryBudget::new(1 << 20);
            let mut copy = CopyInput::new(vec![Identifier { text: "docs".into(), quoted: false }], Some(format), &memory, 3, true).unwrap();
            assert!(spool_bytes(&mut copy).is_none());
            assert_eq!(append(&mut copy, b"abcd").unwrap_err().code, "xtdb.pgwire/copy-limit");
            assert!(spool_bytes(&mut copy).is_none());
            append(&mut copy, b"abc").unwrap();
            assert_eq!(append(&mut copy, b"d").unwrap_err().code, "xtdb.pgwire/copy-limit");
            assert_eq!(spool_bytes(&mut copy).as_deref(), (format == CopyFormat::TransitJson).then_some(b"abc".as_slice()));
            assert_eq!(copy.length, 3); drop(copy); assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn malformed_arrow_lengths_are_rejected_before_large_allocation() {
        let memory = MemoryBudget::new(1 << 20); let mut file = NamedTempFile::new().unwrap();
        file.write_all(&(-1i32).to_le_bytes()).unwrap(); file.write_all(&i32::MAX.to_le_bytes()).unwrap();
        let mut input = file.reopen().unwrap();
        assert!(arrow_stream_allocation(&mut input, 8, &memory, Some(&mut StreamHeaders::default())).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn copy_arrow_inspection_and_readers_preserve_signed_decimal_parameters() {
        for (precision, scale) in [(3, i32::MIN), (3, i32::MAX), (i32::MIN, 0), (256, 129), (i32::MAX, i32::MAX)] {
            let memory = MemoryBudget::new(1 << 24);
            let ty = rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Decimal { precision, scale, bit_width: 128 });
            let value = Value::Decimal(rtdb_types::DecimalValue::new((-123i128).to_be_bytes().to_vec(), precision, scale, 128).unwrap());
            let schema = Arc::new(arrow_schema::Schema::new(vec![rtdb_columnar::to_field("value", &ty).unwrap()]));
            let page = RecordBatch::try_new(schema.clone(), vec![rtdb_columnar::copy_array(&ty, std::slice::from_ref(&value)).unwrap()]).unwrap();
            for is_file in [false, true] {
                let bytes = if is_file { rtdb_format::ipc::write_file(&schema, std::slice::from_ref(&page)).unwrap() } else { rtdb_format::ipc::write_stream(&schema, std::slice::from_ref(&page)).unwrap() };
                let mut file = NamedTempFile::new().unwrap(); file.write_all(&bytes).unwrap();
                let mut input = file.reopen().unwrap();
                let mut headers = StreamHeaders::default();
                let bound = if is_file { arrow_file_allocation(&mut input, bytes.len() as u64, &memory) }
                    else { arrow_stream_allocation(&mut input, bytes.len() as u64, &memory, Some(&mut headers)) }.unwrap();
                let reservation = memory.reserve(bound).unwrap(); input.seek(SeekFrom::Start(0)).unwrap();
                let page = if is_file { rtdb_format::ipc::ArrowFile::from_reader(input, None).unwrap().next().unwrap().unwrap() }
                    else { rtdb_format::ipc::ArrowStream::from_reader(headers.replay(input), None).unwrap().next().unwrap().unwrap() };
                assert_eq!(rtdb_columnar::read_value(page.schema().field(0), page.column(0).as_ref(), 0).unwrap(), value);
                drop(page); drop(reservation); assert_eq!(memory.used_bytes(), 0);
            }
        }
    }

    #[test]
    fn fragmented_stream_reuses_bounded_bytes_and_keeps_compressed_dictionary_and_end_semantics() {
        use arrow_array::{ArrayRef, builder::StringDictionaryBuilder, types::Int8Type};
        use arrow_ipc::{CompressionType, writer::IpcWriteOptions};

        let memory = MemoryBudget::new(1 << 24);
        let mut values = StringDictionaryBuilder::<Int8Type>::new();
        for value in ["first", "second", "first"] { values.append(value).unwrap(); }
        let batch = RecordBatch::try_from_iter([("value", Arc::new(values.finish()) as ArrayRef)]).unwrap();
        let options = IpcWriteOptions::default().try_with_compression(Some(CompressionType::ZSTD)).unwrap();
        let stream = rtdb_format::ipc::write_arrow_stream(batch.schema_ref(), &[batch.clone(), batch.clone()], options).unwrap();
        let end = stream.len() - 8;
        let terminator = &stream[end..];
        assert_eq!(terminator, &[255, 255, 255, 255, 0, 0, 0, 0]);
        assert!(stream.len() < COPY_INPUT_SCRATCH_BYTES);
        let mut decode_bound = None;
        for (marker, length, autocommit) in [
            (terminator, stream.len() + 1, true),
            (&[0; 4], end + 5, true),
            (&[], end, true),
            (terminator, COPY_INPUT_SCRATCH_BYTES, false),
            (terminator, COPY_INPUT_SCRATCH_BYTES + 1, false),
        ] {
            let retains_bytes = length <= COPY_INPUT_SCRATCH_BYTES;
            let mut input = stream[..end].to_vec();
            input.extend_from_slice(marker);
            input.resize(length, 255);
            let table = vec![Identifier { text: "docs".into(), quoted: false }];
            let mut copy = CopyInput::new(table, Some(CopyFormat::ArrowStream), &memory, length as u64, autocommit).unwrap();
            let scratch = memory.used_bytes();
            for chunk in input.chunks(11) { append(&mut copy, chunk).unwrap(); }
            assert_eq!(matches!(&copy.storage, CopyStorage::Retained(stream) if stream.as_ref() == input.as_slice()), retains_bytes);
            assert_eq!(memory.used_bytes(), scratch);
            assert_eq!(append(&mut copy, &[0]).unwrap_err().code, "xtdb.pgwire/copy-limit");
            assert_eq!(spool_bytes(&mut copy).as_deref(), (!retains_bytes).then_some(input.as_slice()));
            let mut reader = copy.arrow_reader(&memory, autocommit).unwrap();
            assert_eq!(*decode_bound.get_or_insert(reader._decoder.bytes()), reader._decoder.bytes());
            assert_eq!(reader.batches.by_ref().collect::<rtdb_types::Result<Vec<_>>>().unwrap(), [batch.clone(), batch.clone()]);
            drop(reader);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn arrow_stream_spills_the_retained_prefix_once_and_keeps_the_disk_limit() {
        for (autocommit, cap) in [(false, COPY_INPUT_SCRATCH_BYTES), (true, COPY_RETAINED_STREAM_BYTES)] {
            let memory = MemoryBudget::new(1 << 20);
            let prefix = vec![7; cap];
            let limit = (prefix.len() + 2) as u64;
            let table = vec![Identifier { text: "docs".into(), quoted: false }];
            let mut copy = CopyInput::new(table, Some(CopyFormat::ArrowStream), &memory, limit, autocommit).unwrap();
            let baseline = memory.used_bytes();
            append(&mut copy, &prefix).unwrap();
            assert_eq!(append(&mut copy, b"abc").unwrap_err().code, "xtdb.pgwire/copy-limit");
            assert!(spool_bytes(&mut copy).is_none());
            append(&mut copy, b"a").unwrap();
            append(&mut copy, b"b").unwrap();
            assert_eq!(copy.length, limit);
            assert_eq!(memory.used_bytes(), baseline);
            let file = spool_bytes(&mut copy).unwrap();
            assert_eq!(&file[..prefix.len()], &prefix);
            assert_eq!(&file[prefix.len()..], b"ab");
        }
    }

    #[test]
    fn larger_arrow_stream_retains_its_input_or_spools_on_memory_pressure() {
        use arrow_array::{ArrayRef, BinaryArray};

        let memory = MemoryBudget::new(1 << 20);
        let payload = vec![42; COPY_INPUT_SCRATCH_BYTES];
        let batch = RecordBatch::try_from_iter([("payload", Arc::new(BinaryArray::from(vec![Some(payload.as_slice())])) as ArrayRef)]).unwrap();
        let bytes = rtdb_format::ipc::write_stream(batch.schema_ref(), &[batch.clone(), batch.clone()]).unwrap();
        let bound = arrow_stream_allocation(&mut Cursor::new(&bytes), bytes.len() as u64, &memory, None).unwrap();

        for pressure in [None, Some("append"), Some("preflight")] {
            let table = vec![Identifier { text: "docs".into(), quoted: false }];
            let mut copy = CopyInput::new(table, Some(CopyFormat::ArrowStream), &memory, u64::MAX, true).unwrap();
            let baseline = memory.used_bytes();
            let append_charge = (pressure == Some("append"))
                .then(|| memory.reserve(memory.available_bytes() - COPY_RETAINED_STREAM_BYTES + 1).unwrap());
            for chunk in bytes.chunks(137) { append(&mut copy, chunk).unwrap(); }
            drop(append_charge);
            let retained = pressure != Some("append");
            assert_eq!(matches!(&copy.storage, CopyStorage::Retained(_)), retained);
            assert_eq!(memory.used_bytes(), baseline + if retained { COPY_RETAINED_STREAM_BYTES } else { 0 });
            assert_eq!(spool_bytes(&mut copy).as_deref(), (!retained).then_some(bytes.as_slice()));

            let preflight_charge = (pressure == Some("preflight"))
                .then(|| memory.reserve(memory.available_bytes() - bound + 1).unwrap());
            let mut reader = copy.arrow_reader(&memory, true).unwrap();
            assert_eq!(reader._decoder.bytes(), bound);
            assert_eq!(reader.batches.by_ref().collect::<rtdb_types::Result<Vec<_>>>().unwrap(), [batch.clone(), batch.clone()]);
            drop(preflight_charge);
            drop(reader);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    fn arrow_input(format: CopyFormat, payload_size: usize, pages: usize) -> (Vec<u8>, RecordBatch) {
        use arrow_array::{ArrayRef, BinaryArray};
        use arrow_ipc::writer::{FileWriter, StreamWriter};

        let payload = vec![42; payload_size];
        let column = Arc::new(BinaryArray::from(vec![Some(payload.as_slice())])) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("payload", column)]).unwrap();
        let mut bytes = Vec::new();
        if format == CopyFormat::ArrowFile {
            let mut writer = FileWriter::try_new(&mut bytes, batch.schema_ref()).unwrap();
            for _ in 0..pages { writer.write(&batch).unwrap(); }
            writer.finish().unwrap();
        } else {
            let mut writer = StreamWriter::try_new(&mut bytes, batch.schema_ref()).unwrap();
            for _ in 0..pages { writer.write(&batch).unwrap(); }
            writer.finish().unwrap();
        }
        (bytes, batch)
    }

    fn copy_data(memory: &Arc<MemoryBudget>, body: Vec<u8>) -> Frame {
        let charge = memory.reserve(body.len()).unwrap();
        Frame { tag: b'd', body, charge }
    }

    #[test]
    fn sole_arrow_frame_is_adopted_and_explicit_decode_detaches() {
        for format in [CopyFormat::ArrowFile, CopyFormat::ArrowStream] {
            for autocommit in [true, false] {
                let memory = MemoryBudget::new(1 << 22);
                let (bytes, expected) = arrow_input(format, 64, 1);
                let bytes = bytes.into_boxed_slice().into_vec();
                let start = bytes.as_ptr() as usize;
                let len = bytes.len();
                let table = vec![Identifier { text: "docs".into(), quoted: false }];
                let mut copy = CopyInput::new(table, Some(format), &memory, len as u64, true).unwrap();
                let scratch = memory.used_bytes();
                copy.append(copy_data(&memory, bytes)).unwrap();
                assert!(matches!(&copy.storage, CopyStorage::Retained(input) if input.bytes.as_ptr() as usize == start));
                assert_eq!(memory.used_bytes(), scratch);
                let mut reader = copy.arrow_reader(&memory, autocommit).unwrap();
                let batch = reader.batches.next().unwrap().unwrap();
                let payload = batch.column(0).as_any().downcast_ref::<arrow_array::BinaryArray>().unwrap().value(0);
                let shares_input = autocommit && start % std::mem::align_of::<u128>() == 0;
                assert_eq!((start..start + len).contains(&(payload.as_ptr() as usize)), shares_input);
                assert_eq!(batch, expected);
                drop(reader);
                assert_eq!(memory.used_bytes(), if shares_input { scratch } else { 0 });
                drop(batch);
                assert_eq!(memory.used_bytes(), 0);
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn large_spools_map_one_record_window_and_fall_back_while_it_is_retained() {
        for format in [CopyFormat::ArrowFile, CopyFormat::ArrowStream] {
            let memory = MemoryBudget::new(1 << 23);
            let (bytes, expected) = arrow_input(format, COPY_RETAINED_STREAM_BYTES / 2 + 64, 2);
            let table = vec![Identifier { text: "docs".into(), quoted: false }];
            let mut copy = CopyInput::new(table, Some(format), &memory, bytes.len() as u64, true).unwrap();
            append(&mut copy, &bytes).unwrap();
            assert!(matches!(&copy.storage, CopyStorage::Spool(_)));
            let mut reader = copy.arrow_reader(&memory, true).unwrap();
            let charge = reader._decoder.bytes();
            let first = reader.batches.next().unwrap().unwrap();
            let second = reader.batches.next().unwrap().unwrap();
            assert_eq!(first, expected);
            assert_eq!(second, expected);
            assert!(reader.batches.next().is_none());
            drop(reader);
            assert_eq!(memory.used_bytes(), charge);
            drop(first);
            assert_eq!(memory.used_bytes(), 0);
            assert_eq!(second, expected);
        }
    }

    #[test]
    fn truncated_last_stream_body_refuses_before_a_reader_is_returned() {
        let memory = MemoryBudget::new(1 << 24);
        let (mut bytes, _) = arrow_input(CopyFormat::ArrowStream, COPY_RETAINED_STREAM_BYTES / 2 + 64, 2);
        bytes.truncate(bytes.len() - 9);
        let table = vec![Identifier { text: "docs".into(), quoted: false }];
        let mut copy = CopyInput::new(table, Some(CopyFormat::ArrowStream), &memory, bytes.len() as u64, true).unwrap();
        append(&mut copy, &bytes).unwrap();
        let error = copy.arrow_reader(&memory, true).err().unwrap();
        assert_eq!(error, super::protocol("IPC body exceeds file"));
        assert_eq!(memory.used_bytes(), 0);
    }

    pub(super) fn assert_copy_data_frames<'a>(mut actual: &[u8], expected: impl IntoIterator<Item = &'a [u8]>) {
        for expected in expected {
            let (header, payload) = actual.split_at(5);
            assert_eq!(header[0], b'd');
            let length = i32::from_be_bytes(header[1..].try_into().unwrap());
            assert_eq!(length as usize, expected.len() + 4);
            let (body, rest) = payload.split_at(expected.len());
            assert_eq!(body, expected);
            actual = rest;
        }
        assert!(actual.is_empty());
    }

    #[tokio::test]
    async fn copy_to_chunks_match_contiguous_arrow_ipc_frames() {
        use arrow_array::BinaryArray;
        use arrow_ipc::writer::StreamWriter;
        use arrow_schema::{DataType, Field, Schema};

        let bytes = vec![0xe1; COPY_CHUNK_BYTES * 2 + 13];
        let name = "payload".repeat(COPY_CHUNK_BYTES / 7 + 1);
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Binary, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BinaryArray::from(vec![Some(bytes.as_slice()), None]))],
        ).unwrap();
        let memory = MemoryBudget::new(1 << 24);
        let mut reference = StreamWriter::try_new(Vec::new(), &schema).unwrap();
        let (mut encoder, header) = CopyArrowEncoder::new(&schema).unwrap();
        let mut wire = Writer::new(Vec::new(), Arc::clone(&memory));
        send_copy_arrow(&mut wire, std::slice::from_ref(&header), usize::MAX).await.unwrap();
        let expected = std::mem::take(reference.get_mut());
        assert!(expected.len() > COPY_CHUNK_BYTES);
        assert_copy_data_frames(&wire.into_inner(), [expected.as_slice()]);
        drop(header);
        for _ in 0..2 {
            reference.write(&batch).unwrap();
            let source = OwnedBatch::new(batch.clone(), &memory).unwrap();
            let bound = 1 << 22;
            let page = encoder.write(source.batch(), false, memory.reserve(bound).unwrap()).unwrap();
            drop(source);
            assert_eq!(memory.used_bytes(), page._reservation.bytes());
            assert!(memory.used_bytes() < bound);

            let mut wire = Writer::new(Vec::new(), Arc::clone(&memory));
            send_copy_arrow(&mut wire, &page.messages, COPY_CHUNK_BYTES).await.unwrap();
            let expected = std::mem::take(reference.get_mut());
            assert!(expected.chunks(COPY_CHUNK_BYTES).len() > 2);
            assert_copy_data_frames(&wire.into_inner(), expected.chunks(COPY_CHUNK_BYTES));
            drop(page);
            assert_eq!(memory.used_bytes(), 0);
        }
        reference.finish().unwrap();
        let mut wire = Writer::new(Vec::new(), memory);
        wire.send(b'd', &COPY_ARROW_END).await.unwrap();
        assert_copy_data_frames(&wire.into_inner(), [reference.get_ref().as_slice()]);
    }
}
