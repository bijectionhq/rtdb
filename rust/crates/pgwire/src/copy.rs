use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use arrow_array::RecordBatch;
use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_node::{AccessSettings, CommitResult, NativeSession, Node, TransactionSettings, TransactionStatus};
use rtdb_pg_codec::Format;
use rtdb_plan::{CopyFormat, CopyOutFormat, Identifier};
use rtdb_tx::{ArrowDocuments, TxOp};
use rtdb_types::{Anomaly, Result, TableRef};
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;
use crate::error::{cancelled, protocol};

mod history_labels;
mod stream_headers;
mod transit;

use stream_headers::StreamHeaders;

const COPY_CHUNK_BYTES: usize = 64 * 1024;
const COPY_INPUT_SCRATCH_BYTES: usize = 8192;

struct CopyChunks {
    chunks: Vec<Vec<u8>>,
    frame_bytes: usize,
}

impl CopyChunks {
    fn new() -> Self {
        Self { chunks: Vec::new(), frame_bytes: usize::MAX }
    }

    fn take_header(&mut self) -> Vec<u8> {
        self.frame_bytes = COPY_CHUNK_BYTES;
        self.chunks.pop().expect("Arrow IPC writes one schema frame")
    }

    fn take_batch(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.chunks)
    }
}

impl Write for CopyChunks {
    fn write(&mut self, mut bytes: &[u8]) -> std::io::Result<usize> {
        let written = bytes.len();
        while !bytes.is_empty() {
            if self.chunks.last().is_none_or(|chunk| chunk.len() == self.frame_bytes) {
                self.chunks.push(Vec::new());
            }
            let chunk = self.chunks.last_mut().expect("A COPY chunk is open");
            let count = bytes.len().min(self.frame_bytes - chunk.len());
            let (prefix, rest) = bytes.split_at(count);
            let capacity = (chunk.len() + count).next_power_of_two();
            if capacity > chunk.capacity() {
                chunk.reserve_exact(capacity - chunk.len());
            }
            chunk.extend_from_slice(prefix);
            bytes = rest;
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

/// Encodes each batch while its source buffers are in scope and releases
/// them before waiting on the connection.
pub(crate) async fn write_arrow<W: tokio::io::AsyncWrite + Unpin>(
    mut cursor: rtdb_query::QueryCursor, format: CopyOutFormat, memory: &Arc<MemoryBudget>, wire: &mut crate::wire::Writer<W>,
) -> Result<()> {
    let schema = cursor.runtime_schema();
    let bound = schema.fields().iter().try_fold(8192usize, |bytes, field| {
        let field_bytes = field.data_type.retained_bytes()?.saturating_add(field.name.len());
        bytes.checked_add(field_bytes.saturating_mul(64).saturating_add(4096))
            .ok_or_else(|| protocol("COPY TO schema is too large"))
    })?;
    let _schema = memory.reserve(bound)?;
    let fields = schema.fields().iter().map(|field| rtdb_columnar::to_field(&field.name, &field.data_type))
        .collect::<Result<Vec<_>>>()?;
    let schema = arrow_schema::Schema::new(fields);
    let labels = match format {
        CopyOutFormat::ArrowStream => None,
        CopyOutFormat::HistoryLabelDictionaryV1 => history_labels::HistoryLabels::for_schema(&schema),
    };
    let wire_schema = labels.as_ref().map_or(&schema, |labels| labels.schema.as_ref());
    let mut encoder = arrow_ipc::writer::StreamWriter::try_new(CopyChunks::new(), wire_schema)
        .map_err(arrow_output_error)?;
    wire.send(b'H', &[1, 0, 1, 0, 1]).await?;
    wire.send(b'd', &encoder.get_mut().take_header()).await?;
    wire.flush().await?;
    let mut count = 0u64;
    loop {
        let encoded = cursor.encode_next_batch(|batch| -> Result<_> {
            let batch = rtdb_columnar::conform_batch(batch.clone(), &schema, memory)?;
            let (dictionary, _dictionary_charge);
            let batch = if let Some(labels) = &labels {
                (dictionary, _dictionary_charge) = labels.encode(batch.batch(), memory)?;
                &dictionary
            } else {
                batch.batch()
            };
            let bytes = batch.get_array_memory_size().checked_mul(6)
                .and_then(|bytes| bytes.checked_add(bound)).ok_or_else(|| protocol("COPY TO batch is too large"))?;
            let reservation = memory.reserve(bytes)?;
            let out = if labels.is_some() {
                // A fresh tracker emits replacement dictionaries without retaining spellings across delivery grants.
                let mut page = arrow_ipc::writer::StreamWriter::try_new(CopyChunks::new(), wire_schema)
                    .map_err(arrow_output_error)?;
                page.get_mut().take_header();
                page.write(batch).map_err(arrow_output_error)?;
                page.get_mut().take_batch()
            } else {
                encoder.write(batch).map_err(arrow_output_error)?;
                encoder.get_mut().take_batch()
            };
            Ok((batch.num_rows() as u64, out, reservation))
        }).await?;
        let Some(encoded) = encoded else { break };
        let (rows, chunks, _reservation) = encoded.inspect_err(|_| cursor.record_consumer_error())?;
        count = count.checked_add(rows).ok_or_else(|| protocol("COPY TO row count overflow"))?;
        let grant = cursor.delivery_grant().await?;
        grant.deliver(async {
            for chunk in chunks {
                wire.send(b'd', &chunk).await?;
            }
            wire.flush().await
        }).await.map_err(crate::wire::delivery_error)?;
    }
    encoder.finish().map_err(arrow_output_error)?;
    let grant = cursor.delivery_grant().await?;
    grant.deliver(async {
        wire.send(b'd', &encoder.get_ref().chunks[0]).await?;
        wire.empty(b'c').await?;
        wire.string(b'C', &format!("COPY {count}")).await?;
        if grant.is_bounded() { wire.flush().await?; }
        Ok(())
    }).await.map_err(crate::wire::delivery_error)?;
    grant.observe_answer();
    Ok(())
}

fn arrow_output_error(error: arrow_schema::ArrowError) -> Anomaly {
    Anomaly::incorrect("xtdb.pgwire/copy-output", error.to_string())
}

type ArrowCopyReader = Box<dyn Iterator<Item = Result<RecordBatch>> + Send>;

struct RetainedStream {
    bytes: Vec<u8>,
    _reservation: Arc<MemoryReservation>,
}

impl AsRef<[u8]> for RetainedStream {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}

pub(crate) struct CopyInput {
    file: NamedTempFile,
    format: CopyFormat,
    table: TableRef,
    length: u64,
    limit: u64,
    retained_stream: Option<Vec<u8>>,
    reservation: Arc<MemoryReservation>,
}

impl CopyInput {
    pub fn new(table: Vec<Identifier>, format: Option<CopyFormat>, memory: &Arc<MemoryBudget>, limit: u64) -> Result<Self> {
        let format = format.ok_or_else(|| Anomaly::incorrect("xtdb.pgwire/invalid-copy-format", "COPY IN requires a valid format: 'arrow-file', 'arrow-stream', 'transit-json', 'transit-msgpack'").with_context("format", serde_json::Value::Null))?;
        let table = match table.as_slice() { [table] => TableRef::new("public", table.normalized()), [schema, table] => TableRef::new(schema.normalized(), table.normalized()), _ => return Err(protocol("COPY table requires a table or schema.table")) };
        let reservation = memory.reserve(table.schema.len() + table.table.len() + COPY_INPUT_SCRATCH_BYTES)?;
        Ok(Self {
            file: NamedTempFile::new().map_err(io_error)?,
            format,
            table,
            length: 0,
            limit,
            retained_stream: (format == CopyFormat::ArrowStream).then(Vec::new),
            reservation,
        })
    }
    pub fn wire_format(&self) -> Format { if self.format == CopyFormat::TransitJson { Format::Text } else { Format::Binary } }
    pub async fn append(&mut self, bytes: &[u8]) -> Result<()> {
        let length = self.length.checked_add(bytes.len() as u64).filter(|length| *length <= self.limit).ok_or_else(|| Anomaly::new(rtdb_types::AnomalyCategory::Busy, "xtdb.pgwire/copy-limit", "COPY input exceeds its disk limit"))?;
        if let Some(stream) = &mut self.retained_stream {
            if bytes.is_empty() { return Ok(()); }
            if length <= COPY_INPUT_SCRATCH_BYTES as u64
                && stream.try_reserve_exact(COPY_INPUT_SCRATCH_BYTES - stream.len()).is_ok()
            {
                stream.extend_from_slice(bytes);
                self.length = length;
                return Ok(());
            }
            // A retry after a partial spill must overwrite the retained prefix.
            self.file.seek(SeekFrom::Start(0)).map_err(io_error)?;
            self.file.write_all(stream).map_err(io_error)?;
            self.retained_stream = None;
        }
        self.file.write_all(bytes).map_err(io_error)?;
        self.length = length;
        Ok(())
    }
    pub async fn finish(mut self, session: &mut NativeSession, node: &Node, cancellation: &CancellationToken) -> Result<u64> {
        self.file.flush().map_err(io_error)?;
        if session.state().is_read_only() { return Err(Anomaly::incorrect("xtdb/copy-in-read-only-tx", "COPY is not allowed in a READ ONLY transaction")); }
        match self.format {
            CopyFormat::TransitJson | CopyFormat::TransitMsgpack => {
                let size = usize::try_from(self.length).map_err(io_error)?;
                let _scratch = node.memory().reserve(size.checked_mul(256).and_then(|n| n.checked_add(65_536)).ok_or_else(|| protocol("COPY allocation overflow"))?)?;
                let owns_transaction = session.state().status() == TransactionStatus::Idle;
                let documents = transit::read(self.file.as_file_mut(), self.format, size, owns_transaction, node.memory())?;
                let count = documents.row_count() as u64;
                self.submit(|| documents.into_operation(self.table.clone(), node.memory()), session, node, cancellation).await?;
                Ok(count)
            }
            CopyFormat::ArrowFile | CopyFormat::ArrowStream => {
                let autocommit = session.state().status() == TransactionStatus::Idle;
                let (_arrow, reader) = self.arrow_reader(node.memory(), autocommit)?;
                let mut count = 0u64;
                for batch in reader {
                    if cancellation.is_cancelled() { return Err(cancelled()); }
                    let batch = batch?;
                    count = count.checked_add(batch.num_rows() as u64).ok_or_else(|| protocol("COPY row count overflow"))?;
                    // An explicit transaction can hold this batch after COPY releases its decoder reservation.
                    let batch = OwnedBatch::new(batch, node.memory())?;
                    if let Some(documents) = ArrowDocuments::from_ingest_batch(&batch, node.memory())? {
                        let operation = TxOp::PutArrowDocs { table: self.table.clone(), documents };
                        self.submit(|| Ok(operation), session, node, cancellation).await?;
                    }
                }
                Ok(count)
            }
        }
    }

    fn arrow_reader(&mut self, memory: &Arc<MemoryBudget>, autocommit: bool) -> Result<(Arc<MemoryReservation>, ArrowCopyReader)> {
        if let Some(bytes) = self.retained_stream.take() {
            let bound = arrow_stream_allocation(&mut Cursor::new(&bytes), self.length, memory, None)?;
            let reservation = memory.reserve(bound)?;
            // Explicit transactions can retain decoded batches after COPY releases its input scratch.
            let reader: ArrowCopyReader = if autocommit {
                let bytes = Bytes::from_owner(RetainedStream { bytes, _reservation: Arc::clone(&self.reservation) });
                Box::new(rtdb_format::ipc::ArrowStream::from_bytes(bytes, None)?)
            } else {
                Box::new(rtdb_format::ipc::ArrowStream::from_reader(Cursor::new(bytes), None)?)
            };
            return Ok((reservation, reader));
        }
        let mut file = self.file.reopen().map_err(io_error)?;
        let mut headers = StreamHeaders::default();
        let is_file = self.format == CopyFormat::ArrowFile;
        let bound = if is_file {
            arrow_file_allocation(&mut file, memory)?
        } else {
            let length = file.metadata().map_err(io_error)?.len();
            arrow_stream_allocation(&mut file, length, memory, Some(&mut headers))?
        };
        let reservation = memory.reserve(bound)?;
        file.seek(SeekFrom::Start(0)).map_err(io_error)?;
        let reader: ArrowCopyReader = if is_file {
            Box::new(rtdb_format::ipc::ArrowFile::from_reader(file, None)?)
        } else {
            Box::new(rtdb_format::ipc::ArrowStream::from_reader(headers.replay(file), None)?)
        };
        Ok((reservation, reader))
    }

    async fn submit(&self, operation: impl FnOnce() -> Result<TxOp>, session: &mut NativeSession, node: &Node, cancellation: &CancellationToken) -> Result<()> {
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

fn arrow_file_allocation(file: &mut File, memory: &Arc<MemoryBudget>) -> Result<usize> {
    let length = file.metadata().map_err(io_error)?.len();
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
        COPY_CHUNK_BYTES, COPY_INPUT_SCRATCH_BYTES, CopyChunks, CopyFormat, CopyInput, Identifier,
        MemoryBudget, NamedTempFile, RecordBatch, Seek, SeekFrom, StreamHeaders, Write, arrow_file_allocation,
        arrow_stream_allocation,
    };
    use rtdb_types::Value;
    use std::sync::Arc;

    #[test]
    fn missing_and_unrecognized_copy_formats_fail_before_table_or_file_admission() {
        for options in ["", "FORMAT 'unknown'", "FORMAT 'ARROW-STREAM'", "FORMAT 'unknown', FORMAT 'arrow-stream'"] {
            let source = format!("COPY docs FROM STDIN WITH ({options})");
            let Some(rtdb_plan::ControlStatement::CopyIn { format, .. }) = rtdb_sql::parse_control(&source, &rtdb_sql::PrepareOptions::default()).unwrap() else { panic!() };
            let memory = MemoryBudget::new(0);
            let error = match CopyInput::new(Vec::new(), format, &memory, 0) { Err(error) => error, Ok(_) => panic!() };
            assert_eq!(error.category, rtdb_types::AnomalyCategory::Incorrect);
            assert_eq!(error.code, "xtdb.pgwire/invalid-copy-format");
            assert_eq!(error.context.get("format"), Some(&serde_json::Value::Null));
            assert_eq!(serde_json::to_value(&error).unwrap()["message"], "COPY IN requires a valid format: 'arrow-file', 'arrow-stream', 'transit-json', 'transit-msgpack'");
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn copy_disk_limit_and_drop_release_temp_file_and_memory() {
        for format in [CopyFormat::TransitJson, CopyFormat::ArrowStream] {
            let memory = MemoryBudget::new(1 << 20);
            let mut copy = CopyInput::new(vec![Identifier { text: "docs".into(), quoted: false }], Some(format), &memory, 3).unwrap();
            let path = copy.file.path().to_owned(); assert!(path.exists());
            copy.append(b"abc").await.unwrap();
            assert_eq!(copy.append(b"d").await.unwrap_err().code, "xtdb.pgwire/copy-limit");
            let expected: &[u8] = if format == CopyFormat::ArrowStream { b"" } else { b"abc" };
            assert_eq!(std::fs::read(&path).unwrap().as_slice(), expected);
            assert_eq!(copy.length, 3); drop(copy); assert!(!path.exists()); assert_eq!(memory.used_bytes(), 0);
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
                let bound = if is_file { arrow_file_allocation(&mut input, &memory) }
                    else { arrow_stream_allocation(&mut input, bytes.len() as u64, &memory, Some(&mut headers)) }.unwrap();
                let reservation = memory.reserve(bound).unwrap(); input.seek(SeekFrom::Start(0)).unwrap();
                let page = if is_file { rtdb_format::ipc::ArrowFile::from_reader(input, None).unwrap().next().unwrap().unwrap() }
                    else { rtdb_format::ipc::ArrowStream::from_reader(headers.replay(input), None).unwrap().next().unwrap().unwrap() };
                assert_eq!(rtdb_columnar::read_value(page.schema().field(0), page.column(0).as_ref(), 0).unwrap(), value);
                drop(page); drop(reservation); assert_eq!(memory.used_bytes(), 0);
            }
        }
    }

    #[tokio::test]
    async fn fragmented_stream_reuses_bounded_bytes_and_keeps_compressed_dictionary_and_end_semantics() {
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
            (terminator, COPY_INPUT_SCRATCH_BYTES + 1, true),
        ] {
            let retains_bytes = length <= COPY_INPUT_SCRATCH_BYTES;
            let mut input = stream[..end].to_vec();
            input.extend_from_slice(marker);
            input.resize(length, 255);
            let table = vec![Identifier { text: "docs".into(), quoted: false }];
            let mut copy = CopyInput::new(table, Some(CopyFormat::ArrowStream), &memory, length as u64).unwrap();
            let scratch = memory.used_bytes();
            for chunk in input.chunks(11) { copy.append(chunk).await.unwrap(); }
            assert_eq!(copy.retained_stream.as_deref(), retains_bytes.then_some(input.as_slice()));
            assert_eq!(memory.used_bytes(), scratch);
            assert_eq!(copy.append(&[0]).await.unwrap_err().code, "xtdb.pgwire/copy-limit");
            assert_eq!(std::fs::read(copy.file.path()).unwrap().as_slice(), if retains_bytes { &[] } else { input.as_slice() });
            copy.file.flush().unwrap();
            let (reservation, reader) = copy.arrow_reader(&memory, autocommit).unwrap();
            assert_eq!(*decode_bound.get_or_insert(reservation.bytes()), reservation.bytes());
            assert_eq!(reader.collect::<rtdb_types::Result<Vec<_>>>().unwrap(), [batch.clone(), batch.clone()]);
            drop(reservation);
            drop(copy);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn arrow_stream_spills_the_retained_prefix_once_and_keeps_the_disk_limit() {
        let memory = MemoryBudget::new(1 << 20);
        let prefix = vec![7; COPY_INPUT_SCRATCH_BYTES];
        let limit = (prefix.len() + 2) as u64;
        let table = vec![Identifier { text: "docs".into(), quoted: false }];
        let mut copy = CopyInput::new(table, Some(CopyFormat::ArrowStream), &memory, limit).unwrap();
        copy.append(&prefix).await.unwrap();
        assert_eq!(copy.append(b"abc").await.unwrap_err().code, "xtdb.pgwire/copy-limit");
        assert_eq!(copy.file.as_file().metadata().unwrap().len(), 0);
        copy.append(b"a").await.unwrap();
        copy.append(b"b").await.unwrap();
        assert_eq!(copy.length, limit);
        assert!(copy.retained_stream.is_none());
        let file = std::fs::read(copy.file.path()).unwrap();
        assert_eq!(&file[..prefix.len()], &prefix);
        assert_eq!(&file[prefix.len()..], b"ab");
    }

    #[test]
    fn copy_to_chunks_match_contiguous_arrow_ipc_frames() {
        use arrow_array::BinaryArray;
        use arrow_ipc::writer::StreamWriter;
        use arrow_schema::{DataType, Field, Schema};

        let bytes = vec![0xe1; COPY_CHUNK_BYTES * 2 + 13];
        let schema = Arc::new(Schema::new(vec![Field::new("payload", DataType::Binary, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BinaryArray::from(vec![Some(bytes.as_slice()), None]))],
        ).unwrap();
        let mut reference = StreamWriter::try_new(Vec::new(), &schema).unwrap();
        let mut chunked = StreamWriter::try_new(CopyChunks::new(), &schema).unwrap();
        assert_eq!(std::mem::take(reference.get_mut()), chunked.get_mut().take_header());
        for _ in 0..2 {
            reference.write(&batch).unwrap();
            chunked.write(&batch).unwrap();
            let expected = std::mem::take(reference.get_mut());
            let actual = chunked.get_mut().take_batch();
            assert!(actual.len() > 2);
            assert!(actual.iter().zip(expected.chunks(COPY_CHUNK_BYTES)).all(|(actual, expected)| actual == expected));
            assert_eq!(actual.concat(), expected);
        }
        reference.finish().unwrap();
        chunked.finish().unwrap();
        assert_eq!(chunked.get_ref().chunks.as_slice(), &[reference.into_inner().unwrap()]);
    }
}
