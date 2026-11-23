use rtdb_types::StructFields;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use arrow_array::RecordBatch;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_node::{AccessSettings, CommitResult, NativeSession, Node, TransactionSettings, TransactionStatus};
use rtdb_pg_codec::Format;
use rtdb_plan::{CopyFormat, Identifier};
use rtdb_tx::{ArrowDocuments, Documents, TxOp};
use rtdb_types::{Anomaly, Result, TableRef, Value};
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;
use crate::error::{cancelled, protocol};

/// Encodes each batch while its source buffers are in scope and releases
/// them before waiting on the connection.
pub(crate) async fn write_arrow<W: tokio::io::AsyncWrite + Unpin>(
    mut cursor: rtdb_query::QueryCursor, memory: &Arc<MemoryBudget>, wire: &mut crate::wire::Writer<W>,
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
    let mut encoder = arrow_ipc::writer::StreamWriter::try_new(Vec::new(), &schema).map_err(arrow_output_error)?;
    wire.send(b'H', &[1, 0, 1, 0, 1]).await?;
    wire.send(b'd', &std::mem::take(encoder.get_mut())).await?;
    wire.flush().await?;
    let mut count = 0u64;
    loop {
        let encoded = cursor.encode_next_batch(|batch| -> Result<_> {
            let batch = rtdb_columnar::conform_batch(batch.clone(), &schema, memory)?;
            let bytes = batch.batch().get_array_memory_size().checked_mul(6)
                .and_then(|bytes| bytes.checked_add(bound)).ok_or_else(|| protocol("COPY TO batch is too large"))?;
            let reservation = memory.reserve(bytes)?;
            encoder.write(batch.batch()).map_err(arrow_output_error)?;
            Ok((batch.batch().num_rows() as u64, std::mem::take(encoder.get_mut()), reservation))
        }).await?;
        let Some(encoded) = encoded else { break };
        let (rows, bytes, _reservation) = encoded.inspect_err(|_| cursor.record_consumer_error())?;
        count = count.checked_add(rows).ok_or_else(|| protocol("COPY TO row count overflow"))?;
        for chunk in bytes.chunks(64 * 1024) {
            wire.send(b'd', chunk).await?;
        }
        wire.flush().await?;
    }
    encoder.finish().map_err(arrow_output_error)?;
    wire.send(b'd', encoder.get_ref()).await?;
    wire.empty(b'c').await?;
    wire.string(b'C', &format!("COPY {count}")).await
}

fn arrow_output_error(error: arrow_schema::ArrowError) -> Anomaly {
    Anomaly::incorrect("xtdb.pgwire/copy-output", error.to_string())
}

pub(crate) struct CopyInput { file: NamedTempFile, format: CopyFormat, table: TableRef, length: u64, limit: u64, _reservation: Arc<MemoryReservation> }
impl CopyInput {
    pub fn new(table: Vec<Identifier>, format: Option<CopyFormat>, memory: &Arc<MemoryBudget>, limit: u64) -> Result<Self> {
        let format = format.ok_or_else(|| Anomaly::incorrect("xtdb.pgwire/invalid-copy-format", "COPY IN requires a valid format: 'arrow-file', 'arrow-stream', 'transit-json', 'transit-msgpack'").with_context("format", serde_json::Value::Null))?;
        let table = match table.as_slice() { [table] => TableRef::new("public", table.normalized()), [schema, table] => TableRef::new(schema.normalized(), table.normalized()), _ => return Err(protocol("COPY table requires a table or schema.table")) };
        let reservation = memory.reserve(table.schema.len() + table.table.len() + 8192)?;
        Ok(Self { file: NamedTempFile::new().map_err(io_error)?, format, table, length: 0, limit, _reservation: reservation })
    }
    pub fn wire_format(&self) -> Format { if self.format == CopyFormat::TransitJson { Format::Text } else { Format::Binary } }
    pub async fn append(&mut self, bytes: &[u8]) -> Result<()> {
        let length = self.length.checked_add(bytes.len() as u64).filter(|length| *length <= self.limit).ok_or_else(|| Anomaly::new(rtdb_types::AnomalyCategory::Busy, "xtdb.pgwire/copy-limit", "COPY input exceeds its disk limit"))?;
        self.file.write_all(bytes).map_err(io_error)?; self.length = length; Ok(())
    }
    pub async fn finish(mut self, session: &mut NativeSession, node: &Node, cancellation: &CancellationToken) -> Result<u64> {
        self.file.flush().map_err(io_error)?;
        if session.state().is_read_only() { return Err(Anomaly::incorrect("xtdb/copy-in-read-only-tx", "COPY is not allowed in a READ ONLY transaction")); }
        match self.format {
            CopyFormat::TransitJson | CopyFormat::TransitMsgpack => {
                let size = usize::try_from(self.length).map_err(io_error)?;
                let _scratch = node.memory().reserve(size.checked_mul(256).and_then(|n| n.checked_add(65_536)).ok_or_else(|| protocol("COPY allocation overflow"))?)?;
                self.file.seek(SeekFrom::Start(0)).map_err(io_error)?; let mut bytes = Vec::with_capacity(size); self.file.read_to_end(&mut bytes).map_err(io_error)?;
                let values = if self.format == CopyFormat::TransitJson { rtdb_format::transit::decode_json_sequence(&bytes)? } else { rtdb_format::transit::decode_msgpack_sequence(&bytes)? };
                let rows = values.into_iter().map(|value| match rtdb_pg_codec::from_transit(value)? { Value::Struct(fields) => Ok(fields), _ => Err(protocol("COPY Transit values must be documents")) }).collect::<Result<Vec<_>>>()?;
                let count = rows.len() as u64;
                self.submit(|| self.put_rows(rows, node.memory()), session, node, cancellation).await?;
                Ok(count)
            }
            CopyFormat::ArrowFile | CopyFormat::ArrowStream => {
                let mut file = self.file.reopen().map_err(io_error)?;
                let bound = arrow_allocation(&mut file, self.format == CopyFormat::ArrowFile, node.memory())?;
                let _arrow = node.memory().reserve(bound)?;
                file.seek(SeekFrom::Start(0)).map_err(io_error)?;
                let reader: Box<dyn Iterator<Item = Result<RecordBatch>> + Send> = if self.format == CopyFormat::ArrowFile {
                    Box::new(rtdb_format::ipc::ArrowFile::from_reader(file, None)?)
                } else { Box::new(rtdb_format::ipc::ArrowStream::from_reader(file, None)?) };
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
    fn put_rows(&self, rows: Vec<StructFields<Value>>, memory: &Arc<MemoryBudget>) -> Result<TxOp> {
        Ok(TxOp::PutDocs { table: self.table.clone(), documents: Documents::new(rows, memory)?, valid_from: None, valid_to: None })
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

fn arrow_allocation(file: &mut File, is_file: bool, memory: &Arc<MemoryBudget>) -> Result<usize> {
    let length = file.metadata().map_err(io_error)?.len();
    let mut permanent = 65_536usize; let mut record = 0usize;
    if is_file {
        if length < 10 { return Err(protocol("Arrow file has no footer")); }
        file.seek(SeekFrom::End(-10)).map_err(io_error)?; let mut tail = [0; 10]; file.read_exact(&mut tail).map_err(io_error)?;
        let size = arrow_ipc::reader::read_footer_length(tail).map_err(io_error)?;
        let start = length.checked_sub(size as u64 + 10).ok_or_else(|| protocol("Arrow footer exceeds file"))?;
        let _footer = memory.reserve(size.checked_mul(64).and_then(|n| n.checked_add(65_536)).ok_or_else(|| protocol("Arrow footer allocation overflow"))?)?;
        let mut bytes = vec![0; size]; file.seek(SeekFrom::Start(start)).map_err(io_error)?; file.read_exact(&mut bytes).map_err(io_error)?;
        let footer = arrow_ipc::root_as_footer(&bytes).map_err(io_error)?; permanent = add(permanent, size.checked_mul(64).ok_or_else(|| protocol("Arrow footer overflow"))?)?;
        rtdb_format::ipc::validate_decimal_metadata(footer.schema().ok_or_else(|| protocol("Arrow footer has no schema"))?)?;
        for (dictionary, blocks) in [(true, footer.dictionaries()), (false, footer.recordBatches())] {
            if let Some(blocks) = blocks { for block in blocks { let offset = u64::try_from(block.offset()).map_err(io_error)?; file.seek(SeekFrom::Start(offset)).map_err(io_error)?; let (bound, end, _) = message_allocation(file, start, memory)?.ok_or_else(|| protocol("Missing Arrow file message"))?; if end > start { return Err(protocol("Arrow block exceeds footer")); } if dictionary { permanent = add(permanent, bound)?; } else { record = record.max(bound); } } }
        }
    } else {
        file.seek(SeekFrom::Start(0)).map_err(io_error)?;
        loop { match message_allocation(file, length, memory)? { None => break, Some((bound, _, dictionary)) => { if dictionary { permanent = add(permanent, bound)?; } else { record = record.max(bound); } } } }
    }
    add(permanent, record)
}

fn message_allocation(file: &mut File, end: u64, memory: &Arc<MemoryBudget>) -> Result<Option<(usize, u64, bool)>> {
    if file.stream_position().map_err(io_error)? == end { return Ok(None); }
    let mut prefix = [0; 4]; file.read_exact(&mut prefix).map_err(io_error)?; let mut length = i32::from_le_bytes(prefix);
    if length == -1 { file.read_exact(&mut prefix).map_err(io_error)?; length = i32::from_le_bytes(prefix); }
    if length == 0 { return Ok(None); }
    let length = usize::try_from(length).map_err(io_error)?;
    let _metadata = memory.reserve(length.checked_mul(64).and_then(|n| n.checked_add(8192)).ok_or_else(|| protocol("IPC metadata allocation overflow"))?)?;
    let mut metadata = vec![0; length]; file.read_exact(&mut metadata).map_err(io_error)?;
    let message = arrow_ipc::root_as_message(&metadata).map_err(io_error)?;
    if let Some(schema) = message.header_as_schema() { rtdb_format::ipc::validate_decimal_metadata(schema)?; }
    let body = file.stream_position().map_err(io_error)?; let body_length = u64::try_from(message.bodyLength()).map_err(io_error)?; let next = body.checked_add(body_length).filter(|next| *next <= end).ok_or_else(|| protocol("IPC body exceeds file"))?;
    let dictionary = message.header_as_record_batch().is_none();
    let batch = message.header_as_record_batch().or_else(|| message.header_as_dictionary_batch().and_then(|dictionary| dictionary.data()));
    let mut bound = add(length.checked_mul(64).ok_or_else(|| protocol("IPC metadata allocation overflow"))?, usize::try_from(body_length).map_err(io_error)?.checked_mul(4).ok_or_else(|| protocol("IPC body allocation overflow"))?)?;
    if let Some(batch) = batch {
        if let Some(nodes) = batch.nodes() { for node in nodes { let rows = usize::try_from(node.length()).map_err(io_error)?; bound = add(bound, rows.checked_add(1).and_then(|n| n.checked_mul(4096)).ok_or_else(|| protocol("IPC node allocation overflow"))?)?; } }
        if let Some(buffers) = batch.buffers() { for buffer in buffers {
            let start = u64::try_from(buffer.offset()).map_err(io_error)?; let size = u64::try_from(buffer.length()).map_err(io_error)?;
            if start.checked_add(size).is_none_or(|end| end > body_length) { return Err(protocol("Arrow buffer exceeds IPC body")); }
            let decoded = if batch.compression().is_some() && size != 0 { if size < 8 { return Err(protocol("Compressed Arrow buffer has no length")); } file.seek(SeekFrom::Start(body + start)).map_err(io_error)?; let mut value = [0; 8]; file.read_exact(&mut value).map_err(io_error)?; let value = i64::from_le_bytes(value); if value == -1 { size - 8 } else { u64::try_from(value).map_err(io_error)? } } else { size };
            bound = add(bound, usize::try_from(decoded).map_err(io_error)?.checked_mul(4).and_then(|n| n.checked_add(512)).ok_or_else(|| protocol("Arrow decompression allocation overflow"))?)?;
        } }
    }
    file.seek(SeekFrom::Start(next)).map_err(io_error)?;
    Ok(Some((bound, next, dictionary)))
}
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(|| protocol("COPY allocation overflow")) }
fn io_error(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("xtdb.pgwire/copy-input", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;

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
        let memory = MemoryBudget::new(1 << 20);
        let mut copy = CopyInput::new(vec![Identifier { text: "docs".into(), quoted: false }], Some(CopyFormat::TransitJson), &memory, 3).unwrap();
        let path = copy.file.path().to_owned(); assert!(path.exists());
        copy.append(b"abc").await.unwrap(); assert!(copy.append(b"d").await.is_err());
        assert_eq!(copy.length, 3); drop(copy); assert!(!path.exists()); assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn malformed_arrow_lengths_are_rejected_before_large_allocation() {
        let memory = MemoryBudget::new(1 << 20); let mut file = NamedTempFile::new().unwrap();
        file.write_all(&(-1i32).to_le_bytes()).unwrap(); file.write_all(&i32::MAX.to_le_bytes()).unwrap();
        let mut input = file.reopen().unwrap(); assert!(arrow_allocation(&mut input, false, &memory).is_err()); assert_eq!(memory.used_bytes(), 0);
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
                let mut input = file.reopen().unwrap(); let bound = arrow_allocation(&mut input, is_file, &memory).unwrap();
                let reservation = memory.reserve(bound).unwrap(); input.seek(SeekFrom::Start(0)).unwrap();
                let page = if is_file { rtdb_format::ipc::ArrowFile::from_reader(input, None).unwrap().next().unwrap().unwrap() }
                    else { rtdb_format::ipc::ArrowStream::from_reader(input, None).unwrap().next().unwrap().unwrap() };
                assert_eq!(rtdb_columnar::read_value(page.schema().field(0), page.column(0).as_ref(), 0).unwrap(), value);
                drop(page); drop(reservation); assert_eq!(memory.used_bytes(), 0);
            }
        }
    }
}
