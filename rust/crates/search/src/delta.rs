use std::{collections::{BTreeMap, BTreeSet}, ops::Range, sync::Arc};

use arrow_array::{Array, BooleanArray, LargeListArray, RecordBatch, builder::{BooleanBuilder, LargeListBuilder, StringBuilder}, cast::AsArray};
use arrow_buffer::Buffer;
use arrow_ipc::reader::{FileDecoder, read_footer_length};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_format::data::PutText;
use rtdb_index::{PreparedSearchDelta, StagedWrite};
use rtdb_types::{Anomaly, Result, TableRef};
use tokio_util::sync::CancellationToken;

use crate::{SearchDefinition, accelerate::search_cancelled, lane::LogicalIndex};

pub(crate) const MAX_BYTES: usize = 1 << 20;
const MAX_ROWS: usize = 2048;

/// A table column under one analyzer: definitions with different analyzers on one column keep separate tokens.
pub(crate) type Column = (TableRef, String, rtdb_text::Analyzer);

fn schema() -> Schema {
    Schema::new_with_metadata(vec![
        Field::new("schema", DataType::Utf8, false),
        Field::new("table", DataType::Utf8, false),
        Field::new("column", DataType::Utf8, false),
        Field::new("analyzer", DataType::Utf8, false),
        Field::new("text", DataType::Boolean, false),
        Field::new("tokens", DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, false))), false),
    ], [("rtdb.search.delta".into(), format!("2/{}", rtdb_text::ANALYZER_VERSION))].into())
}

fn invalid(reason: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.search/invalid-delta", reason.to_string())
}

struct DeltaBytes(Vec<u8>, Arc<MemoryReservation>);

impl AsRef<[u8]> for DeltaBytes {
    fn as_ref(&self) -> &[u8] {
        let Self(bytes, _reservation) = self;
        bytes
    }
}

pub(crate) fn prepare(definitions: &[SearchDefinition], write: &StagedWrite, memory: &Arc<MemoryBudget>,
    cancellation: &CancellationToken) -> Result<Option<PreparedSearchDelta>> {
    let columns: BTreeSet<_> = definitions.iter().filter(|definition| LogicalIndex::supports(definition))
        .filter(|definition| write.tables().get(&definition.table).is_some_and(|write| write.row_count() > 0))
        .flat_map(|definition| definition.columns.iter().map(|column| (&definition.table, column, definition.analyzer))).collect();
    if columns.is_empty() { return Ok(None); }

    let mut texts = [None; MAX_ROWS];
    let mut rows = 0usize;
    let mut source_bytes = 0usize;
    for (table, column, _) in &columns {
        let batch = write.tables()[*table].batch().expect("nonempty write has a batch");
        let end = rows + batch.len();
        if end > MAX_ROWS { return Err(invalid("Search delta exceeds its row allowance")); }
        let text = batch.view().put_text_column(column);
        for (row, slot) in texts[rows..end].iter_mut().enumerate() {
            if cancellation.is_cancelled() { return Err(search_cancelled()); }
            if let (_, PutText::Utf8(value)) = text.read(row)? { *slot = Some(value); }
            source_bytes = source_bytes.saturating_add(80 + table.schema.len() + table.table.len() + column.len()
                + slot.map_or(0, str::len));
            if source_bytes > MAX_BYTES { return Err(invalid("Search delta exceeds its input allowance")); }
        }
        rows = end;
    }
    let mut charge = memory.reserve(16384 + source_bytes * 48)?;
    let mut schemas = StringBuilder::new();
    let mut tables = StringBuilder::new();
    let mut fields = StringBuilder::new();
    let mut analyzers = StringBuilder::new();
    let mut present = BooleanBuilder::new();
    let mut tokens = LargeListBuilder::new(StringBuilder::new()).with_field(Field::new("item", DataType::Utf8, false));
    let mut texts = texts[..rows].iter();
    for (table, column, analyzer) in columns {
        let rows = write.tables()[table].row_count();
        for text in texts.by_ref().take(rows) {
            if cancellation.is_cancelled() { return Err(search_cancelled()); }
            schemas.append_value(&table.schema);
            tables.append_value(&table.table);
            fields.append_value(column);
            analyzers.append_value(analyzer.name());
            if let Some(text) = text {
                analyzer.append_tokens(text, tokens.values());
            }
            present.append_value(text.is_some());
            tokens.append(true);
        }
    }
    let schema = Arc::new(schema());
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(schemas.finish()), Arc::new(tables.finish()),
        Arc::new(fields.finish()), Arc::new(analyzers.finish()), Arc::new(present.finish()), Arc::new(tokens.finish())]).map_err(invalid)?;
    let mut writer = arrow_ipc::writer::FileWriter::try_new(Vec::new(), &schema).map_err(invalid)?;
    writer.write(&batch).map_err(invalid)?;
    let encoded = writer.into_inner().map_err(invalid)?;
    if encoded.len() > MAX_BYTES { return Err(invalid("Search delta exceeds its encoded allowance")); }
    Arc::get_mut(&mut charge).expect("unpublished delta owns its reservation").shrink_to(encoded.capacity())?;
    Ok(Some(PreparedSearchDelta {
        encoded: bytes::Bytes::from_owner(DeltaBytes(encoded, charge)),
        batch: OwnedBatch::new(batch, memory)?,
    }))
}

pub(crate) struct Delta {
    batch: OwnedBatch,
    columns: BTreeMap<Column, Range<usize>>,
    _column_grant: Arc<MemoryReservation>,
}

impl Delta {
    pub(crate) fn read(encoded: bytes::Bytes, write: &StagedWrite, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if encoded.len() > MAX_BYTES { return Err(invalid("Search delta exceeds its encoded allowance")); }
        Self::new(read_batch(Buffer::from(encoded), memory)?, write)
    }

    pub(crate) fn new(owned: OwnedBatch, write: &StagedWrite) -> Result<Self> {
        let batch = owned.batch();
        if **batch.schema_ref() != schema() {
            return Err(invalid("Search delta has the wrong schema or page count"));
        }
        if batch.num_rows() > MAX_ROWS || batch.columns().iter().any(|column| column.null_count() != 0)
            || batch.column(5).as_list::<i64>().values().null_count() != 0 {
            return Err(invalid("Search delta has invalid rows"));
        }
        let schemas = batch.column(0).as_string::<i32>();
        let tables = batch.column(1).as_string::<i32>();
        let fields = batch.column(2).as_string::<i32>();
        let analyzers = batch.column(3).as_string::<i32>();
        let mut column_grant = owned.reservation().budget().reserve(0)?;
        let mut columns = BTreeMap::new();
        let mut first = 0;
        while first < batch.num_rows() {
            let analyzer = rtdb_text::Analyzer::parse(analyzers.value(first)).ok_or_else(|| invalid("Search delta names an unknown analyzer"))?;
            let (schema, table, field) = (schemas.value(first), tables.value(first), fields.value(first));
            let grant = Arc::get_mut(&mut column_grant).expect("delta owns its column grant");
            grant.grow_to(grant.bytes().saturating_add(256).saturating_add(schema.len()).saturating_add(table.len()).saturating_add(field.len()))?;
            let key = (TableRef::new(schema, table), field.to_owned(), analyzer);
            let rows = write.tables().get(&key.0).map_or(0, |table| table.row_count());
            let last = first + rows;
            if rows == 0 || last > batch.num_rows() || (first..last).any(|row| schemas.value(row) != key.0.schema
                || tables.value(row) != key.0.table || fields.value(row) != key.1 || analyzers.value(row) != analyzer.name())
                || columns.insert(key, first..last).is_some() {
                return Err(invalid("Search delta does not cover the accepted transaction's rows"));
            }
            first = last;
        }
        Ok(Self { batch: owned, columns, _column_grant: column_grant })
    }

    pub(crate) fn columns(&self) -> impl Iterator<Item = &Column> { self.columns.keys() }

    pub(crate) fn page(&self, column: &Column, rows: Range<usize>) -> (&BooleanArray, LargeListArray, Range<usize>) {
        let column = &self.columns[column];
        let range = column.start + rows.start..column.start + rows.end;
        assert!(range.end <= column.end);
        let batch = self.batch.batch();
        let present = batch.column(4).as_boolean();
        let lists = batch.column(5).as_list::<i64>().slice(range.start, range.len());
        (present, lists, range)
    }
}

fn read_batch(buffer: Buffer, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
    use arrow_ipc::{MetadataVersion, Type, convert::fb_to_schema, root_as_footer};

    let eof = || invalid("failed to fill whole buffer");
    let profile = || invalid("Search delta has the wrong schema or page count");
    let tail = buffer.len().checked_sub(10).ok_or_else(eof)?;
    let footer = read_footer_length(buffer[tail..].try_into().expect("ten byte trailer")).map_err(invalid)?;
    let start = tail.checked_sub(footer).filter(|&start| start >= 8).ok_or_else(eof)?;
    if !buffer.starts_with(b"ARROW1") { return Err(profile()); }
    let footer = root_as_footer(&buffer[start..tail])
        .map_err(|error| invalid(format!("Parser error: Unable to get root as footer: {error:?}")))?;
    let blocks = footer.recordBatches().ok_or_else(|| invalid("Parser error: Unable to get record batches from IPC Footer"))?;
    let source = footer.schema().ok_or_else(profile)?;
    if !source.endianness().equals_to_target_endianness() {
        return Err(invalid("Ipc error: the endianness of the source system does not match the endianness of the target system."));
    }
    if blocks.len() != 1 || footer.version() != MetadataVersion::V5
        || footer.dictionaries().is_some_and(|dictionaries| !dictionaries.is_empty()) {
        return Err(profile());
    }
    let fields = source.fields().ok_or_else(profile)?;
    let types = [Type::Utf8, Type::Utf8, Type::Utf8, Type::Utf8, Type::Bool, Type::LargeList];
    if fields.len() != types.len() || fields.iter().zip(types).any(|(field, expected)| {
        field.dictionary().is_some() || field.type_type() != expected || match expected {
            Type::LargeList => field.children().is_none_or(|children| {
                children.len() != 1 || {
                    let child = children.get(0);
                    child.type_type() != Type::Utf8 || child.dictionary().is_some()
                        || child.children().is_some_and(|nested| !nested.is_empty())
                }
            }),
            _ => field.children().is_some_and(|children| !children.is_empty()),
        }
    }) {
        return Err(profile());
    }
    let block = blocks.get(0);
    let offset = usize::try_from(block.offset()).map_err(invalid)?;
    let metadata = usize::try_from(block.metaDataLength()).map_err(invalid)?;
    let body = usize::try_from(block.bodyLength()).map_err(invalid)?;
    let end = offset.checked_add(metadata).and_then(|end| end.checked_add(body))
        .filter(|&end| end <= start && offset >= 8).ok_or_else(eof)?;
    let encoded = buffer.slice_with_length(offset, end - offset);
    let bound = crate::file::inspect_uncompressed_record(&encoded, metadata, body, 0..=MAX_ROWS).map_err(|error| {
        if error.message.as_deref() == Some("Search page has an invalid row count") { invalid("Search delta has invalid rows") }
        else { invalid(error) }
    })?;
    let schema_bytes = tail - start;
    let mut charge = memory.reserve(bound.saturating_add(buffer.len()).saturating_add(schema_bytes.saturating_mul(16)))?;
    let source = Arc::new(fb_to_schema(source));
    if *source != schema() { return Err(profile()); }
    let batch = FileDecoder::new(source, footer.version()).read_record_batch(block, &encoded).map_err(invalid)?
        .ok_or_else(|| invalid("Search delta is empty"))?;
    let retained = rtdb_columnar::shared_allocation_size(&batch)?.saturating_add(4096).saturating_add(schema_bytes.saturating_mul(4));
    let batch_charge = Arc::get_mut(&mut charge).expect("unpublished delta owns its reservation").split_off(retained)?;
    OwnedBatch::with_reservation(batch, batch_charge)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use rtdb_index::{IndexBatch, StoredEvent, StoredOperation, TableWrite};
    use rtdb_types::{BlockIndex, EntityId, Interval, StructFields, Timestamp, TransactionKey, TxId, Value};

    #[test]
    fn replay_rejects_oversized_row_declarations_before_decode_admission() {
        let rows = MAX_ROWS + 1;
        let schema = Arc::new(schema());
        let strings = Arc::new(StringArray::from(vec![""; rows]));
        let mut tokens = LargeListBuilder::new(StringBuilder::new()).with_field(Field::new("item", DataType::Utf8, false));
        for _ in 0..rows { tokens.append(true); }
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![strings.clone(), strings.clone(), strings.clone(), strings,
            Arc::new(BooleanArray::from(vec![false; rows])), Arc::new(tokens.finish())]).unwrap();
        let mut writer = arrow_ipc::writer::FileWriter::try_new(Vec::new(), &schema).unwrap();
        writer.write(&batch).unwrap();
        let encoded = Buffer::from_vec(writer.into_inner().unwrap());
        let memory = MemoryBudget::new(0);
        let error = read_batch(encoded, &memory).unwrap_err();
        assert_eq!(error.code, "rtdb.search/invalid-delta");
        assert_eq!(error.message.as_deref(), Some("Search delta has invalid rows"));
        assert_eq!(memory.peak_bytes(), 0);
    }

    #[test]
    fn fts_arrow_delta_preserves_ipc_bytes_and_commit_row_coverage() {
        let memory = MemoryBudget::new(8 << 20);
        let table = TableRef::new("public", "docs");
        let stored = |id, operation| StoredEvent { position: None, origin: None, entity_key: None, iid: EntityId::Integer(id).iid(),
            system_from: Timestamp(2), commit_position: 0, valid_time: Interval::ALL, operation, recency: None };
        let events = [stored(1, StoredOperation::Put(StructFields::from([("body".into(), Value::Utf8("Audit report audit ΟΣ".into()))]))),
            stored(2, StoredOperation::Delete)];
        let staged = |events: &[StoredEvent]| {
            let batch = IndexBatch::copy_events(events, &StructFields::new(), &memory).unwrap();
            let write = TableWrite::new(StructFields::new(), Some(batch));
            StagedWrite::new(TransactionKey { tx_id: TxId(1), system_time: Timestamp(2) }, None, BlockIndex(0),
                BTreeMap::from([(table.clone(), write)])).unwrap()
        };
        let definition = SearchDefinition { name: "body".into(), table: table.clone(), columns: vec!["body".into()], analyzer: rtdb_text::Analyzer::Simple, version: 1, history: true };
        let write = staged(&events);
        let encoded = prepare(&[definition], &write, &memory, &CancellationToken::new()).unwrap().unwrap();
        let mut previous_tokens = LargeListBuilder::new(StringBuilder::new()).with_field(Field::new("item", DataType::Utf8, false));
        for term in ["audit", "report", "audit", "ος"] { previous_tokens.values().append_value(term); }
        previous_tokens.append(true);
        previous_tokens.append(true);
        let previous_schema = Arc::new(schema());
        let previous = RecordBatch::try_new(Arc::clone(&previous_schema), vec![
            Arc::new(StringArray::from(vec!["public"; 2])), Arc::new(StringArray::from(vec!["docs"; 2])),
            Arc::new(StringArray::from(vec!["body"; 2])), Arc::new(StringArray::from(vec!["simple"; 2])), Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(previous_tokens.finish()),
        ]).unwrap();
        let mut writer = arrow_ipc::writer::FileWriter::try_new(Vec::new(), &previous_schema).unwrap();
        writer.write(&previous).unwrap();
        assert_eq!(encoded.encoded.as_ref(), writer.into_inner().unwrap());

        let key = (table.clone(), "body".into(), rtdb_text::Analyzer::Simple);
        let check = |delta: Delta| {
            let (present, lists, rows) = delta.page(&key, 0..2);
            assert!(present.value(rows.start));
            assert!(!present.value(rows.start + 1));
            assert_eq!(lists.value(0).as_string::<i32>().iter().flatten().collect::<Vec<_>>(), ["audit", "report", "audit", "ος"]);
            lists.values().as_string::<i32>().value_data().as_ptr() as usize
        };
        let replayed = check(Delta::read(encoded.encoded.clone(), &write, &memory).unwrap());
        let start = encoded.encoded.as_ptr() as usize;
        assert!((start..start + encoded.encoded.len()).contains(&replayed));
        let prepared = check(Delta::new(encoded.batch.clone(), &write).unwrap());
        assert_eq!(prepared, encoded.batch.batch().column(5).as_list::<i64>().values().as_string::<i32>().value_data().as_ptr() as usize);
        assert!(Delta::read(encoded.encoded, &staged(&events[..1]), &memory).is_err());
    }
}
