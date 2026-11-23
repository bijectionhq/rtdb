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

pub(crate) type Column = (TableRef, String);

fn schema() -> Schema {
    Schema::new_with_metadata(vec![
        Field::new("schema", DataType::Utf8, false),
        Field::new("table", DataType::Utf8, false),
        Field::new("column", DataType::Utf8, false),
        Field::new("text", DataType::Boolean, false),
        Field::new("tokens", DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, false))), false),
    ], [("rtdb.search.delta".into(), format!("1/{}", rtdb_text::ANALYZER_VERSION))].into())
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
        .flat_map(|definition| definition.columns.iter().map(|column| (&definition.table, column))).collect();
    if columns.is_empty() { return Ok(None); }

    let mut rows = 0usize;
    let mut source_bytes = 0usize;
    for (table, column) in &columns {
        let batch = write.tables()[*table].batch().expect("nonempty write has a batch");
        rows += batch.len();
        if rows > MAX_ROWS { return Err(invalid("Search delta exceeds its row allowance")); }
        for row in 0..batch.len() {
            if cancellation.is_cancelled() { return Err(search_cancelled()); }
            let event = batch.view().borrow_event(row)?;
            let text_bytes = match event.put_text(column)? { PutText::Utf8(text) => text.len(), _ => 0 };
            source_bytes = source_bytes.saturating_add(64 + table.schema.len() + table.table.len() + column.len() + text_bytes);
            if source_bytes > MAX_BYTES { return Err(invalid("Search delta exceeds its input allowance")); }
        }
    }
    let mut charge = memory.reserve(16384 + source_bytes * 48)?;
    let mut schemas = StringBuilder::new();
    let mut tables = StringBuilder::new();
    let mut fields = StringBuilder::new();
    let mut present = BooleanBuilder::new();
    let mut tokens = LargeListBuilder::new(StringBuilder::new()).with_field(Field::new("item", DataType::Utf8, false));
    for (table, column) in columns {
        let batch = write.tables()[table].batch().expect("nonempty write has a batch");
        for row in 0..batch.len() {
            if cancellation.is_cancelled() { return Err(search_cancelled()); }
            schemas.append_value(&table.schema);
            tables.append_value(&table.table);
            fields.append_value(column);
            let event = batch.view().borrow_event(row)?;
            if let PutText::Utf8(text) = event.put_text(column)? {
                rtdb_text::append_tokens(text, tokens.values());
                present.append_value(true);
            } else {
                present.append_value(false);
            }
            tokens.append(true);
        }
    }
    let schema = Arc::new(schema());
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(schemas.finish()), Arc::new(tables.finish()),
        Arc::new(fields.finish()), Arc::new(present.finish()), Arc::new(tokens.finish())]).map_err(invalid)?;
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
}

impl Delta {
    pub(crate) fn read(encoded: bytes::Bytes, write: &StagedWrite, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if encoded.len() > MAX_BYTES { return Err(invalid("Search delta exceeds its encoded allowance")); }
        let mut charge = memory.reserve(16384 + encoded.len() * 12)?;
        let batch = read_batch(Buffer::from(encoded))?;
        let retained = rtdb_columnar::shared_allocation_size(&batch)?;
        Arc::get_mut(&mut charge).expect("unpublished delta owns its reservation").shrink_to(retained)?;
        Self::new(OwnedBatch::with_reservation(batch, charge)?, write)
    }

    pub(crate) fn new(owned: OwnedBatch, write: &StagedWrite) -> Result<Self> {
        let batch = owned.batch();
        if **batch.schema_ref() != schema() {
            return Err(invalid("Search delta has the wrong schema or page count"));
        }
        if batch.num_rows() > MAX_ROWS || batch.columns().iter().any(|column| column.null_count() != 0)
            || batch.column(4).as_list::<i64>().values().null_count() != 0 {
            return Err(invalid("Search delta has invalid rows"));
        }
        let schemas = batch.column(0).as_string::<i32>();
        let tables = batch.column(1).as_string::<i32>();
        let fields = batch.column(2).as_string::<i32>();
        let mut columns = BTreeMap::new();
        let mut first = 0;
        while first < batch.num_rows() {
            let key = (TableRef::new(schemas.value(first), tables.value(first)), fields.value(first).to_owned());
            let rows = write.tables().get(&key.0).map_or(0, |table| table.row_count());
            let last = first + rows;
            if rows == 0 || last > batch.num_rows() || (first..last).any(|row| schemas.value(row) != key.0.schema
                || tables.value(row) != key.0.table || fields.value(row) != key.1)
                || columns.insert(key, first..last).is_some() {
                return Err(invalid("Search delta does not cover the accepted transaction's rows"));
            }
            first = last;
        }
        Ok(Self { batch: owned, columns })
    }

    pub(crate) fn columns(&self) -> impl Iterator<Item = &Column> { self.columns.keys() }

    pub(crate) fn page(&self, column: &Column, rows: Range<usize>) -> (&BooleanArray, LargeListArray, Range<usize>) {
        let column = &self.columns[column];
        let range = column.start + rows.start..column.start + rows.end;
        assert!(range.end <= column.end);
        let batch = self.batch.batch();
        let present = batch.column(3).as_boolean();
        let lists = batch.column(4).as_list::<i64>().slice(range.start, range.len());
        (present, lists, range)
    }
}

fn read_batch(buffer: Buffer) -> Result<RecordBatch> {
    use arrow_ipc::{convert::fb_to_schema, root_as_footer};
    let eof = || invalid("failed to fill whole buffer");
    let tail = buffer.len().checked_sub(10).ok_or_else(eof)?;
    let footer = read_footer_length(buffer[tail..].try_into().expect("ten byte trailer")).map_err(invalid)?;
    let start = tail.checked_sub(footer).ok_or_else(eof)?;
    let footer = root_as_footer(&buffer[start..tail])
        .map_err(|error| invalid(format!("Parser error: Unable to get root as footer: {error:?}")))?;
    let blocks = footer.recordBatches().ok_or_else(|| invalid("Parser error: Unable to get record batches from IPC Footer"))?;
    let source = footer.schema().ok_or_else(|| invalid("Search delta has the wrong schema or page count"))?;
    if !source.endianness().equals_to_target_endianness() {
        return Err(invalid("Ipc error: the endianness of the source system does not match the endianness of the target system."));
    }
    let source = Arc::new(fb_to_schema(source));
    let mut decoder = FileDecoder::new(Arc::clone(&source), footer.version());
    let block_bytes = |block: &arrow_ipc::Block| {
        let offset = usize::try_from(block.offset()).map_err(invalid)?;
        let metadata = usize::try_from(block.metaDataLength()).map_err(invalid)?;
        let body = usize::try_from(block.bodyLength()).map_err(invalid)?;
        let length = metadata.checked_add(body).ok_or_else(eof)?;
        if metadata < 8 || offset.checked_add(length).is_none_or(|end| end > buffer.len()) { return Err(eof()); }
        Ok(buffer.slice_with_length(offset, length))
    };
    for block in footer.dictionaries().into_iter().flatten() {
        decoder.read_dictionary(block, &block_bytes(block)?).map_err(invalid)?;
    }
    if *source != schema() || blocks.len() != 1 {
        return Err(invalid("Search delta has the wrong schema or page count"));
    }
    let block = blocks.get(0);
    decoder.read_record_batch(block, &block_bytes(block)?).map_err(invalid)?
        .ok_or_else(|| invalid("Search delta is empty"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use rtdb_index::{IndexBatch, StoredEvent, StoredOperation, TableWrite};
    use rtdb_types::{BlockIndex, EntityId, Interval, StructFields, Timestamp, TransactionKey, TxId, Value};

    #[test]
    fn arrow_build_delta_preserves_ipc_bytes_and_reuses_token_buffers() {
        let memory = MemoryBudget::new(8 << 20);
        let table = TableRef::new("public", "docs");
        let stored = |id, operation| StoredEvent { position: None, origin: None, entity_key: None, iid: EntityId::Integer(id).iid(),
            system_from: Timestamp(2), commit_position: 0, valid_time: Interval::ALL, operation, recency: None };
        let events = [stored(1, StoredOperation::Put(StructFields::from([("body".into(), Value::Utf8("Audit report audit".into()))]))),
            stored(2, StoredOperation::Delete)];
        let staged = |events: &[StoredEvent]| {
            let batch = IndexBatch::copy_events(events, &StructFields::new(), &memory).unwrap();
            let write = TableWrite::new(StructFields::new(), Some(batch));
            StagedWrite::new(TransactionKey { tx_id: TxId(1), system_time: Timestamp(2) }, None, BlockIndex(0),
                BTreeMap::from([(table.clone(), write)])).unwrap()
        };
        let definition = SearchDefinition { name: "body".into(), table: table.clone(), columns: vec!["body".into()], version: 1, history: true };
        let write = staged(&events);
        let encoded = prepare(&[definition], &write, &memory, &CancellationToken::new()).unwrap().unwrap();
        let mut previous_tokens = LargeListBuilder::new(StringBuilder::new()).with_field(Field::new("item", DataType::Utf8, false));
        for term in ["audit", "report", "audit"] { previous_tokens.values().append_value(term); }
        previous_tokens.append(true);
        previous_tokens.append(true);
        let previous_schema = Arc::new(schema());
        let previous = RecordBatch::try_new(Arc::clone(&previous_schema), vec![
            Arc::new(StringArray::from(vec!["public"; 2])), Arc::new(StringArray::from(vec!["docs"; 2])),
            Arc::new(StringArray::from(vec!["body"; 2])), Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(previous_tokens.finish()),
        ]).unwrap();
        let mut writer = arrow_ipc::writer::FileWriter::try_new(Vec::new(), &previous_schema).unwrap();
        writer.write(&previous).unwrap();
        assert_eq!(encoded.encoded.as_ref(), writer.into_inner().unwrap());

        let key = (table.clone(), "body".into());
        let check = |delta: Delta| {
            let (present, lists, rows) = delta.page(&key, 0..2);
            assert!(present.value(rows.start));
            assert!(!present.value(rows.start + 1));
            assert_eq!(lists.value(0).as_string::<i32>().iter().flatten().collect::<Vec<_>>(), ["audit", "report", "audit"]);
            lists.values().as_string::<i32>().value_data().as_ptr() as usize
        };
        let replayed = check(Delta::read(encoded.encoded.clone(), &write, &memory).unwrap());
        let start = encoded.encoded.as_ptr() as usize;
        assert!((start..start + encoded.encoded.len()).contains(&replayed));
        let prepared = check(Delta::new(encoded.batch.clone(), &write).unwrap());
        assert_eq!(prepared, encoded.batch.batch().column(4).as_list::<i64>().values().as_string::<i32>().value_data().as_ptr() as usize);
        assert!(Delta::read(encoded.encoded, &staged(&events[..1]), &memory).is_err());
    }
}
