use std::{collections::{BTreeMap, BTreeSet}, io::Cursor, ops::Range, sync::Arc};

use arrow_array::{Array, BooleanArray, LargeListArray, RecordBatch, StringArray, builder::{BooleanBuilder, LargeListBuilder, StringBuilder}, cast::AsArray};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::data::PutText;
use rtdb_index::StagedWrite;
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
    cancellation: &CancellationToken) -> Result<Option<bytes::Bytes>> {
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
                for term in rtdb_text::analyze(text).iter().flatten() { tokens.values().append_value(term); }
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
    Ok(Some(bytes::Bytes::from_owner(DeltaBytes(encoded, charge))))
}

pub(crate) struct Delta {
    batch: RecordBatch,
    columns: BTreeMap<Column, Range<usize>>,
    _reservation: Arc<MemoryReservation>,
}

impl Delta {
    pub(crate) fn read(encoded: &[u8], write: &StagedWrite, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if encoded.len() > MAX_BYTES { return Err(invalid("Search delta exceeds its encoded allowance")); }
        let charge = memory.reserve(16384 + encoded.len() * 12)?;
        let mut reader = arrow_ipc::reader::FileReader::try_new(Cursor::new(encoded), None).map_err(invalid)?;
        if *reader.schema() != schema() || reader.num_batches() != 1 {
            return Err(invalid("Search delta has the wrong schema or page count"));
        }
        let batch = reader.next().ok_or_else(|| invalid("Search delta is empty"))?.map_err(invalid)?;
        if batch.num_rows() > MAX_ROWS || batch.columns().iter().any(|column| column.null_count() != 0)
            || batch.column(4).as_list::<i64>().values().null_count() != 0 {
            return Err(invalid("Search delta has invalid rows"));
        }
        let entry = |row| (TableRef::new(batch.column(0).as_string::<i32>().value(row),
            batch.column(1).as_string::<i32>().value(row)), batch.column(2).as_string::<i32>().value(row).to_owned());
        let mut columns = BTreeMap::new();
        let mut first = 0;
        while first < batch.num_rows() {
            let key = entry(first);
            let rows = write.tables().get(&key.0).map_or(0, |table| table.row_count());
            let last = first + rows;
            if rows == 0 || last > batch.num_rows() || (first..last).any(|row| entry(row) != key)
                || columns.insert(key, first..last).is_some() {
                return Err(invalid("Search delta does not cover the accepted transaction's rows"));
            }
            first = last;
        }
        Ok(Self { batch, columns, _reservation: charge })
    }

    pub(crate) fn columns(&self) -> impl Iterator<Item = &Column> { self.columns.keys() }

    pub(crate) fn tokens(&self, column: &Column, row: usize) -> Option<StringArray> {
        let range = self.columns.get(column)?;
        let row = range.start.checked_add(row)?;
        let present = self.batch.column(3).as_any().downcast_ref::<BooleanArray>().expect("delta text witness schema");
        if row >= range.end || !present.value(row) { return None; }
        let lists: &LargeListArray = self.batch.column(4).as_list::<i64>();
        let offsets = lists.value_offsets();
        let first = offsets[row] as usize;
        Some(lists.values().as_string::<i32>().slice(first, offsets[row + 1] as usize - first))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_index::{IndexBatch, StoredEvent, StoredOperation, TableWrite};
    use rtdb_types::{BlockIndex, EntityId, Interval, StructFields, Timestamp, TransactionKey, TxId, Value};

    #[test]
    fn accepted_delta_preserves_token_order_and_commit_row_coverage() {
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
        let restored = Delta::read(&encoded, &write, &memory).unwrap();
        let key = (table.clone(), "body".into());
        assert_eq!(restored.tokens(&key, 0).unwrap().iter().flatten().collect::<Vec<_>>(), ["audit", "report", "audit"]);
        assert!(restored.tokens(&key, 1).is_none());
        assert!(Delta::read(&encoded, &staged(&events[..1]), &memory).is_err());
    }
}
