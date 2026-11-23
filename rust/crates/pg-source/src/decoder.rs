use rtdb_types::StructFields;
use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};
use arrow_array::{Array, NullArray, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::{BatchBuilder, MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_types::{Anomaly, LogicalType, Result, TableRef, Timestamp, Value};
use crate::{Lsn, read_text, wire::{Input, protocol}};

pub const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowKind { Put, Delete }
#[derive(Debug)]
pub struct RowOperation { pub kind: RowKind, pub table: TableRef, pub row: StructFields<Value>, _charge: Arc<MemoryReservation> }
#[derive(Debug)]
pub struct PgTransaction { pub lsn: Lsn, pub commit_time: Option<Timestamp>, pub operations: Vec<RowOperation> }
impl PgTransaction { pub fn empty(lsn: Lsn) -> Self { Self { lsn, commit_time: None, operations: Vec::new() } } }
#[derive(Debug)]
pub struct ArrowChanges {
    pub kind: RowKind,
    pub table: TableRef,
    pub rows: OwnedBatch,
}

#[derive(Debug)]
pub struct ArrowPgTransaction {
    pub lsn: Lsn,
    pub commit_time: Option<Timestamp>,
    pub batches: Vec<ArrowChanges>,
}

impl ArrowPgTransaction {
    pub(crate) fn empty(lsn: Lsn) -> Self {
        Self { lsn, commit_time: None, batches: Vec::new() }
    }

    pub(crate) fn row_count(&self) -> usize {
        self.batches.iter().map(|batch| batch.rows.batch().num_rows()).sum()
    }

    pub(crate) fn to_rows(&self, memory: &Arc<MemoryBudget>) -> Result<PgTransaction> {
        let mut operations = Vec::with_capacity(self.row_count());
        for changes in &self.batches {
            let batch = changes.rows.batch();
            for row in 0..batch.num_rows() {
                let fields = batch.schema_ref().fields().iter().zip(batch.columns());
                let bound = fields.clone().try_fold(4096usize, |bytes, (field, column)| {
                    let cell = rtdb_columnar::estimate_read_value_bytes(field, column.as_ref(), row)?;
                    bytes.checked_add(cell).and_then(|bytes| bytes.checked_add(4096 + field.name().len()))
                        .ok_or_else(|| protocol("Row allocation overflow"))
                })?;
                let charge = memory.reserve(bound)?;
                let values = fields.map(|(field, column)| {
                    Ok((field.name().clone(), rtdb_columnar::read_value(field, column.as_ref(), row)?))
                }).collect::<Result<_>>()?;
                operations.push(RowOperation { kind: changes.kind, table: changes.table.clone(), row: values, _charge: charge });
            }
        }
        Ok(PgTransaction { lsn: self.lsn, commit_time: self.commit_time, operations })
    }
}
struct Relation { table: TableRef, columns: Vec<(String, u32)>, _charge: Arc<MemoryReservation> }
#[derive(Clone, Copy)]
enum Cell<'a> { Null, Unchanged, Text(&'a str) }

pub struct Decoder {
    relations: BTreeMap<u32, Relation>, operations: Vec<ArrowChanges>, ignored: BTreeSet<TableRef>, memory: Arc<MemoryBudget>, in_transaction: bool,
}
impl Decoder {
    pub fn new(ignored: BTreeSet<TableRef>, memory: Arc<MemoryBudget>) -> Self { Self { relations: BTreeMap::new(), operations: Vec::new(), ignored, memory, in_transaction: false } }
    pub fn receive(&mut self, bytes: &[u8]) -> Result<Option<PgTransaction>> {
        self.receive_arrow(bytes)?.map(|transaction| transaction.to_rows(&self.memory)).transpose()
    }

    pub(crate) fn receive_arrow(&mut self, bytes: &[u8]) -> Result<Option<ArrowPgTransaction>> {
        let mut input = Input::new(bytes); let tag = input.u8()?;
        match tag {
            b'R' => {
                let charge = self.memory.reserve(bytes.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| protocol("Relation allocation overflow"))?)?;
                let id = input.u32()?; let schema = input.string()?; let table = input.string()?;
                if !matches!(input.u8()?, b'd' | b'n' | b'f' | b'i') { return Err(protocol("Unknown replica identity")); }
                let count = input.u16()?; let mut columns = Vec::with_capacity(usize::from(count));
                for _ in 0..count { input.u8()?; let name = input.string()?.to_owned(); let oid = input.u32()?; input.i32()?; columns.push((name, oid)); }
                input.end()?; self.relations.insert(id, Relation { table: TableRef::new(schema, table), columns, _charge: charge });
            }
            b'Y' => { input.u32()?; input.string()?; input.string()?; input.end()?; }
            b'B' => { input.u64()?; input.i64()?; input.u32()?; input.end()?; self.operations.clear(); self.in_transaction = true; }
            b'C' => {
                input.u8()?; input.u64()?; let lsn = Lsn(input.u64()?); let timestamp = input.i64()?.checked_add(PG_EPOCH_MICROS).ok_or_else(|| protocol("PostgreSQL commit timestamp overflow"))?; input.end()?;
                if !self.in_transaction { return Err(protocol("Commit arrived without Begin")); } self.in_transaction = false;
                if self.operations.is_empty() { return Ok(None); }
                return Ok(Some(ArrowPgTransaction { lsn, commit_time: Some(Timestamp(timestamp)), batches: std::mem::take(&mut self.operations) }));
            }
            b'I' | b'U' | b'D' => {
                if !self.in_transaction { return Err(protocol("Row change arrived outside a transaction")); }
                let id = input.u32()?; let relation = self.relations.get(&id).ok_or_else(|| protocol(format!("Relation {id} has no preceding Relation message")))?;
                let marker = input.u8()?;
                let mut old = None;
                let values = match tag {
                    b'I' if marker == b'N' => tuple(&mut input)?,
                    b'D' if matches!(marker, b'K' | b'O') => tuple(&mut input)?,
                    b'U' => {
                        if matches!(marker, b'K' | b'O') { old = Some(tuple(&mut input)?); if input.u8()? != b'N' { return Err(protocol("UPDATE old tuple is not followed by a new tuple")); } }
                        else if marker != b'N' { return Err(protocol("UPDATE tuple marker is invalid")); }
                        tuple(&mut input)?
                    }
                    _ => return Err(protocol("Unexpected row tuple marker")),
                };
                input.end()?;
                if self.ignored.contains(&relation.table) { return Ok(None); }
                let width = values.len().min(relation.columns.len());
                let rows = decode_columns(&relation.columns[..width], 1, &self.memory, |_, index| {
                    let (name, _) = &relation.columns[index];
                    recover(values[index], old.as_deref(), index, name, &relation.table)
                })?;
                self.operations.push(ArrowChanges {
                    kind: if tag == b'D' { RowKind::Delete } else { RowKind::Put },
                    table: relation.table.clone(),
                    rows,
                });
            }
            _ => return Err(Anomaly::unsupported("xtdb.postgres/unsupported-pgoutput-message", format!("Unsupported pgoutput message tag {tag}"))),
        }
        Ok(None)
    }
}
fn tuple<'a>(input: &mut Input<'a>) -> Result<Vec<Cell<'a>>> {
    let count = usize::from(input.u16()?); let mut values = Vec::with_capacity(count);
    for _ in 0..count { values.push(match input.u8()? { b'n' => Cell::Null, b'u' => Cell::Unchanged, b't' => { let length = usize::try_from(input.i32()?).map_err(protocol)?; Cell::Text(std::str::from_utf8(input.take(length)?).map_err(protocol)?) }, _ => return Err(protocol("Unsupported pgoutput tuple encoding")) }); }
    Ok(values)
}
fn recover<'a>(value: Cell<'a>, old: Option<&[Cell<'a>]>, index: usize, name: &str, table: &TableRef) -> Result<Option<&'a str>> {
    match value { Cell::Null => Ok(None), Cell::Text(value) => Ok(Some(value)), Cell::Unchanged => match old.and_then(|values| values.get(index)) { Some(Cell::Text(value)) => Ok(Some(value)), _ => Err(Anomaly::incorrect("xtdb.postgres/unchanged-toast", format!("Column {name} on {}.{} has an unrecoverable unchanged TOAST value; set REPLICA IDENTITY FULL and recreate the source", table.schema, table.table))) } }
}
pub(crate) fn text_bound(oid: u32, text: &str) -> Result<usize> {
    let nested = matches!(oid, 114 | 3802 | 16384 | 1007 | 1016 | 1009);
    let nodes = if nested { text.bytes().filter(|byte| matches!(byte, b',' | b'{' | b'[' | b':')).count() } else { 0 };
    text.len().checked_mul(8).and_then(|bytes| nodes.checked_mul(4096).and_then(|nodes| bytes.checked_add(nodes))).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| protocol("Column allocation overflow"))
}
pub(crate) fn snapshot_rows(
    table: &TableRef, columns: &[crate::wire::Column], rows: &[crate::wire::Row], memory: &Arc<MemoryBudget>,
) -> Result<ArrowChanges> {
    let schema = columns.iter().map(|column| (column.name.clone(), column.oid)).collect::<Vec<_>>();
    let batch = decode_columns(&schema, rows.len(), memory, |row, column| rows[row].cell(column))?;
    Ok(ArrowChanges { kind: RowKind::Put, table: table.clone(), rows: batch })
}

fn decode_columns<'a>(
    columns: &[(String, u32)], count: usize, memory: &Arc<MemoryBudget>,
    cell: impl Fn(usize, usize) -> Result<Option<&'a str>>,
) -> Result<OwnedBatch> {
    let overflow = || protocol("PostgreSQL Arrow batch allocation overflow");
    let metadata = columns.iter().try_fold(4096usize, |bytes, (name, _)| {
        bytes.checked_add(4096 + 8 * name.len()).ok_or_else(overflow)
    })?;
    let mut output = BatchBuilder::new(metadata, memory)?;
    let mut arrays = Vec::with_capacity(columns.len());
    let mut fields = Vec::with_capacity(columns.len());
    let base = count.checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?;
    for (index, (name, oid)) in columns.iter().enumerate() {
        let temporary = (0..count).try_fold(base, |bytes, row| {
            let size = cell(row, index)?.map(|text| text_bound(*oid, text)).transpose()?.unwrap_or_default();
            bytes.checked_add(size).ok_or_else(overflow)
        })?;
        let _values = memory.reserve(temporary)?;
        if let Some(array) = rtdb_pg_codec::read_text_column(*oid, count, |row| cell(row, index))? {
            let array = if array.null_count() == count { Arc::new(NullArray::new(count)) } else { array };
            let field = Field::new(name, array.data_type().clone(), count != 0
                && (array.null_count() != 0 || array.data_type() == &DataType::Null));
            let bound = array.get_array_memory_size().checked_add(512).ok_or_else(overflow)?;
            arrays.push(output.column(bound, || Ok(array))?);
            fields.push(field);
            continue;
        }
        let values = (0..count).map(|row| {
            cell(row, index)?.map_or(Ok(Value::Null), |text| read_text(*oid, text))
        }).collect::<Result<Vec<_>>>()?;
        let types = values.iter().map(Value::logical_type).collect::<Vec<_>>();
        let logical = LogicalType::join_all(&types);
        let field = rtdb_columnar::to_field(name, &logical)?;
        let bound = rtdb_columnar::estimate_builder_bytes(&logical, &values)?;
        arrays.push(output.column(bound, || rtdb_columnar::copy_array(&logical, &values))?);
        fields.push(field);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(count));
    let batch = RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options).map_err(protocol)?;
    output.finish(batch)
}


#[test]
fn snapshot_columns_keep_postgres_types_nulls_and_tuple_order() {
    let memory = MemoryBudget::new(1 << 20);
    let columns = [("_id".into(), 20), ("note".into(), 25)];
    let tuples = [[Some("2"), None], [Some("1"), Some("a")]];
    let batch = decode_columns(&columns, tuples.len(), &memory, |row, column| Ok(tuples[row][column])).unwrap();
    let batch = batch.batch();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema_ref().field(0).data_type(), &arrow_schema::DataType::Int64);
    let note = batch.schema_ref().field(1);
    assert_eq!(rtdb_columnar::read_value(note, batch.column(1).as_ref(), 0).unwrap(), Value::Null);
    assert_eq!(rtdb_columnar::read_value(note, batch.column(1).as_ref(), 1).unwrap(), Value::Utf8("a".into()));
}


#[test]
fn snapshot_array_columns_match_the_text_reader_for_nulls_and_escaped_values() {
    let memory = MemoryBudget::new(1 << 20);
    let columns = [("values".into(), 1007), ("labels".into(), 1009), ("missing".into(), 20)];
    let tuples = [
        [Some("{1,NULL,-2}"), Some(r#"{"NULL","a,b","q\"r"}"#), None],
        [Some("{}"), Some("{}"), None],
        [None, Some("{NULL}"), None],
    ];
    let batch = decode_columns(&columns, tuples.len(), &memory, |row, column| Ok(tuples[row][column])).unwrap();
    for (position, (_, oid)) in columns.iter().enumerate() {
        for (row, tuple) in tuples.iter().enumerate() {
            let expected = tuple[position].map(|text| read_text(*oid, text).unwrap()).unwrap_or(Value::Null);
            let field = batch.batch().schema_ref().field(position);
            assert_eq!(rtdb_columnar::read_value(field, batch.batch().column(position).as_ref(), row).unwrap(), expected);
        }
    }
}
