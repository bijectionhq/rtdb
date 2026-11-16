use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, Result, TableRef, Timestamp, Value};
use crate::{Lsn, read_text, wire::{Input, protocol}};

pub const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowKind { Put, Delete }
#[derive(Debug)]
pub struct RowOperation { pub kind: RowKind, pub table: TableRef, pub row: BTreeMap<String, Value>, _charge: Arc<MemoryReservation> }
#[derive(Debug)]
pub struct PgTransaction { pub lsn: Lsn, pub commit_time: Option<Timestamp>, pub operations: Vec<RowOperation> }
impl PgTransaction { pub fn empty(lsn: Lsn) -> Self { Self { lsn, commit_time: None, operations: Vec::new() } } }
struct Relation { table: TableRef, columns: Vec<(String, u32)>, _charge: Arc<MemoryReservation> }
#[derive(Clone, Copy)]
enum Cell<'a> { Null, Unchanged, Text(&'a str) }

pub struct Decoder {
    relations: BTreeMap<u32, Relation>, operations: Vec<RowOperation>, ignored: BTreeSet<TableRef>, memory: Arc<MemoryBudget>, in_transaction: bool,
}
impl Decoder {
    pub fn new(ignored: BTreeSet<TableRef>, memory: Arc<MemoryBudget>) -> Self { Self { relations: BTreeMap::new(), operations: Vec::new(), ignored, memory, in_transaction: false } }
    pub fn receive(&mut self, bytes: &[u8]) -> Result<Option<PgTransaction>> {
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
                return Ok(Some(PgTransaction { lsn, commit_time: Some(Timestamp(timestamp)), operations: std::mem::take(&mut self.operations) }));
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
                let mut bound = 4096usize;
                for (index, ((name, oid), cell)) in relation.columns.iter().zip(&values).enumerate() {
                    let cell = recover(*cell, old.as_deref(), index, name, &relation.table)?;
                    let value_bytes = cell.map(|text| text_bound(*oid, text)).transpose()?.unwrap_or_default();
                    bound = bound.checked_add(4096 + name.len()).and_then(|bytes| bytes.checked_add(value_bytes)).ok_or_else(|| protocol("Row allocation overflow"))?;
                }
                let charge = self.memory.reserve(bound)?;
                let mut row = BTreeMap::new();
                for (index, ((name, oid), cell)) in relation.columns.iter().zip(&values).enumerate() {
                    row.insert(name.clone(), recover(*cell, old.as_deref(), index, name, &relation.table)?.map_or(Ok(Value::Null), |text| read_text(*oid, text))?);
                }
                self.operations.push(RowOperation { kind: if tag == b'D' { RowKind::Delete } else { RowKind::Put }, table: relation.table.clone(), row, _charge: charge });
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
pub(crate) fn snapshot_row(table: &TableRef, columns: &[crate::wire::Column], row: &crate::wire::Row, memory: &Arc<MemoryBudget>) -> Result<RowOperation> {
    let mut bound = row.encoded_size().checked_add(4096).ok_or_else(|| protocol("Snapshot row allocation overflow"))?;
    for (index, column) in columns.iter().enumerate() { let value_bytes = row.cell(index)?.map(|text| text_bound(column.oid, text)).transpose()?.unwrap_or_default(); bound = bound.checked_add(4096 + column.name.len()).and_then(|bound| bound.checked_add(value_bytes)).ok_or_else(|| protocol("Snapshot row allocation overflow"))?; }
    let charge = memory.reserve(bound)?;
    let row = columns.iter().enumerate().map(|(index, column)| Ok((column.name.clone(), row.cell(index)?.map_or(Ok(Value::Null), |text| read_text(column.oid, text))?))).collect::<Result<_>>()?;
    Ok(RowOperation { kind: RowKind::Put, table: table.clone(), row, _charge: charge })
}
