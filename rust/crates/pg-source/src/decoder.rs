use rtdb_types::StructFields;
use std::{collections::{BTreeMap, BTreeSet}, ops::Range, sync::Arc};
use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::Schema;
use rtdb_columnar::{BatchBuilder, MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_types::{Anomaly, LogicalType, Result, TableRef, Timestamp, Value};
use crate::{Lsn, read_text, vector_types::{VectorType, VectorTypes}, wire::{Column, Input, protocol}};
use rtdb_pg_codec::{Format, WalArrayRow, read_source_binary, read_source_binary_column, read_source_text_column, read_tstzrange, read_wal_array, tstzrange_field};

mod pending;
use pending::{DecodedCell, PendingTransaction, Run};

pub const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
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
    pub(crate) fn ordered_runs(&self) -> impl Iterator<Item = (&ArrowChanges, Range<usize>)> {
        self.batches.iter().map(|batch| (batch, 0..batch.rows.batch().num_rows()))
    }

    pub(crate) fn to_rows(&self, memory: &Arc<MemoryBudget>) -> Result<PgTransaction> {
        decode_rows(self.lsn, self.commit_time, self.ordered_runs(), memory)
    }
}

#[derive(Debug)]
pub(crate) struct DecodedTransaction {
    pub lsn: Lsn,
    pub commit_time: Option<Timestamp>,
    pub batches: Vec<ArrowChanges>,
    order: Option<Vec<Run>>,
}

impl DecodedTransaction {
    pub fn new(lsn: Lsn, commit_time: Option<Timestamp>, batches: Vec<ArrowChanges>) -> Self {
        Self { lsn, commit_time, batches, order: None }
    }

    pub fn ordered_runs(&self) -> impl Iterator<Item = (&ArrowChanges, Range<usize>)> {
        let ordered = self.order.as_deref().unwrap_or_default().iter()
            .map(|run| (&self.batches[run.batch], run.rows.clone()));
        let natural = self.order.is_none().then_some(&self.batches).into_iter().flatten()
            .map(|batch| (batch, 0..batch.rows.batch().num_rows()));
        ordered.chain(natural)
    }

    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|batch| batch.rows.batch().num_rows()).sum()
    }

    fn to_rows(&self, memory: &Arc<MemoryBudget>) -> Result<PgTransaction> {
        decode_rows(self.lsn, self.commit_time, self.ordered_runs(), memory)
    }

    pub fn compatibility_batches(&self, memory: &Arc<MemoryBudget>) -> Result<(ArrowPgTransaction, Arc<MemoryReservation>)> {
        let wal = self.order.is_some();
        let count = if wal { self.row_count() } else { self.batches.len() };
        let bound = self.ordered_runs().try_fold(4096usize, |size, (changes, range)| {
            let metadata = changes.rows.batch().schema_ref().fields().iter().try_fold(4096usize, |size, field| {
                size.checked_add(field.size().checked_mul(4).ok_or_else(overflow)?).ok_or_else(overflow)
            })?;
            size.checked_add(metadata.checked_mul(if wal { range.len() } else { 1 }).ok_or_else(overflow)?).ok_or_else(overflow)
        })?;
        let charge = memory.reserve(bound)?;
        let mut batches = Vec::with_capacity(count);
        for (changes, rows) in self.ordered_runs() {
            if wal {
                for row in rows {
                    batches.push(ArrowChanges { kind: changes.kind, table: changes.table.clone(), rows: changes.rows.slice(row, 1)? });
                }
            } else {
                batches.push(ArrowChanges { kind: changes.kind, table: changes.table.clone(), rows: changes.rows.clone() });
            }
        }
        Ok((ArrowPgTransaction { lsn: self.lsn, commit_time: self.commit_time, batches }, charge))
    }
}

fn decode_rows<'a>(
    lsn: Lsn, commit_time: Option<Timestamp>, runs: impl Iterator<Item = (&'a ArrowChanges, Range<usize>)>,
    memory: &Arc<MemoryBudget>,
) -> Result<PgTransaction> {
    let mut operations = Vec::new();
    for (changes, rows) in runs {
        let batch = changes.rows.batch();
        for row in rows {
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
    Ok(PgTransaction { lsn, commit_time, operations })
}

struct Relation { table: TableRef, columns: Vec<Column>, _charge: Arc<MemoryReservation> }
#[derive(Clone, Copy)]
enum Cell<'a> { Null, Unchanged, Text(&'a str) }

pub struct Decoder {
    relations: BTreeMap<u32, Relation>, vector_types: Option<VectorTypes>, wal_vectors: BTreeSet<u32>, pending: Option<PendingTransaction>, ignored: BTreeSet<TableRef>, memory: Arc<MemoryBudget>,
}
impl Decoder {
    pub fn new(ignored: BTreeSet<TableRef>, memory: Arc<MemoryBudget>) -> Self { Self { relations: BTreeMap::new(), vector_types: None, wal_vectors: BTreeSet::new(), pending: None, ignored, memory } }
    pub(crate) fn with_vector_types(mut self, types: VectorTypes) -> Self {
        self.vector_types = Some(types);
        self
    }

    pub fn receive(&mut self, bytes: &[u8]) -> Result<Option<PgTransaction>> {
        self.receive_decoded(bytes)?.map(|transaction| transaction.to_rows(&self.memory)).transpose()
    }

    pub fn receive_arrow(&mut self, bytes: &[u8]) -> Result<Option<ArrowPgTransaction>> {
        self.receive_decoded(bytes)?.map(|transaction| {
            transaction.compatibility_batches(&self.memory).map(|(batches, _charge)| batches)
        }).transpose()
    }

    pub(crate) fn receive_decoded(&mut self, bytes: &[u8]) -> Result<Option<DecodedTransaction>> {
        let mut input = Input::new(bytes); let tag = input.u8()?;
        match tag {
            b'R' => {
                let charge = self.memory.reserve(bytes.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| protocol("Relation allocation overflow"))?)?;
                let id = input.u32()?; let schema = input.string()?; let table = input.string()?;
                if !matches!(input.u8()?, b'd' | b'n' | b'f' | b'i') { return Err(protocol("Unknown replica identity")); }
                let count = input.u16()?; let mut columns = Vec::with_capacity(usize::from(count));
                for _ in 0..count { input.u8()?; let name = input.string()?.to_owned(); let oid = input.u32()?; let typmod = input.i32()?; columns.push(Column { name, oid, typmod, format: Format::Text }); }
                input.end()?; self.relations.insert(id, Relation { table: TableRef::new(schema, table), columns, _charge: charge });
            }
            b'Y' => {
                let oid = input.u32()?; let schema = input.string()?; let name = input.string()?; input.end()?;
                if self.vector_types.as_ref().is_some_and(|types| types.get(oid, Some((schema, name))).is_some()) {
                    self.wal_vectors.insert(oid);
                } else {
                    self.wal_vectors.remove(&oid);
                }
            }
            b'B' => { input.u64()?; input.i64()?; input.u32()?; input.end()?; self.pending = Some(PendingTransaction::default()); }
            b'C' => {
                input.u8()?; input.u64()?; let lsn = Lsn(input.u64()?); let timestamp = input.i64()?.checked_add(PG_EPOCH_MICROS).ok_or_else(|| protocol("PostgreSQL commit timestamp overflow"))?; input.end()?;
                let pending = self.pending.take().ok_or_else(|| protocol("Commit arrived without Begin"))?;
                return pending.finish(lsn, Timestamp(timestamp));
            }
            b'I' | b'U' | b'D' => {
                let pending = self.pending.as_mut().ok_or_else(|| protocol("Row change arrived outside a transaction"))?;
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
                let columns = &relation.columns[..width];
                let row_metadata = metadata(columns.iter().map(|column| column.name.as_str()))?;
                let mut row_charge = self.memory.reserve(row_metadata)?;
                let mut cells = Vec::with_capacity(width);
                for (index, Column { name, oid, typmod, .. }) in columns.iter().enumerate() {
                    let vector = self.vector_types.as_ref().filter(|_| self.wal_vectors.contains(oid)).and_then(|types| types.get(*oid, None));
                    let oid = match vector { Some(VectorType::FloatArray(base)) => base, _ => *oid };
                    let text = recover(values[index], old.as_deref(), index, name, &relation.table)?;
                    let dimension = match vector {
                        Some(VectorType::Pgvector(domain_typmod)) => Some(domain_typmod.unwrap_or(*typmod)),
                        _ => None,
                    };
                    if let Some((field, row)) = read_wal_array(name, oid, dimension, text, &self.memory)? {
                        let cell = match row {
                            WalArrayRow::List(row) => DecodedCell::List(row),
                            WalArrayRow::Text(text) => DecodedCell::ArrayText(text),
                        };
                        let charge = Arc::get_mut(&mut row_charge).expect("decoded row owns its reservation");
                        charge.grow_to(charge.bytes().checked_add(cell.retained()?).ok_or_else(overflow)?)?;
                        cells.push((field, cell));
                        continue;
                    }
                    let bound = text.map(|text| text_bound(oid, text)).transpose()?.unwrap_or_default();
                    let mut charge = self.memory.reserve(bound.checked_add(std::mem::size_of::<Value>()).ok_or_else(overflow)?)?;
                    let (field, cell) = if oid == 3910 {
                        match text.map(read_tstzrange).transpose()?.flatten() {
                            Some((lower, upper)) => (tstzrange_field(name, false)?, DecodedCell::Range([lower, upper])),
                            None => (rtdb_columnar::to_field(name, &LogicalType::Null)?, DecodedCell::Scalar(Value::Null)),
                        }
                    } else {
                        let mut column = BatchBuilder::new(0, &self.memory)?;
                        let typed = if oid == 16384 {
                            read_source_text_column(name, oid, 1, |_| Ok(text), &mut column)?
                        } else { None };
                        match typed {
                            Some((field, _)) if field.data_type() == &arrow_schema::DataType::Null => (field, DecodedCell::Scalar(Value::Null)),
                            Some((field, array)) => (field, DecodedCell::Nested(array)),
                            None => {
                                let value = text.map_or(Ok(Value::Null), |text| read_text(oid, text))?;
                                DecodedCell::from_value(name, value, &self.memory)?
                            }
                        }
                    };
                    let retained = cell.retained()?;
                    let reservation = Arc::get_mut(&mut charge).expect("decoded cell owns its reservation");
                    if retained > reservation.bytes() { reservation.grow_to(retained)?; }
                    else { reservation.shrink_to(retained)?; }
                    Arc::get_mut(&mut row_charge).expect("decoded row owns its reservation").absorb(reservation)?;
                    cells.push((field, cell));
                }
                let kind = if tag == b'D' { RowKind::Delete } else { RowKind::Put };
                pending.push(id, &relation.table, kind, cells, row_charge, row_metadata)?;
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
    wire_value_bound(oid, text.as_bytes())
}

fn wire_value_bound(oid: u32, value: &[u8]) -> Result<usize> {
    let nested = matches!(oid, 114 | 3802 | 16384 | 1007 | 1016 | 1009);
    let nodes = if nested { value.iter().copied().filter(|byte| matches!(byte, b',' | b'{' | b'[' | b':')).count() } else { 0 };
    value.len().checked_mul(8).and_then(|bytes| nodes.checked_mul(4096).and_then(|nodes| bytes.checked_add(nodes))).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| protocol("Column allocation overflow"))
}
pub(crate) fn snapshot_rows(
    table: &TableRef, columns: &[Column], rows: &[crate::wire::Row], memory: &Arc<MemoryBudget>, vectors: &VectorTypes,
) -> Result<ArrowChanges> {
    let batch = decode_columns(columns, rows.len(), memory, |oid| vectors.get(oid, None), |row, column| rows[row].cell_bytes(column))?;
    Ok(ArrowChanges { kind: RowKind::Put, table: table.clone(), rows: batch })
}

fn overflow() -> Anomaly { protocol("PostgreSQL Arrow batch allocation overflow") }

fn metadata<'a>(names: impl IntoIterator<Item = &'a str>) -> Result<usize> {
    names.into_iter().try_fold(4096usize, |bytes, name| {
        bytes.checked_add(4096 + 8 * name.len()).ok_or_else(overflow)
    })
}

fn decode_columns<'a>(
    columns: &[Column], count: usize, memory: &Arc<MemoryBudget>, vector: impl Fn(u32) -> Option<VectorType>,
    cell: impl Fn(usize, usize) -> Result<Option<&'a [u8]>>,
) -> Result<OwnedBatch> {
    let mut output = BatchBuilder::new(metadata(columns.iter().map(|column| column.name.as_str()))?, memory)?;
    let mut arrays = Vec::with_capacity(columns.len());
    let mut fields = Vec::with_capacity(columns.len());
    for (index, Column { name, oid, typmod, format }) in columns.iter().enumerate() {
        let vector = vector(*oid);
        let oid = match vector { Some(VectorType::FloatArray(base)) => base, _ => *oid };
        if vector.is_none() && *format == Format::Binary
            && let Some((field, array)) = read_source_binary_column(name, oid, count, |row| cell(row, index), &mut output)?
        {
            arrays.push(array);
            fields.push(field);
            continue;
        }
        let bound_oid = if matches!(vector, Some(VectorType::Pgvector(_))) { 700 } else { oid };
        let text = |row| cell(row, index)?.map(|bytes| std::str::from_utf8(bytes).map_err(protocol)).transpose();
        let base = count.checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?;
        let temporary = (0..count).try_fold(base, |bytes, row| {
            let size = match format {
                Format::Text => text(row)?.map(|text| text_bound(bound_oid, text)).transpose()?,
                Format::Binary => cell(row, index)?.map(|bytes| wire_value_bound(oid, bytes)).transpose()?,
            }.unwrap_or_default();
            bytes.checked_add(size).ok_or_else(overflow)
        })?;
        let _values = memory.reserve(temporary)?;
        if let Some(VectorType::Pgvector(domain_typmod)) = vector {
            let (field, array) = rtdb_pg_codec::read_vector_column(name, domain_typmod.unwrap_or(*typmod), count, text, &mut output)?;
            arrays.push(array);
            fields.push(field);
            continue;
        }
        if *format == Format::Text
            && let Some((field, array)) = read_source_text_column(name, oid, count, text, &mut output)?
        {
            arrays.push(array);
            fields.push(field);
            continue;
        }
        let values = (0..count).map(|row| {
            match format {
                Format::Text => text(row)?.map_or(Ok(Value::Null), |text| read_text(oid, text)),
                Format::Binary => cell(row, index)?.map_or(Ok(Value::Null), |bytes| read_source_binary(oid, bytes)),
            }
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
    let columns = [Column { name: "_id".into(), oid: 20, typmod: -1, format: Format::Text }, Column { name: "note".into(), oid: 25, typmod: -1, format: Format::Text }];
    let tuples = [[Some("2"), None], [Some("1"), Some("a")]];
    let batch = decode_columns(&columns, tuples.len(), &memory, |_| None, |row, column| Ok(tuples[row][column].map(str::as_bytes))).unwrap();
    let batch = batch.batch();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema_ref().field(0).data_type(), &arrow_schema::DataType::Int64);
    let note = batch.schema_ref().field(1);
    assert_eq!(rtdb_columnar::read_value(note, batch.column(1).as_ref(), 0).unwrap(), Value::Null);
    assert_eq!(rtdb_columnar::read_value(note, batch.column(1).as_ref(), 1).unwrap(), Value::Utf8("a".into()));
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn relation(id: u32, table: &str, columns: &[(&str, u32)]) -> Vec<u8> {
        let mut message = vec![b'R'];
        message.extend(id.to_be_bytes());
        message.extend(b"public\0");
        message.extend(table.as_bytes());
        message.extend(b"\0f");
        message.extend((columns.len() as u16).to_be_bytes());
        for (name, oid) in columns {
            message.push(0);
            message.extend(name.as_bytes());
            message.push(0);
            message.extend(oid.to_be_bytes());
            message.extend((-1i32).to_be_bytes());
        }
        message
    }

    fn row(tag: u8, relation: u32, fields: &[Option<&str>]) -> Vec<u8> {
        let mut message = vec![tag];
        message.extend(relation.to_be_bytes());
        message.push(if tag == b'D' { b'O' } else { b'N' });
        message.extend((fields.len() as u16).to_be_bytes());
        for field in fields {
            if let Some(text) = field {
                message.push(b't');
                message.extend((text.len() as i32).to_be_bytes());
                message.extend(text.as_bytes());
            } else { message.push(b'n'); }
        }
        message
    }

    pub fn transactions(memory: &Arc<MemoryBudget>) -> (DecodedTransaction, DecodedTransaction) {
        let mut decoder = Decoder::new(BTreeSet::new(), Arc::clone(memory));
        let columns = [("_id", 23), ("data", 1007), ("_valid_from", 1184), ("_valid_to", 1184)];
        decoder.receive_decoded(&relation(1, "a", &columns)).unwrap();
        decoder.receive_decoded(&relation(2, "b", &columns)).unwrap();
        decoder.receive_decoded(&[b'B'].into_iter().chain([0; 20]).collect::<Vec<_>>()).unwrap();

        let changes = [
            (b'I', 1, vec![Some("1"), Some("{2,NULL}"), Some("2020-01-01 00:00:00+00"), None]),
            (b'I', 1, vec![Some("1"), Some("{5,NULL}"), Some("2020-01-01 00:00:00+00"), None]),
            (b'I', 2, vec![Some("2"), Some("{3,7}"), None, None]),
            (b'D', 1, vec![Some("1")]),
            (b'I', 1, vec![Some("1"), Some("{9,NULL}"), Some("2020-01-01 00:00:00+00"), None]),
            (b'I', 1, vec![Some("3"), None, None, None]),
        ];
        let mut legacy = Vec::new();
        for (tag, relation_id, text) in changes {
            assert!(decoder.receive_decoded(&row(tag, relation_id, &text)).unwrap().is_none());
            let mut output = BatchBuilder::new(16_384, memory).unwrap();
            let mut fields = Vec::new();
            let mut arrays = Vec::new();
            for ((name, oid), value) in columns.iter().zip(text) {
                let value = value.map_or(Ok(Value::Null), |text| read_text(*oid, text)).unwrap();
                let logical = LogicalType::join_all([&value.logical_type()]);
                fields.push(rtdb_columnar::to_field(*name, &logical).unwrap());
                let values = [value];
                let bound = rtdb_columnar::estimate_builder_bytes(&logical, &values).unwrap();
                arrays.push(output.column(bound, || rtdb_columnar::copy_array(&logical, &values)).unwrap());
            }
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
            legacy.push(ArrowChanges {
                kind: if tag == b'D' { RowKind::Delete } else { RowKind::Put },
                table: TableRef::new("public", if relation_id == 1 { "a" } else { "b" }),
                rows: output.finish(batch).unwrap(),
            });
        }
        let actual = decoder.receive_decoded(&[b'C'].into_iter().chain([0; 25]).collect::<Vec<_>>()).unwrap().unwrap();
        let legacy = DecodedTransaction::new(actual.lsn, actual.commit_time, legacy);
        (actual, legacy)
    }

    #[test]
    fn pg_source_arrow_wal_groups_rows_and_restores_order() {
        let memory = MemoryBudget::new(1 << 22);
        let (actual, legacy) = transactions(&memory);
        assert_eq!(actual.batches.iter().map(|batch| batch.rows.batch().num_rows()).collect::<Vec<_>>(), [3, 1, 1, 1]);
        let (compatible, compatibility_charge) = actual.compatibility_batches(&memory).unwrap();
        assert!(compatible.batches.iter().all(|batch| batch.rows.batch().num_rows() == 1));
        let rows = compatible.to_rows(&memory).unwrap();
        let expected = legacy.to_rows(&memory).unwrap();
        assert_eq!(rows.operations.len(), expected.operations.len());
        for (actual, expected) in rows.operations.iter().zip(expected.operations.iter()) {
            assert_eq!((&actual.table, actual.kind, &actual.row), (&expected.table, expected.kind, &expected.row));
        }
        let detached = Arc::clone(actual.batches[0].rows.batch().column(0));
        drop((actual, legacy, compatible, compatibility_charge, rows, expected));
        assert!(memory.used_bytes() > 0);
        drop(detached);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn wal_list_groups_preserve_null_empty_and_text_shapes_in_event_order() {
        let memory = MemoryBudget::new(1 << 20);
        let mut decoder = Decoder::new(BTreeSet::new(), Arc::clone(&memory));
        let description = [("_id", 23), ("floats", 1021), ("ints", 1007), ("strings", 1009)];
        let columns = description.map(|(name, oid)| column(name, oid));
        decoder.receive_decoded(&relation(1, "docs", &description)).unwrap();
        decoder.receive_decoded(&[b'B'].into_iter().chain([0; 20]).collect::<Vec<_>>()).unwrap();

        let typed = [Some("1"), Some("{-0,NULL}"), Some("{NULL,2}"), Some(r#"{"NULL",NULL}"#)];
        let bounded = [Some("2"), Some("[0:1]={1,2}"), None, None];
        let nested = [Some("3"), Some("{{1,2},{3,4}}"), None, None];
        let empty = [Some("4"), Some("{}"), Some("{}"), Some("{}")];
        let all_null = [Some("5"), None, Some("{NULL,NULL}"), Some("{NULL}")];
        let changes = [
            (b'I', typed), (b'I', bounded), (b'D', typed), (b'I', empty),
            (b'I', all_null), (b'I', typed), (b'I', nested), (b'I', all_null), (b'I', typed),
        ];
        for (tag, values) in changes {
            decoder.receive_decoded(&row(tag, 1, &values)).unwrap();
        }
        let transaction = decoder.receive_decoded(&[b'C'].into_iter().chain([0; 25]).collect::<Vec<_>>()).unwrap().unwrap();
        assert_eq!(transaction.batches.iter().map(|batch| batch.rows.batch().num_rows()).collect::<Vec<_>>(), [3, 2, 1, 1, 2]);
        let ordered = transaction.ordered_runs().flat_map(|(changes, rows)| rows.map(move |row| (changes, row))).collect::<Vec<_>>();
        assert_eq!(ordered.len(), changes.len());
        for ((actual, position), (tag, values)) in ordered.into_iter().zip(changes) {
            assert_eq!(actual.kind, if tag == b'D' { RowKind::Delete } else { RowKind::Put });
            let expected = decode_columns(&columns, 1, &memory, |_| None, |_, col| Ok(values[col].map(str::as_bytes))).unwrap();
            let actual = actual.rows.batch().slice(position, 1);
            assert_eq!(actual.schema_ref(), expected.batch().schema_ref());
            for (actual, expected) in actual.columns().iter().zip(expected.batch().columns()) {
                assert_eq!(actual.to_data(), expected.to_data());
            }
        }
    }

    #[test]
    fn malformed_wal_array_releases_row_scratch_without_changing_pending_lists() {
        let memory = MemoryBudget::new(1 << 20);
        let mut decoder = Decoder::new(BTreeSet::new(), Arc::clone(&memory));
        decoder.receive_decoded(&relation(1, "docs", &[("_id", 23), ("ints", 1016), ("floats", 1022)])).unwrap();
        decoder.receive_decoded(&[b'B'].into_iter().chain([0; 20]).collect::<Vec<_>>()).unwrap();
        let before = row(b'I', 1, &[Some("1"), Some("{4,NULL,5}"), Some("{1.5,2.5}")]);
        decoder.receive_decoded(&before).unwrap();
        let held = memory.used_bytes();

        let bad = row(b'I', 1, &[Some("2"), Some("{3,4}"), Some("{1.0,bad}")]);
        let error = decoder.receive_decoded(&bad).unwrap_err();
        let mut output = BatchBuilder::new(8192, &memory).unwrap();
        let expected = read_source_text_column("floats", 1022, 1, |_| Ok(Some("{1.0,bad}")), &mut output).unwrap_err();
        assert_eq!(error, expected);
        drop(output);
        assert_eq!(memory.used_bytes(), held);

        let old = row(b'U', 1, &[Some("1"), Some("{9,NULL}"), Some("{3.5,4.5}")]);
        let mut new = row(b'U', 1, &[Some("1"), None, Some("{5.5,6.5}")]);
        let second_cell = row(b'U', 1, &[Some("1")]).len();
        new[second_cell] = b'u';
        let mut update = old;
        update[5] = b'O';
        update.extend_from_slice(&new[5..]);
        decoder.receive_decoded(&update).unwrap();

        let transaction = decoder.receive_decoded(&[b'C'].into_iter().chain([0; 25]).collect::<Vec<_>>()).unwrap().unwrap();
        assert_eq!(transaction.batches.len(), 1);
        assert_eq!(transaction.row_count(), 2);
        let rows = transaction.to_rows(&memory).unwrap();
        assert_eq!(rows.operations[1].row["ints"], Value::list(vec![Value::I64(9), Value::Null]));
        drop((decoder, transaction, rows));
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn wal_array_relation_changes_keep_equal_arrow_layouts_in_the_same_group() {
        use arrow_array::{Float64Array, ListArray};

        let memory = MemoryBudget::new(1 << 20);
        let mut decoder = Decoder::new(BTreeSet::new(), memory);
        decoder.receive_decoded(&[b'B'].into_iter().chain([0; 20]).collect::<Vec<_>>()).unwrap();
        for (oid, text) in [(114, "[1.5,2.5]"), (1022, "{3.5,4.5}"), (114, "[5.5,6.5]")] {
            decoder.receive_decoded(&relation(1, "docs", &[("prices", oid)])).unwrap();
            decoder.receive_decoded(&row(b'I', 1, &[Some(text)])).unwrap();
        }
        let transaction = decoder.receive_decoded(&[b'C'].into_iter().chain([0; 25]).collect::<Vec<_>>()).unwrap().unwrap();
        assert_eq!(transaction.batches.len(), 1);
        let batch = transaction.batches[0].rows.batch();
        let list = batch.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        let child = list.values().as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(list.value_offsets(), &[0, 2, 4, 6]);
        assert_eq!(child.values().as_ref(), &[1.5, 2.5, 3.5, 4.5, 5.5, 6.5]);
    }

    #[test]
    fn wal_ranges_keep_null_groups_endpoint_order_and_recovered_toast() {
        use arrow_array::{Array, FixedSizeListArray, TimestampMicrosecondArray};

        let memory = MemoryBudget::new(1 << 20);
        let mut decoder = Decoder::new(BTreeSet::new(), Arc::clone(&memory));
        decoder.receive_decoded(&relation(1, "docs", &[("period", 3910)])).unwrap();
        decoder.receive_decoded(&[b'B'].into_iter().chain([0; 20]).collect::<Vec<_>>()).unwrap();

        let lower = "[2000-01-01 00:00:00+02:00,)";
        let upper = "[,2000-01-01 00:00:00+00)";
        for (tag, text) in [(b'I', Some(lower)), (b'I', None), (b'I', Some(upper)), (b'I', Some("empty")), (b'D', Some(lower))] {
            decoder.receive_decoded(&row(tag, 1, &[text])).unwrap();
        }
        let held = memory.used_bytes();
        let invalid = "[invalid,)";
        assert_eq!(decoder.receive_decoded(&row(b'I', 1, &[Some(invalid)])).unwrap_err(), read_text(3910, invalid).unwrap_err());
        assert_eq!(memory.used_bytes(), held);

        let mut update = row(b'U', 1, &[Some("[,)")]);
        update[5] = b'O';
        update.extend_from_slice(b"N\0\x01u");
        decoder.receive_decoded(&update).unwrap();

        let transaction = decoder.receive_decoded(&[b'C'].into_iter().chain([0; 25]).collect::<Vec<_>>()).unwrap().unwrap();
        assert_eq!(transaction.batches.iter().map(|change| change.rows.batch().num_rows()).collect::<Vec<_>>(), [3, 2, 1]);
        let batch = transaction.batches[0].rows.batch();
        assert_eq!(batch.schema_ref().field(0), &tstzrange_field("period", false).unwrap());
        let ranges = batch.column(0).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let endpoints = ranges.values().as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        assert_eq!(ranges.null_count(), 0);
        assert_eq!(endpoints.null_count(), 0);
        assert_eq!(endpoints.timezone(), Some("UTC"));
        assert_eq!(endpoints.values().as_ref(), &[PG_EPOCH_MICROS - 7_200_000_000, i64::MAX, i64::MIN, PG_EPOCH_MICROS, i64::MIN, i64::MAX]);

        let rows = transaction.to_rows(&memory).unwrap();
        let ordered = rows.operations.iter().map(|op| (op.kind, op.row["period"].clone())).collect::<Vec<_>>();
        assert_eq!(ordered, [
            (RowKind::Put, read_text(3910, lower).unwrap()),
            (RowKind::Put, Value::Null),
            (RowKind::Put, read_text(3910, upper).unwrap()),
            (RowKind::Put, Value::Null),
            (RowKind::Delete, read_text(3910, lower).unwrap()),
            (RowKind::Put, Value::TimestampTzRange { lower: i64::MIN, upper: i64::MAX }),
        ]);
        let detached = Arc::clone(ranges.values());
        drop((decoder, transaction, rows));
        assert!(memory.used_bytes() > 0);
        drop(detached);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn pg_source_arrow_snapshot_binary_and_text_layouts_are_identical() {
        let memory = MemoryBudget::new(1 << 22);
        let text = [Column { name: "_id".into(), oid: 20, typmod: -1, format: Format::Text },
            Column { name: "doc".into(), oid: 3802, typmod: -1, format: Format::Text },
            Column { name: "other".into(), oid: 90000, typmod: -1, format: Format::Text }];
        let binary = [Column { format: Format::Binary, ..text[0].clone() },
            Column { format: Format::Binary, ..text[1].clone() }, text[2].clone()];
        let expected_data: [[Option<&[u8]>; 3]; 2] = [
            [Some(b"-17"), Some(br#"{"a":1}"#), Some(b"enum-name")],
            [Some(b"24"), Some(br#"{"b":null}"#), None],
        ];
        let first = (-17i64).to_be_bytes();
        let second = 24i64.to_be_bytes();
        let data: [[Option<&[u8]>; 3]; 2] = [
            [Some(&first), Some(b"\x01{\"a\":1}"), Some(b"enum-name")],
            [Some(&second), Some(b"\x01{\"b\":null}"), None],
        ];
        let expected = decode_columns(&text, 2, &memory, |_| None, |r, c| Ok(expected_data[r][c])).unwrap();
        let actual = decode_columns(&binary, 2, &memory, |_| None, |r, c| Ok(data[r][c])).unwrap();
        let ipc = |batch: &OwnedBatch| rtdb_format::ipc::write_stream(batch.batch().schema_ref(), std::slice::from_ref(batch.batch())).unwrap();
        assert_eq!(actual.batch().schema(), expected.batch().schema());
        assert!(ipc(&actual) == ipc(&expected));
    }

    #[test]
    fn snapshot_binary_text_fits_the_arrow_budget_and_keeps_the_batch_charge() {
        use arrow_array::{Array, StringArray};

        let memory = MemoryBudget::new(48 << 10);
        let columns = [Column { format: Format::Binary, ..column("name", 1043) }];
        let text = b"0123456789abcdef0123456789abcdef";
        let batch = decode_columns(&columns, 1000, &memory, |_| None, |row, _| {
            Ok((row != 499).then_some(text.as_slice()))
        }).unwrap();
        let values = Arc::clone(batch.batch().column(0));
        drop(batch);
        let strings = values.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value(0).as_bytes(), text);
        assert!(strings.is_null(499));
        assert_eq!(strings.value(999).as_bytes(), text);
        assert!(memory.used_bytes() > text.len() * 999);
        drop(values);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn direct_binary_snapshot_columns_preserve_null_layouts_and_payload_errors() {
        let memory = MemoryBudget::new(1 << 20);
        for (oid, payload) in [
            (16, b"\x02".as_slice()),
            (21, b"\x00"),
            (23, b"\x00\x00\x00\x00\x00"),
            (20, b"\x00"),
            (25, b"\xff"),
            (2950, b"\x00"),
        ] {
            let columns = [Column { format: Format::Binary, ..column("value", oid) }];
            let expected = read_source_binary(oid, payload).unwrap_err();
            let actual = decode_columns(&columns, 1, &memory, |_| None, |_, _| Ok(Some(payload))).unwrap_err();
            assert_eq!(actual, expected, "OID {oid}");
            for count in [0, 2] {
                let batch = decode_columns(&columns, count, &memory, |_| None, |_, _| Ok(None)).unwrap();
                let field = batch.batch().schema_ref().field(0);
                assert_eq!(field.data_type(), &arrow_schema::DataType::Null);
                assert_eq!(field.is_nullable(), count != 0);
                assert_eq!(batch.batch().num_rows(), count);
            }
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn snapshot_array_columns_match_the_text_reader_for_nulls_and_escaped_values() {
    let memory = MemoryBudget::new(1 << 20);
    let columns = [column("values", 1007), column("labels", 1009), column("missing", 20)];
    let tuples = [
        [Some("{1,NULL,-2}"), Some(r#"{"NULL","a,b","q\"r"}"#), None],
        [Some("{}"), Some("{}"), None],
        [None, Some("{NULL}"), None],
    ];
    let batch = decode_columns(&columns, tuples.len(), &memory, |_| None, |row, column| Ok(tuples[row][column].map(str::as_bytes))).unwrap();
    for (position, Column { oid, .. }) in columns.iter().enumerate() {
        for (row, tuple) in tuples.iter().enumerate() {
            let expected = tuple[position].map(|text| read_text(*oid, text).unwrap()).unwrap_or(Value::Null);
            let field = batch.batch().schema_ref().field(position);
            assert_eq!(rtdb_columnar::read_value(field, batch.batch().column(position).as_ref(), row).unwrap(), expected);
        }
    }
}

#[test]
fn source_scalar_columns_match_legacy_arrays_metadata_and_errors() {
    let memory = MemoryBudget::new(1 << 20);
    let fixtures: &[(u32, &[&str], &str)] = &[
        (17, &[r"\x00ff", "raw", r"\x"], r"\x0"),
        (2950, &["f81d4fae-7dec-11d0-a765-00a0c91e6bf6"], "bad-uuid"),
        (1082, &["1969-12-31", "2024-02-29"], "2025-02-29"),
        (1114, &["1969-12-31 23:59:59.999999999", "2025-02-28 24:00:00"], "2000-01-01 00:00:00+01:00"),
        (1184, &["2000-01-01 00:00:00+00", "2000-01-01 00:00:00-05:30", "2020-01-01 00:00:00+00"], "2000-01-01 00:00:00"),
        (1186, &["P1Y2M", "P1DT0.000123S", "PT0.000000001S", "P0D"], "bad-interval"),
        (1700, &["0", "-1.20", "1.40", "123456789012345678901234567890123456789", "1E-129", "1E+129"], "NaN"),
    ];
    for &(oid, texts, invalid) in fixtures {
        for cells in [
            std::iter::once(None).chain(texts.iter().copied().map(Some)).collect::<Vec<_>>(),
            texts.iter().copied().map(Some).collect(),
            vec![None, Some(texts[0]), None],
            vec![None],
            vec![],
        ] {
            let values = cells.iter().map(|text| text.map_or(Ok(Value::Null), |text| read_text(oid, text))).collect::<Result<Vec<_>>>().unwrap();
            let logical = LogicalType::join_all(&values.iter().map(Value::logical_type).collect::<Vec<_>>());
            let expected_field = rtdb_columnar::to_field("scalar", &logical).unwrap();
            let expected_array = rtdb_columnar::copy_array(&logical, &values).unwrap();
            let mut builder = BatchBuilder::new(8192, &memory).unwrap();
            let (field, array) = rtdb_pg_codec::read_source_text_column("scalar", oid, cells.len(), |row| Ok(cells[row]), &mut builder).unwrap().unwrap();
            assert_eq!(field, expected_field, "OID {oid}");
            assert_eq!(array.to_data(), expected_array.to_data(), "OID {oid}");
        }
        let expected = read_text(oid, invalid).unwrap_err();
        let actual = decode_columns(&[column("scalar", oid)], 1, &memory, |_| None, |_, _| Ok(Some(invalid.as_bytes()))).unwrap_err();
        assert_eq!((actual.category, actual.code, actual.message), (expected.category, expected.code, expected.message), "OID {oid}");
    }
}

#[cfg(test)]
fn column(name: &str, oid: u32) -> Column { Column { name: name.into(), oid, typmod: -1, format: Format::Text } }

#[test]
fn floating_arrays_and_pgvector_build_typed_children_with_nulls() {
    use arrow_array::{Array, FixedSizeListArray, Float32Array, Float64Array, ListArray};
    use arrow_schema::DataType;

    let memory = MemoryBudget::new(1 << 20);
    let types = VectorTypes::fixture();
    let columns = [column("reals", 1021), column("doubles", 1022), Column { typmod: 2, ..column("embedding", 20345) }];
    let tuples = [[Some("{-0,NaN,NULL}"), Some("{-0,Infinity,NULL}"), Some("[-0,1.5]")], [None; 3], [Some("{}"), Some("{NULL}"), Some("[1,2]")]];
    let batch = decode_columns(&columns, tuples.len(), &memory, |oid| types.get(oid, None), |row, column| Ok(tuples[row][column].map(str::as_bytes))).unwrap();
    let batch = batch.batch();
    let reals = batch.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let children = reals.values().as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(children.value(0).to_bits(), (-0.0_f32).to_bits());
    assert!(children.value(1).is_nan());
    assert!(children.is_null(2));
    assert!(reals.is_null(1));
    assert_eq!(reals.value_length(2), 0);
    let doubles = batch.column(1).as_any().downcast_ref::<ListArray>().unwrap();
    let children = doubles.values().as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(children.value(0).to_bits(), (-0.0_f64).to_bits());
    assert_eq!(children.value(1), f64::INFINITY);
    assert!(children.is_null(2) && children.is_null(3));
    let vector = batch.column(2).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    assert!(vector.is_null(1));
    let children = vector.values().as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(children.value(0).to_bits(), (-0.0_f32).to_bits());
    assert_eq!(children.value(1), 1.5);
    assert!(children.is_null(2) && children.is_null(3));
    assert!(matches!(batch.schema_ref().field(2).data_type(), DataType::FixedSizeList(_, 2)));
}

#[test]
fn wal_uses_base_layouts_and_declared_dimensions_for_vector_types_and_domains() {
    use arrow_array::{Array, FixedSizeListArray, Float32Array, ListArray, StringArray};
    let mut decoder = Decoder::new(BTreeSet::new(), MemoryBudget::new(1 << 20)).with_vector_types(VectorTypes::fixture());
    for (oid, schema, name) in [
        (20345_u32, "embeddings", "vector"), (20346, "embeddings", "halfvec"),
        (20347, "embeddings", "vector"), (20348, "embeddings", "vector"), (20349, "", "_float4"),
    ] {
        let mut message = vec![b'Y'];
        message.extend_from_slice(&oid.to_be_bytes());
        message.extend_from_slice(schema.as_bytes());
        message.push(0);
        message.extend_from_slice(name.as_bytes());
        message.push(0);
        decoder.receive_arrow(&message).unwrap();
    }
    let mut relation = vec![b'R'];
    relation.extend_from_slice(&1_u32.to_be_bytes());
    relation.extend_from_slice(b"public\0docs\0f\0\x05");
    for (name, oid, typmod) in [("fixed", 20345_u32, 2_i32), ("any", 20346, -1), ("unrelated", 20347, 2), ("domain", 20348, -1), ("array_domain", 20349, -1)] {
        relation.push(0);
        relation.extend_from_slice(name.as_bytes());
        relation.push(0);
        relation.extend_from_slice(&oid.to_be_bytes());
        relation.extend_from_slice(&typmod.to_be_bytes());
    }
    decoder.receive_arrow(&relation).unwrap();
    let mut begin = vec![b'B']; begin.extend([0; 20]);
    decoder.receive_arrow(&begin).unwrap();
    let mut row = vec![b'I', 0, 0, 0, 1, b'N', 0, 5];
    for text in [b"[-0,1.5]".as_slice(), b"[1,2,3]", b"[4,5]", b"[-0,1.5]", b"{-0,NULL}"] {
        row.push(b't');
        row.extend_from_slice(&(text.len() as i32).to_be_bytes());
        row.extend_from_slice(text);
    }
    decoder.receive_decoded(&row).unwrap();
    let mut null_row = vec![b'I', 0, 0, 0, 1, b'N', 0, 5];
    null_row.extend([b'n'; 5]);
    decoder.receive_decoded(&null_row).unwrap();
    decoder.receive_decoded(&row).unwrap();
    decoder.receive_decoded(&null_row).unwrap();
    let mut commit = vec![b'C']; commit.extend([0; 25]);
    let transaction = decoder.receive_decoded(&commit).unwrap().unwrap();
    assert_eq!(transaction.batches.len(), 2);
    assert_eq!(transaction.ordered_runs().count(), 4);
    let batch = transaction.batches[0].rows.batch();
    let fixed = batch.column(0).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    assert_eq!(fixed.value_length(), 2);
    let child = fixed.values().as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(child.value(0).to_bits(), (-0.0_f32).to_bits());
    let variable = batch.column(1).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(variable.value_length(0), 3);
    assert_eq!(variable.value_length(1), 3);
    assert!(variable.values().as_any().is::<Float32Array>());
    assert_eq!(batch.column(2).as_any().downcast_ref::<StringArray>().unwrap().value(0), "[4,5]");
    assert!(!fixed.is_null(0));
    assert_eq!(batch.column(3).to_data(), fixed.to_data());
    let array_domain = batch.column(4).as_any().downcast_ref::<ListArray>().unwrap();
    let child = array_domain.values().as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(child.value(0).to_bits(), (-0.0_f32).to_bits());
    assert!(child.is_null(1));
    let null_batch = transaction.batches[1].rows.batch();
    let fixed_nulls = null_batch.column(3).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    assert_eq!(fixed_nulls.null_count(), 2);
    assert_eq!(fixed_nulls.values().null_count(), 4);
    let variable_nulls = null_batch.column(1).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(variable_nulls.null_count(), 2);
    assert!(variable_nulls.values().is_empty());
}

#[test]
fn json_float_lists_retain_bits_when_the_surrounding_document_is_dynamic() {
    use arrow_array::{Array, Float64Array};

    for oid in [114, 3802] {
        let values = read_text(oid, "[-0.0,null,1.2345678901234567]").unwrap();
        let Value::List(list) = values else { panic!("expected JSON list"); };
        let array = list.array().unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(array.value(0).to_bits(), (-0.0_f64).to_bits());
        assert!(array.is_null(1));
        assert_eq!(array.value(2).to_bits(), 1.2345678901234567_f64.to_bits());
    }
}

#[test]
fn float_arrays_keep_multidimensional_and_explicitly_bounded_cells_as_text() {
    use arrow_array::{Float32Array, ListArray, StringArray, UnionArray};

    let memory = MemoryBudget::new(1 << 20);
    let texts = [Some("{-0,NULL}"), Some("[0:1]={2,3}"), Some("{{1,2},{3,4}}"), None];
    let batch = decode_columns(&[column("array", 1021)], texts.len(), &memory, |_| None, |row, _| Ok(texts[row].map(str::as_bytes))).unwrap();
    let column = batch.batch().column(0).as_any().downcast_ref::<UnionArray>().unwrap();
    let child = column.child(column.type_id(0)).as_any().downcast_ref::<ListArray>().unwrap();
    assert!(child.values().as_any().is::<Float32Array>());
    for row in [1, 2] {
        let child = column.child(column.type_id(row)).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(child.value(column.value_offset(row)), texts[row].unwrap());
    }
}
