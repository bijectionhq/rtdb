use rtdb_types::StructFields;
use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};
use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::Schema;
use rtdb_columnar::{BatchBuilder, MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_types::{Anomaly, LogicalType, Result, TableRef, Timestamp, Value};
use crate::{Lsn, read_text, vector_types::VectorTypes, wire::{Column, Input, protocol}};

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
struct Relation { table: TableRef, columns: Vec<Column>, _charge: Arc<MemoryReservation> }
#[derive(Clone, Copy)]
enum Cell<'a> { Null, Unchanged, Text(&'a str) }

pub struct Decoder {
    relations: BTreeMap<u32, Relation>, vector_types: Option<VectorTypes>, wal_vectors: BTreeSet<u32>, operations: Vec<ArrowChanges>, ignored: BTreeSet<TableRef>, memory: Arc<MemoryBudget>, in_transaction: bool,
}
impl Decoder {
    pub fn new(ignored: BTreeSet<TableRef>, memory: Arc<MemoryBudget>) -> Self { Self { relations: BTreeMap::new(), vector_types: None, wal_vectors: BTreeSet::new(), operations: Vec::new(), ignored, memory, in_transaction: false } }
    pub(crate) fn with_vector_types(mut self, types: VectorTypes) -> Self {
        self.vector_types = Some(types);
        self
    }

    pub fn receive(&mut self, bytes: &[u8]) -> Result<Option<PgTransaction>> {
        self.receive_arrow(bytes)?.map(|transaction| transaction.to_rows(&self.memory)).transpose()
    }

    pub fn receive_arrow(&mut self, bytes: &[u8]) -> Result<Option<ArrowPgTransaction>> {
        let mut input = Input::new(bytes); let tag = input.u8()?;
        match tag {
            b'R' => {
                let charge = self.memory.reserve(bytes.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| protocol("Relation allocation overflow"))?)?;
                let id = input.u32()?; let schema = input.string()?; let table = input.string()?;
                if !matches!(input.u8()?, b'd' | b'n' | b'f' | b'i') { return Err(protocol("Unknown replica identity")); }
                let count = input.u16()?; let mut columns = Vec::with_capacity(usize::from(count));
                for _ in 0..count { input.u8()?; let name = input.string()?.to_owned(); let oid = input.u32()?; let typmod = input.i32()?; columns.push(Column { name, oid, typmod }); }
                input.end()?; self.relations.insert(id, Relation { table: TableRef::new(schema, table), columns, _charge: charge });
            }
            b'Y' => {
                let oid = input.u32()?; let schema = input.string()?; let name = input.string()?; input.end()?;
                if self.vector_types.as_ref().is_some_and(|types| types.matches(oid, Some((schema, name)))) {
                    self.wal_vectors.insert(oid);
                } else {
                    self.wal_vectors.remove(&oid);
                }
            }
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
                let rows = decode_columns(&relation.columns[..width], 1, &self.memory, |oid| self.wal_vectors.contains(&oid), |_, index| {
                    let name = &relation.columns[index].name;
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
    table: &TableRef, columns: &[Column], rows: &[crate::wire::Row], memory: &Arc<MemoryBudget>, vectors: &VectorTypes,
) -> Result<ArrowChanges> {
    let batch = decode_columns(columns, rows.len(), memory, |oid| vectors.matches(oid, None), |row, column| rows[row].cell(column))?;
    Ok(ArrowChanges { kind: RowKind::Put, table: table.clone(), rows: batch })
}

fn decode_columns<'a>(
    columns: &[Column], count: usize, memory: &Arc<MemoryBudget>, vector: impl Fn(u32) -> bool,
    cell: impl Fn(usize, usize) -> Result<Option<&'a str>>,
) -> Result<OwnedBatch> {
    let overflow = || protocol("PostgreSQL Arrow batch allocation overflow");
    let metadata = columns.iter().try_fold(4096usize, |bytes, column| {
        bytes.checked_add(4096 + 8 * column.name.len()).ok_or_else(overflow)
    })?;
    let mut output = BatchBuilder::new(metadata, memory)?;
    let mut arrays = Vec::with_capacity(columns.len());
    let mut fields = Vec::with_capacity(columns.len());
    let base = count.checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?;
    for (index, Column { name, oid, typmod }) in columns.iter().enumerate() {
        let vector = vector(*oid);
        let temporary = (0..count).try_fold(base, |bytes, row| {
            let size = cell(row, index)?.map(|text| text_bound(if vector { 700 } else { *oid }, text)).transpose()?.unwrap_or_default();
            bytes.checked_add(size).ok_or_else(overflow)
        })?;
        let _values = memory.reserve(temporary)?;
        if vector {
            let (field, array) = rtdb_pg_codec::read_vector_column(name, *typmod, count, |row| cell(row, index), &mut output)?;
            arrays.push(array);
            fields.push(field);
            continue;
        }
        if let Some((field, array)) = rtdb_pg_codec::read_source_text_column(name, *oid, count, |row| cell(row, index), &mut output)? {
            arrays.push(array);
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
    let columns = [column("_id", 20), column("note", 25)];
    let tuples = [[Some("2"), None], [Some("1"), Some("a")]];
    let batch = decode_columns(&columns, tuples.len(), &memory, |_| false, |row, column| Ok(tuples[row][column])).unwrap();
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
    let columns = [column("values", 1007), column("labels", 1009), column("missing", 20)];
    let tuples = [
        [Some("{1,NULL,-2}"), Some(r#"{"NULL","a,b","q\"r"}"#), None],
        [Some("{}"), Some("{}"), None],
        [None, Some("{NULL}"), None],
    ];
    let batch = decode_columns(&columns, tuples.len(), &memory, |_| false, |row, column| Ok(tuples[row][column])).unwrap();
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
        let actual = decode_columns(&[column("scalar", oid)], 1, &memory, |_| false, |_, _| Ok(Some(invalid))).unwrap_err();
        assert_eq!((actual.category, actual.code, actual.message), (expected.category, expected.code, expected.message), "OID {oid}");
    }
}

#[cfg(test)]
fn column(name: &str, oid: u32) -> Column { Column { name: name.into(), oid, typmod: -1 } }

#[test]
fn floating_arrays_and_pgvector_build_typed_children_with_nulls() {
    use arrow_array::{Array, FixedSizeListArray, Float32Array, Float64Array, ListArray};
    use arrow_schema::DataType;

    let memory = MemoryBudget::new(1 << 20);
    let types = VectorTypes::fixture();
    let columns = [column("reals", 1021), column("doubles", 1022), Column { typmod: 2, ..column("embedding", 20345) }];
    let tuples = [[Some("{-0,NaN,NULL}"), Some("{-0,Infinity,NULL}"), Some("[-0,1.5]")], [None; 3], [Some("{}"), Some("{NULL}"), Some("[1,2]")]];
    let batch = decode_columns(&columns, tuples.len(), &memory, |oid| types.matches(oid, None), |row, column| Ok(tuples[row][column])).unwrap();
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
fn wal_uses_the_declared_dimension_for_the_named_extension_type() {
    use arrow_array::{Array, FixedSizeListArray, Float32Array, ListArray, StringArray};
    let mut decoder = Decoder::new(BTreeSet::new(), MemoryBudget::new(1 << 20)).with_vector_types(VectorTypes::fixture());
    for (oid, name) in [(20345_u32, "vector"), (20346, "halfvec"), (20347, "vector")] {
        let mut message = vec![b'Y'];
        message.extend_from_slice(&oid.to_be_bytes());
        message.extend_from_slice(b"embeddings\0");
        message.extend_from_slice(name.as_bytes());
        message.push(0);
        decoder.receive_arrow(&message).unwrap();
    }
    let mut relation = vec![b'R'];
    relation.extend_from_slice(&1_u32.to_be_bytes());
    relation.extend_from_slice(b"public\0docs\0f\0\x03");
    for (name, oid, typmod) in [("fixed", 20345_u32, 2_i32), ("any", 20346, -1), ("unrelated", 20347, 2)] {
        relation.push(0);
        relation.extend_from_slice(name.as_bytes());
        relation.push(0);
        relation.extend_from_slice(&oid.to_be_bytes());
        relation.extend_from_slice(&typmod.to_be_bytes());
    }
    decoder.receive_arrow(&relation).unwrap();
    let mut begin = vec![b'B']; begin.extend([0; 20]);
    decoder.receive_arrow(&begin).unwrap();
    let mut row = vec![b'I', 0, 0, 0, 1, b'N', 0, 3];
    for text in [b"[-0,1.5]".as_slice(), b"[1,2,3]", b"[4,5]"] {
        row.push(b't');
        row.extend_from_slice(&(text.len() as i32).to_be_bytes());
        row.extend_from_slice(text);
    }
    decoder.receive_arrow(&row).unwrap();
    let mut commit = vec![b'C']; commit.extend([0; 25]);
    let transaction = decoder.receive_arrow(&commit).unwrap().unwrap();
    let batch = transaction.batches[0].rows.batch();
    let fixed = batch.column(0).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    assert_eq!(fixed.value_length(), 2);
    let child = fixed.values().as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(child.value(0).to_bits(), (-0.0_f32).to_bits());
    let variable = batch.column(1).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(variable.value_length(0), 3);
    assert!(variable.values().as_any().is::<Float32Array>());
    assert_eq!(batch.column(2).as_any().downcast_ref::<StringArray>().unwrap().value(0), "[4,5]");
    assert!(!fixed.is_null(0));
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
    let batch = decode_columns(&[column("array", 1021)], texts.len(), &memory, |_| false, |row, _| Ok(texts[row])).unwrap();
    let column = batch.batch().column(0).as_any().downcast_ref::<UnionArray>().unwrap();
    let child = column.child(column.type_id(0)).as_any().downcast_ref::<ListArray>().unwrap();
    assert!(child.values().as_any().is::<Float32Array>());
    for row in [1, 2] {
        let child = column.child(column.type_id(row)).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(child.value(column.value_offset(row)), texts[row].unwrap());
    }
}
