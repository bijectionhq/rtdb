use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, make_array};
use arrow_buffer::{Buffer, NullBuffer};
use arrow_data::{ArrayData, BufferSpec};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode};
use rtdb_types::{LogicalType, ScalarType};

use super::{FILE_MAGIC, FileWriter, supports, write_stream};
use crate::ipc::{
    ArrowFile, ArrowStream, FirstSchemaWriter, invalid, java_layout::JavaLayout, jvm_end_marker, undefined, write_file,
    write_options, write_stream_to,
};

/// The oracle the native stream writer must match byte for byte: arrow-rs's stream, decimal-patched and rewritten into the
/// reference's Java layout as it passes.
fn write_rewritten_stream_to(schema: &Schema, pages: &[RecordBatch], sink: Vec<u8>) -> rtdb_types::Result<Vec<u8>> {
    let decimal_schema = Arc::new(crate::ipc_decimal::DecimalSchema::new(schema)?);
    let sink = FirstSchemaWriter { inner: JavaLayout::new_stream(sink), schema: Arc::clone(&decimal_schema), pending: Vec::new(), expected: None, complete: false };
    let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(sink, &decimal_schema.wire, write_options()?).map_err(invalid)?;
    for page in pages { decimal_schema.validate_batch(page)?; writer.write(page).map_err(invalid)?; }
    writer.finish().map_err(invalid)?;
    let mut sink = writer.into_inner().map_err(invalid)?;
    sink.flush().map_err(invalid)?;
    if !sink.complete { return Err(invalid("Arrow stream ended before its schema")); }
    let mut bytes = sink.inner.inner;
    let end = bytes.len();
    jvm_end_marker(&mut bytes, end)?;
    Ok(bytes)
}

/// The oracle the native file writer must match byte for byte: arrow-rs's stream, decimal-patched and rewritten into the
/// reference's Java file layout with footer and magic.
fn write_rewritten_file(schema: &Schema, pages: &[RecordBatch]) -> rtdb_types::Result<Vec<u8>> {
    let decimal_schema = Arc::new(crate::ipc_decimal::DecimalSchema::new(schema)?);
    let mut bytes = Vec::new();
    bytes.write_all(FILE_MAGIC).map_err(invalid)?;
    bytes.write_all(&[0; 2]).map_err(invalid)?;
    let layout = JavaLayout::new_file(bytes, 8);
    let first = FirstSchemaWriter { inner: layout, schema: Arc::clone(&decimal_schema), pending: Vec::new(), expected: None, complete: false };
    let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(first, &decimal_schema.wire, write_options()?).map_err(invalid)?;
    for page in pages {
        decimal_schema.validate_batch(page)?;
        writer.write(&undefined::undefine(page)?).map_err(invalid)?;
    }
    writer.finish().map_err(invalid)?;
    let first = writer.into_inner().map_err(invalid)?;
    if !first.complete { return Err(invalid("Arrow file ended before its schema")); }
    let layout = first.inner;
    let footer = layout.file_footer().map_err(invalid)?;
    let mut sink = layout.inner;
    let end = sink.len();
    jvm_end_marker(&mut sink, end)?;
    sink.write_all(&footer).map_err(invalid)?;
    sink.write_all(&i32::try_from(footer.len()).map_err(invalid)?.to_le_bytes()).map_err(invalid)?;
    sink.write_all(FILE_MAGIC).map_err(invalid)?;
    Ok(sink)
}

/// xorshift64*, so that a failing case is reproducible from its seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, bound: usize) -> usize { (self.next() % bound as u64) as usize }
    fn chance(&mut self, percent: u64) -> bool { self.next() % 100 < percent }
}

fn decimal_field(name: &str, precision: i32, scale: i32, bit_width: u16) -> Field {
    rtdb_columnar::to_field(name, &LogicalType::Scalar(ScalarType::Decimal { precision, scale, bit_width })).unwrap()
}

fn leaf(rng: &mut Rng, name: String, nullable: bool) -> Field {
    let data_type = match rng.below(30) {
        0 => return Field::new(name, DataType::Null, true),
        1 => DataType::Boolean,
        2 => DataType::Int8,
        3 => DataType::Int16,
        4 => DataType::Int32,
        5 => DataType::Int64,
        6 => DataType::UInt8,
        7 => DataType::UInt16,
        8 => DataType::UInt32,
        9 => DataType::UInt64,
        10 => DataType::Float16,
        11 => DataType::Float32,
        12 => DataType::Float64,
        13 => DataType::Utf8,
        14 => DataType::LargeUtf8,
        15 => DataType::Binary,
        16 => DataType::LargeBinary,
        17 => DataType::FixedSizeBinary(16),
        18 => DataType::Date32,
        19 => DataType::Date64,
        20 => DataType::Time32([TimeUnit::Second, TimeUnit::Millisecond][rng.below(2)]),
        21 => DataType::Time64([TimeUnit::Microsecond, TimeUnit::Nanosecond][rng.below(2)]),
        22 => DataType::Timestamp(TimeUnit::Microsecond, [None, Some("UTC".into()), Some("".into()), Some("Europe/London".into())][rng.below(4)].clone()),
        23 => DataType::Duration([TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond][rng.below(4)]),
        24 => DataType::Interval([IntervalUnit::YearMonth, IntervalUnit::DayTime, IntervalUnit::MonthDayNano][rng.below(3)]),
        25 => DataType::Decimal32(7, 2),
        26 => DataType::Decimal64(15, -3),
        27 => DataType::Decimal128(38, 10),
        28 => DataType::Decimal256(76, 0),
        _ => {
            let (precision, scale, bit_width) = [(12, -400, 128), (4, -300, 64), (30, -200, 128), (70, -129, 256)][rng.below(4)];
            return decimal_field(&name, precision, scale, bit_width).with_nullable(nullable);
        }
    };
    Field::new(name, data_type, nullable)
}

fn field(rng: &mut Rng, name: String, depth: usize) -> Field {
    let nullable = rng.chance(60);
    let field = if depth < 3 && rng.chance(40) {
        let child = |rng: &mut Rng, name: &str| Arc::new(field(rng, name.to_owned(), depth + 1));
        let data_type = match rng.below(7) {
            0 => DataType::List(child(rng, "$data$")),
            1 => DataType::LargeList(child(rng, "$data$")),
            2 => DataType::FixedSizeList(child(rng, "$data$"), 1 + rng.below(3) as i32),
            3 => DataType::Struct((0..1 + rng.below(3)).map(|index| child(rng, &format!("f{index}"))).collect()),
            4 => {
                let key = Arc::new(Field::new("key", DataType::Utf8, false));
                let entries = Field::new("entries", DataType::Struct(vec![key, child(rng, "value")].into()), false);
                DataType::Map(Arc::new(entries), rng.chance(50))
            }
            mode => {
                let count = 1 + rng.below(3);
                let identity = rng.chance(50);
                let ids: Vec<i8> = (0..count as i8).map(|index| if identity { index } else { index * 3 + 1 }).collect();
                let fields: Vec<_> = (0..count).map(|index| child(rng, &format!("u{index}"))).collect();
                DataType::Union(UnionFields::try_new(ids, fields).unwrap(), if mode == 5 { UnionMode::Sparse } else { UnionMode::Dense })
            }
        };
        Field::new(name, data_type, nullable)
    } else {
        leaf(rng, name, nullable)
    };
    let mut metadata = field.metadata().clone();
    match rng.below(5) {
        0 => { metadata.insert("ARROW:extension:name".into(), "xt.uuid".into()); }
        1 => {
            metadata.insert("ARROW:extension:metadata".into(), "{}".into());
            metadata.insert("ARROW:extension:name".into(), "xt.tstz-range".into());
        }
        2 => {
            metadata.insert("zeta".into(), "1".into());
            metadata.insert("alpha".into(), "2".into());
            metadata.insert("ARROW:extension:name".into(), "xt.keyword".into());
        }
        _ => {}
    }
    field.with_metadata(metadata)
}

fn schema(rng: &mut Rng) -> Schema {
    let fields: Vec<_> = (0..rng.below(5)).map(|index| field(rng, format!("c{index}"), 0)).collect();
    let metadata = match rng.below(3) {
        0 => HashMap::from([("b".to_owned(), "2".to_owned()), ("a".to_owned(), "1".to_owned())]),
        _ => HashMap::new(),
    };
    Schema::new_with_metadata(fields, metadata)
}

fn bytes(rng: &mut Rng, length: usize) -> Buffer {
    Buffer::from((0..length).map(|_| rng.next() as u8).collect::<Vec<u8>>())
}

fn offsets<O: arrow_buffer::ArrowNativeType>(rng: &mut Rng, rows: usize, from: impl Fn(usize) -> O) -> (Buffer, usize) {
    let mut ends = vec![from(0)];
    let mut total = 0;
    for _ in 0..rows { total += rng.below(4); ends.push(from(total)); }
    (Buffer::from_vec(ends), total)
}

fn array(rng: &mut Rng, field: &Field, rows: usize) -> ArrayData {
    let data_type = field.data_type();
    let nulls = (field.is_nullable() && !matches!(data_type, DataType::Null | DataType::Union(..)) && rng.chance(50))
        .then(|| NullBuffer::from((0..rows).map(|_| !rng.chance(30)).collect::<Vec<bool>>()));
    let builder = ArrayData::builder(data_type.clone()).len(rows);
    let builder = match data_type {
        DataType::Null => return ArrayData::new_null(data_type, rows),
        DataType::Boolean => builder.add_buffer(bytes(rng, rows.div_ceil(8))),
        DataType::Utf8 | DataType::Binary => {
            let (offsets, total) = offsets(rng, rows, |end| end as i32);
            builder.add_buffer(offsets).add_buffer(Buffer::from(vec![b'x'; total]))
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            let (offsets, total) = offsets(rng, rows, |end| end as i64);
            builder.add_buffer(offsets).add_buffer(Buffer::from(vec![b'y'; total]))
        }
        DataType::List(child) | DataType::Map(child, _) => {
            let (offsets, total) = offsets(rng, rows, |end| end as i32);
            builder.add_buffer(offsets).add_child_data(array(rng, child, total))
        }
        DataType::LargeList(child) => {
            let (offsets, total) = offsets(rng, rows, |end| end as i64);
            builder.add_buffer(offsets).add_child_data(array(rng, child, total))
        }
        DataType::FixedSizeList(child, size) => builder.add_child_data(array(rng, child, rows * *size as usize)),
        DataType::Struct(children) => children.iter().fold(builder, |builder, child| builder.add_child_data(array(rng, child, rows))),
        DataType::Union(children, mode) => {
            let ids: Vec<i8> = children.iter().map(|(id, _)| id).collect();
            let chosen: Vec<usize> = (0..rows).map(|_| rng.below(ids.len())).collect();
            let builder = builder.add_buffer(Buffer::from_vec(chosen.iter().map(|&index| ids[index]).collect::<Vec<i8>>()));
            match mode {
                UnionMode::Sparse => children.iter().fold(builder, |builder, (_, child)| builder.add_child_data(array(rng, child, rows))),
                UnionMode::Dense => {
                    let mut counts = vec![0i32; ids.len()];
                    let offsets: Vec<i32> = chosen.iter().map(|&index| { counts[index] += 1; counts[index] - 1 }).collect();
                    let builder = builder.add_buffer(Buffer::from_vec(offsets));
                    children.iter().zip(counts).fold(builder, |builder, ((_, child), count)| builder.add_child_data(array(rng, child, count as usize)))
                }
            }
        }
        _ => {
            let BufferSpec::FixedWidth { byte_width, .. } = arrow_data::layout(data_type).buffers[0] else { panic!("{data_type} is not fixed width") };
            builder.add_buffer(bytes(rng, rows * byte_width))
        }
    };
    builder.nulls(nulls).build().unwrap()
}

fn batch(rng: &mut Rng, schema: &Arc<Schema>, rows: usize) -> RecordBatch {
    let columns: Vec<ArrayRef> = schema.fields().iter().map(|field| make_array(array(rng, field, rows))).collect();
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows))).unwrap()
}

fn assert_same_stream(schema: &Schema, pages: &[RecordBatch], case: &str) {
    let expected = write_rewritten_stream_to(schema, pages, Vec::new()).unwrap();
    let actual = write_stream(schema, pages, Vec::new()).unwrap();
    assert!(actual == expected, "{case}: {} bytes against {}, first difference at {:?}\n{schema:#?}",
        actual.len(), expected.len(), actual.iter().zip(&expected).position(|(a, b)| a != b));
    assert_eq!(write_stream_to(schema, pages, Vec::new()).unwrap(), expected, "{case}");
}

fn assert_same_file(schema: &Schema, pages: &[RecordBatch], case: &str) {
    let expected = write_rewritten_file(schema, pages).unwrap();
    let actual = write_file(schema, pages).unwrap();
    assert!(actual == expected, "{case} (file): {} bytes against {}, first difference at {:?}\n{schema:#?}",
        actual.len(), expected.len(), actual.iter().zip(&expected).position(|(a, b)| a != b));
}

#[test]
fn native_streams_match_the_rewritten_arrow_rs_stream_byte_for_byte() {
    for seed in 1..=600u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let schema = Arc::new(schema(&mut rng));
        assert!(supports(&schema));
        let mut pages = Vec::new();
        for _ in 0..rng.below(4) {
            let rows = [0, 1, 1, 2, 7, 8, 9, 17][rng.below(8)];
            let page = batch(&mut rng, &schema, rows);
            let start = rng.below(rows + 1);
            let sliced = page.slice(start, rng.below(rows - start + 1));
            pages.push(page);
            pages.push(sliced);
        }
        let case = format!("seed {seed}");
        assert_same_stream(&schema, &pages, &case);
        assert_same_file(&schema, &pages, &case);
    }
}

#[test]
fn native_streams_match_for_log_shaped_schemas() {
    let mut rng = Rng(7);
    let put = Field::new("put-docs", DataType::Struct(vec![
        Arc::new(Field::new("iids", DataType::List(Arc::new(Field::new("$data$", DataType::FixedSizeBinary(16), false))), false)),
        Arc::new(Field::new("documents", DataType::Union(UnionFields::try_new(vec![0], vec![
            Arc::new(Field::new("docs", DataType::List(Arc::new(Field::new("$data$", DataType::Struct(vec![
                Arc::new(Field::new("_id", DataType::Utf8, false).with_metadata(HashMap::from([("ARROW:extension:name".to_owned(), "xt.keyword".to_owned())]))),
                Arc::new(decimal_field("amount", 12, -400, 128).with_nullable(true)),
            ].into()), false))), true)),
        ]).unwrap(), UnionMode::Dense), false)),
        Arc::new(Field::new("_valid_from", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true)),
    ].into()), false);
    let sql = Field::new("sql", DataType::Struct(vec![
        Arc::new(Field::new("query", DataType::Utf8, false)),
        Arc::new(Field::new("args", DataType::Binary, true)),
    ].into()), false);
    let ops = Field::new("tx-ops", DataType::List(Arc::new(Field::new("$data$", DataType::Union(
        UnionFields::try_new(vec![0, 1], vec![Arc::new(put), Arc::new(sql)]).unwrap(), UnionMode::Dense,
    ), false))), false);
    let schema = Arc::new(Schema::new(vec![ops, Field::new("system-time", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true)]));
    for rows in [0, 1, 3] {
        let pages: Vec<_> = (0..3).map(|_| batch(&mut rng, &schema, rows)).collect();
        let case = format!("{rows} rows");
        assert_same_stream(&schema, &pages, &case);
        assert_same_file(&schema, &pages, &case);
    }
    assert_same_stream(&schema, &[], "no pages");
    assert_same_file(&schema, &[], "no pages");
}

#[test]
fn native_streams_reject_what_the_rewritten_stream_rejects() {
    let declared = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let other = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let page = batch(&mut Rng(3), &other, 2);
    let reserved = Schema::new_with_metadata(vec![Field::new("v", DataType::Int64, true)],
        HashMap::from([(rtdb_columnar::INTERNAL_DECIMAL_SCALE_KEY.to_owned(), "-400".to_owned())]));
    let misplaced = Schema::new(vec![Field::new("v", DataType::Int64, true)
        .with_metadata(HashMap::from([(rtdb_columnar::INTERNAL_DECIMAL_SCALE_KEY.to_owned(), "-400".to_owned())]))]);
    for (schema, pages) in [(declared.as_ref(), vec![page]), (&reserved, vec![]), (&misplaced, vec![])] {
        let expected = write_rewritten_stream_to(schema, &pages, Vec::new()).unwrap_err();
        let actual = write_stream_to(schema, &pages, Vec::new()).unwrap_err();
        assert_eq!((actual.code, actual.message), (expected.code, expected.message));
        let expected_file = write_rewritten_file(schema, &pages).unwrap_err();
        let actual_file = write_file(schema, &pages).unwrap_err();
        assert_eq!((actual_file.code, actual_file.message), (expected_file.code, expected_file.message));
    }
}

/// XTDB never writes dictionary, view, list-view or run-end columns, so a stream holding them is written in the plain layout
/// with the same values: the bytes a plain page of those values gives, which XTDB reads.
#[test]
fn layouts_xtdb_does_not_write_are_written_in_their_plain_layout() {
    use arrow_array::{
        BinaryArray, DictionaryArray, Int32Array, Int64Array, ListArray, ListViewArray, RunArray, StringArray, StringViewArray, StructArray,
        types::Int32Type,
    };
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};

    let keys = Int32Array::from(vec![Some(1), None, Some(0), Some(1)]);
    let dictionary = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(StringArray::from(vec!["a", "b"]))).unwrap();
    let views = StringViewArray::from(vec![Some("a string longer than twelve bytes"), Some("b"), None, Some("c")]);
    let view_struct = StructArray::from(vec![(Arc::new(Field::new("v", DataType::Utf8View, true)), Arc::new(views) as ArrayRef)]);
    let runs = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![1, 4]), &Int64Array::from(vec![Some(7), None])).unwrap();
    let item = Arc::new(Field::new("$data$", DataType::Int64, true));
    let list_view = ListViewArray::try_new(Arc::clone(&item), ScalarBuffer::from(vec![2, 0, 0, 1]), ScalarBuffer::from(vec![1, 2, 0, 1]),
        Arc::new(Int64Array::from(vec![10, 11, 12])), Some(vec![true, true, false, true].into())).unwrap();
    let binary_views = arrow_array::BinaryViewArray::from(vec![Some(&b"x"[..]), None, Some(&b"yz"[..]), Some(&b""[..])]);
    let encoded: Vec<ArrayRef> = vec![Arc::new(dictionary), Arc::new(view_struct), Arc::new(runs), Arc::new(list_view), Arc::new(binary_views)];
    let schema = Arc::new(Schema::new(encoded.iter().enumerate()
        .map(|(index, column)| Field::new(format!("c{index}"), column.data_type().clone(), index != 2)).collect::<Vec<_>>()));
    let page = RecordBatch::try_new(Arc::clone(&schema), encoded).unwrap();
    assert!(!supports(&schema));

    let plain_columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![Some("b"), None, Some("a"), Some("b")])),
        Arc::new(StructArray::from(vec![(Arc::new(Field::new("v", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("a string longer than twelve bytes"), Some("b"), None, Some("c")])) as ArrayRef)])),
        Arc::new(Int64Array::from(vec![Some(7), None, None, None])),
        Arc::new(ListArray::try_new(item, OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 3, 3, 4])),
            Arc::new(Int64Array::from(vec![12, 10, 11, 11])), Some(vec![true, true, false, true].into())).unwrap()),
        Arc::new(BinaryArray::from(vec![Some(&b"x"[..]), None, Some(&b"yz"[..]), Some(&b""[..])])),
    ];
    let plain_schema = Arc::new(Schema::new(plain_columns.iter().enumerate()
        .map(|(index, column)| Field::new(format!("c{index}"), column.data_type().clone(), true)).collect::<Vec<_>>()));
    let plain = RecordBatch::try_new(Arc::clone(&plain_schema), plain_columns).unwrap();
    assert!(supports(&plain_schema));

    let written = write_stream_to(&schema, [&page, &page.slice(1, 2)], Vec::new()).unwrap();
    assert_eq!(written, write_stream(&plain_schema, [&plain, &plain.slice(1, 2)], Vec::new()).unwrap());
    let read = ArrowStream::open(written.into(), None).unwrap().collect::<rtdb_types::Result<Vec<_>>>().unwrap();
    assert_eq!(read.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(), [4, 2]);
    assert_eq!(write_stream_to(&schema, &[], Vec::new()).unwrap(), write_stream(&plain_schema, &[], Vec::new()).unwrap());
}

#[test]
fn native_files_write_run_end_encoded_columns_in_java_layout() {
    use arrow_array::{
        Int16Array, Int32Array, Int64Array, RunArray,
        types::{Int16Type, Int32Type, Int64Type},
    };

    fn check_width(ends_type: DataType, runs: ArrayRef, empty_runs: ArrayRef) {
        let values_field = Arc::new(Field::new("values", DataType::Int64, true));
        let ree_field = Field::new("runs", DataType::RunEndEncoded(Arc::new(Field::new("run_ends", ends_type, false)), values_field), false);
        let plain_field = Field::new("tag", DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![ree_field, plain_field]));

        let full = RecordBatch::try_new(Arc::clone(&schema), vec![
            runs,
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60])) as ArrayRef,
        ]).unwrap();
        let empty = RecordBatch::try_new(Arc::clone(&schema), vec![
            empty_runs,
            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
        ]).unwrap();
        let pages = [full.clone(), full.slice(2, 3), full.slice(3, 1), full.slice(4, 0), empty];
        let expected_logical: [&[Option<i64>]; 5] = [
            &[Some(7), Some(7), Some(7), None, Some(9), Some(9)],
            &[Some(7), None, Some(9)],
            &[None],
            &[],
            &[],
        ];
        let expected_run_counts: [i64; 5] = [3, 3, 1, 0, 0];

        let bytes = write_file(&schema, &pages).unwrap();
        assert_eq!(&bytes[..8], b"ARROW1\0\0");
        assert_eq!(&bytes[bytes.len() - 6..], FILE_MAGIC);
        let footer_len = arrow_ipc::reader::read_footer_length(bytes[bytes.len() - 10..].try_into().unwrap()).unwrap();
        let footer_start = bytes.len() - 10 - footer_len;
        assert_eq!(&bytes[footer_start - 4..footer_start], &[0; 4]);
        let footer = arrow_ipc::root_as_footer(&bytes[footer_start..bytes.len() - 10]).unwrap();
        assert_eq!(footer.dictionaries().map_or(0, |blocks| blocks.len()), 0);
        let blocks = footer.recordBatches().unwrap();
        assert_eq!(blocks.len(), pages.len());

        for (index, block) in blocks.iter().enumerate() {
            let start = block.offset() as usize;
            let meta_end = start + block.metaDataLength() as usize;
            let message = arrow_ipc::root_as_message(&bytes[start + 8..meta_end]).unwrap();
            let batch = message.header_as_record_batch().unwrap();
            let nodes = batch.nodes().unwrap();
            assert_eq!(nodes.len(), 4);
            // REE parent has null_count 0 and no buffers; run_ends has null_count 0; nullable values and tag carry -1.
            assert_eq!((nodes.get(0).length(), nodes.get(0).null_count()), (pages[index].num_rows() as i64, 0));
            assert_eq!((nodes.get(1).length(), nodes.get(1).null_count()), (expected_run_counts[index], 0));
            assert_eq!((nodes.get(2).length(), nodes.get(2).null_count()), (expected_run_counts[index], -1));
            assert_eq!((nodes.get(3).length(), nodes.get(3).null_count()), (pages[index].num_rows() as i64, -1));
            // 2 buffers for run_ends + 2 for values + 2 for tag = 6 buffers total (0 for REE parent).
            let buffers = batch.buffers().unwrap();
            assert_eq!(buffers.len(), 6);
            assert_eq!(buffers.get(0).length(), 0); // non-nullable run_ends validity is empty
            let expected_tag_validity = if pages[index].num_rows() > 0 { pages[index].num_rows().div_ceil(8) as i64 } else { 0 };
            assert_eq!(buffers.get(4).length(), expected_tag_validity); // all-valid bitmap materialized for nullable tag
        }

        let mut file = ArrowFile::open(bytes.into(), None).unwrap();
        assert_eq!(file.page_count(), pages.len());
        assert_eq!(file.schema().as_ref(), schema.as_ref());
        for (index, expected) in expected_logical.iter().enumerate() {
            let decoded = file.read_page(index).unwrap();
            assert_eq!(decoded.num_rows(), expected.len());
            let actual: Vec<Option<i64>> = (0..decoded.num_rows()).map(|row| {
                match rtdb_columnar::read_value(schema.field(0), decoded.column(0).as_ref(), row).unwrap() {
                    rtdb_types::Value::Null => None,
                    rtdb_types::Value::I64(value) => Some(value),
                    other => panic!("unexpected value {other:?}"),
                }
            }).collect();
            assert_eq!(actual.as_slice(), *expected, "page {index}");
        }
    }

    let values = Arc::new(Int64Array::from(vec![Some(7), None, Some(9)])) as ArrayRef;
    let empty_values = Arc::new(Int64Array::from(Vec::<Option<i64>>::new())) as ArrayRef;
    check_width(
        DataType::Int16,
        Arc::new(RunArray::<Int16Type>::try_new(&Int16Array::from(vec![3, 4, 6]), &values).unwrap()),
        Arc::new(RunArray::<Int16Type>::try_new(&Int16Array::from(Vec::<i16>::new()), &empty_values).unwrap()),
    );
    check_width(
        DataType::Int32,
        Arc::new(RunArray::<Int32Type>::try_new(&Int32Array::from(vec![3, 4, 6]), &values).unwrap()),
        Arc::new(RunArray::<Int32Type>::try_new(&Int32Array::from(Vec::<i32>::new()), &empty_values).unwrap()),
    );
    check_width(
        DataType::Int64,
        Arc::new(RunArray::<Int64Type>::try_new(&Int64Array::from(vec![3, 4, 6]), &values).unwrap()),
        Arc::new(RunArray::<Int64Type>::try_new(&Int64Array::from(Vec::<i64>::new()), &empty_values).unwrap()),
    );
}

#[test]
fn native_files_reject_layouts_xtdb_does_not_write() {
    let unsupported = [
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        DataType::Utf8View,
        DataType::BinaryView,
        DataType::ListView(Arc::new(Field::new("$data$", DataType::Int64, true))),
        DataType::LargeListView(Arc::new(Field::new("$data$", DataType::Int64, true))),
        DataType::RunEndEncoded(Arc::new(Field::new("run_ends", DataType::Int8, false)), Arc::new(Field::new("values", DataType::Int64, true))),
    ];
    for data_type in unsupported {
        let schema = Schema::new(vec![Field::new("c", data_type, true)]);
        let mut sink = Vec::new();
        assert!(FileWriter::new(&mut sink, &schema).is_err());
        assert!(sink.is_empty(), "rejected schema must not write any bytes");
        assert!(write_file(&schema, &[]).is_err());
    }
}
