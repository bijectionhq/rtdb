use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, make_array};
use arrow_buffer::{Buffer, NullBuffer};
use arrow_data::{ArrayData, BufferSpec};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode};
use rtdb_types::{LogicalType, ScalarType};

use super::{supports, write_stream};
use crate::ipc::{ArrowStream, FirstSchemaWriter, TailWriter, invalid, java_layout::JavaLayout, write_options, write_stream_to};

/// The oracle the native writer must match byte for byte: arrow-rs's stream, decimal-patched and rewritten into the
/// reference's Java layout as it passes.
fn write_rewritten_stream_to(schema: &Schema, pages: &[RecordBatch], sink: Vec<u8>) -> rtdb_types::Result<Vec<u8>> {
    use std::io::Write;
    let decimal_schema = Arc::new(crate::ipc_decimal::DecimalSchema::new(schema)?);
    let sink = FirstSchemaWriter { inner: JavaLayout::new_stream(sink), schema: Arc::clone(&decimal_schema), pending: Vec::new(), expected: None, complete: false };
    let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(TailWriter { inner: sink, pending: Vec::with_capacity(8) }, &decimal_schema.wire, write_options()?).map_err(invalid)?;
    for page in pages { decimal_schema.validate_batch(page)?; writer.write(page).map_err(invalid)?; }
    writer.finish().map_err(invalid)?;
    let mut sink = writer.into_inner().map_err(invalid)?;
    if sink.pending.as_slice() != [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0] { return Err(invalid("Arrow writer produced an unexpected end marker")); }
    sink.inner.write_all(&[0; 4]).map_err(invalid)?;
    sink.inner.flush().map_err(invalid)?;
    if !sink.inner.complete { return Err(invalid("Arrow stream ended before its schema")); }
    Ok(sink.inner.inner.inner)
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
        assert_same_stream(&schema, &pages, &format!("seed {seed}"));
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
        assert_same_stream(&schema, &pages, &format!("{rows} rows"));
    }
    assert_same_stream(&schema, &[], "no pages");
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
