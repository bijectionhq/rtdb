use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, make_array};
use arrow_buffer::{Buffer, NullBuffer};
use arrow_data::{ArrayData, BufferSpec};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode};
use rtdb_types::{LogicalType, ScalarType};

use super::{supports, write_stream};
use crate::ipc::{write_rewritten_stream_to, write_stream_to};

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

#[test]
fn layouts_the_native_writer_leaves_out_keep_the_rewritten_stream() {
    let dictionary = Schema::new(vec![Field::new("d", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), true)]);
    let view = Schema::new(vec![Field::new("s", DataType::Struct(vec![Arc::new(Field::new("v", DataType::Utf8View, true))].into()), true)]);
    for schema in [dictionary, view] {
        assert!(!supports(&schema));
        assert_eq!(write_stream_to(&schema, &[], Vec::new()).unwrap(), write_rewritten_stream_to(&schema, &[], Vec::new()).unwrap());
    }
}
