//! Borrowed sort keys for ordered probes and other simple Arrow sorts.
use std::{cmp::Ordering, mem::size_of};

use arrow_array::{Array, cast::AsArray, types::*};
use arrow_schema::{DataType, Field, TimeUnit as ArrowTime, UnionMode};
use rtdb_columnar::ArrowCell;
use rtdb_plan::{Schema, SortExpr};
use rtdb_types::{LogicalType, ScalarType, TimeUnit, Value};

use crate::columnar::{Column, ColumnBatch};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Kind {
    Null,
    Number,
    Text,
    Keyword,
    Binary,
    Uuid,
    Day,
    Bool,
    Timestamp(TimeUnit, bool),
    Time(TimeUnit),
    Duration(TimeUnit),
    YearMonth,
    MonthDay(bool),
}

impl Kind {
    pub(crate) fn of(ty: &LogicalType) -> Option<Self> {
        let mut kind = None;
        let mut nullable = false;
        for leg in ty.legs() {
            let next = match leg {
                LogicalType::Null => {
                    nullable = true;
                    continue;
                }
                LogicalType::Scalar(
                    ScalarType::I8
                    | ScalarType::I16
                    | ScalarType::I32
                    | ScalarType::I64
                    | ScalarType::U8
                    | ScalarType::U16
                    | ScalarType::U32
                    | ScalarType::U64
                    | ScalarType::F32
                    | ScalarType::F64,
                ) => Self::Number,
                LogicalType::Scalar(ScalarType::Utf8) => Self::Text,
                LogicalType::Scalar(ScalarType::Keyword) => Self::Keyword,
                LogicalType::Scalar(ScalarType::Binary | ScalarType::FixedSizeBinary(_)) => {
                    Self::Binary
                }
                LogicalType::Scalar(ScalarType::Uuid) => Self::Uuid,
                LogicalType::Scalar(ScalarType::DateDay | ScalarType::DateMilli) => Self::Day,
                LogicalType::Scalar(ScalarType::Bool) => Self::Bool,
                LogicalType::Scalar(ScalarType::Timestamp { unit, timezone }) => {
                    Self::Timestamp(unit, timezone.is_some())
                }
                LogicalType::Scalar(ScalarType::Time { unit, .. }) => Self::Time(unit),
                LogicalType::Scalar(ScalarType::Duration(unit)) => Self::Duration(unit),
                LogicalType::Scalar(ScalarType::Interval(rtdb_types::IntervalUnit::YearMonth)) => {
                    Self::YearMonth
                }
                LogicalType::Scalar(ScalarType::Interval(rtdb_types::IntervalUnit::MonthDayNano)) => Self::MonthDay(false),
                LogicalType::Scalar(ScalarType::IntervalMonthDayMicro) => Self::MonthDay(true),
                _ => return None,
            };
            if kind.is_some_and(|known| known != next) {
                return None;
            }
            kind = Some(next);
        }
        kind.or(nullable.then_some(Self::Null))
    }

    fn orders_totally(self, ty: &LogicalType) -> bool {
        match self {
            Self::MonthDay(_) => false,
            Self::Number => {
                let legs = ty.legs();
                let floats = legs.iter().filter(|leg| matches!(leg, LogicalType::Scalar(ScalarType::F32 | ScalarType::F64))).count();
                let numbers = legs.iter().filter(|leg| !matches!(leg, LogicalType::Null)).count();
                floats == 0 || floats == numbers
            }
            _ => true,
        }
    }

    fn accepts(self, column: &Column) -> bool {
        match column {
            Column::Values(values) => values.iter().all(|value| self.value(value).is_some()),
            Column::Arrow {
                array,
                field,
                logical,
            } => self.accepts_arrow(array.as_ref(), field, logical.as_ref()),
            Column::Selected(selected) => selected
                .source_layouts()
                .all(|(array, field, logical)| self.accepts_arrow(array, field, logical)),
        }
    }

    fn accepts_arrow(
        self,
        array: &dyn Array,
        field: &Field,
        logical: Option<&LogicalType>,
    ) -> bool {
        if array.data_type() != field.data_type() {
            return false;
        }
        if array.data_type() == &DataType::Null {
            return self.accepts_field(field) && logical.is_none_or(|ty| *ty == LogicalType::Null);
        }
        let logical_kind = logical.map_or_else(
            || {
                rtdb_columnar::from_field(field)
                    .ok()
                    .and_then(|ty| Self::of(&ty))
            },
            Self::of,
        );
        if logical_kind != Some(self) {
            return false;
        }
        self.accepts_field(field)
    }

    fn accepts_field(self, field: &Field) -> bool {
        let extension = field
            .metadata()
            .get(rtdb_columnar::EXTENSION_NAME_KEY)
            .map(String::as_str);
        match (self, field.data_type(), extension) {
            (_, DataType::Null, None) => {
                field.is_nullable() && rtdb_columnar::from_field(field).is_ok_and(|ty| ty == LogicalType::Null)
            }
            (_, DataType::Union(fields, UnionMode::Dense), None) => {
                fields.iter().all(|(_, field)| self.accepts_field(field))
            }
            (_, DataType::RunEndEncoded(_, values), None) => self.accepts_field(values),
            (
                Self::Number,
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64,
                None,
            ) => true,
            (Self::Text, DataType::Utf8, None)
            | (Self::Keyword, DataType::Utf8, Some("xt/clj-keyword")) => true,
            (Self::Binary, DataType::Binary | DataType::FixedSizeBinary(_), None) => true,
            (Self::Uuid, DataType::FixedSizeBinary(16), Some("uuid")) => true,
            (Self::Day, DataType::Date32 | DataType::Date64, None) => true,
            (Self::Bool, DataType::Boolean, None) => true,
            (Self::Timestamp(unit, zoned), DataType::Timestamp(physical, zone), None) => {
                unit == time_unit(physical) && zoned == zone.is_some()
            }
            (Self::Time(unit), DataType::Time32(physical) | DataType::Time64(physical), None)
            | (Self::Duration(unit), DataType::Duration(physical), None) => unit == time_unit(physical),
            (Self::YearMonth, DataType::Interval(arrow_schema::IntervalUnit::YearMonth), None) => true,
            (Self::MonthDay(false), DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano), None)
            | (Self::MonthDay(true), DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano), Some("xt/intervalDMMType")) => true,
            _ => false,
        }
    }

    fn value<'a>(self, value: &'a Value) -> Option<Key<'a>> {
        Some(match (self, value) {
            (_, Value::Null | Value::Undefined) => Key::Null,
            (Self::Number, Value::I8(value)) => Key::Integer(i64::from(*value)),
            (Self::Number, Value::I16(value)) => Key::Integer(i64::from(*value)),
            (Self::Number, Value::I32(value)) => Key::Integer(i64::from(*value)),
            (Self::Number, Value::I64(value)) => Key::Integer(*value),
            (Self::Number, Value::U8(value)) => Key::Integer(i64::from(*value as i8)),
            (Self::Number, Value::U16(value)) => Key::Integer(i64::from(*value as i16)),
            (Self::Number, Value::U32(value)) => Key::Integer(i64::from(*value as i32)),
            (Self::Number, Value::U64(value)) => Key::Integer(*value as i64),
            (Self::Number, Value::F32(value)) => Key::Float(f64::from(*value)),
            (Self::Number, Value::F64(value)) => Key::Float(*value),
            (Self::Text, Value::Utf8(value)) => Key::Text(value.as_bytes()),
            (Self::Keyword, Value::Keyword(value)) => Key::Text(value.as_str().as_bytes()),
            (Self::Binary, Value::Binary(value) | Value::FixedSizeBinary(value)) => {
                Key::Text(value)
            }
            (Self::Uuid, Value::Uuid(value)) => Key::Text(value),
            (Self::Day, value) => Key::Integer(value.date_days()?),
            (Self::Bool, Value::Bool(value)) => Key::Integer(i64::from(u8::from(*value))),
            (Self::Timestamp(unit, zoned), Value::Timestamp { value, unit: input, timezone })
                if unit == *input && zoned == timezone.is_some() => Key::Integer(*value),
            (Self::Time(unit), Value::Time { value, unit: input, .. })
            | (Self::Duration(unit), Value::Duration { value, unit: input })
                if unit == *input => Key::Integer(*value),
            (Self::YearMonth, Value::IntervalYearMonth(months)) => Key::Integer(i64::from(*months)),
            (Self::MonthDay(false), Value::IntervalMonthDayNano { months, days, nanoseconds })
            | (Self::MonthDay(true), Value::IntervalMonthDayMicro { months, days, nanoseconds }) => {
                Key::month_day(*months, *days, *nanoseconds)
            }
            _ => return None,
        })
    }

    fn read(self, column: &Column, row: usize) -> Key<'_> {
        match column {
            Column::Values(values) => self.value(&values[row]).expect("admitted scalar key"),
            Column::Arrow { array, field, .. } => Self::read_arrow(ArrowCell {
                field,
                array: array.as_ref(),
                row,
            }),
            Column::Selected(_) => column
                .arrow_cell(row)
                .flatten()
                .map_or(Key::Null, |(array, field, _, row)| {
                    Self::read_arrow(ArrowCell { array, field, row })
                }),
        }
    }

    pub(crate) fn compare(
        self, left: (&Column, usize), right: (&Column, usize), descending: bool, nulls_first: bool,
    ) -> rtdb_types::Result<Ordering> {
        self.read(left.0, left.1).compare(self.read(right.0, right.1), descending, nulls_first)
    }

    fn read_arrow(cell: ArrowCell<'_>) -> Key<'_> {
        let ArrowCell { field, array, row } = cell.physical().expect("admitted sort cell");
        if field.data_type() == &DataType::Null || array.is_null(row) {
            return Key::Null;
        }
        macro_rules! value {
            ($ty:ty) => {
                array.as_primitive::<$ty>().value(row)
            };
        }
        match field.data_type() {
            DataType::Int8 => Key::Integer(i64::from(value!(Int8Type))),
            DataType::Int16 => Key::Integer(i64::from(value!(Int16Type))),
            DataType::Int32 => Key::Integer(i64::from(value!(Int32Type))),
            DataType::Int64 => Key::Integer(value!(Int64Type)),
            DataType::UInt8 => Key::Integer(i64::from(value!(UInt8Type) as i8)),
            DataType::UInt16 => Key::Integer(i64::from(value!(UInt16Type) as i16)),
            DataType::UInt32 => Key::Integer(i64::from(value!(UInt32Type) as i32)),
            DataType::UInt64 => Key::Integer(value!(UInt64Type) as i64),
            DataType::Float32 => Key::Float(f64::from(value!(Float32Type))),
            DataType::Float64 => Key::Float(value!(Float64Type)),
            DataType::Utf8 => Key::Text(array.as_string::<i32>().value(row).as_bytes()),
            DataType::Binary => Key::Text(array.as_binary::<i32>().value(row)),
            DataType::FixedSizeBinary(_) => Key::Text(array.as_fixed_size_binary().value(row)),
            DataType::Boolean => Key::Integer(i64::from(u8::from(array.as_boolean().value(row)))),
            DataType::Date32 => Key::Integer(i64::from(value!(Date32Type))),
            DataType::Date64 => Key::Integer(
                value!(Date64Type).div_euclid(rtdb_types::java_date::MILLIS_PER_DAY),
            ),
            DataType::Timestamp(unit, _) => Key::Integer(match unit {
                ArrowTime::Second => value!(TimestampSecondType),
                ArrowTime::Millisecond => value!(TimestampMillisecondType),
                ArrowTime::Microsecond => value!(TimestampMicrosecondType),
                ArrowTime::Nanosecond => value!(TimestampNanosecondType),
            }),
            DataType::Duration(unit) => Key::Integer(match unit {
                ArrowTime::Second => value!(DurationSecondType),
                ArrowTime::Millisecond => value!(DurationMillisecondType),
                ArrowTime::Microsecond => value!(DurationMicrosecondType),
                ArrowTime::Nanosecond => value!(DurationNanosecondType),
            }),
            DataType::Time32(ArrowTime::Second) => Key::Integer(i64::from(value!(Time32SecondType))),
            DataType::Time32(ArrowTime::Millisecond) => Key::Integer(i64::from(value!(Time32MillisecondType))),
            DataType::Time64(ArrowTime::Microsecond) => Key::Integer(value!(Time64MicrosecondType)),
            DataType::Time64(ArrowTime::Nanosecond) => Key::Integer(value!(Time64NanosecondType)),
            DataType::Interval(arrow_schema::IntervalUnit::YearMonth) => {
                Key::Integer(i64::from(value!(IntervalYearMonthType)))
            }
            DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano) => {
                let value = value!(IntervalMonthDayNanoType);
                Key::month_day(value.months, value.days, value.nanoseconds)
            }
            _ => unreachable!("only admitted sort keys are read"),
        }
    }
}

fn time_unit(unit: &ArrowTime) -> TimeUnit {
    match unit {
        ArrowTime::Second => TimeUnit::Second,
        ArrowTime::Millisecond => TimeUnit::Millisecond,
        ArrowTime::Microsecond => TimeUnit::Microsecond,
        ArrowTime::Nanosecond => TimeUnit::Nanosecond,
    }
}

#[derive(Clone, Copy)]
pub(super) enum Key<'a> {
    Null,
    Integer(i64),
    Float(f64),
    Text(&'a [u8]),
    MonthDay { has_positive_months: bool, nanos: i64 },
}

#[derive(Clone, Copy)]
pub(super) enum BytePrefix {
    Null,
    Bytes { bytes: [u8; PREFIX_BYTES], len: u8 },
}

const PREFIX_BYTES: usize = 3 * size_of::<usize>() - 2;
const TRUNCATED_LENGTH: u8 = (PREFIX_BYTES + 1) as u8;

pub(super) struct ByteKey<'a> {
    pub prefix: BytePrefix,
    pub tail: &'a [u8],
}

impl<'a> ByteKey<'a> {
    fn from_bytes(bytes: &'a [u8]) -> Self {
        let (first, tail) = bytes.split_at(bytes.len().min(PREFIX_BYTES));
        let mut prefix = [0; PREFIX_BYTES];
        prefix[..first.len()].copy_from_slice(first);
        Self {
            prefix: BytePrefix::Bytes {
                bytes: prefix,
                len: bytes.len().min(usize::from(TRUNCATED_LENGTH)) as u8,
            },
            tail,
        }
    }
}

impl BytePrefix {
    fn compare(self, right: Self, order: &SortExpr) -> Option<Ordering> {
        let comparison = match (self, right) {
            (Self::Null, Self::Null) => return Some(Ordering::Equal),
            (Self::Null, _) => return Some(if order.nulls_first { Ordering::Less } else { Ordering::Greater }),
            (_, Self::Null) => return Some(if order.nulls_first { Ordering::Greater } else { Ordering::Less }),
            (Self::Bytes { bytes: left, len }, Self::Bytes { bytes: right, len: other_len }) => {
                match left.cmp(&right) {
                    Ordering::Equal if len == TRUNCATED_LENGTH && other_len == TRUNCATED_LENGTH => {
                        return None;
                    }
                    Ordering::Equal => len.cmp(&other_len),
                    comparison => comparison,
                }
            }
        };
        Some(if order.descending { comparison.reverse() } else { comparison })
    }
}

impl Key<'_> {
    fn month_day(months: i32, days: i32, nanos: i64) -> Self {
        Self::MonthDay {
            has_positive_months: months > 0,
            nanos: i64::from(days).wrapping_mul(86_400_000_000_000).wrapping_add(nanos),
        }
    }

    fn compare(self, right: Self, descending: bool, nulls_first: bool) -> rtdb_types::Result<Ordering> {
        let comparison = match (self, right) {
            (Self::Null, Self::Null) => return Ok(Ordering::Equal),
            (Self::Null, _) => {
                return Ok(if nulls_first { Ordering::Less } else { Ordering::Greater });
            }
            (_, Self::Null) => {
                return Ok(if nulls_first { Ordering::Greater } else { Ordering::Less });
            }
            (Self::Integer(left), Self::Integer(right)) => left.cmp(&right),
            (Self::Float(left), Self::Float(right)) => float_order(left, right),
            (Self::Integer(left), Self::Float(right)) => float_order(left as f64, right),
            (Self::Float(left), Self::Integer(right)) => float_order(left, right as f64),
            (Self::Text(left), Self::Text(right)) => left.cmp(right),
            (Self::MonthDay { has_positive_months: a, nanos: left },
             Self::MonthDay { has_positive_months: b, nanos: right }) => {
                if a || b {
                    return Err(rtdb_types::Anomaly::incorrect(
                        "xtdb.expression/cannot-compare-mdn-interval-with-months",
                        "Cannot compare month-day-micro/nano intervals when month component is non-zero.",
                    ));
                }
                left.cmp(&right)
            }
            _ => unreachable!("sort key columns have one admitted kind"),
        };
        Ok(if descending { comparison.reverse() } else { comparison })
    }
}

fn float_order(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.total_cmp(&right),
    }
}

pub(super) struct NativeOrder {
    keys: Vec<(usize, Kind)>,
    total: bool,
}

impl NativeOrder {
    pub fn bind(schema: &Schema, columns: &[usize]) -> Option<Self> {
        let mut total = true;
        let keys = columns
            .iter()
            .map(|&column| {
                let ty = &schema.fields().get(column)?.data_type;
                let kind = Kind::of(ty)?;
                total &= kind.orders_totally(ty);
                Some((column, kind))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self { keys, total })
    }

    /// Whether every comparison succeeds and the order is transitive, so any comparison schedule yields the same order.
    /// Month-day intervals with months raise on comparison, and integers compared with floats lose precision past 2^53.
    pub fn is_total(&self) -> bool { self.total }

    pub fn accepts(&self, batch: &ColumnBatch) -> bool {
        self.keys
            .iter()
            .all(|&(column, kind)| kind.accepts(&batch.columns[column]))
    }

    pub fn has_leading_bytes(&self) -> bool {
        matches!(self.keys.first(), Some((_, Kind::Text | Kind::Keyword | Kind::Binary)))
    }

    pub fn leading_key<'a>(&self, batch: &'a ColumnBatch, row: usize) -> Key<'a> {
        let (column, kind) = self.keys[0];
        kind.read(&batch.columns[column], row)
    }

    pub fn leading_bytes<'a>(&self, batch: &'a ColumnBatch, row: usize) -> ByteKey<'a> {
        match self.leading_key(batch, row) {
            Key::Null => ByteKey { prefix: BytePrefix::Null, tail: &[] },
            Key::Text(bytes) => ByteKey::from_bytes(bytes),
            _ => unreachable!("expected a byte sort key"),
        }
    }

    pub fn compare_prefixed<'a>(
        &self,
        left: (&'a ColumnBatch, usize, BytePrefix),
        right: (&'a ColumnBatch, usize, BytePrefix),
        order: &[SortExpr],
        tails: Option<(&'a [u8], &'a [u8])>,
    ) -> rtdb_types::Result<Ordering> {
        let (left, left_row, left_prefix) = left;
        let (right, right_row, right_prefix) = right;
        let comparison = left_prefix.compare(right_prefix, &order[0]).unwrap_or_else(|| {
            let (a, b) = tails.unwrap_or_else(|| {
                let Key::Text(a) = self.leading_key(left, left_row) else { unreachable!() };
                let Key::Text(b) = self.leading_key(right, right_row) else { unreachable!() };
                (&a[PREFIX_BYTES..], &b[PREFIX_BYTES..])
            });
            let comparison = a.cmp(b);
            if order[0].descending { comparison.reverse() } else { comparison }
        });
        if comparison.is_eq() {
            self.compare_from(1, left, left_row, right, right_row, order)
        } else {
            Ok(comparison)
        }
    }

    pub fn compare(
        &self,
        left: &ColumnBatch,
        left_row: usize,
        right: &ColumnBatch,
        right_row: usize,
        order: &[SortExpr],
    ) -> rtdb_types::Result<Ordering> {
        self.compare_from(0, left, left_row, right, right_row, order)
    }

    pub fn compare_leading(
        &self,
        (left, left_row, left_key): (&ColumnBatch, usize, Key<'_>),
        (right, right_row, right_key): (&ColumnBatch, usize, Key<'_>),
        order: &[SortExpr],
    ) -> rtdb_types::Result<Ordering> {
        let first = &order[0];
        let comparison = left_key.compare(right_key, first.descending, first.nulls_first)?;
        if comparison.is_eq() {
            self.compare_from(1, left, left_row, right, right_row, order)
        } else {
            Ok(comparison)
        }
    }

    fn compare_from(
        &self,
        start: usize,
        left: &ColumnBatch,
        left_row: usize,
        right: &ColumnBatch,
        right_row: usize,
        order: &[SortExpr],
    ) -> rtdb_types::Result<Ordering> {
        for (&(column, kind), order) in self.keys[start..].iter().zip(&order[start..]) {
            let comparison = kind.compare(
                (&left.columns[column], left_row), (&right.columns[column], right_row),
                order.descending, order.nulls_first,
            )?;
            if !comparison.is_eq() {
                return Ok(comparison);
            }
        }
        Ok(Ordering::Equal)
    }
}

#[test]
#[cfg(target_pointer_width = "64")]
fn byte_prefix_resolves_concatenated_name_and_label_keys_inline() {
    use rtdb_plan::{ColumnId, Expr};

    let order = SortExpr {
        expr: Expr::Column {
            id: ColumnId(0),
            data_type: LogicalType::Scalar(ScalarType::Utf8),
        },
        descending: false,
        nulls_first: false,
    };
    let left = ByteKey::from_bytes("name-0000000-éßL8".as_bytes());
    let right = ByteKey::from_bytes("name-0000000-éßL9".as_bytes());
    assert!(left.tail.is_empty() && right.tail.is_empty());
    assert_eq!(
        left.prefix.compare(right.prefix, &order),
        Some(Ordering::Less)
    );
}

#[test]
fn byte_prefix_orders_zero_padding_and_leaves_equal_truncated_prefixes_to_the_tail() {
    use rtdb_plan::{ColumnId, Expr};

    assert!(size_of::<BytePrefix>() <= 3 * size_of::<usize>());
    let exact = [0; PREFIX_BYTES];
    let longer = [0; PREFIX_BYTES + 1];
    let longest = [0; PREFIX_BYTES + 2];
    let shorter = &exact[..PREFIX_BYTES - 1];
    for (descending, nulls_first) in [(false, false), (true, false), (false, true), (true, true)] {
        let order = SortExpr {
            expr: Expr::Column {
                id: ColumnId(0),
                data_type: LogicalType::Scalar(ScalarType::Binary),
            },
            descending,
            nulls_first,
        };
        let directed = |ordering: Ordering| {
            if descending { ordering.reverse() } else { ordering }
        };
        for (left, right, resolved_inline) in [
            (shorter, exact.as_slice(), true),
            (exact.as_slice(), longer.as_slice(), true),
            (longer.as_slice(), longest.as_slice(), false),
            (exact.as_slice(), exact.as_slice(), true),
        ] {
            let (a, b) = (ByteKey::from_bytes(left), ByteKey::from_bytes(right));
            let expected = directed(left.cmp(right));
            let prefix = a.prefix.compare(b.prefix, &order);
            assert_eq!(prefix, resolved_inline.then_some(expected));
            assert_eq!(
                prefix.unwrap_or_else(|| directed(a.tail.cmp(b.tail))),
                expected
            );
        }
        let empty = ByteKey::from_bytes(&[]);
        let null_order = if nulls_first { Ordering::Less } else { Ordering::Greater };
        assert_eq!(
            BytePrefix::Null.compare(empty.prefix, &order),
            Some(null_order)
        );
    }
}

#[test]
fn borrowed_sort_keys_follow_selected_arrow_positions_and_missing_sources() {
    use crate::columnar::SelectedColumn;
    use arrow_array::StringArray;
    use std::sync::Arc;

    let ty = LogicalType::Scalar(ScalarType::Utf8).maybe();
    let field = Arc::new(rtdb_columnar::to_field("name", &ty).unwrap());
    let array = Arc::new(StringArray::from(vec![
        Some("earlier"),
        None,
        Some("later"),
    ]));
    let column = Column::Selected(SelectedColumn::new(
        vec![Some((array, field, Some(ty), None)), None],
        Arc::new(vec![(0, 2), (1, 0), (0, 0), (0, 1)]),
    ));

    assert!(Kind::Text.accepts(&column));
    assert!(matches!(Kind::Text.read(&column, 0), Key::Text(bytes) if bytes == b"later"));
    assert!(matches!(Kind::Text.read(&column, 1), Key::Null));
    assert!(matches!(Kind::Text.read(&column, 2), Key::Text(bytes) if bytes == b"earlier"));
    assert!(matches!(Kind::Text.read(&column, 3), Key::Null));
}

#[test]
fn native_null_sort_key_accepts_sql_nulls_and_rejects_bottom_storage() {
    use arrow_array::NullArray;
    use std::sync::Arc;

    let kind = Kind::of(&LogicalType::Null).unwrap();
    let column = |nullable, logical| Column::Arrow {
        array: Arc::new(NullArray::new(2)),
        field: Arc::new(Field::new("key", DataType::Null, nullable)),
        logical: Some(logical),
    };
    let nulls = column(true, LogicalType::Null);
    assert!(kind.accepts(&nulls));
    assert!(matches!(kind.read(&nulls, 1), Key::Null));
    assert!(!kind.accepts(&column(false, LogicalType::Nothing)));
    assert!(!kind.accepts(&Column::Values(vec![Value::I64(1)])));
}

#[test]
fn arrow_wave4_native_sort_matches_scalar_order_for_index_key_families() {
    use rtdb_plan::{ColumnId, Expr};

    let mut cases = vec![
        vec![
            Value::F64(-0.0),
            Value::F64(0.0),
            Value::F64(f64::NAN),
            Value::F32(f32::NAN),
            Value::I64(9),
            Value::I64(9_007_199_254_740_992),
            Value::I64(9_007_199_254_740_993),
            Value::F64(9_007_199_254_740_992.0),
        ],
        vec![Value::U64(u64::MAX), Value::U64(1), Value::I64(i64::MIN)],
        vec![
            Value::DateDay(-1),
            Value::DateMilli(-1),
            Value::DateMilli(86_400_000),
        ],
        vec![Value::Bool(false), Value::Bool(true)],
        vec![Value::Uuid([0xff; 16]), Value::Uuid([0; 16])],
        vec![
            Value::Binary(vec![128]),
            Value::FixedSizeBinary(vec![127]),
            Value::Binary(vec![128, 0]),
        ],
    ];
    let units = [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond];
    for unit in units {
        for timezone in [None, Some("America/New_York".to_owned())] {
            cases.push([-7, 0, 7].map(|value| Value::Timestamp {
                value, unit, timezone: timezone.clone()
            }).to_vec());
        }
        cases.push([-7, 0, 7].map(|value| Value::Duration { value, unit }).to_vec());
    }
    cases.push(vec![Value::IntervalYearMonth(-1), Value::IntervalYearMonth(0), Value::IntervalYearMonth(12)]);
    cases.push(vec![
        Value::IntervalMonthDayNano { months: -1, days: 1, nanoseconds: i64::MAX },
        Value::IntervalMonthDayNano { months: 0, days: -1, nanoseconds: 0 },
    ]);
    for mut values in cases {
        values.push(Value::Null);
        let ty = LogicalType::join_all(&values.iter().map(Value::logical_type).collect::<Vec<_>>());
        let field = std::sync::Arc::new(rtdb_columnar::to_field("key", &ty).unwrap());
        let array = rtdb_columnar::copy_array(&ty, &values).unwrap();
        let column = Column::Arrow {
            array,
            field,
            logical: Some(ty.clone()),
        };
        let kind = Kind::of(&ty).unwrap();
        assert!(kind.accepts(&column));
        for (left, left_value) in values.iter().enumerate() {
            for (right, right_value) in values.iter().enumerate() {
                for descending in [false, true] {
                    let order = SortExpr {
                        expr: Expr::Column {
                            id: ColumnId(0),
                            data_type: ty.clone(),
                        },
                        descending,
                        nulls_first: false,
                    };
                    let expected = rtdb_expr::ordered_compare_in_timezone(
                        left_value,
                        right_value,
                        false,
                        "UTC",
                    )
                    .unwrap();
                    let expected = if descending
                        && !matches!(left_value, Value::Null)
                        && !matches!(right_value, Value::Null)
                    {
                        expected.reverse()
                    } else {
                        expected
                    };
                    assert_eq!(
                        kind.read(&column, left)
                            .compare(kind.read(&column, right), order.descending, order.nulls_first).unwrap(),
                        expected
                    );
                }
            }
        }
    }
}
