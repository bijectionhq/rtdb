//! Borrowed sort keys for ordered probes and other simple Arrow sorts.
use std::cmp::Ordering;

use arrow_array::{Array, cast::AsArray, types::*};
use arrow_schema::{DataType, Field, TimeUnit as ArrowTime, UnionMode};
use rtdb_columnar::ArrowCell;
use rtdb_plan::{Schema, SortExpr};
use rtdb_types::{LogicalType, ScalarType, TimeUnit, Value};

use crate::columnar::{Column, ColumnBatch};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Kind {
    Number,
    Text,
    Keyword,
    Binary,
    Uuid,
    Day,
    Bool,
    InstantMicros,
}

impl Kind {
    fn of(ty: &LogicalType) -> Option<Self> {
        let mut kind = None;
        for leg in ty.legs() {
            let next = match leg {
                LogicalType::Null => continue,
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
                LogicalType::Scalar(ScalarType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: Some(_),
                }) => Self::InstantMicros,
                _ => return None,
            };
            if kind.is_some_and(|known| known != next) {
                return None;
            }
            kind = Some(next);
        }
        kind
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
        if array.data_type() == &DataType::Null {
            return field.is_nullable() && logical != Some(&LogicalType::Nothing);
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
            (_, DataType::Null, _) => field.is_nullable(),
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
            (Self::InstantMicros, DataType::Timestamp(ArrowTime::Microsecond, Some(_)), None) => {
                true
            }
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
            (
                Self::InstantMicros,
                Value::Timestamp {
                    value,
                    unit: TimeUnit::Microsecond,
                    timezone: Some(_),
                },
            ) => Key::Integer(*value),
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
            DataType::Timestamp(ArrowTime::Microsecond, Some(_)) => {
                Key::Integer(value!(TimestampMicrosecondType))
            }
            _ => unreachable!("only admitted sort keys are read"),
        }
    }
}

enum Key<'a> {
    Null,
    Integer(i64),
    Float(f64),
    Text(&'a [u8]),
}

impl Key<'_> {
    fn compare(self, right: Self, order: &SortExpr) -> Ordering {
        let comparison = match (self, right) {
            (Self::Null, Self::Null) => return Ordering::Equal,
            (Self::Null, _) => {
                return if order.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            (_, Self::Null) => {
                return if order.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            (Self::Integer(left), Self::Integer(right)) => left.cmp(&right),
            (Self::Float(left), Self::Float(right)) => float_order(left, right),
            (Self::Integer(left), Self::Float(right)) => float_order(left as f64, right),
            (Self::Float(left), Self::Integer(right)) => float_order(left, right as f64),
            (Self::Text(left), Self::Text(right)) => left.cmp(right),
            _ => unreachable!("sort key columns have one admitted kind"),
        };
        if order.descending {
            comparison.reverse()
        } else {
            comparison
        }
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

pub(super) struct NativeOrder(Vec<(usize, Kind)>);

impl NativeOrder {
    pub fn bind(schema: &Schema, columns: &[usize]) -> Option<Self> {
        columns
            .iter()
            .map(|&column| Some((column, Kind::of(&schema.fields().get(column)?.data_type)?)))
            .collect::<Option<Vec<_>>>()
            .map(Self)
    }

    pub fn accepts(&self, batch: &ColumnBatch) -> bool {
        self.0
            .iter()
            .all(|&(column, kind)| kind.accepts(&batch.columns[column]))
    }

    pub fn compare(
        &self,
        left: &ColumnBatch,
        left_row: usize,
        right: &ColumnBatch,
        right_row: usize,
        order: &[SortExpr],
    ) -> Ordering {
        for (&(column, kind), order) in self.0.iter().zip(order) {
            let left = kind.read(&left.columns[column], left_row);
            let right = kind.read(&right.columns[column], right_row);
            let comparison = left.compare(right, order);
            if !comparison.is_eq() {
                return comparison;
            }
        }
        Ordering::Equal
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
fn native_sort_matches_scalar_order_for_index_key_families() {
    use rtdb_plan::{ColumnId, Expr};

    let cases = [
        vec![
            Value::F64(-0.0),
            Value::F64(0.0),
            Value::F64(f64::NAN),
            Value::F32(f32::NAN),
            Value::I64(9),
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
                            .compare(kind.read(&column, right), &order),
                        expected
                    );
                }
            }
        }
    }
}
