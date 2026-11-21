//! Borrowed sort keys for ordered probes and other simple Arrow sorts.
use std::cmp::Ordering;

use arrow_array::{
    Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit as ArrowTime};
use rtdb_plan::{Schema, SortExpr};
use rtdb_types::{LogicalType, ScalarType, TimeUnit, Value};

use crate::columnar::{Column, ColumnBatch};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Kind {
    Integer,
    Text,
    UtcMicros,
}

impl Kind {
    fn of(ty: &LogicalType) -> Option<Self> {
        let mut kind = None;
        for leg in ty.legs() {
            let next = match leg {
                LogicalType::Null => continue,
                LogicalType::Scalar(
                    ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64,
                ) => Self::Integer,
                LogicalType::Scalar(ScalarType::Utf8) => Self::Text,
                LogicalType::Scalar(ScalarType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: Some(zone),
                }) if zone == "UTC" => Self::UtcMicros,
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
            } => {
                if array.data_type() == &DataType::Null {
                    return true;
                }
                let logical_kind = logical.as_ref().map_or_else(
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
                match (self, array.data_type()) {
                    (
                        Self::Integer,
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
                    )
                    | (Self::Text, DataType::Utf8) => true,
                    (Self::UtcMicros, DataType::Timestamp(ArrowTime::Microsecond, Some(zone))) => {
                        zone.as_ref() == "UTC"
                    }
                    _ => false,
                }
            }
        }
    }

    fn value<'a>(self, value: &'a Value) -> Option<Key<'a>> {
        Some(match (self, value) {
            (_, Value::Null | Value::Undefined) => Key::Null,
            (Self::Integer, Value::I8(value)) => Key::Number(i64::from(*value)),
            (Self::Integer, Value::I16(value)) => Key::Number(i64::from(*value)),
            (Self::Integer, Value::I32(value)) => Key::Number(i64::from(*value)),
            (Self::Integer, Value::I64(value)) => Key::Number(*value),
            (Self::Text, Value::Utf8(value)) => Key::Text(value.as_bytes()),
            (
                Self::UtcMicros,
                Value::Timestamp {
                    value,
                    unit: TimeUnit::Microsecond,
                    timezone: Some(zone),
                },
            ) if zone == "UTC" => Key::Number(*value),
            _ => return None,
        })
    }

    fn read(self, column: &Column, row: usize) -> Key<'_> {
        match column {
            Column::Values(values) => self.value(&values[row]).expect("admitted scalar key"),
            Column::Arrow { array, .. } => {
                if array.data_type() == &DataType::Null || array.is_null(row) {
                    return Key::Null;
                }
                macro_rules! cell {
                    ($ty:ty) => {
                        array
                            .as_any()
                            .downcast_ref::<$ty>()
                            .expect("admitted physical key")
                            .value(row)
                    };
                }
                match array.data_type() {
                    DataType::Int8 => Key::Number(i64::from(cell!(Int8Array))),
                    DataType::Int16 => Key::Number(i64::from(cell!(Int16Array))),
                    DataType::Int32 => Key::Number(i64::from(cell!(Int32Array))),
                    DataType::Int64 => Key::Number(cell!(Int64Array)),
                    DataType::Utf8 => Key::Text(cell!(StringArray).as_bytes()),
                    DataType::Timestamp(..) => Key::Number(cell!(TimestampMicrosecondArray)),
                    _ => unreachable!("only admitted sort keys are read"),
                }
            }
        }
    }
}

enum Key<'a> {
    Null,
    Number(i64),
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
            (Self::Number(left), Self::Number(right)) => left.cmp(&right),
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
