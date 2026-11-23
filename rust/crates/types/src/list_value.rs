use std::{borrow::Cow, fmt, sync::Arc};

use arrow_array::{Array, Float32Array, Float64Array, UnionArray};
use arrow_schema::{DataType, UnionMode};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Anomaly, LogicalType, Result, ScalarType, Value};

#[derive(Clone)]
pub struct ListValue(Storage);

#[derive(Clone, Debug)]
enum Storage {
    Values(Vec<Value>),
    F32(Arc<Float32Array>),
    F64(Arc<Float64Array>),
    Union(Arc<UnionArray>),
}

#[derive(Clone, Copy)]
pub enum ListView<'a> {
    Values(&'a [Value]),
    F32(&'a Float32Array),
    F64(&'a Float64Array),
    Union(&'a UnionArray),
}

impl<'a> ListView<'a> {
    pub fn len(self) -> usize {
        match self {
            Self::Values(values) => values.len(),
            Self::F32(values) => values.len(),
            Self::F64(values) => values.len(),
            Self::Union(values) => values.len(),
        }
    }

    pub fn is_empty(self) -> bool { self.len() == 0 }

    pub fn get(self, index: usize) -> Option<Cow<'a, Value>> {
        if index >= self.len() { return None; }
        Some(match self {
            Self::Values(values) => Cow::Borrowed(&values[index]),
            Self::F32(values) => Cow::Owned(if values.is_null(index) { Value::Null } else { Value::F32(values.value(index)) }),
            Self::F64(values) => Cow::Owned(if values.is_null(index) { Value::Null } else { Value::F64(values.value(index)) }),
            Self::Union(values) => {
                let child = values.child(values.type_id(index));
                let offset = values.value_offset(index);
                Cow::Owned(if child.is_null(offset) {
                    Value::Null
                } else if let Some(values) = child.as_any().downcast_ref::<Float32Array>() {
                    Value::F32(values.value(offset))
                } else if let Some(values) = child.as_any().downcast_ref::<Float64Array>() {
                    Value::F64(values.value(offset))
                } else {
                    Value::Null
                })
            },
        })
    }

    pub fn float_component(self, index: usize) -> Option<f64> {
        let primitive = |array: &dyn Array, index| {
            if array.is_null(index) { return None; }
            if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
                return Some(f64::from(values.value(index)));
            }
            array.as_any().downcast_ref::<Float64Array>().map(|values| values.value(index))
        };
        match self {
            Self::F32(values) => (!values.is_null(index)).then(|| f64::from(values.value(index))),
            Self::F64(values) => (!values.is_null(index)).then(|| values.value(index)),
            Self::Union(values) => primitive(values.child(values.type_id(index)).as_ref(), values.value_offset(index)),
            Self::Values(_) => None,
        }
    }

    pub fn iter(self) -> impl ExactSizeIterator<Item = Cow<'a, Value>> + Clone + 'a {
        (0..self.len()).map(move |index| self.get(index).expect("index in list"))
    }
}

impl ListValue {
    pub fn from_f32(values: Float32Array) -> Self { Self(Storage::F32(Arc::new(values))) }
    pub fn from_f64(values: Float64Array) -> Self { Self(Storage::F64(Arc::new(values))) }

    pub fn supports_float_union(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Union(fields, UnionMode::Dense)
            if fields.iter().any(|(_, field)| matches!(field.data_type(), DataType::Float32 | DataType::Float64))
                && fields.iter().all(|(_, field)| field.metadata().is_empty()
                    && matches!(field.data_type(), DataType::Float32 | DataType::Float64 | DataType::Null)))
    }

    pub fn from_float_union(values: UnionArray) -> Option<Self> {
        Self::supports_float_union(values.data_type()).then(|| Self(Storage::Union(Arc::new(values))))
    }

    pub fn view(&self) -> ListView<'_> {
        match &self.0 {
            Storage::Values(values) => ListView::Values(values),
            Storage::F32(values) => ListView::F32(values),
            Storage::F64(values) => ListView::F64(values),
            Storage::Union(values) => ListView::Union(values),
        }
    }

    pub fn get(&self, index: usize) -> Option<Cow<'_, Value>> { self.view().get(index) }

    pub fn slice_packed(&self, offset: usize, length: usize) -> Option<Self> {
        match &self.0 {
            Storage::F32(values) => Some(Self::from_f32(values.slice(offset, length))),
            Storage::F64(values) => Some(Self::from_f64(values.slice(offset, length))),
            Storage::Union(values) => Self::from_float_union(values.slice(offset, length)),
            Storage::Values(_) => None,
        }
    }

    pub fn array(&self) -> Option<&dyn Array> {
        match &self.0 {
            Storage::Values(_) => None,
            Storage::F32(values) => Some(values.as_ref()),
            Storage::F64(values) => Some(values.as_ref()),
            Storage::Union(values) => Some(values.as_ref()),
        }
    }

    pub fn len(&self) -> usize { self.view().len() }
    pub fn is_empty(&self) -> bool { self.view().is_empty() }
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Cow<'_, Value>> + Clone { self.view().iter() }

    pub fn values_mut(&mut self) -> Option<&mut Vec<Value>> {
        match &mut self.0 { Storage::Values(values) => Some(values), _ => None }
    }

    pub fn into_values(self) -> Vec<Value> {
        match self.0 {
            Storage::Values(values) => values,
            Storage::F32(values) => values.iter().map(|value| value.map_or(Value::Null, Value::F32)).collect(),
            Storage::F64(values) => values.iter().map(|value| value.map_or(Value::Null, Value::F64)).collect(),
            Storage::Union(values) => ListView::Union(&values).iter().map(Cow::into_owned).collect(),
        }
    }

    pub(crate) fn retained_payload_bytes(&self) -> Result<usize> {
        let overflow = || Anomaly::incorrect("rtdb/value-size-overflow", "Retained value memory exceeds usize");
        match &self.0 {
            Storage::Values(values) => {
                let inline = values.capacity().checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?;
                values.iter().try_fold(inline, |bytes, value| bytes.checked_add(value.heap_bytes()?).ok_or_else(overflow))
            }
            Storage::F32(values) => Ok(values.get_array_memory_size()),
            Storage::F64(values) => Ok(values.get_array_memory_size()),
            Storage::Union(values) => Ok(values.get_array_memory_size()),
        }
    }

    pub fn element_type(&self) -> LogicalType {
        let packed = |scalar, length, nulls| {
            if length == 0 { LogicalType::Nothing }
            else if nulls == length { LogicalType::Null }
            else if nulls != 0 { LogicalType::Scalar(scalar).maybe() }
            else { LogicalType::Scalar(scalar) }
        };
        match &self.0 {
            Storage::Values(values) => {
                let types = values.iter().map(Value::logical_type).collect::<Vec<_>>();
                LogicalType::join_all(&types)
            }
            Storage::F32(values) => packed(ScalarType::F32, values.len(), values.null_count()),
            Storage::F64(values) => packed(ScalarType::F64, values.len(), values.null_count()),
            Storage::Union(values) => {
                let mut types = Vec::with_capacity(3);
                for row in 0..values.len() {
                    let child = values.child(values.type_id(row));
                    let ty = if child.is_null(values.value_offset(row)) { LogicalType::Null } else {
                        match child.data_type() {
                            DataType::Float32 => LogicalType::Scalar(ScalarType::F32),
                            DataType::Float64 => LogicalType::Scalar(ScalarType::F64),
                            _ => LogicalType::Null,
                        }
                    };
                    if !types.contains(&ty) { types.push(ty); }
                    if types.len() == 3 { break; }
                }
                LogicalType::join_all(&types)
            },
        }
    }
}

impl From<Vec<Value>> for ListValue {
    fn from(values: Vec<Value>) -> Self {
        match values.iter().find(|value| !matches!(value, Value::Null)) {
            Some(Value::F32(_)) if values.iter().all(|value| matches!(value, Value::F32(_) | Value::Null)) => {
                Self::from_f32(values.iter().map(|value| if let Value::F32(value) = value { Some(*value) } else { None }).collect())
            }
            Some(Value::F64(_)) if values.iter().all(|value| matches!(value, Value::F64(_) | Value::Null)) => {
                Self::from_f64(values.iter().map(|value| if let Value::F64(value) = value { Some(*value) } else { None }).collect())
            }
            _ => Self(Storage::Values(values)),
        }
    }
}

impl fmt::Debug for ListValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl PartialEq for ListValue {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(left, right)| left == right)
    }
}

impl Serialize for ListValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for ListValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Vec::<Value>::deserialize(deserializer).map(Self::from)
    }
}
