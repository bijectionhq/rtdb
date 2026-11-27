use std::{borrow::Cow, fmt, ops::Range, sync::Arc};

use arrow_array::{Array, ArrayRef, Float32Array, Float64Array, UnionArray};
use arrow_schema::{DataType, Field, UnionMode};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Anomaly, ScalarList, LogicalType, Result, ScalarType, Value, numeric_list::{self, NumericCell, NumericList, NumericListBuilder}};

#[derive(Clone)]
pub struct ListValue(Storage);

#[derive(Clone, Debug)]
enum Storage {
    Values(Vec<Value>),
    F32(Arc<Float32Array>),
    F64(Arc<Float64Array>),
    Union(Arc<UnionArray>),
    Numeric(NumericList),
    Scalars(ScalarList),
}

#[derive(Clone, Copy)]
pub enum ListView<'a> {
    Values(&'a [Value]),
    F32(&'a Float32Array),
    F64(&'a Float64Array),
    Union(&'a UnionArray),
    Numeric(&'a NumericList),
    Scalars(&'a ScalarList),
}

#[derive(Clone, Copy, Debug)]
pub enum FloatComponent {
    Null,
    F32(f32),
    F64(f64),
}

impl FloatComponent {
    fn read(array: &dyn Array, index: usize) -> Option<Self> {
        if array.is_null(index) || array.data_type() == &DataType::Null {
            return Some(Self::Null);
        }
        if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
            return Some(Self::F32(values.value(index)));
        }
        array.as_any().downcast_ref::<Float64Array>().map(|values| Self::F64(values.value(index)))
    }
}

impl Serialize for FloatComponent {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        // The indices are the discriminants used by Value's derived Serialize implementation.
        match self {
            Self::Null => serializer.serialize_unit_variant("Value", 1, "Null"),
            Self::F32(value) => serializer.serialize_newtype_variant("Value", 12, "F32", &value.to_bits()),
            Self::F64(value) => serializer.serialize_newtype_variant("Value", 13, "F64", &value.to_bits()),
        }
    }
}

pub struct FloatListSlice<'a> {
    view: ListView<'a>,
    range: Range<usize>,
}

impl Serialize for FloatListSlice<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_seq(self.range.clone().map(|index| self.view.typed_float(index).expect("float component")))
    }
}

impl<'a> ListView<'a> {
    pub fn len(self) -> usize {
        match self {
            Self::Values(values) => values.len(),
            Self::F32(values) => values.len(),
            Self::F64(values) => values.len(),
            Self::Union(values) => values.len(),
            Self::Numeric(values) => values.array().len(),
            Self::Scalars(values) => values.array().len(),
        }
    }

    pub fn is_empty(self) -> bool { self.len() == 0 }

    pub fn is_float(self) -> bool { matches!(self, Self::F32(_) | Self::F64(_) | Self::Union(_)) }

    pub fn float_slice(self, range: Range<usize>) -> Option<FloatListSlice<'a>> {
        (self.is_float() && range.start <= range.end && range.end <= self.len())
            .then_some(FloatListSlice { view: self, range })
    }

    pub fn typed_float(self, index: usize) -> Option<FloatComponent> {
        if index >= self.len() { return None; }
        match self {
            Self::F32(values) => Some(if values.is_null(index) { FloatComponent::Null } else { FloatComponent::F32(values.value(index)) }),
            Self::F64(values) => Some(if values.is_null(index) { FloatComponent::Null } else { FloatComponent::F64(values.value(index)) }),
            Self::Union(values) => FloatComponent::read(values.child(values.type_id(index)).as_ref(), values.value_offset(index)),
            Self::Numeric(values) => {
                let array = values.array();
                if let Some(union) = array.as_any().downcast_ref::<UnionArray>() {
                    FloatComponent::read(union.child(union.type_id(index)).as_ref(), union.value_offset(index))
                } else {
                    FloatComponent::read(array.as_ref(), index)
                }
            }
            Self::Scalars(_) => None,
            Self::Values(values) => match &values[index] {
                Value::Null => Some(FloatComponent::Null),
                Value::F32(value) => Some(FloatComponent::F32(*value)),
                Value::F64(value) => Some(FloatComponent::F64(*value)),
                _ => None,
            },
        }
    }

    pub fn get(self, index: usize) -> Option<Cow<'a, Value>> {
        if index >= self.len() { return None; }
        Some(match self {
            Self::Values(values) => Cow::Borrowed(&values[index]),
            Self::Numeric(values) => Cow::Owned(values.value(index)),
            Self::Scalars(values) => Cow::Owned(values.value(index)),
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

    pub fn utf8(self, index: usize) -> Option<&'a str> {
        match self {
            Self::Values(values) => match values.get(index)? { Value::Utf8(text) => Some(text), _ => None },
            Self::Scalars(values) if index < self.len() => values.utf8(index),
            _ => None,
        }
    }

    pub fn binary(self, index: usize) -> Option<&'a [u8]> {
        match self {
            Self::Values(values) => match values.get(index)? { Value::Binary(bytes) => Some(bytes), _ => None },
            Self::Scalars(values) if index < self.len() => values.binary(index),
            _ => None,
        }
    }

    pub fn is_packed_numeric(self) -> bool { !matches!(self, Self::Values(_) | Self::Scalars(_)) }

    pub fn numeric_component(self, index: usize) -> NumericCell {
        match self {
            Self::F32(values) => if values.is_null(index) { NumericCell::Null } else { NumericCell::F32(values.value(index)) },
            Self::F64(values) => if values.is_null(index) { NumericCell::Null } else { NumericCell::F64(values.value(index)) },
            Self::Union(values) => {
                let child = values.child(values.type_id(index));
                let offset = values.value_offset(index);
                if child.is_null(offset) { return NumericCell::Null; }
                if let Some(values) = child.as_any().downcast_ref::<Float32Array>() { return NumericCell::F32(values.value(offset)); }
                if let Some(values) = child.as_any().downcast_ref::<Float64Array>() { return NumericCell::F64(values.value(offset)); }
                NumericCell::Null
            }
            Self::Numeric(values) => values.numeric_component(index),
            Self::Values(values) => NumericCell::from_value(&values[index]).expect("numeric component"),
            Self::Scalars(_) => panic!("expected a numeric list"),
        }
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
            Self::Numeric(_) | Self::Values(_) | Self::Scalars(_) => None,
        }
    }

    pub fn iter(self) -> impl ExactSizeIterator<Item = Cow<'a, Value>> + Clone + 'a {
        (0..self.len()).map(move |index| self.get(index).expect("index in list"))
    }
}

impl ListValue {
    /// Retains a non-null canonical ID column, including the original signed integer widths.
    pub fn from_entity_id_arrow(field: Arc<Field>, values: ArrayRef) -> Option<Self> {
        let valid = match field.data_type() {
            DataType::Union(fields, UnionMode::Dense) => {
                field.metadata().is_empty() && fields.iter().all(|(_, field)| entity_id_scalar(field).is_some())
            }
            _ => entity_id_scalar(&field).is_some(),
        };
        if !valid || field.data_type() != values.data_type() || values.logical_null_count() != 0 { return None; }
        if Self::supports_numeric_arrow(&field) {
            Self::from_numeric_arrow(field, values)
        } else {
            Self::from_scalar_arrow(field, values)
        }
    }

    /// Builds a list from fallible cells, keeping a packable numeric input in typed storage.
    pub fn try_from_cells(cells: impl Iterator<Item = Result<Value>>) -> Result<Self> {
        let mut builder = ListBuilder::with_capacity(cells.size_hint().0);
        for cell in cells { builder.push(cell?); }
        Ok(builder.finish())
    }

    pub fn from_f32(values: Float32Array) -> Self { Self(Storage::F32(Arc::new(values))) }
    pub fn from_f64(values: Float64Array) -> Self { Self(Storage::F64(Arc::new(values))) }

    pub fn supports_float_union(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Union(fields, UnionMode::Dense)
            if fields.iter().any(|(_, field)| matches!(field.data_type(), DataType::Float32 | DataType::Float64))
                && fields.iter().all(|(_, field)| field.metadata().is_empty()
                    && matches!(field.data_type(), DataType::Float32 | DataType::Float64 | DataType::Null)))
    }

    pub fn supports_numeric_arrow(field: &Field) -> bool { numeric_list::supported(field) }

    pub fn observed_numeric_type(field: &Field, array: &dyn Array, range: std::ops::Range<usize>) -> Option<LogicalType> {
        (range.start <= range.end && range.end <= array.len()
            && field.data_type() == array.data_type() && Self::supports_numeric_arrow(field))
            .then(|| numeric_list::observed_type_in(field, array, range))
    }

    pub fn from_numeric_arrow(field: Arc<Field>, values: ArrayRef) -> Option<Self> {
        if field.data_type() != values.data_type() { return None; }
        if field.metadata().is_empty() {
            if let Some(array) = values.as_any().downcast_ref::<Float32Array>() {
                return Some(Self::from_f32(array.clone()));
            }
            if let Some(array) = values.as_any().downcast_ref::<Float64Array>() {
                return Some(Self::from_f64(array.clone()));
            }
            if let Some(array) = values.as_any().downcast_ref::<UnionArray>()
                && let Some(list) = Self::from_float_union(array.clone())
            {
                return Some(list);
            }
        }
        NumericList::new(field, values).map(|values| Self(Storage::Numeric(values)))
    }

    pub fn from_scalar_arrow(field: Arc<Field>, values: ArrayRef) -> Option<Self> {
        ScalarList::new(field, values).map(|values| Self(Storage::Scalars(values)))
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
            Storage::Numeric(values) => ListView::Numeric(values),
            Storage::Scalars(values) => ListView::Scalars(values),
        }
    }

    pub fn get(&self, index: usize) -> Option<Cow<'_, Value>> { self.view().get(index) }

    pub fn slice_packed(&self, offset: usize, length: usize) -> Option<Self> {
        match &self.0 {
            Storage::F32(values) => Some(Self::from_f32(values.slice(offset, length))),
            Storage::F64(values) => Some(Self::from_f64(values.slice(offset, length))),
            Storage::Union(values) => Self::from_float_union(values.slice(offset, length)),
            Storage::Numeric(values) => Some(Self(Storage::Numeric(values.slice(offset, length)))),
            Storage::Scalars(values) => {
                let slice = values.slice(offset, length);
                if !matches!(slice.array().data_type(), DataType::Union(..)) && slice.array().null_count() == length { Self::from_numeric_arrow(Arc::new(Field::new("null", DataType::Null, true)), Arc::new(arrow_array::NullArray::new(length))) }
                else { Some(Self(Storage::Scalars(slice))) }
            },
            Storage::Values(_) => None,
        }
    }

    pub fn array(&self) -> Option<&dyn Array> {
        match &self.0 {
            Storage::Values(_) => None,
            Storage::F32(values) => Some(values.as_ref()),
            Storage::F64(values) => Some(values.as_ref()),
            Storage::Union(values) => Some(values.as_ref()),
            Storage::Numeric(values) => Some(values.array().as_ref()),
            Storage::Scalars(values) => Some(values.array().as_ref()),
        }
    }

    pub fn len(&self) -> usize { self.view().len() }
    pub fn is_empty(&self) -> bool { self.view().is_empty() }
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Cow<'_, Value>> + Clone { self.view().iter() }

    pub fn packed_column(&self) -> Option<(Arc<Field>, ArrayRef)> {
        let array: ArrayRef = match &self.0 {
            Storage::Values(_) => return None,
            Storage::F32(array) => array.clone(),
            Storage::F64(array) => array.clone(),
            Storage::Union(array) => array.clone(),
            Storage::Numeric(values) => return Some((Arc::clone(values.field()), Arc::clone(values.array()))),
            Storage::Scalars(values) => return Some((Arc::clone(values.field()), Arc::clone(values.array()))),
        };
        Some((Arc::new(Field::new("value", array.data_type().clone(), array.is_nullable())), array))
    }

    pub fn admit_signed(&mut self) {
        match &mut self.0 {
            Storage::Numeric(values) => values.admit_signed(),
            Storage::Scalars(values) => values.admit_signed(),
            _ => (),
        }
    }

    pub fn scalar_values(&self) -> Option<&[Value]> {
        match &self.0 {
            Storage::Values(values) => Some(values),
            _ if self.is_empty() => Some(&[]),
            _ => None,
        }
    }

    pub fn values_mut(&mut self) -> Option<&mut Vec<Value>> {
        match &mut self.0 { Storage::Values(values) => Some(values), _ => None }
    }

    pub fn into_values(self) -> Vec<Value> {
        match self.0 {
            Storage::Values(values) => values,
            Storage::F32(values) => values.iter().map(|value| value.map_or(Value::Null, Value::F32)).collect(),
            Storage::F64(values) => values.iter().map(|value| value.map_or(Value::Null, Value::F64)).collect(),
            Storage::Union(values) => ListView::Union(&values).iter().map(Cow::into_owned).collect(),
            Storage::Numeric(values) => ListView::Numeric(&values).iter().map(Cow::into_owned).collect(),
            Storage::Scalars(values) => ListView::Scalars(&values).iter().map(Cow::into_owned).collect(),
        }
    }

    pub fn retained_payload_bytes(&self) -> Result<usize> {
        let overflow = || Anomaly::incorrect("rtdb/value-size-overflow", "Retained value memory exceeds usize").with_kind(crate::AnomalyKind::ProgramLimitExceeded);
        match &self.0 {
            Storage::Values(values) => {
                let inline = values.capacity().checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?;
                values.iter().try_fold(inline, |bytes, value| bytes.checked_add(value.heap_bytes()?).ok_or_else(overflow))
            }
            Storage::F32(values) => Ok(values.get_array_memory_size()),
            Storage::F64(values) => Ok(values.get_array_memory_size()),
            Storage::Union(values) => Ok(values.get_array_memory_size()),
            Storage::Numeric(values) => values.array().get_array_memory_size().checked_add(values.field().size()).ok_or_else(overflow),
            Storage::Scalars(values) => values.array().get_array_memory_size().checked_add(values.field().size()).ok_or_else(overflow),
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
            Storage::Numeric(values) => values.observed_type(),
            Storage::Scalars(values) => values.observed_type(),
            Storage::F32(values) => packed(ScalarType::F32, values.len(), values.null_count()),
            Storage::F64(values) => packed(ScalarType::F64, values.len(), values.null_count()),
            Storage::Union(values) => {
                let field = Field::new("", values.data_type().clone(), false);
                numeric_list::observed_numeric_type(&field, values.as_ref())
            },
        }
    }
}

fn entity_id_scalar(field: &Field) -> Option<ScalarType> {
    let extension = field.metadata().get("ARROW:extension:name").map(String::as_str);
    Some(match (field.data_type(), extension) {
        (DataType::Utf8, None) if field.metadata().is_empty() => ScalarType::Utf8,
        (DataType::Utf8, Some("xt/clj-keyword")) => ScalarType::Keyword,
        (DataType::FixedSizeBinary(16), Some("uuid")) => ScalarType::Uuid,
        (DataType::Int8, None) if field.metadata().is_empty() => ScalarType::I8,
        (DataType::Int16, None) if field.metadata().is_empty() => ScalarType::I16,
        (DataType::Int32, None) if field.metadata().is_empty() => ScalarType::I32,
        (DataType::Int64, None) if field.metadata().is_empty() => ScalarType::I64,
        _ => return None,
    })
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
            _ => {
                if values.iter().all(|v| matches!(v, Value::Null | Value::F16(_) | Value::F32(_) | Value::F64(_)
                    | Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_)
                    | Value::U8(_) | Value::U16(_) | Value::U32(_) | Value::U64(_) | Value::Decimal(_)))
                    && let Some(packed) = numeric_list::pack(&values)
                {
                    Self::from_numeric_arrow(Arc::clone(packed.field()), Arc::clone(packed.array()))
                        .expect("constructed numeric buffer")
                } else {
                    Self(Storage::Values(values))
                }
            }
        }
    }
}

pub struct ListBuilder(BuildingList);

enum BuildingList {
    Numeric(NumericListBuilder),
    Mixed { prefix: ListValue, tail: Vec<Value> },
}

impl ListBuilder {
    pub fn with_capacity(size: usize) -> Self {
        Self(BuildingList::Numeric(NumericListBuilder::new(size)))
    }

    pub fn push(&mut self, value: Value) {
        match &mut self.0 {
            BuildingList::Mixed { tail, .. } => tail.push(value),
            BuildingList::Numeric(builder) => {
                if NumericCell::from_value(&value).is_some_and(|number| builder.push(number).is_some()) { return; }
                let BuildingList::Numeric(builder) = std::mem::replace(&mut self.0, BuildingList::Numeric(NumericListBuilder::new(0))) else { unreachable!() };
                self.0 = BuildingList::Mixed { prefix: finish_numeric(builder), tail: vec![value] };
            }
        }
    }

    pub fn try_push_numeric(&mut self, value: NumericCell) -> bool {
        match &mut self.0 {
            BuildingList::Numeric(builder) => builder.push(value).is_some(),
            BuildingList::Mixed { .. } => false,
        }
    }

    pub fn push_numeric(&mut self, value: NumericCell) {
        if !self.try_push_numeric(value) { self.push(value.into_value()); }
    }

    pub fn push_f32(&mut self, value: f32) {
        self.push_numeric(NumericCell::F32(value));
    }

    pub fn push_f64(&mut self, value: f64) {
        self.push_numeric(NumericCell::F64(value));
    }

    pub fn finish(self) -> ListValue {
        match self.0 {
            BuildingList::Numeric(builder) => finish_numeric(builder),
            BuildingList::Mixed { prefix, mut tail } => {
                let prefix_len = prefix.len();
                tail.reserve_exact(prefix_len);
                tail.extend(prefix.iter().map(Cow::into_owned));
                tail.rotate_right(prefix_len);
                ListValue(Storage::Values(tail))
            }
        }
    }
}

fn finish_numeric(builder: NumericListBuilder) -> ListValue {
    let array = builder.finish().expect("validated numeric components");
    ListValue::from_numeric_arrow(Arc::clone(array.field()), Arc::clone(array.array()))
        .expect("constructed numeric buffer")
}

impl fmt::Debug for ListValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl PartialEq for ListValue {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() { return false; }
        let (left, right) = (self.view(), other.view());
        if left.is_packed_numeric() && right.is_packed_numeric() {
            return (0..left.len()).all(|index| left.numeric_component(index) == right.numeric_component(index));
        }
        left.iter().zip(right.iter()).all(|(left, right)| left == right)
    }
}

impl Serialize for ListValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let view = self.view();
        if let Some(floats) = view.float_slice(0..view.len()) {
            return floats.serialize(serializer);
        }
        let mut sequence = serializer.serialize_seq(Some(view.len()))?;
        for index in 0..view.len() {
            if let Some(component) = view.typed_float(index) {
                sequence.serialize_element(&component)?;
            } else {
                sequence.serialize_element(&view.get(index).expect("index in list"))?;
            }
        }
        sequence.end()
    }
}

impl<'de> Deserialize<'de> for ListValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Vec::<Value>::deserialize(deserializer).map(Self::from)
    }
}

#[test]
fn packed_list_serialization_retains_the_value_tags_and_float_bits() {
    let raw = vec![Value::F32(-0.0), Value::Null, Value::F64(f64::from_bits(0xfff8_0000_0000_4321))];
    let packed = ListValue::from(raw.clone());
    let view = packed.view();
    assert!(matches!(view, ListView::Union(_)));
    let expected = serde_json::to_vec(&raw).unwrap();
    assert_eq!(serde_json::to_vec(&packed).unwrap(), expected);
    assert_eq!(serde_json::to_vec(&view.float_slice(1..3).unwrap()).unwrap(), serde_json::to_vec(&raw[1..]).unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wave4_mixed_builder_retains_packed_prefix_until_row_export() {
        let mut builder = ListBuilder::with_capacity(4);
        builder.push(Value::F32(-0.0));
        builder.push(Value::Null);
        builder.push(Value::Utf8("text".into()));
        builder.push(Value::F32(1.0));
        let BuildingList::Mixed { prefix, tail } = &builder.0 else { panic!("expected a split builder"); };
        assert!(matches!(prefix.view(), ListView::F32(_)));
        assert_eq!(prefix.get(1).as_deref(), Some(&Value::Null));
        assert_eq!(tail, &[Value::Utf8("text".into()), Value::F32(1.0)]);
        assert_eq!(serde_json::to_vec(&builder.finish()).unwrap(),
            serde_json::to_vec(&[Value::F32(-0.0), Value::Null, Value::Utf8("text".into()), Value::F32(1.0)]).unwrap());
    }
}
