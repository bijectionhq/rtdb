use std::{ops::Range, sync::Arc};

use arrow_array::{
    Array, ArrayRef, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, Float16Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array, UnionArray,
    cast::AsArray,
    types::{Int16Type, Int32Type, Int64Type},
    builder::{
        ArrayBuilder, Decimal32Builder, Decimal64Builder, Decimal128Builder, Decimal256Builder,
        Float16Builder, Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder,
        Int64Builder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder, NullBuilder, make_builder,
    },
};
use arrow_buffer::{ScalarBuffer, i256};
use arrow_schema::{DataType, Field, UnionFields, UnionMode};

use crate::{DecimalValue, LogicalType, ScalarType, Value, decimal_arrow::*};

#[derive(Clone, Debug)]
pub struct NumericList {
    field: Arc<Field>,
    array: ArrayRef,
}

impl NumericList {
    pub(crate) fn new(field: Arc<Field>, array: ArrayRef) -> Option<Self> {
        (field.data_type() == array.data_type() && supported(&field)).then_some(Self { field, array })
    }
    pub fn field(&self) -> &Arc<Field> { &self.field }
    pub fn array(&self) -> &ArrayRef { &self.array }

    pub(crate) fn slice(&self, offset: usize, length: usize) -> Self {
        Self { field: Arc::clone(&self.field), array: self.array.slice(offset, length) }
    }

    pub(crate) fn admit_signed(&mut self) {
        if let Some((field, array)) = signed_layout(&self.field, &self.array) {
            self.field = Arc::new(field);
            self.array = array;
        }
    }

    pub(crate) fn value(&self, index: usize) -> Value {
        self.numeric_component(index).into_value()
    }

    pub(crate) fn numeric_component(&self, index: usize) -> NumericCell {
        read_number(&self.field, self.array.as_ref(), index)
    }

    pub(crate) fn observed_type(&self) -> LogicalType {
        observed_numeric_type(&self.field, self.array.as_ref())
    }
}

pub(crate) fn signed_layout(field: &Field, array: &ArrayRef) -> Option<(Field, ArrayRef)> {
    macro_rules! reinterpret {
        ($source:ty, $target:ty) => {
            {
            let source = array.as_any().downcast_ref::<$source>()?;
            let values = ScalarBuffer::from(source.values().inner().clone());
            Arc::new(<$target>::new(values, source.nulls().cloned()))
        }
        };
    }
    let signed: ArrayRef = match field.data_type() {
        DataType::UInt8 => reinterpret!(UInt8Array, Int8Array),
        DataType::UInt16 => reinterpret!(UInt16Array, Int16Array),
        DataType::UInt32 => reinterpret!(UInt32Array, Int32Array),
        DataType::UInt64 => reinterpret!(UInt64Array, Int64Array),
        DataType::Union(fields, UnionMode::Dense) => {
            if !fields.iter().any(|(_, field)| matches!(field.data_type(),
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64)) {
                return None;
            }
            let union = array.as_any().downcast_ref::<UnionArray>()?;
            let mut ids = Vec::with_capacity(fields.len());
            let mut signed_fields = Vec::with_capacity(fields.len());
            let mut children = Vec::with_capacity(fields.len());
            for (id, field) in fields.iter() {
                let child = union.child(id);
                let (field, child) = signed_layout(field, child)
                    .unwrap_or_else(|| (field.as_ref().clone(), Arc::clone(child)));
                ids.push(id);
                signed_fields.push(field);
                children.push(child);
            }
            let fields = UnionFields::try_new(ids, signed_fields).expect("valid union fields retain their tags");
            Arc::new(UnionArray::try_new(fields, union.type_ids().clone(), union.offsets().cloned(), children)
                .expect("integer reinterpretation preserves child lengths"))
        }
        _ => return None,
    };
    Some((field.clone().with_data_type(signed.data_type().clone()), signed))
}

pub(crate) fn supported(field: &Field) -> bool {
    if let DataType::Union(fields, UnionMode::Dense) = field.data_type() {
        return field.metadata().is_empty() && fields.iter().all(|(_, field)| supported_scalar(field, false));
    }
    supported_scalar(field, false)
}

fn supported_scalar(field: &Field, allow_annotations: bool) -> bool {
    if !inert_metadata(field) { return false; }
    if decimal_parameters(field.data_type()).is_some() {
        return !matches!(field.data_type(), DataType::Dictionary(_, _))
            && (allow_annotations || field.metadata().keys().all(|key| matches!(key.as_str(), INTERNAL_DECIMAL_SCALE_KEY | INTERNAL_DECIMAL_PRECISION_KEY)));
    }
    if field.data_type() == &DataType::Null { return field.is_nullable(); }
    (allow_annotations || field.metadata().is_empty()) && matches!(field.data_type(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        | DataType::Float16 | DataType::Float32 | DataType::Float64)
}

pub(crate) fn numeric_input(field: &Field) -> bool {
    match field.data_type() {
        DataType::Union(fields, UnionMode::Dense) => inert_metadata(field) && fields.iter().all(|(_, field)| numeric_input(field)),
        DataType::RunEndEncoded(_, child) => inert_metadata(field) && numeric_input(child),
        _ => supported_scalar(field, true),
    }
}

fn inert_metadata(field: &Field) -> bool {
    !field.metadata().contains_key("ARROW:extension:name")
        && decimal_field_parameters(field).is_ok()
}

pub(crate) fn observed_type_in(field: &Field, array: &dyn Array, range: std::ops::Range<usize>) -> LogicalType {
    let selected = array.slice(range.start, range.len());
    observed_numeric_type(field, selected.as_ref())
}

pub(crate) fn observed_numeric_type(field: &Field, array: &dyn Array) -> LogicalType {
    let mut types = Vec::new();
    if let DataType::Union(fields, UnionMode::Dense) = field.data_type() {
        return observed_union_type(fields, array.as_union(), scalar_type);
    } else if !array.is_empty() {
        let nulls = if array.data_type() == &DataType::Null { array.len() } else { array.null_count() };
        if nulls < array.len() { types.push(scalar_type(field)); }
        if nulls > 0 { types.push(LogicalType::Null); }
    }
    LogicalType::join_all(&types)
}

pub(crate) fn observed_union_type(
    fields: &UnionFields, array: &UnionArray, leg_type: impl Fn(&Field) -> LogicalType,
) -> LogicalType {
    let mut types = Vec::new();
    let mut present = [false; 128];
    let mut absent = false;
    let mut remaining = fields.iter().filter(|(_, field)| field.data_type() != &DataType::Null).count();
    for (&tag, &offset) in array.type_ids().iter().zip(array.offsets().expect("dense offsets").iter()) {
        let id = tag as usize;
        if present[id] && absent { continue; }
        let child = array.child(tag);
        if child.data_type() == &DataType::Null || child.is_null(offset as usize) {
            if !std::mem::replace(&mut absent, true) { types.push(LogicalType::Null); }
        } else if !std::mem::replace(&mut present[id], true) {
            let (_, field) = fields.iter().find(|(id, _)| *id == tag).expect("union tag");
            types.push(leg_type(field));
            remaining -= 1;
        }
        if remaining == 0 && absent { break; }
    }
    LogicalType::join_all(&types)
}

/// A numeric component read directly from a primitive buffer or an active dense union leg.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NumericCell {
    Null,
    I8(i8), I16(i16), I32(i32), I64(i64),
    U8(u8), U16(u16), U32(u32), U64(u64),
    F16(u16), F32(f32), F64(f64),
    Decimal([u8; 32], DecimalParameters),
}

impl NumericCell {
    pub fn from_value(value: &Value) -> Option<Self> {
        Some(match value {
            Value::Null => Self::Null,
            Value::I8(v) => Self::I8(*v), Value::I16(v) => Self::I16(*v),
            Value::I32(v) => Self::I32(*v), Value::I64(v) => Self::I64(*v),
            Value::U8(v) => Self::U8(*v), Value::U16(v) => Self::U16(*v),
            Value::U32(v) => Self::U32(*v), Value::U64(v) => Self::U64(*v),
            Value::F16(v) => Self::F16(*v), Value::F32(v) => Self::F32(*v), Value::F64(v) => Self::F64(*v),
            Value::Decimal(v) => {
                if !matches!(v.bit_width, 32 | 64 | 128 | 256) || v.unscaled.len() != usize::from(v.bit_width / 8) { return None; }
                Self::Decimal(extend_coefficient(&v.unscaled), DecimalParameters { precision: v.precision, scale: v.scale, bit_width: v.bit_width })
            }
            _ => return None,
        })
    }

    pub fn into_value(self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::I8(v) => Value::I8(v), Self::I16(v) => Value::I16(v),
            Self::I32(v) => Value::I32(v), Self::I64(v) => Value::I64(v),
            Self::U8(v) => Value::U8(v), Self::U16(v) => Value::U16(v),
            Self::U32(v) => Value::U32(v), Self::U64(v) => Value::U64(v),
            Self::F16(v) => Value::F16(v), Self::F32(v) => Value::F32(v), Self::F64(v) => Value::F64(v),
            Self::Decimal(bytes, p) => Value::Decimal(DecimalValue {
                unscaled: bytes[32 - usize::from(p.bit_width / 8)..].to_vec(), precision: p.precision, scale: p.scale, bit_width: p.bit_width,
            }),
        }
    }

    pub fn signed(self) -> Self {
        match self {
            Self::U8(v) => Self::I8(v as i8), Self::U16(v) => Self::I16(v as i16),
            Self::U32(v) => Self::I32(v as i32), Self::U64(v) => Self::I64(v as i64),
            other => other,
        }
    }

    pub fn integer(self) -> Option<i128> {
        Some(match self {
            Self::I8(v) => v.into(), Self::I16(v) => v.into(), Self::I32(v) => v.into(), Self::I64(v) => v.into(),
            Self::U8(v) => v.into(), Self::U16(v) => v.into(), Self::U32(v) => v.into(), Self::U64(v) => v.into(),
            _ => return None,
        })
    }

    pub fn as_f64(self) -> Option<f64> {
        Some(match self {
            Self::Null => return None,
            Self::F16(v) => half::f16::from_bits(v).to_f64(),
            Self::F32(v) => f64::from(v),
            Self::F64(v) => v,
            Self::Decimal(bytes, p) => crate::jvm_number::decimal_bytes_to_f64(&bytes, p.scale),
            integer => integer.integer()? as f64,
        })
    }

    pub fn scalar_type(self) -> Option<ScalarType> {
        Some(match self {
            Self::Null => return None,
            Self::I8(..) => ScalarType::I8, Self::I16(..) => ScalarType::I16,
            Self::I32(..) => ScalarType::I32, Self::I64(..) => ScalarType::I64,
            Self::U8(..) => ScalarType::U8, Self::U16(..) => ScalarType::U16,
            Self::U32(..) => ScalarType::U32, Self::U64(..) => ScalarType::U64,
            Self::F16(..) => ScalarType::F16, Self::F32(..) => ScalarType::F32, Self::F64(..) => ScalarType::F64,
            Self::Decimal(_, p) => ScalarType::Decimal { precision: p.precision, scale: p.scale, bit_width: p.bit_width },
        })
    }
}

fn extend_coefficient(bytes: &[u8]) -> [u8; 32] {
    let mut extended = [if bytes[0] & 128 == 0 { 0 } else { 255 }; 32];
    extended[32 - bytes.len()..].copy_from_slice(bytes);
    extended
}

#[derive(Clone)]
pub struct NumericSpan<'a> {
    field: &'a Field,
    array: &'a dyn Array,
    range: Range<usize>,
}

impl<'a> NumericSpan<'a> {
    pub fn new(field: &'a Field, array: &'a dyn Array, range: Range<usize>) -> Option<Self> {
        (field.data_type() == array.data_type() && numeric_input(field) && range.start <= range.end && range.end <= array.len())
            .then_some(Self { field, array, range })
    }

    /// Opens the selected numeric positions of a child that also has other logical legs.
    pub fn selected(field: &'a Field, array: &'a dyn Array, range: Range<usize>) -> Option<Self> {
        if let Some(span) = Self::new(field, array, range.clone()) { return Some(span); }
        if field.data_type() != array.data_type() || range.start > range.end || range.end > array.len()
            || !matches!(field.data_type(), DataType::Union(_, UnionMode::Dense) | DataType::RunEndEncoded(..))
            || !inert_metadata(field)
        {
            return None;
        }
        range.clone().all(|index| {
            let (field, _, _) = numeric_position(field, array, index);
            supported_scalar(field, true)
        }).then_some(Self { field, array, range })
    }

    pub fn len(&self) -> usize { self.range.len() }

    pub fn is_empty(&self) -> bool { self.range.is_empty() }

    pub fn slice(mut self, offset: usize, count: usize) -> Option<Self> {
        if offset.checked_add(count)? > self.len() { return None; }
        self.range.start += offset;
        self.range.end = self.range.start + count;
        Some(self)
    }

    pub fn get(&self, index: usize) -> Option<NumericCell> {
        (index < self.range.len()).then(|| read_number(self.field, self.array, self.range.start + index))
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = NumericCell> + '_ {
        self.range.clone().map(|index| read_number(self.field, self.array, index))
    }
}

pub(crate) fn scalar_type(field: &Field) -> LogicalType {
    let scalar = match field.data_type() {
        DataType::Null => return LogicalType::Null,
        DataType::Int8 => ScalarType::I8,
        DataType::UInt8 => ScalarType::U8,
        DataType::Int16 => ScalarType::I16,
        DataType::UInt16 => ScalarType::U16,
        DataType::Int32 => ScalarType::I32,
        DataType::UInt32 => ScalarType::U32,
        DataType::Int64 => ScalarType::I64,
        DataType::UInt64 => ScalarType::U64,
        DataType::Float16 => ScalarType::F16,
        DataType::Float32 => ScalarType::F32,
        DataType::Float64 => ScalarType::F64,
        _ => {
            let parameters = decimal_field_parameters(field).expect("validated numeric field").expect("numeric decimal");
            ScalarType::Decimal { precision: parameters.precision, scale: parameters.scale, bit_width: parameters.bit_width }
        }
    };
    LogicalType::Scalar(scalar)
}

fn numeric_position<'a>(mut field: &'a Field, mut array: &'a dyn Array, mut index: usize) -> (&'a Field, &'a dyn Array, usize) {
    loop {
        (field, array, index) = match field.data_type() {
            DataType::Union(fields, UnionMode::Dense) => {
                let union = array.as_union();
                let id = union.type_id(index);
                let (_, field) = fields.iter().find(|(tag, _)| *tag == id).expect("validated numeric union tag");
                (field.as_ref(), union.child(id).as_ref(), union.value_offset(index))
            }
            DataType::RunEndEncoded(ends, child) => {
                macro_rules! run {
                    ($ty:ty) => {{
                        let run = array.as_run::<$ty>();
                        (child.as_ref(), run.values().as_ref(), run.get_physical_index(index))
                    }};
                }
                match ends.data_type() {
                    DataType::Int16 => run!(Int16Type),
                    DataType::Int32 => run!(Int32Type),
                    DataType::Int64 => run!(Int64Type),
                    _ => unreachable!("validated run ends"),
                }
            }
            _ => return (field, array, index),
        };
    }
}

fn read_number(field: &Field, array: &dyn Array, index: usize) -> NumericCell {
    use NumericCell as N;
    let (field, array, index) = numeric_position(field, array, index);
    if array.is_null(index) { return N::Null; }
    macro_rules! cell {
        ($array:ty) => { array.as_any().downcast_ref::<$array>().expect("validated numeric storage").value(index) };
    }
    match field.data_type() {
        DataType::Null => N::Null,
        DataType::Int8 => N::I8(cell!(Int8Array)),
        DataType::UInt8 => N::U8(cell!(UInt8Array)),
        DataType::Int16 => N::I16(cell!(Int16Array)),
        DataType::UInt16 => N::U16(cell!(UInt16Array)),
        DataType::Int32 => N::I32(cell!(Int32Array)),
        DataType::UInt32 => N::U32(cell!(UInt32Array)),
        DataType::Int64 => N::I64(cell!(Int64Array)),
        DataType::UInt64 => N::U64(cell!(UInt64Array)),
        DataType::Float16 => N::F16(cell!(Float16Array).to_bits()),
        DataType::Float32 => N::F32(cell!(Float32Array)),
        DataType::Float64 => N::F64(cell!(Float64Array)),
        ty => {
            let params = decimal_field_parameters(field).expect("validated numeric decimal").expect("numeric decimal");
            let unscaled = match ty {
                DataType::Decimal32(..) => extend_coefficient(&cell!(Decimal32Array).to_be_bytes()),
                DataType::Decimal64(..) => extend_coefficient(&cell!(Decimal64Array).to_be_bytes()),
                DataType::Decimal128(..) => extend_coefficient(&cell!(Decimal128Array).to_be_bytes()),
                DataType::Decimal256(..) => cell!(Decimal256Array).to_be_bytes(),
                _ => unreachable!(),
            };
            N::Decimal(unscaled, params)
        }
    }
}

fn decimal_field(params: DecimalParameters) -> Option<Field> {
    let DecimalParameters { precision, scale, bit_width } = params;
    let p = decimal_storage_precision(precision);
    let s = decimal_storage_scale(scale);
    let ty = match bit_width {
        32 => DataType::Decimal32(p, s),
        64 => DataType::Decimal64(p, s),
        128 => DataType::Decimal128(p, s),
        256 => DataType::Decimal256(p, s),
        _ => return None,
    };
    with_decimal_parameters(Field::new(format!("decimal-{precision}-{scale}-{bit_width}"), ty, false), params).ok()
}

fn primitive_type(value: NumericCell) -> Option<(&'static str, DataType)> {
    use NumericCell::*;
    Some(match value {
        Null => ("null", DataType::Null),
        I8(_) => ("i8", DataType::Int8), U8(_) => ("u8", DataType::UInt8),
        I16(_) => ("i16", DataType::Int16), U16(_) => ("u16", DataType::UInt16),
        I32(_) => ("i32", DataType::Int32), U32(_) => ("u32", DataType::UInt32),
        I64(_) => ("i64", DataType::Int64), U64(_) => ("u64", DataType::UInt64),
        F16(_) => ("f16", DataType::Float16), F32(_) => ("f32", DataType::Float32), F64(_) => ("f64", DataType::Float64),
        Decimal(..) => return None,
    })
}

fn append(builder: &mut dyn ArrayBuilder, value: NumericCell) -> Option<()> {
    use NumericCell::*;
    macro_rules! push {
        ($builder:ty,$value:expr) => { builder.as_any_mut().downcast_mut::<$builder>()?.append_value($value) };
    }
    match value {
        Null => builder.as_any_mut().downcast_mut::<NullBuilder>()?.append_null(),
        I8(value) => push!(Int8Builder,value), U8(value) => push!(UInt8Builder,value),
        I16(value) => push!(Int16Builder,value), U16(value) => push!(UInt16Builder,value),
        I32(value) => push!(Int32Builder,value), U32(value) => push!(UInt32Builder,value),
        I64(value) => push!(Int64Builder,value), U64(value) => push!(UInt64Builder,value),
        F16(value) => push!(Float16Builder,half::f16::from_bits(value)),
        F32(value) => push!(Float32Builder,value), F64(value) => push!(Float64Builder,value),
        Decimal(bytes, params) => match params.bit_width {
            32 => push!(Decimal32Builder,i32::from_be_bytes(bytes[28..].try_into().ok()?)),
            64 => push!(Decimal64Builder,i64::from_be_bytes(bytes[24..].try_into().ok()?)),
            128 => push!(Decimal128Builder,i128::from_be_bytes(bytes[16..].try_into().ok()?)),
            256 => push!(Decimal256Builder,i256::from_be_bytes(bytes)),
            _ => return None,
        },
    }
    Some(())
}

pub(crate) struct NumericListBuilder {
    fields: Vec<Field>,
    decimal_legs: Vec<(DecimalParameters, usize)>,
    builders: Vec<Box<dyn ArrayBuilder>>,
    ids: Vec<i8>,
    offsets: Vec<i32>,
}

impl NumericListBuilder {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            fields: Vec::new(),
            decimal_legs: Vec::new(),
            builders: Vec::new(),
            ids: Vec::with_capacity(capacity),
            offsets: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn push(&mut self, value: NumericCell) -> Option<()> {
        let field = if let Some((name, ty)) = primitive_type(value) {
            if let Some(i) = self.fields.iter().position(|candidate| *candidate.data_type() == ty) {
                return self.append_leg(i, value);
            }
            Field::new(name, ty, value == NumericCell::Null)
        } else {
            let NumericCell::Decimal(_, params) = value else { unreachable!() };
            if let Some(&(_, leg)) = self.decimal_legs.iter().find(|(key, _)| *key == params) {
                return self.append_leg(leg, value);
            }
            let leg = self.add_leg(decimal_field(params)?)?;
            self.decimal_legs.push((params, leg));
            return self.append_leg(leg, value);
        };
        let leg = self.add_leg(field)?;
        self.append_leg(leg, value)
    }

    fn append_leg(&mut self, leg: usize, value: NumericCell) -> Option<()> {
        let builder = self.builders.get_mut(leg)?;
        let id = i8::try_from(leg).ok()?;
        let offset = i32::try_from(builder.len()).ok()?;
        append(builder.as_mut(), value)?;
        self.ids.push(id);
        self.offsets.push(offset);
        Some(())
    }

    fn add_leg(&mut self, field: Field) -> Option<usize> {
        let leg = self.fields.len();
        if leg >= i8::MAX as usize { return None; }
        self.builders.push(make_builder(field.data_type(), 0));
        self.fields.push(field);
        Some(leg)
    }

    pub(crate) fn finish(mut self) -> Option<NumericList> {
        if self.fields.is_empty() {
            return NumericList::new(Arc::new(Field::new("null", DataType::Null, true)), Arc::new(arrow_array::NullArray::new(0)));
        }
        if self.fields.len() == 1 {
            return NumericList::new(Arc::new(self.fields.pop()?), self.builders.pop()?.finish());
        }
        if self.fields.len() == 2
            && let Some(null) = self.fields.iter().position(|field| field.data_type() == &DataType::Null)
        {
            let data = 1 - null;
            let child = self.builders[data].finish();
            let indices = self.ids.into_iter().zip(self.offsets)
                .map(|(tag, offset)| (tag as usize == data).then_some(offset))
                .collect::<Int32Array>();
            let array = arrow_select::take::take(child.as_ref(), &indices, None).ok()?;
            return NumericList::new(Arc::new(self.fields.swap_remove(data).with_nullable(true)), array);
        }
        let children = self.builders.iter_mut().map(|builder| builder.finish()).collect();
        let fields = UnionFields::try_new(0..i8::try_from(self.fields.len()).ok()?, self.fields).ok()?;
        let array = Arc::new(UnionArray::try_new(fields, self.ids.into(), Some(self.offsets.into()), children).ok()?);
        NumericList::new(Arc::new(Field::new("$data$", array.data_type().clone(), false)), array)
    }
}

pub(crate) fn pack(values: &[Value]) -> Option<NumericList> {
    let mut builder = NumericListBuilder::new(values.len());
    for value in values { builder.push(NumericCell::from_value(value)?)?; }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ListBuilder, ListValue, ListView};

    #[test]
    fn wave4_decimal_legs_keep_tags_and_full_width_parameters() {
        let value = |coefficient: i64, scale| Value::Decimal(DecimalValue::new(coefficient.to_be_bytes().to_vec(), 4, scale, 64).unwrap());
        let input = [value(1, -300), Value::I32(7), value(-1, -300), value(2, 1), value(3, -300)];
        let packed = pack(&input).unwrap();
        let union = packed.array().as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(union.type_ids().as_ref(), &[0, 1, 0, 2, 0]);
        assert_eq!(union.offsets().unwrap().as_ref(), &[0, 0, 1, 0, 2]);
        assert_eq!(packed.observed_type(), LogicalType::from_legs([
            LogicalType::Scalar(ScalarType::Decimal { precision: 4, scale: -300, bit_width: 64 }),
            LogicalType::Scalar(ScalarType::I32),
            LogicalType::Scalar(ScalarType::Decimal { precision: 4, scale: 1, bit_width: 64 }),
        ]));
        for (index, expected) in input.iter().enumerate() { assert_eq!(&packed.value(index), expected); }
    }

    #[test]
    fn optional_numeric_lists_store_validity_on_the_primitive_child() {
        let mut builder = ListBuilder::with_capacity(3);
        for value in [Value::F32(-0.0), Value::Null, Value::F32(2.0)] {
            builder.push(value);
        }
        let list = builder.finish();
        let ListView::F32(array) = list.view() else { panic!("expected a nullable float vector"); };
        assert_eq!(array.value(0).to_bits(), (-0.0f32).to_bits());
        assert!(array.is_null(1));
        assert_eq!(array.value(2), 2.0);
    }

    #[test]
    fn mixed_numeric_lists_retain_decimal_metadata_and_unsigned_storage() {
        let decimal = Value::Decimal(DecimalValue::new(123_i128.to_be_bytes().to_vec(), 12, -400, 128).unwrap());
        let unsigned = Value::U32(u32::MAX);
        let mut list = ListValue::from(vec![Value::F16(0x3c00), Value::Null, decimal.clone(), unsigned.clone()]);
        assert!(matches!(list.array().unwrap().data_type(), DataType::Union(_, UnionMode::Dense)));
        assert_eq!(list.get(2).unwrap().as_ref(), &decimal);
        assert_eq!(list.get(3).unwrap().as_ref(), &unsigned);
        list.admit_signed();
        assert_eq!(list.get(3).unwrap().as_ref(), &Value::I32(-1));
        let slice = list.slice_packed(1, 2).unwrap();
        assert_eq!(slice.into_values(), vec![Value::Null, decimal]);
    }
}
