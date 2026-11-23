use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, Float16Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array, UnionArray,
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
        number_cell(&self.field, self.array.as_ref(), index)
    }

    pub(crate) fn observed_type(&self) -> LogicalType {
        let mut legs = Vec::new();
        if let DataType::Union(fields, UnionMode::Dense) = self.field.data_type() {
            let array = self.array.as_any().downcast_ref::<UnionArray>().expect("validated numeric union");
            for i in 0..array.len() {
                let id = array.type_id(i);
                let (_, field) = fields.iter().find(|(tag, _)| *tag == id).expect("validated numeric union tag");
                let child = array.child(id);
                let ty = if child.is_null(array.value_offset(i)) { LogicalType::Null } else { scalar_type(field) };
                if !legs.contains(&ty) { legs.push(ty); }
            }
        } else if !self.array.is_empty() {
            let nulls = self.array.logical_null_count();
            if nulls < self.array.len() { legs.push(scalar_type(&self.field)); }
            if nulls > 0 { legs.push(LogicalType::Null); }
        }
        LogicalType::join_all(&legs)
    }
}

fn signed_layout(field: &Field, array: &ArrayRef) -> Option<(Field, ArrayRef)> {
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
        return field.metadata().is_empty() && fields.iter().all(|(_, field)| supported_scalar(field));
    }
    supported_scalar(field)
}

fn supported_scalar(field: &Field) -> bool {
    if decimal_parameters(field.data_type()).is_some() {
        return !matches!(field.data_type(), DataType::Dictionary(_, _))
            && field.metadata().keys().all(|key| matches!(key.as_str(), INTERNAL_DECIMAL_SCALE_KEY | INTERNAL_DECIMAL_PRECISION_KEY))
            && decimal_field_parameters(field).is_ok();
    }
    if field.data_type() == &DataType::Null { return field.is_nullable(); }
    field.metadata().is_empty() && matches!(field.data_type(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        | DataType::Float16 | DataType::Float32 | DataType::Float64)
}

fn scalar_type(field: &Field) -> LogicalType {
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

fn number_cell(field: &Field, array: &dyn Array, index: usize) -> Value {
    if array.is_null(index) { return Value::Null; }
    macro_rules! cell {
        ($array:ty) => { array.as_any().downcast_ref::<$array>().expect("validated numeric storage").value(index) };
    }
    match field.data_type() {
        DataType::Null => Value::Null,
        DataType::Int8 => Value::I8(cell!(Int8Array)),
        DataType::UInt8 => Value::U8(cell!(UInt8Array)),
        DataType::Int16 => Value::I16(cell!(Int16Array)),
        DataType::UInt16 => Value::U16(cell!(UInt16Array)),
        DataType::Int32 => Value::I32(cell!(Int32Array)),
        DataType::UInt32 => Value::U32(cell!(UInt32Array)),
        DataType::Int64 => Value::I64(cell!(Int64Array)),
        DataType::UInt64 => Value::U64(cell!(UInt64Array)),
        DataType::Float16 => Value::F16(cell!(Float16Array).to_bits()),
        DataType::Float32 => Value::F32(cell!(Float32Array)),
        DataType::Float64 => Value::F64(cell!(Float64Array)),
        DataType::Union(fields, UnionMode::Dense) => {
            let union = array.as_any().downcast_ref::<UnionArray>().expect("validated numeric union");
            let id = union.type_id(index);
            let (_, field) = fields.iter().find(|(tag, _)| *tag == id).expect("validated numeric union tag");
            number_cell(field, union.child(id).as_ref(), union.value_offset(index))
        }
        ty => {
            let Some(params) = decimal_field_parameters(field).expect("validated numeric decimal") else { unreachable!() };
            let unscaled = match ty {
                DataType::Decimal32(..) => cell!(Decimal32Array).to_be_bytes().to_vec(),
                DataType::Decimal64(..) => cell!(Decimal64Array).to_be_bytes().to_vec(),
                DataType::Decimal128(..) => cell!(Decimal128Array).to_be_bytes().to_vec(),
                DataType::Decimal256(..) => cell!(Decimal256Array).to_be_bytes().to_vec(),
                _ => unreachable!(),
            };
            Value::Decimal(DecimalValue { unscaled, precision: params.precision, scale: params.scale, bit_width: params.bit_width })
        }
    }
}

fn field(value: &Value) -> Option<Field> {
    let (name, data_type) = match value {
        Value::Null => ("null", DataType::Null),
        Value::I8(_) => ("i8", DataType::Int8),
        Value::U8(_) => ("u8", DataType::UInt8),
        Value::I16(_) => ("i16", DataType::Int16),
        Value::U16(_) => ("u16", DataType::UInt16),
        Value::I32(_) => ("i32", DataType::Int32),
        Value::U32(_) => ("u32", DataType::UInt32),
        Value::I64(_) => ("i64", DataType::Int64),
        Value::U64(_) => ("u64", DataType::UInt64),
        Value::F16(_) => ("f16", DataType::Float16),
        Value::F32(_) => ("f32", DataType::Float32),
        Value::F64(_) => ("f64", DataType::Float64),
        Value::Decimal(DecimalValue { precision, scale, bit_width, .. }) => {
            let p = decimal_storage_precision(*precision);
            let s = decimal_storage_scale(*scale);
            let ty = match bit_width {
                32 => DataType::Decimal32(p,s),
                64 => DataType::Decimal64(p,s),
                128 => DataType::Decimal128(p,s),
                256 => DataType::Decimal256(p,s),
                _ => return None,
            };
            let f = Field::new(format!("decimal-{precision}-{scale}-{bit_width}"), ty, false);
            return with_decimal_parameters(f, DecimalParameters { precision: *precision, scale: *scale, bit_width: *bit_width }).ok();
        }
        _ => return None,
    };
    Some(Field::new(name, data_type, matches!(value, Value::Null)))
}

fn append(builder: &mut dyn ArrayBuilder, value: &Value) -> Option<()> {
    macro_rules! push {
        ($builder:ty,$value:expr) => { builder.as_any_mut().downcast_mut::<$builder>()?.append_value($value) };
    }
    match value {
        Value::Null => builder.as_any_mut().downcast_mut::<NullBuilder>()?.append_null(),
        Value::I8(value) => push!(Int8Builder,*value),
        Value::U8(value) => push!(UInt8Builder,*value),
        Value::I16(value) => push!(Int16Builder,*value),
        Value::U16(value) => push!(UInt16Builder,*value),
        Value::I32(value) => push!(Int32Builder,*value),
        Value::U32(value) => push!(UInt32Builder,*value),
        Value::I64(value) => push!(Int64Builder,*value),
        Value::U64(value) => push!(UInt64Builder,*value),
        Value::F16(value) => push!(Float16Builder,half::f16::from_bits(*value)),
        Value::F32(value) => push!(Float32Builder,*value),
        Value::F64(value) => push!(Float64Builder,*value),
        Value::Decimal(value) => match value.bit_width {
            32 => push!(Decimal32Builder,i32::from_be_bytes(value.unscaled.as_slice().try_into().ok()?)),
            64 => push!(Decimal64Builder,i64::from_be_bytes(value.unscaled.as_slice().try_into().ok()?)),
            128 => push!(Decimal128Builder,i128::from_be_bytes(value.unscaled.as_slice().try_into().ok()?)),
            256 => push!(Decimal256Builder,i256::from_be_bytes(value.unscaled.as_slice().try_into().ok()?)),
            _ => return None,
        },
        _ => return None,
    }
    Some(())
}

pub(crate) struct NumericListBuilder {
    fields: Vec<Field>,
    builders: Vec<Box<dyn ArrayBuilder>>,
    ids: Vec<i8>,
    offsets: Vec<i32>,
}

impl NumericListBuilder {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            fields: Vec::new(),
            builders: Vec::new(),
            ids: Vec::with_capacity(capacity),
            offsets: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn push(&mut self, value: &Value) -> Option<()> {
        let field = field(value)?;
        let leg = if let Some(i) = self.fields.iter().position(|candidate| candidate == &field) { i } else {
            let i = self.fields.len();
            if i >= i8::MAX as usize { return None; }
            self.builders.push(make_builder(field.data_type(), 0));
            self.fields.push(field);
            i
        };
        let builder = self.builders.get_mut(leg)?;
        let id = i8::try_from(leg).ok()?;
        let offset = i32::try_from(builder.len()).ok()?;
        append(builder.as_mut(), value)?;
        self.ids.push(id);
        self.offsets.push(offset);
        Some(())
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
    for value in values { builder.push(value)?; }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ListBuilder, ListValue, ListView};

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
