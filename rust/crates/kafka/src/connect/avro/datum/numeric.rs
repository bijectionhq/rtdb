use arrow_array::{Array, Float32Array, Float64Array, Int32Array, Int64Array, builder::PrimitiveBuilder,
    types::{ArrowPrimitiveType, Float32Type, Float64Type, Int32Type, Int64Type}};
use arrow_schema::DataType;
use rtdb_types::ListValue;

use super::super::super::numeric;
use super::{Avro, Preflight, Result, Schema, binary::block, error};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect::avro) enum NumericKind { Int, Long, Float, Double }

impl NumericKind {
    pub fn from_schema(schema: &Schema) -> Option<Self> {
        Some(match schema {
            Schema::Int => Self::Int, Schema::Long => Self::Long, Schema::Float => Self::Float, Schema::Double => Self::Double,
            _ => return None,
        })
    }

    pub fn matches_connect(self, kind: &super::super::SchemaKind) -> bool {
        use super::super::SchemaKind;
        matches!((self, kind), (Self::Int, SchemaKind::I8 | SchemaKind::I16 | SchemaKind::I32)
            | (Self::Long, SchemaKind::I64) | (Self::Float, SchemaKind::F32) | (Self::Double, SchemaKind::F64))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(in crate::connect::avro) struct PackedArray {
    pub kind: NumericKind,
    pub branches: Option<(u32, u32)>,
    pub values: ListValue,
}

impl PackedArray {
    pub fn layout(schema: &Schema) -> Option<(NumericKind, Option<(u32, u32)>)> {
        if let Schema::Union(union) = schema {
            let members = union.variants();
            let [first, second] = members else { return None; };
            if matches!(first, Schema::Null) { return Some((NumericKind::from_schema(second)?, Some((0, 1)))); }
            if matches!(second, Schema::Null) { return Some((NumericKind::from_schema(first)?, Some((1, 0)))); }
        }
        Some((NumericKind::from_schema(schema)?, None))
    }

    pub fn read(
        writer: NumericKind, reader: NumericKind, branches: Option<(u32, u32)>,
        input: &mut Preflight<'_>, depth: usize,
    ) -> Result<Self> {
        use NumericKind::{Double, Float, Int, Long};
        let values = match (writer, reader) {
            (Int, Int) => read_array::<Int32Type>(input, branches, depth, |input| input.int())?,
            (Int, Long) => read_array::<Int64Type>(input, branches, depth, |input| Ok(i64::from(input.int()?)))?,
            (Int, Float) => read_array::<Float32Type>(input, branches, depth, |input| Ok(input.int()? as f32))?,
            (Int, Double) => read_array::<Float64Type>(input, branches, depth, |input| Ok(f64::from(input.int()?)))?,
            (Long, Long) => read_array::<Int64Type>(input, branches, depth, |input| input.long())?,
            (Long, Float) => read_array::<Float32Type>(input, branches, depth, |input| Ok(input.long()? as f32))?,
            (Long, Double) => read_array::<Float64Type>(input, branches, depth, |input| Ok(input.long()? as f64))?,
            (Float, Float) => read_array::<Float32Type>(input, branches, depth, float)?,
            (Float, Double) => read_array::<Float64Type>(input, branches, depth, |input| Ok(f64::from(float(input)?)))?,
            (Double, Double) => read_array::<Float64Type>(input, branches, depth, double)?,
            _ => unreachable!("compatible numeric array resolution"),
        };
        Ok(Self { kind: reader, branches, values })
    }

    pub fn promote(mut self, reader: NumericKind) -> Self {
        use NumericKind::{Double, Float, Int, Long};
        if self.kind == reader || self.values.is_empty() || self.values.array().is_some_and(|array| *array.data_type() == DataType::Null) {
            self.kind = reader;
            return self;
        }
        let input = self.values.array().expect("packed numeric array");
        macro_rules! cast {
            ($source:ty, $target:ty, $f:expr) => {
                numeric::pack_primitive(input.as_any().downcast_ref::<$source>().expect("packed Avro primitive").unary::<_, $target>($f))
            };
        }
        self.values = match (self.kind, reader) {
            (Int, Long) => cast!(Int32Array, Int64Type, i64::from),
            (Int, Float) => cast!(Int32Array, Float32Type, |value| value as f32),
            (Int, Double) => cast!(Int32Array, Float64Type, f64::from),
            (Long, Float) => cast!(Int64Array, Float32Type, |value| value as f32),
            (Long, Double) => cast!(Int64Array, Float64Type, |value| value as f64),
            (Float, Double) => cast!(Float32Array, Float64Type, f64::from),
            _ => unreachable!("compatible numeric array resolution"),
        };
        self.kind = reader;
        self
    }

    pub fn into_physical_values(self) -> Vec<Avro> {
        let wrap = |value| match self.branches {
            Some((null, present)) => {
                let branch = if matches!(value, Avro::Null) { null } else { present };
                Avro::Union(branch, Box::new(value))
            }
            None => value,
        };
        let array = self.values.array().expect("packed numeric array");
        macro_rules! unpack {
            ($array:ty, $variant:ident) => {
                array.as_any().downcast_ref::<$array>().expect("packed Avro primitive")
                    .iter().map(|value| wrap(value.map_or(Avro::Null, Avro::$variant))).collect()
            };
        }
        match array.data_type() {
            DataType::Null => (0..array.len()).map(|_| wrap(Avro::Null)).collect(),
            DataType::Int32 => unpack!(Int32Array, Int),
            DataType::Int64 => unpack!(Int64Array, Long),
            DataType::Float32 => unpack!(Float32Array, Float),
            DataType::Float64 => unpack!(Float64Array, Double),
            _ => unreachable!("packed Avro primitive"),
        }
    }
}

fn read_array<T: ArrowPrimitiveType>(
    input: &mut Preflight<'_>, branches: Option<(u32, u32)>, depth: usize,
    read: impl Fn(&mut Preflight<'_>) -> Result<T::Native>,
) -> Result<ListValue> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(0);
    loop {
        let count = block(input)?;
        if count == 0 { break; }
        for _ in 0..count {
            if depth > 128 { return Err(error("avro-decode", "Avro datum nesting limit exceeded")); }
            if let Some((null, present)) = branches {
                let index = usize::try_from(input.int()?).map_err(|cause| error("avro-decode", cause))?;
                if index != null as usize && index != present as usize {
                    return Err(error("avro-decode", "Union index is out of range"));
                }
                if depth == 128 { return Err(error("avro-decode", "Avro datum nesting limit exceeded")); }
                if index == null as usize { builder.append_null(); continue; }
            }
            builder.append_value(read(input)?);
        }
    }
    Ok(numeric::pack_primitive(builder.finish()))
}

pub(super) fn float(input: &mut Preflight<'_>) -> Result<f32> {
    Ok(f32::from_le_bytes(input.number_bytes(4)?.try_into().expect("four bytes")))
}
pub(super) fn double(input: &mut Preflight<'_>) -> Result<f64> {
    Ok(f64::from_le_bytes(input.number_bytes(8)?.try_into().expect("eight bytes")))
}
