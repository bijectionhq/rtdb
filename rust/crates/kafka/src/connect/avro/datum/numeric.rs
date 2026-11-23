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

    pub fn from_wire_schema(schema: &Schema) -> Option<Self> {
        match schema {
            Schema::Date | Schema::TimeMillis => Some(Self::Int),
            Schema::TimestampMillis => Some(Self::Long),
            _ => Self::from_schema(schema),
        }
    }

    pub fn matches_connect(self, kind: &super::super::SchemaKind) -> bool {
        use super::super::SchemaKind;
        matches!((self, kind), (Self::Int, SchemaKind::I8 | SchemaKind::I16 | SchemaKind::I32)
            | (Self::Long, SchemaKind::I64) | (Self::Float, SchemaKind::F32) | (Self::Double, SchemaKind::F64))
    }
}

pub(in crate::connect::avro) type Branches = (Option<u32>, u32);
pub(in crate::connect::avro) type PackedLayout = (NumericKind, Option<Branches>);

#[derive(Clone, Copy)]
pub(in crate::connect::avro) enum NumericInput<'a> {
    Plain(NumericKind),
    Union(&'a [Schema]),
}

impl<'a> NumericInput<'a> {
    pub fn from_schema(schema: &'a Schema) -> Option<Self> {
        if let Schema::Union(union) = schema {
            return Some(Self::Union(union.variants()));
        }
        Some(Self::Plain(NumericKind::from_wire_schema(schema)?))
    }

    fn next(self, input: &mut Preflight<'_>, depth: usize) -> Result<Option<NumericKind>> {
        let too_deep = || error("avro-decode", "Avro datum nesting limit exceeded");
        if depth > 128 { return Err(too_deep()); }
        match self {
            Self::Plain(kind) => Ok(Some(kind)),
            Self::Union(members) => {
                let index = usize::try_from(input.int()?).map_err(|cause| error("avro-decode", cause))?;
                let member = members.get(index).ok_or_else(|| error("avro-decode", "Union index is out of range"))?;
                if depth == 128 { return Err(too_deep()); }
                if matches!(member, Schema::Null) {
                    Ok(None)
                } else {
                    NumericKind::from_wire_schema(member).map(Some)
                        .ok_or_else(|| error("avro-decode", "Array member lacks a numeric wire type"))
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(in crate::connect::avro) struct PackedArray {
    pub kind: NumericKind,
    pub branches: Option<Branches>,
    pub values: ListValue,
}

impl PackedArray {
    pub fn layout(schema: &Schema) -> Option<PackedLayout> {
        if let Schema::Union(union) = schema {
            return match union.variants() {
                [only] => Some((NumericKind::from_schema(only)?, Some((None, 0)))),
                [Schema::Null, present] => Some((NumericKind::from_schema(present)?, Some((Some(0), 1)))),
                [present, Schema::Null] => Some((NumericKind::from_schema(present)?, Some((Some(1), 0)))),
                _ => None,
            };
        }
        Some((NumericKind::from_schema(schema)?, None))
    }

    pub fn read(writer: NumericInput<'_>, (kind, branches): PackedLayout, input: &mut Preflight<'_>, depth: usize) -> Result<Self> {
        use NumericKind::{Double, Float, Int, Long};
        let values = match kind {
            Int => read_array::<Int32Type>(input, writer, depth, |_, input| input.int())?,
            Long => read_array::<Int64Type>(input, writer, depth, |kind, input| match kind {
                Int => Ok(i64::from(input.int()?)),
                _ => input.long(),
            })?,
            Float => read_array::<Float32Type>(input, writer, depth, |kind, input| match kind {
                Int => Ok(input.int()? as f32),
                Long => Ok(input.long()? as f32),
                _ => float(input),
            })?,
            Double => read_array::<Float64Type>(input, writer, depth, |kind, input| match kind {
                Int => Ok(f64::from(input.int()?)),
                Long => Ok(input.long()? as f64),
                Float => Ok(f64::from(float(input)?)),
                Double => double(input),
            })?,
        };
        Ok(Self { kind, branches, values })
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
                let branch = if matches!(value, Avro::Null) { null.expect("packed null requires an Avro branch") } else { present };
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
    input: &mut Preflight<'_>, writer: NumericInput<'_>, depth: usize,
    read: impl Fn(NumericKind, &mut Preflight<'_>) -> Result<T::Native>,
) -> Result<ListValue> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(0);
    loop {
        let count = block(input)?;
        if count == 0 { break; }
        for _ in 0..count {
            let value = writer.next(input, depth)?.map(|kind| read(kind, input)).transpose()?;
            builder.append_option(value);
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
