use rtdb_types::{Anomaly,Result};
use super::{Object,invalid};
pub(super) use crate::jvm_number::Number;
use crate::jvm_number::{Cast,Error};
use crate::decimal::decimal_parts_text as decimal_text;

pub(super) fn error(error: Error) -> Anomaly { match error { Error::Invalid => invalid(),Error::External(error) => error } }
pub(super) fn cast(tag: &str,value: &Object) -> Result<Number> {
    let kind = match tag { "i8" => Cast::Byte,"i16" => Cast::Short,"i32" => Cast::Int,"i64" => Cast::Long,"f32" => Cast::Float,"f64" => Cast::Double,_ => return Err(invalid()) };
    match value {
        Object::Number(value) => Number::cast(kind,value).map_err(error),
        Object::Character(value) if tag.starts_with('i') => Number::cast(kind,&Number::Long(i64::from(*value))).map_err(error),
        _ => Err(invalid()),
    }
}

pub(super) fn wire(value: &Number) -> rtdb_format::transit::TransitValue {
    use rtdb_format::transit::TransitValue as T;
    let tag = |name: &str,value| T::Tagged { tag: name.into(),representation: Box::new(value) };
    match value {
        Number::Byte(value) => T::Integer(i64::from(*value)),Number::Short(value) => T::Integer(i64::from(*value)),Number::Int(value) => T::Integer(i64::from(*value)),Number::Long(value) => T::Integer(*value),
        Number::BigInt(value) => tag("n",T::String(value.to_string())),Number::Float(value) => T::Float32(*value),Number::Double(value) => T::Float(*value),
        Number::Decimal { coefficient,scale } => tag("f",T::String(decimal_text(coefficient,*scale))),
        Number::Ratio { numerator,denominator } => tag("ratio",T::Array(vec![tag("n",T::String(numerator.to_string())),tag("n",T::String(denominator.to_string()))])),
    }
}
