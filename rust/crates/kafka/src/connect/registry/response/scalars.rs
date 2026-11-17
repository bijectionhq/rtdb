use rtdb_types::{decimal_digit, java_text::{JavaString, JavaText}};
use super::{ConversionContext, Failure, FailureKind, Profile, Result, cursor::{Cursor, Kind}};

pub(super) fn text(cursor: &mut Cursor<'_>, context: &mut ConversionContext) -> Result<Option<JavaString>> {
    Ok(match cursor.kind(context)? {
        Kind::Null => { cursor.take_null(context)?; None }
        Kind::String => Some(cursor.string(context)?),
        Kind::Integer | Kind::Float => Some(cursor.number_text(context)?),
        Kind::True | Kind::False => {
            let value = cursor.take_boolean_token(context)?.unwrap();
            Some(constant(if value { "true" } else { "false" }, context)?)
        }
        _ => return Err(cursor.coercion("Cannot deserialize a string from a container")),
    })
}

pub(super) fn integer(cursor: &mut Cursor<'_>, context: &mut ConversionContext) -> Result<Option<i32>> {
    let kind = cursor.kind(context)?;
    if kind == Kind::Null { cursor.take_null(context)?; return Ok(None); }
    if kind == Kind::Float {
        let token = cursor.number_text(context)?;
        let value = double(token.view()).ok_or_else(|| cursor.coercion("Invalid floating-point token"))?;
        if value < f64::from(i32::MIN) || value > f64::from(i32::MAX) {
            return Err(cursor.coercion("Numeric value is outside java.lang.Integer range"));
        }
        return Ok(Some(value as i32));
    }
    let token = match kind {
        Kind::Integer => cursor.number_text(context)?,
        Kind::String => cursor.string(context)?,
        _ => return Err(cursor.coercion("Cannot deserialize java.lang.Integer from this token")),
    };
    let value = if kind == Kind::String { trim(token.view()) } else { token.view() };
    if kind == Kind::String && (value.is_empty() || equal(value, "null")) { return Ok(None); }
    if kind == Kind::String && cursor.profile() == Profile::Jackson221 && value.len_units() > 1000 {
        return Err(Failure::at(FailureKind::Constraint, cursor.offset(), "Coerced integer string length exceeds 1000"));
    }
    signed(value).and_then(|value| i32::try_from(value).ok()).map(Some)
        .ok_or_else(|| cursor.coercion("Invalid java.lang.Integer value"))
}

pub(super) fn boolean(cursor: &mut Cursor<'_>, context: &mut ConversionContext) -> Result<bool> {
    Ok(match cursor.kind(context)? {
        Kind::Null => { cursor.take_null(context)?; false }
        Kind::True | Kind::False => cursor.take_boolean_token(context)?.unwrap(),
        Kind::Integer => {
            let token = cursor.number_text(context)?;
            token.view().units().any(|unit| matches!(unit, 0x31..=0x39))
        }
        Kind::String => {
            let token = cursor.string(context)?;
            let value = trim(token.view());
            if value.is_empty() || equal(value, "null") || ["false", "False", "FALSE"].iter().any(|text| equal(value, text)) { false }
            else if ["true", "True", "TRUE"].iter().any(|text| equal(value, text)) { true }
            else { return Err(cursor.coercion("Invalid boolean value")); }
        }
        _ => return Err(cursor.coercion("Cannot deserialize boolean from this token")),
    })
}

pub(super) fn enumeration(cursor: &mut Cursor<'_>, context: &mut ConversionContext, names: &[&str]) -> Result<Option<usize>> {
    let index = match cursor.kind(context)? {
        Kind::Null => { cursor.take_null(context)?; return Ok(None); }
        Kind::Integer => {
            let token = cursor.number_text(context)?;
            signed(token.view()).and_then(|value| i32::try_from(value).ok())
        }
        Kind::String => {
            let token = cursor.string(context)?;
            let value = trim(token.view());
            if let Some(index) = names.iter().position(|name| equal(value, name)) { return Ok(Some(index)); }
            let first = value.units().next();
            if first.is_some_and(|unit| matches!(unit, 0x30..=0x39))
                && !(cursor.profile() == Profile::Jackson221 && first == Some(0x30) && value.len_units() > 1) {
                signed(value).and_then(|value| i32::try_from(value).ok())
            } else { None }
        }
        _ => return Err(cursor.coercion("Cannot deserialize enum from this token")),
    };
    index.and_then(|index| usize::try_from(index).ok()).filter(|index| *index < names.len()).map(Some)
        .ok_or_else(|| cursor.coercion("Unknown enum value"))
}

pub(super) fn constant(text: &str, context: &mut ConversionContext) -> Result<JavaString> {
    let mut units = [0u16; 16];
    let mut length = 0;
    for unit in text.encode_utf16() { units[length] = unit; length += 1; }
    Ok(JavaString::copy_utf16_with_admission(&units[..length], |bytes| context.reserve(bytes))?)
}

pub(super) fn equal(value: JavaText<'_>, text: &str) -> bool { value.units().eq(text.encode_utf16()) }
pub(super) fn trim(value: JavaText<'_>) -> JavaText<'_> {
    let start = value.units().take_while(|unit| *unit <= 0x20).count();
    let end = value.units().enumerate().filter(|(_, unit)| *unit > 0x20).map(|(index, _)| index + 1).last().unwrap_or(start);
    value.slice_units(start..end).unwrap()
}

fn signed(value: JavaText<'_>) -> Option<i64> {
    let mut units = value.units().peekable();
    let negative = units.peek() == Some(&0x2d);
    if units.peek().is_some_and(|unit| matches!(*unit, 0x2b | 0x2d)) { units.next(); }
    let mut result = 0i64;
    let mut found = false;
    let limit = if negative { i64::MIN } else { -i64::MAX };
    for unit in units {
        let digit = decimal_digit(char::from_u32(u32::from(unit))?)?;
        found = true;
        result = result.checked_mul(10)?.checked_sub(i64::from(digit))?;
        if result < limit { return None; }
    }
    found.then(|| if negative { result } else { -result })
}

fn double(value: JavaText<'_>) -> Option<f64> {
    if equal(value, "NaN") { return Some(f64::NAN); }
    if ["Infinity", "+Infinity", "+INF"].iter().any(|text| equal(value, text)) { return Some(f64::INFINITY); }
    if ["-Infinity", "-INF"].iter().any(|text| equal(value, text)) { return Some(f64::NEG_INFINITY); }
    let mut bytes = [0u8; 1024];
    for (index, unit) in value.units().enumerate() { *bytes.get_mut(index)? = u8::try_from(unit).ok()?; }
    std::str::from_utf8(&bytes[..value.len_units()]).ok()?.parse().ok()
}
