use super::{Kind, invalid};
use crate::{DecimalValue, Result, StructFields, TimeUnit, Value};

const DECIMAL_DIGITS: u8 = 38;
const DECIMAL_TAG: u8 = 6;

/// The receiver-side correspondence from canonical clear cells to the installed
/// compound wrapper. Shapes and discriminants follow Bijection's value format.
pub struct CompoundAdoption<'a>(pub(super) &'a Kind);

pub(super) fn is_compound(kind: &Kind) -> bool {
    compound_shape(kind) == Some(true)
}

fn compound_shape(kind: &Kind) -> Option<bool> {
    Some(match kind {
        Kind::List { element } => {
            compound_shape(element)?;
            true
        }
        Kind::Struct { .. } if is_native_decimal(kind) => false,
        Kind::Struct { fields } => {
            for field in fields { compound_shape(&field.kind)?; }
            true
        }
        Kind::Union { legs } => {
            let mut compound = false;
            for leg in legs { compound |= compound_shape(leg)?; }
            compound
        }
        _ => {
            tag(kind)?;
            false
        }
    })
}

impl CompoundAdoption<'_> {
    #[doc(hidden)]
    pub fn convert(&self, bytes: &[u8]) -> Result<Value> {
        let mut image = bytes;
        let value = cell(self.0, &mut image).filter(|_| image.is_empty())
            .ok_or_else(|| invalid("Canonical cell is outside the admitted compound layout"))?;
        Ok(Value::Struct(StructFields::from([("v2_value".into(), value)])))
    }
}

fn word<const N: usize>(bytes: &mut &[u8]) -> Option<[u8; N]> {
    let (head, rest) = bytes.split_first_chunk()?;
    *bytes = rest;
    Some(*head)
}

fn close(bytes: &mut &[u8]) -> bool {
    if let Some(rest) = bytes.strip_prefix(&[0, 0]) {
        *bytes = rest;
        true
    } else {
        false
    }
}

fn payload(bytes: &mut &[u8]) -> Option<Vec<u8>> {
    let mut value = Vec::new();
    loop {
        let end = bytes.iter().position(|byte| *byte == 0)?;
        value.extend_from_slice(&bytes[..end]);
        *bytes = &bytes[end..];
        match word::<2>(bytes)? {
            [0, 1] => return Some(value),
            [0, 255] => value.push(0),
            _ => return None,
        }
    }
}

fn tag(kind: &Kind) -> Option<u8> {
    Some(match kind {
        Kind::Null {} => 2,
        Kind::Bool {} => 3,
        Kind::I64 {} => 4,
        Kind::F64 {} => 5,
        Kind::Struct { .. } if is_native_decimal(kind) => DECIMAL_TAG,
        Kind::DateDay {} => 7,
        Kind::Instant {} => 8,
        Kind::Utf8 {} => 9,
        Kind::Binary {} => 10,
        Kind::List { .. } => 12,
        Kind::Struct { .. } => 13,
        _ => return None,
    })
}

fn is_native_decimal(kind: &Kind) -> bool {
    let Kind::Struct { fields } = kind else { return false; };
    let [coefficient, scale] = fields.as_slice() else { return false; };
    coefficient.name == "coefficient" && scale.name == "scale"
        && !coefficient.optional && !coefficient.nullable && !scale.optional && !scale.nullable
        && matches!(coefficient.kind, Kind::Decimal { precision, scale: 0, bit_width: 128 }
            if precision == i32::from(DECIMAL_DIGITS))
        && matches!(scale.kind, Kind::I8 {})
}

fn decimal_parts(bytes: &mut &[u8]) -> Option<(i128, i8)> {
    let [sign, power, digits @ ..] = word::<40>(bytes)?;
    let negative = match sign {
        0 => true,
        1 if power == 0 && digits == [0; 38] => return Some((0, 0)),
        2 => false,
        _ => return None,
    };
    let plain = |byte| if negative { !byte } else { byte };
    let power = plain(power);
    if !(1..=2 * DECIMAL_DIGITS).contains(&power) || plain(digits[0]) == 0 {
        return None;
    }
    let mut significant = 0;
    let mut magnitude = 0i128;
    for (index, digit) in digits.into_iter().map(plain).enumerate() {
        if digit >= 10 || (digit != 0 && index >= usize::from(power)) {
            return None;
        }
        if digit != 0 {
            significant = index + 1;
        }
        magnitude = magnitude * 10 + i128::from(digit);
    }
    let exponent = i16::from(power) - i16::from(DECIMAL_DIGITS);
    // Integer zeros can be significant to the coefficient even when the
    // canonical key's right padding is all zero.
    let count = (significant as i16).max(exponent);
    let coefficient = magnitude / 10i128.pow((i16::from(DECIMAL_DIGITS) - count) as u32);
    let scale = (count - exponent) as i8;
    Some((if negative { -coefficient } else { coefficient }, scale))
}

fn cell(kind: &Kind, bytes: &mut &[u8]) -> Option<Value> {
    if let Kind::Union { legs } = kind {
        let head = *bytes.first()?;
        return cell(legs.iter().find(|leg| tag(leg) == Some(head))?, bytes);
    }
    let [head] = word(bytes)?;
    if tag(kind) != Some(head) { return None; }
    let signed = |word| (u64::from_be_bytes(word) ^ (1 << 63)) as i64;
    Some(match kind {
        Kind::Null {} => Value::Null,
        Kind::Bool {} => Value::Bool(match word(bytes)? { [0] => false, [1] => true, _ => return None }),
        Kind::I64 {} => Value::I64(signed(word(bytes)?)),
        Kind::F64 {} => {
            let word = u64::from_be_bytes(word(bytes)?);
            let bits = if word < 1 << 63 { !word } else { word ^ (1 << 63) };
            let value = f64::from_bits(bits);
            if value.is_nan() && bits != 0x7ff8_0000_0000_0000 { return None; }
            Value::F64(value)
        }
        Kind::DateDay {} => Value::DateDay((u32::from_be_bytes(word(bytes)?) ^ (1 << 31)) as i32),
        Kind::Instant {} => Value::Timestamp {
            value: signed(word(bytes)?), unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
        },
        Kind::Utf8 {} => Value::Utf8(String::from_utf8(payload(bytes)?).ok()?),
        Kind::Binary {} => Value::Binary(payload(bytes)?),
        Kind::Struct { .. } if head == DECIMAL_TAG => {
            let (coefficient, scale) = decimal_parts(bytes)?;
            Value::Struct(StructFields::from([
                ("coefficient".into(), Value::Decimal(DecimalValue::new(
                    coefficient.to_be_bytes().to_vec(), i32::from(DECIMAL_DIGITS), 0, 128,
                ).ok()?)),
                ("scale".into(), Value::I8(scale)),
            ]))
        }
        Kind::List { element } => {
            let mut values = Vec::new();
            while !close(bytes) { values.push(cell(element, bytes)?); }
            Value::list(values)
        }
        Kind::Struct { fields } => {
            let mut values = StructFields::new();
            let mut previous = String::new();
            while !close(bytes) {
                let name = String::from_utf8(payload(bytes)?).ok()?;
                if name <= previous { return None; }
                let physical = format!("value_{name}");
                let field = fields.iter().find(|field| field.name == physical)?;
                let value = if field.nullable && bytes.first() == Some(&2) {
                    *bytes = &bytes[1..];
                    Value::Null
                } else {
                    cell(&field.kind, bytes)?
                };
                values.insert(physical, value);
                previous = name;
            }
            for field in fields {
                let Some(name) = field.name.strip_prefix("is_present_") else {
                    if !field.name.starts_with("value_") { return None; }
                    continue;
                };
                if !matches!(field.kind, Kind::Bool {}) || field.optional || field.nullable { return None; }
                let value = format!("value_{name}");
                let present = values.contains_key(&value);
                if !present { values.insert(value, Value::Null); }
                values.insert(field.name.clone(), Value::Bool(present));
            }
            if fields.iter().any(|field| !field.accepts(values.get(&field.name))) { return None; }
            Value::Struct(values)
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::{Value as Json, json};

    use crate::{DecimalValue, OwnedSchema, StructFields, Value};

    #[derive(Deserialize)]
    struct DecimalCases {
        ordered: Vec<(String, i8, Vec<u8>, u8)>,
        invalid: Vec<(Vec<u8>, u8)>,
    }

    fn decimal_kind() -> Json {
        json!({"kind":"struct", "fields":[
            {"name":"coefficient", "type":{"kind":"decimal","precision":38,"scale":0,"bitWidth":128},
                "optional":false,"nullable":false},
            {"name":"scale","type":{"kind":"i8"},"optional":false,"nullable":false}
        ]})
    }

    fn declaration(kind: Json) -> crate::Result<OwnedSchema> {
        OwnedSchema::from_json(json!({"version":2,"validity":"acceptance","fields":[
            {"name":"payload","type":kind,"optional":true,"nullable":false}
        ]}).to_string().as_bytes())
    }

    fn successor(kind: Json) -> crate::Result<OwnedSchema> {
        declaration(json!({"kind":"union","legs":[{"kind":"binary"},{"kind":"struct","fields":[
            {"name":"v2_value","type":kind,"optional":false,"nullable":true}
        ]}]}))
    }

    fn list_image(prefix: &[u8], padding: u8) -> Vec<u8> {
        let mut image = vec![12, 6];
        image.extend_from_slice(prefix);
        image.resize(42, padding);
        image.extend_from_slice(&[0, 0]);
        image
    }

    #[test]
    fn decimal_adoption_matches_native_columnar_canonical_cases() -> crate::Result<()> {
        let cases: DecimalCases = serde_json::from_str(include_str!("compound_adoption/decimal_cases.json")).unwrap();
        let original = declaration(json!({"kind":"canonical_binary"}))?;
        let successor = successor(json!({"kind":"list","element":{"kind":"union","legs":[
            {"kind":"null"}, decimal_kind()
        ]}}))?;
        let (field, codec) = original.clear_compound_successor(&successor, &[])?;
        let mut previous = None;
        for (coefficient, scale, prefix, padding) in cases.ordered {
            let image = list_image(&prefix, padding);
            assert!(previous.as_ref().is_none_or(|prior| prior < &image));
            let converted = codec.convert(&image)?;
            let document = StructFields::from([(field.to_owned(), converted.clone())]);
            successor.validate_row(&document)?;

            let Value::Struct(wrapper) = converted else { unreachable!() };
            let Some(Value::List(list)) = wrapper.get("v2_value") else { unreachable!() };
            let item = list.iter().next().unwrap();
            let Value::Struct(parts) = item.as_ref() else { unreachable!() };
            assert_eq!(list.len(), 1);
            assert!(parts.keys().map(String::as_str).eq(["coefficient", "scale"]));
            let coefficient = coefficient.parse::<i128>().unwrap();
            assert_eq!(parts.get("coefficient"), Some(&Value::Decimal(DecimalValue::new(
                coefficient.to_be_bytes().to_vec(), 38, 0, 128,
            )?)));
            assert_eq!(parts.get("scale"), Some(&Value::I8(scale)));
            previous = Some(image);
        }
        for (prefix, padding) in cases.invalid {
            assert!(codec.convert(&list_image(&prefix, padding)).is_err());
        }
        let mut truncated = list_image(&[2, 39, 1], 0);
        truncated.truncate(41);
        assert!(codec.convert(&truncated).is_err());
        let mut surplus = list_image(&[2, 39, 1], 0);
        surplus.push(0);
        assert!(codec.convert(&surplus).is_err());
        assert_eq!(codec.convert(&[12, 2, 0, 0])?, Value::Struct(StructFields::from([(
            "v2_value".into(), Value::list(vec![Value::Null]),
        )])));
        Ok(())
    }

    #[test]
    fn decimal_adoption_keeps_presence_and_refuses_other_decimal_layouts() -> crate::Result<()> {
        let original = declaration(json!({"kind":"canonical_binary"}))?;
        let optional = successor(json!({"kind":"struct","fields":[
            {"name":"value_amount","type":decimal_kind(),"optional":false,"nullable":true},
            {"name":"is_present_amount","type":{"kind":"bool"},"optional":false,"nullable":false}
        ]}))?;
        let (_, codec) = original.clear_compound_successor(&optional, &[])?;
        for (image, present) in [
            (&[13, 0, 0][..], false),
            (b"\x0damount\x00\x01\x02\x00\x00".as_slice(), true),
        ] {
            assert_eq!(codec.convert(image)?, Value::Struct(StructFields::from([(
                "v2_value".into(), Value::Struct(StructFields::from([
                    ("value_amount".into(), Value::Null),
                    ("is_present_amount".into(), Value::Bool(present)),
                ])),
            )])));
        }
        assert!(original.clear_compound_successor(&successor(decimal_kind())?, &[]).is_err());
        let fixed = json!({"kind":"decimal","precision":38,"scale":2,"bitWidth":128});
        let mut wrong_struct = decimal_kind();
        wrong_struct["fields"][0]["type"]["precision"] = json!(37);
        for kind in [fixed, wrong_struct] {
            let wrong = successor(json!({"kind":"list","element":kind}))?;
            assert!(original.clear_compound_successor(&wrong, &[]).is_err());
        }
        Ok(())
    }

    #[test]
    fn canonical_list_uses_the_declared_wrapper_and_rejects_a_changed_leg() -> crate::Result<()> {
        let original = OwnedSchema::from_json(br#"{"version":2,"validity":"acceptance","fields":[
            {"name":"payload","type":{"kind":"canonical_binary"},"optional":true,"nullable":false}
        ]}"#)?;
        let successor = OwnedSchema::from_json(br#"{"version":2,"validity":"acceptance","fields":[
            {"name":"payload","type":{"kind":"union","legs":[{"kind":"binary"},{"kind":"struct","fields":[
                {"name":"v2_value","type":{"kind":"list","element":{"kind":"i64"}},"optional":false,"nullable":true}
            ]}]},"optional":true,"nullable":false}
        ]}"#)?;
        let (field, codec) = original.clear_compound_successor(&successor, &[])?;
        assert_eq!(field, "payload");
        let image = [12, 4, 0x80, 0, 0, 0, 0, 0, 0, 9, 0, 0];
        let Value::Struct(fields) = codec.convert(&image)? else { unreachable!() };
        assert_eq!(fields.get("v2_value"), Some(&Value::list(vec![Value::I64(9)])));
        assert!(codec.convert(&[12, 3, 1, 0, 0]).is_err());
        assert!(codec.convert(&[12, 0, 0, 9]).is_err());
        assert!(successor.clear_compound_successor(&successor, &[]).is_err());
        Ok(())
    }
}
