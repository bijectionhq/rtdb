use super::{Kind, invalid};
use crate::{Result, StructFields, TimeUnit, Value};

/// The receiver-side correspondence from canonical clear cells to the installed
/// compound wrapper. Shapes and discriminants follow Bijection's value format.
pub struct CompoundAdoption<'a>(pub(super) &'a Kind);

pub(super) fn is_compound(kind: &Kind) -> bool {
    match kind {
        Kind::List { .. } | Kind::Struct { .. } => true,
        Kind::Union { legs } => legs.iter().any(is_compound),
        _ => false,
    }
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
        Kind::DateDay {} => 7,
        Kind::Instant {} => 8,
        Kind::Utf8 {} => 9,
        Kind::Binary {} => 10,
        Kind::List { .. } => 12,
        Kind::Struct { .. } => 13,
        _ => return None,
    })
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
    use crate::{OwnedSchema, Value};

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
