use rtdb_types::java_text::JavaString;
use super::{ConversionContext, RegistrySchema, Result, error, response::{self, Failure, Profile}};

#[derive(Clone, Copy)]
enum SchemaType { Missing, Null, Avro }

pub(super) fn decode(bytes: &[u8], context: &mut ConversionContext) -> Result<Option<RegistrySchema>> {
    let Some(schema_type) = common_utf8_schema_type(bytes) else { return Ok(None); };
    project(bytes, schema_type, Profile::Jackson221, context)
}

fn project(bytes: &[u8], presence: SchemaType, profile: Profile, context: &mut ConversionContext) -> Result<Option<RegistrySchema>> {
    let bean = match response::schema_string(bytes, profile, context) {
        Ok(Some(bean)) => bean,
        Ok(None) | Err(Failure::Decode { .. }) => return Ok(None),
        Err(Failure::Native(cause)) => return Err(cause),
    };
    let avro = match (&bean.schema_type, presence) {
        (None, SchemaType::Null) => None,
        (Some(text), SchemaType::Missing | SchemaType::Avro) if text.view().units().eq("AVRO".encode_utf16()) => Some(text),
        _ => return Ok(None),
    };
    if bean.references.is_some_and(|references| references.len() != 0)
        || bean.metadata.is_some() || bean.rule_set.is_some() || bean.schema_tags.is_some() || bean.max_id.is_some() {
        return Ok(None);
    }
    let Some(text) = bean.schema else { return Ok(None); };
    let Some(schema) = scalar_text(&text, context)? else { return Ok(None); };
    let schema_type = match (presence, avro) {
        (SchemaType::Avro, Some(text)) => {
            let Some(value) = scalar_text(text, context)? else { return Ok(None); };
            Some(value)
        }
        _ => None,
    };
    Ok(Some(RegistrySchema { schema, schema_type, references: Vec::new(), rule_set: None, metadata: None }))
}

fn scalar_text(value: &JavaString, context: &mut ConversionContext) -> Result<Option<String>> {
    let overflow = || error("allocation-overflow", "Registry schema text allocation exceeds addressable memory");
    let mut length = 0usize;
    for point in value.view().code_points() {
        let Some(ch) = char::from_u32(point) else { return Ok(None); };
        length = length.checked_add(ch.len_utf8()).ok_or_else(overflow)?;
    }
    context.reserve(length.checked_add(256).ok_or_else(overflow)?)?;
    let mut result = String::new();
    result.try_reserve_exact(length).map_err(|_| error("allocation-failed", "Registry schema text allocation failed"))?;
    for point in value.view().code_points() { result.push(char::from_u32(point).expect("validated Registry schema scalar")); }
    Ok(Some(result))
}

fn common_utf8_schema_type(bytes: &[u8]) -> Option<SchemaType> {
    let text = space(std::str::from_utf8(bytes).ok()?);
    let mut rest = space(text.strip_prefix('{')?);
    let mut schema_seen = false;
    let mut references_seen = false;
    let mut schema_type = SchemaType::Missing;
    loop {
        let (key, after_key) = rest.strip_prefix('"')?.split_once('"')?;
        let value = space(space(after_key).strip_prefix(':')?);
        rest = space(match key {
            "schema" if !schema_seen => {
                schema_seen = true;
                after_string(value)?
            }
            "schemaType" if matches!(schema_type, SchemaType::Missing) => {
                if let Some(suffix) = value.strip_prefix("null") {
                    schema_type = SchemaType::Null;
                    suffix
                } else {
                    schema_type = SchemaType::Avro;
                    value.strip_prefix("\"AVRO\"")?
                }
            }
            "references" if !references_seen => {
                references_seen = true;
                match value.strip_prefix("null") {
                    Some(suffix) => suffix,
                    None => space(value.strip_prefix('[')?).strip_prefix(']')?,
                }
            }
            _ => return None,
        });
        if let Some(trailing) = rest.strip_prefix('}') {
            return (schema_seen && space(trailing).is_empty()).then_some(schema_type);
        }
        rest = space(rest.strip_prefix(',')?);
    }
}

fn space(text: &str) -> &str { text.trim_start_matches([' ', '\t', '\r', '\n']) }

fn after_string(text: &str) -> Option<&str> {
    let content = text.strip_prefix('"')?;
    let mut escaped = false;
    for (index, byte) in content.bytes().enumerate() {
        if escaped {
            if !matches!(byte, b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') { return None; }
            escaped = false;
        } else {
            match byte {
                b'"' => return Some(&content[index + 1..]),
                b'\\' => escaped = true,
                0..=31 => return None,
                _ => {}
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use rtdb_columnar::MemoryBudget;
    use super::super::{json_admission, references};

    #[test]
    fn both_source_profiles_project_the_legacy_dto_for_common_utf8_envelopes() {
        for body in [
            r#"{"schema":"\"string\""}"#,
            r#"{"schemaType":null,"schema":"é😀\\\n\r\t\b\f\/","references":null}"#,
            "{\r\n \"references\" : [ \t ], \"schema\" : \"\", \"schemaType\" : \"AVRO\" } \n",
        ] {
            let memory = MemoryBudget::new(1 << 20);
            let mut context = ConversionContext::new(Arc::clone(&memory));
            let value = json_admission::parse(body.as_bytes(), &mut context).unwrap();
            references::admit_typed_references(&value, &mut context).unwrap();
            let legacy: RegistrySchema = serde_json::from_value(value).unwrap();
            let legacy = serde_json::to_value(legacy).unwrap();
            for profile in [Profile::Jackson216, Profile::Jackson221] {
                let schema_type = common_utf8_schema_type(body.as_bytes()).unwrap();
                let projected = project(body.as_bytes(), schema_type, profile, &mut context).unwrap().unwrap();
                assert_eq!(serde_json::to_value(projected).unwrap(), legacy);
            }
            drop(context);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn out_of_profile_does_not_allocate_or_interpret_the_legacy_body() {
        let memory = MemoryBudget::new(1 << 20);
        let mut context = ConversionContext::with_limit(Arc::clone(&memory), 0);
        for body in [
            b"".as_slice(),
            b"\xef\xbb\xbf{\"schema\":\"x\"}",
            b"{\"\xc3\xa9\":0,\"schema\":\"x\"}",
            b"{\"schema\":\"\xed\xa0\x80\"}",
            br#"null"#,
            br#""string""#,
            br#"{}"#,
            br#"{"schema":1}"#,
            br#"{"schema":"x","schema":"y"}"#,
            br#"{"schema":"x","schemaType":null,"schemaType":"AVRO"}"#,
            br#"{"schema":"x","references":[],"references":null}"#,
            br#"{"schema":"x","schemaType":"JSON"}"#,
            br#"{"schema":"x","references":[null]}"#,
            br#"{"schema":"x","metadata":null}"#,
            br#"{"schema":"x","maxId":4}"#,
            br#"{"schema":"x","unknown":1}"#,
            br#"{"sch\u0065ma":"x"}"#,
            br#"{"schema":"\uD800"}"#,
            br#"{"schema":"\u0061"}"#,
            br#"{"schema":"\x"}"#,
            br#"{"schema":"x",}"#,
            br#"{"schema":"x"} true"#,
            b"{\"schema\":\"x\n\"}",
        ] {
            assert!(decode(body, &mut context).unwrap().is_none(), "{body:?}");
        }
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn eligible_schema_id_decode_propagates_native_admission_refusal() {
        let memory = MemoryBudget::new(1 << 20);
        let mut context = ConversionContext::with_limit(Arc::clone(&memory), 0);
        let refusal = decode(br#"{"schema":"x"}"#, &mut context).err().unwrap();
        assert!(refusal.code.ends_with("/batch-too-large"));
        assert_eq!(memory.used_bytes(), 0);
    }
}
