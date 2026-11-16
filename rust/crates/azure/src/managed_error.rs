use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};

enum Token { Object, EndObject, Array, EndArray, Field(String), Scalar(String), Null }

struct Tokens<'a>(&'a mut Vec<Token>);

impl<'de> DeserializeSeed<'de> for Tokens<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Tokens<'_> {
    type Value = ();
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("a managed identity error response") }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> { self.0.push(Token::Null); Ok(()) }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<(), E> { self.0.push(Token::Scalar(value.to_string())); Ok(()) }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<(), E> { self.0.push(Token::Scalar(value.to_string())); Ok(()) }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<(), E> { self.0.push(Token::Scalar(value.to_string())); Ok(()) }
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<(), E> { self.0.push(Token::Scalar(value.to_string())); Ok(()) }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<(), E> { self.0.push(Token::Scalar(value.to_owned())); Ok(()) }
    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<(), E> { self.0.push(Token::Scalar(value)); Ok(()) }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
        self.0.push(Token::Object);
        while let Some(field) = map.next_key::<String>()? {
            self.0.push(Token::Field(field));
            map.next_value_seed(Tokens(&mut *self.0))?;
        }
        self.0.push(Token::EndObject);
        Ok(())
    }
    fn visit_seq<S: SeqAccess<'de>>(self, mut sequence: S) -> Result<(), S::Error> {
        self.0.push(Token::Array);
        while sequence.next_element_seed(Tokens(&mut *self.0))?.is_some() {}
        self.0.push(Token::EndArray);
        Ok(())
    }
}

struct Reader { tokens: Vec<Token>, offset: usize, contexts: Vec<Option<usize>>, field: Option<usize> }

impl Reader {
    fn next(&mut self) -> Option<&Token> {
        let index = self.offset;
        let token = self.tokens.get(index)?;
        self.offset += 1;
        match token {
            Token::Field(_) => { *self.contexts.last_mut()? = Some(index); self.field = Some(index); }
            Token::Object | Token::Array => { self.field = self.contexts.last().copied().flatten(); self.contexts.push(None); }
            Token::EndObject | Token::EndArray => { self.contexts.pop(); self.field = self.contexts.last().copied().flatten(); }
            _ => self.field = self.contexts.last().copied().flatten(),
        }
        Some(token)
    }
    fn current(&self) -> Option<&Token> { self.offset.checked_sub(1).and_then(|index| self.tokens.get(index)) }
    fn field(&self) -> Option<&str> { match self.field.and_then(|index| self.tokens.get(index)) { Some(Token::Field(name)) => Some(name), _ => None } }
    fn text(&self) -> Option<String> { match self.current() { Some(Token::Scalar(value) | Token::Field(value)) => Some(value.clone()), _ => None } }
    fn skip_children(&mut self) -> Option<()> {
        if !matches!(self.current(), Some(Token::Object | Token::Array)) { return Some(()); }
        let mut depth = 1;
        while depth > 0 {
            match self.next()? { Token::Object | Token::Array => depth += 1, Token::EndObject | Token::EndArray => depth -= 1, _ => () }
        }
        Some(())
    }
}

#[derive(Default)]
struct ErrorMessage { message: Option<String>, correlation: Option<String>, error: Option<String>, description: Option<String> }

impl ErrorMessage {
    fn contains_unavailable(&self) -> bool {
        let fields = if self.message.as_ref().is_some_and(|message| !message.is_empty()) {
            [&self.message, &self.correlation]
        } else { [&self.error, &self.description] };
        fields.into_iter().any(|field| field.as_ref().is_some_and(|value| value.contains("Authentication unavailable")))
    }
}

fn read_object(reader: &mut Reader, nested: bool) -> Option<ErrorMessage> {
    let mut result = ErrorMessage::default();
    while !matches!(reader.next()?, Token::EndObject) {
        let field = match reader.field()? {
            "message" => 0, "correlationId" if !nested => 1, "error" if !nested => 2,
            "error_description" if !nested => 3, "code" if nested => 4, _ => 5,
        };
        reader.next()?;
        match field {
            0 => result.message = reader.text(),
            1 => result.correlation = reader.text(),
            2 if matches!(reader.current(), Some(Token::Object)) => {
                let child = read_object(reader, true)?;
                result.error = child.error;
                result.message = child.message;
            }
            2 | 4 => result.error = reader.text(),
            3 => result.description = reader.text(),
            _ => reader.skip_children()?,
        }
    }
    Some(result)
}

pub(crate) fn is_unavailable(bytes: &[u8], imds_unavailable_status: bool) -> bool {
    if bytes.iter().all(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r')) { return true; }
    let mut tokens = Vec::new();
    if Tokens(&mut tokens).deserialize(&mut serde_json::Deserializer::from_slice(bytes)).is_err() { return false; }
    let mut reader = Reader { tokens, offset: 0, contexts: Vec::new(), field: None };
    match reader.next() {
        Some(Token::Null) => true,
        Some(Token::Object) => read_object(&mut reader, false).is_some_and(|message| imds_unavailable_status || message.contains_unavailable()),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_error_envelopes_allow_fallback_for_every_source() {
        assert!(is_unavailable(b"null", false));
        assert!(is_unavailable(b"null", true));
        for body in [b"".as_slice(), b" \t\r\n"] {
            assert!(is_unavailable(body, false));
            assert!(is_unavailable(body, true));
        }
        assert!(!is_unavailable(b"\x0b", false));
        assert!(!is_unavailable(b"{}", false));
        assert!(is_unavailable(b"{}", true));
        for body in [b"invalid".as_slice(), b"[]", b"true", b"{\"error\":"] {
            assert!(!is_unavailable(body, true));
        }
    }

    #[test]
    fn only_rendered_error_fields_control_principal_fallback() {
        for body in [
            r#"{"message":"Authentication unavailable"}"#,
            r#"{"error":{"code":"failure","message":"Authentication unavailable"}}"#,
            r#"{"error":"Authentication unavailable"}"#,
            r#"{"message":"","error_description":"Authentication unavailable"}"#,
            r#"{"message":true,"correlationId":"Authentication unavailable"}"#,
        ] { assert!(is_unavailable(body.as_bytes(), false), "{body}"); }
        for body in [
            r#"{"ignored":"Authentication unavailable"}"#,
            r#"{"message":"authentication failed","error_description":"Authentication unavailable"}"#,
            r#"{"correlationId":"Authentication unavailable"}"#,
            r#"{"message":"Authentication unavailable","message":"authentication failed"}"#,
            r#"{"message":"Authentication unavailable","error":{"message":"authentication failed"}}"#,
        ] { assert!(!is_unavailable(body.as_bytes(), false), "{body}"); }
    }

    #[test]
    fn default_json_reader_permissiveness_preserves_nested_token_position() {
        assert!(is_unavailable(br#"{"message":{}}"#, true));
        assert!(is_unavailable(br#"{"message":{"message":"Authentication unavailable"}}"#, false));
        assert!(!is_unavailable(br#"{"message":[1]}"#, true));
        assert!(is_unavailable(br#"null trailing content is never read"#, false));
        assert!(!is_unavailable(br#"{"ignored":{"message":"Authentication unavailable"}}"#, false));
    }
}
