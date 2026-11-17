use serde::de::DeserializeOwned;
use super::invalid;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Token { Object, EndObject, Array, EndArray, Field, Scalar, Null }
#[derive(Clone, Copy)]
enum State { Field { end: bool }, Value, Element { end: bool }, ObjectSeparator, ArraySeparator }
struct Frame { state: State, field: Option<String> }
enum Scalar { Text(String), Number(String) }

pub(super) struct Reader<'a> {
    text: &'a str,
    offset: usize,
    frames: Vec<Frame>,
    current: Token,
    field: Option<String>,
    value: Option<Scalar>,
}

impl<'a> Reader<'a> {
    pub fn new(text: &'a str) -> Self {
        Self { text, offset: 0, frames: Vec::new(), current: Token::Null, field: None, value: None }
    }

    fn whitespace(&mut self) {
        while self.text.as_bytes().get(self.offset).is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r')) { self.offset += 1; }
    }

    fn byte(&mut self, expected: u8) -> azure_core::Result<()> {
        self.whitespace();
        if self.text.as_bytes().get(self.offset) != Some(&expected) { return Err(invalid("Azure credential JSON has an invalid token")); }
        self.offset += 1;
        Ok(())
    }

    fn decode<T: DeserializeOwned>(&mut self) -> azure_core::Result<T> {
        let mut stream = serde_json::Deserializer::from_str(&self.text[self.offset..]).into_iter::<T>();
        let value = stream.next().transpose().map_err(|_| invalid("Azure credential JSON has an invalid value"))?
            .ok_or_else(|| invalid("Azure credential JSON has no value"))?;
        self.offset += stream.byte_offset();
        Ok(value)
    }

    pub fn next(&mut self) -> azure_core::Result<Token> {
        self.value = None;
        loop {
            self.whitespace();
            let byte = self.text.as_bytes().get(self.offset).copied().ok_or_else(|| invalid("Azure credential JSON ended before its value"))?;
            let token = match self.frames.last().map(|frame| frame.state) {
                Some(State::Field { end }) if byte == b'}' && end => {
                    self.offset += 1;
                    self.frames.pop();
                    self.field = self.frames.last().and_then(|frame| frame.field.clone());
                    Token::EndObject
                }
                Some(State::Field { .. }) => {
                    let field = self.decode::<String>()?;
                    self.byte(b':')?;
                    let frame = self.frames.last_mut().expect("object frame");
                    frame.state = State::Value;
                    frame.field = Some(field.clone());
                    self.field = Some(field.clone());
                    self.value = Some(Scalar::Text(field));
                    Token::Field
                }
                Some(State::ObjectSeparator) if byte != b'}' => {
                    self.byte(b',')?;
                    self.frames.last_mut().expect("object frame").state = State::Field { end: false };
                    continue;
                }
                Some(State::ObjectSeparator) => {
                    self.frames.last_mut().expect("object frame").state = State::Field { end: true };
                    continue;
                }
                Some(State::ArraySeparator) => {
                    if byte == b']' { self.frames.last_mut().expect("array frame").state = State::Element { end: true }; }
                    else { self.byte(b',')?; self.frames.last_mut().expect("array frame").state = State::Element { end: false }; }
                    continue;
                }
                Some(State::Element { end }) if byte == b']' && end => {
                    self.offset += 1;
                    self.frames.pop();
                    self.field = self.frames.last().and_then(|frame| frame.field.clone());
                    Token::EndArray
                }
                _ => {
                    self.field = self.frames.last().and_then(|frame| frame.field.clone());
                    if let Some(frame) = self.frames.last_mut() {
                        frame.state = match frame.state { State::Value => State::ObjectSeparator, _ => State::ArraySeparator };
                    }
                    match byte {
                        b'{' | b'[' => {
                            if self.frames.len() >= 128 { return Err(invalid("Azure credential JSON nesting exceeds its limit")); }
                            self.offset += 1;
                            self.frames.push(Frame { state: if byte == b'{' { State::Field { end: true } } else { State::Element { end: true } }, field: None });
                            if byte == b'{' { Token::Object } else { Token::Array }
                        }
                        b'"' => { self.value = Some(Scalar::Text(self.decode::<String>()?)); Token::Scalar }
                        _ => {
                            let raw = self.decode::<Box<serde_json::value::RawValue>>()?;
                            if raw.get() == "null" { Token::Null } else {
                                self.value = Some(if matches!(byte, b'-' | b'0'..=b'9') { Scalar::Number(raw.get().to_owned()) } else { Scalar::Text(raw.get().to_owned()) });
                                Token::Scalar
                            }
                        }
                    }
                }
            };
            self.current = token;
            return Ok(token);
        }
    }

    pub fn field(&self) -> Option<&str> { self.field.as_deref() }
    pub fn string(&self) -> Option<&str> { match &self.value { Some(Scalar::Text(value) | Scalar::Number(value)) => Some(value), None => None } }
    pub fn number(&self) -> Option<&str> { match &self.value { Some(Scalar::Number(value)) => Some(value), _ => None } }

    pub fn skip_children(&mut self) -> azure_core::Result<()> {
        if !matches!(self.current, Token::Object | Token::Array) { return Ok(()); }
        let mut depth = 1;
        while depth > 0 {
            match self.next()? { Token::Object | Token::Array => depth += 1, Token::EndObject | Token::EndArray => depth -= 1, _ => () }
        }
        Ok(())
    }
}
