use super::{JsonDecimal, JsonNumber, JsonObject, JsonValue, NumberMode, error};
use rtdb_types::Anomaly;
use serde_json::value::RawValue;
use std::collections::BTreeMap;

enum Rejected {
    Value(Anomaly),
    Key,
}

pub(super) fn parse(bytes: &[u8], mode: NumberMode) -> Result<rtdb_types::Result<JsonValue>, ()> {
    let mut parser = Parser { rest: bytes, mode };
    let value = parser.value(0)?;
    parser.skip_whitespace();
    if !parser.rest.is_empty() {
        return Err(());
    }
    match value {
        Ok(value) => Ok(Ok(value)),
        Err(Rejected::Value(cause)) if std::str::from_utf8(bytes).is_ok() => Ok(Err(cause)),
        Err(_) => Err(()),
    }
}

struct Parser<'a> {
    rest: &'a [u8],
    mode: NumberMode,
}

impl Parser<'_> {
    fn value(&mut self, depth: usize) -> Result<Result<JsonValue, Rejected>, ()> {
        self.skip_whitespace();
        if depth > 128 {
            self.skip_raw_token()?;
            return Ok(Err(Rejected::Value(error(
                "avro-schema",
                "Default nesting limit exceeded",
            ))));
        }
        Ok(match self.rest.first() {
            Some(b'n') => Ok(self.literal(b"null", JsonValue::Null)?),
            Some(b't') => Ok(self.literal(b"true", JsonValue::Boolean(true))?),
            Some(b'f') => Ok(self.literal(b"false", JsonValue::Boolean(false))?),
            Some(b'"') => self
                .string()?
                .map(JsonValue::String)
                .map_err(Rejected::Value),
            Some(b'[') => {
                self.rest = &self.rest[1..];
                let mut values = Ok(Vec::new());
                self.entries(b']', |parser| {
                    let value = parser.value(depth + 1)?;
                    if let Ok(list) = &mut values {
                        match value {
                            Ok(value) => list.push(value),
                            Err(cause) => values = Err(cause),
                        }
                    }
                    Ok(())
                })?;
                values.map(JsonValue::Array)
            }
            Some(b'{') => self.object(depth)?,
            Some(b'-' | b'0'..=b'9') => self.number()?,
            _ => return Err(()),
        })
    }

    fn object(&mut self, depth: usize) -> Result<Result<JsonValue, Rejected>, ()> {
        self.rest = &self.rest[1..];
        let mut object = JsonObject {
            fields: Vec::new(),
            positions: Default::default(),
        };
        let mut rejected = BTreeMap::new();
        let mut invalid_key = false;
        self.entries(b'}', |parser| {
            let key = parser.string()?;
            parser.skip_whitespace();
            if !parser.accept(b':') {
                return Err(());
            }
            let value = parser.value(depth + 1)?;
            if let Ok(key) = key {
                match value {
                    Ok(value) => {
                        let position = object.insert(key, value);
                        rejected.remove(&position);
                    }
                    Err(cause) => {
                        let position = object.insert(key, JsonValue::Null);
                        rejected.insert(position, cause);
                    }
                }
            } else {
                invalid_key = true;
            }
            Ok(())
        })?;
        // Object keys are decoded before the surviving values, even when a value appears first.
        if invalid_key {
            Ok(Err(Rejected::Key))
        } else if let Some((_, cause)) = rejected.pop_first() {
            Ok(Err(cause))
        } else {
            Ok(Ok(JsonValue::Object(object)))
        }
    }

    fn entries(
        &mut self,
        end: u8,
        mut read: impl FnMut(&mut Self) -> Result<(), ()>,
    ) -> Result<(), ()> {
        self.skip_whitespace();
        if self.accept(end) {
            return Ok(());
        }
        loop {
            read(self)?;
            self.skip_whitespace();
            if self.accept(end) {
                return Ok(());
            }
            if !self.accept(b',') {
                return Err(());
            }
            self.skip_whitespace();
        }
    }

    fn string(&mut self) -> Result<rtdb_types::Result<String>, ()> {
        if self.rest.first() != Some(&b'"') {
            return Err(());
        }
        let mut strings = serde_json::Deserializer::from_slice(self.rest).into_iter::<String>();
        match strings.next().ok_or(())? {
            Ok(value) => {
                self.rest = &self.rest[strings.byte_offset()..];
                Ok(Ok(value))
            }
            Err(cause) => {
                self.skip_raw_token()?;
                Ok(Err(error("conversion-error", cause)))
            }
        }
    }

    fn skip_raw_token(&mut self) -> Result<(), ()> {
        let mut tokens = serde_json::Deserializer::from_slice(self.rest).into_iter::<&RawValue>();
        tokens.next().ok_or(())?.map_err(|_| ())?;
        self.rest = &self.rest[tokens.byte_offset()..];
        Ok(())
    }

    fn literal(&mut self, text: &[u8], value: JsonValue) -> Result<JsonValue, ()> {
        self.rest = self.rest.strip_prefix(text).ok_or(())?;
        Ok(value)
    }

    fn number(&mut self) -> Result<Result<JsonValue, Rejected>, ()> {
        let start = self.rest;
        self.accept(b'-');
        if !self.accept(b'0') {
            self.digits()?;
        }
        let fraction = self.accept(b'.');
        if fraction {
            self.digits()?;
        }
        let exponent = self.accept(b'e') || self.accept(b'E');
        if exponent {
            if !self.accept(b'+') {
                self.accept(b'-');
            }
            self.digits()?;
        }
        let text =
            std::str::from_utf8(&start[..start.len() - self.rest.len()]).expect("ASCII number");
        if matches!(self.mode, NumberMode::Decimal) && (fraction || exponent) {
            Ok(JsonDecimal::parse(text)
                .map(JsonValue::Decimal)
                .map_err(Rejected::Value))
        } else {
            Ok(Ok(JsonValue::Number(JsonNumber::parse(text))))
        }
    }

    fn digits(&mut self) -> Result<(), ()> {
        let length = self
            .rest
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if length == 0 {
            return Err(());
        }
        self.rest = &self.rest[length..];
        Ok(())
    }

    fn accept(&mut self, expected: u8) -> bool {
        if let Some(rest) = self.rest.strip_prefix(&[expected]) {
            self.rest = rest;
            true
        } else {
            false
        }
    }

    fn skip_whitespace(&mut self) {
        let length = self
            .rest
            .iter()
            .take_while(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
            .count();
        self.rest = &self.rest[length..];
    }
}
