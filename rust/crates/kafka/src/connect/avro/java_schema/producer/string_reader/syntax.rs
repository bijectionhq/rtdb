use super::*;

fn syntax(message: &'static str) -> Anomaly { error("avro-reader-json", message) }
fn capacity_bound<T>(capacity: usize) -> Result<usize> { add(mul(mul(capacity, size_of::<T>())?, 16)?, 256) }
fn push<T>(values: &mut Vec<T>, value: T, context: &mut ConversionContext) -> Result<()> {
    let next = growth(values)?;
    if let Some((_, bytes)) = next { context.reserve(bytes)?; }
    grow(values, next)?;
    values.push(value);
    Ok(())
}
pub(super) fn tree(session: &mut Session<'_>, intern: &mut dyn InternAuthority, context: &mut ConversionContext) -> Result<Option<JsonNode>> {
    let mut decoder = Decoder { session, intern, context };
    decoder.whitespace()?;
    if decoder.peek().is_none() { return Ok(None); }
    decoder.value(0).map(Some)
}
struct Decoder<'a, 'input> {
    session: &'a mut Session<'input>,
    intern: &'a mut dyn InternAuthority,
    context: &'a mut ConversionContext,
}
impl Decoder<'_, '_> {
    fn peek(&mut self) -> Option<u16> { self.session.input.as_mut().unwrap().peek() }
    fn next(&mut self) -> Option<u16> { self.session.input.as_mut().unwrap().next() }
    fn expect(&mut self, expected: u16) -> Result<()> {
        if self.next() == Some(expected) { Ok(()) } else { Err(syntax("Unexpected JSON character")) }
    }
    fn whitespace(&mut self) -> Result<()> {
        loop {
            match self.peek() {
                Some(9 | 10 | 13 | 32) => { self.next(); },
                Some(47) => {
                    self.next();
                    match self.next() {
                        Some(47) => {
                            while self.peek().is_some_and(|unit| unit != 10 && unit != 13) {
                                let unit = self.next().unwrap();
                                if unit < 32 && unit != 9 { return Err(syntax("Invalid control character in JSON comment")); }
                            }
                        }
                        Some(42) => {
                            let mut star = false;
                            loop {
                                let unit = self.next().ok_or_else(|| syntax("Unterminated JSON comment"))?;
                                if star && unit == 47 { break; }
                                if unit < 32 && !matches!(unit, 9 | 10 | 13) { return Err(syntax("Invalid control character in JSON comment")); }
                                star = unit == 42;
                            }
                        }
                        _ => return Err(syntax("Invalid JSON comment")),
                    }
                }
                Some(unit) if unit < 32 => return Err(syntax("Invalid JSON whitespace")),
                _ => return Ok(()),
            }
        }
    }
    fn value(&mut self, depth: usize) -> Result<JsonNode> {
        guard_depth(depth, "Schema Reader tree exceeds the native traversal depth limit of128")?;
        self.whitespace()?;
        match self.peek() {
            Some(34) => {
                let units = self.quoted()?;
                let text = Text::copy_units(&units, self.context)?;
                JsonNode::with(0, self.context, |_| Ok(JsonValue::Text(text)))
            }
            Some(123) => self.object(depth),
            Some(91) => self.array(depth),
            Some(116) => { self.word("true")?; JsonNode::with(0, self.context, |_| Ok(JsonValue::Bool(true))) },
            Some(102) => { self.word("false")?; JsonNode::with(0, self.context, |_| Ok(JsonValue::Bool(false))) },
            Some(110) => { self.word("null")?; JsonNode::with(0, self.context, |_| Ok(JsonValue::Null)) },
            Some(45 | 48..=57) => self.number(depth),
            _ => Err(syntax("Expected a JSON value")),
        }
    }
    fn quoted(&mut self) -> Result<Vec<u16>> {
        self.expect(34)?;
        let mut units = Vec::new();
        loop {
            let unit = self.next().ok_or_else(|| syntax("Unterminated JSON string"))?;
            if unit == 34 { return Ok(units); }
            let unit = if unit == 92 {
                match self.next().ok_or_else(|| syntax("Unterminated JSON escape"))? {
                    34 => 34, 47 => 47, 92 => 92, 98 => 8, 102 => 12, 110 => 10, 114 => 13, 116 => 9,
                    117 => {
                        let mut value = 0u16;
                        for _ in 0..4 {
                            let digit = match self.next() {
                                Some(unit @ 48..=57) => unit - 48,
                                Some(unit @ 65..=70) => unit - 65 + 10,
                                Some(unit @ 97..=102) => unit - 97 + 10,
                                _ => return Err(syntax("Invalid JSON Unicode escape")),
                            };
                            value = value * 16 + digit;
                        }
                        value
                    }
                    _ => return Err(syntax("Invalid JSON escape")),
                }
            } else {
                if unit < 32 { return Err(syntax("Unquoted JSON control character")); }
                unit
            };
            push(&mut units, unit, self.context)?;
        }
    }
    fn name(&mut self) -> Result<Text> {
        let units = self.quoted()?;
        let name = self.session.child.as_mut().unwrap().find(&units, self.intern, self.context).map_err(symbol_error)?;
        Text::copy(name.view(), self.context)
    }
    fn object(&mut self, depth: usize) -> Result<JsonNode> {
        self.expect(123)?;
        self.whitespace()?;
        let mut fields = Vec::new();
        if self.peek() == Some(125) { self.next(); }
        else {
            loop {
                if self.peek() != Some(34) { return Err(syntax("Expected a quoted JSON field name")); }
                let key = self.name()?;
                self.whitespace()?;
                self.expect(58)?;
                let value = self.value(depth + 1)?;
                push(&mut fields, (key, value), self.context)?;
                self.whitespace()?;
                match self.next() {
                    Some(125) => break,
                    Some(44) => self.whitespace()?,
                    _ => return Err(syntax("Expected a JSON object separator")),
                }
            }
        }
        let members = Members::with_context(capacity_bound::<(Text, JsonNode)>(fields.capacity())?, self.context, |_| Ok(fields))?;
        JsonNode::with(0, self.context, |_| Ok(JsonValue::Object(members)))
    }
    fn array(&mut self, depth: usize) -> Result<JsonNode> {
        self.expect(91)?;
        self.whitespace()?;
        let mut values = Vec::new();
        if self.peek() == Some(93) { self.next(); }
        else {
            loop {
                let value = self.value(depth + 1)?;
                push(&mut values, value, self.context)?;
                self.whitespace()?;
                match self.next() {
                    Some(93) => break,
                    Some(44) => (),
                    _ => return Err(syntax("Expected a JSON array separator")),
                }
            }
        }
        JsonNode::with(capacity_bound::<JsonNode>(values.capacity())?, self.context, |_| Ok(JsonValue::Array(values)))
    }
    fn word(&mut self, word: &str) -> Result<()> {
        for expected in word.encode_utf16() { self.expect(expected)?; }
        if self.peek().is_some_and(|unit| unit >= 48 && unit != 93 && unit != 125 && identifier::part(unit)) {
            return Err(syntax("Invalid JSON literal token"));
        }
        Ok(())
    }
    fn number(&mut self, depth: usize) -> Result<JsonNode> {
        let mut units = Vec::new();
        if self.peek() == Some(45) { self.take_number(&mut units)?; }
        match self.peek() {
            Some(48) => {
                self.take_number(&mut units)?;
                if self.peek().is_some_and(|unit| (48..=57).contains(&unit)) { return Err(syntax("Leading zero in JSON number")); }
            }
            Some(49..=57) => self.digits(&mut units)?,
            _ => return Err(syntax("Expected a JSON number digit")),
        }
        let mut floating = false;
        if self.peek() == Some(46) {
            floating = true;
            self.take_number(&mut units)?;
            self.digits(&mut units)?;
        }
        if matches!(self.peek(), Some(69 | 101)) {
            floating = true;
            self.take_number(&mut units)?;
            if matches!(self.peek(), Some(43 | 45)) { self.take_number(&mut units)?; }
            self.digits(&mut units)?;
        }
        if depth == 0 {
            match self.peek() {
                Some(9 | 10 | 32) => { self.next(); },
                Some(13) | None => (),
                _ => return Err(syntax("JSON root number requires separating whitespace")),
            }
        }
        let text = JavaText::from_utf16(&units);
        if floating {
            let number = input::double(text, self.context)?;
            JsonNode::with(0, self.context, |_| Ok(JsonValue::F64(number)))
        } else {
            self.context.reserve(add(units.len(), 256)?)?;
            let mut ascii = String::new();
            ascii.try_reserve_exact(units.len()).map_err(allocation)?;
            ascii.extend(units.iter().map(|unit| char::from(*unit as u8)));
            if let Ok(number) = ascii.parse::<i64>() {
                JsonNode::with(0, self.context, |_| Ok(match i32::try_from(number) {
                    Ok(number) => JsonValue::I32(number), Err(_) => JsonValue::I64(number),
                }))
            } else { JsonNode::integer_decimal(text, self.context) }
        }
    }
    fn take_number(&mut self, units: &mut Vec<u16>) -> Result<()> {
        if units.len() == 1000 {
            return Err(Anomaly::unsupported("rtdb/avro-reader-native-profile", "Schema number exceeds the native Reader token profile"));
        }
        let unit = self.next().ok_or_else(|| syntax("Unexpected end of JSON number"))?;
        push(units, unit, self.context)
    }
    fn digits(&mut self, units: &mut Vec<u16>) -> Result<()> {
        let start = units.len();
        while self.peek().is_some_and(|unit| (48..=57).contains(&unit)) { self.take_number(units)?; }
        if units.len() == start { return Err(syntax("Expected a JSON number digit")); }
        Ok(())
    }
}
