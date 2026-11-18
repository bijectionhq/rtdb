use rtdb_types::java_text::JavaString;
use super::{ConversionContext, Failure, FailureKind, Frame, Kind, Profile, Result, TextBufferObservation, Token, Usage,
    copy_string, input::{Encoding, Input, Utf8Demand}, scan_name, text_buffer::TextBufferDemand, utf8_unit_bytes};
use super::super::{owned::Values, symbols::{ChildSymbols, InternAuthority}, window::{LexicalObservations, NameObservation, WindowInput, symbol_failure, unavailable}};

struct Lookahead { value: Option<u16>, before: usize, after: usize }
struct WindowCursor<'a> { source: WindowInput<'a>, lookahead: Option<Lookahead>, at: usize }
impl<'a> WindowCursor<'a> {
    fn new(source: WindowInput<'a>) -> Self { Self { source, lookahead: None, at: 0 } }
    fn initialize(&mut self) -> Result<()> { self.at = self.source.position()?; Ok(()) }
    fn peek(&mut self) -> Result<Option<u16>> {
        if self.lookahead.is_none() {
            let before = self.at;
            let value = self.source.next()?;
            let after = self.source.position()?;
            self.lookahead = Some(Lookahead { value, before, after });
        }
        Ok(self.lookahead.as_ref().unwrap().value)
    }
    fn next(&mut self) -> Result<Option<u16>> {
        if let Some(value) = self.lookahead.take() {
            self.at = if value.value.is_some() { value.after } else { value.before };
            return Ok(value.value);
        }
        let value = self.source.next()?;
        self.at = self.source.position()?;
        Ok(value)
    }
    fn utf8_character(&mut self, first: u16, profile: Profile, demand: Utf8Demand) -> Result<(u16, Option<u16>)> {
        if !self.source.is_utf8() || first < 0x80 { return Ok((first, None)); }
        let (count, mut point) = if first & 0xe0 == 0xc0 { (1, i32::from(first & 0x1f)) }
            else if first & 0xf0 == 0xe0 { (2, i32::from(first & 0x0f)) }
            else if first & 0xf8 == 0xf0 { (3, i32::from(first & 7)) }
            else { return Err(Failure::at(FailureKind::Syntax, self.at.saturating_sub(1), "Invalid UTF8 initial byte")); };
        for _ in 0..count {
            let at = self.at;
            let next = self.next()?.ok_or_else(|| Failure::at(FailureKind::Syntax, at, "Truncated UTF8 character"))?;
            if next & 0xc0 != 0x80 { return Err(Failure::at(FailureKind::Syntax, at, "Invalid UTF8 continuation byte")); }
            point = (point << 6) | i32::from(next & 0x3f);
        }
        if count == 2 && !matches!(demand, Utf8Demand::Skip) && profile == Profile::Jackson221 && (0xd800..=0xdfff).contains(&point) {
            return Err(Failure::at(FailureKind::Syntax, self.at - 3, "Illegal UTF8 surrogate character"));
        }
        if count == 3 {
            let point = point - 0x10000;
            let high = match demand { Utf8Demand::Value => 0xd800 | (point >> 10), _ => 0xd800 + (point >> 10) };
            Ok((high as u16, Some(0xdc00 | (point & 0x3ff) as u16)))
        } else { Ok((point as u16, None)) }
    }
}

struct Spelling { units: [u16; 1008], len: usize }
impl Spelling {
    fn new() -> Self { Self { units: [0; 1008], len: 0 } }
    fn push(&mut self, unit: u16) { if self.len < self.units.len() { self.units[self.len] = unit; self.len += 1; } }
}

enum NameAuthority<'a> {
    Observed { names: &'a [NameObservation<'a>], next: usize },
    Reader { child: ChildSymbols, intern: &'a mut dyn InternAuthority },
    Closed,
}

pub(in crate::connect::registry::response) struct ObservedCursor<'a> {
    input: WindowCursor<'a>, profile: Profile, frames: Values<Frame>, token: Option<Token>,
    spelling: Spelling, names: NameAuthority<'a>, text_buffers: &'a [TextBufferObservation],
}
impl<'a> ObservedCursor<'a> {
    pub(super) fn new(input: WindowInput<'a>, profile: Profile, observations: LexicalObservations<'a>) -> Self {
        Self { input: WindowCursor::new(input), profile, frames: Values::new(), token: None,
            spelling: Spelling::new(), names: NameAuthority::Observed { names: observations.names, next: 0 }, text_buffers: observations.text_buffers }
    }
    pub(super) fn reader(input: WindowInput<'a>, child: ChildSymbols, authority: &'a mut dyn InternAuthority) -> Self {
        let profile = child.factory().profile;
        Self { input: WindowCursor::new(input), profile, frames: Values::new(), token: None,
            spelling: Spelling::new(), names: NameAuthority::Reader { child, intern: authority }, text_buffers: &[] }
    }
    pub(super) fn start(&mut self) -> Result<()> {
        if matches!(&self.names, NameAuthority::Reader { .. }) && self.input.source.is_utf8() {
            return Err(unavailable("Reader symbols cannot qualify a UTF8 byte parser"));
        }
        self.input.source.validate_profile(self.profile)?; self.input.initialize()?; self.token = Some(self.lex(true)?); Ok(())
    }
    pub(super) fn offset(&self) -> usize { self.token.map_or(self.input.at, |token| token.start) }
    pub(super) fn profile(&self) -> Profile { self.profile }
    pub(super) fn kind(&mut self, context: &mut ConversionContext) -> Result<Kind> {
        let token = self.token.as_mut().ok_or_else(|| Failure::at(FailureKind::Syntax, self.input.at, "Expected a response value"))?;
        if !token.active {
            if matches!(token.kind, Kind::Object | Kind::Array) {
                if self.frames.len() >= 1000 { return Err(Failure::at(FailureKind::Constraint, token.start, "JSON nesting depth exceeds 1000")); }
                self.frames.push(Frame { kind: token.kind, first: true }, context)?;
            }
            token.active = true;
        }
        Ok(token.kind)
    }
    pub(super) fn begin_object(&mut self, context: &mut ConversionContext) -> Result<()> {
        if self.kind(context)? != Kind::Object { return Err(self.coercion("Expected a Registry bean object")); }
        self.token = None; Ok(())
    }
    pub(super) fn begin_array(&mut self, context: &mut ConversionContext) -> Result<()> {
        if self.kind(context)? != Kind::Array { return Err(self.coercion("Expected a collection array")); }
        self.token = None; Ok(())
    }
    pub(super) fn field(&mut self, context: &mut ConversionContext) -> Result<Option<JavaString>> {
        self.discard_scalar()?;
        let first = self.frames.as_slice().last().filter(|frame| frame.kind == Kind::Object)
            .ok_or_else(|| self.syntax("Expected an object context"))?.first;
        self.space()?;
        if self.input.peek()? == Some(0x7d) { self.input.next()?; self.frames.pop(); return Ok(None); }
        if !first { self.expect(0x2c, "Expected a comma between object fields")?; self.space()?; }
        let start = self.input.at;
        let quote = self.input.next()?;
        if quote != Some(0x22) {
            if let Some(first) = quote { self.input.utf8_character(first, self.profile, Utf8Demand::Skip)?; }
            return Err(self.syntax("Expected a quoted object field name"));
        }
        let name = self.name(context)?;
        if let NameAuthority::Observed { names, next } = &mut self.names {
            let observed = names.get(*next).ok_or_else(|| unavailable("Field name requires an observed successful canonicalizer result"))?;
            if observed.start != start || !name.view().units().eq(observed.units.iter().copied()) {
                return Err(unavailable("Observed canonicalizer position or UTF16 result differs from the consumed field name"));
            }
            *next += 1;
        }
        self.space()?;
        self.expect(0x3a, "Expected a colon after the field name")?;
        self.frames.as_mut_slice().last_mut().unwrap().first = false;
        self.token = Some(self.lex(false)?);
        Ok(Some(name))
    }
    pub(super) fn element(&mut self) -> Result<bool> {
        self.discard_scalar()?;
        let first = self.frames.as_slice().last().filter(|frame| frame.kind == Kind::Array)
            .ok_or_else(|| self.syntax("Expected an array context"))?.first;
        self.space()?;
        if self.input.peek()? == Some(0x5d) { self.input.next()?; self.frames.pop(); return Ok(false); }
        if !first { self.expect(0x2c, "Expected a comma between array elements")?; }
        self.frames.as_mut_slice().last_mut().unwrap().first = false;
        self.token = Some(self.lex(false)?); Ok(true)
    }
    pub(super) fn take_null(&mut self, context: &mut ConversionContext) -> Result<bool> {
        if self.kind(context)? == Kind::Null { self.token = None; Ok(true) } else { Ok(false) }
    }
    pub(super) fn string(&mut self, context: &mut ConversionContext) -> Result<JavaString> {
        if self.kind(context)? != Kind::String { return Err(self.coercion("Expected a string token")); }
        let start = self.token.unwrap().start;
        let capacity = if self.input.source.is_utf8() {
            self.text_buffers.iter().find(|observation| observation.value_start == start).map(|observation| observation.capacity_units)
        } else { None };
        let mut units = Values::new();
        self.string_units(Usage::Value, capacity, Some((&mut units, context)))?;
        let value = JavaString::copy_utf16_with_admission(units.as_slice(), |bytes| context.reserve(bytes))?;
        self.token = None; Ok(value)
    }
    pub(super) fn take_boolean_token(&mut self, context: &mut ConversionContext) -> Result<Option<bool>> {
        let result = match self.kind(context)? { Kind::True => Some(true), Kind::False => Some(false), _ => None };
        if result.is_some() { self.token = None; } Ok(result)
    }
    pub(super) fn number_text(&mut self, context: &mut ConversionContext) -> Result<JavaString> {
        if !matches!(self.kind(context)?, Kind::Integer | Kind::Float) { return Err(self.coercion("Expected a numeric token")); }
        let value = JavaString::copy_utf16_with_admission(&self.spelling.units[..self.spelling.len], |bytes| context.reserve(bytes))?;
        self.token = None; Ok(value)
    }
    pub(super) fn skip(&mut self, context: &mut ConversionContext) -> Result<()> {
        let already_open = self.token.is_some_and(|token| token.active && matches!(token.kind, Kind::Object | Kind::Array));
        let before = self.frames.len() - usize::from(already_open);
        if !matches!(self.kind(context)?, Kind::Object | Kind::Array) { return Ok(()); }
        self.token = None;
        while self.frames.len() > before {
            let more = match self.frames.as_slice().last().unwrap().kind {
                Kind::Object => self.field(context)?.is_some(), Kind::Array => self.element()?, _ => unreachable!(),
            };
            if more && matches!(self.kind(context)?, Kind::Object | Kind::Array) { self.token = None; }
        }
        Ok(())
    }
    fn discard_scalar(&mut self) -> Result<()> {
        if let Some(token) = self.token.take() {
            if token.kind == Kind::String { self.string_units(Usage::Skip, None, None)?; }
            else if matches!(token.kind, Kind::Object | Kind::Array) { return Err(self.syntax("Container value was not consumed")); }
        }
        Ok(())
    }
    fn name(&mut self, context: &mut ConversionContext) -> Result<JavaString> {
        if !self.input.source.is_utf8() {
            let mut units = Values::new();
            self.string_units(Usage::Name, None, Some((&mut units, context)))?;
            let offset = self.input.at;
            return match &mut self.names {
                NameAuthority::Observed { .. } => Ok(JavaString::copy_utf16_with_admission(units.as_slice(), |bytes| context.reserve(bytes))?),
                NameAuthority::Reader { child, intern } => child.find(units.as_slice(), &mut **intern, context).map_err(|cause| symbol_failure(cause, offset)),
                NameAuthority::Closed => Err(unavailable("Reader name authority has already been closed")),
            };
        }
        let mut raw = Values::new();
        let mut length = 0usize;
        loop {
            let at = self.input.at;
            let unit = self.name_byte(&mut raw, context)?.ok_or_else(|| self.syntax("Unterminated field name"))?;
            if unit == 0x22 { break; }
            if unit < 0x20 { return Err(self.syntax("Unescaped control character in field name")); }
            if unit == 0x5c {
                let unit = self.name_escape(&mut raw, context)?;
                if self.profile == Profile::Jackson221 && (0xd800..=0xdfff).contains(&unit) {
                    if unit >= 0xdc00 || self.name_byte(&mut raw, context)? != Some(0x5c) {
                        return Err(self.syntax("Expected an escaped surrogate pair in field name"));
                    }
                    let low = self.name_escape(&mut raw, context)?;
                    if !(0xdc00..=0xdfff).contains(&low) { return Err(self.syntax("Invalid escaped low surrogate in field name")); }
                    length += 4;
                } else { length += utf8_unit_bytes(unit); }
            } else { length += 1; }
            if length > 65_540 { return Err(Failure::at(FailureKind::Constraint, at, "JSON field name decode buffer exceeds allowed length")); }
        }
        if length > 50_000 { return Err(Failure::at(FailureKind::Constraint, self.input.at, "JSON field name length exceeds 50000")); }
        let mut input = Input::slice(raw.as_slice(), Encoding::Utf8);
        if scan_name(&mut input, self.profile).is_err() {
            return Err(unavailable("UTF8 name decoding failure requires the actual canonicalizer lookup outcome"));
        }
        copy_string(Input::slice(raw.as_slice(), Encoding::Utf8), self.profile, Usage::Name, context)
    }
    fn name_byte(&mut self, raw: &mut Values<u8>, context: &mut ConversionContext) -> Result<Option<u16>> {
        let value = self.input.next()?;
        if let Some(unit) = value { raw.push(unit as u8, context)?; }
        Ok(value)
    }
    fn name_escape(&mut self, raw: &mut Values<u8>, context: &mut ConversionContext) -> Result<u16> {
        let unit = self.name_byte(raw, context)?;
        match unit {
            Some(0x22) => Ok(0x22), Some(0x5c) => Ok(0x5c), Some(0x2f) => Ok(0x2f),
            Some(0x62) => Ok(8), Some(0x66) => Ok(12), Some(0x6e) => Ok(10), Some(0x72) => Ok(13), Some(0x74) => Ok(9),
            Some(0x75) => {
                let mut value = 0;
                for _ in 0..4 {
                    let unit = self.name_byte(raw, context)?;
                    value = (value << 4) | hex(unit).ok_or_else(|| self.syntax("Invalid JSON Unicode escape"))?;
                }
                Ok(value)
            }
            Some(first) => { self.input.utf8_character(first, self.profile, Utf8Demand::Skip)?; Err(self.syntax("Invalid JSON string escape")) }
            None => Err(self.syntax("Invalid JSON string escape")),
        }
    }
    fn string_units(&mut self, usage: Usage, capacity: Option<usize>, mut output: Option<(&mut Values<u16>, &mut ConversionContext)>) -> Result<()> {
        let mut units = 0usize;
        let mut buffer = if usage == Usage::Value { Some(TextBufferDemand::new(capacity, self.input.at)?) } else { None };
        loop {
            let at = self.input.at;
            let first = self.input.next()?.ok_or_else(|| self.syntax("Unterminated JSON string"))?;
            if let Some(buffer) = &mut buffer { buffer.before_character(at)?; }
            if first == 0x22 {
                if usage == Usage::Name && units > 50_000 { return Err(Failure::at(FailureKind::Constraint, at, "JSON field name length exceeds 50000")); }
                if let Some(buffer) = &buffer { buffer.complete(at)?; }
                return Ok(());
            }
            let (unit, second) = if first == 0x5c { (self.escape()?, None) }
                else {
                    if first < 0x20 { return Err(self.syntax("Unescaped control character in JSON string")); }
                    let demand = match usage { Usage::Name => Utf8Demand::Name, Usage::Value => Utf8Demand::Value, Usage::Skip => Utf8Demand::Skip };
                    self.input.utf8_character(first, self.profile, demand)?
                };
            if let Some((values, context)) = &mut output { values.push(unit, context)?; }
            units += 1;
            if usage == Usage::Name && units > 50_000 { return Err(unavailable("Over-limit Reader names require the source shared-buffer and text-segment branches")); }
            if let Some(buffer) = &mut buffer { buffer.unit(at)?; }
            if let Some(second) = second {
                if let Some(buffer) = &mut buffer { buffer.before_character(at)?; }
                if let Some((values, context)) = &mut output { values.push(second, context)?; }
                units += 1;
                if let Some(buffer) = &mut buffer { buffer.unit(at)?; }
            }
        }
    }
    fn escape(&mut self) -> Result<u16> {
        match self.input.next()? {
            Some(0x22) => Ok(0x22), Some(0x5c) => Ok(0x5c), Some(0x2f) => Ok(0x2f),
            Some(0x62) => Ok(8), Some(0x66) => Ok(12), Some(0x6e) => Ok(10), Some(0x72) => Ok(13), Some(0x74) => Ok(9),
            Some(0x75) => {
                let mut value = 0;
                for _ in 0..4 { value = (value << 4) | hex(self.input.next()?).ok_or_else(|| self.syntax("Invalid JSON Unicode escape"))?; }
                Ok(value)
            }
            Some(first) => { self.input.utf8_character(first, self.profile, Utf8Demand::Skip)?; Err(self.syntax("Invalid JSON string escape")) }
            None => Err(self.syntax("Invalid JSON string escape")),
        }
    }
    fn space(&mut self) -> Result<()> {
        while self.input.peek()?.is_some_and(|unit| matches!(unit, 0x20 | 0x09 | 0x0a | 0x0d)) { self.input.next()?; }
        Ok(())
    }
    fn expect(&mut self, expected: u16, detail: &'static str) -> Result<()> {
        if self.input.next()? != Some(expected) { return Err(self.syntax(detail)); } Ok(())
    }
    fn lex(&mut self, root: bool) -> Result<Token> {
        self.space()?;
        let start = self.input.at;
        let first = self.input.next()?.ok_or_else(|| self.syntax("Unexpected end of JSON input"))?;
        let start_processed = self.input.source.processed();
        self.spelling.len = 0; self.spelling.push(first);
        let kind = match first {
            0x7b => Kind::Object, 0x5b => Kind::Array, 0x22 => Kind::String,
            0x74 => { self.literal("rue")?; Kind::True }
            0x66 => { self.literal("alse")?; Kind::False }
            0x6e => { self.literal("ull")?; Kind::Null }
            0x4e => { self.literal("aN")?; Kind::Float }
            0x49 => { self.literal("nfinity")?; Kind::Float }
            0x2b | 0x2d if self.input.peek()? == Some(0x49) => {
                self.spelled_next()?;
                match self.spelled_next()? {
                    Some(0x4e) => self.literal("F")?, Some(0x6e) => self.literal("finity")?,
                    _ => return Err(unavailable("Invalid nonnumeric token requires source invalid-token error scanning")),
                }
                Kind::Float
            }
            0x2d | 0x30..=0x39 => self.numeric(first, root, start_processed)?,
            _ => return Err(unavailable("Unexpected value requires the source error-token scanner")),
        };
        Ok(Token { kind, start, end: self.input.at, active: false })
    }
    fn literal(&mut self, rest: &str) -> Result<()> {
        for expected in rest.bytes() {
            if self.spelled_next()? != Some(u16::from(expected)) {
                return Err(unavailable("Invalid literal requires source invalid-token error scanning"));
            }
        }
        if let Some(unit) = self.input.peek()? {
            if unit >= 0x30 && unit != 0x5d && unit != 0x7d {
                if self.input.source.is_utf8() && unit >= 0x80 {
                    if self.profile == Profile::Jackson216 {
                        self.input.utf8_character(unit, self.profile, Utf8Demand::Skip)?;
                    }
                    return Err(unavailable("Non-ASCII literal suffix requires the source error decoder and same-window rewind"));
                }
                let flags = include_bytes!("identifier-parts.bin");
                if flags[usize::from(unit) >> 3] & (1 << (unit & 7)) != 0 {
                    return Err(unavailable("Identifier literal suffix requires source invalid-token error scanning"));
                }
            }
        }
        Ok(())
    }
    fn spelled_next(&mut self) -> Result<Option<u16>> { let value = self.input.next()?; if let Some(unit) = value { self.spelling.push(unit); } Ok(value) }
    fn numeric(&mut self, first: u16, root: bool, start_processed: i64) -> Result<Kind> {
        let first = if first == 0x2d { self.spelled_next()?.ok_or_else(|| self.syntax("Expected a digit after minus"))? } else { first };
        if !matches!(first, 0x30..=0x39) { return Err(unavailable("Invalid signed number requires the source unexpected-number scanner")); }
        let mut digits = 1usize;
        if first == 0x30 && self.input.peek()?.is_some_and(|unit| matches!(unit, 0x30..=0x39)) { return Err(self.syntax("Leading zeroes are disabled")); }
        while self.input.peek()?.is_some_and(|unit| matches!(unit, 0x30..=0x39)) { self.spelled_next()?; digits += 1; }
        let mut kind = Kind::Integer;
        let fraction_present = self.input.peek()? == Some(0x2e);
        if fraction_present {
            self.spelled_next()?; kind = Kind::Float;
            let mut fraction = 0usize;
            while self.input.peek()?.is_some_and(|unit| matches!(unit, 0x30..=0x39)) { self.spelled_next()?; digits += 1; fraction += 1; }
            if fraction == 0 { self.input.next()?; return Err(self.syntax("Expected a digit after decimal point")); }
        }
        let exponent_present = self.input.peek()?.is_some_and(|unit| matches!(unit, 0x65 | 0x45));
        if exponent_present {
            self.spelled_next()?; kind = Kind::Float;
            if self.input.peek()?.is_some_and(|unit| matches!(unit, 0x2b | 0x2d)) { self.spelled_next()?; }
            let mut exponent = 0usize;
            while self.input.peek()?.is_some_and(|unit| matches!(unit, 0x30..=0x39)) { self.spelled_next()?; digits += 1; exponent += 1; }
            if exponent == 0 { self.input.next()?; return Err(self.syntax("Expected an exponent digit")); }
        }
        if root {
            if let Some(unit) = self.input.peek()? {
                if !matches!(unit, 0x20 | 0x09 | 0x0a | 0x0d) { self.input.next()?; return Err(self.syntax("Expected whitespace after a root number")); }
                if unit != 0x0d { self.input.next()?; }
            }
        }
        let reader_slow = !self.input.source.is_utf8() && (first == 0x30 || self.input.source.processed() != start_processed);
        let measured = if kind == Kind::Float && reader_slow { digits - usize::from(!fraction_present) - usize::from(!exponent_present) } else { digits };
        if measured > 1000 { return Err(Failure::at(FailureKind::Constraint, self.input.at, "JSON number length exceeds 1000")); }
        Ok(kind)
    }
    fn syntax(&self, detail: &'static str) -> Failure { Failure::at(FailureKind::Syntax, self.input.at, detail) }
    fn coercion(&self, detail: &'static str) -> Failure { Failure::at(FailureKind::Coercion, self.offset(), detail) }
    pub(super) fn close(&mut self, context: &mut ConversionContext) -> Result<()> {
        if matches!(&self.names, NameAuthority::Closed) { return Ok(()); }
        if matches!(&self.names, NameAuthority::Observed { .. }) { return self.input.source.close(); }
        let input_close = self.input.source.close_input_stage();
        let NameAuthority::Reader { child, .. } = std::mem::replace(&mut self.names, NameAuthority::Closed) else { unreachable!() };
        let symbol_close = child.release(context).map(|_| ()).map_err(|cause| symbol_failure(cause, self.input.at));
        if symbol_close.is_ok() { self.input.source.release_parser_buffer(); }
        symbol_close.and(input_close)
    }
    pub(super) fn observations_consumed(&self) -> usize { self.input.source.observations_consumed() }
}

fn hex(unit: Option<u16>) -> Option<u16> {
    match unit { Some(unit @ 0x30..=0x39) => Some(unit - 0x30), Some(unit @ 0x61..=0x66) => Some(unit - 0x61 + 10),
        Some(unit @ 0x41..=0x46) => Some(unit - 0x41 + 10), _ => None }
}
