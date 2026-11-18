use rtdb_types::java_text::JavaString;
use super::{ConversionContext, Failure, FailureKind, Profile, Result, TextBufferObservation, owned::Values, overflow};

mod input;
mod text_buffer;
use input::{Encoding, Input, Utf8Demand};
use text_buffer::TextBufferDemand;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind { Object, Array, String, Integer, Float, True, False, Null }
#[derive(Clone, Copy)]
struct Frame { kind: Kind, first: bool }
#[derive(Clone, Copy)]
struct Token { kind: Kind, start: usize, end: usize, active: bool }

#[derive(Clone, Copy)]
pub(super) struct SliceName<'a> { input: Input<'a>, profile: Profile }
impl SliceName<'_> {
    pub(super) fn is(&self, expected: &str) -> Result<bool> {
        let mut input = self.input;
        let mut units = expected.encode_utf16();
        let mut equal = true;
        scan_string(&mut input, self.profile, Usage::Name, |unit| { equal &= units.next() == Some(unit); })?;
        Ok(equal && units.next().is_none())
    }
    pub(super) fn copy(&self, context: &mut ConversionContext) -> Result<JavaString> {
        copy_string(self.input, self.profile, Usage::Name, context)
    }
}

pub(super) struct SliceCursor<'a> {
    input: Input<'a>, profile: Profile, frames: Values<Frame>, token: Option<Token>, text_buffers: &'a [TextBufferObservation],
}
impl<'a> SliceCursor<'a> {
    pub(super) fn new(bytes: &'a [u8], profile: Profile) -> Result<Self> {
        Self::with_text_buffers(bytes, profile, &[])
    }
    pub(super) fn with_text_buffers(bytes: &'a [u8], profile: Profile, text_buffers: &'a [TextBufferObservation]) -> Result<Self> {
        let mut value = Self { input: Input::bootstrap(bytes)?, profile, frames: Values::new(), token: None, text_buffers };
        value.token = Some(value.lex(true)?);
        Ok(value)
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
        self.token = None;
        Ok(())
    }
    pub(super) fn begin_array(&mut self, context: &mut ConversionContext) -> Result<()> {
        if self.kind(context)? != Kind::Array { return Err(self.coercion("Expected a collection array")); }
        self.token = None;
        Ok(())
    }
    pub(super) fn field(&mut self, _context: &mut ConversionContext) -> Result<Option<SliceName<'a>>> {
        self.discard_scalar()?;
        let first = self.frames.as_slice().last().filter(|frame| frame.kind == Kind::Object)
            .ok_or_else(|| self.syntax("Expected an object context"))?.first;
        self.space()?;
        if self.input.peek()? == Some(0x7d) { self.input.next()?; self.frames.pop(); return Ok(None); }
        if !first { self.expect(0x2c, "Expected a comma between object fields")?; self.space()?; }
        self.expect(0x22, "Expected a quoted object field name")?;
        let start = self.input;
        scan_name(&mut self.input, self.profile)?;
        self.space()?;
        self.expect(0x3a, "Expected a colon after the field name")?;
        self.frames.as_mut_slice().last_mut().unwrap().first = false;
        self.token = Some(self.lex(false)?);
        Ok(Some(SliceName { input: start, profile: self.profile }))
    }
    pub(super) fn element(&mut self) -> Result<bool> {
        self.discard_scalar()?;
        let first = self.frames.as_slice().last().filter(|frame| frame.kind == Kind::Array)
            .ok_or_else(|| self.syntax("Expected an array context"))?.first;
        self.space()?;
        if self.input.peek()? == Some(0x5d) { self.input.next()?; self.frames.pop(); return Ok(false); }
        if !first { self.expect(0x2c, "Expected a comma between array elements")?; }
        self.frames.as_mut_slice().last_mut().unwrap().first = false;
        self.token = Some(self.lex(false)?);
        Ok(true)
    }
    pub(super) fn take_null(&mut self, context: &mut ConversionContext) -> Result<bool> {
        if self.kind(context)? == Kind::Null { self.token = None; Ok(true) } else { Ok(false) }
    }
    pub(super) fn string(&mut self, context: &mut ConversionContext) -> Result<JavaString> {
        if self.kind(context)? != Kind::String { return Err(self.coercion("Expected a string token")); }
        let start = self.input;
        let value_start = self.token.unwrap().start;
        let capacity = self.text_buffers.iter().find(|observation| observation.value_start == value_start)
            .map(|observation| observation.capacity_units);
        scan_string_with_capacity(&mut self.input, self.profile, Usage::Value, capacity, |_| ())?;
        let value = copy_string(start, self.profile, Usage::Value, context)?;
        self.token = None;
        Ok(value)
    }
    pub(super) fn take_boolean_token(&mut self, context: &mut ConversionContext) -> Result<Option<bool>> {
        let result = match self.kind(context)? { Kind::True => Some(true), Kind::False => Some(false), _ => None };
        if result.is_some() { self.token = None; }
        Ok(result)
    }
    pub(super) fn number_text(&mut self, context: &mut ConversionContext) -> Result<JavaString> {
        if !matches!(self.kind(context)?, Kind::Integer | Kind::Float) { return Err(self.coercion("Expected a numeric token")); }
        let token = self.token.take().unwrap();
        let mut input = Input::slice(self.input.range(token.start, token.end), self.input.encoding);
        let mut count = 0usize;
        while input.next()?.is_some() { count = count.checked_add(1).ok_or_else(overflow)?; }
        let mut units = Values::with_capacity(count, context)?;
        let mut input = Input::slice(self.input.range(token.start, token.end), self.input.encoding);
        while let Some(unit) = input.next()? { units.push(unit, context)?; }
        Ok(JavaString::copy_utf16_with_admission(units.as_slice(), |bytes| context.reserve(bytes))?)
    }
    pub(super) fn skip(&mut self, context: &mut ConversionContext) -> Result<()> {
        let already_open = self.token.is_some_and(|token| token.active && matches!(token.kind, Kind::Object | Kind::Array));
        let before = self.frames.len() - usize::from(already_open);
        let kind = self.kind(context)?;
        if !matches!(kind, Kind::Object | Kind::Array) { return self.discard_scalar(); }
        self.token = None;
        while self.frames.len() > before {
            let more = match self.frames.as_slice().last().unwrap().kind {
                Kind::Object => self.field(context)?.is_some(),
                Kind::Array => self.element()?,
                _ => unreachable!(),
            };
            if more {
                let kind = self.kind(context)?;
                if matches!(kind, Kind::Object | Kind::Array) { self.token = None; }
                else { self.discard_scalar()?; }
            }
        }
        Ok(())
    }
    fn discard_scalar(&mut self) -> Result<()> {
        if let Some(token) = self.token.take() {
            if token.kind == Kind::String { scan_string(&mut self.input, self.profile, Usage::Skip, |_| ())?; }
            else if matches!(token.kind, Kind::Object | Kind::Array) { return Err(self.syntax("Container value was not consumed")); }
        }
        Ok(())
    }
    fn space(&mut self) -> Result<()> {
        while self.input.peek()?.is_some_and(|unit| matches!(unit, 0x20 | 0x09 | 0x0a | 0x0d)) { self.input.next()?; }
        Ok(())
    }
    fn expect(&mut self, expected: u16, detail: &'static str) -> Result<()> {
        if self.input.next()? != Some(expected) { return Err(self.syntax(detail)); }
        Ok(())
    }
    pub(super) fn coercion(&self, detail: &'static str) -> Failure { Failure::at(FailureKind::Coercion, self.offset(), detail) }
    pub(super) fn constructor(&self, detail: &'static str) -> Failure { Failure::at(FailureKind::Constructor, self.offset(), detail) }
    fn syntax(&self, detail: &'static str) -> Failure { Failure::at(FailureKind::Syntax, self.input.at, detail) }
    fn lex(&mut self, root: bool) -> Result<Token> {
        self.space()?;
        let start = self.input.at;
        let first = self.input.next()?.ok_or_else(|| self.syntax("Unexpected end of JSON input"))?;
        let kind = match first {
            0x7b => Kind::Object,
            0x5b => Kind::Array,
            0x22 => Kind::String,
            0x74 => { self.literal("rue")?; Kind::True }
            0x66 => { self.literal("alse")?; Kind::False }
            0x6e => { self.literal("ull")?; Kind::Null }
            0x4e => { self.literal("aN")?; Kind::Float }
            0x49 => { self.literal("nfinity")?; Kind::Float }
            0x2b | 0x2d if self.input.peek()? == Some(0x49) => {
                self.input.next()?;
                match self.input.next()? {
                    Some(0x4e) => self.literal("F")?,
                    Some(0x6e) => self.literal("finity")?,
                    _ => return Err(self.syntax("Invalid nonnumeric floating token")),
                }
                Kind::Float
            }
            0x2d | 0x30..=0x39 => self.numeric(first, root)?,
            _ => return Err(self.syntax("Unexpected JSON value token")),
        };
        Ok(Token { kind, start, end: self.input.at, active: false })
    }
    fn literal(&mut self, rest: &str) -> Result<()> {
        for expected in rest.bytes() { self.expect(u16::from(expected), "Invalid JSON literal")?; }
        if let Some(unit) = self.input.peek()? {
            if unit >= 0x30 && unit != 0x5d && unit != 0x7d {
                let unit = if self.input.encoding == Encoding::Utf8 && unit >= 0x80 {
                    if self.profile == Profile::Jackson216 { return Err(self.syntax("Invalid UTF8 continuation byte after literal")); }
                    let mut suffix = self.input;
                    suffix.next()?;
                    let (first, second) = suffix.utf8_character(unit, self.profile, Utf8Demand::Skip)?;
                    second.map_or(first, |low| ((u32::from(first & 0x3f) << 10) | u32::from(low & 0x3ff)) as u16)
                } else { unit };
                let flags = include_bytes!("cursor/identifier-parts.bin");
                if flags[usize::from(unit) >> 3] & (1 << (unit & 7)) != 0 {
                    return Err(self.syntax("Invalid JSON literal suffix"));
                }
            }
        }
        Ok(())
    }
    fn numeric(&mut self, first: u16, root: bool) -> Result<Kind> {
        let first = if first == 0x2d { self.input.next()?.ok_or_else(|| self.syntax("Expected a digit after minus"))? } else { first };
        if !matches!(first, 0x30..=0x39) { return Err(self.syntax("Expected a numeric digit")); }
        let mut digits = 1usize;
        if first == 0x30 && self.input.peek()?.is_some_and(|unit| matches!(unit, 0x30..=0x39)) { return Err(self.syntax("Leading zeroes are disabled")); }
        while self.input.peek()?.is_some_and(|unit| matches!(unit, 0x30..=0x39)) { self.input.next()?; digits += 1; }
        let mut kind = Kind::Integer;
        if self.input.peek()? == Some(0x2e) {
            self.input.next()?; kind = Kind::Float;
            let mut fraction = 0;
            while self.input.peek()?.is_some_and(|unit| matches!(unit, 0x30..=0x39)) { self.input.next()?; digits += 1; fraction += 1; }
            if fraction == 0 { return Err(self.syntax("Expected a digit after decimal point")); }
        }
        if self.input.peek()?.is_some_and(|unit| matches!(unit, 0x65 | 0x45)) {
            self.input.next()?; kind = Kind::Float;
            if self.input.peek()?.is_some_and(|unit| matches!(unit, 0x2b | 0x2d)) { self.input.next()?; }
            let mut exponent = 0;
            while self.input.peek()?.is_some_and(|unit| matches!(unit, 0x30..=0x39)) { self.input.next()?; digits += 1; exponent += 1; }
            if exponent == 0 { return Err(self.syntax("Expected an exponent digit")); }
        }
        if root && self.input.peek()?.is_some_and(|unit| !matches!(unit, 0x20 | 0x09 | 0x0a | 0x0d)) { return Err(self.syntax("Expected whitespace after a root number")); }
        if digits > 1000 { return Err(Failure::at(FailureKind::Constraint, self.input.at, "JSON number length exceeds 1000")); }
        Ok(kind)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Usage { Name, Value, Skip }

fn scan_name(input: &mut Input<'_>, profile: Profile) -> Result<()> {
    if input.encoding != Encoding::Utf8 { return scan_string(input, profile, Usage::Name, |_| ()); }
    let start = *input;
    let mut length = 0;
    loop {
        let at = input.at;
        let unit = input.next()?.ok_or_else(|| Failure::at(FailureKind::Syntax, at, "Unterminated field name"))?;
        if unit == 0x22 { break; }
        if unit < 0x20 { return Err(Failure::at(FailureKind::Syntax, at, "Unescaped control character in field name")); }
        if unit == 0x5c {
            let unit = escaped(input)?;
            if profile == Profile::Jackson221 && (0xd800..=0xdfff).contains(&unit) {
                if unit >= 0xdc00 || input.next()? != Some(0x5c) { return Err(Failure::at(FailureKind::Syntax, at, "Expected an escaped surrogate pair in field name")); }
                let low = escaped(input)?;
                if !(0xdc00..=0xdfff).contains(&low) { return Err(Failure::at(FailureKind::Syntax, at, "Invalid escaped low surrogate in field name")); }
                length += 4;
            } else { length += utf8_unit_bytes(unit); }
        } else { length += 1; }
        if length > 65_540 { return Err(Failure::at(FailureKind::Constraint, at, "JSON field name decode buffer exceeds allowed length")); }
    }
    if length > 50_000 { return Err(Failure::at(FailureKind::Constraint, input.at, "JSON field name length exceeds 50000")); }
    let mut decoded = start;
    scan_string(&mut decoded, profile, Usage::Name, |_| ())
}

fn copy_string(start: Input<'_>, profile: Profile, usage: Usage, context: &mut ConversionContext) -> Result<JavaString> {
    let mut input = start;
    let mut count = 0usize;
    scan_string(&mut input, profile, usage, |_| { count += 1; })?;
    let mut units = Values::with_capacity(count, context)?;
    let mut input = start;
    let mut failure = None;
    scan_string(&mut input, profile, usage, |unit| { if failure.is_none() { if let Err(cause) = units.push(unit, context) { failure = Some(cause); } } })?;
    if let Some(cause) = failure { return Err(cause); }
    Ok(JavaString::copy_utf16_with_admission(units.as_slice(), |bytes| context.reserve(bytes))?)
}

fn scan_string(input: &mut Input<'_>, profile: Profile, usage: Usage, emit: impl FnMut(u16)) -> Result<()> {
    scan_string_with_capacity(input, profile, usage, None, emit)
}

fn scan_string_with_capacity(input: &mut Input<'_>, profile: Profile, usage: Usage, capacity: Option<usize>,
    mut emit: impl FnMut(u16)) -> Result<()> {
    let mut units = 0usize;
    let mut name_bytes = 0usize;
    let capacity = if input.encoding == Encoding::Utf8 { capacity } else { None };
    let mut buffer = if usage == Usage::Value { Some(TextBufferDemand::new(capacity, input.at)?) } else { None };
    loop {
        let at = input.at;
        let first = input.next()?.ok_or_else(|| Failure::at(FailureKind::Syntax, at, "Unterminated JSON string"))?;
        if let Some(buffer) = &mut buffer { buffer.before_character(at)?; }
        if first == 0x22 {
            if usage == Usage::Name {
                let length = if input.encoding == Encoding::Utf8 { name_bytes } else { units };
                if length > 50_000 { return Err(Failure::at(FailureKind::Constraint, at, "JSON field name length exceeds 50000")); }
            }
            if let Some(buffer) = &buffer { buffer.complete(at)?; }
            return Ok(());
        }
        let (unit, second) = if first == 0x5c {
            let unit = escaped(input)?;
            if usage == Usage::Name && input.encoding == Encoding::Utf8 && profile == Profile::Jackson221 {
                if (0xd800..=0xdbff).contains(&unit) {
                    if input.next()? != Some(0x5c) { return Err(Failure::at(FailureKind::Syntax, at, "Expected an escaped low surrogate in field name")); }
                    let low = escaped(input)?;
                    if !(0xdc00..=0xdfff).contains(&low) { return Err(Failure::at(FailureKind::Syntax, at, "Invalid low surrogate in field name")); }
                    name_bytes += 4;
                    (unit, Some(low))
                } else if (0xdc00..=0xdfff).contains(&unit) {
                    return Err(Failure::at(FailureKind::Syntax, at, "Unexpected low surrogate in field name"));
                } else { name_bytes += utf8_unit_bytes(unit); (unit, None) }
            } else { name_bytes += utf8_unit_bytes(unit); (unit, None) }
        } else {
            if first < 0x20 { return Err(Failure::at(FailureKind::Syntax, at, "Unescaped control character in JSON string")); }
            let demand = match usage { Usage::Name => Utf8Demand::Name, Usage::Value => Utf8Demand::Value, Usage::Skip => Utf8Demand::Skip };
            let value = input.utf8_character(first, profile, demand)?;
            name_bytes += input.at - at;
            value
        };
        emit(unit); units += 1;
        if let Some(buffer) = &mut buffer { buffer.unit(at)?; }
        if let Some(second) = second {
            if let Some(buffer) = &mut buffer { buffer.before_character(at)?; }
            emit(second); units += 1;
            if let Some(buffer) = &mut buffer { buffer.unit(at)?; }
        }
    }
}
fn utf8_unit_bytes(unit: u16) -> usize { if unit < 0x80 { 1 } else if unit < 0x800 { 2 } else { 3 } }
fn escaped(input: &mut Input<'_>) -> Result<u16> {
    let at = input.at;
    Ok(match input.next()? {
        Some(0x22) => 0x22, Some(0x5c) => 0x5c, Some(0x2f) => 0x2f,
        Some(0x62) => 8, Some(0x66) => 12, Some(0x6e) => 10, Some(0x72) => 13, Some(0x74) => 9,
        Some(0x75) => {
            let mut value = 0;
            for _ in 0..4 {
                let digit = match input.next()? {
                    Some(unit @ 0x30..=0x39) => unit - 0x30,
                    Some(unit @ 0x61..=0x66) => unit - 0x61 + 10,
                    Some(unit @ 0x41..=0x46) => unit - 0x41 + 10,
                    _ => return Err(Failure::at(FailureKind::Syntax, input.at, "Invalid JSON Unicode escape")),
                };
                value = (value << 4) | digit;
            }
            value
        }
        _ => return Err(Failure::at(FailureKind::Syntax, at, "Invalid JSON string escape")),
    })
}


#[cfg(not(test))]
pub(super) type Cursor<'a> = SliceCursor<'a>;

#[cfg(test)]
mod observed;

#[cfg(test)]
pub(super) enum Name<'a> { Slice(SliceName<'a>), Observed(JavaString) }
#[cfg(test)]
impl Name<'_> {
    pub(super) fn is(&self, expected: &str) -> Result<bool> {
        match self { Self::Slice(name) => name.is(expected), Self::Observed(name) => Ok(name.view().units().eq(expected.encode_utf16())) }
    }
    pub(super) fn copy(&self, context: &mut ConversionContext) -> Result<JavaString> {
        match self { Self::Slice(name) => name.copy(context), Self::Observed(name) => Ok(name.clone()) }
    }
}

#[cfg(test)]
pub(super) enum Cursor<'a> { Slice(SliceCursor<'a>), Observed(observed::ObservedCursor<'a>) }
#[cfg(test)]
impl<'a> Cursor<'a> {
    pub(super) fn new(bytes: &'a [u8], profile: Profile) -> Result<Self> { Ok(Self::Slice(SliceCursor::new(bytes, profile)?)) }
    pub(super) fn with_text_buffers(bytes: &'a [u8], profile: Profile, observations: &'a [TextBufferObservation]) -> Result<Self> {
        Ok(Self::Slice(SliceCursor::with_text_buffers(bytes, profile, observations)?))
    }
    pub(super) fn observed(input: super::window::WindowInput<'a>, profile: Profile, observations: super::window::LexicalObservations<'a>) -> Self {
        Self::Observed(observed::ObservedCursor::new(input, profile, observations))
    }
    pub(super) fn reader(input: super::window::WindowInput<'a>, child: super::symbols::ChildSymbols,
        authority: &'a mut dyn super::symbols::InternAuthority) -> Self {
        Self::Observed(observed::ObservedCursor::reader(input, child, authority))
    }
    pub(super) fn start(&mut self) -> Result<()> { match self { Self::Slice(_) => Ok(()), Self::Observed(cursor) => cursor.start() } }
    pub(super) fn offset(&self) -> usize { match self { Self::Slice(cursor) => cursor.offset(), Self::Observed(cursor) => cursor.offset() } }
    pub(super) fn profile(&self) -> Profile { match self { Self::Slice(cursor) => cursor.profile(), Self::Observed(cursor) => cursor.profile() } }
    pub(super) fn kind(&mut self, context: &mut ConversionContext) -> Result<Kind> {
        match self { Self::Slice(cursor) => cursor.kind(context), Self::Observed(cursor) => cursor.kind(context) }
    }
    pub(super) fn begin_object(&mut self, context: &mut ConversionContext) -> Result<()> {
        match self { Self::Slice(cursor) => cursor.begin_object(context), Self::Observed(cursor) => cursor.begin_object(context) }
    }
    pub(super) fn begin_array(&mut self, context: &mut ConversionContext) -> Result<()> {
        match self { Self::Slice(cursor) => cursor.begin_array(context), Self::Observed(cursor) => cursor.begin_array(context) }
    }
    pub(super) fn field(&mut self, context: &mut ConversionContext) -> Result<Option<Name<'a>>> {
        match self { Self::Slice(cursor) => Ok(cursor.field(context)?.map(Name::Slice)), Self::Observed(cursor) => Ok(cursor.field(context)?.map(Name::Observed)) }
    }
    pub(super) fn element(&mut self) -> Result<bool> {
        match self { Self::Slice(cursor) => cursor.element(), Self::Observed(cursor) => cursor.element() }
    }
    pub(super) fn take_null(&mut self, context: &mut ConversionContext) -> Result<bool> {
        match self { Self::Slice(cursor) => cursor.take_null(context), Self::Observed(cursor) => cursor.take_null(context) }
    }
    pub(super) fn string(&mut self, context: &mut ConversionContext) -> Result<JavaString> {
        match self { Self::Slice(cursor) => cursor.string(context), Self::Observed(cursor) => cursor.string(context) }
    }
    pub(super) fn take_boolean_token(&mut self, context: &mut ConversionContext) -> Result<Option<bool>> {
        match self { Self::Slice(cursor) => cursor.take_boolean_token(context), Self::Observed(cursor) => cursor.take_boolean_token(context) }
    }
    pub(super) fn number_text(&mut self, context: &mut ConversionContext) -> Result<JavaString> {
        match self { Self::Slice(cursor) => cursor.number_text(context), Self::Observed(cursor) => cursor.number_text(context) }
    }
    pub(super) fn skip(&mut self, context: &mut ConversionContext) -> Result<()> {
        match self { Self::Slice(cursor) => cursor.skip(context), Self::Observed(cursor) => cursor.skip(context) }
    }
    pub(super) fn coercion(&self, detail: &'static str) -> Failure { Failure::at(FailureKind::Coercion, self.offset(), detail) }
    pub(super) fn constructor(&self, detail: &'static str) -> Failure { Failure::at(FailureKind::Constructor, self.offset(), detail) }
    pub(super) fn close(&mut self, context: &mut ConversionContext) -> Result<()> { match self { Self::Slice(_) => Ok(()), Self::Observed(cursor) => cursor.close(context) } }
    pub(super) fn observations_consumed(&self) -> usize { match self { Self::Slice(_) => 0, Self::Observed(cursor) => cursor.observations_consumed() } }
}
