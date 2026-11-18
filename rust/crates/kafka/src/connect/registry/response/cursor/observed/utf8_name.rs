use super::{ConversionContext, Failure, FailureKind, Profile, Result, Values, WindowCursor, hex};
use super::super::super::{symbols::{InternAuthority, bytes}, window::byte_symbol_failure};

pub(super) struct QuadBuffer { values: Values<u32> }
impl QuadBuffer {
    pub(super) fn new(context: &mut ConversionContext) -> Result<Self> { Ok(Self { values: zeros(16, context)? }) }
    fn store(&mut self, index: usize, value: u32, at: usize, context: &mut ConversionContext) -> Result<()> {
        if index == self.values.len() {
            let length = self.values.len();
            if length * 4 > 50_000 { return Err(Failure::at(FailureKind::Constraint, at, "JSON field name decode buffer exceeds allowed length")); }
            let mut replacement = zeros(length * 2, context)?;
            replacement.as_mut_slice()[..length].copy_from_slice(self.values.as_slice());
            self.values = replacement;
        }
        self.values.as_mut_slice()[index] = value;
        Ok(())
    }
}
fn zeros(length: usize, context: &mut ConversionContext) -> Result<Values<u32>> {
    let mut result = Values::with_capacity(length, context)?;
    for _ in 0..length { result.push(0, context)?; }
    Ok(result)
}

struct PackedName { length: usize, last_bytes: usize }
struct Packing { length: usize, quad: u32, bytes: usize }
impl Packing {
    fn emit(&mut self, byte: u8, buffer: &mut QuadBuffer, at: usize, context: &mut ConversionContext) -> Result<()> {
        if self.bytes == 4 {
            buffer.store(self.length, self.quad, at, context)?;
            self.length += 1; self.quad = 0; self.bytes = 0;
        }
        self.quad = (self.quad << 8) | u32::from(byte); self.bytes += 1;
        Ok(())
    }
    fn finish(self, buffer: &mut QuadBuffer, at: usize, context: &mut ConversionContext) -> Result<PackedName> {
        if self.bytes == 0 { return Ok(PackedName { length: 0, last_bytes: 0 }); }
        let value = if self.bytes == 4 { self.quad } else { self.quad | (u32::MAX << (self.bytes * 8)) };
        buffer.store(self.length, value, at, context)?;
        Ok(PackedName { length: self.length + 1, last_bytes: self.bytes })
    }
}

pub(super) fn name(input: &mut WindowCursor<'_>, profile: Profile, buffer: &mut QuadBuffer, child: &mut bytes::ByteChild,
    authority: &mut dyn InternAuthority, context: &mut ConversionContext) -> Result<rtdb_types::java_text::JavaString> {
    let packed = pack(input, profile, buffer, context)?;
    let quads = &buffer.values.as_slice()[..packed.length];
    if let Some(name) = child.find(quads).map_err(|cause| byte_symbol_failure(cause, input.at))? { return Ok(name); }
    let byte_length = (packed.length - 1) * 4 + packed.last_bytes;
    if byte_length > 50_000 { return Err(Failure::at(FailureKind::Constraint, input.at, "JSON field name length exceeds 50000")); }
    let units = decode(quads, packed.last_bytes, byte_length, profile, input.at, context)?;
    let candidate = bytes::prepare_name(units.as_slice(), context).map_err(|cause| byte_symbol_failure(cause, input.at))?;
    child.add(bytes::Quads::new(quads).unwrap(), candidate, authority, context).map_err(|cause| byte_symbol_failure(cause, input.at))
}

fn pack(input: &mut WindowCursor<'_>, profile: Profile, buffer: &mut QuadBuffer, context: &mut ConversionContext) -> Result<PackedName> {
    let mut packed = Packing { length: 0, quad: 0, bytes: 0 };
    loop {
        let unit = input.next()?.ok_or_else(|| syntax(input.at, "Unterminated field name"))?;
        if unit == 0x22 { return packed.finish(buffer, input.at, context); }
        if unit < 0x20 { return Err(syntax(input.at, "Unescaped control character in field name")); }
        if unit != 0x5c { packed.emit(unit as u8, buffer, input.at, context)?; continue; }
        let mut point = u32::from(escaped(input)?);
        if profile == Profile::Jackson221 {
            if (0xd800..=0xdbff).contains(&point) {
                if input.peek()? != Some(0x5c) { return Err(syntax(input.at, "Expected an escaped surrogate pair in field name")); }
                input.next()?;
                let low = u32::from(escaped(input)?);
                if !(0xdc00..=0xdfff).contains(&low) { return Err(syntax(input.at, "Invalid escaped low surrogate in field name")); }
                point = 0x10000 + ((point - 0xd800) << 10) + (low - 0xdc00);
            } else if (0xdc00..=0xdfff).contains(&point) { return Err(syntax(input.at, "Unexpected escaped low surrogate in field name")); }
        }
        let mut encoded = [0u8; 4];
        let length = if point < 0x80 { encoded[0] = point as u8; 1 }
            else if point < 0x800 { encoded[0] = (0xc0 | (point >> 6)) as u8; encoded[1] = (0x80 | (point & 0x3f)) as u8; 2 }
            else if point < 0x10000 {
                encoded[0] = (0xe0 | (point >> 12)) as u8; encoded[1] = (0x80 | ((point >> 6) & 0x3f)) as u8; encoded[2] = (0x80 | (point & 0x3f)) as u8; 3
            } else {
                encoded[0] = (0xf0 | (point >> 18)) as u8; encoded[1] = (0x80 | ((point >> 12) & 0x3f)) as u8;
                encoded[2] = (0x80 | ((point >> 6) & 0x3f)) as u8; encoded[3] = (0x80 | (point & 0x3f)) as u8; 4
            };
        for byte in &encoded[..length] { packed.emit(*byte, buffer, input.at, context)?; }
    }
}

fn escaped(input: &mut WindowCursor<'_>) -> Result<u16> {
    match input.next()? {
        Some(0x22) => Ok(0x22), Some(0x5c) => Ok(0x5c), Some(0x2f) => Ok(0x2f),
        Some(0x62) => Ok(8), Some(0x66) => Ok(12), Some(0x6e) => Ok(10), Some(0x72) => Ok(13), Some(0x74) => Ok(9),
        Some(0x75) => {
            let mut value = 0;
            for _ in 0..4 { value = (value << 4) | hex(input.next()?).ok_or_else(|| syntax(input.at, "Invalid JSON Unicode escape"))?; }
            Ok(value)
        }
        _ => Err(syntax(input.at, "Invalid JSON string escape")),
    }
}

fn decode(quads: &[u32], last_bytes: usize, length: usize, profile: Profile, at: usize, context: &mut ConversionContext) -> Result<Values<u16>> {
    let byte = |index: usize| {
        let mut quad = quads[index >> 2];
        if index >> 2 == quads.len() - 1 && last_bytes < 4 { quad <<= (4 - last_bytes) * 8; }
        ((quad >> ((3 - (index & 3)) * 8)) & 0xff) as u8
    };
    let mut units = Values::new();
    let mut index = 0;
    while index < length {
        let first = byte(index); index += 1;
        let (needed, mut point) = if first < 0x80 { (0, i32::from(first)) }
            else if first & 0xe0 == 0xc0 { (1, i32::from(first & 0x1f)) }
            else if first & 0xf0 == 0xe0 { (2, i32::from(first & 0x0f)) }
            else if first & 0xf8 == 0xf0 { (3, i32::from(first & 7)) }
            else { return Err(syntax(at, "Invalid UTF8 initial byte in field name")); };
        if index + needed > length { return Err(syntax(at, "Truncated UTF8 field name")); }
        for _ in 0..needed {
            let next = byte(index); index += 1;
            if next & 0xc0 != 0x80 { return Err(syntax(at, "Invalid UTF8 continuation byte in field name")); }
            point = (point << 6) | i32::from(next & 0x3f);
        }
        if needed == 2 && profile == Profile::Jackson221 && (0xd800..=0xdfff).contains(&point) { return Err(syntax(at, "Illegal UTF8 surrogate character in field name")); }
        if needed == 3 {
            point -= 0x10000;
            units.push((0xd800 + (point >> 10)) as u16, context)?;
            point = 0xdc00 | (point & 0x3ff);
        }
        units.push(point as u16, context)?;
    }
    Ok(units)
}
fn syntax(at: usize, detail: &'static str) -> Failure { Failure::at(FailureKind::Syntax, at, detail) }
