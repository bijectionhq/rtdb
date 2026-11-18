use std::{alloc::Layout, mem::size_of, sync::Arc};
use rtdb_types::{Anomaly, MemoryReservation};
use super::{ConversionContext, error};

mod utf16;
pub(super) use utf16::{Utf16Reader, Utf16Window};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Profile { Jackson216, Jackson221 }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReadMethod { Slice, WholeBuffer }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ReadRequest { pub(super) method: ReadMethod, pub(super) offset: usize, pub(super) length: usize }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Call { Read(ReadRequest), Available, Close }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Returned<'a> { Bytes(&'a [u8]), Zero, Eof, Available(i32), Io(&'a str), Closed }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Observation<'a> { pub(super) call: Call, pub(super) returned: Returned<'a> }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stage { Bootstrap, Utf8Refill, Utf32Refill, Utf16Read, Utf16Available, SourceClose }
#[derive(Debug)]
pub(super) enum Failure<'a> {
    Native(Anomaly),
    Observation { index: usize, expected: Call, detail: &'static str },
    Io { stage: Stage, detail: &'a str },
    UnsupportedOrder(&'static str),
    ZeroRead(Stage),
    TruncatedUtf32 { got: usize, byte_position: i32, char_position: i32 },
    InvalidUtf32 { value: u32, byte_position: i32, char_position: i32 },
}
impl<'a> From<Anomaly> for Failure<'a> { fn from(cause: Anomaly) -> Self { Self::Native(cause) } }
pub(super) type Result<'a, T> = std::result::Result<T, Failure<'a>>;

pub(super) struct ReadTape<'a> { observations: &'a [Observation<'a>], next: usize }
impl<'a> ReadTape<'a> {
    pub(super) fn new(observations: &'a [Observation<'a>]) -> Self { Self { observations, next: 0 } }
    pub(super) fn consumed(&self) -> usize { self.next }
    fn observe(&mut self, call: Call, stage: Stage) -> Result<'a, Returned<'a>> {
        let index = self.next;
        let observed = self.observations.get(index).ok_or(Failure::Observation { index, expected: call, detail: "Missing source call observation" })?;
        self.next += 1;
        if observed.call != call { return Err(Failure::Observation { index, expected: call, detail: "Source call differs from its observation" }); }
        match (call, observed.returned) {
            (_, Returned::Io(detail)) => Err(Failure::Io { stage, detail }),
            (Call::Close, Returned::Closed) => Ok(Returned::Closed),
            (Call::Close, _) => Err(Failure::Observation { index, expected: call, detail: "Read result used for close" }),
            (Call::Available, returned @ Returned::Available(_)) => Ok(returned),
            (Call::Available, _) => Err(Failure::Observation { index, expected: call, detail: "Non-availability result used for available" }),
            (Call::Read(request), Returned::Bytes(bytes)) => {
                if bytes.is_empty() || bytes.len() > request.length { return Err(Failure::Observation { index, expected: call, detail: "Observed byte count is outside the requested capacity" }); }
                Ok(Returned::Bytes(bytes))
            }
            (Call::Read(_), returned @ (Returned::Zero | Returned::Eof)) => Ok(returned),
            (Call::Read(_), Returned::Closed) => Err(Failure::Observation { index, expected: call, detail: "Close result used for a read" }),
            (Call::Read(_), Returned::Available(_)) => Err(Failure::Observation { index, expected: call, detail: "Availability result used for a read" }),
        }
    }
}

struct Buffer<T> { values: Vec<T>, _charge: Arc<MemoryReservation> }
impl<T: Copy + Default> Buffer<T> {
    fn new<'a>(capacity: usize, context: &mut ConversionContext) -> Result<'a, Self> {
        if capacity > i32::MAX as usize { return Err(error("allocation-overflow", "Observed Java input array exceeds signed int length").into()); }
        Layout::array::<T>(capacity).map_err(|_| overflow())?;
        let bytes = capacity.checked_mul(size_of::<T>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?;
        let charge = context.reserve(bytes)?;
        let mut values = Vec::new();
        values.try_reserve_exact(capacity).map_err(|_| error("allocation-failed", "Registry input buffer allocation failed"))?;
        values.resize(capacity, T::default());
        Ok(Self { values, _charge: charge })
    }
}

pub(super) struct ByteInput<'a> { buffer: Option<Buffer<u8>>, tape: ReadTape<'a>, ptr: usize, end: usize, source_open: bool }
impl<'a> ByteInput<'a> {
    fn new(capacity: usize, tape: ReadTape<'a>, context: &mut ConversionContext) -> Result<'a, Self> {
        if capacity < 4 { return Err(Failure::Observation { index: tape.consumed(), expected: Call::Read(ReadRequest { method: ReadMethod::Slice, offset: 0, length: capacity }), detail: "Bootstrap read buffer must hold its four-byte detection word" }); }
        Ok(Self { buffer: Some(Buffer::new(capacity, context)?), tape, ptr: 0, end: 0, source_open: true })
    }
    pub(super) fn observations_consumed(&self) -> usize { self.tape.consumed() }
    pub(super) fn buffered(&self) -> &[u8] { self.buffer.as_ref().map_or(&[], |buffer| &buffer.values[self.ptr..self.end]) }
    fn capacity(&self) -> usize { self.buffer.as_ref().map_or(0, |buffer| buffer.values.len()) }
    fn read(&mut self, method: ReadMethod, offset: usize, stage: Stage) -> Result<'a, Returned<'a>> {
        let length = self.capacity() - offset;
        let returned = self.tape.observe(Call::Read(ReadRequest { method, offset, length }), stage)?;
        if let Returned::Bytes(bytes) = returned { self.buffer.as_mut().unwrap().values[offset..offset + bytes.len()].copy_from_slice(bytes); }
        Ok(returned)
    }
    fn ensure_loaded(&mut self, minimum: usize) -> Result<'a, bool> {
        let mut gotten = self.end - self.ptr;
        while gotten < minimum {
            match self.read(ReadMethod::Slice, self.end, Stage::Bootstrap)? {
                Returned::Bytes(bytes) => { self.end += bytes.len(); gotten += bytes.len(); }
                Returned::Zero | Returned::Eof => return Ok(false),
                _ => unreachable!(),
            }
        }
        Ok(true)
    }
    fn close_after_success(&mut self) -> Result<'a, ()> {
        if self.source_open { self.tape.observe(Call::Close, Stage::SourceClose)?; self.source_open = false; }
        Ok(())
    }
    fn close_before_call(&mut self) -> Result<'a, ()> {
        if self.source_open {
            self.source_open = false;
            self.buffer = None;
            self.tape.observe(Call::Close, Stage::SourceClose)?;
        }
        Ok(())
    }
}

pub(super) enum DetectedInput<'a> { Utf8(Utf8Window<'a>), Utf16 { input: ByteInput<'a>, little: bool }, Utf32(Utf32Reader<'a>) }
pub(super) fn bootstrap<'a>(capacity: usize, tape: ReadTape<'a>, profile: Profile, context: &mut ConversionContext) -> Result<'a, DetectedInput<'a>> {
    let mut input = ByteInput::new(capacity, tape, context)?;
    let mut encoding = (1, false);
    if input.ensure_loaded(4)? {
        let word = u32::from_be_bytes(input.buffer.as_ref().unwrap().values[..4].try_into().unwrap());
        match word {
            0x0000feff => { encoding = (4, false); input.ptr = 4; }
            0xfffe0000 => { encoding = (4, true); input.ptr = 4; }
            0x0000fffe => return Err(Failure::UnsupportedOrder("2143")),
            0xfeff0000 => return Err(Failure::UnsupportedOrder("3412")),
            _ if word >> 16 == 0xfeff => { encoding = (2, false); input.ptr = 2; }
            _ if word >> 16 == 0xfffe => { encoding = (2, true); input.ptr = 2; }
            _ if word >> 8 == 0xefbbbf => { input.ptr = 3; }
            _ if word >> 8 == 0 => encoding = (4, false),
            _ if word & 0x00ff_ffff == 0 => encoding = (4, true),
            _ if word & !0x00ff_0000 == 0 => return Err(Failure::UnsupportedOrder("3412")),
            _ if word & !0x0000_ff00 == 0 => return Err(Failure::UnsupportedOrder("2143")),
            _ if word >> 16 & 0xff00 == 0 => encoding = (2, false),
            _ if word >> 16 & 0xff == 0 => encoding = (2, true),
            _ => (),
        }
    } else if input.ensure_loaded(2)? {
        let word = u16::from_be_bytes(input.buffer.as_ref().unwrap().values[..2].try_into().unwrap());
        if word & 0xff00 == 0 { encoding = (2, false); }
        else if word & 0xff == 0 { encoding = (2, true); }
    }
    Ok(match encoding {
        (1, _) => DetectedInput::Utf8(Utf8Window { input, processed: 0, closed: false }),
        (2, little) => DetectedInput::Utf16 { input, little },
        (4, little) => DetectedInput::Utf32(Utf32Reader { input, profile, little, pending: 0, byte_count: 0, char_count: 0 }),
        _ => unreachable!(),
    })
}

pub(super) struct Utf8Window<'a> { input: ByteInput<'a>, processed: i64, closed: bool }
impl<'a> Utf8Window<'a> {
    pub(super) fn observations_consumed(&self) -> usize { self.input.observations_consumed() }
    pub(super) fn processed(&self) -> i64 { self.processed }
    pub(super) fn position(&self) -> usize { self.input.ptr }
    pub(super) fn next(&mut self) -> Result<'a, Option<u8>> {
        if self.closed { return Ok(None); }
        if self.input.ptr == self.input.end && !self.load_more()? { return Ok(None); }
        let value = self.input.buffer.as_ref().unwrap().values[self.input.ptr];
        self.input.ptr += 1;
        Ok(Some(value))
    }
    fn load_more(&mut self) -> Result<'a, bool> {
        if !self.input.source_open || self.input.capacity() == 0 { return Ok(false); }
        self.processed = self.processed.wrapping_add(self.input.end as i64);
        match self.input.read(ReadMethod::Slice, 0, Stage::Utf8Refill)? {
            Returned::Bytes(bytes) => { self.input.ptr = 0; self.input.end = bytes.len(); Ok(true) }
            returned => {
                self.input.ptr = 0; self.input.end = 0;
                self.input.close_after_success()?;
                if returned == Returned::Zero { Err(Failure::ZeroRead(Stage::Utf8Refill)) } else { Ok(false) }
            }
        }
    }
    pub(super) fn close(&mut self) -> Result<'a, ()> {
        if self.closed { return Ok(()); }
        self.input.ptr = self.input.ptr.max(self.input.end);
        self.closed = true;
        let result = self.input.close_after_success();
        self.input.buffer = None;
        result
    }
}

pub(super) struct Utf32Reader<'a> { input: ByteInput<'a>, profile: Profile, little: bool, pending: u16, byte_count: i32, char_count: i32 }
impl<'a> Utf32Reader<'a> {
    pub(super) fn observations_consumed(&self) -> usize { self.input.observations_consumed() }
    pub(super) fn counters(&self) -> (i32, i32) { (self.byte_count, self.char_count) }
    pub(super) fn buffered_bytes(&self) -> usize { self.input.end - self.input.ptr }
    pub(super) fn buffer_live(&self) -> bool { self.input.buffer.is_some() }
    pub(super) fn read_into(&mut self, output: &mut [u16]) -> Result<'a, Option<usize>> {
        if self.input.buffer.is_none() { return Ok(None); }
        if output.is_empty() { return Ok(Some(0)); }
        if output.len() > i32::MAX as usize { return Err(overflow().into()); }
        let mut written = 0;
        if self.pending != 0 { output[written] = self.pending; written += 1; self.pending = 0; }
        else {
            let left = self.input.end - self.input.ptr;
            if left < 4 && !self.load_more(left)? {
                if left == 0 { return Ok(None); }
                return Err(self.truncated(self.input.end - self.input.ptr));
            }
        }
        while written < output.len() && self.input.ptr + 4 <= self.input.end {
            let word: [u8; 4] = self.input.buffer.as_ref().unwrap().values[self.input.ptr..self.input.ptr + 4].try_into().unwrap();
            self.input.ptr += 4;
            let point = if self.little { u32::from_le_bytes(word) } else { u32::from_be_bytes(word) };
            let high = point >> 16;
            let mut low = (point & 0xffff) as u16;
            if high != 0 {
                let normalized = point.wrapping_sub(0x10000);
                if high > 0x10 {
                    return Err(Failure::InvalidUtf32 { value: normalized, byte_position: self.byte_count.wrapping_add(self.input.ptr as i32).wrapping_sub(1), char_position: self.char_count.wrapping_add(written as i32) });
                }
                output[written] = (0xd800 + (normalized >> 10)) as u16;
                written += 1;
                low = 0xdc00 | (normalized & 0x3ff) as u16;
                if written == output.len() {
                    self.pending = match self.profile { Profile::Jackson216 => normalized as u16, Profile::Jackson221 => low };
                    break;
                }
            }
            output[written] = low;
            written += 1;
        }
        self.char_count = self.char_count.wrapping_add(written as i32);
        Ok(Some(written))
    }
    fn truncated(&self, got: usize) -> Failure<'a> { Failure::TruncatedUtf32 { got, byte_position: self.byte_count.wrapping_add(got as i32), char_position: self.char_count } }
    fn load_more(&mut self, available: usize) -> Result<'a, bool> {
        if !self.input.source_open || self.input.buffer.is_none() { return Ok(false); }
        self.byte_count = self.byte_count.wrapping_add((self.input.end - available) as i32);
        if available > 0 {
            if self.input.ptr > 0 {
                self.input.buffer.as_mut().unwrap().values.copy_within(self.input.ptr..self.input.ptr + available, 0);
                self.input.ptr = 0;
            }
            self.input.end = available;
        } else {
            self.input.ptr = 0;
            match self.input.read(ReadMethod::WholeBuffer, 0, Stage::Utf32Refill)? {
                Returned::Bytes(bytes) => self.input.end = bytes.len(),
                returned => {
                    self.input.end = 0;
                    if returned == Returned::Eof { self.input.buffer = None; return Ok(false); }
                    return Err(Failure::ZeroRead(Stage::Utf32Refill));
                }
            }
        }
        while self.input.end < 4 {
            match self.input.read(ReadMethod::Slice, self.input.end, Stage::Utf32Refill)? {
                Returned::Bytes(bytes) => self.input.end += bytes.len(),
                returned => {
                    if returned == Returned::Eof { self.input.buffer = None; return Err(self.truncated(self.input.end)); }
                    return Err(Failure::ZeroRead(Stage::Utf32Refill));
                }
            }
        }
        Ok(true)
    }
    pub(super) fn close(&mut self) -> Result<'a, ()> { self.input.close_before_call() }
}

pub(super) struct ReaderWindow<'a> { reader: Utf32Reader<'a>, buffer: Option<Buffer<u16>>, ptr: usize, end: usize, processed: i64, attached: bool, closed: bool }
impl<'a> ReaderWindow<'a> {
    pub(super) fn profile(&self) -> Profile { self.reader.profile }
    pub(super) fn new(reader: Utf32Reader<'a>, capacity: usize, context: &mut ConversionContext) -> Result<'a, Self> {
        if capacity == 0 { return Err(Failure::Observation { index: reader.observations_consumed(), expected: Call::Read(ReadRequest { method: ReadMethod::Slice, offset: 0, length: 0 }), detail: "Parser character buffer must have positive observed capacity" }); }
        Ok(Self { reader, buffer: Some(Buffer::new(capacity, context)?), ptr: 0, end: 0, processed: 0, attached: true, closed: false })
    }
    pub(super) fn observations_consumed(&self) -> usize { self.reader.observations_consumed() }
    pub(super) fn processed(&self) -> i64 { self.processed }
    pub(super) fn position(&self) -> usize { self.ptr }
    pub(super) fn next(&mut self) -> Result<'a, Option<u16>> {
        if self.closed { return Ok(None); }
        if self.ptr == self.end && !self.load_more()? { return Ok(None); }
        let value = self.buffer.as_ref().unwrap().values[self.ptr]; self.ptr += 1;
        Ok(Some(value))
    }
    fn load_more(&mut self) -> Result<'a, bool> {
        if !self.attached { return Ok(false); }
        self.processed = self.processed.wrapping_add(self.end as i64);
        let count = self.reader.read_into(&mut self.buffer.as_mut().unwrap().values)?;
        if let Some(count) = count.filter(|count| *count > 0) { self.ptr = 0; self.end = count; return Ok(true); }
        self.ptr = 0; self.end = 0;
        self.close_input()?;
        if count == Some(0) { return Err(Failure::ZeroRead(Stage::Utf32Refill)); }
        Ok(false)
    }
    fn close_input(&mut self) -> Result<'a, ()> { if self.attached { self.reader.close()?; self.attached = false; } Ok(()) }
    pub(super) fn close_input_stage(&mut self) -> Result<'a, ()> {
        if self.closed { return Ok(()); }
        self.ptr = self.ptr.max(self.end); self.closed = true;
        self.close_input()
    }
    pub(super) fn release_parser_buffer(&mut self) { self.buffer = None; }
    pub(super) fn close(&mut self) -> Result<'a, ()> {
        let result = self.close_input_stage(); self.release_parser_buffer(); result
    }
}

fn overflow() -> Anomaly { error("allocation-overflow", "Registry input buffer exceeds addressable memory") }

#[cfg(test)]
mod tests;
