use super::{Buffer, ByteInput, Call, ConversionContext, Failure, ReadMethod, ReadRequest, Result, Returned, Stage, overflow};

const DECODER_BYTES: usize = 8192;

struct MergedInput<'a> { input: ByteInput<'a> }
impl<'a> MergedInput<'a> {
    fn read_into(&mut self, output: &mut [u8], offset: usize) -> Result<'a, Option<usize>> {
        if self.input.ptr < self.input.end {
            let count = (output.len() - offset).min(self.input.end - self.input.ptr);
            output[offset..offset + count].copy_from_slice(&self.input.buffer.as_ref().unwrap().values[self.input.ptr..self.input.ptr + count]);
            self.input.ptr += count;
            if self.input.ptr == self.input.end { self.input.buffer = None; }
            return Ok(Some(count));
        }
        let call = Call::Read(ReadRequest { method: ReadMethod::Slice, offset, length: output.len() - offset });
        match self.input.tape.observe(call, Stage::Utf16Read)? {
            Returned::Bytes(bytes) => { output[offset..offset + bytes.len()].copy_from_slice(bytes); Ok(Some(bytes.len())) }
            Returned::Zero => Ok(Some(0)),
            Returned::Eof => Ok(None),
            _ => unreachable!(),
        }
    }
    fn available(&mut self) -> Result<'a, i32> {
        if self.input.ptr < self.input.end { return Ok((self.input.end - self.input.ptr) as i32); }
        match self.input.tape.observe(Call::Available, Stage::Utf16Available)? { Returned::Available(count) => Ok(count), _ => unreachable!() }
    }
    fn close(&mut self) -> Result<'a, ()> {
        self.input.buffer = None;
        self.input.close_after_success()
    }
}

enum DecodeStatus { Underflow, Overflow }

pub(in crate::connect::registry) struct Utf16Reader<'a> {
    source: MergedInput<'a>, bytes: Buffer<u8>, ptr: usize, end: usize, little: bool, leftover: Option<u16>, closed: bool,
}
impl<'a> Utf16Reader<'a> {
    pub(in crate::connect::registry) fn new(input: ByteInput<'a>, little: bool, context: &mut ConversionContext) -> Result<'a, Self> {
        Ok(Self { source: MergedInput { input }, bytes: Buffer::new(DECODER_BYTES, context)?, ptr: 0, end: 0, little, leftover: None, closed: false })
    }
    pub(in crate::connect::registry) fn observations_consumed(&self) -> usize { self.source.input.observations_consumed() }
    pub(in crate::connect::registry) fn read_into(&mut self, output: &mut [u16]) -> Result<'a, Option<usize>> {
        self.ensure_open()?;
        if output.is_empty() { return Ok(Some(0)); }
        if output.len() > i32::MAX as usize { return Err(overflow().into()); }
        let mut written = 0;
        if let Some(unit) = self.leftover.take() {
            output[0] = unit; written = 1;
            if output.len() == 1 || !self.impl_ready()? { return Ok(Some(1)); }
        }
        if output.len() - written == 1 {
            if let Some(unit) = self.read_one()? { output[written] = unit; return Ok(Some(written + 1)); }
            return Ok((written > 0).then_some(written));
        }
        match self.impl_read(&mut output[written..])? {
            Some(count) => Ok(Some(written + count)),
            None => Ok((written > 0).then_some(written)),
        }
    }
    pub(in crate::connect::registry) fn read_one(&mut self) -> Result<'a, Option<u16>> {
        if let Some(unit) = self.leftover.take() { return Ok(Some(unit)); }
        let mut output = [0; 2];
        match self.read_into(&mut output)? {
            Some(2) => { self.leftover = Some(output[1]); Ok(Some(output[0])) }
            Some(1) => Ok(Some(output[0])),
            _ => Ok(None),
        }
    }
    pub(in crate::connect::registry) fn ready(&mut self) -> Result<'a, bool> { self.ensure_open()?; Ok(self.leftover.is_some() || self.impl_ready()?) }
    pub(in crate::connect::registry) fn close(&mut self) -> Result<'a, ()> {
        if self.closed { return Ok(()); }
        let result = self.source.close(); self.closed = true; result
    }
    fn ensure_open(&self) -> Result<'a, ()> {
        if self.closed { return Err(Failure::Io { stage: Stage::Utf16Read, detail: "Stream closed" }); }
        Ok(())
    }
    fn impl_ready(&mut self) -> Result<'a, bool> { Ok(self.ptr < self.end || self.in_ready()?) }
    fn in_ready(&mut self) -> Result<'a, bool> {
        match self.source.available() { Ok(count) => Ok(count > 0), Err(Failure::Io { .. }) => Ok(false), Err(cause) => Err(cause) }
    }
    fn read_bytes(&mut self) -> Result<'a, Option<usize>> {
        let remaining = self.end - self.ptr;
        self.bytes.values.copy_within(self.ptr..self.end, 0);
        self.ptr = 0; self.end = remaining;
        let result = self.source.read_into(&mut self.bytes.values, remaining);
        match result {
            Ok(Some(0)) => Err(Failure::ZeroRead(Stage::Utf16Read)),
            Ok(Some(count)) => { self.end += count; Ok(Some(self.end)) }
            Ok(None) => Ok(None),
            Err(cause) => Err(cause),
        }
    }
    fn impl_read(&mut self, output: &mut [u16]) -> Result<'a, Option<usize>> {
        let mut written = 0;
        let mut eof = false;
        loop {
            let (count, status) = self.decode_replace(&mut output[written..], eof);
            written += count;
            match status {
                DecodeStatus::Overflow => break,
                DecodeStatus::Underflow => {
                    if eof || written == output.len() { break; }
                    if written > 0 && !self.in_ready()? { break; }
                    if self.read_bytes()?.is_none() {
                        eof = true;
                        if written == 0 && self.ptr == self.end { break; }
                    }
                }
            }
        }
        Ok(if written == 0 && eof { None } else { Some(written) })
    }
    fn decode_replace(&mut self, output: &mut [u16], eof: bool) -> (usize, DecodeStatus) {
        let mut written = 0;
        loop {
            let remaining = self.end - self.ptr;
            if remaining < 2 {
                if remaining == 0 || !eof { return (written, DecodeStatus::Underflow); }
                if written == output.len() { return (written, DecodeStatus::Overflow); }
                output[written] = 0xfffd; self.ptr = self.end;
                return (written + 1, DecodeStatus::Underflow);
            }
            let first = self.word(self.ptr);
            let (unit, consumed, second) = if (0xd800..=0xdbff).contains(&first) {
                if remaining < 4 {
                    if !eof { return (written, DecodeStatus::Underflow); }
                    (0xfffd, remaining, None)
                } else {
                    let low = self.word(self.ptr + 2);
                    if (0xdc00..=0xdfff).contains(&low) {
                        if output.len() - written < 2 { return (written, DecodeStatus::Overflow); }
                        (first, 4, Some(low))
                    } else { (0xfffd, 4, None) }
                }
            } else if (0xdc00..=0xdfff).contains(&first) { (0xfffd, 2, None) }
            else { (first, 2, None) };
            if written == output.len() { return (written, DecodeStatus::Overflow); }
            output[written] = unit; written += 1;
            if let Some(low) = second { output[written] = low; written += 1; }
            self.ptr += consumed;
        }
    }
    fn word(&self, position: usize) -> u16 {
        let bytes: [u8; 2] = self.bytes.values[position..position + 2].try_into().unwrap();
        if self.little { u16::from_le_bytes(bytes) } else { u16::from_be_bytes(bytes) }
    }
}

pub(in crate::connect::registry) struct Utf16Window<'a> {
    reader: Utf16Reader<'a>, buffer: Option<Buffer<u16>>, ptr: usize, end: usize, processed: i64, attached: bool, closed: bool,
}
impl<'a> Utf16Window<'a> {
    pub(in crate::connect::registry) fn new(reader: Utf16Reader<'a>, capacity: usize, context: &mut ConversionContext) -> Result<'a, Self> {
        if capacity == 0 { return Err(Failure::Observation { index: reader.observations_consumed(), expected: Call::Read(ReadRequest { method: ReadMethod::Slice, offset: 0, length: 0 }), detail: "Parser character buffer must have positive observed capacity" }); }
        Ok(Self { reader, buffer: Some(Buffer::new(capacity, context)?), ptr: 0, end: 0, processed: 0, attached: true, closed: false })
    }
    pub(in crate::connect::registry) fn observations_consumed(&self) -> usize { self.reader.observations_consumed() }
    pub(in crate::connect::registry) fn processed(&self) -> i64 { self.processed }
    pub(in crate::connect::registry) fn position(&self) -> usize { self.ptr }
    pub(in crate::connect::registry) fn next(&mut self) -> Result<'a, Option<u16>> {
        if self.closed { return Ok(None); }
        if self.ptr == self.end && !self.load_more()? { return Ok(None); }
        let unit = self.buffer.as_ref().unwrap().values[self.ptr]; self.ptr += 1; Ok(Some(unit))
    }
    fn load_more(&mut self) -> Result<'a, bool> {
        if !self.attached { return Ok(false); }
        self.processed = self.processed.wrapping_add(self.end as i64);
        let count = self.reader.read_into(&mut self.buffer.as_mut().unwrap().values)?;
        if let Some(count) = count.filter(|count| *count > 0) { self.ptr = 0; self.end = count; return Ok(true); }
        self.ptr = 0; self.end = 0;
        self.close_input()?;
        if count == Some(0) { return Err(Failure::ZeroRead(Stage::Utf16Read)); }
        Ok(false)
    }
    fn close_input(&mut self) -> Result<'a, ()> { if self.attached { self.reader.close()?; self.attached = false; } Ok(()) }
    pub(in crate::connect::registry) fn close_input_stage(&mut self) -> Result<'a, ()> {
        if self.closed { return Ok(()); }
        self.ptr = self.ptr.max(self.end); self.closed = true;
        self.close_input()
    }
    pub(in crate::connect::registry) fn release_parser_buffer(&mut self) { self.buffer = None; }
    pub(in crate::connect::registry) fn close(&mut self) -> Result<'a, ()> {
        let result = self.close_input_stage(); self.release_parser_buffer(); result
    }
}

#[cfg(test)]
mod tests;
