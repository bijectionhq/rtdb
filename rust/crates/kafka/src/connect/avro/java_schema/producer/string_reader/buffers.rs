use super::*;

pub(in crate::connect) struct TokenBuffer { units: Vec<u16>, charge: Arc<MemoryReservation> }
impl TokenBuffer {
    pub(in crate::connect) fn new(length: usize, context: &mut ConversionContext) -> Result<Self> {
        let charge = context.reserve(add(mul(mul(length, size_of::<u16>())?, 16)?, 4096)?)?;
        let mut units = Vec::new();
        units.try_reserve_exact(length).map_err(allocation)?;
        units.resize(length, 0);
        Ok(Self { units, charge })
    }
    pub(in crate::connect) fn len(&self) -> usize { self.units.len() }
    pub(super) fn data(&self) -> &[u16] { &self.units }
    pub(super) fn data_mut(&mut self) -> &mut [u16] { &mut self.units }
    fn retain(&self, context: &mut ConversionContext) -> Result<()> { context.retain(Arc::clone(&self.charge)) }
}

/// A parser's capture lasts through close or native abandonment; storage stays charged through final use.
pub(in crate::connect) trait InputBuffers {
    fn profile(&self) -> Profile;
    fn acquire(&mut self, _: &mut ConversionContext) -> Result<()> { Ok(()) }
    fn take(&mut self, minimum: usize, context: &mut ConversionContext) -> Result<TokenBuffer>;
    fn give(&mut self, buffer: TokenBuffer, context: &mut ConversionContext) -> Result<()>;
    fn close(&mut self, _: &mut ConversionContext) -> Result<()> { Ok(()) }
    fn abandon(&mut self) {}
}

pub(in crate::connect) struct TokenRecycler { profile: Profile, idle: Option<TokenBuffer> }
impl TokenRecycler {
    pub(in crate::connect) fn empty(profile: Profile) -> Self { Self { profile, idle: None } }
}
impl InputBuffers for TokenRecycler {
    fn profile(&self) -> Profile { self.profile }
    fn take(&mut self, minimum: usize, context: &mut ConversionContext) -> Result<TokenBuffer> {
        let minimum = minimum.max(4000);
        let previous = self.idle.take();
        if let Some(buffer) = previous.as_ref().filter(|buffer| buffer.len() >= minimum) {
            buffer.retain(context)?;
            return Ok(previous.unwrap());
        }
        let replacement = TokenBuffer::new(minimum, context)?;
        drop(previous);
        Ok(replacement)
    }
    fn give(&mut self, buffer: TokenBuffer, _: &mut ConversionContext) -> Result<()> {
        let replace = self.profile == Profile::Jackson216
            || self.idle.as_ref().is_none_or(|old| old.len() < buffer.len());
        if replace { self.idle = Some(buffer); }
        Ok(())
    }
}

pub(super) struct StringInput<'a> {
    source: rtdb_types::java_text::Utf16Units<'a>,
    buffer: TokenBuffer,
    direct: bool,
    pointer: usize,
    end: usize,
}
impl<'a> StringInput<'a> {
    pub(super) fn direct(text: JavaText<'a>, mut buffer: TokenBuffer) -> Result<Self> {
        if buffer.len() < text.len_units().max(4000) { return Err(buffer_profile()); }
        for (slot, unit) in buffer.data_mut().iter_mut().zip(text.units()) { *slot = unit; }
        Ok(Self { source: text.units(), buffer, direct: true, pointer: 0, end: text.len_units() })
    }
    pub(super) fn reader(text: JavaText<'a>, buffer: TokenBuffer) -> Result<Self> {
        if buffer.len() < 4000 { return Err(buffer_profile()); }
        Ok(Self { source: text.units(), buffer, direct: false, pointer: 0, end: 0 })
    }
    pub(super) fn peek(&mut self) -> Option<u16> {
        if self.pointer == self.end {
            if self.direct { return None; }
            self.pointer = 0;
            self.end = 0;
            for slot in self.buffer.data_mut() {
                let Some(unit) = self.source.next() else { break; };
                *slot = unit;
                self.end += 1;
            }
        }
        self.buffer.data().get(self.pointer).copied().filter(|_| self.pointer < self.end)
    }
    pub(super) fn next(&mut self) -> Option<u16> { let unit = self.peek()?; self.pointer += 1; Some(unit) }
    pub(super) fn buffered_tail(&self) -> &[u16] { &self.buffer.data()[self.pointer..self.end] }
    pub(super) fn into_buffer(self) -> TokenBuffer { self.buffer }
}
fn buffer_profile() -> Anomaly {
    Anomaly::unsupported("rtdb/avro-reader-buffer-profile", "Captured source token buffer does not satisfy its requested size")
}
