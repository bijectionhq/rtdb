use super::super::{Failure, FailureKind, Profile, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Encoding { Utf8, Utf16 { little: bool }, Utf32 { little: bool } }

#[derive(Clone, Copy)]
pub(super) enum Utf8Demand { Name, Value, Skip }

#[derive(Clone, Copy)]
pub(super) struct Input<'a> { bytes: &'a [u8], pub(super) at: usize, pending: Option<u16>, pub(super) encoding: Encoding }

impl<'a> Input<'a> {
    pub(super) fn bootstrap(bytes: &'a [u8]) -> Result<Self> {
        let mut at = 0;
        let mut encoding = Encoding::Utf8;
        if bytes.len() >= 4 {
            let quad = u32::from_be_bytes(bytes[..4].try_into().unwrap());
            match quad {
                0x0000feff => { encoding = Encoding::Utf32 { little: false }; at = 4; }
                0xfffe0000 => { encoding = Encoding::Utf32 { little: true }; at = 4; }
                0xfeff0000 | 0x0000fffe => return Err(Self::invalid(0, "Unsupported UCS4 byte order")),
                _ if quad >> 16 == 0xfeff => { encoding = Encoding::Utf16 { little: false }; at = 2; }
                _ if quad >> 16 == 0xfffe => { encoding = Encoding::Utf16 { little: true }; at = 2; }
                _ if quad >> 8 == 0xefbbbf => { at = 3; }
                _ if quad >> 8 == 0 => encoding = Encoding::Utf32 { little: false },
                _ if quad & 0x00ff_ffff == 0 => encoding = Encoding::Utf32 { little: true },
                _ if quad & !0x00ff_0000 == 0 || quad & !0x0000_ff00 == 0 => return Err(Self::invalid(0, "Unsupported UCS4 byte order")),
                _ if quad >> 16 & 0xff00 == 0 => encoding = Encoding::Utf16 { little: false },
                _ if quad >> 16 & 0xff == 0 => encoding = Encoding::Utf16 { little: true },
                _ => (),
            }
        } else if bytes.len() >= 2 {
            let pair = u16::from_be_bytes(bytes[..2].try_into().unwrap());
            if pair & 0xff00 == 0 { encoding = Encoding::Utf16 { little: false }; }
            else if pair & 0xff == 0 { encoding = Encoding::Utf16 { little: true }; }
        }
        Ok(Self { bytes, at, pending: None, encoding })
    }
    pub(super) fn remaining(&self) -> &'a [u8] { &self.bytes[self.at..] }
    pub(super) fn range(&self, start: usize, end: usize) -> &'a [u8] { &self.bytes[start..end] }
    pub(super) fn slice(bytes: &'a [u8], encoding: Encoding) -> Self { Self { bytes, at: 0, pending: None, encoding } }
    pub(super) fn peek(&self) -> Result<Option<u16>> { let mut copy = *self; copy.next() }
    pub(super) fn next(&mut self) -> Result<Option<u16>> {
        if let Some(unit) = self.pending.take() { return Ok(Some(unit)); }
        if self.at == self.bytes.len() { return Ok(None); }
        match self.encoding {
            Encoding::Utf8 => { let unit = u16::from(self.bytes[self.at]); self.at += 1; Ok(Some(unit)) }
            Encoding::Utf16 { little } => {
                let Some(first) = self.u16(little) else { self.at = self.bytes.len(); return Ok(Some(0xfffd)); };
                if (0xd800..=0xdbff).contains(&first) {
                    match self.u16(little) {
                        Some(second) if (0xdc00..=0xdfff).contains(&second) => { self.pending = Some(second); Ok(Some(first)) }
                        Some(_) => Ok(Some(0xfffd)),
                        None => { self.at = self.bytes.len(); Ok(Some(0xfffd)) }
                    }
                } else if (0xdc00..=0xdfff).contains(&first) { Ok(Some(0xfffd)) }
                else { Ok(Some(first)) }
            }
            Encoding::Utf32 { little } => {
                if self.remaining().len() < 4 { return Err(Self::invalid(self.at, "Truncated UTF32 input")); }
                let bytes: [u8; 4] = self.remaining()[..4].try_into().unwrap();
                self.at += 4;
                let point = if little { u32::from_le_bytes(bytes) } else { u32::from_be_bytes(bytes) };
                if point > 0x10ffff { return Err(Self::invalid(self.at - 4, "UTF32 character exceeds Unicode range")); }
                if point <= 0xffff { Ok(Some(point as u16)) }
                else {
                    let point = point - 0x10000;
                    self.pending = Some(0xdc00 | (point & 0x3ff) as u16);
                    Ok(Some(0xd800 + (point >> 10) as u16))
                }
            }
        }
    }
    fn u16(&mut self, little: bool) -> Option<u16> {
        let bytes: [u8; 2] = self.remaining().get(..2)?.try_into().unwrap();
        self.at += 2;
        Some(if little { u16::from_le_bytes(bytes) } else { u16::from_be_bytes(bytes) })
    }
    pub(super) fn utf8_character(&mut self, first: u16, profile: Profile, demand: Utf8Demand) -> Result<(u16, Option<u16>)> {
        if self.encoding != Encoding::Utf8 || first < 0x80 { return Ok((first, None)); }
        let first = first as u8;
        let (count, mut point) = if first & 0xe0 == 0xc0 { (1, i32::from(first & 0x1f)) }
            else if first & 0xf0 == 0xe0 { (2, i32::from(first & 0x0f)) }
            else if first & 0xf8 == 0xf0 { (3, i32::from(first & 7)) }
            else { return Err(Self::invalid(self.at - 1, "Invalid UTF8 initial byte")); };
        for _ in 0..count {
            let at = self.at;
            let next = self.next()?.ok_or_else(|| Self::invalid(at, "Truncated UTF8 character"))?;
            if next & 0xc0 != 0x80 { return Err(Self::invalid(at, "Invalid UTF8 continuation byte")); }
            point = (point << 6) | i32::from(next & 0x3f);
        }
        if count == 2 && !matches!(demand, Utf8Demand::Skip) && profile == Profile::Jackson221 && (0xd800..=0xdfff).contains(&point) {
            return Err(Self::invalid(self.at - 3, "Illegal UTF8 surrogate character"));
        }
        if count == 3 {
            let point = point - 0x10000;
            let high = match demand {
                Utf8Demand::Value => 0xd800 | (point >> 10),
                Utf8Demand::Name | Utf8Demand::Skip => 0xd800 + (point >> 10),
            };
            Ok((high as u16, Some(0xdc00 | (point & 0x3ff) as u16)))
        } else { Ok((point as u16, None)) }
    }
    fn invalid(at: usize, detail: &'static str) -> Failure { Failure::at(FailureKind::Syntax, at, detail) }
}
