use super::Mapping;
use super::probe::{DecodedUnit, DecodeError, ProbeStop, StrictProbe};
mod aliases;
mod tables;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super::super) enum EucJp { Standard, Linux, Solaris }

pub(in super::super) fn lookup(name: &str) -> Option<EucJp> {
    let index = aliases::ALIASES.binary_search_by_key(&name, |&(alias, _)| alias).ok()?;
    Some(aliases::ALIASES[index].1)
}

impl EucJp {
    fn decode_unit(self, bytes: &[u8]) -> std::result::Result<Option<DecodedUnit>, DecodeError> {
        let Some(&first) = bytes.first() else { return Ok(None); };
        if first < 0x80 { return Ok(Some(DecodedUnit { unit: u16::from(first), bytes: 1 })); }
        let count = if first == 0x8f { 3 } else { 2 };
        if bytes.len() < count { return Ok(None); }
        let unit = if first == 0x8f {
            match self {
                Self::Standard => jis_pair(&tables::JIS0212, bytes[1], bytes[2]),
                Self::Linux => 0xfffd,
                Self::Solaris => jis_pair(&tables::SOLARIS0212, bytes[1], bytes[2]),
            }
        } else {
            let value = if first == 0x8e {
                let second = usize::from(bytes[1]);
                if second < 0x80 { 0xfffd }
                else { u16::from_be_bytes([tables::JIS0201[second * 2], tables::JIS0201[second * 2 + 1]]) }
            } else { jis_pair(&tables::JIS0208, first, bytes[1]) };
            if value == 0xfffd && self == Self::Solaris {
                jis_pair(&tables::SOLARIS0208, first, bytes[1])
            } else { value }
        };
        if unit == 0xfffd { Err(DecodeError::Unmappable { bytes: count }) }
        else { Ok(Some(DecodedUnit { unit, bytes: count })) }
    }

    pub(in super::super) fn decode(self, bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len() * 3);
        self.decode_into(bytes, &mut output);
        output
    }

    pub(super) fn decode_into(self, mut bytes: &[u8], output: &mut String) {
        let units = std::iter::from_fn(|| {
            if bytes.is_empty() { return None; }
            let (unit, count) = match self.decode_unit(bytes) {
                Ok(Some(decoded)) => (decoded.unit, decoded.bytes),
                Ok(None) => (0xfffd, bytes.len()),
                Err(error) => (0xfffd, error.bytes()),
            };
            bytes = &bytes[count..];
            Some(unit)
        });
        output.extend(char::decode_utf16(units).map(|unit| unit.unwrap_or('\u{fffd}')));
    }

    pub(super) fn probe(self, bytes: &[u8], mut emit: impl FnMut(u16)) -> StrictProbe {
        let (mut consumed, mut utf16_units) = (0, 0);
        while consumed < bytes.len() {
            match self.decode_unit(&bytes[consumed..]) {
                Ok(Some(decoded)) => {
                    emit(decoded.unit);
                    consumed += decoded.bytes;
                    utf16_units += 1;
                },
                Ok(None) => break,
                Err(error) => return StrictProbe { consumed, utf16_units, stop: ProbeStop::Error(error) },
            }
        }
        StrictProbe { consumed, utf16_units, stop: ProbeStop::Underflow }
    }
}

fn jis_pair(mapping: &Mapping, first: u8, second: u8) -> u16 {
    let (Some(first), Some(second)) = (first.checked_sub(0x80), second.checked_sub(0x80)) else { return 0xfffd; };
    mapping.pair(first, second)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_names_select_three_immutable_decoders_without_replacing_existing_mapping_bytes() {
        assert_eq!(aliases::ALIASES.len(), 14);
        for pair in aliases::ALIASES.windows(2) { assert!(pair[0].0 < pair[1].0); }
        for &(name, variant) in aliases::ALIASES { assert_eq!(lookup(name), Some(variant)); }
        assert_eq!(tables::JIS0208.0, include_bytes!("subclasses/x-jis0208.bin"));
        assert_eq!(tables::JIS0212.0, include_bytes!("subclasses/jis_x0212-1990.bin"));
        assert_eq!(tables::JIS0201, super::super::super::single_byte::lookup("jis_x0201").unwrap());
        for mapping in [&tables::JIS0208, &tables::JIS0212, &tables::SOLARIS0208, &tables::SOLARIS0212] {
            assert_eq!(&mapping.0[..2], &[0x21, 0x7e]);
            assert!(mapping.row(0x0e).is_none());
            for byte in 0u8..=255 { assert_eq!(mapping.single(byte), 0xfffd); }
        }
    }

    #[test]
    fn variants_keep_ascii_and_halfwidth_symbols_distinct_from_standalone_jis_decoding() {
        for variant in [EucJp::Standard, EucJp::Linux, EucJp::Solaris] {
            assert_eq!(variant.decode(b"\\~"), "\\~");
            assert_eq!(variant.decode(&[0x8e, 0xa1, 0x8e, 0xdf]), "｡ﾟ");
            assert_eq!(variant.decode(&[0x21, 0x21]), "!!");
            assert_eq!(variant.decode(&[0xa1, 0xa1]), "\u{3000}");
            assert_eq!(variant.decode(&[0x8f, 0xa1]), "\u{fffd}");
            assert_eq!(variant.decode(b"A"), "A");
        }
    }
}
