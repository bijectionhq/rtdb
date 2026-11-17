use super::Mapping;
mod aliases;
mod tables;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super::super) enum EucJp { Standard, Linux, Solaris }

pub(in super::super) fn lookup(name: &str) -> Option<EucJp> {
    let index = aliases::ALIASES.binary_search_by_key(&name, |&(alias, _)| alias).ok()?;
    Some(aliases::ALIASES[index].1)
}

impl EucJp {
    pub(in super::super) fn decode(self, mut bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len() * 3);
        let units = std::iter::from_fn(|| {
            let (&first, tail) = bytes.split_first()?;
            bytes = tail;
            if first < 0x80 { return Some(u16::from(first)); }
            let count = if first == 0x8f { 2 } else { 1 };
            if bytes.len() < count { bytes = &[]; return Some(0xfffd); }
            let (trails, tail) = bytes.split_at(count);
            bytes = tail;
            Some(if first == 0x8f {
                match self {
                    Self::Standard => jis_pair(&tables::JIS0212, trails[0], trails[1]),
                    Self::Linux => 0xfffd,
                    Self::Solaris => jis_pair(&tables::SOLARIS0212, trails[0], trails[1]),
                }
            } else {
                let value = if first == 0x8e {
                    let second = usize::from(trails[0]);
                    if second < 0x80 { 0xfffd }
                    else { u16::from_be_bytes([tables::JIS0201[second * 2], tables::JIS0201[second * 2 + 1]]) }
                } else { jis_pair(&tables::JIS0208, first, trails[0]) };
                if value == 0xfffd && self == Self::Solaris {
                    jis_pair(&tables::SOLARIS0208, first, trails[0])
                } else { value }
            })
        });
        output.extend(char::decode_utf16(units).map(|unit| unit.unwrap_or('\u{fffd}')));
        output
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
