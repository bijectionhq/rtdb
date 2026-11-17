mod aliases;
mod tables;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super::super) enum IbmEuc { Ibm29626c, Ibm33722 }

struct Tables(&'static [u8; 35_852]);
const G1_START: usize = 160;
const G2_START: usize = G1_START + 94 * 94;
const G3_START: usize = G2_START + 94;

pub(in super::super) fn lookup(name: &str) -> Option<IbmEuc> {
    let index = aliases::ALIASES.binary_search_by_key(&name, |&(alias, _)| alias).ok()?;
    Some(aliases::ALIASES[index].1)
}

impl IbmEuc {
    pub(in super::super) fn decode(self, mut bytes: &[u8]) -> String {
        let tables = match self { Self::Ibm29626c => &tables::IBM29626C, Self::Ibm33722 => &tables::IBM33722 };
        let mut output = String::with_capacity(bytes.len() * 3);
        let units = std::iter::from_fn(|| {
            let (&first, tail) = bytes.split_first()?;
            bytes = tail;
            if first == 0x8f {
                if bytes.len() < 2 { bytes = &[]; return Some(0xfffd); }
                let second = bytes[0];
                bytes = &bytes[1..];
                if self == Self::Ibm33722 && !(0xa1..=0xfe).contains(&second) { return Some(0xfffd); }
                let third = bytes[0];
                bytes = &bytes[1..];
                return Some(tables.pair(G3_START, second, third));
            }
            if first != 0x8e && first < 0xa0 { return Some(tables.unit(usize::from(first))); }
            if self == Self::Ibm33722 && matches!(first, 0xa0 | 0xff) { return Some(0xfffd); }
            let Some((&second, tail)) = bytes.split_first() else { return Some(0xfffd); };
            bytes = tail;
            Some(if first == 0x8e {
                if (0xa1..=0xfe).contains(&second) { tables.unit(G2_START + usize::from(second - 0xa1)) }
                else { 0xfffd }
            } else { tables.pair(G1_START, first, second) })
        });
        output.extend(char::decode_utf16(units).map(|unit| unit.unwrap_or('\u{fffd}')));
        output
    }
}

impl Tables {
    fn unit(&self, index: usize) -> u16 { u16::from_be_bytes([self.0[index * 2], self.0[index * 2 + 1]]) }

    fn pair(&self, offset: usize, first: u8, second: u8) -> u16 {
        if !(0xa1..=0xfe).contains(&first) || !(0xa1..=0xfe).contains(&second) { return 0xfffd; }
        self.unit(offset + usize::from(first - 0xa1) * 94 + usize::from(second - 0xa1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_names_select_two_immutable_decoder_variants() {
        assert_eq!(aliases::ALIASES.len(), 13);
        for pair in aliases::ALIASES.windows(2) { assert!(pair[0].0 < pair[1].0); }
        for &(name, variant) in aliases::ALIASES { assert_eq!(lookup(name), Some(variant)); }
        for tables in [&tables::IBM29626C, &tables::IBM33722] {
            assert_eq!(tables.unit(0x8e), 0xfffd);
            assert_eq!(tables.unit(0x8f), 0xfffd);
            assert_eq!(tables.pair(G1_START, 0xa1, 0xa1), 0x3000);
            assert_eq!(tables.pair(G3_START, 0xf3, 0xb8), 0x2116);
            assert_eq!(tables.pair(G1_START, 0xf5, 0xa1), 0xe000);
            assert_eq!(tables.pair(G3_START, 0xf5, 0xa1), 0xe3ac);
        }
    }

    #[test]
    fn interleaved_variants_keep_symbol_and_error_consumption_differences() {
        assert_eq!(IbmEuc::Ibm29626c.decode(b"\\~"), "\\~");
        assert_eq!(IbmEuc::Ibm33722.decode(b"\\~"), "¥‾");
        assert_eq!(IbmEuc::Ibm29626c.decode(&[0x8f, b'A', b'B']), "\u{fffd}");
        assert_eq!(IbmEuc::Ibm33722.decode(&[0x8f, b'A', b'B']), "\u{fffd}B");
        assert_eq!(IbmEuc::Ibm33722.decode(&[0x8f, b'A']), "\u{fffd}");
        assert_eq!(IbmEuc::Ibm29626c.decode(&[0x8f, 0xa2, 0xb7]), "～");
        assert_eq!(IbmEuc::Ibm33722.decode(&[0x8f, 0xa2, 0xb7]), "\u{fffd}");
    }
}
