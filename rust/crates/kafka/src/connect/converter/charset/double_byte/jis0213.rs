use super::Mapping;
mod aliases;
mod tables;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super::super) enum Jis0213 { Standard, Windows }

struct JisMapping(&'static [u8]);

enum Pair { Scalar(u32), Composite(u16, u16) }

pub(in super::super) fn lookup(name: &str) -> Option<Jis0213> {
    let index = aliases::ALIASES.binary_search_by_key(&name, |&(alias, _)| alias).ok()?;
    Some(aliases::ALIASES[index].1)
}

impl JisMapping {
    fn unit(&self, offset: usize) -> u16 {
        u16::from_be_bytes([self.0[offset], self.0[offset + 1]])
    }

    fn single(&self, byte: u8) -> u16 { self.unit(tables::SINGLE + usize::from(byte) * 2) }

    fn bmp(&self, first: u8, second: u8) -> u16 {
        if !(0x40..0xfe).contains(&second) { return 0xfffd; }
        let (offset, row) = match first {
            0x81..=0x9f => (tables::DB1, first - 0x81),
            0xe0..=0xfc => (tables::DB2, first - 0xe0),
            _ => return 0xfffd,
        };
        self.unit(offset + (usize::from(row) * 191 + usize::from(second - 0x40)) * 2)
    }

    fn find_key(&self, offset: usize, count: usize, stride: usize, key: u16) -> Option<usize> {
        let (mut low, mut high) = (0, count);
        while low < high {
            let middle = low + (high - low) / 2;
            match self.unit(offset + middle * stride).cmp(&key) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Some(middle),
            }
        }
        None
    }

    fn extended(&self, first: u8, second: u8) -> Option<Pair> {
        let key = u16::from_be_bytes([first, second]);
        if let Some(index) = self.find_key(tables::SUPPLEMENTARY, tables::SUPPLEMENTARY_COUNT, 2, key) {
            let unit = self.unit(tables::SUPPLEMENTARY + (tables::SUPPLEMENTARY_COUNT + index) * 2);
            return Some(Pair::Scalar(0x20000 + u32::from(unit)));
        }
        let index = self.find_key(tables::COMPOSITE, tables::COMPOSITE_COUNT, 6, key)?;
        let offset = tables::COMPOSITE + index * 6;
        Some(Pair::Composite(self.unit(offset + 2), self.unit(offset + 4)))
    }
}

impl Jis0213 {
    fn pair(self, first: u8, second: u8) -> Option<Pair> {
        if self == Self::Windows {
            let unit = tables::MS932.pair(first, second);
            if unit != 0xfffd { return Some(Pair::Scalar(u32::from(unit))); }
        }
        let unit = tables::JIS.bmp(first, second);
        if unit != 0xfffd { return Some(Pair::Scalar(u32::from(unit))); }
        tables::JIS.extended(first, second)
    }

    pub(in super::super) fn decode(self, mut bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len() * 3);
        while let Some(&first) = bytes.first() {
            let single = tables::JIS.single(first);
            if single != 0xfffd {
                output.push(char::from_u32(u32::from(single)).unwrap());
                bytes = &bytes[1..];
                continue;
            }
            let Some(&second) = bytes.get(1) else { output.push('\u{fffd}'); break; };
            match self.pair(first, second) {
                Some(Pair::Scalar(value)) => output.push(char::from_u32(value).unwrap()),
                Some(Pair::Composite(first, second)) => {
                    output.push(char::from_u32(u32::from(first)).unwrap());
                    output.push(char::from_u32(u32::from(second)).unwrap());
                },
                None => {
                    output.push('\u{fffd}');
                    if tables::JIS.single(second) != 0xfffd {
                        bytes = &bytes[1..];
                        continue;
                    }
                },
            }
            bytes = &bytes[2..];
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thirteen_provider_names_select_two_immutable_configurations() {
        assert_eq!(aliases::ALIASES.len(), 13);
        for pair in aliases::ALIASES.windows(2) { assert!(pair[0].0 < pair[1].0); }
        for &(name, variant) in aliases::ALIASES { assert_eq!(lookup(name), Some(variant)); }
        assert_eq!(tables::MS932.0, include_bytes!("windows-31j.bin"));
        assert_eq!(tables::JIS.0.len(), 26_566);
        assert_eq!(tables::JIS.single(0x5c), 0x5c);
        assert_eq!(tables::JIS.single(0x7e), 0x7e);
        for first in 0u8..=255 {
            assert_eq!(tables::JIS.bmp(first, 0x3f), 0xfffd);
            assert_eq!(tables::JIS.bmp(first, 0xfe), 0xfffd);
            assert_eq!(tables::JIS.bmp(first, 0xff), 0xfffd);
        }
    }

    #[test]
    fn every_supplementary_and_composite_key_resolves_to_its_source_units() {
        for index in 0..tables::SUPPLEMENTARY_COUNT {
            let key = tables::JIS.unit(tables::SUPPLEMENTARY + index * 2);
            let [first, second] = key.to_be_bytes();
            let expected = 0x20000 + u32::from(tables::JIS.unit(tables::SUPPLEMENTARY + (tables::SUPPLEMENTARY_COUNT + index) * 2));
            assert!(matches!(tables::JIS.extended(first, second), Some(Pair::Scalar(value)) if value == expected));
        }
        for index in 0..tables::COMPOSITE_COUNT {
            let offset = tables::COMPOSITE + index * 6;
            let [first, second] = tables::JIS.unit(offset).to_be_bytes();
            assert!(matches!(tables::JIS.extended(first, second), Some(Pair::Composite(a, b)) if a == tables::JIS.unit(offset + 2) && b == tables::JIS.unit(offset + 4)));
        }
        assert!(tables::JIS.extended(0, 0).is_none());
        assert!(tables::JIS.extended(0xff, 0xff).is_none());
    }
}
