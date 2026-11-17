mod aliases;
mod tables;

pub(in super::super) struct Mapping(&'static [u8]);

pub(super) fn lookup(name: &str) -> Option<&'static Mapping> {
    let index = aliases::ALIASES.binary_search_by_key(&name, |&(alias, _)| alias).ok()?;
    Some(&tables::TABLES[aliases::ALIASES[index].1])
}

impl Mapping {
    fn unit(&self, offset: usize) -> u16 {
        u16::from_be_bytes([self.0[offset], self.0[offset + 1]])
    }

    fn single(&self, byte: u8) -> u16 { self.unit(2 + usize::from(byte) * 2) }

    fn row(&self, byte: u8) -> Option<usize> {
        let index = self.unit(514 + usize::from(byte) * 2);
        (index != 0xffff).then_some(usize::from(index))
    }

    fn pair(&self, first: u8, second: u8) -> u16 {
        let (minimum, maximum) = (self.0[0], self.0[1]);
        if !(minimum..=maximum).contains(&second) { return 0xfffd; }
        let Some(row) = self.row(first) else { return 0xfffd; };
        let index = row * (usize::from(maximum) - usize::from(minimum) + 1) + usize::from(second - minimum);
        self.unit(1026 + index * 2)
    }

    pub(super) fn decode(&self, mut bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len() * 3);
        let units = std::iter::from_fn(|| {
            let (&first, tail) = bytes.split_first()?;
            bytes = tail;
            let single = self.single(first);
            if single != 0xfffd { return Some(single); }
            let Some(&second) = bytes.first() else { return Some(0xfffd); };
            let pair = self.pair(first, second);
            // OpenJDK25 DoubleByte.Decoder.crMalformedOrUnmappable uses lead-row identity.
            if pair != 0xfffd || (self.row(first).is_some() && self.row(second).is_none() && self.single(second) == 0xfffd) {
                bytes = &bytes[1..];
            }
            Some(pair)
        });
        output.extend(char::decode_utf16(units).map(|unit| unit.unwrap_or('\u{fffd}')));
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_names_borrow_complete_static_mapping_rows() {
        assert_eq!(tables::TABLES.len(), 20);
        assert_eq!(aliases::ALIASES.len(), 100);
        for pair in aliases::ALIASES.windows(2) { assert!(pair[0].0 < pair[1].0); }
        for &(name, index) in aliases::ALIASES {
            assert!(std::ptr::eq(lookup(name).unwrap(), &tables::TABLES[index]));
        }
        for name in ["shift_jis", "windows-31j"] {
            for byte in 0u8..128 { assert_eq!(lookup(name).unwrap().single(byte), u16::from(byte)); }
        }
        for table in tables::TABLES {
            let width = usize::from(table.0[1] - table.0[0]) + 1;
            assert_eq!((table.0.len() - 1026) % (width * 2), 0);
            let row_count = (table.0.len() - 1026) / (width * 2);
            assert!(table.0[0] > 0);
            assert_eq!(table.single(0), 0);
            for byte in 0u8..=255 {
                if let Some(row) = table.row(byte) { assert!(row < row_count); }
            }
        }
    }
}
