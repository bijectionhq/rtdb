mod aliases;
use probe::{DecodedUnit, DecodeError, ProbeStop, StrictProbe};
pub(super) mod big5;
pub(super) mod ebcdic;
pub(super) mod euc_jp;
pub(super) mod euc_tw;
pub(super) mod gb18030;
pub(super) mod hkscs;
pub(super) mod ibm_euc;
pub(super) mod iso2022cnkr;
pub(super) mod iso2022jp;
pub(super) mod jis0213;
mod probe;
pub(super) mod subclasses;
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

    pub(super) fn decode(&self, bytes: &[u8]) -> String {
        self.decode_with_pairs(bytes, |first, second| self.pair(first, second))
    }

    fn decode_into(&self, bytes: &[u8], output: &mut String) {
        self.decode_with_pairs_into(bytes, |first, second| self.pair(first, second), output);
    }

    fn decode_with_pairs(&self, bytes: &[u8], pair: impl Fn(u8, u8) -> u16) -> String {
        let mut output = String::with_capacity(bytes.len() * 3);
        self.decode_with_pairs_into(bytes, pair, &mut output);
        output
    }

    fn decode_unit(&self, bytes: &[u8], pair: &impl Fn(u8, u8) -> u16) -> std::result::Result<Option<DecodedUnit>, DecodeError> {
        let Some(&first) = bytes.first() else { return Ok(None); };
        let single = self.single(first);
        if single != 0xfffd { return Ok(Some(DecodedUnit { unit: single, bytes: 1 })); }
        let Some(&second) = bytes.get(1) else { return Ok(None); };
        let unit = pair(first, second);
        if unit != 0xfffd { return Ok(Some(DecodedUnit { unit, bytes: 2 })); }
        if self.row(first).is_none() || self.row(second).is_some() || self.single(second) != 0xfffd {
            Err(DecodeError::Malformed { bytes: 1 })
        } else { Err(DecodeError::Unmappable { bytes: 2 }) }
    }

    fn decode_with_pairs_into(&self, mut bytes: &[u8], pair: impl Fn(u8, u8) -> u16, output: &mut String) {
        let units = std::iter::from_fn(|| {
            if bytes.is_empty() { return None; }
            let (unit, count) = match self.decode_unit(bytes, &pair) {
                Ok(Some(decoded)) => (decoded.unit, decoded.bytes),
                Ok(None) => (0xfffd, bytes.len()),
                Err(error) => (0xfffd, error.bytes()),
            };
            bytes = &bytes[count..];
            Some(unit)
        });
        output.extend(char::decode_utf16(units).map(|unit| unit.unwrap_or('\u{fffd}')));
    }

    fn probe(&self, bytes: &[u8]) -> StrictProbe {
        let (mut consumed, mut utf16_units) = (0, 0);
        while consumed < bytes.len() {
            match self.decode_unit(&bytes[consumed..], &|first, second| self.pair(first, second)) {
                Ok(Some(decoded)) => { consumed += decoded.bytes; utf16_units += 1; },
                Ok(None) => break,
                Err(error) => return StrictProbe { consumed, utf16_units, stop: ProbeStop::Error(error) },
            }
        }
        StrictProbe { consumed, utf16_units, stop: ProbeStop::Underflow }
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
