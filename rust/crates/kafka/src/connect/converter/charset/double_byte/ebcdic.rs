use super::Mapping;
mod aliases;
mod tables;

#[derive(Clone, Copy)]
enum ShiftState { SingleByte, DoubleByte }

pub(in super::super) fn lookup(name: &str) -> Option<&'static Mapping> {
    let index = aliases::ALIASES.binary_search_by_key(&name, |&(alias, _)| alias).ok()?;
    Some(&tables::TABLES[aliases::ALIASES[index].1])
}

pub(in super::super) fn decode(mapping: &Mapping, mut bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 3);
    let mut state = ShiftState::SingleByte;
    let units = std::iter::from_fn(|| loop {
        let (&first, tail) = bytes.split_first()?;
        bytes = tail;
        match (first, state) {
            (0x0e, ShiftState::SingleByte) => state = ShiftState::DoubleByte,
            (0x0f, ShiftState::DoubleByte) => state = ShiftState::SingleByte,
            (0x0e | 0x0f, _) => return Some(0xfffd),
            (_, ShiftState::SingleByte) => return Some(mapping.single(first)),
            (_, ShiftState::DoubleByte) => {
                let Some((&second, tail)) = bytes.split_first() else { return Some(0xfffd); };
                bytes = tail;
                return Some(mapping.pair(first, second));
            }
        }
    });
    output.extend(char::decode_utf16(units).map(|unit| unit.unwrap_or('\u{fffd}')));
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_names_borrow_static_ebcdic_tables_without_ascii_identity() {
        assert_eq!(tables::TABLES.len(), 6);
        assert_eq!(aliases::ALIASES.len(), 30);
        for pair in aliases::ALIASES.windows(2) { assert!(pair[0].0 < pair[1].0); }
        for &(name, index) in aliases::ALIASES {
            assert!(std::ptr::eq(lookup(name).unwrap(), &tables::TABLES[index]));
        }
        for mapping in tables::TABLES {
            assert_eq!(&mapping.0[..2], &[0x40, 0xfe]);
            assert_eq!(mapping.single(0xc1), 0x41);
            assert_eq!(mapping.single(0xf0), 0x30);
            assert_eq!(mapping.single(0x40), 0x20);
            assert_eq!(mapping.single(0x0e), 0xfffd);
            assert_eq!(mapping.single(0x0f), 0xfffd);
            assert_eq!(mapping.pair(0x40, 0x40), 0x3000);
            assert!((0u8..128).any(|byte| mapping.single(byte) != u16::from(byte)));
            assert_eq!(decode(mapping, &[0x0e]), "");
            assert_eq!(decode(mapping, &[0x0f]), "\u{fffd}");
            assert_eq!(decode(mapping, &[0xc1]), "A");
        }
    }

    #[test]
    fn interleaved_calls_share_mapping_bytes_without_sharing_shift_state() {
        let mapping = lookup("x-ibm933").unwrap();
        assert!(std::ptr::eq(mapping, lookup("cp933").unwrap()));
        assert_eq!(decode(mapping, &[0x0e, 0x40, 0x40]), "\u{3000}");
        assert_eq!(decode(mapping, &[0x40, 0x40]), "  ");
        assert_eq!(decode(mapping, &[0x0e, 0x40]), "\u{fffd}");
        assert_eq!(decode(mapping, &[0xc1]), "A");
        assert_eq!(mapping.0, include_bytes!("subclasses/x-ibm834.bin"));
    }
}
