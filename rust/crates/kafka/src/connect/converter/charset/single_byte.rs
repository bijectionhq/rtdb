mod aliases;

static TABLES: &[[u8; 512]] = include_bytes!("single_byte/java25-single-byte.bin").as_chunks::<512>().0;

pub(super) fn lookup(name: &str) -> Option<&'static [u8; 512]> {
    let index = aliases::ALIASES.binary_search_by_key(&name, |&(alias, _)| alias).ok()?;
    Some(&TABLES[aliases::ALIASES[index].1])
}

pub(super) fn decode(table: &[u8; 512], bytes: &[u8]) -> String {
    let units = bytes.iter().map(|byte| {
        let offset = usize::from(*byte) * 2;
        u16::from_be_bytes([table[offset], table[offset + 1]])
    });
    let mut output = String::with_capacity(bytes.len() * 3);
    output.extend(char::decode_utf16(units).map(|unit| unit.unwrap_or('\u{fffd}')));
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping_units_combine_adjacent_surrogates_at_the_native_string_boundary() {
        let mut table = [0; 512];
        for (index, unit) in [0xd83du16, 0xde00, 0xd800, 0xfffd, 0x0061].into_iter().enumerate() {
            table[index * 2..index * 2 + 2].copy_from_slice(&unit.to_be_bytes());
        }
        assert_eq!(decode(&table, &[0, 1, 2, 4, 1, 0, 3, 1]), "😀\u{fffd}a\u{fffd}\u{fffd}\u{fffd}\u{fffd}");
        assert_eq!(decode(&table, &[0]), "\u{fffd}");
        assert_eq!(decode(&table, &[1]), "\u{fffd}");
    }

    #[test]
    fn all_aliases_borrow_complete_immutable_tables() {
        assert_eq!(TABLES.len(), 102);
        assert_eq!(aliases::ALIASES.len(), 589);
        for pair in aliases::ALIASES.windows(2) { assert!(pair[0].0 < pair[1].0); }
        for &(name, index) in aliases::ALIASES {
            assert!(index < TABLES.len());
            assert!(std::ptr::eq(lookup(name).unwrap(), &TABLES[index]));
        }
    }
}
