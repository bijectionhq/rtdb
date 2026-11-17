use super::Mapping;
mod aliases;
mod tables;

#[derive(Clone, Copy)]
pub(in super::super::super) enum Decoder {
    DbcsOnly(&'static Mapping),
    EucSimple(&'static Mapping),
}

pub(in super::super) fn lookup(name: &str) -> Option<Decoder> {
    let index = aliases::ALIASES.binary_search_by_key(&name, |&(alias, _)| alias).ok()?;
    Some(tables::DECODERS[aliases::ALIASES[index].1])
}

impl Decoder {
    pub(in super::super) fn decode(self, bytes: &[u8]) -> String {
        match self {
            Self::DbcsOnly(mapping) => decode_dbcs_only(mapping, bytes),
            Self::EucSimple(mapping) => decode_euc_simple(mapping, bytes),
        }
    }
}

fn decode_dbcs_only(mapping: &Mapping, bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 3);
    // OpenJDK25 Decoder_DBCSONLY uses an all-unmappable single-byte table in its inherited ArrayDecoder path.
    let units = bytes.chunks(2).map(|pair| match pair {
        [first, second] => mapping.pair(*first, *second),
        _ => 0xfffd,
    });
    output.extend(char::decode_utf16(units).map(|unit| unit.unwrap_or('\u{fffd}')));
    output
}

fn decode_euc_simple(mapping: &Mapping, mut bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 3);
    let units = std::iter::from_fn(|| {
        let (&first, tail) = bytes.split_first()?;
        bytes = tail;
        let single = mapping.single(first);
        if single != 0xfffd { return Some(single); }
        let Some(&second) = bytes.first() else { return Some(0xfffd); };
        let pair = mapping.pair(first, second);
        // OpenJDK25 Decoder_EUC_SIM retries the second byte only after unmappable SS2/SS3 prefixes.
        if pair != 0xfffd || !matches!(first, 0x8e | 0x8f) { bytes = &bytes[1..]; }
        Some(pair)
    });
    output.extend(char::decode_utf16(units).map(|unit| unit.unwrap_or('\u{fffd}')));
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_subclasses_borrow_static_source_tables_with_distinct_decoder_kinds() {
        assert_eq!(tables::DECODERS.len(), 6);
        assert_eq!(tables::MAPPINGS.len(), 6);
        assert_eq!(aliases::ALIASES.len(), 36);
        for pair in aliases::ALIASES.windows(2) { assert!(pair[0].0 < pair[1].0); }
        for &(name, index) in aliases::ALIASES {
            match (lookup(name).unwrap(), tables::DECODERS[index]) {
                (Decoder::DbcsOnly(actual), Decoder::DbcsOnly(expected)) | (Decoder::EucSimple(actual), Decoder::EucSimple(expected)) => {
                    assert!(std::ptr::eq(actual, expected));
                    assert!(std::ptr::eq(actual, &tables::MAPPINGS[index]));
                }
                _ => panic!("decoder kind differs for {name}"),
            }
        }
        for &decoder in tables::DECODERS {
            match decoder {
                Decoder::DbcsOnly(_) => {
                    for byte in 0u8..=255 { assert_eq!(decoder.decode(&[byte]), "\u{fffd}"); }
                }
                Decoder::EucSimple(mapping) => {
                    for byte in 0u8..=255 {
                        assert_eq!(mapping.single(byte), if byte < 0xa0 && !matches!(byte, 0x8e | 0x8f) { u16::from(byte) } else { 0xfffd });
                    }
                    assert!(mapping.row(0x8e).is_none());
                    assert!(mapping.row(0x8f).is_none());
                }
            }
        }
        let Decoder::DbcsOnly(ibm834) = lookup("x-ibm834").unwrap() else { panic!("expected DBCS-only decoder") };
        assert_eq!(ibm834.single(0), 0);
        assert_eq!(lookup("x-ibm834").unwrap().decode(&[0]), "\u{fffd}");
    }
}
