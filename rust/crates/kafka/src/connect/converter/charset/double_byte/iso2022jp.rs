use super::Mapping;
mod aliases;
mod tables;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super::super) enum Iso2022Jp { Standard, Jp2, Ms5022x, MsIso }

#[derive(Clone, Copy)]
enum Mode { Ascii, Roman, Kana, DoubleByte(&'static Mapping) }

pub(in super::super) fn lookup(name: &str) -> Option<Iso2022Jp> {
    let index = aliases::ALIASES.binary_search_by_key(&name, |&(alias, _)| alias).ok()?;
    Some(aliases::ALIASES[index].1)
}

impl Iso2022Jp {
    pub(in super::super) fn decode(self, mut bytes: &[u8]) -> String {
        let (jis0208, jis0212) = match self {
            Self::Standard => (&tables::JIS0208, None),
            Self::Jp2 => (&tables::JIS0208, Some(&tables::JIS0212)),
            Self::Ms5022x => (&tables::MS5022X_0208, Some(&tables::MS5022X_0212)),
            Self::MsIso => (&tables::MS932_0208, None),
        };
        let mut current = Mode::Ascii;
        let mut previous = Mode::Ascii;
        let mut output = String::with_capacity(bytes.len() * 3);
        while let Some(&first) = bytes.first() {
            if first >= 0x80 {
                output.push('\u{fffd}');
                bytes = &bytes[1..];
                continue;
            }
            if first == 0x1b {
                if bytes.len() < 3 { output.push('\u{fffd}'); break; }
                let (next, count) = match (bytes[1], bytes[2]) {
                    (b'(', b'B') => (Some(Mode::Ascii), 3),
                    (b'(', b'J') => (Some(Mode::Roman), 3),
                    (b'(', b'I') => (Some(Mode::Kana), 3),
                    (b'$', b'@' | b'B') => (Some(Mode::DoubleByte(jis0208)), 3),
                    (b'$', b'(') if jis0212.is_some() => {
                        if bytes.len() < 4 { output.push('\u{fffd}'); break; }
                        (if bytes[3] == b'D' { jis0212.map(Mode::DoubleByte) } else { None }, 4)
                    },
                    (b'(' | b'$', _) => (None, 3),
                    _ => (None, 2),
                };
                if let Some(next) = next { current = next; } else { output.push('\u{fffd}'); }
                bytes = &bytes[count..];
                continue;
            }
            if first == 0x0e {
                previous = current;
                current = Mode::Kana;
                bytes = &bytes[1..];
                continue;
            }
            if first == 0x0f {
                current = previous;
                bytes = &bytes[1..];
                continue;
            }
            let (unit, count) = match current {
                Mode::Ascii => (u16::from(first), 1),
                Mode::Roman => (match first { 0x5c => 0x00a5, 0x7e => 0x203e, _ => u16::from(first) }, 1),
                Mode::Kana => (if first <= 0x5f { u16::from(first) + 0xff40 } else { 0xfffd }, 1),
                Mode::DoubleByte(mapping) => {
                    let Some(&second) = bytes.get(1) else { output.push('\u{fffd}'); break; };
                    (mapping.pair(first, second), 2)
                },
            };
            output.push(char::from_u32(u32::from(unit)).unwrap_or('\u{fffd}'));
            bytes = &bytes[count..];
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_names_select_four_immutable_configurations_for_five_java_classes() {
        assert_eq!(aliases::ALIASES.len(), 17);
        for pair in aliases::ALIASES.windows(2) { assert!(pair[0].0 < pair[1].0); }
        for &(name, variant) in aliases::ALIASES { assert_eq!(lookup(name), Some(variant)); }
        assert_eq!(lookup("x-windows-50220"), Some(Iso2022Jp::Ms5022x));
        assert_eq!(lookup("x-windows-50221"), Some(Iso2022Jp::Ms5022x));
        assert_eq!(tables::JIS0208.0, include_bytes!("subclasses/x-jis0208.bin"));
        assert_eq!(tables::JIS0212.0, include_bytes!("subclasses/jis_x0212-1990.bin"));
        for mapping in [&tables::JIS0208, &tables::JIS0212, &tables::MS5022X_0208, &tables::MS5022X_0212, &tables::MS932_0208] {
            assert_eq!(&mapping.0[..2], &[0x21, 0x7e]);
            for byte in 0u8..=255 { assert_eq!(mapping.single(byte), 0xfffd); }
        }
    }

    #[test]
    fn decoding_keeps_current_and_previous_modes_local_to_each_record() {
        for variant in [Iso2022Jp::Standard, Iso2022Jp::Jp2, Iso2022Jp::Ms5022x, Iso2022Jp::MsIso] {
            assert_eq!(variant.decode(b"\x1b$B\x0e!\x0f$\""), "｡あ");
            assert_eq!(variant.decode(b"\x1b$B\x0e\x0e\x0f$\""), "､｢");
            assert_eq!(variant.decode(b"\x1b$B\x0e\x1b(B\x0f$\""), "あ");
            assert_eq!(variant.decode(b"\x1b(J\\~\x0f\\~"), "¥‾\\~");
            assert_eq!(variant.decode(b"\x1b$B"), "");
            assert_eq!(variant.decode(b"$\""), "$\"");
        }
    }
}
