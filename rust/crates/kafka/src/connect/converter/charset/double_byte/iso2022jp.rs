use super::Mapping;
use super::probe::{DecodedUnit, DecodeError, ProbeStop, StrictProbe};
mod aliases;
mod tables;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super::super) enum Iso2022Jp { Standard, Jp2, Ms5022x, MsIso }

#[derive(Clone, Copy)]
enum Mode { Ascii, Roman, Kana, DoubleByte(&'static Mapping) }

struct Decoder {
    jis0208: &'static Mapping,
    jis0212: Option<&'static Mapping>,
    current: Mode,
    previous: Mode,
}

enum Group { Character(DecodedUnit), Control { bytes: usize }, Incomplete, Error(DecodeError) }

pub(in super::super) fn lookup(name: &str) -> Option<Iso2022Jp> {
    let index = aliases::ALIASES.binary_search_by_key(&name, |&(alias, _)| alias).ok()?;
    Some(aliases::ALIASES[index].1)
}

impl Iso2022Jp {
    fn decoder(self) -> Decoder {
        let (jis0208, jis0212) = match self {
            Self::Standard => (&tables::JIS0208, None),
            Self::Jp2 => (&tables::JIS0208, Some(&tables::JIS0212)),
            Self::Ms5022x => (&tables::MS5022X_0208, Some(&tables::MS5022X_0212)),
            Self::MsIso => (&tables::MS932_0208, None),
        };
        Decoder { jis0208, jis0212, current: Mode::Ascii, previous: Mode::Ascii }
    }

    pub(in super::super) fn decode(self, bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len() * 3);
        self.decode_into(bytes, &mut output);
        output
    }

    pub(super) fn decode_into(self, mut bytes: &[u8], output: &mut String) {
        let mut decoder = self.decoder();
        while !bytes.is_empty() {
            let count = match decoder.group(bytes) {
                Group::Character(decoded) => {
                    output.push(char::from_u32(u32::from(decoded.unit)).unwrap_or('\u{fffd}'));
                    decoded.bytes
                },
                Group::Control { bytes } => bytes,
                Group::Incomplete => { output.push('\u{fffd}'); break; },
                Group::Error(error) => { output.push('\u{fffd}'); error.bytes() },
            };
            bytes = &bytes[count..];
        }
    }

    pub(super) fn probe(self, bytes: &[u8]) -> StrictProbe {
        let mut decoder = self.decoder();
        let (mut consumed, mut utf16_units) = (0, 0);
        while consumed < bytes.len() {
            match decoder.group(&bytes[consumed..]) {
                Group::Character(decoded) => { consumed += decoded.bytes; utf16_units += 1; },
                Group::Control { bytes } => consumed += bytes,
                Group::Incomplete => break,
                Group::Error(error) => return StrictProbe { consumed, utf16_units, stop: ProbeStop::Error(error) },
            }
        }
        StrictProbe { consumed, utf16_units, stop: ProbeStop::Underflow }
    }
}

impl Decoder {
    fn group(&mut self, bytes: &[u8]) -> Group {
        let Some(&first) = bytes.first() else { return Group::Incomplete; };
        if first >= 0x80 { return Group::Error(DecodeError::Malformed { bytes: 1 }); }
        if first == 0x1b {
            if bytes.len() < 3 { return Group::Incomplete; }
            let (next, count) = match (bytes[1], bytes[2]) {
                (b'(', b'B') => (Some(Mode::Ascii), 3),
                (b'(', b'J') => (Some(Mode::Roman), 3),
                (b'(', b'I') => (Some(Mode::Kana), 3),
                (b'$', b'@' | b'B') => (Some(Mode::DoubleByte(self.jis0208)), 3),
                (b'$', b'(') if self.jis0212.is_some() => {
                    if bytes.len() < 4 { return Group::Incomplete; }
                    (if bytes[3] == b'D' { self.jis0212.map(Mode::DoubleByte) } else { None }, 4)
                },
                (b'(' | b'$', _) => (None, 3),
                _ => (None, 2),
            };
            return if let Some(next) = next {
                self.current = next;
                Group::Control { bytes: count }
            } else { Group::Error(DecodeError::Malformed { bytes: count }) };
        }
        if first == 0x0e {
            self.previous = self.current;
            self.current = Mode::Kana;
            return Group::Control { bytes: 1 };
        }
        if first == 0x0f {
            self.current = self.previous;
            return Group::Control { bytes: 1 };
        }
        let (unit, count) = match self.current {
            Mode::Ascii => (u16::from(first), 1),
            Mode::Roman => (match first { 0x5c => 0x00a5, 0x7e => 0x203e, _ => u16::from(first) }, 1),
            Mode::Kana => {
                if first > 0x5f { return Group::Error(DecodeError::Malformed { bytes: 1 }); }
                (u16::from(first) + 0xff40, 1)
            },
            Mode::DoubleByte(mapping) => {
                let Some(&second) = bytes.get(1) else { return Group::Incomplete; };
                let unit = mapping.pair(first, second);
                if unit == 0xfffd { return Group::Error(DecodeError::Unmappable { bytes: 2 }); }
                (unit, 2)
            },
        };
        Group::Character(DecodedUnit { unit, bytes: count })
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
