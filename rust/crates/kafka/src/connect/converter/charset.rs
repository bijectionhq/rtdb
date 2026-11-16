use std::collections::BTreeMap;
use rtdb_types::Result;
use super::error;

#[derive(Clone, Copy)]
pub(super) enum Charset {
    Utf8,
    Latin1,
    Ascii,
    Utf16(Utf16Order),
}

#[derive(Clone, Copy)]
pub(super) enum Utf16Order { Big, Little, BomBig, BomLittle }

impl Charset {
    pub(super) fn configured(options: &BTreeMap<String, String>, is_key: bool) -> Result<Self> {
        let fallback = options.get("converter.encoding").map_or("UTF-8", |value| value.trim_matches(|ch| ch <= '\u{20}'));
        let (serializer, deserializer) = if is_key {
            ("key.serializer.encoding", "key.deserializer.encoding")
        } else {
            ("value.serializer.encoding", "value.deserializer.encoding")
        };
        Self::parse(options.get(serializer).map_or(fallback, String::as_str))?;
        Self::parse(options.get(deserializer).map_or(fallback, String::as_str))
    }

    fn parse(name: &str) -> Result<Self> {
        Ok(match name.to_ascii_lowercase().as_str() {
            "utf-8" | "utf8" | "unicode-1-1-utf-8" => Self::Utf8,
            "iso-8859-1" | "iso-ir-100" | "iso_8859-1" | "latin1" | "l1" | "ibm819"
            | "cp819" | "csisolatin1" | "819" | "ibm-819" | "iso8859_1" | "iso_8859-1:1987"
            | "iso_8859_1" | "8859_1" | "iso8859-1" => Self::Latin1,
            "us-ascii" | "iso-ir-6" | "ansi_x3.4-1986" | "iso_646.irv:1991" | "ascii"
            | "iso646-us" | "us" | "ibm367" | "cp367" | "csascii" | "646" | "iso_646.irv:1983"
            | "ansi_x3.4-1968" | "ascii7" => Self::Ascii,
            "utf-16" | "utf_16" | "utf16" | "unicode" | "unicodebig" => Self::Utf16(Utf16Order::BomBig),
            "utf-16be" | "utf_16be" | "iso-10646-ucs-2" | "x-utf-16be" | "unicodebigunmarked" => Self::Utf16(Utf16Order::Big),
            "utf-16le" | "utf_16le" | "x-utf-16le" | "unicodelittleunmarked" => Self::Utf16(Utf16Order::Little),
            "x-utf-16le-bom" | "unicodelittle" => Self::Utf16(Utf16Order::BomLittle),
            _ => return Err(error("invalid-plugin-config", format!("Unsupported string encoding '{name}'"))),
        })
    }

    pub(super) fn decode(self, bytes: &[u8]) -> String {
        match self {
            Self::Utf8 => rtdb_types::decode_java_utf8(bytes),
            Self::Latin1 => bytes.iter().map(|byte| char::from(*byte)).collect(),
            Self::Ascii => bytes.iter().map(|byte| if *byte < 128 { char::from(*byte) } else { '\u{fffd}' }).collect(),
            Self::Utf16(order) => decode_utf16(bytes, order),
        }
    }
}

fn decode_utf16(mut bytes: &[u8], order: Utf16Order) -> String {
    let little = match order {
        Utf16Order::Big => false,
        Utf16Order::Little => true,
        Utf16Order::BomBig | Utf16Order::BomLittle => {
            if bytes.starts_with(&[0xff, 0xfe]) { bytes = &bytes[2..]; true }
            else if bytes.starts_with(&[0xfe, 0xff]) { bytes = &bytes[2..]; false }
            else { matches!(order, Utf16Order::BomLittle) }
        }
    };
    let unit = |bytes: &[u8]| if little { u16::from_le_bytes([bytes[0], bytes[1]]) } else { u16::from_be_bytes([bytes[0], bytes[1]]) };
    let mut output = String::with_capacity(bytes.len());
    while bytes.len() >= 2 {
        let first = unit(bytes);
        if (0xd800..=0xdbff).contains(&first) {
            if bytes.len() < 4 { output.push('\u{fffd}'); return output; }
            let second = unit(&bytes[2..]);
            if (0xdc00..=0xdfff).contains(&second) {
                let code = 0x10000 + ((u32::from(first) - 0xd800) << 10) + u32::from(second) - 0xdc00;
                output.push(char::from_u32(code).expect("paired UTF-16 surrogate"));
            } else { output.push('\u{fffd}'); }
            bytes = &bytes[4..];
        } else {
            output.push(char::from_u32(u32::from(first)).unwrap_or('\u{fffd}'));
            bytes = &bytes[2..];
        }
    }
    if !bytes.is_empty() { output.push('\u{fffd}'); }
    output
}
