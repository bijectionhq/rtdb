use std::collections::BTreeMap;
use rtdb_types::Result;
use super::error;
mod double_byte;
mod single_byte;

#[derive(Clone, Copy)]
pub(super) enum Charset {
    Utf8,
    Cesu8,
    Latin1,
    Ascii,
    Big5(&'static double_byte::big5::Big5Family),
    Big5Solaris(&'static double_byte::big5::Big5Family),
    Hkscs(&'static double_byte::hkscs::Hkscs),
    DoubleByteSubclass(double_byte::subclasses::Decoder),
    Ebcdic(&'static double_byte::Mapping),
    EucJp(double_byte::euc_jp::EucJp),
    EucTw(double_byte::euc_tw::EucTw),
    IbmEuc(double_byte::ibm_euc::IbmEuc),
    Iso2022Jp(double_byte::iso2022jp::Iso2022Jp),
    DoubleByte(&'static double_byte::Mapping),
    SingleByte(&'static [u8; 512]),
    Utf16(Utf16Order),
    Utf32(Utf32Order),
}

#[derive(Clone, Copy)]
pub(super) enum Utf16Order { Big, Little, BomBig, BomLittle }

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Utf32Order { Detect, Big, Little }

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
            "cesu-8" | "cesu8" | "cscesu-8" => Self::Cesu8,
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
            "utf-32" | "utf_32" | "utf32" => Self::Utf32(Utf32Order::Detect),
            "utf-32be" | "utf_32be" | "x-utf-32be" | "x-utf-32be-bom" | "utf_32be_bom" | "utf-32be-bom" => Self::Utf32(Utf32Order::Big),
            "utf-32le" | "utf_32le" | "x-utf-32le" | "x-utf-32le-bom" | "utf_32le_bom" | "utf-32le-bom" => Self::Utf32(Utf32Order::Little),
            "big5" | "csbig5" => Self::Big5(&double_byte::big5::FAMILY),
            "x-big5-solaris" | "big5_solaris" => Self::Big5Solaris(&double_byte::big5::FAMILY),
            normalized => {
                if let Some(table) = single_byte::lookup(normalized) { Self::SingleByte(table) }
                else if let Some(table) = double_byte::hkscs::lookup(normalized) { Self::Hkscs(table) }
                else if let Some(decoder) = double_byte::subclasses::lookup(normalized) { Self::DoubleByteSubclass(decoder) }
                else if let Some(table) = double_byte::ebcdic::lookup(normalized) { Self::Ebcdic(table) }
                else if let Some(decoder) = double_byte::euc_jp::lookup(normalized) { Self::EucJp(decoder) }
                else if let Some(decoder) = double_byte::euc_tw::lookup(normalized) { Self::EucTw(decoder) }
                else if let Some(decoder) = double_byte::ibm_euc::lookup(normalized) { Self::IbmEuc(decoder) }
                else if let Some(decoder) = double_byte::iso2022jp::lookup(normalized) { Self::Iso2022Jp(decoder) }
                else if let Some(table) = double_byte::lookup(normalized) { Self::DoubleByte(table) }
                else { return Err(error("invalid-plugin-config", format!("Unsupported string encoding '{name}'"))); }
            },
        })
    }

    pub(super) fn decode(self, bytes: &[u8]) -> String {
        match self {
            Self::Utf8 => rtdb_types::decode_java_utf8(bytes),
            Self::Cesu8 => decode_cesu8(bytes),
            Self::Latin1 => bytes.iter().map(|byte| char::from(*byte)).collect(),
            Self::Ascii => bytes.iter().map(|byte| if *byte < 128 { char::from(*byte) } else { '\u{fffd}' }).collect(),
            Self::Big5(family) => family.decode_big5(bytes),
            Self::Big5Solaris(family) => family.decode_solaris(bytes),
            Self::Hkscs(table) => table.decode(bytes),
            Self::DoubleByteSubclass(decoder) => decoder.decode(bytes),
            Self::Ebcdic(table) => double_byte::ebcdic::decode(table, bytes),
            Self::EucJp(decoder) => decoder.decode(bytes),
            Self::EucTw(decoder) => decoder.decode(bytes),
            Self::IbmEuc(decoder) => decoder.decode(bytes),
            Self::Iso2022Jp(decoder) => decoder.decode(bytes),
            Self::DoubleByte(table) => table.decode(bytes),
            Self::SingleByte(table) => single_byte::decode(table, bytes),
            Self::Utf16(order) => decode_utf16(bytes, order),
            Self::Utf32(order) => decode_utf32(bytes, order),
        }
    }
}

fn decode_cesu8(mut bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 3);
    let units = std::iter::from_fn(|| {
        let (&first, tail) = bytes.split_first()?;
        bytes = tail;
        if first < 0x80 { return Some(u16::from(first)); }
        if !(0xc2..=0xef).contains(&first) { return Some(0xfffd); }
        let Some(&second) = bytes.first() else { return Some(0xfffd); };
        if second & 0xc0 != 0x80 || (first == 0xe0 && second < 0xa0) {
            return Some(0xfffd);
        }
        bytes = &bytes[1..];
        if first < 0xe0 {
            return Some((u16::from(first & 0x1f) << 6) | u16::from(second & 0x3f));
        }
        let Some(&third) = bytes.first() else { return Some(0xfffd); };
        if third & 0xc0 != 0x80 { return Some(0xfffd); }
        bytes = &bytes[1..];
        Some((u16::from(first & 0x0f) << 12) | (u16::from(second & 0x3f) << 6) | u16::from(third & 0x3f))
    });
    output.extend(char::decode_utf16(units).map(|unit| unit.unwrap_or('\u{fffd}')));
    output
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

fn decode_utf32(mut bytes: &[u8], order: Utf32Order) -> String {
    let little = if bytes.starts_with(&[0, 0, 0xfe, 0xff]) && order != Utf32Order::Little {
        bytes = &bytes[4..]; false
    } else if bytes.starts_with(&[0xff, 0xfe, 0, 0]) && order != Utf32Order::Big {
        bytes = &bytes[4..]; true
    } else { order == Utf32Order::Little };
    let words = bytes.chunks_exact(4);
    let tail = (!words.remainder().is_empty()).then_some(0xfffdu16);
    let units = words.flat_map(|word| {
        let word = [word[0], word[1], word[2], word[3]];
        let point = if little { u32::from_le_bytes(word) } else { u32::from_be_bytes(word) };
        // OpenJDK25 UTF_32Coder.Decoder emits BMP surrogate values as UTF16 units.
        if point <= 0xffff { [Some(point as u16), None] }
        else if point <= 0x10ffff {
            let point = point - 0x10000;
            [Some(0xd800 | (point >> 10) as u16), Some(0xdc00 | (point & 0x3ff) as u16)]
        } else { [Some(0xfffd), None] }
    }).flatten().chain(tail);
    char::decode_utf16(units).map(|unit| unit.unwrap_or('\u{fffd}')).collect()
}
