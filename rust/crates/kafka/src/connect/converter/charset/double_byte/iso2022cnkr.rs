use super::Mapping;
mod aliases;
mod tables;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super::super) enum Iso2022CnKr { Cn, Kr }

#[derive(Clone, Copy)]
enum CnDesignation { Gb, Cns }

struct CnsPlane(&'static [u8; 17_672]);

impl CnsPlane {
    fn pair(&self, first: u8, second: u8) -> u16 {
        if !(0xa1..=0xfe).contains(&first) || !(0xa1..=0xfe).contains(&second) { return 0xfffd; }
        let index = (usize::from(first - 0xa1) * 94 + usize::from(second - 0xa1)) * 2;
        u16::from_be_bytes([self.0[index], self.0[index + 1]])
    }
}

pub(in super::super) fn lookup(name: &str) -> Option<Iso2022CnKr> {
    let index = aliases::ALIASES.binary_search_by_key(&name, |&(alias, _)| alias).ok()?;
    Some(aliases::ALIASES[index].1)
}

impl Iso2022CnKr {
    pub(in super::super) fn decode(self, bytes: &[u8]) -> String {
        match self { Self::Cn => decode_cn(bytes), Self::Kr => decode_kr(bytes) }
    }
}

fn decode_cn(mut bytes: &[u8]) -> String {
    let mut shifted = false;
    let mut designation = CnDesignation::Gb;
    let mut output = String::with_capacity(bytes.len() * 3);
    while let Some(&first) = bytes.first() {
        let (unit, count) = match first {
            0x0e => { shifted = true; (None, 1) },
            0x0f => { shifted = false; (None, 1) },
            0x1b => {
                designation = CnDesignation::Gb;
                let Some(&second) = bytes.get(1) else { output.push('\u{fffd}'); break; };
                match second {
                    b'$' => {
                        let Some(&third) = bytes.get(2) else { output.push('\u{fffd}'); break; };
                        match third {
                            b'A' => (None, 3),
                            b')' | b'*' | b'+' => {
                                let Some(&fourth) = bytes.get(3) else { output.push('\u{fffd}'); break; };
                                match (third, fourth) {
                                    (b')', b'G') => { designation = CnDesignation::Cns; (None, 4) },
                                    (b')', b'A') | (b'*', b'H') | (b'+', b'I') => (None, 4),
                                    _ => (Some(0xfffd), 4),
                                }
                            },
                            _ => (Some(0xfffd), 3),
                        }
                    },
                    b'N' | b'O' => {
                        if bytes.len() < 4 { output.push('\u{fffd}'); break; }
                        let plane = if second == b'N' { &tables::CNS2 } else { &tables::CNS3 };
                        (Some(plane.pair(bytes[2] | 0x80, bytes[3] | 0x80)), 4)
                    },
                    _ => (Some(0xfffd), 2),
                }
            },
            _ if !shifted => (Some(u16::from(first)), 1),
            _ => {
                let Some(&second) = bytes.get(1) else { output.push('\u{fffd}'); break; };
                let unit = match designation {
                    CnDesignation::Gb => tables::GB2312.pair(first | 0x80, second | 0x80),
                    CnDesignation::Cns => tables::CNS1.pair(first | 0x80, second | 0x80),
                };
                (Some(unit), 2)
            },
        };
        if let Some(unit) = unit { output.push(char::from_u32(u32::from(unit)).unwrap_or('\u{fffd}')); }
        bytes = &bytes[count..];
    }
    output
}

fn decode_kr(mut bytes: &[u8]) -> String {
    let mut shifted = false;
    let mut output = String::with_capacity(bytes.len() * 3);
    while let Some(&first) = bytes.first() {
        let (unit, count) = match first {
            0x0e => { shifted = true; (None, 1) },
            0x0f => { shifted = false; (None, 1) },
            0x1b => {
                if bytes.len() < 4 { output.push('\u{fffd}'); break; }
                if &bytes[1..4] == b"$)C" { (None, 4) }
                else if matches!(bytes[1], b'N' | b'O') { (Some(0xfffd), 4) }
                else { (Some(0xfffd), 2) }
            },
            _ if !shifted => (Some(u16::from(first)), 1),
            _ => {
                let Some(&second) = bytes.get(1) else { output.push('\u{fffd}'); break; };
                (Some(tables::KSC5601.pair(first | 0x80, second | 0x80)), 2)
            },
        };
        if let Some(unit) = unit { output.push(char::from_u32(u32::from(unit)).unwrap_or('\u{fffd}')); }
        bytes = &bytes[count..];
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_names_select_two_immutable_decoders_without_changing_previous_resources() {
        assert_eq!(aliases::ALIASES.len(), 12);
        for pair in aliases::ALIASES.windows(2) { assert!(pair[0].0 < pair[1].0); }
        for &(name, variant) in aliases::ALIASES { assert_eq!(lookup(name), Some(variant)); }
        for name in ["iso-2022-cn", "x-iso-2022-cn-cns", "x-iso-2022-cn-gb"] {
            assert_eq!(lookup(name), Some(Iso2022CnKr::Cn));
        }
        assert_eq!(tables::GB2312.0, include_bytes!("gb2312.bin"));
        assert_eq!(tables::KSC5601.0, include_bytes!("euc-kr.bin"));
        assert_eq!(tables::CNS3.pair(0xa1, 0xc4), 0xfffd);
        assert_eq!(tables::CNS1.pair(0xa1, 0xa2), 0xff0c);
    }

    #[test]
    fn cn_escape_resets_designation_and_kr_requires_four_bytes_before_escape_validation() {
        assert_eq!(Iso2022CnKr::Cn.decode(b"\x1b$)G\x0e!\"\x1bX!\""), "，\u{fffd}、");
        assert_eq!(Iso2022CnKr::Cn.decode(b"\x1bXA"), "\u{fffd}A");
        assert_eq!(Iso2022CnKr::Kr.decode(b"\x1bXA"), "\u{fffd}");
        assert_eq!(Iso2022CnKr::Kr.decode(b"\x1bXAB"), "\u{fffd}AB");
        assert_eq!(Iso2022CnKr::Kr.decode(b"\x0e0!\x0f\xff"), "가ÿ");
        assert_eq!(Iso2022CnKr::Kr.decode(b"0!"), "0!");
    }
}
