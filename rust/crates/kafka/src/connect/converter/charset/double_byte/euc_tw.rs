mod aliases;
mod tables;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super::super) enum EucTw { Standard, Ibm964 }

struct StandardPlanes {
    planes: &'static [u8; 141_376],
    supplementary: &'static [u8; 17_672],
}

struct IbmTables(&'static [u8; 71_008]);

pub(in super::super) fn lookup(name: &str) -> Option<EucTw> {
    let index = aliases::ALIASES.binary_search_by_key(&name, |&(alias, _)| alias).ok()?;
    Some(aliases::ALIASES[index].1)
}

impl EucTw {
    pub(in super::super) fn decode(self, bytes: &[u8]) -> String {
        match self { Self::Standard => tables::STANDARD.decode(bytes), Self::Ibm964 => tables::IBM964.decode(bytes) }
    }
}

fn legal(byte: u8) -> bool { (0xa1..=0xfe).contains(&byte) }

impl StandardPlanes {
    fn pair(&self, plane: usize, first: u8, second: u8) -> char {
        if !legal(first) || !legal(second) { return '\u{fffd}'; }
        let index = usize::from(first - 0xa1) * 94 + usize::from(second - 0xa1);
        let offset = (plane * 8836 + index) * 2;
        let value = u16::from_be_bytes([self.planes[offset], self.planes[offset + 1]]);
        if value == 0xfffd { return '\u{fffd}'; }
        let value = u32::from(value) + if self.supplementary[index] & (1 << plane) != 0 { 0x20000 } else { 0 };
        char::from_u32(value).unwrap_or('\u{fffd}')
    }

    fn decode(&self, mut bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len() * 3);
        output.extend(std::iter::from_fn(|| {
            let (&first, tail) = bytes.split_first()?;
            bytes = tail;
            if first == 0x8e {
                if bytes.len() < 3 { bytes = &[]; return Some('\u{fffd}'); }
                let selector = bytes[0];
                bytes = &bytes[1..];
                let plane = match selector { 0xa2..=0xa7 => usize::from(selector - 0xa1), 0xaf => 7, _ => return Some('\u{fffd}') };
                let value = self.pair(plane, bytes[0], bytes[1]);
                bytes = &bytes[2..];
                return Some(value);
            }
            if first < 0x80 { return Some(char::from(first)); }
            let Some(&second) = bytes.first() else { return Some('\u{fffd}'); };
            if !legal(first) || !legal(second) { return Some('\u{fffd}'); }
            bytes = &bytes[1..];
            Some(self.pair(0, first, second))
        }));
        output
    }
}

impl IbmTables {
    fn unit(&self, index: usize) -> char {
        char::from_u32(u32::from(u16::from_be_bytes([self.0[index * 2], self.0[index * 2 + 1]]))).unwrap_or('\u{fffd}')
    }

    fn pair(&self, plane: usize, first: u8, second: u8) -> char {
        if !legal(first) || !legal(second) { return '\u{fffd}'; }
        self.unit(160 + plane * 8836 + usize::from(first - 0xa1) * 94 + usize::from(second - 0xa1))
    }

    fn decode(&self, mut bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len() * 3);
        output.extend(std::iter::from_fn(|| {
            let (&first, tail) = bytes.split_first()?;
            bytes = tail;
            if first == 0x8e {
                if bytes.len() < 3 { bytes = &[]; return Some('\u{fffd}'); }
                let selector = bytes[0];
                bytes = &bytes[1..];
                let plane = match selector { 0xa2 => 1, 0xac => 2, 0xad => 3, _ => return Some('\u{fffd}') };
                let second = bytes[0];
                bytes = &bytes[1..];
                if !legal(second) { return Some('\u{fffd}'); }
                let third = bytes[0];
                bytes = &bytes[1..];
                return Some(self.pair(plane, second, third));
            }
            if first == 0x8f { return Some('\u{fffd}'); }
            if first < 0xa0 { return Some(self.unit(usize::from(first))); }
            if !legal(first) { return Some('\u{fffd}'); }
            let Some((&second, tail)) = bytes.split_first() else { return Some('\u{fffd}'); };
            bytes = tail;
            Some(self.pair(0, first, second))
        }));
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_names_select_two_immutable_plane_collections() {
        assert_eq!(aliases::ALIASES.len(), 11);
        for pair in aliases::ALIASES.windows(2) { assert!(pair[0].0 < pair[1].0); }
        for &(name, variant) in aliases::ALIASES { assert_eq!(lookup(name), Some(variant)); }
        assert_eq!(tables::STANDARD.pair(0, 0xa1, 0xa1), '\u{3000}');
        assert_eq!(tables::IBM964.pair(0, 0xa1, 0xa1), '\u{3000}');
        assert_eq!(tables::STANDARD.pair(2, 0xa1, 0xc4), '\u{2000b}');
        assert_eq!(tables::STANDARD.pair(5, 0xa1, 0xa1), '\u{2f802}');
        assert!(tables::STANDARD.supplementary[8836..].iter().all(|&byte| byte == 0));
    }

    #[test]
    fn interleaved_calls_preserve_c1_and_malformed_length_differences() {
        assert_eq!(EucTw::Standard.decode(&[0x80, b'A']), "\u{fffd}A");
        assert_eq!(EucTw::Ibm964.decode(&[0x80, b'A']), "\u{80}A");
        assert_eq!(EucTw::Standard.decode(&[0x8e, 0xa2, 0, b'A']), "\u{fffd}");
        assert_eq!(EucTw::Ibm964.decode(&[0x8e, 0xa2, 0, b'A']), "\u{fffd}A");
        assert_eq!(EucTw::Standard.decode(&[0x8e, 0xa2, 0]), "\u{fffd}");
        assert_eq!(EucTw::Ibm964.decode(&[0x8e, 0xa2, 0]), "\u{fffd}");
    }
}
