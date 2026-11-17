const PAIRS: &[u8; 48_132] = include_bytes!("gb18030/pairs.bin");
const BMP: &[u8; 78_840] = include_bytes!("gb18030/bmp.bin");

pub(in super::super) fn decode(mut bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 3);
    while let Some(&first) = bytes.first() {
        let (scalar, count) = if first < 0x80 { (u32::from(first), 1) }
        else if !(0x81..=0xfe).contains(&first) { (0xfffd, 1) }
        else {
            let Some(&second) = bytes.get(1) else { output.push('\u{fffd}'); break; };
            if second < 0x30 { (0xfffd, 1) }
            else if (0x30..=0x39).contains(&second) {
                if bytes.len() < 4 { output.push('\u{fffd}'); break; }
                if !(0x81..=0xfe).contains(&bytes[2]) { (0xfffd, 3) }
                else if !(0x30..=0x39).contains(&bytes[3]) { (0xfffd, 4) }
                else {
                    let offset = (((u32::from(first - 0x81) * 10 + u32::from(second - 0x30)) * 126
                        + u32::from(bytes[2] - 0x81)) * 10) + u32::from(bytes[3] - 0x30);
                    let scalar = match offset {
                        0..=0x99fb => u32::from(unit(BMP, offset as usize)),
                        0x2e248..=0x12e247 => offset - 0x1e248,
                        _ => 0xfffd,
                    };
                    (scalar, 4)
                }
            } else if second < 0x40 || matches!(second, 0x7f | 0xff) { (0xfffd, 2) }
            else { (u32::from(unit(PAIRS, usize::from(first - 0x81) * 191 + usize::from(second - 0x40))), 2) }
        };
        output.push(char::from_u32(scalar).unwrap_or('\u{fffd}'));
        bytes = &bytes[count..];
    }
    output
}

fn unit(table: &[u8], index: usize) -> u16 { u16::from_be_bytes([table[index * 2], table[index * 2 + 1]]) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn four_byte_precheck_precedes_third_byte_validation() {
        assert_eq!(decode(b"\x810A"), "\u{fffd}");
        assert_eq!(decode(b"\x810AB"), "\u{fffd}B");
        assert_eq!(decode(b"\x81\x2fA"), "\u{fffd}/A");
        assert_eq!(decode(b"\x81\x3aA"), "\u{fffd}A");
    }

    #[test]
    fn supplementary_range_keeps_both_endpoints() {
        assert_eq!(decode(b"\x900\x810"), "\u{10000}");
        assert_eq!(decode(b"\xe32\x9a5"), "\u{10ffff}");
        assert_eq!(decode(b"\xe32\x9a6"), "\u{fffd}");
        assert_eq!(decode(b"\x849\xfe9"), "\u{fffd}");
    }
}
