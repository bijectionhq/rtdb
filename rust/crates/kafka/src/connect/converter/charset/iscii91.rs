mod aliases;

static DIRECT_MAP: &[u8; 510] = include_bytes!("iscii91/direct-map.bin");

#[derive(Clone, Copy)]
enum Context { Empty, Emitted(u16), Pending(u16) }

pub(super) fn supports(name: &str) -> bool { aliases::ALIASES.binary_search(&name).is_ok() }

fn composition(base: u16) -> Option<u16> {
    Some(match base {
        0x0901 => 0x0950,
        0x0907 => 0x090c,
        0x0908 => 0x0961,
        0x090b => 0x0960,
        0x093f => 0x0962,
        0x0940 => 0x0963,
        0x0943 => 0x0944,
        0x0964 => 0x093d,
        _ => return None,
    })
}

pub(super) fn decode(bytes: &[u8]) -> String {
    let mut context = Context::Empty;
    let mut output = String::with_capacity(bytes.len() * 3);
    for &byte in bytes {
        let index = usize::from(if byte < 128 { byte } else { byte - 1 }) * 2;
        let current = u16::from_be_bytes([DIRECT_MAP[index], DIRECT_MAP[index + 1]]);
        let previous = match context { Context::Empty => None, Context::Emitted(unit) | Context::Pending(unit) => Some(unit) };
        if previous == Some(0xfffd) {
            output.push('\u{fffd}');
            context = Context::Empty;
            continue;
        }
        if composition(current).is_some() {
            if let Context::Pending(unit) = context { output.push(char::from_u32(u32::from(unit)).expect("ISCII91 BMP unit")); }
            context = Context::Pending(current);
            continue;
        }
        if current == 0x093c {
            if let Some(composed) = previous.and_then(composition) {
                output.push(char::from_u32(u32::from(composed)).expect("ISCII91 composition"));
                context = Context::Emitted(current);
                continue;
            }
        }
        if let Context::Pending(unit) = context {
            output.push(char::from_u32(u32::from(unit)).expect("ISCII91 BMP unit"));
            context = Context::Pending(current);
            continue;
        }
        let unit = match current {
            0x093c if previous == Some(0x094d) => 0x200d,
            0x094d if previous == Some(0x094d) => 0x200c,
            0xffff => { output.push('\u{fffd}'); continue; },
            _ => current,
        };
        output.push(char::from_u32(u32::from(unit)).expect("ISCII91 BMP unit"));
        context = Context::Emitted(current);
    }
    if let Context::Pending(unit) = context { output.push(char::from_u32(u32::from(unit)).expect("ISCII91 BMP unit")); }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_names_share_the_complete_static_source_table_and_signed_byte_indexing() {
        assert_eq!(aliases::ALIASES.len(), 6);
        for pair in aliases::ALIASES.windows(2) { assert!(pair[0] < pair[1]); }
        for &name in aliases::ALIASES { assert!(supports(name)); }
        assert_eq!(DIRECT_MAP.len(), 510);
        assert_eq!(decode(&[0x7f, 0x80]), "\u{7f}\u{7f}");
        assert_eq!(decode(&[0x81, 0xff]), "\u{fffd}\u{fffd}");
        assert_eq!(decode(&[0xa1, 0xe9]), "\u{0950}");
    }

    #[test]
    fn pending_units_flush_literal_noncharacters_while_unmappable_bytes_retain_emitted_context() {
        assert_eq!(decode(&[0xa1, 0x81]), "\u{0901}\u{ffff}");
        assert_eq!(decode(&[0xa1, 0x81, 0xe9]), "\u{0901}\u{ffff}\u{093c}");
        assert_eq!(decode(&[0xe8, 0x81, 0xe8]), "\u{094d}\u{fffd}\u{200c}");
        assert_eq!(decode(&[0xe8, 0x81, 0xe9]), "\u{094d}\u{fffd}\u{200d}");
        assert_eq!(decode(&[0xa1, 0xef, b'A']), "\u{0901}\u{fffd}");
        assert_eq!(decode(&[0xef, b'A']), "\u{fffd}\u{fffd}");
        assert_eq!(decode(&[0xe9]), "\u{093c}");
    }
}
