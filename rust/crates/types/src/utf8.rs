/// Decodes Java UTF-8 with replacement, including one replacement for an encoded surrogate.
/// The returned text requires at most three UTF-8 bytes per input byte.
pub fn decode_java_utf8(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len());
    append_java_utf8(&mut output,bytes.len(),|index|bytes[index]);
    output
}

pub(crate) fn append_java_utf8(output: &mut String,length: usize,byte: impl Fn(usize)->u8) {
    let mut index = 0;
    while index < length {
        let first = byte(index);
        let width = match first { 0..=0x7f => 1,0xc2..=0xdf => 2,0xe0..=0xef => 3,0xf0..=0xf4 => 4,_ => 0 };
        if width == 0 { output.push('\u{fffd}');index += 1;continue; }
        let mut consumed = 1;
        let mut code = if width == 1 { u32::from(first) } else { u32::from(first & (0x7f >> width)) };
        while consumed < width && index + consumed < length {
            let next = byte(index + consumed);
            if !(0x80..=0xbf).contains(&next) || (consumed == 1 && ((first == 0xe0 && next < 0xa0) || (first == 0xf0 && next < 0x90) || (first == 0xf4 && next > 0x8f))) { break; }
            code = (code << 6) | u32::from(next & 0x3f);consumed += 1;
        }
        output.push(if consumed == width { char::from_u32(code).unwrap_or('\u{fffd}') } else { '\u{fffd}' });
        index += consumed;
    }
}
