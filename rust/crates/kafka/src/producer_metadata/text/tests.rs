use super::*;

fn decoded(bytes: &[u8]) -> String {
    let text = JavaText::new(bytes);
    let mut output = vec![0; text.utf8_len().unwrap()];
    assert_eq!(text.write_utf8(&mut output).unwrap(), output.len());
    String::from_utf8(output).unwrap()
}

#[test]
fn java_malformed_groups_preserve_valid_following_bytes() {
    let cases: &[(&[u8], &str)] = &[
        (b"", ""), (b"a\0z", "a\0z"), (&[0x80], "�"), (&[0xc0,0x80], "��"),
        (&[0xc1,0xbf], "��"), (&[0xc2], "�"), (&[0xc2,b'a'], "�a"), (&[0xc2,0x80], "\u{80}"),
        (&[0xdf,0xbf], "\u{7ff}"), (&[0xe0,0x80,0x80], "���"), (&[0xe0,0xa0,0x80], "\u{800}"),
        (&[0xe1], "�"), (&[0xe1,0x80], "�"), (&[0xe1,0x80,b'a'], "�a"), (&[0xe1,b'a',0x80], "�a�"),
        (&[0xed,0xa0], "�"), (&[0xed,0xa0,0x80], "�"), (&[0xed,0xa0,b'a'], "�a"),
        (&[0xed,0xbf,0xbf], "�"), (&[0xed,0x9f,0xbf], "\u{d7ff}"),
        (&[0xef,0xbb,0xbf], "\u{feff}"), (&[0xef,0xbf,0xbd], "�"),
        (&[0xf0], "�"), (&[0xf0,0x90], "�"), (&[0xf0,0x90,0x80], "�"),
        (&[0xf0,0x90,0x80,0x80], "\u{10000}"), (&[0xf0,0x80,0x80,0x80], "����"),
        (&[0xf0,b'a',0x80], "�a�"), (&[0xf0,0x90,b'a'], "�a"), (&[0xf0,0x90,0x80,b'a'], "�a"),
        (&[0xf4,0x8f,0xbf,0xbf], "\u{10ffff}"), (&[0xf4,0x90,0x80,0x80], "����"),
        (&[0xf5,0x80,0x80,0x80], "����"), (&[0xff,0x80,b'z'], "��z"),
    ];
    for (bytes, expected) in cases {
        assert_eq!(decoded(bytes), *expected, "{bytes:x?}");
        assert_eq!(JavaText::new(bytes).utf16().collect::<Vec<_>>(), expected.encode_utf16().collect::<Vec<_>>());
        assert_eq!(JavaText::new(bytes).utf16_len(), Ok(expected.encode_utf16().count()));
        assert!(JavaText::new(bytes).equals_str(expected));
    }
}

#[test]
fn exact_size_precedes_writes_and_retains_unused_output() {
    let raw = [0x80; 32767]; let text = JavaText::new(&raw);
    assert_eq!(text.raw().len(), 32767); assert_eq!(text.utf8_len(), Ok(98301));
    let mut short = vec![0x55; 98300];
    assert_eq!(text.write_utf8(&mut short), Err(TextError::OutputTooSmall {required: 98301}));
    assert!(short.iter().all(|byte| *byte == 0x55));
    let mut output = vec![0x55; 98302];
    assert_eq!(text.write_utf8(&mut output), Ok(98301)); assert_eq!(output[98301], 0x55);
    assert_eq!(std::str::from_utf8(&output[..98301]).unwrap().chars().count(), 32767);
}

#[test]
fn java_string_identity_hash_and_order_use_decoded_utf16_units() {
    let left = JavaText::new(&[0x80]); let right = JavaText::new(&[0x81]);
    assert_ne!(left.raw(), right.raw()); assert_eq!(left, right); assert_eq!(left.hash_code(), 65533);
    assert_eq!(JavaText::new(b"Aa").hash_code(), JavaText::new(b"BB").hash_code());
    let emoji = JavaText::new("😀".as_bytes());
    assert_eq!(emoji.utf16().collect::<Vec<_>>(), [0xd83d, 0xde00]); assert_eq!(emoji.hash_code(), 1772899);
    assert!(JavaText::new("\u{10000}".as_bytes()) < JavaText::new("\u{e000}".as_bytes()));
    assert_eq!(JavaText::new(b"polygenelubricants").hash_code(), i32::MIN);
}

#[test]
fn all_single_bytes_and_iterator_clones_have_stable_boundaries() {
    for byte in 0..=255u8 {
        let raw = [byte]; let text = JavaText::new(&raw);
        let mut chars = text.chars(); let mut copy = chars.clone();
        assert_eq!(chars.next(), Some(if byte < 128 {char::from(byte)} else {'�'}));
        assert_eq!(copy.next(), Some(if byte < 128 {char::from(byte)} else {'�'}));
        assert_eq!(chars.next(), None); assert_eq!(chars.next(), None);
    }
    let mut units = JavaText::new("😀x".as_bytes()).utf16();
    assert_eq!(units.next(), Some(0xd83d)); let copy = units.clone();
    assert_eq!(units.collect::<Vec<_>>(), [0xde00,120]); assert_eq!(copy.collect::<Vec<_>>(), [0xde00,120]);
}
