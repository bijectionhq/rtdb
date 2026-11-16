use rtdb_types::decode_java_utf8;

#[test]
fn java_utf8_replacement_groups_encoded_surrogates_and_truncated_prefixes() {
    for (bytes,text) in [
        (&b"plain"[..],"plain"),(&[0xed,0xa0,0x80][..],"\u{fffd}"),(&[0xe1,0x80][..],"\u{fffd}"),
        (&[0xe1,0x80,b'x'][..],"\u{fffd}x"),(&[0xe0,0x80,0x80][..],"\u{fffd}\u{fffd}\u{fffd}"),
        (&[0xf4,0x90,0x80,0x80][..],"\u{fffd}\u{fffd}\u{fffd}\u{fffd}"),
        (&[0xf0,0x9f,0x98,0x80][..],"😀"),
    ] { assert_eq!(decode_java_utf8(bytes),text); }
}
