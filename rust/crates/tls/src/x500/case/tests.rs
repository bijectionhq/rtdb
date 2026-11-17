use super::*;

#[test]
fn upper_then_lower_preserves_java_expansions_and_order() {
    for (input, expected) in [
        ("", ""), ("CN=Example, OU=TLS", "cn=example, ou=tls"),
        ("ÉCOLE", "école"), ("Straße", "strasse"), ("ẞ", "ß"),
        ("İıI", "i\u{307}ii"), ("ﬃﬄ", "ffiffl"), ("ŉ", "ʼn"),
        ("ᾀ", "ἀι"), ("ΐ", "ι\u{308}\u{301}"), ("µ", "μ"),
        ("𐐀𐐨", "𐐨𐐨"), ("\0\u{ffff}", "\0\u{ffff}"),
    ] { assert_eq!(fold(input), expected, "{input:?}"); }
}

#[test]
fn final_sigma_uses_java_word_boundaries_including_internal_punctuation() {
    for (input, expected) in [
        ("Σ", "σ"), ("1Σ", "1σ"), ("ΟΣ", "ος"), ("οσ", "ος"),
        ("ΟΣΑ", "οσα"), ("ΟΣ ΟΣ", "ος ος"),
        ("A.Σ", "a.ς"), ("A:Σ", "a:σ"),
        ("AΣ.B", "aσ.b"), ("AΣ..B", "aς..b"),
        ("AΣ-B", "aσ-b"), ("AΣ--B", "aς--b"),
        ("AΣ'B", "aσ'b"), ("AΣ''B", "aς''b"),
        ("AΣ\u{301}", "aς\u{301}"), ("AΣ\u{301}B", "aσ\u{301}b"),
        ("AΣ\u{200d}B", "aσ\u{200d}b"),
        ("𐐨Σ", "𐐨ς"), ("AΣ𐐨", "aσ𐐨"),
    ] { assert_eq!(fold(input), expected, "{input:?}"); }
}

#[test]
fn final_sigma_uses_conditional_special_casings_exact_cased_set() {
    for (input, expected) in [
        ("AΣʰ", "aσʰ"), ("AΣᵢ", "aςᵢ"), ("AΣª", "aςª"),
        ("ʰΣ", "ʰς"), ("ᵢΣ", "ᵢσ"), ("ªΣ", "ªσ"),
        ("AΣⅠ", "aσⅰ"), ("ⅠΣ", "ⅰς"),
    ] { assert_eq!(fold(input), expected, "{input:?}"); }
    assert!(is_cased('\u{1d61}'));
    assert!(!is_cased('\u{1d62}'));
    assert!(!is_cased('ª'));
}

#[test]
fn unicode_sixteen_mappings_are_pinned_independently_of_rust_unicode_version() {
    assert_eq!(fold("\u{1c89}\u{1c8a}"), "\u{1c8a}\u{1c8a}");
    assert_eq!(fold("\u{10d50}\u{10d70}"), "\u{10d70}\u{10d70}");
    assert_eq!(fold("\u{a7ce}\u{a7cf}"), "\u{a7ce}\u{a7cf}");
    assert!(!is_cased('\u{a7ce}'));
    assert!(!is_cased('\u{a7cf}'));
}

#[test]
fn resources_keep_exact_runtime_bytes_and_generated_mapping_rows() {
    let digest = |bytes: &[u8]| openssl::sha::sha256(bytes).iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    for (bytes, expected) in [
        (UPPER, "843b309e95006a6ede0e086a28b290f2518b163f6162c1cc0e9161b39ad5c065"),
        (LOWER, "753c397062138931d7c3fdf0bcce2c6beaf55b2e97f4817620d9c40dfca2050a"),
        (CASED, "b8b01339018a2ea6faae19601f861f50c92c1cb85dd7d86caa1c81bcbacbd875"),
        (WORDS, "76c4756446bce5cf2fe7d644be3dafe27fcd3dec4499a4ffba60ffef31c55c75"),
    ] { assert_eq!(digest(bytes), expected); }
    assert_eq!((UPPER.len() / 16, LOWER.len() / 16, CASED.len() / 8), (1552, 1460, 150));
    for table in [UPPER, LOWER] {
        let mut previous = None;
        for row in table.chunks_exact(16) {
            let character = number(row, 0);
            assert!(char::from_u32(character).is_some());
            assert!(previous.is_none_or(|previous| previous < character));
            previous = Some(character);
            let mut expected = String::new();
            for offset in [4, 8, 12] {
                let mapped = number(row, offset);
                if mapped != 0 { expected.push(char::from_u32(mapped).unwrap()); }
            }
            let mut actual = String::new();
            map(char::from_u32(character).unwrap(), table, |character| actual.push(character));
            assert_eq!(actual, expected);
        }
    }
}

#[test]
fn word_table_indices_cover_unicode_scalars_and_always_advance() {
    let rules = WordRules::new();
    assert_eq!(rules.categories, 22);
    assert_eq!(&WORDS[..8], b"BIdata\0\x01");
    assert_eq!(number(WORDS, 8) as usize, WORDS.len() - 12);
    assert_eq!(rules.ends.len(), 20);
    for code in 0..=0x10ffff {
        if let Some(character) = char::from_u32(code) {
            let category = rules.category(character);
            assert!(category == 255 || usize::from(category) < rules.categories);
        }
    }
    for row in rules.states.chunks_exact(2) { assert!(short(row, 0) < rules.ends.len()); }
    for value in ["AΣ..B", "Σ\u{ffff}Σ", "\u{200d}\u{200d}Σ", "AΣ\u{10ffff}", "𐐨Σ"] {
        let text = value.chars().collect::<Vec<_>>();
        let mut start = 0;
        while start < text.len() {
            let end = rules.next(&text, start);
            assert!(end > start && end <= text.len(), "{value:?}: {start}..{end}");
            start = end;
        }
    }
}

#[test]
fn java_english_lowercase_preserves_expansions_sigma_and_raw_utf16_aliases() {
    for (input, expected) in [
        ("Straße", "straße"), ("ſﬃ", "ſﬃ"), ("İıI", "i\u{307}ıi"),
        ("A.Σ", "a.ς"), ("A:Σ", "a:σ"), ("οσ", "οσ"),
        ("AΣ.B", "aσ.b"), ("AΣ..B", "aς..b"),
        ("𐐀", "𐐨"), ("\u{a7ce}", "\u{a7ce}"),
    ] {
        assert_eq!(lowercase_utf16(&input.encode_utf16().collect::<Vec<_>>()), expected.encode_utf16().collect::<Vec<_>>());
    }
    assert_eq!(lowercase_utf16(&[0xd800, b'A' as u16, 0xdc00]), [0xd800, b'a' as u16, 0xdc00]);
    assert_eq!(lowercase_utf16(&[b'A' as u16, 0xd800, 'Σ' as u16]), [b'a' as u16, 0xd800, 'σ' as u16]);
}

#[test]
fn java_ignore_case_uses_simple_character_case_without_full_string_expansion() {
    for (left, right, matches) in [
        ("İ", "i", true), ("ı", "I", true), ("ſ", "S", true),
        ("ß", "ẞ", true), ("ß", "SS", false), ("İ", "i\u{307}", false),
        ("Σ", "ς", true), ("𐐀", "𐐨", true),
        ("\u{1c89}", "\u{1c8a}", true), ("\u{a7ce}", "\u{a7cf}", false),
    ] {
        assert_eq!(equals_ignore_case_utf16(&left.encode_utf16().collect::<Vec<_>>(), &right.encode_utf16().collect::<Vec<_>>()), matches);
    }
    assert!(equals_ignore_case_utf16(&[0xd800, b'A' as u16], &[0xd800, b'a' as u16]));
    assert!(!equals_ignore_case_utf16(&[0xd800], &[0xdc00]));
    assert_eq!(SIMPLE_FOLD.len(), 1483 * 8);
    assert_eq!(openssl::sha::sha256(SIMPLE_FOLD).iter().map(|byte| format!("{byte:02x}")).collect::<String>(), "76b08a5b0961021d3a57d1bc49b42e23dad71d440b8d16ab1de0097eb54cabf2");
}
