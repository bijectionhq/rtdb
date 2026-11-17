use super::*;

#[test]
fn every_extended_provider_key_retains_its_source_canonical_target() {
    assert_eq!(names::NAMES.len(), 550);
    for pair in names::NAMES.windows(2) { assert!(pair[0].0 < pair[1].0); }
    for &(name, canonical) in names::NAMES {
        assert!(name.is_ascii() && canonical.is_ascii());
        assert_eq!(lookup(name), Some(canonical));
        assert_eq!(lookup(&name.to_ascii_uppercase()), Some(canonical));
        assert_eq!(lookup(canonical), Some(canonical));
    }
}

#[test]
fn unicode_equivalents_resolve_at_every_matching_extended_key_position() {
    for &(name, canonical) in names::NAMES {
        for (index, byte) in name.bytes().enumerate() {
            let alternatives: &[char] = match byte { b'i' => &['\u{130}', '\u{131}'], b's' => &['\u{17f}'], b'k' => &['\u{212a}'], _ => &[] };
            for replacement in alternatives {
                let mut changed = name.to_owned();
                changed.replace_range(index..index + 1, &replacement.to_string());
                assert_eq!(lookup(&changed), Some(canonical), "{changed}");
            }
        }
        let combined: String = name.chars().map(|unit| match unit { 'i' => '\u{130}', 's' => '\u{17f}', 'k' => '\u{212a}', unit => unit }).collect();
        assert_eq!(lookup(&combined), Some(canonical));
    }
}

#[test]
fn extension_membership_preserves_standard_only_and_unknown_name_rejection() {
    for name in ["\u{17f}hift_jis", "\u{130}SO-8859-1", "a\u{17f}cii", "\u{212a}OI8-R", "utf-8",
        "x-iscii91\0", " x-iscii91", "x-iscii91 ", "x-\u{ff29}scii91", "x-i\u{307}scii91",
        "x-\u{df}cii91", "x-\u{fb06}cii91", "x-\u{10400}scii91", "x-\u{1d408}scii91"] {
        assert_eq!(lookup(name), None, "{name}");
    }
}

#[test]
fn extension_inventory_and_case_equivalence_resources_keep_their_source_hashes() {
    let hash = |bytes: &[u8]| openssl::sha::sha256(bytes).iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    assert_eq!(hash(include_bytes!("names.rs")), "ce3652c3d775e7f644c837a23d3b74ebda99334959a91d93b6344f7a8d19a304");
    assert_eq!(hash(include_bytes!("names.tsv")), "851285904aa25360747db1e5c0f37b4a901799887f4ea77a2954c30deecd6e9f");
    assert_eq!(hash(include_bytes!("case-audit.json")), "c88e0acc3e088d41ce4a79039d603af293efef2ab57128b7cf9df97fcde898ec");
    assert_eq!(hash(include_bytes!("sources.sha256")), "bdc51cf0b1c943cf4dd8104232f33fc6774f1ba718caad59c839b22e708987a4");
}
