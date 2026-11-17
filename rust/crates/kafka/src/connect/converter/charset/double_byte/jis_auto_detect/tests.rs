use openssl::sha::Sha256;
use super::*;

const PROBES: &str = include_str!("probes.tsv");

fn hex(bytes: &[u8]) -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() }
fn unhex(text: &str) -> Vec<u8> {
    text.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

fn selected(bytes: &[u8]) -> u8 {
    let prefix = bytes.iter().take_while(|&&byte| byte < 0x80 && byte != 0x1b).count();
    if prefix == bytes.len() { return 0; }
    match select(&bytes[prefix..]) {
        Selection::Iso2022Jp => 1,
        Selection::EucJp => 2,
        Selection::ShiftJis => 3,
        Selection::Incomplete => 4,
    }
}

#[test]
fn detector_matches_source_selection_and_replacement_for_every_prefixed_single_and_pair() {
    assert_eq!(PROBES.lines().count(), 18);
    for line in PROBES.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 6);
        let prefix = unhex(fields[1]);
        let suffix = unhex(fields[2]);
        let mut singles = Sha256::new();
        for byte in 0u8..=255 {
            let bytes = [&prefix[..], &[byte], &suffix[..]].concat();
            singles.update(decode(&bytes).as_bytes());
            singles.update(&[0]);
        }
        assert_eq!(hex(&singles.finish()), fields[3], "{} singles", fields[0]);
        let mut pairs = Sha256::new();
        let mut choices = Sha256::new();
        for first in 0u8..=255 {
            for second in 0u8..=255 {
                let bytes = [&prefix[..], &[first, second], &suffix[..]].concat();
                let text = decode(&bytes);
                assert!(text.len() <= bytes.len() * 3);
                pairs.update(text.as_bytes());
                pairs.update(&[0]);
                choices.update(&[selected(&bytes)]);
            }
        }
        assert_eq!(hex(&pairs.finish()), fields[4], "{} pairs", fields[0]);
        assert_eq!(hex(&choices.finish()), fields[5], "{} choices", fields[0]);
    }
}

#[test]
fn nonerror_iso_underflow_wins_and_failed_iso_prefixes_are_replayed_to_the_winner() {
    for bytes in [&b"\x1b"[..], &b"\x1b$"[..], &b"\x1b$B$"[..]] {
        assert_eq!(selected(bytes), 1);
        assert_eq!(decode(bytes), "\u{fffd}");
    }
    assert_eq!(decode(b"A\x1b$B$"), "A\u{fffd}");
    assert_eq!(selected(b"\x1b(J\x80A"), 3);
    assert_eq!(decode(b"\x1b(J\x80A"), "\x1b(J\u{fffd}A");
    assert_eq!(decode(b"\x1b$B\x80$\""), "\x1b$B\u{fffd}$\"");
}

#[test]
fn euc_jp_and_shift_jis_probes_select_by_error_then_consumed_input_then_heuristic() {
    for (bytes, choice, text) in [
        (&b"\x80 "[..], 3, "\u{fffd} "),
        (&b"\x8f\0"[..], 2, "\u{fffd}"),
        (&b"\xa1\xe0"[..], 2, "÷"),
        (&b"\xa1"[..], 3, "｡"),
        (&b"\x8f"[..], 4, "\u{fffd}"),
        (&b"\xa1\xa1"[..], 3, "｡｡"),
        (&b"\xa4\xa2\xa4\xa4"[..], 2, "あい"),
        (&b"\xa4\xa2"[..], 3, "､｢"),
        (&b"\x8e\xa6\x8e\xa7"[..], 2, "ｦｧ"),
        (&b"\xa4\xa2\x8e\xa6"[..], 3, "､｢示"),
        (&b"\x8e\xa1\x8e\xa4"[..], 3, "治痔"),
        (&b"\xa5\xa2\xa5\xa4"[..], 3, "･｢･､"),
        (&b"\x8e\xa5\x8e\xdf"[..], 2, "･ﾟ"),
    ] {
        assert_eq!(selected(bytes), choice, "{bytes:x?}");
        assert_eq!(decode(bytes), text, "{bytes:x?}");
    }
}

#[test]
fn japanese_thresholds_are_separate_inclusive_and_saturated_without_retaining_text() {
    for (unit, counted) in [(0x303f, false), (0x3040, true), (0x309f, true), (0x30a0, false), (0xff64, false), (0xff65, true), (0xff9f, true), (0xffa0, false)] {
        let mut counts = JapaneseCounts::default();
        counts.observe(unit);
        assert!(!counts.looks_japanese());
        counts.observe(unit);
        assert_eq!(counts.looks_japanese(), counted);
        for _ in 0..1024 { counts.observe(unit); }
        assert!(counts.hiragana <= 2 && counts.katakana <= 2);
        assert_eq!(counts.looks_japanese(), counted);
    }
    let mut counts = JapaneseCounts::default();
    counts.observe(0x3040);
    counts.observe(0xff65);
    assert!(!counts.looks_japanese());
    counts.observe(0xff9f);
    assert!(counts.looks_japanese());
}

#[test]
fn leading_ascii_controls_stay_literal_and_each_record_discards_selection_and_probe_state() {
    let ascii: Vec<_> = (0u8..128).filter(|byte| *byte != 0x1b).collect();
    assert_eq!(selected(&ascii), 0);
    assert_eq!(decode(&ascii).as_bytes(), ascii);
    assert_eq!(decode(b"\x0e\x1b(J\\~"), "\x0e¥‾");
    assert_eq!(decode(b"\x1b$B$\""), "あ");
    assert_eq!(decode(b"$\""), "$\"");
    assert_eq!(decode(b"\xa4\xa2\xa4\xa4"), "あい");
    assert_eq!(decode(b"\xa4\xa2"), "､｢");
    assert_eq!(decode(b"\x81\x60"), "〜");
    assert_eq!(decode(&[]), "");
}

#[test]
fn detector_provenance_and_shared_mapping_hashes_are_pinned_without_new_mapping_owners() {
    for (bytes, hash) in [
        (PROBES.as_bytes(), "981b592153362f82ab1b9cd80b2f7ea51fb1809943b752abb13f5f3d0a5cd3f9"),
        (include_bytes!("charsets.tsv").as_slice(), "e4799a4ba5d6bd6d64c9f1e3395f6fa0f7d1bfb04da46df5758e9e1c4645dd84"),
        (include_bytes!("mappings.tsv").as_slice(), "c9c1e3a89ab2dc9bb7c3cfc6a90cafae7fda79bea75418e1213e1369be174047"),
        (include_bytes!("sources.sha256").as_slice(), "9ce015fbad1d50d0e3401112ffb1c2191325ffb6604cd422d61a03c90e72451c"),
    ] { assert_eq!(hex(&openssl::sha::sha256(bytes)), hash); }
    assert_eq!(include_bytes!("mappings.tsv"), include_bytes!("../probe/mappings.tsv"));
}
