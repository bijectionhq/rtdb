use openssl::sha::Sha256;
use super::*;
use super::super::{Mapping, euc_jp::EucJp, iso2022jp::Iso2022Jp};

const MODES: &str = include_str!("modes.tsv");

#[derive(Clone, Copy)]
enum Candidate { Double(&'static Mapping), Euc(EucJp), Iso(Iso2022Jp) }

impl Candidate {
    fn select(family: &str, variant: &str) -> Self {
        match (family, variant) {
            ("double", "SJIS") => Self::Double(super::super::lookup("shift_jis").unwrap()),
            ("double", "MS932") => Self::Double(super::super::lookup("windows-31j").unwrap()),
            ("euc", "Standard") => Self::Euc(EucJp::Standard),
            ("euc", "Linux") => Self::Euc(EucJp::Linux),
            ("euc", "Solaris") => Self::Euc(EucJp::Solaris),
            ("iso", "Standard") => Self::Iso(Iso2022Jp::Standard),
            ("iso", "Jp2") => Self::Iso(Iso2022Jp::Jp2),
            ("iso", "Ms5022x") => Self::Iso(Iso2022Jp::Ms5022x),
            ("iso", "MsIso") => Self::Iso(Iso2022Jp::MsIso),
            _ => panic!("unknown source candidate {family} {variant}"),
        }
    }

    fn write_probe(self, bytes: &[u8], units: &mut Vec<u16>, hash: &mut Sha256) {
        units.clear();
        let probe = match self {
            Self::Double(mapping) => mapping.probe(bytes),
            Self::Euc(decoder) => decoder.probe(bytes, |unit| units.push(unit)),
            Self::Iso(decoder) => decoder.probe(bytes),
        };
        assert!(probe.consumed <= bytes.len());
        assert!(probe.utf16_units <= probe.consumed);
        hash.update(&u32::try_from(probe.consumed).unwrap().to_be_bytes());
        hash.update(&u32::try_from(probe.utf16_units).unwrap().to_be_bytes());
        let (kind, width) = match probe.stop {
            ProbeStop::Underflow => (0, 0),
            ProbeStop::Error(DecodeError::Malformed { bytes }) => (1, bytes),
            ProbeStop::Error(DecodeError::Unmappable { bytes }) => (2, bytes),
        };
        hash.update(&[kind, u8::try_from(width).unwrap()]);
        if matches!(self, Self::Euc(_)) {
            assert_eq!(units.len(), probe.utf16_units);
            for unit in units { hash.update(&unit.to_be_bytes()); }
        } else { assert!(units.is_empty()); }
    }

    fn append(self, bytes: &[u8], output: &mut String) {
        match self {
            Self::Double(mapping) => mapping.decode_into(bytes, output),
            Self::Euc(decoder) => decoder.decode_into(bytes, output),
            Self::Iso(decoder) => decoder.decode_into(bytes, output),
        }
    }
}

fn digest_hex(digest: Sha256) -> String {
    digest.finish().iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    openssl::sha::sha256(bytes).iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn strict_probes_and_appends_cover_all_single_bytes_and_pairs_in_every_source_mode() {
    assert_eq!(MODES.lines().count(), 54);
    for line in MODES.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 7);
        let candidate = Candidate::select(fields[0], fields[1]);
        let mut record: Vec<_> = fields[3].as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect();
        let suffix = record.len();
        record.push(0);
        let mut singles = Sha256::new();
        let mut units = Vec::with_capacity(record.len() + 1);
        for byte in 0u8..=255 {
            record[suffix] = byte;
            candidate.write_probe(&record, &mut units, &mut singles);
        }
        assert_eq!(digest_hex(singles), fields[4], "{} {} {} singles", fields[0], fields[1], fields[2]);
        record.push(0);
        let mut pairs = Sha256::new();
        let mut appended = Sha256::new();
        let mut output = String::with_capacity(7 + record.len() * 3);
        let capacity = output.capacity();
        for first in 0u8..=255 {
            record[suffix] = first;
            for second in 0u8..=255 {
                record[suffix + 1] = second;
                candidate.write_probe(&record, &mut units, &mut pairs);
                output.clear();
                output.push_str("caller:");
                candidate.append(&record, &mut output);
                assert_eq!(output.capacity(), capacity);
                appended.update(output.as_bytes());
                appended.update(&[0]);
            }
        }
        assert_eq!(digest_hex(pairs), fields[5], "{} {} {} pairs", fields[0], fields[1], fields[2]);
        assert_eq!(digest_hex(appended), fields[6], "{} {} {} append", fields[0], fields[1], fields[2]);
    }
}

#[test]
fn strict_double_byte_results_keep_the_bad_group_unconsumed_and_classify_lead_row_identity() {
    let mapping = super::super::lookup("shift_jis").unwrap();
    for (bytes, expected) in [
        (&b"A\x82\xa0\x81"[..], StrictProbe { consumed: 3, utf16_units: 2, stop: ProbeStop::Underflow }),
        (&b"A\x80\0"[..], StrictProbe { consumed: 1, utf16_units: 1, stop: ProbeStop::Error(DecodeError::Malformed { bytes: 1 }) }),
        (&b"A\x81\0"[..], StrictProbe { consumed: 1, utf16_units: 1, stop: ProbeStop::Error(DecodeError::Malformed { bytes: 1 }) }),
        (&b"A\x81\xe9"[..], StrictProbe { consumed: 1, utf16_units: 1, stop: ProbeStop::Error(DecodeError::Malformed { bytes: 1 }) }),
        (&b"A\x81\xeb"[..], StrictProbe { consumed: 1, utf16_units: 1, stop: ProbeStop::Error(DecodeError::Unmappable { bytes: 2 }) }),
        (&b"\xff"[..], StrictProbe { consumed: 0, utf16_units: 0, stop: ProbeStop::Underflow }),
    ] { assert_eq!(mapping.probe(bytes), expected, "{bytes:x?}"); }
}

#[test]
fn strict_euc_probes_emit_only_completed_units_and_check_group_availability_before_trails() {
    for decoder in [EucJp::Standard, EucJp::Linux, EucJp::Solaris] {
        let mut units = Vec::new();
        assert_eq!(decoder.probe(b"A\xa4\xa2\x8e\xa6\x8f\xa1", |unit| units.push(unit)), StrictProbe { consumed: 5, utf16_units: 3, stop: ProbeStop::Underflow });
        assert_eq!(units, [0x41, 0x3042, 0xff66]);
        for (bytes, stop) in [
            (&b"A\x8f "[..], ProbeStop::Underflow),
            (&b"A\x8f  "[..], ProbeStop::Error(DecodeError::Unmappable { bytes: 3 })),
            (&b"A\x8e "[..], ProbeStop::Error(DecodeError::Unmappable { bytes: 2 })),
            (&b"A\x80 "[..], ProbeStop::Error(DecodeError::Unmappable { bytes: 2 })),
        ] {
            units.clear();
            assert_eq!(decoder.probe(bytes, |unit| units.push(unit)), StrictProbe { consumed: 1, utf16_units: 1, stop }, "{decoder:?} {bytes:x?}");
            assert_eq!(units, [0x41]);
        }
    }
}

#[test]
fn strict_iso_probes_preserve_control_progress_and_variant_specific_malformed_widths() {
    for decoder in [Iso2022Jp::Standard, Iso2022Jp::Jp2, Iso2022Jp::Ms5022x, Iso2022Jp::MsIso] {
        for (bytes, expected) in [
            (&b"A\x1b$B$\""[..], StrictProbe { consumed: 6, utf16_units: 2, stop: ProbeStop::Underflow }),
            (&b"A\x1bX"[..], StrictProbe { consumed: 1, utf16_units: 1, stop: ProbeStop::Underflow }),
            (&b"A\x1bXZ"[..], StrictProbe { consumed: 1, utf16_units: 1, stop: ProbeStop::Error(DecodeError::Malformed { bytes: 2 }) }),
            (&b"A\x1b(Q"[..], StrictProbe { consumed: 1, utf16_units: 1, stop: ProbeStop::Error(DecodeError::Malformed { bytes: 3 }) }),
            (&b"\x1b$B\x80$\""[..], StrictProbe { consumed: 3, utf16_units: 0, stop: ProbeStop::Error(DecodeError::Malformed { bytes: 1 }) }),
            (&b"\x1b$B$\0"[..], StrictProbe { consumed: 3, utf16_units: 0, stop: ProbeStop::Error(DecodeError::Unmappable { bytes: 2 }) }),
            (&b"\x1b$B\x0e\x1b(B\x0f$\""[..], StrictProbe { consumed: 10, utf16_units: 1, stop: ProbeStop::Underflow }),
        ] { assert_eq!(decoder.probe(bytes), expected, "{decoder:?} {bytes:x?}"); }
        let supports0212 = matches!(decoder, Iso2022Jp::Jp2 | Iso2022Jp::Ms5022x);
        assert_eq!(decoder.probe(b"\x1b$("), StrictProbe {
            consumed: 0, utf16_units: 0,
            stop: if supports0212 { ProbeStop::Underflow } else { ProbeStop::Error(DecodeError::Malformed { bytes: 3 }) },
        });
        assert_eq!(decoder.probe(b"\x1b$(Q"), StrictProbe {
            consumed: 0, utf16_units: 0,
            stop: ProbeStop::Error(DecodeError::Malformed { bytes: if supports0212 { 4 } else { 3 } }),
        });
    }
}

#[test]
fn append_keeps_the_caller_prefix_and_resets_only_decoder_state_between_records() {
    for decoder in [Iso2022Jp::Standard, Iso2022Jp::Jp2, Iso2022Jp::Ms5022x, Iso2022Jp::MsIso] {
        let mut output = String::from("caller:");
        decoder.decode_into(b"\x1b$B\x80$\"", &mut output);
        assert_eq!(output, "caller:\u{fffd}あ");
        decoder.decode_into(b"$\"", &mut output);
        assert_eq!(output, "caller:\u{fffd}あ$\"");
        decoder.decode_into(&[], &mut output);
        assert_eq!(output, "caller:\u{fffd}あ$\"");
    }
    for decoder in [EucJp::Standard, EucJp::Linux, EucJp::Solaris] {
        let mut output = String::from("caller:");
        decoder.decode_into(b"A\x8f ", &mut output);
        assert_eq!(output, "caller:A\u{fffd}");
        decoder.decode_into(b"\xa4\xa2", &mut output);
        assert_eq!(output, "caller:A\u{fffd}あ");
    }
    let mut output = String::from("caller:");
    super::super::lookup("shift_jis").unwrap().decode_into(b"A\x81\x82\xa0", &mut output);
    assert_eq!(output, "caller:A≠\u{fffd}");
}

#[test]
fn append_preserves_pair_callback_count_order_and_mutations_between_pairs() {
    use std::cell::{Cell, RefCell};
    let mapping = super::super::lookup("big5").unwrap();
    let calls = RefCell::new(Vec::new());
    let initialized = Cell::new(false);
    let mut output = String::from("caller:");
    mapping.decode_with_pairs_into(&[b'A', 0xf9, 0xd6, 0, 0xf9, 0xd7], |first, second| {
        calls.borrow_mut().push((first, second));
        let unit = if initialized.get() && (first, second) == (0xf9, 0xd7) { 0x92b9 } else { mapping.pair(first, second) };
        initialized.set(true);
        unit
    }, &mut output);
    assert_eq!(&*calls.borrow(), &[(0xf9, 0xd6), (0xd6, 0), (0xf9, 0xd7)]);
    assert_eq!(output, "caller:A\u{fffd}\u{fffd}\0銹");
    calls.borrow_mut().clear();
    mapping.decode_with_pairs_into(&[b'A', 0xf9], |first, second| {
        calls.borrow_mut().push((first, second));
        mapping.pair(first, second)
    }, &mut output);
    assert!(calls.borrow().is_empty());
    assert_eq!(output, "caller:A\u{fffd}\u{fffd}\0銹A\u{fffd}");
}

#[test]
fn double_byte_append_retains_utf16_pairing_and_every_existing_static_configuration() {
    for mapping in &super::super::tables::TABLES {
        for bytes in [&b""[..], &b"A\0\xff\x81\x82\xa0"[..], &b"\xf9\xd6\0\xf9\xd7"[..]] {
            let mut output = String::from("caller:");
            mapping.decode_into(bytes, &mut output);
            assert_eq!(output, format!("caller:{}", mapping.decode(bytes)));
        }
    }
    let mapping = super::super::lookup("shift_jis").unwrap();
    let mut output = String::from("caller:");
    mapping.decode_with_pairs_into(&[0x81, 0x40, 0x81, 0x40, 0x81], {
        let high = std::cell::Cell::new(true);
        move |_, _| if high.replace(false) { 0xd840 } else { 0xdc00 }
    }, &mut output);
    assert_eq!(output, "caller:\u{20000}\u{fffd}");
}

#[test]
fn probe_sources_and_reused_mapping_resources_match_the_pinned_derivation() {
    assert_eq!(sha256_hex(MODES.as_bytes()), "1ed9975d8542a3f2ef7d7a3326af0e55bc27a3cc3c95c6f039f68d57a3640934");
    assert_eq!(sha256_hex(include_bytes!("mappings.tsv")), "c9c1e3a89ab2dc9bb7c3cfc6a90cafae7fda79bea75418e1213e1369be174047");
    assert_eq!(sha256_hex(include_bytes!("sources.sha256")), "0d52785f8162d20531d8dc0e5f9b8b6dda4eba64a42bc0e1cfd5913f89f35f16");
    let resources: [&[u8]; 10] = [
        include_bytes!("../shift_jis.bin"), include_bytes!("../windows-31j.bin"),
        include_bytes!("../euc_jp/jis_x_0201.bin"), include_bytes!("../euc_jp/jis_x_0208.bin"),
        include_bytes!("../euc_jp/jis_x_0212.bin"), include_bytes!("../euc_jp/jis_x_0208_solaris.bin"),
        include_bytes!("../euc_jp/jis_x_0212_solaris.bin"), include_bytes!("../iso2022jp/jis_x_0208_ms5022x.bin"),
        include_bytes!("../iso2022jp/jis_x_0212_ms5022x.bin"), include_bytes!("../iso2022jp/jis_x_0208_ms932.bin"),
    ];
    let mappings = include_str!("mappings.tsv");
    assert_eq!(mappings.lines().count(), resources.len() + 1);
    for (line, resource) in mappings.lines().skip(1).zip(resources) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 6);
        assert_eq!(resource.len(), fields[2].parse::<usize>().unwrap());
        assert_eq!(sha256_hex(resource), fields[3], "{}", fields[0]);
    }
}
