use rtdb_plan::SourceSpan;
use rtdb_xtql::{EdnKind, read};

#[test]
fn integer_reader_normalizes_signed_octal_hex_and_both_radix_markers() {
    for (source, expected) in [("012", 10), ("0xFE", 254), ("+0Xff", 255), ("-16rFF", -255), ("2R101", 5), ("36rz", 35)] {
        assert_eq!(read(source).unwrap().kind, EdnKind::Integer(expected), "{source}");
        assert_eq!(read(&format!("{source}N")).unwrap().kind, EdnKind::BigInteger(expected.to_string()), "{source}N");
    }
    for source in ["9223372036854775808", "9223372036854775808N", "0x8000000000000000N"] {
        assert_eq!(read(source).unwrap().kind, EdnKind::BigInteger("9223372036854775808".into()), "{source}");
    }
    assert_eq!(read("-9223372036854775809N").unwrap().kind, EdnKind::BigInteger("-9223372036854775809".into()));
    assert_eq!(read("1e3").unwrap().kind, EdnKind::Float(1000.0));
}

#[test]
fn invalid_integer_spellings_cannot_be_preserved_as_unchecked_bigint_text() {
    for source in ["08N", "0xN", "1r0N", "37R0N", "02r1N", "100r1N", "2r2N", "16r-1N", "1_0N", "1e3N", "1.0N"] {
        assert!(read(source).is_err(), "{source}");
    }
    for source in ["{012N :a 10N :b}", "#{0xffN 16rffN}", "#{10 012N}", "{012N :a 10 :b}", "#{[012N] (10)}", "{1.0M :a 1.00M :b}"] {
        assert!(read(source).is_err(), "{source}");
    }
    assert!(read("#{1 1.0 1M}").is_ok());
}

#[test]
fn integral_ratio_tokens_preserve_clojure_integer_widths() {
    for (source, expected) in [("06/03", 2), ("-10/2", -5), ("+4/2", 2), ("0/3", 0)] {
        assert_eq!(read(source).unwrap().kind, EdnKind::Integer(expected), "{source}");
    }
    assert_eq!(read("9223372036854775808/2").unwrap().kind, EdnKind::BigInteger("4611686018427387904".into()));
    assert_eq!(read("-9223372036854775808/1").unwrap().kind, EdnKind::BigInteger(i64::MIN.to_string()));
    assert_eq!(read("0/9223372036854775808").unwrap().kind, EdnKind::BigInteger("0".into()));
    assert!(read("#{6/3 2}").is_err());
}

#[test]
fn non_integral_ratio_tokens_reduce_exactly_and_keep_the_source_span() {
    let form = read("  +006/008").unwrap();
    assert_eq!(form.span, SourceSpan { start: 2, end: 10 });
    assert_eq!(form.kind, EdnKind::Ratio("3/4".into()));

    for (source, expected) in [
        ("-010/06", "-5/3"),
        ("08/09", "8/9"),
        ("300000000000000000000000000000000000000/900000000000000000000000000000000000000", "1/3"),
        ("100000000000000000000000000000000000001/100000000000000000000000000000000000003",
         "100000000000000000000000000000000000001/100000000000000000000000000000000000003"),
    ] {
        assert_eq!(read(source).unwrap().kind, EdnKind::Ratio(expected.into()), "{source}");
    }
    assert_eq!(read("#{2/4 1/2}").unwrap_err().message.as_deref(), Some("Duplicate EDN set element"));
    assert_eq!(read("{[2/4] :a (1/2) :b}").unwrap_err().message.as_deref(), Some("Duplicate EDN map key"));
    assert!(read("#{1/2 0.5 0.5M}").is_ok());
}

#[test]
fn ratio_reader_rejects_zero_denominators_and_non_edn_spellings() {
    for source in ["1/0", "0/0", "-10/000"] {
        let error = read(source).unwrap_err();
        assert_eq!(error.code, "xtql/edn-reader");
        assert_eq!(error.message.as_deref(), Some("Ratio denominator is zero"));
    }
    for source in ["2/-1", "2/+3", "2/", "2/3/4", "2/0x3", "0x2/3", "2/3M", "2/3N", "2/٣"] {
        assert_eq!(read(source).unwrap_err().code, "xtql/edn-reader", "{source}");
    }
    assert_eq!(read("2/-1").unwrap_err().message.as_deref(), Some("Invalid ratio literal"));
}
