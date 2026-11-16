use rtdb_types::{decimal_digit, decimal_spelling, decimal_text_scale};

#[test]
fn ascii_decimal_spelling_borrows_original_input() {
    let text = String::from("-001.250E+0002");
    let spelling = decimal_spelling(&text).unwrap();
    assert_eq!(spelling.ascii().as_ptr(), text.as_ptr());
    assert_eq!(spelling.ascii(), text);
    assert_eq!(spelling.scale(), 1);
}

#[test]
fn openjdk21_unicode15_bmp_decimal_ranges_match_pinned_unicode_data() {
    let fixture = include_str!("fixtures/openjdk21-bmp-decimal-zeroes.txt");
    for line in fixture.lines() {
        let fields = line.split(';').collect::<Vec<_>>();
        assert_eq!(fields[2], "Nd");
        assert_eq!(fields[6], "0");
        let zero = u32::from_str_radix(fields[0], 16).unwrap();
        for digit in 0..10 {
            let character = char::from_u32(zero + digit).unwrap();
            assert_eq!(decimal_digit(character), Some(digit as u8), "{line}");
            let text = character.to_string();
            let spelling = decimal_spelling(&text).unwrap();
            assert_eq!(spelling.ascii(), digit.to_string(), "{line}");
            assert_eq!(spelling.scale(), 0);
            assert!(spelling.ascii().len() <= text.len());
        }
        assert_eq!(decimal_digit(char::from_u32(zero - 1).unwrap()), None, "{line}");
        assert_eq!(decimal_digit(char::from_u32(zero + 10).unwrap()), None, "{line}");
    }
    assert_eq!(fixture.lines().count(), 37);
}

#[test]
fn mixed_bmp_digits_preserve_sign_point_and_exponent_spelling() {
    for (text, expected, scale) in [
        ("+١२.３৪e-๕", "+12.34e-5", 7),
        ("-٠.۰੦", "-0.00", 2),
        (".٩", ".9", 1),
        ("１.", "1.", 0),
        ("١E+٢١٤٧٤٨٣٦٤٨", "1E+2147483648", i32::MIN),
        ("१.०E+२१४७४८३६४९", "1.0E+2147483649", i32::MIN),
        ("１E-２１４７４８３６４７", "1E-2147483647", i32::MAX),
        ("1e٠۰०০০੦૦୦௦౦೦൦๐໐༠၀0١", "1e000000000000000001", -1),
    ] {
        let spelling = decimal_spelling(text).unwrap();
        assert_eq!(spelling.ascii(), expected, "{text}");
        assert_eq!(spelling.scale(), scale, "{text}");
        assert_eq!(decimal_text_scale(text).unwrap(), scale, "{text}");
    }
}

#[test]
fn java_char_digit_excludes_astral_digits_and_non_decimal_numbers() {
    for character in ['𝟎', '𐒠', '𞥐', '²', '①', 'Ⅷ', '四', '௰', '½', 'A', 'ａ'] {
        assert_eq!(decimal_digit(character), None, "{character}");
        assert!(decimal_spelling(&character.to_string()).is_err(), "{character}");
        assert!(decimal_spelling(&format!("1e{character}")).is_err(), "{character}");
    }
}

#[test]
fn unicode_digits_do_not_expand_decimal_punctuation_or_scale_range() {
    for text in [
        "", "+", "-", ".", "e١", "١e", "١e+", "١e-", "١e++١", "١e١e١", "١..٢", "١.٢.٣",
        "−١", "＋١", "١٫٢", "١．٢", "١Ｅ٢", "١e−٢", "١_٢", "١ ٢", " ١", "١\n", "١\0",
        "١E-٢١٤٧٤٨٣٦٤٨", "١E+٢١٤٧٤٨٣٦٤٩", "١e١٠٠٠٠٠٠٠٠٠٠", "١e٠٠٠١٠٠٠٠٠٠٠٠٠٠",
    ] { assert!(decimal_spelling(text).is_err(), "{text:?}"); }
}
