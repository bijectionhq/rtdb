use super::*;

fn accepts(body: &str, value: char) -> bool {
    fancy_regex::Regex::new(&format!("\\A[{body}]\\z")).unwrap().is_match(&value.to_string()).unwrap()
}

#[test]
fn names_follow_java_control_labels_and_unnamed_assigned_ranges() {
    for (name, value) in [
        ("latin small letter a", 0x61), ("  LATIN SMALL LETTER A\t", 0x61),
        ("LATIN SMALL LETTER A WITH BREVE AND ACUTE", 0x1eaf),
        ("BEL", 7), ("BELL", 0x1f514), ("NULL", 0),
        ("PADDING CHARACTER", 0x80), ("HIGH OCTET PRESET", 0x81),
        ("SINGLE GRAPHIC CHARACTER INTRODUCER", 0x99),
        ("HANGUL SYLLABLES AC00", 0xac00),
        ("CJK UNIFIED IDEOGRAPHS 4E00", 0x4e00),
        ("PRIVATE USE AREA E000", 0xe000), ("HIGH SURROGATES D800", 0xd800),
        ("LATIN SMALL LETTER LONG S", 0x17f),
    ] { assert_eq!(named_character(name), Some(value), "{name}"); }
    for name in ["LF", "U+0061", "HANGUL SYLLABLE GA", "CJK UNIFIED IDEOGRAPH-4E00", "BASIC LATIN 61", "PRIVATE USE AREA 0E000", "GREEK 378", "\u{a0}NULL", "not a character"] {
        assert_eq!(named_character(name), None, "{name}");
    }
    assert_eq!(named_character("latin ſmall letter a"), Some(0x61));
}

#[test]
fn java_property_names_are_validated_before_native_class_compilation() {
    for name in ["L", "Lu", "LC", "LD", "L1", "all", "javaIdeographic", "IsAlphabetic", "IsGreek", "IsGrek", "IsWhite_Space", "InBasic Latin", "InBasicLatin", "InBASIC_LATIN", "InGreekandCoptic", "InGreek", "InCyrillicSupplementary", "sc=Old_Italic", "SCRIPT=Ital", "block=Basic_Latin", "gc=Lu", "general_category=javaLetter"] {
        assert!(property_class(name, false, false).is_ok(), "{name}");
    }
    for name in ["Letter", "Alphabetic", "White_Space", "Latin", "IsMath", "IsDefault_Ignorable_Code_Point", "gc=Uppercase_Letter", "gc=lu", "GeneralCategory=Lu", "sc=Old Italic", "InBasic-Latin", "In Basic Latin", "InBasic__Latin", "InNo_Block", "scx=Latin", "IsL&", "\u{a0}L"] {
        assert!(property_class(name, false, false).is_err(), "{name}");
    }
    assert!(property_class("lower", false, false).is_err());
    assert!(property_class("lower", true, false).is_ok());
    assert!(property_class("InBaſicLatin", false, false).is_ok());
    assert!(property_class("blocK=Basic_Latin", false, false).is_ok());
}

#[test]
fn properties_use_pinned_tables_and_java_identifier_profiles() {
    for (name, unicode, insensitive, value, expected) in [
        ("javaUnicodeIdentifierStart", false, false, '\u{2e2f}', true),
        ("javaUnicodeIdentifierPart", false, false, '\u{2e2f}', true),
        ("javaUnicodeIdentifierPart", false, false, '\0', true),
        ("javaJavaIdentifierStart", false, false, '$', true),
        ("javaUnicodeIdentifierStart", false, false, '$', false),
        ("IsEmoji", false, false, '😀', true),
        ("IsExtended_Pictographic", false, false, '😀', true),
        ("IsHexDigit", false, false, '٣', true),
        ("IsAssigned", false, false, '\u{378}', false),
        ("IsUnknown", false, false, '\u{378}', true),
        ("InGreek", false, false, '\u{378}', true),
        ("Lower", false, true, 'A', true),
        ("Lower", false, true, 'é', false),
        ("Lower", true, true, 'É', true),
        ("Lu", false, true, 'é', true),
        ("Print", true, false, '\t', false),
        ("Print", true, false, '\u{a0}', true),
        ("IsWord", false, false, '\u{200c}', true),
        ("javaWhitespace", false, false, '\u{a0}', false),
        ("IsWhite_Space", false, false, '\u{a0}', true),
        ("Cs", false, false, 'a', false),
        ("InSURROGATES_AREA", false, false, 'a', false),
        ("InTULU_TIGALARI", false, false, '\u{11380}', true),
    ] { assert_eq!(accepts(&property_class(name, unicode, insensitive).unwrap(), value), expected, "{name} {value:?}"); }
}

#[test]
fn case_expansion_preserves_literal_single_slice_and_range_distinctions() {
    assert!(!accepts(&literal_single('ß'), 'ẞ'));
    assert!(accepts(&folded_literal('ß' as u32), 'ẞ'));
    assert!(!accepts(&class_single('ß'), 'ẞ'));
    assert!(!accepts(&class_range('ẞ', 'ẞ'), 'ß'));
    assert!(accepts(&class_range('ß', 'ß'), 'ẞ'));
    assert!(accepts(&literal_single('ẞ'), 'ß'));
    for value in ['I', 'i', '\u{130}', '\u{131}'] {
        assert!(accepts(&literal_single('i'), value));
        assert!(accepts(&class_single('i'), value));
    }
    assert!(accepts(&class_range('a', 'z'), 'K'));
    assert!(accepts(&class_range('a', 'z'), 'İ'));
    assert!(accepts(&class_single('µ'), 'Μ'));
    assert!(accepts(&class_single('µ'), 'μ'));
    assert!(accepts(&literal_single('Σ'), 'ς'));
    assert!(accepts(&literal_single('𐐀'), '𐐨'));
}
