use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext, EvaluationScope, scalar_call};
use rtdb_types::Value;

fn text(value: &str) -> Value { Value::Utf8(value.into()) }
fn matches(source: &str, pattern: &str, flags: &str) -> bool {
    scalar_call("like_regex", &[text(source), text(pattern), text(flags)], &EvalContext::default()).unwrap() == Value::Bool(true)
}
fn replace(source: &str, pattern: &str, replacement: &str) -> String {
    match scalar_call("regexp_replace", &[text(source), text(pattern), text(replacement)], &EvalContext::default()).unwrap() {
        Value::Utf8(value) => value,
        value => panic!("Expected text, got {value:?}"),
    }
}

#[test]
fn lookarounds_capture_backreferences_atomic_groups_and_possessive_quantifiers() {
    for (source, pattern, expected) in [
        ("zabz", r"(?<=a)b(?=z)", true),
        ("zabz", r"(?<!a)b", false),
        ("abac", r"a(?!b)", true),
        ("aa", r"(a)\1", true),
        ("abab", r"(?<first>a)(b)\k<first>\2", true),
        ("aba", r"\A(?>ab|a)b\z", false),
        ("ab", r"\A(?>ab|a)b\z", false),
        ("ab", r"\A(ab|a)b\z", true),
        ("aa", r"\Aa++a\z", false),
        ("aa", r"\Aa+a\z", true),
        ("aaa", r"\Aa{2,3}\z", true),
        ("aaa", r"\Aa{2}?\z", false),
    ] { assert_eq!(matches(source, pattern, ""), expected, "{source:?} / {pattern}"); }
}

#[test]
fn variable_lookbehind_uses_nearest_start_and_preserves_capture_atomicity() {
    for (source, pattern, expected) in [
        ("aaab", r"(?<=a{1,3})b", true),
        ("aaab", r"(?<!a{1,3})b", false),
        ("xxb", r"(?<!a{1,3})b", true),
        ("aaab", r"(?<=a+)b", true),
        ("aaab", r"(?<=a*)b", true),
        ("aab", r"(?<=a{2,4})b", true),
        ("ab", r"(?<=a{2,4})b", false),
        ("aaa", r"(?<=a{1,3})a", true),
        ("aab", r"\A(?=(a|aa))\1b\z", false),
        ("aaab", r"aaa(?<=(a{1,3}))\1b", false),
        ("😀😀b", r"(?<=😀{1,3})b", true),
    ] { assert_eq!(matches(source, pattern, ""), expected, "{source:?} / {pattern}"); }
    assert_eq!(replace("aaab", r"(?<=(a{1,3}))b", "$1"), "aaaa");
    assert_eq!(replace("abc", r"(?<=(ab|b))c", "$1"), "abb");
    assert_eq!(replace("aa", r"(?<=(a+))a", "$1"), "aa");
    assert!(scalar_call("like_regex", &[text("aa"), text(r"(a)(?<=\1)a"), text("")], &EvalContext::default()).is_err());
}

#[test]
fn previous_match_anchor_retains_match_end_after_empty_replacements() {
    assert_eq!(replace("ab", r"\G", "x"), "xab");
    assert_eq!(replace("ab", r"\G|b", "x"), "xaxx");
    assert_eq!(replace("aa", r"\Ga*", "x"), "xx");
    assert_eq!(replace("ab", r"\G.", "x"), "xx");
    assert_eq!(replace("éa", r"\G|a", "x"), "xéxx");
}

#[test]
fn java_quoted_hex_octal_control_and_supplementary_escapes() {
    for (source, pattern) in [
        ("[a]+.$", r"\A\Q[a]+.$\E\z"),
        ("a", r"\x61"), ("é", r"\u00e9"), ("😀", r"\x{1f600}"),
        ("😀", r"\uD83D\uDE00"), ("A", r"\0101"), ("\u{1}", r"\cA"),
        ("\u{1b}", r"\e"), ("\u{7}", r"\a"),
    ] { assert!(matches(source, pattern, ""), "{source:?} / {pattern}"); }
}

#[test]
fn grapheme_boundaries_preserve_java_source_node_selection_and_synthetic_atoms() {
    for (source, pattern, expected) in [
        ("ab", r"\Aa+\b{g}b\z", true),
        ("ab", r"\A(a)+\b{g}b\z", false),
        ("ab", r"\A(?:a)+\b{g}b\z", false),
        ("ab", r"\A(a|a)+\b{g}b\z", true),
        ("ab", r"\A(a)?\b{g}b\z", true),
        ("ab", r"\Aa?\b{g}b\z", false),
        ("ab", r"\A(?=a)a\b{g}b\z", false),
        ("ab", r"\Aa(?<=a)\b{g}b\z", true),
        ("a\u{301}b\u{301}", r"\Aa\x{301}b\b{g}\x{301}\z", true),
        ("\na", r"\A\R\b{g}a\z", true),
        ("a ", r"\Aa\b\b{g} \z", true),
        ("a\u{301} ", r"\Aa\x{301}\b\b{g} \z", true),
        ("𐐀 ", r"(?U)\A.\b\b{g} \z", true),
        ("a\n", r"\Aa\Z\b{g}\n\z", true),
        ("abc", r"\Aab\b{g}c\z", true),
        ("e\u{301}b", r"\A(?c)[é]\b{g}b\z", true),
        ("ab", r"(?x)\A a+ \b { g } b \z", true),
        ("aa", r"\Aaa(?<=a+a)\b{g}\z", true),
        ("aa", r"\Aaa(?<=a+a)\b{g}(?:|😀)", false),
    ] { assert_eq!(matches(source, pattern, ""), expected, "{source:?} / {pattern}"); }
    assert_eq!(scalar_call("like_regex", &[text("ab"), text(r"\A(?:(?=ab)c|a\b{g}b)\z"), text("")], &EvalContext::default()).unwrap_err().code, "xtdb.error/unknown");
}

#[test]
fn grapheme_boundary_replacement_assembles_utf16_before_java_utf8_encoding() {
    assert_eq!(replace("😀", r"\b{g}", "x"), "x😀x");
    assert_eq!(replace("😀", r"\b{g}|", "x"), "x?x?x");
    assert_eq!(replace("😀", r"\b{g}|", ""), "😀");
    assert_eq!(replace("😀b", r"(?<=(.))\b{g}b", "$1"), "😀?");
    assert_eq!(replace("😀b", r"(?<=(\p{Cs}))\b{g}b", "$1"), "😀?");
    assert_eq!(replace("😀b", r"(?<=([\uD800-\uDFFF]))\b{g}b", "$1"), "😀?");
    assert_eq!(replace("😀b", r"(?<=(.))\b{g}(?:b|😀)", "$1"), "😀😀");
    assert_eq!(replace("😀b", r"(?<=(.))\b{g}(?:b|\x{1f600})", "$1"), "😀?");
    assert_eq!(replace("😀b", r"a\uD800|(?!\A).\b{g}b", "x"), "?x");
    assert_eq!(replace("😀b", r"(?iu)a\uD800|(?!\A).\b{g}b", "x"), "?x");
}

#[test]
fn java_predefined_classes_are_ascii_until_unicode_character_class_is_enabled() {
    for (source, pattern, expected) in [
        ("٣", r"\d", false), ("٣", r"(?U)\d", true),
        ("é", r"\w", false), ("é", r"(?U)\w", true),
        ("\u{a0}", r"\s", false), ("\u{a0}", r"(?U)\s", true),
        ("\u{180e}", r"\h", true), ("F", r"\h", false),
        ("\u{2028}", r"\v", true), ("a", r"\H", true),
        ("\r\n", r"\A\R\z", true), ("\r\n", r"\A\R\n\z", true),
        ("\u{85}", r"(?U)\p{Cntrl}", true),
    ] { assert_eq!(matches(source, pattern, ""), expected, "{source:?} / {pattern}"); }
}

#[test]
fn word_boundaries_follow_java_combining_mark_base_search_in_utf16_units() {
    for (source, pattern, expected) in [
        ("a\u{301}", "a\\B\u{301}\\b", true),
        ("a\u{301}\u{302}", "a\\B\u{301}\\B\u{302}\\b", true),
        ("_\u{301}", "_\\b\u{301}\\B", true),
        (" \u{301}", " \\B\u{301}\\B", true),
        ("é\u{301}", "é\\b\u{301}\\b", true),
        ("é\u{301}", "(?U)é\\B\u{301}\\b", true),
        ("é", "\\bé\\b", false),
        ("é", "(?U)\\bé\\b", true),
        ("𐐀\u{301}", "𐐀\\B\u{301}\\B", true),
        ("a\u{1d167}", "a\\B\u{1d167}\\B", true),
        ("a\u{1d167}\u{301}", "a\\B\u{1d167}\\B\u{301}\\B", true),
        ("a\u{1d167}", "(?U)a\\B\u{1d167}\\b", true),
        ("", r"\B", true),
        ("", r"\b", false),
    ] { assert_eq!(matches(source, pattern, ""), expected, "{source:?} / {pattern}"); }
    assert_eq!(replace("a\u{301} b", r"\b", "|"), "|a\u{301}| |b|");
}

#[test]
fn extended_graphemes_flow_through_matching_captures_and_replacement() {
    for source in ["a\u{301}", "\r\n", "🇫🇷", "👩🏽\u{200d}💻", "\u{915}\u{94d}\u{915}"] {
        assert!(matches(source, r"\A\X\z", ""), "{source:?}");
        assert_eq!(replace(source, r"(\X)", "[$1]"), format!("[{source}]"));
    }
    assert_eq!(replace("a\u{301}🇫🇷b", r"\X", "x"), "xxx");
    assert!(!matches("a\u{301}", r"\A\X\p{Mn}\z", ""));
    assert!(!matches("", r"\X", ""));
    assert!(!matches("ab", r"\A\X\z", ""));
    assert!(!matches("ba", r"(?<=\X)a", ""));
    assert!(scalar_call("like_regex", &[text("x"), text(r"[\X]"), text("")], &EvalContext::default()).is_err());
}

#[test]
fn inline_canonical_equivalence_wraps_classes_and_properties_but_not_literal_runs() {
    for (source, pattern, expected) in [
        ("e\u{301}", r"\A(?c)[é]\z", true),
        ("e\u{301}", r"\A(?c)é\z", false),
        ("e\u{301}", r"\A(?c)[e]", false),
        ("e\u{301}", r"\A(?c)\p{Ll}\z", true),
        ("e\u{301}", r"\A(?c)\w\z", false),
        ("e\u{301}", r"\A(?c)(?-c:[é])\z", false),
        ("A\u{30a}\u{301}", r"\A(?c)[ÅǺ]\x{301}\z", true),
        ("\u{1100}\u{1161}\u{11a8}", r"\A(?c)[각]\z", true),
        ("\u{340}", r"\A(?c)[\u0340]\z", true),
        ("\u{300}", r"\A(?c)[\u0340]\z", false),
        ("e\u{301}e\u{301}", r"\A(?c)([é])\1\z", true),
    ] { assert_eq!(matches(source, pattern, ""), expected, "{source:?} / {pattern}"); }
    assert_eq!(replace("e\u{301}e\u{301}", r"(?c)([é])", "[$1]"), "[e\u{301}][e\u{301}]");
}

#[test]
fn class_intersection_and_complement_apply_case_folding_to_members() {
    for (source, pattern, expected) in [
        ("b", r"[a-z&&[^aeiou]]", true), ("a", r"[a-z&&[^aeiou]]", false),
        ("B", r"(?i)[a-z&&[^aeiou]]", true), ("A", r"(?i)[a-z&&[^aeiou]]", false),
        ("B", r"(?i)[\x61-\x7a]", true), ("A", r"(?i)[^\x61]", false),
        ("A", r"(?i)\p{Lower}", true), ("É", r"(?i)\p{Ll}", true),
        ("K", r"(?iu)\p{ASCII}", false), ("K", r"(?iu)[a-z]", true),
        ("İ", r"(?iu)[a-z]", true), ("I", r"(?iu)[ı]", true),
        ("A", r"(?i)[\Qa\E]", true),
    ] { assert_eq!(matches(source, pattern, ""), expected, "{source:?} / {pattern}"); }
}

#[test]
fn inline_flags_restore_at_group_boundaries_and_external_i_enables_unicode_case() {
    assert!(!matches("é", "(?i)É", ""));
    assert!(matches("é", "(?iu)É", ""));
    assert!(matches("é", "É", "i"));
    assert!(matches("ı", "I", "i"));
    assert!(!matches("é", "(?U)(?-U)(?i)É", ""));
    assert!(matches("Abc", "(?i:a)b(?i)c", ""));
    assert!(!matches("ABc", "(?i:a)bc", ""));
    assert!(matches("ab", "(?x)a # ignored\n b", ""));
    assert!(matches("a b", "a b", "x"));
}

#[test]
fn comments_mode_skips_spacing_inside_repeat_bounds_names_and_modifiers() {
    for (source, pattern) in [
        ("aa", "(?x)\\Aa{1 , 2}\\z"),
        ("aa", "(?x)\\Aa{1, # upper bound\n2}\\z"),
        ("aaaaaaaaaa", "(?x)\\Aa{1 0}\\z"),
        ("Aa", "(?x)(? i :a)a"),
        ("aa", "(?x)(?< n a m e >a)\\k < n a m e >"),
        ("aab", "(?x)(a)\\1 b"),
        ("ab", "(?x)(?< =a)b"),
        ("a", "(?x)(? :a)"), ("ab", "a(?)b"), ("a", "(?-:a)"),
        ("a", r"(?x)\x 6 1"), ("a", r"(?x)\u0 0 6 1"),
        ("a", "(?x)\\x{ 6 # digit\n1 }"),
        ("A", r"(?x)\0 1 0 1"), ("\u{1}", r"(?x)\c A"),
        ("b", r"(?x)[ a - z ]"), ("b", r"[\Qa\E-z]"),
        ("y", r"[\Qab\E-z]"),
        ("A", r"(?x)\p {Lu}"), ("A", r"(?x)\p{ Lu}"),
        ("A", r"(?x)\p L"), ("a", r"(?x)[^ B]"),
    ] { assert!(matches(source, pattern, ""), "{source:?} / {pattern}"); }
    assert!(scalar_call("like_regex", &[text("aa"), text("(?x)a{ 2}"), text("")], &EvalContext::default()).is_err());
    for pattern in [r"\u+061", r"\x{+61}", r"\x{110000}"] {
        assert!(scalar_call("like_regex", &[text("a"), text(pattern), text("")], &EvalContext::default()).is_err());
    }
}

#[test]
fn quoted_source_preserves_java_preprocessing_inside_other_syntax() {
    for (source, pattern) in [
        ("A", r"\A\p{\QLu\E}\z"),
        ("ª", r"\A\x\Qaa\E\z"),
        ("ꯍ", r"\A\u\Qabcd\E\z"),
        ("aa1", r"\A(a)\1\Q1\E\z"),
        ("a b#c\\d", r"\A(?x)\Qa b#c\d\E\z"),
        ("a\\", r"\Aa\Q\"),
        ("b", "(?x)#\\Qa\nb\\E"),
    ] { assert!(matches(source, pattern, ""), "{source:?} / {pattern}"); }
    for pattern in [r"\x\Q41\E", r"\u\Q0061\E"] {
        assert!(scalar_call("like_regex", &[text("a"), text(pattern), text("")], &EvalContext::default()).is_err(), "{pattern}");
    }
}

#[test]
fn comments_preserve_non_ascii_line_ends_and_class_syntax_positions() {
    for separator in ['\u{85}', '\u{2028}', '\u{2029}', '\0'] {
        let pattern = format!("(?x)\\A# comment{separator}a\\z");
        assert!(matches(&format!("{separator}a"), &pattern, ""));
        assert!(!matches("a", &pattern, ""));
        let pattern = format!("(?x)\\x6# comment{separator}1");
        assert!(scalar_call("like_regex", &[text("a"), text(&pattern), text("")], &EvalContext::default()).is_err());
    }
    for (source, pattern, expected) in [
        ("a", r"(?x)[ ^ B]", false), ("^", r"(?x)[ ^ B]", true),
        ("B", r"(?x)[ ^ B]", true), ("a", r"(?x)[^ B]", true),
        ("a", r"(?x)[a & & b]", false), ("&", r"(?x)[a & & b]", false),
        ("b", r"(?x)[ab & & bc]", true), ("a", r"(?x)[ab & & bc]", false),
        ("b", "(?x)[ab &# comment\n& bc]", true),
    ] { assert_eq!(matches(source, pattern, ""), expected, "{source:?} / {pattern}"); }
}

#[test]
fn vertical_escape_uses_the_legacy_control_value_at_range_boundaries() {
    for (source, pattern, expected) in [
        ("\t", r"[\t-\v]", true), ("\n", r"[\t-\v]", true),
        ("\x0b", r"[\t-\v]", true), ("\x0c", r"[\t-\v]", false),
        ("\x0b", r"[\v-\r]", true), ("\x0c", r"[\v-\r]", true),
        ("\r", r"[\v-\r]", true), ("\n", r"[\v-\r]", false),
        ("\u{85}", r"[\v-\r]", false), ("-", r"[\v-]", true),
        ("\n", r"[\v-]", false), ("\n", r"(?x)[\v -\r]", true),
    ] { assert_eq!(matches(source, pattern, ""), expected, "{source:?} / {pattern}"); }
}

#[test]
fn java_anchors_and_dot_treat_crlf_and_unicode_line_terminators_as_lines() {
    for separator in ["\n", "\r", "\r\n", "\u{85}", "\u{2028}", "\u{2029}"] {
        let source = format!("a{separator}b");
        assert!(matches(&source, "^b$", "m"));
        assert!(!matches(separator, ".", ""));
        assert!(matches(separator, ".", "s"));
        assert!(matches(&format!("a{separator}"), "a$", ""));
        assert!(matches(&format!("a{separator}"), r"a\Z", ""));
        assert!(!matches(&format!("a{separator}"), r"a\z", ""));
    }
    assert!(!matches("\r\n", r"\r^\n", "m"));
    assert!(!matches("", "^", "m"));
    assert!(matches("\r", "(?d).", ""));
    assert!(!matches("a\rb", "(?dm)^b", ""));
}

#[test]
fn java_named_properties_and_supported_block_properties() {
    for (source, pattern, expected) in [
        ("λ", r"\p{IsGreek}", true), ("λ", r"\p{InGreek}", true),
        ("A", r"\p{javaLetter}", true), ("$", r"\p{javaJavaIdentifierStart}", true),
        ("\u{a0}", r"\p{javaWhitespace}", false), ("\u{a0}", r"\p{javaSpaceChar}", true),
        ("\u{1f}", r"\p{javaWhitespace}", true), ("é", r"\p{InBasicLatin}", false),
        ("\t", r"(?U)\p{Print}", false), (" ", r"(?U)\p{Print}", true),
        ("\u{a0}", r"(?U)\p{Print}", true), ("\n", r"(?U)\p{Print}", false),
        ("\u{11380}", r"\p{InTULU_TIGALARI}", true),
        ("😀", r"\p{IsEmoji}", true),
        ("\u{2e2f}", r"\p{javaUnicodeIdentifierStart}", true),
        ("\u{378}", r"\p{IsUnknown}", true),
        ("A", r"\p{Cs}", false),
    ] { assert_eq!(matches(source, pattern, ""), expected, "{source:?} / {pattern}"); }
    for pattern in [r"\p{Alphabetic}", r"\p{White_Space}", r"\p{gc=Uppercase_Letter}", r"\p{InBasic-Latin}", r"\p{scx=Latin}"] {
        assert!(scalar_call("like_regex", &[text("a"), text(pattern), text("")], &EvalContext::default()).is_err(), "{pattern}");
    }
}

#[test]
fn java_character_names_use_control_names_and_assigned_block_hex_fallbacks() {
    for (source, pattern) in [
        ("a", r"\N{latin small letter a}"),
        ("\u{7}", r"\N{BEL}"), ("🔔", r"\N{BELL}"),
        ("가", r"\N{HANGUL SYLLABLES AC00}"),
        ("一", r"\N{CJK UNIFIED IDEOGRAPHS 4E00}"),
        ("a", r"[\N{LATIN SMALL LETTER A}-z]"),
        ("A", r"(?iu)\N{LATIN SMALL LETTER A}"),
    ] { assert!(matches(source, pattern, ""), "{source:?} / {pattern}"); }
    for pattern in [r"\N{LF}", r"\N{HANGUL SYLLABLE GA}", r"\N{BASIC LATIN 61}", r"\N{CJK UNIFIED IDEOGRAPH-4E00}", r"\N{unknown}", r"\N{BEL"] {
        assert!(scalar_call("like_regex", &[text("a"), text(pattern), text("")], &EvalContext::default()).is_err(), "{pattern}");
    }
}

#[test]
fn unicode_case_matching_preserves_literal_run_and_explicit_range_shapes() {
    for (source, pattern, expected) in [
        ("ẞ", r"\A(?iu)ß\z", false),
        ("ẞa", r"\A(?iu)ßa\z", true),
        ("ẞa", r"\A(?iu)ßa+\z", false),
        ("ẞab", r"\A(?iu)ßab+\z", true),
        ("ẞa", r"\A(?iu)\Qßa\E\z", true),
        ("ẞa", r"\A(?iu)\Qß\Ea\z", true),
        ("ẞa", r"\A(?iu)\u00dfa\z", true),
        ("ẞa", r"\A(?iu)\N{LATIN SMALL LETTER SHARP S}a\z", true),
        ("ẞ", r"\A(?iu)[ß]\z", false),
        ("ẞ", r"\A(?iu)[ß-ß]\z", true),
        ("ß", r"\A(?iu)[ẞ-ẞ]\z", false),
        ("ß", r"\A(?iu)[ẞ]\z", true),
        ("ẞa", "\\A(?iux)ß # ignored\na\\z", true),
    ] { assert_eq!(matches(source, pattern, ""), expected, "{source:?} / {pattern}"); }
}

#[test]
fn replacement_uses_java_group_number_name_escaping_and_empty_match_rules() {
    assert_eq!(replace("ab ab", r"(?<first>a)(b)", "${first}-$2"), "a-b a-b");
    assert_eq!(replace("a", "(a)", "$10"), "a0");
    assert_eq!(replace("b", "(a)?b", "$1!"), "!");
    assert_eq!(replace("aba", r"\A(?:(?=(a(b)?))ab?)+\z", "$1-$2"), "a-b");
    assert_eq!(replace("aba", r"\A(?:(?>(a(b)?)))+\z", "$1-$2"), "a-b");
    assert_eq!(replace("aab", r"\A(?:(?=(a(b)?))ab?)+ab\z", "$1-$2"), "a-");
    assert_eq!(replace("a", "(a)", r"\1\\$0"), "a\\a");
    assert_eq!(replace("a", "a*", "x"), "xx");
    assert_eq!(replace("ab", "", "x"), "xaxbx");
    assert_eq!(replace("x", "z", "${missing}"), "x");
    assert!(scalar_call("regexp_replace", &[text("x"), text("x"), text("${missing}")], &EvalContext::default()).is_err());
    assert!(scalar_call("regexp_replace", &[text("x"), text("x"), text("\\")], &EvalContext::default()).is_err());
}

#[test]
fn unicode_case_backreferences_accept_different_utf8_widths_without_dynamic_compilation() {
    assert!(matches("kK", r"(?iu)(k)\1", ""));
    assert!(matches("Kk", r"(?iu)(K)\1", ""));
    assert!(!matches("kx", r"(?iu)(k)\1", ""));
}

#[test]
fn backreference_case_modes_use_java_simple_upper_and_lower_mappings() {
    for (source, pattern, expected) in [
        ("aA", r"\A(?i)(a)\1\z", true),
        ("éÉ", r"\A(?i)(é)\1\z", false),
        ("éÉ", r"\A(?iu)(é)\1\z", true),
        ("iİ", r"\A(?iu)(i)\1\z", true),
        ("ıI", r"\A(?iu)(ı)\1\z", true),
        ("σς", r"\A(?iu)(σ)\1\z", true),
        ("ßẞ", r"\A(?iu)(ß)\1\z", true),
        ("ßSS", r"\A(?iu)(ß)\1\z", false),
        ("𐐀𐐨", r"\A(?iu)(𐐀)\1\z", true),
        ("aAa", r"\A(a)(?i:\1)\1\z", true),
        ("aAA", r"\A(a)(?i:\1)\1\z", false),
    ] { assert_eq!(matches(source, pattern, ""), expected, "{source:?} / {pattern}"); }
}

#[test]
fn forward_numeric_references_fail_until_a_capture_exists_and_retain_prior_iterations() {
    for (source, pattern, expected) in [
        ("a", r"\1(a)", false),
        ("a", r"\1?(a)", true),
        ("a", r"\9|a", true),
        ("a", r"\9a", false),
        ("", r"\9*", true),
        ("aaa", r"\A(?:\1?(a)){2}\z", true),
        ("aa0", r"\A(a)\10\z", true),
        ("a0", r"\A\10?(a)\z", false),
    ] { assert_eq!(matches(source, pattern, ""), expected, "{source:?} / {pattern}"); }
    assert!(scalar_call("like_regex", &[text("a"), text(r"(?<=\9)a"), text("")], &EvalContext::default()).is_err());
}

#[test]
fn malformed_java_syntax_does_not_enable_foreign_regex_extensions() {
    for pattern in [r"(?P<name>a)", r"(?<bad_name>a)", r"\K", r"\g<1>", r"(?R)", r"[z-a]", r"a{,2}", r"(?<x>a)(?<x>b)"] {
        assert!(scalar_call("like_regex", &[text("a"), text(pattern), text("")], &EvalContext::default()).is_err(), "{pattern}");
    }
}

#[test]
fn regex_limits_bound_patterns_nesting_and_retained_backtracking() {
    let cumulative = "(?:(?:(?:(?:(?:a{10000}){10000}){10000}){10000}){10000})";
    assert_eq!(scalar_call("like_regex", &[text("a"), text(cumulative), text("")], &EvalContext::default()).unwrap_err().code, "rtdb.expr/regex-limit");
    let oversized = "a".repeat(65_537);
    assert_eq!(scalar_call("like_regex", &[text("a"), text(&oversized), text("")], &EvalContext::default()).unwrap_err().code, "rtdb.expr/regex-limit");
    let nested = format!("{}a{}", "[".repeat(129), "]".repeat(129));
    assert_eq!(scalar_call("like_regex", &[text("a"), text(&nested), text("")], &EvalContext::default()).unwrap_err().code, "rtdb.expr/regex-limit");
    let source = "a".repeat(600);
    assert_eq!(scalar_call("like_regex", &[text(&source), text(r"\A(?:a(?=a))*a"), text("")], &EvalContext::default()).unwrap_err().code, "rtdb.expr/regex-limit");
}

#[test]
fn ordinary_java_patterns_keep_surrogate_atoms_and_lookbehind_intervals() {
    assert!(!matches("😀",r"\uDE00",""));
    assert!(matches("😀",r"\uD83D\uDE00",""));
    assert!(matches("😀b",r"(?<=\uDE00)b",""));
    assert!(matches("😀b",r"(?<=[\uD800-\uDFFF])b",""));
    assert_eq!(replace("😀b",r"(?<=(.))b","$1"),"😀?");
    assert_eq!(replace("😀b",r"(?<=(.))b","$1$1"),"😀??");
}

#[test]
fn ordinary_empty_replacement_advances_one_utf16_unit_and_recombines_output() {
    assert_eq!(replace("😀","","x"),"x?x?x");
    assert_eq!(replace("😀","",""),"😀");
    assert_eq!(replace("😀",r"(?=.)","x"),"x?x?");
    assert_eq!(replace("😀",r"\G","x"),"x😀");
}

#[test]
fn regex_compilation_and_replacement_output_use_the_callers_memory_scope() {
    let memory = MemoryBudget::new(32_768);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope), ..Default::default() };
    assert_eq!(scalar_call("like_regex", &[text("a"), text("a"), text("")], &context).unwrap_err().code, "rtdb/memory-budget-exceeded");
    drop(scope);
    assert_eq!(memory.used_bytes(), 0);

    let memory = MemoryBudget::new(64 * 1024 * 1024);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope), ..Default::default() };
    let value = scalar_call("regexp_replace", &[text(&"a".repeat(1000)), text("a"), text(&"b".repeat(1000))], &context).unwrap();
    assert_eq!(value, text(&"b".repeat(1_000_000)));
    assert!(memory.used_bytes() >= 1_000_000);
    drop(value); drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_node_and_utf16_replacement_reservations_release_on_success_and_error() {
    for (source, pattern, expected) in [
        ("😀", r"\b{g}|", Ok("x?x?x")),
        ("😀", "", Ok("x?x?x")),
        ("😀b", r"(?<=(.))b", Ok("😀x")),
        ("ab", r"\A(?:(?=ab)c|a\b{g}b)\z", Err("xtdb.error/unknown")),
    ] {
        let memory = MemoryBudget::new(256 * 1024 * 1024);
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope), ..Default::default() };
        let result = scalar_call("regexp_replace", &[text(source), text(pattern), text("x")], &context);
        match expected {
            Ok(expected) => assert_eq!(result.as_ref().unwrap(), &text(expected)),
            Err(code) => assert_eq!(result.as_ref().unwrap_err().code, code),
        }
        assert!(memory.used_bytes() > 0);
        drop(result); drop(scope);
        assert_eq!(memory.used_bytes(), 0);
    }
}
