use fancy_regex::{Regex, RegexBuilder};

#[test]
fn omitted_delegated_captures_retain_the_last_successful_iteration() {
    for pattern in [r"\A(?:(?=(a(b)?))ab?)+\z", r"\A(?:(?>(a(b)?)))+\z", r"\A(a(b)?)+\z"] {
        let regex = RegexBuilder::new(pattern).java_matching(true).build().unwrap();
        let captures = regex.captures("aba").unwrap().unwrap();
        assert_eq!(captures.get(1).unwrap().as_str(), "a", "{pattern}");
        assert_eq!(captures.get(2).unwrap().as_str(), "b", "{pattern}");
        assert!(regex.captures("a").unwrap().unwrap().get(2).is_none(), "{pattern}");
    }
    let regex = RegexBuilder::new(r"\A(?:(?=(a(b)?))ab?)+ab\z").java_matching(true).build().unwrap();
    assert!(regex.captures("aab").unwrap().unwrap().get(2).is_none());
    let regex = Regex::new(r"\A(?:(?=(a(b)?))ab?)+\z").unwrap();
    assert!(regex.captures("aba").unwrap().unwrap().get(2).is_none());
}

#[test]
fn graphemes_consume_complete_clusters_with_java_unicode16_categories() {
    let one = RegexBuilder::new(r"\A\X\z").java_matching(true).build().unwrap();
    for text in ["a\u{301}", "\r\n", "\u{1100}\u{1161}\u{11a8}", "🇫🇷", "👩🏽\u{200d}💻", "\u{915}\u{94d}\u{915}", "\u{915}\u{94d}\u{915}\u{94d}\u{915}", "\u{378}\u{301}", "😀\u{903}\u{200d}😀"] {
        assert!(one.is_match(text).unwrap(), "{text:?}");
    }
    for text in ["", "ab", "\ra", "🇫🇷🇫", "\u{379}\u{301}", "\u{600}😀\u{200d}😀"] {
        assert!(!one.is_match(text).unwrap(), "{text:?}");
    }
    assert!(!Regex::new(r"\A\X\p{Mn}\z").unwrap().is_match("a\u{301}").unwrap());
    assert!(Regex::new(r"\Aa\X\z").unwrap().is_match("a\u{301}").unwrap());
    assert!(!RegexBuilder::new(r"(?<=\X)a").java_matching(true).build().unwrap().is_match("ba").unwrap());
    assert_eq!(Regex::new(r"\X").unwrap().find_iter("a\u{301}🇫🇷b").map(|value| value.unwrap().as_str()).collect::<Vec<_>>(), ["a\u{301}", "🇫🇷", "b"]);
}

#[test]
fn canonical_class_atoms_try_composed_prefixes_longest_first_without_single_fallback() {
    for (pattern, source, expected) in [
        (r"\A(?n:[é])\z", "e\u{301}", true),
        (r"\A(?n:[e])", "e\u{301}", false),
        (r"\A(?n:[ÅǺ])\x{301}\z", "A\u{30a}\u{301}", true),
        (r"\A(?n:[각])\z", "\u{1100}\u{1161}\u{11a8}", true),
        (r"\A(?n:[\x{340}])\z", "\u{340}", true),
        (r"\A(?n:[\x{340}])\z", "\u{300}", false),
        (r"\A(?n:[à])\x{315}\z", "a\u{300}\u{315}", true),
        (r"\A(?n:[à])\x{315}\z", "a\u{315}\u{300}", false),
    ] {
        assert_eq!(RegexBuilder::new(pattern).java_matching(true).build().unwrap().is_match(source).unwrap(), expected, "{source:?} / {pattern}");
    }
}

#[test]
fn cumulative_nested_repeat_width_returns_a_compile_error() {
    let error = RegexBuilder::new("(?:(?:(?:(?:(?:a{10000}){10000}){10000}){10000}){10000})").java_matching(true).build().unwrap_err();
    assert!(matches!(error, fancy_regex::Error::CompileError(fancy_regex::CompileError::PatternSizeOverflow)));
}

#[test]
fn previous_match_end_is_independent_of_search_start_and_lookbehind_position() {
    let regex = Regex::new(r"\G|b").unwrap();
    assert_eq!(regex.captures_from_pos_with_previous_end("ab", 1, 0).unwrap().unwrap().get(0).unwrap().range(), 1..2);
    assert!(Regex::new(r"\G").unwrap().captures_from_pos_with_previous_end("ab", 1, 0).unwrap().is_none());
    let regex = Regex::new(r"(?<=\G.)b").unwrap();
    assert_eq!(regex.captures_from_pos_with_previous_end("ab", 1, 0).unwrap().unwrap().get(0).unwrap().range(), 1..2);
}

#[test]
fn variable_lookbehind_checks_end_before_committing_the_nearest_capture() {
    let regex = RegexBuilder::new(r"(?<=(ab|b))c").java_matching(true).build().unwrap();
    let captures = regex.captures("abc").unwrap().unwrap();
    assert_eq!(captures.get(1).unwrap().as_str(), "b");
    let regex = RegexBuilder::new(r"(?<=(a+))a").java_matching(true).build().unwrap();
    let captures = regex.captures("aa").unwrap().unwrap();
    assert_eq!(captures.get(0).unwrap().range(), 1..2);
    assert_eq!(captures.get(1).unwrap().as_str(), "a");
}

#[test]
fn lookbehind_candidate_attempts_obey_backtrack_limits() {
    let regex = RegexBuilder::new(r"\Ax{20}(?<=ax*)").java_matching(true).backtrack_limit(10).build().unwrap();
    assert!(matches!(regex.is_match(&"x".repeat(20)), Err(fancy_regex::Error::RuntimeError(fancy_regex::RuntimeError::BacktrackLimitExceeded))));
}

#[test]
fn successful_java_positive_lookaround_does_not_retry_after_outer_failure() {
    let regex = RegexBuilder::new(r"\A(?=(a|aa))\1b\z").java_matching(true).build().unwrap();
    assert!(!regex.is_match("aab").unwrap());
    let regex = RegexBuilder::new(r"\Aaaa(?<=(a|aa))\1b\z").java_matching(true).build().unwrap();
    assert!(regex.is_match("aaaab").unwrap());
    assert!(!regex.is_match("aaaaab").unwrap());
}

#[test]
fn ascii_and_unicode_backreferences_use_distinct_case_tables() {
    for (pattern, source, expected) in [
        (r"(a)(?i-u:\1)", "aA", true),
        (r"(é)(?i-u:\1)", "éÉ", false),
        (r"(i)(?i:\1)", "iİ", true),
        (r"(ß)(?i:\1)", "ßẞ", true),
        (r"(ß)(?i:\1)", "ßSS", false),
    ] {
        assert_eq!(RegexBuilder::new(pattern).java_matching(true).build().unwrap().is_match(source).unwrap(), expected);
    }
}

#[test]
fn missing_groups_fail_dynamically_without_aliasing_internal_save_slots() {
    for (pattern, source, expected) in [(r"\9|a", "a", true), (r"\9*", "", true), (r"\1(a)", "a", false), (r"\A(?:\1?(a)){2}\z", "aaa", true)] {
        assert_eq!(RegexBuilder::new(pattern).java_matching(true).build().unwrap().is_match(source).unwrap(), expected);
    }
    assert!(RegexBuilder::new(r"(?<=\9)").java_matching(true).build().is_err());
    assert!(Regex::new(r"\9").is_err());
    assert!(RegexBuilder::new(r"\99999999").java_matching(true).build().is_err());
}
