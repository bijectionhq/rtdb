use fancy_regex::{Error, RegexBuilder, RuntimeError};

fn matcher(pattern: &str) -> fancy_regex::Regex {
    RegexBuilder::new(pattern).java_matching(true).build().unwrap()
}

#[test]
fn boundary_uses_the_next_cluster_from_last_acceptance() {
    for (pattern, input, expected) in [
        (r"\Aa\b{g}\x{301}b\z", "a\u{301}b", false),
        (r"\Aa\x{301}b\b{g}\x{301}\z", "a\u{301}b\u{301}", true),
        (r"\b{g}", "", true),
        (r"\Aa+\b{g}b\z", "ab", true),
        (r"\A(a)+\b{g}b\z", "ab", false),
        (r"\A(?:a)+\b{g}b\z", "ab", false),
        (r"\A(a|a)+\b{g}b\z", "ab", true),
        (r"\Aa{1}\b{g}b\z", "ab", false),
        (r"\Aa?\b{g}b\z", "ab", false),
        (r"\A(a)?\b{g}b\z", "ab", true),
        (r"\Aa++\b{g}b\z", "ab", false),
        (r"\Aa+?\b{g}b\z", "ab", false),
        (r"\A(?=a)a\b{g}b\z", "ab", false),
        (r"\Aa(?<=a)\b{g}b\z", "ab", true),
        (r"\A(?>a)\b{g}b\z", "ab", false),
        (r"\A(?:(?!a)c|a\b{g}b)\z", "ab", false),
        (r"\A\R\b{g}a\z", "\na", true),
        (r"\A\R\b{g}\n\z", "\r\n", false),
    ] {
        assert_eq!(matcher(pattern).is_match(input).unwrap(), expected, "{pattern} / {input:?}");
    }
}

#[test]
fn line_ending_backtracks_from_crlf_only_when_the_atom_continuation_fails() {
    assert!(matcher(r"\A\R\n\z").is_match("\r\n").unwrap());
    assert!(!matcher(r"\A\R{1}\n\b{g}\z").is_match("\r\n").unwrap());
}

#[test]
fn native_end_anchor_keeps_zero_width_and_trailing_newline_semantics_in_java_nodes() {
    let regex = matcher(r"(a\R)\Z");
    for input in ["a\n", "a\n\n"] {
        let captures = regex.captures(input).unwrap().unwrap();
        assert_eq!(captures.get(0).unwrap().range(), 0..2);
        assert_eq!(captures.get(1).unwrap().range(), 0..2);
    }
    assert!(!regex.is_match("a\nx").unwrap());
    assert_eq!(matcher(r"a\b{g}\Z").find("a").unwrap().unwrap().range(), 0..1);
    assert_eq!(matcher(r"a\b{g}\Z").find("a\n\n").unwrap().unwrap().range(), 0..1);
}

#[test]
fn native_class_operators_and_invalid_ranges_keep_their_compiler_semantics_in_java_nodes() {
    let subtraction = matcher(r"[a-z--aeiou]\R");
    assert!(subtraction.is_match("b\n").unwrap());
    assert!(!subtraction.is_match("a\n").unwrap());
    let difference = matcher(r"[a-c~~b-d]\b{g}\R");
    assert!(difference.is_match("a\n").unwrap());
    assert!(difference.is_match("d\n").unwrap());
    assert!(!difference.is_match("b\n").unwrap());
    let pipes = matcher(r"[a||b]\R");
    for input in ["a\n", "|\n", "b\n"] {
        assert!(pipes.is_match(input).unwrap());
    }
    assert!(!pipes.is_match("c\n").unwrap());
    for pattern in [r"[z-a]\R", r"[z-a]\b{g}"] {
        assert!(RegexBuilder::new(pattern).java_matching(true).build().is_err());
    }
}

#[test]
fn native_class_codepoints_keep_unicode_limits_and_java_surrogate_ranges() {
    for pattern in [r"[\x{110000}]\R", r"[\x{0}-\x{110000}]\b{g}", r"[\U00110000]\R"] {
        assert!(RegexBuilder::new(pattern).java_matching(true).build().is_err());
    }
    assert!(matcher(r"[\x{10ffff}]\R").is_match("\u{10ffff}\n").unwrap());
    assert!(RegexBuilder::new(r"[\x{d800}-\x{dfff}]\R").java_matching(true).build().is_ok());
}

#[test]
fn failed_branches_retain_acceptance_and_can_leave_an_invalid_boundary_start() {
    assert!(!matcher(r"\A(?:a{1}|a)+\b{g}b\z").is_match("ab").unwrap());
    assert!(!matcher(r"\A(?:(?=ab)c|a\b{g}b)c\z").is_match("abc").unwrap());
    assert!(matches!(matcher(r"\A(?:(?=ab)c|a\b{g}b)\z").is_match("ab"), Err(Error::RuntimeError(RuntimeError::GraphemeBoundaryStart))));
}

#[test]
fn deterministic_group_reinvocation_uses_utf16_widths() {
    assert!(!matcher(r"\A(?:.\b{g})*😀\b{g}b\z").is_match("a😀b").unwrap());
    assert!(matches!(matcher(r"\A(?:.\b{g})*é\b{g}b\z").is_match("aéb"), Err(Error::RuntimeError(RuntimeError::GraphemeBoundaryStart))));
}

#[test]
fn failed_top_loop_positions_survive_search_retries_and_backreferences_disable_them() {
    let pattern = r"(?:(?=.)a|aa)*\b{g}b|(?=a)c";
    let captures = matcher(pattern).captures("aaabx").unwrap().unwrap();
    assert_eq!(captures.get(0).unwrap().range(), 1..4);
    assert!(matches!(matcher(r"(?:(?=.)a|aa)*\b{g}b|(?=a)c|()\1z").is_match("aaabx"), Err(Error::RuntimeError(RuntimeError::GraphemeBoundaryStart))));
}

#[test]
fn utf16_capture_api_retains_surrogate_intervals_without_fabricating_utf8_slices() {
    let regex = matcher(r"(?<=(.))\b{g}b");
    let units: Vec<u16> = "😀b".encode_utf16().collect();
    let captures = regex.captures_utf16(&units, 0, 0).unwrap().unwrap();
    assert_eq!(captures, [2, 3, 1, 2]);
    let range = matcher(r"(?<=([\x{d800}-\x{dfff}]))\b{g}b");
    assert_eq!(range.captures_utf16(&units, 0, 0).unwrap().unwrap(), [2, 3, 1, 2]);
    let reference = matcher(r"(?<=(.))(?i:\1)\b{g}");
    assert_eq!(reference.captures_utf16(&[0xd83d, 0xde00, 0xde00], 0, 0).unwrap().unwrap(), [2, 3, 1, 2]);
    assert!(regex.is_match("😀b").unwrap());
    assert_eq!(regex.find("😀b").unwrap().unwrap().range(), 4..5);
    assert!(matches!(regex.captures("😀b"), Err(Error::RuntimeError(RuntimeError::NonUtf8Capture))));
}

#[test]
fn java_node_recursion_and_attempts_obey_configured_limits() {
    let regex = RegexBuilder::new(r"(?:a|aa)+\b{g}z").java_matching(true).backtrack_stack_limit(3).build().unwrap();
    assert!(matches!(regex.is_match("aaaa"), Err(Error::RuntimeError(RuntimeError::StackOverflow))));
    let regex = RegexBuilder::new(r"(?:a|aa)+\b{g}z").java_matching(true).backtrack_stack_limit(usize::MAX).build().unwrap();
    assert!(matches!(regex.is_match(&"a".repeat(600)), Err(Error::RuntimeError(RuntimeError::StackOverflow))));
    let regex = RegexBuilder::new(r"a\b{g}z").java_matching(true).backtrack_limit(2).build().unwrap();
    assert!(matches!(regex.is_match("aaaa"), Err(Error::RuntimeError(RuntimeError::BacktrackLimitExceeded))));
}

#[test]
fn borrowed_iterators_retain_the_preceding_java_acceptance_position() {
    let regex = matcher(r"a|\b{g}b");
    let matches = regex.find_iter("abx").map(|matched| matched.unwrap().range()).collect::<Vec<_>>();
    assert_eq!(matches, [0..1]);
    let matches = regex.captures_iter("abx").map(|captures| captures.unwrap().get(0).unwrap().range()).collect::<Vec<_>>();
    assert_eq!(matches, [0..1]);
}
