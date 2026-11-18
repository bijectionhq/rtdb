use fancy_regex::{Error,Regex,RegexBuilder,RuntimeError};

fn matcher(pattern: &str) -> Regex { RegexBuilder::new(pattern).java_utf16(true).build().unwrap() }

#[test]
fn explicit_java_utf16_selects_node_execution_without_marker_atoms() {
    for pattern in ["", "a", ".", "[a-z]", "(a|b)+", "(?<=a)b"] {
        assert!(matcher(pattern).uses_java_nodes(),"{pattern}");
    }
    assert!(!Regex::new("a").unwrap().uses_java_nodes());
    assert!(!RegexBuilder::new("a").java_utf16(true).java_matching(false).build().unwrap().uses_java_nodes());
    assert!(!RegexBuilder::new("a").java_utf16(true).java_utf16(false).build().unwrap().uses_java_nodes());
}

#[test]
fn ordinary_lookbehind_retains_utf16_captures_and_borrowed_capture_boundary() {
    let regex = matcher(r"(?<=(.))b");
    let units: Vec<u16> = "😀b".encode_utf16().collect();
    assert_eq!(regex.captures_utf16(&units,0,0).unwrap().unwrap(),[2,3,1,2]);
    assert!(regex.is_match("😀b").unwrap());
    assert_eq!(regex.find("😀b").unwrap().unwrap().range(),4..5);
    assert!(matches!(regex.captures("😀b"),Err(Error::RuntimeError(RuntimeError::NonUtf8Capture))));
    assert_eq!(matcher(r"(?<=([\x{d800}-\x{dfff}]))b").captures_utf16(&units,0,0).unwrap().unwrap(),[2,3,1,2]);
}

#[test]
fn empty_patterns_and_surrogate_atoms_accept_every_requested_code_unit_position() {
    let units = [0xd83d,0xde00];
    let empty = matcher("");
    for position in 0..=units.len() { assert_eq!(empty.captures_utf16(&units,position,0).unwrap().unwrap(),[position,position]); }
    assert_eq!(matcher(r"[\x{de00}]").captures_utf16(&units,1,0).unwrap().unwrap(),[1,2]);
    assert_eq!(matcher(r"[\x{d800}-\x{dfff}]").captures_utf16(&[0xd800],0,0).unwrap().unwrap(),[0,1]);
}

#[test]
fn ordinary_node_programs_keep_execution_limits() {
    let regex = RegexBuilder::new("(a|aa)*b").java_utf16(true).backtrack_limit(1).build().unwrap();
    assert!(matches!(regex.is_match("aaaac"),Err(Error::RuntimeError(RuntimeError::BacktrackLimitExceeded))));
}

#[test]
fn whole_region_matches_backtrack_at_the_root_end_without_anchoring_inner_acceptance() {
    let ab = [0x61, 0x62];
    assert_eq!(matcher("a|ab").captures_utf16(&ab, 0, 0).unwrap().unwrap(), [0, 1]);
    assert!(matcher("a|ab").matches_utf16(&ab).unwrap());
    assert!(matcher("(?=a)(?>a)b").matches_utf16(&ab).unwrap());
    assert!(!matcher("(?>a|ab)").matches_utf16(&ab).unwrap());
    assert!(!matcher("b").matches_utf16(&ab).unwrap());
    assert!(!matcher("a$").matches_utf16(&[0x61, 0x0a]).unwrap());
    assert!(matcher(r"\Gab").matches_utf16(&ab).unwrap());
    assert!(matcher("").matches_utf16(&[]).unwrap());
    assert!(!matcher("").matches_utf16(&[0xd800]).unwrap());
    assert!(matcher(r"[\x{d800}]").matches_utf16(&[0xd800]).unwrap());
    assert!(Regex::new("a").unwrap().matches_utf16(&ab).is_err());
}
