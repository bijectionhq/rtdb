use rtdb_xtql::{EdnKind, read};

#[test]
fn character_literals_can_contain_delimiters_and_octal_escapes() {
    let source = r#"[\] \; \, \( \\ \" \o141 \u００６１]"#;
    let EdnKind::Vector(forms) = read(source).unwrap().kind else { panic!("Expected vector") };
    let expected = [(']', r"\]"), (';', r"\;"), (',', r"\,"), ('(', r"\("), ('\\', r"\\"), ('"', "\\\""), ('a', r"\o141"), ('a', r"\u００６１")];
    assert_eq!(forms.len(), expected.len());
    for (form, (character, spelling)) in forms.iter().zip(expected) {
        assert_eq!(form.kind, EdnKind::Character(character));
        assert_eq!(&source[form.span.start..form.span.end], spelling);
    }
    assert_eq!(read("\\ ").unwrap().kind, EdnKind::Character(' '));
    assert_eq!(read(r"\o7").unwrap().kind, EdnKind::Character('\u{7}'));
}

#[test]
fn strings_read_octal_escapes_and_keep_the_next_character() {
    for (source, expected) in [
        (r#""\141b\0\377""#, "ab\0ÿ"),
        (r#""\1 \2""#, "\u{1} \u{2}"),
        (r#""\u００６１""#, "a"),
    ] {
        assert_eq!(read(source).unwrap().kind, EdnKind::String(expected.into()));
    }
    for source in [r"\o400", r"\o1234", r"\o8", r"\uD800", r#""\400""#, r#""\1a""#] {
        assert_eq!(read(source).unwrap_err().code, "xtql/edn-reader", "{source}");
    }
}

#[test]
fn carriage_return_ends_a_comment_and_dispatch_metadata_is_inert() {
    let value = read("[; ignored\r#^:private abc ^:private def]").unwrap();
    let EdnKind::Vector(forms) = value.kind else { panic!("Expected vector") };
    assert_eq!(forms.len(), 2);
    assert_eq!(forms[0].kind, EdnKind::Symbol("abc".into()));
    assert_eq!(forms[1].kind, EdnKind::Symbol("def".into()));
}

#[test]
fn code_reader_forms_are_rejected_before_the_consumer_sees_them() {
    for source in [r#"#"a""#, "#?(:clj 1)", "[#?@(:clj [1])]", "#=(str 1)", "::alias/key", "#<Object>"] {
        assert_eq!(read(source).unwrap_err().code, "xtql/edn-reader", "{source}");
    }
}
