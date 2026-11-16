use rtdb_xtql::{EdnKind, read};

#[test]
fn namespaced_maps_qualify_only_immediate_unqualified_keyword_and_symbol_keys() {
    let source = "#:person{:name name age 42 :other/id 7 :_/plain 8 _/symbol 9 \"text\" {:nested 10}}";
    let value = read(source).unwrap();
    assert_eq!(value.span.start, 0);
    assert_eq!(value.span.end, source.len());
    let EdnKind::Map(pairs) = value.kind else { panic!("expected map") };
    assert_eq!(pairs[0].0.kind, EdnKind::Keyword("person/name".into()));
    assert_eq!(pairs[0].1.kind, EdnKind::Symbol("name".into()));
    assert_eq!(&source[pairs[0].0.span.start..pairs[0].0.span.end], ":name");
    assert_eq!(pairs[1].0.kind, EdnKind::Symbol("person/age".into()));
    assert_eq!(pairs[2].0.kind, EdnKind::Keyword("other/id".into()));
    assert_eq!(pairs[3].0.kind, EdnKind::Keyword("plain".into()));
    assert_eq!(pairs[4].0.kind, EdnKind::Symbol("symbol".into()));
    assert_eq!(pairs[5].0.kind, EdnKind::String("text".into()));
    let EdnKind::Map(nested) = &pairs[5].1.kind else { panic!("expected nested map") };
    assert_eq!(nested[0].0.kind, EdnKind::Keyword("nested".into()));
}

#[test]
fn namespaced_maps_detect_collisions_after_key_transformation() {
    for source in ["#:person{:name 1 :person/name 2}", "#:person{name 1 person/name 2}",
                   "#:person{:_/name 1 :name 2 :person/name 3}", "#:person{:name}"] {
        assert!(read(source).is_err(), "{source}");
    }
    let EdnKind::Map(pairs) = read("#:person{:name 1 name 2}").unwrap().kind else { panic!("expected map") };
    assert_eq!(pairs.len(), 2);
}

#[test]
fn namespace_removal_uses_the_complete_namespace_and_preserves_slash_names() {
    let EdnKind::Map(pairs) = read("#:person{:_/a/name 1 :/ 2 / 3}").unwrap().kind else { panic!("expected map") };
    assert_eq!(pairs[0].0.kind, EdnKind::Keyword("_/a/name".into()));
    assert_eq!(pairs[1].0.kind, EdnKind::Keyword("person//".into()));
    assert_eq!(pairs[2].0.kind, EdnKind::Symbol("person//".into()));
}

#[test]
fn namespace_prefix_requires_an_unqualified_symbol_and_immediate_map() {
    for source in ["#::{:a 1}", "#::alias{:a 1}", "#:foo/bar{:a 1}", "#::foo {:a 1}",
                   "#:123{:a 1}", "#:nil{:a 1}", "#:foo [:a 1]", "#:foo ;comment\n{:a 1}"] {
        assert!(read(source).is_err(), "{source}");
    }
    for source in ["#: person {:name 1}", "#:person, {:name 1}", "#:person{}"] {
        assert!(matches!(read(source).unwrap().kind, EdnKind::Map(_)), "{source}");
    }
}
