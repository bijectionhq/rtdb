use super::*;
use apache_avro::schema::Name;

#[test]
fn borrowed_reference_lookup_preserves_vendor_fullname_rules() {
    let cases = [
        ("R", None, "R"),
        ("R", Some(""), "R"),
        ("R", Some("n"), "n.R"),
        ("R", Some("n.inner"), "n.inner.R"),
        ("n.R", Some("ignored"), "n.R"),
        (".R", Some("ignored"), ".R"),
        ("R", Some("é namespace"), "é namespace.R"),
        ("", Some("n"), "n."),
    ];
    let mut names = BTreeMap::new();
    for (_, _, fullname) in cases { names.insert(fullname.into(), Schema::Long); }
    names.insert("ignored.n.R".into(), Schema::Int);
    names.insert("ignored..R".into(), Schema::Int);
    for (simple, namespace, fullname) in cases {
        let name = Name { name: simple.into(), namespace: namespace.map(str::to_owned) };
        assert_eq!(name.fullname(None), fullname);
        let reference = Schema::Ref { name };
        assert!(std::ptr::eq(resolve(&reference, &names).unwrap(), &names[fullname]));
    }
}

#[test]
fn long_reference_names_select_the_existing_schema_owner() {
    let namespace = "n".repeat(65_536);
    let reference = Schema::Ref { name: Name { name: "R".into(), namespace: Some(namespace.clone()) } };
    let names = BTreeMap::from([(format!("{namespace}.R"), Schema::Long)]);
    let target = names.values().next().unwrap();
    assert!(std::ptr::eq(resolve(&reference, &names).unwrap(), target));
    let scalar = Schema::String;
    assert!(std::ptr::eq(resolve(&scalar, &names).unwrap(), &scalar));
}

#[test]
fn missing_references_keep_the_existing_fullname_diagnostic() {
    let reference = Schema::Ref { name: Name { name: "Missing".into(), namespace: Some("n".into()) } };
    let error = resolve(&reference, &BTreeMap::new()).unwrap_err();
    assert_eq!(error.code, "xtdb.kafka-connect-source/avro-schema");
    assert_eq!(error.message.as_deref(), Some("Unknown schema reference 'n.Missing'"));
}
