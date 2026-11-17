use super::*;
use std::cell::{Cell, RefCell};

struct OpenNames { fallback: bool }

impl SchemaNamePolicy for OpenNames {
    fn name(&self, text: &str, namespace: &Namespace) -> AvroResult<Name> {
        let (namespace, name) = match text.rsplit_once('.') {
            Some((namespace, name)) => ((!namespace.is_empty()).then(|| namespace.to_owned()), name),
            None => (namespace.clone(), text),
        };
        if name.is_empty() || name == "reject" {
            return Err(Details::InvalidSchemaName(text.into(), "scoped test policy").into());
        }
        Ok(Name { name: name.into(), namespace })
    }

    fn record_field(&self, name: &str) -> AvroResult<()> {
        if name == "reject" { Err(Details::FieldName(name.into()).into()) } else { Ok(()) }
    }

    fn enum_symbol(&self, symbol: &str) -> AvroResult<()> {
        if symbol == "reject" { Err(Details::EnumSymbolName(symbol.into()).into()) } else { Ok(()) }
    }

    fn reference_fallback(&self, name: &str, _namespace: &Namespace) -> AvroResult<Option<Name>> {
        if self.fallback { self.name(name, &None).map(Some) } else { Ok(None) }
    }
}

fn parse(text: &str) -> Schema {
    Schema::parse_str_with_list_with_name_policy(text, Vec::<&str>::new(), &OpenNames { fallback: true }).unwrap().0
}

#[test]
fn scoped_names_fields_symbols_aliases_and_unions_ignore_initialized_global_validators() {
    Schema::parse_str(r#"{"type":"record","name":"GlobalBaseline","fields":[]}"#).unwrap();
    assert!(Name::new("Événement").is_err());
    let schema = parse(r#"{"type":"record","name":"Événement","aliases":["ÉvénementAncien"],"fields":[
        {"name":"donnée","aliases":["field alias with spaces"],"type":{"type":"enum","name":"État","symbols":["Été","Hiver"],"aliases":["ÉtatAncien"]}},
        {"name":"clé","type":{"type":"fixed","name":"Clé","aliases":["AncienneClé"],"size":2}},
        {"name":"suivant","type":["null","Événement"],"default":null}] }"#);
    let Schema::Record(record) = &schema else { panic!("expected record") };
    assert_eq!(record.name.fullname(None), "Événement");
    assert_eq!(record.aliases.as_ref().unwrap()[0].fullname(None), "ÉvénementAncien");
    assert_eq!(record.fields[0].name, "donnée");
    assert_eq!(record.fields[0].aliases.as_ref().unwrap(), &["field alias with spaces"]);
    let Schema::Enum(enumeration) = &record.fields[0].schema else { panic!("expected enum") };
    assert_eq!(enumeration.symbols, ["Été", "Hiver"]);
    assert_eq!(enumeration.aliases.as_ref().unwrap()[0].fullname(None), "ÉtatAncien");
    let Schema::Fixed(fixed) = &record.fields[1].schema else { panic!("expected fixed") };
    assert_eq!(fixed.name.name, "Clé");
    assert_eq!(fixed.aliases.as_ref().unwrap()[0].fullname(None), "AncienneClé");
    let Schema::Union(union) = &record.fields[2].schema else { panic!("expected union") };
    let Schema::Ref { name } = &union.variants()[1] else { panic!("expected reference") };
    assert_eq!(name, &record.name);
    ResolvedSchema::try_from(&schema).unwrap();
    assert!(Name::new("Événement").is_err());
    assert!(Schema::parse_str(r#"{"type":"record","name":"Ordinary","fields":[{"name":"é","type":"int"}]}"#).is_err());
}

#[test]
fn explicit_global_names_keep_hash_lookup_qualification_and_serialized_empty_namespace() {
    let schema = parse(r#"{"type":"record","name":"Root","namespace":"local","fields":[
        {"name":"definition","type":{"type":"record","name":".Global","aliases":[".OldGlobal"],"fields":[{"name":"n","type":"int"}]}},
        {"name":"reference","type":".Global","default":{"n":1}},
        {"name":"empty","type":{"type":"fixed","name":"Other","namespace":"","size":1}}]}"#);
    let Schema::Record(root) = &schema else { panic!("expected record") };
    let Schema::Record(global) = &root.fields[0].schema else { panic!("expected global definition") };
    assert_eq!(global.name.namespace, Some(String::new()));
    assert_eq!(global.name.fullname(Some("different".into())), "Global");
    assert_eq!(global.name.fully_qualified_name(&Some("different".into())), global.name);
    let Schema::Ref { name } = &root.fields[1].schema else { panic!("expected global reference") };
    assert_eq!(name, &global.name);
    let names = HashMap::from([(global.name.clone(), 7)]);
    assert_eq!(names.get(&name.fully_qualified_name(&Some("local".into()))), Some(&7));
    assert_eq!(global.aliases.as_ref().unwrap()[0].namespace(), Some(String::new()));
    let Schema::Fixed(other) = &root.fields[2].schema else { panic!("expected fixed") };
    assert_eq!(other.name.namespace, Some(String::new()));
    ResolvedSchema::try_from(&schema).unwrap();
    let serialized = serde_json::to_value(&schema).unwrap();
    assert_eq!(serialized["fields"][0]["type"]["namespace"], "");
    assert_eq!(serialized["fields"][2]["type"]["namespace"], "");
    assert_eq!(parse(&serialized.to_string()), schema);
}

#[test]
fn reference_candidates_prefer_local_definitions_then_policy_global_fallback() {
    let references = [r#"{"type":"fixed","name":"Choice","size":1}"#,
        r#"{"type":"fixed","name":"local.Choice","size":2}"#,
        r#"{"type":"fixed","name":"OnlyGlobal","size":3}"#];
    let root = r#"{"type":"record","name":"local.Root","fields":[{"name":"local","type":"Choice"},{"name":"global","type":"OnlyGlobal"}]}"#;
    let (schema, parsed) = Schema::parse_str_with_list_with_name_policy(root, references, &OpenNames { fallback: true }).unwrap();
    assert_eq!(parsed.len(), 3);
    let Schema::Record(record) = schema else { panic!("expected record") };
    let Schema::Ref { name: local } = &record.fields[0].schema else { panic!("expected local reference") };
    let Schema::Ref { name: global } = &record.fields[1].schema else { panic!("expected global reference") };
    assert_eq!(local.fullname(None), "local.Choice");
    assert_eq!(global.fullname(Some("local".into())), "OnlyGlobal");
    assert!(Schema::parse_str_with_list_with_name_policy(root, references, &OpenNames { fallback: false }).is_err());
}

#[test]
fn scoped_external_cyclic_references_and_aliases_keep_one_absolute_key_space() {
    let references = [r#"{"type":"record","name":"É","aliases":["AliasÉ"],"fields":[{"name":"next","type":["null","Ö"]}]}"#,
        r#"{"type":"record","name":"Ö","fields":[{"name":"next","type":["null","É"]}]}"#];
    let (schema, parsed) = Schema::parse_str_with_list_with_name_policy("\"AliasÉ\"", references, &OpenNames { fallback: true }).unwrap();
    assert_eq!(parsed.len(), 2);
    let Schema::Ref { name } = schema else { panic!("expected alias reference") };
    assert_eq!(name.fullname(None), "É");
    let schema = Schema::Ref { name };
    let borrowed = parsed.iter().map(|schema| (schema.name().unwrap().clone(), schema)).collect::<HashMap<_, _>>();
    ResolvedSchema::new_with_known_schemata(vec![&schema], &None, &borrowed).unwrap();
    let owned = parsed.iter().map(|schema| (schema.name().unwrap().clone(), schema.clone())).collect::<HashMap<_, _>>();
    let value = types::Value::Record(vec![("next".into(), types::Value::Union(0, Box::new(types::Value::Null)))]);
    value.resolve_internal(&schema, &owned, &None, &None).unwrap();
    drop(borrowed);
    for schema in parsed {
        let Schema::Record(record) = schema else { panic!("expected referenced record") };
        assert_eq!(record.name.namespace, Some(String::new()));
        let Schema::Union(union) = &record.fields[0].schema else { panic!("expected union") };
        let Schema::Ref { name } = &union.variants()[1] else { panic!("expected recursive reference") };
        assert_eq!(name.namespace, Some(String::new()));
    }
}

#[test]
fn policy_errors_from_each_dynamic_name_boundary_are_returned_without_global_constructor_panics() {
    for text in [r#"{"type":"fixed","name":"reject","size":1}"#,
        r#"{"type":"record","name":"Allowed","aliases":["reject"],"fields":[]}"#,
        r#"{"type":"record","name":"Allowed","fields":[{"name":"reject","type":"int"}]}"#,
        r#"{"type":"enum","name":"Allowed","symbols":["reject"]}"#,
        r#"{"type":"record","name":"Allowed","fields":[{"name":"field","type":"reject"}]}"#] {
        assert!(Schema::parse_str_with_list_with_name_policy(text, Vec::<&str>::new(), &OpenNames { fallback: true }).is_err(), "{text}");
    }
    assert!(Schema::parse_str_with_list_with_name_policy("\"int\"", [r#"{"type":"fixed","name":"reject","size":1}"#], &OpenNames { fallback: true }).is_err());
    let policy = OpenNames { fallback: true };
    let parser = Parser { name_policy: Some(&policy), ..Default::default() };
    let seen = serde_json::json!({"type":"reject"});
    assert!(parser.already_seen_schema(seen.as_object().unwrap(), &None).is_err());
    let nested = serde_json::json!({"name":"Allowed","type":{"name":"reject"}});
    assert!(parser.parse_name(nested.as_object().unwrap(), &None).is_err());
    let name = Name { name: "Allowed".into(), namespace: Some(String::new()) };
    assert!(parser.schema_type_name(name, nested, &Schema::Int).is_err());
}

#[test]
fn nested_and_interleaved_policies_do_not_modify_each_other_or_the_global_route() {
    struct Prefix<'a> { prefix: &'a str, calls: Cell<usize>, nested: RefCell<Option<Schema>> }
    impl SchemaNamePolicy for Prefix<'_> {
        fn name(&self, name: &str, _namespace: &Namespace) -> AvroResult<Name> {
            let count = self.calls.replace(self.calls.get() + 1);
            if count == 0 && self.prefix == "outer" {
                Schema::parse_str(r#"{"type":"fixed","name":"Ordinary","size":1}"#)?;
                let inner = Prefix { prefix: "inner", calls: Cell::new(0), nested: RefCell::new(None) };
                let nested = Schema::parse_str_with_list_with_name_policy(r#"{"type":"fixed","name":"É","size":1}"#, Vec::<&str>::new(), &inner)?.0;
                *self.nested.borrow_mut() = Some(nested);
            }
            Ok(Name { name: name.into(), namespace: Some(self.prefix.into()) })
        }
        fn record_field(&self, _: &str) -> AvroResult<()> { Ok(()) }
        fn enum_symbol(&self, _: &str) -> AvroResult<()> { Ok(()) }
    }
    let outer = Prefix { prefix: "outer", calls: Cell::new(0), nested: RefCell::new(None) };
    let schema = Schema::parse_str_with_list_with_name_policy(r#"{"type":"fixed","name":"É","size":1}"#, Vec::<&str>::new(), &outer).unwrap().0;
    let Schema::Fixed(fixed) = schema else { panic!("expected outer fixed") };
    assert_eq!(fixed.name.fullname(None), "outer.É");
    let nested = outer.nested.borrow();
    let Some(Schema::Fixed(fixed)) = nested.as_ref() else { panic!("expected inner fixed") };
    assert_eq!(fixed.name.fullname(None), "inner.É");
    assert!(Name::new("É").is_err());
    assert!(Schema::parse_str(r#"{"type":"fixed","name":"Ordinary","size":1}"#).is_ok());
}

#[test]
fn duration_logical_type_does_not_insert_an_unscoped_named_key() {
    let policy = OpenNames { fallback: true };
    let mut parser = Parser { name_policy: Some(&policy), ..Default::default() };
    let schema = parser.parse_str(r#"{"type":"fixed","name":"Διάρκεια","size":12,"logicalType":"duration"}"#).unwrap();
    assert_eq!(schema, Schema::Duration);
    assert!(parser.parsed_schemas.keys().all(|name| name.namespace.as_deref() == Some("")));
    assert!(parser.parsed_schemas.keys().all(|name| name.name != "duration"));
    let serialized = serde_json::to_string(&schema).unwrap();
    assert_eq!(parse(&serialized), Schema::Duration);
    ResolvedSchema::try_from(&schema).unwrap();
}

#[test]
fn nested_named_type_wrappers_keep_effective_definition_keys_and_input_order() {
    let references = [
        r#"{"name":"Outer","namespace":"pkg","type":{"type":"fixed","name":"Inner","size":1}}"#,
        r#"{"type":"fixed","name":"Plain","size":4}"#,
        r#"{"name":"RecordWrapper","namespace":"discarded","type":{"type":"record","name":"Record","namespace":"selected","fields":[]}}"#,
        r#"{"name":"Wrapper1","namespace":"outer","type":{"name":"Wrapper2","namespace":"middle","type":{"type":"enum","name":"EnumGlobal","symbols":["A"]}}}"#,
    ];
    let root = r#"["Inner","Plain","selected.Record","EnumGlobal"]"#;
    let (schema, parsed) = Schema::parse_str_with_list_with_name_policy(root, references, &OpenNames { fallback: true }).unwrap();
    let expected = ["Inner", "Plain", "selected.Record", "EnumGlobal"];
    assert_eq!(parsed.iter().map(|schema| schema.name().unwrap().fullname(None)).collect::<Vec<_>>(), expected);
    let Schema::Union(union) = &schema else { panic!("expected union") };
    assert_eq!(union.variants().iter().map(|schema| schema.name().unwrap().fullname(None)).collect::<Vec<_>>(), expected);
    let known = parsed.iter().map(|schema| (schema.name().unwrap().clone(), schema)).collect::<HashMap<_, _>>();
    ResolvedSchema::new_with_known_schemata(vec![&schema], &None, &known).unwrap();
    let (_, reversed) = Schema::parse_str_with_list_with_name_policy(root, references.into_iter().rev(), &OpenNames { fallback: true }).unwrap();
    assert_eq!(reversed.iter().map(|schema| schema.name().unwrap().fullname(None)).collect::<Vec<_>>(), expected.into_iter().rev().collect::<Vec<_>>());
    assert!(Schema::parse_str_with_list_with_name_policy("\"Outer\"", references, &OpenNames { fallback: true }).is_err());
}

#[test]
fn scoped_duplicate_effective_names_collide_after_wrapper_and_empty_namespace_normalization() {
    for references in [
        [r#"{"type":"fixed","name":".A","size":1}"#, r#"{"type":"fixed","name":"A","namespace":"","size":2}"#],
        [r#"{"name":"Ignored","namespace":"ignored","type":{"type":"fixed","name":"A","size":1}}"#, r#"{"type":"fixed","name":"A","size":2}"#],
    ] {
        let error = Schema::parse_str_with_list_with_name_policy("\"int\"", references, &OpenNames { fallback: true }).unwrap_err();
        assert!(matches!(error.details(), Details::NameCollision(name) if name == "A"));
    }
}

#[test]
fn scoped_reference_inputs_require_an_effective_named_declaration() {
    for reference in [
        r#"{"name":"Ignored","type":{"type":"array","items":"int"}}"#,
        r#"{"name":"Ignored","type":{"type":"fixed","size":1}}"#,
        r#"{"name":"Ignored","type":{"name":"IgnoredAgain","type":{"type":"fixed","name":"reject","size":1}}}"#,
    ] {
        assert!(Schema::parse_str_with_list_with_name_policy("\"int\"", [reference], &OpenNames { fallback: true }).is_err());
    }
}

#[test]
fn scoped_type_alias_references_use_canonical_record_enum_fixed_names_and_not_field_aliases() {
    let schema = parse(r#"{"type":"record","name":"Root","fields":[
        {"name":"record","aliases":["FieldAlias"],"type":{"type":"record","name":"CanonicalRecord","aliases":["OldRecord"],"fields":[]}},
        {"name":"enum","type":{"type":"enum","name":"CanonicalEnum","aliases":["OldEnum"],"symbols":["A"]}},
        {"name":"fixed","type":{"type":"fixed","name":"CanonicalFixed","aliases":["OldFixed"],"size":1}},
        {"name":"recordReference","type":"OldRecord"},
        {"name":"enumReference","type":"OldEnum"},
        {"name":"fixedReference","type":"OldFixed"}]}"#);
    let Schema::Record(record) = &schema else { panic!("expected record") };
    for (index, name) in [(3, "CanonicalRecord"), (4, "CanonicalEnum"), (5, "CanonicalFixed")] {
        let Schema::Ref { name: actual } = &record.fields[index].schema else { panic!("expected canonical reference") };
        assert_eq!(actual.fullname(None), name);
    }
    ResolvedSchema::try_from(&schema).unwrap();
    resolve_names(&schema, &mut HashMap::new(), &None).unwrap();
    let field_alias_type = r#"{"type":"record","name":"Root","fields":[{"name":"value","aliases":["FieldAlias"],"type":"int"},{"name":"other","type":"FieldAlias"}]}"#;
    assert!(Schema::parse_str_with_list_with_name_policy(field_alias_type, Vec::<&str>::new(), &OpenNames { fallback: true }).is_err());
}

#[test]
fn scoped_aliases_follow_the_final_canonical_logical_type_after_input_parsing() {
    let reference = r#"{"type":"fixed","name":"D","aliases":["OldD"],"size":12,"logicalType":"duration"}"#;
    let root = r#"{"type":"record","name":"Root","fields":[{"name":"canonical","type":"D"},{"name":"alias","type":"OldD"}]}"#;
    let (schema, parsed) = Schema::parse_str_with_list_with_name_policy(root, [reference], &OpenNames { fallback: true }).unwrap();
    assert_eq!(parsed, [Schema::Duration]);
    let Schema::Record(record) = &schema else { panic!("expected record") };
    assert_eq!(record.fields[0].schema, Schema::Duration);
    assert_eq!(record.fields[1].schema, Schema::Duration);
    ResolvedSchema::try_from(&schema).unwrap();
}

#[test]
fn nested_and_external_fixed_logical_definitions_register_the_final_type_for_canonical_and_alias_names() {
    for (declaration, kind) in [
        (r#"{"type":"fixed","name":"D","aliases":["OldD"],"size":12,"logicalType":"duration"}"#, SchemaKind::Duration),
        (r#"{"type":"fixed","name":"D","aliases":["OldD"],"size":16,"logicalType":"uuid"}"#, SchemaKind::Uuid),
        (r#"{"type":"fixed","name":"D","aliases":["OldD"],"size":2,"logicalType":"decimal","precision":4,"scale":1}"#, SchemaKind::Decimal),
    ] {
        let logical = parse(declaration);
        assert_eq!(SchemaKind::from(&logical), kind);
        match &logical {
            Schema::Duration | Schema::Uuid => {},
            Schema::Decimal(decimal) => {
                assert_eq!((decimal.precision, decimal.scale), (4, 1));
                let Schema::Fixed(fixed) = decimal.inner.as_ref() else { panic!("expected fixed decimal") };
                assert_eq!(fixed.size, 2);
                assert_eq!(fixed.name.fullname(None), "D");
                assert_eq!(fixed.aliases.as_ref().unwrap()[0].fullname(None), "OldD");
            }
            _ => panic!("expected fixed logical type"),
        }
        let root = format!(r#"{{"type":"record","name":"Root","fields":[{{"name":"definition","type":{declaration}}},{{"name":"canonical","type":"D"}},{{"name":"alias","type":"OldD"}}]}}"#);
        let schema = parse(&root);
        let Schema::Record(record) = &schema else { panic!("expected nested record") };
        assert_eq!(record.fields[0].schema, logical);
        assert_eq!(record.fields[1].schema, logical);
        assert_eq!(record.fields[2].schema, logical);
        ResolvedSchema::try_from(&schema).unwrap();
        let root = r#"{"type":"record","name":"Root","fields":[{"name":"canonical","type":"D"},{"name":"alias","type":"OldD"}]}"#;
        let (schema, parsed) = Schema::parse_str_with_list_with_name_policy(root, [declaration], &OpenNames { fallback: true }).unwrap();
        assert_eq!(parsed, [logical.clone()]);
        let Schema::Record(record) = &schema else { panic!("expected external record") };
        assert_eq!(record.fields[0].schema, logical);
        assert_eq!(record.fields[1].schema, logical);
        ResolvedSchema::try_from(&schema).unwrap();
    }
}

struct RawFieldLookup;
impl SchemaNamePolicy for RawFieldLookup {
    fn name(&self, name: &str, namespace: &Namespace) -> AvroResult<Name> { OpenNames { fallback: false }.name(name, namespace) }
    fn record_field(&self, name: &str) -> AvroResult<()> { OpenNames { fallback: false }.record_field(name) }
    fn enum_symbol(&self, symbol: &str) -> AvroResult<()> { OpenNames { fallback: false }.enum_symbol(symbol) }
    fn record_field_lookup_uses_aliases(&self) -> bool { false }
}

fn raw_fields(text: &str) -> RecordSchema {
    let (schema, _) = Schema::parse_str_with_list_with_name_policy(text, Vec::<&str>::new(), &RawFieldLookup).unwrap();
    let Schema::Record(record) = schema else { panic!("expected record") };
    record
}

#[test]
fn scoped_raw_field_lookup_accepts_an_alias_that_matches_a_later_raw_name() {
    let text = r#"{"type":"record","name":"R","fields":[{"name":"a","aliases":["b"],"type":"int"},{"name":"b","type":"long"}]}"#;
    assert!(Schema::parse_str(text).is_err());
    assert!(Schema::parse_str_with_list_with_name_policy(text, Vec::<&str>::new(), &OpenNames { fallback: false }).is_err());
    let record = raw_fields(text);
    assert_eq!(record.lookup.get("a"), Some(&0));
    assert_eq!(record.lookup.get("b"), Some(&1));
    assert_eq!(record.fields[0].aliases.as_deref(), Some(["b".to_owned()].as_slice()));
    assert_eq!(record.fields[1].schema, Schema::Long);
}

#[test]
fn scoped_raw_field_lookup_preserves_a_raw_field_hidden_by_a_later_alias() {
    let text = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","aliases":["a"],"type":"long"}]}"#;
    let record = raw_fields(text);
    assert_eq!(record.lookup.get("a"), Some(&0));
    assert_eq!(record.lookup.get("b"), Some(&1));
    let Schema::Record(ordinary) = Schema::parse_str(text).unwrap() else { panic!("expected record") };
    assert_eq!(ordinary.lookup.get("a"), Some(&1));
    let Schema::Record(scoped_default) = parse(text) else { panic!("expected record") };
    assert_eq!(scoped_default.lookup.get("a"), Some(&1));
    assert_eq!(record.fields[1].aliases.as_deref(), Some(["a".to_owned()].as_slice()));
}

#[test]
fn scoped_raw_field_lookup_retains_shared_alias_metadata_without_indexing_it() {
    let text = r#"{"type":"record","name":"R","fields":[{"name":"a","aliases":["shared"],"type":"int"},{"name":"b","aliases":["shared"],"type":"long"}]}"#;
    let record = raw_fields(text);
    assert_eq!(record.lookup.len(), 2);
    assert!(!record.lookup.contains_key("shared"));
    for field in &record.fields { assert_eq!(field.aliases.as_deref(), Some(["shared".to_owned()].as_slice())); }
    let Schema::Record(ordinary) = Schema::parse_str(text).unwrap() else { panic!("expected record") };
    assert_eq!(ordinary.lookup.get("shared"), Some(&1));
    let duplicate = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"a","type":"long"}]}"#;
    assert!(Schema::parse_str_with_list_with_name_policy(duplicate, Vec::<&str>::new(), &RawFieldLookup).is_err());
}

#[test]
fn scoped_raw_field_lookup_keeps_reader_field_alias_compatibility() {
    let writer = raw_fields(r#"{"type":"record","name":"R","fields":[{"name":"original","type":"int"}]}"#);
    let reader = raw_fields(r#"{"type":"record","name":"R","fields":[{"name":"renamed","aliases":["original"],"type":"int"}]}"#);
    assert!(!reader.lookup.contains_key("original"));
    assert!(crate::schema_compatibility::SchemaCompatibility::can_read(&Schema::Record(writer), &Schema::Record(reader)).is_ok());
}
