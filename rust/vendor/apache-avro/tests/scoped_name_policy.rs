use apache_avro::{AvroResult, Schema, error::{Details, Error}, schema::{Name, Namespace, ResolvedSchema, SchemaKind, SchemaNamePolicy}, validator::{self, SchemaNameValidator, SchemaNamespaceValidator, EnumSymbolNameValidator, RecordFieldNameValidator}};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

struct Deny(Arc<AtomicUsize>);
impl Deny {
    fn error(&self, name: &str) -> Error {
        self.0.fetch_add(1, Ordering::SeqCst);
        Details::FieldName(format!("global validator called: {name}")).into()
    }
}
impl SchemaNameValidator for Deny {
    fn validate(&self, name: &str) -> AvroResult<(String, Namespace)> { Err(self.error(name)) }
}
impl SchemaNamespaceValidator for Deny {
    fn validate(&self, name: &str) -> AvroResult<()> { Err(self.error(name)) }
}
impl EnumSymbolNameValidator for Deny {
    fn validate(&self, name: &str) -> AvroResult<()> { Err(self.error(name)) }
}
impl RecordFieldNameValidator for Deny {
    fn validate(&self, name: &str) -> AvroResult<()> { Err(self.error(name)) }
}

struct Local;
impl SchemaNamePolicy for Local {
    fn name(&self, text: &str, namespace: &Namespace) -> AvroResult<Name> {
        Ok(match text.rsplit_once('.') {
            Some((namespace, name)) => Name { name: name.into(), namespace: (!namespace.is_empty()).then(|| namespace.into()) },
            None => Name { name: text.into(), namespace: namespace.clone() },
        })
    }
    fn record_field(&self, _: &str) -> AvroResult<()> { Ok(()) }
    fn enum_symbol(&self, _: &str) -> AvroResult<()> { Ok(()) }
    fn reference_fallback(&self, name: &str, _: &Namespace) -> AvroResult<Option<Name>> { self.name(name, &None).map(Some) }
}

#[test]
fn scoped_parsing_and_default_error_diagnostics_never_read_registered_global_validators() {
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(validator::set_schema_name_validator(Box::new(Deny(Arc::clone(&calls)))).is_ok());
    assert!(validator::set_schema_namespace_validator(Box::new(Deny(Arc::clone(&calls)))).is_ok());
    assert!(validator::set_enum_symbol_name_validator(Box::new(Deny(Arc::clone(&calls)))).is_ok());
    assert!(validator::set_record_field_name_validator(Box::new(Deny(Arc::clone(&calls)))).is_ok());
    let root = r#"{"type":"record","name":"local.É","aliases":["AliasÉ"],"fields":[
        {"name":"état","type":{"type":"enum","name":"État","symbols":["Été"],"aliases":["AncienÉtat"]}},
        {"name":"clé","type":{"type":"fixed","name":".Clé","size":2,"aliases":[".AncienneClé"]}},
        {"name":"autre","type":"Autre"},
        {"name":"suivant","type":["null","local.É"],"default":null}]}"#;
    let references = [r#"{"type":"fixed","name":"Autre","size":1}"#];
    let (schema, parsed) = Schema::parse_str_with_list_with_name_policy(root, references, &Local).unwrap();
    assert!(matches!(schema, Schema::Record(_)));
    assert_eq!(parsed.len(), 1);
    let wrapped = r#"{"name":"Ignored","namespace":"discarded","type":{"type":"fixed","name":"Éffective","aliases":["AncienÉffective"],"size":1}}"#;
    let (schema, parsed) = Schema::parse_str_with_list_with_name_policy("\"AncienÉffective\"", [wrapped], &Local).unwrap();
    let Schema::Ref { name } = &schema else { panic!("expected canonical wrapped reference") };
    assert_eq!(name.fullname(None), "Éffective");
    assert_eq!(parsed[0].name(), Some(name));
    let known = std::collections::HashMap::from([(name.clone(), &parsed[0])]);
    ResolvedSchema::new_with_known_schemata(vec![&schema], &None, &known).unwrap();
    for (declaration, kind) in [
        (r#"{"type":"fixed","name":"D","aliases":["OldD"],"size":12,"logicalType":"duration"}"#, SchemaKind::Duration),
        (r#"{"type":"fixed","name":"D","aliases":["OldD"],"size":16,"logicalType":"uuid"}"#, SchemaKind::Uuid),
        (r#"{"type":"fixed","name":"D","aliases":["OldD"],"size":2,"logicalType":"decimal","precision":4,"scale":1}"#, SchemaKind::Decimal),
    ] {
        let nested = format!(r#"{{"type":"record","name":"Root","fields":[{{"name":"definition","type":{declaration}}},{{"name":"canonical","type":"D"}},{{"name":"alias","type":"OldD"}}]}}"#);
        let (schema, _) = Schema::parse_str_with_list_with_name_policy(&nested, Vec::<&str>::new(), &Local).unwrap();
        let Schema::Record(record) = &schema else { panic!("expected nested record") };
        assert_eq!(SchemaKind::from(&record.fields[0].schema), kind);
        assert_eq!(record.fields[1].schema, record.fields[0].schema);
        assert_eq!(record.fields[2].schema, record.fields[0].schema);
        ResolvedSchema::try_from(&schema).unwrap();
        let root = r#"{"type":"record","name":"Root","fields":[{"name":"canonical","type":"D"},{"name":"alias","type":"OldD"}]}"#;
        let (schema, parsed) = Schema::parse_str_with_list_with_name_policy(root, [declaration], &Local).unwrap();
        assert_eq!(SchemaKind::from(&parsed[0]), kind);
        let Schema::Record(record) = &schema else { panic!("expected external record") };
        assert_eq!(record.fields[0].schema, parsed[0]);
        assert_eq!(record.fields[1].schema, parsed[0]);
        ResolvedSchema::try_from(&schema).unwrap();
    }
    let invalid_default = r#"{"type":"record","name":"R","fields":[{"name":"d","type":{"type":"fixed","name":"Durée","size":12,"logicalType":"duration"},"default":1}]}"#;
    let error = Schema::parse_str_with_list_with_name_policy(invalid_default, Vec::<&str>::new(), &Local).unwrap_err();
    assert!(matches!(error.details(), Details::GetDefaultRecordField(_, _, _)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(Name::new("Ordinary").is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
