use super::*;
use super::super::test_support as support;

fn policy() -> producer::SourcePolicy { producer::SourcePolicy { validate_defaults: false, fixed_bytes: FixedBytesLimit::DEFAULT } }
fn parse(text: &str, context: &mut ConversionContext) -> SchemaHandle {
    producer::parse(text, &[], policy(), context).unwrap().root
}
fn logical(text: &str, context: &mut ConversionContext) -> Option<LogicalType> { parse(text, context).view().logical_type() }

#[test]
fn builtins_are_constructed_on_their_pinned_base_types_and_uuid_has_no_base_restriction() {
    let mut context = support::context();
    for (name, kind, expected) in [
        ("uuid", "boolean", Builtin::Uuid), ("big-decimal", "bytes", Builtin::BigDecimal),
        ("date", "int", Builtin::Date), ("time-millis", "int", Builtin::TimeMillis),
        ("time-micros", "long", Builtin::TimeMicros), ("timestamp-millis", "long", Builtin::TimestampMillis),
        ("timestamp-micros", "long", Builtin::TimestampMicros),
        ("local-timestamp-millis", "long", Builtin::LocalTimestampMillis),
        ("local-timestamp-micros", "long", Builtin::LocalTimestampMicros),
    ] {
        let text = format!(r#"{{"type":"{kind}","logicalType":"{name}"}}"#);
        assert_eq!(logical(&text, &mut context), Some(LogicalType::Builtin(expected)));
    }
    assert_eq!(logical(r#"{"type":"array","items":"int","logicalType":"uuid"}"#, &mut context), Some(LogicalType::Builtin(Builtin::Uuid)));
    assert_eq!(logical(r#"{"type":"record","name":"R","fields":[],"logicalType":"uuid"}"#, &mut context), Some(LogicalType::Builtin(Builtin::Uuid)));
}

#[test]
fn ignored_invalid_and_case_mismatched_types_preserve_the_raw_connect_property() {
    let mut context = support::context();
    for text in [r#"{"type":"long","logicalType":"date"}"#, r#"{"type":"string","logicalType":"big-decimal"}"#,
        r#"{"type":"int","logicalType":"DATE"}"#, r#"{"type":"bytes","logicalType":"Decimal","precision":4}"#] {
        let source = parse(text, &mut context);
        assert_eq!(source.view().logical_type(), None);
        assert!(source.view().raw_logical_name().is_some());
    }
    for text in [r#"{"type":"int","logicalType":null}"#, r#"{"type":"int","logicalType":1}"#, r#"{"type":"int"}"#] {
        let source = parse(text, &mut context);
        assert_eq!(source.view().logical_type(), None);
        assert!(source.view().raw_logical_name().is_none());
    }
    let upper = parse(r#"{"type":"int","logicalType":"DATE"}"#, &mut context);
    assert!(upper.view().raw_logical_name().unwrap().units().eq("DATE".encode_utf16()));
}

#[test]
fn incompatible_properties_reject_only_textual_values_including_empty_text() {
    let mut context = support::context();
    for key in ["avro.java.string", "java-class", "java-key-class", "java-element-class"] {
        for value in [r#""""#, r#""x""#] {
            let text = format!(r#"{{"type":"int","logicalType":"date","{key}":{value}}}"#);
            assert_eq!(logical(&text, &mut context), None);
        }
        for value in ["null", "1", "{}", "[]", "true"] {
            let text = format!(r#"{{"type":"int","logicalType":"date","{key}":{value}}}"#);
            assert_eq!(logical(&text, &mut context), Some(LogicalType::Builtin(Builtin::Date)));
        }
    }
}

#[test]
fn decimal_requires_actual_integer_nodes_and_preserves_optional_scale_zero() {
    let mut context = support::context();
    assert_eq!(logical(r#"{"type":"bytes","logicalType":"decimal","precision":4}"#, &mut context), Some(LogicalType::Decimal { precision: 4, scale: 0 }));
    assert_eq!(logical(r#"{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}"#, &mut context), Some(LogicalType::Decimal { precision: 4, scale: 2 }));
    assert_eq!(logical(r#"{"type":"bytes","logicalType":"decimal","precision":4,"scale":9223372036854775808}"#, &mut context), Some(LogicalType::Decimal { precision: 4, scale: 0 }));
    for precision in ["null", "0", "-1", "4.0", "2147483648", "9223372036854775808", r#""4""#, "{}"] {
        let text = format!(r#"{{"type":"bytes","logicalType":"decimal","precision":{precision}}}"#);
        assert_eq!(logical(&text, &mut context), None);
    }
    for scale in ["null", "-1", "5", "2.0", "2147483648", r#""2""#] {
        let text = format!(r#"{{"type":"bytes","logicalType":"decimal","precision":4,"scale":{scale}}}"#);
        assert_eq!(logical(&text, &mut context), None);
    }
    assert_eq!(logical(r#"{"type":"bytes","logicalType":"decimal"}"#, &mut context), None);
    assert_eq!(logical(r#"{"type":"int","logicalType":"decimal","precision":4}"#, &mut context), None);
}

#[test]
fn decimal_object_access_distinguishes_unmatched_numeric_fallthrough_from_null_sentinel() {
    let mut context = support::context();
    for (value, expected) in [("decimal", Some(LogicalType::Decimal { precision: 4, scale: 0 })), ("integer", Some(LogicalType::Decimal { precision: 4, scale: 0 })), ("null", None), ("long", None)] {
        let mut builder = SchemasBuilder::new(&mut context).unwrap();
        let id = builder.primitive(Type::Bytes, &mut context).unwrap();
        let name = support::text("decimal", &mut context);
        let name = support::node(JsonValue::Text(name), &mut context);
        let precision = support::node(JsonValue::I32(4), &mut context);
        let scale = match value {
            "decimal" => JsonNode::decimal("2.0", &mut context).unwrap(),
            "integer" => JsonNode::integer_decimal(JavaText::from_utf8("2"), &mut context).unwrap(),
            "null" => support::node(JsonValue::Null, &mut context),
            _ => support::node(JsonValue::I64(2), &mut context),
        };
        let properties = support::properties(vec![("logicalType", name), ("precision", precision), ("scale", scale)], &mut context);
        builder.properties(id, properties).unwrap();
        builder.construct_logical(id, &mut Unregistered, &mut context).unwrap();
        let source = builder.finish(id, &mut context).unwrap();
        assert_eq!(source.view().logical_type(), expected);
    }
}

#[test]
fn fixed_decimal_precision_uses_source_wrapping_int_arithmetic_without_a_payload_allocation() {
    let mut context = support::context();
    for (size, precision, valid) in [(1, 2, true), (1, 3, false), (2, 4, true), (2, 5, false), (16, 38, true), (16, 39, false), (0, 1, false), (268435456, 1, true), (536870912, 1, false)] {
        let text = format!(r#"{{"type":"fixed","name":"F","size":{size},"logicalType":"decimal","precision":{precision}}}"#);
        assert_eq!(logical(&text, &mut context).is_some(), valid, "size {size}, precision {precision}");
    }
    assert_eq!(fixed_precision_bounds(0), (-1, -1));
    assert_eq!(fixed_precision_bounds(268435456), (646456992, 646456992));
    assert_eq!(fixed_precision_bounds(536870912), (-1, -1));
    assert_eq!(fixed_precision_bounds(16), (38, 38));
}

#[derive(Clone, Copy)]
enum Answer { NoType, Runtime, Singleton(Builtin), Unmodelled, ResourceError }
struct Environment {
    answer: Answer,
    names: Vec<Vec<u16>>,
    aliases: Vec<usize>,
    field_counts: Vec<Option<usize>>,
    math: Vec<i32>,
    precision: i64,
}
impl Environment {
    fn new(answer: Answer) -> Self { Self { answer, names: Vec::new(), aliases: Vec::new(), field_counts: Vec::new(), math: Vec::new(), precision: 0 } }
}
impl LogicalEnvironment for Environment {
    fn factory(&mut self, name: JavaText<'_>, schema: SchemaView<'_>, context: &mut ConversionContext) -> Result<FactoryOutcome> {
        self.names.push(name.units().collect());
        self.aliases.push(schema.node().aliases.len());
        self.field_counts.push(match &schema.node().kind { Kind::Record { fields: Some(fields), .. } => Some(fields.len()), _ => None });
        Ok(match self.answer {
            Answer::NoType => FactoryOutcome::NoType,
            Answer::Runtime => FactoryOutcome::RuntimeException,
            Answer::Singleton(value) => FactoryOutcome::Singleton(value),
            Answer::Unmodelled => FactoryOutcome::UnmodelledType,
            Answer::ResourceError => { context.reserve(context.allocation_remaining().checked_add(1).unwrap())?; unreachable!() },
        })
    }
    fn fixed_decimal_precision(&mut self, size: i32, _: &mut ConversionContext) -> Result<i64> {
        self.math.push(size);
        Ok(self.precision)
    }
}

#[test]
fn ambiguous_math_threshold_requires_an_observation_only_when_the_decision_depends_on_it() {
    let mut context = support::context();
    let mut environment = Environment::new(Answer::NoType);
    let size = 129397790;
    assert_eq!(fixed_precision_bounds(size), (311620928, 311620929));
    assert!(decimal_fits(size, 311620928, &mut environment, &mut context).unwrap());
    assert!(!decimal_fits(size, 311620930, &mut environment, &mut context).unwrap());
    assert!(environment.math.is_empty());
    assert!(decimal_fits(size, 311620929, &mut Unregistered, &mut context).unwrap_err().code.ends_with("avro-logical-math-profile-required"));
    environment.precision = 311620928;
    assert!(!decimal_fits(size, 311620929, &mut environment, &mut context).unwrap());
    environment.precision = 311620929;
    assert!(decimal_fits(size, 311620929, &mut environment, &mut context).unwrap());
    assert_eq!(environment.math, vec![size, size]);
    environment.precision = 0;
    assert!(decimal_fits(size, 311620929, &mut environment, &mut context).unwrap_err().code.ends_with("avro-logical-math-observation"));
}

#[test]
fn builtin_singletons_precede_factories_and_factory_runtime_failures_are_ignored() {
    let mut context = support::context();
    let mut environment = Environment::new(Answer::Runtime);
    let known = producer::parse_with_logical(r#"{"type":"int","logicalType":"date"}"#, &[], policy(), &mut environment, &mut context).unwrap();
    assert_eq!(known.root.view().logical_type(), Some(LogicalType::Builtin(Builtin::Date)));
    assert!(environment.names.is_empty());
    let custom = producer::parse_with_logical(r#"{"type":"int","logicalType":"custom"}"#, &[], policy(), &mut environment, &mut context).unwrap();
    assert_eq!(custom.root.view().logical_type(), None);
    assert_eq!(environment.names, vec!["custom".encode_utf16().collect::<Vec<_>>()]);
    assert!(custom.root.view().raw_logical_name().unwrap().units().eq("custom".encode_utf16()));
    environment.answer = Answer::Singleton(Builtin::Date);
    let custom = producer::parse_with_logical(r#"{"type":"int","logicalType":"custom"}"#, &[], policy(), &mut environment, &mut context).unwrap();
    assert_eq!(custom.root.view().logical_type(), Some(LogicalType::Builtin(Builtin::Date)));
    assert_eq!(custom.root.view().logical_type().unwrap().name(), "date");
    let invalid = producer::parse_with_logical(r#"{"type":"long","logicalType":"custom"}"#, &[], policy(), &mut environment, &mut context).unwrap();
    assert_eq!(invalid.root.view().logical_type(), None);
}

#[test]
fn unknown_factory_class_and_native_resource_failures_remain_outer_errors() {
    let mut context = support::context();
    let text = r#"{"type":"int","logicalType":"custom"}"#;
    let mut environment = Environment::new(Answer::Unmodelled);
    assert!(producer::parse_with_logical(text, &[], policy(), &mut environment, &mut context).err().unwrap().code.ends_with("avro-custom-logical-type"));
    environment.answer = Answer::ResourceError;
    assert!(producer::parse_with_logical(text, &[], policy(), &mut environment, &mut context).err().unwrap().code.ends_with("batch-too-large"));
}

#[test]
fn factory_runs_after_children_and_properties_but_before_named_aliases() {
    let mut context = support::context();
    let mut environment = Environment::new(Answer::Runtime);
    let source = producer::parse_with_logical(r#"{"type":"record","name":"R","aliases":["OldR"],"logicalType":"parent","fields":[{"name":"x","type":{"type":"int","logicalType":"child"}}]}"#, &[], policy(), &mut environment, &mut context).unwrap();
    assert_eq!(environment.names, vec!["child".encode_utf16().collect::<Vec<_>>(), "parent".encode_utf16().collect::<Vec<_>>()]);
    assert_eq!(environment.aliases, vec![0, 0]);
    assert_eq!(environment.field_counts, vec![None, Some(1)]);
    assert_eq!(source.root.view().node().aliases.len(), 1);
    assert!(source.root.view().raw_logical_name().unwrap().units().eq("parent".encode_utf16()));
}

#[test]
fn singleton_identity_and_decimal_value_equality_are_distinct_from_fresh_decimal_identity() {
    let mut context = support::context();
    let date = parse(r#"{"type":"int","logicalType":"date"}"#, &mut context);
    let other = parse(r#"{"type":"int","logicalType":"date"}"#, &mut context);
    assert_eq!(date.view().logical_identity(), other.view().logical_identity());
    let text = r#"{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}"#;
    let decimal = parse(text, &mut context);
    let equal = parse(text, &mut context);
    assert_ne!(decimal.view().logical_identity(), equal.view().logical_identity());
    assert_eq!(decimal.view().logical_type(), equal.view().logical_type());
    assert_eq!(decimal.view().logical_type().unwrap().decimal_hash(), Some(126));
}

#[test]
fn logical_object_construction_does_not_enter_schema_equality_or_hash() {
    let mut context = support::context();
    let constructed = parse(r#"{"type":"int","logicalType":"date"}"#, &mut context);
    let mut builder = SchemasBuilder::new(&mut context).unwrap();
    let id = builder.primitive(Type::Int, &mut context).unwrap();
    let text = support::text("date", &mut context);
    let value = support::node(JsonValue::Text(text), &mut context);
    let properties = support::properties(vec![("logicalType", value)], &mut context);
    builder.properties(id, properties).unwrap();
    let unconstructed = builder.finish(id, &mut context).unwrap();
    assert!(unconstructed.view().logical_type().is_none());
    let mut seen = RecursionState::new();
    let mut witness = support::Observed::default();
    let mut evaluation = Evaluation::new(&mut seen, &mut context, &mut witness).unwrap();
    assert!(evaluation.equal(&constructed, &unconstructed).unwrap());
    assert_eq!(evaluation.hash_code(&constructed).unwrap(), evaluation.hash_code(&unconstructed).unwrap());
}

#[test]
fn admitted_datum_references_keep_the_actual_source_logical_object_after_context_drop() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let references = vec![r#"{"type":"fixed","name":"ns.Money","size":2,"logicalType":"decimal","precision":4,"scale":2}"#.into()];
    let datum = crate::connect::avro::AvroDatumPlan::parse_admitted_source(r#"{"type":"record","name":"ns.R","fields":[{"name":"value","type":"Money"}]}"#, &references, &BTreeMap::new(), policy(), &mut context).unwrap();
    let selected = datum.paired_root().unwrap().field(0).unwrap().canonical().unwrap();
    let definition = datum.paired_definition("ns.Money").unwrap().unwrap().canonical().unwrap();
    assert!(selected.same_object(&definition));
    assert_eq!(selected.view().logical_identity(), definition.view().logical_identity());
    drop(context);
    assert_eq!(selected.view().logical_type(), Some(LogicalType::Decimal { precision: 4, scale: 2 }));
    drop(datum);
    drop(definition);
    assert!(budget.used_bytes() > 0);
    drop(selected);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn profiled_datum_entry_recovers_factory_failure_without_dropping_raw_properties() {
    let mut context = support::context();
    let mut environment = Environment::new(Answer::Runtime);
    let datum = crate::connect::avro::AvroDatumPlan::parse_admitted_source_with_logical(r#"{"type":"int","logicalType":"custom"}"#, &[], &BTreeMap::new(), policy(), &mut environment, &mut context).unwrap();
    let selected = datum.paired_root().unwrap().canonical().unwrap();
    assert_eq!(selected.view().logical_type(), None);
    assert!(selected.view().raw_logical_name().unwrap().units().eq("custom".encode_utf16()));
    assert_eq!(environment.names.len(), 1);
}

#[test]
fn factory_observes_named_docs_before_fields_complete_and_aliases_are_installed() {
    struct Docs {
        seen: Vec<(Type, Option<Vec<u16>>, usize, Option<Vec<u16>>)>,
    }
    impl LogicalEnvironment for Docs {
        fn factory(&mut self, _: JavaText<'_>, schema: SchemaView<'_>, _: &mut ConversionContext) -> Result<FactoryOutcome> {
            let own = schema.node().doc.as_ref().map(|doc| doc.view().units().collect());
            let parent = schema.owner.nodes.iter()
                .find(|node| matches!(&node.kind, Kind::Record { .. }))
                .and_then(|node| node.doc.as_ref())
                .map(|doc| doc.view().units().collect());
            self.seen.push((source_type(&schema.node().kind), own, schema.node().aliases.len(), parent));
            Ok(FactoryOutcome::NoType)
        }
        fn fixed_decimal_precision(&mut self, _: i32, _: &mut ConversionContext) -> Result<i64> { unreachable!() }
    }
    let mut context = support::context();
    let mut docs = Docs { seen: Vec::new() };
    let source = producer::parse_with_logical(r#"{"type":"record","name":"R","doc":"parent doc","aliases":["OldR"],"logicalType":"parent","fields":[{"name":"x","type":{"type":"int","logicalType":"child"}},{"name":"e","type":{"type":"enum","name":"E","symbols":["ONE"],"doc":"enum doc","aliases":["OldE"],"logicalType":"enum"}},{"name":"f","type":{"type":"fixed","name":"F","size":1,"doc":"fixed doc","aliases":["OldF"],"logicalType":"fixed"}}]}"#, &[], policy(), &mut docs, &mut context).unwrap();
    let units = |text: &str| text.encode_utf16().collect::<Vec<_>>();
    assert_eq!(docs.seen, vec![
        (Type::Int, None, 0, Some(units("parent doc"))),
        (Type::Enum, Some(units("enum doc")), 0, Some(units("parent doc"))),
        (Type::Fixed, Some(units("fixed doc")), 0, Some(units("parent doc"))),
        (Type::Record, Some(units("parent doc")), 0, Some(units("parent doc"))),
    ]);
    let view = source.root.view();
    let Kind::Record { fields: Some(fields), .. } = &view.node().kind else { panic!("expected record") };
    assert_eq!(view.node().aliases.len(), 1);
    assert_eq!(view.child(fields[1].schema).unwrap().node().aliases.len(), 1);
    assert_eq!(view.child(fields[2].schema).unwrap().node().aliases.len(), 1);
}
