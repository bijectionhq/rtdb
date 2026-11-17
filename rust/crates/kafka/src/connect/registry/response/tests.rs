use super::*;
use std::sync::Arc;
use rtdb_columnar::MemoryBudget;
use rtdb_types::java_text::JavaString;

const PROFILES: [Profile; 2] = [Profile::Jackson216, Profile::Jackson221];
fn run(inspect: impl FnOnce(&mut ConversionContext)) {
    let budget = MemoryBudget::new(1 << 26);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    inspect(&mut context);
    drop(context);
    assert_eq!(budget.used_bytes(), 0);
}
fn units(value: &Option<JavaString>) -> Option<Vec<u16>> { value.as_ref().map(|value| value.view().units().collect()) }
fn text(value: &Option<JavaString>, expected: &str) { assert_eq!(units(value), Some(expected.encode_utf16().collect())); }
fn failed<T>(value: Result<T>, expected: FailureKind) {
    match value { Err(Failure::Decode { kind, .. }) => assert_eq!(kind, expected), Err(Failure::Native(cause)) => panic!("Unexpected native failure: {cause}"), Ok(_) => panic!("Expected a decode failure") }
}
fn rules<'a>(value: &'a beans::SchemaString) -> &'a beans::Rule { value.rule_set.as_ref().unwrap().domain_rules.as_ref().unwrap().as_slice()[0].as_ref().unwrap() }

#[test]
fn schema_string_uses_default_setters_and_a_string_creator_while_schema_uses_its_object_creator() {
    for profile in PROFILES { run(|context| {
        assert!(schema_string(b"null", profile, context).unwrap().is_none());
        text(&schema_string(br#""text""#, profile, context).unwrap().unwrap().schema, "text");
        failed(schema(br#""text""#, profile, context), FailureKind::Coercion);
        let first = schema_string(b"{}", profile, context).unwrap().unwrap();
        text(&first.schema_type, "AVRO");
        assert_eq!(first.references.unwrap().len(), 0);
        assert!(first.schema.is_none());
    }); }
}

#[test]
fn explicit_nulls_preserve_setter_values_and_creator_normalization_only_runs_once() {
    for profile in PROFILES { run(|context| {
        let first = schema_string(br#"{"schemaType":null,"references":null}"#, profile, context).unwrap().unwrap();
        assert!(first.schema_type.is_none() && first.references.is_none());
        let second = schema(br#"{"schemaType":null,"references":null}"#, profile, context).unwrap().unwrap();
        text(&second.schema_type, "AVRO");
        assert_eq!(second.references.unwrap().len(), 0);
        let last = schema(br#"{"subject":null,"version":null,"id":null,"schemaType":null,"references":null,"metadata":null,"ruleset":null,"schema":null,"schemaTags":null,"schemaType":null,"references":null}"#, profile, context).unwrap().unwrap();
        assert!(last.schema_type.is_none() && last.references.is_none());
    }); }
}

#[test]
fn duplicate_invalid_unused_ids_fail_at_their_first_typed_occurrence() {
    for profile in PROFILES { run(|context| {
        failed(schema(br#"{"id":"bad","id":1,"schema":"x"}"#, profile, context), FailureKind::Coercion);
        failed(schema_string(br#"{"maxId":{},"maxId":1}"#, profile, context), FailureKind::Coercion);
        let value = schema_string(br#"{"$serde_json::private::RawValue":"?","schema":"x"}"#, profile, context).unwrap().unwrap();
        text(&value.schema, "x");
    }); }
}

#[test]
fn strings_keep_numeric_token_spelling_and_integer_coercion_uses_java_units() {
    for profile in PROFILES { run(|context| {
        for (bytes, expected) in [(br#"{"schema":1.20e+3}"#.as_slice(), "1.20e+3"), (br#"{"schema":+INF}"#, "+INF"), (br#"{"schema":true}"#, "true")] {
            text(&schema_string(bytes, profile, context).unwrap().unwrap().schema, expected);
        }
        assert_eq!(schema(br#"{"version":" \u0661\u0662 ","id":NaN}"#, profile, context).unwrap().unwrap().version, Some(12));
        assert_eq!(schema(br#"{"version":NaN}"#, profile, context).unwrap().unwrap().version, Some(0));
        assert_eq!(schema(br#"{"version":-1.9}"#, profile, context).unwrap().unwrap().version, Some(-1));
        for bytes in [br#"{"version":Infinity}"#.as_slice(), br#"{"version":2147483647.5}"#, br#"{"version":"\uD835\uDFD9"}"#] { failed(schema(bytes, profile, context), FailureKind::Coercion); }
        for bytes in [br#"{"version":"null"}"#.as_slice(), br#"{"version":" "}"#] { assert!(schema(bytes, profile, context).unwrap().unwrap().version.is_none()); }
    }); }
}

#[test]
fn rule_enums_accept_ordinals_exact_names_and_creator_null_defaults() {
    for profile in PROFILES { run(|context| {
        let value = schema_string(br#"{"ruleSet":{"domainRules":[{"kind":"1","mode":3}]}}"#, profile, context).unwrap().unwrap();
        assert_eq!(rules(&value).kind, Some(beans::RuleKind::Condition));
        assert_eq!(rules(&value).mode, Some(beans::RuleMode::Write));
        let value = schema_string(br#"{"ruleSet":{"domainRules":[{"kind":null,"mode":null}]}}"#, profile, context).unwrap().unwrap();
        assert_eq!(rules(&value).kind, Some(beans::RuleKind::Transform));
        assert_eq!(rules(&value).mode, Some(beans::RuleMode::WriteRead));
        for bytes in [br#"{"ruleSet":{"domainRules":[{"kind":"transform"}]}}"#.as_slice(), br#"{"ruleSet":{"domainRules":[{"mode":""}]}}"#, br#"{"ruleSet":{"domainRules":[{"kind":1.0}]}}"#] { failed(schema_string(bytes, profile, context), FailureKind::Coercion); }
    }); }
}

#[test]
fn primitive_rule_boolean_uses_integer_and_restricted_string_coercion() {
    for profile in PROFILES { run(|context| {
        for (bytes, expected) in [(br#"{"ruleSet":{"domainRules":[{"disabled":999999999999999999999}]}}"#.as_slice(), true), (br#"{"ruleSet":{"domainRules":[{"disabled":"TRUE"}]}}"#, true), (br#"{"ruleSet":{"domainRules":[{"disabled":-0}]}}"#, false), (br#"{"ruleSet":{"domainRules":[{"disabled":"null"}]}}"#, false)] {
            let value = schema_string(bytes, profile, context).unwrap().unwrap();
            assert_eq!(rules(&value).disabled, expected);
        }
        for bytes in [br#"{"ruleSet":{"domainRules":[{"disabled":1.0}]}}"#.as_slice(), br#"{"ruleSet":{"domainRules":[{"disabled":"tRuE"}]}}"#] { failed(schema_string(bytes, profile, context), FailureKind::Coercion); }
    }); }
}

#[test]
fn pending_regular_rule_set_setters_replay_in_reverse_after_the_schema_creator() {
    for profile in PROFILES { run(|context| {
        let value = schema(br#"{"ruleSet":{"domainRules":[{"name":"first"}]},"ruleSet":{"domainRules":[{"name":"second"}]},"ruleset":{"domainRules":[{"name":"creator"}]}}"#, profile, context).unwrap().unwrap();
        let rule = value.rule_set.unwrap().domain_rules.unwrap().pop().unwrap().unwrap();
        text(&rule.name, "first");
        let value = schema(br#"{"subject":null,"version":null,"id":null,"schemaType":null,"references":null,"metadata":null,"ruleset":null,"schema":null,"schemaTags":null,"ruleSet":{"domainRules":[{"name":"later"}]}}"#, profile, context).unwrap().unwrap();
        text(&value.rule_set.unwrap().domain_rules.unwrap().pop().unwrap().unwrap().name, "later");
    }); }
}

#[test]
fn all_creator_arguments_advance_the_parser_before_running_a_failing_entity_constructor() {
    for profile in PROFILES { run(|context| {
        failed(schema_string(br#"{"schemaTags":[{"schemaEntity":{"entityPath":null,"entityType":null,"later":?}}]}"#, profile, context), FailureKind::Syntax);
        failed(schema_string(br#"{"schemaTags":[{"schemaEntity":{"entityPath":null,"entityType":null,"later":"unterminated}}]}"#, profile, context), FailureKind::Constructor);
        failed(schema_string(br#"{"schemaTags":[{"schemaEntity":{"entityPath":null,"later":?}}]}"#, profile, context), FailureKind::Syntax);
    }); }
}

#[test]
fn the_schema_creator_only_ruleset_property_checks_its_missing_setter_before_lazy_text() {
    for profile in PROFILES { run(|context| {
        failed(schema(br#"{"subject":null,"version":null,"id":null,"schemaType":null,"references":null,"metadata":null,"ruleset":null,"schema":null,"schemaTags":null,"ruleset":"unterminated}"#, profile, context), FailureKind::MissingSetter);
        failed(schema(br#"{"subject":null,"version":null,"id":null,"schemaType":null,"references":null,"metadata":null,"ruleset":null,"schema":null,"schemaTags":null,"ruleset":?}"#, profile, context), FailureKind::Syntax);
    }); }
}

#[test]
fn metadata_constructor_failures_wait_for_creator_fields_and_then_sort_all_collections() {
    for profile in PROFILES { run(|context| {
        failed(schema_string(br#"{"metadata":{"tags":{"x":null},"properties":{"bad":{}},"sensitive":[]}}"#, profile, context), FailureKind::Coercion);
        failed(schema_string(br#"{"metadata":{"tags":{"x":null},"properties":{},"sensitive":[],"ignored":"unterminated}}"#, profile, context), FailureKind::Constructor);
        failed(schema_string(br#"{"metadata":{"sensitive":[null]}}"#, profile, context), FailureKind::Constructor);
        let value = schema_string(br#"{"metadata":{"tags":{"b":["z","a","a"]},"properties":{"z":false,"a":1},"sensitive":["z","a"]}}"#, profile, context).unwrap().unwrap().metadata.unwrap();
        assert!(value.tags.as_ref().unwrap().sorted && value.properties.as_ref().unwrap().sorted && value.sensitive.as_ref().unwrap().sorted);
        text(&value.tags.unwrap().values.as_slice()[0].1.as_ref().unwrap().values.as_slice()[0], "a");
        assert_eq!(value.properties.unwrap().values.as_slice()[0].0.view().units().collect::<Vec<_>>(), vec![0x61]);
    }); }
}

#[test]
fn metadata_post_creator_reflection_keeps_null_and_rejects_creator_map_or_set_classes() {
    for profile in PROFILES { run(|context| {
        let value = schema_string(br#"{"metadata":{"tags":null,"properties":null,"sensitive":null,"tags":null,"properties":null,"sensitive":null}}"#, profile, context).unwrap().unwrap().metadata.unwrap();
        assert!(value.tags.is_none() && value.properties.is_none() && value.sensitive.is_none());
        failed(schema_string(br#"{"metadata":{"tags":null,"properties":null,"sensitive":null,"properties":{}}}"#, profile, context), FailureKind::Coercion);
        failed(schema_string(br#"{"metadata":{"tags":null,"properties":null,"sensitive":null,"sensitive":[null]}}"#, profile, context), FailureKind::Coercion);
        failed(schema_string(br#"{"metadata":{"tags":null,"properties":null,"sensitive":null,"properties":{"bad":?}}}"#, profile, context), FailureKind::Syntax);
    }); }
}

#[test]
fn rule_post_creator_reflection_uses_creator_collection_types_without_renormalizing() {
    for profile in PROFILES { run(|context| {
        failed(schema_string(br#"{"ruleSet":{"domainRules":[{"name":null,"doc":null,"kind":null,"mode":null,"type":null,"tags":null,"params":null,"expr":null,"onSuccess":null,"onFailure":null,"disabled":null,"tags":[]}]}}"#, profile, context), FailureKind::Coercion);
        let value = schema_string(br#"{"ruleSet":{"domainRules":[{"name":null,"doc":null,"kind":null,"mode":null,"type":null,"tags":null,"params":null,"expr":null,"onSuccess":null,"onFailure":null,"disabled":null,"tags":null,"kind":null}]}}"#, profile, context).unwrap().unwrap();
        assert!(rules(&value).tags.is_none() && rules(&value).kind.is_none());
    }); }
}

#[test]
fn entity_normalized_path_keeps_the_creator_generation_after_reflected_path_changes() {
    for profile in PROFILES { run(|context| {
        let value = schema_string(br#"{"schemaTags":[{"schemaEntity":{"entityPath":".first","entityType":"sr_record","entityPath":"second","entityType":1}}]}"#, profile, context).unwrap().unwrap();
        let entity = value.schema_tags.as_ref().unwrap().as_slice()[0].as_ref().unwrap().entity.as_ref().unwrap();
        text(&entity.path, "second");
        assert_eq!(entity.normalized_path.view().units().collect::<Vec<_>>(), "first".encode_utf16().collect::<Vec<_>>());
        assert_eq!(entity.entity_type, Some(beans::EntityType::Field));
    }); }
}

#[test]
fn nullable_list_entries_and_duplicate_map_values_remain_distinct_from_empty_lists() {
    for profile in PROFILES { run(|context| {
        let value = schema_string(br#"{"references":[null,{}],"schemaTags":[null,{"tags":[null,true]}],"ruleSet":{"domainRules":[null]},"metadata":{"properties":{"key":"first","key":null}}}"#, profile, context).unwrap().unwrap();
        assert!(value.references.as_ref().unwrap().as_slice()[0].is_none());
        assert!(value.references.as_ref().unwrap().as_slice()[1].as_ref().unwrap().name.is_none());
        assert!(value.schema_tags.as_ref().unwrap().as_slice()[0].is_none());
        assert!(value.rule_set.as_ref().unwrap().domain_rules.as_ref().unwrap().as_slice()[0].is_none());
        let properties = value.metadata.unwrap().properties.unwrap();
        assert_eq!(properties.values.len(), 1);
        assert!(properties.values.as_slice()[0].1.is_none());
    }); }
}

#[test]
fn escaped_surrogate_values_preserve_java_units_and_field_name_rules_are_profile_specific() {
    run(|context| {
        for profile in PROFILES {
            assert_eq!(units(&schema_string(br#"{"schema":"\uD800"}"#, profile, context).unwrap().unwrap().schema), Some(vec![0xd800]));
        }
        assert!(schema_string(br#"{"\uD800":null}"#, Profile::Jackson216, context).is_ok());
        failed(schema_string(br#"{"\uD800":null}"#, Profile::Jackson221, context), FailureKind::Syntax);
        assert!(schema_string(br#"{"\uD83D\uDE00":null}"#, Profile::Jackson221, context).is_ok());
    });
}

#[test]
fn lazy_unknown_text_skips_raw_utf8_surrogates_that_the_newer_text_decoder_rejects() {
    run(|context| {
        let ignored = b"{\"ignored\":\"\xed\xa0\x80\",\"schema\":\"ok\"}";
        let demanded = b"{\"schema\":\"\xed\xa0\x80\"}";
        assert!(schema_string(ignored, Profile::Jackson221, context).is_ok());
        failed(schema_string(demanded, Profile::Jackson221, context), FailureKind::Syntax);
        assert_eq!(units(&schema_string(demanded, Profile::Jackson216, context).unwrap().unwrap().schema), Some(vec![0xd800]));
    });
}

#[test]
fn utf16_decoder_replaces_the_complete_malformed_high_surrogate_group() {
    for profile in PROFILES { run(|context| {
        let mut bytes = vec![0xfe, 0xff];
        for unit in [0x22u16, 0xd800, 0x78, 0x22] { bytes.extend_from_slice(&unit.to_be_bytes()); }
        assert_eq!(units(&schema_string(&bytes, profile, context).unwrap().unwrap().schema), Some(vec![0xfffd]));
    }); }
}

#[test]
fn root_literal_suffixes_use_actual_utf8_profile_and_reader_identifier_rules() {
    run(|context| {
        failed(schema_string("null×".as_bytes(), Profile::Jackson216, context), FailureKind::Syntax);
        assert!(schema_string("null×".as_bytes(), Profile::Jackson221, context).unwrap().is_none());
        for profile in PROFILES {
            failed(schema_string("nullé".as_bytes(), profile, context), FailureKind::Syntax);
            let bytes: Vec<_> = [0xfeu8, 0xff].into_iter().chain("null×".encode_utf16().flat_map(u16::to_be_bytes)).collect();
            assert!(schema_string(&bytes, profile, context).unwrap().is_none());
        }
    });
}

#[test]
fn field_names_finish_escaping_and_check_length_before_decoding_utf8_quads() {
    for profile in PROFILES { run(|context| {
        let invalid_escape = b"{\"\xff\\q\":null}";
        match schema_string(invalid_escape, profile, context) {
            Err(Failure::Decode { detail, .. }) => assert_eq!(detail, "Invalid JSON string escape"),
            _ => panic!("Expected the escape failure before field-name UTF8 decoding"),
        }
        let mut bytes = b"{\"".to_vec();
        bytes.extend(std::iter::repeat_n(b'x', 50_000)); bytes.push(0xff); bytes.extend_from_slice(b"\":null}");
        failed(schema_string(&bytes, profile, context), FailureKind::Constraint);
    }); }
}

#[test]
fn unknown_numeric_values_keep_lexical_limits_without_materializing_a_number() {
    for profile in PROFILES { run(|context| {
        let allowed = format!("{{\"ignored\":{},\"schema\":\"ok\"}}", "1".repeat(1000));
        text(&schema_string(allowed.as_bytes(), profile, context).unwrap().unwrap().schema, "ok");
        let rejected = format!("{{\"ignored\":{}}}", "1".repeat(1001));
        failed(schema_string(rejected.as_bytes(), profile, context), FailureKind::Constraint);
        assert!(schema_string(b"{}arbitrary suffix", profile, context).is_ok());
    }); }
}

#[test]
fn native_resource_failures_propagate_and_the_decoded_text_owns_its_lease_after_context_drop() {
    let budget = MemoryBudget::new(1 << 20);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let value = schema_string(br#"{"schema":"owned","references":[{"name":"key"}]}"#, Profile::Jackson216, &mut context).unwrap().unwrap();
    drop(context);
    assert!(budget.used_bytes() > 0);
    text(&value.schema, "owned");
    drop(value);
    assert_eq!(budget.used_bytes(), 0);
    let budget = MemoryBudget::new(0);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    match schema_string(b"{}", Profile::Jackson216, &mut context) {
        Err(Failure::Native(cause)) => assert!(cause.code.ends_with("/batch-too-large")),
        _ => panic!("Expected unchanged native admission failure"),
    }
    drop(context);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn newer_databind_rejects_zero_prefixed_enum_indices_and_overlong_coerced_integer_text() {
    run(|context| {
        let bytes = br#"{"ruleSet":{"domainRules":[{"kind":"01"}]}}"#;
        assert_eq!(rules(&schema_string(bytes, Profile::Jackson216, context).unwrap().unwrap()).kind, Some(beans::RuleKind::Condition));
        failed(schema_string(bytes, Profile::Jackson221, context), FailureKind::Coercion);
        let bytes = format!("{{\"version\":\"{}\"}}", "0".repeat(1001));
        assert_eq!(schema(bytes.as_bytes(), Profile::Jackson216, context).unwrap().unwrap().version, Some(0));
        failed(schema(bytes.as_bytes(), Profile::Jackson221, context), FailureKind::Constraint);
    });
}

#[test]
fn ignored_nested_containers_use_the_same_demanded_depth_guard_as_known_properties() {
    for profile in PROFILES { run(|context| {
        let allowed = format!("{{\"ignored\":{}null{}}}", "[".repeat(999), "]".repeat(999));
        assert!(schema_string(allowed.as_bytes(), profile, context).is_ok());
        let rejected = format!("{{\"ignored\":{}null{}}}", "[".repeat(1000), "]".repeat(1000));
        failed(schema_string(rejected.as_bytes(), profile, context), FailureKind::Constraint);
        for bytes in [br#"{"schemaTags":[{"schemaEntity":{"entityPath":"x","entityType":"bad"}}]}"#.as_slice(), br#"{"metadata":{"sensitive":{}}}"#, br#"{"references":[{"version":"bad"}]}"#] {
            failed(schema_string(bytes, profile, context), FailureKind::Coercion);
        }
    }); }
}

#[test]
fn overlong_four_byte_utf8_uses_value_or_and_name_addition_without_changing_lazy_skip() {
    for profile in PROFILES { run(|context| {
        let value = schema_string(b"\"\xf0\x80\x80\x80\"", profile, context).unwrap().unwrap();
        assert_eq!(units(&value.schema), Some(vec![0xffc0, 0xdc00]));
        let value = schema_string(b"{\"schema\":\"\xf0\x8f\xbf\xbf\"}", profile, context).unwrap().unwrap();
        assert_eq!(units(&value.schema), Some(vec![0xffff, 0xdfff]));
        let value = schema_string(b"{\"metadata\":{\"properties\":{\"\xf0\x80\x80\x80\":\"\xf0\x80\x80\x80\"}}}", profile, context).unwrap().unwrap();
        let properties = value.metadata.as_ref().unwrap().properties.as_ref().unwrap().values.as_slice();
        assert_eq!(properties[0].0.view().units().collect::<Vec<_>>(), vec![0xd7c0, 0xdc00]);
        assert_eq!(units(&properties[0].1), Some(vec![0xffc0, 0xdc00]));
        let value = schema_string(br#""\uD7C0\uDC00""#, profile, context).unwrap().unwrap();
        assert_eq!(units(&value.schema), Some(vec![0xd7c0, 0xdc00]));
        let value = schema_string(b"{\"ignored\":\"\xf0\x80\x80\x80\",\"schema\":\"ok\"}", profile, context).unwrap().unwrap();
        text(&value.schema, "ok");
    }); }
}

#[test]
fn observed_text_segments_preserve_malformed_escape_final_contents_and_eof_order() {
    let observations = [TextBufferObservation { value_start: 0, capacity_units: 200 }];
    let mut bytes = Vec::with_capacity(20_051_120);
    bytes.push(b'"');
    bytes.resize(20_000_002, b'a');
    bytes.extend_from_slice(b"\\q");
    for profile in PROFILES { run(|context| {
        failed(schema_string_with_text_buffers(&bytes, profile, &observations, context), FailureKind::Syntax);
        let reused = [TextBufferObservation { value_start: 0, capacity_units: 65_536 }];
        failed(schema_string_with_text_buffers(&bytes, profile, &reused, context), FailureKind::Syntax);
        match schema_string(&bytes, profile, context) {
            Err(Failure::Native(cause)) => assert!(cause.code.ends_with("/unsupported-source-profile")),
            _ => panic!("Expected the missing text-buffer observation"),
        }
    }); }
    bytes.truncate(20_000_002);
    bytes.push(b'"');
    for profile in PROFILES { run(|context| {
        failed(schema_string_with_text_buffers(&bytes, profile, &observations, context), FailureKind::Constraint);
    }); }
    bytes.pop();
    bytes.resize(20_051_113, b'a');
    for profile in PROFILES { run(|context| {
        failed(schema_string_with_text_buffers(&bytes, profile, &observations, context), FailureKind::Syntax);
    }); }
    bytes.extend_from_slice(b"\\q");
    for profile in PROFILES { run(|context| {
        failed(schema_string_with_text_buffers(&bytes, profile, &observations, context), FailureKind::Constraint);
    }); }
    bytes.truncate(20_051_112);
    bytes.extend_from_slice(&[0xf0, 0x90, 0x80, 0x80]);
    for profile in PROFILES { run(|context| {
        failed(schema_string_with_text_buffers(&bytes, profile, &observations, context), FailureKind::Constraint);
    }); }
    *bytes.last_mut().unwrap() = 0xff;
    for profile in PROFILES { run(|context| {
        failed(schema_string_with_text_buffers(&bytes, profile, &observations, context), FailureKind::Syntax);
    }); }
}

#[test]
fn text_capacity_observations_are_per_value_byte_offsets_and_only_cover_utf8() {
    for profile in PROFILES { run(|context| {
        let observations = [TextBufferObservation { value_start: 10, capacity_units: 0 }];
        match schema_with_text_buffers(br#"{"schema":"x"}"#, profile, &observations, context) {
            Err(Failure::Native(cause)) => assert!(cause.code.ends_with("/unsupported-source-profile")),
            _ => panic!("Expected invalid observed capacity to fail at its demanded value"),
        }
        let observations = [TextBufferObservation { value_start: 11, capacity_units: 0 }];
        text(&schema_with_text_buffers(br#"{"schema":"x"}"#, profile, &observations, context).unwrap().unwrap().schema, "x");
        let bytes = [0xfe, 0xff, 0, b'"', 0, b'o', 0, b'k', 0, b'"'];
        let observations = [TextBufferObservation { value_start: 2, capacity_units: 0 }];
        text(&schema_string_with_text_buffers(&bytes, profile, &observations, context).unwrap().unwrap().schema, "ok");
    }); }
}

#[test]
fn lazy_ignored_strings_do_not_demand_text_buffer_capacity_or_length_validation() {
    let mut bytes = Vec::with_capacity(20_000_040);
    bytes.extend_from_slice(b"{\"ignored\":\"");
    bytes.resize(bytes.len() + 20_000_001, b'a');
    bytes.extend_from_slice(b"\",\"schema\":\"ok\"}");
    for profile in PROFILES { run(|context| {
        text(&schema_string(&bytes, profile, context).unwrap().unwrap().schema, "ok");
    }); }
}
