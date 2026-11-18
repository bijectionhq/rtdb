use super::*;
use std::sync::Arc;
use rtdb_types::MemoryBudget;
use crate::connect::{ConversionContext, registry::response::{self, Profile}};

fn with_schema(bytes: &[u8], mut inspect: impl FnMut(RuleSchema<'_>, &mut ConversionContext)) {
    for profile in [Profile::Jackson216, Profile::Jackson221] {
        let memory = MemoryBudget::new(1 << 26);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        {
            let bean = response::schema_string(bytes, profile, &mut context).unwrap().unwrap();
            inspect(RuleSchema::new(bean.rule_set.as_ref(), bean.metadata.as_ref()), &mut context);
        }
        drop(context);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Event { Disabled(Option<Vec<u16>>), Header }
struct Decisions<'a> { events: Vec<Event>, disabled: Option<bool>, header: FailedRuleHeader<'a>, fail_header: bool, fail_disabled: bool }
impl Default for Decisions<'_> {
    fn default() -> Self { Self { events: Vec::new(), disabled: None, header: FailedRuleHeader::Absent, fail_header: false, fail_disabled: false } }
}
impl RuleDecisions for Decisions<'_> {
    fn disabled(&mut self, rule: RuleView<'_>) -> NativeResult<bool> {
        self.events.push(Event::Disabled(rule.name().map(|text| text.units().collect())));
        if self.fail_disabled { return Err(Anomaly::incorrect("fixture/disabled", "Disabled capability denied")); }
        Ok(self.disabled.unwrap_or(rule.rule.disabled))
    }
    fn failed_rule_header(&mut self) -> NativeResult<FailedRuleHeader<'_>> {
        self.events.push(Event::Header);
        if self.fail_header { return Err(Anomaly::incorrect("fixture/header", "Header capability denied")); }
        Ok(self.header)
    }
}
fn next_rule<'a>(step: RuleStep<'a>) -> (RuleView<'a>, RuleWalk<'a>) {
    match step { RuleStep::Rule { rule, remaining } => (rule, remaining), RuleStep::Done => panic!("expected a demanded rule") }
}
fn unchecked<T>(result: Result<T>, expected: Unchecked) {
    match result { Err(Failure::Unchecked(actual)) => assert_eq!(actual, expected), Err(Failure::Native(cause)) => panic!("unexpected native failure: {cause}"), Ok(_) => panic!("expected unchecked source failure") }
}
fn text(actual: Option<JavaText<'_>>, expected: Option<&str>) {
    assert_eq!(actual.map(|text| text.units().collect::<Vec<_>>()), expected.map(|text| text.encode_utf16().collect::<Vec<_>>()));
}

#[test]
fn membership_short_circuits_nullable_tails_and_selects_each_modes_actual_list() {
    with_schema(br#"{"ruleSet":{"domainRules":[{"mode":"READ","disabled":true},null],"migrationRules":[{"mode":"UPDOWN"},null]}}"#, |schema, _| {
        let rules = RuleSetView::new(schema.rules.unwrap());
        for mode in [RuleMode::Upgrade, RuleMode::Downgrade, RuleMode::UpDown, RuleMode::Read] { assert!(rules.has_rules(mode).unwrap()); }
        for mode in [RuleMode::Write, RuleMode::WriteRead] { unchecked(rules.has_rules(mode), Unchecked::NullRule); }
    });
    with_schema(br#"{"ruleSet":{"domainRules":[null,{"mode":"READ"}]}}"#, |schema, _| {
        unchecked(RuleSetView::new(schema.rules.unwrap()).has_rules(RuleMode::Read), Unchecked::NullRule);
    });
}

#[test]
fn post_creator_null_mode_is_not_defaulted_again_and_disabled_and_header_are_still_demanded() {
    with_schema(br#"{"ruleSet":{"domainRules":[{"name":"mode","doc":null,"kind":null,"mode":"READ","type":null,"tags":null,"params":null,"expr":null,"onSuccess":null,"onFailure":null,"disabled":false,"mode":null}]}}"#, |schema, _| {
        assert!(!RuleSetView::new(schema.rules.unwrap()).has_rules(RuleMode::Read).unwrap());
        let mut decisions = Decisions::default();
        assert!(matches!(RuleWalk::start(Mode::Read, false, None, Some(schema)).unwrap().next(&mut decisions).unwrap(), RuleStep::Done));
        assert_eq!(decisions.events, [Event::Disabled(Some("mode".encode_utf16().collect())), Event::Header]);
    });
}

#[test]
fn post_creator_null_kind_is_only_demanded_after_an_executor_successful_transform() {
    with_schema(br#"{"ruleSet":{"domainRules":[{"name":"kind","doc":null,"kind":null,"mode":"READ","type":"absent","tags":null,"params":null,"expr":null,"onSuccess":null,"onFailure":"NONE","disabled":false,"kind":null}]}}"#, |schema, _| {
        let (rule, remaining) = next_rule(RuleWalk::start(Mode::Read, false, None, Some(schema)).unwrap().next(&mut Decisions::default()).unwrap());
        text(Some(rule.executor_type().unwrap()), Some("absent"));
        text(rule.action_name(rule.on_failure()).unwrap(), Some("NONE"));
        assert!(rule.on_success().is_none());
        unchecked(rule.kind_after_transform(), Unchecked::NullKind);
        assert!(matches!(remaining.next(&mut Decisions::default()).unwrap(), RuleStep::Done));
    });
}

#[test]
fn null_executor_type_is_preserved_until_the_applicable_rules_executor_lookup() {
    with_schema(br#"{"ruleSet":{"domainRules":[{"name":"type","mode":"READ","type":null}]}}"#, |schema, _| {
        let (rule, _) = next_rule(RuleWalk::start(Mode::Read, false, None, Some(schema)).unwrap().next(&mut Decisions::default()).unwrap());
        assert_eq!(rule.mode(), Some(RuleMode::Read));
        unchecked(rule.executor_type(), Unchecked::NullExecutorType);
        assert_eq!(rule.kind_after_transform().unwrap(), RuleKind::Transform);
    });
}

#[test]
fn post_creator_null_lists_fail_at_membership_copy_or_size_demand_after_null_guards() {
    with_schema(br#"{"ruleSet":{"migrationRules":[],"domainRules":[],"domainRules":null,"migrationRules":null}}"#, |schema, _| {
        unchecked(RuleSetView::new(schema.rules.unwrap()).has_rules(RuleMode::Read), Unchecked::NullList(ListDemand::Membership));
        unchecked(RuleWalk::start(Mode::Read, false, None, Some(schema)), Unchecked::NullList(ListDemand::ReadCopy));
        unchecked(RuleWalk::start(Mode::Upgrade, false, None, Some(schema)), Unchecked::NullList(ListDemand::UpgradeSize));
        unchecked(RuleWalk::start(Mode::Downgrade, false, Some(schema), Some(schema)), Unchecked::NullList(ListDemand::DowngradeCopy));
        unchecked(RuleWalk::start(Mode::Downgrade, false, None, Some(schema)), Unchecked::MissingSource);
        for mode in [Mode::Read, Mode::Upgrade, Mode::Downgrade] {
            assert!(matches!(RuleWalk::start(mode, true, None, Some(schema)).unwrap().next(&mut Decisions::default()).unwrap(), RuleStep::Done));
            assert!(matches!(RuleWalk::start(mode, false, Some(schema), None).unwrap().next(&mut Decisions::default()).unwrap(), RuleStep::Done));
        }
    });
}

#[test]
fn read_and_downgrade_reverse_the_selected_list_but_upgrade_keeps_encounter_order() {
    with_schema(br#"{"ruleSet":{"domainRules":[{"name":"domain-first","mode":"READ"},{"name":"domain-last","mode":"READ"}],"migrationRules":[{"name":"migration-first","mode":"UPDOWN"},{"name":"migration-last","mode":"UPDOWN"}]}}"#, |schema, _| {
        for (mode, names) in [(Mode::Read, ["domain-last", "domain-first"]), (Mode::Upgrade, ["migration-first", "migration-last"]), (Mode::Downgrade, ["migration-last", "migration-first"])] {
            let target = if mode == Mode::Downgrade { RuleSchema::new(None, schema.metadata) } else { schema };
            let mut walk = RuleWalk::start(mode, false, Some(schema), Some(target)).unwrap();
            for (index, name) in names.into_iter().enumerate() {
                let (rule, remaining) = next_rule(walk.next(&mut Decisions::default()).unwrap());
                text(rule.name(), Some(name));
                assert_eq!(rule.index(), index);
                walk = remaining;
            }
            assert!(matches!(walk.next(&mut Decisions::default()).unwrap(), RuleStep::Done));
        }
    });
}

#[test]
fn nullable_entries_are_only_unwrapped_when_their_directional_slot_is_reached() {
    with_schema(br#"{"ruleSet":{"domainRules":[null,{"name":"read","mode":"READ"}],"migrationRules":[{"name":"upgrade","mode":"UPGRADE"},null]}}"#, |schema, _| {
        for (mode, expected) in [(Mode::Read, "read"), (Mode::Upgrade, "upgrade")] {
            let mut decisions = Decisions::default();
            let (rule, remaining) = next_rule(RuleWalk::start(mode, false, None, Some(schema)).unwrap().next(&mut decisions).unwrap());
            text(rule.name(), Some(expected));
            assert_eq!(decisions.events.len(), 2);
            unchecked(remaining.next(&mut decisions), Unchecked::NullRule);
            assert_eq!(decisions.events.len(), 2);
        }
    });
    with_schema(br#"{"ruleSet":{"domainRules":[{"name":"read","mode":"READ"},null]}}"#, |schema, _| {
        let mut decisions = Decisions::default();
        unchecked(RuleWalk::start(Mode::Read, false, None, Some(schema)).unwrap().next(&mut decisions), Unchecked::NullRule);
        assert!(decisions.events.is_empty());
    });
}

#[test]
fn disabled_decisions_precede_headers_and_headers_precede_mode_selection() {
    with_schema(br#"{"ruleSet":{"domainRules":[{"mode":"WRITE"}]}}"#, |schema, _| {
        let mut decisions = Decisions { disabled: Some(true), header: FailedRuleHeader::NullValue, ..Decisions::default() };
        assert!(matches!(RuleWalk::start(Mode::Read, false, None, Some(schema)).unwrap().next(&mut decisions).unwrap(), RuleStep::Done));
        assert_eq!(decisions.events, [Event::Disabled(None)]);
        let mut decisions = Decisions::default();
        assert!(matches!(RuleWalk::start(Mode::Read, false, None, Some(schema)).unwrap().next(&mut decisions).unwrap(), RuleStep::Done));
        assert_eq!(decisions.events, [Event::Disabled(None), Event::Header]);
        let mut decisions = Decisions { header: FailedRuleHeader::Name(JavaText::from_utf8("failed")), ..Decisions::default() };
        unchecked(RuleWalk::start(Mode::Read, false, None, Some(schema)).unwrap().next(&mut decisions), Unchecked::NullRuleName);
        let mut decisions = Decisions { header: FailedRuleHeader::NullValue, ..Decisions::default() };
        unchecked(RuleWalk::start(Mode::Read, false, None, Some(schema)).unwrap().next(&mut decisions), Unchecked::NullHeaderValue);
    });
}

#[test]
fn a_matching_failed_rule_header_skips_before_executor_type_and_kind_demand() {
    with_schema(br#"{"ruleSet":{"domainRules":[{"name":"\uD83D\uDE00","mode":"READ","type":null}]}}"#, |schema, _| {
        let units = [0xd83d, 0xde00];
        let mut decisions = Decisions { header: FailedRuleHeader::Name(JavaText::from_utf16(&units)), ..Decisions::default() };
        assert!(matches!(RuleWalk::start(Mode::Read, false, None, Some(schema)).unwrap().next(&mut decisions).unwrap(), RuleStep::Done));
        assert_eq!(decisions.events, [Event::Disabled(Some(units.to_vec())), Event::Header]);
    });
}

#[test]
fn native_decision_failures_propagate_at_their_demand_stage_without_continuation() {
    with_schema(br#"{"ruleSet":{"domainRules":[{"name":"rule","mode":"READ"}]}}"#, |schema, _| {
        for fail_header in [false, true] {
            let mut decisions = Decisions { fail_disabled: !fail_header, fail_header, ..Decisions::default() };
            match RuleWalk::start(Mode::Read, false, None, Some(schema)).unwrap().next(&mut decisions) {
                Err(Failure::Native(cause)) => assert_eq!(cause, Anomaly::incorrect(if fail_header { "fixture/header" } else { "fixture/disabled" }, if fail_header { "Header capability denied" } else { "Disabled capability denied" })),
                _ => panic!("expected the unchanged capability failure"),
            }
            assert_eq!(decisions.events.len(), if fail_header { 2 } else { 1 });
        }
    });
}

#[test]
fn rule_parameters_fall_back_to_target_metadata_only_for_missing_or_null_values() {
    with_schema(br#"{"metadata":{"properties":{"missing":"metadata","null":"metadata-null","empty":"metadata-empty","exact":"metadata-exact"}},"ruleSet":{"domainRules":[{"mode":"READ","params":{"null":null,"empty":"","exact":"\uD800"}}]}}"#, |schema, _| {
        let (rule, _) = next_rule(RuleWalk::start(Mode::Read, false, None, Some(schema)).unwrap().next(&mut Decisions::default()).unwrap());
        text(rule.parameter(JavaText::from_utf8("missing")), Some("metadata"));
        text(rule.parameter(JavaText::from_utf8("null")), Some("metadata-null"));
        text(rule.parameter(JavaText::from_utf8("empty")), Some(""));
        assert_eq!(rule.parameter(JavaText::from_utf8("exact")).unwrap().units().collect::<Vec<_>>(), [0xd800]);
        assert!(rule.parameter(JavaText::from_utf8("absent")).is_none());
        text(MetadataView::new(schema.metadata.unwrap()).property(JavaText::from_utf8("exact")), Some("metadata-exact"));
    });
    with_schema(br#"{"metadata":{"tags":null,"properties":null,"sensitive":null,"properties":null},"ruleSet":{"domainRules":[{}]}}"#, |schema, _| {
        let (rule, _) = next_rule(RuleWalk::start(Mode::Read, false, None, Some(schema)).unwrap().next(&mut Decisions::default()).unwrap());
        assert!(rule.parameter(JavaText::from_utf8("missing")).is_none());
    });
}

#[test]
fn downgrade_parameters_use_the_target_metadata_with_source_migration_rules() {
    for profile in [Profile::Jackson216, Profile::Jackson221] {
        let memory = MemoryBudget::new(1 << 26);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        {
            let source_bean = response::schema_string(br#"{"metadata":{"properties":{"where":"source"}},"ruleSet":{"migrationRules":[{"name":"source-rule","mode":"DOWNGRADE"}]}}"#, profile, &mut context).unwrap().unwrap();
            let target_bean = response::schema_string(br#"{"metadata":{"properties":{"where":"target"}},"ruleSet":{"domainRules":[{"name":"target-rule"}]}}"#, profile, &mut context).unwrap().unwrap();
            let source = RuleSchema::new(source_bean.rule_set.as_ref(), source_bean.metadata.as_ref());
            let target = RuleSchema::new(target_bean.rule_set.as_ref(), target_bean.metadata.as_ref());
            let (rule, _) = next_rule(RuleWalk::start(Mode::Downgrade, false, Some(source), Some(target)).unwrap().next(&mut Decisions::default()).unwrap());
            text(rule.name(), Some("source-rule"));
            text(rule.parameter(JavaText::from_utf8("where")), Some("target"));
        }
        drop(context);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn action_pairs_preserve_java_trailing_empty_removal_and_nullable_actions() {
    with_schema(br#"{"ruleSet":{"domainRules":[{"mode":"WRITEREAD"}],"migrationRules":[{"mode":"UPDOWN"}]}}"#, |schema, _| {
        for mode in [Mode::Read, Mode::Upgrade, Mode::Downgrade] {
            let (rule, _) = next_rule(RuleWalk::start(mode, false, Some(schema), Some(schema)).unwrap().next(&mut Decisions::default()).unwrap());
            assert!(rule.action_name(None).unwrap().is_none());
            text(rule.action_name(Some(JavaText::from_utf8(""))).unwrap(), Some(""));
            text(rule.action_name(Some(JavaText::from_utf8("NONE,ERROR,ignored"))).unwrap(), Some(if mode == Mode::Upgrade { "NONE" } else { "ERROR" }));
            text(rule.action_name(Some(JavaText::from_utf8(",NONE"))).unwrap(), Some(if mode == Mode::Upgrade { "" } else { "NONE" }));
            text(rule.action_name(Some(JavaText::from_utf8("NONE,,ERROR"))).unwrap(), Some(if mode == Mode::Upgrade { "NONE" } else { "" }));
            for value in [",", ",,"] { unchecked(rule.action_name(Some(JavaText::from_utf8(value))), Unchecked::ActionIndex(usize::from(mode != Mode::Upgrade))); }
            if mode == Mode::Upgrade { text(rule.action_name(Some(JavaText::from_utf8("NONE,"))).unwrap(), Some("NONE")); }
            else { unchecked(rule.action_name(Some(JavaText::from_utf8("NONE,"))), Unchecked::ActionIndex(1)); }
        }
    });
    with_schema(br#"{"ruleSet":{"domainRules":[{"mode":"READ","onFailure":"NONE,ERROR"}]}}"#, |schema, _| {
        let (rule, _) = next_rule(RuleWalk::start(Mode::Read, false, None, Some(schema)).unwrap().next(&mut Decisions::default()).unwrap());
        text(rule.action_name(rule.on_failure()).unwrap(), Some("NONE,ERROR"));
    });
}

#[test]
fn borrowed_selection_requires_no_additional_admission_and_keeps_its_owners_external() {
    with_schema(br#"{"metadata":{"properties":{"dlq.topic":"destination"}},"ruleSet":{"domainRules":[{"name":"borrowed","mode":"READ","type":"absent","onFailure":"NONE"}]}}"#, |schema, context| {
        let remaining = context.allocation_remaining();
        let _exhausted = context.reserve(remaining).unwrap();
        let (rule, _) = next_rule(RuleWalk::start(Mode::Read, false, None, Some(schema)).unwrap().next(&mut Decisions::default()).unwrap());
        text(rule.name(), Some("borrowed"));
        text(rule.parameter(JavaText::from_utf8("dlq.topic")), Some("destination"));
        text(rule.action_name(rule.on_failure()).unwrap(), Some("NONE"));
        assert_eq!(context.allocation_remaining(), 0);
    });
}
