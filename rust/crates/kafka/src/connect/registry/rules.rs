use std::collections::{BTreeMap, BTreeSet};
use rtdb_types::Result;
use serde::Deserialize;
use serde_json::Value as Json;
use super::super::error;

#[derive(Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
enum RuleMode { Upgrade, Downgrade, Updown, Write, Read, #[default] Writeread }
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum RuleKind { #[default] Transform, Condition }
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Rule {
    name: Option<String>,
    #[serde(rename = "type")]
    executor: Option<String>,
    mode: Option<RuleMode>,
    #[serde(rename = "kind")]
    _kind: Option<RuleKind>,
    disabled: Option<bool>,
    on_failure: Option<String>,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuleSet {
    domain_rules: Option<Vec<Rule>>,
    #[serde(rename = "migrationRules")]
    _migration_rules: Option<Vec<Rule>>,
}
pub(super) struct ReadRules { failures: Vec<RuleFailure> }
enum RuleFailure { Error { name: String, executor: String }, MissingAction(String), MissingExecutorType, InvalidActionPair, Dlq }

impl ReadRules {
    pub fn allocation_bound(value: Option<&Json>, options: &BTreeMap<String, String>) -> Result<usize> {
        let overflow = || error("allocation-overflow", "Registry rule configuration exceeds addressable memory");
        let count = value.and_then(|rules| rules.get("domainRules")).and_then(Json::as_array).map_or(0, Vec::len);
        let mut configuration = 0usize;
        let mut failure_bytes = 0usize;
        for (key, value) in options.iter().filter(|(key, _)| key.starts_with("rule.")) {
            configuration = configuration.checked_add(key.len()).and_then(|bytes| bytes.checked_add(value.len())).ok_or_else(overflow)?;
            if key.starts_with("rule.executors.") && key.ends_with(".onFailure") { failure_bytes = failure_bytes.max(value.len()); }
        }
        configuration.checked_mul(8).and_then(|bytes| bytes.checked_add(count.checked_mul(failure_bytes.checked_mul(4)?.checked_add(512)?)?)).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)
    }
    pub fn compile(value: Option<&Json>, options: &BTreeMap<String, String>) -> Result<Self> {
        let rules: RuleSet = value.map(|value| serde_json::from_value(value.clone()).map_err(|cause| error("schema-registry", cause))).transpose()?.unwrap_or_default();
        let registered = configured_actions(options)?;
        let mut failures = Vec::new();
        for rule in rules.domain_rules.unwrap_or_default().into_iter().rev() {
            let name = rule.name.as_deref().unwrap_or("null");
            let executor = rule.executor.as_deref().unwrap_or("null");
            let disabled = override_value(options, name, executor, "disabled").map_or(rule.disabled.unwrap_or(false), |value| value.eq_ignore_ascii_case("true"));
            if disabled { continue; }
            let mode = rule.mode.unwrap_or_default();
            if !matches!(mode, RuleMode::Read | RuleMode::Writeread) { continue; }
            if rule.executor.is_none() { failures.push(RuleFailure::MissingExecutorType); continue; }
            let action = override_value(options, name, executor, "onFailure").or(rule.on_failure.as_deref()).unwrap_or("ERROR");
            let action = if mode == RuleMode::Writeread && action.contains(',') {
                let Some(action) = action.trim_end_matches(',').split(',').nth(1) else { failures.push(RuleFailure::InvalidActionPair); continue; };
                action
            } else { action };
            let resolved = match action {
                "ERROR" | "NONE" => Some(action),
                _ => registered.get(&action.to_uppercase()).map(String::as_str),
            };
            match resolved {
                Some("NONE") => (),
                Some("ERROR") => failures.push(RuleFailure::Error { name: name.into(), executor: executor.into() }),
                Some("DLQ") => failures.push(RuleFailure::Dlq),
                _ => failures.push(RuleFailure::MissingAction(action.into())),
            }
        }
        Ok(Self { failures })
    }
    pub fn check(&self, message_is_null: bool) -> Result<()> {
        if message_is_null { return Ok(()); }
        match self.failures.first() {
            None => Ok(()),
            Some(RuleFailure::Error { name, executor }) => Err(error("conversion-error", format!("Rule failed: {name}; could not find rule executor of type {executor}"))),
            Some(RuleFailure::MissingAction(action)) => Err(error("invalid-plugin-config", format!("Could not find rule action of type {action}"))),
            Some(RuleFailure::MissingExecutorType) => Err(error("conversion-error", "Read rule executor type is null")),
            Some(RuleFailure::InvalidActionPair) => Err(error("conversion-error", "Read rule failure action has no second action")),
            Some(RuleFailure::Dlq) => Err(rtdb_types::Anomaly::unsupported("xtdb.kafka-connect-source/unsupported-registry-rules", "Registry DLQ rules require the native producer action")),
        }
    }
}

fn override_value<'a>(options: &'a BTreeMap<String, String>, name: &str, executor: &str, suffix: &str) -> Option<&'a str> {
    [name.to_owned(), format!("_{executor}_"), "_default_".into()].into_iter().find_map(|name| options.get(&format!("rule.executors.{name}.{suffix}")).map(String::as_str))
}

pub(super) fn validate_plugins(options: &BTreeMap<String, String>) -> Result<()> {
    if let Some(names) = options.get("rule.executors") {
        for name in names.split(',').map(str::trim).filter(|name| !name.is_empty()) {
            if options.contains_key(&format!("rule.executors.{name}.class")) { return Err(error("invalid-plugin-config", "Configured Registry rule executor class requires a native converter adapter")); }
        }
    }
    configured_actions(options)?;
    Ok(())
}

fn configured_actions(options: &BTreeMap<String, String>) -> Result<BTreeSet<String>> {
    let mut types = BTreeSet::new();
    if super::super::converter::option_bool(options, "rule.service.loader.enable", true)? { types.insert("DLQ".into()); }
    if let Some(names) = options.get("rule.actions") {
        for name in names.split(',').map(str::trim).filter(|name| !name.is_empty()) {
            let Some(class) = options.get(&format!("rule.actions.{name}.class")) else { continue; };
            types.insert(match class.as_str() {
                "io.confluent.kafka.schemaregistry.rules.ErrorAction" => "ERROR",
                "io.confluent.kafka.schemaregistry.rules.NoneAction" => "NONE",
                "io.confluent.kafka.schemaregistry.rules.DlqAction" => "DLQ",
                _ => return Err(error("invalid-plugin-config", "Configured Registry rule action class requires a native converter adapter")),
            }.into());
        }
    }
    Ok(types)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn check(rules: Json, options: BTreeMap<String, String>) -> Result<()> { ReadRules::compile(Some(&rules), &options)?.check(false) }

    #[test]
    fn reads_ignore_write_migration_disabled_and_tolerated_rules() {
        let rules = json!({"domainRules":[
            {"name":"write","mode":"WRITE","type":"unknown"},
            {"name":"disabled","disabled":true,"type":"unknown"},
            {"name":"tolerated","mode":"READ","type":"unknown","onFailure":"NONE"},
            {"name":"paired","type":"unknown","onFailure":"ERROR,NONE"}
        ],"migrationRules":[{"name":"migration","mode":"UPGRADE","type":"unknown"}]});
        check(rules, BTreeMap::new()).unwrap();
    }

    #[test]
    fn default_rules_fail_in_reverse_order_and_avro_null_skips_them() {
        let rules = json!({"domainRules":[{"name":"first","type":"one"},{"name":"second","type":"two"}]});
        let plan = ReadRules::compile(Some(&rules), &BTreeMap::new()).unwrap();
        assert!(plan.check(false).unwrap_err().to_string().contains("second"));
        plan.check(true).unwrap();
    }

    #[test]
    fn overrides_use_rule_then_exact_type_then_default() {
        let rules = json!({"domainRules":[{"name":"selected","type":"Mixed","onFailure":"ERROR","disabled":true}]});
        let options = BTreeMap::from([
            ("rule.executors._default_.disabled".into(), "true".into()),
            ("rule.executors._Mixed_.disabled".into(), "true".into()),
            ("rule.executors.selected.disabled".into(), "anything-else".into()),
            ("rule.executors._default_.onFailure".into(), "ERROR".into()),
            ("rule.executors._Mixed_.onFailure".into(), "NONE".into()),
        ]);
        check(rules.clone(), options.clone()).unwrap();
        let mut options = options;
        options.insert("rule.executors.selected.onFailure".into(), "ERROR".into());
        assert!(check(rules, options).is_err());
    }

    #[test]
    fn action_identity_and_pairs_match_registered_classes() {
        let rule = |action| json!({"domainRules":[{"name":"rule","type":"unknown","onFailure":action}]});
        assert!(check(rule("none"), BTreeMap::new()).is_err());
        assert!(check(rule("ERROR,"), BTreeMap::new()).is_err());
        check(rule("ERROR,NONE,ignored"), BTreeMap::new()).unwrap();
        check(rule("none"), BTreeMap::from([("rule.actions".into(), "tolerate".into()), ("rule.actions.tolerate.class".into(), "io.confluent.kafka.schemaregistry.rules.NoneAction".into())])).unwrap();
    }

    #[test]
    fn schema_rule_enums_are_validated_even_when_not_executed() {
        assert!(check(json!({"migrationRules":[{"mode":"future"}]}), BTreeMap::new()).is_err());
        assert!(check(json!({"domainRules":[{"mode":"WRITE","kind":"future"}]}), BTreeMap::new()).is_err());
        check(json!({"domainRules":null,"migrationRules":null}), BTreeMap::new()).unwrap();
    }
}
