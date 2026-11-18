use std::{collections::BTreeMap, sync::Arc};
use rtdb_types::Result;
use serde::Deserialize;
use serde_json::Value as Json;
use super::super::error;

#[cfg(test)]
mod typed;

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
    params: Option<BTreeMap<String, Option<String>>>,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuleSet {
    domain_rules: Option<Vec<Rule>>,
    migration_rules: Option<Vec<Rule>>,
}
pub(super) fn validate(value: &Json) -> Result<()> {
    serde_json::from_value::<RuleSet>(value.clone()).map(|_| ()).map_err(|cause| error("schema-registry", cause))
}
pub(super) struct ReadRules { failures: Vec<RuleFailure> }
enum RuleFailure { Error { name: String, executor: String }, MissingAction(String), MissingExecutorType, InvalidActionPair, Dlq(Arc<DlqRule>) }
#[derive(Debug)]
pub(super) struct DlqAction { pub name: String, pub options: BTreeMap<String, String> }
#[derive(Debug)]
pub(super) struct DlqRule {
    pub name: Option<String>, pub executor: String, pub mode: Mode, pub subject: String,
    pub topic: Option<String>, pub action: Arc<DlqAction>,
}
impl DlqRule {
    pub fn failure(&self) -> rtdb_types::Anomaly { error("conversion-error", format!("Rule failed: {}; could not find rule executor of type {}", self.name.as_deref().unwrap_or("null"), self.executor)) }
}
struct Actions { names: BTreeMap<&'static str, Vec<String>>, dlq: BTreeMap<String, Arc<DlqAction>> }
impl Actions {
    fn selected(&self, kind: &str, subject: &str, name: Option<&str>) -> Option<&str> {
        let names = self.names.get(kind)?;
        let qualified = format!("{subject}:{}", name.unwrap_or("null"));
        names.iter().find(|candidate| **candidate == qualified)
            .or_else(|| name.and_then(|name| names.iter().find(|candidate| candidate.as_str() == name)))
            .or_else(|| names.first()).map(String::as_str)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Mode { Read, Upgrade, Downgrade }
impl Mode {
    pub fn as_str(self) -> &'static str { match self { Self::Read => "READ", Self::Upgrade => "UPGRADE", Self::Downgrade => "DOWNGRADE" } }
    fn accepts(self, mode: RuleMode) -> bool {
        matches!((self, mode), (Self::Read, RuleMode::Read | RuleMode::Writeread) | (Self::Upgrade, RuleMode::Upgrade | RuleMode::Updown) | (Self::Downgrade, RuleMode::Downgrade | RuleMode::Updown))
    }
}

impl ReadRules {
    pub fn allocation_bound(value: Option<&Json>, options: &BTreeMap<String, String>) -> Result<usize> {
        let overflow = || error("allocation-overflow", "Registry rule configuration exceeds addressable memory");
        let count = ["domainRules", "migrationRules"].into_iter().try_fold(0usize, |count, field| count.checked_add(value.and_then(|rules| rules.get(field)).and_then(Json::as_array).map_or(0, Vec::len)).ok_or_else(overflow))?;
        let mut configuration = 0usize;
        let mut failure_bytes = 0usize;
        for (key, value) in options {
            configuration = configuration.checked_add(key.len()).and_then(|bytes| bytes.checked_add(value.len())).ok_or_else(overflow)?;
            if key.starts_with("rule.executors.") && key.ends_with(".onFailure") { failure_bytes = failure_bytes.max(value.len()); }
        }
        let actions = options.get("rule.actions").map_or(0, |names| names.split(',').count()).checked_add(2).ok_or_else(overflow)?;
        configuration.checked_mul(actions).and_then(|bytes| bytes.checked_mul(8)).and_then(|bytes| bytes.checked_add(count.checked_mul(failure_bytes.checked_mul(4)?.checked_add(512)?)?)).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)
    }
    #[cfg(test)]
    pub fn compile(value: Option<&Json>, options: &BTreeMap<String, String>) -> Result<Self> {
        Self::compile_mode(value, options, Mode::Read)
    }
    pub fn has_migration(value: Option<&Json>, mode: Mode) -> Result<bool> {
        let rules: RuleSet = value.map(|value| serde_json::from_value(value.clone()).map_err(|cause| error("schema-registry", cause))).transpose()?.unwrap_or_default();
        Ok(rules.migration_rules.unwrap_or_default().iter().any(|rule| mode.accepts(rule.mode.unwrap_or_default())))
    }
    #[cfg(test)]
    pub fn compile_mode(value: Option<&Json>, options: &BTreeMap<String, String>, execution: Mode) -> Result<Self> { Self::compile_at(value, options, execution, "", None) }
    pub fn compile_at(value: Option<&Json>, options: &BTreeMap<String, String>, execution: Mode, subject: &str, metadata: Option<&Json>) -> Result<Self> {
        let rules: RuleSet = value.map(|value| serde_json::from_value(value.clone()).map_err(|cause| error("schema-registry", cause))).transpose()?.unwrap_or_default();
        let registered = configured_actions(options)?;
        let mut failures = Vec::new();
        let mut rules = if execution == Mode::Read { rules.domain_rules.unwrap_or_default() } else { rules.migration_rules.unwrap_or_default() };
        if execution != Mode::Upgrade { rules.reverse(); }
        for rule in rules {
            let name = rule.name.as_deref().unwrap_or("null");
            let executor = rule.executor.as_deref().unwrap_or("null");
            let disabled = override_value(options, name, executor, "disabled").map_or(rule.disabled.unwrap_or(false), |value| value.eq_ignore_ascii_case("true"));
            if disabled { continue; }
            let mode = rule.mode.unwrap_or_default();
            if !execution.accepts(mode) { continue; }
            if rule.executor.is_none() { failures.push(RuleFailure::MissingExecutorType); continue; }
            let action = override_value(options, name, executor, "onFailure").or(rule.on_failure.as_deref()).unwrap_or("ERROR");
            let action = if matches!(mode, RuleMode::Writeread | RuleMode::Updown) && action.contains(',') {
                let trimmed = action.trim_end_matches(',');
                let action = (!trimmed.is_empty()).then(|| trimmed.split(',').nth(usize::from(execution != Mode::Upgrade))).flatten();
                let Some(action) = action else { failures.push(RuleFailure::InvalidActionPair); continue; };
                action
            } else { action };
            let resolved = match action {
                "ERROR" | "NONE" => Some(action),
                _ => registered.names.get_key_value(action.to_uppercase().as_str()).map(|(kind, _)| *kind),
            };
            match resolved {
                Some("NONE") => (),
                Some("ERROR") => failures.push(RuleFailure::Error { name: name.into(), executor: executor.into() }),
                Some("DLQ") => {
                    let selected = registered.selected("DLQ", subject, rule.name.as_deref()).expect("registered action has a name");
                    let action = Arc::clone(&registered.dlq[selected]);
                    let topic = action.options.get("dlq.topic").filter(|topic| !topic.is_empty()).map(String::as_str)
                        .or_else(|| rule.params.as_ref().and_then(|params| params.get("dlq.topic")).and_then(Option::as_deref))
                        .or_else(|| metadata.and_then(|metadata| metadata.get("properties")).and_then(|properties| properties.get("dlq.topic")).and_then(Json::as_str)).map(str::to_owned);
                    failures.push(RuleFailure::Dlq(Arc::new(DlqRule { name: rule.name.clone(), executor: executor.into(), mode: execution, subject: subject.into(), topic, action })));
                },
                _ => failures.push(RuleFailure::MissingAction(action.into())),
            }
        }
        Ok(Self { failures })
    }
    pub fn check(&self, message_is_null: bool) -> Result<Option<Arc<DlqRule>>> {
        if message_is_null { return Ok(None); }
        match self.failures.first() {
            None => Ok(None),
            Some(RuleFailure::Error { name, executor }) => Err(error("conversion-error", format!("Rule failed: {name}; could not find rule executor of type {executor}"))),
            Some(RuleFailure::MissingAction(action)) => Err(error("invalid-plugin-config", format!("Could not find rule action of type {action}"))),
            Some(RuleFailure::MissingExecutorType) => Err(error("conversion-error", "Read rule executor type is null")),
            Some(RuleFailure::InvalidActionPair) => Err(error("conversion-error", "Read rule failure action has no second action")),
            Some(RuleFailure::Dlq(rule)) => Ok(Some(Arc::clone(rule))),
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
    configured_names(options)?;
    Ok(())
}

fn configured_names(options: &BTreeMap<String, String>) -> Result<BTreeMap<&'static str, Vec<String>>> {
    let mut names = BTreeMap::<&'static str, Vec<String>>::new();
    if let Some(configured) = options.get("rule.actions") {
        for name in configured.split(',').map(str::trim).filter(|name| !name.is_empty()) {
            let Some(class) = options.get(&format!("rule.actions.{name}.class")) else { continue; };
            let kind = match class.as_str() {
                "io.confluent.kafka.schemaregistry.rules.ErrorAction" => "ERROR",
                "io.confluent.kafka.schemaregistry.rules.NoneAction" => "NONE",
                "io.confluent.kafka.schemaregistry.rules.DlqAction" => "DLQ",
                _ => return Err(error("invalid-plugin-config", "Configured Registry rule action class requires a native converter adapter")),
            };
            let registered = names.entry(kind).or_default();
            if !registered.iter().any(|registered| registered == name) { registered.push(name.into()); }
        }
    }
    if super::super::converter::option_bool(options, "rule.service.loader.enable", true)? {
        let registered = names.entry("DLQ").or_default();
        if !registered.iter().any(|registered| registered == "_default_") { registered.push("_default_".into()); }
    }
    Ok(names)
}
fn configured_actions(options: &BTreeMap<String, String>) -> Result<Actions> {
    let names = configured_names(options)?;
    let dlq = names.get("DLQ").into_iter().flatten().map(|name| {
        let mut configured = options.clone();
        configured.remove("key.serializer"); configured.remove("value.serializer");
        for source in ["_default_", "_DLQ_", name.as_str()] {
            let prefix = format!("rule.actions.{source}.param.");
            configured.extend(options.iter().filter_map(|(key, value)| key.strip_prefix(&prefix).map(|key| (key.into(), value.clone()))));
        }
        (name.clone(), Arc::new(DlqAction { name: name.clone(), options: configured }))
    }).collect();
    Ok(Actions { names, dlq })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn check(rules: Json, options: BTreeMap<String, String>) -> Result<()> { ReadRules::compile(Some(&rules), &options)?.check(false).map(|_| ()) }

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

    #[test]
    fn migration_membership_includes_disabled_rules_and_execution_uses_directional_order() {
        let rules = json!({"migrationRules":[
            {"name":"disabled","mode":"UPDOWN","disabled":true,"type":"unknown"},
            {"name":"first","mode":"UPDOWN","type":"unknown","onFailure":"ERROR,NONE"},
            {"name":"last","mode":"UPDOWN","type":"unknown","onFailure":"NONE,ERROR"}
        ]});
        assert!(ReadRules::has_migration(Some(&rules),Mode::Upgrade).unwrap());
        assert!(ReadRules::has_migration(Some(&rules),Mode::Downgrade).unwrap());
        assert!(ReadRules::compile_mode(Some(&rules),&BTreeMap::new(),Mode::Upgrade).unwrap().check(false).unwrap_err().to_string().contains("first"));
        assert!(ReadRules::compile_mode(Some(&rules),&BTreeMap::new(),Mode::Downgrade).unwrap().check(false).unwrap_err().to_string().contains("last"));
        let disabled = json!({"migrationRules":[{"mode":"UPGRADE","disabled":true}]});
        assert!(ReadRules::has_migration(Some(&disabled),Mode::Upgrade).unwrap());
        ReadRules::compile_mode(Some(&disabled),&BTreeMap::new(),Mode::Upgrade).unwrap().check(false).unwrap();
    }

    #[test]
    fn upgrade_failure_pair_keeps_first_action_when_trailing_empty_is_discarded() {
        let rules = json!({"migrationRules":[{"mode":"UPDOWN","type":"unknown","onFailure":"NONE,"}]});
        ReadRules::compile_mode(Some(&rules),&BTreeMap::new(),Mode::Upgrade).unwrap().check(false).unwrap();
        assert!(ReadRules::compile_mode(Some(&rules),&BTreeMap::new(),Mode::Downgrade).unwrap().check(false).is_err());
    }
}

#[cfg(test)]
mod dlq_tests {
    use super::*;
    use serde_json::json;

    fn options(names: &str) -> BTreeMap<String, String> {
        let mut options = BTreeMap::from([("rule.actions".into(), names.into())]);
        for name in names.split(',') { options.insert(format!("rule.actions.{name}.class"), "io.confluent.kafka.schemaregistry.rules.DlqAction".into()); }
        options
    }
    fn selected(options: &BTreeMap<String, String>, params: Json, metadata: Option<&Json>) -> Arc<DlqRule> {
        ReadRules::compile_at(Some(&json!({"domainRules":[{"name":"guard","type":"missing","onFailure":"DLQ","params":params}]})), options, Mode::Read, "events-value", metadata).unwrap().check(false).unwrap().unwrap()
    }
    #[test]
    fn dlq_selects_subject_then_rule_then_first_registration_and_preserves_config_precedence() {
        let mut options = options("first,guard,events-value:guard,first");
        options.extend(BTreeMap::from([
            ("dlq.topic".into(), "original".into()),
            ("key.serializer".into(), "ignored-key".into()),
            ("value.serializer".into(), "ignored-value".into()),
            ("rule.actions._default_.param.dlq.topic".into(), "default".into()),
            ("rule.actions._DLQ_.param.dlq.topic".into(), "type".into()),
            ("rule.actions.events-value:guard.param.dlq.topic".into(), "subject".into()),
        ]));
        let rule = selected(&options, Json::Null, None);
        assert_eq!(rule.action.name, "events-value:guard");
        assert_eq!(rule.topic.as_deref(), Some("subject"));
        assert!(!rule.action.options.contains_key("key.serializer"));
        assert!(!rule.action.options.contains_key("value.serializer"));
        options.remove("rule.actions.events-value:guard.class");
        let rule = selected(&options, Json::Null, None);
        assert_eq!(rule.action.name, "guard"); assert_eq!(rule.topic.as_deref(), Some("type"));
        options.remove("rule.actions.guard.class");
        assert_eq!(selected(&options, Json::Null, None).action.name, "first");
    }
    #[test]
    fn default_action_named_parameters_are_reapplied_after_type_parameters() {
        let options = BTreeMap::from([
            ("rule.actions._default_.param.dlq.topic".into(), "named-default".into()),
            ("rule.actions._DLQ_.param.dlq.topic".into(), "type".into()),
        ]);
        assert_eq!(selected(&options, Json::Null, None).topic.as_deref(), Some("named-default"));
    }
    #[test]
    fn dlq_topic_uses_rule_then_target_metadata_when_action_topic_is_empty() {
        let options = BTreeMap::from([("dlq.topic".into(), "".into())]);
        let metadata = json!({"properties":{"dlq.topic":"metadata"}});
        assert_eq!(selected(&options, json!({"dlq.topic":"rule"}), Some(&metadata)).topic.as_deref(), Some("rule"));
        assert_eq!(selected(&options, json!({"dlq.topic":null}), Some(&metadata)).topic.as_deref(), Some("metadata"));
        assert_eq!(selected(&options, json!({"dlq.topic":""}), Some(&metadata)).topic.as_deref(), Some(""));
        assert!(selected(&options, Json::Null, None).topic.is_none());
    }
    #[test]
    fn disabled_service_loader_requires_an_explicit_dlq_action() {
        let rules = json!({"domainRules":[{"type":"absent","onFailure":"DLQ"}]});
        assert!(ReadRules::compile(Some(&rules), &BTreeMap::from([("rule.service.loader.enable".into(), "false".into())])).unwrap().check(false).is_err());
    }
}
