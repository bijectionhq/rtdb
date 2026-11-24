use std::{collections::BTreeMap, sync::Arc};

use rtdb_columnar::MemoryBudget;
use rtdb_server::config::{ConfigurationEnvironment, EnvironmentValue, NodeConfiguration, ParsedConfiguration};
use rtdb_types::Result;

struct Environment(BTreeMap<String, String>);
impl ConfigurationEnvironment for Environment {
    fn lookup(&self, name: &str, memory: &Arc<MemoryBudget>) -> Result<Option<EnvironmentValue>> {
        self.0.get(name).map(|value| EnvironmentValue::copy(value, memory)).transpose()
    }
}
fn empty() -> Environment { Environment(BTreeMap::new()) }

#[test]
fn bounded_yaml_preserves_provider_tags_aliases_and_environment_defaults() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let yaml = "nodeId: &name shared-host\nserver: {host: *name, port: 0}\nflightSql: null\nlog: !Local {path: !Env RTDB_LOG_PATH}\n";
    let environment = Environment(BTreeMap::from([("RTDB_LOG_PATH".into(), "/tmp/native-log".into()), ("XTDB_PASSWORD".into(), "password".into())]));
    let parsed = ParsedConfiguration::from_yaml(yaml, &environment, &memory, false).unwrap();
    let expected = NodeConfiguration::from_yaml_with_environment(yaml, &|name| environment.0.get(name).cloned(), false).unwrap();
    assert_eq!(parsed.configuration().primary, expected.primary);
    assert_eq!(parsed.configuration().node_id, "shared-host");
    assert_eq!(parsed.configuration().server.as_ref().unwrap().host, "shared-host");
    assert!(memory.used_bytes() > yaml.len());
    drop(parsed); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn multi_yaml_environment_requires_explicit_input_and_hides_invalid_values() {
    let memory = MemoryBudget::new(1 << 24);
    let environment = Environment(BTreeMap::from([
        ("MULTI_CONFIG".into(), "server: null\nflightSql: {port: !Env FLIGHT_PORT}".into()),
        ("FLIGHT_PORT".into(), "3000".into()),
        ("EMPTY_CONFIG".into(), " ".into()),
        ("INVALID_CONFIG".into(), "authn: !private-config-marker {}".into()),
    ]));
    let parsed = ParsedConfiguration::from_yaml_environment("MULTI_CONFIG", &environment, &memory)
        .unwrap();
    assert!(parsed.configuration().server.is_none());
    assert_eq!(parsed.configuration().flight_sql.as_ref().unwrap().port, 3000);

    for name in ["MISSING_CONFIG", "EMPTY_CONFIG", "INVALID_CONFIG"] {
        let error = ParsedConfiguration::from_yaml_environment(name, &environment, &memory)
            .err()
            .expect("invalid configuration must fail");
        assert!(!format!("{error:?}").contains("private-config-marker"));
    }
}

#[test]
fn yaml_alias_expansion_reserves_each_copied_node_before_construction() {
    let memory = MemoryBudget::new(400_000);
    let yaml = "a: &a [0, 0, 0, 0, 0, 0, 0, 0]\nb: &b [*a, *a, *a, *a, *a, *a, *a, *a]\nc: [*b, *b, *b, *b, *b, *b, *b, *b]\n";
    assert!(matches!(ParsedConfiguration::from_yaml(yaml, &empty(), &memory, false), Err(error) if error.code == "rtdb/memory-budget-exceeded"));
    assert!(memory.peak_bytes() <= memory.limit_bytes());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn environment_expansion_fails_before_copying_an_oversized_explicit_or_default_value() {
    for yaml in ["nodeId: !Env BIG", ""] {
        let memory = MemoryBudget::new(200_000);
        let environment = Environment(BTreeMap::from([("BIG".into(), "x".repeat(4096)), ("XTDB_NODE_ID".into(), "x".repeat(4096))]));
        assert!(matches!(ParsedConfiguration::from_yaml(yaml, &environment, &memory, false), Err(error) if error.code == "rtdb/memory-budget-exceeded"));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn configuration_charge_transfers_before_execution_region_initialization() {
    let bootstrap = MemoryBudget::new(8 * 1024 * 1024);
    let parsed = ParsedConfiguration::from_yaml("server: null\nflightSql: null\n", &empty(), &bootstrap, false).unwrap();
    let bound = bootstrap.used_bytes();
    let (configuration, execution, lease) = parsed.into_execution(8 * 1024 * 1024).unwrap();
    assert_eq!(bootstrap.used_bytes(), 0);
    assert_eq!(execution.limit_bytes(), 4 * 1024 * 1024);
    assert_eq!(execution.used_bytes(), bound);
    drop(configuration); drop(lease);
    assert_eq!(execution.used_bytes(), 0);
}

#[test]
fn data_only_edn_environment_expansion_uses_the_same_retained_budget() {
    let memory = MemoryBudget::new(1_000_000);
    let environment = Environment(BTreeMap::from([("RTDB_HOST".into(), "127.0.0.2".into())]));
    let parsed = ParsedConfiguration::from_edn("{:server {:host #env RTDB_HOST :port 0} :unknown true}", &environment, &memory).unwrap();
    assert_eq!(parsed.configuration().server.as_ref().unwrap().host, "127.0.0.2");
    assert_eq!(parsed.warnings(), ["Unknown configuration key: :unknown"]);
    drop(parsed); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn duplicate_keys_and_recursive_aliases_do_not_escape_the_parser_region() {
    for yaml in ["nodeId: first\nnodeId: second", "nodeId: &a [*a]"] {
        let memory = MemoryBudget::new(2 * 1024 * 1024);
        assert!(ParsedConfiguration::from_yaml(yaml, &empty(), &memory, false).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn bounded_document_exposes_unknown_options_before_semantic_decode_and_retains_its_charge() {
    let memory = MemoryBudget::new(2 * 1024 * 1024);
    let document = rtdb_server::config::ParsedYamlDocument::from_yaml("customHook: !Jvm {factory: example.Factory}\n", &empty(), &memory).unwrap();
    assert!(document.value().as_mapping().unwrap().contains_key(rtdb_format::config::Yaml::String("customHook".into())));
    assert_eq!(document.allocation_bound(), memory.used_bytes());
    assert!(document.into_node_configuration(&empty(), false).is_err());
    assert_eq!(memory.used_bytes(), 0);
}
