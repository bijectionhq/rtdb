use rtdb_server::config::{NodeConfiguration, QUERY_SPILL_LIMIT_SETTING};

fn spill_limit(yaml: &str, environment: &impl Fn(&str) -> Option<String>, ingest: bool) -> rtdb_types::Result<u64> {
    Ok(NodeConfiguration::from_yaml_with_environment(yaml, environment, ingest)?.node_options(1 << 20)?.query.spill_limit_bytes)
}

#[test]
fn spill_limit_environment_reaches_node_query_options() {
    assert_eq!(QUERY_SPILL_LIMIT_SETTING, "RTDB_QUERY_SPILL_LIMIT_BYTES");
    for ingest in [false, true] {
        assert_eq!(spill_limit("", &|_: &str| None, ingest).unwrap(), rtdb_query::QueryOptions::UNLIMITED_SPILL, "no limit unless configured");
    }
    for (text, expected) in [("0", 0), (" 2048 ", 2048), ("18446744073709551615", u64::MAX)] {
        let env = |key: &str| (key == QUERY_SPILL_LIMIT_SETTING).then(|| text.to_owned());
        for ingest in [false, true] {
            assert_eq!(spill_limit("", &env, ingest).unwrap(), expected, "{text:?}");
        }
    }
    for text in ["-1", "1.5", "1GiB", ""] {
        let env = |key: &str| (key == QUERY_SPILL_LIMIT_SETTING).then(|| text.to_owned());
        assert!(spill_limit("", &env, false).is_err(), "{text:?}");
    }
}

#[test]
fn the_xtdb_spelling_of_the_spill_limit_is_not_read() {
    let env = |key: &str| (key == "XTDB_QUERY_SPILL_LIMIT_BYTES").then(|| "2048".to_owned());
    assert_eq!(spill_limit("", &env, false).unwrap(), rtdb_query::QueryOptions::UNLIMITED_SPILL);
}

#[test]
fn the_yaml_spill_limit_overrides_the_environment() {
    let env = |key: &str| (key == QUERY_SPILL_LIMIT_SETTING).then(|| "2048".to_owned());
    for ingest in [false, true] {
        assert_eq!(spill_limit("query: {spillLimitBytes: 4096}", &env, ingest).unwrap(), 4096);
        assert_eq!(spill_limit("query: {spillLimitBytes: 0}", &|_| None, ingest).unwrap(), 0);
        assert_eq!(spill_limit("query: {}", &env, ingest).unwrap(), 2048);
    }
    for yaml in ["query: {spillLimitBytes: -1}", "query: {spillLimitBytes: 1GiB}"] {
        assert_eq!(spill_limit(yaml, &|_| None, false).unwrap_err().code, "xtdb/invalid-node-config", "{yaml}");
    }
    for yaml in ["query: {spillLimit: 4096}", "query: 4096"] {
        assert!(spill_limit(yaml, &|_| None, false).is_err(), "{yaml}");
    }
}
