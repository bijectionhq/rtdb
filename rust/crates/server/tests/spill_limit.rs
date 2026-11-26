use rtdb_server::config::NodeConfiguration;

#[test]
fn spill_limit_environment_reaches_node_query_options() {
    let default = rtdb_query::QueryOptions::default().spill_limit_bytes;
    assert!(default >= 1 << 30);
    for (text, expected) in [(None, default), (Some("0"), 0), (Some(" 2048 "), 2048), (Some("18446744073709551615"), u64::MAX)] {
        let env = |key: &str| (key == "XTDB_QUERY_SPILL_LIMIT_BYTES").then(|| text.map(str::to_owned)).flatten();
        for ingest in [false, true] {
            let config = NodeConfiguration::from_yaml_with_environment("", &env, ingest).unwrap();
            assert_eq!(config.node_options(1 << 20).unwrap().query.spill_limit_bytes, expected, "{text:?}");
        }
    }
    for text in ["-1", "1.5", "1GiB", ""] {
        let env = |key: &str| (key == "XTDB_QUERY_SPILL_LIMIT_BYTES").then(|| text.to_owned());
        assert!(NodeConfiguration::from_yaml_with_environment("", &env, false).is_err(), "{text:?}");
    }
}
