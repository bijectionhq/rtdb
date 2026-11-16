use rtdb_server::config::NodeConfiguration;

#[test]
fn join_threshold_environment_reaches_shared_node_query_options() {
    for (text, expected) in [("0", 0), ("2", 2), ("-1", -1), ("+१२", 12), ("9223372036854775807", i64::MAX), ("-9223372036854775808", i64::MIN), ("9223372036854775808", 100_000), ("1.0", 100_000), (" 2", 100_000), ("invalid", 100_000)] {
        let env = |key: &str| (key == "XTDB_JOIN_SPILL_THRESHOLD").then(|| text.into());
        for ingest in [false, true] {
            let config = NodeConfiguration::from_yaml_with_environment("", &env, ingest).unwrap();
            assert_eq!(config.node_options(1 << 20).unwrap().query.join_build_rows, expected, "{text}");
        }
        let (config, _) = NodeConfiguration::from_edn_with_environment("{}", &env).unwrap();
        assert_eq!(config.node_options(1 << 20).unwrap().query.join_build_rows, expected, "{text}");
    }
}
