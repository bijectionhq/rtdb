use rtdb_server::config::NodeConfiguration;

#[test]
fn spill_limit_environment_reaches_node_query_options() {
    let floor = rtdb_query::QueryOptions::default().spill_limit_bytes;
    for ingest in [false, true] {
        let config = NodeConfiguration::from_yaml_with_environment("", &|_: &str| None, ingest).unwrap();
        assert!(config.node_options(1 << 20).unwrap().query.spill_limit_bytes >= floor, "the default is read from the free disk");
    }
    for (text, expected) in [("0", 0), (" 2048 ", 2048), ("18446744073709551615", u64::MAX)] {
        let env = |key: &str| (key == "XTDB_QUERY_SPILL_LIMIT_BYTES").then(|| text.to_owned());
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
