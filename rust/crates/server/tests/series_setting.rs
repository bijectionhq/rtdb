use rtdb_server::config::NodeConfiguration;

#[test]
fn series_endpoint_environment_reaches_shared_node_query_options() {
    for (text, expected) in [(None,false),(Some("true"),true),(Some("TRUE"),true),(Some("TrUe"),true),(Some("false"),false),(Some(" true"),false),(Some("true "),false),(Some("1"),false),(Some(""),false)] {
        let env = |key: &str| if key == "XTDB_GENERATE_SERIES_END_EXCLUSIVE" { text.map(str::to_owned) } else { None };
        for ingest in [false,true] {
            let config = NodeConfiguration::from_yaml_with_environment("", &env, ingest).unwrap();
            assert_eq!(config.node_options(1 << 20).unwrap().query.generate_series_end_exclusive, expected, "{text:?}");
        }
        let (config, _) = NodeConfiguration::from_edn_with_environment("{}", &env).unwrap();
        assert_eq!(config.node_options(1 << 20).unwrap().query.generate_series_end_exclusive, expected, "{text:?}");
    }
}
