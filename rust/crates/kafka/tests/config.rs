use std::collections::BTreeMap;
use rtdb_kafka::{KafkaClusterConfig, KafkaLogConfig, native_properties, parse_properties, producer_properties};

#[test]
fn native_configuration_creation_and_duplication_cover_extended_property_offsets() {
    let mut configuration = rdkafka::ClientConfig::new();
    configuration.set("client.id", "extended-config");
    let native = configuration.create_native_config().unwrap();
    let duplicate = native.clone();
    drop(native);
    assert_eq!(duplicate.get("client.id").unwrap(), "extended-config");
}

#[test]
fn producer_forces_durable_acknowledgements_and_preserves_order_without_idempotence() {
    let properties = BTreeMap::from([("acks".into(), "0".into()), ("enable.idempotence".into(), "false".into()), ("max.in.flight.requests.per.connection".into(), "5".into())]);
    let producer = producer_properties(&properties, false);
    assert_eq!(producer["acks"], "all");
    assert_eq!(producer["max.in.flight.requests.per.connection"], "1");
    assert_eq!(producer["linger.ms"], "0");
    assert_eq!(producer["compression.type"], "snappy");
    assert!(!producer_properties(&BTreeMap::new(), true).contains_key("linger.ms"));
    assert_eq!(producer_properties(&BTreeMap::from([("linger.ms".into(), "20".into())]), false)["linger.ms"], "20");
}

#[test]
fn java_properties_keep_iso_8859_1_continuations_and_escaped_separators() {
    let properties = parse_properties(b"# comment\r\nname = caf\xe9\r\nlong = first\\\r\n  second\r\nkey\\:part: value\\nline\nemoji=\\uD83D\\uDE00\nslash=last\\\\\nnext = yes\n").unwrap();
    assert_eq!(properties["name"], "caf\u{e9}");
    assert_eq!(properties["long"], "firstsecond");
    assert_eq!(properties["key:part"], "value\nline");
    assert_eq!(properties["emoji"], "\u{1f600}");
    assert_eq!(properties["slash"], "last\\");
    assert_eq!(properties["next"], "yes");
    assert!(parse_properties(b"bad=\\uXYZ1").is_err());
}

#[test]
fn java_properties_distinguish_an_empty_continuation_from_a_pending_final_line() {
    for source in [b"\\".as_slice(), b"\\\n", b"\\\r"] {
        assert_eq!(parse_properties(source).unwrap().get("").map(String::as_str), Some(""));
    }
    for source in [b"\\\r\n".as_slice(), b"\\\n\n", b"\\\n  \t"] {
        assert!(parse_properties(source).unwrap().is_empty());
    }
    let source = b"\\\n # hidden\r\nname=part\\\r\n #suffix\r\nother : =literal";
    let properties = parse_properties(source).unwrap();
    assert_eq!(properties.len(), 2);
    assert_eq!(properties["name"], "part#suffix");
    assert_eq!(properties["other"], "=literal");
}

#[test]
fn property_file_overrides_map_and_debug_does_not_expose_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("kafka.properties");
    std::fs::write(&path, "bootstrap.servers=file:9092\nsasl.password=secret-file").unwrap();
    let mut config = KafkaClusterConfig::new("base:9092");
    config.properties_map.insert("bootstrap.servers".into(), "map:9092".into());
    config.properties_map.insert("sasl.password".into(), "secret-map".into());
    config.properties_file = Some(path);
    assert_eq!(config.properties().unwrap()["bootstrap.servers"], "file:9092");
    assert_eq!(config.properties().unwrap()["sasl.password"], "secret-file");
    assert!(!format!("{config:?}").contains("secret-map"));
}

#[test]
fn persisted_log_configuration_resolves_replica_defaults_and_retains_epoch() {
    let config = KafkaLogConfig::new("cluster", "source");
    let restored = KafkaLogConfig::from_proto(&config.to_proto());
    assert_eq!(restored.replica_cluster(), "cluster");
    assert_eq!(restored.replica_topic(), "source-replica");
    let config = KafkaLogConfig { replica_cluster: Some("other".into()), replica_topic: Some("replica".into()), epoch: 42, ..config };
    assert_eq!(KafkaLogConfig::from_proto(&config.to_proto()), config);
}

#[test]
fn documented_plain_jaas_credentials_translate_without_revealing_secrets_in_errors() {
    let config = BTreeMap::from([("security.protocol".into(), "SASL_SSL".into()), ("sasl.mechanism".into(), "PLAIN".into()),
        ("sasl.jaas.config".into(), r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="service user" password="a\"b\\c";"#.into())]);
    let native = native_properties(&config).unwrap();
    assert!(!native.contains_key("sasl.jaas.config"));
    assert_eq!(native["sasl.username"], "service user");
    assert_eq!(native["sasl.password"], "a\"b\\c");
    assert_eq!(native["security.protocol"], "SASL_SSL");
    let invalid = BTreeMap::from([("sasl.jaas.config".into(), "bad module secret-value".into())]);
    assert!(!format!("{:?}", native_properties(&invalid).unwrap_err()).contains("secret-value"));
}

#[test]
fn plain_and_scram_accept_all_jaas_flags_with_java_uppercase_matching() {
    for module in ["plain.PlainLoginModule", "scram.ScramLoginModule"] {
        for flag in ["required", "ReQuIsItE", "SUFFICIENT", "optional", "REQUıRED", "ſufficient", "SUﬃCIENT"] {
            let config = BTreeMap::from([("sasl.jaas.config".into(), format!("org.apache.kafka.common.security.{module} {flag} username=\"u\" password=\"p\";"))]);
            let native = native_properties(&config).unwrap();
            assert_eq!(native["sasl.username"], "u");
            assert_eq!(native["sasl.password"], "p");
        }
    }
    for flag in ["mandatory", "require", "requİred", "OPTIONALLY"] {
        let config = BTreeMap::from([("sasl.jaas.config".into(), format!("org.apache.kafka.common.security.plain.PlainLoginModule {flag} username=\"u\" password=\"secret-value\";"))]);
        let error = native_properties(&config).unwrap_err();
        assert!(!format!("{error:?}").contains("secret-value"));
    }
}

#[test]
fn later_jaas_options_replace_earlier_credentials() {
    let config = BTreeMap::from([("sasl.jaas.config".into(), r#"org.apache.kafka.common.security.plain.PlainLoginModule optional username="old" password="old" username="u" password="p";"#.into())]);
    let native = native_properties(&config).unwrap();
    assert_eq!(native["sasl.username"], "u");
    assert_eq!(native["sasl.password"], "p");
}

#[test]
fn jaas_comments_and_control_whitespace_follow_stream_tokenizer() {
    for options in [
        "/* header */ required // line\nusername=user / single slash\npassword=secret; /* unterminated trailing comment",
        "required username=user /\nignored=discard\npassword=secret;",
        "\u{0}required\u{1f}username\u{0}=user\u{b}password=secret;",
        "required username=user /* middle\r\n comment */ password=secret; // trailing",
    ] {
        let properties = BTreeMap::from([("sasl.jaas.config".into(), format!("org.apache.kafka.common.security.plain.PlainLoginModule {options}"))]);
        let native = native_properties(&properties).unwrap();
        assert_eq!(native["sasl.username"], "user");
        assert_eq!(native["sasl.password"], "secret");
    }
    let properties = BTreeMap::from([("sasl.jaas.config".into(), "org.apache.kafka.common.security.plain.PlainLoginModule required username=user password=secret /* missing terminator".into())]);
    assert!(native_properties(&properties).is_err());
}

#[test]
fn jaas_quotes_decode_java_octal_escapes_and_end_at_unescaped_line_breaks() {
    for (value, expected) in [
        (r#""\141\377\477\08\7\a\v\u0041""#, "a\u{ff}'7\u{0}8\u{7}\u{7}\u{b}u0041"),
        ("\"line\n", "line"),
        ("'line\r", "line"),
        ("\"first\\\nsecond\"", "first\nsecond"),
        ("''", ""),
    ] {
        let properties = BTreeMap::from([("sasl.jaas.config".into(), format!("org.apache.kafka.common.security.plain.PlainLoginModule required username=user password={value};"))]);
        let native = native_properties(&properties).unwrap();
        assert_eq!(native["sasl.password"], expected);
    }
}

#[test]
fn jaas_word_boundaries_require_quotes_for_numbers_and_punctuation() {
    for value in ["service-user.v1", "_user", "$user", "é", "😀", "\u{a0}"] {
        let properties = BTreeMap::from([("sasl.jaas.config".into(), format!("org.apache.kafka.common.security.plain.PlainLoginModule required username={value} password=secret;"))]);
        assert_eq!(native_properties(&properties).unwrap()["sasl.username"], value);
    }
    for value in ["123", "123abc", "-user", ".user", "user@host", "user:host", "user+suffix", "user\\name", "\u{85}", "user\u{85}name"] {
        let jaas = format!("org.apache.kafka.common.security.plain.PlainLoginModule required username={value} password=secret-value;");
        let properties = BTreeMap::from([("sasl.jaas.config".into(), jaas)]);
        let error = native_properties(&properties).unwrap_err();
        assert!(!format!("{error:?}").contains("secret-value"));
        let properties = BTreeMap::from([("sasl.jaas.config".into(), format!("org.apache.kafka.common.security.plain.PlainLoginModule required username='{value}' password=secret;"))]);
        assert!(native_properties(&properties).is_ok());
    }
}

#[test]
fn jaas_null_option_keys_do_not_replace_named_credentials() {
    let properties = BTreeMap::from([("sasl.jaas.config".into(), "org.apache.kafka.common.security.plain.PlainLoginModule required username=user password=secret ?=ignored 123=ignored . =ignored - =ignored;".into())]);
    let native = native_properties(&properties).unwrap();
    assert_eq!(native["sasl.username"], "user");
    assert_eq!(native["sasl.password"], "secret");
    let properties = BTreeMap::from([("sasl.jaas.config".into(), "org.apache.kafka.common.security.plain.PlainLoginModule required username=user password=secret 123abc=ignored;".into())]);
    assert!(native_properties(&properties).is_err());
}

#[test]
fn login_modules_ignore_unconsumed_options_and_scram_preserves_token_authentication() {
    for module in ["plain.PlainLoginModule", "scram.ScramLoginModule"] {
        for (token, enabled) in [("true", true), ("TrUe", true), ("false", false), ("yes", false), (" true ", false)] {
            let properties = BTreeMap::from([("sasl.jaas.config".into(), format!("org.apache.kafka.common.security.{module} required username=user password=secret debug=true user_other=ignored tokenauth='{token}';"))]);
            let native = native_properties(&properties).unwrap();
            assert_eq!(native["sasl.username"], "user");
            assert_eq!(native["sasl.password"], "secret");
            assert!(!native.contains_key("debug"));
            assert!(!native.contains_key("user_other"));
            assert!(!native.contains_key("tokenauth"));
            assert_eq!(native.get("sasl.scram.tokenauth").map(String::as_str), if module.starts_with("scram.") { Some(if enabled { "true" } else { "false" }) } else { None });
        }
    }
}

#[test]
fn scram_native_token_configuration_must_agree_with_jaas() {
    for (jaas, native, accepted) in [("true", "TRUE", true), ("true", "1", true), ("true", "t", true), ("false", "0", true), ("false", "F", true), ("true", "false", false), ("false", "true", false), ("true", "invalid", false)] {
        let properties = BTreeMap::from([
            ("sasl.jaas.config".into(), format!("org.apache.kafka.common.security.scram.ScramLoginModule required username=user password=secret-value tokenauth={jaas};")),
            ("sasl.scram.tokenauth".into(), native.into()),
        ]);
        let result = native_properties(&properties);
        assert_eq!(result.is_ok(), accepted);
        if let Err(error) = result { assert_eq!(error.code, "xtdb.kafka/conflicting-config"); assert!(!format!("{error:?}").contains("secret-value")); }
    }
}

#[test]
fn krb5_jaas_translates_keytab_and_ticket_cache_options_to_native_kerberos() {
    let keytab = BTreeMap::from([
        ("security.protocol".into(), "SASL_SSL".into()),
        ("sasl.mechanism".into(), "GSSAPI".into()),
        ("sasl.jaas.config".into(), r#"com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab="/etc/kafka/client.keytab" principal="xtdb/host@EXAMPLE.COM" serviceName="kafka";"#.into()),
    ]);
    let native = native_properties(&keytab).unwrap();
    assert!(!native.contains_key("sasl.jaas.config"));
    assert_eq!(native["sasl.kerberos.principal"], "xtdb/host@EXAMPLE.COM");
    assert_eq!(native["sasl.kerberos.service.name"], "kafka");
    assert_eq!(native["sasl.kerberos.keytab"], "/etc/kafka/client.keytab");
    assert!(!native.contains_key("sasl.kerberos.min.time.before.relogin"));
    let mut cfg = rdkafka::ClientConfig::new();
    for (k, v) in &native { cfg.set(k, v); }
    assert!(cfg.create_native_config().is_ok());

    let ticket_cache = BTreeMap::from([
        ("security.protocol".into(), "SASL_PLAINTEXT".into()),
        ("sasl.mechanism".into(), "GSSAPI".into()),
        ("sasl.jaas.config".into(), r#"com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true useKeyTab=false keyTab="/ignored" principal="xtdb@EXAMPLE.COM";"#.into()),
    ]);
    let native = native_properties(&ticket_cache).unwrap();
    assert_eq!(native["sasl.kerberos.principal"], "xtdb@EXAMPLE.COM");
    assert!(!native.contains_key("sasl.kerberos.keytab"));
    assert_eq!(native["sasl.kerberos.min.time.before.relogin"], "0");

    let conflict = BTreeMap::from([
        ("sasl.kerberos.principal".into(), "other@EXAMPLE.COM".into()),
        ("sasl.jaas.config".into(), r#"com.sun.security.auth.module.Krb5LoginModule required principal="xtdb@EXAMPLE.COM";"#.into()),
    ]);
    assert_eq!(native_properties(&conflict).unwrap_err().code, "xtdb.kafka/conflicting-config");
}

#[test]
fn oauthbearer_unsecured_jaas_enables_native_token_generation_and_incomplete_oidc_is_rejected() {
    let unsecured = BTreeMap::from([
        ("security.protocol".into(), "SASL_PLAINTEXT".into()),
        ("sasl.mechanism".into(), "OAUTHBEARER".into()),
        ("sasl.login.callback.handler.class".into(), "org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler".into()),
        ("sasl.jaas.config".into(), r#"org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub="admin" unsecuredLoginListClaim_scope="|read|write" unsecuredLoginLifetimeSeconds="600" unsecuredLoginExtension_logicalCluster="lkc-1";"#.into()),
    ]);
    let native = native_properties(&unsecured).unwrap();
    assert!(!native.contains_key("sasl.jaas.config"));
    assert!(!native.contains_key("sasl.login.callback.handler.class"));
    assert_eq!(native["enable.sasl.oauthbearer.unsecure.jwt"], "true");
    assert_eq!(native["sasl.oauthbearer.config"], "principal=admin scope=read,write lifeSeconds=600 extension_logicalCluster=lkc-1");
    let mut cfg = rdkafka::ClientConfig::new();
    for (k, v) in &native { cfg.set(k, v); }
    assert!(cfg.create_native_config().is_ok());

    let oidc = BTreeMap::from([
        ("security.protocol".into(), "SASL_SSL".into()),
        ("sasl.mechanism".into(), "OAUTHBEARER".into()),
        ("sasl.jaas.config".into(), r#"org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="cid" clientSecret="top-secret";"#.into()),
    ]);
    let error = native_properties(&oidc).unwrap_err();
    assert_eq!(error.code, "xtdb.kafka/invalid-config");
    assert!(error.to_string().contains("token endpoint"));
    assert!(!format!("{error:?}").contains("top-secret"));
}
