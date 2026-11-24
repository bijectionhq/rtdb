use rtdb_auth::{AuthConfig, Method};
use rtdb_format::database::proto::database_config;
use rtdb_server::config::NodeConfiguration;

#[test]
fn clojure_config_aliases_backends_millisecond_durations_and_env_are_decoded() {
    let (config, warnings) = NodeConfiguration::from_edn_with_environment(r#"
      {:xtdb/log [:local {:path #env "LOG_PATH", :epoch 3, :instant-source-for-non-tx-msgs? true}]
       :storage [:local {:path "storage"}]
       :server {:host nil :port 0 :read-only-port -1}
       :flight-sql {:session-idle-timeout 2500}
       :memory-cache {:max-cache-bytes 4096}
       :indexer {:enabled? false :flush-duration 5000 :skip-txs [1 2]}
       :compactor {:threads 0 :opt-in-221? true}
       :ignored-option {:secret "must not be echoed"}}
    "#, &|name| (name == "LOG_PATH").then(|| "logs".into())).unwrap();
    assert!(matches!(config.primary.log, Some(database_config::Log::LocalLog(ref log)) if log.path.as_deref() == Some("logs") && log.epoch == Some(3)));
    assert!(config.local_log_uses_clock);
    assert_eq!(config.server.unwrap().host, "0.0.0.0");
    assert_eq!(config.flight_sql.unwrap().session_idle_timeout, std::time::Duration::from_millis(2500));
    assert_eq!(config.memory_cache.maximum_bytes, Some(4096));
    assert_eq!(config.indexer.flush_duration, std::time::Duration::from_secs(5));
    assert!(!config.indexer.enabled);
    assert!(config.compactor.run_end_encoded_iids && config.compactor.within_file_recency_branching);
    assert_eq!(warnings, ["Unknown configuration key: :ignored-option"]);
}

#[test]
fn edn_authentication_retains_its_default_differences_from_yaml() {
    let (config, _) = NodeConfiguration::from_edn_with_environment("{:authn [:single-root-user {}]}", &|name| (name == "XTDB_PASSWORD").then(|| "environment-password".into())).unwrap();
    assert!(matches!(config.auth, AuthConfig::SingleRootUser { password: None }));
    let (config, _) = NodeConfiguration::from_edn_with_environment(r#"{:authn [:openid-connect {:issuer-url "https://issuer.example" :client-id "client" :client-secret "secret"}]}"#, &|_| None).unwrap();
    let AuthConfig::OpenIdConnect(config) = config.auth else { panic!("expected OIDC") };
    assert!(config.rules.is_empty());
    let (config, _) = NodeConfiguration::from_edn_with_environment(r#"{:authn [:user-list {:users {"alice" [:argon2id "encoded"]} :rules [{:method :password :user "alice" :remote-addr "127.0.0.1"}]}]}"#, &|_| None).unwrap();
    let AuthConfig::UserList { users, rules, .. } = config.auth else { panic!("expected user list") };
    assert_eq!(users["alice"].encoded(), "encoded");
    assert_eq!(rules[0].method, Method::Password);
    assert_eq!(rules[0].remote_address.as_deref(), Some("127.0.0.1"));
}

#[test]
fn edn_remote_storage_nested_credentials_and_named_backends_preserve_values() {
    let (config, _) = NodeConfiguration::from_edn_with_environment(r#"
      {:log-clusters {:broker [:kafka {:bootstrap-servers "localhost:9092" :properties-map {"security.protocol" "PLAINTEXT"}}]}
       :log [:kafka {:cluster :broker :topic "source" :create-topic? false}]
       :remotes {:upstream [:postgres {:host "postgres" :database "source" :username "user" :password "secret"}]}
       :storage [:remote {:object-store [:s3 {:bucket "bucket" :credentials {:access-key "access" :secret-key "secret"}}]}]}
    "#, &|_| None).unwrap();
    assert!(config.remotes.contains_key("broker"));
    assert!(config.remotes.contains_key("upstream"));
    assert!(!config.kafka_auto_create_topic);
    assert!(matches!(config.primary.storage, Some(database_config::Storage::RemoteStorage(_))));
}

#[test]
fn config_rejects_reader_evaluation_and_non_data_backend_objects() {
    assert!(NodeConfiguration::from_edn_with_environment("#=(System/exit 0)", &|_| None).is_err());
    assert!(NodeConfiguration::from_edn_with_environment("{:log [:unknown {}]}", &|_| None).is_err());
    assert!(NodeConfiguration::from_edn_with_environment("{:authn [:user-list {:users {\"alice\" [:unknown \"secret\"]}}]}", &|_| None).is_err());
}
