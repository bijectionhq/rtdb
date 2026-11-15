use std::collections::BTreeMap;
use rtdb_azure::{AzureConfig, AzureCredentials};
use rtdb_storage::ObjectKey;

#[test]
fn alias_auth_is_node_local_and_configuration_roundtrips() {
    let mut config = AzureConfig::new("account", "container");
    config.prefix = Some("cluster/db".into());
    config.remote = Some("auth".into());
    let credentials = AzureCredentials { connection_string: Some("sensitive".into()), storage_account_key: None };
    let remotes = BTreeMap::from([("auth".into(), credentials.clone())]);
    assert_eq!(config.resolve_credentials(&remotes).unwrap(), credentials);
    assert!(config.to_proto().unwrap().storage_account_key.is_none());
    assert_eq!(AzureConfig::from_proto(&config.to_proto().unwrap()), config);
    assert_eq!(config.object_prefix(&ObjectKey::new("parts/2/v06").unwrap()).unwrap().as_str(), "cluster/db/parts/2/v06");
    assert!(!format!("{remotes:?}").contains("sensitive"));
    config.storage_account_key = Some("inline-secret".into());
    assert_eq!(config.to_proto().unwrap_err().code, "xtdb.azure/conflicting-config");
    assert!(!format!("{config:?}").contains("inline-secret"));
}
