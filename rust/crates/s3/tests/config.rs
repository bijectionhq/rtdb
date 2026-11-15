use std::collections::BTreeMap;
use rtdb_s3::{S3Config, S3Credentials};
use rtdb_storage::ObjectKey;

#[test]
fn node_local_alias_credentials_do_not_enter_the_persisted_config() {
    let mut config = S3Config::new("bucket");
    config.remote = Some("credentials".into());
    config.region = Some("eu-west-1".into());
    config.prefix = Some("deployment/db".into());
    config.path_style_access_enabled = true;
    let credentials = S3Credentials { access_key: "key".into(), secret_key: "secret".into() };
    let registry = BTreeMap::from([("credentials".into(), credentials.clone())]);
    assert_eq!(config.resolve_credentials(&registry).unwrap(), Some(credentials));
    let persisted = config.to_proto().unwrap();
    assert!(persisted.credentials.is_none());
    assert_eq!(S3Config::from_proto(&persisted), config);
    assert_eq!(config.object_prefix(&ObjectKey::new("parts/1/v06_e11").unwrap()).unwrap().as_str(), "deployment/db/parts/1/v06_e11");
    assert!(!format!("{registry:?}").contains("secret"));
}

#[test]
fn conflicting_or_missing_remotes_and_parent_paths_are_rejected() {
    let registry = BTreeMap::<String, S3Credentials>::new();
    let mut config = S3Config::new("bucket");
    config.remote = Some("missing".into());
    assert_eq!(config.resolve_credentials(&registry).unwrap_err().code, "xtdb.aws/missing-remote");
    config.credentials = Some(S3Credentials { access_key: "key".into(), secret_key: "secret".into() });
    assert_eq!(config.to_proto().unwrap_err().code, "xtdb.aws/conflicting-config");
    config.prefix = Some("../escape".into());
    assert!(config.object_prefix(&ObjectKey::new("v06").unwrap()).is_err());
}
