use rtdb_gcs::GcsConfig;
use rtdb_storage::ObjectKey;

#[test]
fn project_bucket_prefix_and_partition_root_roundtrip() {
    let mut config = GcsConfig::new("project", "bucket");
    config.prefix = Some("deployment/db".into());
    assert_eq!(GcsConfig::from_proto(&config.to_proto()), config);
    assert_eq!(config.object_prefix(&ObjectKey::new("parts/3/v06").unwrap()).unwrap().as_str(), "deployment/db/parts/3/v06");
    config.prefix = Some("../other".into());
    assert!(config.object_prefix(&ObjectKey::new("v06").unwrap()).is_err());
}
