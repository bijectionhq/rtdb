#![cfg(target_os="linux")]

use std::{fs, sync::{Arc,atomic::AtomicBool}};
use rtdb_filesystem_observer::{Config,driver};

#[test]
#[ignore="requires an explicitly provisioned Linux /dev/fuse worker with mount and allow_other privileges"]
fn failed_readiness_publication_unmounts_and_joins_before_returning_error() {
    let directory=tempfile::tempdir().unwrap();let root=directory.path().canonicalize().unwrap();
    let config=Config{namespace:"observer-startup-failure".into(),storage_placement:"test-device".into(),backing:root.join("backing"),mount:root.join("mount"),
        ready:root.join("r".repeat(300)),final_receipt:root.join("final.json"),expected_binary_sha256:driver::binary_digest().unwrap(),max_inodes:128,max_handles:128,max_io_bytes:65536};
    fs::create_dir(&config.backing).unwrap();fs::create_dir(&config.mount).unwrap();
    assert!(driver::run(config.clone(),Arc::new(AtomicBool::new(false))).is_err());
    let receipt:serde_json::Value=serde_json::from_slice(&fs::read(config.final_receipt).unwrap()).unwrap();
    assert_eq!(receipt["joined"],true);assert_eq!(receipt["unmounted"],true);
    assert!(!receipt["errors"].as_array().unwrap().is_empty());
    assert!(!fs::read_to_string("/proc/self/mountinfo").unwrap().lines().any(|line|line.split_whitespace().nth(4)==config.mount.to_str()));
}
