use std::path::PathBuf;
use rtdb_auth::Secret;
use rtdb_columnar::MemoryBudget;
use rtdb_server::{config::KeyStoreSettings, tls};

fn fixture(file: &str) -> PathBuf { PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../src/test/resources/xtdb/pgwire").join(file) }

#[test]
fn pinned_java_keystores_load_and_release_charged_configuration() {
    for name in ["xtdb.jks", "xtdb.example.com.p12"] {
        let memory = MemoryBudget::new(16 * 1024 * 1024);
        let settings = KeyStoreSettings { path: fixture(name), password: Secret::new("password123") };
        let configuration = tls::load(&settings, &memory).unwrap();
        assert!(memory.used_bytes() > 0);
        drop(configuration);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn incorrect_password_and_allocation_failure_release_keystore_reservations() {
    for name in ["xtdb.jks", "xtdb.example.com.p12"] {
        let memory = MemoryBudget::new(16 * 1024 * 1024);
        let settings = KeyStoreSettings { path: fixture(name), password: Secret::new("wrong password") };
        assert_eq!(tls::load(&settings, &memory).unwrap_err().code, "rtdb.server/invalid-tls-keystore");
        assert_eq!(memory.used_bytes(), 0);
        let memory = MemoryBudget::new(1);
        assert!(tls::load(&settings, &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
}
