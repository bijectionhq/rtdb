//! Per-deployment data keys (`KeyService`): each deployment's objects are encrypted under its own key, and destroying the key is what deletion proves.

use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use rtdb_directory::{DeletionFacts, DeletionRecord};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

pub const KEY_BYTES: usize = 32;
const DESTROYED: &[u8] = b"RTDB-key-destroyed-v1";
const DELETION_SIGNATURE_DOMAIN: &[u8] = b"rtdb.multi.deletion.v1\0";

pub trait KeyService: Send + Sync {
    /// A durable identity for the namespace in which this service creates and destroys keys.
    fn namespace_id(&self) -> Result<[u8; KEY_BYTES]>;
    /// The deployment's key, created on first use.
    fn key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]>;
    /// Reads the deployment's existing key without allocating one.
    fn existing(&self, deployment: &str) -> Result<[u8; KEY_BYTES]>;
    /// Destroys this service's copy and prevents recreation under the same deployment id.
    fn destroy(&self, deployment: &str) -> Result<()>;
    /// Checks whether an incoming key could be installed without replacing a different key.
    fn validate_install(&self, deployment: &str, key: &[u8; KEY_BYTES]) -> Result<()>;
    /// Installs a key brought from another cell's key service, for a deployment that moved here.
    fn install(&self, deployment: &str, key: [u8; KEY_BYTES]) -> Result<()>;
    fn sign_deletion(&self, facts: DeletionFacts) -> Result<DeletionRecord>;
    fn deletion_verification_key(&self) -> Result<String>;
}

/// Keys kept as files in a directory apart from the objects they protect, standing in for a key-management service.
pub struct LocalKeyService { directory: PathBuf }

impl LocalKeyService {
    pub fn new(directory: impl Into<PathBuf>) -> Result<Self> {
        let directory = directory.into();
        std::fs::create_dir_all(&directory).map_err(io)?;
        Ok(Self { directory })
    }
    fn path(&self, deployment: &str) -> PathBuf { self.directory.join(format!("{deployment}.key")) }

    fn get_or_create(&self, path: &Path) -> Result<[u8; KEY_BYTES]> {
        match std::fs::read(path) {
            Ok(bytes) => read_key(path, bytes),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let mut key = [0; KEY_BYTES];
                SystemRandom::new().fill(&mut key).map_err(|_| Anomaly::fault("rtdb.multi/random", "No randomness for a new key"))?;
                self.install_if_missing(path, key)
            }
            Err(error) => Err(io(error)),
        }
    }

    fn sync_directory(&self) -> Result<()> {
        std::fs::File::open(&self.directory).and_then(|directory| rtdb_storage::sync_file(&directory)).map_err(io)
    }

    fn deletion_signer(&self) -> Result<Ed25519KeyPair> {
        let seed = self.get_or_create(&self.directory.join(".deletion-signer"))?;
        Ed25519KeyPair::from_seed_unchecked(&seed).map_err(|_| Anomaly::fault("rtdb.multi/deletion-key", "The deletion signing seed is invalid"))
    }

    fn install_if_missing(&self, path: &Path, key: [u8; KEY_BYTES]) -> Result<[u8; KEY_BYTES]> {
        let mut temporary = tempfile::NamedTempFile::new_in(&self.directory).map_err(io)?;
        temporary.write_all(&key).map_err(io)?;
        rtdb_storage::sync_file(temporary.as_file()).map_err(io)?;
        match temporary.persist_noclobber(path) {
            Ok(_) => {
                self.sync_directory()?;
                Ok(key)
            }
            Err(error) if error.error.kind() == ErrorKind::AlreadyExists => read_key(path, std::fs::read(path).map_err(io)?),
            Err(error) => Err(io(error.error)),
        }
    }
}

impl KeyService for LocalKeyService {
    fn namespace_id(&self) -> Result<[u8; KEY_BYTES]> { self.get_or_create(&self.directory.join(".namespace")) }

    fn key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]> { self.get_or_create(&self.path(deployment)) }

    fn existing(&self, deployment: &str) -> Result<[u8; KEY_BYTES]> {
        let path = self.path(deployment);
        read_key(&path, std::fs::read(&path).map_err(io)?)
    }

    fn validate_install(&self, deployment: &str, key: &[u8; KEY_BYTES]) -> Result<()> {
        let path = self.path(deployment);
        match std::fs::read(&path) {
            Ok(bytes) => require_matching_key(deployment, key, &read_key(&path, bytes)?),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io(error)),
        }
    }

    fn install(&self, deployment: &str, key: [u8; KEY_BYTES]) -> Result<()> {
        let path = self.path(deployment);
        let installed = match std::fs::read(&path) {
            Ok(bytes) => read_key(&path, bytes)?,
            Err(error) if error.kind() == ErrorKind::NotFound => self.install_if_missing(&path, key)?,
            Err(error) => return Err(io(error)),
        };
        require_matching_key(deployment, &key, &installed)
    }
    fn destroy(&self, deployment: &str) -> Result<()> {
        let path = self.path(deployment);
        match std::fs::read(&path) {
            Ok(bytes) if bytes == DESTROYED => return self.sync_directory(),
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(io(error)),
        }
        let mut replacement = tempfile::NamedTempFile::new_in(&self.directory).map_err(io)?;
        replacement.write_all(DESTROYED).map_err(io)?;
        rtdb_storage::sync_file(replacement.as_file()).map_err(io)?;
        // Replacing the file keeps an in-flight install from finding the key's name vacant.
        replacement.persist(&path).map_err(|error| io(error.error))?;
        self.sync_directory()
    }

    fn sign_deletion(&self, facts: DeletionFacts) -> Result<DeletionRecord> {
        let signature = self.deletion_signer()?.sign(&deletion_message(&facts));
        Ok(DeletionRecord { facts, signature: URL_SAFE_NO_PAD.encode(signature.as_ref()) })
    }

    fn deletion_verification_key(&self) -> Result<String> {
        Ok(URL_SAFE_NO_PAD.encode(self.deletion_signer()?.public_key().as_ref()))
    }
}

fn deletion_message(facts: &DeletionFacts) -> Vec<u8> {
    let mut message = DELETION_SIGNATURE_DOMAIN.to_vec();
    serde_json::to_writer(&mut message, facts).expect("deletion facts serialize into memory");
    message
}

/// Verifies the receipt using a cell public key obtained through a trusted channel.
pub fn verify_deletion(receipt: &DeletionRecord, public_key: &str) -> Result<()> {
    let public_key = URL_SAFE_NO_PAD.decode(public_key).map_err(|_| {
        Anomaly::incorrect("rtdb.multi/deletion-key", "The verification key must be base64url")
    })?;
    let invalid_signature = || Anomaly::incorrect("rtdb.multi/deletion-signature", "The deletion receipt signature is invalid");
    let signature = URL_SAFE_NO_PAD.decode(&receipt.signature).map_err(|_| invalid_signature())?;
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(&deletion_message(&receipt.facts), &signature)
        .map_err(|_| invalid_signature())
}

fn require_matching_key(deployment: &str, key: &[u8; KEY_BYTES], installed: &[u8; KEY_BYTES]) -> Result<()> {
    if key != installed {
        return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/key-conflict", format!("The key for {deployment} is already installed with a different value")));
    }
    Ok(())
}

fn read_key(path: &Path, bytes: Vec<u8>) -> Result<[u8; KEY_BYTES]> {
    if bytes == DESTROYED {
        return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb.multi/key-destroyed", format!("The key at {} was destroyed", path.display())));
    }
    bytes.try_into().map_err(|_| Anomaly::fault("rtdb.multi/key-corrupt", format!("{} has an invalid key length", path.display())))
}

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/keys", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_survives_a_key_service_restart_and_binds_the_inventory_and_identity() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        let public_key = keys.deletion_verification_key().unwrap();
        let facts = DeletionFacts {
            deployment: "deployment-id".into(),
            name: "shop".into(),
            epoch: 14,
            storage_prefix: "1234/acme/lineage/deployment-id/1".into(),
            namespaces: rtdb_directory::StorageNamespaces {
                objects: uuid::Uuid::new_v4(),
                keys: keys.namespace_id().unwrap(),
            },
            inventory: rtdb_directory::DeletionInventory::CurrentObjects,
            key_destroyed_at_ms: 2,
            objects_erased_at_ms: 1,
            first_empty_observed_at_ms: None,
            verified_empty: true,
        };
        let receipt = keys.sign_deletion(facts).unwrap();
        let json = serde_json::to_string_pretty(&receipt).unwrap();
        let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["facts"]["verified_empty"], true);
        assert!(value["facts"].get("first_empty_observed_at_ms").is_none());
        value["facts"]["inventory_claim"] = serde_json::json!("all_versions");
        assert!(serde_json::from_value::<DeletionRecord>(value).is_err());
        let mut receipt: DeletionRecord = serde_json::from_str(&json).unwrap();

        let restarted = LocalKeyService::new(directory.path()).unwrap();
        assert_eq!(restarted.deletion_verification_key().unwrap(), public_key);
        verify_deletion(&receipt, &public_key).unwrap();
        let other_directory = tempfile::tempdir().unwrap();
        let other_key = LocalKeyService::new(other_directory.path()).unwrap().deletion_verification_key().unwrap();
        assert!(verify_deletion(&receipt, &other_key).is_err());
        receipt.facts.epoch += 1;
        assert!(verify_deletion(&receipt, &public_key).is_err());
        receipt.facts.epoch -= 1;
        let objects = receipt.facts.namespaces.objects;
        receipt.facts.namespaces.objects = uuid::Uuid::new_v4();
        assert!(verify_deletion(&receipt, &public_key).is_err());
        receipt.facts.namespaces.objects = objects;
        receipt.facts.verified_empty = false;
        assert!(verify_deletion(&receipt, &public_key).is_err());
    }

    #[test]
    fn a_destroyed_key_cannot_be_read_recreated_or_reinstalled() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        let key = keys.key("a").unwrap();
        assert_eq!(keys.key("a").unwrap(), key);
        keys.install("b", keys.existing("a").unwrap()).unwrap();
        keys.destroy("a").unwrap();
        assert_eq!(keys.existing("a").unwrap_err().code, "rtdb.multi/key-destroyed");
        let restarted = LocalKeyService::new(directory.path()).unwrap();
        assert_eq!(restarted.key("a").unwrap_err().code, "rtdb.multi/key-destroyed");
        assert_eq!(restarted.install("a", key).unwrap_err().code, "rtdb.multi/key-destroyed");
        assert_eq!(restarted.validate_install("a", &key).unwrap_err().code, "rtdb.multi/key-destroyed");
        assert_eq!(restarted.key("b").unwrap(), key);
    }

    #[test]
    fn destroying_a_key_replaces_only_the_selected_copy() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        let key = keys.key("source").unwrap();
        std::fs::hard_link(keys.path("source"), keys.path("copy")).unwrap();
        keys.destroy("source").unwrap();
        keys.destroy("source").unwrap();
        assert_eq!(keys.key("copy").unwrap(), key);
        assert_eq!(keys.existing("source").unwrap_err().code, "rtdb.multi/key-destroyed");
    }

    #[test]
    fn installing_a_key_is_idempotent_and_cannot_replace_a_different_key() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        keys.install("deployment", [1; KEY_BYTES]).unwrap();
        keys.install("deployment", [1; KEY_BYTES]).unwrap();
        let conflict = keys.install("deployment", [2; KEY_BYTES]).unwrap_err();
        assert_eq!(conflict.code, "rtdb.multi/key-conflict");
        assert_eq!(keys.key("deployment").unwrap(), [1; KEY_BYTES]);
    }

    #[test]
    fn a_concurrent_install_cannot_leave_a_destroyed_key_readable() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        std::thread::scope(|scope| {
            for id in 0..16 {
                let keys = &keys;
                scope.spawn(move || {
                    let _ = keys.install(&id.to_string(), [1; KEY_BYTES]);
                });
                keys.destroy(&id.to_string()).unwrap();
            }
        });
        for id in 0..16 {
            assert_eq!(keys.key(&id.to_string()).unwrap_err().code, "rtdb.multi/key-destroyed");
        }
    }

    #[test]
    fn concurrent_first_uses_agree_on_one_key() {
        let directory = tempfile::tempdir().unwrap();
        let keys: Vec<[u8; KEY_BYTES]> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..16).map(|_| scope.spawn(|| LocalKeyService::new(directory.path()).unwrap().key("raced").unwrap())).collect();
            handles.into_iter().map(|handle| handle.join().unwrap()).collect()
        });
        assert!(keys.windows(2).all(|pair| pair[0] == pair[1]), "every racer uses the key that was stored");
    }
}
