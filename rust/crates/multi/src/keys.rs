//! Per-deployment key provisioning, fork copies and durable withdrawal.

use std::fs::{self, File, TryLockError};
use std::io::{ErrorKind, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use rtdb_directory::{DeletionFacts, DeletionRecord};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

pub const KEY_BYTES: usize = 32;
const NAMESPACE_KEY_ID: &str = ".rtdb-key-namespace";
const DELETION_SIGNING_KEY_ID: &str = ".rtdb-deletion-signing-seed";
const DESTROYED: &[u8] = b"RTDB-key-destroyed-v1";
const DELETION_SIGNATURE_DOMAIN: &[u8] = b"rtdb.multi.deletion.v1\0";

#[async_trait::async_trait]
pub trait KeyService: Send + Sync {
    async fn namespace_id(&self) -> Result<[u8; KEY_BYTES]> { self.key(NAMESPACE_KEY_ID).await }
    async fn existing(&self, deployment: &str) -> Result<[u8; KEY_BYTES]> {
        self.existing_key(deployment).await?.ok_or_else(|| {
            Anomaly::new(AnomalyCategory::NotFound, "rtdb.multi/key-missing", "The deployment has no installed key")
        })
    }
    async fn validate_install(&self, deployment: &str, key: &[u8; KEY_BYTES]) -> Result<()> {
        if let Some(existing) = self.existing_key(deployment).await? {
            require_matching_key(deployment, key, &existing)?;
        }
        Ok(())
    }
    async fn sign_deletion(&self, facts: DeletionFacts) -> Result<DeletionRecord> {
        sign_deletion(self.key(DELETION_SIGNING_KEY_ID).await?, facts)
    }
    async fn deletion_verification_key(&self) -> Result<String> {
        verification_key(self.key(DELETION_SIGNING_KEY_ID).await?)
    }

    /// The deployment's key, created on first use.
    async fn key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]>;
    /// Read current custody without creating a key.
    async fn existing_key(&self, deployment: &str) -> Result<Option<[u8; KEY_BYTES]>>;
    /// Gives `to` the key `from` has, for a fork whose objects are copies of its parent's.
    async fn copy(&self, from: &str, to: &str) -> Result<()>;
    /// Withdraws the key and permanently refuses provisioning for this deployment ID.
    async fn destroy(&self, deployment: &str) -> Result<()>;
    /// Identifies custody so a move cannot install into the vault it will withdraw.
    fn custody_namespace(&self) -> Result<String>;
    /// Installs a key brought from another cell's key service, for a deployment that moved here.
    async fn install(&self, deployment: &str, key: [u8; KEY_BYTES]) -> Result<()>;

    /// The standalone key authority can witness the lifetime of a local I/O
    /// region. Providers without a local authority use explicit drain receipts.
    fn hold_local_storage_region(&self, _region: uuid::Uuid) -> Result<Option<File>> { Ok(None) }
    fn local_storage_region_drained(&self, _region: uuid::Uuid) -> Result<bool> { Ok(false) }
    fn forget_local_storage_region(&self, _region: uuid::Uuid) -> Result<()> { Ok(()) }
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
    fn region_path(&self, region: uuid::Uuid) -> PathBuf {
        self.directory.join(format!("{region}.storage-region"))
    }

    fn lock_key(&self, path: &Path) -> Result<File> {
        let lock = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.with_extension("lock"))
            .map_err(io)?;
        lock.lock().map_err(io)?;
        match fs::remove_file(path.with_extension("pending")) {
            Ok(()) => self.sync_directory()?,
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(io(error)),
        }
        Ok(lock)
    }

    fn publish(&self, path: &Path, contents: &[u8]) -> Result<()> {
        let temporary = path.with_extension("pending");
        let mut options = File::options();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut pending = options.open(&temporary).map_err(io)?;
        pending.write_all(contents).map_err(io)?;
        pending.sync_all().map_err(io)?;
        drop(pending);
        fs::rename(temporary, path).map_err(io)?;
        self.sync_directory()
    }

    fn sync_directory(&self) -> Result<()> {
        rtdb_types::directory_io::open_for_sync(&self.directory).and_then(|directory| directory.sync_all()).map_err(io)
    }
    async fn load_key(&self, path: PathBuf) -> Result<[u8; KEY_BYTES]> {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else { return self.key_at(&path); };
        let authority = Self { directory: self.directory.clone() };
        runtime.spawn_blocking(move || authority.key_at(&path)).await
            .map_err(|error| Anomaly::fault("rtdb.multi/local-key-task", error.to_string()))?
    }
    fn key_at(&self, path: &Path) -> Result<[u8; KEY_BYTES]> {
        let _lock = self.lock_key(path)?;
        match fs::read(path) {
            Ok(bytes) => {
                let key = read_key(path, bytes)?;
                self.sync_directory()?;
                Ok(key)
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let key = new_key()?;
                self.publish(path, &key)?;
                Ok(key)
            }
            Err(error) => Err(io(error)),
        }
    }
}

#[async_trait::async_trait]
impl KeyService for LocalKeyService {
    async fn namespace_id(&self) -> Result<[u8; KEY_BYTES]> { self.load_key(self.directory.join(".namespace")).await }

    async fn sign_deletion(&self, facts: DeletionFacts) -> Result<DeletionRecord> {
        sign_deletion(self.load_key(self.directory.join(".deletion-signer")).await?, facts)
    }

    async fn deletion_verification_key(&self) -> Result<String> {
        verification_key(self.load_key(self.directory.join(".deletion-signer")).await?)
    }

    async fn key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]> {
        self.load_key(self.path(deployment)).await
    }

    fn custody_namespace(&self) -> Result<String> {
        Ok(format!("local:{:?}", self.directory.canonicalize().map_err(io)?))
    }

    async fn existing_key(&self, deployment: &str) -> Result<Option<[u8; KEY_BYTES]>> {
        let path = self.path(deployment);
        match fs::read(&path) {
            Ok(bytes) => read_key(&path, bytes).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io(error)),
        }
    }

    async fn copy(&self, from: &str, to: &str) -> Result<()> {
        self.install(to, self.existing(from).await?).await
    }

    async fn install(&self, deployment: &str, key: [u8; KEY_BYTES]) -> Result<()> {
        let path = self.path(deployment);
        let _lock = self.lock_key(&path)?;
        match fs::read(&path) {
            Err(error) if error.kind() == ErrorKind::NotFound => self.publish(&path, &key),
            Err(error) => Err(io(error)),
            Ok(bytes) => {
                require_matching_key(deployment, &key, &read_key(&path, bytes)?)?;
                self.sync_directory()
            }
        }
    }
    async fn destroy(&self, deployment: &str) -> Result<()> {
        let path = self.path(deployment);
        let _lock = self.lock_key(&path)?;
        match fs::read(&path) {
            Ok(bytes) if is_destroyed(&bytes) => return self.sync_directory(),
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(io(error)),
        }
        self.publish(&path, DESTROYED)
    }

    fn hold_local_storage_region(&self, region: uuid::Uuid) -> Result<Option<File>> {
        let mut options = File::options();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let witness = options.open(self.region_path(region)).map_err(io)?;
        witness.lock().map_err(io)?;
        witness.sync_all().map_err(io)?;
        self.sync_directory()?;
        Ok(Some(witness))
    }

    fn local_storage_region_drained(&self, region: uuid::Uuid) -> Result<bool> {
        let witness = match File::options().read(true).write(true).open(self.region_path(region)) {
            Ok(witness) => witness,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(io(error)),
        };
        match witness.try_lock() {
            Ok(()) => Ok(true),
            Err(TryLockError::WouldBlock) => Ok(false),
            Err(TryLockError::Error(error)) => Err(io(error)),
        }
    }

    fn forget_local_storage_region(&self, region: uuid::Uuid) -> Result<()> {
        match fs::remove_file(self.region_path(region)) {
            Ok(()) => self.sync_directory(),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io(error)),
        }
    }
}

fn sign_deletion(seed: [u8; KEY_BYTES], facts: DeletionFacts) -> Result<DeletionRecord> {
    let signature = deletion_signer(seed)?.sign(&deletion_message(&facts));
    Ok(DeletionRecord { facts, signature: URL_SAFE_NO_PAD.encode(signature.as_ref()) })
}

fn verification_key(seed: [u8; KEY_BYTES]) -> Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(deletion_signer(seed)?.public_key().as_ref()))
}

fn deletion_signer(seed: [u8; KEY_BYTES]) -> Result<Ed25519KeyPair> {
    Ed25519KeyPair::from_seed_unchecked(&seed)
        .map_err(|_| Anomaly::fault("rtdb.multi/deletion-key", "The deletion signing seed is invalid"))
}

fn is_destroyed(bytes: &[u8]) -> bool { bytes == DESTROYED || bytes.is_empty() }

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
    if is_destroyed(&bytes) {
        return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb.multi/key-destroyed", format!("The key at {} was destroyed", path.display())));
    }
    bytes.try_into().map_err(|_| Anomaly::fault("rtdb.multi/key-corrupt", format!("{} has an invalid key length", path.display())))
}

fn new_key() -> Result<[u8; KEY_BYTES]> {
    let mut key = [0u8; KEY_BYTES];
    SystemRandom::new().fill(&mut key).map_err(|_| Anomaly::fault("rtdb.multi/random", "No randomness for a new key"))?;
    Ok(key)
}

fn malformed() -> Anomaly { Anomaly::fault("rtdb.multi/key-corrupt", "Malformed deployment data key") }

#[cfg(feature = "dynamodb")]
pub mod dynamo;
#[cfg(feature = "s3-key-vault")]
pub mod s3;

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/keys", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn opening_a_key_does_not_block_the_executor_on_a_held_file_lock() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        let lock = keys.lock_key(&keys.path("shop")).unwrap();
        let (release, released) = std::sync::mpsc::channel();
        let watchdog = std::thread::spawn(move || {
            let _ = released.recv_timeout(std::time::Duration::from_secs(1));
            drop(lock);
        });

        let opening = keys.key("shop");
        tokio::pin!(opening);
        let waiting = tokio::time::timeout(std::time::Duration::from_millis(30), &mut opening).await.is_err();
        let _ = release.send(());
        watchdog.join().unwrap();
        assert!(waiting);
        assert_eq!(opening.await.unwrap(), keys.existing("shop").await.unwrap());
    }

    #[tokio::test]
    async fn receipt_survives_a_key_service_restart_and_binds_the_inventory_and_identity() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        let public_key = keys.deletion_verification_key().await.unwrap();
        let facts = DeletionFacts {
            deployment: "deployment-id".into(),
            name: "shop".into(),
            epoch: 14,
            storage_prefix: "1234/acme/lineage/deployment-id/1".into(),
            namespaces: rtdb_directory::StorageNamespaces {
                objects: uuid::Uuid::new_v4(),
                keys: keys.namespace_id().await.unwrap(),
            },
            inventory: rtdb_directory::DeletionInventory::CurrentObjects,
            key_destroyed_at_ms: 2,
            objects_erased_at_ms: 1,
            first_empty_observed_at_ms: None,
            verified_empty: true,
        };
        let receipt = keys.sign_deletion(facts).await.unwrap();
        let json = serde_json::to_string_pretty(&receipt).unwrap();
        let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["facts"]["verified_empty"], true);
        assert!(value["facts"].get("first_empty_observed_at_ms").is_none());
        value["facts"]["inventory_claim"] = serde_json::json!("all_versions");
        assert!(serde_json::from_value::<DeletionRecord>(value).is_err());
        let mut receipt: DeletionRecord = serde_json::from_str(&json).unwrap();

        let restarted = LocalKeyService::new(directory.path()).unwrap();
        assert_eq!(restarted.deletion_verification_key().await.unwrap(), public_key);
        verify_deletion(&receipt, &public_key).unwrap();
        let other_directory = tempfile::tempdir().unwrap();
        let other_key = LocalKeyService::new(other_directory.path()).unwrap().deletion_verification_key().await.unwrap();
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

    #[tokio::test]
    async fn a_destroyed_key_cannot_be_read_recreated_or_reinstalled() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        let key = keys.key("a").await.unwrap();
        assert_eq!(keys.key("a").await.unwrap(), key);
        keys.install("b", keys.existing("a").await.unwrap()).await.unwrap();
        keys.destroy("a").await.unwrap();
        assert_eq!(keys.existing("a").await.unwrap_err().code, "rtdb.multi/key-destroyed");
        let restarted = LocalKeyService::new(directory.path()).unwrap();
        assert_eq!(restarted.key("a").await.unwrap_err().code, "rtdb.multi/key-destroyed");
        assert_eq!(restarted.install("a", key).await.unwrap_err().code, "rtdb.multi/key-destroyed");
        assert_eq!(restarted.validate_install("a", &key).await.unwrap_err().code, "rtdb.multi/key-destroyed");
        assert_eq!(restarted.key("b").await.unwrap(), key);
    }

    #[tokio::test]
    async fn destroying_a_key_replaces_only_the_selected_copy() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        let key = keys.key("source").await.unwrap();
        std::fs::hard_link(keys.path("source"), keys.path("copy")).unwrap();
        keys.destroy("source").await.unwrap();
        keys.destroy("source").await.unwrap();
        assert_eq!(keys.key("copy").await.unwrap(), key);
        assert_eq!(keys.existing("source").await.unwrap_err().code, "rtdb.multi/key-destroyed");
    }

    #[tokio::test]
    async fn installing_a_key_is_idempotent_and_cannot_replace_a_different_key() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        keys.install("deployment", [1; KEY_BYTES]).await.unwrap();
        keys.install("deployment", [1; KEY_BYTES]).await.unwrap();
        let conflict = keys.install("deployment", [2; KEY_BYTES]).await.unwrap_err();
        assert_eq!(conflict.code, "rtdb.multi/key-conflict");
        assert_eq!(keys.key("deployment").await.unwrap(), [1; KEY_BYTES]);
    }

    #[tokio::test]
    async fn a_key_is_stable_until_destroyed_and_a_copy_is_independent() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        let key = keys.key("a").await.unwrap();
        assert_eq!(keys.key("a").await.unwrap(), key);
        keys.copy("a", "b").await.unwrap();
        let interrupted = keys.path("a").with_extension("pending");
        fs::write(&interrupted, key).unwrap();
        keys.destroy("a").await.unwrap();
        assert!(!interrupted.exists());
        keys.destroy("a").await.unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        assert!(keys.key("a").await.is_err());
        assert!(keys.install("a", key).await.is_err());
        assert!(keys.copy("b", "a").await.is_err());
        assert_eq!(keys.key("b").await.unwrap(), key, "a fork's copy survives its parent's deletion");
    }

    #[test]
    fn a_concurrent_install_cannot_leave_a_destroyed_key_readable() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        std::thread::scope(|scope| {
            for id in 0..16 {
                let keys = &keys;
                scope.spawn(move || {
                    let _ = futures::executor::block_on(keys.install(&id.to_string(), [1; KEY_BYTES]));
                });
                futures::executor::block_on(keys.destroy(&id.to_string())).unwrap();
            }
        });
        for id in 0..16 {
            assert_eq!(futures::executor::block_on(keys.key(&id.to_string())).unwrap_err().code, "rtdb.multi/key-destroyed");
        }
    }

    #[test]
    fn concurrent_first_uses_agree_on_one_key() {
        let directory = tempfile::tempdir().unwrap();
        let keys: Vec<[u8; KEY_BYTES]> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..16).map(|_| scope.spawn(|| futures::executor::block_on(LocalKeyService::new(directory.path()).unwrap().key("raced")).unwrap())).collect();
            handles.into_iter().map(|handle| handle.join().unwrap()).collect()
        });
        assert!(keys.windows(2).all(|pair| pair[0] == pair[1]), "every racer uses the key that was stored");
    }
}
