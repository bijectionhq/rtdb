//! Per-deployment key provisioning, fork copies and durable withdrawal.

use std::fs::{self, File, TryLockError};
use std::io::{ErrorKind, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use ring::rand::{SecureRandom, SystemRandom};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

pub const KEY_BYTES: usize = 32;

#[async_trait::async_trait]
pub trait KeyService: Send + Sync {
    /// The deployment's key, created on first use.
    async fn key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]>;
    /// Read current custody without creating a key.
    async fn existing_key(&self, deployment: &str) -> Result<Option<[u8; KEY_BYTES]>>;
    /// Gives `to` the key `from` has, for a fork whose objects are copies of its parent's.
    async fn copy(&self, from: &str, to: &str) -> Result<()>;
    /// Withdraws the key and permanently refuses provisioning for this deployment ID.
    async fn destroy(&self, deployment: &str) -> Result<()>;
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
        File::open(&self.directory).and_then(|directory| directory.sync_all()).map_err(io)
    }
    fn local_key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]> {
        let path = self.path(deployment);
        let _lock = self.lock_key(&path)?;
        match fs::read(&path) {
            Ok(bytes) => {
                let key = decode_key(deployment, bytes)?;
                self.sync_directory()?;
                Ok(key)
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let key = new_key()?;
                self.publish(&path, &key)?;
                Ok(key)
            }
            Err(error) => Err(io(error)),
        }
    }
}

fn decode_key(deployment: &str, bytes: Vec<u8>) -> Result<[u8; KEY_BYTES]> {
    if bytes.is_empty() {
        return Err(Anomaly::incorrect("rtdb.multi/key-withdrawn", format!("The key of {deployment} was withdrawn")));
    }
    bytes.try_into().map_err(|_| malformed())
}

#[async_trait::async_trait]
impl KeyService for LocalKeyService {
    async fn key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]> {
        self.local_key(deployment)
    }

    async fn existing_key(&self, deployment: &str) -> Result<Option<[u8; KEY_BYTES]>> {
        match fs::read(self.path(deployment)) {
            Ok(bytes) => decode_key(deployment, bytes).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io(error)),
        }
    }

    async fn copy(&self, from: &str, to: &str) -> Result<()> {
        self.install(to, self.local_key(from)?).await
    }

    async fn install(&self, deployment: &str, key: [u8; KEY_BYTES]) -> Result<()> {
        let path = self.path(deployment);
        let _lock = self.lock_key(&path)?;
        match fs::read(&path) {
            Err(error) if error.kind() == ErrorKind::NotFound => self.publish(&path, &key),
            Err(error) => Err(io(error)),
            Ok(bytes) => {
                if decode_key(deployment, bytes)? != key {
                    return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/key-conflict", format!("A different key is already installed for {deployment}")));
                }
                self.sync_directory()
            }
        }
    }
    async fn destroy(&self, deployment: &str) -> Result<()> {
        let path = self.path(deployment);
        let _lock = self.lock_key(&path)?;
        if fs::metadata(&path).is_ok_and(|metadata| metadata.is_file() && metadata.len() == 0) {
            return self.sync_directory();
        }
        self.publish(&path, &[])
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

fn new_key() -> Result<[u8; KEY_BYTES]> {
    let mut key = [0u8; KEY_BYTES];
    SystemRandom::new().fill(&mut key).map_err(|_| Anomaly::fault("rtdb.multi/random", "No randomness for a new key"))?;
    Ok(key)
}

fn malformed() -> Anomaly { Anomaly::fault("rtdb.multi/key-corrupt", "Malformed deployment data key") }

#[cfg(feature = "dynamodb")]
pub mod dynamo;

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/keys", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;

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
    fn concurrent_first_uses_agree_on_one_key() {
        let directory = tempfile::tempdir().unwrap();
        let keys: Vec<[u8; KEY_BYTES]> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..16).map(|_| scope.spawn(|| LocalKeyService::new(directory.path()).unwrap().local_key("raced").unwrap())).collect();
            handles.into_iter().map(|handle| handle.join().unwrap()).collect()
        });
        assert!(keys.windows(2).all(|pair| pair[0] == pair[1]), "every racer uses the key that was stored");
    }
}
