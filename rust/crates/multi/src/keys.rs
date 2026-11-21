//! Per-deployment key provisioning, fork copies and durable withdrawal.

use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use ring::rand::{SecureRandom, SystemRandom};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

pub const KEY_BYTES: usize = 32;

pub trait KeyService: Send + Sync {
    /// The deployment's key, created on first use.
    fn key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]>;
    /// Gives `to` the key `from` has, for a fork whose objects are copies of its parent's.
    fn copy(&self, from: &str, to: &str) -> Result<()>;
    /// Withdraws the key and permanently refuses provisioning for this deployment ID.
    fn destroy(&self, deployment: &str) -> Result<()>;
    /// Installs a key brought from another cell's key service, for a deployment that moved here.
    fn install(&self, deployment: &str, key: [u8; KEY_BYTES]) -> Result<()>;
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

    fn publish(&self, path: &Path, contents: &[u8], withdraw: bool) -> Result<()> {
        let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4().simple()));
        let mut pending = File::options().write(true).create_new(true).open(&temporary).map_err(io)?;
        let published = (|| -> std::io::Result<()> {
            pending.write_all(contents)?;
            pending.sync_all()?;
            if withdraw {
                fs::rename(&temporary, path)?;
            } else {
                // Provisioning cannot overwrite another writer's key or undo withdrawal.
                match fs::hard_link(&temporary, path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
            }
            Ok(())
        })();
        drop(pending);
        match fs::remove_file(&temporary) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(io(error)),
        }
        published.map_err(io)?;
        File::open(&self.directory).and_then(|directory| directory.sync_all()).map_err(io)
    }
}

fn decode_key(deployment: &str, bytes: Vec<u8>) -> Result<[u8; KEY_BYTES]> {
    if bytes.is_empty() {
        return Err(Anomaly::incorrect("rtdb.multi/key-withdrawn", format!("The key of {deployment} was withdrawn")));
    }
    bytes.try_into().map_err(|_| Anomaly::fault("rtdb.multi/key-corrupt", format!("The key of {deployment} is malformed")))
}

impl KeyService for LocalKeyService {
    fn key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]> {
        let path = self.path(deployment);
        match fs::read(&path) {
            Ok(bytes) => decode_key(deployment, bytes),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let mut key = [0u8; KEY_BYTES];
                SystemRandom::new().fill(&mut key).map_err(|_| Anomaly::fault("rtdb.multi/random", "No randomness for a new key"))?;
                self.publish(&path, &key, false)?;
                decode_key(deployment, fs::read(&path).map_err(io)?)
            }
            Err(error) => Err(io(error)),
        }
    }
    fn copy(&self, from: &str, to: &str) -> Result<()> {
        self.install(to, self.key(from)?)
    }
    fn install(&self, deployment: &str, key: [u8; KEY_BYTES]) -> Result<()> {
        let path = self.path(deployment);
        self.publish(&path, &key, false)?;
        if decode_key(deployment, fs::read(&path).map_err(io)?)? != key {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/key-conflict", format!("A different key is already installed for {deployment}")));
        }
        Ok(())
    }
    fn destroy(&self, deployment: &str) -> Result<()> {
        self.publish(&self.path(deployment), &[], true)
    }
}

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/keys", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_is_stable_until_destroyed_and_a_copy_is_independent() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        let key = keys.key("a").unwrap();
        assert_eq!(keys.key("a").unwrap(), key);
        keys.copy("a", "b").unwrap();
        keys.destroy("a").unwrap();
        keys.destroy("a").unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        assert!(keys.key("a").is_err());
        assert!(keys.install("a", key).is_err());
        assert!(keys.copy("b", "a").is_err());
        assert_eq!(keys.key("b").unwrap(), key, "a fork's copy survives its parent's deletion");
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
