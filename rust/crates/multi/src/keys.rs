//! Per-deployment data keys (`KeyService`): each deployment's objects are encrypted under its own key, and destroying the key is what deletion proves.

use std::path::PathBuf;

use ring::rand::{SecureRandom, SystemRandom};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

pub const KEY_BYTES: usize = 32;

#[async_trait::async_trait]
pub trait KeyService: Send + Sync {
    /// The deployment's key, created on first use.
    async fn key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]>;
    /// Gives `to` the key `from` has, for a fork whose objects are copies of its parent's.
    async fn copy(&self, from: &str, to: &str) -> Result<()>;
    /// Destroys the deployment's key irreversibly; its objects are unreadable from then on.
    async fn destroy(&self, deployment: &str) -> Result<()>;
    /// Installs a key brought from another cell's key service, for a deployment that moved here.
    async fn install(&self, deployment: &str, key: [u8; KEY_BYTES]) -> Result<()>;
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

    fn local_key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]> {
        let path = self.path(deployment);
        match std::fs::read(&path) {
            Ok(bytes) => bytes.try_into().map_err(|_| malformed()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let key = new_key()?;
                let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4().simple()));
                std::fs::write(&temporary, key).map_err(io)?;
                let published = std::fs::hard_link(&temporary, &path);
                let _ = std::fs::remove_file(&temporary);
                match published {
                    Ok(()) => Ok(key),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists =>
                        std::fs::read(&path).map_err(io)?.try_into().map_err(|_| malformed()),
                    Err(error) => Err(io(error)),
                }
            }
            Err(error) => Err(io(error)),
        }
    }
}

#[async_trait::async_trait]
impl KeyService for LocalKeyService {
    async fn key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]> {
        self.local_key(deployment)
    }
    async fn copy(&self, from: &str, to: &str) -> Result<()> {
        let key = self.local_key(from)?;
        std::fs::write(self.path(to), key).map_err(io)
    }
    async fn install(&self, deployment: &str, key: [u8; KEY_BYTES]) -> Result<()> { std::fs::write(self.path(deployment), key).map_err(io) }
    async fn destroy(&self, deployment: &str) -> Result<()> {
        let path = self.path(deployment);
        if path.exists() {
            std::fs::write(&path, [0u8; KEY_BYTES]).map_err(io)?;
            std::fs::remove_file(&path).map_err(io)?;
        }
        Ok(())
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
        keys.destroy("a").await.unwrap();
        assert_ne!(keys.key("a").await.unwrap(), key, "a destroyed key is never recovered");
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
