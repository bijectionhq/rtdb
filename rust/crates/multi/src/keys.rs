//! Per-deployment data keys (`KeyService`): each deployment's objects are encrypted under its own key, and destroying the key is what deletion proves.

use std::path::PathBuf;

use ring::rand::{SecureRandom, SystemRandom};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

pub const KEY_BYTES: usize = 32;

pub trait KeyService: Send + Sync {
    /// The deployment's key, created on first use.
    fn key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]>;
    /// Gives `to` the key `from` has, for a fork whose objects are copies of its parent's.
    fn copy(&self, from: &str, to: &str) -> Result<()>;
    /// Destroys the deployment's key irreversibly; its objects are unreadable from then on.
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
}

impl KeyService for LocalKeyService {
    fn key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]> {
        let path = self.path(deployment);
        match std::fs::read(&path) {
            Ok(bytes) => bytes.try_into().map_err(|_| Anomaly::fault("rtdb.multi/key-corrupt", format!("The key of {deployment} is malformed"))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut key = [0u8; KEY_BYTES];
                SystemRandom::new().fill(&mut key).map_err(|_| Anomaly::fault("rtdb.multi/random", "No randomness for a new key"))?;
                let temporary = path.with_extension("tmp");
                std::fs::write(&temporary, key).map_err(io)?;
                // Two nodes racing to create a key must agree on one: the first rename wins and the loser reads it.
                match std::fs::hard_link(&temporary, &path) {
                    Ok(()) => { let _ = std::fs::remove_file(&temporary); Ok(key) }
                    Err(_) => { let _ = std::fs::remove_file(&temporary); self.key(deployment) }
                }
            }
            Err(error) => Err(io(error)),
        }
    }
    fn copy(&self, from: &str, to: &str) -> Result<()> {
        let key = self.key(from)?;
        std::fs::write(self.path(to), key).map_err(io)
    }
    fn install(&self, deployment: &str, key: [u8; KEY_BYTES]) -> Result<()> { std::fs::write(self.path(deployment), key).map_err(io) }
    fn destroy(&self, deployment: &str) -> Result<()> {
        let path = self.path(deployment);
        if path.exists() {
            std::fs::write(&path, [0u8; KEY_BYTES]).map_err(io)?;
            std::fs::remove_file(&path).map_err(io)?;
        }
        Ok(())
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
        assert_ne!(keys.key("a").unwrap(), key, "a destroyed key is never recovered");
        assert_eq!(keys.key("b").unwrap(), key, "a fork's copy survives its parent's deletion");
    }
}
