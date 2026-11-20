//! Per-deployment data keys (`KeyService`): each deployment's objects are encrypted under its own key, and destroying the key is what deletion proves.

use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use ring::rand::{SecureRandom, SystemRandom};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

pub const KEY_BYTES: usize = 32;

pub trait KeyService: Send + Sync {
    /// A durable identity for the namespace in which this service creates and destroys keys.
    fn namespace_id(&self) -> Result<[u8; KEY_BYTES]>;
    /// The deployment's key, created on first use.
    fn key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]>;
    /// Gives `to` the key `from` has, for a fork whose objects are copies of its parent's.
    fn copy(&self, from: &str, to: &str) -> Result<()>;
    /// Removes this service's copy of the deployment key.
    fn destroy(&self, deployment: &str) -> Result<()>;
    /// Checks whether an incoming key could be installed without replacing a different key.
    fn validate_install(&self, deployment: &str, key: &[u8; KEY_BYTES]) -> Result<()>;
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

    fn copy(&self, from: &str, to: &str) -> Result<()> { self.install(to, self.key(from)?) }

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
        match std::fs::remove_file(self.path(deployment)) {
            Ok(()) => self.sync_directory(),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io(error)),
        }
    }
}

fn require_matching_key(deployment: &str, key: &[u8; KEY_BYTES], installed: &[u8; KEY_BYTES]) -> Result<()> {
    if key != installed {
        return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/key-conflict", format!("The key for {deployment} is already installed with a different value")));
    }
    Ok(())
}

fn read_key(path: &Path, bytes: Vec<u8>) -> Result<[u8; KEY_BYTES]> {
    bytes.try_into().map_err(|_| Anomaly::fault("rtdb.multi/key-corrupt", format!("{} has an invalid key length", path.display())))
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

    #[test]
    fn destroying_a_key_unlinks_only_the_selected_copy() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        let key = keys.key("source").unwrap();
        std::fs::hard_link(keys.path("source"), keys.path("copy")).unwrap();
        keys.destroy("source").unwrap();
        keys.destroy("source").unwrap();
        assert_eq!(keys.key("copy").unwrap(), key);
        assert!(!keys.path("source").exists());
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
    fn concurrent_first_uses_agree_on_one_key() {
        let directory = tempfile::tempdir().unwrap();
        let keys: Vec<[u8; KEY_BYTES]> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..16).map(|_| scope.spawn(|| LocalKeyService::new(directory.path()).unwrap().key("raced").unwrap())).collect();
            handles.into_iter().map(|handle| handle.join().unwrap()).collect()
        });
        assert!(keys.windows(2).all(|pair| pair[0] == pair[1]), "every racer uses the key that was stored");
    }
}
