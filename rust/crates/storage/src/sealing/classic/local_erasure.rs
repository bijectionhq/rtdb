//! Original-vault enrollment and generation admission for a sealed Local residence.

use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::io;
use crate::{Error, Result, sync_directory, sync_file};

pub(super) const LINEAGE_VERSION: u8 = 1;
pub(super) const KAFKA_LINEAGE_VERSION: u8 = 2;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Directory {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl Directory {
    #[cfg(unix)]
    fn open(path: &Path) -> Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let path = path.canonicalize().map_err(io(path))?;
        let metadata = path.metadata().map_err(io(&path))?;
        if !metadata.is_dir() { return Err(invalid()); }
        Ok(Self { path, device: metadata.dev(), inode: metadata.ino() })
    }

    #[cfg(not(unix))]
    fn open(_: &Path) -> Result<Self> { Err(invalid()) }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalErasureResidence {
    payload: Directory,
    journal: Option<Directory>,
    checkpoint_vault: Directory,
    log_epoch: i32,
}

impl LocalErasureResidence {
    pub fn open(payload: &Path, journal: &Path, checkpoint_vault: &Path, log_epoch: i32) -> Result<Self> {
        Self::open_residence(payload, Some(journal), checkpoint_vault, log_epoch)
    }

    pub fn open_kafka(payload: &Path, checkpoint_vault: &Path, log_epoch: i32) -> Result<Self> {
        Self::open_residence(payload, None, checkpoint_vault, log_epoch)
    }

    fn open_residence(payload: &Path, journal: Option<&Path>, checkpoint_vault: &Path, log_epoch: i32) -> Result<Self> {
        let payload = Directory::open(payload)?;
        let journal = journal.map(Directory::open).transpose()?;
        let checkpoint_vault = Directory::open(checkpoint_vault)?;
        let paths: Vec<_> = [&payload, &checkpoint_vault].into_iter().chain(journal.iter()).map(|dir| &dir.path).collect();
        if log_epoch < 0 || paths.iter().enumerate().any(|(i, path)| {
            paths.iter().skip(i + 1).any(|other| path.starts_with(other) || other.starts_with(path))
        }) { return Err(invalid()); }
        Ok(Self { payload, journal, checkpoint_vault, log_epoch })
    }

    pub(super) fn version(&self) -> u8 {
        if self.journal.is_some() { LINEAGE_VERSION } else { KAFKA_LINEAGE_VERSION }
    }

    pub(super) fn enroll_if_pristine(&self, scope: &Path) -> Result<bool> {
        let Some(parent) = self.payload.path.parent() else { return Err(invalid()); };
        let payload_name = self.payload.path.file_name().ok_or_else(invalid)?;
        let boot_name = std::ffi::OsStr::new(".rtdb-local-residence");
        for entry in fs::read_dir(parent).map_err(io(parent))? {
            let file = entry.map_err(io(parent))?.file_name();
            if file != payload_name && file != boot_name { return Ok(false); }
        }
        if !empty(&self.payload.path)? || self.journal.as_ref().map(|journal| empty(&journal.path)).transpose()? == Some(false) {
            return Ok(false);
        }
        persist(scope, "local-erasure-residence", &serde_json::to_vec(self).map_err(|_| invalid())?)?;
        Ok(true)
    }

    pub(super) fn reopen(self, scope: PathBuf, owner: Uuid) -> Result<LocalErasureCustody> {
        let expected = read(&scope.join("local-erasure-residence"), 16 << 10)?.ok_or_else(invalid)?;
        let enrolled: Self = serde_json::from_slice(&expected).map_err(|_| invalid())?;
        if enrolled != self { return Err(invalid()); }
        Ok(LocalErasureCustody { scope, journal: self.journal.map(|journal| journal.path), owner })
    }
}

fn empty(path: &Path) -> Result<bool> {
    match fs::read_dir(path).map_err(io(path))?.next() {
        Some(entry) => { entry.map_err(io(path))?; Ok(false) }
        None => Ok(true),
    }
}

/// Keeps a sealed Local residence from being enrolled while another attachment owns it.
pub fn open_local_residence(parent: &Path, exclusive: bool) -> Result<File> {
    let path = parent.join(".rtdb-local-residence");
    let file = lock_file(&path)?;
    let lock = if exclusive { FileExt::try_lock_exclusive(&file) } else { FileExt::try_lock_shared(&file) };
    lock.map_err(io(&path))?;
    sync_directory(parent).map_err(io(parent))?;
    Ok(file)
}

/// Durable admission and drain under the original Classic object vault.
pub struct LocalErasureCustody {
    scope: PathBuf,
    journal: Option<PathBuf>,
    owner: Uuid,
}

impl LocalErasureCustody {
    pub fn kafka_owner(&self) -> Option<Uuid> { self.journal.is_none().then_some(self.owner) }

    pub fn bound_kafka_topics(&self) -> Result<Option<[Uuid; 2]>> {
        if self.kafka_owner().is_none() { return Err(invalid()); }
        let _gate = self.gate()?;
        let Some(bytes) = read(&self.scope.join("local-erasure-journals"), 32)? else { return Ok(None); };
        let (source, replica) = bytes.split_at_checked(16).ok_or_else(invalid)?;
        Ok(Some([Uuid::from_slice(source).map_err(|_| invalid())?, Uuid::from_slice(replica).map_err(|_| invalid())?]))
    }

    pub fn bind(&self, database: Uuid, source: Uuid, replica: Uuid) -> Result<()> {
        let _gate = self.gate()?;
        if source.is_nil() || replica.is_nil() || source == replica { return Err(invalid()); }
        let mut identities = Vec::with_capacity(64);
        let vault = self.scope.parent().ok_or_else(invalid)?;
        if let Some(journal) = &self.journal {
            for log in ["LOG", "REPLICA_LOG"] {
                let marker = journal.join(log).with_extension("key-lineage");
                let bytes = read(&marker, 16)?.ok_or_else(invalid)?;
                let lineage = Uuid::from_slice(&bytes).map_err(|_| invalid())?;
                if !vault.join(lineage.simple().to_string()).is_dir() { return Err(invalid()); }
                identities.extend_from_slice(&bytes);
            }
            if identities[..16] == identities[16..32] { return Err(invalid()); }
        } else {
            for (topic, replica) in [(source, false), (replica, true)] {
                super::require_kafka_erasure_owner(vault, topic, Some((self.owner, replica)))?;
            }
        }
        identities.extend_from_slice(source.as_bytes());
        identities.extend_from_slice(replica.as_bytes());
        persist(&self.scope, "local-erasure-journals", &identities)?;
        persist(&self.scope, "local-erasure-database", database.as_bytes())
    }

    pub fn admit(&self, database: Uuid, generation: u64) -> Result<File> {
        let _gate = self.gate()?;
        self.require_database(database, generation)?;
        let name = generation_name(generation, "admission");
        if read(&self.scope.join(name), 0)?.is_some() {
            return Err(Error::Closed("The original Local generation is closed".into()));
        }
        let path = self.scope.join(generation_name(generation, "readers"));
        let file = lock_file(&path)?;
        FileExt::lock_shared(&file).map_err(io(&path))?;
        Ok(file)
    }

    pub fn seal_admission(&self, database: Uuid, generation: u64) -> Result<()> {
        let _gate = self.gate()?;
        self.require_database(database, generation)?;
        persist(&self.scope, &generation_name(generation, "admission"), &[])
    }

    pub fn bind_cut(&self, database: Uuid, generation: u64, commitment: [u8; 32]) -> Result<()> {
        let _gate = self.gate()?;
        self.require_database(database, generation)?;
        if read(&self.scope.join(generation_name(generation, "admission")), 0)?.is_none() { return Err(invalid()); }
        persist(&self.scope, &generation_name(generation, "closed"), &commitment)
    }

    /// Records the drain while holding exclusive custody of the closed generation.
    pub fn finish_drain(&self, database: Uuid, generation: u64, commitment: [u8; 32]) -> Result<bool> {
        let _gate = self.gate()?;
        self.require_database(database, generation)?;
        if !matches_commitment(&self.scope, generation, "closed", commitment)? { return Err(invalid()); }
        let path = self.scope.join(generation_name(generation, "readers"));
        let file = lock_file(&path)?;
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => {
                persist(&self.scope, &generation_name(generation, "drained"), &commitment)?;
                Ok(true)
            }
            Err(error) if error.kind() == fs2::lock_contended_error().kind() => Ok(false),
            Err(error) => Err(io(&path)(error)),
        }
    }

    pub fn verifies_drain(&self, database: Uuid, generation: u64, commitment: [u8; 32]) -> Result<bool> {
        let _gate = self.gate()?;
        self.require_database(database, generation)?;
        Ok(read(&self.scope.join(generation_name(generation, "admission")), 0)?.is_some()
            && matches_commitment(&self.scope, generation, "closed", commitment)?
            && matches_commitment(&self.scope, generation, "drained", commitment)?)
    }

    pub fn had_drain(&self, database: Uuid, generation: u64) -> Result<bool> {
        let _gate = self.gate()?;
        self.require_database(database, generation)?;
        let Some(commitment) = read(&self.scope.join(generation_name(generation, "drained")), 32)? else { return Ok(false); };
        Ok(read(&self.scope.join(generation_name(generation, "admission")), 0)?.is_some()
            && matches_commitment(&self.scope, generation, "closed", commitment.try_into().map_err(|_| invalid())?)?)
    }

    fn gate(&self) -> Result<File> {
        let path = self.scope.join("local-erasure-gate");
        let file = lock_file(&path)?;
        FileExt::lock_exclusive(&file).map_err(io(&path))?;
        Ok(file)
    }

    fn require_database(&self, database: Uuid, generation: u64) -> Result<()> {
        if generation == 0 || read(&self.scope.join("local-erasure-database"), 16)?.as_deref() != Some(database.as_bytes().as_slice()) {
            return Err(invalid());
        }
        Ok(())
    }
}

fn generation_name(generation: u64, kind: &str) -> String { format!("local-erasure-{generation:016x}-{kind}") }

fn matches_commitment(scope: &Path, generation: u64, kind: &str, commitment: [u8; 32]) -> Result<bool> {
    Ok(read(&scope.join(generation_name(generation, kind)), 32)?.as_deref() == Some(commitment.as_slice()))
}

fn lock_file(path: &Path) -> Result<File> {
    OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path).map_err(io(path))
}

fn persist(scope: &Path, name: &str, expected: &[u8]) -> Result<()> {
    let path = scope.join(name);
    if let Some(existing) = read(&path, expected.len())? {
        if existing.as_slice() != expected { return Err(invalid()); }
        return sync_directory(scope).map_err(io(scope));
    }
    let mut file = tempfile::NamedTempFile::new_in(scope).map_err(io(scope))?;
    file.write_all(expected).map_err(io(&path))?;
    sync_file(file.as_file()).map_err(io(&path))?;
    file.persist_noclobber(&path).map_err(|error| io(&path)(error.error))?;
    sync_directory(scope).map_err(io(scope))
}

fn read(path: &Path, limit: usize) -> Result<Option<Vec<u8>>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io(path)(error)),
    };
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes).map_err(io(path))?;
    if bytes.len() > limit { return Err(invalid()); }
    Ok(Some(bytes))
}

fn invalid() -> Error { Error::Closed("The Local erasure custody does not match its sealed enrollment".into()) }

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn a_prior_journal_or_payload_root_prevents_sealed_from_birth_enrollment() {
        let base = tempfile::tempdir().unwrap();
        let vault = tempfile::tempdir().unwrap();
        let object_root = base.path().join("objects");
        let payload = object_root.join("v06");
        let journal = base.path().join("journal");
        fs::create_dir_all(&payload).unwrap();
        fs::create_dir(&journal).unwrap();
        let residence = LocalErasureResidence::open(&payload, &journal, vault.path(), 0).unwrap();
        fs::write(journal.join("LOG"), b"prior plaintext").unwrap();
        assert!(!residence.enroll_if_pristine(vault.path()).unwrap());
        fs::remove_file(journal.join("LOG")).unwrap();
        fs::create_dir(object_root.join("v05")).unwrap();
        assert!(!residence.enroll_if_pristine(vault.path()).unwrap());
        fs::remove_dir(object_root.join("v05")).unwrap();
        assert!(residence.enroll_if_pristine(vault.path()).unwrap());
        assert!(residence.clone().reopen(vault.path().to_owned(), Uuid::new_v4()).is_ok());
    }

    #[test]
    fn generation_admission_stays_closed_across_reopen_and_waits_for_existing_readers() {
        let directory = tempfile::tempdir().unwrap();
        let scope = directory.path().join("scope");
        fs::create_dir(&scope).unwrap();
        for name in ["LOG", "REPLICA_LOG"] {
            let id = Uuid::new_v4();
            fs::create_dir(directory.path().join(id.simple().to_string())).unwrap();
            fs::write(directory.path().join(name).with_extension("key-lineage"), id.as_bytes()).unwrap();
        }
        let owner = || LocalErasureCustody { scope: scope.clone(), journal: Some(directory.path().to_owned()), owner: Uuid::nil() };
        let database = Uuid::new_v4();
        let cut = [42; 32];
        owner().bind(database, Uuid::new_v4(), Uuid::new_v4()).unwrap();
        let read = owner().admit(database, 1).unwrap();
        owner().seal_admission(database, 1).unwrap();
        owner().bind_cut(database, 1, cut).unwrap();
        assert!(owner().admit(database, 1).is_err());
        assert!(!owner().finish_drain(database, 1, cut).unwrap());
        assert!(!owner().verifies_drain(database, 1, cut).unwrap());
        drop(read);
        assert!(owner().finish_drain(database, 1, cut).unwrap());
        assert!(owner().verifies_drain(database, 1, cut).unwrap());
        assert!(!owner().verifies_drain(database, 1, [43; 32]).unwrap());
        assert!(owner().admit(database, 2).is_ok());
    }

    #[test]
    fn kafka_residence_reopens_only_with_its_original_topic_pair_and_roles() -> rtdb_types::Result<()> {
        let base = tempfile::tempdir().unwrap();
        let vault_dir = crate::sealing::private_tempdir();
        let checkpoint = tempfile::tempdir().unwrap();
        let payload = base.path().join("v06");
        fs::create_dir(&payload).unwrap();
        let owner = Uuid::new_v4();
        let scope = vault_dir.path().join(owner.simple().to_string());
        fs::create_dir(&scope).unwrap();
        let residence = LocalErasureResidence::open_kafka(&payload, checkpoint.path(), 0)?;
        assert!(residence.enroll_if_pristine(&scope)?);
        let custody = residence.clone().reopen(scope.clone(), owner)?;
        let vault = super::super::ClassicKeyVault::open(vault_dir.path())?;
        let io = crate::IoRegion::new();
        let [source, replica, other] = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let database = Uuid::new_v4();
        assert_eq!(custody.bound_kafka_topics()?, None);
        vault.kafka_erasure_keys(source.into_bytes(), true, io.clone(), owner.into_bytes(), false)?;
        vault.kafka_erasure_keys(replica.into_bytes(), true, io.clone(), owner.into_bytes(), true)?;
        assert!(vault.kafka_log_keys(source.into_bytes(), false, io.clone()).is_err());
        assert!(!vault_dir.path().join(format!("kafka-{}", source.simple())).exists());
        assert!(vault.kafka_erasure_keys(replica.into_bytes(), false, io.clone(), owner.into_bytes(), false).is_err());
        assert!(custody.bind(database, replica, source).is_err());
        custody.bind(database, source, replica)?;
        let reopened = residence.reopen(scope, owner)?;
        assert_eq!(reopened.bound_kafka_topics()?, Some([source, replica]));
        let legacy = Uuid::new_v4().into_bytes();
        vault.kafka_log_keys(legacy, true, io.clone())?;
        assert!(vault.kafka_erasure_keys(legacy, true, io.clone(), owner.into_bytes(), false).is_err());
        vault.kafka_erasure_keys(other.into_bytes(), true, io, owner.into_bytes(), true)?;
        assert!(reopened.bind(database, source, other).is_err());
        Ok(())
    }
}
