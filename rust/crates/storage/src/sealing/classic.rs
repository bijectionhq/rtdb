//! File-backed key custody for an explicitly enrolled Classic object root.

use std::{
    fs::{self, File},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use ring::{
    digest::{SHA256, digest},
    rand::{SecureRandom, SystemRandom},
};
use rtdb_types::{Anomaly, Result};
use uuid::Uuid;

use super::{EncryptingStore, ObjectCipher, UnitOperation, UnitOutput, cipher::Cipher};
use crate::{Error, IoRegion, ObjectKey, ObjectStore, sync_directory};

const OWNER: &str = "~rtdb-classic-key-lineage";
const KEY_BYTES: usize = 32;
pub const KAFKA_LOG_SEAL_OVERHEAD: usize = LOG_SEAL_OVERHEAD;
pub(crate) const LOG_SEAL_OVERHEAD: usize = 16 + super::cipher::SEALED_OVERHEAD;

#[derive(Clone)]
pub struct ClassicKeyVault(Arc<Path>);

impl ClassicKeyVault {
    /// The vault path belongs to the original key owner and is excluded from payload backups.
    pub fn open(path: &Path) -> Result<Self> {
        let path = path.canonicalize().map_err(io(path))?;
        let metadata = fs::metadata(&path).map_err(io(&path))?;
        if !metadata.is_dir() {
            return Err(invalid("The Classic key vault is not a directory"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(invalid("The Classic key vault directory must be private"));
            }
        }
        Ok(Self(path.into()))
    }

    pub fn require_separate_path(&self, payload: &Path) -> Result<()> {
        let payload = payload.canonicalize().map_err(io(payload))?;
        if payload.starts_with(self.0.as_ref()) || self.0.starts_with(payload) {
            return Err(invalid(
                "The Classic key vault must be outside the payload tree",
            ));
        }
        Ok(())
    }

    pub async fn require_unenrolled(store: &dyn ObjectStore) -> Result<()> {
        let marker = ObjectKey::new(OWNER)?;
        match store.head_fresh(&marker).await {
            Err(Error::MissingObject(_)) => Ok(()),
            Ok(_) => Err(invalid("The Classic root requires its original key vault")),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn units(
        &self,
        store: &dyn ObjectStore,
        region: Arc<IoRegion>,
        allow_enrollment: bool,
    ) -> Result<Arc<ClassicObjectKeys>> {
        let marker = ObjectKey::new(OWNER)?;
        let anchor = match store.get_fresh_exact(&marker, 16).await {
            Ok(bytes) => bytes,
            Err(Error::MissingObject(_)) if allow_enrollment => {
                let objects = store.list(None).await?;
                if objects.iter().any(|object| object.key != marker) {
                    return Err(invalid(
                        "Existing Classic data has no immutable key lineage",
                    ));
                }
                let vault = self.clone();
                let proposal = region
                    .run_blocking(move || {
                        let id = Uuid::new_v4();
                        let scope = vault.0.join(id.simple().to_string());
                        fs::create_dir(&scope).map_err(io(&scope))?;
                        sync_directory(&vault.0).map_err(io(&vault.0))?;
                        Ok(id)
                    })
                    .await?;
                let publication = store
                    .put(&marker, Bytes::copy_from_slice(proposal.as_bytes()))
                    .await;
                match store.get_fresh_exact(&marker, 16).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        publication?;
                        return Err(error.into());
                    }
                }
            }
            Err(error) => return Err(error.into()),
        };
        let id = Uuid::from_slice(&anchor)
            .map_err(|_| invalid("The Classic key lineage is malformed"))?;
        let path = self.0.join(id.simple().to_string());
        if !path.is_dir() {
            return Err(invalid("The original Classic key authority is unavailable"));
        }
        Ok(Arc::new(ClassicObjectKeys {
            path: path.into(),
            region,
        }))
    }

    pub fn log_keys(
        &self,
        path: &Path,
        allow_enrollment: bool,
        region: Arc<IoRegion>,
    ) -> Result<Arc<ClassicObjectKeys>> {
        let parent = path
            .parent()
            .ok_or_else(|| invalid("The Classic log has no owner directory"))?;
        if allow_enrollment {
            fs::create_dir_all(parent).map_err(io(parent))?;
        }
        self.require_separate_path(parent)?;
        let marker = path.with_extension("key-lineage");
        let read = || -> Result<Option<Uuid>> {
            match fs::read(&marker) {
                Ok(bytes) => {
                    Ok(Some(Uuid::from_slice(&bytes).map_err(|_| {
                        invalid("The Classic log lineage is malformed")
                    })?))
                }
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
                Err(error) => Err(io(&marker)(error).into()),
            }
        };
        let id = match read()? {
            Some(id) => id,
            None if allow_enrollment => {
                let _gate = lock(&marker)?;
                if let Some(id) = read()? {
                    id
                } else {
                    match fs::metadata(path) {
                        Ok(metadata) if metadata.len() == 0 => {}
                        Ok(_) => {
                            return Err(invalid(
                                "Existing Classic log data has no immutable key lineage",
                            ));
                        }
                        Err(error) if error.kind() == ErrorKind::NotFound => {}
                        Err(error) => return Err(io(path)(error).into()),
                    }
                    let id = Uuid::new_v4();
                    let scope = self.0.join(id.simple().to_string());
                    fs::create_dir(&scope).map_err(io(&scope))?;
                    sync_directory(&self.0).map_err(io(&self.0))?;
                    let mut temporary =
                        tempfile::NamedTempFile::new_in(parent).map_err(io(parent))?;
                    temporary.write_all(id.as_bytes()).map_err(io(&marker))?;
                    crate::sync_file(temporary.as_file()).map_err(io(&marker))?;
                    temporary
                        .persist_noclobber(&marker)
                        .map_err(|error| io(&marker)(error.error))?;
                    sync_directory(parent).map_err(io(parent))?;
                    id
                }
            }
            None => return Err(invalid("The Classic log has no enrolled key lineage")),
        };
        let namespace = self.0.join(id.simple().to_string());
        if !namespace.is_dir() {
            return Err(invalid(
                "The original Classic log key authority is unavailable",
            ));
        }
        Ok(Arc::new(ClassicObjectKeys {
            path: namespace.into(),
            region,
        }))
    }

    /// Kafka's immutable topic ID selects a vault lineage. Only the caller
    /// that created the still-empty topic may enroll its vault namespace.
    pub fn kafka_log_keys(&self, topic: [u8; 16], enroll: bool, region: Arc<IoRegion>) -> Result<Arc<ClassicObjectKeys>> {
        let topic = Uuid::from_bytes(topic);
        if topic.is_nil() {
            return Err(invalid("A Classic Kafka log must have a broker topic identity"));
        }
        let path = self.0.join(format!("kafka-{}", topic.simple()));
        if enroll {
            match fs::create_dir(&path) {
                Ok(()) => sync_directory(&self.0).map_err(io(&self.0))?,
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(io(&path)(error).into()),
            }
        }
        if !path.is_dir() {
            return Err(invalid("The Classic Kafka topic requires its original enrolled key vault"));
        }
        Ok(Arc::new(ClassicObjectKeys { path: path.into(), region }))
    }

    pub fn require_unsealed_log(path: &Path) -> Result<()> {
        if path
            .with_extension("key-lineage")
            .try_exists()
            .map_err(io(path))?
        {
            return Err(invalid("The Classic log requires its original key vault"));
        }
        Ok(())
    }

    pub fn wrap(
        store: Arc<dyn ObjectStore>,
        keys: Arc<ClassicObjectKeys>,
    ) -> Result<EncryptingStore> {
        Ok(EncryptingStore::with_units(store, keys).with_classic_control(ObjectKey::new(OWNER)?))
    }
}

pub struct ClassicObjectKeys {
    path: Arc<Path>,
    region: Arc<IoRegion>,
}

impl ClassicObjectKeys {
    fn name(&self, key: &ObjectKey) -> PathBuf {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        let digest = digest(&SHA256, key.as_str().as_bytes());
        self.path.join(URL_SAFE_NO_PAD.encode(digest.as_ref()))
    }

    /// Settles the slot at the original vault after the residence has excluded all uses.
    pub async fn withdraw(&self, key: &ObjectKey) -> crate::Result<()> {
        self.withdraw_units(std::slice::from_ref(key)).await
    }

    pub(crate) async fn withdraw_units(&self, keys: &[ObjectKey]) -> crate::Result<()> {
        if keys.is_empty() { return Ok(()); }
        let paths = keys.iter().map(|key| {
            if key.as_str() == OWNER { return Err(Error::InvalidKey(key.to_string())); }
            Ok(self.name(key))
        }).collect::<crate::Result<Vec<_>>>()?;
        self.region.run_blocking(move || {
            for path in paths { withdraw_slot(&path)?; }
            Ok(())
        }).await?;
        super::drain_unit_delivery().await;
        Ok(())
    }
}

fn withdraw_slot(path: &Path) -> crate::Result<()> {
    let _lock = lock(path)?;
    let fence = path.with_extension("closed");
    match File::options().write(true).create_new(true).open(&fence) {
        Ok(file) => crate::sync_file(&file).map_err(io(&fence))?,
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
        Err(error) => return Err(io(&fence)(error)),
    }
    let parent = path.parent().expect("the slot has its vault directory");
    sync_directory(parent).map_err(io(parent))?;
    for file in [path, path.with_extension("pending").as_path()] {
        match fs::remove_file(file) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(io(file)(error)),
        }
    }
    sync_directory(parent).map_err(io(parent))
}

#[async_trait::async_trait]
impl ObjectCipher for ClassicObjectKeys {
    async fn withdraw(&self, key: &ObjectKey) -> crate::Result<()> {
        ClassicObjectKeys::withdraw(self, key).await
    }

    async fn withdraw_all(&self, keys: &[ObjectKey]) -> crate::Result<()> {
        self.withdraw_units(keys).await
    }

    async fn operate(
        &self,
        key: &ObjectKey,
        operation: UnitOperation,
    ) -> crate::Result<UnitOutput> {
        if key.as_str() == OWNER {
            return Err(Error::InvalidKey(key.to_string()));
        }
        let path = self.name(key);
        let key = key.clone();
        self.region
            .run_blocking(move || {
                with_key(&path, operation.provisions(), |cipher| {
                    operation.apply(cipher, &key)
                })
            })
            .await
    }
}

impl ClassicObjectKeys {
    pub async fn seal_kafka_record(&self, plaintext: Bytes) -> crate::Result<Bytes> {
        let publication = Uuid::new_v4();
        let name = ObjectKey::new(format!("frames/{publication}"))?;
        let associated = format!("rtdb.classic.kafka-frame/v1/{publication}");
        let ciphertext = self.operate(&name, UnitOperation::SealValue(associated, plaintext)).await?.bytes()?;
        let mut frame = Vec::with_capacity(16 + ciphertext.len());
        frame.extend_from_slice(publication.as_bytes());
        frame.extend_from_slice(&ciphertext);
        Ok(frame.into())
    }

    pub async fn open_kafka_record(&self, frame: Bytes) -> crate::Result<Bytes> {
        let publication = frame.get(..16).and_then(|prefix| Uuid::from_slice(prefix).ok())
            .filter(|publication| !publication.is_nil())
            .ok_or_else(|| Error::Task("Kafka frame has no sealed publication identity".into()))?;
        let name = ObjectKey::new(format!("frames/{publication}"))?;
        let associated = format!("rtdb.classic.kafka-frame/v1/{publication}");
        self.operate(&name, UnitOperation::OpenValue(associated, frame.slice(16..))).await?.bytes()
    }

    pub(crate) fn seal_record(
        &self,
        offset: i64,
        timestamp: i64,
        payload: &[u8],
    ) -> crate::Result<Bytes> {
        let publication = Uuid::new_v4();
        let name = ObjectKey::new(format!("frames/{publication}"))?;
        let associated = format!("rtdb.classic.log-frame/v1/{publication}/{offset}/{timestamp}");
        let ciphertext = with_key(&self.name(&name), true, |cipher| {
            cipher.seal(&associated, payload)
        })?;
        let mut sealed = Vec::with_capacity(16 + ciphertext.len());
        sealed.extend_from_slice(publication.as_bytes());
        sealed.extend_from_slice(&ciphertext);
        Ok(sealed.into())
    }

    pub(crate) fn open_record(
        &self,
        offset: i64,
        timestamp: i64,
        payload: &[u8],
    ) -> crate::Result<Bytes> {
        let (publication, ciphertext) = payload.split_at_checked(16).ok_or_else(|| {
            Error::Task("The sealed Classic log frame has no publication identity".into())
        })?;
        let publication = Uuid::from_slice(publication)
            .map_err(|_| Error::Task("Invalid log publication identity".into()))?;
        let name = ObjectKey::new(format!("frames/{publication}"))?;
        let associated = format!("rtdb.classic.log-frame/v1/{publication}/{offset}/{timestamp}");
        with_key(&self.name(&name), false, |cipher| {
            cipher.open(&associated, ciphertext)
        })
    }

    pub(crate) fn retire_record(&self, offset: i64, timestamp: i64, sealed: &[u8]) -> crate::Result<()> {
        let (publication, _) = sealed.split_at_checked(16)
            .ok_or_else(|| Error::Task("The sealed Classic frame has no publication identity".into()))?;
        let publication = Uuid::from_slice(publication)
            .map_err(|_| Error::Task("Invalid log publication identity".into()))?;
        let slot = self.name(&ObjectKey::new(format!("frames/{publication}"))?);
        let marker = slot.with_extension("retirement");
        let mut binding = ring::digest::Context::new(&SHA256);
        binding.update(b"rtdb.classic.log-retirement/v1\0");
        binding.update(&offset.to_be_bytes());
        binding.update(&timestamp.to_be_bytes());
        binding.update(sealed);
        let binding = binding.finish();
        match fs::read(&marker) {
            Ok(prior) if prior == binding.as_ref() => {}
            Ok(_) => return Err(Error::Task("The sealed log retirement intent differs".into())),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                // Authenticate this offset and its original publication before
                // recording authority to destroy its slot.
                self.open_record(offset, timestamp, sealed)?;
                let mut intent = tempfile::NamedTempFile::new_in(self.path.as_ref()).map_err(io(&marker))?;
                intent.write_all(binding.as_ref()).map_err(io(&marker))?;
                crate::sync_file(intent.as_file()).map_err(io(&marker))?;
                intent.persist_noclobber(&marker).map_err(|error| io(&marker)(error.error))?;
                sync_directory(self.path.as_ref()).map_err(io(&marker))?;
            }
            Err(error) => return Err(io(&marker)(error)),
        }
        withdraw_slot(&slot)
    }

}

fn with_key<T>(
    path: &Path,
    provision: bool,
    operate: impl FnOnce(&Cipher) -> crate::Result<T>,
) -> crate::Result<T> {
    let _lock = lock(path)?;
    if path
        .with_extension("closed")
        .try_exists()
        .map_err(io(path))?
    {
        return Err(Error::ErasedUnderView);
    }
    let mut material: [u8; KEY_BYTES] = match fs::read(path) {
        Ok(bytes) => bytes
            .try_into()
            .map_err(|_| Error::Task("Invalid Classic unit key".into()))?,
        Err(error) if error.kind() == ErrorKind::NotFound && provision => create(path)?,
        Err(error) => return Err(io(path)(error)),
    };
    let cipher = Cipher::new(&material).map_err(|error| Error::Task(error.to_string()));
    material.fill(0);
    operate(&cipher?)
}

fn lock(path: &Path) -> crate::Result<File> {
    let lock = path.with_extension("lock");
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock)
        .map_err(io(&lock))?;
    file.lock().map_err(io(&lock))?;
    Ok(file)
}

fn create(path: &Path) -> crate::Result<[u8; KEY_BYTES]> {
    let mut material = [0; KEY_BYTES];
    SystemRandom::new()
        .fill(&mut material)
        .map_err(|_| Error::Task("No randomness for the Classic unit key".into()))?;
    let temporary = path.with_extension("pending");
    match fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(io(&temporary)(error)),
    }
    let mut options = File::options();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary).map_err(io(&temporary))?;
    file.write_all(&material).map_err(io(&temporary))?;
    crate::sync_file(&file).map_err(io(&temporary))?;
    drop(file);
    fs::rename(&temporary, path).map_err(io(path))?;
    let parent = path.parent().expect("the slot has its vault directory");
    sync_directory(parent).map_err(io(parent))?;
    Ok(material)
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::incorrect("rtdb.storage/classic-key-vault", message)
}

fn io(path: &Path) -> impl FnOnce(std::io::Error) -> Error + '_ {
    |source| Error::Io {
        path: path.to_owned(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;

    #[tokio::test]
    async fn kafka_frames_require_the_enrolled_topic_and_original_vault() -> Result<()> {
        let original = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let vault = ClassicKeyVault::open(original.path())?;
        let other = ClassicKeyVault::open(other.path())?;
        let topic = Uuid::new_v4().into_bytes();
        let io = IoRegion::new();
        let keys = vault.kafka_log_keys(topic, true, Arc::clone(&io))?;
        let body = Bytes::from_static(b"sealed Kafka record");
        let frame = keys.seal_kafka_record(body.clone()).await?;
        assert_ne!(frame, body);
        assert!(other.kafka_log_keys(topic, false, Arc::clone(&io)).is_err());
        let different = vault.kafka_log_keys(Uuid::new_v4().into_bytes(), true, Arc::clone(&io))?;
        assert!(different.open_kafka_record(frame.clone()).await.is_err());
        assert_eq!(vault.kafka_log_keys(topic, false, io)?.open_kafka_record(frame).await?, body);
        Ok(())
    }

    #[tokio::test]
    async fn an_enrolled_root_requires_its_original_vault_and_a_withdrawn_slot_stays_closed()
    -> Result<()> {
        let root = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for path in [root.path(), other.path()] {
                fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
            }
        }
        let vault = ClassicKeyVault::open(root.path())?;
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
        let io = IoRegion::new();
        let units = vault.units(store.as_ref(), Arc::clone(&io), true).await?;
        let sealed = ClassicKeyVault::wrap(Arc::clone(&store), Arc::clone(&units))?;
        let key = ObjectKey::new("blocks/root")?;
        let body = Bytes::from_static(b"root");
        sealed.put(&key, body.clone()).await?;
        assert_ne!(store.get(&key).await?, body);
        assert_eq!(sealed.get(&key).await?, body);
        assert!(
            ClassicKeyVault::open(other.path())?
                .units(store.as_ref(), Arc::clone(&io), true)
                .await
                .is_err()
        );
        units.withdraw(&key).await?;
        assert!(matches!(
            sealed.get(&key).await,
            Err(Error::ErasedUnderView)
        ));
        assert!(matches!(
            sealed.put(&key, body).await,
            Err(Error::ErasedUnderView)
        ));
        Ok(())
    }
}
