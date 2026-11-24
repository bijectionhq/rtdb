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

mod kafka_references;
mod kafka_retirement;
mod local_erasure;
pub use local_erasure::{LocalErasureCustody, LocalErasureResidence, open_local_residence};
pub use kafka_retirement::KafkaPrefixRetirement;

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
        self.object_units(store, region, allow_enrollment, None).await
    }

    pub async fn local_units(
        &self,
        store: &crate::LocalStore,
        region: Arc<IoRegion>,
        allow_enrollment: bool,
        erasure: Option<LocalErasureResidence>,
    ) -> Result<Arc<ClassicObjectKeys>> {
        self.object_units(store, region, allow_enrollment, erasure).await
    }

    async fn object_units(
        &self,
        store: &dyn ObjectStore,
        region: Arc<IoRegion>,
        allow_enrollment: bool,
        erasure: Option<LocalErasureResidence>,
    ) -> Result<Arc<ClassicObjectKeys>> {
        let marker = ObjectKey::new(OWNER)?;
        let read = || async {
            let size = store.head_fresh(&marker).await?.size;
            if !matches!(size, 16 | 17) {
                return Err(Error::Task("The Classic key lineage is malformed".into()));
            }
            store.get_fresh_exact(&marker, size as usize).await
        };
        let anchor = match read().await {
            Ok(bytes) => bytes,
            Err(Error::MissingObject(_)) if allow_enrollment => {
                let objects = store.list(None).await?;
                if objects.iter().any(|object| object.key != marker) {
                    return Err(invalid(
                        "Existing Classic data has no immutable key lineage",
                    ));
                }
                let vault = self.clone();
                let proposed = erasure.clone();
                let proposal = region.run_blocking(move || {
                    let id = Uuid::new_v4();
                    let scope = vault.0.join(id.simple().to_string());
                    fs::create_dir(&scope).map_err(io(&scope))?;
                    let version = match proposed {
                        Some(residence) if residence.enroll_if_pristine(&scope)? => Some(residence.version()),
                        _ => None,
                    };
                    sync_directory(&vault.0).map_err(io(&vault.0))?;
                    let mut bytes = id.as_bytes().to_vec();
                    bytes.extend(version);
                    Ok(bytes)
                }).await?;
                let publication = store.put(&marker, Bytes::from(proposal)).await;
                match read().await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        publication?;
                        return Err(error.into());
                    }
                }
            }
            Err(error) => return Err(error.into()),
        };
        let id = Uuid::from_slice(&anchor[..16])
            .map_err(|_| invalid("The Classic key lineage is malformed"))?;
        let path = self.0.join(id.simple().to_string());
        if !path.is_dir() {
            return Err(invalid("The original Classic key authority is unavailable"));
        }
        let erasure = match anchor.get(16) {
            None => None,
            Some(&version) if matches!(version, local_erasure::LINEAGE_VERSION | local_erasure::KAFKA_LINEAGE_VERSION) => {
                let residence = erasure.ok_or_else(|| invalid("The enrolled Local root requires its original erasure custody"))?;
                if residence.version() != version { return Err(invalid("The Local root has another erasure journal provider")); }
                let path = path.clone();
                Some(Arc::new(region.run_blocking(move || residence.reopen(path, id)).await?))
            }
            _ => return Err(invalid("The Classic key lineage version is unsupported")),
        };
        Ok(Arc::new(ClassicObjectKeys { path: path.into(), region, erasure }))
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
            erasure: None,
        }))
    }

    /// Kafka's immutable topic ID selects a vault lineage. Only the caller
    /// that created the still-empty topic may enroll its vault namespace.
    pub fn kafka_log_keys(&self, topic: [u8; 16], enroll: bool, region: Arc<IoRegion>) -> Result<Arc<KafkaFrameKeys>> {
        self.kafka_topic_keys(topic, enroll, region, None)
    }

    pub fn kafka_erasure_keys(&self, topic: [u8; 16], enroll: bool, region: Arc<IoRegion>, owner: [u8; 16], replica: bool) -> Result<Arc<KafkaFrameKeys>> {
        self.kafka_topic_keys(topic, enroll, region, Some((Uuid::from_bytes(owner), replica)))
    }

    fn kafka_topic_keys(&self, topic: [u8; 16], enroll: bool, region: Arc<IoRegion>, owner: Option<(Uuid, bool)>) -> Result<Arc<KafkaFrameKeys>> {
        let topic = Uuid::from_bytes(topic);
        if topic.is_nil() {
            return Err(invalid("A Classic Kafka log must have a broker topic identity"));
        }
        let classic = self.0.join(format!("kafka-{}", topic.simple()));
        let erasure = kafka_erasure_scope(&self.0, topic);
        let _gate = (enroll || owner.is_some()).then(|| lock(&classic)).transpose()?;
        if let Some(owner) = owner
            && enroll
            && !classic.try_exists().map_err(io(&classic))?
            && !erasure.try_exists().map_err(io(&erasure))?
        {
            let staging = tempfile::tempdir_in(&self.0).map_err(io(&self.0))?;
            write_kafka_marker(&staging.path().join("owner"), &kafka_owner_bytes(owner), false)?;
            fs::rename(staging.path(), &erasure).map_err(io(&erasure))?;
            sync_directory(&self.0).map_err(io(&self.0))?;
        }
        require_kafka_erasure_owner(&self.0, topic, owner)?;
        if enroll && owner.is_none() {
            match fs::create_dir(&classic) {
                Ok(()) => sync_directory(&self.0).map_err(io(&self.0))?,
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(io(&classic)(error).into()),
            }
        }
        let path = if owner.is_some() { erasure } else { classic };
        if !path.is_dir() {
            return Err(invalid("The Classic Kafka topic requires its original enrolled key vault"));
        }
        Ok(Arc::new(KafkaFrameKeys(ClassicObjectKeys { path: path.into(), region, erasure: None })))
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
        let vault = Arc::clone(&keys.path);
        let erasure = keys.erasure.clone();
        Ok(EncryptingStore::with_units(store, keys).with_classic_control(ObjectKey::new(OWNER)?, vault)
            .with_local_erasure_custody(erasure))
    }
}

fn kafka_erasure_scope(vault: &Path, topic: Uuid) -> PathBuf {
    vault.join(format!("kafka-erasure-{}", topic.simple()))
}

fn kafka_owner_bytes((owner, replica): (Uuid, bool)) -> [u8; 17] {
    let mut bytes = [0; 17];
    bytes[..16].copy_from_slice(owner.as_bytes());
    bytes[16] = u8::from(replica);
    bytes
}

fn require_kafka_erasure_owner(vault: &Path, topic: Uuid, owner: Option<(Uuid, bool)>) -> crate::Result<()> {
    let path = kafka_erasure_scope(vault, topic).join("owner");
    let found: Option<[u8; 17]> = match fs::read(&path) {
        Ok(bytes) => Some(bytes.try_into().map_err(|_| kafka_owner_mismatch())?),
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => return Err(io(&path)(error)),
    };
    if found != owner.map(kafka_owner_bytes) {
        return Err(kafka_owner_mismatch());
    }
    Ok(())
}

fn kafka_owner_mismatch() -> Error {
    Error::Closed("The Kafka topic requires its original Default erasure residence".into())
}

pub struct ClassicObjectKeys {
    path: Arc<Path>,
    region: Arc<IoRegion>,
    erasure: Option<Arc<LocalErasureCustody>>,
}

impl ClassicObjectKeys {
    pub fn has_erasure_custody(&self) -> bool { self.erasure.is_some() }

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
        self.region
            .read(Arc::clone(&self.path), key.clone(), move |_, key| {
                if matches!(operation, UnitOperation::Grant) {
                    let custody = Arc::new(open_slot_lock(&path)?);
                    custody.lock_shared().map_err(io(&path))?;
                    if path.with_extension("closed").try_exists().map_err(io(&path))? {
                        return Err(Error::ErasedUnderView);
                    }
                    if fs::metadata(&path).map_err(io(&path))?.len() != KEY_BYTES as u64 {
                        return Err(Error::Task("Invalid Classic unit key".into()));
                    }
                    return Ok(UnitOutput::HeldGrant { until: None, custody });
                }
                with_key(&path, operation.provisions(), |cipher| {
                    operation.apply(cipher, key)
                })
            })
            .await
    }
}

pub struct KafkaFrameKeys(ClassicObjectKeys);

impl KafkaFrameKeys {
    pub async fn seal_kafka_record(&self, plaintext: Bytes) -> crate::Result<Bytes> {
        let publication = Uuid::new_v4();
        let name = ObjectKey::new(format!("frames/{publication}"))?;
        let associated = format!("rtdb.classic.kafka-frame/v1/{publication}");
        let ciphertext = self.0.operate(&name, UnitOperation::SealValue(associated, plaintext)).await?.bytes()?;
        let mut frame = Vec::with_capacity(16 + ciphertext.len());
        frame.extend_from_slice(publication.as_bytes());
        frame.extend_from_slice(&ciphertext);
        Ok(frame.into())
    }

    fn open_kafka_record(&self, frame: &[u8]) -> crate::Result<Bytes> {
        let publication = kafka_publication(frame)?;
        let name = ObjectKey::new(format!("frames/{publication}"))?;
        let associated = format!("rtdb.classic.kafka-frame/v1/{publication}");
        with_key(&self.0.name(&name), false, |cipher| cipher.open(&associated, &frame[16..]))
    }

    /// Persists the exact broker-acknowledged position before acknowledging it to RTDB.
    pub fn record_kafka_publication(&self, offset: i64, frame: &[u8]) -> crate::Result<()> {
        self.record_kafka_publication_and_reference(offset, frame, None)
    }

    pub fn record_kafka_publication_and_reference(
        &self, offset: i64, frame: &[u8], reference: Option<&crate::log::CheckpointReplicaReference>,
    ) -> crate::Result<()> {
        if offset < 0 {
            return Err(Error::Task("Invalid Kafka frame publication".into()));
        }
        let publication = kafka_publication(frame)?;
        let floor = self.0.path.join("retired-before");
        let _gate = lock(&floor)?;
        if offset < kafka_retired_before(&floor)? {
            return Err(Error::ErasedUnderView);
        }
        let erasure_floor = self.kafka_erasure_floor()?;
        if offset >= erasure_floor { self.check_retiring_slot(publication, erasure_floor)?; }
        self.record_publication_locked(offset, frame, offset >= erasure_floor)?;
        if let Some(reference) = reference.filter(|_| offset >= erasure_floor) {
            self.record_reference_locked(offset, frame, reference)?;
        }
        Ok(())
    }

    fn record_publication_locked(&self, offset: i64, frame: &[u8], retain_new: bool) -> crate::Result<()> {
        let index = self.0.path.join("positions");
        match fs::create_dir(&index) {
            Ok(()) => sync_directory(self.0.path.as_ref()).map_err(io(&index))?,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => (),
            Err(error) => return Err(io(&index)(error)),
        }
        let mut identity = [0; 48];
        identity[..16].copy_from_slice(&frame[..16]);
        identity[16..].copy_from_slice(digest(&SHA256, frame).as_ref());
        let marker = index.join(offset.to_string());
        match fs::read(&marker) {
            Ok(bytes) if bytes == identity => Ok(()),
            Ok(_) => Err(Error::Task("The Kafka position changed its sealed publication".into())),
            Err(error) if error.kind() == ErrorKind::NotFound && retain_new => write_kafka_marker(&marker, &identity, false),
            Err(error) if error.kind() == ErrorKind::NotFound => Err(Error::ErasedUnderView),
            Err(error) => Err(io(&marker)(error)),
        }
    }

    /// Repairs the census after an ambiguous broker acknowledgement while the record is retained.
    pub async fn open_kafka_publication(self: &Arc<Self>, offset: i64, frame: Bytes) -> crate::Result<Bytes> {
        self.open_kafka_publication_with_references(offset, frame, false).await
    }

    pub async fn open_kafka_publication_with_references(
        self: &Arc<Self>, offset: i64, frame: Bytes, capture_references: bool,
    ) -> crate::Result<Bytes> {
        self.0.region.read(Arc::clone(self), offset, move |keys, &offset| {
            let broker_floor = keys.0.path.join("retired-before");
            let _gate = lock(&broker_floor)?;
            let erasure_floor = keys.kafka_erasure_floor()?;
            let first_retained = kafka_retired_before(&broker_floor)?.max(erasure_floor);
            if offset < first_retained { return Err(Error::UnavailableLogRange { requested: offset, first_retained }); }
            keys.check_retiring_slot(kafka_publication(&frame)?, erasure_floor)?;
            let plaintext = keys.open_kafka_record(&frame)?;
            keys.record_publication_locked(offset, &frame, true)?;
            if capture_references && let Some(reference) = Self::replica_reference(&plaintext) {
                keys.record_reference_locked(offset, &frame, &reference)?;
            }
            Ok(plaintext)
        }).await
    }

    /// Consumes the broker's confirmed exclusive floor for this topic lineage.
    pub async fn retire_kafka_before(&self, before: i64) -> crate::Result<bool> {
        if before < 0 {
            return Err(Error::Task("The Kafka floor is negative".into()));
        }
        let keys = Self(ClassicObjectKeys { path: Arc::clone(&self.0.path), region: Arc::clone(&self.0.region), erasure: None });
        self.0.region.run_blocking(move || {
            let path = &keys.0.path;
            let floor = path.join("retired-before");
            let _gate = lock(&floor)?;
            let mut next = kafka_retired_before(&floor)?;
            if before < next {
                return Err(Error::Task("The Kafka broker rolled back a retired floor".into()));
            }
            let index = path.join("positions");
            let stop = before.min(next.saturating_add(128));
            while next < stop {
                let marker = index.join(next.to_string());
                let publication = kafka_retirement::publication_at(&index, next)?;
                let name = ObjectKey::new(format!("frames/{publication}"))?;
                withdraw_slot(&keys.0.name(&name))?;
                let advanced = next + 1;
                write_kafka_marker(&floor, &advanced.to_be_bytes(), true)?;
                // The floor now carries the receipt and makes a failed unlink harmless.
                match fs::remove_file(&marker) {
                    Ok(()) => sync_directory(&index).map_err(io(&index))?,
                    Err(error) if error.kind() == ErrorKind::NotFound => (),
                    Err(error) => return Err(io(&marker)(error)),
                }
                next = advanced;
            }
            Ok(next == before)
        }).await
    }
}

impl ClassicObjectKeys {
    fn local_prefix_path(&self) -> PathBuf { self.path.join("local-prefix-retirement") }

    pub(crate) fn with_local_prefix_retirement<T>(&self, work: impl FnOnce(&Path) -> crate::Result<T>) -> crate::Result<T> {
        let path = self.local_prefix_path();
        let _gate = lock(&path)?;
        work(&path)
    }

    pub(crate) fn require_local_offset(&self, offset: i64) -> crate::Result<()> {
        let first_retained = crate::log::local::erasure::read_floor(&self.local_prefix_path())?;
        if offset < first_retained { return Err(Error::UnavailableLogRange { requested: offset, first_retained }); }
        Ok(())
    }
}

fn kafka_publication(frame: &[u8]) -> crate::Result<Uuid> {
    frame.first_chunk::<16>()
        .filter(|_| frame.len() >= KAFKA_LOG_SEAL_OVERHEAD)
        .map(|identity| Uuid::from_bytes(*identity))
        .filter(|publication| !publication.is_nil())
        .ok_or_else(|| Error::Task("Kafka frame has no sealed publication identity".into()))
}

impl ClassicObjectKeys {
    pub(crate) fn seal_record(
        &self,
        offset: i64,
        timestamp: i64,
        payload: &[u8],
    ) -> crate::Result<Bytes> {
        self.require_local_offset(offset)?;
        let publication = Uuid::new_v4();
        let name = ObjectKey::new(format!("frames/{publication}"))?;
        let associated = format!("rtdb.classic.log-frame/v1/{publication}/{offset}/{timestamp}");
        with_key(&self.name(&name), true, |cipher| {
            cipher.seal_prefixed(&associated, publication.as_bytes(), payload)
        })
    }

    pub(crate) fn open_record(
        &self,
        offset: i64,
        timestamp: i64,
        payload: &[u8],
    ) -> crate::Result<Bytes> {
        let gate = open_slot_lock(&self.local_prefix_path())?;
        gate.lock_shared().map_err(io(&self.local_prefix_path()))?;
        self.require_local_offset(offset)?;
        self.open_original_record(offset, timestamp, payload)
    }

    fn open_original_record(&self, offset: i64, timestamp: i64, payload: &[u8]) -> crate::Result<Bytes> {
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
                self.open_original_record(offset, timestamp, sealed)?;
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

fn kafka_retired_before(path: &Path) -> crate::Result<i64> {
    match fs::read(path) {
        Ok(bytes) => {
            let bytes = bytes.try_into().map_err(|_| Error::Task("Malformed Kafka retirement floor".into()))?;
            let floor = i64::from_be_bytes(bytes);
            if floor < 0 { return Err(Error::Task("Malformed Kafka retirement floor".into())); }
            Ok(floor)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
        Err(error) => Err(io(path)(error)),
    }
}

fn write_kafka_marker(path: &Path, bytes: &[u8], replace: bool) -> crate::Result<()> {
    let parent = path.parent().expect("Kafka marks have a vault directory");
    let mut file = tempfile::NamedTempFile::new_in(parent).map_err(io(path))?;
    file.write_all(bytes).map_err(io(path))?;
    crate::sync_file(file.as_file()).map_err(io(path))?;
    if replace { file.persist(path) } else { file.persist_noclobber(path) }
        .map_err(|error| io(path)(error.error))?;
    sync_directory(parent).map_err(io(parent))
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

fn open_slot_lock(path: &Path) -> crate::Result<File> {
    let lock = path.with_extension("lock");
    File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock)
        .map_err(io(&lock))
}

fn lock(path: &Path) -> crate::Result<File> {
    let file = open_slot_lock(path)?;
    file.lock().map_err(io(&path.with_extension("lock")))?;
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
    async fn a_confirmed_kafka_floor_retires_exact_recorded_publications() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = ClassicKeyVault::open(directory.path())?;
        let keys = vault.kafka_log_keys(Uuid::new_v4().into_bytes(), true, IoRegion::new())?;
        let earlier = keys.seal_kafka_record(Bytes::from_static(b"old")).await?;
        let later = keys.seal_kafka_record(Bytes::from_static(b"live")).await?;
        keys.record_kafka_publication(1, &later)?;
        assert!(keys.retire_kafka_before(1).await.is_err());
        keys.record_kafka_publication(0, &earlier)?;
        assert!(keys.record_kafka_publication(0, &later).is_err());
        assert!(keys.retire_kafka_before(1).await?);
        assert!(keys.retire_kafka_before(1).await?);
        assert!(matches!(keys.open_kafka_record(&earlier), Err(Error::ErasedUnderView)));
        assert_eq!(keys.open_kafka_record(&later)?, Bytes::from_static(b"live"));
        Ok(())
    }

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
        assert!(different.open_kafka_record(&frame).is_err());
        assert_eq!(vault.kafka_log_keys(topic, false, io)?.open_kafka_record(&frame)?, body);
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
