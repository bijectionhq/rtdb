//! Per-deployment key provisioning, fork copies and durable withdrawal.

use std::fs::{self, File, TryLockError};
use std::io::{ErrorKind, Write};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use rtdb_directory::{DeletionFacts, DeletionRecord};
use rtdb_storage::IoRegion;
use rtdb_types::{Anomaly, AnomalyCategory, Result};

mod archive;
mod delivery;
pub(crate) use delivery::DeploymentDelivery;
mod assertion;
pub mod assertion_deadline;
mod occurrence;
pub use occurrence::{AssertionMintKey, AssertionRoute, AssertionRouteKey};
mod journal;
mod journal_identity;
mod unit;
pub use rtdb_storage::sealing::{UnitOperation, UnitOutput};
pub use unit::{UnitScope, UnitSlot};
pub(crate) use unit::UnitLedger;
pub use assertion::family::NativeFamilyLanding;
pub(crate) use assertion::family::{assertion_root_line, family_vault_address, world_row_scope, world_row_slot_commitment};
pub use assertion::family::{AssertionFamilies, DerivedRow, EnrolledWorldRow, FamilyFence, FamilyOccurrence, LandingLink, LandingReservation, WorldRowSlot, CarrierJournalIdentity, FamilyReleaseAuthority, FamilyReleaseInput, FamilyReleaseObservation, FamilyReleaseRegistration, FamilyResponsePart};
pub use assertion::family::{ManagedFamilyTransfer, ManagedFamilyTransferSource};
pub use assertion::family::{FamilyTransfer, FamilyTransferArtifact, FamilyTransferAuthority, FamilyTransferContributor, FamilyTransferDestination,
    FamilyTransferDestinationObservation, FamilyTransferDestinationState, FamilyTransferFanout, FamilyTransferSource,
    FamilyTransferRead, FamilyTransferSourceDrain, FamilyTransferSourceObservation};
pub(crate) use assertion::family::{RankTransferAgreement, RankTransferMember, WorldRankFrame};
pub use assertion::family::{
    AcceptedWorldRankReplay, WorldRankDestinationCustody, WorldRankSourceCensus, WorldRankSourceDrain,
    WorldRankTransfer, WorldRankTransferAuthority, WorldRankTransferBinding, WorldRankTransferContributor,
    WorldRankTransferDestination, WorldRankTransferRead, WorldRankTransferSource, WorldRankTransferState,
};
pub use assertion::issuer::{AssertionIngressTicket, AssertionIssuerScope, AssertionIssuers};
pub use assertion::{AssertionAuthoring, AssertionBackup, AssertionEnvelope, AssertionKeyService, AssertionStage};
pub use journal::JournalKey;
pub use archive::ArchiveKey;
pub use crate::cipher::object::FramedObject;
pub(crate) use archive::legacy_archive_key;

/// The trim owner recorded these names before the certified ciphertext disappeared.
pub(crate) async fn withdraw_archive_records(
    keys: &dyn KeyService, residence: &str, records: &[String], region: &Arc<IoRegion>,
) -> Result<()> {
    use futures::{StreamExt, TryStreamExt, stream};
    let scope = UnitScope::archive(residence);
    let withdrawals = records.iter().map(|record| {
        let slot = archive::associated_unit(record).map(|name| scope.slot(&name));
        async move { keys.withdraw_unit_key(slot?, region).await }
    }).collect::<Vec<_>>();
    stream::iter(withdrawals).buffer_unordered(16).try_collect::<Vec<()>>().await?;
    Ok(())
}

pub const KEY_BYTES: usize = 32;
const NAMESPACE_KEY_ID: &str = ".rtdb-key-namespace";
const DELETION_SIGNING_KEY_ID: &str = ".rtdb-deletion-signing-seed";
const DESTROYED: &[u8] = b"RTDB-key-destroyed-v1";
const ARCHIVE_VAULT_MARKER: &str = ".archive-operation-authority-v1";
const DELETION_SIGNATURE_DOMAIN: &[u8] = b"rtdb.multi.deletion.v1\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchiveKeyWithdrawal {
    Vault,
    OperationAuthority,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveRevocation {
    pub operation_id: uuid::Uuid,
    pub request_sha256: [u8; 32],
    pub not_before_ms: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveRevocationScope {
    pub deployment: uuid::Uuid,
    pub incarnation: u64,
}

impl ArchiveRevocationScope {
    pub(crate) fn capture_key(self, capture: uuid::Uuid) -> String {
        format!("checkpoint-{}-{}-{capture}", self.deployment, self.incarnation)
    }
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveRevocationOutcome {
    pub request: ArchiveRevocation,
    pub revoked_at_ms: Option<i64>,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveRevocationPage {
    pub outcomes: Vec<ArchiveRevocationOutcome>,
    pub next_after: Option<uuid::Uuid>,
}

#[async_trait::async_trait]
pub trait KeyService: Send + Sync {
    async fn namespace_id(&self) -> Result<[u8; KEY_BYTES]> { self.key(NAMESPACE_KEY_ID).await }
    async fn existing_namespace_id(&self) -> Result<Option<[u8; KEY_BYTES]>> { self.existing_key(NAMESPACE_KEY_ID).await }
    async fn existing(&self, deployment: &str) -> Result<[u8; KEY_BYTES]> {
        self.existing_key(deployment).await?.ok_or_else(|| {
            Anomaly::new(AnomalyCategory::NotFound, "rtdb.multi/key-missing", "The deployment has no installed key")
        })
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
    /// Confirms the original key and keeps the authority live through delivery.
    async fn grant_deployment_key(&self, _id: &str, _binding: [u8; KEY_BYTES]) -> rtdb_storage::Result<UnitOutput> {
        Err(rtdb_storage::Error::Closed("The deployment key owner cannot grant delivery".into()))
    }
    /// Gives `to` the key `from` has, for a fork whose objects are copies of its parent's.
    async fn copy(&self, from: &str, to: &str) -> Result<()>;
    /// Withdraws the key and permanently refuses provisioning for this deployment ID.
    async fn destroy(&self, deployment: &str) -> Result<()>;
    /// Identifies custody so a move cannot install into the vault it will withdraw.
    fn custody_namespace(&self) -> Result<String>;
    /// Installs a key brought from another cell's key service, for a deployment that moved here.
    async fn install(&self, deployment: &str, key: [u8; KEY_BYTES]) -> Result<()>;

    fn assertion_keys(&self) -> Option<&dyn AssertionKeyService> { None }
    fn assertion_families(&self, _region: Arc<IoRegion>) -> Option<AssertionFamilies<'_>> { None }
    fn assertion_issuers(&self, _region: Arc<IoRegion>) -> Option<AssertionIssuers<'_>> { None }

    async fn use_unit_key(&self, _slot: UnitSlot, _name: rtdb_storage::ObjectKey, _operation: UnitOperation,
        _io: &Arc<IoRegion>) -> rtdb_storage::Result<UnitOutput> {
        Err(journal::key_error(unit::unsupported()))
    }

    async fn withdraw_unit_key(&self, _slot: UnitSlot, _io: &Arc<IoRegion>) -> Result<()> {
        Err(unit::unsupported())
    }

    async fn withdraw_unit_scope(&self, _scope: UnitScope, _io: &Arc<IoRegion>) -> Result<()> {
        Err(unit::unsupported())
    }

    fn retained_unit_ledger(&self, _scope: UnitScope, _io: Arc<IoRegion>) -> Result<Arc<dyn rtdb_storage::sealing::ObjectCipher>> {
        Err(unit::unsupported())
    }

    async fn provision_journal_key(&self, key: JournalKey) -> Result<()> {
        self.key(&key.id()).await.map(|_| ())
    }

    async fn seal_journal_payload(&self, key: JournalKey, _io: &Arc<IoRegion>, plaintext: Bytes) -> rtdb_storage::Result<Bytes> {
        let id = key.id();
        let deadline = Instant::now() + unit::OPERATION_LIMIT;
        let material = tokio::time::timeout_at(deadline.into(), self.existing(&id))
            .await.map_err(|_| unit::expired())?.map_err(journal::key_error)?;
        unit::with_material(material, deadline, |cipher| cipher.seal(&id, &plaintext))
    }

    async fn open_journal_payload(&self, key: JournalKey, _io: &Arc<IoRegion>, ciphertext: Bytes) -> rtdb_storage::Result<Bytes> {
        let id = key.id();
        let deadline = Instant::now() + unit::OPERATION_LIMIT;
        let material = tokio::time::timeout_at(deadline.into(), self.existing(&id))
            .await.map_err(|_| unit::expired())?.map_err(journal::key_error)?;
        unit::with_material(material, deadline, |cipher| cipher.open(&id, &ciphertext))
    }

    async fn archive_key(&self, id: &str, provision: bool, _region: Arc<IoRegion>) -> Result<Arc<dyn ArchiveKey>> {
        let key = if provision { self.key(id).await? } else { self.existing(id).await? };
        legacy_archive_key(&key)
    }

    /// Returns the original residence authority for independently withdrawable units.
    async fn scoped_unit_key(&self, _id: &str, _region: Arc<IoRegion>) -> Result<Arc<dyn ArchiveKey>> {
        Err(unit::unsupported())
    }

    async fn archive_key_is_operation_only(&self, _id: &str) -> Result<bool> { Ok(false) }

    async fn schedule_archive_key(&self, _id: &str, _due_at_ms: i64) -> Result<bool> {
        Err(Anomaly::unsupported("rtdb.multi/archive-vault", "The archive vault does not own scheduled revocation"))
    }

    async fn schedule_archive_revocation(&self, _id: &str, _request: ArchiveRevocation, _due_at_ms: i64) -> Result<bool> {
        Err(Anomaly::unsupported("rtdb.multi/archive-vault", "The archive vault cannot admit an exact schedule"))
    }

    async fn withdraw_archive_key(&self, id: &str) -> Result<ArchiveKeyWithdrawal> {
        self.destroy(id).await?;
        Ok(ArchiveKeyWithdrawal::Vault)
    }

    async fn reconcile_archive_revocation(&self, _id: &str, _request: ArchiveRevocation) -> Result<i64> {
        Err(Anomaly::unsupported("rtdb.multi/archive-vault", "The archive vault cannot settle an exact revocation"))
    }

    async fn close_archive_revocations(&self, _scope: ArchiveRevocationScope) -> Result<()> {
        Err(Anomaly::unsupported("rtdb.multi/archive-vault", "The archive vault cannot close revocation admission"))
    }

    async fn archive_revocation_outcomes(&self, _scope: ArchiveRevocationScope, _after: Option<uuid::Uuid>) -> Result<ArchiveRevocationPage> {
        Err(Anomaly::unsupported("rtdb.multi/archive-vault", "The archive vault cannot enumerate revocation outcomes"))
    }

    /// The standalone key authority can witness the lifetime of a local I/O
    /// region. Providers without a local authority use explicit drain receipts.
    fn hold_local_storage_region(&self, _region: uuid::Uuid) -> Result<Option<File>> { Ok(None) }
    fn local_storage_region_drained(&self, _region: uuid::Uuid) -> Result<bool> { Ok(false) }
    fn forget_local_storage_region(&self, _region: uuid::Uuid) -> Result<()> { Ok(()) }
}

/// Keys kept as files in a directory apart from the objects they protect, standing in for a key-management service.
#[derive(Clone)]
pub struct LocalKeyService {
    directory: PathBuf,
    provision_archive_keys: bool,
}

impl LocalKeyService {
    pub fn new(directory: impl Into<PathBuf>) -> Result<Self> {
        let directory = directory.into();
        std::fs::create_dir_all(&directory).map_err(io)?;
        let service = Self { directory, provision_archive_keys: false };
        service.require_service_root()?;
        Ok(service)
    }
    pub fn with_archive_broker(directory: impl Into<PathBuf>) -> Result<Self> {
        let mut vault = Self::new(directory)?;
        vault.archive_vault()?;
        vault.provision_archive_keys = true;
        Ok(vault)
    }

    fn path(&self, id: &str) -> Result<PathBuf> {
        let mut components = Path::new(id).components();
        if !matches!(components.next(), Some(Component::Normal(name)) if name == id)
            || components.next().is_some()
        {
            return Err(Anomaly::incorrect("rtdb.multi/key-id", "Invalid local key identity"));
        }
        Ok(self.directory.join(format!("{id}.key")))
    }

    fn archive_directory(&self) -> PathBuf {
        self.directory.join(".archive-vault")
    }

    fn archive_vault(&self) -> Result<Self> {
        let directory = self.archive_directory();
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        builder.mode(0o700);
        match builder.create(&directory) {
            Ok(()) => self.sync_directory()?,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io(error)),
        }
        let metadata = fs::symlink_metadata(&directory).map_err(io)?;
        if !metadata.is_dir() {
            return Err(Anomaly::incorrect("rtdb.multi/archive-vault-path", "The operation vault requires its own directory"));
        }
        #[cfg(unix)]
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Anomaly::incorrect("rtdb.multi/archive-vault-path", "The operation vault directory must be private"));
        }
        let vault = Self { directory, provision_archive_keys: false };
        let marker = vault.directory.join(ARCHIVE_VAULT_MARKER);
        let _lock = vault.lock_key(&marker)?;
        match fs::read(&marker) {
            Ok(binding) if binding == ARCHIVE_VAULT_MARKER.as_bytes() => (),
            Err(error) if error.kind() == ErrorKind::NotFound => vault.publish(&marker, ARCHIVE_VAULT_MARKER.as_bytes())?,
            Err(error) => return Err(io(error)),
            Ok(_) => return Err(malformed()),
        }
        Ok(vault)
    }

    fn is_brokered(&self, id: &str) -> Result<bool> {
        let broker = Self { directory: self.archive_directory(), provision_archive_keys: false };
        broker.path(id)?.try_exists().map_err(io)
    }

    fn require_service_root(&self) -> Result<()> {
        if self.directory.join(ARCHIVE_VAULT_MARKER).try_exists().map_err(io)? {
            return Err(Anomaly::incorrect("rtdb.multi/archive-vault-path", "The operation vault cannot be opened as a raw key service"));
        }
        Ok(())
    }

    fn require_raw_key(&self, id: &str) -> Result<()> {
        require_general_key_namespace(id)?;
        self.require_service_root()?;
        if self.is_brokered(id)? {
            return Err(Anomaly::new(AnomalyCategory::Forbidden, "rtdb.multi/key-non-exportable",
                "The archive key is confined to its operation authority"));
        }
        Ok(())
    }

    fn open_archive_key(&self, id: String, provision: bool, region: Arc<IoRegion>) -> Result<Arc<dyn ArchiveKey>> {
        require_general_key_namespace(&id)?;
        self.require_service_root()?;
        let path = self.path(&id)?;
        let raw_lock = self.lock_key(&path)?;
        if self.is_brokered(&id)? || provision && self.provision_archive_keys && !path.try_exists().map_err(io)? {
            let vault = self.archive_vault()?;
            let vault_path = vault.path(&id)?;
            let _lock = vault.lock_key(&vault_path)?;
            if archive::expire_locked(&vault, &vault_path)?.is_some() {
                return Err(archive::revoked());
            }
            vault.read_or_create_key(&vault_path, provision)?;
            return Ok(archive::tracked_archive_key(archive::LocalArchiveKey { unit_vault: vault.clone(), vault, id, region }));
        }
        if provision {
            drop(raw_lock);
            self.key_at(&path, Some(&id))?;
        } else {
            read_key(&path, fs::read(&path).map_err(io)?)?;
        }
        Ok(archive::tracked_archive_key(archive::LocalArchiveKey { vault: self.clone(), unit_vault: self.archive_vault()?, id, region }))
    }
    fn region_path(&self, region: uuid::Uuid) -> PathBuf {
        self.directory.join(format!("{region}.storage-region"))
    }

    fn open_key_lock(&self, path: &Path) -> Result<File> {
        File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.with_extension("lock"))
            .map_err(io)
    }

    fn lock_key_shared(&self, path: &Path) -> Result<File> {
        let lock = self.open_key_lock(path)?;
        lock.lock_shared().map_err(io)?;
        Ok(lock)
    }

    fn lock_key(&self, path: &Path) -> Result<File> {
        let lock = self.open_key_lock(path)?;
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
    async fn load_key(&self, path: PathBuf, raw_id: Option<String>) -> Result<[u8; KEY_BYTES]> {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else { return self.key_at(&path, raw_id.as_deref()); };
        let authority = self.clone();
        runtime.spawn_blocking(move || authority.key_at(&path, raw_id.as_deref())).await
            .map_err(|error| Anomaly::fault("rtdb.multi/local-key-task", error.to_string()))?
    }
    fn key_at(&self, path: &Path, raw_id: Option<&str>) -> Result<[u8; KEY_BYTES]> {
        let _lock = self.lock_key(path)?;
        if let Some(id) = raw_id { self.require_raw_key(id)?; }
        self.read_or_create_key(path, true)
    }

    fn read_or_create_key(&self, path: &Path, provision: bool) -> Result<[u8; KEY_BYTES]> {
        match fs::read(path) {
            Ok(bytes) => {
                let key = read_key(path, bytes)?;
                self.sync_directory()?;
                Ok(key)
            }
            Err(error) if provision && error.kind() == ErrorKind::NotFound => {
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
    fn retained_unit_ledger(&self, scope: UnitScope, io: Arc<IoRegion>) -> Result<Arc<dyn rtdb_storage::sealing::ObjectCipher>> {
        Ok(Arc::new(UnitLedger::new(Arc::new(self.clone()), scope, io)))
    }
    async fn grant_deployment_key(&self, id: &str, binding: [u8; KEY_BYTES]) -> rtdb_storage::Result<UnitOutput> {
        let vault = self.clone();
        let id = id.to_owned();
        IoRegion::new().run_blocking(move || {
            let map = journal::key_error;
            let path = vault.path(&id).map_err(map)?;
            let custody = Arc::new(vault.lock_key_shared(&path).map_err(map)?);
            vault.require_raw_key(&id).map_err(map)?;
            let material = read_key(&path, fs::read(&path).map_err(io).map_err(map)?).map_err(map)?;
            delivery::require_deployment_key(Some(material), binding)?;
            Ok(UnitOutput::HeldGrant { until: None, custody })
        }).await
    }

    async fn namespace_id(&self) -> Result<[u8; KEY_BYTES]> { self.load_key(self.directory.join(".namespace"), None).await }

    async fn existing_namespace_id(&self) -> Result<Option<[u8; KEY_BYTES]>> {
        let path = self.directory.join(".namespace");
        match tokio::fs::read(&path).await {
            Ok(bytes) => read_key(&path, bytes).map(Some),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io(error)),
        }
    }

    async fn sign_deletion(&self, facts: DeletionFacts) -> Result<DeletionRecord> {
        sign_deletion(self.load_key(self.directory.join(".deletion-signer"), None).await?, facts)
    }

    async fn deletion_verification_key(&self) -> Result<String> {
        verification_key(self.load_key(self.directory.join(".deletion-signer"), None).await?)
    }

    async fn key(&self, deployment: &str) -> Result<[u8; KEY_BYTES]> {
        self.load_key(self.path(deployment)?, Some(deployment.to_owned())).await
    }

    fn custody_namespace(&self) -> Result<String> {
        Ok(format!("local:{:?}", self.directory.canonicalize().map_err(io)?))
    }

    async fn existing_key(&self, deployment: &str) -> Result<Option<[u8; KEY_BYTES]>> {
        self.require_raw_key(deployment)?;
        let path = self.path(deployment)?;
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
        let path = self.path(deployment)?;
        let _lock = self.lock_key(&path)?;
        self.require_raw_key(deployment)?;
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
        {
            let path = self.path(deployment)?;
            let _lock = self.lock_key(&path)?;
            self.require_raw_key(deployment)?;
            match fs::read(&path) {
                Ok(bytes) if is_destroyed(&bytes) => self.sync_directory()?,
                Ok(_) => self.publish(&path, DESTROYED)?,
                Err(error) if error.kind() == ErrorKind::NotFound => self.publish(&path, DESTROYED)?,
                Err(error) => return Err(io(error)),
            }
        }
        self.withdraw_local_scope(UnitScope::archive(deployment), &IoRegion::new()).await?;
        Ok(())
    }

    fn assertion_keys(&self) -> Option<&dyn AssertionKeyService> {
        self.provision_archive_keys.then_some(self as &dyn AssertionKeyService)
    }

    fn assertion_families(&self, region: Arc<IoRegion>) -> Option<AssertionFamilies<'_>> {
        self.provision_archive_keys.then(|| AssertionFamilies::new(self, region))
    }

    fn assertion_issuers(&self, region: Arc<IoRegion>) -> Option<AssertionIssuers<'_>> {
        self.provision_archive_keys.then(|| AssertionIssuers::new(self, region))
    }

    async fn use_unit_key(&self, slot: UnitSlot, name: rtdb_storage::ObjectKey, operation: UnitOperation,
        io: &Arc<IoRegion>) -> rtdb_storage::Result<UnitOutput> {
        self.use_local_unit(slot, name, operation, io).await
    }

    async fn withdraw_unit_key(&self, slot: UnitSlot, io: &Arc<IoRegion>) -> Result<()> {
        self.withdraw_local_unit(slot, io).await
    }

    async fn withdraw_unit_scope(&self, scope: UnitScope, io: &Arc<IoRegion>) -> Result<()> {
        self.withdraw_local_scope(scope, io).await
    }

    async fn provision_journal_key(&self, key: JournalKey) -> Result<()> {
        let vault = self.clone();
        tokio::task::spawn_blocking(move || vault.provision_journal(key)).await
            .map_err(|error| Anomaly::fault("rtdb.multi/local-key-task", error.to_string()))?
    }

    async fn seal_journal_payload(&self, key: JournalKey, io: &Arc<IoRegion>, plaintext: Bytes) -> rtdb_storage::Result<Bytes> {
        self.journal_payload(key, io, move |cipher, id| cipher.seal(id, &plaintext)).await
    }

    async fn open_journal_payload(&self, key: JournalKey, io: &Arc<IoRegion>, ciphertext: Bytes) -> rtdb_storage::Result<Bytes> {
        self.journal_payload(key, io, move |cipher, id| cipher.open(id, &ciphertext)).await
    }

    async fn archive_key(&self, id: &str, provision: bool, region: Arc<IoRegion>) -> Result<Arc<dyn ArchiveKey>> {
        let vault = self.clone();
        let id = id.to_owned();
        let key_region = Arc::clone(&region);
        region.run_blocking(move || Ok(vault.open_archive_key(id, provision, key_region))).await?
    }

    async fn scoped_unit_key(&self, id: &str, region: Arc<IoRegion>) -> Result<Arc<dyn ArchiveKey>> {
        self.archive_key(id, false, region).await
    }

    async fn archive_key_is_operation_only(&self, id: &str) -> Result<bool> {
        require_general_key_namespace(id)?;
        self.require_service_root()?;
        self.is_brokered(id)
    }

    async fn schedule_archive_key(&self, id: &str, due_at_ms: i64) -> Result<bool> {
        require_general_key_namespace(id)?;
        let service = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            service.require_service_root()?;
            let scope = service.lock_capture_revocations(&id)?;
            if let Some(scope) = &scope { scope.require_open()?; }
            let raw_path = service.path(&id)?;
            let _raw_lock = service.lock_key(&raw_path)?;
            if !service.is_brokered(&id)? {
                return Err(Anomaly::unsupported("rtdb.multi/archive-vault", "This archive key exports its material"));
            }
            let vault = service.archive_vault()?;
            let path = vault.path(&id)?;
            let _lock = vault.lock_key(&path)?;
            archive::schedule_locked(&vault, &path, due_at_ms)
        }).await.map_err(|error| Anomaly::fault("rtdb.multi/archive-vault-task", error.to_string()))?
    }

    async fn schedule_archive_revocation(&self, id: &str, request: ArchiveRevocation, due_at_ms: i64) -> Result<bool> {
        self.schedule_exact_revocation(id, request, due_at_ms).await
    }

    async fn withdraw_archive_key(&self, id: &str) -> Result<ArchiveKeyWithdrawal> {
        require_general_key_namespace(id)?;
        let vault = self.clone();
        let scope = UnitScope::archive(id);
        let id = id.to_owned();
        let withdrawal = tokio::task::spawn_blocking(move || -> Result<ArchiveKeyWithdrawal> {
            vault.require_service_root()?;
            let scope = vault.lock_capture_revocations(&id)?;
            let raw_path = vault.path(&id)?;
            let _raw_lock = vault.lock_key(&raw_path)?;
            let (authority, path) = if vault.is_brokered(&id)? {
                let broker = vault.archive_vault()?;
                let path = broker.path(&id)?;
                (broker, path)
            } else {
                vault.publish(&raw_path, DESTROYED)?;
                return Ok(ArchiveKeyWithdrawal::Vault);
            };
            let _lock = authority.lock_key(&path)?;
            if let Some(scope) = &scope {
                scope.require_legacy_withdrawal(&path)?;
            }
            archive::withdraw_locked(&authority, &path)?;
            Ok(ArchiveKeyWithdrawal::OperationAuthority)
        }).await.map_err(|error| Anomaly::fault("rtdb.multi/archive-vault-task", error.to_string()))??;
        self.withdraw_local_scope(scope, &IoRegion::new()).await?;
        Ok(withdrawal)
    }

    async fn reconcile_archive_revocation(&self, id: &str, request: ArchiveRevocation) -> Result<i64> {
        self.revoke_archive(id, request).await
    }

    async fn close_archive_revocations(&self, scope: ArchiveRevocationScope) -> Result<()> {
        self.close_exact_revocations(scope).await
    }

    async fn archive_revocation_outcomes(&self, scope: ArchiveRevocationScope, after: Option<uuid::Uuid>) -> Result<ArchiveRevocationPage> {
        self.exact_revocation_outcomes(scope, after).await
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

fn require_general_key_namespace(id: &str) -> Result<()> {
    if id.starts_with(unit::SLOT_PREFIX) {
        return Err(Anomaly::incorrect("rtdb.multi/key-id", "An immutable-unit key requires its custody API"));
    }
    assertion::require_other_key_namespace(id)
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
        let lock = keys.lock_key(&keys.path("shop").unwrap()).unwrap();
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
        assert_eq!(restarted.key("b").await.unwrap(), key);
    }

    #[tokio::test]
    async fn destroying_a_key_replaces_only_the_selected_copy() {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path()).unwrap();
        let key = keys.key("source").await.unwrap();
        std::fs::hard_link(keys.path("source").unwrap(), keys.path("copy").unwrap()).unwrap();
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
        let interrupted = keys.path("a").unwrap().with_extension("pending");
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_concurrent_install_cannot_leave_a_destroyed_key_readable() {
        let directory = tempfile::tempdir().unwrap();
        let keys = Arc::new(LocalKeyService::new(directory.path()).unwrap());
        for id in 0..16 {
            let id = id.to_string();
            let install = tokio::spawn({
                let keys = keys.clone();
                let id = id.clone();
                async move { keys.install(&id, [1; KEY_BYTES]).await }
            });
            keys.destroy(&id).await.unwrap();
            if let Err(error) = install.await.unwrap() {
                assert_eq!(error.code, "rtdb.multi/key-destroyed");
            }
            assert_eq!(keys.key(&id).await.unwrap_err().code, "rtdb.multi/key-destroyed");
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
