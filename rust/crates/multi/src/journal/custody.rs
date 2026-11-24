//! Provider-backed checkpoint custody for installed journals.

mod local;

use super::InstalledJournal;
use crate::keys::{KeyService, UnitLedger, UnitScope};
use crate::store::{EncryptingStore, exact_retirement::installed_object_name};
use async_trait::async_trait;
use bytes::Bytes;
use ordered_logkeeper::ordered::{
    Checkpoint, CheckpointPreparation, Custody, Cut, Fingerprint, Grant, MAX_OBJECT_REFERENCES,
    ObjectName, Placement, RequestId, service::Route,
};
use ring::digest::{SHA256, digest};
use rtdb_directory::Checkpoint as RtdbCheckpoint;
use rtdb_format::{catalog::BlockCatalog, keys::block_key};
use rtdb_storage::{Error, IoRegion, ObjectKey, ObjectObservation, ObjectStore, object_store::StoredObject};
use rtdb_types::{Anomaly, PhysicalObjectVersion, Result, StorageEpoch};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

const MAX_MANIFEST_BYTES: usize = 16 << 20;

/// An isolated immutable object namespace provisioned for one installed
/// placement. Its store owns encryption and allocation admission.
pub struct CheckpointCustody {
    bucket: Arc<dyn ObjectStore>,
    keys: Arc<dyn KeyService>,
    io: Arc<IoRegion>,
    placement: Placement,
    epoch: StorageEpoch,
    prefix: ObjectKey,
    provider: Fingerprint,
    local: Option<local::LocalArchive>,
}

/// Exact space reserved by the custodian before staging any checkpoint copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Reservation {
    pub custody: Custody,
    pub usable_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Scope {
    deployment: Uuid,
    incarnation: Uuid,
    group: Uuid,
    generation: u64,
    log_epoch: i32,
    provider: Fingerprint,
}

/// The immutable origin of an unfinished checkpoint capture. A replacement
/// reopens the original writer namespace with this exact published base.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Preparation {
    pub authority: CheckpointAuthority,
    pub writer: Grant,
    pub base: Checkpoint,
    pub through: Cut,
    pub checkpoint: RtdbCheckpoint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ErasureBinding {
    pub operation: Uuid,
    pub custody_domain: Uuid,
    pub rotation: RequestId,
    pub fixed_before_ms: Option<u64>,
}

impl ErasureBinding {
    pub(crate) fn descriptor(self) -> Result<Fingerprint> {
        let mut bytes = b"rtdb.multi/checkpoint-erasure-attempt/v1\0".to_vec();
        bytes.extend_from_slice(&serde_json::to_vec(&self).map_err(serialization)?);
        Ok(fingerprint(&bytes))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CheckpointAuthority {
    Upload,
    Erasure(ErasureBinding),
}

impl Preparation {
    pub fn ordered(self, reservation: Reservation) -> Result<CheckpointPreparation> {
        let root = validate_cut(self.checkpoint, self.through)?;
        let mut descriptor = [0; 32];
        match self.authority {
            CheckpointAuthority::Upload => {
                for (bytes, field) in descriptor.chunks_exact_mut(8).zip([
                    self.checkpoint.block,
                    self.checkpoint.boundary,
                    self.checkpoint.source,
                    self.checkpoint.system_time,
                ]) {
                    bytes.copy_from_slice(&field.to_be_bytes());
                }
            }
            CheckpointAuthority::Erasure(binding) => descriptor = binding.descriptor()?,
        }
        Ok(CheckpointPreparation {
            erasure: matches!(self.authority, CheckpointAuthority::Erasure(_)),
            checkpoint: self.through,
            root,
            source: self.writer,
            base: self.base,
            custody: reservation.custody,
            usable_bytes: reservation.usable_bytes,
            descriptor,
        })
    }

    pub fn recover(chosen: CheckpointPreparation, reservation: Reservation) -> Result<Self> {
        if chosen.erasure || chosen.custody != reservation.custody || chosen.usable_bytes != reservation.usable_bytes
        {
            return Err(invalid(
                "The installed provider reservation differs from the ordered preparation",
            ));
        }
        let field = |offset: usize| {
            i64::from_be_bytes(
                chosen.descriptor[offset..offset + 8]
                    .try_into()
                    .expect("the ordered checkpoint field is eight bytes"),
            )
        };
        let checkpoint = RtdbCheckpoint {
            block: field(0),
            boundary: field(8),
            source: field(16),
            system_time: field(24),
            root_sha256: Some(chosen.root),
        };
        validate_cut(checkpoint, chosen.checkpoint)?;
        Ok(Self {
            authority: CheckpointAuthority::Upload,
            writer: chosen.source,
            base: chosen.base,
            through: chosen.checkpoint,
            checkpoint,
        })
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    scope: Scope,
    reservation: Reservation,
    source: Preparation,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    scope: Scope,
    allocation: Uuid,
    revision: u64,
    next_lsn: u64,
    history: Fingerprint,
    authority: CheckpointAuthority,
    checkpoint: RtdbCheckpoint,
    members: BTreeMap<String, Member>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    bytes: u64,
    sha256: Fingerprint,
    version: Option<PhysicalObjectVersion>,
}

pub struct OpenedCheckpoint {
    pub checkpoint: RtdbCheckpoint,
    pub store: Arc<dyn ObjectStore>,
    pub(super) authority: CheckpointAuthority,
}

impl CheckpointCustody {
    pub fn new(
        bucket: Arc<dyn ObjectStore>,
        prefix: ObjectKey,
        placement: Placement,
        epoch: StorageEpoch,
        provider: Fingerprint,
        keys: Arc<dyn KeyService>,
        io: Arc<IoRegion>,
    ) -> Self {
        Self {
            bucket,
            keys,
            io,
            placement,
            epoch,
            prefix,
            provider,
            local: None,
        }
    }

    pub(crate) fn on_local_volume(mut self, path: &Path) -> Result<Self> {
        self.local = Some(local::LocalArchive::new(path)?);
        Ok(self)
    }

    fn ledger(&self, custody: Custody, io: Arc<IoRegion>) -> UnitLedger {
        let mut residence = self.provider.to_vec();
        residence.extend_from_slice(self.allocation(custody).as_bytes());
        let scope = UnitScope::new(self.placement, b"checkpoint", &residence);
        UnitLedger::new(Arc::clone(&self.keys), scope, io)
    }

    fn remote_store(&self, custody: Custody) -> EncryptingStore {
        EncryptingStore::with_units(Arc::clone(&self.bucket), Arc::new(self.ledger(custody, Arc::clone(&self.io))))
    }

    async fn reader(&self, custody: Custody) -> Result<Arc<dyn ObjectStore>> {
        self.require_provider(custody)?;
        let Some(local) = &self.local else {
            return Ok(Arc::new(self.remote_store(custody)));
        };
        let scope = self.allocation(custody);
        let allocation = local.acquire(&scope, false).await?;
        if allocation.closed().await?.is_some() {
            return Err(invalid("The local checkpoint archive is closed"));
        }
        allocation.read_store(self.ledger(custody, allocation.region()))
    }

    async fn put(
        &self,
        custody: Custody,
        key: &ObjectKey,
        body: Bytes,
    ) -> rtdb_storage::Result<()> {
        self.require_provider(custody)
            .map_err(|error| Error::Task(error.to_string()))?;
        let Some(local) = &self.local else {
            return self.remote_store(custody).put(key, body).await;
        };
        let scope = self.allocation(custody);
        let allocation = local
            .acquire(&scope, true)
            .await
            .map_err(|error| Error::Task(error.to_string()))?;
        let ledger = self.ledger(custody, allocation.region());
        allocation.put(&ledger, key, body).await
    }

    pub(crate) async fn pending(&self, reservation: &Reservation) -> Result<Option<Preparation>> {
        let locator = self.intent(reservation.custody)?;
        let reader = self.reader(reservation.custody).await?;
        let Some(frame) = Self::metadata(reader.as_ref(), &locator).await? else {
            return Ok(None);
        };
        let intent: Intent = serde_json::from_slice(&frame).map_err(serialization)?;
        if intent.scope != self.scope() || intent.reservation != *reservation {
            return Err(invalid(
                "The checkpoint preparation addresses a different reservation",
            ));
        }
        validate_cut(intent.source.checkpoint, intent.source.through)?;
        Ok(Some(intent.source))
    }

    /// Registers the only allowed source for this allocation before issuing
    /// any provider writes. Immutable creation selects one racing proposal.
    pub(crate) async fn begin(
        &self,
        reservation: &Reservation,
        source: Preparation,
    ) -> Result<Preparation> {
        validate_cut(source.checkpoint, source.through)?;
        Self::copy_budget(reservation)?;
        if let Some(existing) = self.pending(reservation).await? {
            return if existing == source {
                Ok(existing)
            } else {
                Err(invalid(
                    "The provider preparation differs from the ordered source",
                ))
            };
        }
        let intent = Intent {
            scope: self.scope(),
            reservation: *reservation,
            source,
        };
        let frame = serde_json::to_vec(&intent).map_err(serialization)?;
        if frame.len() > MAX_MANIFEST_BYTES {
            return Err(invalid(
                "The checkpoint preparation exceeds its metadata reservation",
            ));
        }
        match self
            .put(
                reservation.custody,
                &self.intent(reservation.custody)?,
                Bytes::from(frame),
            )
            .await
        {
            Ok(()) => Ok(source),
            Err(Error::ConflictingObject(_)) => match self.pending(reservation).await? {
                Some(existing) if existing == source => Ok(existing),
                _ => Err(invalid(
                    "The provider preparation differs from the ordered source",
                )),
            },
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn copy_budget(reservation: &Reservation) -> Result<u64> {
        if reservation.custody.allocation.is_nil()
            || reservation.custody.revision <= 1
            || reservation.custody.provider == [0; 32]
        {
            return Err(invalid("The checkpoint has no new provider reservation"));
        }
        reservation
            .usable_bytes
            .checked_sub(2 * MAX_MANIFEST_BYTES as u64)
            .ok_or_else(|| invalid("The reservation cannot hold checkpoint metadata"))
    }

    pub(crate) async fn preflight(
        &self,
        reservation: &Reservation,
        origin: Preparation,
        source: &dyn ObjectStore,
    ) -> Result<Vec<ObjectName>> {
        self.require_provider(reservation.custody)?;
        let budget = Self::copy_budget(reservation)?;
        // A permanent size refusal must precede the first provider effect
        // and the ordered binding of a reservation to a source.
        let members = crate::archive::ordered_checkpoint_sources(
            source,
            origin.checkpoint,
            self.epoch,
            budget,
        )
        .await?;
        let mut references = Vec::with_capacity(members.len().saturating_add(2));
        let custody = reservation.custody;
        references.push(self.object_name(&self.intent(custody)?)?);
        references.push(self.object_name(&self.manifest(custody)?)?);
        for member in members {
            references.push(self.object_name(&self.member(custody, &member.name)?)?);
        }
        references.sort_unstable();
        references.dedup();
        if references.len() > MAX_OBJECT_REFERENCES {
            return Err(invalid("The checkpoint closure exceeds the journal's object reference bound"));
        }
        Ok(references)
    }

    fn object_name(&self, key: &ObjectKey) -> Result<ObjectName> {
        if let Some((namespace, name)) = self.bucket.s3_publication_address(key) {
            return installed_object_name(&namespace, &name);
        }
        if self.local.is_some() {
            return Ok(fingerprint(&serde_json::to_vec(&("local-checkpoint", self.provider, key.as_str())).map_err(serialization)?));
        }
        Err(invalid("The checkpoint archive cannot identify an immutable allocation"))
    }

    pub(crate) async fn prepared(
        &self,
        reservation: Reservation,
        origin: Preparation,
    ) -> Result<Option<Checkpoint>> {
        let reader = self.reader(reservation.custody).await?;
        let Some(frame) =
            Self::metadata(reader.as_ref(), &self.manifest(reservation.custody)?).await?
        else {
            return Ok(None);
        };
        let checkpoint = Checkpoint {
            cut: origin.through,
            root: validate_cut(origin.checkpoint, origin.through)?,
            manifest: fingerprint(&frame),
            custody: reservation.custody,
        };
        let manifest = self.read_manifest(reader.as_ref(), checkpoint).await?;
        if origin.authority != manifest.authority {
            return Err(invalid("The prepared checkpoint has different recovery authority"));
        }
        Ok(Some(checkpoint))
    }

    /// Copies the exact reconstructable closure to the independently retained
    /// allocation. The provider rejects a different publication of any member.
    pub(crate) async fn prepare(
        &self,
        reservation: &Reservation,
        origin: Preparation,
        source: &dyn ObjectStore,
    ) -> Result<Checkpoint> {
        if self.pending(reservation).await? != Some(origin) {
            return Err(invalid(
                "The checkpoint copy has no matching prepared source",
            ));
        }
        let Preparation {
            through,
            checkpoint,
            ..
        } = origin;
        let root = validate_cut(checkpoint, through)?;
        let custody = reservation.custody;
        let manifest_locator = self.manifest(custody)?;
        let copy_budget = Self::copy_budget(reservation)?;
        let members =
            crate::archive::ordered_checkpoint_sources(source, checkpoint, self.epoch, copy_budget)
                .await?;
        let mut manifest = Manifest {
            scope: self.scope(),
            allocation: custody.allocation,
            revision: custody.revision,
            next_lsn: through.next_lsn,
            history: through.history,
            authority: origin.authority,
            checkpoint,
            members: BTreeMap::new(),
        };
        for member in members {
            let body = member.read(source).await?;
            let name = self.member(custody, &member.name)?;
            let sha256 = fingerprint(&body);
            self.put(custody, &name, body).await?;
            let reader = self.reader(custody).await?;
            let written = reader.observe_head(&name).await?;
            if written.value.key != name || written.value.size != member.observed_bytes as u64 {
                return Err(invalid(
                    "The copied checkpoint member has a different publication",
                ));
            }
            manifest.members.insert(
                member.name.as_str().to_owned(),
                Member {
                    bytes: written.value.size,
                    sha256,
                    version: written.version,
                },
            );
        }
        let frame = serde_json::to_vec(&manifest).map_err(serialization)?;
        if frame.len() > MAX_MANIFEST_BYTES {
            return Err(invalid("The reserved checkpoint manifest is full"));
        }
        let manifest_sha256 = fingerprint(&frame);
        self.put(custody, &manifest_locator, Bytes::from(frame))
            .await?;
        Ok(Checkpoint {
            cut: through,
            root,
            manifest: manifest_sha256,
            custody,
        })
    }

    /// Establishes provider evidence before the custodian orders publication.
    pub async fn publish(
        &self,
        journal: &InstalledJournal,
        route: &Route,
        checkpoint: Checkpoint,
    ) -> Result<()> {
        if route.placement != self.placement {
            return Err(invalid("The custodian does not own this checkpoint scope"));
        }
        if self.open(checkpoint).await?.authority != CheckpointAuthority::Upload {
            return Err(invalid("A clean checkpoint must publish and rotate together"));
        }
        journal.publish(route, checkpoint).await
    }

    pub async fn open(&self, chosen: Checkpoint) -> Result<OpenedCheckpoint> {
        let reader = self.reader(chosen.custody).await?;
        let manifest = self.read_manifest(reader.as_ref(), chosen).await?;
        let checkpoint = manifest.checkpoint;
        let store = Arc::new(ExactStore {
            bucket: reader,
            prefix: self.allocation(chosen.custody),
            members: manifest.members,
        });
        let root = ObjectKey::new(block_key(rtdb_types::BlockIndex(checkpoint.block))?)?;
        let catalog = BlockCatalog::decode(&store.get(&root).await?)?;
        crate::archive::verify_cut(&catalog, checkpoint, self.epoch)?;
        // Check that the selected catalog's closure is present in this manifest.
        let _ = crate::archive::recovery_manifest_sha256(store.as_ref(), checkpoint, self.epoch)
            .await?;
        Ok(OpenedCheckpoint { checkpoint, store, authority: manifest.authority })
    }

    pub(crate) async fn reopen_survivor(&self, chosen: Checkpoint, binding: ErasureBinding) -> Result<()> {
        let opened = self.open(chosen).await?;
        if opened.authority != CheckpointAuthority::Erasure(binding) {
            return Err(invalid("The provider checkpoint belongs to a different erasure attempt"));
        }
        for member in opened.store.list(None).await? {
            let length = usize::try_from(member.size)
                .map_err(|_| invalid("A clean checkpoint member exceeds addressable length"))?;
            opened.store.observe_fresh_exact(&member.key, length).await?;
        }
        Ok(())
    }

    async fn read_manifest(
        &self,
        reader: &dyn ObjectStore,
        chosen: Checkpoint,
    ) -> Result<Manifest> {
        let locator = self.manifest(chosen.custody)?;
        let frame = Self::metadata(reader, &locator)
            .await?
            .ok_or_else(|| invalid("The exact captured checkpoint manifest is unavailable"))?;
        if fingerprint(&frame) != chosen.manifest {
            return Err(invalid(
                "The checkpoint manifest differs from the installed receipt",
            ));
        }
        let manifest: Manifest = serde_json::from_slice(&frame).map_err(serialization)?;
        if manifest.scope != self.scope()
            || manifest.allocation != chosen.custody.allocation
            || manifest.revision != chosen.custody.revision
            || manifest.next_lsn != chosen.cut.next_lsn
            || manifest.history != chosen.cut.history
            || validate_cut(manifest.checkpoint, chosen.cut)? != chosen.root
        {
            return Err(invalid(
                "The checkpoint manifest addresses a different installed history",
            ));
        }
        Ok(manifest)
    }

    pub(crate) async fn retire_local(&self, chosen: Checkpoint) -> Result<Fingerprint> {
        self.require_provider(chosen.custody)?;
        let local = self
            .local
            .as_ref()
            .ok_or_else(|| invalid("The checkpoint has no local archive owner"))?;
        let scope = self.allocation(chosen.custody);
        let allocation = local.acquire(&scope, true).await?;
        let ledger = self.ledger(chosen.custody, allocation.region());
        if !allocation.require_disposition(chosen.manifest).await? {
            let reader = allocation.read_store(ledger.clone())?;
            self.read_manifest(reader.as_ref(), chosen).await?;
            allocation.close(chosen.manifest).await?;
        }
        ledger.withdraw_scope().await?;
        allocation.retire().await?;
        self.disposition(chosen)
    }

    /// The manifest fence is installed last. Its durable token settles a
    /// retry after the member bytes and the manifest have become unreadable.
    pub(crate) async fn retire_s3(
        &self,
        config: &rtdb_s3::S3Config,
        chosen: Checkpoint,
    ) -> Result<Fingerprint> {
        self.require_provider(chosen.custody)?;
        if config.terminal_scope.is_none() {
            return Err(invalid(
                "Checkpoint retirement requires enrolled terminal storage",
            ));
        }
        let inventory = crate::deployment::CellStorage::s3_inventory(config).await?;
        let effects = inventory.never_versioned_retirement().await?;
        let token = chosen.custody.allocation;
        let root = self.manifest(chosen.custody)?;
        let preparation = self.intent(chosen.custody)?;
        let ledger = self.ledger(chosen.custody, Arc::clone(&self.io));
        if !effects.confirms(&root, token).await? {
            let selected = self.read_manifest(&self.remote_store(chosen.custody), chosen).await?;
            for logical in selected.members.keys() {
                let key = self.member(chosen.custody, &ObjectKey::new(logical.as_str())?)?;
                effects.fence_create_only(&key, token).await?;
            }
            effects.fence_create_only(&preparation, token).await?;
            effects.fence_create_only(&root, token).await?;
        }
        ledger.withdraw_scope().await?;
        if !effects.confirms(&preparation, token).await? {
            return Err(invalid("The provider has not fenced the preparation"));
        }
        self.disposition(chosen)
    }

    fn disposition(&self, chosen: Checkpoint) -> Result<Fingerprint> {
        let token = chosen.custody.allocation;
        let scope = serde_json::to_vec(&self.scope()).map_err(serialization)?;
        let mut attestation = ring::digest::Context::new(&SHA256);
        attestation.update(b"rtdb.multi.ordered-checkpoint-disposition/v2");
        attestation.update(&scope);
        attestation.update(token.as_bytes());
        attestation.update(&chosen.custody.revision.to_be_bytes());
        attestation.update(&chosen.manifest);
        Ok(attestation
            .finish()
            .as_ref()
            .try_into()
            .expect("SHA-256 is 32 bytes"))
    }

    async fn metadata(store: &dyn ObjectStore, key: &ObjectKey) -> Result<Option<Bytes>> {
        let info = match store.head_fresh(key).await {
            Ok(info) => info,
            Err(Error::MissingObject(_)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let length = usize::try_from(info.size)
            .ok()
            .filter(|bytes| *bytes <= MAX_MANIFEST_BYTES)
            .ok_or_else(|| invalid("Checkpoint metadata exceeds its reservation"))?;
        Ok(Some(store.get_fresh_exact(key, length).await?))
    }

    fn intent(&self, id: Custody) -> rtdb_storage::Result<ObjectKey> {
        ObjectKey::new(format!("{}/prepare", self.allocation(id)))
    }

    fn scope(&self) -> Scope {
        Scope {
            deployment: self.placement.deployment,
            incarnation: self.placement.incarnation,
            group: self.placement.group,
            generation: self.placement.generation,
            log_epoch: self.epoch.0,
            provider: self.provider,
        }
    }

    fn require_provider(&self, custody: Custody) -> Result<()> {
        if custody.provider != self.provider {
            return Err(invalid(
                "The installed checkpoint belongs to a different physical archive",
            ));
        }
        Ok(())
    }

    fn allocation(&self, id: Custody) -> String {
        format!(
            "{}/{}/{}/{}/{}",
            self.prefix.as_str(),
            self.placement.deployment,
            self.placement.incarnation,
            id.revision,
            id.allocation
        )
    }

    fn manifest(&self, id: Custody) -> rtdb_storage::Result<ObjectKey> {
        ObjectKey::new(format!("{}/manifest", self.allocation(id)))
    }

    fn member(&self, id: Custody, logical: &ObjectKey) -> rtdb_storage::Result<ObjectKey> {
        ObjectKey::new(format!(
            "{}/objects/{}",
            self.allocation(id),
            logical.as_str()
        ))
    }
}

/// Evidence has its own lifetime. Advancing the readable checkpoint must
/// not retire the witnesses for an outstanding destruction.
pub(super) struct ErasureRecords<'a> {
    provider: &'a CheckpointCustody,
    scope: String,
}

impl CheckpointCustody {
    pub(super) fn erasure_records(&self, placement: Placement, operation: Uuid) -> Result<ErasureRecords<'_>> {
        if self.placement != placement || operation.is_nil() {
            return Err(invalid("The erasure evidence belongs to another placement"));
        }
        Ok(ErasureRecords {
            provider: self,
            scope: format!("{}/{}/{}/erasure/{}/{operation}", self.prefix.as_str(), placement.deployment,
                placement.incarnation, placement.generation),
        })
    }
}

impl ErasureRecords<'_> {
    fn ledger(&self, io: Arc<IoRegion>) -> UnitLedger {
        let scope = UnitScope::new(self.provider.placement, b"erasure-evidence", self.scope.as_bytes());
        UnitLedger::new(Arc::clone(&self.provider.keys), scope, io)
    }

    fn key(&self, name: &str) -> rtdb_storage::Result<ObjectKey> {
        ObjectKey::new(format!("{}/{name}", self.scope))
    }

    fn store(&self) -> EncryptingStore {
        let ledger = self.ledger(Arc::clone(&self.provider.io));
        EncryptingStore::with_units(Arc::clone(&self.provider.bucket), Arc::new(ledger))
    }

    pub(super) async fn get(&self, name: &str) -> Result<Option<Bytes>> {
        let key = self.key(name)?;
        let Some(local) = &self.provider.local else {
            return CheckpointCustody::metadata(&self.store(), &key).await;
        };
        let allocation = local.acquire(&self.scope, false).await?;
        let store = allocation.read_store(self.ledger(allocation.region()))?;
        CheckpointCustody::metadata(store.as_ref(), &key).await
    }

    pub(super) async fn put(&self, name: &str, bytes: Bytes) -> Result<()> {
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(invalid("The erasure record exceeds the custody metadata bound"));
        }
        let key = self.key(name)?;
        let Some(local) = &self.provider.local else {
            self.store().put(&key, bytes).await?;
            return Ok(());
        };
        let allocation = local.acquire(&self.scope, true).await?;
        allocation.put(&self.ledger(allocation.region()), &key, bytes).await?;
        Ok(())
    }
}

struct ExactStore {
    bucket: Arc<dyn ObjectStore>,
    prefix: String,
    members: BTreeMap<String, Member>,
}

impl ExactStore {
    fn member(&self, logical: &ObjectKey) -> rtdb_storage::Result<(&Member, ObjectKey)> {
        let member = self
            .members
            .get(logical.as_str())
            .ok_or_else(|| Error::MissingObject(logical.to_string()))?;
        let physical = ObjectKey::new(format!("{}/objects/{}", self.prefix, logical.as_str()))?;
        Ok((member, physical))
    }

    fn mismatch(logical: &ObjectKey) -> Error {
        Error::ConflictingObject(logical.to_string())
    }
}

#[async_trait]
impl ObjectStore for ExactStore {
    fn s3_publication_address(&self, key: &ObjectKey) -> Option<(rtdb_types::S3ObjectNamespace, String)> {
        let (_, physical) = self.member(key).ok()?;
        self.bucket.s3_publication_address(&physical)
    }
    fn permits_shared_read_cache(&self) -> bool { self.bucket.permits_shared_read_cache() }
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        Ok(self.observe_head(key).await?.value)
    }

    async fn observe_head(
        &self,
        key: &ObjectKey,
    ) -> rtdb_storage::Result<ObjectObservation<StoredObject>> {
        let (member, physical) = self.member(key)?;
        let observed = self.bucket.observe_head(&physical).await?;
        if observed.value.key != physical
            || observed.value.size != member.bytes
            || observed.version != member.version
        {
            return Err(Self::mismatch(key));
        }
        Ok(ObjectObservation {
            value: StoredObject {
                key: key.clone(),
                size: member.bytes,
            },
            version: observed.version,
        })
    }

    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        let (member, _) = self.member(key)?;
        let size = usize::try_from(member.bytes).map_err(|_| Self::mismatch(key))?;
        Ok(self.observe_fresh_exact(key, size).await?.value)
    }

    async fn observe_fresh_exact(
        &self,
        key: &ObjectKey,
        length: usize,
    ) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        let (member, physical) = self.member(key)?;
        if member.bytes != length as u64 {
            return Err(Self::mismatch(key));
        }
        let observed = self.bucket.observe_fresh_exact(&physical, length).await?;
        if observed.version != member.version || fingerprint(&observed.value) != member.sha256 {
            return Err(Self::mismatch(key));
        }
        Ok(observed)
    }

    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        Ok(self.observe_range(key, range).await?.value)
    }

    fn supports_scan_ranges(&self, key: &ObjectKey) -> bool {
        self.member(key).is_ok_and(|(member, physical)| {
            member.version.is_some() && self.bucket.supports_scan_ranges(&physical)
        })
    }

    async fn observe_range(
        &self,
        key: &ObjectKey,
        range: Range<u64>,
    ) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        let (member, physical) = self.member(key)?;
        if range.end > member.bytes || range.end < range.start {
            return Err(Error::InvalidRange {
                key: key.to_string(),
                start: range.start,
                end: range.end,
                length: member.bytes,
            });
        }
        if member.version.is_none() {
            let bytes = self.get(key).await?;
            return Ok(ObjectObservation {
                value: bytes.slice(range.start as usize..range.end as usize),
                version: None,
            });
        }
        let observed = self.bucket.observe_range(&physical, range).await?;
        if observed.version != member.version {
            return Err(Self::mismatch(key));
        }
        Ok(observed)
    }

    async fn put(&self, key: &ObjectKey, _: Bytes) -> rtdb_storage::Result<()> {
        Err(read_only(key))
    }
    async fn put_file(&self, key: &ObjectKey, _: &Path) -> rtdb_storage::Result<()> {
        Err(read_only(key))
    }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        Err(read_only(key))
    }

    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        self.members
            .iter()
            .filter_map(|(name, member)| match ObjectKey::new(name.clone()) {
                Ok(key) if directory.is_none_or(|parent| key.is_under(parent)) => {
                    Some(Ok(StoredObject {
                        key,
                        size: member.bytes,
                    }))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }
}

fn validate_cut(checkpoint: RtdbCheckpoint, cut: Cut) -> Result<Fingerprint> {
    let root = checkpoint
        .root_sha256
        .ok_or_else(|| invalid("The checkpoint lacks its exact RTDB root"))?;
    if checkpoint.block < 0
        || checkpoint
            .boundary
            .checked_add(1)
            .and_then(|next| u64::try_from(next).ok())
            != Some(cut.next_lsn)
    {
        return Err(invalid("The RTDB block and ordered cut disagree"));
    }
    Ok(root)
}

fn fingerprint(bytes: &[u8]) -> Fingerprint {
    digest(&SHA256, bytes)
        .as_ref()
        .try_into()
        .expect("SHA-256 contains exactly 32 bytes")
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::incorrect("rtdb.multi/ordered-checkpoint-custody", message.to_owned())
}

fn serialization(error: serde_json::Error) -> Anomaly {
    invalid(&error.to_string())
}

fn read_only(key: &ObjectKey) -> Error {
    Error::PermissionDenied {
        key: key.to_string(),
        message: "A captured checkpoint is immutable".into(),
    }
}
