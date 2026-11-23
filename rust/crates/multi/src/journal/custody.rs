//! Provider-backed checkpoint custody for installed journals.

use super::InstalledJournal;
use async_trait::async_trait;
use bytes::Bytes;
use ordered_logkeeper::ordered::{
    Checkpoint, Custody, Cut, Fingerprint, Grant, Placement, service::Route,
};
use ring::digest::{SHA256, digest};
use rtdb_directory::Checkpoint as RtdbCheckpoint;
use rtdb_format::{catalog::BlockCatalog, keys::block_key};
use rtdb_storage::{Error, ObjectKey, ObjectObservation, ObjectStore, object_store::StoredObject};
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
    store: Arc<dyn ObjectStore>,
    placement: Placement,
    epoch: StorageEpoch,
    prefix: ObjectKey,
}

/// Exact space reserved by the custodian before staging any checkpoint copy.
#[derive(Clone, Copy, Deserialize)]
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
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    scope: Scope,
    allocation: Uuid,
    revision: u64,
    next_lsn: u64,
    history: Fingerprint,
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
}

impl CheckpointCustody {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: ObjectKey,
        placement: Placement,
        epoch: StorageEpoch,
    ) -> Self {
        Self {
            store,
            placement,
            epoch,
            prefix,
        }
    }

    /// Copies the exact reconstructable closure to the independently retained
    /// allocation. The manifest becomes the root of the recovery receipt.
    pub async fn prepare(
        &self,
        reservation: &Reservation,
        source: &dyn ObjectStore,
        through: Cut,
        checkpoint: RtdbCheckpoint,
    ) -> Result<Checkpoint> {
        let root = validate_cut(checkpoint, through)?;
        let custody = reservation.custody;
        if custody.allocation.is_nil() || custody.revision <= 1 {
            return Err(invalid("The checkpoint has no new provider reservation"));
        }
        let copy_budget = reservation
            .usable_bytes
            .checked_sub(MAX_MANIFEST_BYTES as u64)
            .ok_or_else(|| invalid("The reservation cannot hold the checkpoint manifest"))?;
        let members =
            crate::archive::ordered_checkpoint_sources(source, checkpoint, self.epoch, copy_budget)
                .await?;
        let mut manifest = Manifest {
            scope: self.scope(),
            allocation: custody.allocation,
            revision: custody.revision,
            next_lsn: through.next_lsn,
            history: through.history,
            checkpoint,
            members: BTreeMap::new(),
        };
        for member in members {
            let body = member.read(source).await?;
            let name = self.member(custody, &member.name)?;
            let sha256 = fingerprint(&body);
            self.store.put(&name, body).await?;
            let written = self.store.observe_head(&name).await?;
            if written.value.key != name || written.value.size != member.logical_bytes as u64 {
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
        self.store
            .put(&self.manifest(custody)?, Bytes::from(frame))
            .await?;
        let prepared = Checkpoint {
            cut: through,
            root,
            manifest: manifest_sha256,
            custody,
        };
        self.open(prepared).await?;
        Ok(prepared)
    }

    /// Establishes provider evidence before the custodian orders publication.
    pub async fn publish(
        &self,
        journal: &InstalledJournal,
        route: &Route,
        grant: Grant,
        checkpoint: Checkpoint,
    ) -> Result<()> {
        if route.placement != self.placement || route.writer.grant != Some(grant) {
            return Err(invalid(
                "The custodian does not hold the active checkpoint scope",
            ));
        }
        self.open(checkpoint).await?;
        journal.publish(route, grant, checkpoint).await
    }

    pub async fn open(&self, chosen: Checkpoint) -> Result<OpenedCheckpoint> {
        let locator = self.manifest(chosen.custody)?;
        let info = self.store.head_fresh(&locator).await?;
        let length = usize::try_from(info.size)
            .ok()
            .filter(|bytes| *bytes <= MAX_MANIFEST_BYTES)
            .ok_or_else(|| invalid("The captured checkpoint manifest exceeds its reservation"))?;
        let frame = self.store.get_fresh_exact(&locator, length).await?;
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
        let checkpoint = manifest.checkpoint;
        let store = Arc::new(ExactStore {
            bucket: Arc::clone(&self.store),
            prefix: self.allocation(chosen.custody),
            members: manifest.members,
        });
        let root = ObjectKey::new(block_key(rtdb_types::BlockIndex(checkpoint.block))?)?;
        let catalog = BlockCatalog::decode(&store.get(&root).await?)?;
        crate::archive::verify_cut(&catalog, checkpoint, self.epoch)?;
        // Enumerating the actual root again ensures every referenced member
        // can be opened by this exact manifest before the journal can use it.
        let _ = crate::archive::recovery_manifest_sha256(store.as_ref(), checkpoint, self.epoch)
            .await?;
        Ok(OpenedCheckpoint { checkpoint, store })
    }

    fn scope(&self) -> Scope {
        Scope {
            deployment: self.placement.deployment,
            incarnation: self.placement.incarnation,
            group: self.placement.group,
            generation: self.placement.generation,
            log_epoch: self.epoch.0,
        }
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
