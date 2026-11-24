//! Retirement of the serving prefixes written by superseded writer grants of
//! an installed journal. Each grant writes below its own epoch prefix; the
//! custodian removes one only after the journal shows that nothing retained,
//! held or pending can read it and the provider shows that nothing can still
//! write it.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use ordered_logkeeper::ordered::{Cut, Grant, service::Route};
use rtdb_storage::{Error, IoRegion, ObjectKey, ObjectStore, sync_directory};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{Placement, genesis};
use crate::deployment::CellStorage;
use crate::journal::InstalledJournal;
use crate::keys::KeyService;
use crate::journal::prefix_retirement::{leases_expired, may_retire, superseded};

pub(crate) const DESCRIPTOR: &str = "~grant";
const RETIRED: &str = "~retired";
const CLOSED: &str = "closed";

/// Written by a writer when it first opens its grant's prefix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GrantDescriptor {
    pub deployment: String,
    pub incarnation: u64,
    pub placement: Uuid,
    pub placement_incarnation: Uuid,
    pub group: Uuid,
    pub generation: u64,
    pub worker: Uuid,
    pub serial: u64,
    pub epoch: u64,
}

impl GrantDescriptor {
    pub(crate) fn new(placement: &Placement, grant: Grant) -> Self {
        let installed = placement.worker.placement();
        Self {
            deployment: placement.record.id.clone(),
            incarnation: placement.record.incarnation,
            placement: installed.deployment,
            placement_incarnation: installed.incarnation,
            group: installed.group,
            generation: installed.generation,
            worker: grant.enrollment.worker,
            serial: grant.enrollment.serial,
            epoch: grant.epoch,
        }
    }

    fn binds(&self, placement: &Placement, epoch: u64) -> bool {
        let installed = placement.worker.placement();
        self.deployment == placement.record.id && self.incarnation == placement.record.incarnation
            && self.placement == installed.deployment && self.placement_incarnation == installed.incarnation
            && self.group == installed.group && self.generation == installed.generation && self.epoch == epoch
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PrefixInventory {
    LocalDirectory,
    S3ExactVersions,
    S3ConditionalTombstones,
}

/// The identity of one retirement, created once before any provider effect.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    grant: GrantDescriptor,
    provider: [u8; 32],
    inventory: PrefixInventory,
    token: Uuid,
    fence_observed_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    intent: Intent,
    checkpoint_lsn: u64,
    checkpoint_root: [u8; 32],
    floor_lsn: u64,
    inspected_through_lsn: u64,
    settled_at_ms: i64,
    key_scope_withdrawn: bool,
}

/// What one pass established for one superseded epoch.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Outcome {
    Retired,
    Refused(&'static str),
    /// The provider or journal failed for this epoch; later epochs still run.
    Failed(Anomaly),
}

fn generation_root(placement: &Placement) -> String {
    format!("{}/~ordered/{}", placement.record.storage_prefix(), placement.worker.placement().generation)
}

fn grant_prefix(placement: &Placement, epoch: u64) -> String {
    super::storage::grant_prefix(&placement.record.storage_prefix(), placement.worker.placement().generation, epoch)
}

pub(crate) fn descriptor_key(prefix: &str) -> Result<ObjectKey> {
    Ok(ObjectKey::new(format!("{prefix}/{DESCRIPTOR}"))?)
}

fn key(name: String) -> Result<ObjectKey> { Ok(ObjectKey::new(name)?) }

fn unavailable(message: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/serving-prefix-retirement", message)
}

/// The retirement token conditions every provider effect on this grant.
fn token(grant: &GrantDescriptor) -> Result<Uuid> {
    let encoded = serde_json::to_vec(grant).map_err(|error| Anomaly::fault("rtdb.multi/serving-prefix-retirement", error.to_string()))?;
    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    digest.update(b"rtdb.multi.serving-prefix-retirement/v1");
    digest.update(&encoded);
    let digest = digest.finish();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_ref()[..16]);
    Ok(uuid::Builder::from_random_bytes(bytes).into_uuid())
}

async fn read_json<T: serde::de::DeserializeOwned>(bucket: &dyn ObjectStore, key: &ObjectKey) -> Result<Option<T>> {
    match bucket.get_fresh(key).await {
        Ok(bytes) if bytes.is_empty() => Ok(None),
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some)
            .map_err(|_| Anomaly::incorrect("rtdb.multi/serving-prefix-retirement", format!("{key} is not a retirement record"))),
        Err(Error::MissingObject(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn put_json<T: Serialize>(bucket: &dyn ObjectStore, key: &ObjectKey, value: &T) -> Result<()> {
    let encoded = serde_json::to_vec(value).map_err(|error| Anomaly::fault("rtdb.multi/serving-prefix-retirement", error.to_string()))?;
    bucket.put(key, Bytes::from(encoded)).await.map_err(Into::into)
}

/// The shared lock a local reader or writer holds for the life of its store.
/// A closed marker is permanent, so a late open is refused.
pub(crate) fn hold_local(root: &Path, prefix: &str, exclusive: bool) -> Result<File> {
    let lifecycle = lifecycle_path(root, prefix)?;
    fs::create_dir_all(&lifecycle).map_err(|error| local_error(&lifecycle, error))?;
    let lock_path = lifecycle.join("lock");
    let lock = OpenOptions::new().create(true).truncate(false).write(true).open(&lock_path)
        .map_err(|error| local_error(&lock_path, error))?;
    match if exclusive { lock.try_lock() } else { lock.try_lock_shared() } {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/serving-prefix-held", "Another process holds this serving prefix")),
        Err(TryLockError::Error(error)) => return Err(local_error(&lock_path, error)),
    }
    if !exclusive && lifecycle.join(CLOSED).exists() {
        return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/serving-prefix-retired", "The writer grant's serving prefix is permanently retired"));
    }
    Ok(lock)
}

fn lifecycle_path(root: &Path, prefix: &str) -> Result<PathBuf> {
    let relative = ObjectKey::new(prefix)?;
    Ok(root.join(format!("{relative}.lifecycle")))
}

fn local_error(path: &Path, error: std::io::Error) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/serving-prefix-local", format!("{}: {error}", path.display()))
}

fn sync_up(root: &Path, mut directory: PathBuf) -> Result<()> {
    loop {
        sync_directory(&directory).map_err(|error| local_error(&directory, error))?;
        if directory == root || !directory.pop() { return Ok(()); }
    }
}

enum Provider {
    Local(PathBuf),
    ExactVersions(rtdb_s3::VersionedInventory),
    Tombstones(rtdb_s3::VersionedInventory),
}

impl Provider {
    async fn qualify(storage: &CellStorage) -> Result<Option<Self>> {
        Ok(match storage {
            CellStorage::Local { root } => Some(Self::Local(root.canonicalize().map_err(|error| local_error(root, error))?)),
            CellStorage::S3 { config } => {
                let inventory = CellStorage::s3_inventory(config).await?;
                // An enrollment that cannot be confirmed must not record another inventory kind.
                if inventory.enrolled_terminal_retirement().await?.is_some() {
                    Some(Self::Tombstones(inventory))
                } else if inventory.require_versioned().await.is_ok() {
                    Some(Self::ExactVersions(inventory))
                } else {
                    None
                }
            }
        })
    }

    fn inventory(&self) -> PrefixInventory {
        match self {
            Self::Local(_) => PrefixInventory::LocalDirectory,
            Self::ExactVersions(_) => PrefixInventory::S3ExactVersions,
            Self::Tombstones(_) => PrefixInventory::S3ConditionalTombstones,
        }
    }
}

/// The grant epochs of the records the journal still retains.
async fn retained_epochs(custodian: &InstalledJournal, route: &Route) -> Result<BTreeSet<u64>> {
    let maximum = (crate::process::MAXIMUM_LOG_MESSAGE_BYTES
        + ordered_logkeeper::ordered::APPEND_RECORD_CHARGE_BYTES as usize) as u64;
    let mut epochs = BTreeSet::new();
    let mut from: Cut = route.checkpoint.cut;
    while from != route.head {
        let page = custodian.inspect_checkpoint(route.checkpoint, from, route.head, maximum).await?;
        let Some(last) = page.records.last() else {
            return Err(unavailable("The journal returned an empty inspection page"));
        };
        from = last.result.cut;
        epochs.extend(page.records.iter().map(|record| record.grant.epoch));
    }
    Ok(epochs)
}

async fn superseded_prefixes(provider: &Provider, placement: &Placement, route: &Route) -> Result<BTreeSet<u64>> {
    let root = generation_root(placement);
    let children: Vec<String> = match provider {
        Provider::Local(local) => match fs::read_dir(local.join(ObjectKey::new(root.as_str())?.as_str())) {
            Ok(entries) => entries.filter_map(|entry| entry.ok()?.file_name().into_string().ok()).collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(local_error(local, error)),
        },
        Provider::ExactVersions(inventory) | Provider::Tombstones(inventory) => {
            inventory.child_directories(&key(root)?).await?.into_iter().collect()
        }
    };
    let current = route.writer.grant.map(|grant| grant.epoch);
    Ok(children.iter().filter_map(|name| name.parse::<u64>().ok())
        .filter(|epoch| superseded(*epoch, route.writer.epoch, current)).collect())
}

/// Retires every superseded serving prefix that the evidence covers and
/// reports, per epoch, what this pass established.
pub(crate) async fn retire(placement: &Placement, storage: &CellStorage, keys: &Arc<dyn KeyService>) -> Result<Vec<(u64, Outcome)>> {
    let Some(custodian) = placement.custodian.as_ref() else { return Ok(Vec::new()); };
    let route = custodian.route().await?;
    let Some(provider) = Provider::qualify(storage).await? else { return Ok(Vec::new()); };
    let bucket = storage.bucket(IoRegion::new()).await?;
    let mut epochs = superseded_prefixes(&provider, placement, &route).await?;
    // An intent whose provider effects removed the prefix before its receipt
    // was written is found from the intent itself.
    let retired_root = key(format!("{}/{RETIRED}", generation_root(placement)))?;
    let current = route.writer.grant.map(|grant| grant.epoch);
    for object in bucket.list(Some(&retired_root)).await? {
        let name = object.key.as_str().rsplit('/').next().unwrap_or_default();
        if let Ok(epoch) = name.parse::<u64>() && superseded(epoch, route.writer.epoch, current) {
            epochs.insert(epoch);
        }
    }
    let mut candidates = Vec::new();
    for epoch in epochs {
        let settled = key(format!("{}/{RETIRED}/{epoch}.settled", generation_root(placement)))?;
        if read_json::<Receipt>(bucket.as_ref(), &settled).await?.is_none() {
            candidates.push((epoch, settled));
        }
    }
    if candidates.is_empty() { return Ok(Vec::new()); }
    let identity = storage.custody_namespace()?;
    let independent = !genesis(route.checkpoint);
    let trimmed = route.floor == route.checkpoint.cut;
    let retained = if independent && trimmed { Some(retained_epochs(custodian, &route).await?) } else { None };
    let storage_root = crate::archive::checkpoint_storage_root()?;
    let mut outcomes = Vec::new();
    for (epoch, settled) in candidates {
        let prefix = grant_prefix(placement, epoch);
        let objects = format!("{prefix}/{storage_root}");
        let descriptor = descriptor_key(&objects)?;
        let outcome = retire_one(RetireOne {
            placement, bucket: bucket.as_ref(), provider: Some(&provider), identity, route: &route,
            retained: retained.as_ref(), independent, trimmed, epoch, prefix: &prefix, settled: &settled,
            objects: &objects,
            descriptor: &descriptor,
            keys,
        }).await;
        outcomes.push((epoch, outcome.unwrap_or_else(Outcome::Failed)));
    }
    Ok(outcomes)
}

struct RetireOne<'a> {
    placement: &'a Placement,
    bucket: &'a dyn ObjectStore,
    provider: Option<&'a Provider>,
    identity: [u8; 32],
    route: &'a Route,
    retained: Option<&'a BTreeSet<u64>>,
    independent: bool,
    trimmed: bool,
    epoch: u64,
    prefix: &'a str,
    settled: &'a ObjectKey,
    objects: &'a str,
    descriptor: &'a ObjectKey,
    keys: &'a Arc<dyn KeyService>,
}

async fn retire_one(one: RetireOne<'_>) -> Result<Outcome> {
    let RetireOne { placement, bucket, provider, identity, route, retained, independent, trimmed, epoch, prefix, settled, objects, descriptor, keys } = one;
    let qualified = provider.is_some();
    let Some(provider) = provider else { return Ok(Outcome::Refused("unqualified provider")) };
    let intent_key = key(format!("{}/{RETIRED}/{epoch}", generation_root(placement)))?;
    let mut intent = read_json::<Intent>(bucket, &intent_key).await?;
    let grant = match (&intent, read_json::<GrantDescriptor>(bucket, descriptor).await?) {
        (Some(intent), _) => intent.grant.clone(),
        (None, Some(grant)) => grant,
        (None, None) => return Ok(Outcome::Refused("no grant descriptor")),
    };
    let exact = grant.binds(placement, epoch) && intent.as_ref().is_none_or(|intent| {
        intent.provider == identity && intent.inventory == provider.inventory()
    });
    if !exact { return Ok(Outcome::Refused("binding mismatch")); }
    let token = token(&grant)?;
    if intent.is_none() {
        let created = Intent { grant: grant.clone(), provider: identity, inventory: provider.inventory(), token, fence_observed_at_ms: rtdb_directory::now_ms() };
        put_json(bucket, &intent_key, &created).await?;
        intent = read_json(bucket, &intent_key).await?;
    }
    let intent = intent.ok_or_else(|| unavailable("The retirement intent was not recorded"))?;
    if intent.token != token { return Ok(Outcome::Refused("binding mismatch")); }

    let is_superseded = superseded(epoch, route.writer.epoch, route.writer.grant.map(|grant| grant.epoch));
    let retained_from_grant = retained.is_none_or(|epochs| epochs.contains(&epoch));
    let preparing = route.preparing.is_some_and(|preparation| preparation.source.epoch == epoch);
    let evidence = |drained| may_retire(is_superseded, exact, independent, trimmed, retained_from_grant, preparing, drained, qualified);
    // The journal's evidence is checked before any provider step; the drain
    // is then established and the whole guard decided again.
    if !evidence(true) {
        return Ok(Outcome::Refused(if !is_superseded { "current grant" }
            else if !independent || !trimmed { "history not trimmed to an independent checkpoint" }
            else if preparing { "pending preparation" }
            else { "retained history" }));
    }
    let scope = key(prefix.to_owned())?;
    let (lock, drained) = match provider {
        Provider::Local(root) => match hold_local(root, prefix, true) {
            Ok(lock) => (Some(lock), true),
            Err(error) if error.code == "rtdb.multi/serving-prefix-held" => (None, false),
            Err(error) => return Err(error),
        },
        Provider::ExactVersions(inventory) | Provider::Tombstones(inventory) => {
            let horizon = (rtdb_s3::SCOPED_S3_WRITE_TTL + rtdb_s3::SCOPED_S3_CLOCK_ALLOWANCE).as_millis() as i64;
            let expired = leases_expired(rtdb_directory::now_ms(), intent.fence_observed_at_ms, horizon);
            if expired { inventory.drain_uploads(&scope).await?; }
            (None, expired)
        }
    };
    if !evidence(drained) {
        return Ok(Outcome::Refused(if matches!(provider, Provider::Local(_)) { "held" } else { "scoped write leases outstanding" }));
    }

    match provider {
        Provider::Local(root) => {
            let lock = lock.ok_or_else(|| unavailable("The local prefix lock was not acquired"))?;
            let lifecycle = lifecycle_path(root, prefix)?;
            let digest = serde_json::to_vec(&intent).map_err(|error| Anomaly::fault("rtdb.multi/serving-prefix-retirement", error.to_string()))?;
            let lifetime: Arc<dyn Send + Sync> = Arc::new(lock);
            let io = IoRegion::new().child_holding(Some(lifetime));
            let state = rtdb_storage::LocalStore::open_in(&lifecycle, Arc::clone(&io))?;
            let closed = ObjectKey::new(CLOSED)?;
            match state.get_fresh(&closed).await {
                Ok(existing) if existing.as_ref() == digest.as_slice() => {}
                Ok(_) => return Err(Anomaly::incorrect("rtdb.multi/serving-prefix-retirement", "Another retirement closed this serving prefix")),
                Err(Error::MissingObject(_)) => state.put(&closed, Bytes::from(digest)).await?,
                Err(error) => return Err(error.into()),
            }
            let volume = root.clone();
            io.run_blocking(move || {
                sync_up(&volume, lifecycle).map_err(|error| Error::Task(error.to_string()))
            }).await?;
            let ledger = super::storage::serving_ledger(
                Arc::clone(keys), placement.worker.placement(), identity, objects, Arc::clone(&io),
            );
            ledger.withdraw_scope().await?;
            let root = root.clone();
            let prefix_path = root.join(ObjectKey::new(prefix)?.as_str());
            io.run_blocking(move || {
                match fs::remove_dir_all(&prefix_path) {
                    Ok(()) => {}
                    Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                    Err(source) => return Err(Error::Io { path: prefix_path.clone(), source }),
                }
                if let Some(parent) = prefix_path.parent() {
                    sync_up(&root, parent.to_path_buf()).map_err(|error| Error::Task(error.to_string()))?;
                }
                Ok(())
            }).await?;
        }
        Provider::ExactVersions(inventory) | Provider::Tombstones(inventory) => {
            let ledger = super::storage::serving_ledger(
                Arc::clone(keys), placement.worker.placement(), identity, objects, IoRegion::new(),
            );
            ledger.withdraw_scope().await?;
            if matches!(provider, Provider::ExactVersions(_)) {
                inventory.purge(&scope).await?;
                if !inventory.is_empty(&scope).await? || !bucket.list(Some(&scope)).await?.is_empty() {
                    return Ok(Outcome::Refused("provider inventory still populated"));
                }
            } else {
                let effects = inventory.never_versioned_retirement().await?;
                // The descriptor is fenced last, so its tombstone settles a
                // retry after every other member is already unreadable.
                if !effects.confirms(descriptor, token).await? {
                    for object in bucket.list(Some(&scope)).await? {
                        if &object.key != descriptor { effects.fence_create_only(&object.key, token).await?; }
                    }
                    effects.fence_create_only(descriptor, token).await?;
                }
                inventory.drain_uploads(&scope).await?;
                for object in bucket.list(Some(&scope)).await? {
                    if !effects.confirms(&object.key, token).await? {
                        return Ok(Outcome::Refused("provider inventory still populated"));
                    }
                }
            }
        }
    }
    put_json(bucket, settled, &Receipt {
        intent,
        checkpoint_lsn: route.checkpoint.cut.next_lsn,
        checkpoint_root: route.checkpoint.root,
        floor_lsn: route.floor.next_lsn,
        inspected_through_lsn: route.head.next_lsn,
        settled_at_ms: rtdb_directory::now_ms(),
        key_scope_withdrawn: true,
    }).await?;
    Ok(Outcome::Retired)
}

/// Opens a reader or writer's hold on a local grant prefix for its store's lifetime.
pub(crate) fn local_lifetime(storage: &CellStorage, prefix: &str, io: &Arc<IoRegion>) -> Result<Arc<IoRegion>> {
    match storage {
        CellStorage::Local { root } => {
            let root = root.canonicalize().map_err(|error| local_error(root, error))?;
            let lock = hold_local(&root, prefix, false)?;
            Ok(io.child_holding(Some(Arc::new(lock))))
        }
        CellStorage::S3 { .. } => Ok(Arc::clone(io)),
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_prefix_refuses_retirement_and_a_closed_prefix_refuses_every_later_open() {
        let volume = tempfile::tempdir().unwrap();
        let root = volume.path().canonicalize().unwrap();
        let prefix = "0000/acme/lineage/id/1/~ordered/0/1";
        let successor = "0000/acme/lineage/id/1/~ordered/0/10";
        let writer = hold_local(&root, prefix, false).unwrap();
        let reader = hold_local(&root, prefix, false).unwrap();
        assert_eq!(hold_local(&root, prefix, true).unwrap_err().code, "rtdb.multi/serving-prefix-held");
        drop((writer, reader));
        let successor_writer = hold_local(&root, successor, false).unwrap();

        let retirement = hold_local(&root, prefix, true).unwrap();
        assert_eq!(hold_local(&root, prefix, false).unwrap_err().code, "rtdb.multi/serving-prefix-held");
        fs::write(lifecycle_path(&root, prefix).unwrap().join(CLOSED), b"intent").unwrap();
        drop(retirement);
        assert_eq!(hold_local(&root, prefix, false).unwrap_err().code, "rtdb.multi/serving-prefix-retired");
        assert!(hold_local(&root, prefix, true).is_ok(), "a retry can resume the closed retirement");
        drop(successor_writer);
        assert!(hold_local(&root, successor, false).is_ok(), "the successor's prefix stays open");
    }

    #[test]
    fn the_token_binds_the_exact_grant() {
        let grant = GrantDescriptor {
            deployment: "id".into(), incarnation: 1, placement: Uuid::from_u128(1), placement_incarnation: Uuid::from_u128(2),
            group: Uuid::from_u128(3), generation: 0, worker: Uuid::from_u128(4), serial: 1, epoch: 3,
        };
        let token = token(&grant).unwrap();
        assert_eq!(token, super::token(&grant).unwrap());
        for changed in [
            GrantDescriptor { epoch: 4, ..grant.clone() },
            GrantDescriptor { serial: 2, ..grant.clone() },
            GrantDescriptor { generation: 1, ..grant.clone() },
        ] {
            assert_ne!(super::token(&changed).unwrap(), token);
        }
    }
}
