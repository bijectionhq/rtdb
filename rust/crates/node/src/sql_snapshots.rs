use std::{collections::VecDeque, sync::{Arc, Weak}, time::Duration};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_query::QueryContext;
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, CatalogCut, Instant, PlacedAttemptScope, ReplicaPosition, Result, Timestamp, TransactionKey};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const CAPACITY: usize = 128;
const HOLD_FOR: Duration = Duration::from_secs(60);
const DURABLE_PREFIX: &str = "rtdb-snapshot-v2:";
const MAX_ENVELOPE_BYTES: usize = 2048;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExactSnapshot {
    owner: Option<String>,
    database: String,
    history: [u8; 32],
    cut: CatalogCut,
    head: Option<TransactionKey>,
    block: BlockIndex,
    cap: Option<Timestamp>,
    clock: Instant,
    timezone: String,
    issued: Timestamp,
    replay: Option<SnapshotReplayBase>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum SnapshotReplayBase {
    Genesis,
    Checkpoint { block: BlockIndex, boundary: ReplicaPosition, root: [u8; 32] },
}

impl ExactSnapshot {
    fn capture(
        view: &rtdb_query::SqlSnapshotView, database: &crate::Database,
        owner: Option<&str>, scope: PlacedAttemptScope, issued: Timestamp,
    ) -> Result<Option<Self>> {
        if view.catalog.database_names().count() != 1 || database.name() != view.database() {
            return Ok(None);
        }
        let captured = view.catalog.database(view.database())?;
        let [snapshot] = captured.partitions.as_slice() else { return Ok(None); };
        if !Arc::ptr_eq(&captured.storage, &database.storage) || snapshot.has_overlays() {
            return Ok(None);
        }
        let Some(cut) = snapshot.catalog_cut() else { return Ok(None); };
        let replay = match snapshot.table_catalog().block() {
            Some(block) => block.boundary_replica_position().zip(block.publication_sha256())
                .filter(|(boundary, _)| *boundary <= cut.through && boundary.epoch() == cut.replica_epoch)
                .map(|(boundary, root)| SnapshotReplayBase::Checkpoint { block: block.block_index(), boundary, root }),
            None if snapshot.block_index() == BlockIndex(0) => Some(SnapshotReplayBase::Genesis),
            None => None,
        };
        Ok(Some(Self {
            owner: owner.map(str::to_owned), database: view.database().to_owned(),
            history: scope.history_fingerprint(), cut, head: snapshot.tx_basis(),
            block: snapshot.block_index(), cap: snapshot.system_time_cap(),
            clock: view.current_time, timezone: view.timezone.clone(), issued, replay,
        }))
    }

    fn matches(&self, snapshot: &rtdb_index::Snapshot) -> bool {
        !snapshot.has_overlays() && snapshot.catalog_cut() == Some(self.cut)
            && snapshot.tx_basis() == self.head && snapshot.block_index() == self.block
    }

    fn is_live(&self, now: Timestamp) -> bool {
        now >= self.issued && now.0.checked_sub(self.issued.0)
            .is_some_and(|age| age < HOLD_FOR.as_micros() as i64)
    }
}

#[derive(Default)]
pub(crate) struct SqlSnapshots {
    entries: parking_lot::Mutex<VecDeque<(String, Arc<SqlSnapshot>)>>,
}

pub(crate) struct SqlSnapshot {
    pub(crate) owner: Option<Arc<str>>,
    pub(crate) view: rtdb_query::SqlSnapshotView,
    expires: tokio::time::Instant,
    _charge: Arc<MemoryReservation>,
}

impl SqlSnapshots {
    fn issue(
        &self, owner: Option<Arc<str>>, view: rtdb_query::SqlSnapshotView, memory: &Arc<MemoryBudget>,
        database: Option<&crate::Database>, issued: Timestamp,
    ) -> Result<String> {
        let mut token = None;
        if let Some(database) = database
            && let Some(codec) = database.await_authority.codec()
            && let Some(exact) = ExactSnapshot::capture(&view, database, owner.as_deref(), codec.scope(), issued)?
        {
            let encoded = serde_json::to_vec(&exact).map_err(|_| unretained())?;
            let sealed = codec.seal_snapshot(&encoded)?;
            if sealed.len() > MAX_ENVELOPE_BYTES { return Err(unretained()); }
            token = Some(format!("{DURABLE_PREFIX}{sealed}"));
        }
        let token = token.unwrap_or_else(|| format!("rtdb-snapshot:{}", Uuid::new_v4()));
        let bytes = view.retained_bytes()?.checked_add(owner.as_ref().map_or(0, |owner| owner.len()) + 256)
            .and_then(|bytes| bytes.checked_add(token.len())).ok_or_else(unretained)?;
        let charge = memory.reserve(bytes)?;
        let expires = tokio::time::Instant::now() + HOLD_FOR;
        let view = with_expiry(view, expires.into_std());
        let entry = Arc::new(SqlSnapshot { owner, view, expires, _charge: charge });
        let evicted = {
            let mut entries = self.entries.lock();
            let evicted = (entries.len() >= CAPACITY).then(|| entries.pop_front()).flatten();
            entries.push_back((token.clone(), entry));
            evicted
        };
        drop(evicted);
        Ok(token)
    }

    pub(crate) fn start(self: &Arc<Self>, cancellation: tokio_util::sync::CancellationToken) {
        let registry = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = ticks.tick() => {}
                }
                let Some(registry) = registry.upgrade() else { break; };
                let now = tokio::time::Instant::now();
                loop {
                    let entry = {
                        let mut entries = registry.entries.lock();
                        if entries.front().is_some_and(|(_, entry)| entry.expires <= now) {
                            entries.pop_front()
                        } else {
                            None
                        }
                    };
                    if entry.is_none() { break; }
                }
            }
        });
    }

    fn select(&self, token: &str, owner: Option<&str>, database: &str) -> Option<Arc<SqlSnapshot>> {
        self.entries.lock().iter().find_map(|(key, entry)| {
            (key == token && entry.owner.as_deref() == owner && entry.view.database() == database
                && tokio::time::Instant::now() < entry.expires)
                .then(|| Arc::clone(entry))
        })
    }

    pub(crate) fn with_issuer(
        self: &Arc<Self>, context: Arc<QueryContext>, owner: Option<&str>,
        node: crate::Node, database: Option<Weak<crate::Database>>,
    ) -> Arc<QueryContext> {
        let registry = Arc::downgrade(self);
        let owner = owner.map(Arc::<str>::from);
        context.with_snapshot_token_issuer(Arc::new(move |view| {
            let registry = registry.upgrade().ok_or_else(unretained)?;
            let database = database.as_ref().and_then(Weak::upgrade);
            registry.issue(owner.clone(), view, node.memory(), database.as_deref(), node.now())
        }))
    }

}

fn with_expiry(mut view: rtdb_query::SqlSnapshotView, deadline: std::time::Instant) -> rtdb_query::SqlSnapshotView {
    let lease = Arc::new(rtdb_storage::ReadLease::until(deadline));
    let lease = view.catalog.read_lease().map_or_else(|| Arc::clone(&lease), |original| original.earlier(Arc::clone(&lease)));
    view.catalog = view.catalog.with_read_lease(lease);
    view
}

pub(crate) fn unretained() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb/snapshot-unretained", "The exact SQL snapshot cannot be readmitted")
}

pub(crate) struct RecoveredSqlSnapshot {
    pub(crate) partition: rtdb_index::Snapshot,
    pub(crate) suffix: Option<Arc<dyn Send + Sync>>,
    pub(crate) storage: Option<Arc<rtdb_storage::cache::BufferPool>>,
}

pub(crate) struct SqlSnapshotAdmission {
    pub(crate) node: crate::Node,
    pub(crate) registry: Arc<SqlSnapshots>,
    pub(crate) identity: Arc<dyn crate::SessionIdentity>,
}

impl SqlSnapshotAdmission {
    pub(crate) async fn restore(
        &self, token: &str, current: &QueryContext, cancellation: &CancellationToken,
    ) -> Result<rtdb_query::SqlSnapshotView> {
        use rtdb_plan::Catalog;

        let name = current.catalog.default_database();
        if let Some(retained) = self.registry.select(token, self.identity.user(), name) {
            if current.timezone != retained.view.timezone { return Err(unretained()); }
            self.node.admit_retained_catalog(&retained.view.catalog, self.identity.as_ref(), cancellation).await?;
            return Ok(retained.view.clone());
        }
        let envelope = token.strip_prefix(DURABLE_PREFIX)
            .filter(|sealed| !sealed.is_empty() && sealed.len() <= MAX_ENVELOPE_BYTES)
            .ok_or_else(unretained)?;
        self.node.admit_retained_catalog(&current.catalog, self.identity.as_ref(), cancellation).await?;
        let database = self.node.catalog().database(name).map_err(|_| unretained())?;
        let current_database = current.catalog.database(name)?;
        if !Arc::ptr_eq(&database.storage, &current_database.storage) || current.catalog.database_names().count() != 1 {
            return Err(unretained());
        }
        let codec = database.await_authority.codec().ok_or_else(unretained)?;
        let bytes = codec.open_snapshot(envelope).map_err(|_| unretained())?;
        let exact: ExactSnapshot = serde_json::from_slice(&bytes).map_err(|_| unretained())?;
        if exact.owner.as_deref() != self.identity.user() || exact.database != name
            || exact.history != codec.scope().history_fingerprint() || exact.timezone != current.timezone
            || !exact.is_live(self.node.now())
        {
            return Err(unretained());
        }
        let expiry = exact.issued.0.checked_add(HOLD_FOR.as_micros() as i64).ok_or_else(unretained)?;
        let remaining = expiry.checked_sub(self.node.now().0).and_then(|micros| u64::try_from(micros).ok())
            .filter(|micros| *micros > 0 && *micros <= HOLD_FOR.as_micros() as u64).ok_or_else(unretained)?;
        let deadline = std::time::Instant::now().checked_add(Duration::from_micros(remaining)).ok_or_else(unretained)?;
        let [partition] = current_database.partitions.as_slice() else { return Err(unretained()); };
        let recovered = if exact.matches(partition) {
            RecoveredSqlSnapshot { partition: partition.clone(), suffix: None, storage: None }
        } else {
            let cut = partition.catalog_cut().filter(|cut| cut.storage_version == exact.cut.storage_version
                && cut.storage_epoch == exact.cut.storage_epoch && cut.replica_epoch == exact.cut.replica_epoch)
                .ok_or_else(unretained)?;
            if cut.through < exact.cut.through {
                tokio::time::timeout_at(deadline.into(), database.await_installed(
                    |prefix| prefix.replica >= exact.cut.through, cancellation,
                )).await.map_err(|_| unretained())??;
            }
            let checkpoint = match exact.replay.as_ref().ok_or_else(unretained)? {
                SnapshotReplayBase::Genesis if exact.block == BlockIndex(0) => None,
                SnapshotReplayBase::Genesis => return Err(unretained()),
                SnapshotReplayBase::Checkpoint { block, boundary, root } => Some(rtdb_replication::PublishedCheckpoint {
                    block: *block, boundary: *boundary, root_sha256: Some(*root),
                }),
            };
            let recovery = self.node.recover_sql_snapshot(&database, exact.cut, checkpoint, cancellation);
            tokio::time::timeout_at(deadline.into(), recovery).await.map_err(|_| unretained())?
                .map_err(|error| {
                    if matches!(error.code.as_str(), "rtdb/erased-under-view" | "xtdb/operation-cancelled") { error }
                    else { unretained() }
                })?
        };
        if !exact.matches(&recovered.partition) || !exact.is_live(self.node.now()) { return Err(unretained()); }
        let snapshot = recovered.partition.with_system_time_cap(exact.cap).map_err(|_| unretained())?;
        let view = rtdb_query::SqlSnapshotView::from_exact_partition(
            current, snapshot, exact.clock, recovered.suffix, recovered.storage,
        )?;
        let view = with_expiry(view, deadline);
        self.node.admit_retained_catalog(&view.catalog, self.identity.as_ref(), cancellation).await?;
        Ok(view)
    }
}

#[async_trait::async_trait]
impl rtdb_query::SnapshotAdmission for SqlSnapshotAdmission {
    async fn reopen(&self, token: &str, current: &QueryContext) -> Result<rtdb_query::SqlSnapshotView> {
        self.restore(token, current, &current.cancellation).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_index::{PartitionState, TransactionBuffer};
    use rtdb_types::{StorageEpoch, StorageVersion, TxId};

    #[test]
    fn an_exact_resume_distinguishes_tied_appends_and_keeps_the_original_clock() {
        let memory = MemoryBudget::new(1 << 20);
        let partition = PartitionState::empty(rtdb_trie::TrieConfig::default()).unwrap();
        let head = TransactionKey { tx_id: TxId(5), system_time: Timestamp(22) };
        let cut = CatalogCut {
            storage_version: StorageVersion(2), storage_epoch: StorageEpoch(7),
            replica_epoch: StorageEpoch(1), through: ReplicaPosition(20),
        };
        let apply = |key| {
            let write = TransactionBuffer::new(partition.snapshot(), key, Arc::clone(&memory)).unwrap().stage().unwrap();
            partition.apply(&write).unwrap();
        };
        apply(head);
        partition.publish_catalog_cut(cut).unwrap();
        let snapshot = partition.snapshot();
        let clock = Instant::new(60, 123).unwrap();
        let exact = ExactSnapshot {
            owner: Some("alice".to_owned()), database: "xtdb".to_owned(), history: [3; 32], cut,
            head: Some(head), block: snapshot.block_index(), cap: Some(Timestamp(19)),
            clock, timezone: "UTC".to_owned(), issued: Timestamp(61_000_000), replay: Some(SnapshotReplayBase::Genesis),
        };
        let restored: ExactSnapshot = serde_json::from_slice(&serde_json::to_vec(&exact).unwrap()).unwrap();
        assert!(restored.matches(&snapshot));
        assert_eq!(restored.cap, Some(Timestamp(19)));
        assert_eq!(restored.clock, clock);
        assert!(restored.is_live(Timestamp(120_999_999)));
        assert!(!restored.is_live(Timestamp(121_000_000)));
        apply(TransactionKey { tx_id: TxId(6), system_time: head.system_time });
        assert!(!restored.matches(&partition.snapshot()));
    }
}
