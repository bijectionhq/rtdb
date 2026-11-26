use std::{sync::{Arc, Mutex, Weak}, time::Duration};

use arrow_array::{Int64Array, RecordBatch, TimestampMicrosecondArray};
use rtdb_columnar::{MemoryBudget, MemoryReservation, selected_cell};
use rtdb_storage::cache::{BufferPool, CatalogAdmission};
use rtdb_tx::HostedControlWrite;
use rtdb_types::{Anomaly, AnomalyCategory, CatalogCut, EntityId, Interval, LogicalType, OwnedRelation, PlacedAttemptScope, Result, ScalarType, Timestamp, TransactionKey, Value};

use crate::Database;
use super::{AppliedCatalogAdmission, CatalogReadCut, DurableRange, NativeSession, OwnedRangeWitness, OwnedReadSession, QueryBasis, TimeBasis, covers_transaction, hosted_source_unavailable, prepare_query};
use super::transaction_evidence::exact_transaction_row;
use rtdb_query::QueryContext;

/// A lower bound on the physical catalogs read by a prepared session, with any custody its storage owner granted.
/// Only a hosted journal grants custody; other storage keeps the catalog readable for the GC window.
pub(super) struct PreparedAppliedCatalog {
    attachment: Weak<Database>,
    floor: CatalogCut,
    storage: Arc<BufferPool>,
    registration: Arc<Mutex<Option<Arc<CatalogAdmission>>>>,
    expires_at: tokio::time::Instant,
    expiry: tokio::task::JoinHandle<()>,
    _reservation: Arc<MemoryReservation>,
}

impl PreparedAppliedCatalog {
    pub(super) fn from_read(read: &OwnedReadSession, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(2048)?;
        let database = read.context.catalog.database(&read.database)?;
        let [snapshot] = database.partitions.as_slice() else { return Err(unavailable()); };
        let floor = snapshot.catalog_cut().ok_or_else(unavailable)?;
        let registration = snapshot.catalog_read_permit::<CatalogAdmission>()
            .filter(|admission| database.storage.admitted(admission, Some(floor)));
        let registration = Arc::new(Mutex::new(registration));
        let weak = Arc::downgrade(&registration);
        let expires_at = tokio::time::Instant::now() + Duration::from_secs(180);
        let expiry = tokio::spawn(async move {
            tokio::time::sleep_until(expires_at).await;
            if let Some(registration) = weak.upgrade() {
                let retired = registration.lock().expect("applied catalog lease").take();
                drop(retired);
            }
        });
        Ok(Self { attachment: read.attachment.clone(), floor, storage: Arc::clone(&database.storage), registration, expires_at, expiry, _reservation: reservation })
    }

    pub(super) fn storage(&self) -> &Arc<BufferPool> { &self.storage }

    pub(super) fn remaining_micros(&self) -> Result<u64> {
        self.check_live()?;
        let remaining = self.expires_at.saturating_duration_since(tokio::time::Instant::now()).as_micros();
        u64::try_from(remaining).ok().filter(|remaining| *remaining != 0).ok_or_else(unavailable)
    }

    pub(super) fn check_live(&self) -> Result<()> {
        if tokio::time::Instant::now() >= self.expires_at {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/applied-preparation-expired",
                "The prepared applied catalog lease expired"));
        }
        Ok(())
    }

    /// The custody granted for the prepared floor, which a later capture at or after it shares.
    pub(super) fn pin(&self, database: &Arc<Database>, cut: CatalogCut) -> Result<Option<Arc<CatalogAdmission>>> {
        self.check_live()?;
        if !self.attachment.upgrade().is_some_and(|original| Arc::ptr_eq(&original, database))
            || !covers_catalog(self.floor, cut)
        {
            return Err(unavailable());
        }
        Ok(self.registration.lock().expect("applied catalog lease").as_ref().cloned())
    }
}

impl Drop for PreparedAppliedCatalog {
    fn drop(&mut self) { self.expiry.abort(); }
}

fn covers_catalog(floor: CatalogCut, cut: CatalogCut) -> bool {
    cut.storage_version == floor.storage_version && cut.storage_epoch == floor.storage_epoch
        && cut.replica_epoch == floor.replica_epoch && cut.through >= floor.through
}

/// Exact original ordinary Root point condition. Absence is a condition.
pub struct AppliedOwnedPoint {
    pub relation: OwnedRelation,
    pub id: EntityId,
    pub valid_at: Timestamp,
    pub expected: Option<rtdb_types::OwnedPointVersion>,
}

pub struct AppliedReadFloor {
    history: uuid::Uuid,
    transactions: Vec<TransactionKey>,
    minimum: TransactionKey,
}

impl AppliedReadFloor {
    pub fn new(history: uuid::Uuid, mut transactions: Vec<TransactionKey>) -> Result<Self> {
        if history.is_nil() || !(1..=4).contains(&transactions.len())
            || transactions.iter().any(|key| key.tx_id.0 < 0)
        {
            return Err(Anomaly::incorrect("rtdb.owned/applied-floor", "Invalid applied read history or transaction floor"));
        }
        transactions.sort_unstable_by_key(|key| key.tx_id.0);
        transactions.dedup();
        let minimum = transactions.iter().copied().try_fold(transactions[0], confirmation_floor)?;
        Ok(Self { history, transactions, minimum })
    }

    pub fn history(&self) -> uuid::Uuid { self.history }
    pub(super) fn minimum(&self) -> TransactionKey { self.minimum }

    pub(super) fn cut<'a>(
        &self, original: Option<TransactionKey>, memory: &'a Arc<MemoryBudget>,
        admission: AppliedCatalogAdmission<'a>,
    ) -> Result<CatalogReadCut<'a>> {
        let minimum = match original {
            Some(original) => confirmation_floor(original, self.minimum)?,
            None => self.minimum,
        };
        Ok(CatalogReadCut::HostedApplied { minimum, history: self.history, memory, admission })
    }

    pub(super) async fn require_transactions(
        &self, context: &Arc<QueryContext>, head: TransactionKey, original: Option<TransactionKey>,
    ) -> Result<()> {
        let original = original.filter(|key| !self.transactions.contains(key));
        let mut needed = self.transactions.iter().copied().chain(original).filter(|expected| *expected != head).peekable();
        if needed.peek().is_none() { return Ok(()); }
        let options = rtdb_sql::PrepareOptions {
            parameter_types: [(0, LogicalType::Scalar(ScalarType::I64))].into(),
            default_timezone: context.timezone.clone(),
            generate_series_end_exclusive: context.options.generate_series_end_exclusive,
            ..Default::default()
        };
        let query = prepare_query("SELECT _id, system_time FROM xt.txs FOR ALL VALID_TIME WHERE _id = ?", context, &options)?;
        for expected in needed {
            let mut cursor = query.execute(Arc::clone(context), vec![Value::I64(expected.tx_id.0)]).await?;
            let mut seen = false;
            while let Some(batch) = cursor.next_batch().await? {
                let batch = batch.batch();
                if batch.num_rows() == 0 { continue; }
                if !matches_transaction_result(batch, seen, expected)? {
                    return Err(unavailable());
                }
                seen = true;
            }
            if !seen { return Err(unavailable()); }
        }
        Ok(())
    }
}

pub(super) fn matches_transaction_result(batch: &RecordBatch, seen: bool, expected: TransactionKey) -> Result<bool> {
    let rows = batch.num_rows();
    let (position, revision) = if rows == 1 && batch.num_columns() >= 2 {
        let position = selected_cell(batch.schema_ref().field(0), batch.column(0).as_ref(), 0)?
            .and_then(|(_, column, row)| column.as_any().downcast_ref::<Int64Array>()
                .map(|column| column.value(row)));
        let revision = selected_cell(batch.schema_ref().field(1), batch.column(1).as_ref(), 0)?
            .and_then(|(_, column, row)| column.as_any().downcast_ref::<TimestampMicrosecondArray>()
                .map(|column| column.value(row)));
        (position, revision)
    } else {
        (None, None)
    };
    Ok(exact_transaction_row(rows, seen, position, revision, expected.tx_id.0, expected.system_time.0))
}

impl NativeSession {
    pub async fn confirm_hosted_source_prefix(
        &self,
        history: uuid::Uuid,
        floor: Option<&AppliedReadFloor>,
    ) -> Result<Option<TransactionKey>> {
        if history.is_nil() || floor.is_some_and(|floor| floor.history() != history)
            || self.service.audience_storage.is_some() || self.placed_history()?.is_none()
        {
            return Err(hosted_source_unavailable());
        }
        let database = self.state.database();
        let cancellation = self.state.cancellation();
        let receipt = self.service.await_durable_source(database, true, cancellation).await?
            .ok_or_else(hosted_source_unavailable)?;
        if uuid::Uuid::parse_str(&receipt.history).ok() != Some(history) {
            return Err(hosted_source_unavailable());
        }
        let Some(sampled) = receipt.applied else {
            return if floor.is_none() { Ok(None) } else { Err(hosted_source_unavailable()) };
        };
        if floor.is_some_and(|floor| !covers_transaction(sampled, floor.minimum())) {
            return Err(hosted_source_unavailable());
        }
        let basis = QueryBasis {
            snapshot: TimeBasis::default(),
            replica_capture: None,
            resume_token: None,
            has_await_floor: false,
            snapshot_time: Some(sampled.system_time),
            current_time: sampled.system_time.into(),
            timezone: self.state.session_timezone().to_owned(),
        };
        let context = self.service.query_context(database, &basis, cancellation).await?;
        let captured = context.catalog.database(database)?;
        let [partition] = captured.partitions.as_slice() else {
            return Err(hosted_source_unavailable());
        };
        let head = partition.tx_basis().filter(|head| covers_transaction(*head, sampled))
            .ok_or_else(hosted_source_unavailable)?;
        let sampled_floor = AppliedReadFloor::new(history, vec![sampled])?;
        floor.unwrap_or(&sampled_floor).require_transactions(&context, head, Some(sampled)).await?;
        context.check_cancelled()?;
        self.service.database()?;
        Ok(Some(sampled))
    }
}

/// The held read's covered position and its own receiver observation.
pub struct AppliedReadConfirmation {
    pub covered: TransactionKey,
    pub observed_at: Timestamp,
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.hosted/observation-unavailable",
        "The original hosted dependency scope is unavailable",
    )
}

fn confirmation_floor(original: TransactionKey, acknowledged: TransactionKey) -> Result<TransactionKey> {
    if covers_transaction(acknowledged, original) {
        Ok(acknowledged)
    } else if covers_transaction(original, acknowledged) {
        Ok(original)
    } else {
        Err(unavailable())
    }
}

async fn compare_controls(
    original: &Arc<QueryContext>,
    current: &Arc<QueryContext>,
    controls: &[HostedControlWrite],
) -> Result<bool> {
    for control in controls {
        let observed = rtdb_query::read_hosted_control_point(
            Arc::clone(original), control.table(), control.id(), false,
        ).await?;
        if observed.row.map(|(_, revision)| revision) != control.expected() {
            return Err(unavailable());
        }
        let found = rtdb_query::read_hosted_control_point(
            Arc::clone(current), control.table(), control.id(), false,
        ).await?;
        if found.row.map(|(_, revision)| revision) != control.expected() {
            return Ok(false);
        }
    }
    Ok(true)
}

impl NativeSession {
    /// Validate an ordinary answer at a new, certified and protected journal cut.
    pub async fn validate_durable_hosted_read(
        &mut self,
        scope: PlacedAttemptScope,
        floor: &AppliedReadFloor,
        until: Timestamp,
        controls: &[HostedControlWrite],
        points: &[AppliedOwnedPoint],
        ranges: &[DurableRange],
    ) -> Result<Option<AppliedReadConfirmation>> {
        if self.placed_history()? != Some(scope) || controls.is_empty()
            || controls.iter().any(|control| !control.is_check() || control.scope() != scope)
            || points.iter().any(|point| point.relation.world().is_some() || point.relation.is_world_catalog())
        {
            return Err(unavailable());
        }
        let checks = ranges.iter().map(DurableRange::evidence);
        let until = checks.clone().fold(until, |until, range| until.min(range.until));
        if checks.clone().any(|range| range.scope != scope || !covers_transaction(floor.minimum(), range.basis)) {
            return Err(unavailable());
        }
        let read = self.open_fresh_owned_read(floor.history(), Some(floor)).await?;
        let observed = read.query_time()?;
        if observed >= until || read.present.system_time >= until {
            return Err(unavailable());
        }
        let guard = read.relation_release_guard_for(
            checks.clone().map(|range| &range.relation).chain(points.iter().map(|point| &point.relation)),
            true,
        )?;
        guard()?;
        let database = read.context.catalog.database(&read.database)?;
        let [captured] = database.partitions.as_slice() else { return Err(unavailable()); };
        for range in checks {
            let frontier = captured.relation_event_frontier(&range.relation.table(&read.owner))?;
            if frontier == range.frontier { continue; }
            let unchanged = match range.region.as_deref() {
                Some(rtdb_types::OwnedRangeEvidence::FixedPath(evidence)) =>
                    rtdb_query::verify_owned_fixed_range(
                        &read.context, captured, &read.owner, &range.relation,
                        range.basis, range.frontier, frontier, evidence,
                    ).await?,
                Some(rtdb_types::OwnedRangeEvidence::Entity { anchor, region }) =>
                    rtdb_query::verify_owned_entity_region(
                        &read.context, captured, &read.owner, &range.relation,
                        range.basis, range.frontier, frontier, *anchor, region,
                    ).await?,
                None => false,
            };
            if !unchanged { return Ok(None); }
        }
        for point in points {
            let at = if captured.contains_table(&point.relation.lifetime_table(&read.owner)) {
                if point.valid_at > observed { return Err(unavailable()); }
                observed
            } else {
                point.valid_at
            };
            let revision = rtdb_query::read_owned_point_version(
                Arc::clone(&read.context), &read.owner, &point.relation, &point.id, Interval::at(at)?,
            ).await?;
            if revision != point.expected { return Ok(None); }
        }
        for control in controls {
            let point = rtdb_query::read_hosted_control_point(
                Arc::clone(&read.context), control.table(), control.id(), false,
            ).await?;
            if point.row.map(|(_, revision)| revision) != control.expected() { return Ok(None); }
        }
        guard()?;
        read.check()?;
        let clock = Arc::clone(&read.node.factory.clock);
        let sampled = clock.monotonic_micros();
        let now = read.node.receiver_time().await?;
        if now >= until {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/expired", "The hosted validation deadline has passed"));
        }
        let deadline = sampled.saturating_add(until.0.saturating_sub(now.0));
        let cutoff = Arc::new(rtdb_storage::ReadLease::new(Some(deadline), Arc::new(move || clock.monotonic_micros())));
        read.context.retain_delivery_cut(cutoff).await?;
        guard()?;
        Ok(Some(AppliedReadConfirmation { covered: read.present, observed_at: observed }))
    }
}

impl OwnedReadSession {
    /// Compare cumulative journal events from a live held Root against a single
    /// current placed snapshot. The returned time is coverage, not a new hold.
    pub async fn observe_hosted_dependencies(
        &self,
        scope: PlacedAttemptScope,
        until: Timestamp,
        witnesses: &[OwnedRangeWitness],
        controls: &[HostedControlWrite],
    ) -> Result<Option<Timestamp>> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        let until = witnesses
            .iter()
            .fold(until.min(self.witness_until), |bound, witness| {
                bound.min(witness.until)
            });
        if self.historical.is_some()
            || self.witness_scope != Some(scope)
            || self.node.now() >= until
            || witnesses.is_empty()
            || controls.is_empty()
            || controls
                .iter()
                .any(|control| !control.is_check() || control.scope() != scope)
        {
            return Err(unavailable());
        }
        let original_database = self.context.catalog.database(&self.database)?;
        let [original] = original_database.partitions.as_slice() else {
            return Err(unavailable());
        };
        if original.tx_basis() != Some(self.present) {
            return Err(unavailable());
        }
        for witness in witnesses {
            if witness.scope != scope
                || witness.basis != self.present
                || witness.region.is_some()
                || witness.relation.world().is_some()
                || witness.relation.is_world_catalog()
            {
                return Err(unavailable());
            }
        }
        let purge_guard =
            self.relation_release_guard(witnesses.iter().map(|witness| &witness.relation))?;
        purge_guard()?;
        let database = self
            .node
            .attached_database(&self.database, Some(&self.attachment))?;
        if database.await_authority.codec().map(|codec| codec.scope()) != Some(scope) {
            return Err(unavailable());
        }
        let basis = QueryBasis {
            snapshot: TimeBasis::default(),
            replica_capture: None,
            resume_token: None,
            has_await_floor: false,
            snapshot_time: None,
            current_time: self.node.query_instant(),
            timezone: self.context.timezone.clone(),
        };
        let context = self
            .node
            .query_context_in_placement(
                &self.database,
                &basis,
                self.identity.as_ref(),
                Some(&self.attachment),
                CatalogReadCut::Current,
                &self.cancellation,
            )
            .await?;
        let current_database = context.catalog.database(&self.database)?;
        let [current] = current_database.partitions.as_slice() else {
            return Err(unavailable());
        };
        let head = current
            .tx_basis()
            .filter(|head| {
                head.tx_id >= self.present.tx_id
                    && head.system_time >= self.present.system_time
                    && ((head.tx_id == self.present.tx_id)
                        == (head.system_time == self.present.system_time))
            })
            .ok_or_else(unavailable)?;
        for witness in witnesses {
            if current.relation_event_frontier(&witness.relation.table(&self.owner))?
                != witness.frontier
            {
                return Ok(None);
            }
        }
        if !compare_controls(&self.context, &context, controls).await? {
            return Ok(None);
        }
        purge_guard()?;
        if self.node.now() >= until || head.system_time >= until {
            return Err(unavailable());
        }
        Ok(Some(head.system_time))
    }

    /// Validate a held read's original checks on one newly captured
    /// applied catalog of the same recovered hosted attachment.
    pub async fn confirm_applied_hosted_read(
        &self,
        scope: PlacedAttemptScope,
        floor: &AppliedReadFloor,
        until: Timestamp,
        controls: &[HostedControlWrite],
        point: Option<&AppliedOwnedPoint>,
    ) -> Result<Option<AppliedReadConfirmation>> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        if self.applied_scope != Some(scope)
            || self.node.now() >= until
            || controls.is_empty()
            || controls.iter().any(|control| !control.is_check() || control.scope() != scope)
        {
            return Err(unavailable());
        }
        let original = self.context.catalog.database(&self.database)?;
        let [original] = original.partitions.as_slice() else {
            return Err(unavailable());
        };
        if original.tx_basis() != Some(self.present) {
            return Err(unavailable());
        }
        let purge_guard = point.map(|point| {
            let audience_marker = self.applied_preparation.is_some()
                && point.relation.name() == rtdb_types::OWNED_ROOT_PUBLICATION_RELATION
                && matches!(&point.id, rtdb_types::EntityId::String(id) if id
                    .strip_prefix("fact_partition_head:")
                    .and_then(|relation| rtdb_types::OwnedRelation::new(relation).ok())
                    .is_some_and(|relation| relation.is_root_data()));
            if !(point.relation.is_root_data() || audience_marker) || point.valid_at != self.present.system_time {
                return Err(unavailable());
            }
            self.relation_release_guard([&point.relation])
        }).transpose()?;
        if let Some(guard) = &purge_guard {
            guard()?;
        }
        let attachment = self.node.attached_database(&self.database, Some(&self.attachment))?;
        if attachment.await_authority.codec().map(|codec| codec.scope()) != Some(scope) {
            return Err(unavailable());
        }
        let basis = QueryBasis {
            snapshot: TimeBasis::default(),
            replica_capture: None,
            resume_token: None,
            has_await_floor: false,
            snapshot_time: None,
            current_time: self.node.query_instant(),
            timezone: self.context.timezone.clone(),
        };
        let admission = self.applied_preparation.as_deref().map_or(
            AppliedCatalogAdmission::Ordinary, AppliedCatalogAdmission::PreparedAudience,
        );
        let current = self.node.query_context_in_placement(
            &self.database, &basis, self.identity.as_ref(), Some(&self.attachment),
            floor.cut(Some(self.present), &self.context.memory, admission)?, &self.cancellation,
        ).await?;
        let database = current.catalog.database(&self.database)?;
        let [captured] = database.partitions.as_slice() else {
            return Err(unavailable());
        };
        let head = captured.tx_basis().ok_or_else(unavailable)?;
        floor.require_transactions(&current, head, Some(self.present)).await?;
        let mut matches = compare_controls(&self.context, &current, controls).await?;
        if let Some(point) = point {
            let valid = Interval::at(point.valid_at)?;
            let observed = rtdb_query::read_owned_point_version(
                Arc::clone(&self.context), &self.owner, &point.relation, &point.id, valid,
            ).await?;
            if observed != point.expected {
                return Err(unavailable());
            }
            if matches {
                let observed = rtdb_query::read_owned_point_version(
                    Arc::clone(&current), &self.owner, &point.relation, &point.id, valid,
                ).await?;
                matches = observed == point.expected;
            }
        }
        if let Some(guard) = &purge_guard {
            guard()?;
        }
        self.check()?;
        current.check_cancelled()?;
        let observed_at = self.node.receiver_time().await?;
        if observed_at >= until {
            return Err(unavailable());
        }
        Ok(matches.then_some(AppliedReadConfirmation { covered: head, observed_at }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::TxId;

    fn cut(offset: i64, time: i64) -> TransactionKey {
        TransactionKey { tx_id: TxId(offset), system_time: Timestamp(time) }
    }

    #[test]
    fn a_preparation_covers_only_forward_catalogs_in_its_residence() {
        use rtdb_types::{ReplicaPosition, StorageEpoch, StorageVersion};

        let floor = CatalogCut {
            storage_version: StorageVersion(1), storage_epoch: StorageEpoch(2),
            replica_epoch: StorageEpoch(3), through: ReplicaPosition(10),
        };
        assert!(covers_catalog(floor, floor));
        assert!(covers_catalog(floor, CatalogCut { through: ReplicaPosition(11), ..floor }));
        for other in [
            CatalogCut { through: ReplicaPosition(9), ..floor },
            CatalogCut { storage_version: StorageVersion(2), ..floor },
            CatalogCut { storage_epoch: StorageEpoch(4), ..floor },
            CatalogCut { replica_epoch: StorageEpoch(4), ..floor },
        ] {
            assert!(!covers_catalog(floor, other));
        }
    }

    #[test]
    fn confirmation_requires_both_floors_in_the_same_order() {
        let original = cut(3, 5);
        let later = cut(4, 8);
        assert_eq!(confirmation_floor(original, original).unwrap(), original);
        assert_eq!(confirmation_floor(original, later).unwrap(), later);
        assert_eq!(confirmation_floor(later, original).unwrap(), later);
        let tied = cut(4, 5);
        assert_eq!(confirmation_floor(original, tied).unwrap(), tied);
        for divergent in [cut(3, 8), cut(4, 2)] {
            assert!(confirmation_floor(original, divergent).is_err());
        }
    }
}
