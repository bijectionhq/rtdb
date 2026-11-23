use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_tx::HostedControlWrite;
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, Interval, LogicalType, OwnedRelation, PlacedAttemptScope, Result, ScalarType, TimeUnit, Timestamp, TransactionKey, Value};

use super::{CatalogReadCut, OwnedRangeWitness, OwnedReadSession, QueryBasis, TimeBasis, covers_transaction, prepare_query, read_value};
use rtdb_query::QueryContext;

/// Exact original ordinary Root point condition. Absence is a condition.
pub struct AppliedOwnedPoint {
    pub relation: OwnedRelation,
    pub id: EntityId,
    pub valid_at: Timestamp,
    pub expected: Option<Timestamp>,
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

    pub(super) fn cut<'a>(&self, original: Option<TransactionKey>, memory: &'a Arc<MemoryBudget>) -> Result<CatalogReadCut<'a>> {
        let minimum = match original {
            Some(original) => confirmation_floor(original, self.minimum)?,
            None => self.minimum,
        };
        Ok(CatalogReadCut::HostedApplied { minimum, history: self.history, memory })
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
        let query = prepare_query("SELECT system_time FROM xt.txs FOR ALL VALID_TIME WHERE _id = ?", context, &options)?;
        for expected in needed {
            let mut cursor = query.execute(Arc::clone(context), vec![Value::I64(expected.tx_id.0)]).await?;
            let mut seen = false;
            while let Some(batch) = cursor.next_batch().await? {
                let batch = batch.batch();
                if batch.num_rows() == 0 { continue; }
                if seen || batch.num_rows() != 1 { return Err(unavailable()); }
                let value = read_value(batch.schema_ref().field(0), batch.column(0).as_ref(), 0)?;
                if !matches!(value, Value::Timestamp { value, unit: TimeUnit::Microsecond, .. } if value == expected.system_time.0) {
                    return Err(unavailable());
                }
                seen = true;
            }
            if !seen { return Err(unavailable()); }
        }
        Ok(())
    }
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
                || witness.fixed.is_some()
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
    ) -> Result<Option<TransactionKey>> {
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
            if !point.relation.is_root_data() || point.valid_at != self.present.system_time {
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
            snapshot_time: None,
            current_time: self.node.query_instant(),
            timezone: self.context.timezone.clone(),
        };
        let current = self.node.query_context_in_placement(
            &self.database, &basis, self.identity.as_ref(), Some(&self.attachment),
            floor.cut(Some(self.present), &self.context.memory)?, &self.cancellation,
        ).await?;
        let database = current.catalog.database(&self.database)?;
        let [captured] = database.partitions.as_slice() else {
            return Err(unavailable());
        };
        let head = captured.tx_basis().filter(|head| head.system_time < until)
            .ok_or_else(unavailable)?;
        floor.require_transactions(&current, head, Some(self.present)).await?;
        let mut matches = compare_controls(&self.context, &current, controls).await?;
        if let Some(point) = point {
            let valid = Interval::at(point.valid_at)?;
            let observed = rtdb_query::read_owned_point_revision(
                Arc::clone(&self.context), &self.owner, &point.relation, &point.id, valid,
            ).await?;
            if observed != point.expected {
                return Err(unavailable());
            }
            if matches {
                let observed = rtdb_query::read_owned_point_revision(
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
        if self.node.now() >= until {
            return Err(unavailable());
        }
        Ok(matches.then_some(head))
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
    fn confirmation_requires_both_floors_in_the_same_order() {
        let original = cut(3, 5);
        let later = cut(4, 8);
        assert_eq!(confirmation_floor(original, original).unwrap(), original);
        assert_eq!(confirmation_floor(original, later).unwrap(), later);
        assert_eq!(confirmation_floor(later, original).unwrap(), later);
        for divergent in [cut(3, 8), cut(4, 5), cut(4, 2)] {
            assert!(confirmation_floor(original, divergent).is_err());
        }
    }
}
