use std::sync::Arc;

use rtdb_tx::HostedControlWrite;
use rtdb_types::{Anomaly, AnomalyCategory, PlacedAttemptScope, Result, Timestamp};

use super::{OwnedRangeWitness, OwnedReadSession, QueryBasis, TimeBasis};

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
        let unavailable = || {
            Anomaly::new(
                AnomalyCategory::Unavailable,
                "rtdb.hosted/observation-unavailable",
                "The original hosted dependency scope is unavailable",
            )
        };
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
                None,
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
        for control in controls {
            let observed = rtdb_query::read_hosted_control_point(
                Arc::clone(&self.context),
                control.table(),
                control.id(),
                false,
            )
            .await?;
            if observed.row.map(|(_, revision)| revision) != control.expected() {
                return Err(unavailable());
            }
            let found = rtdb_query::read_hosted_control_point(
                Arc::clone(&context),
                control.table(),
                control.id(),
                false,
            )
            .await?;
            if found.row.map(|(_, revision)| revision) != control.expected() {
                return Ok(None);
            }
        }
        purge_guard()?;
        if self.node.now() >= until || head.system_time >= until {
            return Err(unavailable());
        }
        Ok(Some(head.system_time))
    }
}
