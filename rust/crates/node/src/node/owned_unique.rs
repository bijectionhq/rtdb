use std::sync::Arc;

use rtdb_query::OwnedUniqueHit;
use rtdb_types::{Anomaly, AnomalyCategory, OwnedRelation, Result, Timestamp};

use super::{OwnedRangeWitness, OwnedReadSession};

impl OwnedReadSession {
    /// Resolve one complete unique key or authoritative absence.
    pub async fn unique(
        &self,
        relation: &OwnedRelation,
        path: &OwnedRelation,
        key: &[u8],
        valid: Timestamp,
    ) -> Result<Option<OwnedUniqueHit>> {
        self.admit_read().await?;
        if relation.world().is_some()
            || relation.is_world_catalog()
            || valid != self.present.system_time
        {
            return Err(Anomaly::incorrect(
                "rtdb.owned/unique-scope",
                "Unique lookup requires an owned root and the held present valid instant",
            ));
        }
        let unavailable = || {
            Anomaly::new(
                AnomalyCategory::Unavailable,
                "rtdb.owned/unique-uncovered",
                "The held unique path cannot prove its complete occupancy",
            )
        };
        let database = self.context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else {
            return Err(unavailable());
        };
        if snapshot.tx_basis() != Some(self.present) {
            return Err(unavailable());
        }
        let frontier = snapshot.relation_event_frontier(&relation.table(&self.owner))?;
        let context = self.relation_context(relation)?;
        let policy =
            rtdb_query::read_owned_unique_policy(Arc::clone(&context), &self.owner, relation).await?;
        if policy.through_events != frontier {
            return Err(unavailable());
        }
        let column = policy.paths.get(path).ok_or_else(unavailable)?;
        let hit = rtdb_query::lookup_owned_unique(
            Arc::clone(&context),
            &self.owner,
            relation,
            path,
            column,
            key,
            valid,
        )
        .await?;
        context.check_cancelled()?;
        Ok(hit)
    }

    /// Bind a completed lookup to this original held commit witness.
    pub async fn unique_observed(
        &self,
        relation: &OwnedRelation,
        path: &OwnedRelation,
        key: &[u8],
        valid: Timestamp,
    ) -> Result<(Option<OwnedUniqueHit>, OwnedRangeWitness)> {
        let hit = self.unique(relation, path, key, valid).await?;
        let witness = self.relation_witness(relation)?.ok_or_else(|| {
            Anomaly::new(
                AnomalyCategory::Unavailable,
                "rtdb.owned/unique-uncovered",
                "The held unique path has no original ordered witness",
            )
        })?;
        Ok((hit, witness))
    }
}
