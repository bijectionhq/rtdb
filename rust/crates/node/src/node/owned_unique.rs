use std::sync::Arc;

use rtdb_query::OwnedUniqueHit;
use rtdb_types::{Anomaly, AnomalyCategory, Interval, OwnedRelation, Result, Timestamp};

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
        if relation.world().is_some() || relation.is_world_catalog() {
            return Err(Anomaly::incorrect(
                "rtdb.owned/unique-scope",
                "Unique lookup requires an owned Root relation",
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

    pub async fn unique_window(
        &self,
        relation: &OwnedRelation,
        path: &OwnedRelation,
        key: Option<&[u8]>,
        valid: Interval,
        system: Timestamp,
        mask: Option<(Timestamp, u32)>,
        limit: usize,
    ) -> Result<(Vec<rtdb_columnar::OwnedBatch>, String, Option<OwnedRangeWitness>)> {
        let unavailable = || Anomaly::new(
            AnomalyCategory::Unavailable,
            "rtdb.owned/unique-window",
            "The held unique window is unavailable",
        );
        if !relation.is_root_data() || !(1..=1024).contains(&limit) {
            return Err(unavailable());
        }
        let at_head = self.admit_root_system(relation, system).await?;
        let retained = mask.map_or(system, |(commit, _)| system.min(commit));
        if let Some((commit, _)) = mask {
            if commit > self.present.system_time { return Err(unavailable()); }
            self.certify_historical_cut(relation, retained).await?;
        }
        let guard = self.relation_release_guard([relation])?;
        let context = self.relation_context(relation)?.with_owned_read_guard(Arc::clone(&guard));
        let schema = rtdb_query::read_owned_relation_schema(Arc::clone(&context), &self.owner, relation)
            .await?;
        let Some((schema, _charge)) = schema else {
            return Err(unavailable());
        };
        if schema.has_opaque_columns() || schema.lifetime_micros().is_some() {
            return Err(unavailable());
        }
        let policy = rtdb_query::read_owned_unique_policy(Arc::clone(&context), &self.owner, relation).await?;
        let column = policy.paths.get(path).ok_or_else(unavailable)?;
        let mut pages = Vec::new();
        let mut len = 0usize;
        let mut remaining = 8usize << 20;
        if let Some(mut cursor) = rtdb_query::open_owned_unique_window(
            Arc::clone(&context), &self.owner, relation, column, key, valid, system, mask, limit + 1,
        ).await? {
            while let Some(page) = cursor.next_batch().await? {
                let batch = page.batch();
                if batch.num_rows() == 0 { continue; }
                len += batch.num_rows();
                remaining = remaining.checked_sub(batch.get_array_memory_size()).ok_or_else(unavailable)?;
                if len > limit {
                    return Err(Anomaly::new(
                        AnomalyCategory::Unavailable,
                        "rtdb.owned/unique-window-bound",
                        "The complete held unique window exceeds its slice limit",
                    ));
                }
                let (index, field) = batch.schema_ref().column_with_name("_id").ok_or_else(unavailable)?;
                let values = batch.column(index).as_ref();
                for row in 0..batch.num_rows() {
                    let size = rtdb_columnar::estimate_read_value_bytes(field, values, row)?;
                    let _charge = context.memory.reserve(size.saturating_add(4096))?;
                    let id = rtdb_columnar::read_execution_value(field, values, row)?.owned_entity_id()?;
                    rtdb_query::owned_row_revision(batch, row, &id, &context.memory)?;
                    self.check_point_purge_floor(relation, &id, retained).await?;
                }
                pages.push(page);
            }
        }
        guard()?;
        let witness = if at_head && mask.is_none() { self.relation_witness(relation)? } else { None };
        Ok((pages, column.clone(), witness))
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
