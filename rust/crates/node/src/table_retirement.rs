use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_columnar::MemoryBudget;
use rtdb_index::PartitionState;
use rtdb_maintenance::TableRetirements;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions};
use rtdb_replication::TermHandle;
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Result, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;

/// Publishes retirement of expired owned relations after their last selectable
/// pre-retirement fork closes (gc.allium: RetireDueTables).
pub(crate) struct RetiredRelations {
    pub(crate) database: String,
    pub(crate) partition: Arc<PartitionState>,
    pub(crate) storage: Arc<BufferPool>,
    pub(crate) memory: Arc<MemoryBudget>,
    pub(crate) term: TermHandle,
}

#[async_trait]
impl TableRetirements for RetiredRelations {
    async fn due(&self, cancellation: &CancellationToken) -> Result<Option<(Vec<TableRef>, Timestamp)>> {
        let snapshot = self.partition.snapshot();
        let Some(present) = snapshot.tx_basis().map(|basis| basis.system_time) else { return Ok(None); };
        let catalogs: Vec<_> = snapshot.tables().into_iter().filter(TableRef::is_owned_retired_catalog).collect();
        if catalogs.is_empty() { return Ok(None); }
        let catalog = CatalogSnapshot::new(&self.database, BTreeMap::from([(
            self.database.clone(), DatabaseSnapshot { partitions: vec![snapshot.clone()], storage: Arc::clone(&self.storage) },
        )]), false)?;
        let context = QueryContext::new(catalog, Arc::clone(&self.memory), present, "UTC", QueryOptions::default(), cancellation)?;
        let mut due = BTreeSet::new();
        for retired in catalogs {
            let ready: Vec<_> = rtdb_query::read_owned_retirements(Arc::clone(&context), retired.clone()).await?
                .into_iter().filter(|(_, retirement)| retirement.reclaimable_at() <= present).collect();
            if ready.is_empty() { continue; }
            let worlds = match retired.owned_world_catalog() {
                Some(worlds) if snapshot.contains_table(&worlds) => rtdb_query::read_owned_worlds(Arc::clone(&context), worlds).await?,
                _ => Vec::new(),
            };
            for (relation, retirement) in ready {
                if worlds.iter().any(|world| {
                    world.schemas.relation(&relation).is_some()
                        && fork_retains_relation(world.dropped, world.until, world.basis.system_time, present, retirement.retired_at)
                })
                { continue; }
                let Some(tables) = relation.physical_tables(&retired) else { continue; };
                due.extend(tables.into_iter().filter(|table| {
                    snapshot.table_catalog().entry(table).is_some() && snapshot.trie_catalog().retired_as_of(table).is_none()
                }));
            }
        }
        Ok((!due.is_empty()).then(|| (due.into_iter().collect(), present)))
    }

    async fn retire(&self, tables: Vec<TableRef>, as_of: Timestamp) -> Result<()> {
        self.term.publish_retirements(tables, as_of).await
    }
}

fn fork_retains_relation(dropped: bool, until: Timestamp, basis: Timestamp, present: Timestamp, retired: Timestamp) -> bool {
    !dropped && present < until && basis < retired
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_selectable_earlier_fork_defers_owned_relation_retirement() {
        assert!(fork_retains_relation(false, Timestamp(20), Timestamp(8), Timestamp(19), Timestamp(9)));
        assert!(!fork_retains_relation(false, Timestamp(20), Timestamp(8), Timestamp(20), Timestamp(9)));
        assert!(!fork_retains_relation(true, Timestamp(20), Timestamp(8), Timestamp(19), Timestamp(9)));
        assert!(!fork_retains_relation(false, Timestamp(20), Timestamp(9), Timestamp(19), Timestamp(9)));
    }
}
