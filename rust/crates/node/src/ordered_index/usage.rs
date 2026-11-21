//! Account for one published checkpoint per definition in the captured catalog.

use std::{collections::BTreeMap, sync::Arc};

use rtdb_plan::CatalogRows;
use rtdb_query::{OrderedIndexInventory, OrderedIndexStorage, QueryContext};
use rtdb_types::Result;

use super::{checkpoint, definitions, format_error, maintenance::source_frontier, tree::Tree};

pub(crate) struct Inventory;

#[async_trait::async_trait]
impl OrderedIndexInventory for Inventory {
    async fn sample(
        &self,
        name: &str,
        context: &QueryContext,
    ) -> Result<CatalogRows<OrderedIndexStorage>> {
        let database = context.catalog.database(name)?;
        let mut indexes = BTreeMap::new();
        let mut allocations = Vec::new();
        for snapshot in &database.partitions {
            context.check_cancelled()?;
            let now = snapshot
                .tx_basis()
                .map_or(context.current_time, |key| key.system_time.into());
            for definition in definitions(
                name,
                snapshot,
                &database.storage,
                &context.memory,
                now,
                &context.cancellation,
            )
            .await?
            {
                let bytes = [
                    &definition.name,
                    &definition.table.schema,
                    &definition.table.table,
                ]
                .into_iter()
                .chain(&definition.columns)
                .fold(1024usize, |bytes, name| {
                    bytes.saturating_add(name.len().saturating_mul(2))
                });
                allocations.push(context.memory.reserve(bytes)?);
                let through = source_frontier(snapshot, &definition.table);
                let current = indexes.entry(definition).or_insert(through);
                *current = (*current).max(through);
            }
        }

        let mut rows = Vec::new();
        for (definition, through) in indexes {
            context.check_cancelled()?;
            let directory = definition.directory()?;
            let Some(manifest) =
                checkpoint::latest(&database.storage, &directory, &context.memory, through).await?
            else {
                continue;
            };
            let tree = Tree::new(
                &database.storage,
                &directory,
                &context.memory,
                manifest.scope,
            )
            .await?;
            let bytes = manifest.file_bytes(&tree, &context.cancellation).await?;
            rows.push(OrderedIndexStorage {
                table: definition.table,
                name: definition.name,
                version: definition.version,
                through_block: manifest.through,
                bytes: i64::try_from(bytes).map_err(format_error)?,
            });
        }
        Ok(CatalogRows::new(rows, Arc::new(allocations)))
    }
}
