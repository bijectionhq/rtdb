//! Account for one published checkpoint per definition in the captured catalog.

use std::{collections::BTreeMap, sync::Arc};

use rtdb_plan::CatalogRows;
use rtdb_query::{OrderedIndexInventory, OrderedIndexStorage, QueryContext};
use rtdb_types::{OwnedRelation, Result, TableRef};

use super::{
    DefinitionSet, checkpoint, definition_allocation_overflow, definitions, format_error,
    maintenance::source_frontier, tree::Tree,
};

/// Totals only requested Root tables from the held owner's protected path catalog.
pub(crate) async fn owned_file_bytes(
    context: &Arc<QueryContext>,
    database_name: &str,
    owner: &str,
    totals: &mut BTreeMap<TableRef, i64>,
) -> Result<()> {
    let database = context.catalog.database(database_name)?;
    let [snapshot] = database.partitions.as_slice() else {
        return Err(format_error("Owned inventory requires exactly one held partition"));
    };
    let catalog = OwnedRelation::path_catalog(owner);
    let Some(cursor) = rtdb_query::open_owned_path_catalog(Arc::clone(context), &catalog, None).await? else {
        return Ok(());
    };
    let mut definitions = DefinitionSet::new(&context.memory)?;
    super::read_definitions(cursor, &mut definitions).await?;
    for definition in definitions.iter() {
        if !catalog.owns_path_target(&definition.table) {
            return Err(format_error("Owned path definition refers to another owner or an internal catalog"));
        }
        let Some(bytes) = totals.get_mut(&definition.table) else { continue; };
        context.check_cancelled()?;
        let through = source_frontier(snapshot, &definition.table);
        if let Some((_, size)) = checkpoint_file_bytes(context, &database.storage, definition, through).await? {
            *bytes = bytes.checked_add(size).ok_or_else(|| format_error("Ordered index file bytes overflow"))?;
        }
    }
    Ok(())
}

async fn checkpoint_file_bytes(
    context: &QueryContext,
    storage: &Arc<rtdb_storage::cache::BufferPool>,
    definition: &super::Definition,
    through: i64,
) -> Result<Option<(i64, i64)>> {
    let directory = definition.directory(storage)?;
    let Some(manifest) = checkpoint::latest(storage, &directory, &context.memory, through).await? else {
        return Ok(None);
    };
    let tree = Tree::new(storage, &directory, &context.memory, manifest.scope).await?;
    let bytes = manifest.file_bytes(&tree, &context.cancellation).await?;
    Ok(Some((manifest.through, i64::try_from(bytes).map_err(format_error)?)))
}

pub(crate) struct Inventory;

#[async_trait::async_trait]
impl OrderedIndexInventory for Inventory {
    async fn sample(
        &self,
        name: &str,
        context: &QueryContext,
    ) -> Result<CatalogRows<OrderedIndexStorage>> {
        let database = context.catalog.database(name)?;
        let mut allocations = Vec::new();
        let mut indexes = BTreeMap::new();
        for snapshot in &database.partitions {
            context.check_cancelled()?;
            let now = snapshot
                .tx_basis()
                .map_or(context.current_time, |key| key.system_time.into());
            let Some(definitions) = definitions(
                name,
                snapshot,
                &database.storage,
                &context.memory,
                now,
                &context.cancellation,
            )
            .await? else {
                continue;
            };
            for definition in definitions.iter() {
                let bytes = definition.payload_bytes_for(&definition.table)?
                    .checked_add(1024).ok_or_else(definition_allocation_overflow)?;
                allocations.push(context.memory.reserve(bytes)?);
                let through = source_frontier(snapshot, &definition.table);
                let current = indexes.entry(definition.clone()).or_insert(through);
                *current = (*current).max(through);
            }
        }

        let mut rows = Vec::new();
        for (definition, through) in indexes {
            context.check_cancelled()?;
            let Some((through_block, bytes)) = checkpoint_file_bytes(context, &database.storage, &definition, through).await? else {
                continue;
            };
            rows.push(OrderedIndexStorage {
                table: definition.table,
                name: definition.name,
                version: definition.version,
                through_block,
                bytes,
            });
        }
        Ok(CatalogRows::new(rows, Arc::new(allocations)))
    }
}
