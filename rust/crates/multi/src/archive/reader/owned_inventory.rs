use std::{collections::BTreeMap, sync::Arc, time::Duration};

use rtdb_columnar::MemoryBudget;
use rtdb_directory::{Directory, HostedTask};
use rtdb_format::catalog::CheckpointContents;
use rtdb_node::{NodeOptions, ReadFence};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, read_owned_schema_catalog};
use rtdb_replication::{BlockDiscovery, BlockStore, PublishedCheckpoint};
use rtdb_storage::{ObjectStore, cache::{BufferPool, CacheBudget}};
use rtdb_types::{BlockIndex, ReplicaPosition, Result, SourcePosition, StorageEpoch, TableRef, Timestamp, TransactionKey};
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{StageReadAuthority, StageSelector, inconsistent, unavailable, with_stage};
use crate::{deployment::CellStorage, keys::KeyService};

const MAX_OWNED_RELATIONS: usize = 4096;

#[derive(Serialize)]
pub(crate) struct StageOwnedRelations {
    database: String,
    source_manifest_sha256: String,
    destination: Uuid,
    stage_operation: Uuid,
    staged_manifest_sha256: String,
    transaction: TransactionKey,
    owner_namespace: Option<String>,
    relations: Vec<String>,
    declarations: String,
}

pub(crate) async fn inspect_staged_owned_relations(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    keys: &dyn KeyService,
    selector: StageSelector,
    hosted_task: Option<HostedTask>,
) -> Result<StageOwnedRelations> {
    if selector.source.is_none() {
        return Err(inconsistent("Owned relation inventory requires the original stage source"));
    }
    with_stage(directory, storage, keys, selector, hosted_task, |authority, _, verified| async move {
        if verified.contents != CheckpointContents::Replay {
            return Err(inconsistent("Owned relation inventory requires a complete replay stage"));
        }
        let stop = CancellationToken::new();
        let result = tokio::time::timeout(Duration::from_secs(25), inspect(&authority, verified.store, &stop)).await;
        stop.cancel();
        result.map_err(|_| unavailable("The staged owned relation inventory timed out"))?
    }).await
}

async fn inspect(
    authority: &StageReadAuthority,
    store: Arc<dyn ObjectStore>,
    stop: &CancellationToken,
) -> Result<StageOwnedRelations> {
    let stage = &authority.stage;
    let database = &stage.intent.name;
    authority.confirm(database).await?;
    let options = NodeOptions::default();
    let memory = options.query.query_memory(&MemoryBudget::new(options.execution_memory_bytes));
    let storage = Arc::new(BufferPool::with_metadata_budget(
        store.clone(), CacheBudget::new(options.cache_bytes), memory.clone(),
    ));
    let blocks = BlockStore::new(store, storage.clone(), memory.clone(), options.file_size_target)?;
    let checkpoint = stage.checkpoint;
    let root_sha256 = checkpoint.root_sha256
        .ok_or_else(|| inconsistent("The staged checkpoint has no original root digest"))?;
    let epoch = StorageEpoch(stage.source_log_epoch);
    let published = PublishedCheckpoint {
        block: BlockIndex(checkpoint.block),
        boundary: ReplicaPosition::from_epoch_offset(epoch, checkpoint.boundary)?,
        root_sha256: Some(root_sha256),
    };
    let recovered = blocks.recover(
        options.trie,
        SourcePosition::from_epoch_offset(epoch, checkpoint.source)?,
        Timestamp(checkpoint.system_time),
        BlockDiscovery::AtPublished(published),
    ).await?;
    let snapshot = recovered.partition.snapshot();
    let transaction = recovered.progress.tx_key.filter(|cut| {
        cut.tx_id.0 >= 0 && cut.system_time.0 == checkpoint.system_time && snapshot.tx_basis() == Some(*cut)
    })
        .ok_or_else(|| inconsistent("The staged checkpoint has a different transaction key"))?;
    let _catalog_charge = memory.reserve(snapshot.schema_allocation_bound(database)?)?;
    let selected = select_namespace(snapshot.tables())?;
    let (owner_namespace, relations, declarations) = if let Some(selected) = selected {
        let snapshot = snapshot.with_system_time_cap(Some(transaction.system_time))?;
        let catalog = CatalogSnapshot::new(database.as_str(), BTreeMap::from([(
            database.clone(), DatabaseSnapshot { partitions: vec![snapshot], storage },
        )]), true)?;
        let context = QueryContext::new(catalog, memory, transaction.system_time, "UTC", options.query, stop)?;
        let (declarations, _charge) = read_owned_schema_catalog(context, selected.catalog).await?;
        let declarations = String::from_utf8(declarations.into_catalog_json())
            .map_err(|_| inconsistent("The staged relation declarations are malformed"))?;
        (Some(selected.owner), selected.relations, declarations)
    } else {
        (None, Vec::new(), "{}".to_owned())
    };
    authority.confirm(database).await?;
    Ok(StageOwnedRelations {
        database: database.clone(),
        source_manifest_sha256: stage.intent.source_manifest_sha256.clone(),
        destination: stage.intent.destination,
        stage_operation: stage.intent.operation,
        staged_manifest_sha256: stage.staged_manifest_sha256.clone()
            .ok_or_else(|| inconsistent("The staged checkpoint has no accepted inventory"))?,
        transaction,
        owner_namespace,
        relations,
        declarations,
    })
}

struct CapturedNamespace {
    owner: String,
    catalog: TableRef,
    relations: Vec<String>,
}

fn select_namespace(tables: impl IntoIterator<Item = TableRef>) -> Result<Option<CapturedNamespace>> {
    let mut owner: Option<String> = None;
    let mut catalog = None;
    let mut relations = Vec::new();
    for table in tables.into_iter().filter(|table| TableRef::owned_schema(&table.schema)) {
        let name = table.owned_storage_name()
            .ok_or_else(|| inconsistent("The stage contains an unsupported owned relation"))?;
        let (namespace, _) = table.table.split_once('-')
            .ok_or_else(|| inconsistent("The staged owned relation has no namespace"))?;
        if owner.as_deref().is_some_and(|previous| previous != namespace) {
            return Err(inconsistent("The stage contains multiple owned relation namespaces"));
        }
        if relations.len() == MAX_OWNED_RELATIONS {
            return Err(inconsistent("The staged owned relation inventory exceeds its bound"));
        }
        if owner.is_none() { owner = Some(namespace.to_owned()); }
        relations.push(name.to_owned());
        if table.is_owned_relation_catalog() { catalog = Some(table); }
    }
    let Some(owner) = owner else { return Ok(None); };
    let catalog = catalog.ok_or_else(|| inconsistent("The stage has owned relations without their declaration catalog"))?;
    relations.sort_unstable();
    if relations.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(inconsistent("The staged owned relation inventory contains a duplicate"));
    }
    Ok(Some(CapturedNamespace { owner, catalog, relations }))
}

#[cfg(test)]
mod tests {
    use rtdb_types::OwnedRelation;

    use super::*;

    #[test]
    fn stage_inventory_is_bound_to_one_captured_declaration_namespace() -> Result<()> {
        let facts = OwnedRelation::new("facts")?;
        let catalog = OwnedRelation::relation_catalog("source-owner");
        let data = facts.table("source-owner");
        let selected = select_namespace([data.clone(), catalog.clone()])?.unwrap();
        assert_eq!(selected.catalog, catalog);
        assert_eq!(selected.relations, [".relation-schemas", "facts"]);
        assert!(select_namespace([data]).is_err());
        assert!(select_namespace([catalog, facts.table("other-owner")]).is_err());
        assert!(select_namespace([TableRef::new("public", "ordinary")])?.is_none());
        assert!(select_namespace([TableRef::new("rtdb_owned_v1", "unsigned-facts")]).is_err());
        Ok(())
    }
}
