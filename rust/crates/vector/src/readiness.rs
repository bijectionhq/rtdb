use std::sync::Arc;

use rtdb_index::{TrieState, branches::{RankedIndexDeclaration, RankedIndexKind}};
use rtdb_query::QueryContext;
use rtdb_storage::{ObjectKey, cache::CacheLease};
use rtdb_types::{Anomaly, Result, TableRef, TemporalBounds};

use crate::{definition::{self, VectorDefinition}, file::{Directory, Locator, verified_directory}};

const MAX_BYTES: u64 = 256 << 20;

pub async fn committed_native_ready(
    context: Arc<QueryContext>, table: &TableRef, name: &str, declaration: &RankedIndexDeclaration,
) -> Result<bool> {
    context.reading(async {
        let Some(definition) = native_definition(&context, table, name, declaration).await? else {
            return Ok(false);
        };
        visit_native_generations(&context, &definition, None, |_, _| Ok(())).await
    }).await
}

pub(crate) async fn native_definition(
    context: &Arc<QueryContext>, table: &TableRef, name: &str, declaration: &RankedIndexDeclaration,
) -> Result<Option<VectorDefinition>> {
    let RankedIndexKind::Ivf { dimensions, recall_bits } = &declaration.definition.kind else { return Ok(None); };
    let Some((column, partition)) = declaration.definition.columns.split_first() else { return Ok(None); };
    Ok(definition::committed_native_definition(Arc::clone(context), table, name).await?.filter(|definition|
        i64::try_from(declaration.position).ok() == Some(definition.generation)
            && *dimensions as usize == definition.dimensions && *recall_bits == definition.approximate.recall.to_bits()
            && column == &definition.column && partition == definition.partition.as_slice()))
}

pub fn mounted_native_definition(table: &TableRef, name: &str,
    declaration: &RankedIndexDeclaration) -> Result<VectorDefinition> {
    let RankedIndexKind::Ivf { dimensions, recall_bits } = declaration.definition.kind else {
        return Err(Anomaly::fault("rtdb.vector/native-catalog", "The mounted definition is not IVF"));
    };
    let (column, partition) = declaration.definition.columns.split_first()
        .ok_or_else(|| Anomaly::fault("rtdb.vector/native-catalog", "The mounted definition has no vector column"))?;
    let generation = i64::try_from(declaration.position)
        .map_err(|_| Anomaly::fault("rtdb.vector/native-catalog", "The mounted declaration generation exceeds the file format"))?;
    Ok(VectorDefinition {
        name: format!("{name}~native"),
        table: table.clone(),
        column: column.clone(),
        partition: partition.to_vec(),
        dimensions: dimensions as usize,
        approximate: crate::Approximate::new(crate::Method::Ivf, f64::from_bits(recall_bits))?,
        generation,
    })
}

pub async fn mounted_native_ready(context: &Arc<QueryContext>, definition: &VectorDefinition) -> Result<bool> {
    visit_native_generations(context, definition, None, |_, _| Ok(())).await
}

pub(crate) async fn visit_native_generations(
    context: &Arc<QueryContext>, definition: &VectorDefinition, bounds: Option<TemporalBounds>,
    mut visit: impl FnMut(CacheLease, Directory) -> Result<()>,
) -> Result<bool> {
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Ok(false); };
    let table = &definition.table;
    if snapshot.catalog_cut().is_none() || snapshot.has_overlays()
        || snapshot.iter_segments(table).any(|segment| segment.row_count() != 0)
        || bounds.is_some_and(|bounds| snapshot.system_time_cap() != Some(bounds.system_time.lower()))
    {
        return Ok(false);
    }
    let Some(table_entry) = snapshot.table_entry(table) else { return Ok(false); };
    let count = snapshot.trie_catalog().iter_tries(table).count();
    let _inventory = context.memory.reserve(4096usize.saturating_add(count.saturating_mul(128)))?;
    let artefact = definition.artefact();
    let mut found = false;
    let mut bytes_read = 0u64;
    for trie in snapshot.trie_entries(table).into_iter().filter(|trie| trie.state == TrieState::Live) {
        context.check_cancelled()?;
        if trie.key.recency().is_some() {
            if bounds.is_some_and(|bounds| crate::shortlist::visible(trie, bounds)) { return Ok(false); }
            continue;
        }
        let Some(record) = trie.artefact(&artefact.kind, &artefact.id, artefact.version) else { return Ok(false); };
        let Some(size) = record.file_size.and_then(|size| u64::try_from(size).ok()).filter(|size| *size > 0) else {
            return Ok(false);
        };
        let Some(locator) = record.stats.as_deref().and_then(Locator::decode)
            .filter(|locator| locator.dimensions as usize == definition.dimensions) else { return Ok(false); };
        if record.scope() != rtdb_format::log::proto::ArtefactScope::Trie || !record.pack_members.is_empty() {
            return Ok(false);
        }
        bytes_read = bytes_read.saturating_add(size);
        if bytes_read > MAX_BYTES { return Ok(false); }
        let key = ObjectKey::new(table_entry.slug.recorded_artefact_key(&trie.key, record)?)?;
        let Some(bytes) = database.storage.get_published_exact_or(&key, size as usize,
            record.published_version.as_ref(), &context.memory).await? else { return Ok(false); };
        let directory = verified_directory(&bytes, &locator, &context.memory, &context.cancellation)?;
        visit(bytes, directory)?;
        found = true;
    }
    Ok(found)
}
