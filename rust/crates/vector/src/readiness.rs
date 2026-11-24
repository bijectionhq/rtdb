use std::sync::Arc;

use rtdb_index::branches::{RankedIndexDeclaration, RankedIndexKind};
use rtdb_query::QueryContext;
use rtdb_storage::ObjectKey;
use rtdb_types::{Result, TableRef};

use crate::{definition, file::{Locator, verify_generation}};

const MAX_BYTES: u64 = 256 << 20;

pub async fn committed_native_ready(
    context: Arc<QueryContext>, table: &TableRef, name: &str, declaration: &RankedIndexDeclaration,
) -> Result<bool> {
    let RankedIndexKind::Ivf { dimensions, recall_bits } = &declaration.definition.kind else { return Ok(false); };
    let Some((column, partition)) = declaration.definition.columns.split_first() else { return Ok(false); };
    context.reading(async {
        let database = context.catalog.database(context.catalog.default_database())?;
        let [snapshot] = database.partitions.as_slice() else { return Ok(false); };
        if snapshot.catalog_cut().is_none() || snapshot.has_overlays()
            || snapshot.iter_segments(table).any(|segment| segment.row_count() != 0)
        {
            return Ok(false);
        }
        let Some(table_entry) = snapshot.table_entry(table) else { return Ok(false); };
        let Some(definition) = definition::committed_native_definition(Arc::clone(&context), table, name).await? else {
            return Ok(false);
        };
        if i64::try_from(declaration.position).ok() != Some(definition.generation)
            || *dimensions as usize != definition.dimensions || *recall_bits != definition.approximate.recall.to_bits()
            || column != &definition.column || partition != definition.partition.as_slice()
        {
            return Ok(false);
        }
        let artefact = definition.artefact();
        let count = snapshot.trie_catalog().iter_tries(table).count();
        let _inventory = context.memory.reserve(4096usize.saturating_add(count.saturating_mul(128)))?;
        let mut found = false;
        let mut bytes_read = 0u64;
        for trie in snapshot.trie_entries(table).into_iter().filter(|trie| trie.key.recency().is_none()) {
            context.check_cancelled()?;
            let Some(record) = trie.artefact(&artefact.kind, &artefact.id, artefact.version) else { return Ok(false); };
            let Some(size) = record.file_size.and_then(|size| u64::try_from(size).ok()).filter(|size| *size > 0) else {
                return Ok(false);
            };
            let Some(locator) = record.stats.as_deref().and_then(Locator::decode).filter(|locator| locator.dimensions == *dimensions) else {
                return Ok(false);
            };
            if record.scope() != rtdb_format::log::proto::ArtefactScope::Trie || !record.pack_members.is_empty() {
                return Ok(false);
            }
            bytes_read = bytes_read.saturating_add(size);
            if bytes_read > MAX_BYTES { return Ok(false); }
            let key = ObjectKey::new(table_entry.slug.recorded_artefact_key(&trie.key, record)?)?;
            let Some(bytes) = database.storage.get_published_exact_or(&key, size as usize,
                record.published_version.as_ref(), &context.memory).await? else { return Ok(false); };
            verify_generation(bytes, &locator, &context.memory, &context.cancellation)?;
            found = true;
        }
        Ok(found)
    }).await
}
