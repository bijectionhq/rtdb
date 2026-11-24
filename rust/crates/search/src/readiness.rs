use std::sync::Arc;

use rtdb_format::keys::ArtefactOwner;
use rtdb_index::branches::{RankedIndexDeclaration, RankedIndexKind};
use rtdb_query::QueryContext;
use rtdb_storage::ObjectKey;
use rtdb_types::{Result, TableRef};

use crate::{SearchFile, definitions, file::{FORMAT_VERSION, artefact_identity, decode_footer_locator}, lane::KIND};

const MAX_BYTES: usize = 128 << 20;

pub async fn committed_native_ready(
    context: Arc<QueryContext>, table: &TableRef, name: &str, declaration: &RankedIndexDeclaration,
) -> Result<bool> {
    let RankedIndexKind::Search { analyzer, history } = &declaration.definition.kind else { return Ok(false); };
    context.reading(async {
        let database = context.catalog.database(context.catalog.default_database())?;
        let [snapshot] = database.partitions.as_slice() else { return Ok(false); };
        if snapshot.catalog_cut().is_none() || snapshot.has_overlays()
            || snapshot.iter_segments(table).any(|segment| segment.row_count() != 0)
        {
            return Ok(false);
        }
        let Some(table_entry) = snapshot.table_entry(table) else { return Ok(false); };
        let Some(definition) = definitions::committed_native_definition(Arc::clone(&context), table, name).await? else {
            return Ok(false);
        };
        if i64::try_from(declaration.position).ok() != Some(definition.version)
            || declaration.definition.columns != definition.columns || analyzer.as_str() != definition.analyzer.name()
            || *history != definition.history
        {
            return Ok(false);
        }
        let id = definition.artefact_id_for(FORMAT_VERSION);
        let count = snapshot.trie_catalog().iter_tries(table).count();
        let _inventory = context.memory.reserve(4096usize.saturating_add(count.saturating_mul(128)))?;
        let mut found = false;
        let mut bytes_read = 0usize;
        for trie in snapshot.trie_entries(table).into_iter().filter(|trie| *history || trie.key.recency().is_none()) {
            context.check_cancelled()?;
            let Some(record) = trie.artefact(KIND, &id, definition.version) else { return Ok(false); };
            let Some(locator) = record.stats.as_deref().and_then(decode_footer_locator)
                .filter(|locator| i64::try_from(locator.length).ok() == record.file_size) else { return Ok(false); };
            let owner = ArtefactOwner::of(&trie.key, record)?;
            let identity = artefact_identity(table_entry.slug.as_str(), &owner, &id, definition.version);
            let key = ObjectKey::new(table_entry.slug.recorded_artefact_key(&trie.key, record)?)?;
            bytes_read = bytes_read.saturating_add(locator.length);
            if bytes_read > MAX_BYTES { return Ok(false); }
            let bytes = database.storage.get_fresh_exact_or(&key, locator.length, &context.memory).await?;
            let _decode = context.memory.reserve(locator.length.saturating_mul(4).saturating_add(4096))?;
            let file = SearchFile::open_located(bytes.into(), &definition.columns, Some(&locator))?;
            if !file.matches_artefact(&identity, definition.version) { return Ok(false); }
            file.verify()?;
            found = true;
        }
        Ok(found)
    }).await
}
