use std::collections::BTreeSet;

use async_trait::async_trait;
use rtdb_format::data::StoredOperation;
use rtdb_index::TrieState;
use rtdb_plan::{BinaryOp, Expr};
use rtdb_query::{ScanAccelerator, ScanRequest};
use rtdb_storage::ObjectKey;
use rtdb_text::Query;
use rtdb_types::{Iid, Result, TemporalBounds, Timestamp, Value};

use crate::{KIND, SearchDefinition, SearchFile, search_rows};

/// Narrows a scan with a `MATCH` predicate to the IIDs whose events hold the query's terms (fts.allium: the query plan).
pub struct SearchAccelerator {
    /// Above this many candidates the scan runs unnarrowed: reading every page costs less than resolving that many point lookups.
    pub max_candidates: usize,
    /// Above this many bytes of tries the lane should have indexed but has not, the scan runs unnarrowed rather than reading them twice.
    pub max_unindexed_bytes: i64,
    runtime: std::sync::Arc<crate::SearchRuntime>,
}

impl SearchAccelerator {
    pub fn with_runtime(runtime: std::sync::Arc<crate::SearchRuntime>) -> Self { Self { max_candidates: 100_000, max_unindexed_bytes: 64 * 1024 * 1024, runtime } }
}

impl Default for SearchAccelerator {
    fn default() -> Self { Self::with_runtime(crate::SearchRuntime::new(256 << 20)) }
}

fn match_conjuncts<'a>(expression: &'a Expr, found: &mut Vec<(&'a rtdb_plan::ColumnId, &'a Expr)>) {
    match expression {
        Expr::Binary { op: BinaryOp::And, left, right } => { match_conjuncts(left, found); match_conjuncts(right, found); }
        Expr::Call { name, args, .. } if name.eq_ignore_ascii_case("match") && args.len() == 2 => {
            if let Expr::Column { id, .. } = &args[0] { found.push((id, &args[1])); }
        }
        _ => {}
    }
}

/// An event can be the visible version only if its own valid and system interval reaches the query's, and its recency is past the query's earliest instant.
/// Resolution only ever narrows an event's interval, so this rules candidates out and never in.
fn may_be_visible(valid_from: i64, valid_to: i64, system_from: i64, recency: i64, bounds: TemporalBounds) -> bool {
    let (valid, system) = (bounds.valid_time, bounds.system_time);
    valid_from < valid.upper().0 && valid_to > valid.lower().0 && system_from < system.upper().0 && valid.lower().0.min(system.lower().0) < recency
}

#[async_trait]
impl ScanAccelerator for SearchAccelerator {
    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
        let scan = request.scan;
        let mut conjuncts = Vec::new();
        for predicate in &scan.predicates { match_conjuncts(predicate, &mut conjuncts); }
        if conjuncts.is_empty() { return Ok(None); }
        let table = &scan.table.name.table;
        let definitions = self.runtime.definitions(request.database, request.snapshot, request.storage, request.memory, request.current_time, request.cancellation).await?;
        let mut chosen: Option<(String, Query, &SearchDefinition)> = None;
        for (id, query) in conjuncts {
            let Some(column) = scan.columns.iter().find(|field| field.id == *id).map(|field| field.name.clone()) else { continue };
            let Some(Value::Utf8(text)) = request.constants.constant(query).await? else { continue };
            let Some(definition) = definitions.iter().find(|definition| &definition.table == table && definition.columns.contains(&column)) else { continue };
            chosen = Some((column, Query::parse(&text), definition));
            break;
        }
        let Some((column, query, definition)) = chosen else { return Ok(None) };
        if query.terms().is_empty() { return Ok(Some(BTreeSet::new())); }
        let bounds = request.bounds;
        let Some(entry) = request.snapshot.table_entry(table) else { return Ok(Some(self.live_candidates(request, &column, &query)?)) };
        let artefact_id = definition.artefact_id();
        let tries: Vec<_> = request.snapshot.trie_catalog().current_tries(table).into_iter().filter(|trie| trie.state == TrieState::Live).collect();
        let mut candidates = BTreeSet::new();
        let mut unindexed = Vec::new();
        let mut unindexed_bytes = 0i64;
        for trie in tries {
            let indexed = match trie.artefact(KIND, &artefact_id, definition.version) {
                Some(_) => {
                    let key = ObjectKey::new(entry.slug.artefact_key(&trie.key, KIND, &artefact_id, definition.version)?)?;
                    match self.runtime.file(request.database, request.storage, &key, &definition.columns).await {
                            Ok(file) => {
                                if trie.key.recency().is_none() { request.corpora.register(format!("{key}#{column}"), std::sync::Arc::new(FileCorpus { file: std::sync::Arc::clone(&file), column: column.clone() })); }
                                for doc in file.matches(&column, &query)? {
                                    if may_be_visible(doc.valid_from, doc.valid_to, doc.system_from, doc.recency, bounds) { candidates.insert(Iid(doc.iid)); }
                                }
                                true
                            }
                            Err(_) => {
                                self.runtime.report(crate::runtime::Suspect { database: request.database.to_string(), table: table.clone(), trie: trie.key.clone(), artefact_id: artefact_id.clone(), version: definition.version });
                                false
                            }
                    }
                }
                None => false,
            };
            if !indexed {
                // The lane never indexes level 0 or the partial level-1 current trie, so only a trie it should have indexed counts against the budget.
                let by_policy = trie.key.level() == 0 || (trie.key.level() == 1 && trie.key.recency().is_none() && trie.data_file_size < request.snapshot.trie_catalog().file_size_target());
                if !by_policy {
                    unindexed_bytes = unindexed_bytes.saturating_add(trie.data_file_size);
                    if unindexed_bytes > self.max_unindexed_bytes { return Ok(None); }
                }
                unindexed.push(trie.key.clone());
            }
            if candidates.len() > self.max_candidates { return Ok(None); }
        }
        for key in unindexed {
            let lease = request.storage.get(&ObjectKey::new(entry.slug.data_key(&key))?).await?;
            for row in search_rows(lease.as_slice(), std::slice::from_ref(&column))? {
                if query.matches(&row.tokens[0]) && may_be_visible(row.valid_from, row.valid_to, row.system_from, row.recency.unwrap_or(i64::MAX), bounds) {
                    candidates.insert(Iid(row.iid));
                }
            }
            if candidates.len() > self.max_candidates { return Ok(None); }
        }
        candidates.extend(self.live_candidates(request, &column, &query)?);
        Ok((candidates.len() <= self.max_candidates).then_some(candidates))
    }
}

/// A current-side search file a scan opened, counting its column's corpus for whatever terms a score asks about.
struct FileCorpus { file: std::sync::Arc<SearchFile>, column: String }

impl rtdb_query::CorpusSource for FileCorpus {
    fn corpus(&self, query: &Query) -> Result<rtdb_text::Corpus> { self.file.corpus(&self.column, query) }
}

impl SearchAccelerator {
    fn live_candidates(&self, request: &ScanRequest<'_>, column: &str, query: &Query) -> Result<BTreeSet<Iid>> {
        let mut candidates = BTreeSet::new();
        for segment in request.snapshot.iter_segments(&request.scan.table.name.table) {
            for row in 0..segment.row_count() {
                let event = segment.row(row)?.event()?;
                let StoredOperation::Put(document) = &event.operation else { continue };
                let Some(Value::Utf8(text)) = document.get(column) else { continue };
                if query.matches(&rtdb_text::tokens(text))
                    && may_be_visible(event.valid_time.lower().0, event.valid_time.upper().0, event.system_from.0, Timestamp::MAX.0, request.bounds) {
                    candidates.insert(event.iid);
                }
            }
        }
        Ok(candidates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::Interval;

    fn at(valid: i64, system: i64) -> TemporalBounds {
        TemporalBounds { valid_time: Interval::new(Timestamp(valid), Timestamp(valid + 1)).unwrap(), system_time: Interval::new(Timestamp(system), Timestamp(system + 1)).unwrap() }
    }

    #[test]
    fn an_event_stays_a_candidate_exactly_when_its_own_intervals_reach_the_query() {
        assert!(may_be_visible(10, 20, 5, i64::MAX, at(10, 5)), "an event created at the queried system instant is visible then");
        assert!(may_be_visible(10, 20, 5, i64::MAX, at(19, 100)));
        assert!(!may_be_visible(10, 20, 5, i64::MAX, at(20, 100)), "valid-to is exclusive");
        assert!(!may_be_visible(10, 20, 5, i64::MAX, at(9, 100)));
        assert!(!may_be_visible(10, 20, 6, i64::MAX, at(10, 5)), "an event recorded after the queried system time is not yet known");
        assert!(!may_be_visible(10, 100, 5, 30, at(35, 40)), "superseded by its recency before both of the query's instants");
        assert!(may_be_visible(10, 100, 5, 30, at(29, 40)), "the earlier of the two instants decides, as the scan prunes a page");
        assert!(may_be_visible(10, 100, 5, 30, at(35, 29)));
    }
}
