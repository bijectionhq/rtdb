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
        let definitions = self.runtime.definitions(request.database, request.partitions, request.storage, request.memory, request.current_time, request.cancellation).await?;
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
        if let Some(top_k) = &scan.top_k {
            let ranked = scan.columns.iter().find(|field| field.id == top_k.column).is_some_and(|field| field.name == column);
            if ranked { if let Some(candidates) = self.top_k(request, top_k, &column, &query, definition).await? { return Ok(Some(candidates)); } }
        }
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
                            Err(error) => {
                                if error.needs_repair() { self.runtime.report(crate::runtime::Suspect { database: request.database.to_string(), table: table.clone(), trie: trie.key.clone(), artefact_id: artefact_id.clone(), version: definition.version }); }
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
                if row.values[0].as_deref().is_some_and(|tokens| query.matches(tokens)) && may_be_visible(row.valid_from, row.valid_to, row.system_from, row.recency.unwrap_or(i64::MAX), bounds) {
                    candidates.insert(Iid(row.iid));
                }
            }
            if candidates.len() > self.max_candidates { return Ok(None); }
        }
        candidates.extend(self.live_candidates(request, &column, &query)?);
        Ok((candidates.len() <= self.max_candidates).then_some(candidates))
    }
}

/// A row's ranking key: its score, then its IID, so that ties sort deterministically.
fn descending(left: &(f64, Iid), right: &(f64, Iid)) -> std::cmp::Ordering { right.0.total_cmp(&left.0).then_with(|| left.1.cmp(&right.1)) }

/// A scored event waiting to be handed out, best score first.
struct Pending(f64, Iid);
impl PartialEq for Pending { fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() } }
impl Eq for Pending {}
impl PartialOrd for Pending { fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) } }
impl Ord for Pending { fn cmp(&self, other: &Self) -> std::cmp::Ordering { descending(&(other.0, other.1), &(self.0, self.1)) } }

/// The IIDs of the matching events that may be visible, each at its best event's score, best first.
/// Search files are expanded a batch of windows at a time, only while one may hold an event above every event already scored.
struct Candidates { files: Vec<crate::file::Ranked>, pending: std::collections::BinaryHeap<Pending>, seen: BTreeSet<Iid>, scored: usize, bounds: TemporalBounds }

impl Candidates {
    fn push(&mut self, iid: Iid, score: f64) { self.scored += 1; self.pending.push(Pending(score, iid)); }
    fn drop_seen(&mut self) { while self.pending.peek().is_some_and(|top| self.seen.contains(&top.1)) { self.pending.pop(); } }
    /// The file whose next windows may hold the best event not yet scored.
    fn best_file(&self) -> Option<(usize, f64)> {
        self.files.iter().enumerate().filter_map(|(index, file)| file.bound().map(|bound| (index, bound))).max_by(|left, right| left.1.total_cmp(&right.1))
    }
    /// No IID not yet handed out has a matching event scoring above this.
    fn frontier(&mut self) -> f64 {
        self.drop_seen();
        let pending = self.pending.peek().map_or(f64::NEG_INFINITY, |top| top.0);
        self.best_file().map_or(pending, |(_, bound)| bound.max(pending))
    }
    fn next(&mut self) -> Result<Option<(f64, Iid)>> {
        loop {
            self.drop_seen();
            match (self.pending.peek().map(|top| top.0), self.best_file()) {
                (pending, Some((index, bound))) if pending.is_none_or(|score| bound > score) => {
                    for (doc, score) in self.files[index].expand()? {
                        if may_be_visible(doc.valid_from, doc.valid_to, doc.system_from, doc.recency, self.bounds) { self.push(Iid(doc.iid), score); }
                    }
                }
                _ => {
                    let Some(Pending(score, iid)) = self.pending.pop() else { return Ok(None) };
                    self.seen.insert(iid);
                    return Ok(Some((score, iid)));
                }
            }
        }
    }
}

impl SearchAccelerator {
    /// The IIDs that can hold one of the `limit` best-scoring visible rows, or `None` to narrow as an unranked MATCH does.
    /// Every matching event is scored exactly, and an IID's best event bounds every row it can resolve to.
    /// IIDs are verified best bound first, in batches, until `limit` verified rows outscore every bound still unverified; a search file's events are scored only as far as that takes.
    async fn top_k(&self, request: &ScanRequest<'_>, top_k: &rtdb_plan::ScanTopK, column: &str, matching: &Query, definition: &SearchDefinition) -> Result<Option<BTreeSet<Iid>>> {
        let Some(Value::Utf8(text)) = request.constants.constant(&top_k.query).await? else { return Ok(None) };
        let limit = match request.constants.constant(&top_k.limit).await.ok().flatten() {
            Some(Value::I64(limit)) => limit, Some(Value::I32(limit)) => i64::from(limit), Some(Value::I16(limit)) => i64::from(limit), _ => return Ok(None),
        };
        let Ok(limit) = usize::try_from(limit) else { return Ok(None) };
        if limit == 0 { return Ok(Some(BTreeSet::new())); }
        let table = &request.scan.table.name.table;
        let Some(entry) = request.snapshot.table_entry(table) else { return Ok(None) };
        let scoring = Query::parse(&text);
        let artefact_id = definition.artefact_id();
        let mut files = Vec::new();
        let mut unindexed = Vec::new();
        let mut unindexed_bytes = 0i64;
        for trie in request.snapshot.trie_catalog().current_tries(table).into_iter().filter(|trie| trie.state == TrieState::Live) {
            let file = match trie.artefact(KIND, &artefact_id, definition.version) {
                Some(_) => {
                    let key = ObjectKey::new(entry.slug.artefact_key(&trie.key, KIND, &artefact_id, definition.version)?)?;
                    match self.runtime.file(request.database, request.storage, &key, &definition.columns).await {
                        Ok(file) => Some(file),
                        Err(error) => {
                            if error.needs_repair() { self.runtime.report(crate::runtime::Suspect { database: request.database.to_string(), table: table.clone(), trie: trie.key.clone(), artefact_id: artefact_id.clone(), version: definition.version }); }
                            None
                        }
                    }
                }
                None => None,
            };
            match file {
                Some(file) => files.push(file),
                None => {
                    let by_policy = trie.key.level() == 0 || (trie.key.level() == 1 && trie.key.recency().is_none() && trie.data_file_size < request.snapshot.trie_catalog().file_size_target());
                    if !by_policy { unindexed_bytes = unindexed_bytes.saturating_add(trie.data_file_size); }
                    if unindexed_bytes > self.max_unindexed_bytes { return Ok(None); }
                    unindexed.push(trie.key.clone());
                }
            }
        }
        self.register_corpora(request, column, definition).await?;
        let corpus = rtdb_expr::CorpusLookup::corpus(request.corpora, &text);
        let ranked = files.iter().map(|file| file.ranked(column, matching, &scoring, corpus.as_ref())).collect::<Result<Vec<_>>>()?;
        let mut candidates = Candidates { files: ranked, pending: std::collections::BinaryHeap::new(), seen: BTreeSet::new(), scored: 0, bounds: request.bounds };
        for key in unindexed {
            let lease = request.storage.get(&ObjectKey::new(entry.slug.data_key(&key))?).await?;
            for row in search_rows(lease.as_slice(), std::slice::from_ref(&column.to_string()))? {
                let Some(tokens) = row.values[0].as_deref() else { continue };
                if matching.matches(tokens) && may_be_visible(row.valid_from, row.valid_to, row.system_from, row.recency.unwrap_or(i64::MAX), request.bounds) {
                    candidates.push(Iid(row.iid), rtdb_text::bm25(&scoring, tokens, corpus.as_ref()));
                }
            }
        }
        for segment in request.snapshot.iter_segments(table) {
            for row in self.runtime.live_rows(request.database, segment, column, matching)? {
                let event = segment.row(row)?.event()?;
                if !may_be_visible(event.valid_time.lower().0, event.valid_time.upper().0, event.system_from.0, Timestamp::MAX.0, request.bounds) { continue; }
                let StoredOperation::Put(document) = &event.operation else { continue };
                let Some(Value::Utf8(value)) = document.get(column) else { continue };
                let tokens = rtdb_text::tokens(value);
                if matching.matches(&tokens) { candidates.push(event.iid, rtdb_text::bm25(&scoring, &tokens, corpus.as_ref())); }
            }
        }
        let mut verified: Vec<(f64, Iid)> = Vec::new();
        let batch = limit.saturating_mul(2).max(64);
        let mut rounds = 0;
        loop {
            if verified.len() >= limit {
                verified.sort_by(descending);
                if verified[limit - 1].0 > candidates.frontier() { break; }
            }
            let mut iids = BTreeSet::new();
            while iids.len() < batch {
                let Some((_, iid)) = candidates.next()? else { break };
                iids.insert(iid);
            }
            if candidates.scored > self.max_candidates { return Ok(None); }
            if iids.is_empty() { break; }
            rounds += 1;
            if rounds > 16 { return Ok(None); }
            for (iid, value) in request.verifier.visible(iids, top_k.column).await? {
                let Value::Utf8(value) = value else { continue };
                verified.push((rtdb_text::bm25(&scoring, &rtdb_text::tokens(&value), corpus.as_ref()), iid));
            }
        }
        verified.sort_by(descending);
        let kth = verified.get(limit - 1).map_or(f64::NEG_INFINITY, |(score, _)| *score);
        Ok(Some(verified.into_iter().filter(|(score, _)| *score >= kth).map(|(_, iid)| iid).collect()))
    }
}

/// A current-side search file a scan opened, counting its column's corpus for whatever terms a score asks about.
struct FileCorpus { file: std::sync::Arc<SearchFile>, column: String }

impl rtdb_query::CorpusSource for FileCorpus {
    fn corpus(&self, query: &Query) -> Result<rtdb_text::Corpus> { self.file.corpus(&self.column, query) }
}

impl SearchAccelerator {
    /// Registers the table's current-side search files in every partition, so the corpus a score fixes is the same whichever partition is ranked first.
    async fn register_corpora(&self, request: &ScanRequest<'_>, column: &str, definition: &SearchDefinition) -> Result<()> {
        let table = &request.scan.table.name.table;
        let artefact_id = definition.artefact_id();
        for snapshot in request.partitions {
            let Some(entry) = snapshot.table_entry(table) else { continue };
            for trie in snapshot.trie_catalog().current_tries(table) {
                if trie.state != TrieState::Live || trie.key.recency().is_some() || trie.artefact(KIND, &artefact_id, definition.version).is_none() { continue; }
                let key = ObjectKey::new(entry.slug.artefact_key(&trie.key, KIND, &artefact_id, definition.version)?)?;
                let name = format!("{key}#{column}");
                if request.corpora.holds(&name) { continue; }
                match self.runtime.file(request.database, request.storage, &key, &definition.columns).await {
                    Ok(file) => request.corpora.register(name, std::sync::Arc::new(FileCorpus { file, column: column.to_string() })),
                    Err(error) => if error.needs_repair() {
                        self.runtime.report(crate::runtime::Suspect { database: request.database.to_string(), table: table.clone(), trie: trie.key.clone(), artefact_id: artefact_id.clone(), version: definition.version });
                    },
                }
            }
        }
        Ok(())
    }

    fn live_candidates(&self, request: &ScanRequest<'_>, column: &str, query: &Query) -> Result<BTreeSet<Iid>> {
        let mut candidates = BTreeSet::new();
        for segment in request.snapshot.iter_segments(&request.scan.table.name.table) {
            for row in self.runtime.live_rows(request.database, segment, column, query)? {
                let source = segment.row(row)?;
                let header = source.header()?;
                if !may_be_visible(header.valid_time.lower().0, header.valid_time.upper().0, header.system_from.0, Timestamp::MAX.0, request.bounds) { continue; }
                if !query.phrases().is_empty() {
                    let event = source.event()?;
                    let StoredOperation::Put(document) = &event.operation else { continue };
                    let Some(Value::Utf8(text)) = document.get(column) else { continue };
                    if !query.matches(&rtdb_text::tokens(text)) { continue; }
                }
                candidates.insert(header.iid);
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
