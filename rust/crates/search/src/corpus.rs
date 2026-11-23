//! The corpus a score is relative to: a column's values in the rows a scan of the table sees at the snapshot's own instant, as valid time and system time both (fts.allium: CorpusAsOfTheSnapshot).
//! A file's runs count where nothing outside the file cuts them; an entity with events in more than one source, or in the live index, is resolved across them all.

use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::sync::Arc;

use parking_lot::Mutex;
use rtdb_index::LiveTable;
use rtdb_text::{Corpus, Query};
use rtdb_types::{EventHeader, Iid, Interval, Result, Timestamp, Value};
use tantivy::DocSet;

use crate::file::KeyedEvent;
use crate::runtime::TrieFile;
use crate::SearchRuntime;

/// A file's runs of a column that are visible at the snapshot's instant where no event outside the file cuts them.
/// A stable run is visible at every instant from the file's latest event on; an exception only over a bounded valid interval, or from a later valid time.
pub(crate) struct OpenRuns { stable: Vec<u64>, stable_docs: u64, stable_tokens: u64, exceptions: Vec<(u32, i64, i64)>, stable_frequencies: Mutex<BTreeMap<String, u64>> }

impl OpenRuns {
    pub(crate) fn of(file: &crate::SearchFile, member: u32, column: &str) -> Result<Self> {
        let rows = file.member_rows(member)?;
        let latest = file.latest_system_from(rows.clone())?;
        let docs: BTreeSet<u32> = rows.filter_map(|row| file.run_of(column, row).flatten()).collect();
        let mut runs = Self { stable: vec![0; docs.last().map_or(0, |last| *last as usize / 64 + 1)], stable_docs: 0, stable_tokens: 0, exceptions: Vec::new(), stable_frequencies: Mutex::new(BTreeMap::new()) };
        for doc in docs {
            let open: Vec<[i64; 4]> = file.rectangles(doc).filter(|rectangle| rectangle[3] == i64::MAX).collect();
            if open.iter().any(|rectangle| rectangle[1] == i64::MAX && rectangle[0] <= latest) {
                runs.stable[doc as usize / 64] |= 1 << (doc % 64);
                runs.stable_docs += 1;
                runs.stable_tokens += u64::from(file.stored_length(column, doc)?);
            } else {
                runs.exceptions.extend(open.iter().map(|rectangle| (doc, rectangle[0], rectangle[1])));
            }
        }
        Ok(runs)
    }
    fn stable(&self, doc: u32) -> bool { self.stable.get(doc as usize / 64).is_some_and(|word| word & (1 << (doc % 64)) != 0) }
    /// Whether the run counts at `at` as far as its own file decides.
    fn counted(&self, doc: u32, at: i64) -> bool {
        self.stable(doc) || self.exceptions.iter().any(|(held, from, to)| *held == doc && *from <= at && at < *to)
    }
}

/// Where an event of a resolved entity came from.
#[derive(Clone, Copy)]
enum Origin { Live(usize, u32), File(usize, u32) }

/// The value a resolved entity's visible row holds: a file's run, or a live row's tokens.
enum Counted { Run(usize, u32), Live(Vec<String>) }

struct CorpusMember {
    trie: TrieFile,
    runs: Arc<OpenRuns>,
    adjustments: BTreeMap<u32, i64>,
}

impl CorpusMember {
    fn adjust(&mut self, doc: u32, delta: i64) {
        let adjustment = self.adjustments.entry(doc).or_default();
        *adjustment += delta;
        if *adjustment == 0 { self.adjustments.remove(&doc); }
    }

    fn frequency<'a>(&'a self, column: &str, term: &str, decoded: &mut BTreeMap<&'a str, Vec<u32>>) -> Result<i64> {
        let cached = self.runs.stable_frequencies.lock().get(term).copied();
        let stable = match cached {
            Some(frequency) => frequency,
            None => {
                let docs = match decoded.entry(self.trie.identity.as_str()) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => entry.insert(self.trie.file.holding(column, term)?),
                };
                let frequency = docs.iter().filter(|doc| self.runs.stable(**doc)).count() as u64;
                self.runs.stable_frequencies.lock().insert(term.to_string(), frequency);
                frequency
            }
        };
        let adjustment = if self.adjustments.is_empty() {
            0
        } else if let Some(docs) = decoded.get(self.trie.identity.as_str()) {
            self.adjustments.iter().filter(|(doc, _)| docs.binary_search(doc).is_ok()).map(|(_, delta)| delta).sum()
        } else if let Some(mut postings) = self.trie.file.postings(column, term)? {
            self.adjustments.iter().filter(|(doc, _)| {
                if postings.doc() < **doc { postings.seek(**doc); }
                postings.doc() == **doc
            }).map(|(_, delta)| delta).sum()
        } else {
            0
        };
        Ok(stable as i64 + adjustment)
    }
}

/// The corpus of one column of one table at one snapshot.
pub struct SnapshotCorpus {
    members: Vec<CorpusMember>,
    column: String,
    at: i64,
    documents: u64,
    tokens: u64,
    live_frequencies: BTreeMap<String, u64>,
}

impl SnapshotCorpus {
    pub(crate) fn compute(runtime: &SearchRuntime, database: &str, files: Vec<TrieFile>, segments: &[Arc<LiveTable>], column: &str, at: i64) -> Result<Self> {
        let mut resolved = shared_iids(&files)?;
        let mut corpus = Self { members: Vec::with_capacity(files.len()), column: column.to_string(), at, documents: 0, tokens: 0, live_frequencies: BTreeMap::new() };
        for trie in files {
            let runs = runtime.open_runs(&trie, column)?;
            corpus.documents += runs.stable_docs;
            corpus.tokens += runs.stable_tokens;
            let mut adjustments = BTreeMap::new();
            for (doc, from, to) in &runs.exceptions {
                if *from <= at && at < *to {
                    corpus.documents += 1;
                    corpus.tokens += u64::from(trie.file.stored_length(column, *doc)?);
                    *adjustments.entry(*doc).or_default() += 1;
                }
            }
            corpus.members.push(CorpusMember { trie, runs, adjustments });
        }
        for segment in segments { resolved.extend(runtime.live_iids(database, segment)?); }
        for iid in resolved { corpus.resolve(runtime, database, segments, &iid)?; }
        Ok(corpus)
    }

    /// Replaces what the entity's runs counted file by file with its one visible row across every source.
    fn resolve(&mut self, runtime: &SearchRuntime, database: &str, segments: &[Arc<LiveTable>], iid: &[u8; 16]) -> Result<()> {
        let mut events: Vec<(Origin, KeyedEvent)> = Vec::new();
        for (index, segment) in segments.iter().enumerate() {
            events.extend(runtime.live_rows_of(database, segment, iid)?.into_iter().map(|(row, event)| (Origin::Live(index, row), event)));
        }
        for (index, member) in self.members.iter_mut().enumerate() {
            let file = &member.trie.file;
            let member_rows = file.member_rows(member.trie.member)?;
            let mut docs = BTreeSet::new();
            for (row, event) in file.rows_of(iid)?.into_iter().filter(|(row, _)| member_rows.contains(row)) {
                if let Some(Some(doc)) = file.run_of(&self.column, row) { docs.insert(doc); }
                events.push((Origin::File(index, row), event));
            }
            for doc in docs {
                if member.runs.counted(doc, self.at) {
                    self.documents -= 1;
                    self.tokens -= u64::from(member.trie.file.stored_length(&self.column, doc)?);
                    member.adjust(doc, -1);
                }
            }
        }
        // Preserve the scan's source and row order when events share a system time.
        events.sort_by_key(|(_, event)| std::cmp::Reverse(event.system_from));
        let headers = events.iter().map(|(_, event)| Ok(EventHeader { iid: Iid(*iid), system_from: Timestamp(event.system_from), valid_time: Interval::from_micros(event.valid_from, event.valid_to)?, op: event.op })).collect::<Result<Vec<_>>>()?;
        let at = self.at;
        let winner = rtdb_temporal::visibility(headers.iter().enumerate().map(|(index, header)| (header, index)))?.into_iter()
            .find(|(_, rectangle)| rectangle.valid_time.lower().0 <= at && at < rectangle.valid_time.upper().0 && rectangle.system_time.lower().0 <= at && at < rectangle.system_time.upper().0)
            .map(|(index, _)| events[index].0);
        let counted = match winner {
            None => None,
            Some(Origin::File(index, row)) => self.members[index].trie.file.run_of(&self.column, row).flatten().map(|doc| Counted::Run(index, doc)),
            Some(Origin::Live(index, row)) => match segments[index].row(row)?.read_put_field(&self.column)? {
                Value::Null | Value::Undefined => None,
                Value::Utf8(text) => Some(Counted::Live(rtdb_text::tokens(&text))),
                _ => Some(Counted::Live(Vec::new())),
            },
        };
        if let Some(counted) = counted {
            self.documents += 1;
            match counted {
                Counted::Run(index, doc) => {
                    let member = &mut self.members[index];
                    self.tokens += u64::from(member.trie.file.stored_length(&self.column, doc)?);
                    member.adjust(doc, 1);
                }
                Counted::Live(mut tokens) => {
                    self.tokens += u64::from(rtdb_text::stored_length(tokens.len()));
                    tokens.sort_unstable();
                    tokens.dedup();
                    for term in tokens { *self.live_frequencies.entry(term).or_default() += 1; }
                }
            }
        }
        Ok(())
    }

    /// The corpus for a query's terms.
    pub fn corpus(&self, query: &Query) -> Result<Corpus> {
        let mut corpus = Corpus { documents: self.documents, tokens: self.tokens, frequencies: BTreeMap::new() };
        for term in query.terms() {
            let mut decoded = BTreeMap::new();
            let mut frequency = self.live_frequencies.get(term).copied().unwrap_or_default() as i64;
            for member in &self.members { frequency += member.frequency(&self.column, term, &mut decoded)?; }
            corpus.frequencies.insert(term.clone(), frequency.max(0) as u64);
        }
        Ok(corpus)
    }
}

/// The entities with events in more than one selected trie, merged from each member's IIDs in order.
fn shared_iids(files: &[TrieFile]) -> Result<BTreeSet<[u8; 16]>> {
    let mut streams = files.iter().map(|trie| {
        Ok(trie.file.member_iids(trie.file.member_rows(trie.member)?))
    }).collect::<Result<Vec<_>>>()?;
    let mut heap: BinaryHeap<std::cmp::Reverse<([u8; 16], usize)>> = BinaryHeap::new();
    for (index, stream) in streams.iter_mut().enumerate() { if let Some(iid) = stream.next() { heap.push(std::cmp::Reverse((iid, index))); } }
    let mut shared = BTreeSet::new();
    let mut last: Option<[u8; 16]> = None;
    while let Some(std::cmp::Reverse((iid, index))) = heap.pop() {
        if last == Some(iid) { shared.insert(iid); }
        last = Some(iid);
        if let Some(next) = streams[index].next() { heap.push(std::cmp::Reverse((next, index))); }
    }
    Ok(shared)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SearchFile, SearchRow, build_search_file};
    use rtdb_storage::ObjectKey;
    use rtdb_types::EventOp;

    #[test]
    fn cached_frequencies_follow_cross_file_corrections_and_validity_boundaries() {
        let columns = ["body".to_string()];
        let put = |iid, system_from, valid_from, valid_to, body: &str| SearchRow {
            iid: [iid; 16], system_from, valid_from, valid_to, op: EventOp::Put,
            values: vec![Some(rtdb_text::tokens(body))],
        };
        let make_file = |identity: &str, rows: &[SearchRow]| Arc::new(SearchFile::open(
            build_search_file(&columns, rows, identity).unwrap(), &columns,
        ).unwrap());
        let base = make_file("base", &[put(1, 0, 0, i64::MAX, "alpha"), put(2, 0, 10, 20, "alpha beta")]);
        let correction = make_file("correction", &[put(1, 5, 5, 15, "beta")]);
        let member = |key: &str, file: &Arc<SearchFile>| TrieFile {
            trie: key.parse().unwrap(), data: ObjectKey::new(format!("tables/docs/data/{key}.arrow")).unwrap(),
            identity: key.into(), file: Arc::clone(file), member: 0,
        };
        let runtime = SearchRuntime::new(1 << 20);
        let query = Query::parse("alpha beta missing");
        for (at, documents, tokens, alpha, beta) in [(6, 1, 1, 0, 1), (12, 2, 3, 1, 2), (20, 1, 1, 1, 0)] {
            let files = vec![member("l01-rc-b00", &base), member("l01-rc-b01", &correction)];
            let corpus = runtime.snapshot_corpus("xtdb", files, &[], "body", at).unwrap();
            let expected = Corpus {
                documents, tokens,
                frequencies: BTreeMap::from([("alpha".into(), alpha), ("beta".into(), beta), ("missing".into(), 0)]),
            };
            assert_eq!(corpus.corpus(&query).unwrap(), expected, "at {at}");
            assert_eq!(corpus.corpus(&query).unwrap(), expected, "cached at {at}");
        }
    }
}
