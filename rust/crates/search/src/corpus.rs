//! The corpus a score is relative to: a column's values in the rows a scan of the table sees at the snapshot's own instant, as valid time and system time both (fts.allium: CorpusAsOfTheSnapshot).
//! A file's runs count where nothing outside the file cuts them; an entity with events in more than one source, or in the live index, is resolved across them all.

use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::sync::Arc;

use parking_lot::Mutex;
use rtdb_format::data::StoredOperation;
use rtdb_index::LiveTable;
use rtdb_text::{Corpus, Query};
use rtdb_types::{EventHeader, Iid, Interval, Result, Timestamp, Value};

use crate::file::KeyedEvent;
use crate::runtime::TrieFile;
use crate::SearchRuntime;

/// A file's runs of a column that are visible at the snapshot's instant where no event outside the file cuts them.
/// A stable run is visible at every instant from the file's latest event on; an exception only over a bounded valid interval, or from a later valid time.
pub(crate) struct OpenRuns { stable: Vec<u64>, stable_docs: u64, stable_tokens: u64, exceptions: Vec<(u32, i64, i64)>, stable_frequencies: Mutex<BTreeMap<String, u64>> }

impl OpenRuns {
    pub(crate) fn of(file: &crate::SearchFile, column: &str) -> Result<Self> {
        let latest = file.latest_system_from()?;
        let docs: BTreeSet<u32> = (0..file.rows()).filter_map(|row| file.run_of(column, row).flatten()).collect();
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
    fn stable_frequency(&self, file: &crate::SearchFile, column: &str, term: &str) -> Result<u64> {
        if let Some(count) = self.stable_frequencies.lock().get(term) { return Ok(*count); }
        let count = file.holding(column, term)?.into_iter().filter(|doc| self.stable(*doc)).count() as u64;
        self.stable_frequencies.lock().insert(term.to_string(), count);
        Ok(count)
    }
}

/// Where an event of a resolved entity came from.
#[derive(Clone, Copy)]
enum Origin { Live(usize, u32), File(usize, u32) }

/// The value a resolved entity's visible row holds: a file's run, or a live row's tokens.
enum Counted { Run(usize, u32), Live(Vec<String>) }

/// The corpus of one column of one table at one snapshot.
pub struct SnapshotCorpus { files: Vec<TrieFile>, open: Vec<Arc<OpenRuns>>, column: String, at: i64, documents: u64, tokens: u64, added: Vec<Counted>, removed: Vec<(usize, u32)> }

impl SnapshotCorpus {
    pub(crate) fn compute(runtime: &SearchRuntime, database: &str, files: Vec<TrieFile>, segments: &[Arc<LiveTable>], column: &str, at: i64) -> Result<Self> {
        let open = files.iter().map(|trie| runtime.open_runs(trie, column)).collect::<Result<Vec<_>>>()?;
        let mut corpus = Self { files, open, column: column.to_string(), at, documents: 0, tokens: 0, added: Vec::new(), removed: Vec::new() };
        for (index, runs) in corpus.open.iter().enumerate() {
            corpus.documents += runs.stable_docs;
            corpus.tokens += runs.stable_tokens;
            for (doc, from, to) in &runs.exceptions {
                if *from <= at && at < *to { corpus.documents += 1; corpus.tokens += u64::from(corpus.files[index].file.stored_length(column, *doc)?); }
            }
        }
        let mut resolved: BTreeSet<[u8; 16]> = shared_iids(&corpus.files);
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
        for (index, trie) in self.files.iter().enumerate() {
            let rows = trie.file.rows_of(iid)?;
            let mut docs = BTreeSet::new();
            for (row, event) in rows {
                if let Some(Some(doc)) = trie.file.run_of(&self.column, row) { docs.insert(doc); }
                events.push((Origin::File(index, row), event));
            }
            for doc in docs {
                if self.open[index].counted(doc, self.at) {
                    self.documents -= 1;
                    self.tokens -= u64::from(trie.file.stored_length(&self.column, doc)?);
                    self.removed.push((index, doc));
                }
            }
        }
        // The scan reads live rows ahead of tries for events at one instant, and so does this.
        events.sort_by_key(|(origin, event)| (std::cmp::Reverse(event.system_from), match origin { Origin::Live(index, row) => (0, *index, *row), Origin::File(index, row) => (1, *index, *row) }));
        let headers = events.iter().map(|(_, event)| Ok(EventHeader { iid: Iid(*iid), system_from: Timestamp(event.system_from), valid_time: Interval::from_micros(event.valid_from, event.valid_to)?, op: event.op })).collect::<Result<Vec<_>>>()?;
        let at = self.at;
        let winner = rtdb_temporal::visibility(headers.iter().enumerate().map(|(index, header)| (header, index)))?.into_iter()
            .find(|(_, rectangle)| rectangle.valid_time.lower().0 <= at && at < rectangle.valid_time.upper().0 && rectangle.system_time.lower().0 <= at && at < rectangle.system_time.upper().0)
            .map(|(index, _)| events[index].0);
        let counted = match winner {
            None => None,
            Some(Origin::File(index, row)) => self.files[index].file.run_of(&self.column, row).flatten().map(|doc| Counted::Run(index, doc)),
            Some(Origin::Live(index, row)) => match segments[index].row(row)?.event()?.operation {
                StoredOperation::Put(document) => match document.get(&self.column) {
                    None | Some(Value::Null | Value::Undefined) => None,
                    Some(Value::Utf8(text)) => Some(Counted::Live(rtdb_text::tokens(text))),
                    Some(_) => Some(Counted::Live(Vec::new())),
                },
                _ => None,
            },
        };
        if let Some(counted) = counted {
            self.documents += 1;
            self.tokens += u64::from(match &counted { Counted::Run(index, doc) => self.files[*index].file.stored_length(&self.column, *doc)?, Counted::Live(tokens) => rtdb_text::stored_length(tokens.len()) });
            self.added.push(counted);
        }
        Ok(())
    }

    /// The corpus for a query's terms.
    pub fn corpus(&self, query: &Query) -> Result<Corpus> {
        let mut corpus = Corpus { documents: self.documents, tokens: self.tokens, frequencies: BTreeMap::new() };
        for term in query.terms() {
            let holding = self.files.iter().map(|trie| trie.file.holding(&self.column, term)).collect::<Result<Vec<_>>>()?;
            let holds = |index: usize, doc: u32| holding[index].binary_search(&doc).is_ok();
            let mut frequency = 0i64;
            for (index, runs) in self.open.iter().enumerate() {
                frequency += runs.stable_frequency(&self.files[index].file, &self.column, term)? as i64;
                frequency += runs.exceptions.iter().filter(|(doc, from, to)| *from <= self.at && self.at < *to && holds(index, *doc)).count() as i64;
            }
            frequency -= self.removed.iter().filter(|(index, doc)| holds(*index, *doc)).count() as i64;
            frequency += self.added.iter().filter(|counted| match counted { Counted::Run(index, doc) => holds(*index, *doc), Counted::Live(tokens) => tokens.iter().any(|token| token == term) }).count() as i64;
            corpus.frequencies.insert(term.clone(), frequency.max(0) as u64);
        }
        Ok(corpus)
    }
}

/// The entities with events in more than one of the files, merged from each file's IIDs in order.
fn shared_iids(files: &[TrieFile]) -> BTreeSet<[u8; 16]> {
    let mut streams: Vec<_> = files.iter().map(|trie| trie.file.iids()).collect();
    let mut heap: BinaryHeap<std::cmp::Reverse<([u8; 16], usize)>> = BinaryHeap::new();
    for (index, stream) in streams.iter_mut().enumerate() { if let Some(iid) = stream.next() { heap.push(std::cmp::Reverse((iid, index))); } }
    let mut shared = BTreeSet::new();
    let mut last: Option<[u8; 16]> = None;
    while let Some(std::cmp::Reverse((iid, index))) = heap.pop() {
        if last == Some(iid) { shared.insert(iid); }
        last = Some(iid);
        if let Some(next) = streams[index].next() { heap.push(std::cmp::Reverse((next, index))); }
    }
    shared
}
