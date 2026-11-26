//! Family 7: text search over owned relations (store-cut RANKED §1.1,
//! §4.1, §4.2; RTDB `SearchHeld.v7`).
//!
//! A search path is an inverted index written in the same append as the
//! events it indexes. Each put image whose cell holds text writes one
//! document entry (values `0x00`, value `tokens u32 ‖ stored length u32`)
//! and one posting per distinct analysed term (values `0x01 ‖ term`, value
//! `tf u32`); the image it replaces leaves the entries it does not enter
//! again (`kernel_storage`'s `path_writes`).
//!
//! A ranked read takes each query term's posting run in every partition,
//! resolves candidates at the cut and re-analyses their winning text, so
//! the index only nominates: matches, frequencies and phrases are exact
//! (`kernel_text::Query::text_terms`). No key holds a document frequency
//! (a term in a key would outlive an erase).
//!
//! - **Counted paths** (acceptance validity, no lifetime, the present valid
//!   instant, no mask) take each term's frequency from its live posting
//!   run at the cut: postings enter and leave in the same append as their
//!   rows, so the latest entry of `(term, id)` at or below the cut is an
//!   enter exactly when the winner's text holds the term. Only the ids live
//!   in every term's run (the AND intersection) are resolved. An id the
//!   run cannot settle (no entry at or below the cut, or an entry erased
//!   after the hold) is resolved as before, so its purge floor refuses.
//!   Totals are the counters at the cut, less the erasure adjustments of
//!   documents counted there ([`Erasure`]). A world adds each level's
//!   signed counter delta to the Root counters at its parent cut.
//! - **Otherwise** (explicit validity, lifetimes, another valid instant, a
//!   masked cut) every id of every term's run is resolved and the totals
//!   are recounted from the document run. Scores are
//!   `kernel_text::Scorer`'s BM25.
//!
//! The read set of each partition is the path's whole index tablet, its
//! counters and its erasure adjustments: any document write changes every
//! score (coordinator Q1).
//!
//! A read gathers [`TextFeatures`] (the corpus contribution and every
//! match's frequencies) and [`finish`]es them into a window; a read split
//! between the store and Bijection (sealed partitions) gathers on both
//! sides and finishes once over their sum.
use std::collections::{
    BTreeMap,
    BTreeSet,
};

use anyhow::{
    ensure,
    Context,
};
use kernel_text::{
    stored_length,
    Analyzer,
    Corpus,
    Query,
    Scorer,
};

use super::frame::Frame;
use crate::{
    catalog::{
        self,
        Declaration,
        PathKind,
        Validity,
    },
    event::{
        self,
        Cut,
        Event,
        Image,
    },
    keys::{
        self,
        RowId,
    },
    layout::{
        self,
        Family,
        Place,
        ROOT_WORLD,
    },
    segment,
    source::{
        row_events,
        scan,
        scan_at,
        RangeRead,
        RangeSource,
    },
    store::{
        KeyRange,
        Version,
    },
};

pub const MAX_SEARCH_PARTITIONS: usize = 32;
pub const MAX_SEARCH_OFFSET: usize = 4096;
pub const MAX_SEARCH_LIMIT: usize = 128;
pub const MAX_SEARCH_QUERY: usize = 4096;
pub const MAX_STAGED_DOCUMENTS: usize = 64;
/// More candidates than this refuse the read (`rtdb.owned/search-candidate-limit`, ACC:982).
pub const MAX_SEARCH_CANDIDATES: usize = 100_000;

const DOCUMENT: u8 = 0x00;
const POSTING: u8 = 0x01;

/// The cell encodings a search column holds, as the Arrow adapter writes
/// them (`store_flight::owned_engine::value`): a null, a Utf8 string
/// (`0x0d ‖ escaped bytes ‖ 00 00`), or the one-member shared-text struct
/// `Struct{v2_value: Utf8?}` (`0x13 ‖ (0x00 | 0x01 ‖ cell)`).
const NULL_CELL: u8 = 0x00;
const UTF8_CELL: u8 = 0x0d;
const STRUCT_CELL: u8 = 0x13;

/// The text of a search column cell; `None` for a null (not a document).
pub fn cell_text(cell: &[u8]) -> anyhow::Result<Option<String>> {
    let column = "rtdb.owned/search-column: a search column holds Utf8 or shared text";
    match cell.split_first() {
        Some((&NULL_CELL, [])) => Ok(None),
        Some((&UTF8_CELL, rest)) => {
            let mut out = Vec::with_capacity(rest.len());
            let mut bytes = rest.iter();
            loop {
                match bytes.next().context(column)? {
                    0 => match bytes.next().context(column)? {
                        0 => break,
                        0xff => out.push(0),
                        _ => anyhow::bail!(column),
                    },
                    &byte => out.push(byte),
                }
            }
            ensure!(bytes.next().is_none(), column);
            Ok(Some(String::from_utf8(out).context(column)?))
        },
        Some((&STRUCT_CELL, [0])) => Ok(None),
        Some((&STRUCT_CELL, [1, inner @ ..])) if inner.first() != Some(&STRUCT_CELL) => cell_text(inner),
        _ => anyhow::bail!(column),
    }
}

/// The analyzer a search path names (`rtdb.search/unknown-analyzer`).
pub fn analyzer(name: &str) -> anyhow::Result<Analyzer> {
    Analyzer::parse(name).with_context(|| format!("rtdb.search/unknown-analyzer: {name}"))
}

/// Whether a relation's search paths keep corpus counters: its rows are
/// valid from their commit onwards, so its present corpus changes only at
/// commits.
pub fn counts(declaration: &Declaration) -> bool {
    declaration.validity == Validity::Acceptance && declaration.lifetime_micros.is_none()
}

/// One indexed document: its exact token count and term frequencies.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Document {
    pub tokens: u64,
    pub terms: BTreeMap<String, u32>,
}

fn document(analyzer: Analyzer, column: &str, image: &Image) -> anyhow::Result<Option<Document>> {
    let Some(cell) = image.get(column) else { return Ok(None) };
    let Some(text) = cell_text(cell)? else { return Ok(None) };
    let tokens = analyzer.tokens(&text);
    let mut terms = BTreeMap::<String, u32>::new();
    for token in &tokens {
        let count = terms.entry(token.clone()).or_default();
        *count = count.saturating_add(1);
    }
    Ok(Some(Document { tokens: tokens.len() as u64, terms }))
}

/// The entries one image writes into a search path over `column`.
pub fn entries(analyzer: Analyzer, column: &str, image: &Image) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let Some(document) = document(analyzer, column, image)? else { return Ok(Vec::new()) };
    let tokens = u32::try_from(document.tokens).unwrap_or(u32::MAX);
    let mut value = tokens.to_be_bytes().to_vec();
    value.extend_from_slice(&stored_length(tokens as usize).to_be_bytes());
    let mut out = vec![(vec![DOCUMENT], value)];
    for (term, frequency) in document.terms {
        out.push(([&[POSTING][..], term.as_bytes()].concat(), frequency.to_be_bytes().to_vec()));
    }
    Ok(out)
}

/// The document a row's present winner holds at `valid` (every event
/// applies: the caller passes a commit's planned events).
pub fn present_document(analyzer: Analyzer, column: &str, events: &[Event], valid: i64) -> anyhow::Result<Option<Document>> {
    let all = Cut { system: i64::MAX, tx: i64::MAX, mask: None };
    match event::point(events, &all, valid).present().and_then(|event| event.image.as_ref()) {
        None => Ok(None),
        Some(image) => document(analyzer, column, image),
    }
}

/// The token count of the document an image holds, if any.
pub fn counted_tokens(analyzer: Analyzer, column: &str, image: Option<&Image>) -> anyhow::Result<Option<u64>> {
    Ok(match image {
        None => None,
        Some(image) => document(analyzer, column, image)?.map(|document| document.tokens),
    })
}

/// The document and token totals of a counted search path.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counter {
    pub documents: i128,
    pub tokens: i128,
}

impl Counter {
    pub fn add(&mut self, document: &Document, sign: i128) {
        self.documents += sign;
        self.tokens += sign * i128::from(document.tokens);
    }

    pub fn encode(self) -> Vec<u8> {
        let documents = u64::try_from(self.documents.max(0)).unwrap_or(u64::MAX);
        let tokens = u64::try_from(self.tokens.max(0)).unwrap_or(u64::MAX);
        [documents.to_be_bytes(), tokens.to_be_bytes()].concat()
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        ensure!(bytes.len() == 16, "Invalid owned search counters");
        Ok(Self {
            documents: i128::from(u64::from_be_bytes(bytes[..8].try_into()?)),
            tokens: i128::from(u64::from_be_bytes(bytes[8..].try_into()?)),
        })
    }

    /// A world level's counters are a signed delta over what it inherits.
    pub fn encode_delta(self) -> anyhow::Result<Vec<u8>> {
        let documents = i64::try_from(self.documents).context("The owned search delta overflowed")?;
        let tokens = i64::try_from(self.tokens).context("The owned search delta overflowed")?;
        Ok([documents.to_be_bytes(), tokens.to_be_bytes()].concat())
    }

    pub fn decode_delta(bytes: &[u8]) -> anyhow::Result<Self> {
        ensure!(bytes.len() == 16, "Invalid owned search counter delta");
        Ok(Self {
            documents: i128::from(i64::from_be_bytes(bytes[..8].try_into()?)),
            tokens: i128::from(i64::from_be_bytes(bytes[8..].try_into()?)),
        })
    }

    pub fn sum(self, other: Self) -> Self {
        Self { documents: self.documents + other.documents, tokens: self.tokens + other.tokens }
    }
}

/// The erasure adjustment of one erased document (coordinator, store-cut-perf
/// Q1): no read at any cut may count an erased document, so the erasing
/// append records, under the path's search statistics, which counter cuts
/// counted it and with how many tokens. A counted read at cut `c` subtracts
/// every adjustment erased after `c` whose document `c` counted.
///
/// `owned.search-erasures ‖ relation ‖ path ‖ ordered(erase tx) ‖ id` →
/// `u32 id length ‖ id ‖ u32 n ‖ n × (tx ‖ present u8 ‖ tokens u64)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Erasure {
    pub id: RowId,
    /// The counted document from each transaction on: `(tx, tokens)`.
    pub history: Vec<(i64, Option<u64>)>,
}

impl Erasure {
    /// The tokens of the document the counters at `tx` counted, if any.
    pub fn counted_at(&self, tx: i64) -> Option<u64> {
        self.history.iter().rev().find(|(at, _)| *at <= tx).and_then(|(_, tokens)| *tokens)
    }

    pub fn encode(&self) -> Vec<u8> {
        let id = self.id.encode();
        let mut out = (id.len() as u32).to_be_bytes().to_vec();
        out.extend_from_slice(&id);
        out.extend_from_slice(&(self.history.len() as u32).to_be_bytes());
        for (tx, tokens) in &self.history {
            out.extend_from_slice(&tx.to_be_bytes());
            out.push(u8::from(tokens.is_some()));
            out.extend_from_slice(&tokens.unwrap_or(0).to_be_bytes());
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let invalid = || anyhow::anyhow!("Invalid owned search erasure");
        let take = |bytes: &mut &[u8], length: usize| -> anyhow::Result<Vec<u8>> {
            let (head, tail) = bytes.split_at_checked(length).ok_or_else(invalid)?;
            *bytes = tail;
            Ok(head.to_vec())
        };
        let mut rest = bytes;
        let length = u32::from_be_bytes(take(&mut rest, 4)?.try_into().map_err(|_| invalid())?) as usize;
        let id = RowId::decode(&take(&mut rest, length)?)?;
        let count = u32::from_be_bytes(take(&mut rest, 4)?.try_into().map_err(|_| invalid())?) as usize;
        let mut history = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            let tx = i64::from_be_bytes(take(&mut rest, 8)?.try_into().map_err(|_| invalid())?);
            let present = take(&mut rest, 1)?[0] == 1;
            let tokens = u64::from_be_bytes(take(&mut rest, 8)?.try_into().map_err(|_| invalid())?);
            history.push((tx, present.then_some(tokens)));
        }
        ensure!(rest.is_empty(), "Invalid owned search erasure");
        Ok(Self { id, history })
    }

    /// The counted history of a row's events (in precedence order) before
    /// their erase: the document present after each transaction.
    pub fn of(analyzer: Analyzer, column: &str, events: &[Event]) -> anyhow::Result<Self> {
        let mut history: Vec<(i64, Option<u64>)> = Vec::new();
        for (index, event) in events.iter().enumerate() {
            let document = present_document(analyzer, column, &events[..=index], event.precedence.system_from)?.map(|document| document.tokens);
            match history.last_mut() {
                Some((tx, tokens)) if *tx == event.precedence.tx => *tokens = document,
                _ => history.push((event.precedence.tx, document)),
            }
        }
        Ok(Self { id: RowId::Binary(Vec::new()), history })
    }
}

fn erasure_prefix(place: Place, relation: &str, path: &str) -> Vec<u8> {
    let segments = [segment::segment(b"owned.search-erasures"), segment::segment(relation.as_bytes()), segment::segment(path.as_bytes())].concat();
    layout::stored(place, Family::Persistence as u8, &segments)
}

/// Every erasure adjustment of one search path.
pub fn erasures_all(place: Place, relation: &str, path: &str) -> KeyRange {
    KeyRange::prefix(&erasure_prefix(place, relation, path))
}

/// The key of one erasure adjustment.
pub fn erasure_key(place: Place, relation: &str, path: &str, tx: i64, id: &RowId) -> Vec<u8> {
    [erasure_prefix(place, relation, path), segment::segment(&event::ordered_i64(tx)), segment::segment(&id.encode())].concat()
}

/// The adjustments of erasures after transaction `tx`.
fn erasures_range_after(place: Place, relation: &str, path: &str, tx: i64) -> KeyRange {
    let start = [erasure_prefix(place, relation, path), segment::segment(&event::ordered_i64(tx.saturating_add(1)))].concat();
    KeyRange { start, end: erasures_all(place, relation, path).end }
}

/// The precedence of a path entry key.
fn entry_precedence(place: Place, relation: &str, path: &str, stored: &[u8]) -> anyhow::Result<event::Precedence> {
    let base = keys::path_all(place, relation, path).start;
    ensure!(stored.starts_with(&base), "The key is not an entry of the path");
    let mut probe = vec![Family::Index as u8];
    probe.extend_from_slice(&stored[base.len()..]);
    let (values, _) = segment::decode_prefix(&probe, 3).context("Invalid owned path entry")?;
    event::Precedence::decode(&values[2])
}

/// The counters of a counted search path at `place` as of transaction
/// `tx`: the Root's totals, or a world level's signed delta.
pub async fn search_counter<S: RangeSource + ?Sized>(source: &S, place: Place, relation: &str, path: &str, tx: i64, at: Version) -> anyhow::Result<Counter> {
    let range = KeyRange { start: keys::search_stats(place, relation, path, tx), end: keys::search_stats_all(place, relation, path).end };
    let page = source.read(RangeRead { ranges: vec![range], at: Some(at), after: None, limit: 1 }).await?;
    let entry = page.pages.into_iter().next().and_then(|page| page.entries.into_iter().next());
    match entry.and_then(|entry| entry.value) {
        None => Ok(Counter::default()),
        Some(value) if place.world == ROOT_WORLD => Counter::decode(&value),
        Some(value) => Counter::decode_delta(&value),
    }
}

/// The adjustments of documents erased after `tx` at `place`, at the
/// latest version: an erase reaches every older hold, like the purge floor.
pub async fn erasures_after<S: RangeSource + ?Sized>(source: &S, place: Place, relation: &str, path: &str, tx: i64) -> anyhow::Result<Vec<Erasure>> {
    let (_, entries) = scan_at(source, erasures_range_after(place, relation, path, tx), None).await?;
    entries.iter().map(|entry| Erasure::decode(entry.value.as_deref().unwrap_or_default())).collect()
}

/// The Root totals of a counted path at `cut`: the counters there, less
/// every document erased since that they counted.
async fn root_totals<S: RangeSource + ?Sized>(source: &S, place: Place, relation: &str, path: &str, tx: i64, at: Version) -> anyhow::Result<Counter> {
    let mut counter = search_counter(source, place, relation, path, tx, at).await?;
    for erasure in erasures_after(source, place, relation, path, tx).await? {
        if let Some(tokens) = erasure.counted_at(tx) {
            counter.documents -= 1;
            counter.tokens -= i128::from(tokens);
        }
    }
    Ok(counter)
}

/// The live postings of one run at a Root frame: for each id whose latest
/// entry at or below the cut enters, its entry value. Ids the run cannot
/// settle are returned apart for the caller to resolve: an id with entries
/// but none at or below the cut, and an id with an entry erased after the
/// hold (its floor refuses the cut). `nominated` counts each distinct id,
/// as a resolving read would.
async fn live_run<S: RangeSource + ?Sized>(
    source: &S,
    frame: &Frame,
    relation: &str,
    path: &str,
    values: &[u8],
    nominated: &mut usize,
) -> anyhow::Result<(BTreeMap<RowId, Vec<u8>>, BTreeSet<RowId>)> {
    let place = frame.place;
    let range = keys::path_run(place, relation, path, values);
    let (latest_version, latest) = scan_at(source, range.clone(), None).await?;
    let (entries, survivors) = if latest_version == frame.version {
        (latest, None)
    } else {
        let survivors = latest.into_iter().map(|entry| entry.key).collect::<BTreeSet<_>>();
        (scan(source, range, frame.version).await?, Some(survivors))
    };
    let mut live = BTreeMap::new();
    let mut unsettled = BTreeSet::new();
    // Entries of one id are contiguous, ascending by precedence.
    let mut current: Option<(RowId, Option<Vec<u8>>, bool)> = None;
    fn settle(state: Option<(RowId, Option<Vec<u8>>, bool)>, live: &mut BTreeMap<RowId, Vec<u8>>, unsettled: &mut BTreeSet<RowId>) {
        let Some((id, latest, settled)) = state else { return };
        match latest {
            _ if !settled => {
                unsettled.insert(id);
            },
            None => {
                unsettled.insert(id);
            },
            Some(value) if !value.is_empty() => {
                live.insert(id, value);
            },
            Some(_) => {},
        }
    }
    for entry in entries {
        let (_, id) = keys::path_entry_parts(place, relation, path, &entry.key)?;
        let precedence = entry_precedence(place, relation, path, &entry.key)?;
        let erased = survivors.as_ref().is_some_and(|survivors| !survivors.contains(&entry.key));
        if current.as_ref().is_none_or(|(at, ..)| *at != id) {
            settle(current.take(), &mut live, &mut unsettled);
            *nominated += 1;
            ensure!(*nominated <= MAX_SEARCH_CANDIDATES, "rtdb.owned/search-candidate-limit: the query nominates more than {MAX_SEARCH_CANDIDATES} documents");
            current = Some((id, None, true));
        }
        let (_, latest, settled) = current.as_mut().expect("Set above");
        if erased {
            *settled = false;
        }
        if precedence.system_from <= frame.cut.system && precedence.tx <= frame.cut.tx {
            *latest = Some(entry.value.unwrap_or_default());
        }
    }
    settle(current.take(), &mut live, &mut unsettled);
    Ok((live, unsettled))
}

/// The totals of a counted path over world levels: the Root totals at the
/// parent cut (less erased documents no nearer level masks) plus each
/// nearer level's delta at its cut. `None` when a level is masked or the
/// frame's levels were not stacked at the hold, so the caller recounts.
async fn world_totals<S: RangeSource + ?Sized>(source: &S, frame: &Frame, relation: &str, path: &str) -> anyhow::Result<Option<Counter>> {
    let world = frame.world.as_ref().context("A world total reads a world frame")?;
    let levels = &world.levels;
    if !world.stacked {
        return Ok(None);
    }
    let version = frame.version;
    let mut counter = Counter::default();
    for (index, level) in levels.iter().enumerate() {
        if level.cut.mask.is_some() {
            return Ok(None);
        }
        if level.world == ROOT_WORLD {
            ensure!(index + 1 == levels.len(), "A world's Root level is its last");
            counter = counter.sum(search_counter(source, frame.place, relation, path, level.cut.tx, version).await?);
            'erasures: for erasure in erasures_after(source, frame.place, relation, path, level.cut.tx).await? {
                let Some(tokens) = erasure.counted_at(level.cut.tx) else { continue };
                // A nearer level that claims the row took its delta over
                // this document: the world never counts it twice.
                for nearer in &levels[..index] {
                    let row = keys::row(frame.world_place(nearer.world), relation, &erasure.id);
                    if row_events(source, &row, version).await?.iter().any(|event| nearer.cut.applies(event)) {
                        continue 'erasures;
                    }
                }
                counter.documents -= 1;
                counter.tokens -= i128::from(tokens);
            }
            continue;
        }
        counter = counter.sum(search_counter(source, frame.world_place(level.world), relation, path, level.cut.tx, version).await?);
    }
    Ok(Some(counter))
}

/// The document a row holds at the frame's cut and `valid`.
pub async fn held_document<S: RangeSource + ?Sized>(source: &S, frame: &Frame, relation: &str, id: &RowId, column: &str, valid: i64, reads: &mut Vec<KeyRange>) -> anyhow::Result<Option<String>> {
    match frame.image(source, relation, id, valid, reads).await?.as_ref().and_then(|image| image.get(column)) {
        None => Ok(None),
        Some(cell) => cell_text(cell),
    }
}

/// Every distinct id with an entry at `values` of the path, in every place
/// the frame reads.
pub async fn run_ids<S: RangeSource + ?Sized>(source: &S, frame: &Frame, relation: &str, path: &str, values: &[u8], bound: &mut usize) -> anyhow::Result<BTreeSet<RowId>> {
    let mut ids = BTreeSet::new();
    for place in frame.places() {
        for entry in scan(source, keys::path_run(place, relation, path, values), frame.version).await? {
            if ids.insert(keys::path_entry_parts(place, relation, path, &entry.key)?.1) {
                *bound += 1;
                ensure!(*bound <= MAX_SEARCH_CANDIDATES, "rtdb.owned/search-candidate-limit: the query nominates more than {MAX_SEARCH_CANDIDATES} documents");
            }
        }
    }
    Ok(ids)
}

/// One partition of a text read: a relation's search path in one frame.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TextPartition {
    pub frame: usize,
    pub relation: String,
    pub path: String,
    /// The partition's position in the caller's request: hits and read sets
    /// name it, so a read split between the store and Bijection merges back
    /// into the caller's order.
    pub ordinal: usize,
}

/// One staged document of a search overlay (RTDB `HeldTextCorpus::shadow`):
/// it replaces the stored document of `id` in the partition at position
/// `partition` of the request, or, with no text, removes it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Staged {
    pub partition: usize,
    pub id: RowId,
    pub text: Option<String>,
}

/// A ranked text read (`SearchHeld.v7`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TextQuery {
    pub partitions: Vec<TextPartition>,
    pub query: String,
    pub offset: usize,
    pub limit: usize,
    /// The valid instant; `None` is each frame's present.
    pub valid: Option<i64>,
    /// Sorted by `(partition, id)`, distinct, at most 64; current validity only.
    pub staged: Vec<Staged>,
}

/// One scored match.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TextHit {
    pub id: RowId,
    #[serde(with = "super::wire::f64_bits")]
    pub score: f64,
    /// The partition's ordinal.
    pub partition: usize,
}

/// One match before scoring: its term frequencies and stored length.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TextCandidate {
    pub id: RowId,
    pub partition: usize,
    pub frequencies: Vec<u32>,
    pub length: u32,
}

/// What a text read gathered over its partitions: their corpus
/// contribution (documents, tokens, and each query term's document
/// frequency), every match's features and each partition's read set.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TextFeatures {
    pub analyzer: String,
    pub present_micros: i64,
    pub valid_micros: i64,
    pub documents: i128,
    pub tokens: i128,
    pub frequencies: Vec<i128>,
    pub candidates: Vec<TextCandidate>,
    /// `(ordinal, read set)` per partition.
    pub reads: Vec<(usize, Vec<KeyRange>)>,
}

/// A text window: score descending, then id, then partition.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TextPage {
    pub present_micros: i64,
    pub valid_micros: i64,
    pub hits: Vec<TextHit>,
    /// Every match of the query.
    pub total: usize,
    /// The statistics the scores are relative to: every partition, staged
    /// documents applied; frequencies of the query's terms.
    pub documents: u64,
    pub tokens: u64,
    pub frequencies: BTreeMap<String, u64>,
    /// The read set of each partition, by ordinal.
    pub reads: Vec<Vec<KeyRange>>,
}

impl TextPage {
    /// The corpus as `kernel_text` states it.
    pub fn corpus(&self) -> Corpus {
        Corpus { documents: self.documents, tokens: self.tokens, frequencies: self.frequencies.clone() }
    }
}

/// Check a text read's bounds (`rtdb.owned/search-limit`).
pub fn check(query: &TextQuery, frames: &[Frame]) -> anyhow::Result<()> {
    ensure!(
        (1..=MAX_SEARCH_PARTITIONS).contains(&query.partitions.len())
            && query.query.len() <= MAX_SEARCH_QUERY
            && query.offset <= MAX_SEARCH_OFFSET
            && (1..=MAX_SEARCH_LIMIT).contains(&query.limit),
        "rtdb.owned/search-limit: a search names 1 to {MAX_SEARCH_PARTITIONS} partitions, a query of at most {MAX_SEARCH_QUERY} bytes, an offset of at most {MAX_SEARCH_OFFSET} and a limit of 1 to {MAX_SEARCH_LIMIT}"
    );
    ensure!(query.staged.is_empty() || query.valid.is_none(), "A staged search requires current validity");
    ensure!(
        query.staged.len() <= MAX_STAGED_DOCUMENTS
            && query.staged.windows(2).all(|pair| (pair[0].partition, &pair[0].id) < (pair[1].partition, &pair[1].id))
            && query.staged.iter().all(|staged| staged.partition < query.partitions.len()),
        "A staged search holds at most {MAX_STAGED_DOCUMENTS} distinct documents of its partitions, in order"
    );
    ensure!(query.partitions.iter().all(|partition| partition.frame < frames.len()), "A search partition names one of the request's frames");
    let distinct = query.partitions.iter().map(|partition| (frames[partition.frame].place, &partition.relation, &partition.path)).collect::<BTreeSet<_>>();
    ensure!(distinct.len() == query.partitions.len(), "rtdb.owned/search-relations: a search names each partition once");
    Ok(())
}

/// Gather a text read over its partitions.
pub async fn gather<S: RangeSource + ?Sized>(source: &S, frames: &[Frame], request: &TextQuery) -> anyhow::Result<TextFeatures> {
    check(request, frames)?;
    let first = &frames[request.partitions[0].frame];
    let mut routes = Vec::with_capacity(request.partitions.len());
    let mut chosen: Option<(Analyzer, String)> = None;
    for partition in &request.partitions {
        let frame = &frames[partition.frame];
        let (relation, path) = (&partition.relation, &partition.path);
        frame.admit(relation)?;
        let valid = request.valid.unwrap_or(frame.present_micros);
        let declaration = catalog::declaration(source, frame.place, relation, Some(frame.version)).await?.context("rtdb.owned/unknown-relation")?;
        ensure!(
            declaration.lifetime_micros.is_none() || valid == frame.present_micros,
            "rtdb.owned/lifetime-uncovered: a lifetime relation is read at the held present"
        );
        let installed = catalog::path(source, frame.place, relation, path, Some(frame.version)).await?.context("rtdb.owned/search-path: the partition has no search path")?;
        let PathKind::Search { analyzer: name, analyzer_version, counted } = &installed.kind else {
            anyhow::bail!("rtdb.owned/search-path: {relation}.{path} is not a search path");
        };
        ensure!(*analyzer_version == kernel_text::ANALYZER_VERSION, "rtdb.owned/search-analyzer: {relation}.{path} was indexed under another analyzer version");
        let found = analyzer(name)?;
        ensure!(chosen.as_ref().is_none_or(|(chosen, _)| *chosen == found), "rtdb.owned/search-analyzers: the partitions use different analyzers");
        chosen = Some((found, name.clone()));
        let counted = *counted && counts(&declaration) && request.valid.is_none() && frame.cut.mask.is_none();
        routes.push((frame, relation.as_str(), path.as_str(), installed.columns[0].clone(), counted, valid));
    }
    let (analyzer, analyzer_name) = chosen.context("A search names a partition")?;
    let query = Query::analyzed(analyzer, &request.query);
    let terms = query.terms().to_vec();
    let mut frequencies = vec![0i128; terms.len()];
    let mut counter = Counter::default();
    let mut candidates: Vec<TextCandidate> = Vec::new();
    let mut reads = Vec::with_capacity(routes.len());
    let mut nominated = 0usize;
    for (position, (frame, relation, path, column, counted, valid)) in routes.iter().enumerate() {
        let (frame, valid) = (*frame, *valid);
        let ordinal = request.partitions[position].ordinal;
        let mut partition_reads = match &frame.world {
            None => vec![keys::path_all(frame.place, relation, path), keys::search_stats_all(frame.place, relation, path), erasures_all(frame.place, relation, path)],
            Some(world) => frame.world_reads(world, relation, path),
        };
        let staged = request.staged.iter().filter(|staged| staged.partition == position).collect::<Vec<_>>();
        // Corpus totals at the cut: counters where they apply, else a
        // recount of the document run.
        let totals = match &frame.world {
            _ if !*counted => None,
            None => Some(root_totals(source, frame.place, relation, path, frame.cut.tx, frame.version).await?),
            Some(world) => {
                let totals = world_totals(source, frame, relation, path).await?;
                if totals.is_some() {
                    if world.own && world.levels.first().is_some_and(|level| level.world == world.world) {
                        partition_reads.push(keys::search_stats_all(frame.world_place(world.world), relation, path));
                    }
                    partition_reads.push(erasures_all(frame.place, relation, path));
                }
                totals
            },
        };
        if let Some(found) = totals {
            counter = counter.sum(found);
        } else {
            for id in run_ids(source, frame, relation, path, &[DOCUMENT], &mut nominated).await? {
                if let Some(text) = held_document(source, frame, relation, &id, column, valid, &mut partition_reads).await? {
                    counter.documents += 1;
                    counter.tokens += analyzer.tokens(&text).len() as i128;
                }
            }
        }
        // A staged document replaces the stored one in the totals; its
        // document frequencies never count, as the posting pass below skips
        // shadowed ids.
        let mut shadowed = BTreeSet::new();
        for staged in &staged {
            if let Some(text) = held_document(source, frame, relation, &staged.id, column, valid, &mut partition_reads).await? {
                counter.documents -= 1;
                counter.tokens -= analyzer.tokens(&text).len() as i128;
            }
            shadowed.insert(staged.id.clone());
            if let Some(text) = &staged.text {
                let (count, found, matched) = query.text_terms(text, true);
                counter.documents += 1;
                counter.tokens += count.len() as i128;
                for (total, found) in frequencies.iter_mut().zip(&found) {
                    *total += i128::from(*found > 0);
                }
                if matched {
                    candidates.push(TextCandidate { id: staged.id.clone(), partition: ordinal, length: stored_length(count.len()), frequencies: found });
                }
            }
        }
        // A counted Root path takes document frequencies from the live
        // postings at the cut and resolves only the ids live in every term's
        // run, plus those the runs cannot settle.
        if *counted && frame.world.is_none() {
            let mut runs = Vec::with_capacity(terms.len());
            let mut unsettled = BTreeSet::new();
            for term in &terms {
                let values = [&[POSTING][..], term.as_bytes()].concat();
                let (live, open) = live_run(source, frame, relation, path, &values, &mut nominated).await?;
                unsettled.extend(open);
                runs.push(live);
            }
            let settled = |id: &RowId| !shadowed.contains(id) && !unsettled.contains(id);
            for (total, live) in frequencies.iter_mut().zip(&runs) {
                *total += live.keys().filter(|id| settled(id)).count() as i128;
            }
            let mut resolve = runs.first().map_or_else(BTreeSet::new, |first| {
                first.keys().filter(|id| settled(id) && runs[1..].iter().all(|live| live.contains_key(*id))).cloned().collect::<BTreeSet<_>>()
            });
            // An unsettled id resolves as a nominated one did: its floor
            // refuses, or its text counts.
            let unsettled = unsettled.into_iter().filter(|id| !shadowed.contains(id)).collect::<Vec<_>>();
            resolve.extend(unsettled.iter().cloned());
            for id in resolve {
                let Some(text) = held_document(source, frame, relation, &id, column, valid, &mut partition_reads).await? else { continue };
                let (count, found, matched) = query.text_terms(&text, true);
                if unsettled.contains(&id) {
                    for (total, found) in frequencies.iter_mut().zip(&found) {
                        *total += i128::from(*found > 0);
                    }
                }
                if matched {
                    candidates.push(TextCandidate { id, partition: ordinal, length: stored_length(count.len()), frequencies: found });
                }
            }
            reads.push((ordinal, partition_reads));
            continue;
        }
        // Postings nominate; each candidate is resolved and re-analysed.
        let mut ids = BTreeSet::new();
        for term in &terms {
            let values = [&[POSTING][..], term.as_bytes()].concat();
            ids.extend(run_ids(source, frame, relation, path, &values, &mut nominated).await?);
        }
        for id in ids {
            if shadowed.contains(&id) {
                continue;
            }
            let Some(text) = held_document(source, frame, relation, &id, column, valid, &mut partition_reads).await? else { continue };
            let (count, found, matched) = query.text_terms(&text, true);
            for (total, found) in frequencies.iter_mut().zip(&found) {
                *total += i128::from(*found > 0);
            }
            if matched {
                candidates.push(TextCandidate { id, partition: ordinal, length: stored_length(count.len()), frequencies: found });
            }
        }
        reads.push((ordinal, partition_reads));
    }
    Ok(TextFeatures {
        analyzer: analyzer_name,
        present_micros: first.page_present(),
        valid_micros: request.valid.unwrap_or(first.present_micros),
        documents: counter.documents,
        tokens: counter.tokens,
        frequencies,
        candidates,
        reads,
    })
}

/// Score and window what one or more parts of a text read gathered: the
/// corpus is their sum, the order `(score desc, id, partition)`.
pub fn finish(request: &TextQuery, parts: Vec<TextFeatures>) -> anyhow::Result<TextPage> {
    let first = parts.first().context("A text read gathers at least one part")?;
    ensure!(parts.iter().all(|part| part.analyzer == first.analyzer), "rtdb.owned/search-analyzers: the partitions use different analyzers");
    let analyzer = analyzer(&first.analyzer)?;
    let query = Query::analyzed(analyzer, &request.query);
    let terms = query.terms().to_vec();
    let (present_micros, valid_micros) = (first.present_micros, first.valid_micros);
    let (mut documents, mut tokens, mut frequencies) = (0i128, 0i128, vec![0i128; terms.len()]);
    let mut candidates = Vec::new();
    let mut reads = Vec::new();
    for part in parts {
        ensure!(part.frequencies.len() == terms.len(), "A gathered text part counts every query term");
        documents += part.documents;
        tokens += part.tokens;
        for (total, found) in frequencies.iter_mut().zip(&part.frequencies) {
            *total += found;
        }
        candidates.extend(part.candidates);
        reads.extend(part.reads);
    }
    reads.sort_by_key(|(ordinal, _)| *ordinal);
    let documents = u64::try_from(documents).context("The owned search corpus went negative")?;
    let tokens = u64::try_from(tokens).context("The owned search corpus went negative")?;
    let frequencies = frequencies.into_iter().map(u64::try_from).collect::<Result<Vec<_>, _>>().context("The owned search corpus went negative")?;
    let corpus = terms.iter().cloned().zip(frequencies.iter().copied()).collect();
    let scorer = Scorer::from_frequencies(query, documents, tokens, &frequencies);
    let mut scored = candidates
        .into_iter()
        .map(|candidate| TextHit { score: scorer.score(&candidate.frequencies, candidate.length), id: candidate.id, partition: candidate.partition })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| right.score.total_cmp(&left.score).then_with(|| left.id.cmp(&right.id)).then_with(|| left.partition.cmp(&right.partition)));
    let total = scored.len();
    let hits = scored.into_iter().skip(request.offset).take(request.limit).collect();
    Ok(TextPage { present_micros, valid_micros, hits, total, documents, tokens, frequencies: corpus, reads: reads.into_iter().map(|(_, reads)| reads).collect() })
}
