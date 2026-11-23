//! A search file: one single-segment tantivy index over a trie or its partition group, bundled into one object.
//! A document is a text run: every version of one entity whose indexed values are the same, with the rectangles of valid time × system time where one of them is visible.
//! Tokens come from `rtdb-text`, so the index and the residual `MATCH` agree.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rtdb_text::{Corpus, Query, RankScorer, Scorer};
use rtdb_types::{Anomaly, EntityId, EventHeader, EventOp, Iid, Interval, Result, TemporalBounds, Timestamp};
use tantivy::collector::DocSetCollector;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, FileSlice, OwnedBytes, RamDirectory, WatchCallback, WatchHandle, WritePtr};
use tantivy::postings::SegmentPostings;
use tantivy::query::{BooleanQuery, Occur, TermQuery};
use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::store::Compressor;
use tantivy::tokenizer::{PreTokenizedString, Token};
use tantivy::{Directory, HasLen, Index, IndexSettings, TantivyDocument, Term};

use crate::member_bounds::{MemberStart, document_member};
use crate::rank_windows::{document_range, extend_impact, window_of};
use crate::rectangle::merge_adjacent_valid_time;

#[cfg(feature = "experiments")]
pub(crate) mod experiment;

const MAGIC: &[u8; 8] = b"RTSRCH03";
/// Identifies the bundle bytes produced by the builder, including visibility encoding.
pub const FORMAT_VERSION: i32 = 11;
const WRITER_MEMORY: usize = 32 << 20;
/// The bundle entry holding what tantivy's segment does not: rows, runs, rectangles, nulls, every row's key, and term impacts.
const SIDECAR: &str = "rtdb.sidecar";
/// A term in at least this many documents has its window bounds stored; a rarer term's are computed from its postings when asked.
const STORED_IMPACTS: u32 = 1024;
/// A row's place in a column's runs where the row is visible nowhere, and where it is visible with the column null or absent.
const NO_DOC: u32 = u32::MAX;
const NULL_DOC: u32 = u32::MAX - 1;
/// A row's key in the sidecar: IID, system from, valid from, valid to, operation.
const KEY_BYTES: usize = 16 + 8 + 8 + 8 + 1;

/// One data-file row as the search file sees it: its key and the tokens of each indexed column.
/// Rows arrive as the data file holds them: IID ascending, then system time descending.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchRow {
    /// Full entity identity on every owned effect, including delete and erase.
    pub identity: Option<EntityId>,
    pub iid: [u8; 16],
    pub system_from: i64,
    pub valid_from: i64,
    pub valid_to: i64,
    pub op: EventOp,
    /// Parallel to the file's columns: `None` where the column is null or absent, and otherwise the tokens of its UTF-8 value, none for a value of another type.
    pub values: Vec<Option<Vec<String>>>,
}

/// Lifespans, in weeks, below which a document falls into each class but the last, which is unbounded.
const LIFESPAN_WEEKS: [i64; 5] = [1, 4, 16, 64, 256];
const WEEK: i64 = 7 * 86_400_000_000;

/// The bounds of one lifespan class's documents in a search file: where any of them can be visible, the latest any stops being, and the earliest any starts.
/// A document visible at a point of valid time × system time is born before both its coordinates' later one and dies after their earlier one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Section { pub class: u8, pub valid_from: i64, pub valid_to: i64, pub system_from: i64, pub system_to: i64, pub birth: i64, pub death: i64 }

impl Section {
    fn of(class: u8, rectangle: [i64; 4], ceiling: i64) -> Self {
        Self { class, valid_from: rectangle[0], valid_to: rectangle[1], system_from: rectangle[2], system_to: rectangle[3], birth: rectangle[0].max(rectangle[2]), death: rectangle[1].min(rectangle[3]).min(ceiling) }
    }
    fn widen(&mut self, other: &Self) {
        self.valid_from = self.valid_from.min(other.valid_from); self.valid_to = self.valid_to.max(other.valid_to);
        self.system_from = self.system_from.min(other.system_from); self.system_to = self.system_to.max(other.system_to);
        self.birth = self.birth.min(other.birth); self.death = self.death.max(other.death);
    }
    /// Whether any document of the section can be visible in `bounds`.
    pub fn meets(&self, bounds: TemporalBounds) -> bool {
        let (valid, system) = (bounds.valid_time, bounds.system_time);
        self.valid_from < valid.upper().0 && valid.lower().0 < self.valid_to && self.system_from < system.upper().0 && system.lower().0 < self.system_to
            && self.death > valid.lower().0.min(system.lower().0) && self.birth < valid.upper().0.max(system.upper().0)
    }
}

/// A trie's recency as an instant: no row of it is visible from then on.
pub fn ceiling_of(recency: Option<rtdb_types::CivilDate>) -> Option<i64> { recency.map(|date| date.epoch_day().saturating_mul(86_400_000_000)) }

/// Sections as an artefact record's stats hold them.
pub fn encode_sections(sections: &[Section]) -> Vec<u8> {
    let mut bytes = vec![sections.len() as u8];
    for section in sections {
        bytes.push(section.class);
        for value in [section.valid_from, section.valid_to, section.system_from, section.system_to, section.birth, section.death] { bytes.extend_from_slice(&value.to_le_bytes()); }
    }
    bytes
}

/// The sections a record's stats hold, or `None` for stats in no form this version wrote.
pub fn decode_sections(bytes: &[u8]) -> Option<Vec<Section>> {
    let (count, rest) = bytes.split_first()?;
    if rest.len() != *count as usize * 49 { return None; }
    Some(rest.chunks_exact(49).map(|chunk| {
        let at = |index: usize| i64::from_le_bytes(chunk[1 + index * 8..9 + index * 8].try_into().unwrap_or_default());
        Section { class: chunk[0], valid_from: at(0), valid_to: at(1), system_from: at(2), system_to: at(3), birth: at(4), death: at(5) }
    }).collect())
}

/// A document's lifespan class, from its rectangles clipped to the trie's ceiling.
fn lifespan(rectangles: &[[i64; 4]], ceiling: i64) -> (u8, Section) {
    let mut bounds = Section::of(0, rectangles[0], ceiling);
    for rectangle in &rectangles[1..] { bounds.widen(&Section::of(0, *rectangle, ceiling)); }
    let span = bounds.death.saturating_sub(bounds.birth);
    let class = if bounds.death == i64::MAX { LIFESPAN_WEEKS.len() } else { LIFESPAN_WEEKS.iter().position(|weeks| span < weeks.saturating_mul(WEEK)).unwrap_or(LIFESPAN_WEEKS.len()) };
    (class as u8, Section { class: class as u8, ..bounds })
}

/// A document visible somewhere in the bounds asked about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DocMatch { pub doc: u32, pub iid: [u8; 16], pub member: u32 }

/// An event of the file's data, as another file's documents are cut by it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyedEvent { pub system_from: i64, pub valid_from: i64, pub valid_to: i64, pub op: EventOp }

fn schema(columns: &[String]) -> (Schema, Vec<Field>) {
    let mut builder = Schema::builder();
    let text = TextOptions::default().set_indexing_options(TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqsAndPositions).set_fieldnorms(true));
    let fields = columns.iter().map(|column| builder.add_text_field(&format!("c_{column}"), text.clone())).collect();
    (builder.build(), fields)
}

fn fault(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.search/file", error.to_string()) }
fn corrupt(message: &str) -> Anomaly { Anomaly::fault("rtdb.search/corrupt-file", message.to_string()) }

fn op_code(op: EventOp) -> u8 { match op { EventOp::Put => 0, EventOp::Delete => 1, EventOp::Erase => 2 } }
fn op_of(code: u8) -> Result<EventOp> { match code { 0 => Ok(EventOp::Put), 1 => Ok(EventOp::Delete), 2 => Ok(EventOp::Erase), _ => Err(corrupt("Search file key has no such operation")) } }

/// Builds the bundled search file over rows in data-file order, as one page, in memory.
pub fn build_search_file(columns: &[String], rows: &[SearchRow], identity: &str) -> Result<Vec<u8>> {
    let mut bundle = Vec::new();
    write_search_file(columns, [Ok(rows.to_vec())], identity, None, RamDirectory::create(), &mut bundle)?;
    Ok(bundle)
}

/// What the writer accumulates beside tantivy's segment, to become the sidecar.
struct Accumulated { ceiling: i64, member_starts: Vec<MemberStart>, page_starts: Vec<u32>, row_docs: Vec<Vec<u32>>, column_docs: Vec<u32>, keys: Vec<u8>, row_identity_starts: Vec<u32>, row_identities: Vec<u8>, doc_iids: Vec<[u8; 16]>, token_lengths: Vec<u32>, identity_starts: Vec<u32>, identities: Vec<u8>, doc_classes: Vec<u8>, sections: BTreeMap<u8, Section>, rectangle_starts: Vec<u32>, rectangles: Vec<[i64; 4]> }

/// One entity's rows while they arrive: each row's position and header, and per column, its value as an index into the column's distinct values.
#[derive(Default)]
struct Entity { iid: Option<[u8; 16]>, identity: Option<EntityId>, rows: Vec<(u32, EventHeader)>, values: Vec<(Vec<Option<usize>>, Vec<Vec<String>>, std::collections::HashMap<Vec<String>, usize>)> }

impl Entity {
    fn new(columns: usize) -> Self { Self { values: (0..columns).map(|_| Default::default()).collect(), ..Self::default() } }
    fn push(&mut self, ordinal: u32, row: SearchRow) -> Result<()> {
        self.iid = Some(row.iid);
        if let Some(id) = row.identity {
            if id.iid().0 != row.iid || self.identity.as_ref().is_some_and(|previous| previous != &id) {
                return Err(fault("Owned search input has conflicting complete identities"));
            }
            self.identity = Some(id);
        }
        self.rows.push((ordinal, EventHeader { commit_position: 0, iid: Iid(row.iid), system_from: Timestamp(row.system_from), valid_time: Interval::from_micros(row.valid_from, row.valid_to)?, op: row.op }));
        for ((indices, distinct, seen), value) in self.values.iter_mut().zip(row.values) {
            indices.push(value.map(|tokens| *seen.entry(tokens.clone()).or_insert_with(|| { distinct.push(tokens); distinct.len() - 1 })));
        }
        Ok(())
    }
}

/// Builds the search file over a data file's pages of rows, in order, building its segment in `directory` and writing the bundle to `output`.
/// The bytes are a function of the rows, their pages, the columns, `identity` and `ceiling` alone, wherever the segment is built, so every builder of one job writes the same object.
pub fn write_search_file<D: Directory + Clone + 'static, W: std::io::Write>(columns: &[String], pages: impl IntoIterator<Item = Result<Vec<SearchRow>>>, identity: &str, ceiling: Option<i64>, directory: D, output: W) -> Result<Vec<Section>> {
    write_group_search_file(columns, [pages], identity, ceiling, directory, output)
}

/// Builds one search file over its members' data files in bucket order.
/// Siblings share the same recency ceiling, and the returned sections bound the whole file.
pub fn write_group_search_file<D, W, P>(columns: &[String], members: impl IntoIterator<Item = P>, identity: &str, ceiling: Option<i64>, directory: D, output: W) -> Result<Vec<Section>>
where
    D: Directory + Clone + 'static,
    W: std::io::Write,
    P: IntoIterator<Item = Result<Vec<SearchRow>>>,
{
    write_group(columns, members, identity, None, ceiling, directory, output)
}

/// Builds a catalog-published file bound to an ordered definition version.
pub(crate) fn write_versioned_group<D, W, P>(
    columns: &[String], members: impl IntoIterator<Item = P>, identity: &str, version: i64,
    ceiling: Option<i64>, directory: D, output: W,
) -> Result<Vec<Section>>
where
    D: Directory + Clone + 'static,
    W: std::io::Write,
    P: IntoIterator<Item = Result<Vec<SearchRow>>>,
{
    write_group(columns, members, identity, Some(version), ceiling, directory, output)
}

fn write_group<D, W, P>(
    columns: &[String], members: impl IntoIterator<Item = P>, identity: &str, version: Option<i64>,
    ceiling: Option<i64>, directory: D, output: W,
) -> Result<Vec<Section>>
where
    D: Directory + Clone + 'static,
    W: std::io::Write,
    P: IntoIterator<Item = Result<Vec<SearchRow>>>,
{
    let (schema, fields) = schema(columns);
    let settings = IndexSettings { docstore_compression: Compressor::None, ..IndexSettings::default() };
    let mut writer = Index::builder().schema(schema).settings(settings).single_segment_index_writer(directory.clone(), WRITER_MEMORY).map_err(fault)?;
    let mut sidecar = Accumulated { ceiling: ceiling.unwrap_or(i64::MAX), member_starts: Vec::new(), page_starts: Vec::new(), row_docs: vec![Vec::new(); columns.len()], column_docs: vec![0; columns.len()], keys: Vec::new(), row_identity_starts: vec![0], row_identities: Vec::new(), doc_iids: Vec::new(), token_lengths: Vec::new(), identity_starts: vec![0], identities: Vec::new(), doc_classes: Vec::new(), sections: BTreeMap::new(), rectangle_starts: vec![0], rectangles: Vec::new() };
    let mut rows = 0u32;
    let mut previous_member_iid = None;
    for pages in members {
        sidecar.member_starts.push(MemberStart {
            page: u32::try_from(sidecar.page_starts.len()).map_err(fault)?,
            doc: u32::try_from(sidecar.doc_iids.len()).map_err(fault)?,
        });
        let mut entities = BTreeMap::<Option<EntityId>, Entity>::new();
        let mut previous: Option<([u8; 16], i64)> = None;
        for page in pages {
            sidecar.page_starts.push(rows);
            for row in page? {
                if row.values.len() != columns.len() { return Err(fault("Search row values do not cover the file's columns")); }
                if previous.is_none() && previous_member_iid.is_some_and(|iid| iid >= row.iid) {
                    return Err(Anomaly::unsupported("rtdb.search/input-order", "Search file members do not hold disjoint IID ranges in order"));
                }
                if let Some((iid, system)) = previous {
                    if iid > row.iid || (iid == row.iid && system < row.system_from) {
                        return Err(Anomaly::unsupported("rtdb.search/input-order", "Search rows are not in IID and descending system-time order"));
                    }
                    if iid != row.iid {
                        for entity in std::mem::take(&mut entities).into_values() {
                            runs(&mut writer, &fields, &mut sidecar, entity)?;
                        }
                    }
                }
                previous = Some((row.iid, row.system_from));
                for docs in &mut sidecar.row_docs { docs.push(NO_DOC); }
                sidecar.keys.extend_from_slice(&row.iid);
                for value in [row.system_from, row.valid_from, row.valid_to] { sidecar.keys.extend_from_slice(&value.to_le_bytes()); }
                sidecar.keys.push(op_code(row.op));
                if let Some(id) = &row.identity { sidecar.row_identities.extend_from_slice(&id.handle_bytes()); }
                sidecar.row_identity_starts.push(u32::try_from(sidecar.row_identities.len()).map_err(fault)?);
                entities.entry(row.identity.clone()).or_insert_with(|| Entity::new(columns.len())).push(rows, row)?;
                rows = rows.checked_add(1).ok_or_else(|| fault("A search file holds at most u32::MAX rows"))?;
            }
        }
        previous_member_iid = previous.map(|(iid, _)| iid).or(previous_member_iid);
        for entity in entities.into_values() {
            runs(&mut writer, &fields, &mut sidecar, entity)?;
        }
    }
    if sidecar.member_starts.is_empty() { return Err(fault("A search file holds at least one member")); }
    writer.finalize().map_err(fault)?;
    let bytes = encode(&Index::open(directory.clone()).map_err(fault)?, &fields, rows, &sidecar, version, identity)?;
    directory.atomic_write(Path::new(SIDECAR), &bytes).map_err(fault)?;
    bundle(&directory, &segment_uuid(identity), output)?;
    Ok(sidecar.sections.into_values().collect())
}

fn coalesce(mut rectangles: Vec<[i64; 4]>) -> Vec<[i64; 4]> {
    rectangles.sort_unstable_by_key(|rectangle| (rectangle[2], rectangle[3], rectangle[0]));
    let mut merged = Vec::with_capacity(rectangles.len());
    for rectangle in rectangles {
        if let Some(last) = merged.last_mut()
            && let Some(joined) = merge_adjacent_valid_time(*last, rectangle)
        {
            *last = joined;
        } else {
            merged.push(rectangle);
        }
    }
    merged.sort_unstable_by_key(|rectangle| (rectangle[2], rectangle[0]));
    merged
}

/// One entity's rows become its text runs, per column: a document for each distinct value its visible versions hold there, with every rectangle where one of them is visible.
/// A visible version whose column is null or absent belongs to no run of that column, and is marked as null.
fn runs(writer: &mut tantivy::SingleSegmentIndexWriter, fields: &[Field], sidecar: &mut Accumulated, entity: Entity) -> Result<()> {
    let Some(iid) = entity.iid else { return Ok(()) };
    let mut visible: Vec<Vec<[i64; 4]>> = vec![Vec::new(); entity.rows.len()];
    for (index, rectangle) in rtdb_temporal::visibility(entity.rows.iter().enumerate().map(|(index, (_, header))| (header, index)))? {
        visible[index].push([rectangle.valid_time.lower().0, rectangle.valid_time.upper().0, rectangle.system_time.lower().0, rectangle.system_time.upper().0]);
    }
    for (column, ((indices, distinct, _), field)) in entity.values.into_iter().zip(fields).enumerate() {
        let mut members: Vec<Vec<usize>> = vec![Vec::new(); distinct.len()];
        for (index, value) in indices.iter().enumerate() {
            if visible[index].is_empty() { continue; }
            match value { Some(value) => members[*value].push(index), None => sidecar.row_docs[column][entity.rows[index].0 as usize] = NULL_DOC }
        }
        for (tokens, members) in distinct.into_iter().zip(members) {
            if members.is_empty() { continue; }
            let doc = u32::try_from(sidecar.doc_iids.len()).map_err(fault)?;
            sidecar.token_lengths.push(u32::try_from(tokens.len()).map_err(fault)?);
            let mut document = TantivyDocument::new();
            if !tokens.is_empty() {
                let mut offset = 0;
                let tokens = tokens.into_iter().enumerate().map(|(position, text)| {
                    let token = Token { offset_from: offset, offset_to: offset + text.len(), position, text, position_length: 1 };
                    offset = token.offset_to + 1;
                    token
                }).collect();
                document.add_pre_tokenized_text(*field, PreTokenizedString { text: String::new(), tokens });
            }
            writer.add_document(document).map_err(fault)?;
            let rectangles = coalesce(members.iter().flat_map(|index| visible[*index].iter().copied()).collect());
            let (class, bounds) = lifespan(&rectangles, sidecar.ceiling);
            sidecar.sections.entry(class).and_modify(|section| section.widen(&bounds)).or_insert(bounds);
            sidecar.doc_classes.push(class);
            sidecar.rectangles.extend(rectangles);
            sidecar.rectangle_starts.push(u32::try_from(sidecar.rectangles.len()).map_err(fault)?);
            sidecar.doc_iids.push(iid);
            if let Some(id) = &entity.identity { sidecar.identities.extend_from_slice(&id.handle_bytes()); }
            sidecar.identity_starts.push(u32::try_from(sidecar.identities.len()).map_err(fault)?);
            sidecar.column_docs[column] += 1;
            for index in members { sidecar.row_docs[column][entity.rows[index].0 as usize] = doc; }
        }
    }
    Ok(())
}

/// A term's documents within one window: the highest frequency among them, and the shortest stored length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Impact { window: u32, frequency: u32, length: u32 }

/// Every window holding the term, in order, from its postings.
fn impacts_of(postings: &mut tantivy::postings::SegmentPostings, lengths: &tantivy::fieldnorm::FieldNormReader) -> Vec<Impact> {
    use tantivy::{DocSet, postings::Postings};
    let mut impacts: Vec<Impact> = Vec::new();
    let mut doc = postings.doc();
    while doc != tantivy::TERMINATED {
        let (window, frequency, length) = (window_of(doc), postings.term_freq(), lengths.fieldnorm(doc));
        match impacts.last_mut() {
            Some(last) if last.window == window => {
                (last.frequency, last.length) = extend_impact(last.frequency, last.length, frequency, length);
            }
            _ => impacts.push(Impact { window, frequency, length }),
        }
        doc = postings.advance();
    }
    impacts
}

/// The sidecar, in order: format, analyzer and definition versions, artefact identity, row, document, page and member counts, each member's page and document starts, page starts, each column's document count, each row's run in each column, each document's IID, exact token length, optional typed identity and lifespan class, the sections, each document's rectangles, every row's physical route and canonical identity, the impacts of every term in at least STORED_IMPACTS documents by column and term, and a checksum of the rest.
fn encode(index: &Index, fields: &[Field], rows: u32, sidecar: &Accumulated, version: Option<i64>, identity: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&rtdb_text::ANALYZER_VERSION.to_le_bytes());
    bytes.push(u8::from(version.is_some()));
    bytes.extend_from_slice(&version.unwrap_or_default().to_le_bytes());
    bytes.extend_from_slice(&identity_digest(identity));
    let documents = sidecar.doc_iids.len() as u32;
    for count in [rows, documents, sidecar.page_starts.len() as u32, u32::try_from(sidecar.member_starts.len()).map_err(fault)?] {
        bytes.extend_from_slice(&count.to_le_bytes());
    }
    for member in &sidecar.member_starts {
        bytes.extend_from_slice(&member.page.to_le_bytes());
        bytes.extend_from_slice(&member.doc.to_le_bytes());
    }
    for value in sidecar.page_starts.iter().chain(&sidecar.column_docs).chain(sidecar.row_docs.iter().flatten()) { bytes.extend_from_slice(&value.to_le_bytes()); }
    for iid in &sidecar.doc_iids { bytes.extend_from_slice(iid); }
    for length in sidecar.token_lengths.iter().chain(&sidecar.identity_starts) {
        bytes.extend_from_slice(&length.to_le_bytes());
    }
    bytes.extend_from_slice(&sidecar.identities);
    bytes.extend_from_slice(&sidecar.doc_classes);
    bytes.extend_from_slice(&encode_sections(&sidecar.sections.values().copied().collect::<Vec<_>>()));
    for start in &sidecar.rectangle_starts { bytes.extend_from_slice(&start.to_le_bytes()); }
    for rectangle in &sidecar.rectangles { for value in rectangle { bytes.extend_from_slice(&value.to_le_bytes()); } }
    bytes.extend_from_slice(&sidecar.keys);
    for offset in &sidecar.row_identity_starts { bytes.extend_from_slice(&offset.to_le_bytes()); }
    bytes.extend_from_slice(&sidecar.row_identities);
    let reader = index.reader_builder().reload_policy(tantivy::ReloadPolicy::Manual).try_into().map_err(fault)?;
    let searcher = reader.searcher();
    let mut entries = Vec::new();
    for segment in searcher.segment_readers() {
        for (column, field) in fields.iter().enumerate() {
            let inverted = segment.inverted_index(*field).map_err(fault)?;
            let lengths = segment.get_fieldnorms_reader(*field).map_err(fault)?;
            let mut terms = inverted.terms().stream().map_err(fault)?;
            while terms.advance() {
                if terms.value().doc_freq < STORED_IMPACTS { continue; }
                let mut postings = inverted.read_postings_from_terminfo(terms.value(), IndexRecordOption::WithFreqs).map_err(fault)?;
                entries.push((column as u32, terms.key().to_vec(), impacts_of(&mut postings, &lengths)));
            }
        }
    }
    bytes.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (column, term, impacts) in entries {
        bytes.extend_from_slice(&column.to_le_bytes());
        bytes.extend_from_slice(&u16::try_from(term.len()).map_err(fault)?.to_le_bytes());
        bytes.extend_from_slice(&term);
        bytes.extend_from_slice(&(impacts.len() as u32).to_le_bytes());
        for impact in impacts { for value in [impact.window, impact.frequency, impact.length] { bytes.extend_from_slice(&value.to_le_bytes()); } }
    }
    bytes.extend_from_slice(&checksum(&bytes));
    Ok(bytes)
}

/// Tantivy checks its own files; the sidecar's bounds and nulls decide results, so it is checked too.
fn checksum(bytes: &[u8]) -> [u8; 8] {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)[..8].try_into().unwrap_or_default()
}

pub(crate) fn artefact_identity(table_slug: &str, owner: &rtdb_format::keys::ArtefactOwner, id: &str, version: i64) -> String {
    format!("{table_slug}/{owner}/{id}-v{version}")
}

fn identity_digest(identity: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(identity.as_bytes()).into()
}

/// A uuid-shaped name derived from the job's identity, standing in for tantivy's random segment id.
fn segment_uuid(identity: &str) -> String {
    identity_digest(identity)[..16].iter().map(|byte| format!("{byte:02x}")).collect()
}

fn bundle(directory: &dyn Directory, uuid: &str, mut output: impl std::io::Write) -> Result<()> {
    let meta = String::from_utf8(directory.atomic_read(Path::new("meta.json")).map_err(fault)?).map_err(fault)?;
    let parsed: serde_json::Value = serde_json::from_str(&meta).map_err(fault)?;
    let segments = parsed.get("segments").and_then(serde_json::Value::as_array).ok_or_else(|| fault("Search index meta has no segments"))?;
    let original = match segments.as_slice() {
        [] => None,
        [segment] => Some(segment.get("segment_id").and_then(serde_json::Value::as_str).ok_or_else(|| fault("Search segment has no id"))?.replace('-', "")),
        _ => return Err(fault("A search file holds exactly one segment")),
    };
    let rename = |name: &str| original.as_ref().map_or(name.to_string(), |original| name.replace(original, uuid));
    let meta = original.as_ref().map_or(meta.clone(), |original| {
        let dashed = |id: &str| format!("{}-{}-{}-{}-{}", &id[..8], &id[8..12], &id[12..16], &id[16..20], &id[20..]);
        meta.replace(&dashed(original), &dashed(uuid)).replace(original, uuid)
    });
    let mut files: BTreeMap<String, Option<PathBuf>> = BTreeMap::from([("meta.json".to_string(), None), (SIDECAR.to_string(), Some(PathBuf::from(SIDECAR)))]);
    if let Some(original) = &original {
        for extension in ["idx", "term", "fieldnorm", "fast", "store", "pos"] {
            let path = PathBuf::from(format!("{original}.{extension}"));
            if directory.exists(&path).map_err(fault)? { files.insert(rename(&path.to_string_lossy()), Some(path)); }
        }
    }
    let write = |output: &mut dyn std::io::Write, bytes: &[u8]| output.write_all(bytes).map_err(fault);
    write(&mut output, MAGIC)?;
    let mut position = MAGIC.len() as u64;
    let mut index = Vec::new();
    for (name, source) in &files {
        let length = match source {
            None => { write(&mut output, meta.as_bytes())?; meta.len() }
            Some(path) => {
                let bytes = directory.open_read(path).map_err(fault)?.read_bytes().map_err(fault)?;
                write(&mut output, bytes.as_slice())?;
                bytes.len()
            }
        } as u64;
        index.push((name.clone(), position, length));
        position += length;
    }
    let mut footer = Vec::new();
    footer.extend_from_slice(&(index.len() as u32).to_le_bytes());
    for (name, offset, length) in index {
        footer.extend_from_slice(&(name.len() as u16).to_le_bytes());
        footer.extend_from_slice(name.as_bytes());
        footer.extend_from_slice(&offset.to_le_bytes());
        footer.extend_from_slice(&length.to_le_bytes());
    }
    footer.extend_from_slice(&position.to_le_bytes());
    footer.extend_from_slice(MAGIC);
    write(&mut output, &footer)?;
    output.flush().map_err(fault)
}

/// The bytes a search file is read from: the object as read, a lease on the buffer pool's copy of it, or this node's local file mapped into memory.
pub enum Source { Bytes(bytes::Bytes), Lease(rtdb_storage::cache::CacheLease), Mapped(memmap2::Mmap, rtdb_storage::LocalFile) }
impl From<bytes::Bytes> for Source { fn from(bytes: bytes::Bytes) -> Self { Self::Bytes(bytes) } }
impl From<Vec<u8>> for Source { fn from(bytes: Vec<u8>) -> Self { Self::Bytes(bytes.into()) } }
impl From<rtdb_storage::cache::CacheLease> for Source { fn from(lease: rtdb_storage::cache::CacheLease) -> Self { Self::Lease(lease) } }
impl std::ops::Deref for Source {
    type Target = [u8];
    fn deref(&self) -> &[u8] { match self { Self::Bytes(bytes) => bytes, Self::Lease(lease) => lease.as_slice(), Self::Mapped(map, _) => map } }
}

impl Source {
    /// Maps a local file whose object is immutable.
    pub fn mapped(local: rtdb_storage::LocalFile) -> Result<Self> {
        // SAFETY: an object is never rewritten under its key, and the local file is held for as long as the map, so the mapped bytes do not change beneath it.
        let map = unsafe { memmap2::Mmap::map(local.file()) }.map_err(fault)?;
        Ok(Self::Mapped(map, local))
    }
}
// SAFETY: every owner keeps its bytes at a fixed address for as long as it lives, wherever it moves: behind a reference count, or in a mapping.
unsafe impl stable_deref_trait::StableDeref for Source {}

/// A read-only directory over an immutable search bundle.
#[derive(Clone)]
struct BundleDirectory { bytes: FileSlice, files: Arc<BTreeMap<PathBuf, Range<usize>>> }

impl std::fmt::Debug for BundleDirectory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.debug_struct("BundleDirectory").field("files", &self.files.keys()).finish() }
}

impl BundleDirectory {
    fn open(bytes: FileSlice) -> Result<Self> {
        let len = bytes.len();
        if len < 2 * MAGIC.len() + 8 { return Err(corrupt("Search file has no bundle magic")); }
        let trailer = bytes.slice(len - 16..).read_bytes().map_err(fault)?;
        let head = bytes.slice(..8).read_bytes().map_err(fault)?;
        if head.as_slice() != MAGIC || trailer.len() != 16 || &trailer.as_slice()[8..] != MAGIC {
            return Err(corrupt("Search file has no bundle magic"));
        }
        let footer = usize::try_from(u64::from_le_bytes(trailer.as_slice()[..8].try_into().map_err(fault)?)).map_err(fault)?;
        if !(8..=len - 16).contains(&footer) { return Err(corrupt("Search file footer is out of range")); }
        let footer_bytes = bytes.slice(footer..len - 16).read_bytes().map_err(fault)?;
        let mut cursor = footer_bytes.as_slice();
        let mut take = |count: usize| -> Result<&[u8]> {
            if cursor.len() < count { return Err(corrupt("Search file footer is truncated")); }
            let (head, tail) = cursor.split_at(count);
            cursor = tail;
            Ok(head)
        };
        let count = u32::from_le_bytes(take(4)?.try_into().map_err(fault)?);
        let mut files = BTreeMap::new();
        for _ in 0..count {
            let length = u16::from_le_bytes(take(2)?.try_into().map_err(fault)?) as usize;
            let name = std::str::from_utf8(take(length)?).map_err(fault)?.to_string();
            let offset = usize::try_from(u64::from_le_bytes(take(8)?.try_into().map_err(fault)?)).map_err(fault)?;
            let size = usize::try_from(u64::from_le_bytes(take(8)?.try_into().map_err(fault)?)).map_err(fault)?;
            let end = offset.checked_add(size).filter(|end| *end <= footer).ok_or_else(|| corrupt("Search file entry is out of range"))?;
            files.insert(PathBuf::from(name), offset..end);
        }
        Ok(Self { bytes, files: Arc::new(files) })
    }
    fn read_only(path: &Path) -> std::io::Error { std::io::Error::new(std::io::ErrorKind::PermissionDenied, format!("Search file is read-only: {}", path.display())) }
}

impl Directory for BundleDirectory {
    fn get_file_handle(&self, path: &Path) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        let range = self.files.get(path).ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(Arc::new(self.bytes.slice(range.clone())))
    }
    fn delete(&self, path: &Path) -> std::result::Result<(), DeleteError> { Err(DeleteError::IoError { io_error: Arc::new(Self::read_only(path)), filepath: path.to_path_buf() }) }
    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> { Ok(self.files.contains_key(path)) }
    fn open_write(&self, path: &Path) -> std::result::Result<WritePtr, OpenWriteError> { Err(OpenWriteError::wrap_io_error(Self::read_only(path), path.to_path_buf())) }
    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> { Ok(self.get_file_handle(path)?.read_bytes(0..self.files[path].len()).map_err(|io_error| OpenReadError::wrap_io_error(io_error, path.to_path_buf()))?.as_slice().to_vec()) }
    fn atomic_write(&self, path: &Path, _: &[u8]) -> std::io::Result<()> { Err(Self::read_only(path)) }
    fn sync_directory(&self) -> std::io::Result<()> { Ok(()) }
    fn acquire_lock(&self, _: &tantivy::directory::Lock) -> std::result::Result<tantivy::directory::DirectoryLock, tantivy::directory::error::LockError> {
        Ok(tantivy::directory::DirectoryLock::from(Box::new(())))
    }
    fn watch(&self, _: WatchCallback) -> tantivy::Result<WatchHandle> { Ok(WatchHandle::empty()) }
}

/// What the sidecar holds, located within the bundle's bytes.
struct Sidecar { bytes: OwnedBytes, version: Option<i64>, identity: [u8; 32], rows: u32, documents: u32, member_starts: Vec<MemberStart>, page_starts: Vec<u32>, column_docs: Vec<u32>, row_docs: usize, doc_iids: usize, token_lengths: usize, identity_starts: usize, identities: usize, doc_classes: usize, sections: Vec<Section>, rectangle_starts: usize, rectangles: usize, keys: usize, row_identity_starts: usize, row_identities: usize, has_full_keys: bool, impacts: BTreeMap<(u32, Vec<u8>), Range<usize>> }

impl Sidecar {
    fn parse(bytes: OwnedBytes, columns: usize, documents: u32) -> Result<Self> {
        let whole = bytes.as_slice();
        let (data, sum) = whole.split_at_checked(whole.len().saturating_sub(8)).ok_or_else(|| corrupt("Search file sidecar is truncated"))?;
        if sum.len() != 8 || checksum(data) != sum { return Err(corrupt("Search file sidecar fails its checksum")); }
        let mut position = 0usize;
        let mut take = |count: usize| -> Result<Range<usize>> {
            let end = position.checked_add(count).filter(|end| *end <= data.len()).ok_or_else(|| corrupt("Search file sidecar is truncated"))?;
            let range = position..end;
            position = end;
            Ok(range)
        };
        let u32_at = |range: Range<usize>| u32::from_le_bytes(data[range].try_into().unwrap_or_default());
        let format = i32::from_le_bytes(data[take(4)?].try_into().map_err(fault)?);
        let analyzer = i32::from_le_bytes(data[take(4)?].try_into().map_err(fault)?);
        if format != FORMAT_VERSION || analyzer != rtdb_text::ANALYZER_VERSION {
            return Err(corrupt("Search file format or analyzer version does not match the reader"));
        }
        let version_tag = data[take(1)?.start];
        let version_value = i64::from_le_bytes(data[take(8)?].try_into().map_err(fault)?);
        let version = match version_tag {
            0 if version_value == 0 => None,
            1 => Some(version_value),
            _ => return Err(corrupt("Search file has invalid definition version metadata")),
        };
        let identity = data[take(32)?].try_into().map_err(fault)?;
        let (rows, counted, pages) = (u32_at(take(4)?), u32_at(take(4)?), u32_at(take(4)?) as usize);
        if counted != documents { return Err(corrupt("Search file sidecar counts other documents than its segment")); }
        let members = u32_at(take(4)?) as usize;
        let member_starts = (0..members).map(|_| {
            Ok(MemberStart { page: u32_at(take(4)?), doc: u32_at(take(4)?) })
        }).collect::<Result<Vec<_>>>()?;
        if !member_starts.first().is_some_and(|first| first.page == 0 && first.doc == 0)
            || member_starts.windows(2).any(|pair| pair[0].page > pair[1].page || pair[0].doc > pair[1].doc)
            || member_starts.last().is_some_and(|last| last.page as usize > pages || last.doc > documents)
        {
            return Err(corrupt("Search file member starts are out of order"));
        }
        let page_starts = (0..pages).map(|_| take(4).map(u32_at)).collect::<Result<Vec<_>>>()?;
        if page_starts.first().is_some_and(|first| *first != 0) || page_starts.windows(2).any(|pair| pair[0] > pair[1]) || page_starts.last().is_some_and(|last| *last > rows) {
            return Err(corrupt("Search file page starts are out of order"));
        }
        let column_docs = (0..columns).map(|_| take(4).map(u32_at)).collect::<Result<Vec<_>>>()?;
        if column_docs.iter().map(|count| u64::from(*count)).sum::<u64>() != u64::from(documents) { return Err(corrupt("Search file columns count other documents than it holds")); }
        let row_docs = take(rows as usize * columns * 4)?;
        if data[row_docs.clone()].chunks_exact(4).any(|chunk| { let doc = u32::from_le_bytes(chunk.try_into().unwrap_or_default()); doc != NO_DOC && doc != NULL_DOC && doc >= documents }) {
            return Err(corrupt("Search file row names no document"));
        }
        let doc_iids = take(documents as usize * 16)?.start;
        let token_lengths = take(documents as usize * 4)?.start;
        let identity_starts = take((documents as usize + 1) * 4)?;
        let mut previous = 0;
        for index in (identity_starts.start..identity_starts.end).step_by(4) {
            let offset = u32_at(index..index + 4);
            if offset < previous || (index == identity_starts.start && offset != 0) {
                return Err(corrupt("Search file identity offsets are out of order"));
            }
            previous = offset;
        }
        let identities = take(previous as usize)?.start;
        let doc_classes = take(documents as usize)?.start;
        let counted = take(1)?;
        let body = take(data[counted.start] as usize * 49)?;
        let sections = decode_sections(&data[counted.start..body.end]).ok_or_else(|| corrupt("Search file sections are malformed"))?;
        if data[doc_classes..doc_classes + documents as usize].iter().any(|class| !sections.iter().any(|section| section.class == *class)) { return Err(corrupt("Search file document has no section")); }
        let starts = take((documents as usize + 1) * 4)?;
        let rectangle_starts: Vec<u32> = data[starts.clone()].chunks_exact(4).map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap_or_default())).collect();
        if rectangle_starts.first() != Some(&0) || rectangle_starts.windows(2).any(|pair| pair[0] > pair[1]) { return Err(corrupt("Search file rectangles are out of order")); }
        let rectangles = take(*rectangle_starts.last().unwrap_or(&0) as usize * 32)?.start;
        let keys = take(rows as usize * KEY_BYTES)?.start;
        let row_identity_starts = take((rows as usize + 1) * 4)?;
        let mut previous = 0;
        let mut has_full_keys = true;
        for index in (row_identity_starts.start..row_identity_starts.end).step_by(4) {
            let offset = u32_at(index..index + 4);
            if offset < previous || (index == row_identity_starts.start && offset != 0) {
                return Err(corrupt("Search file row identity offsets are out of order"));
            }
            if index != row_identity_starts.start && offset == previous { has_full_keys = false; }
            previous = offset;
        }
        let row_identities = take(previous as usize)?.start;
        let entries = u32_at(take(4)?);
        let mut impacts = BTreeMap::new();
        for _ in 0..entries {
            let column = u32_at(take(4)?);
            let length = take(2)?;
            let term = data[take(u16::from_le_bytes(data[length].try_into().unwrap_or_default()) as usize)?].to_vec();
            let windows = u32_at(take(4)?) as usize;
            let range = take(windows.checked_mul(12).ok_or_else(|| corrupt("Search file impacts overflow"))?)?;
            let ascending = data[range.clone()].chunks_exact(12).map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])).collect::<Vec<_>>().windows(2).all(|pair| pair[0] < pair[1]);
            if !ascending || column as usize >= columns { return Err(corrupt("Search file impacts are out of order")); }
            impacts.insert((column, term), range);
        }
        if position != data.len() { return Err(corrupt("Search file sidecar has trailing bytes")); }
        Ok(Self { bytes, version, identity, rows, documents, member_starts, page_starts, column_docs, row_docs: row_docs.start, doc_iids, token_lengths, identity_starts: identity_starts.start, identities, doc_classes, sections, rectangle_starts: starts.start, rectangles, keys, row_identity_starts: row_identity_starts.start, row_identities, has_full_keys, impacts })
    }
    fn data(&self) -> &[u8] { self.bytes.as_slice() }
    fn u32_at(&self, offset: usize) -> u32 { u32::from_le_bytes(self.data()[offset..offset + 4].try_into().unwrap_or_default()) }
    fn i64_at(&self, offset: usize) -> i64 { i64::from_le_bytes(self.data()[offset..offset + 8].try_into().unwrap_or_default()) }
    fn impacts(&self, column: u32, term: &str) -> Option<Vec<Impact>> {
        let range = self.impacts.get(&(column, term.as_bytes().to_vec()))?;
        Some(self.data()[range.clone()].chunks_exact(12).map(|chunk| {
            let at = |offset: usize| u32::from_le_bytes([chunk[offset], chunk[offset + 1], chunk[offset + 2], chunk[offset + 3]]);
            Impact { window: at(0), frequency: at(4), length: at(8) }
        }).collect())
    }
    /// A row's place among a column's runs.
    fn row_doc(&self, column: usize, row: u32) -> RowDoc {
        if row >= self.rows || column >= self.column_docs.len() { return RowDoc::Invisible; }
        match self.u32_at(self.row_docs + (column * self.rows as usize + row as usize) * 4) { NO_DOC => RowDoc::Invisible, NULL_DOC => RowDoc::Null, doc => RowDoc::Run(doc) }
    }
    fn section(&self, doc: u32) -> Option<&Section> { let class = self.data()[self.doc_classes + doc as usize]; self.sections.iter().find(|section| section.class == class) }
    fn member_pages(&self, member: u32) -> Result<Range<usize>> {
        let member = member as usize;
        let first = self.member_starts.get(member).ok_or_else(|| corrupt("Search file has no such member"))?.page as usize;
        let end = self.member_starts.get(member + 1).map_or(self.page_starts.len(), |start| start.page as usize);
        Ok(first..end)
    }
    fn doc_match(&self, doc: u32) -> DocMatch {
        let member = document_member(&self.member_starts, doc);
        DocMatch { doc, iid: self.doc_iid(doc), member: member as u32 }
    }
    fn doc_iid(&self, doc: u32) -> [u8; 16] { self.data()[self.doc_iids + doc as usize * 16..][..16].try_into().unwrap_or_default() }
    fn rectangles(&self, doc: u32) -> impl Iterator<Item = [i64; 4]> + '_ {
        let (start, end) = (self.u32_at(self.rectangle_starts + doc as usize * 4), self.u32_at(self.rectangle_starts + (doc as usize + 1) * 4));
        (start..end).map(move |index| { let at = self.rectangles + index as usize * 32; [self.i64_at(at), self.i64_at(at + 8), self.i64_at(at + 16), self.i64_at(at + 24)] })
    }
    fn row_iid(&self, row: u32) -> &[u8] { &self.data()[self.keys + row as usize * KEY_BYTES..][..16] }
    /// The rows of one entity's events, IID being the rows' order.
    fn rows_of(&self, iid: &[u8; 16]) -> Range<u32> {
        let (mut low, mut high) = (0u32, self.rows);
        while low < high { let middle = low + (high - low) / 2; if self.row_iid(middle) < &iid[..] { low = middle + 1; } else { high = middle; } }
        let start = low;
        high = self.rows;
        while low < high { let middle = low + (high - low) / 2; if self.row_iid(middle) <= &iid[..] { low = middle + 1; } else { high = middle; } }
        start..low
    }
    fn row_identity(&self, row: u32) -> &[u8] {
        let offset = self.row_identity_starts + row as usize * 4;
        let from = self.row_identities + self.u32_at(offset) as usize;
        let to = self.row_identities + self.u32_at(offset + 4) as usize;
        &self.data()[from..to]
    }

    fn key(&self, row: u32) -> Result<KeyedEvent> {
        let at = self.keys + row as usize * KEY_BYTES + 16;
        Ok(KeyedEvent { system_from: self.i64_at(at), valid_from: self.i64_at(at + 8), valid_to: self.i64_at(at + 16), op: op_of(self.data()[at + 24])? })
    }
}

/// Where a row stands in one column: visible nowhere within the file, visible with the column null or absent, or visible in a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RowDoc { Invisible, Null, Run(u32) }

pub fn meets(rectangle: [i64; 4], bounds: TemporalBounds) -> bool {
    rectangle[0] < bounds.valid_time.upper().0 && bounds.valid_time.lower().0 < rectangle[1] && rectangle[2] < bounds.system_time.upper().0 && bounds.system_time.lower().0 < rectangle[3]
}

/// An opened search file.
/// The searcher is made once: a search file never changes, so neither does its one segment's reader.
pub struct SearchFile { searcher: tantivy::Searcher, columns: Vec<String>, sidecar: Sidecar }

impl SearchFile {
    pub fn open(bytes: impl Into<Source>, columns: &[String]) -> Result<Self> {
        Self::open_slice(FileSlice::from(bytes.into()), columns)
    }

    /// Opens a bundle through an immutable range reader.
    pub fn open_with_ranges(reader: Arc<dyn FileHandle>, columns: &[String]) -> Result<Self> {
        Self::open_slice(FileSlice::new(reader), columns)
    }

    fn open_slice(bytes: FileSlice, columns: &[String]) -> Result<Self> {
        let directory = BundleDirectory::open(bytes)?;
        let sidecar = directory.get_file_handle(Path::new(SIDECAR)).map_err(|_| corrupt("Search file has no sidecar"))?;
        let sidecar = sidecar.read_bytes(0..sidecar.len()).map_err(fault)?;
        let index = Index::open(directory).map_err(fault)?;
        let (expected, _) = schema(columns);
        if index.schema() != expected { return Err(corrupt("Search file schema does not match its definition")); }
        let segments = index.searchable_segment_metas().map_err(fault)?;
        if segments.len() > 1 { return Err(corrupt("A search file holds at most one segment")); }
        let documents = segments.iter().map(|segment| segment.max_doc()).sum();
        let reader: tantivy::IndexReader = index.reader_builder().reload_policy(tantivy::ReloadPolicy::Manual).try_into().map_err(fault)?;
        Ok(Self { sidecar: Sidecar::parse(sidecar, columns.len(), documents)?, searcher: reader.searcher(), columns: columns.to_vec() })
    }

    fn column(&self, column: &str) -> Option<usize> { self.columns.iter().position(|candidate| candidate == column) }
    fn field(&self, column: &str) -> Option<Field> { self.column(column).map(|index| Field::from_field_id(index as u32)) }

    /// Whether the document is visible anywhere in `bounds` as far as this file's own events decide; events elsewhere can only cut it further.
    pub fn visible(&self, doc: u32, bounds: TemporalBounds) -> bool {
        self.sidecar.section(doc).is_some_and(|section| section.meets(bounds)) && self.sidecar.rectangles(doc).any(|rectangle| meets(rectangle, bounds))
    }

    /// The bounds of each lifespan class's documents.
    pub fn sections(&self) -> &[Section] { &self.sidecar.sections }

    /// The rectangles where the document is visible as far as this file's own events decide, as valid from, valid to, system from, system to.
    pub fn rectangles(&self, doc: u32) -> impl Iterator<Item = [i64; 4]> + '_ { self.sidecar.rectangles(doc) }

    /// Every event of one entity this file's data holds, system time descending.
    pub fn events(&self, iid: &[u8; 16]) -> Result<Vec<KeyedEvent>> { self.sidecar.rows_of(iid).map(|row| self.sidecar.key(row)).collect() }

    /// Every event of one entity this file's data holds, with its row, system time descending.
    pub fn rows_of(&self, iid: &[u8; 16]) -> Result<Vec<(u32, KeyedEvent)>> { self.sidecar.rows_of(iid).map(|row| Ok((row, self.sidecar.key(row)?))).collect() }

    pub(crate) fn has_full_event_keys(&self) -> bool {
        self.sidecar.has_full_keys
    }

    pub(crate) fn rows_for_key<'a>(&'a self, iid: &[u8; 16], key: &'a [u8]) -> impl Iterator<Item = Result<(u32, KeyedEvent)>> + 'a {
        self.sidecar.rows_of(iid).filter(move |row| self.sidecar.row_identity(*row) == key)
            .map(move |row| Ok((row, self.sidecar.key(row)?)))
    }

    /// A row's run in a column: `None` where the row is visible nowhere within the file, `Some(None)` where it is visible with the column null or absent.
    pub fn run_of(&self, column: &str, row: u32) -> Option<Option<u32>> {
        match self.column(column).map(|index| self.sidecar.row_doc(index, row)) { Some(RowDoc::Run(doc)) => Some(Some(doc)), Some(RowDoc::Null) => Some(None), _ => None }
    }

    /// The text runs belonging to one member of a single-column artefact.
    pub(crate) fn single_column_member_runs(&self, member: u32) -> Result<impl Iterator<Item = DocMatch> + '_> {
        if self.columns.len() != 1 { return Err(corrupt("Owned search must cover exactly one column")); }
        let member = member as usize;
        let first = self.sidecar.member_starts.get(member).ok_or_else(|| corrupt("Search file has no such member"))?.doc;
        let end = self.sidecar.member_starts.get(member + 1).map_or(self.sidecar.documents, |start| start.doc);
        Ok((first..end).map(|doc| self.sidecar.doc_match(doc)))
    }

    pub(crate) fn matches_artefact(&self, identity: &str, version: i64) -> bool {
        self.sidecar.version == Some(version) && self.sidecar.identity == identity_digest(identity)
    }

    pub(crate) fn run_identity_bytes(&self, run: DocMatch) -> Result<&[u8]> {
        if run.doc >= self.sidecar.documents { return Err(corrupt("Search file has no such run")); }
        let offset = self.sidecar.identity_starts + run.doc as usize * 4;
        let from = self.sidecar.identities + self.sidecar.u32_at(offset) as usize;
        let to = self.sidecar.identities + self.sidecar.u32_at(offset + 4) as usize;
        if from == to { return Err(corrupt("Search run omitted its full identity")); }
        Ok(&self.sidecar.data()[from..to])
    }

    pub(crate) fn run_identity(&self, run: DocMatch) -> Result<EntityId> {
        EntityId::from_handle_bytes(self.run_identity_bytes(run)?)
            .map_err(|_| corrupt("Search run has an invalid complete identity key"))
    }

    /// Counts analyzer tokens before the ranker's field-norm quantization.
    pub fn token_length(&self, doc: u32) -> Result<u32> {
        if doc >= self.sidecar.documents { return Err(corrupt("Search file has no such run")); }
        Ok(self.sidecar.u32_at(self.sidecar.token_lengths + doc as usize * 4))
    }

    /// A document's length as the index stores it.
    pub fn stored_length(&self, column: &str, doc: u32) -> Result<u32> {
        let Some(field) = self.field(column) else { return Ok(0) };
        let Some(segment) = self.searcher.segment_readers().first() else { return Ok(0) };
        Ok(segment.get_fieldnorms_reader(field).map_err(fault)?.fieldnorm(doc))
    }

    /// The documents holding a term in a column, in order.
    pub fn holding(&self, column: &str, term: &str) -> Result<Vec<u32>> {
        use tantivy::DocSet;
        let Some(mut postings) = self.postings(column, term)? else { return Ok(Vec::new()) };
        let mut docs = Vec::new();
        while postings.doc() != tantivy::TERMINATED {
            docs.push(postings.doc());
            postings.advance();
        }
        Ok(docs)
    }

    pub(crate) fn postings(&self, column: &str, term: &str) -> Result<Option<SegmentPostings>> {
        let (Some(field), Some(segment)) = (self.field(column), self.searcher.segment_readers().first()) else { return Ok(None) };
        segment.inverted_index(field).map_err(fault)?.read_postings(&Term::from_field_text(field, term), IndexRecordOption::Basic).map_err(fault)
    }

    pub(crate) fn document_frequency(&self, column: &str, term: &str) -> Result<u64> {
        let (Some(field), Some(segment)) = (self.field(column), self.searcher.segment_readers().first()) else { return Ok(0) };
        let inverted = segment.inverted_index(field).map_err(fault)?;
        inverted.doc_freq(&Term::from_field_text(field, term)).map(u64::from).map_err(fault)
    }

    /// Every query term and phrase as one conjunction over the column.
    fn conjunction(field: Field, query: &Query) -> BooleanQuery {
        let mut clauses: Vec<(Occur, Box<dyn tantivy::query::Query>)> = query.terms().iter().map(|term| (Occur::Must, Box::new(TermQuery::new(Term::from_field_text(field, term), IndexRecordOption::Basic)) as Box<dyn tantivy::query::Query>)).collect();
        for phrase in query.phrases() {
            clauses.push((Occur::Must, Box::new(tantivy::query::PhraseQuery::new(phrase.iter().map(|term| Term::from_field_text(field, term)).collect()))));
        }
        BooleanQuery::new(clauses)
    }

    /// The ids of every document holding all of the query's terms and phrases in the column, in order.
    fn matching_docs(&self, column: &str, query: &Query) -> Result<Vec<u32>> {
        let Some(field) = self.field(column) else { return Ok(Vec::new()) };
        if query.terms().is_empty() { return Ok(Vec::new()); }
        let found = self.searcher.search(&Self::conjunction(field, query), &DocSetCollector).map_err(fault)?;
        let mut docs: Vec<u32> = found.into_iter().map(|address| address.doc_id).collect();
        docs.sort_unstable();
        Ok(docs)
    }

    /// Every document holding all of the query's terms and phrases in the column that is visible somewhere in `bounds`.
    pub fn matches(&self, column: &str, query: &Query, bounds: TemporalBounds) -> Result<Vec<DocMatch>> {
        Ok(self.matching_docs(column, query)?.into_iter().filter(|doc| self.visible(*doc, bounds)).map(|doc| self.sidecar.doc_match(doc)).collect())
    }

    /// The visible documents matching `matching`, with each one's score for `scoring`'s terms, from the postings' frequencies and the stored length.
    /// It equals `rtdb_text::bm25` of the row's tokens, which is what SEARCH_SCORE computes on a resolved row.
    pub fn scored(&self, column: &str, matching: &Query, scoring: &Query, corpus: Option<&Corpus>, bounds: TemporalBounds) -> Result<Vec<(DocMatch, f64)>> {
        let Some(field) = self.field(column) else { return Ok(Vec::new()) };
        let docs = self.matches(column, matching, bounds)?;
        let Some(segment) = self.searcher.segment_readers().first() else { return Ok(Vec::new()) };
        let mut scorer = ExactScorer::open(segment, field, Arc::new(Scorer::new(scoring.clone(), corpus)))?;
        docs.into_iter().map(|doc| Ok((doc, scorer.score(doc.doc)))).collect()
    }

    /// The documents matching `matching` and visible in `bounds`, best possible score for `scoring` first, a window of documents at a time.
    pub fn ranked(self: &Arc<Self>, column: &str, matching: &Query, scoring: &RankScorer, bounds: TemporalBounds) -> Result<Ranked> {
        let (Some(column), Some(field)) = (self.column(column), self.field(column)) else { return Ok(Ranked(None)) };
        let Some(segment) = self.searcher.segment_readers().first() else { return Ok(Ranked(None)) };
        if matching.terms().is_empty() { return Ok(Ranked(None)); }
        let inverted = segment.inverted_index(field).map_err(fault)?;
        let lengths = segment.get_fieldnorms_reader(field).map_err(fault)?;
        let impacts = |term: &str| -> Result<Vec<Impact>> {
            if let Some(impacts) = self.sidecar.impacts(column as u32, term) { return Ok(impacts); }
            Ok(match inverted.read_postings(&Term::from_field_text(field, term), IndexRecordOption::WithFreqs).map_err(fault)? {
                Some(mut postings) => impacts_of(&mut postings, &lengths),
                None => Vec::new(),
            })
        };
        let mut windows: Option<Vec<u32>> = None;
        for term in matching.terms() {
            let held: Vec<u32> = impacts(term)?.into_iter().map(|impact| impact.window).collect();
            windows = Some(match windows { None => held, Some(previous) => previous.into_iter().filter(|window| held.binary_search(window).is_ok()).collect() });
        }
        let mut ordered: Vec<(f64, u32)> = windows.unwrap_or_default().into_iter().map(|window| (0.0, window)).collect();
        for (term_index, term) in scoring.scorer().query().terms().iter().enumerate() {
            let impacts = impacts(term)?;
            for (bound, window) in &mut ordered {
                if let Ok(index) = impacts.binary_search_by_key(window, |impact| impact.window) {
                    *bound += scoring.term_bound(term_index, impacts[index].frequency, impacts[index].length);
                }
            }
        }
        ordered.sort_by(|left, right| right.0.total_cmp(&left.0).then(left.1.cmp(&right.1)));
        Ok(Ranked(Some(Windows { file: Arc::clone(self), field, matching: matching.clone(), scoring: Arc::clone(scoring.scorer()), bounds, windows: ordered, next: 0, batch: 4 })))
    }

    /// Whether MATCH holds on each row, through the run it belongs to: `None` where the column is null or absent, as MATCH is on such a row.
    pub fn match_set(self: &Arc<Self>, column: &str, query: &Query) -> Result<MatchSet> {
        let documents = self.sidecar.documents;
        let mut matching = vec![0u64; documents.div_ceil(64) as usize];
        for doc in self.matching_docs(column, query)? { matching[doc as usize / 64] |= 1 << (doc % 64); }
        Ok(MatchSet { file: Arc::clone(self), column: self.column(column), matching: MatchingDocuments::All(matching) })
    }

    pub(crate) fn match_selected_iids(self: &Arc<Self>, column: &str, query: &Query, iids: &BTreeSet<Iid>) -> Result<MatchSet> {
        use tantivy::{DocSet, query::Query as _};

        let mut selected = BTreeMap::new();
        let column = self.column(column);
        if let Some(column) = column {
            for iid in iids {
                for row in self.sidecar.rows_of(&iid.0) {
                    if let RowDoc::Run(doc) = self.sidecar.row_doc(column, row) {
                        selected.insert(doc, false);
                        if selected.len() > 1024 { return self.match_set(&self.columns[column], query); }
                    }
                }
            }
            if !selected.is_empty() && !query.terms().is_empty() {
                let segment = &self.searcher.segment_readers()[0];
                let weight = Self::conjunction(Field::from_field_id(column as u32), query)
                    .weight(tantivy::query::EnableScoring::disabled_from_searcher(&self.searcher)).map_err(fault)?;
                let mut docs = weight.scorer(segment, 1.0).map_err(fault)?;
                for (&doc, matched) in &mut selected {
                    if docs.doc() < doc { docs.seek(doc); }
                    *matched = docs.doc() == doc;
                }
            }
        }
        Ok(MatchSet { file: Arc::clone(self), column, matching: MatchingDocuments::Selected(selected) })
    }

    /// The column's corpus statistics for the query's terms: its runs, their tokens, and each term's document frequency.
    pub fn corpus(&self, column: &str, query: &Query) -> Result<Corpus> {
        let (Some(index), Some(field)) = (self.column(column), self.field(column)) else { return Ok(Corpus::default()) };
        let mut corpus = Corpus { documents: u64::from(self.sidecar.column_docs[index]), ..Corpus::default() };
        for segment in self.searcher.segment_readers() {
            let inverted = segment.inverted_index(field).map_err(fault)?;
            corpus.tokens += inverted.total_num_tokens();
            for term in query.terms() {
                *corpus.frequencies.entry(term.clone()).or_default() += u64::from(inverted.doc_freq(&Term::from_field_text(field, term)).map_err(fault)?);
            }
        }
        Ok(corpus)
    }
}

enum MatchingDocuments {
    All(Vec<u64>),
    Selected(BTreeMap<u32, bool>),
}

/// MATCH of one query on rows of a search file, by page and row, through each row's run.
pub struct MatchSet { file: Arc<SearchFile>, column: Option<usize>, matching: MatchingDocuments }

impl MatchSet {
    /// MATCH on the row at `row` of data page `page`: `None` where the column is null or absent; an error where the file has no such row, or the row is visible nowhere.
    pub fn value(&self, page: usize, row: usize) -> Result<Option<bool>> {
        let sidecar = &self.file.sidecar;
        let start = *sidecar.page_starts.get(page).ok_or_else(|| corrupt("Search file has no such page"))?;
        let end = sidecar.page_starts.get(page + 1).copied().unwrap_or(sidecar.rows);
        let row = u32::try_from(row).ok().and_then(|row| start.checked_add(row)).filter(|row| *row < end).ok_or_else(|| corrupt("Search file has no such row"))?;
        let Some(column) = self.column else { return Ok(None) };
        match sidecar.row_doc(column, row) {
            RowDoc::Invisible => Err(corrupt("Search file row is visible nowhere")),
            RowDoc::Null => Ok(None),
            RowDoc::Run(doc) => match &self.matching {
                MatchingDocuments::All(bits) => Ok(Some(bits.get(doc as usize / 64).is_some_and(|word| word & (1 << (doc % 64)) != 0))),
                MatchingDocuments::Selected(docs) => docs.get(&doc).copied().map(Some)
                    .ok_or_else(|| corrupt("Search file MATCH was not requested for this row")),
            },
        }
    }
    /// MATCH on one member's data file, by its own page and row.
    pub fn member(self: &Arc<Self>, member: u32) -> Result<MemberMatches> {
        let pages = self.file.sidecar.member_pages(member)?;
        Ok(MemberMatches { set: Arc::clone(self), first: pages.start, pages: pages.len() })
    }

    /// The bytes held, for a cache's weight.
    pub fn bytes(&self) -> usize {
        match &self.matching {
            MatchingDocuments::All(bits) => bits.len() * 8,
            MatchingDocuments::Selected(docs) => docs.len() * std::mem::size_of::<(u32, bool)>(),
        }
    }
}

/// MATCH on the rows of one member of a search file.
pub struct MemberMatches { set: Arc<MatchSet>, first: usize, pages: usize }

impl rtdb_query::FileMatches for MemberMatches {
    fn value(&self, page: usize, row: usize) -> Result<Option<bool>> {
        if page >= self.pages { return Err(corrupt("Search file member has no such page")); }
        self.set.value(self.first + page, row)
    }
}

impl rtdb_query::FileMatches for MatchSet {
    fn value(&self, page: usize, row: usize) -> Result<Option<bool>> { MatchSet::value(self, page, row) }
}

/// Scores documents in ascending order from the postings of the scoring terms.
struct ExactScorer { postings: Vec<Option<tantivy::postings::SegmentPostings>>, lengths: tantivy::fieldnorm::FieldNormReader, scoring: Arc<Scorer> }

impl ExactScorer {
    fn open(segment: &tantivy::SegmentReader, field: Field, scoring: Arc<Scorer>) -> Result<Self> {
        let inverted = segment.inverted_index(field).map_err(fault)?;
        let postings = scoring.query().terms().iter().map(|term| inverted.read_postings(&Term::from_field_text(field, term), IndexRecordOption::WithFreqs).map_err(fault)).collect::<Result<_>>()?;
        Ok(Self { postings, lengths: segment.get_fieldnorms_reader(field).map_err(fault)?, scoring })
    }
    fn score(&mut self, doc: u32) -> f64 {
        use tantivy::{DocSet, postings::Postings};
        let frequencies: Vec<u32> = self.postings.iter_mut().map(|postings| match postings {
            Some(postings) => if postings.doc() <= doc && postings.seek(doc) == doc { postings.term_freq() } else { 0 },
            None => 0,
        }).collect();
        self.scoring.score(&frequencies, self.lengths.fieldnorm(doc))
    }
}

/// A search file's matching visible documents, handed out a batch of windows at a time, best bound first; `None` where nothing in the file can match.
/// No document left behind scores above `bound`.
pub struct Ranked(Option<Windows>);

/// The windows holding every matching term, each with the best score a document in it can have, best first, and how far they have been handed out.
struct Windows { file: Arc<SearchFile>, field: Field, matching: Query, scoring: Arc<Scorer>, bounds: TemporalBounds, windows: Vec<(f64, u32)>, next: usize, batch: usize }

impl Ranked {
    /// The highest score any document not yet handed out can have, or `None` once all are.
    pub fn bound(&self) -> Option<f64> { self.0.as_ref().and_then(|windows| windows.windows.get(windows.next).map(|(bound, _)| *bound)) }

    /// The matching visible documents of the next windows, with their exact scores; twice as many windows as the time before.
    pub fn expand(&mut self) -> Result<Vec<(DocMatch, f64)>> { self.expand_in(None, &mut 0) }

    pub(crate) fn expand_in(&mut self, scope: Option<&BTreeSet<Iid>>, examined: &mut usize) -> Result<Vec<(DocMatch, f64)>> {
        self.0.as_mut().map_or(Ok(Vec::new()), |windows| windows.expand(scope, examined))
    }

    /// The file the documents belong to.
    pub fn file(&self) -> Option<&Arc<SearchFile>> { self.0.as_ref().map(|windows| &windows.file) }
}

impl Windows {
    fn expand(&mut self, scope: Option<&BTreeSet<Iid>>, examined: &mut usize) -> Result<Vec<(DocMatch, f64)>> {
        use tantivy::{DocSet, query::Query as _};
        let end = self.next.saturating_add(self.batch).min(self.windows.len());
        let mut windows: Vec<u32> = self.windows[self.next..end].iter().map(|(_, window)| *window).collect();
        self.next = end;
        self.batch = self.batch.saturating_mul(2);
        windows.sort_unstable();
        let searcher = &self.file.searcher;
        let segment = &searcher.segment_readers()[0];
        let weight = SearchFile::conjunction(self.field, &self.matching).weight(tantivy::query::EnableScoring::disabled_from_searcher(searcher)).map_err(fault)?;
        let mut docs = weight.scorer(segment, 1.0).map_err(fault)?;
        let mut scorer = ExactScorer::open(segment, self.field, Arc::clone(&self.scoring))?;
        let mut found = Vec::new();
        for window in windows {
            let (start, stop) = document_range(window);
            let mut doc = if docs.doc() < start { docs.seek(start) } else { docs.doc() };
            while doc < stop && doc != tantivy::TERMINATED {
                if cfg!(feature = "experiments") { *examined += 1; }
                if scope.is_none_or(|scope| scope.contains(&Iid(self.file.sidecar.doc_iid(doc)))) && self.file.visible(doc, self.bounds) {
                    found.push((self.file.sidecar.doc_match(doc), scorer.score(doc)));
                }
                doc = docs.advance();
            }
        }
        Ok(found)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_text::tokens;

    fn all() -> TemporalBounds { TemporalBounds { valid_time: Interval::ALL, system_time: Interval::ALL } }
    fn at(valid: i64, system: i64) -> TemporalBounds {
        TemporalBounds { valid_time: Interval::from_micros(valid, valid + 1).unwrap(), system_time: Interval::from_micros(system, system + 1).unwrap() }
    }
    fn row(id: u8, system_from: i64, title: &str, body: &str) -> SearchRow {
        SearchRow { identity: None, iid: [id; 16], system_from, valid_from: 0, valid_to: i64::MAX, op: EventOp::Put, values: vec![Some(tokens(title)), Some(tokens(body))] }
    }
    fn columns() -> Vec<String> { vec!["title".into(), "body".into()] }
    fn rows() -> Vec<SearchRow> {
        vec![row(1, 30, "Invoice", "the invoice is overdue"), row(1, 20, "Invoice", "the invoice is paid"), row(1, 10, "Invoice", "the invoice is overdue"), row(2, 20, "Library", "an overdue book"), row(3, 20, "", "")]
    }
    fn open(rows: &[SearchRow]) -> Arc<SearchFile> { Arc::new(SearchFile::open(build_search_file(&columns(), rows, "job").unwrap(), &columns()).unwrap()) }
    /// A row's run, by the rows' order in the file.
    fn run(file: &SearchFile, row: u32) -> Option<u32> { match file.sidecar.row_doc(1, row) { RowDoc::Run(doc) => Some(doc), _ => None } }

    #[test]
    fn range_reads_preserve_ranked_results_without_requesting_the_entire_bundle() {
        #[derive(Debug)]
        struct Tracked {
            data: OwnedBytes,
            reads: std::sync::Mutex<Vec<Range<usize>>>,
        }

        impl HasLen for Tracked {
            fn len(&self) -> usize { self.data.len() }
        }

        #[async_trait::async_trait]
        impl FileHandle for Tracked {
            fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
                self.reads.lock().unwrap().push(range.clone());
                Ok(self.data.slice(range))
            }
        }

        let data = build_search_file(&columns(), &rows(), "ranges").unwrap();
        let full = SearchFile::open(data.clone(), &columns()).unwrap();
        let tracked = Arc::new(Tracked { data: OwnedBytes::new(data), reads: Default::default() });
        let ranged = SearchFile::open_with_ranges(tracked.clone(), &columns()).unwrap();
        let query = Query::parse("invoice");
        let score = |file: &SearchFile| file.scored("body", &query, &query, None, all())
            .unwrap().into_iter().map(|(hit, score)| (hit.iid, hit.doc, score)).collect::<Vec<_>>();
        assert_eq!(score(&ranged), score(&full));
        let reads = tracked.reads.lock().unwrap();
        assert!(reads.contains(&(tracked.len() - 16..tracked.len())));
        assert!(reads.iter().all(|range| range.len() < tracked.len()));
    }

    #[test]
    fn published_runs_keep_exact_lengths_and_definition_identity() {
        let columns = vec!["body".to_string()];
        let tokens = vec!["term".to_string(); 79];
        let row = SearchRow { identity: None, iid: [1; 16], system_from: 1, valid_from: 1, valid_to: 10, op: EventOp::Put, values: vec![Some(tokens)] };
        let mut bytes = Vec::new();
        write_versioned_group(&columns, [[Ok(vec![row])]], "owned-file", 13, None, RamDirectory::create(), &mut bytes).unwrap();
        let file = SearchFile::open(bytes, &columns).unwrap();
        assert!(file.matches_artefact("owned-file", 13));
        assert!(!file.matches_artefact("other-file", 13));
        assert!(!file.matches_artefact("owned-file", 14));
        assert_eq!(file.token_length(0).unwrap(), 79);
        assert_eq!(file.stored_length("body", 0).unwrap(), 76);
        assert!(file.token_length(1).is_err());
    }

    #[test]
    fn an_owned_run_round_trips_its_full_typed_identity() {
        let columns = vec!["body".to_string()];
        let id = EntityId::String("complete-id".into());
        let row = SearchRow { identity: Some(id.clone()), iid: id.iid().0, system_from: 1, valid_from: 1, valid_to: 10,
            op: EventOp::Put, values: vec![Some(tokens("invoice"))] };
        let bytes = build_search_file(&columns, &[row], "owned-run").unwrap();
        let file = SearchFile::open(bytes, &columns).unwrap();
        let run = file.matches("body", &Query::parse("invoice"), at(2, 2)).unwrap()[0];
        assert_eq!(file.run_identity(run).unwrap(), id);
    }

    #[test]
    fn a_job_builds_the_same_bytes_every_time() {
        let first = build_search_file(&columns(), &rows(), "public$docs/l01-rc-b00/docs_text").unwrap();
        assert_eq!(first, build_search_file(&columns(), &rows(), "public$docs/l01-rc-b00/docs_text").unwrap());
        assert_ne!(first, build_search_file(&columns(), &rows(), "public$docs/l01-rc-b01/docs_text").unwrap());
    }

    #[test]
    fn a_segment_built_on_disk_bundles_to_the_same_bytes_as_one_built_in_memory() {
        let directory = tempfile::tempdir().unwrap();
        let mut streamed = Vec::new();
        write_search_file(&columns(), [Ok(rows())], "job", None, tantivy::directory::MmapDirectory::open(directory.path()).unwrap(), &mut streamed).unwrap();
        assert_eq!(streamed, build_search_file(&columns(), &rows(), "job").unwrap());
    }

    #[test]
    fn an_entitys_versions_with_the_same_text_are_one_document_visible_where_any_of_them_is() {
        let file = open(&rows());
        let (first, second, third) = (run(&file, 0).unwrap(), run(&file, 1).unwrap(), run(&file, 2).unwrap());
        assert!(first == third && first != second, "the first and third versions share their text");
        assert_eq!(file.sidecar.row_doc(0, 0), file.sidecar.row_doc(0, 1), "every version shares its title, so the title is one run");
        let overdue: Vec<u32> = file.rectangles(first).map(|rectangle| rectangle[2]).map(|system| system as u32).collect();
        assert_eq!(overdue, [10, 30], "visible from system time 10 to 20, and from 30 on");
        let query = Query::parse("overdue");
        for (system, expected) in [(5, vec![]), (15, vec![1]), (25, vec![2]), (35, vec![1, 2])] {
            let found: Vec<u8> = file.matches("body", &query, at(1, system)).unwrap().iter().map(|doc| doc.iid[0]).collect();
            assert_eq!(found, expected, "at system time {system}");
        }
        assert_eq!(file.matches("body", &Query::parse("paid"), at(1, 25)).unwrap().len(), 1);
        assert!(file.matches("body", &Query::parse("paid"), at(1, 35)).unwrap().is_empty(), "superseded by system time 30");
        assert_eq!(file.matches("body", &Query::parse(r#""invoice is overdue""#), all()).unwrap().len(), 1);
        assert!(file.matches("body", &Query::parse(r#""overdue invoice""#), all()).unwrap().is_empty(), "the terms are there, but not side by side");
        assert!(file.matches("absent", &query, all()).unwrap().is_empty());
        assert!(file.matches("body", &Query::parse("!!"), all()).unwrap().is_empty());
    }

    #[test]
    fn a_column_shares_postings_while_its_versions_keep_their_system_start() {
        let rows: Vec<SearchRow> = (0..50).map(|version| row(1, 100 - version, &format!("title {version}"), "the same body")).collect();
        let file = open(&rows);
        let body = run(&file, 0).unwrap();
        assert!((0..50).all(|row| run(&file, row) == Some(body)), "the body is one run across fifty title changes");
        let expected: Vec<_> = (51..100).map(|s| [0, i64::MAX, s, s + 1])
            .chain([[0, i64::MAX, 100, i64::MAX]]).collect();
        assert_eq!(file.rectangles(body).collect::<Vec<_>>(), expected);
        assert_eq!(file.corpus("body", &Query::parse("same")).unwrap().documents, 1);
        assert_eq!(file.corpus("title", &Query::parse("title")).unwrap().documents, 50);
        assert_eq!(
            coalesce(vec![[0, 5, 10, 20], [5, 9, 10, 20], [0, 5, 20, 30], [7, 8, 1, 2]]),
            [[7, 8, 1, 2], [0, 9, 10, 20], [0, 5, 20, 30]],
        );
    }

    #[test]
    fn a_version_superseded_within_the_file_everywhere_has_no_document_and_deletes_have_none() {
        let mut rows = vec![row(1, 20, "", "now"), row(1, 20, "", "same instant, earlier in the transaction")];
        rows.push(SearchRow { op: EventOp::Delete, ..row(2, 30, "", "") });
        rows.push(row(2, 10, "", "deleted later"));
        let file = open(&rows);
        assert_eq!((run(&file, 0).is_some(), run(&file, 1), run(&file, 2), run(&file, 3).is_some()), (true, None, None, true));
        assert!(file.matches("body", &Query::parse("deleted"), at(1, 35)).unwrap().is_empty());
        assert_eq!(file.matches("body", &Query::parse("deleted"), at(1, 15)).unwrap().len(), 1);
        assert_eq!(file.events(&[2; 16]).unwrap(), [KeyedEvent { system_from: 30, valid_from: 0, valid_to: i64::MAX, op: EventOp::Delete }, KeyedEvent { system_from: 10, valid_from: 0, valid_to: i64::MAX, op: EventOp::Put }]);
        assert!(file.events(&[7; 16]).unwrap().is_empty());
    }

    #[test]
    fn a_documents_score_from_the_index_is_its_rows_score() {
        let rows = rows();
        let file = open(&rows);
        let scoring = Query::parse("invoice overdue paid absent");
        let corpus = file.corpus("body", &scoring).unwrap();
        let tokens_of = |doc: u32| (0..rows.len() as u32).find(|row| run(&file, *row) == Some(doc)).map(|row| rows[row as usize].values[1].clone().unwrap()).unwrap();
        for (doc, score) in file.scored("body", &Query::parse("invoice"), &scoring, Some(&corpus), all()).unwrap() {
            assert_eq!(score, rtdb_text::bm25(&scoring, &tokens_of(doc.doc), Some(&corpus)), "doc {}", doc.doc);
        }
        let long: Vec<String> = (0..500).map(|index| if index % 7 == 0 { "invoice".into() } else { format!("w{index}") }).collect();
        let file = open(&[SearchRow { values: vec![None, Some(long.clone())], ..row(9, 0, "", "") }]);
        let [(_, score)] = file.scored("body", &Query::parse("invoice"), &Query::parse("invoice"), None, all()).unwrap()[..] else { panic!() };
        assert_eq!(score, rtdb_text::bm25(&Query::parse("invoice"), &long, None), "a long document's stored length is rounded the same way");
    }

    #[test]
    fn corpus_statistics_count_runs_tokens_and_term_frequencies() {
        let corpus = open(&rows()).corpus("body", &Query::parse("overdue invoice")).unwrap();
        assert_eq!(corpus.documents, 4);
        assert_eq!(corpus.tokens, 11);
        assert_eq!(corpus.frequencies, BTreeMap::from([("overdue".into(), 2), ("invoice".into(), 2)]));
    }

    #[test]
    fn an_empty_trie_and_a_corrupt_object_are_told_apart() {
        assert!(open(&[]).matches("body", &Query::parse("anything"), all()).unwrap().is_empty());
        let mut bytes = build_search_file(&columns(), &rows(), "job").unwrap();
        let length = bytes.len();
        bytes[length - 1] ^= 1;
        assert!(SearchFile::open(bytes, &columns()).is_err());
        assert!(SearchFile::open(build_search_file(&columns(), &rows(), "job").unwrap(), &["body".to_string()]).is_err(), "a definition with other columns does not read the file");
        assert!(build_search_file(&columns(), &[row(2, 0, "", ""), row(1, 0, "", "")], "job").is_err(), "rows out of IID order");
        assert!(build_search_file(&columns(), &[row(1, 0, "", ""), row(1, 5, "", "")], "job").is_err(), "an entity's rows out of system-time order");
    }

    /// Entities of twenty versions each, IID ascending and system time descending, every version valid from 0 on.
    fn corpus_rows(count: usize, seed: u64) -> Vec<SearchRow> {
        let words = ["invoice", "overdue", "paid", "book", "late", "red", "blue", "green"];
        let mut state = seed;
        let mut next = || { state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (state >> 33) as usize };
        (0..count).map(|index| {
            let body: Vec<String> = (0..1 + next() % 12).map(|_| words[next() % words.len()].to_string()).collect();
            let value = match index % 17 { 0 => None, 1 => Some(Vec::new()), _ => Some(body) };
            SearchRow { identity: None, iid: [(index / 20) as u8; 16], system_from: 1_000_000 - index as i64, valid_from: 0, valid_to: i64::MAX, op: EventOp::Put, values: vec![None, value] }
        }).collect()
    }

    #[test]
    fn ranked_windows_bound_every_match_and_hand_each_out_once() {
        let rows = corpus_rows(5000, 7);
        let file = open(&rows);
        assert!(file.sidecar.impacts.keys().any(|(column, _)| *column == 1), "common terms have their impacts stored");
        for bounds in [all(), at(1, 999_000)] {
            for (matching, scoring) in [("invoice", "invoice overdue"), ("red blue", "red"), ("\"paid late\"", "paid late green"), ("absent", "absent")] {
                let (matching, scoring) = (Query::parse(matching), Query::parse(scoring));
                let corpus = file.corpus("body", &scoring).unwrap();
                let mut expected = file.scored("body", &matching, &scoring, Some(&corpus), bounds).unwrap();
                let rank_scorer = Arc::new(Scorer::new(scoring.clone(), Some(&corpus))).for_ranking().unwrap();
                let mut ranked = file.ranked("body", &matching, &rank_scorer, bounds).unwrap();
                let windows: BTreeMap<u32, f64> = ranked.0.as_ref().map_or_else(BTreeMap::new, |windows| windows.windows.iter().map(|(bound, window)| (*window, *bound)).collect());
                for (doc, score) in &expected {
                    let bound = windows.get(&window_of(doc.doc)).unwrap_or_else(|| panic!("{matching:?}: doc {} is in no window", doc.doc));
                    assert!(score <= bound, "{matching:?}: doc {} scores {score} above its window's bound {bound}", doc.doc);
                }
                let mut handed = Vec::new();
                let mut expansions = 0;
                while let Some(bound) = ranked.bound() {
                    let batch = ranked.expand().unwrap();
                    expansions += 1;
                    assert!(batch.iter().all(|(_, score)| *score <= bound), "{matching:?}: a document scored above the bound before it");
                    handed.extend(batch);
                }
                assert!(ranked.expand().unwrap().is_empty());
                handed.sort_by_key(|(doc, _)| doc.doc);
                expected.sort_by_key(|(doc, _)| doc.doc);
                assert_eq!(handed, expected, "{matching:?}");
                if expected.len() > 200 { assert!(expansions > 1, "{matching:?} was handed out in one batch"); }
            }
        }
    }

    #[test]
    fn match_values_tell_a_null_column_from_one_that_does_not_match_by_page_and_row() {
        let rows = corpus_rows(300, 3);
        let pages: Vec<Vec<SearchRow>> = rows.chunks(70).map(<[SearchRow]>::to_vec).collect();
        let mut bytes = Vec::new();
        write_search_file(&columns(), pages.iter().cloned().map(Ok), "job", None, RamDirectory::create(), &mut bytes).unwrap();
        let file = Arc::new(SearchFile::open(bytes, &columns()).unwrap());
        let query = Query::parse("invoice");
        let set = file.match_set("body", &query).unwrap();
        for (page, rows) in pages.iter().enumerate() {
            for (row, search_row) in rows.iter().enumerate() {
                let expected = search_row.values[1].as_ref().map(|tokens| query.matches(tokens));
                assert_eq!(set.value(page, row).unwrap(), expected, "page {page} row {row}");
            }
            assert!(set.value(page, rows.len()).is_err() || page + 1 < pages.len(), "past the last row of the last page");
        }
        assert!(set.value(pages.len(), 0).is_err());
        assert!(file.match_set("title", &query).unwrap().value(0, 0).unwrap().is_none(), "a column no row holds is null everywhere");
    }

    #[test]
    fn selected_matches_check_phrases_and_leave_unselected_rows_unanswered() {
        let mut rows = vec![
            row(1, 0, "", "overdue invoice"),
            row(2, 0, "", "invoice overdue"),
            row(3, 0, "", "overdue invoice"),
            row(4, 0, "", "missing"),
        ];
        rows[3].values[1] = None;
        let file = open(&rows);
        let query = Query::parse("\"overdue invoice\"");
        let iids = BTreeSet::from([Iid(rows[0].iid), Iid(rows[1].iid), Iid(rows[3].iid)]);
        let selected = file.match_selected_iids("body", &query, &iids).unwrap();
        assert_eq!(selected.value(0, 0).unwrap(), Some(true));
        assert_eq!(selected.value(0, 1).unwrap(), Some(false));
        assert!(selected.value(0, 2).is_err());
        assert_eq!(selected.value(0, 3).unwrap(), None);
    }

    #[test]
    fn a_word_longer_than_tantivy_keeps_is_matched_and_scored_as_the_row_is() {
        let long = format!("{} overdue", "x".repeat(70_000));
        let rows = vec![row(1, 0, "", &long), row(2, 0, "", "overdue")];
        let file = open(&rows);
        for query in [Query::parse(&"x".repeat(70_000)), Query::parse("overdue")] {
            let corpus = file.corpus("body", &query).unwrap();
            let expected: Vec<u8> = rows.iter().filter(|row| query.matches(row.values[1].as_deref().unwrap())).map(|row| row.iid[0]).collect();
            let scored = file.scored("body", &query, &query, Some(&corpus), all()).unwrap();
            assert_eq!(scored.iter().map(|(doc, _)| doc.iid[0]).collect::<Vec<_>>(), expected);
            for (doc, score) in scored { assert_eq!(score, rtdb_text::bm25(&query, rows[doc.iid[0] as usize - 1].values[1].as_deref().unwrap(), Some(&corpus))); }
        }
    }

    #[test]
    fn a_group_file_keeps_each_members_runs_and_pages_with_that_member() {
        use rtdb_query::FileMatches;

        let first = vec![Ok(vec![row(1, 30, "", "overdue"), row(1, 20, "", "paid")]),
                         Ok(vec![row(1, 10, "", "overdue")])];
        let third = vec![Ok(vec![row(128, 30, "", "overdue"),
                                 SearchRow { values: vec![None, None], ..row(129, 30, "", "") }])];
        let mut bytes = Vec::new();
        write_group_search_file(&columns(), [first, Vec::new(), third], "group", None, RamDirectory::create(), &mut bytes).unwrap();
        let file = Arc::new(SearchFile::open(bytes, &columns()).unwrap());
        let query = Query::parse("overdue");
        let selected = |bounds| file.matches("body", &query, bounds).unwrap().into_iter()
            .map(|doc| (doc.member, doc.iid[0])).collect::<Vec<_>>();
        assert!(selected(at(1, 25)).is_empty());
        assert_eq!(selected(at(1, 35)), [(0, 1), (2, 128)]);
        assert_eq!(file.events(&[1; 16]).unwrap().into_iter().map(|key| key.system_from).collect::<Vec<_>>(), [30, 20, 10]);
        assert_eq!(file.events(&[128; 16]).unwrap().len(), 1);

        let set = Arc::new(file.match_set("body", &query).unwrap());
        let (first, empty, third) = (set.member(0).unwrap(), set.member(1).unwrap(), set.member(2).unwrap());
        assert_eq!((first.value(0, 0).unwrap(), first.value(0, 1).unwrap(), first.value(1, 0).unwrap()), (Some(true), Some(false), Some(true)));
        assert!(first.value(2, 0).is_err());
        assert!(empty.value(0, 0).is_err());
        assert_eq!((third.value(0, 0).unwrap(), third.value(0, 1).unwrap()), (Some(true), None));
        assert!(set.member(3).is_err());

        let rank_scorer = Arc::new(Scorer::new(query.clone(), None)).for_ranking().unwrap();
        let mut ranked = file.ranked("body", &query, &rank_scorer, at(1, 35)).unwrap();
        let mut handed = Vec::new();
        while ranked.bound().is_some() {
            handed.extend(ranked.expand().unwrap().into_iter().map(|(doc, _)| (doc.member, doc.iid[0])));
        }
        handed.sort_unstable();
        assert_eq!(handed, [(0, 1), (2, 128)]);
    }

    #[test]
    fn a_sidecar_whose_bytes_changed_is_unreadable() {
        let bytes = build_search_file(&columns(), &corpus_rows(2000, 1), "job").unwrap();
        let directory = BundleDirectory::open(FileSlice::from(Source::from(bytes.clone()))).unwrap();
        let range = directory.files[Path::new(SIDECAR)].clone();
        for offset in [range.start, range.start + range.len() / 2, range.end - 1] {
            let mut corrupted = bytes.clone();
            corrupted[offset] ^= 0x10;
            assert!(SearchFile::open(corrupted, &columns()).is_err(), "byte {offset}");
        }
    }

    #[test]
    fn documents_fall_into_lifespan_classes_whose_bounds_say_where_none_can_be_visible() {
        let day = 86_400_000_000i64;
        let rows = vec![
            SearchRow { iid: [1; 16], system_from: 10 * day, ..row(1, 0, "", "short lived") },
            SearchRow { iid: [1; 16], system_from: 9 * day, ..row(1, 0, "", "superseded the next day") },
            SearchRow { iid: [2; 16], system_from: 400 * day, ..row(2, 0, "", "long lived") },
            SearchRow { iid: [2; 16], system_from: 0, ..row(2, 0, "", "superseded after 400 days") },
        ];
        let mut bytes = Vec::new();
        let sections = write_search_file(&columns(), [Ok(rows)], "job", Some(1000 * day), RamDirectory::create(), &mut bytes).unwrap();
        let file = SearchFile::open(bytes, &columns()).unwrap();
        assert_eq!(file.sections(), sections.as_slice());
        let class = |text: &str| sections.iter().find(|section| section.class == file.sidecar.data()[file.sidecar.doc_classes + file.matches("body", &Query::parse(text), all()).unwrap()[0].doc as usize]).copied().unwrap();
        let (short, long, open) = (class("next day"), class("400 days"), class("long lived"));
        assert_eq!((short.class, long.class, open.class), (0, 3, 4), "four hundred days is under 64 weeks; the open run, clipped to the ceiling at 1000 days, under 256");
        assert_eq!((short.birth, short.death), (9 * day, 10 * day));
        assert!(short.meets(at(9 * day + 1, 9 * day + 1)) && !short.meets(at(20 * day, 20 * day)), "a version alive for one day is visible only around it");
        assert!(!short.meets(at(5 * day, 5 * day)), "nor before it was born");
        let later_valid = TemporalBounds { valid_time: Interval::from_micros(500 * day, 500 * day + 1).unwrap(), system_time: Interval::from_micros(9 * day + day / 2, 9 * day + day / 2 + 1).unwrap() };
        assert!(short.meets(later_valid), "visible at a far later valid time, as known during its one day");
        assert!(!open.meets(at(1000 * day, 1000 * day)), "the ceiling ends every version");
        assert_eq!(decode_sections(&encode_sections(&sections)), Some(sections.clone()));
        assert_eq!(decode_sections(&[1, 2, 3]), None);
    }
}
