//! A search file: one single-segment tantivy index over a trie or its partition group, bundled into one object.
//! A document is a text run: every version of one entity whose indexed values are the same, with the rectangles of valid time × system time where one of them is visible.
//! Tokens come from `rtdb-text`, so the index and the residual `MATCH` agree.

use std::collections::BTreeMap;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rtdb_types::{Anomaly, EntityId, EventHeader, EventOp, Iid, Interval, Result, TemporalBounds, Timestamp};
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, FileSlice, OwnedBytes, WatchCallback, WatchHandle, WritePtr};
use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::store::Compressor;
use tantivy::tokenizer::{PreTokenizedString, Token};
use tantivy::{Directory, HasLen, Index, IndexSettings, TantivyDocument};

use crate::member_bounds::{MemberStart, document_member};
use crate::rank_windows::{extend_impact, window_of};
use crate::rectangle::merge_adjacent_valid_time;

const MAGIC: &[u8; 8] = b"RTSRCH03";
pub(super) fn is_bundle(bytes: &[u8]) -> bool { bytes.starts_with(MAGIC) }
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

use super::SearchRow;

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

pub(super) fn encode_catalog(sections: &[super::Section]) -> Vec<u8> {
    encode_sections(&sections.iter().map(|section| Section {
        class: section.class, valid_from: section.valid_from, valid_to: section.valid_to,
        system_from: section.system_from, system_to: section.system_to, birth: section.birth, death: section.death,
    }).collect::<Vec<_>>())
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

fn op_code(op: EventOp) -> u8 { match op { EventOp::Put => 0, EventOp::Delete => 1, EventOp::Erase => 2, EventOp::Redacted => 3 } }
fn op_of(code: u8) -> Result<EventOp> { match code { 0 => Ok(EventOp::Put), 1 => Ok(EventOp::Delete), 2 => Ok(EventOp::Erase), 3 => Ok(EventOp::Redacted), _ => Err(corrupt("Search file key has no such operation")) } }

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
pub struct SearchFile { searcher: tantivy::Searcher, sidecar: Sidecar }

struct BundleBytes(bytes::Bytes);

impl std::ops::Deref for BundleBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] { &self.0 }
}

// SAFETY: Bytes retains its immutable backing allocation when the wrapper moves.
unsafe impl stable_deref_trait::StableDeref for BundleBytes {}

impl SearchFile {
    fn open(bytes: bytes::Bytes, columns: &[String]) -> Result<Self> {
        Self::open_slice(FileSlice::from(BundleBytes(bytes)), columns)
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
        Ok(Self { sidecar: Sidecar::parse(sidecar, columns.len(), documents)?, searcher: reader.searcher() })
    }

}

pub(super) fn decode(bytes: bytes::Bytes, columns: &[String]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    decode_to(bytes, columns, &mut output, None)?;
    Ok(output)
}

pub(super) fn decode_to(bytes: bytes::Bytes, columns: &[String], output: impl Write,
    memory: Option<&Arc<rtdb_columnar::MemoryBudget>>) -> Result<()> {
    use arrow_array::{ArrayRef, FixedSizeBinaryArray, Int32Array, Int64Array, StringArray, UInt32Array, UInt64Array, builder::StringBuilder};
    use tantivy::{DocSet, postings::Postings as _};
    use super::{LEGACY_FORMAT_VERSION, layout::{Table, Writer, item_of, list}, postings::{POSTING_TERMS, PostingBuilder, posting_table}, write::{Events, Runs}};

    let file = SearchFile::open(bytes, columns)?;
    let sidecar = &file.sidecar;
    let segment = file.searcher.segment_readers().first();
    let inverted = columns.iter().enumerate().map(|(column, _)| {
        segment.map(|segment| segment.inverted_index(Field::from_field_id(column as u32)).map_err(fault)).transpose()
    }).collect::<Result<Vec<_>>>()?;
    let tokens = inverted.iter().flatten().map(|index| index.total_num_tokens()).fold(0u64, u64::saturating_add);
    let bytes = sidecar.data().len().saturating_mul(4)
        .saturating_add(usize::try_from(tokens).unwrap_or(usize::MAX).saturating_mul(20))
        .saturating_add(4096);
    let _scratch = memory.map(|memory| memory.reserve(bytes)).transpose()?;
    let mut writer = Writer::new(output, LEGACY_FORMAT_VERSION)?;
    let header = Table::Header.array(vec![
        Arc::new(Int32Array::from(vec![LEGACY_FORMAT_VERSION])), Arc::new(Int32Array::from(vec![rtdb_text::ANALYZER_VERSION])),
        Arc::new(Int64Array::from(vec![sidecar.version])), Arc::new(FixedSizeBinaryArray::try_from_iter([sidecar.identity].iter()).map_err(fault)?),
    ])?;
    let members = Table::Members.array(vec![
        Arc::new(UInt32Array::from_iter_values(sidecar.member_starts.iter().map(|member| member.page))),
        Arc::new(UInt32Array::from_iter_values(sidecar.member_starts.iter().map(|member| member.doc))),
    ])?;
    let pages = Table::Pages.array(vec![Arc::new(UInt32Array::from(sidecar.page_starts.clone()))])?;
    let mut events = Events::new();
    for row in 0..sidecar.rows {
        let header = sidecar.key(row)?;
        let iid = Iid(sidecar.row_iid(row).try_into().map_err(fault)?);
        let key = sidecar.row_identity(row);
        events.push(EventHeader { iid, system_from: Timestamp(header.system_from), commit_position: 0,
            valid_time: Interval::new(Timestamp(header.valid_from), Timestamp(header.valid_to))?, op: header.op },
            (!key.is_empty()).then_some(key))?;
    }
    let mut runs = Runs::new();
    for doc in 0..sidecar.documents {
        let position = sidecar.identity_starts + doc as usize * 4;
        let first = sidecar.identities + sidecar.u32_at(position) as usize;
        let last = sidecar.identities + sidecar.u32_at(position + 4) as usize;
        let length = sidecar.u32_at(sidecar.token_lengths + doc as usize * 4);
        runs.push(Iid(sidecar.doc_iid(doc)), (first != last).then(|| &sidecar.data()[first..last]), length,
            sidecar.data()[sidecar.doc_classes + doc as usize], sidecar.rectangles(doc).collect())?;
    }
    let runs_for_rows = UInt32Array::from_iter_values((0..columns.len()).flat_map(|column| {
        (0..sidecar.rows).map(move |row| match sidecar.row_doc(column, row) {
            RowDoc::Invisible => NO_DOC,
            RowDoc::Null => NULL_DOC,
            RowDoc::Run(doc) => doc,
        })
    }));
    let row_runs = Table::RowRuns.array(vec![list(Arc::new(runs_for_rows),
        std::iter::repeat_n(sidecar.rows as usize, columns.len()), item_of(&Table::RowRuns.fields()[0]))?])?;
    let mut terms = StringBuilder::new();
    let mut term_columns = Vec::new();
    let mut frequencies = Vec::new();
    for (column, index) in inverted.iter().enumerate() {
        let Some(index) = index else { continue; };
        let mut stream = index.terms().stream().map_err(fault)?;
        while stream.advance() {
            terms.append_value(std::str::from_utf8(stream.key()).map_err(fault)?);
            term_columns.push(column as u32);
            frequencies.push(stream.value().doc_freq);
        }
    }
    let count = frequencies.len();
    let term_table = Table::Terms.array(vec![
        Arc::new(UInt32Array::from(term_columns)), Arc::new(terms.finish()),
        Arc::new(UInt32Array::from_iter_values((0..count).map(|index| (index / POSTING_TERMS + 1) as u32))),
        Arc::new(UInt32Array::from_iter_values((0..count).map(|index| (index % POSTING_TERMS) as u32))),
        Arc::new(UInt32Array::from(frequencies)),
    ])?;
    let column_table = Table::Columns.array(vec![Arc::new(StringArray::from_iter_values(columns)) as ArrayRef,
        Arc::new(UInt32Array::from(sidecar.column_docs.clone())), Arc::new(UInt64Array::from_iter_values(inverted.iter().map(|index| index.as_ref().map_or(0, |index| index.total_num_tokens()))))])?;
    let sections = sidecar.sections.iter().copied().map(Section::into_arrow).collect::<Vec<_>>();
    writer.page([(Table::Header, header), (Table::Members, members), (Table::Pages, pages), (Table::Columns, column_table),
        (Table::Events, events.finish()?.0), (Table::Runs, runs.finish()?), (Table::RowRuns, row_runs),
        (Table::Terms, term_table), (Table::Sections, super::sections_array(&sections)?)])?;
    let mut groups = Vec::with_capacity(POSTING_TERMS);
    for index in inverted.iter().flatten() {
        let mut stream = index.terms().stream().map_err(fault)?;
        while stream.advance() {
            let mut postings = index.read_postings_from_terminfo(stream.value(), IndexRecordOption::WithFreqsAndPositions).map_err(fault)?;
            let mut builder = PostingBuilder::default();
            let mut positions = Vec::new();
            while postings.doc() != tantivy::TERMINATED {
                let doc = postings.doc();
                if doc >= sidecar.documents { return Err(corrupt("Legacy search posting exceeds its document count")); }
                let length = rtdb_text::stored_length(sidecar.u32_at(sidecar.token_lengths + doc as usize * 4) as usize);
                postings.positions(&mut positions);
                builder.push(doc, length, std::mem::take(&mut positions), None);
                postings.advance();
            }
            groups.push(builder);
            if groups.len() == POSTING_TERMS {
                writer.page([(Table::Postings, posting_table(std::mem::take(&mut groups), STORED_IMPACTS as usize)?)])?;
            }
        }
    }
    if !groups.is_empty() {
        writer.page([(Table::Postings, posting_table(groups, STORED_IMPACTS as usize)?)])?;
    }
    writer.finish()?;
    Ok(())
}

impl Section {
    pub(super) fn into_arrow(self) -> super::Section {
        super::Section { class: self.class, valid_from: self.valid_from, valid_to: self.valid_to,
            system_from: self.system_from, system_to: self.system_to, birth: self.birth, death: self.death }
    }
}
