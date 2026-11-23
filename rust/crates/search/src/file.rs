mod layout;
mod legacy11;
mod manifest;
mod paged;
mod postings;
mod page_tree;
pub(crate) use page_tree::{IoStats as TreeIoStats, observe_io as observe_tree_io};
mod tree_file;
mod tree_rank;
mod rank_tree;
mod write;

#[cfg(feature = "experiments")]
pub(crate) mod experiment;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Write};
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use arrow_array::{
    Array, BooleanArray, Float64Array, RecordBatch, StringArray, StructArray, UInt8Array, UInt32Array, Int64Array,
    cast::AsArray, types::{Int64Type, UInt8Type},
};
use arrow_buffer::{BooleanBufferBuilder, Buffer};
use arrow_schema::Schema;
use rtdb_text::{Corpus, Query, RankScorer, Scorer, stored_length};
use rtdb_types::{Anomaly, EntityId, EventHeader, EventOp, Iid, Interval, Result, TemporalBounds, Timestamp};
use sha2::{Digest, Sha256};

use crate::rank_windows::document_range;
use layout::Reader;
use manifest::{Manifest, Term, postings_table};
use postings::{Impact, Matching, Postings, needs_positions};
pub use layout::Source;
pub(crate) use layout::FooterLocator;

pub const FORMAT_VERSION: i32 = 13;
pub(crate) const TREE_FORMAT_VERSION: i32 = 14;
pub(crate) const PACKED_FORMAT_VERSION: i32 = 15;
pub(crate) const LOGICAL_FORMAT_VERSION: i32 = 16;
pub(crate) type TextRevision = [u8; 32];
pub(crate) use page_tree::PageCache;
pub(crate) use tree_file::{MAX_ROOT_BYTES, TreeFile, TreeLocator};
pub(crate) use tree_file::LogicalDocument;
pub(crate) use tree_rank::{TreeRankOrder, TreeRanked};
pub(crate) use write::{BuildMode, write_group_as};
pub(crate) fn encode_tantivy_stats(sections: &[Section]) -> Vec<u8> { legacy11::encode_catalog(sections) }
pub(crate) const LEGACY_FORMAT_VERSION: i32 = 12;
pub(crate) const TANTIVY_FORMAT_VERSION: i32 = 11;
const STORED_IMPACTS: usize = 1024;
const NO_DOC: u32 = u32::MAX;
const NULL_DOC: u32 = u32::MAX - 1;

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

pub(crate) struct IndexedRow<'a> {
    pub header: EventHeader,
    pub key: Option<Cow<'a, [u8]>>,
    pub sql_id: Option<i64>,
    pub values: Vec<Option<StringArray>>,
    pub revisions: Vec<Option<TextRevision>>,
}

pub(crate) trait SearchPage {
    fn len(&self) -> usize;
    fn row(&self, row: usize, columns: &[String]) -> Result<IndexedRow<'_>>;
}

impl SearchPage for Vec<SearchRow> {
    fn len(&self) -> usize { Vec::len(self) }

    fn row(&self, row: usize, columns: &[String]) -> Result<IndexedRow<'_>> {
        let row = &self[row];
        if row.values.len() != columns.len() { return Err(fault("Search row values do not cover the definition's columns")); }
        let header = EventHeader {
            iid: Iid(row.iid), system_from: Timestamp(row.system_from), commit_position: 0,
            valid_time: Interval::from_micros(row.valid_from, row.valid_to)?, op: row.op,
        };
        let values = row.values.iter().map(|tokens| tokens.as_ref().map(StringArray::from_iter_values)).collect();
        let sql_id = match &row.identity {
            Some(EntityId::Integer(id)) if EntityId::Integer(*id).iid() == header.iid => Some(*id),
            _ => None,
        };
        Ok(IndexedRow { header, key: row.identity.as_ref().map(|id| Cow::Owned(id.handle_bytes())), sql_id, values, revisions: Vec::new() })
    }
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

impl From<rtdb_types::EventHeader> for KeyedEvent {
    fn from(header: rtdb_types::EventHeader) -> Self {
        Self { system_from: header.system_from.0, valid_from: header.valid_time.lower().0, valid_to: header.valid_time.upper().0, op: header.op }
    }
}

fn fault(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.search/file", error.to_string()) }
fn corrupt(message: &str) -> Anomaly { Anomaly::fault("rtdb.search/corrupt-file", message.to_string()) }

fn op_code(op: EventOp) -> u8 { match op { EventOp::Put => 0, EventOp::Delete => 1, EventOp::Erase => 2 } }
fn op_of(code: u8) -> Result<EventOp> { match code { 0 => Ok(EventOp::Put), 1 => Ok(EventOp::Delete), 2 => Ok(EventOp::Erase), _ => Err(corrupt("Search file key has no such operation")) } }

pub fn meets(rectangle: [i64; 4], bounds: TemporalBounds) -> bool {
    rectangle[0] < bounds.valid_time.upper().0 && bounds.valid_time.lower().0 < rectangle[1] && rectangle[2] < bounds.system_time.upper().0 && bounds.system_time.lower().0 < rectangle[3]
}


fn sections_array(sections: &[Section]) -> Result<StructArray> {
    let instants = |instant: fn(&Section) -> i64| Arc::new(Int64Array::from_iter_values(sections.iter().map(instant)));
    layout::Table::Sections.array(vec![
        Arc::new(UInt8Array::from_iter_values(sections.iter().map(|section| section.class))),
        instants(|section| section.valid_from),
        instants(|section| section.valid_to),
        instants(|section| section.system_from),
        instants(|section| section.system_to),
        instants(|section| section.birth),
        instants(|section| section.death),
    ])
}

fn sections_of(array: &StructArray) -> Result<Vec<Section>> {
    let classes = array.column(0).as_primitive::<UInt8Type>();
    if array.len() > LIFESPAN_WEEKS.len() + 1 || array.columns().iter().any(|column| column.null_count() != 0)
        || classes.values().windows(2).any(|pair| pair[0] >= pair[1])
        || classes.values().last().is_some_and(|&class| class as usize > LIFESPAN_WEEKS.len())
    {
        return Err(corrupt("Search file has invalid lifespan sections"));
    }
    let bounds: [_; 6] = std::array::from_fn(|index| array.column(index + 1).as_primitive::<Int64Type>());
    Ok((0..array.len()).map(|row| {
        let [valid_from, valid_to, system_from, system_to, birth, death] = bounds.map(|column| column.value(row));
        Section { class: classes.value(row), valid_from, valid_to, system_from, system_to, birth, death }
    }).collect())
}

pub fn encode_sections(sections: &[Section]) -> Result<Vec<u8>> {
    encode_catalog_stats(sections, None)
}

const FOOTER_LOCATOR: &str = "rtdb.search.footer";
const ROOT_LOCATOR: &str = "rtdb.search.root";

pub(crate) fn encode_catalog_stats(sections: &[Section], footer: Option<&FooterLocator>) -> Result<Vec<u8>> {
    let metadata = footer.map(|footer| (FOOTER_LOCATOR.to_string(), footer.encode())).into_iter().collect();
    encode_sections_with(sections, metadata)
}

pub(crate) fn encode_tree_stats(sections: &[Section], locator: &TreeLocator) -> Result<Vec<u8>> {
    encode_sections_with(sections, [(ROOT_LOCATOR.to_owned(), locator.encode())].into_iter().collect())
}

fn encode_sections_with(sections: &[Section], metadata: std::collections::HashMap<String, String>) -> Result<Vec<u8>> {
    let sections = sections_array(sections)?;
    let schema = Arc::new(Schema::new_with_metadata(layout::section_fields(), metadata));
    let batch = RecordBatch::try_new(Arc::clone(&schema), sections.columns().to_vec()).map_err(fault)?;
    rtdb_format::ipc::write_arrow_stream(&schema, &[batch], arrow_ipc::writer::IpcWriteOptions::default())
}

pub(crate) fn decode_tree_locator(bytes: &[u8]) -> Option<TreeLocator> {
    let reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes), None).ok()?;
    TreeLocator::decode(reader.schema().metadata().get(ROOT_LOCATOR)?)
}

pub(crate) fn decode_footer_locator(bytes: &[u8]) -> Option<FooterLocator> {
    let reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes), None).ok()?;
    FooterLocator::decode(reader.schema().metadata().get(FOOTER_LOCATOR)?)
}

pub fn decode_sections(bytes: &[u8]) -> Option<Vec<Section>> {
    let mut reader = match arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes), None) {
        Ok(reader) => reader,
        Err(_) => return legacy11::decode_sections(bytes).map(|sections| sections.into_iter().map(legacy11::Section::into_arrow).collect()),
    };
    if reader.schema().fields() != &layout::section_fields() { return None; }
    let batch = reader.next()?.ok()?;
    if reader.next().is_some() { return None; }
    sections_of(&StructArray::from(batch)).ok()
}

pub(crate) fn artefact_identity(table_slug: &str, owner: &rtdb_format::keys::ArtefactOwner, id: &str, version: i64) -> String {
    format!("{table_slug}/{owner}/{id}/v{version}")
}

struct ConversionOutput<'a, 'file> {
    writer: &'a mut std::io::BufWriter<&'file std::fs::File>,
    error: &'a mut Option<Anomaly>,
}

impl Write for ConversionOutput<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.writer.write(bytes).inspect_err(|error| *self.error = Some(transcode_io(error)))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush().inspect_err(|error| *self.error = Some(transcode_io(error)))
    }
}

fn transcode_io(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.search/transcode-io", error.to_string())
}

fn artefact_digest(identity: &str) -> [u8; 32] { Sha256::digest(identity.as_bytes()).into() }

pub fn build_search_file(columns: &[String], rows: &[SearchRow], identity: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    write_search_file(columns, [Ok(rows.to_vec())], identity, None, &mut bytes)?;
    Ok(bytes)
}

/// Writes one Arrow search file over a trie's rows.
pub fn write_search_file(columns: &[String], pages: impl IntoIterator<Item = Result<Vec<SearchRow>>>, identity: &str, ceiling: Option<i64>, output: impl Write) -> Result<Vec<Section>> {
    write_group_search_file(columns, [pages], identity, ceiling, output)
}

pub(crate) fn write_query_pages<I: SearchPage>(
    columns: &[String], pages: impl IntoIterator<Item = Result<I>>, ceiling: Option<i64>, output: impl Write,
    reservation: Arc<rtdb_columnar::MemoryReservation>,
) -> Result<Vec<Section>> {
    write::write_group(columns, [pages], "built", None, ceiling, output, Some(reservation), FORMAT_VERSION)
}

/// Writes one Arrow search file over the members in bucket order with a shared recency ceiling.
pub fn write_group_search_file<P>(columns: &[String], members: impl IntoIterator<Item = P>, identity: &str, ceiling: Option<i64>, output: impl Write) -> Result<Vec<Section>>
where P: IntoIterator<Item = Result<Vec<SearchRow>>>,
{
    write::write_group(columns, members, identity, None, ceiling, output, None, FORMAT_VERSION)
}

pub(crate) fn write_versioned_group<P, I>(
    columns: &[String], members: impl IntoIterator<Item = P>, identity: &str, version: i64,
    ceiling: Option<i64>, output: impl Write, reservation: Option<Arc<rtdb_columnar::MemoryReservation>>, format: i32,
) -> Result<Vec<Section>>
where P: IntoIterator<Item = Result<I>>, I: SearchPage,
{
    write::write_group(columns, members, identity, Some(version), ceiling, output, reservation, format)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RowDoc { Invisible, Null, Run(u32) }

/// An immutable search artefact with independently checked Arrow batches.
pub struct SearchFile {
    reader: Reader,
    columns: Vec<String>,
    manifest: Manifest,
    validated_postings: Box<[OnceLock<()>]>,
    _reservation: Option<Arc<rtdb_columnar::MemoryReservation>>,
}

struct TranscodedSearchMap {
    map: memmap2::Mmap,
    _reservation: Option<Arc<rtdb_columnar::MemoryReservation>>,
}

impl AsRef<[u8]> for TranscodedSearchMap {
    fn as_ref(&self) -> &[u8] { &self.map }
}

impl SearchFile {
    pub fn open(bytes: impl Into<Source>, columns: &[String]) -> Result<Self> {
        let source = bytes.into();
        let source = if legacy11::is_bundle(source.as_ref()) {
            legacy11::decode(bytes::Bytes::from_owner(source), columns)?.into()
        } else { source };
        Self::open_reader(Reader::from_source(source)?, columns)
    }

    pub(crate) fn with_reservation(mut self, reservation: Arc<rtdb_columnar::MemoryReservation>) -> Self {
        self._reservation = Some(reservation);
        self
    }

    pub(crate) async fn open_catalog(source: Source, columns: &[String], locator: Option<&FooterLocator>,
        memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<(Self, Option<usize>)> {
        if locator.is_some() || !legacy11::is_bundle(source.as_ref()) {
            return Self::open_located(source, columns, locator).map(|file| (file, None));
        }
        let columns = columns.to_vec();
        let memory = Arc::clone(memory);
        tokio::task::spawn_blocking(move || {
            let output = tempfile::tempfile().map_err(transcode_io)?;
            let mut writer = std::io::BufWriter::new(&output);
            let mut error = None;
            let conversion = legacy11::decode_to(bytes::Bytes::from_owner(source), &columns,
                ConversionOutput { writer: &mut writer, error: &mut error }, Some(&memory));
            if let Some(error) = error { return Err(error); }
            conversion?;
            writer.flush().map_err(transcode_io)?;
            drop(writer);
            let bytes = usize::try_from(output.metadata().map_err(transcode_io)?.len()).map_err(transcode_io)?;
            let reservation = memory.is_prepaid().then(|| memory.reserve(bytes)).transpose()?;
            // SAFETY: the anonymous file has no other handle and all writes have finished.
            let map = unsafe { memmap2::Mmap::map(&output) }.map_err(transcode_io)?;
            let mapped = TranscodedSearchMap { map, _reservation: reservation };
            Self::open_located(bytes::Bytes::from_owner(mapped).into(), &columns, None).map(|file| (file, Some(bytes)))
        }).await.map_err(transcode_io)?
    }

    pub(crate) fn open_located(source: Source, columns: &[String], locator: Option<&FooterLocator>) -> Result<Self> {
        if let Some(locator) = locator {
            let bytes = source.as_ref();
            locator.verify(bytes.get(locator.start..).ok_or_else(|| corrupt("Search file has no catalog footer"))?)?;
        }
        Self::open(source, columns)
    }

    pub(crate) async fn open_remote(storage: &Arc<rtdb_storage::cache::BufferPool>, key: &rtdb_storage::ObjectKey,
        footer: &FooterLocator, columns: &[String]) -> Result<Self> {
        Self::open_reader(Reader::remote(storage, key, footer).await?, columns)
    }

    pub(crate) fn metadata_bytes(&self) -> usize {
        self.reader.metadata_bytes()
    }

    pub(crate) async fn prepare_terms(&self, column: &str, queries: &[&Query]) -> Result<()> {
        let Some(column) = self.column(column) else { return Ok(()); };
        let mut pages = Vec::new();
        for text in queries.iter().flat_map(|query| query.terms()).collect::<BTreeSet<_>>() {
            let Some(term) = self.manifest.term(column, text) else { continue; };
            if let Some(blocks) = self.manifest.blocks(&term) {
                let positions = queries.iter().any(|query| needs_positions(query, text));
                for block in blocks {
                    pages.push(block.page);
                    if positions { pages.push(block.page + 1); }
                }
            } else {
                pages.push(term.page);
            }
        }
        self.reader.prefetch(pages).await
    }

    /// Opens a file backed by byte ranges within an immutable source of the given length.
    pub fn open_with_ranges(
        length: usize, read: impl Fn(Range<usize>) -> std::io::Result<Buffer> + Send + Sync + 'static, columns: &[String],
    ) -> Result<Self> {
        Self::open_reader(Reader::open(length, move |range| read(range).map_err(fault))?, columns)
    }

    fn open_reader(reader: Reader, columns: &[String]) -> Result<Self> {
        let manifest = Manifest::read(&reader, columns)?;
        let validated_postings = (0..manifest.terms().len()).map(|_| OnceLock::new()).collect();
        Ok(Self { reader, columns: columns.to_vec(), manifest, validated_postings, _reservation: None })
    }

    fn column(&self, column: &str) -> Option<usize> { self.columns.iter().position(|candidate| candidate == column) }

    pub fn visible(&self, doc: u32, bounds: TemporalBounds) -> bool {
        doc < self.manifest.documents
            && self.sections().iter().find(|section| section.class == self.manifest.doc_class(doc)).is_some_and(|section| section.meets(bounds))
            && self.rectangles(doc).any(|rectangle| meets(rectangle, bounds))
    }

    pub fn sections(&self) -> &[Section] { &self.manifest.sections }

    pub fn rectangles(&self, doc: u32) -> impl Iterator<Item = [i64; 4]> + '_ { self.manifest.rectangles(doc) }

    pub fn events(&self, iid: &[u8; 16]) -> Result<Vec<KeyedEvent>> {
        self.manifest.rows_of(iid).map(|row| self.manifest.event(row)).collect()
    }

    pub fn rows_of(&self, iid: &[u8; 16]) -> Result<Vec<(u32, KeyedEvent)>> {
        self.manifest.rows_of(iid).map(|row| Ok((row, self.manifest.event(row)?))).collect()
    }

    pub fn run_of(&self, column: &str, row: u32) -> Option<Option<u32>> {
        match self.manifest.row_doc(self.column(column)?, row) {
            RowDoc::Invisible => None, RowDoc::Null => Some(None), RowDoc::Run(doc) => Some(Some(doc)),
        }
    }

    pub(crate) fn single_column_member_runs(&self, member: u32) -> Result<impl Iterator<Item = DocMatch> + '_> {
        if self.columns.len() != 1 { return Err(corrupt("Owned search must cover exactly one column")); }
        let member = member as usize;
        let first = self.manifest.members.get(member).ok_or_else(|| corrupt("Search file has no such member"))?.doc;
        let end = self.manifest.members.get(member + 1).map_or(self.manifest.documents, |start| start.doc);
        Ok((first..end).map(|doc| self.manifest.doc_match(doc)))
    }

    pub(crate) fn matches_artefact(&self, identity: &str, version: i64) -> bool {
        self.manifest.matches_artefact(identity, version)
    }

    pub(crate) fn has_full_event_keys(&self) -> bool { self.manifest.has_full_event_keys() }

    pub(crate) fn rows_for_key<'a>(&'a self, iid: &[u8; 16], key: &'a [u8]) -> impl Iterator<Item = Result<(u32, KeyedEvent)>> + 'a {
        self.manifest.rows_of(iid).filter(move |&row| self.manifest.event_key(row) == Some(key))
            .map(|row| Ok((row, self.manifest.event(row)?)))
    }

    pub(crate) fn run_identity_bytes(&self, run: DocMatch) -> Result<&[u8]> { self.manifest.run_key(run.doc) }

    pub(crate) fn run_identity_key(&self, run: DocMatch) -> Result<rtdb_columnar::EntityIdRef<'_>> {
        rtdb_columnar::EntityIdRef::from_handle_bytes(self.run_identity_bytes(run)?)
            .map_err(|_| corrupt("Search run has an invalid complete identity"))
    }

    pub(crate) fn integer_id(&self, run: DocMatch) -> Option<i64> {
        match self.run_identity_key(run).ok()? {
            rtdb_columnar::EntityIdRef::Integer(id) if EntityId::Integer(id).iid().0 == run.iid => Some(id),
            _ => None,
        }
    }

    pub fn token_length(&self, doc: u32) -> Result<u32> {
        if doc >= self.manifest.documents { return Err(corrupt("Search file has no such run")); }
        Ok(self.manifest.token_length(doc))
    }

    pub fn stored_length(&self, column: &str, doc: u32) -> Result<u32> {
        if self.column(column).is_none() { return Ok(0); }
        Ok(stored_length(self.token_length(doc)? as usize))
    }

    pub fn holding(&self, column: &str, term: &str) -> Result<Vec<u32>> {
        Ok(self.postings(column, term)?.map_or_else(Vec::new, |postings| postings.documents().to_vec()))
    }

    pub(crate) fn postings(&self, column: &str, term: &str) -> Result<Option<Postings>> {
        let Some(column) = self.column(column) else { return Ok(None) };
        PostingPages::new(self, column).get(term, false)
    }

    pub(crate) fn document_frequency(&self, column: &str, term: &str) -> Result<u64> {
        Ok(self.column(column).and_then(|column| self.manifest.term(column, term)).map_or(0, |term| term.frequency.into()))
    }

    pub(crate) fn check_terms(&self, column: &str, queries: &[&Query]) -> Result<()> {
        let Some(column) = self.column(column) else { return Ok(()); };
        let mut pages = PostingPages::new(self, column);
        for query in queries {
            for term in query.terms() { pages.get(term, needs_positions(query, term))?; }
        }
        Ok(())
    }

    pub(crate) fn verify(&self) -> Result<()> {
        let mut pages = PostingPages::new(self, 0);
        for term in self.manifest.terms() { pages.read(term, true)?; }
        Ok(())
    }

    pub(crate) fn matched_runs(&self, column: &str, query: &Query, bounds: TemporalBounds) -> Result<Matches<'_>> {
        let matching = match self.column(column) {
            Some(column) => {
                let mut pages = PostingPages::new(self, column);
                Matching::new(query, |term, positions| pages.get(term, positions))?
            }
            None => None,
        };
        let documents = UInt32Array::from_iter_values(matching.iter().flat_map(|matching| {
            matching.documents().iter().copied().filter(|&doc| matching.includes(doc) && self.visible(doc, bounds))
        }));
        Ok(Matches { file: self, documents })
    }

    pub fn matches(&self, column: &str, query: &Query, bounds: TemporalBounds) -> Result<Vec<DocMatch>> {
        Ok(self.matched_runs(column, query, bounds)?.iter().collect())
    }

    pub fn scored(&self, column: &str, matching: &Query, scoring: &Query, corpus: Option<&Corpus>, bounds: TemporalBounds) -> Result<Vec<(DocMatch, f64)>> {
        let Some(column) = self.column(column) else { return Ok(Vec::new()) };
        let mut pages = PostingPages::new(self, column);
        let Some(matching) = Matching::new(matching, |term, positions| pages.get(term, positions))? else { return Ok(Vec::new()) };
        let mut scorer = ExactScorer::new(&mut pages, Arc::new(Scorer::new(scoring.clone(), corpus)))?;
        Ok(matching.documents().iter().copied().filter(|&doc| matching.includes(doc) && self.visible(doc, bounds))
            .map(|doc| (self.manifest.doc_match(doc), scorer.score(doc, self.manifest.token_length(doc)))).collect())
    }

    pub fn ranked(self: &Arc<Self>, column: &str, matching: &Query, scoring: &RankScorer, bounds: TemporalBounds) -> Result<Ranked> {
        self.ranked_with_id(column, matching, scoring, bounds, false)
    }

    pub(crate) fn ranked_for_query(self: &Arc<Self>, column: &str, matching: &Query, scoring: &RankScorer, bounds: TemporalBounds,
        ascending_id: bool) -> Result<RankedSource> {
        if self.reader.format >= FORMAT_VERSION && let Some(column) = self.column(column) {
            return Ok(RankedSource::Paged(paged::PagedRanked::new(Arc::clone(self), column, matching, scoring, bounds, ascending_id)));
        }
        self.ranked_with_id(column, matching, scoring, bounds, ascending_id).map(RankedSource::Legacy)
    }

    pub(crate) fn needs_ranked_postings(&self) -> bool { self.reader.format == LEGACY_FORMAT_VERSION }

    pub(crate) fn ranked_with_id(self: &Arc<Self>, column: &str, matching: &Query, scoring: &RankScorer, bounds: TemporalBounds, ascending_id: bool) -> Result<Ranked> {
        let Some(column) = self.column(column) else { return Ok(Ranked(None)) };
        let mut pages = PostingPages::new(self, column);
        let Some(matching) = Matching::new(matching, |term, positions| pages.get(term, positions))? else { return Ok(Ranked(None)) };
        let length = |doc| stored_length(self.manifest.token_length(doc) as usize);
        let mut windows: Option<Vec<u32>> = None;
        for posting in matching.terms() {
            let holding: Vec<_> = posting.impacts(length).into_iter().map(|impact| impact.window).collect();
            windows = Some(match windows {
                None => holding,
                Some(previous) => previous.into_iter().filter(|window| holding.binary_search(window).is_ok()).collect(),
            });
        }
        let scorer = ExactScorer::new(&mut pages, Arc::clone(scoring.scorer()))?;
        let mut ordered: Vec<_> = windows.unwrap_or_default().into_iter().map(|window| (0.0, window)).collect();
        for (term, postings) in scorer.postings.iter().enumerate() {
            let Some(postings) = postings else { continue };
            let impacts = postings.impacts(length);
            for (bound, window) in &mut ordered {
                if let Ok(index) = impacts.binary_search_by_key(window, |impact| impact.window) {
                    let Impact { frequency, length, same_frequency, same_length, .. } = impacts[index];
                    let exact = same_frequency && (same_length || !scoring.scorer().uses_length_normalization());
                    *bound += if ascending_id && exact { scoring.scorer().term_score(term, frequency, length) }
                        else { scoring.term_bound(term, frequency, length) };
                }
            }
        }
        let tie = |window: u32| ascending_id.then(|| self.manifest.window_id(window)).flatten();
        ordered.sort_unstable_by(|left, right| right.0.total_cmp(&left.0).then_with(|| tie(left.1).cmp(&tie(right.1)))
            .then(left.1.cmp(&right.1)));
        Ok(Ranked(Some(Windows { file: Arc::clone(self), matching, scorer, bounds, windows: ordered, next: 0, ascending_id })))
    }

    pub fn match_set(self: &Arc<Self>, column: &str, query: &Query) -> Result<MatchSet> {
        let column = self.column(column);
        let mut bits = BooleanBufferBuilder::new(self.manifest.documents as usize);
        bits.append_n(self.manifest.documents as usize, false);
        if let Some(column) = column {
            let mut pages = PostingPages::new(self, column);
            if let Some(matching) = Matching::new(query, |term, positions| pages.get(term, positions))? {
                for &doc in matching.documents() {
                    if matching.includes(doc) { bits.set_bit(doc as usize, true); }
                }
            }
        }
        Ok(MatchSet { file: Arc::clone(self), column, matching: MatchingDocuments::All(BooleanArray::new(bits.finish(), None)) })
    }

    pub(crate) fn match_selected_iids(self: &Arc<Self>, column: &str, query: &Query, iids: &BTreeSet<Iid>) -> Result<MatchSet> {
        let column = self.column(column);
        let mut documents = BTreeSet::new();
        let mut bits = BooleanBufferBuilder::new(iids.len());
        if let Some(column) = column {
            for iid in iids {
                for row in self.manifest.rows_of(&iid.0) {
                    if let RowDoc::Run(doc) = self.manifest.row_doc(column, row) {
                        documents.insert(doc);
                        if documents.len() > 1024 { return self.match_set(&self.columns[column], query); }
                    }
                }
            }
            let mut pages = PostingPages::new(self, column);
            let matching = if documents.is_empty() { None } else { Matching::new(query, |term, positions| pages.get(term, positions))? };
            for &doc in &documents {
                bits.append(matching.as_ref().is_some_and(|matching| matching.includes(doc)));
            }
        }
        let matching = MatchingDocuments::Selected(UInt32Array::from_iter_values(documents), BooleanArray::new(bits.finish(), None));
        Ok(MatchSet { file: Arc::clone(self), column, matching })
    }

    pub fn corpus(&self, column: &str, query: &Query) -> Result<Corpus> {
        let Some(index) = self.column(column) else { return Ok(Corpus::default()) };
        let (documents, tokens) = self.manifest.column_totals(index);
        let frequencies = query.terms().iter().map(|term| {
            let frequency = self.manifest.term(index, term).map_or(0, |posting| posting.frequency.into());
            (term.clone(), frequency)
        }).collect();
        Ok(Corpus { documents, tokens, frequencies })
    }
}

pub(crate) struct Matches<'a> {
    file: &'a SearchFile,
    documents: UInt32Array,
}

impl Matches<'_> {
    pub(crate) fn len(&self) -> usize { self.documents.len() }

    pub(crate) fn iter(&self) -> impl Iterator<Item = DocMatch> + '_ {
        self.documents.values().iter().map(|&doc| self.file.manifest.doc_match(doc))
    }
}

struct PostingPages<'a> {
    file: &'a SearchFile,
    column: usize,
    pages: BTreeMap<usize, StructArray>,
}

impl<'a> PostingPages<'a> {
    fn new(file: &'a SearchFile, column: usize) -> Self { Self { file, column, pages: BTreeMap::new() } }

    fn get(&mut self, text: &str, positions: bool) -> Result<Option<Postings>> {
        let Some(term) = self.file.manifest.term(self.column, text) else { return Ok(None) };
        self.read(term, positions).map(Some)
    }

    fn read(&mut self, term: Term, positions: bool) -> Result<Postings> {
        let postings = if let Some(blocks) = self.file.manifest.blocks(&term) {
            paged::postings(&self.file.reader, &blocks, self.file.manifest.documents, positions)?
        } else {
            let page = match self.pages.entry(term.page) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(postings_table(&self.file.reader.page(term.page)?))
                }
            };
            Postings::from_table(page, term.index)?
        };
        let validity = &self.file.validated_postings[term.ordinal];
        if validity.get().is_none() {
            postings.validate(term.frequency as usize, self.file.manifest.documents)?;
            let _ = validity.set(());
        }
        Ok(postings)
    }
}

enum MatchingDocuments {
    All(BooleanArray),
    Selected(UInt32Array, BooleanArray),
}

pub struct MatchSet { file: Arc<SearchFile>, column: Option<usize>, matching: MatchingDocuments }

impl MatchSet {
    pub fn value(&self, page: usize, row: usize) -> Result<Option<bool>> {
        let manifest = &self.file.manifest;
        if page >= manifest.pages.len() { return Err(corrupt("Search file has no such page")); }
        let start = manifest.pages.value(page);
        let end = if page + 1 < manifest.pages.len() { manifest.pages.value(page + 1) } else { manifest.rows };
        let row = u32::try_from(row).ok().and_then(|row| start.checked_add(row)).filter(|row| *row < end)
            .ok_or_else(|| corrupt("Search file has no such row"))?;
        let Some(column) = self.column else { return Ok(None) };
        match manifest.row_doc(column, row) {
            RowDoc::Invisible => Err(corrupt("Search file row is visible nowhere")),
            RowDoc::Null => Ok(None),
            RowDoc::Run(doc) => match &self.matching {
                MatchingDocuments::All(bits) => Ok(Some(bits.value(doc as usize))),
                MatchingDocuments::Selected(docs, bits) => docs.values().binary_search(&doc)
                    .map(|row| Some(bits.value(row))).map_err(|_| corrupt("Search file MATCH was not requested for this row")),
            },
        }
    }

    pub fn member(self: &Arc<Self>, member: u32) -> Result<MemberMatches> {
        let member = member as usize;
        let manifest = &self.file.manifest;
        let first = manifest.members.get(member).ok_or_else(|| corrupt("Search file has no such member"))?.page as usize;
        let end = manifest.members.get(member + 1).map_or(manifest.pages.len(), |start| start.page as usize);
        Ok(MemberMatches { set: Arc::clone(self), first, pages: end - first })
    }

    pub fn bytes(&self) -> usize {
        match &self.matching {
            MatchingDocuments::All(bits) => bits.get_buffer_memory_size(),
            MatchingDocuments::Selected(docs, bits) => docs.get_buffer_memory_size() + bits.get_buffer_memory_size(),
        }
    }
}

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

struct ExactScorer {
    scoring: Arc<Scorer>,
    postings: Vec<Option<Postings>>,
    frequencies: Vec<u32>,
}

impl ExactScorer {
    fn new(pages: &mut PostingPages<'_>, scoring: Arc<Scorer>) -> Result<Self> {
        let postings = scoring.query().terms().iter().map(|term| pages.get(term, false)).collect::<Result<Vec<_>>>()?;
        let frequencies = vec![0; postings.len()];
        Ok(Self { scoring, postings, frequencies })
    }

    fn score(&mut self, doc: u32, length: u32) -> f64 {
        for (frequency, postings) in self.frequencies.iter_mut().zip(&self.postings) {
            *frequency = postings.as_ref().map_or(0, |postings| postings.frequency(doc));
        }
        self.scoring.score(&self.frequencies, stored_length(length as usize))
    }
}

/// A file's ranked candidates, emitted in batches of windows ordered by score bound.
pub struct Ranked(Option<Windows>);

pub(crate) enum RankedSource {
    Legacy(Ranked),
    Paged(paged::PagedRanked),
}

impl RankedSource {
    pub fn bound(&self) -> Option<crate::rank_cutoff::RankKey> {
        match self {
            Self::Legacy(rank) => rank.rank_bound(),
            Self::Paged(rank) => rank.bound(),
        }
    }

    pub async fn expand(&mut self, scope: Option<&BTreeSet<Iid>>, examined: &mut usize) -> Result<ScoredRuns> {
        match self {
            Self::Legacy(rank) => rank.expand_in(scope, examined).ok_or_else(|| corrupt("Ranked file has no remaining windows")),
            Self::Paged(rank) => rank.expand(scope, examined).await,
        }
    }
}

struct Windows {
    file: Arc<SearchFile>,
    matching: Matching,
    scorer: ExactScorer,
    bounds: TemporalBounds,
    windows: Vec<(f64, u32)>,
    next: usize,
    ascending_id: bool,
}

pub(crate) struct ScoredRuns {
    file: Arc<SearchFile>,
    documents: UInt32Array,
    scores: Float64Array,
}

impl ScoredRuns {
    pub(crate) fn len(&self) -> usize { self.documents.len() }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (DocMatch, f64)> + '_ {
        self.documents.values().iter().zip(self.scores.values().iter())
            .map(|(&doc, &score)| (self.file.manifest.doc_match(doc), score))
    }
}

impl Ranked {
    pub fn bound(&self) -> Option<f64> { self.0.as_ref().and_then(|windows| windows.windows.get(windows.next).map(|(bound, _)| *bound)) }

    pub(crate) fn rank_bound(&self) -> Option<crate::rank_cutoff::RankKey> {
        let windows = self.0.as_ref()?;
        let &(score, window) = windows.windows.get(windows.next)?;
        let id = windows.ascending_id.then(|| windows.file.manifest.window_id(window)).flatten();
        Some(crate::rank_cutoff::RankKey::integer(score, id))
    }

    pub fn expand(&mut self) -> Result<Vec<(DocMatch, f64)>> {
        Ok(self.expand_in(None, &mut 0).map_or_else(Vec::new, |found| found.iter().collect()))
    }

    pub(crate) fn expand_in(&mut self, scope: Option<&BTreeSet<Iid>>, examined: &mut usize) -> Option<ScoredRuns> {
        self.0.as_mut().map(|windows| windows.expand(scope, examined))
    }

    pub fn file(&self) -> Option<&Arc<SearchFile>> { self.0.as_ref().map(|windows| &windows.file) }
}

impl Windows {
    fn expand(&mut self, scope: Option<&BTreeSet<Iid>>, examined: &mut usize) -> ScoredRuns {
        let end = self.next.saturating_add(4).min(self.windows.len());
        let mut found = Vec::new();
        let mut scores = Vec::new();
        let documents = self.matching.documents();
        for &(_, window) in &self.windows[self.next..end] {
            let (start, end) = document_range(window);
            let first = documents.partition_point(|&doc| doc < start);
            let last = documents.partition_point(|&doc| doc < end);
            for &doc in &documents[first..last] {
                *examined += 1;
                if self.matching.includes(doc)
                    && scope.is_none_or(|scope| scope.contains(&Iid(self.file.manifest.doc_iid(doc))))
                    && self.file.visible(doc, self.bounds)
                {
                    found.push(doc);
                    scores.push(self.scorer.score(doc, self.file.manifest.token_length(doc)));
                }
            }
        }
        self.next = end;
        ScoredRuns { file: Arc::clone(&self.file), documents: UInt32Array::from(found), scores: Float64Array::from(scores) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_text::tokens;
    use rtdb_types::Interval;
    use crate::rank_windows::window_of;

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
    fn run(file: &SearchFile, row: u32) -> Option<u32> { match file.manifest.row_doc(1, row) { RowDoc::Run(doc) => Some(doc), _ => None } }

    #[tokio::test]
    async fn scoped_tantivy_conversion_keeps_its_arrow_map_charged_until_drop() -> Result<()> {
        let columns = columns();
        let mut bundle = Vec::new();
        write_versioned_group(&columns, [[Ok(rows())]], "legacy", 1, None, &mut bundle, None, TANTIVY_FORMAT_VERSION)?;
        let memory = rtdb_columnar::MemoryBudget::new(8 << 20).prepay(4 << 20)?;
        let (file, converted) = SearchFile::open_catalog(bundle.into(), &columns, None, &memory).await?;
        assert_eq!(memory.used_bytes(), converted.unwrap());
        drop(file);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[test]
    fn range_reads_preserve_ranked_results_without_requesting_the_entire_bundle() {
        let data = build_search_file(&columns(), &rows(), "ranges").unwrap();
        let full = SearchFile::open(data.clone(), &columns()).unwrap();
        let data = Buffer::from(data);
        let length = data.len();
        let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&reads);
        let ranged = SearchFile::open_with_ranges(length, move |range| {
            recorded.lock().unwrap().push(range.clone());
            Ok(data.slice_with_length(range.start, range.len()))
        }, &columns()).unwrap();
        let query = Query::parse("invoice");
        let score = |file: &SearchFile| file.scored("body", &query, &query, None, all())
            .unwrap().into_iter().map(|(hit, score)| (hit.iid, hit.doc, score)).collect::<Vec<_>>();
        assert_eq!(score(&ranged), score(&full));
        let reads = reads.lock().unwrap();
        assert!(reads.contains(&(length - 10..length)));
        assert!(reads.iter().all(|range| range.len() < length));
    }

    #[test]
    fn published_runs_keep_exact_lengths_and_definition_identity() {
        let columns = vec!["body".to_string()];
        let tokens = vec!["term".to_string(); 79];
        let row = SearchRow { identity: None, iid: [1; 16], system_from: 1, valid_from: 1, valid_to: 10, op: EventOp::Put, values: vec![Some(tokens)] };
        let mut bytes = Vec::new();
        write_versioned_group(&columns, [[Ok(vec![row])]], "owned-file", 13, None, &mut bytes, None, FORMAT_VERSION).unwrap();
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
        assert_eq!(file.run_identity_key(run).unwrap(), rtdb_columnar::EntityIdRef::from(&id));
    }

    #[test]
    fn search_file_cuts_only_the_matching_identity_in_a_shared_iid() {
        let columns = ["body".into()];
        let text = EntityId::String("invoice".into());
        let iid = text.iid().0;
        let uuid = EntityId::Uuid(iid);
        let event = |identity, system_from, op, values| SearchRow {
            identity: Some(identity), iid, system_from, valid_from: 0, valid_to: i64::MAX,
            op, values: vec![values],
        };
        let bytes = build_search_file(&columns, &[
            event(text.clone(), 20, EventOp::Delete, None),
            event(uuid.clone(), 10, EventOp::Put, Some(tokens("invoice"))),
            event(text.clone(), 10, EventOp::Put, Some(tokens("invoice"))),
        ], "shared-iid").unwrap();
        let file = SearchFile::open(bytes, &columns).unwrap();
        let earlier = file.matches("body", &Query::parse("invoice"), at(15, 15)).unwrap();
        let later = file.matches("body", &Query::parse("invoice"), at(25, 25)).unwrap();
        assert_eq!(earlier.len(), 2);
        assert_eq!(later.len(), 1);
        assert_eq!(file.run_identity_key(later[0]).unwrap(), rtdb_columnar::EntityIdRef::from(&uuid));
        assert!(file.has_full_event_keys());
        let text_key = text.handle_bytes();
        let rows = file.rows_for_key(&iid, &text_key).collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(rows.iter().map(|(ordinal, _)| *ordinal).collect::<Vec<_>>(), [0, 2]);
    }

    #[test]
    fn a_job_builds_the_same_bytes_every_time() {
        let first = build_search_file(&columns(), &rows(), "public$docs/l01-rc-b00/docs_text").unwrap();
        assert_eq!(first, build_search_file(&columns(), &rows(), "public$docs/l01-rc-b00/docs_text").unwrap());
        assert_ne!(first, build_search_file(&columns(), &rows(), "public$docs/l01-rc-b01/docs_text").unwrap());
    }

    #[test]
    fn a_search_file_streamed_to_disk_matches_one_built_in_memory() {
        let path = tempfile::NamedTempFile::new().unwrap();
        let output = std::io::BufWriter::new(path.reopen().unwrap());
        write_search_file(&columns(), [Ok(rows())], "job", None, output).unwrap();
        assert_eq!(std::fs::read(path.path()).unwrap(), build_search_file(&columns(), &rows(), "job").unwrap());
    }

    #[test]
    fn an_entitys_versions_with_the_same_text_are_one_document_visible_where_any_of_them_is() {
        let file = open(&rows());
        let (first, second, third) = (run(&file, 0).unwrap(), run(&file, 1).unwrap(), run(&file, 2).unwrap());
        assert!(first == third && first != second, "the first and third versions share their text");
        assert_eq!(file.manifest.row_doc(0, 0), file.manifest.row_doc(0, 1), "every version shares its title, so the title is one run");
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
            write::coalesce(vec![[0, 5, 10, 20], [5, 9, 10, 20], [0, 5, 20, 30], [7, 8, 1, 2]]),
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
        let mut legacy = Vec::new();
        write_versioned_group(&columns(), [[Ok(rows)]], "legacy", 1, None, &mut legacy, None, LEGACY_FORMAT_VERSION).unwrap();
        let file = Arc::new(SearchFile::open(legacy, &columns()).unwrap());
        assert!(file.postings("body", "invoice").unwrap().unwrap().has_stored_impacts());
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
        write_search_file(&columns(), pages.iter().cloned().map(Ok), "job", None, &mut bytes).unwrap();
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
    fn an_overlength_word_is_split_into_the_same_terms_for_indexing_and_scoring() {
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
        write_group_search_file(&columns(), [first, Vec::new(), third], "group", None, &mut bytes).unwrap();
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
    fn posting_access_checks_only_the_arrow_batches_for_requested_terms() {
        let indexed = [String::from("body")];
        let row = SearchRow {
            values: vec![Some((0..96).map(|term| format!("token{term:03}")).collect())],
            ..row(1, 0, "", "")
        };
        let mut bytes = build_search_file(&indexed, &[row], "postings").unwrap();
        let ipc = arrow_ipc::reader::FileReader::try_new(Cursor::new(bytes.as_slice()), None).unwrap();
        assert_eq!(ipc.num_batches(), 8);
        drop(ipc);
        let reader = Reader::from_source(Source::from(bytes.clone())).unwrap();
        let damaged = reader.page_range(6);
        bytes[damaged.start + damaged.len() / 2] ^= 1;
        let search = SearchFile::open(bytes, &indexed).unwrap();
        assert_eq!(search.matches("body", &Query::parse("token002"), all()).unwrap().len(), 1);
        assert!(search.matches("body", &Query::parse("token095"), all()).is_err());
        assert!(search.verify().is_err());
    }

    #[tokio::test]
    async fn paged_ranking_agrees_with_the_legacy_files_for_terms_and_phrases() {
        let indexed = ["body".to_owned()];
        let mut rows: Vec<_> = (-2500..2500).map(|id| {
            let identity = EntityId::Integer(id);
            let body = if id % 5 == 0 { "an overdue invoice overdue" } else { "invoice is paid" };
            SearchRow { iid: identity.iid().0, identity: Some(identity), values: vec![Some(tokens(body))], ..row(0, 1, "", "") }
        }).collect();
        rows.sort_unstable_by_key(|row| row.iid);
        let mut legacy = Vec::new();
        write_versioned_group(&indexed, [[Ok(rows.clone())]], "old", 1, None, &mut legacy, None, LEGACY_FORMAT_VERSION).unwrap();
        let legacy = SearchFile::open(legacy, &indexed).unwrap();
        let pages = Arc::new(SearchFile::open(build_search_file(&indexed, &rows, "new").unwrap(), &indexed).unwrap());
        for matching in ["invoice", "\"overdue invoice\"", "\"invoice overdue\"", "invoice missing"] {
            let matching = Query::parse(matching);
            let scoring = Arc::new(Scorer::new(Query::parse("paid overdue"), None)).for_ranking().unwrap();
            let mut ranked = pages.ranked_for_query("body", &matching, &scoring, at(2, 2), true).unwrap();
            let mut got = Vec::new();
            while ranked.bound().is_some() {
                let batches = ranked.expand(None, &mut 0).await.unwrap();
                got.extend(batches.iter().map(|(doc, score)| (doc.doc, score.to_bits())));
            }
            got.sort_unstable();
            let expected: Vec<_> = legacy.scored("body", &matching, scoring.scorer().query(), None, at(2, 2)).unwrap()
                .into_iter().map(|(doc, score)| (doc.doc, score.to_bits())).collect();
            assert_eq!(got, expected);
        }
    }

    use rtdb_storage::{MemoryStore, ObjectStore, StoredObject};

    #[derive(Default)]
    struct Remote {
        store: MemoryStore,
        ranges: parking_lot::Mutex<Vec<Range<u64>>>,
    }

    #[async_trait::async_trait]
    impl ObjectStore for Remote {
        async fn head(&self, key: &rtdb_storage::ObjectKey) -> rtdb_storage::Result<StoredObject> { self.store.head(key).await }
        async fn get(&self, key: &rtdb_storage::ObjectKey) -> rtdb_storage::Result<bytes::Bytes> { self.store.get(key).await }
        async fn get_range(&self, key: &rtdb_storage::ObjectKey, range: Range<u64>) -> rtdb_storage::Result<bytes::Bytes> {
            self.ranges.lock().push(range.clone());
            self.store.get_range(key, range).await
        }
        async fn put(&self, key: &rtdb_storage::ObjectKey, bytes: bytes::Bytes) -> rtdb_storage::Result<()> { self.store.put(key, bytes).await }
        async fn put_file(&self, key: &rtdb_storage::ObjectKey, source: &std::path::Path) -> rtdb_storage::Result<()> { self.store.put_file(key, source).await }
        async fn list(&self, key: Option<&rtdb_storage::ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.store.list(key).await }
        async fn delete(&self, key: &rtdb_storage::ObjectKey) -> rtdb_storage::Result<()> { self.store.delete(key).await }
    }

    #[tokio::test]
    async fn a_cold_ranked_read_fetches_neither_phrase_positions_nor_unrelated_postings() {
        use rtdb_storage::{ObjectKey, ObjectStore, cache::BufferPool};

        let columns = ["body".to_owned()];
        let mut rows: Vec<_> = (0..8192).map(|id| {
            let identity = EntityId::Integer(id);
            SearchRow { iid: identity.iid().0, identity: Some(identity),
                values: vec![Some(tokens(&format!("invoice document{id}")))], ..row(0, 1, "", "") }
        }).collect();
        rows.sort_unstable_by_key(|row| row.iid);
        let bytes = build_search_file(&columns, &rows, "remote").unwrap();
        let mut on_disk = tempfile::tempfile().unwrap();
        std::io::Write::write_all(&mut on_disk, &bytes).unwrap();
        let locator = FooterLocator::read(&mut on_disk).unwrap();
        let stats = encode_catalog_stats(&[], Some(&locator)).unwrap();
        assert_eq!(decode_footer_locator(&stats), Some(locator.clone()));
        assert_eq!(decode_sections(&stats), Some(vec![]));
        let source = Arc::new(Remote::default());
        let key = ObjectKey::new("search/docs").unwrap();
        source.put(&key, bytes.clone().into()).await.unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let disk = rtdb_storage::disk_cache::DiskCache::open(cache_dir.path(), 32 << 20, rtdb_columnar::MemoryBudget::new(32 << 20)).unwrap();
        let store = disk.scope(ObjectKey::new("database").unwrap(), source.clone()).unwrap();
        let pool = Arc::new(BufferPool::new(store, 16 << 20));
        let runtime = crate::SearchRuntime::new(16 << 20);
        let file = runtime.file_with_locator("db", &pool, &key, &columns, Some(&locator)).await.unwrap();
        let matching = Query::parse("invoice");
        let scorer = Arc::new(Scorer::new(matching.clone(), None)).for_ranking().unwrap();
        let mut ranked = file.ranked_for_query("body", &matching, &scorer, at(2, 2), true).unwrap();
        assert_eq!(ranked.bound().unwrap().integer_id(), Some(0));
        let hits = ranked.expand(None, &mut 0).await.unwrap();
        assert!(hits.iter().any(|(doc, _)| file.integer_id(doc) == Some(0)));

        let reads = source.ranges.lock().clone();
        let sent: u64 = reads.iter().map(|range| range.end - range.start).sum();
        assert!(sent < bytes.len() as u64 / 2, "read {sent} of {} bytes", bytes.len());
        let term = file.manifest.term(0, "invoice").unwrap();
        for block in file.manifest.blocks(&term).unwrap() {
            let position = file.reader.page_range(block.page + 1);
            assert!(reads.iter().all(|range| range.start >= position.end as u64 || range.end <= position.start as u64));
        }
        assert!(pool.object_store().cached_local_file(&key).await.unwrap().is_none());
        drop(file);
        drop(runtime);
        drop(pool);
        disk.close_and_wait().await;
    }


    #[tokio::test]
    async fn tree_rank_and_visibility_read_only_authenticated_ranges() {
        use rtdb_storage::{ObjectKey, cache::BufferPool};
        use tree_file::{DOCUMENT, POSTING, TERM, VISIBILITY, event_key, ordinal, term_key};

        let columns = ["body".to_owned()];
        let mut rows: Vec<_> = (0..16_384).map(|id| {
            let identity = EntityId::Integer(id);
            SearchRow { iid: identity.iid().0, identity: Some(identity),
                values: vec![Some(tokens(&format!("invoice document{id}")))], ..row(0, 1, "", "") }
        }).collect();
        rows.sort_unstable_by_key(|row| row.iid);
        let mut on_disk = tempfile::tempfile().unwrap();
        write_versioned_group(&columns, [[Ok(rows.clone())]], "remote-v14", 42, None, &mut on_disk, None, TREE_FORMAT_VERSION).unwrap();
        let locator = TreeLocator::read(&mut on_disk, TREE_FORMAT_VERSION).unwrap();
        use std::io::{Read, Seek};
        on_disk.rewind().unwrap();
        let mut bytes = Vec::new();
        on_disk.read_to_end(&mut bytes).unwrap();

        let store = Arc::new(Remote::default());
        let key = ObjectKey::new("search/tree").unwrap();
        store.put(&key, bytes.clone().into()).await.unwrap();
        let pool = Arc::new(BufferPool::new(store.clone(), 16 << 20));
        let cache = Arc::new(PageCache::new(4 << 20));
        let tree = Arc::new(TreeFile::remote(pool.clone(), key.clone(), cache.clone(), &locator, &columns, "remote-v14", 42).await.unwrap());
        let term = Query::parse("invoice");
        let scoring = Arc::new(Scorer::new(term.clone(), None)).for_ranking().unwrap();
        let mut rank = TreeRanked::open(tree.clone(), 0, &term, &scoring, at(2, 2), TreeRankOrder::Integer).await.unwrap();
        assert_eq!(rank.bound().unwrap().integer_id(), Some(0));
        assert!(rank.expand(None, &mut 0).await.unwrap().is_empty());
        let mut hits = Vec::new();
        while hits.is_empty() {
            hits = rank.expand(None, &mut 0).await.unwrap();
        }
        assert!(hits.iter().any(|hit| hit.key.integer_id() == Some(0)));
        assert_eq!(tree.events(rows[12_000].iid).await.unwrap().len(), 1);
        let reads = store.ranges.lock().clone();
        let sent: u64 = reads.iter().map(|range| range.end - range.start).sum();
        assert!(sent < bytes.len() as u64 / 4, "sparse read fetched {sent} of {} bytes", bytes.len());

        let term_ref = tree.page_reference_for_test(TERM, &term_key(0, "invoice")).await.unwrap().unwrap();
        let position = tree.page_reference_for_test(POSTING, &ordinal(3)).await.unwrap().unwrap().range().unwrap();
        let mut unused_document_pages = 0;
        for first in (0..rows.len()).step_by(tree_file::DOCUMENT_ROWS) {
            let range = tree.page_reference_for_test(DOCUMENT, &ordinal(first)).await.unwrap().unwrap().range().unwrap();
            if reads.iter().all(|read| read.start >= range.end || read.end <= range.start) { unused_document_pages += 1; }
        }
        assert!(reads.iter().all(|range| range.start >= position.end || range.end <= position.start));
        assert!(unused_document_pages > 0);
        let row = 12_000;
        let event = tree.page_reference_for_test(VISIBILITY, &event_key(&rows[row].iid, row as u32)).await.unwrap().unwrap().range().unwrap();
        assert!(reads.iter().any(|range| range.start == event.start && range.end == event.end));
        let holder = rtdb_columnar::MemoryBudget::new(32 << 20);
        let scoped = Arc::new(BufferPool::with_metadata_budget(store.clone(),
            rtdb_storage::cache::CacheBudget::with_memory(holder.prepay(16 << 20).unwrap()),
            holder.prepay(1 << 20).unwrap()));
        let runtime = crate::SearchRuntime::with_tree_files(16 << 20);
        let scoped_first = runtime.tree_file(&scoped, &key, &locator, &columns, "remote-v14", 42, false).await.unwrap();
        let scoped_second = runtime.tree_file(&scoped, &key, &locator, &columns, "remote-v14", 42, false).await.unwrap();
        assert!(!Arc::ptr_eq(&scoped_first, &scoped_second));
        assert!(scoped.pool_retained_bytes() > 0);
        drop((scoped_first, scoped_second));
        assert_eq!(scoped.pool_retained_bytes(), 0);

        let mut damaged = bytes;
        damaged[term_ref.offset as usize + term_ref.metadata as usize] ^= 1;
        let bad_key = ObjectKey::new("search/bad-tree").unwrap();
        store.put(&bad_key, damaged.into()).await.unwrap();
        let bad = Arc::new(TreeFile::remote(pool.clone(), bad_key, cache, &locator, &columns, "remote-v14", 42).await.unwrap());
        assert!(TreeRanked::open(bad, 0, &term, &scoring, at(2, 2), TreeRankOrder::Integer).await.is_err());
    }

    #[test]
    fn a_manifest_whose_bytes_changed_is_unreadable() {
        let bytes = build_search_file(&columns(), &corpus_rows(2000, 1), "job").unwrap();
        let reader = Reader::from_source(Source::from(bytes.clone())).unwrap();
        let range = reader.page_range(0);
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
        let sections = write_search_file(&columns(), [Ok(rows)], "job", Some(1000 * day), &mut bytes).unwrap();
        let file = SearchFile::open(bytes, &columns()).unwrap();
        assert_eq!(file.sections(), sections.as_slice());
        let class = |text: &str| sections.iter().find(|section| section.class == file.manifest.doc_class(file.matches("body", &Query::parse(text), all()).unwrap()[0].doc)).copied().unwrap();
        let (short, long, open) = (class("next day"), class("400 days"), class("long lived"));
        assert_eq!((short.class, long.class, open.class), (0, 3, 4), "four hundred days is under 64 weeks; the open run, clipped to the ceiling at 1000 days, under 256");
        assert_eq!((short.birth, short.death), (9 * day, 10 * day));
        assert!(short.meets(at(9 * day + 1, 9 * day + 1)) && !short.meets(at(20 * day, 20 * day)), "a version alive for one day is visible only around it");
        assert!(!short.meets(at(5 * day, 5 * day)), "nor before it was born");
        let later_valid = TemporalBounds { valid_time: Interval::from_micros(500 * day, 500 * day + 1).unwrap(), system_time: Interval::from_micros(9 * day + day / 2, 9 * day + day / 2 + 1).unwrap() };
        assert!(short.meets(later_valid), "visible at a far later valid time, as known during its one day");
        assert!(!open.meets(at(1000 * day, 1000 * day)), "the ceiling ends every version");
        assert_eq!(decode_sections(&encode_sections(&sections).unwrap()), Some(sections.clone()));
        assert_eq!(decode_sections(&[1, 2, 3]), None);
    }
}
