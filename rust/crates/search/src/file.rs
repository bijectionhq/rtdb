//! A search file: one single-segment tantivy index over one trie data file, bundled into one object.
//! Document ids are the data file's row ordinals; tokens come from `rtdb-text`, so the index and the residual `MATCH` agree.

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rtdb_text::{Corpus, Query};
use rtdb_types::{Anomaly, Result};
use tantivy::collector::DocSetCollector;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, FileSlice, OwnedBytes, RamDirectory, WatchCallback, WatchHandle, WritePtr};
use tantivy::query::{BooleanQuery, Occur, TermQuery};
use tantivy::schema::{Field, FAST, IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::store::Compressor;
use tantivy::tokenizer::{PreTokenizedString, Token};
use tantivy::{Directory, Index, IndexSettings, TantivyDocument, Term};

const MAGIC: &[u8; 8] = b"RTSRCH01";
/// Bumped with the bundle layout or the tantivy version, either of which changes a search file's bytes.
pub const FORMAT_VERSION: i32 = 3;
const WRITER_MEMORY: usize = 32 << 20;
/// The bundle entry holding what tantivy's segment does not: page starts, each column's non-null rows, and term impacts.
const SIDECAR: &str = "rtdb.sidecar";
/// Documents are bounded in windows of this many consecutive ids.
const WINDOW: u32 = 128;
/// A term in at least this many documents has its window bounds stored; a rarer term's are computed from its postings when asked.
const STORED_IMPACTS: u32 = 1024;

/// One data-file row as the search file sees it: its own temporal columns and the tokens of each indexed column.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchRow {
    pub iid: [u8; 16],
    pub system_from: i64,
    pub valid_from: i64,
    pub valid_to: i64,
    pub recency: Option<i64>,
    /// Parallel to the file's columns: `None` where the column is null or absent, and otherwise the tokens of its UTF-8 value, none for a value of another type.
    pub values: Vec<Option<Vec<String>>>,
}

/// A document whose terms satisfied a query, with the row's own temporal columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DocMatch { pub doc: u32, pub iid: [u8; 16], pub system_from: i64, pub valid_from: i64, pub valid_to: i64, pub recency: i64 }

struct Fields { columns: Vec<Field>, iid_high: Field, iid_low: Field, system_from: Field, valid_from: Field, valid_to: Field, recency: Field }

fn schema(columns: &[String]) -> (Schema, Fields) {
    let mut builder = Schema::builder();
    let text = TextOptions::default().set_indexing_options(TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqsAndPositions).set_fieldnorms(true));
    let columns = columns.iter().map(|column| builder.add_text_field(&format!("c_{column}"), text.clone())).collect();
    let fields = Fields {
        columns,
        iid_high: builder.add_u64_field("_iid_high", FAST), iid_low: builder.add_u64_field("_iid_low", FAST),
        system_from: builder.add_i64_field("_system_from", FAST), valid_from: builder.add_i64_field("_valid_from", FAST),
        valid_to: builder.add_i64_field("_valid_to", FAST), recency: builder.add_i64_field("_recency", FAST),
    };
    (builder.build(), fields)
}

fn fault(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.search/file", error.to_string()) }
fn corrupt(message: &str) -> Anomaly { Anomaly::fault("rtdb.search/corrupt-file", message.to_string()) }

/// Builds the bundled search file over rows in data-file order, as one page, in memory.
pub fn build_search_file(columns: &[String], rows: &[SearchRow], identity: &str) -> Result<Vec<u8>> {
    let mut bundle = Vec::new();
    write_search_file(columns, [Ok(rows.to_vec())], identity, RamDirectory::create(), &mut bundle)?;
    Ok(bundle)
}

/// Builds the search file over a data file's pages of rows, in order, building its segment in `directory` and writing the bundle to `output`.
/// The bytes are a function of the rows, their pages, the columns and `identity` alone, wherever the segment is built, so every builder of one job writes the same object.
pub fn write_search_file<D: Directory + Clone + 'static, W: std::io::Write>(columns: &[String], pages: impl IntoIterator<Item = Result<Vec<SearchRow>>>, identity: &str, directory: D, output: W) -> Result<()> {
    let (schema, fields) = schema(columns);
    let settings = IndexSettings { docstore_compression: Compressor::None, ..IndexSettings::default() };
    let mut writer = Index::builder().schema(schema).settings(settings).single_segment_index_writer(directory.clone(), WRITER_MEMORY).map_err(fault)?;
    let mut page_starts = Vec::new();
    let mut present: Vec<Vec<u8>> = vec![Vec::new(); columns.len()];
    let mut documents = 0u32;
    for page in pages {
        page_starts.push(documents);
        for row in page? {
            if row.values.len() != columns.len() { return Err(fault("Search row values do not cover the file's columns")); }
            let mut document = TantivyDocument::new();
            for (column, (field, value)) in fields.columns.iter().zip(&row.values).enumerate() {
                let Some(tokens) = value else { continue };
                let bits = &mut present[column];
                if bits.len() <= documents as usize / 8 { bits.resize(documents as usize / 8 + 1, 0); }
                bits[documents as usize / 8] |= 1 << (documents % 8);
                if tokens.is_empty() { continue; }
                let mut offset = 0;
                let tokens = tokens.iter().enumerate().map(|(position, text)| {
                    let token = Token { offset_from: offset, offset_to: offset + text.len(), position, text: text.clone(), position_length: 1 };
                    offset += text.len() + 1;
                    token
                }).collect();
                document.add_pre_tokenized_text(*field, PreTokenizedString { text: String::new(), tokens });
            }
            let (high, low) = row.iid.split_at(8);
            document.add_u64(fields.iid_high, u64::from_be_bytes(high.try_into().map_err(fault)?));
            document.add_u64(fields.iid_low, u64::from_be_bytes(low.try_into().map_err(fault)?));
            document.add_i64(fields.system_from, row.system_from);
            document.add_i64(fields.valid_from, row.valid_from);
            document.add_i64(fields.valid_to, row.valid_to);
            document.add_i64(fields.recency, row.recency.unwrap_or(i64::MAX));
            writer.add_document(document).map_err(fault)?;
            documents = documents.checked_add(1).ok_or_else(|| fault("A search file holds at most u32::MAX documents"))?;
        }
    }
    writer.finalize().map_err(fault)?;
    for bits in &mut present { bits.resize(documents.div_ceil(8) as usize, 0); }
    let sidecar = sidecar(&Index::open(directory.clone()).map_err(fault)?, &fields.columns, &page_starts, &present)?;
    directory.atomic_write(Path::new(SIDECAR), &sidecar).map_err(fault)?;
    bundle(&directory, &segment_uuid(identity), output)
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
        let (window, frequency, length) = (doc / WINDOW, postings.term_freq(), lengths.fieldnorm(doc));
        match impacts.last_mut() {
            Some(last) if last.window == window => { last.frequency = last.frequency.max(frequency); last.length = last.length.min(length); }
            _ => impacts.push(Impact { window, frequency, length }),
        }
        doc = postings.advance();
    }
    impacts
}

/// Page starts, then each column's non-null bitset, then the impacts of every term in at least STORED_IMPACTS documents, by column and term, then a checksum of the rest.
fn sidecar(index: &Index, columns: &[Field], page_starts: &[u32], present: &[Vec<u8>]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(page_starts.len() as u32).to_le_bytes());
    for start in page_starts { bytes.extend_from_slice(&start.to_le_bytes()); }
    for bits in present { bytes.extend_from_slice(bits); }
    let reader = index.reader_builder().reload_policy(tantivy::ReloadPolicy::Manual).try_into().map_err(fault)?;
    let searcher = reader.searcher();
    let mut entries = Vec::new();
    for segment in searcher.segment_readers() {
        for (column, field) in columns.iter().enumerate() {
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

/// A uuid-shaped name derived from the job's identity, standing in for tantivy's random segment id.
fn segment_uuid(identity: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(identity.as_bytes());
    digest[..16].iter().map(|byte| format!("{byte:02x}")).collect()
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

/// The bytes a search file is read from: the object as read, or a lease on the buffer pool's copy of it.
pub enum Source { Bytes(bytes::Bytes), Lease(rtdb_storage::cache::CacheLease) }
impl From<bytes::Bytes> for Source { fn from(bytes: bytes::Bytes) -> Self { Self::Bytes(bytes) } }
impl From<Vec<u8>> for Source { fn from(bytes: Vec<u8>) -> Self { Self::Bytes(bytes.into()) } }
impl From<rtdb_storage::cache::CacheLease> for Source { fn from(lease: rtdb_storage::cache::CacheLease) -> Self { Self::Lease(lease) } }
impl std::ops::Deref for Source {
    type Target = [u8];
    fn deref(&self) -> &[u8] { match self { Self::Bytes(bytes) => bytes, Self::Lease(lease) => lease.as_slice() } }
}
// SAFETY: both owners keep their buffer at a fixed address, behind a reference count, for as long as they live, wherever they move.
unsafe impl stable_deref_trait::StableDeref for Source {}

/// A read-only directory over a bundle held in memory.
#[derive(Clone)]
struct BundleDirectory { bytes: OwnedBytes, files: Arc<BTreeMap<PathBuf, Range<usize>>> }

impl std::fmt::Debug for BundleDirectory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.debug_struct("BundleDirectory").field("files", &self.files.keys()).finish() }
}

impl BundleDirectory {
    fn open(bytes: Source) -> Result<Self> {
        let data: &[u8] = &bytes;
        if data.len() < 2 * MAGIC.len() + 8 || &data[..8] != MAGIC || &data[data.len() - 8..] != MAGIC { return Err(corrupt("Search file has no bundle magic")); }
        let footer = usize::try_from(u64::from_le_bytes(data[data.len() - 16..data.len() - 8].try_into().map_err(fault)?)).map_err(fault)?;
        let mut cursor = data.get(footer..data.len() - 16).ok_or_else(|| corrupt("Search file footer is out of range"))?;
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
        Ok(Self { bytes: OwnedBytes::new(bytes), files: Arc::new(files) })
    }
    fn read_only(path: &Path) -> std::io::Error { std::io::Error::new(std::io::ErrorKind::PermissionDenied, format!("Search file is read-only: {}", path.display())) }
}

impl Directory for BundleDirectory {
    fn get_file_handle(&self, path: &Path) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        let range = self.files.get(path).ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(Arc::new(FileSlice::new(Arc::new(self.bytes.slice(range.clone())))))
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
struct Sidecar { bytes: OwnedBytes, page_starts: Vec<u32>, present: Vec<Range<usize>>, impacts: BTreeMap<(u32, Vec<u8>), Range<usize>> }

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
        let pages = u32_at(take(4)?) as usize;
        let page_starts = (0..pages).map(|_| take(4).map(u32_at)).collect::<Result<Vec<_>>>()?;
        if page_starts.first().is_some_and(|first| *first != 0) || page_starts.windows(2).any(|pair| pair[0] > pair[1]) || page_starts.last().is_some_and(|last| *last > documents) {
            return Err(corrupt("Search file page starts are out of order"));
        }
        let present = (0..columns).map(|_| take(documents.div_ceil(8) as usize)).collect::<Result<Vec<_>>>()?;
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
        Ok(Self { bytes, page_starts, present, impacts })
    }
    fn impacts(&self, column: u32, term: &str) -> Option<Vec<Impact>> {
        let range = self.impacts.get(&(column, term.as_bytes().to_vec()))?;
        Some(self.bytes.as_slice()[range.clone()].chunks_exact(12).map(|chunk| {
            let at = |offset: usize| u32::from_le_bytes([chunk[offset], chunk[offset + 1], chunk[offset + 2], chunk[offset + 3]]);
            Impact { window: at(0), frequency: at(4), length: at(8) }
        }).collect())
    }
    fn present(&self, column: usize, doc: u32) -> bool {
        self.present.get(column).and_then(|range| self.bytes.as_slice()[range.clone()].get(doc as usize / 8)).is_some_and(|byte| byte & (1 << (doc % 8)) != 0)
    }
}

/// The fast columns a document's temporal fields are read from.
struct DocColumns { iid_high: tantivy::columnar::Column<u64>, iid_low: tantivy::columnar::Column<u64>, system_from: tantivy::columnar::Column<i64>, valid_from: tantivy::columnar::Column<i64>, valid_to: tantivy::columnar::Column<i64>, recency: tantivy::columnar::Column<i64> }

impl DocColumns {
    fn open(segment: &tantivy::SegmentReader) -> Result<Self> {
        let fast = segment.fast_fields();
        Ok(Self { iid_high: fast.u64("_iid_high").map_err(fault)?, iid_low: fast.u64("_iid_low").map_err(fault)?, system_from: fast.i64("_system_from").map_err(fault)?,
            valid_from: fast.i64("_valid_from").map_err(fault)?, valid_to: fast.i64("_valid_to").map_err(fault)?, recency: fast.i64("_recency").map_err(fault)? })
    }
    fn doc(&self, doc: u32) -> Result<DocMatch> {
        let missing = || corrupt("Search document has no fast value");
        let mut iid = [0u8; 16];
        iid[..8].copy_from_slice(&self.iid_high.first(doc).ok_or_else(missing)?.to_be_bytes());
        iid[8..].copy_from_slice(&self.iid_low.first(doc).ok_or_else(missing)?.to_be_bytes());
        Ok(DocMatch { doc, iid, system_from: self.system_from.first(doc).ok_or_else(missing)?, valid_from: self.valid_from.first(doc).ok_or_else(missing)?,
            valid_to: self.valid_to.first(doc).ok_or_else(missing)?, recency: self.recency.first(doc).ok_or_else(missing)? })
    }
}

/// An opened search file.
pub struct SearchFile { index: Index, columns: Vec<String>, documents: u32, sidecar: Sidecar }

impl SearchFile {
    pub fn open(bytes: impl Into<Source>, columns: &[String]) -> Result<Self> {
        let directory = BundleDirectory::open(bytes.into())?;
        let sidecar = directory.get_file_handle(Path::new(SIDECAR)).map_err(|_| corrupt("Search file has no sidecar"))?;
        let sidecar = sidecar.read_bytes(0..sidecar.len()).map_err(fault)?;
        let index = Index::open(directory).map_err(fault)?;
        let (expected, _) = schema(columns);
        if index.schema() != expected { return Err(corrupt("Search file schema does not match its definition")); }
        let documents = index.searchable_segment_metas().map_err(fault)?.iter().map(|segment| segment.max_doc()).sum();
        Ok(Self { sidecar: Sidecar::parse(sidecar, columns.len(), documents)?, index, columns: columns.to_vec(), documents })
    }

    fn column(&self, column: &str) -> Option<usize> { self.columns.iter().position(|candidate| candidate == column) }
    fn field(&self, column: &str) -> Option<Field> { self.column(column).map(|index| Field::from_field_id(index as u32)) }

    fn searcher(&self) -> Result<tantivy::Searcher> {
        let reader: tantivy::IndexReader = self.index.reader_builder().reload_policy(tantivy::ReloadPolicy::Manual).try_into().map_err(fault)?;
        Ok(reader.searcher())
    }

    /// Every query term and phrase as one conjunction over the column.
    fn conjunction(field: Field, query: &Query) -> BooleanQuery {
        let mut clauses: Vec<(Occur, Box<dyn tantivy::query::Query>)> = query.terms().iter().map(|term| (Occur::Must, Box::new(TermQuery::new(Term::from_field_text(field, term), IndexRecordOption::Basic)) as Box<dyn tantivy::query::Query>)).collect();
        for phrase in query.phrases() {
            clauses.push((Occur::Must, Box::new(tantivy::query::PhraseQuery::new(phrase.iter().map(|term| Term::from_field_text(field, term)).collect()))));
        }
        BooleanQuery::new(clauses)
    }

    /// Every document holding all of the query's terms in the column.
    pub fn matches(&self, column: &str, query: &Query) -> Result<Vec<DocMatch>> {
        let Some(field) = self.field(column) else { return Ok(Vec::new()) };
        if query.terms().is_empty() { return Ok(Vec::new()); }
        let searcher = self.searcher()?;
        let found = searcher.search(&Self::conjunction(field, query), &DocSetCollector).map_err(fault)?;
        let mut docs: Vec<_> = found.into_iter().collect();
        docs.sort();
        let mut matches = Vec::with_capacity(docs.len());
        let mut columns: Option<(u32, DocColumns)> = None;
        for address in docs {
            if columns.as_ref().is_none_or(|(ordinal, _)| *ordinal != address.segment_ord) { columns = Some((address.segment_ord, DocColumns::open(searcher.segment_reader(address.segment_ord))?)); }
            let Some((_, columns)) = &columns else { continue };
            matches.push(columns.doc(address.doc_id)?);
        }
        Ok(matches)
    }

    /// Every document matching `matching`, with its score for `scoring`'s terms, from the postings' frequencies and the stored length.
    /// It equals `rtdb_text::bm25` of the row's tokens, which is what SEARCH_SCORE computes on a resolved row.
    pub fn scored(&self, column: &str, matching: &Query, scoring: &Query, corpus: Option<&Corpus>) -> Result<Vec<(DocMatch, f64)>> {
        let Some(field) = self.field(column) else { return Ok(Vec::new()) };
        let docs = self.matches(column, matching)?;
        let searcher = self.searcher()?;
        let [segment] = searcher.segment_readers() else { return Err(corrupt("A search file holds exactly one segment")) };
        let mut scorer = ExactScorer::open(segment, field, scoring, corpus)?;
        docs.into_iter().map(|doc| Ok((doc, scorer.score(doc.doc)))).collect()
    }

    /// The documents matching `matching`, best possible score for `scoring` first, a window of documents at a time.
    pub fn ranked(&self, column: &str, matching: &Query, scoring: &Query, corpus: Option<&Corpus>) -> Result<Ranked> {
        let searcher = self.searcher()?;
        let (Some(column), Some(field)) = (self.column(column), self.field(column)) else { return Ok(Ranked(None)) };
        if matching.terms().is_empty() { return Ok(Ranked(None)); }
        if searcher.segment_readers().len() != 1 { return Err(corrupt("A search file holds exactly one segment")); }
        let segment = &searcher.segment_readers()[0];
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
        let mut bounds: Vec<(f64, u32)> = windows.unwrap_or_default().into_iter().map(|window| (0.0, window)).collect();
        for term in scoring.terms() {
            let impacts = impacts(term)?;
            for (bound, window) in &mut bounds {
                if let Ok(index) = impacts.binary_search_by_key(window, |impact| impact.window) {
                    *bound += rtdb_text::term_score(term, impacts[index].frequency, impacts[index].length, corpus).max(0.0);
                }
            }
        }
        // A term's part rises with frequency and falls with length exactly, but its rounded value need not; the margin covers that.
        for (bound, _) in &mut bounds { *bound = *bound * (1.0 + 1e-9) + 1e-12; }
        bounds.sort_by(|left, right| right.0.total_cmp(&left.0).then(left.1.cmp(&right.1)));
        Ok(Ranked(Some(Windows { searcher, field, matching: matching.clone(), scoring: scoring.clone(), corpus: corpus.cloned(), windows: bounds, next: 0, batch: 4 })))
    }

    /// Whether MATCH holds on each document: `None` where the column is null or absent, as MATCH is on such a row.
    pub fn match_values(&self, column: &str, query: &Query) -> Result<MatchValues> {
        let documents = self.documents;
        let Some(index) = self.column(column) else { return Ok(MatchValues { present: Vec::new(), matching: Vec::new(), page_starts: self.sidecar.page_starts.clone(), documents }) };
        let mut present = vec![0u64; documents.div_ceil(64) as usize];
        for doc in 0..documents { if self.sidecar.present(index, doc) { present[doc as usize / 64] |= 1 << (doc % 64); } }
        let mut matching = vec![0u64; present.len()];
        for doc in self.matches(column, query)? { matching[doc.doc as usize / 64] |= 1 << (doc.doc % 64); }
        Ok(MatchValues { present, matching, page_starts: self.sidecar.page_starts.clone(), documents })
    }

    /// The column's corpus statistics for the query's terms: documents, tokens, and each term's document frequency.
    pub fn corpus(&self, column: &str, query: &Query) -> Result<Corpus> {
        let Some(field) = self.field(column) else { return Ok(Corpus::default()) };
        let searcher = self.searcher()?;
        let mut corpus = Corpus::default();
        for segment in searcher.segment_readers() {
            let inverted = segment.inverted_index(field).map_err(fault)?;
            corpus.documents += u64::from(segment.max_doc());
            corpus.tokens += inverted.total_num_tokens();
            for term in query.terms() {
                *corpus.frequencies.entry(term.clone()).or_default() += u64::from(inverted.doc_freq(&Term::from_field_text(field, term)).map_err(fault)?);
            }
        }
        Ok(corpus)
    }
}

/// MATCH of one query on every document of a search file, by data-file page and row.
pub struct MatchValues { present: Vec<u64>, matching: Vec<u64>, page_starts: Vec<u32>, documents: u32 }

impl MatchValues {
    /// MATCH on the row at `row` of data page `page`: `None` where the column is null or absent; an error where the file has no such row.
    pub fn value(&self, page: usize, row: usize) -> Result<Option<bool>> {
        let start = *self.page_starts.get(page).ok_or_else(|| corrupt("Search file has no such page"))?;
        let end = self.page_starts.get(page + 1).copied().unwrap_or(self.documents);
        let doc = u32::try_from(row).ok().and_then(|row| start.checked_add(row)).filter(|doc| *doc < end).ok_or_else(|| corrupt("Search file has no such row"))?;
        let bit = |bits: &[u64]| bits.get(doc as usize / 64).is_some_and(|word| word & (1 << (doc % 64)) != 0);
        Ok(bit(&self.present).then(|| bit(&self.matching)))
    }
    /// The bytes held, for a cache's weight.
    pub fn bytes(&self) -> usize { (self.present.len() + self.matching.len()) * 8 + self.page_starts.len() * 4 }
}

impl rtdb_query::FileMatches for MatchValues {
    fn value(&self, page: usize, row: usize) -> Result<Option<bool>> { MatchValues::value(self, page, row) }
}

/// Scores documents in ascending order from the postings of the scoring terms.
struct ExactScorer { postings: Vec<Option<tantivy::postings::SegmentPostings>>, lengths: tantivy::fieldnorm::FieldNormReader, scoring: Query, corpus: Option<Corpus> }

impl ExactScorer {
    fn open(segment: &tantivy::SegmentReader, field: Field, scoring: &Query, corpus: Option<&Corpus>) -> Result<Self> {
        let inverted = segment.inverted_index(field).map_err(fault)?;
        let postings = scoring.terms().iter().map(|term| inverted.read_postings(&Term::from_field_text(field, term), IndexRecordOption::WithFreqs).map_err(fault)).collect::<Result<_>>()?;
        Ok(Self { postings, lengths: segment.get_fieldnorms_reader(field).map_err(fault)?, scoring: scoring.clone(), corpus: corpus.cloned() })
    }
    fn score(&mut self, doc: u32) -> f64 {
        use tantivy::{DocSet, postings::Postings};
        let frequencies: Vec<u32> = self.postings.iter_mut().map(|postings| match postings {
            Some(postings) => if postings.doc() <= doc && postings.seek(doc) == doc { postings.term_freq() } else { 0 },
            None => 0,
        }).collect();
        rtdb_text::score(&self.scoring, &frequencies, self.lengths.fieldnorm(doc), self.corpus.as_ref())
    }
}

/// A search file's matching documents, handed out a batch of windows at a time, best bound first; `None` where nothing in the file can match.
/// No document left behind scores above `bound`.
pub struct Ranked(Option<Windows>);

/// The windows holding every matching term, each with the best score a document in it can have, best first, and how far they have been handed out.
struct Windows { searcher: tantivy::Searcher, field: Field, matching: Query, scoring: Query, corpus: Option<Corpus>, windows: Vec<(f64, u32)>, next: usize, batch: usize }

impl Ranked {
    /// The highest score any document not yet handed out can have, or `None` once all are.
    pub fn bound(&self) -> Option<f64> { self.0.as_ref().and_then(|windows| windows.windows.get(windows.next).map(|(bound, _)| *bound)) }

    /// The matching documents of the next windows, with their exact scores; twice as many windows as the time before.
    pub fn expand(&mut self) -> Result<Vec<(DocMatch, f64)>> { self.0.as_mut().map_or(Ok(Vec::new()), Windows::expand) }
}

impl Windows {
    fn expand(&mut self) -> Result<Vec<(DocMatch, f64)>> {
        use tantivy::{DocSet, query::Query as _};
        let end = self.next.saturating_add(self.batch).min(self.windows.len());
        let mut windows: Vec<u32> = self.windows[self.next..end].iter().map(|(_, window)| *window).collect();
        self.next = end;
        self.batch = self.batch.saturating_mul(2);
        windows.sort_unstable();
        let segment = &self.searcher.segment_readers()[0];
        let weight = SearchFile::conjunction(self.field, &self.matching).weight(tantivy::query::EnableScoring::disabled_from_searcher(&self.searcher)).map_err(fault)?;
        let mut docs = weight.scorer(segment, 1.0).map_err(fault)?;
        let columns = DocColumns::open(segment)?;
        let mut scorer = ExactScorer::open(segment, self.field, &self.scoring, self.corpus.as_ref())?;
        let mut found = Vec::new();
        for window in windows {
            let (start, stop) = (window.saturating_mul(WINDOW), window.saturating_add(1).saturating_mul(WINDOW));
            let mut doc = if docs.doc() < start { docs.seek(start) } else { docs.doc() };
            while doc < stop && doc != tantivy::TERMINATED {
                found.push((columns.doc(doc)?, scorer.score(doc)));
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

    fn row(id: u8, system_from: i64, title: &str, body: &str) -> SearchRow {
        SearchRow { iid: [id; 16], system_from, valid_from: 0, valid_to: i64::MAX, recency: None, values: vec![Some(tokens(title)), Some(tokens(body))] }
    }
    fn columns() -> Vec<String> { vec!["title".into(), "body".into()] }
    fn rows() -> Vec<SearchRow> {
        vec![row(1, 30, "Invoice", "the invoice is overdue"), row(1, 10, "Invoice", "the invoice is paid"), row(2, 20, "Library", "an overdue book"), row(3, 20, "", "")]
    }

    #[test]
    fn a_job_builds_the_same_bytes_every_time() {
        let first = build_search_file(&columns(), &rows(), "public$docs/l01-rc-b00/docs_text").unwrap();
        let second = build_search_file(&columns(), &rows(), "public$docs/l01-rc-b00/docs_text").unwrap();
        assert_eq!(first, second);
        assert_ne!(first, build_search_file(&columns(), &rows(), "public$docs/l01-rc-b01/docs_text").unwrap());
    }

    #[test]
    fn a_segment_built_on_disk_bundles_to_the_same_bytes_as_one_built_in_memory() {
        let directory = tempfile::tempdir().unwrap();
        let mut streamed = Vec::new();
        write_search_file(&columns(), [Ok(rows())], "job", tantivy::directory::MmapDirectory::open(directory.path()).unwrap(), &mut streamed).unwrap();
        assert_eq!(streamed, build_search_file(&columns(), &rows(), "job").unwrap());
    }

    #[test]
    fn matches_name_every_row_holding_all_terms_with_its_own_temporal_columns() {
        let file = SearchFile::open(build_search_file(&columns(), &rows(), "job").unwrap(), &columns()).unwrap();
        let found = file.matches("body", &Query::parse("Overdue")).unwrap();
        assert_eq!(found.iter().map(|doc| (doc.doc, doc.iid[0], doc.system_from)).collect::<Vec<_>>(), [(0, 1, 30), (2, 2, 20)]);
        assert_eq!(found[0].recency, i64::MAX);
        assert_eq!(file.matches("body", &Query::parse("overdue invoice")).unwrap().iter().map(|doc| doc.doc).collect::<Vec<_>>(), [0]);
        assert_eq!(file.matches("title", &Query::parse("invoice")).unwrap().len(), 2);
        assert!(file.matches("absent", &Query::parse("invoice")).unwrap().is_empty());
        assert!(file.matches("body", &Query::parse("!!")).unwrap().is_empty());
        assert_eq!(file.matches("body", &Query::parse(r#""invoice is overdue""#)).unwrap().iter().map(|doc| doc.doc).collect::<Vec<_>>(), [0]);
        assert!(file.matches("body", &Query::parse(r#""overdue invoice""#)).unwrap().is_empty(), "the terms are there, but not side by side");
    }

    #[test]
    fn a_documents_score_from_the_index_is_its_rows_score() {
        let rows = rows();
        let file = SearchFile::open(build_search_file(&columns(), &rows, "job").unwrap(), &columns()).unwrap();
        let scoring = Query::parse("invoice overdue paid absent");
        let corpus = file.corpus("body", &scoring).unwrap();
        for (doc, score) in file.scored("body", &Query::parse("invoice"), &scoring, Some(&corpus)).unwrap() {
            assert_eq!(score, rtdb_text::bm25(&scoring, rows[doc.doc as usize].values[1].as_deref().unwrap(), Some(&corpus)), "doc {}", doc.doc);
        }
        let long: Vec<String> = (0..500).map(|index| if index % 7 == 0 { "invoice".into() } else { format!("w{index}") }).collect();
        let rows = vec![SearchRow { iid: [9; 16], system_from: 0, valid_from: 0, valid_to: i64::MAX, recency: None, values: vec![None, Some(long.clone())] }];
        let file = SearchFile::open(build_search_file(&columns(), &rows, "job").unwrap(), &columns()).unwrap();
        let [(_, score)] = file.scored("body", &Query::parse("invoice"), &Query::parse("invoice"), None).unwrap()[..] else { panic!() };
        assert_eq!(score, rtdb_text::bm25(&Query::parse("invoice"), &long, None), "a long document's stored length is rounded the same way");
    }

    #[test]
    fn corpus_statistics_count_documents_tokens_and_term_frequencies() {
        let file = SearchFile::open(build_search_file(&columns(), &rows(), "job").unwrap(), &columns()).unwrap();
        let corpus = file.corpus("body", &Query::parse("overdue invoice")).unwrap();
        assert_eq!(corpus.documents, 4);
        assert_eq!(corpus.tokens, 11);
        assert_eq!(corpus.frequencies, BTreeMap::from([("overdue".into(), 2), ("invoice".into(), 2)]));
    }

    #[test]
    fn an_empty_trie_and_a_corrupt_object_are_told_apart() {
        let empty = SearchFile::open(build_search_file(&columns(), &[], "job").unwrap(), &columns()).unwrap();
        assert!(empty.matches("body", &Query::parse("anything")).unwrap().is_empty());
        let mut bytes = build_search_file(&columns(), &rows(), "job").unwrap();
        let length = bytes.len();
        bytes[length - 1] ^= 1;
        assert!(SearchFile::open(bytes, &columns()).is_err());
        assert!(SearchFile::open(build_search_file(&columns(), &rows(), "job").unwrap(), &["body".to_string()]).is_err(), "a definition with other columns does not read the file");
    }

    fn corpus_rows(count: usize, seed: u64) -> Vec<SearchRow> {
        let words = ["invoice", "overdue", "paid", "book", "late", "red", "blue", "green"];
        let mut state = seed;
        let mut next = || { state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (state >> 33) as usize };
        (0..count).map(|index| {
            let body: Vec<String> = (0..1 + next() % 12).map(|_| words[next() % words.len()].to_string()).collect();
            let value = match index % 17 { 0 => None, 1 => Some(Vec::new()), _ => Some(body) };
            SearchRow { iid: [(index % 251) as u8; 16], system_from: 0, valid_from: 0, valid_to: i64::MAX, recency: None, values: vec![None, value] }
        }).collect()
    }

    #[test]
    fn ranked_windows_bound_every_match_and_hand_each_out_once() {
        let rows = corpus_rows(5000, 7);
        let file = SearchFile::open(build_search_file(&columns(), &rows, "job").unwrap(), &columns()).unwrap();
        assert!(file.sidecar.impacts.keys().any(|(column, _)| *column == 1), "common terms have their impacts stored");
        for (matching, scoring) in [("invoice", "invoice overdue"), ("red blue", "red"), ("\"paid late\"", "paid late green"), ("absent", "absent")] {
            let (matching, scoring) = (Query::parse(matching), Query::parse(scoring));
            let corpus = file.corpus("body", &scoring).unwrap();
            let mut expected = file.scored("body", &matching, &scoring, Some(&corpus)).unwrap();
            let mut ranked = file.ranked("body", &matching, &scoring, Some(&corpus)).unwrap();
            let windows: BTreeMap<u32, f64> = ranked.0.as_ref().map_or_else(BTreeMap::new, |windows| windows.windows.iter().map(|(bound, window)| (*window, *bound)).collect());
            for (doc, score) in &expected {
                let bound = windows.get(&(doc.doc / WINDOW)).unwrap_or_else(|| panic!("{matching:?}: doc {} is in no window", doc.doc));
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
            if !expected.is_empty() { assert!(expansions > 1, "{matching:?} was handed out in one batch"); }
        }
    }

    #[test]
    fn match_values_tell_a_null_column_from_one_that_does_not_match_by_page_and_row() {
        let rows = corpus_rows(300, 3);
        let pages: Vec<Vec<SearchRow>> = rows.chunks(70).map(<[SearchRow]>::to_vec).collect();
        let mut bytes = Vec::new();
        write_search_file(&columns(), pages.iter().cloned().map(Ok), "job", RamDirectory::create(), &mut bytes).unwrap();
        let file = SearchFile::open(bytes, &columns()).unwrap();
        let query = Query::parse("invoice");
        let values = file.match_values("body", &query).unwrap();
        for (page, rows) in pages.iter().enumerate() {
            for (row, search_row) in rows.iter().enumerate() {
                let expected = search_row.values[1].as_ref().map(|tokens| query.matches(tokens));
                assert_eq!(values.value(page, row).unwrap(), expected, "page {page} row {row}");
            }
            assert!(values.value(page, rows.len()).is_err() || page + 1 < pages.len(), "past the last row of the last page");
        }
        assert!(values.value(pages.len(), 0).is_err());
        assert!(file.match_values("title", &query).unwrap().value(0, 0).unwrap().is_none(), "a column no row holds is null everywhere");
    }

    #[test]
    fn a_word_longer_than_tantivy_keeps_is_matched_and_scored_as_the_row_is() {
        let long = format!("{} overdue", "x".repeat(70_000));
        let rows = vec![row(1, 0, "", &long), row(2, 0, "", "overdue")];
        let file = SearchFile::open(build_search_file(&columns(), &rows, "job").unwrap(), &columns()).unwrap();
        for query in [Query::parse(&"x".repeat(70_000)), Query::parse("overdue")] {
            let corpus = file.corpus("body", &query).unwrap();
            let expected: Vec<u32> = (0..2).filter(|doc| query.matches(rows[*doc as usize].values[1].as_deref().unwrap())).collect();
            let scored = file.scored("body", &query, &query, Some(&corpus)).unwrap();
            assert_eq!(scored.iter().map(|(doc, _)| doc.doc).collect::<Vec<_>>(), expected);
            for (doc, score) in scored { assert_eq!(score, rtdb_text::bm25(&query, rows[doc.doc as usize].values[1].as_deref().unwrap(), Some(&corpus))); }
        }
    }

    #[test]
    fn a_sidecar_whose_bytes_changed_is_unreadable() {
        let bytes = build_search_file(&columns(), &corpus_rows(2000, 1), "job").unwrap();
        let directory = BundleDirectory::open(Source::from(bytes.clone())).unwrap();
        let range = directory.files[Path::new(SIDECAR)].clone();
        for offset in [range.start, range.start + range.len() / 2, range.end - 1] {
            let mut corrupted = bytes.clone();
            corrupted[offset] ^= 0x10;
            assert!(SearchFile::open(corrupted, &columns()).is_err(), "byte {offset}");
        }
    }
}
