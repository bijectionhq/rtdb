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
pub const FORMAT_VERSION: i32 = 1;
const WRITER_MEMORY: usize = 32 << 20;

/// One data-file row as the search file sees it: its own temporal columns and the tokens of each indexed column.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchRow {
    pub iid: [u8; 16],
    pub system_from: i64,
    pub valid_from: i64,
    pub valid_to: i64,
    pub recency: Option<i64>,
    /// Parallel to the file's columns; a row with no text in a column holds no tokens there.
    pub tokens: Vec<Vec<String>>,
}

/// A document whose terms satisfied a query, with the row's own temporal columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DocMatch { pub doc: u32, pub iid: [u8; 16], pub system_from: i64, pub valid_from: i64, pub valid_to: i64, pub recency: i64 }

struct Fields { columns: Vec<Field>, iid_high: Field, iid_low: Field, system_from: Field, valid_from: Field, valid_to: Field, recency: Field }

fn schema(columns: &[String]) -> (Schema, Fields) {
    let mut builder = Schema::builder();
    let text = TextOptions::default().set_indexing_options(TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs).set_fieldnorms(true));
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

/// Builds the bundled search file over rows in data-file order, in memory.
pub fn build_search_file(columns: &[String], rows: &[SearchRow], identity: &str) -> Result<Vec<u8>> {
    let mut bundle = Vec::new();
    write_search_file(columns, rows.iter().cloned().map(Ok), identity, RamDirectory::create(), &mut bundle)?;
    Ok(bundle)
}

/// Builds the search file over rows in data-file order, building its segment in `directory` and writing the bundle to `output`.
/// The bytes are a function of the rows, the columns and `identity` alone, wherever the segment is built, so every builder of one job writes the same object.
pub fn write_search_file<D: Directory + Clone + 'static, W: std::io::Write>(columns: &[String], rows: impl IntoIterator<Item = Result<SearchRow>>, identity: &str, directory: D, output: W) -> Result<()> {
    let (schema, fields) = schema(columns);
    let settings = IndexSettings { docstore_compression: Compressor::None, ..IndexSettings::default() };
    let mut writer = Index::builder().schema(schema).settings(settings).single_segment_index_writer(directory.clone(), WRITER_MEMORY).map_err(fault)?;
    for row in rows {
        let row = row?;
        let row = &row;
        if row.tokens.len() != columns.len() { return Err(fault("Search row tokens do not cover the file's columns")); }
        let mut document = TantivyDocument::new();
        for (field, tokens) in fields.columns.iter().zip(&row.tokens) {
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
    }
    writer.finalize().map_err(fault)?;
    bundle(&directory, &segment_uuid(identity), output)
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
    let mut files: BTreeMap<String, Option<PathBuf>> = BTreeMap::from([("meta.json".to_string(), None)]);
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

/// An opened search file.
pub struct SearchFile { index: Index, columns: Vec<String> }

impl SearchFile {
    pub fn open(bytes: impl Into<Source>, columns: &[String]) -> Result<Self> {
        let index = Index::open(BundleDirectory::open(bytes.into())?).map_err(fault)?;
        let (expected, _) = schema(columns);
        if index.schema() != expected { return Err(corrupt("Search file schema does not match its definition")); }
        Ok(Self { index, columns: columns.to_vec() })
    }

    fn field(&self, column: &str) -> Option<Field> {
        self.columns.iter().position(|candidate| candidate == column).map(|index| Field::from_field_id(index as u32))
    }

    /// Every document holding all of the query's terms in the column.
    pub fn matches(&self, column: &str, query: &Query) -> Result<Vec<DocMatch>> {
        let Some(field) = self.field(column) else { return Ok(Vec::new()) };
        if query.terms().is_empty() { return Ok(Vec::new()); }
        let reader = self.index.reader_builder().reload_policy(tantivy::ReloadPolicy::Manual).try_into().map_err(fault)?;
        let searcher = reader.searcher();
        let clauses = query.terms().iter().map(|term| (Occur::Must, Box::new(TermQuery::new(Term::from_field_text(field, term), IndexRecordOption::Basic)) as Box<dyn tantivy::query::Query>)).collect();
        let found = searcher.search(&BooleanQuery::new(clauses), &DocSetCollector).map_err(fault)?;
        let mut docs: Vec<_> = found.into_iter().collect();
        docs.sort();
        let mut matches = Vec::with_capacity(docs.len());
        for address in docs {
            let segment = searcher.segment_reader(address.segment_ord);
            let fast = segment.fast_fields();
            let doc = address.doc_id;
            let u64_at = |name: &str| -> Result<u64> { fast.u64(name).map_err(fault)?.first(doc).ok_or_else(|| corrupt("Search document has no fast value")) };
            let i64_at = |name: &str| -> Result<i64> { fast.i64(name).map_err(fault)?.first(doc).ok_or_else(|| corrupt("Search document has no fast value")) };
            let mut iid = [0u8; 16];
            iid[..8].copy_from_slice(&u64_at("_iid_high")?.to_be_bytes());
            iid[8..].copy_from_slice(&u64_at("_iid_low")?.to_be_bytes());
            matches.push(DocMatch { doc, iid, system_from: i64_at("_system_from")?, valid_from: i64_at("_valid_from")?, valid_to: i64_at("_valid_to")?, recency: i64_at("_recency")? });
        }
        Ok(matches)
    }

    /// The column's corpus statistics for the query's terms: documents, tokens, and each term's document frequency.
    pub fn corpus(&self, column: &str, query: &Query) -> Result<Corpus> {
        let Some(field) = self.field(column) else { return Ok(Corpus::default()) };
        let reader = self.index.reader_builder().reload_policy(tantivy::ReloadPolicy::Manual).try_into().map_err(fault)?;
        let searcher = reader.searcher();
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

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_text::tokens;

    fn row(id: u8, system_from: i64, title: &str, body: &str) -> SearchRow {
        SearchRow { iid: [id; 16], system_from, valid_from: 0, valid_to: i64::MAX, recency: None, tokens: vec![tokens(title), tokens(body)] }
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
        write_search_file(&columns(), rows().into_iter().map(Ok), "job", tantivy::directory::MmapDirectory::open(directory.path()).unwrap(), &mut streamed).unwrap();
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
}
