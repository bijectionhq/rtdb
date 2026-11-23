use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StructArray, cast::AsArray, types::{Int32Type, Int64Type, UInt32Type}};
use arrow_buffer::Buffer;
use rtdb_storage::{ObjectKey, cache::BufferPool};
use rtdb_types::{EventHeader, Iid, Interval, MemoryBudget, MemoryReservation, Result, Timestamp};
use sha2::{Digest, Sha256};
use crate::rank_windows::window_of;
use crate::term_directory::TermSummaries;

use super::{LOGICAL_FORMAT_VERSION, PACKED_FORMAT_VERSION, TREE_FORMAT_VERSION, TextRevision, artefact_digest, corrupt, fault, layout::{FooterLocator, PageRef, Table, Writer, item_of, list, table},
    page_tree::{Child, DirectoryRef, PageCache, PageKind, PageOwner, PageSource, TreeReader, inline_routes, read_inline_routes, read_root, root_table, write_directory}, paged::PostingPlan, rank_tree};

pub(super) const POSTING: u8 = 0;
pub(super) const TERM: u8 = 1;
pub(super) const DOCUMENT: u8 = 2;
pub(super) const VISIBILITY: u8 = 3;
pub(super) const PAGE_ROWS: u8 = 4;
const ERASES: u8 = 5;
const TERM_ROWS: usize = 32;
pub(super) const DOCUMENT_ROWS: usize = 256;
const EVENT_ROWS: usize = 1024;
pub(crate) const MAX_ROOT_BYTES: u64 = 64 * 1024;
const INLINE_ROOT_BYTES: usize = 8 * 1024;
const SYNOPSIS_BYTES: usize = 512;

fn page_kind(directory: u8) -> PageKind {
    match directory {
        POSTING => PageKind::Postings,
        TERM => PageKind::Terms,
        DOCUMENT => PageKind::Documents,
        VISIBILITY | ERASES => PageKind::Visibility,
        PAGE_ROWS => PageKind::PrimaryLayout,
        _ => unreachable!("the tree accepts only known directory types"),
    }
}

fn synopsis_bits(key: &[u8]) -> impl Iterator<Item = usize> {
    let digest: [u8; 32] = Sha256::digest(key).into();
    (0..4).map(move |part| u16::from_le_bytes([digest[part * 2], digest[part * 2 + 1]]) as usize % (SYNOPSIS_BYTES * 8))
}

pub(super) fn term_key(column: u32, term: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + term.len());
    key.extend_from_slice(&column.to_be_bytes());
    key.extend_from_slice(term.as_bytes());
    key
}

pub(super) fn ordinal(row: usize) -> [u8; 4] { (row as u32).to_be_bytes() }

pub(super) fn event_key(iid: &[u8], row: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(20);
    key.extend_from_slice(iid);
    key.extend_from_slice(&row.to_be_bytes());
    key
}

fn start_rows(table: Table, array: &StructArray, start: usize, len: usize) -> Result<StructArray> {
    let item = item_of(&table.fields()[0]);
    let column = array.column(0).as_list::<i64>();
    let values = (0..column.len()).map(|row| column.value(row).slice(start, len)).collect::<Vec<_>>();
    let first = column.values().as_primitive::<UInt32Type>();
    let counts = vec![len; values.len()];
    let rows = arrow_array::UInt32Array::from_iter_values(values.iter().flat_map(|value| {
        value.as_primitive::<UInt32Type>().values().iter().copied()
    }));
    if rows.len() > first.len() { return Err(fault("Search row allocation exceeds its source")); }
    table.array(vec![list(Arc::new(rows), counts, item)?])
}

pub(super) fn write(writer: &mut Writer<impl Write>, mut fields: Vec<(Table, StructArray)>,
    [(_, events), (_, runs), (_, row_runs)]: [(Table, StructArray); 3], plan: PostingPlan,
    positions: Option<Vec<u32>>, revisions: Option<StructArray>, token_sequences: Option<StructArray>,
    erases: Option<StructArray>) -> Result<TermSummaries> {
    let take = |fields: &mut Vec<(Table, StructArray)>, field: Table| -> StructArray {
        let at = fields.iter().position(|(table, _)| *table as usize == field as usize).expect("writer populated this table");
        fields.remove(at).1
    };
    let terms = take(&mut fields, Table::Terms);
    let blocks = take(&mut fields, Table::Blocks);
    let impact_ids = take(&mut fields, Table::ImpactIds);
    let windows = take(&mut fields, Table::Windows);
    let pages = take(&mut fields, Table::Pages);
    let mut routing = BTreeMap::<u8, Vec<Child>>::new();
    let packed = positions.is_some();
    let mut directory = |writer: &mut _, kind, children: Vec<Child>| {
        if packed && children.len() <= 48 && children.iter().all(|child| child.lower.len() + child.upper.len() <= 256) {
            routing.insert(kind, children.clone());
        }
        write_directory(writer, children)
    };
    let members = fields.iter().find(|(shape, _)| matches!(shape, Table::Members)).expect("member starts are recorded");
    let member_starts = members.1.column(0).as_primitive::<UInt32Type>().clone();
    let page_starts = pages.column(0).as_primitive::<UInt32Type>();
    let frequencies = plan.write(writer)?;
    let postings = frequencies.into_iter().enumerate().map(|(index, page)| {
        let key = ordinal(index + super::paged::FIRST_POSTING_PAGE).to_vec();
        Child { lower: key.clone(), upper: key, page }
    }).collect();
    let posting_dir = directory(writer, POSTING, postings)?;

    let term_columns = terms.column(0).as_primitive::<UInt32Type>();
    let term_names = terms.column(1).as_string::<i32>();
    let mut synopsis = [0u8; SYNOPSIS_BYTES];
    for row in 0..terms.len() {
        for bit in synopsis_bits(&term_key(term_columns.value(row), term_names.value(row))) {
            synopsis[bit / 8] |= 1 << (bit % 8);
        }
    }
    let mut term_pages = Vec::new();
    let mut summaries = TermSummaries::default();
    let blocks = blocks.column(0).as_list::<i64>();
    let impact_ids = impact_ids.column(0).as_list::<i64>();
    for start in (0..terms.len()).step_by(TERM_ROWS) {
        let end = (start + TERM_ROWS).min(terms.len());
        let key = |row| term_key(term_columns.value(row), term_names.value(row));
        let nodes = (start..end).map(|row| rank_tree::write_term(writer, blocks, impact_ids, row)).collect::<Result<Vec<_>>>()?;
        let entry = [
            (Table::Terms, terms.slice(start, end - start)),
            (Table::RankNodes, rank_tree::nodes_array(&nodes)?),
            (Table::Blocks, rank_tree::inline_rows(blocks, &nodes, start, Table::Blocks)?),
            (Table::ImpactIds, rank_tree::inline_rows(impact_ids, &nodes, start, Table::ImpactIds)?),
        ];
        if token_sequences.is_some() {
            summaries.push(entry[0].1.clone(), entry[1].1.clone());
        }
        if packed && start == 0 && end == terms.len() && entry.iter().map(|(_, array)| array.get_array_memory_size()).sum::<usize>() <= 8192 {
            fields.extend(entry.iter().cloned());
        }
        let page = writer.page(entry)?;
        term_pages.push(Child { lower: key(start), upper: key(end - 1), page });
    }
    let term_dir = directory(writer, TERM, term_pages)?;

    let mut doc_pages = Vec::new();
    for start in (0..runs.len()).step_by(DOCUMENT_ROWS) {
        let end = (start + DOCUMENT_ROWS).min(runs.len());
        let first_window = window_of(start as u32) as usize;
        let last_window = window_of((end - 1) as u32) as usize;
        let start_row = Table::Pages.array(vec![Arc::new(arrow_array::UInt32Array::from(vec![start as u32]))])?;
        let mut fields = vec![(Table::Runs, runs.slice(start, end - start)), (Table::Pages, start_row),
            (Table::Windows, windows.slice(first_window, last_window - first_window + 1))];
        if let Some(sequences) = &token_sequences {
            let sequences = sequences.slice(start, end - start);
            let item = item_of(&Table::TokenSequences.fields()[2]);
            fields.push((Table::TokenSequences, Table::TokenSequences.array(vec![
                sequences.column(0).clone(), sequences.column(1).clone(),
                list(Arc::new(arrow_array::StringArray::from(Vec::<&str>::new())), std::iter::repeat_n(0, end - start), item)?,
                sequences.column(3).clone(), sequences.column(4).clone(), sequences.column(5).clone(),
            ])?));
        }
        let page = writer.page(fields)?;
        doc_pages.push(Child { lower: ordinal(start).to_vec(), upper: ordinal(end - 1).to_vec(), page });
    }
    let doc_dir = directory(writer, DOCUMENT, doc_pages)?;

    let iids = events.column(0).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search IID layout");
    let mut event_pages = Vec::new();
    if let Some(sequences) = &token_sequences {
        for start in (0..sequences.len()).step_by(DOCUMENT_ROWS) {
            let end = (start + DOCUMENT_ROWS).min(sequences.len());
            let page = writer.page([(Table::TokenSequences, sequences.slice(start, end - start))])?;
            event_pages.push(Child { lower: ordinal(start).to_vec(), upper: ordinal(end - 1).to_vec(), page });
        }
    }
    for start in (0..events.len()).step_by(EVENT_ROWS) {
        let end = (start + EVENT_ROWS).min(events.len());
        let start_row = Table::Pages.array(vec![Arc::new(arrow_array::UInt32Array::from(vec![start as u32]))])?;
        let mut fields = vec![(Table::Events, events.slice(start, end - start)), (Table::Pages, start_row),
            (Table::RowRuns, start_rows(Table::RowRuns, &row_runs, start, end - start)?)];
        if let Some(positions) = &positions {
            let (mut members, mut pages, mut local_rows) = (Vec::new(), Vec::new(), Vec::new());
            for row in start as u32..end as u32 {
                let page = page_starts.values().partition_point(|&first| first <= row).checked_sub(1)
                    .ok_or_else(|| fault("Search event precedes its primary page"))?;
                let member = member_starts.values().partition_point(|&first| first <= page as u32).checked_sub(1)
                    .ok_or_else(|| fault("Search page precedes its primary member"))?;
                members.push(member as u32);
                pages.push(page as u32 - member_starts.value(member));
                local_rows.push(row - page_starts.value(page));
            }
            fields.push((Table::Versions, Table::Versions.array(vec![
                Arc::new(arrow_array::UInt32Array::from(positions[start..end].to_vec())),
                Arc::new(arrow_array::UInt32Array::from(members)), Arc::new(arrow_array::UInt32Array::from(pages)),
                Arc::new(arrow_array::UInt32Array::from(local_rows)),
            ])?));
        }
        if let Some(revisions) = &revisions {
            let item = item_of(&Table::TextRevisions.fields()[0]);
            let columns = revisions.column(0).as_list::<i64>();
            let pieces: Vec<_> = (0..columns.len()).map(|index| columns.value(index).slice(start, end - start)).collect();
            let refs = pieces.iter().map(|value| value.as_ref()).collect::<Vec<_>>();
            fields.push((Table::TextRevisions, Table::TextRevisions.array(vec![list(
                arrow_select::concat::concat(&refs).map_err(fault)?, std::iter::repeat_n(end - start, columns.len()), item,
            )?])?));
        }
        let page = writer.page(fields)?;
        event_pages.push(Child {
            lower: event_key(iids.value(start), start as u32), upper: event_key(iids.value(end - 1), (end - 1) as u32), page,
        });
    }
    let event_dir = directory(writer, VISIBILITY, event_pages)?;
    let counts = Table::Pages.array(vec![Arc::new(arrow_array::UInt32Array::from(vec![events.len() as u32, runs.len() as u32]))])?;
    let page_map = writer.page([(Table::Pages, pages)])?;
    let page_dir = directory(writer, PAGE_ROWS, vec![Child { lower: ordinal(0).to_vec(), upper: ordinal(0).to_vec(), page: page_map }])?;
    fields.push((Table::Pages, counts));
    fields.push((Table::Synopsis, Table::Synopsis.array(vec![Arc::new(
        arrow_array::FixedSizeBinaryArray::try_from_iter([synopsis].iter()).map_err(fault)?,
    )])?));
    let mut directories = vec![(POSTING, posting_dir), (TERM, term_dir), (DOCUMENT, doc_dir), (VISIBILITY, event_dir), (PAGE_ROWS, page_dir)];
    if token_sequences.is_some() || revisions.is_some() {
        let mut erase_pages = Vec::new();
        if let Some(erases) = erases {
            for start in (0..erases.len()).step_by(EVENT_ROWS) {
                let end = (start + EVENT_ROWS).min(erases.len());
                let page = writer.page([(Table::Erases, erases.slice(start, end - start))])?;
                erase_pages.push(Child { lower: ordinal(start).to_vec(), upper: ordinal(end - 1).to_vec(), page });
            }
        }
        directories.push((ERASES, directory(writer, ERASES, erase_pages)?));
    }
    fields.push((Table::Root, root_table(&directories)?));
    if packed {
        let mut direct = Vec::new();
        let mut bytes = 0;
        for kind in [TERM, VISIBILITY, DOCUMENT, PAGE_ROWS, POSTING] {
            let Some(children) = routing.remove(&kind) else { continue; };
            let added: usize = children.iter().map(|child| child.lower.len() + child.upper.len() + 64).sum();
            if direct.len() + children.len() <= 48 && bytes + added <= 4096 {
                bytes += added;
                direct.extend(children.into_iter().map(|child| (kind, child)));
            }
        }
        fields.push((Table::Routes, inline_routes(&direct)?));
    }
    let root = writer.page(fields)?;
    let range = root.range()?;
    if range.end - range.start > MAX_ROOT_BYTES { return Err(fault("Search root exceeds its read budget")); }
    Ok(summaries)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TreeLocator {
    pub length: u64,
    page: PageRef,
    synopsis: Option<Arc<[u8; SYNOPSIS_BYTES]>>,
    canonical_uuid_iids: bool,
    format: i32,
    inline_root: Option<Arc<[u8]>>,
}

impl TreeLocator {
    pub fn read(input: &mut File, format: i32) -> Result<Self> {
        let locator = FooterLocator::read(input)?;
        let footer_len = locator.length - locator.start - 10;
        input.seek(SeekFrom::Start(locator.start as u64)).map_err(fault)?;
        let mut footer = vec![0; footer_len];
        input.read_exact(&mut footer).map_err(fault)?;
        let footer = arrow_ipc::root_as_footer(&footer).map_err(fault)?;
        let batches = footer.recordBatches().filter(|batches| !batches.is_empty())
            .ok_or_else(|| corrupt("Search tree has no root batch"))?;
        let block = batches.get(batches.len() - 1);
        let mut page = PageRef { offset: u64::try_from(block.offset()).map_err(fault)?,
            metadata: u32::try_from(block.metaDataLength()).map_err(fault)?, body: u64::try_from(block.bodyLength()).map_err(fault)?, digest: [0; 32] };
        let range = page.range()?;
        if range.end > locator.start as u64 || range.end - range.start > MAX_ROOT_BYTES {
            return Err(corrupt("Search root range is invalid"));
        }
        input.seek(SeekFrom::Start(range.start)).map_err(fault)?;
        let mut bytes = vec![0; (range.end - range.start) as usize];
        input.read_exact(&mut bytes).map_err(fault)?;
        page.digest = Sha256::digest(&bytes).into();
        let decoder = arrow_ipc::reader::FileDecoder::new(Arc::clone(super::layout::schema(format)), arrow_ipc::MetadataVersion::V5);
        let inline_root = (format >= PACKED_FORMAT_VERSION && lz4_flex::block::compress(&bytes).len() <= INLINE_ROOT_BYTES)
            .then(|| Arc::from(bytes.as_slice()));
        let root = page.decode(Buffer::from_vec(bytes), &decoder)?;
        let synopsis = table(&root, Table::Synopsis);
        let bits = synopsis.column(0).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search synopsis layout");
        if bits.len() != 1 || bits.is_null(0) { return Err(corrupt("Search root has no complete term synopsis")); }
        let synopsis = Arc::new(bits.value(0).try_into().map_err(fault)?);
        Ok(Self { length: locator.length as u64, page, synopsis: Some(synopsis), format, inline_root, canonical_uuid_iids: false })
    }

    pub fn encode(&self) -> String {
        let digest = encode_hex(&self.page.digest);
        let prefix = match self.format { LOGICAL_FORMAT_VERSION => "v16:", PACKED_FORMAT_VERSION => "v15:", _ => "" };
        let header = format!("{prefix}{}:{}:{}:{}:{digest}", self.length, self.page.offset, self.page.metadata, self.page.body);
        let Some(bits) = self.synopsis.as_deref() else { return header; };
        let check = synopsis_digest(&self.page.digest, bits);
        let mut encoded = format!("{header}:{}:{}", encode_hex(bits), encode_hex(&check));
        if let Some(root) = &self.inline_root {
            encoded.push(':');
            encoded.push_str(&encode_hex(&lz4_flex::block::compress(root)));
        } else if self.format >= PACKED_FORMAT_VERSION && self.canonical_uuid_iids {
            encoded.push(':');
        }
        if self.canonical_uuid_iids {
            encoded.push(':');
            encoded.push_str(&encode_hex(&uuid_evidence_digest(&self.page.digest)));
        }
        encoded
    }

    pub fn decode(value: &str) -> Option<Self> {
        Self::decode_inner(value, true)
    }

    pub fn decode_route(value: &str) -> Option<Self> {
        Self::decode_inner(value, false)
    }

    fn decode_inner(value: &str, load_inline: bool) -> Option<Self> {
        let (format, value) = if let Some(value) = value.strip_prefix("v16:") { (LOGICAL_FORMAT_VERSION, value) }
            else if let Some(value) = value.strip_prefix("v15:") { (PACKED_FORMAT_VERSION, value) }
            else { (TREE_FORMAT_VERSION, value) };
        let mut fields = value.split(':');
        let length = fields.next()?.parse().ok()?;
        let offset = fields.next()?.parse().ok()?;
        let metadata = fields.next()?.parse().ok()?;
        let body = fields.next()?.parse().ok()?;
        let digest = decode_hex(fields.next()?)?;
        let page = PageRef { offset, metadata, body, digest };
        let range = page.range().ok()?;
        if range.end > length || range.end - range.start > MAX_ROOT_BYTES { return None; }
        let synopsis = fields.next().and_then(decode_hex::<SYNOPSIS_BYTES>).zip(fields.next().and_then(decode_hex::<32>))
            .filter(|(bits, check)| synopsis_digest(&digest, bits) == *check).map(|(bits, _)| Arc::new(bits));
        let root = if format >= PACKED_FORMAT_VERSION { fields.next() } else { None };
        let expected = (range.end - range.start) as usize;
        let inline_root = root.filter(|hex| load_inline && hex.len() <= INLINE_ROOT_BYTES * 2).and_then(decode_bytes)
            .and_then(|bytes| lz4_flex::block::decompress(&bytes, expected).ok())
            .filter(|bytes| bytes.len() == expected)
            .filter(|bytes| <[u8; 32]>::from(Sha256::digest(bytes)) == digest).map(Arc::from);
        let canonical_uuid_iids = synopsis.is_some() && fields.next().and_then(decode_hex::<32>)
            .is_some_and(|check| check == uuid_evidence_digest(&digest));
        Some(Self { length, page, synopsis, format, inline_root, canonical_uuid_iids })
    }

    pub fn with_uuid_iid_evidence(self, canonical_uuid_iids: bool) -> Self {
        Self { canonical_uuid_iids, ..self }
    }

    pub fn has_canonical_uuid_iids(&self) -> bool { self.canonical_uuid_iids }

    pub fn digest(&self) -> [u8; 32] { self.page.digest }
    pub fn format(&self) -> i32 { self.format }
    pub(crate) fn inline_root_bytes(&self) -> usize { self.inline_root.as_ref().map_or(0, |root| root.len()) }

    pub fn without_inline_root(&self) -> Self {
        Self { inline_root: None, ..self.clone() }
    }

    pub fn might_contain(&self, column: u32, term: &str) -> bool {
        self.synopsis.as_ref().is_none_or(|synopsis| synopsis_bits(&term_key(column, term))
            .all(|bit| synopsis[bit / 8] & (1 << (bit % 8)) != 0))
    }
}

fn encode_hex(bytes: &[u8]) -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() }

fn decode_bytes(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) { return None; }
    (0..hex.len()).step_by(2).map(|position| u8::from_str_radix(&hex[position..position + 2], 16).ok()).collect()
}

fn decode_hex<const N: usize>(hex: &str) -> Option<[u8; N]> {
    if hex.len() != N * 2 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) { return None; }
    let mut bytes = [0; N];
    for (position, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * position..2 * position + 2], 16).ok()?;
    }
    Some(bytes)
}

fn uuid_evidence_digest(root: &[u8; 32]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"rtdb.search/canonical-uuid-iids");
    digest.update(root);
    digest.finalize().into()
}

fn synopsis_digest(root: &[u8; 32], bits: &[u8; SYNOPSIS_BYTES]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(root);
    digest.update(bits);
    digest.finalize().into()
}

pub(crate) struct TreeFile {
    reader: TreeReader,
    root: RecordBatch,
    directories: BTreeMap<u8, DirectoryRef>,
    routes: BTreeMap<u8, Vec<Child>>,
    format: i32,
}

pub(crate) struct PackedEvent {
    pub header: EventHeader,
    pub member: u32,
    pub page: u32,
    pub row: u32,
    pub document: Option<u32>,
    pub revision: Option<TextRevision>,
}

pub(crate) struct LogicalDocument {
    pub iid: Iid,
    pub sql_id: Option<i64>,
    pub field: usize,
    pub revision: TextRevision,
    pub tokens: arrow_array::StringArray,
    pub position: super::SearchPosition,
}

impl TreeFile {
    pub async fn remote(storage: Arc<BufferPool>, object: ObjectKey, cache: Arc<PageCache>, locator: &TreeLocator,
        columns: &[String], identity: &str, version: i64) -> Result<Self> {
        Self::open(PageSource::Remote(PageOwner::new(&storage, object, cache)), locator, columns, identity, version).await
    }

    pub async fn local(bytes: Buffer, cache: Option<(Arc<BufferPool>, ObjectKey, Arc<PageCache>)>, locator: &TreeLocator,
        columns: &[String], identity: &str, version: i64) -> Result<Self> {
        if bytes.len() as u64 != locator.length { return Err(corrupt("Search root size disagrees with the object")); }
        let owner = cache.map(|(storage, object, cache)| PageOwner::new(&storage, object, cache));
        Self::open(PageSource::Local { bytes, owner }, locator, columns, identity, version).await
    }

    async fn open(source: PageSource, locator: &TreeLocator, columns: &[String], identity: &str, version: i64) -> Result<Self> {
        let reader = TreeReader::new(source, locator.length, locator.format);
        let root = match &locator.inline_root {
            Some(bytes) => reader.inline_page(locator.page, Arc::clone(bytes))?,
            None => reader.page(locator.page, PageKind::Root).await?,
        };
        let header = table(&root, Table::Header);
        let definitions = header.column(2).as_primitive::<Int64Type>();
        let names = table(&root, Table::Columns);
        let keys = header.column(3).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search identity layout");
        if header.len() != 1 || definitions.is_null(0) || definitions.value(0) != version
            || header.column(0).as_primitive::<Int32Type>().value(0) != locator.format
            || header.column(1).as_primitive::<Int32Type>().value(0) != rtdb_text::ANALYZER_VERSION
            || keys.value(0) != artefact_digest(identity)
            || !names.column(0).as_string::<i32>().iter().eq(columns.iter().map(|column| Some(column.as_str()))) {
            return Err(corrupt("Search tree belongs to another index"));
        }
        let directories = read_root(&root)?;
        if !directories.keys().copied().eq([POSTING, TERM, DOCUMENT, VISIBILITY, PAGE_ROWS].into_iter()
            .chain((locator.format == LOGICAL_FORMAT_VERSION).then_some(ERASES)))
            || directories.values().any(|dir| dir.page.offset >= locator.page.offset) {
            return Err(corrupt("Search tree root has incomplete directories"));
        }
        let routes = if locator.format >= PACKED_FORMAT_VERSION { read_inline_routes(&root, &directories)? } else { BTreeMap::new() };
        Ok(Self { reader, root, directories, routes, format: locator.format })
    }

    pub(super) fn root(&self) -> &RecordBatch { &self.root }
    pub(crate) fn format(&self) -> i32 { self.format }

    pub(super) fn reader(&self) -> &TreeReader { &self.reader }

    pub fn sections(&self) -> Result<Vec<super::Section>> { super::sections_of(&table(&self.root, Table::Sections)) }

    pub async fn verify(&self) -> Result<()> {
        for &kind in self.directories.keys() {
            let pages = self.reader.leaves(self.directories[&kind]).await?;
            if self.routes.get(&kind).is_some_and(|route| !route.iter().map(|child| child.page).eq(pages.iter().copied())) {
                return Err(corrupt("Inline search route does not cover its directory"));
            }
            for page in pages { self.reader.page(page, page_kind(kind)).await?; }
        }
        rank_tree::materialize_blocks(&self.reader, &self.all(TERM).await?).await?;
        Ok(())
    }

    pub async fn materialize(&self, columns: &[String], reservation: Arc<rtdb_columnar::MemoryReservation>) -> Result<super::SearchFile> {
        let terms = self.all(TERM).await?;
        let documents = self.all(DOCUMENT).await?;
        let visibility = self.all(VISIBILITY).await?;
        let pages = self.find(PAGE_ROWS, &ordinal(0)).await?.ok_or_else(|| corrupt("Search tree has no primary page starts"))?;
        let row_maps = visibility.iter().map(|batch| table(batch, Table::RowRuns)).collect::<Vec<_>>();
        let mut rows = Vec::new();
        let mut lengths = Vec::new();
        for column in 0..columns.len() {
            let start = rows.len();
            for map in &row_maps {
                let entries = map.column(0).as_list::<i64>();
                if column >= entries.len() { return Err(corrupt("Search row map has the wrong column count")); }
                let entries = entries.value(column);
                rows.extend_from_slice(entries.as_primitive::<UInt32Type>().values());
            }
            lengths.push(rows.len() - start);
        }
        let row_maps = Table::RowRuns.array(vec![list(Arc::new(arrow_array::UInt32Array::from(rows)), lengths,
            item_of(&Table::RowRuns.fields()[0]))?])?;
        let layout = super::layout::record(self.format, [
            (Table::Header, table(&self.root, Table::Header)),
            (Table::Columns, table(&self.root, Table::Columns)),
            (Table::Members, table(&self.root, Table::Members)),
            (Table::Sections, table(&self.root, Table::Sections)),
            (Table::Pages, table(&pages, Table::Pages)),
            (Table::Terms, concat(&terms, Table::Terms)?),
            (Table::Blocks, rank_tree::materialize_blocks(&self.reader, &terms).await?),
            (Table::Windows, concat(&documents, Table::Windows)?),
        ])?;
        let visibility = super::layout::record(self.format, [
            (Table::Events, concat(&visibility, Table::Events)?),
            (Table::Runs, concat(&documents, Table::Runs)?),
            (Table::RowRuns, row_maps),
        ])?;
        let mut pages = vec![layout, visibility];
        pages.extend(self.all(POSTING).await?);
        super::SearchFile::open_reader(super::layout::Reader::from_checked_batches(pages, self.format), columns)
            .map(|file| file.with_reservation(reservation))
    }

    #[cfg(test)]
    pub(super) async fn page_reference_for_test(&self, kind: u8, key: &[u8]) -> Result<Option<PageRef>> {
        self.reader.find(self.directories[&kind], key).await
    }

    pub(super) async fn find(&self, kind: u8, key: &[u8]) -> Result<Option<RecordBatch>> {
        if let Some(page) = self.reference(kind, key).await? {
            return self.reader.page(page, page_kind(kind)).await.map(Some);
        }
        Ok(None)
    }

    async fn reference(&self, kind: u8, key: &[u8]) -> Result<Option<PageRef>> {
        match self.routes.get(&kind) {
            Some(children) => Ok(direct_reference(children, key)),
            None => self.reader.find(self.directories[&kind], key).await,
        }
    }

    pub(super) async fn term_pages(&self, column: u32, names: &[String]) -> Result<Vec<Option<RecordBatch>>> {
        use futures::{FutureExt, TryStreamExt};
        if self.format >= PACKED_FORMAT_VERSION && !table(&self.root, Table::Terms).is_empty() {
            return Ok(names.iter().map(|_| Some(self.root.clone())).collect());
        }
        let mut in_flight = futures::stream::FuturesOrdered::new();
        let mut requests = Vec::with_capacity(names.len());
        for name in names {
            let key = term_key(column, name);
            in_flight.push_back(async move { self.reference(TERM, &key).await }.boxed());
            if in_flight.len() == 8 {
                requests.push(in_flight.try_next().await?.expect("queued term returns its reference"));
            }
        }
        while let Some(reference) = in_flight.try_next().await? { requests.push(reference); }
        let unique = requests.iter().flatten().copied().collect::<BTreeSet<_>>().into_iter().collect();
        let pages = self.reader.pages(unique, PageKind::Terms).await?;
        Ok(requests.into_iter().map(|request| request.map(|page| pages[&page].clone())).collect())
    }

    pub(super) async fn all(&self, kind: u8) -> Result<Vec<RecordBatch>> {
        let references = self.reader.leaves(self.directories[&kind]).await?;
        let mut pages = self.reader.pages(references.clone(), page_kind(kind)).await?;
        references.into_iter().map(|reference| pages.remove(&reference).ok_or_else(|| corrupt("Search page was omitted"))).collect()
    }

    pub(crate) async fn visit_term_summaries(&self, mut visit: impl FnMut(u32, &str, crate::term_directory::TermStats) -> Result<()> + Send) -> Result<()> {
        for reference in self.reader.leaves(self.directories[&TERM]).await? {
            let page = self.reader.page(reference, PageKind::Terms).await?;
            let terms = table(&page, Table::Terms);
            let nodes = table(&page, Table::RankNodes);
            TermSummaries::visit_page(&terms, &nodes, &mut visit)?;
        }
        Ok(())
    }

    pub(super) async fn find_ordinals(&self, kind: u8, ordinals: std::collections::BTreeSet<usize>) -> Result<BTreeMap<usize, RecordBatch>> {
        let references = match self.routes.get(&kind) {
            Some(children) => ordinals.into_iter().filter_map(|row| direct_reference(children, &ordinal(row)).map(|page| (row, page))).collect(),
            None => self.reader.find_ordinals(self.directories[&kind], ordinals.into_iter().collect()).await?,
        };
        let pages = self.reader.pages(references.iter().map(|(_, reference)| *reference).collect(), page_kind(kind)).await?;
        references.into_iter().map(|(ordinal, reference)| {
            pages.get(&reference).cloned().map(|page| (ordinal, page)).ok_or_else(|| corrupt("Search page was omitted"))
        }).collect()
    }

    pub(crate) async fn events(&self, iid: [u8; 16]) -> Result<Vec<super::KeyedEvent>> {
        let low = event_key(&iid, 0);
        let high = event_key(&iid, u32::MAX);
        let mut found = Vec::new();
        for page in self.reader.between(self.directories[&VISIBILITY], Some((&low, &high))).await? {
            let batch = self.reader.page(page, PageKind::Visibility).await?;
            let events = table(&batch, Table::Events);
            let keys = events.column(0).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search IID layout");
            let bounds: [_; 3] = std::array::from_fn(|column| events.column(column + 1).as_primitive::<Int64Type>());
            let operations = events.column(4).as_primitive::<arrow_array::types::UInt8Type>();
            let start = lower_bound(keys.len(), |row| keys.value(row) < iid.as_slice());
            let end = lower_bound(keys.len(), |row| keys.value(row) <= iid.as_slice());
            for row in start..end {
                found.push(super::KeyedEvent { position: super::read_event_position(events.column(6).as_ref(), row), system_from: bounds[0].value(row), valid_from: bounds[1].value(row),
                    valid_to: bounds[2].value(row), op: super::op_of(operations.value(row))? });
            }
        }
        Ok(found)
    }

    pub(crate) async fn versions(&self, iids: &BTreeSet<Iid>, column: usize, budget: &Arc<MemoryBudget>)
        -> Result<(BTreeMap<Iid, Vec<PackedEvent>>, Arc<MemoryReservation>)> {
        if self.format < PACKED_FORMAT_VERSION { return Err(corrupt("Search pack does not record logical versions")); }
        let mut reservation = budget.reserve(4096usize.saturating_add(iids.len().saturating_mul(192)))?;
        let ranges: Vec<_> = iids.iter().map(|iid| (event_key(&iid.0, 0), event_key(&iid.0, u32::MAX))).collect();
        let pages = match self.routes.get(&VISIBILITY) {
            Some(children) => children.iter().filter(|child| {
                let index = ranges.partition_point(|(_, upper)| upper < &child.lower);
                ranges.get(index).is_some_and(|(lower, _)| lower <= &child.upper)
            }).map(|child| child.page).collect(),
            None => self.reader.covering(self.directories[&VISIBILITY], &ranges).await?,
        };
        let batches = self.reader.pages(pages, PageKind::Visibility).await?;
        let mut found = BTreeMap::<Iid, Vec<PackedEvent>>::new();
        let mut count = 0usize;
        for batch in batches.values() {
            let events = table(batch, Table::Events);
            let versions = table(batch, Table::Versions);
            let keys = events.column(0).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search IID layout");
            let bounds: [_; 3] = std::array::from_fn(|index| events.column(index + 1).as_primitive::<Int64Type>());
            let operations = events.column(4).as_primitive::<arrow_array::types::UInt8Type>();
            let locations: [_; 4] = std::array::from_fn(|index| versions.column(index).as_primitive::<UInt32Type>());
            let row_runs = table(batch, Table::RowRuns);
            let columns = row_runs.column(0).as_list::<i64>();
            if events.is_empty() || versions.len() != events.len() || column >= columns.len() || versions.columns().iter().any(|values| values.null_count() != 0) {
                return Err(corrupt("Search visibility page has incomplete version references"));
            }
            let docs = columns.value(column);
            let docs = docs.as_primitive::<UInt32Type>();
            if docs.len() != events.len() || docs.null_count() != 0 {
                return Err(corrupt("Search version references have an incomplete row map"));
            }
            let proofs = if self.format == LOGICAL_FORMAT_VERSION {
                let proofs = table(batch, Table::TextRevisions);
                let fields = proofs.column(0).as_list::<i64>();
                if column >= fields.len() { return Err(corrupt("History pack omits its text revisions")); }
                let digests = fields.value(column);
                if digests.len() != events.len() || digests.null_count() != 0 { return Err(corrupt("History text revisions have the wrong size")); }
                Some(digests)
            } else { None };
            let first = Iid::from_slice(keys.value(0))?;
            let last = Iid::from_slice(keys.value(keys.len() - 1))?;
            if first > last { return Err(corrupt("Search versions have an invalid IID range")); }
            for &iid in iids.range(first..=last) {
                let start = lower_bound(keys.len(), |row| keys.value(row) < iid.0.as_slice());
                let end = lower_bound(keys.len(), |row| keys.value(row) <= iid.0.as_slice());
                count = count.saturating_add(end - start);
                Arc::get_mut(&mut reservation).expect("version reads own their scratch")
                    .grow_to(4096usize.saturating_add(iids.len().saturating_mul(192)).saturating_add(count.saturating_mul(192)))?;
                for row in start..end {
                    let document = docs.value(row);
                    found.entry(iid).or_default().push(PackedEvent {
                        header: EventHeader { position: super::read_event_position(events.column(6).as_ref(), row), iid, system_from: Timestamp(bounds[0].value(row)), commit_position: locations[0].value(row),
                            valid_time: Interval::from_micros(bounds[1].value(row), bounds[2].value(row))?, op: super::op_of(operations.value(row))? },
                        member: locations[1].value(row), page: locations[2].value(row), row: locations[3].value(row),
                        document: (document < super::NULL_DOC).then_some(document),
                        revision: proofs.as_ref().and_then(|proofs| {
                            let array = proofs.as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("history revision layout");
                            let value = array.value(row);
                            (value[0] == 1).then(|| value[1..].try_into().expect("history revisions are 32 bytes"))
                        }),
                    });
                }
            }
        }
        Ok((found, reservation))
    }

    pub(crate) async fn documents(&self) -> Result<Vec<LogicalDocument>> {
        use rtdb_columnar::EntityIdRef;
        if self.format != LOGICAL_FORMAT_VERSION { return Err(corrupt("Expected a logical search run")); }
        let mut documents = Vec::new();
        let records = self.all(DOCUMENT).await?;
        let payload = self.all(VISIBILITY).await?;
        if records.len() != payload.len() { return Err(corrupt("Search run omits analyzed token pages")); }
        for (batch, payload) in records.into_iter().zip(payload) {
            let runs = table(&batch, Table::Runs);
            let sequences = table(&payload, Table::TokenSequences);
            if sequences.len() != runs.len() { return Err(corrupt("Search run is missing token sequences")); }
            let iids = runs.column(0).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search IID layout");
            let ids = runs.column(2).as_binary::<i32>();
            let fields = sequences.column(0).as_primitive::<UInt32Type>();
            let revisions = sequences.column(1).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search revision layout");
            let tokens = sequences.column(2).as_list::<i64>();
            for row in 0..sequences.len() {
                let sql_id = if ids.is_valid(row) {
                    match EntityIdRef::from_handle_bytes(ids.value(row))? { EntityIdRef::Integer(id) => Some(id), _ => None }
                } else { None };
                documents.push(LogicalDocument { iid: Iid::from_slice(iids.value(row))?, sql_id,
                    field: fields.value(row) as usize, revision: revisions.value(row).try_into().map_err(fault)?,
                    tokens: tokens.value(row).as_string::<i32>().clone(),
                    position: (Timestamp(sequences.column(3).as_primitive::<Int64Type>().value(row)),
                        super::read_event_position(sequences.column(5).as_ref(), row),
                        sequences.column(4).as_primitive::<UInt32Type>().value(row)) });
            }
        }
        Ok(documents)
    }

    pub(crate) async fn erases(&self, memory: &Arc<rtdb_types::MemoryBudget>) -> Result<(BTreeMap<Iid, super::SearchPosition>, Arc<rtdb_types::MemoryReservation>)> {
        let mut erases = BTreeMap::new();
        let mut charge = memory.reserve(4096)?;
        for batch in self.all(ERASES).await? {
            let rows = table(&batch, Table::Erases);
            let next = 4096usize.saturating_add(erases.len().saturating_add(rows.len()).saturating_mul(128));
            let charge = Arc::get_mut(&mut charge).expect("unpublished erase witnesses own their charge");
            charge.grow_to(next)?;
            let iids = rows.column(0).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("erase IID layout");
            let times = rows.column(1).as_primitive::<Int64Type>();
            let positions = rows.column(2).as_primitive::<UInt32Type>();
            for row in 0..rows.len() {
                if erases.insert(Iid::from_slice(iids.value(row))?, (Timestamp(times.value(row)), super::read_event_position(rows.column(3).as_ref(), row), positions.value(row))).is_some() {
                    return Err(corrupt("Search file repeats an erase witness"));
                }
            }
        }
        Ok((erases, charge))
    }
}

fn direct_reference(children: &[Child], key: &[u8]) -> Option<PageRef> {
    let index = children.partition_point(|child| child.upper.as_slice() < key);
    children.get(index).filter(|child| child.lower.as_slice() <= key).map(|child| child.page)
}

fn lower_bound(len: usize, before: impl Fn(usize) -> bool) -> usize {
    let mut from = 0;
    let mut to = len;
    while from < to {
        let middle = from + (to - from) / 2;
        if before(middle) { from = middle + 1; } else { to = middle; }
    }
    from
}

fn concat(batches: &[RecordBatch], shape: Table) -> Result<StructArray> {
    if batches.is_empty() {
        let empty = arrow_array::new_empty_array(&arrow_schema::DataType::Struct(shape.fields()));
        return Ok(empty.as_struct().clone());
    }
    let parts: Vec<_> = batches.iter().map(|batch| table(batch, shape)).collect();
    let arrays: Vec<&dyn Array> = parts.iter().map(|part| part as &dyn Array).collect();
    Ok(arrow_select::concat::concat(&arrays).map_err(fault)?.as_struct().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_text::{Query, Scorer, tokens};
    use rtdb_types::{EntityId, EventOp, Interval, TemporalBounds, Timestamp};

    #[tokio::test]
    async fn root_routes_by_term_and_iid_and_preserves_canonical_scores() {
        let columns = ["body".to_owned()];
        let mut rows: Vec<_> = (0..8192).map(|id| {
            let identity = EntityId::Integer(id);
            super::super::SearchRow { position: None, iid: identity.iid().0, identity: Some(identity), system_from: 1, valid_from: 0,
                valid_to: i64::MAX, op: EventOp::Put, values: vec![Some(tokens(if id % 2 == 0 { "report audit audit" }
                    else { "report audit about the library" }))] }
        }).collect();
        rows.sort_unstable_by_key(|row| row.iid);
        let mut file = tempfile::tempfile().unwrap();
        super::super::write_versioned_group(&columns, [[Ok(rows.clone())]], "tree", 42, None, &mut file, None, TREE_FORMAT_VERSION).unwrap();
        let locator = TreeLocator::read(&mut file, TREE_FORMAT_VERSION).unwrap();
        assert_eq!(TreeLocator::decode(&locator.encode()), Some(locator.clone()));
        assert!(!locator.has_canonical_uuid_iids());
        let certified = locator.clone().with_uuid_iid_evidence(true);
        assert_eq!(TreeLocator::decode(&certified.encode()), Some(certified.clone()));
        let wrong_proof = format!("{}:{}", locator.encode(), encode_hex(&uuid_evidence_digest(&[0; 32])));
        assert_eq!(TreeLocator::decode(&wrong_proof), Some(locator.clone()));
        assert!(locator.might_contain(0, "report"));
        assert!(!locator.might_contain(0, "unpublished-term"));
        file.rewind().unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        let tree = Arc::new(TreeFile::local(Buffer::from_vec(bytes), None, &locator, &columns, "tree", 42).await.unwrap());
        let found = tree.events(rows[900].iid).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].system_from, 1);
        assert!(tree.events([0xff; 16]).await.unwrap().is_empty());

        let matching = Query::parse("report");
        let scoring = Query::parse("audit");
        let bounds = TemporalBounds { valid_time: Interval::at(Timestamp(2)).unwrap(), system_time: Interval::at(Timestamp(2)).unwrap() };
        let scorer = Arc::new(Scorer::new(scoring.clone(), None)).for_ranking().unwrap();
        let mut rank = super::super::TreeRanked::open(Arc::clone(&tree), 0, &matching, &scorer, bounds, super::super::TreeRankOrder::Integer).await.unwrap();
        assert_eq!(rank.bound().unwrap().integer_id(), Some(0));
        let mut actual = BTreeMap::new();
        while rank.bound().is_some() {
            for hit in rank.expand(None, &mut 0).await.unwrap() { actual.insert(hit.iid.0, hit.key.score.to_bits()); }
        }
        let dense = tree.materialize(&columns, rtdb_columnar::MemoryBudget::new(32 << 20).reserve(16 << 20).unwrap()).await.unwrap();
        let expected = dense.scored("body", &matching, &scoring, None, bounds).unwrap().into_iter()
            .map(|(doc, score)| (doc.iid, score.to_bits())).collect::<BTreeMap<_, _>>();
        assert_eq!(actual.len(), rows.len());
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn a_pack_authenticates_inline_routes_and_finds_versions_in_both_members() {
        let columns = ["body".to_owned()];
        let row = |iid, system_from, op, value: Option<&str>| super::super::SearchRow {
            position: None,
            identity: None, iid: [iid; 16], system_from, valid_from: 0, valid_to: i64::MAX,
            op, values: vec![value.map(tokens)],
        };
        let mut file = tempfile::tempfile().unwrap();
        let newer = super::super::SearchRow {
            position: Some(rtdb_types::EventPosition { transaction: rtdb_types::TxId(10), ordinal: 0 }),
            ..row(1, 2, EventOp::Delete, None)
        };
        let older = super::super::SearchRow {
            position: Some(rtdb_types::EventPosition { transaction: rtdb_types::TxId(9), ordinal: 0 }),
            ..row(1, 2, EventOp::Put, Some("audit report"))
        };
        let expected = [newer.position, older.position];
        let members = [
            vec![Ok(vec![newer]), Ok(vec![older])],
            vec![Ok(vec![row(9, 2, EventOp::Put, Some("separate report"))])],
        ];
        super::super::write_versioned_group(&columns, members, "pack", 1, None, &mut file, None, PACKED_FORMAT_VERSION).unwrap();
        let locator = TreeLocator::read(&mut file, PACKED_FORMAT_VERSION).unwrap();
        assert!(locator.inline_root.is_some());
        let encoded = locator.encode();
        assert_eq!(TreeLocator::decode(&encoded), Some(locator.clone()));
        let (prefix, bytes) = encoded.rsplit_once(':').unwrap();
        let byte = if bytes.ends_with('0') { '1' } else { '0' };
        let corrupted = format!("{prefix}:{}{byte}", &bytes[..bytes.len() - 1]);
        assert_eq!(TreeLocator::decode(&corrupted), Some(locator.without_inline_root()));

        file.rewind().unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        let tree = TreeFile::local(Buffer::from_vec(bytes), None, &locator, &columns, "pack", 1).await.unwrap();
        tree.verify().await.unwrap();
        let first = Iid([1; 16]);
        let second = Iid([9; 16]);
        let queried = BTreeSet::from([first, second, Iid([5; 16])]);
        let budget = MemoryBudget::new(1 << 20);
        let (versions, _charge) = tree.versions(&queried, 0, &budget).await.unwrap();
        assert_eq!(versions.len(), 2);
        let first = &versions[&first];
        assert_eq!(first.iter().map(|event| (event.header.system_from, event.member, event.page, event.row)).collect::<Vec<_>>(),
            [(Timestamp(2), 0, 0, 0), (Timestamp(2), 0, 1, 0)]);
        assert_eq!(first.iter().map(|event| event.header.position).collect::<Vec<_>>(), expected);
        assert_eq!(first.iter().map(|event| event.document.is_some()).collect::<Vec<_>>(), [false, false]);
        let second = &versions[&second][0];
        assert_eq!((second.member, second.page, second.row), (1, 0, 0));
    }

    #[test]
    fn format_eleven_can_be_read_with_the_arrow_search_api() {
        let columns = ["body".to_owned()];
        let row = super::super::SearchRow { position: None, identity: None, iid: [7; 16], system_from: 1, valid_from: 0,
            valid_to: i64::MAX, op: EventOp::Put, values: vec![Some(tokens("one two one"))] };
        let mut bytes = Vec::new();
        super::super::write_versioned_group(&columns, [[Ok(vec![row])]], "legacy", 42, None, &mut bytes, None,
            super::super::TANTIVY_FORMAT_VERSION).unwrap();
        let file = super::super::SearchFile::open(bytes, &columns).unwrap();
        assert!(file.matches_artefact("legacy", 42));
        let bounds = TemporalBounds { valid_time: Interval::at(Timestamp(2)).unwrap(), system_time: Interval::at(Timestamp(2)).unwrap() };
        assert_eq!(file.matches("body", &Query::parse("\"two one\""), bounds).unwrap()[0].iid, [7; 16]);
        assert!(file.matches("body", &Query::parse("\"one one\""), bounds).unwrap().is_empty());
    }
}
