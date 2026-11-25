use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
#[cfg(test)]
use std::{fs::File, io::{Read, Seek, SeekFrom}};
use std::sync::Arc;

use arrow_array::{Array, FixedSizeBinaryArray, RecordBatch, StructArray, UInt32Array, cast::AsArray, types::{Int32Type, Int64Type, UInt32Type}};
use arrow_buffer::{Buffer, MutableBuffer};
use bytes::Bytes;
use rtdb_storage::{ObjectKey, audience_work, cache::BufferPool};
use rtdb_types::{EventHeader, Iid, Interval, MemoryBudget, MemoryReservation, Result, Timestamp};
use sha2::{Digest, Sha256};
use crate::rank_windows::{document_range, window_of};
use crate::term_directory::TermSummaries;

use super::{LOGICAL_FORMAT_VERSION, PACKED_FORMAT_VERSION, TREE_FORMAT_VERSION, TextRevision, artefact_digest, corrupt, fault, layout::{AuthenticatedPage, PageRef, Table, Writer, item_of, list, table},
    page_tree::{self, Child, DecodedChild, DirectoryRef, PageCache, PageKind, PageOwner, PageSource, TreeReader, inline_routes, read_inline_routes, read_root, root_table, write_directory}, paged::PostingPlan, rank_tree};

#[cfg(test)]
use super::layout::FooterLocator;

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

fn row_runs_for_page(array: &StructArray, start: usize, len: usize) -> Result<StructArray> {
    let column = array.column(0).as_list::<i64>();
    let offsets = column.value_offsets();
    let field_starts = &offsets[..column.len()];
    let Some(&last_field) = field_starts.last() else { return Ok(array.clone()); };
    let end = start.checked_add(len).ok_or_else(|| fault("Search row range exceeds its source"))?;
    if offsets.windows(2).any(|field| end > (field[1] - field[0]) as usize) {
        return Err(fault("Search row range exceeds its source"));
    }

    let values = column.values().as_primitive::<UInt32Type>();
    let count = len.checked_mul(column.len()).filter(|&count| count <= values.len())
        .ok_or_else(|| fault("Search row allocation exceeds its source"))?;
    let span = offsets[0] as usize + start..last_field as usize + end;
    let rows = if span.len() == count {
        if start == 0 && span.end == offsets[column.len()] as usize {
            return Ok(array.clone());
        }
        values.slice(span.start, count)
    } else {
        let mut rows = Vec::with_capacity(count);
        for &offset in field_starts {
            let offset = offset as usize;
            rows.extend_from_slice(&values.values()[offset + start..offset + end]);
        }
        UInt32Array::from(rows)
    };
    let item = item_of(&Table::RowRuns.fields()[0]);
    Table::RowRuns.array(vec![list(Arc::new(rows), std::iter::repeat_n(len, column.len()), item)?])
}

fn text_revision_rows(columns: &[FixedSizeBinaryArray], start: usize, len: usize) -> Result<StructArray> {
    let mut values = columns[0].slice(start, len);
    if columns.len() > 1 {
        let mut bytes = MutableBuffer::new(values.value_data().len() * columns.len());
        bytes.extend_from_slice(values.value_data());
        for column in &columns[1..] {
            bytes.extend_from_slice(column.slice(start, len).value_data());
        }
        values = FixedSizeBinaryArray::try_new(values.value_length(), bytes.into(), None).map_err(fault)?;
    }
    let item = item_of(&Table::TextRevisions.fields()[0]);
    Table::TextRevisions.array(vec![list(Arc::new(values), std::iter::repeat_n(len, columns.len()), item)?])
}

pub(super) fn write(writer: &mut Writer<impl Write>, mut fields: Vec<(Table, StructArray)>,
    [(_, events), (_, runs), (_, row_runs)]: [(Table, StructArray); 3], plan: PostingPlan,
    positions: Option<Vec<u32>>, revisions: Option<Vec<FixedSizeBinaryArray>>, token_sequences: Option<StructArray>,
    erases: Option<StructArray>) -> Result<(TermSummaries, WrittenRoot)> {
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
    let positions = positions.map(arrow_array::UInt32Array::from);
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
            (Table::RowRuns, row_runs_for_page(&row_runs, start, end - start)?)];
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
                Arc::new(positions.slice(start, end - start)),
                Arc::new(arrow_array::UInt32Array::from(members)), Arc::new(arrow_array::UInt32Array::from(pages)),
                Arc::new(arrow_array::UInt32Array::from(local_rows)),
            ])?));
        }
        if let Some(revisions) = &revisions {
            fields.push((Table::TextRevisions, text_revision_rows(revisions, start, end - start)?));
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
    let (page, raw) = writer.root_page(fields)?;
    let range = page.range()?;
    if range.end - range.start > MAX_ROOT_BYTES { return Err(fault("Search root exceeds its read budget")); }
    let inline = raw.map(|root| lz4_flex::block::compress(&root)).filter(|root| root.len() <= INLINE_ROOT_BYTES);
    Ok((summaries, WrittenRoot { page, synopsis, inline }))
}

pub(super) struct WrittenRoot {
    page: PageRef,
    synopsis: [u8; SYNOPSIS_BYTES],
    inline: Option<Vec<u8>>,
}

impl WrittenRoot {
    pub(super) fn finish(self, length: u64, format: i32) -> WrittenTree {
        WrittenTree { root: self, length, format }
    }
}

pub(crate) struct WrittenTree {
    root: WrittenRoot,
    length: u64,
    format: i32,
}

impl WrittenTree {
    pub(crate) fn encode(self, canonical_uuid_iids: bool) -> EncodedTreeLocator {
        let WrittenRoot { page, synopsis, inline } = self.root;
        let locator = TreeLocator { length: self.length, page, synopsis: Some(Arc::new(synopsis)),
            format: self.format, inline_root: None, canonical_uuid_iids };
        EncodedTreeLocator { length: self.length, text: locator.encode_with_inline_root(|| inline) }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TreeLocator {
    pub length: u64,
    page: PageRef,
    synopsis: Option<Arc<[u8; SYNOPSIS_BYTES]>>,
    canonical_uuid_iids: bool,
    format: i32,
    inline_root: Option<Bytes>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TreeLocation {
    length: u64,
    page: PageRef,
    format: i32,
}

pub(crate) struct EncodedTreeLocator {
    pub length: u64,
    pub text: String,
}

#[cfg(test)]
impl EncodedTreeLocator {
    pub fn read(input: &mut File, format: i32, canonical_uuid_iids: bool) -> Result<Self> {
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
        let bytes = Bytes::from(bytes);
        let inline_root = (format >= PACKED_FORMAT_VERSION).then(|| lz4_flex::block::compress(&bytes))
            .filter(|root| root.len() <= INLINE_ROOT_BYTES);
        let root = page.decode(Buffer::from(bytes), &decoder)?;
        let synopsis = table(&root, Table::Synopsis);
        let bits = synopsis.column(0).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search synopsis layout");
        if bits.len() != 1 || bits.is_null(0) { return Err(corrupt("Search root has no complete term synopsis")); }
        let synopsis = bits.value(0).try_into().map_err(fault)?;
        Ok(WrittenRoot { page, synopsis, inline: inline_root }.finish(locator.length as u64, format).encode(canonical_uuid_iids))
    }
}

impl TreeLocator {
    pub fn encode(&self) -> String {
        self.encode_with_inline_root(|| self.inline_root.as_ref().map(|root| lz4_flex::block::compress(root)))
    }

    fn encode_with_inline_root(&self, compressed_root: impl FnOnce() -> Option<Vec<u8>>) -> String {
        let digest = encode_hex(&self.page.digest);
        let prefix = match self.format { LOGICAL_FORMAT_VERSION => "v16:", PACKED_FORMAT_VERSION => "v15:", _ => "" };
        let header = format!("{prefix}{}:{}:{}:{}:{digest}", self.length, self.page.offset, self.page.metadata, self.page.body);
        let Some(bits) = self.synopsis.as_deref() else { return header; };
        let check = synopsis_digest(&self.page.digest, bits);
        let mut encoded = format!("{header}:{}:{}", encode_hex(bits), encode_hex(&check));
        if let Some(root) = compressed_root() {
            encoded.push(':');
            encoded.push_str(&encode_hex(&root));
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
        let (mut locator, inline) = Self::decode_route_with_inline(value)?;
        let expected = locator.page.metadata as usize + locator.page.body as usize;
        locator.inline_root = inline.and_then(|hex| expand_inline_root(hex, expected))
            .filter(|bytes| <[u8; 32]>::from(Sha256::digest(bytes)) == locator.page.digest).map(Bytes::from);
        Some(locator)
    }

    pub fn decode_route(value: &str) -> Option<Self> {
        Self::decode_route_with_inline(value).map(|(locator, _)| locator)
    }

    pub fn decode_route_with_inline(value: &str) -> Option<(Self, Option<&str>)> {
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
        let root = if format >= PACKED_FORMAT_VERSION { fields.next().filter(|hex| !hex.is_empty()) } else { None };
        let canonical_uuid_iids = synopsis.is_some() && fields.next().and_then(decode_hex::<32>)
            .is_some_and(|check| check == uuid_evidence_digest(&digest));
        Some((Self { length, page, synopsis, format, inline_root: None, canonical_uuid_iids }, root))
    }

    fn admit_inline_root(&self, hex: &str, memory: &Arc<MemoryBudget>) -> Result<Option<AuthenticatedPage>> {
        if hex.len() > INLINE_ROOT_BYTES * 2 || hex.len() % 2 != 0 { return Ok(None); }
        let expected = self.page.metadata as usize + self.page.body as usize;
        let mut reservation = match memory.reserve(expected + hex.len() / 2) {
            Ok(reservation) => reservation,
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => return Ok(None),
            Err(error) => return Err(error),
        };
        audience_work::decode_bytes(expected)?;
        let Some(bytes) = expand_inline_root(hex, expected) else { return Ok(None); };
        Arc::get_mut(&mut reservation).expect("Inline root expansion owns its reservation").shrink_to(bytes.capacity())?;
        let bytes = rtdb_columnar::retain_buffer(&Buffer::from_vec(bytes), &reservation);
        Ok(self.page.authenticate(bytes).ok())
    }

    pub fn has_canonical_uuid_iids(&self) -> bool { self.canonical_uuid_iids }

    pub fn location(&self) -> TreeLocation { TreeLocation { length: self.length, page: self.page, format: self.format } }
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

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(DIGITS[(byte >> 4) as usize] as char);
        text.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    text
}

fn expand_inline_root(hex: &str, expected: usize) -> Option<Vec<u8>> {
    if hex.len() > INLINE_ROOT_BYTES * 2 || hex.len() % 2 != 0 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) { return None; }
    let mut compressed = Vec::with_capacity(hex.len() / 2);
    for position in (0..hex.len()).step_by(2) {
        compressed.push(u8::from_str_radix(&hex[position..position + 2], 16).ok()?);
    }
    let mut bytes = vec![0; expected];
    let written = lz4_flex::block::decompress_into(&compressed, &mut bytes).ok()?;
    (written == expected).then_some(bytes)
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
    routes: BTreeMap<u8, Vec<DecodedChild>>,
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
        columns: &[String], identity: &str, version: i64, inline: Option<(&str, &Arc<MemoryBudget>)>, memory: Arc<MemoryBudget>) -> Result<Self> {
        Self::open(PageSource::Remote(PageOwner::new(&storage, object, cache)), locator, columns, identity, version, inline, memory).await
    }

    pub async fn local(bytes: Buffer, cache: Option<(Arc<BufferPool>, ObjectKey, Arc<PageCache>)>, locator: &TreeLocator,
        columns: &[String], identity: &str, version: i64, inline: Option<(&str, &Arc<MemoryBudget>)>, memory: Arc<MemoryBudget>) -> Result<Self> {
        if bytes.len() as u64 != locator.length { return Err(corrupt("Search root size disagrees with the object")); }
        let owner = cache.map(|(storage, object, cache)| PageOwner::new(&storage, object, cache));
        Self::open(PageSource::Local { bytes, owner }, locator, columns, identity, version, inline, memory).await
    }

    async fn open(source: PageSource, locator: &TreeLocator, columns: &[String], identity: &str, version: i64,
        inline: Option<(&str, &Arc<MemoryBudget>)>, memory: Arc<MemoryBudget>) -> Result<Self> {
        let reader = TreeReader::new(source, locator.length, locator.format, memory);
        let root = if let Some((hex, memory)) = inline && let Some(page) = locator.admit_inline_root(hex, memory)? {
            let wire = locator.page.metadata as usize + locator.page.body as usize;
            match reader.authenticated_page(page, wire) {
                Ok(root) => root,
                Err(error) if error.code == "rtdb/memory-budget-exceeded" => reader.page(locator.page, PageKind::Root).await?,
                Err(error) => return Err(error),
            }
        } else if let Some(bytes) = &locator.inline_root {
            reader.inline_page(locator.page, bytes.clone())?
        } else {
            reader.page(locator.page, PageKind::Root).await?
        };
        Self::check_root_identity(&root, locator.format, columns, identity, version)?;
        let directories = read_root(&root)?;
        if !directories.keys().copied().eq([POSTING, TERM, DOCUMENT, VISIBILITY, PAGE_ROWS].into_iter()
            .chain((locator.format == LOGICAL_FORMAT_VERSION).then_some(ERASES)))
            || directories.values().any(|dir| dir.page.offset >= locator.page.offset) {
            return Err(corrupt("Search tree root has incomplete directories"));
        }
        let routes = if locator.format >= PACKED_FORMAT_VERSION { read_inline_routes(&root, &directories)? } else { BTreeMap::new() };
        Ok(Self { reader, root, directories, routes, format: locator.format })
    }

    fn check_root_identity(root: &RecordBatch, format: i32, columns: &[String], identity: &str, version: i64) -> Result<()> {
        let header = table(root, Table::Header);
        let definitions = header.column(2).as_primitive::<Int64Type>();
        let names = table(root, Table::Columns);
        let keys = header.column(3).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search identity layout");
        if header.len() != 1 || definitions.is_null(0) || definitions.value(0) != version
            || header.column(0).as_primitive::<Int32Type>().value(0) != format
            || header.column(1).as_primitive::<Int32Type>().value(0) != rtdb_text::ANALYZER_VERSION
            || keys.value(0) != artefact_digest(identity)
            || !names.column(0).as_string::<i32>().iter().eq(columns.iter().map(|column| Some(column.as_str()))) {
            return Err(corrupt("Search tree belongs to another index"));
        }
        Ok(())
    }

    pub fn check_identity(&self, columns: &[String], identity: &str, version: i64) -> Result<()> {
        Self::check_root_identity(&self.root, self.format, columns, identity, version)
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

    pub(crate) async fn exact(&self, columns: &[String], column: usize, queries: &[&rtdb_text::Query], all_runs: bool,
        memory: &Arc<MemoryBudget>) -> Result<super::SearchFile> {
        use super::manifest::{Manifest, validate_term_blocks, window_count};

        if !matches!(self.format, TREE_FORMAT_VERSION | PACKED_FORMAT_VERSION) {
            return Err(corrupt("Exact trie search requires a combined search tree"));
        }
        let counts = table(&self.root, Table::Pages);
        if counts.len() != 2 || counts.column(0).null_count() != 0 {
            return Err(corrupt("Search tree has no row and document counts"));
        }
        let documents = counts.column(0).as_primitive::<UInt32Type>().value(1);
        if documents >= super::NULL_DOC { return Err(corrupt("Search tree has too many documents")); }

        let query_bytes = queries.iter().flat_map(|query| query.terms()).fold(4096usize, |bytes, term| {
            bytes.saturating_add(512).saturating_add(term.len().saturating_mul(3))
        });
        let mut plan = memory.reserve(query_bytes)?;
        let mut retained = memory.reserve(columns.iter().fold(4096usize, |bytes, name| bytes.saturating_add(128 + name.len()))
            .saturating_add(table(&self.root, Table::Members).len().saturating_mul(32))
            .saturating_add(table(&self.root, Table::Sections).len().saturating_mul(128)))?;
        let terms = queries.iter().flat_map(|query| query.terms()).cloned().collect::<BTreeSet<_>>()
            .into_iter().collect::<Vec<_>>();
        let term_pages = self.term_pages(column as u32, &terms).await?;
        let mut selected = Vec::new();
        let mut posting_ordinals = BTreeSet::new();
        let mut document_ordinals = BTreeSet::new();
        let mut slots = BTreeSet::new();
        for (name, page) in terms.into_iter().zip(term_pages) {
            let Some(page) = page else { continue; };
            let Some(term) = self.exact_term(&page, column as u32, name, documents, &mut retained).await? else { continue; };
            grow_exact(&mut plan, term.blocks.len().saturating_mul(128))?;
            validate_term_blocks(&term.blocks, term.frequency, window_count(documents), &mut slots)?;
            let positions = queries.iter().any(|query| super::needs_positions(query, &term.name));
            for block in &term.blocks {
                plan_ordinal(&mut posting_ordinals, block.page, &mut plan)?;
                if positions { plan_ordinal(&mut posting_ordinals, block.page + 1, &mut plan)?; }
                if !all_runs {
                    for impact in &block.impacts {
                        let page = (document_range(impact.window).0 as usize / DOCUMENT_ROWS) * DOCUMENT_ROWS;
                        plan_ordinal(&mut document_ordinals, page, &mut plan)?;
                    }
                }
            }
            selected.push(term);
        }
        if all_runs {
            for page in (0..documents as usize).step_by(DOCUMENT_ROWS) {
                plan_ordinal(&mut document_ordinals, page, &mut plan)?;
            }
        }
        grow_exact(&mut retained, posting_ordinals.len().saturating_add(document_ordinals.len()).saturating_mul(256))?;
        let postings = self.find_ordinals(POSTING, posting_ordinals.clone()).await?;
        let runs = self.find_ordinals(DOCUMENT, document_ordinals.clone()).await?;
        if !postings.keys().copied().eq(posting_ordinals) || !runs.keys().copied().eq(document_ordinals) {
            return Err(corrupt("Search tree omits a requested posting or document page"));
        }

        // Other tries use these events to cut visibility, even when this tree has no queried term.
        let mut references = Vec::new();
        let walk = memory.reserve(page_tree::DIRECTORY_WALK_BYTES)?;
        self.reader.visit_leaves(self.directories[&VISIBILITY], None, |page| {
            grow_exact(&mut plan, 512)?;
            references.push(page);
            Ok(())
        }).await?;
        drop(walk);
        grow_exact(&mut retained, references.len().saturating_mul(256))?;
        let mut pages = self.reader.pages(references.clone(), PageKind::Visibility).await?;
        let events = references.into_iter()
            .map(|reference| pages.remove(&reference).ok_or_else(|| corrupt("Search page was omitted")))
            .collect::<Result<Vec<_>>>()?;
        let primary = self.find(PAGE_ROWS, &ordinal(0)).await?.ok_or_else(|| corrupt("Search tree has no primary page starts"))?;
        let manifest = Manifest::read_tree(&self.root, &primary, documents, column, selected, events, runs)?;
        let reader = super::layout::Reader::from_sparse_checked_batches(postings, self.format);
        Ok(super::SearchFile::with_manifest(reader, columns, manifest).with_reservation(retained))
    }

    async fn exact_term(&self, page: &RecordBatch, column: u32, name: String, documents: u32,
        retained: &mut Arc<MemoryReservation>,
    ) -> Result<Option<super::manifest::ScopedTerm>> {
        use futures::TryStreamExt;

        let terms = table(page, Table::Terms);
        let columns = terms.column(0).as_primitive::<UInt32Type>();
        let names = terms.column(1).as_string::<i32>();
        let frequencies = terms.column(4).as_primitive::<UInt32Type>();
        let mut entries = (0..terms.len()).filter(|&row| columns.value(row) == column && names.value(row) == name.as_str());
        let Some(row) = entries.next() else { return Ok(None); };
        let frequency = frequencies.value(row);
        if entries.next().is_some() || !(1..=documents).contains(&frequency) {
            return Err(corrupt("Search tree term directory is invalid"));
        }
        grow_exact(retained, 256usize.saturating_add(name.capacity()))?;
        let node = rank_tree::node_at(&table(page, Table::RankNodes), row)?;
        let mut blocks = Vec::new();
        if node.page.is_some() {
            let _walk = retained.budget().reserve(rank_tree::TRAVERSAL_BYTES)?;
            let leaves = rank_tree::leaf_stream(&self.reader, node, node.first, node.last);
            futures::pin_mut!(leaves);
            while let Some(leaf) = leaves.try_next().await? {
                let (table, _) = rank_tree::leaf_arrays(&self.reader, leaf).await?;
                let entries = table.column(0).as_list::<i64>();
                if entries.len() != 1 || entries.is_null(0) { return Err(corrupt("Search term has an invalid rank leaf")); }
                append_exact_blocks(&mut blocks, entries, 0, retained)?;
            }
        } else {
            let table = table(page, Table::Blocks);
            let entries = table.column(0).as_list::<i64>();
            if row >= entries.len() || entries.is_null(row) { return Err(corrupt("Search term has no inline blocks")); }
            append_exact_blocks(&mut blocks, entries, row, retained)?;
        }
        Ok(Some(super::manifest::ScopedTerm { name, frequency, blocks }))
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
        if self.format >= PACKED_FORMAT_VERSION && !table(&self.root, Table::Terms).is_empty() {
            return Ok(names.iter().map(|_| Some(self.root.clone())).collect());
        }
        let requests = if let Some(children) = self.routes.get(&TERM) {
            names.iter().map(|name| direct_reference(children, &term_key(column, name))).collect::<Vec<_>>()
        } else {
            let mut positions: Vec<_> = names.iter().enumerate().map(|(i, name)| (term_key(column, name), i)).collect();
            positions.sort_unstable();
            let keys: Vec<_> = positions.iter().map(|(key, _)| key.as_slice()).collect();
            let pages = self.reader.find_sorted_keys(self.directories[&TERM], &keys).await?;
            let mut requests = vec![None; names.len()];
            for ((_, position), page) in positions.into_iter().zip(pages) { requests[position] = page; }
            requests
        };
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
                let index = ranges.partition_point(|(_, upper)| upper.as_slice() < child.lower.as_slice());
                ranges.get(index).is_some_and(|(lower, _)| lower.as_slice() <= child.upper.as_slice())
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
        let mut documents = Vec::new();
        self.visit_document_pages(|page| {
            for document in page { documents.push(document?); }
            Ok(())
        }).await?;
        Ok(documents)
    }

    pub(crate) async fn visit_document_pages(
        &self, mut visit: impl FnMut(&mut dyn ExactSizeIterator<Item = Result<LogicalDocument>>) -> Result<()> + Send,
    ) -> Result<()> {
        use rtdb_columnar::EntityIdRef;
        if self.format != LOGICAL_FORMAT_VERSION { return Err(corrupt("Expected a logical search run")); }
        let records = self.reader.leaves(self.directories[&DOCUMENT]).await?;
        let payload = self.reader.leaves(self.directories[&VISIBILITY]).await?;
        if records.len() != payload.len() { return Err(corrupt("Search run omits analyzed token pages")); }
        for (record, payload) in records.into_iter().zip(payload) {
            let batch = self.reader.page(record, page_kind(DOCUMENT)).await?;
            let payload = self.reader.page(payload, page_kind(VISIBILITY)).await?;
            let runs = table(&batch, Table::Runs);
            let sequences = table(&payload, Table::TokenSequences);
            if sequences.len() != runs.len() { return Err(corrupt("Search run is missing token sequences")); }
            let iids = runs.column(0).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search IID layout");
            let ids = runs.column(2).as_binary::<i32>();
            let fields = sequences.column(0).as_primitive::<UInt32Type>();
            let revisions = sequences.column(1).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search revision layout");
            let tokens = sequences.column(2).as_list::<i64>();
            let mut documents = (0..sequences.len()).map(|row| {
                let sql_id = if ids.is_valid(row) {
                    match EntityIdRef::from_handle_bytes(ids.value(row))? { EntityIdRef::Integer(id) => Some(id), _ => None }
                } else { None };
                Ok(LogicalDocument { iid: Iid::from_slice(iids.value(row))?, sql_id,
                    field: fields.value(row) as usize, revision: revisions.value(row).try_into().map_err(fault)?,
                    tokens: tokens.value(row).as_string::<i32>().clone(),
                    position: (Timestamp(sequences.column(3).as_primitive::<Int64Type>().value(row)),
                        super::read_event_position(sequences.column(5).as_ref(), row),
                        sequences.column(4).as_primitive::<UInt32Type>().value(row)) })
            });
            visit(&mut documents)?;
        }
        Ok(())
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

fn grow_exact(reservation: &mut Arc<MemoryReservation>, bytes: usize) -> Result<()> {
    let reservation = Arc::get_mut(reservation).expect("the unfinished exact view owns its reservation");
    reservation.grow_to(reservation.bytes().saturating_add(bytes))
}

fn plan_ordinal(ordinals: &mut BTreeSet<usize>, ordinal: usize, plan: &mut Arc<MemoryReservation>) -> Result<()> {
    if !ordinals.contains(&ordinal) {
        grow_exact(plan, 512)?;
        ordinals.insert(ordinal);
    }
    Ok(())
}

fn append_exact_blocks(blocks: &mut Vec<super::paged::Block>, entries: &arrow_array::LargeListArray, row: usize,
    retained: &mut Arc<MemoryReservation>,
) -> Result<()> {
    let rows = entries.value(row);
    let impacts = rows.as_struct().column(3).as_list::<i64>().value_offsets();
    let impact_count = (impacts[impacts.len() - 1] - impacts[0]) as usize;
    grow_exact(retained, rows.len().saturating_mul(2 * size_of::<super::paged::Block>())
        .saturating_add(impact_count.saturating_mul(size_of::<super::postings::Impact>())))?;
    let _staging = retained.budget().reserve(rows.len().saturating_mul(size_of::<super::paged::Block>()))?;
    blocks.extend(super::paged::blocks(entries, row));
    Ok(())
}

fn direct_reference(children: &[DecodedChild], key: &[u8]) -> Option<PageRef> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::{keys::TableSlug, log::proto::SearchRun};
    use rtdb_storage::{MemoryStore, ObjectStore, audience_work::Limits};
    use rtdb_text::{Query, Scorer};
    fn tokens(text: &str) -> Vec<String> { rtdb_text::Analyzer::Simple.tokens(text) }
    use rtdb_types::{EntityId, EventOp, Interval, TemporalBounds, Timestamp};

    fn logical_run(columns: &[String]) -> Result<(TableSlug, SearchRun, Vec<u8>, std::ops::Range<usize>)> {
        use super::super::{BuildMode, SearchRow, write_group_as};

        let slug = TableSlug::recorded("docs")?;
        let mut run = SearchRun {
            definition_id: Some("docs-f16".into()), definition_version: Some(1), blocks: vec![0],
            source_rows: Some(1), ..Default::default()
        };
        let identity = crate::lane::run_identity(&slug, run.definition_id(), run.definition_version(), &run.blocks, run.rebuild(), run.allocation());
        let rows = vec![SearchRow {
            position: None, identity: None, iid: [1; 16], system_from: 1, valid_from: 0, valid_to: i64::MAX,
            op: EventOp::Put, values: vec![Some(tokens("invoice"))],
        }];
        let mut bytes = Vec::new();
        let (_, _, written) = write_group_as(columns, [[Ok(rows)]], &identity, Some(1), None, &mut bytes, None,
            LOGICAL_FORMAT_VERSION, BuildMode::Postings)?;
        let encoded = written.unwrap().encode(false);
        let (locator, inline) = TreeLocator::decode_route_with_inline(&encoded.text).unwrap();
        assert!(inline.is_some());
        let range = locator.page.range()?;
        run.digest = Some(Sha256::digest(&bytes).to_vec());
        run.file_size = Some(encoded.length as i64);
        run.locator = Some(encoded.text.into_bytes());
        Ok((slug, run, bytes, range.start as usize..range.end as usize))
    }

    fn root_work(decode_bytes: usize) -> Limits {
        Limits { provider_requests: 0, provider_bytes: 0, provider_metadata_bytes: 0, scan_items: 0, decode_bytes }
    }

    fn root_decode_bytes(bytes: &[u8], range: std::ops::Range<usize>, run: &SearchRun) -> Result<usize> {
        let locator = TreeLocator::decode_route(std::str::from_utf8(run.locator()).unwrap()).unwrap();
        let wire = range.len();
        let page = locator.page.authenticate(Buffer::from_slice_ref(&bytes[range]))?;
        Ok(page.decode_bound()?.max(wire))
    }

    #[tokio::test]
    async fn logical_inline_root_spends_one_decode_and_warm_reuse_checks_identity() -> Result<()> {
        let columns = ["body".to_owned()];
        let (slug, mut run, bytes, range) = logical_run(&columns)?;
        let work = root_decode_bytes(&bytes, range, &run)?;
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let runtime = crate::SearchRuntime::with_logical_search(1 << 20);

        let denied = audience_work::within(Some(root_work(work - 1)), runtime.logical_file(&storage, &slug, &run, &columns)).await;
        assert_eq!(denied.err().unwrap().code, "rtdb.storage/audience-work-limit");
        let opened = audience_work::within(Some(root_work(work)), runtime.logical_file(&storage, &slug, &run, &columns)).await?;
        let warm = audience_work::within(Some(root_work(0)), runtime.logical_file(&storage, &slug, &run, &columns)).await?;
        assert!(Arc::ptr_eq(&opened, &warm));
        run.rebuild = Some(1);
        let mismatched = audience_work::within(Some(root_work(0)), runtime.logical_file(&storage, &slug, &run, &columns)).await;
        assert_eq!(mismatched.err().unwrap().code, "rtdb.search/corrupt-file");
        Ok(())
    }

    #[tokio::test]
    async fn logical_fresh_and_unusable_inline_roots_read_the_persisted_root() -> Result<()> {
        let columns = ["body".to_owned()];
        let (slug, mut run, bytes, range) = logical_run(&columns)?;
        let work = root_decode_bytes(&bytes, range.clone(), &run)?;
        let store = Arc::new(MemoryStore::default());
        let storage = Arc::new(BufferPool::new(store.clone(), 1 << 20));
        let runtime = crate::SearchRuntime::with_logical_search(1 << 20);
        let key = ObjectKey::new(run.object_key(&slug)?)?;

        let mut corrupted = bytes.clone();
        corrupted[range.start] ^= 1;
        store.put(&key, corrupted.into()).await?;
        let opened = runtime.logical_file(&storage, &slug, &run, &columns).await?;
        let fresh = runtime.open_logical(&storage, &slug, &run, &columns, true).await;
        assert_eq!(fresh.err().unwrap().code, "rtdb.search/corrupt-file");
        drop(opened);
        store.delete(&key).await?;
        store.put(&key, bytes.clone().into()).await?;
        audience_work::within(Some(root_work(work)), runtime.open_logical(&storage, &slug, &run, &columns, true)).await?;

        let constrained = crate::SearchRuntime::with_logical_search(0);
        audience_work::within(Some(root_work(work)), constrained.logical_file(&storage, &slug, &run, &columns)).await?;

        let mut wrong_root = bytes[range.clone()].to_vec();
        wrong_root[0] ^= 1;
        let (prefix, _) = std::str::from_utf8(run.locator()).unwrap().rsplit_once(':').unwrap();
        run.locator = Some(format!("{prefix}:{}", encode_hex(&lz4_flex::block::compress(&wrong_root))).into_bytes());
        audience_work::within(Some(root_work(work + range.len())), runtime.logical_file(&storage, &slug, &run, &columns)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn term_routing_reports_corruption_without_recharging_a_warm_ancestor() -> Result<()> {
        use rtdb_storage::{MemoryStore, audience_work::{self, Limits}};

        let key = term_key(0, "invoice");
        let child = |page| Child { lower: key.clone(), upper: key.clone(), page };
        let mut bytes = Vec::new();
        let mut writer = Writer::new(&mut bytes, TREE_FORMAT_VERSION)?;
        let leaf = writer.page([])?;
        let lower = write_directory(&mut writer, vec![child(leaf)])?.page;
        let root = write_directory(&mut writer, vec![child(lower)])?.page;
        writer.finish()?;
        bytes[lower.range()?.start as usize] ^= 1;
        let length = bytes.len();
        let pool = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), length));
        let owner = PageOwner::new(&pool, ObjectKey::new("search/corrupt-term-directory")?, Arc::new(PageCache::new(length)));
        let source = PageSource::Local { bytes: Buffer::from_vec(bytes), owner: Some(owner) };
        let reader = TreeReader::new(source, length as u64, TREE_FORMAT_VERSION, MemoryBudget::new(1 << 20));
        reader.page(root, PageKind::Directory).await?;
        let tree = TreeFile {
            reader,
            root: RecordBatch::new_empty(Arc::clone(super::super::layout::schema(TREE_FORMAT_VERSION))),
            directories: BTreeMap::from([(TERM, DirectoryRef { page: root, height: 2 })]),
            routes: BTreeMap::new(),
            format: TREE_FORMAT_VERSION,
        };
        let bytes_for = |page: PageRef| page.metadata as usize + page.body as usize;
        let limits = Limits {
            provider_requests: 0, provider_bytes: 0, provider_metadata_bytes: 0, scan_items: 0,
            decode_bytes: bytes_for(root) + bytes_for(lower),
        };
        let names = ["invoice".to_owned()];
        let error = audience_work::within(Some(limits), tree.term_pages(0, &names)).await.unwrap_err();
        assert_eq!(error.code, "rtdb.search/corrupt-file");
        Ok(())
    }

    #[test]
    fn row_run_pages_share_contiguous_buffers_and_preserve_ipc_bytes() -> Result<()> {
        use super::super::{NO_DOC, NULL_DOC};

        let values = UInt32Array::from(vec![99, 80, 10, NO_DOC, 12, 20, NULL_DOC, 22, 23, 90, 99]);
        let source = |lengths| {
            let item = item_of(&Table::RowRuns.fields()[0]);
            Table::RowRuns.array(vec![list(Arc::new(values.slice(1, 9)), lengths, item)?])
        };
        let uneven = source([1, 3, 4, 1])?;
        let even = source([1, 3, 3, 2])?;
        let cases = [
            (uneven.slice(1, 1), 1, 2, Some(2), false),
            (uneven.slice(1, 1), 0, 3, Some(1), true),
            (even.slice(1, 2), 0, 3, Some(1), true),
            (uneven.slice(1, 2), 0, 3, Some(1), false),
            (uneven.slice(1, 2), 1, 2, None, false),
        ];
        let encode = |row_runs| -> Result<(PageRef, Vec<u8>)> {
            let mut bytes = Vec::new();
            let mut writer = Writer::new(&mut bytes, PACKED_FORMAT_VERSION)?;
            let page = writer.page([(Table::RowRuns, row_runs)])?;
            writer.finish()?;
            Ok((page, bytes))
        };
        for (input, start, len, shared_at, shares_list) in cases {
            let page = row_runs_for_page(&input, start, len)?;
            let input_list = input.column(0).as_list::<i64>();
            let page_list = page.column(0).as_list::<i64>();
            assert_eq!(Arc::ptr_eq(input.column(0), page.column(0)), shares_list);
            let first = page_list.value_offsets()[0] as usize;
            let child = page_list.values().as_primitive::<UInt32Type>().values();
            let source_values = input_list.values().as_primitive::<UInt32Type>().values();
            if let Some(offset) = shared_at {
                assert_eq!(child[first..].as_ptr(), source_values[offset..].as_ptr());
            } else {
                assert_ne!(child.as_ptr(), source_values[2..].as_ptr());
                assert_eq!(child.len(), len * input_list.len());
            }

            let mut copied = Vec::new();
            for field in 0..input_list.len() {
                let child = input_list.value(field);
                let child = child.as_primitive::<UInt32Type>().values();
                copied.extend_from_slice(&child[start..start + len]);
            }
            let item = item_of(&Table::RowRuns.fields()[0]);
            let reference = Table::RowRuns.array(vec![list(Arc::new(UInt32Array::from(copied)),
                std::iter::repeat_n(len, input_list.len()), item)?])?;
            assert_eq!(encode(page)?, encode(reference)?);
        }
        assert!(row_runs_for_page(&uneven.slice(1, 2), 1, 3).is_err());
        Ok(())
    }

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
        let encoded = EncodedTreeLocator::read(&mut file, TREE_FORMAT_VERSION, false).unwrap();
        let locator = TreeLocator::decode(&encoded.text).unwrap();
        assert_eq!(locator.encode(), encoded.text);
        assert!(!locator.has_canonical_uuid_iids());
        let certified = EncodedTreeLocator::read(&mut file, TREE_FORMAT_VERSION, true).unwrap();
        let certified = TreeLocator::decode(&certified.text).unwrap();
        assert_eq!(certified, TreeLocator { canonical_uuid_iids: true, ..locator.clone() });
        let wrong_proof = format!("{}:{}", locator.encode(), encode_hex(&uuid_evidence_digest(&[0; 32])));
        assert_eq!(TreeLocator::decode(&wrong_proof), Some(locator.clone()));
        assert!(locator.might_contain(0, "report"));
        assert!(!locator.might_contain(0, "unpublished-term"));
        file.rewind().unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        let tree = Arc::new(TreeFile::local(Buffer::from_vec(bytes), None, &locator, &columns, "tree", 42, None, MemoryBudget::new(64 << 20)).await.unwrap());
        let found = tree.events(rows[900].iid).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].system_from, 1);
        assert!(tree.events([0xff; 16]).await.unwrap().is_empty());

        let matching = Query::parse("report");
        let scoring = Query::parse("audit");
        let bounds = TemporalBounds { valid_time: Interval::at(Timestamp(2)).unwrap(), system_time: Interval::at(Timestamp(2)).unwrap() };
        let scorer = Arc::new(Scorer::new(scoring.clone(), None)).for_ranking().unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let mut rank = super::super::TreeRanked::open(Arc::clone(&tree), 0, &matching, &scorer, bounds,
            super::super::TreeRankOrder::Integer, &memory).await.unwrap();
        assert_eq!(rank.bound().unwrap().integer_id(), Some(0));
        let mut actual = BTreeMap::new();
        while rank.bound().is_some() {
            for hit in rank.expand(None, &mut 0).await.unwrap() { actual.insert(hit.iid.0, hit.key.score.to_bits()); }
        }
        let flat = super::super::build_search_file(&columns, &rows, "flat").unwrap();
        let flat = super::super::SearchFile::open(flat, &columns).unwrap();
        let expected = flat.scored("body", &matching, &scoring, None, bounds).unwrap().into_iter()
            .map(|(doc, score)| (doc.iid, score.to_bits())).collect::<BTreeMap<_, _>>();
        assert_eq!(actual.len(), rows.len());
        assert_eq!(actual, expected);
        let exact = tree.exact(&columns, 0, &[&matching, &scoring], false, &MemoryBudget::new(32 << 20)).await.unwrap();
        let scored = exact.scored("body", &matching, &scoring, None, bounds).unwrap().into_iter()
            .map(|(doc, score)| (doc.iid, score.to_bits())).collect::<BTreeMap<_, _>>();
        assert_eq!(scored, expected);
    }

    #[tokio::test]
    async fn sparse_phrase_answers_skip_unrelated_documents_but_corpus_requires_them() -> Result<()> {
        let columns = ["body".to_owned()];
        let iid = |id: u32| {
            let mut iid = [0u8; 16];
            iid[..4].copy_from_slice(&id.to_be_bytes());
            iid
        };
        let rows = (0..1026).map(|id| super::super::SearchRow {
            position: None, iid: iid(id), identity: None, system_from: 1, valid_from: 0, valid_to: i64::MAX,
            op: EventOp::Put, values: vec![match id {
                0 => Some(tokens("overdue invoice")),
                1 => Some(tokens("invoice overdue")),
                1025 => None,
                _ => Some(tokens("unrelated")),
            }],
        }).collect::<Vec<_>>();
        let mut file = tempfile::tempfile().unwrap();
        super::super::write_versioned_group(&columns, [[Ok(rows)]], "sparse", 1, None, &mut file, None, PACKED_FORMAT_VERSION)?;
        let encoded = EncodedTreeLocator::read(&mut file, PACKED_FORMAT_VERSION, false)?;
        let locator = TreeLocator::decode(&encoded.text).unwrap();
        file.rewind().unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        let tree = TreeFile::local(Buffer::from_vec(bytes.clone()), None, &locator, &columns, "sparse", 1, None, MemoryBudget::new(64 << 20)).await?;
        let skipped = tree.page_reference_for_test(DOCUMENT, &ordinal(DOCUMENT_ROWS)).await?.unwrap();
        bytes[skipped.range()?.start as usize] ^= 1;
        let tree = TreeFile::local(Buffer::from_vec(bytes), None, &locator, &columns, "sparse", 1, None, MemoryBudget::new(64 << 20)).await?;
        let query = Query::parse("\"overdue invoice\"");
        let memory = MemoryBudget::new(16 << 20);
        let exact = Arc::new(tree.exact(&columns, 0, &[&query], false, &memory).await?);
        let bounds = TemporalBounds { valid_time: Interval::at(Timestamp(2))?, system_time: Interval::at(Timestamp(2))? };
        assert_eq!(exact.matches("body", &query, bounds)?.iter().map(|doc| doc.iid).collect::<Vec<_>>(), [iid(0)]);
        assert_eq!(exact.events(&iid(1024))?.len(), 1);
        let set = Arc::new(exact.match_set("body", &query, memory.reserve(exact.match_all_bytes())?)?);
        let member = set.member(0)?;
        use rtdb_query::FileMatches;
        assert_eq!(member.value(0, 0)?, Some(true));
        assert_eq!(member.value(0, 1)?, Some(false));
        assert_eq!(member.value(0, 1024)?, Some(false));
        assert_eq!(member.value(0, 1025)?, None);
        let absent = Query::parse("missing");
        let visibility = tree.exact(&columns, 0, &[&absent], false, &memory).await?;
        assert_eq!(visibility.events(&iid(1024))?.len(), 1);
        assert!(visibility.matches("body", &absent, bounds)?.is_empty());
        let corpus = tree.exact(&columns, 0, &[&query], true, &memory).await;
        assert_eq!(corpus.err().unwrap().code, "rtdb.search/corrupt-file");
        Ok(())
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
        let (_, _, tree) = super::super::write_group_as(&columns, members, "pack", Some(1), None, &mut file, None,
            PACKED_FORMAT_VERSION, super::super::BuildMode::Combined).unwrap();
        let encoded = tree.unwrap().encode(true);
        let readback = EncodedTreeLocator::read(&mut file, PACKED_FORMAT_VERSION, true).unwrap();
        assert_eq!((encoded.length, &encoded.text), (readback.length, &readback.text));
        let locator = TreeLocator::decode(&encoded.text).unwrap();
        assert!(locator.has_canonical_uuid_iids());
        assert_eq!(locator.encode(), encoded.text);
        assert_eq!(TreeLocator::decode_route(&encoded.text), Some(locator.without_inline_root()));

        let mut corrupt_root = locator.inline_root.as_ref().unwrap().to_vec();
        assert_eq!(locator.inline_root_bytes() as u64, locator.page.metadata as u64 + locator.page.body);
        corrupt_root[0] ^= 1;
        let (prefix, proof) = encoded.text.rsplit_once(':').unwrap();
        let (prefix, _) = prefix.rsplit_once(':').unwrap();
        let corrupted = format!("{prefix}:{}:{proof}", encode_hex(&lz4_flex::block::compress(&corrupt_root)));
        let fallback = TreeLocator::decode(&corrupted).unwrap();
        assert_eq!(fallback, locator.without_inline_root());

        file.rewind().unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        let bytes = Buffer::from_vec(bytes);
        let tree = TreeFile::local(bytes.clone(), None, &locator, &columns, "pack", 1, None, MemoryBudget::new(64 << 20)).await.unwrap();
        tree.verify().await.unwrap();
        TreeFile::local(bytes, None, &fallback, &columns, "pack", 1, None, MemoryBudget::new(64 << 20)).await.unwrap().verify().await.unwrap();
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

    #[tokio::test]
    async fn history_preserves_field_revisions_for_versions_across_visibility_pages() -> Result<()> {
        use super::super::{BuildMode, IndexedRow, SearchPage, write_group_as};
        use rtdb_types::{EventPosition, TxId};

        struct HistoryRows(std::ops::Range<u32>);

        fn header(row: u32) -> EventHeader {
            let mut iid = [0; 16];
            iid[..4].copy_from_slice(&((row + 1) / 2).to_be_bytes());
            EventHeader {
                position: Some(EventPosition { transaction: TxId(i64::from(row) + 1), ordinal: 0 }),
                iid: Iid(iid),
                system_from: Timestamp(10_000 - i64::from(row)),
                commit_position: row,
                valid_time: Interval::ALL,
                op: EventOp::Put,
            }
        }

        impl SearchPage for HistoryRows {
            fn len(&self) -> usize { self.0.len() }

            fn row(&mut self, index: usize, columns: &[String]) -> Result<IndexedRow<'_>> {
                let row = self.0.start + index as u32;
                let revisions = [Some([row as u8; 32]), (row % 2 == 0).then_some([42; 32])];
                Ok(IndexedRow { header: header(row), key: None, sql_id: None, values: Vec::new(),
                    revisions: revisions[..columns.len()].to_vec() })
            }
        }

        let all_columns = ["body".to_owned(), "title".to_owned()];
        let before = EVENT_ROWS as u32 - 1;
        let after = before + 1;
        let iid = header(before).iid;
        let queried = BTreeSet::from([iid]);
        let budget = MemoryBudget::new(1 << 20);
        for field_count in [1, 2] {
            let columns = &all_columns[..field_count];
            let mut file = tempfile::tempfile().map_err(fault)?;
            let members = [[Ok(HistoryRows(0..before)), Ok(HistoryRows(before..after + 2))]];
            write_group_as(columns, members, "history", Some(1), None, &mut file, None, LOGICAL_FORMAT_VERSION, BuildMode::History)?;
            let encoded = EncodedTreeLocator::read(&mut file, LOGICAL_FORMAT_VERSION, false)?;
            let locator = TreeLocator::decode(&encoded.text).unwrap();
            file.rewind().map_err(fault)?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).map_err(fault)?;
            let tree = TreeFile::local(Buffer::from_vec(bytes), None, &locator, columns, "history", 1, None, MemoryBudget::new(64 << 20)).await?;
            tree.verify().await?;
            let page_sizes = tree.all(VISIBILITY).await?.iter().map(|batch| table(batch, Table::Events).len()).collect::<Vec<_>>();
            assert_eq!(page_sizes, [EVENT_ROWS, 2]);

            let expected = [[Some([before as u8; 32]), Some([after as u8; 32])], [None, Some([42; 32])]];
            for (field, [first_revision, second_revision]) in expected.into_iter().take(field_count).enumerate() {
                let (versions, _charge) = tree.versions(&queried, field, &budget).await?;
                let versions = versions[&iid].iter().map(|event| {
                    (event.header.version(), event.member, event.page, event.row, event.revision)
                }).collect::<Vec<_>>();
                assert_eq!(versions, [
                    (header(before).version(), 0, 1, 0, first_revision),
                    (header(after).version(), 0, 1, 1, second_revision),
                ]);
            }
        }
        Ok(())
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
