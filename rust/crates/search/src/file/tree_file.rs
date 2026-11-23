use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StructArray, cast::AsArray, types::{Int32Type, Int64Type, UInt32Type}};
use arrow_buffer::Buffer;
use rtdb_storage::{ObjectKey, cache::BufferPool};
use rtdb_types::Result;
use sha2::{Digest, Sha256};
use crate::rank_windows::window_of;

use super::{TREE_FORMAT_VERSION, artefact_digest, corrupt, fault, layout::{FooterLocator, PageRef, Table, Writer, item_of, list, table},
    page_tree::{Child, DirectoryRef, PageCache, PageKind, PageOwner, PageSource, TreeReader, read_root, root_table, write_directory}, paged::PostingPlan, rank_tree};

pub(super) const POSTING: u8 = 0;
pub(super) const TERM: u8 = 1;
pub(super) const DOCUMENT: u8 = 2;
pub(super) const VISIBILITY: u8 = 3;
pub(super) const PAGE_ROWS: u8 = 4;
const TERM_ROWS: usize = 32;
pub(super) const DOCUMENT_ROWS: usize = 256;
const EVENT_ROWS: usize = 1024;
const MAX_ROOT_BYTES: u64 = 64 * 1024;
const SYNOPSIS_BYTES: usize = 512;

fn page_kind(directory: u8) -> PageKind {
    match directory {
        POSTING => PageKind::Postings,
        TERM => PageKind::Terms,
        DOCUMENT => PageKind::Documents,
        VISIBILITY => PageKind::Visibility,
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
    [(_, events), (_, runs), (_, row_runs)]: [(Table, StructArray); 3], plan: PostingPlan) -> Result<()> {
    let take = |fields: &mut Vec<(Table, StructArray)>, field: Table| -> StructArray {
        let at = fields.iter().position(|(table, _)| *table as usize == field as usize).expect("writer populated this table");
        fields.remove(at).1
    };
    let terms = take(&mut fields, Table::Terms);
    let blocks = take(&mut fields, Table::Blocks);
    let impact_ids = take(&mut fields, Table::ImpactIds);
    let windows = take(&mut fields, Table::Windows);
    let pages = take(&mut fields, Table::Pages);
    let frequencies = plan.write(writer)?;
    let postings = frequencies.into_iter().enumerate().map(|(index, page)| {
        let key = ordinal(index + super::paged::FIRST_POSTING_PAGE).to_vec();
        Child { lower: key.clone(), upper: key, page }
    }).collect();
    let posting_dir = write_directory(writer, postings)?;

    let term_columns = terms.column(0).as_primitive::<UInt32Type>();
    let term_names = terms.column(1).as_string::<i32>();
    let mut synopsis = [0u8; SYNOPSIS_BYTES];
    for row in 0..terms.len() {
        for bit in synopsis_bits(&term_key(term_columns.value(row), term_names.value(row))) {
            synopsis[bit / 8] |= 1 << (bit % 8);
        }
    }
    let mut term_pages = Vec::new();
    let blocks = blocks.column(0).as_list::<i64>();
    let impact_ids = impact_ids.column(0).as_list::<i64>();
    for start in (0..terms.len()).step_by(TERM_ROWS) {
        let end = (start + TERM_ROWS).min(terms.len());
        let key = |row| term_key(term_columns.value(row), term_names.value(row));
        let nodes = (start..end).map(|row| rank_tree::write_term(writer, blocks, impact_ids, row)).collect::<Result<Vec<_>>>()?;
        let page = writer.page([
            (Table::Terms, terms.slice(start, end - start)),
            (Table::RankNodes, rank_tree::nodes_array(&nodes)?),
            (Table::Blocks, rank_tree::inline_rows(blocks, &nodes, start, Table::Blocks)?),
            (Table::ImpactIds, rank_tree::inline_rows(impact_ids, &nodes, start, Table::ImpactIds)?),
        ])?;
        term_pages.push(Child { lower: key(start), upper: key(end - 1), page });
    }
    let term_dir = write_directory(writer, term_pages)?;

    let mut doc_pages = Vec::new();
    for start in (0..runs.len()).step_by(DOCUMENT_ROWS) {
        let end = (start + DOCUMENT_ROWS).min(runs.len());
        let first_window = window_of(start as u32) as usize;
        let last_window = window_of((end - 1) as u32) as usize;
        let start_row = Table::Pages.array(vec![Arc::new(arrow_array::UInt32Array::from(vec![start as u32]))])?;
        let page = writer.page([(Table::Runs, runs.slice(start, end - start)), (Table::Pages, start_row),
            (Table::Windows, windows.slice(first_window, last_window - first_window + 1))])?;
        doc_pages.push(Child { lower: ordinal(start).to_vec(), upper: ordinal(end - 1).to_vec(), page });
    }
    let doc_dir = write_directory(writer, doc_pages)?;

    let iids = events.column(0).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search IID layout");
    let mut event_pages = Vec::new();
    for start in (0..events.len()).step_by(EVENT_ROWS) {
        let end = (start + EVENT_ROWS).min(events.len());
        let start_row = Table::Pages.array(vec![Arc::new(arrow_array::UInt32Array::from(vec![start as u32]))])?;
        let page = writer.page([(Table::Events, events.slice(start, end - start)), (Table::Pages, start_row),
            (Table::RowRuns, start_rows(Table::RowRuns, &row_runs, start, end - start)?)])?;
        event_pages.push(Child {
            lower: event_key(iids.value(start), start as u32), upper: event_key(iids.value(end - 1), (end - 1) as u32), page,
        });
    }
    let event_dir = write_directory(writer, event_pages)?;
    let counts = Table::Pages.array(vec![Arc::new(arrow_array::UInt32Array::from(vec![events.len() as u32, runs.len() as u32]))])?;
    let page_map = writer.page([(Table::Pages, pages)])?;
    let page_dir = write_directory(writer, vec![Child { lower: ordinal(0).to_vec(), upper: ordinal(0).to_vec(), page: page_map }])?;
    fields.push((Table::Pages, counts));
    fields.push((Table::Synopsis, Table::Synopsis.array(vec![Arc::new(
        arrow_array::FixedSizeBinaryArray::try_from_iter([synopsis].iter()).map_err(fault)?,
    )])?));
    fields.push((Table::Root, root_table(&[(POSTING, posting_dir), (TERM, term_dir), (DOCUMENT, doc_dir), (VISIBILITY, event_dir), (PAGE_ROWS, page_dir)])?));
    let root = writer.page(fields)?;
    let range = root.range()?;
    if range.end - range.start > MAX_ROOT_BYTES { return Err(fault("Search root exceeds its read budget")); }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TreeLocator {
    pub length: u64,
    page: PageRef,
    synopsis: Option<Arc<[u8; SYNOPSIS_BYTES]>>,
}

impl TreeLocator {
    pub fn read(input: &mut File) -> Result<Self> {
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
        let decoder = arrow_ipc::reader::FileDecoder::new(Arc::clone(super::layout::schema(TREE_FORMAT_VERSION)), arrow_ipc::MetadataVersion::V5);
        let root = page.decode(Buffer::from_vec(bytes), &decoder)?;
        let synopsis = table(&root, Table::Synopsis);
        let bits = synopsis.column(0).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search synopsis layout");
        if bits.len() != 1 || bits.is_null(0) { return Err(corrupt("Search root has no complete term synopsis")); }
        let synopsis = Arc::new(bits.value(0).try_into().map_err(fault)?);
        Ok(Self { length: locator.length as u64, page, synopsis: Some(synopsis) })
    }

    pub fn encode(&self) -> String {
        let digest = encode_hex(&self.page.digest);
        let header = format!("{}:{}:{}:{}:{digest}", self.length, self.page.offset, self.page.metadata, self.page.body);
        let Some(bits) = self.synopsis.as_deref() else { return header; };
        let check = synopsis_digest(&self.page.digest, bits);
        format!("{header}:{}:{}", encode_hex(bits), encode_hex(&check))
    }

    pub fn decode(value: &str) -> Option<Self> {
        let mut fields = value.split(':');
        let length = fields.next()?.parse().ok()?;
        let offset = fields.next()?.parse().ok()?;
        let metadata = fields.next()?.parse().ok()?;
        let body = fields.next()?.parse().ok()?;
        let digest = decode_hex(fields.next()?)?;
        let page = PageRef { offset, metadata, body, digest };
        let range = page.range().ok()?;
        let synopsis = fields.next().and_then(decode_hex::<SYNOPSIS_BYTES>).zip(fields.next().and_then(decode_hex::<32>))
            .filter(|(bits, check)| synopsis_digest(&digest, bits) == *check).map(|(bits, _)| Arc::new(bits));
        (range.end <= length && range.end - range.start <= MAX_ROOT_BYTES).then_some(Self { length, page, synopsis })
    }

    pub fn digest(&self) -> [u8; 32] { self.page.digest }

    pub fn might_contain(&self, column: u32, term: &str) -> bool {
        self.synopsis.as_ref().is_none_or(|synopsis| synopsis_bits(&term_key(column, term))
            .all(|bit| synopsis[bit / 8] & (1 << (bit % 8)) != 0))
    }
}

fn encode_hex(bytes: &[u8]) -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() }

fn decode_hex<const N: usize>(hex: &str) -> Option<[u8; N]> {
    if hex.len() != N * 2 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) { return None; }
    let mut bytes = [0; N];
    for (position, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * position..2 * position + 2], 16).ok()?;
    }
    Some(bytes)
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
        let reader = TreeReader::new(source, locator.length);
        let root = reader.page(locator.page, PageKind::Root).await?;
        let header = table(&root, Table::Header);
        let definitions = header.column(2).as_primitive::<Int64Type>();
        let names = table(&root, Table::Columns);
        let keys = header.column(3).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search identity layout");
        if header.len() != 1 || definitions.is_null(0) || definitions.value(0) != version
            || header.column(0).as_primitive::<Int32Type>().value(0) != TREE_FORMAT_VERSION
            || header.column(1).as_primitive::<Int32Type>().value(0) != rtdb_text::ANALYZER_VERSION
            || keys.value(0) != artefact_digest(identity)
            || !names.column(0).as_string::<i32>().iter().eq(columns.iter().map(|column| Some(column.as_str()))) {
            return Err(corrupt("Search tree belongs to another index"));
        }
        let directories = read_root(&root)?;
        if !directories.keys().copied().eq([POSTING, TERM, DOCUMENT, VISIBILITY, PAGE_ROWS])
            || directories.values().any(|dir| dir.page.offset >= locator.page.offset) {
            return Err(corrupt("Search tree root has incomplete directories"));
        }
        Ok(Self { reader, root, directories })
    }

    pub(super) fn root(&self) -> &RecordBatch { &self.root }

    pub(super) fn reader(&self) -> &TreeReader { &self.reader }

    pub fn sections(&self) -> Result<Vec<super::Section>> { super::sections_of(&table(&self.root, Table::Sections)) }

    pub async fn verify(&self) -> Result<()> {
        for kind in [TERM, POSTING, DOCUMENT, VISIBILITY, PAGE_ROWS] {
            for page in self.reader.leaves(self.directories[&kind]).await? { self.reader.page(page, page_kind(kind)).await?; }
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
        let layout = super::layout::record(TREE_FORMAT_VERSION, [
            (Table::Header, table(&self.root, Table::Header)),
            (Table::Columns, table(&self.root, Table::Columns)),
            (Table::Members, table(&self.root, Table::Members)),
            (Table::Sections, table(&self.root, Table::Sections)),
            (Table::Pages, table(&pages, Table::Pages)),
            (Table::Terms, concat(&terms, Table::Terms)?),
            (Table::Blocks, rank_tree::materialize_blocks(&self.reader, &terms).await?),
            (Table::Windows, concat(&documents, Table::Windows)?),
        ])?;
        let visibility = super::layout::record(TREE_FORMAT_VERSION, [
            (Table::Events, concat(&visibility, Table::Events)?),
            (Table::Runs, concat(&documents, Table::Runs)?),
            (Table::RowRuns, row_maps),
        ])?;
        let mut pages = vec![layout, visibility];
        pages.extend(self.all(POSTING).await?);
        super::SearchFile::open_reader(super::layout::Reader::from_checked_batches(pages), columns)
            .map(|file| file.with_reservation(reservation))
    }

    #[cfg(test)]
    pub(super) async fn page_reference_for_test(&self, kind: u8, key: &[u8]) -> Result<Option<PageRef>> {
        self.reader.find(self.directories[&kind], key).await
    }

    pub(super) async fn find(&self, kind: u8, key: &[u8]) -> Result<Option<RecordBatch>> {
        if let Some(page) = self.reader.find(self.directories[&kind], key).await? {
            return self.reader.page(page, page_kind(kind)).await.map(Some);
        }
        Ok(None)
    }

    pub(super) async fn all(&self, kind: u8) -> Result<Vec<RecordBatch>> {
        let references = self.reader.leaves(self.directories[&kind]).await?;
        let mut pages = self.reader.pages(references.clone(), page_kind(kind)).await?;
        references.into_iter().map(|reference| pages.remove(&reference).ok_or_else(|| corrupt("Search page was omitted"))).collect()
    }

    pub(super) async fn find_ordinals(&self, kind: u8, ordinals: std::collections::BTreeSet<usize>) -> Result<BTreeMap<usize, RecordBatch>> {
        let references = self.reader.find_ordinals(self.directories[&kind], ordinals.into_iter().collect()).await?;
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
                found.push(super::KeyedEvent { system_from: bounds[0].value(row), valid_from: bounds[1].value(row),
                    valid_to: bounds[2].value(row), op: super::op_of(operations.value(row))? });
            }
        }
        Ok(found)
    }
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
            super::super::SearchRow { iid: identity.iid().0, identity: Some(identity), system_from: 1, valid_from: 0,
                valid_to: i64::MAX, op: EventOp::Put, values: vec![Some(tokens(if id % 2 == 0 { "report audit audit" }
                    else { "report audit about the library" }))] }
        }).collect();
        rows.sort_unstable_by_key(|row| row.iid);
        let mut file = tempfile::tempfile().unwrap();
        super::super::write_versioned_group(&columns, [[Ok(rows.clone())]], "tree", 42, None, &mut file, None, TREE_FORMAT_VERSION).unwrap();
        let locator = TreeLocator::read(&mut file).unwrap();
        assert_eq!(TreeLocator::decode(&locator.encode()), Some(locator.clone()));
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
        let mut rank = super::super::TreeRanked::open(Arc::clone(&tree), 0, &matching, &scorer, bounds, true).await.unwrap();
        assert_eq!(rank.bound().unwrap().id, Some(0));
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

    #[test]
    fn format_eleven_can_be_read_with_the_arrow_search_api() {
        let columns = ["body".to_owned()];
        let row = super::super::SearchRow { identity: None, iid: [7; 16], system_from: 1, valid_from: 0,
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
