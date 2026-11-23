use std::{
    collections::BTreeSet,
    future::Future,
    io::Cursor,
    ops::Range,
    pin::Pin,
    sync::{Arc, LazyLock},
};

use arrow_array::{
    Array, BinaryArray, FixedSizeBinaryArray, RecordBatch, UInt64Array,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder},
};
use arrow_ipc::{reader::FileReader, writer::FileWriter};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use sha2::{Digest, Sha256};

pub const MAX_LEAF_ROWS: usize = 2048;
pub const MAX_FANOUT: usize = 64;
const MAX_HEIGHT: u8 = 64;
const MAX_PAGE_BYTES: u64 = 128 << 20;
const PAGE_GENERATION: &str = "rtdb.cow-tree.generation";
const ROOT_MAGIC: &[u8; 8] = b"RTDBCOW1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageDigest(pub [u8; 32]);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootRef {
    pub digest: PageDigest,
    pub length: u64,
    pub height: u8,
    pub summary: Vec<u8>,
}

impl RootRef {
    pub fn encode(&self) -> Vec<u8> {
        [
            ROOT_MAGIC.as_slice(),
            &self.digest.0,
            &self.length.to_be_bytes(),
            &[self.height],
            &self.summary,
        ]
        .concat()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 49 || &bytes[..8] != ROOT_MAGIC {
            return Err(invalid("Invalid root encoding"));
        }
        let root = Self {
            digest: PageDigest(bytes[8..40].try_into().expect("digest width")),
            length: u64::from_be_bytes(bytes[40..48].try_into().expect("length width")),
            height: bytes[48],
            summary: bytes[49..].to_vec(),
        };
        root.validate()?;
        Ok(root)
    }

    fn validate(&self) -> Result<()> {
        if self.height > MAX_HEIGHT || !(1..=MAX_PAGE_BYTES).contains(&self.length) {
            return Err(invalid("Page reference exceeds the tree limits"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub enum Edit {
    Put(Row),
    Delete(Vec<u8>),
}

impl Edit {
    fn key(&self) -> &[u8] {
        match self {
            Self::Put(row) => &row.key,
            Self::Delete(key) => key,
        }
    }
}

pub trait Summary: Send + Sync {
    fn leaf(&self, rows: &[Row]) -> Vec<u8>;
    fn branch(&self, children: &[&[u8]]) -> Vec<u8>;
}

pub struct NoSummary;

impl Summary for NoSummary {
    fn leaf(&self, _: &[Row]) -> Vec<u8> {
        Vec::new()
    }
    fn branch(&self, _: &[&[u8]]) -> Vec<u8> {
        Vec::new()
    }
}

#[async_trait::async_trait]
pub trait PageStore: Send + Sync {
    async fn get(&self, digest: PageDigest, length: u64) -> Result<Bytes>;
    async fn put(&self, digest: PageDigest, bytes: Bytes) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Edge {
    first: Vec<u8>,
    last: Vec<u8>,
    root: RootRef,
}

enum Page {
    Leaf(Vec<Row>),
    Branch(Vec<Edge>),
}

impl Page {
    fn fences(&self) -> (&[u8], &[u8]) {
        match self {
            Self::Leaf(rows) => (&rows[0].key, &rows[rows.len() - 1].key),
            Self::Branch(children) => (&children[0].first, &children[children.len() - 1].last),
        }
    }
}

fn schema(height: u8) -> &'static SchemaRef {
    static LEAF: LazyLock<SchemaRef> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, false),
            Field::new("value", DataType::Binary, false),
        ]))
    });
    static BRANCH: LazyLock<SchemaRef> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("first_key", DataType::Binary, false),
            Field::new("last_key", DataType::Binary, false),
            Field::new("digest", DataType::FixedSizeBinary(32), false),
            Field::new("length", DataType::UInt64, false),
            Field::new("summary", DataType::Binary, false),
        ]))
    });
    if height == 0 { &LEAF } else { &BRANCH }
}

fn binary<'a>(values: impl Iterator<Item = &'a [u8]>) -> BinaryArray {
    let mut builder = BinaryBuilder::new();
    for value in values {
        builder.append_value(value);
    }
    builder.finish()
}

fn encode(page: &Page, height: u8, generation: Option<i64>) -> Result<Bytes> {
    let columns: Vec<Arc<dyn Array>> = match page {
        Page::Leaf(rows) => vec![
            Arc::new(binary(rows.iter().map(|row| row.key.as_slice()))),
            Arc::new(binary(rows.iter().map(|row| row.value.as_slice()))),
        ],
        Page::Branch(children) => {
            let mut digests = FixedSizeBinaryBuilder::with_capacity(children.len(), 32);
            for child in children {
                digests.append_value(child.root.digest.0).map_err(invalid)?;
            }
            vec![
                Arc::new(binary(children.iter().map(|child| child.first.as_slice()))),
                Arc::new(binary(children.iter().map(|child| child.last.as_slice()))),
                Arc::new(digests.finish()),
                Arc::new(UInt64Array::from_iter_values(
                    children.iter().map(|child| child.root.length),
                )),
                Arc::new(binary(
                    children.iter().map(|child| child.root.summary.as_slice()),
                )),
            ]
        }
    };
    let schema = match generation {
        Some(generation) => Arc::new(schema(height).as_ref().clone().with_metadata(
            std::collections::HashMap::from([(PAGE_GENERATION.to_owned(), generation.to_string())]),
        )),
        None => Arc::clone(schema(height)),
    };
    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(invalid)?;
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut bytes, &schema).map_err(invalid)?;
        writer.write(&batch).map_err(invalid)?;
        writer.finish().map_err(invalid)?;
    }
    Ok(bytes.into())
}

fn decode(bytes: &Bytes, height: u8) -> Result<Page> {
    let mut reader = FileReader::try_new(Cursor::new(bytes.as_ref()), None).map_err(invalid)?;
    let actual = reader.schema();
    if actual.fields() != schema(height).fields()
        || reader.num_batches() != 1
        || (!actual.metadata().is_empty()
            && (actual.metadata().len() != 1
                || !actual
                    .metadata()
                    .get(PAGE_GENERATION)
                    .is_some_and(|generation| generation.parse::<i64>().is_ok())))
    {
        return Err(invalid("Unexpected Arrow page layout"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("Missing Arrow batch"))?
        .map_err(invalid)?;
    let count = batch.num_rows();
    let max_rows = if height == 0 {
        MAX_LEAF_ROWS
    } else {
        MAX_FANOUT
    };
    if !(1..=max_rows).contains(&count)
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("Invalid Arrow page rows"));
    }
    let binary = |i: usize| {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("validated binary column")
    };
    if height == 0 {
        let (keys, values) = (binary(0), binary(1));
        if (1..count).any(|row| keys.value(row - 1) >= keys.value(row)) {
            return Err(invalid("Leaf keys are out of order"));
        }
        return Ok(Page::Leaf(
            (0..count)
                .map(|row| Row {
                    key: keys.value(row).to_vec(),
                    value: values.value(row).to_vec(),
                })
                .collect(),
        ));
    }
    let (firsts, lasts, summaries) = (binary(0), binary(1), binary(4));
    let digests = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("validated digest column");
    let lengths = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("validated length column");
    let mut children = Vec::with_capacity(count);
    for row in 0..count {
        let (first, last) = (firsts.value(row), lasts.value(row));
        if first > last || (row > 0 && lasts.value(row - 1) >= first) {
            return Err(invalid("Branch fences overlap or are out of order"));
        }
        let root = RootRef {
            digest: PageDigest(digests.value(row).try_into().expect("digest width")),
            length: lengths.value(row),
            height: height - 1,
            summary: summaries.value(row).to_vec(),
        };
        root.validate()?;
        children.push(Edge {
            first: first.to_vec(),
            last: last.to_vec(),
            root,
        });
    }
    Ok(Page::Branch(children))
}

async fn load(
    store: &dyn PageStore,
    root: &RootRef,
    edge: Option<&Edge>,
    memory: Option<&Arc<MemoryBudget>>,
) -> Result<(Page, Option<Arc<MemoryReservation>>)> {
    root.validate()?;
    let charge = memory
        .map(|memory| {
            memory.reserve(
                (root.length as usize)
                    .saturating_mul(4)
                    .saturating_add(4096),
            )
        })
        .transpose()?;
    let bytes = store.get(root.digest, root.length).await?;
    if bytes.len() as u64 != root.length || digest(&bytes) != root.digest {
        return Err(invalid("Page digest or length differs from its reference"));
    }
    let page = decode(&bytes, root.height)?;
    if let Some(edge) = edge
        && page.fences() != (edge.first.as_slice(), edge.last.as_slice())
    {
        return Err(invalid("Child contents differ from their branch fences"));
    }
    Ok((page, charge))
}

fn digest(bytes: &[u8]) -> PageDigest {
    PageDigest(Sha256::digest(bytes).into())
}

pub fn page_key(prefix: &str, digest: PageDigest) -> String {
    use std::fmt::Write;
    let mut key = format!("{prefix}/pages/");
    for byte in digest.0 {
        write!(key, "{byte:02x}").expect("writing to a string succeeds");
    }
    key.push_str(".arrow");
    key
}

struct Writer<'a> {
    store: &'a dyn PageStore,
    summary: &'a dyn Summary,
    memory: &'a Arc<MemoryBudget>,
    written: BTreeSet<PageDigest>,
    generation: Option<i64>,
}

impl Writer<'_> {
    async fn write(&mut self, page: Page, height: u8, old: Option<&RootRef>) -> Result<Edge> {
        let (estimated, summary) = match &page {
            Page::Leaf(rows) => (
                rows.iter()
                    .map(|r| r.key.len() + r.value.len() + 32)
                    .sum::<usize>(),
                self.summary.leaf(rows),
            ),
            Page::Branch(children) => (
                children
                    .iter()
                    .map(|e| e.first.len() + e.last.len() + e.root.summary.len() + 80)
                    .sum::<usize>(),
                self.summary.branch(
                    &children
                        .iter()
                        .map(|e| e.root.summary.as_slice())
                        .collect::<Vec<_>>(),
                ),
            ),
        };
        if estimated as u64 > MAX_PAGE_BYTES {
            return Err(invalid("Page exceeds maximum encoded size"));
        }
        let _charge = self
            .memory
            .reserve(estimated.saturating_mul(3).saturating_add(4096))?;
        let bytes = encode(&page, height, self.generation)?;
        let root = RootRef {
            digest: digest(&bytes),
            length: bytes.len() as u64,
            height,
            summary,
        };
        root.validate()?;
        if old.is_none_or(|old| old.digest != root.digest) && !self.written.contains(&root.digest) {
            self.store.put(root.digest, bytes).await?;
            self.written.insert(root.digest);
        }
        let (first, last) = page.fences();
        Ok(Edge {
            first: first.to_vec(),
            last: last.to_vec(),
            root,
        })
    }

    async fn leaves(&mut self, mut rows: Vec<Row>, old: Option<&RootRef>) -> Result<Vec<Edge>> {
        let mut children = Vec::new();
        let width = split_width(rows.len(), MAX_LEAF_ROWS);
        while rows.len() > width {
            let rest = rows.split_off(width);
            children.push(self.write(Page::Leaf(rows), 0, old).await?);
            rows = rest;
        }
        if !rows.is_empty() {
            children.push(self.write(Page::Leaf(rows), 0, old).await?);
        }
        Ok(children)
    }

    async fn branches(
        &mut self,
        mut children: Vec<Edge>,
        height: u8,
        old: Option<&RootRef>,
    ) -> Result<Vec<Edge>> {
        let mut parents = Vec::new();
        let width = split_width(children.len(), MAX_FANOUT);
        while children.len() > width {
            let rest = children.split_off(width);
            parents.push(self.write(Page::Branch(children), height, old).await?);
            children = rest;
        }
        if !children.is_empty() {
            parents.push(self.write(Page::Branch(children), height, old).await?);
        }
        Ok(parents)
    }

    fn rewrite<'a>(
        &'a mut self,
        root: &'a RootRef,
        edge: Option<&'a Edge>,
        edits: &'a [Edit],
        top: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Edge>>> + Send + 'a>> {
        Box::pin(async move {
            let (page, _charge) = load(self.store, root, edge, Some(self.memory)).await?;
            let children = match page {
                Page::Branch(children) => children,
                Page::Leaf(rows) => return self.leaves(merge(rows, edits), Some(root)).await,
            };
            let mut updated = Vec::new();
            let mut remaining = edits;
            let mut children = children.into_iter().peekable();
            while let Some(child) = children.next() {
                let end = children.peek().map_or(remaining.len(), |next| {
                    remaining.partition_point(|edit| edit.key() < next.first.as_slice())
                });
                let (touching, rest) = remaining.split_at(end);
                remaining = rest;
                if touching.is_empty() {
                    updated.push(child);
                    continue;
                }
                if child.root.height == 0
                    && touching.iter().any(|edit| matches!(edit, Edit::Delete(_)))
                {
                    let (Page::Leaf(rows), _left) =
                        load(self.store, &child.root, Some(&child), Some(self.memory)).await?
                    else {
                        unreachable!()
                    };
                    let mut rows = merge(rows, touching);
                    if !rows.is_empty()
                        && rows.len() < MAX_LEAF_ROWS / 2
                        && let Some(next) = children.peek()
                        && remaining.is_empty()
                    {
                        let (Page::Leaf(right), _right) =
                            load(self.store, &next.root, Some(next), Some(self.memory)).await?
                        else {
                            unreachable!()
                        };
                        if rows.len() + right.len() <= MAX_LEAF_ROWS {
                            rows.extend(right);
                            children.next();
                            updated.extend(self.leaves(rows, None).await?);
                            continue;
                        }
                    }
                    updated.extend(self.leaves(rows, Some(&child.root)).await?);
                } else {
                    updated.extend(
                        self.rewrite(&child.root, Some(&child), touching, false)
                            .await?,
                    );
                }
            }
            if top && updated.len() <= 1 {
                return Ok(updated);
            }
            self.branches(updated, root.height, Some(root)).await
        })
    }
}

fn split_width(count: usize, max: usize) -> usize {
    count.div_ceil(count.div_ceil(max).max(1))
}

fn merge(rows: Vec<Row>, edits: &[Edit]) -> Vec<Row> {
    let mut rows: std::collections::BTreeMap<_, _> =
        rows.into_iter().map(|row| (row.key, row.value)).collect();
    for edit in edits {
        match edit {
            Edit::Put(row) => {
                rows.insert(row.key.clone(), row.value.clone());
            }
            Edit::Delete(key) => {
                rows.remove(key);
            }
        }
    }
    rows.into_iter()
        .map(|(key, value)| Row { key, value })
        .collect()
}

pub async fn apply(
    store: &dyn PageStore,
    base: Option<&RootRef>,
    edits: Vec<Edit>,
    summary: &dyn Summary,
    memory: &Arc<MemoryBudget>,
) -> Result<(Option<RootRef>, Vec<PageDigest>)> {
    apply_with_generation(store, base, edits, summary, memory, None).await
}

/// The seal's generation prevents a later edit from recreating a digest after its retirement.
pub async fn apply_with_generation(
    store: &dyn PageStore,
    base: Option<&RootRef>,
    edits: Vec<Edit>,
    summary: &dyn Summary,
    memory: &Arc<MemoryBudget>,
    generation: Option<i64>,
) -> Result<(Option<RootRef>, Vec<PageDigest>)> {
    if edits.is_empty() {
        return Ok((base.cloned(), Vec::new()));
    }
    if edits.windows(2).any(|pair| pair[0].key() >= pair[1].key()) {
        return Err(invalid("Edits must be strictly ordered by key"));
    }
    let bytes = edits.iter().fold(4096usize, |bytes, edit| {
        bytes
            .saturating_add(128)
            .saturating_add(edit.key().len())
            .saturating_add(match edit {
                Edit::Put(row) => row.value.len(),
                Edit::Delete(_) => 0,
            })
    });
    let _charge = memory.reserve(bytes.saturating_mul(2))?;
    let mut writer = Writer {
        store,
        summary,
        memory,
        written: BTreeSet::new(),
        generation,
    };
    let mut edges = if let Some(base) = base {
        writer.rewrite(base, None, &edits, true).await?
    } else {
        writer.leaves(merge(Vec::new(), &edits), None).await?
    };
    while edges.len() > 1 {
        let height = edges[0].root.height + 1;
        edges = writer.branches(edges, height, None).await?;
    }
    Ok((
        edges.pop().map(|edge| edge.root),
        writer.written.into_iter().collect(),
    ))
}

pub async fn scan(
    store: &dyn PageStore,
    root: &RootRef,
    range: Range<Vec<u8>>,
    memory: &Arc<MemoryBudget>,
) -> Result<Vec<Row>> {
    scan_where(store, root, range, &|_| true, memory).await
}

pub async fn scan_where(
    store: &dyn PageStore,
    root: &RootRef,
    range: Range<Vec<u8>>,
    admit: &(dyn Fn(&[u8]) -> bool + Send + Sync),
    memory: &Arc<MemoryBudget>,
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    let mut charge = memory.reserve(0)?;
    visit(store, root, range, admit, memory, |row| {
        let allocation = Arc::get_mut(&mut charge).expect("unpublished rows own their reservation");
        allocation.grow_to(
            allocation
                .bytes()
                .saturating_add(128)
                .saturating_add(row.key.len())
                .saturating_add(row.value.len()),
        )?;
        rows.push(row);
        Ok(true)
    })
    .await?;
    Ok(rows)
}

pub async fn visit(
    store: &dyn PageStore,
    root: &RootRef,
    range: Range<Vec<u8>>,
    admit: &(dyn Fn(&[u8]) -> bool + Send + Sync),
    memory: &Arc<MemoryBudget>,
    mut row: impl FnMut(Row) -> Result<bool> + Send,
) -> Result<()> {
    if range.start >= range.end || !admit(&root.summary) {
        return Ok(());
    }
    visit_page(store, root, None, &range, admit, memory, &mut row).await?;
    Ok(())
}

fn visit_page<'a, F: FnMut(Row) -> Result<bool> + Send + 'a>(
    store: &'a dyn PageStore,
    root: &'a RootRef,
    edge: Option<&'a Edge>,
    range: &'a Range<Vec<u8>>,
    admit: &'a (dyn Fn(&[u8]) -> bool + Send + Sync),
    memory: &'a Arc<MemoryBudget>,
    row: &'a mut F,
) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>> {
    Box::pin(async move {
        let (page, _charge) = load(store, root, edge, Some(memory)).await?;
        match page {
            Page::Leaf(rows) => {
                let start = rows.partition_point(|row| row.key < range.start);
                let end = rows.partition_point(|row| row.key < range.end);
                for value in rows.into_iter().skip(start).take(end - start) {
                    if !row(value)? {
                        return Ok(false);
                    }
                }
            }
            Page::Branch(children) => {
                for child in children.iter().filter(|child| {
                    child.last >= range.start
                        && child.first < range.end
                        && admit(&child.root.summary)
                }) {
                    if !visit_page(store, &child.root, Some(child), range, admit, memory, row)
                        .await?
                    {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    })
}

pub async fn reachable(store: &dyn PageStore, root: &RootRef) -> Result<Vec<PageDigest>> {
    let mut seen = BTreeSet::new();
    let mut pending = vec![(root.clone(), None)];
    while let Some((reference, edge)) = pending.pop() {
        if !seen.insert(reference.digest) {
            continue;
        }
        if let (Page::Branch(children), _) = load(store, &reference, edge.as_ref(), None).await? {
            pending.extend(
                children
                    .into_iter()
                    .map(|edge| (edge.root.clone(), Some(edge))),
            );
        }
    }
    Ok(seen.into_iter().collect())
}

pub async fn retirement_order(
    store: &dyn PageStore,
    root: &RootRef,
    protected: &BTreeSet<PageDigest>,
) -> Result<Vec<PageDigest>> {
    let mut seen = BTreeSet::new();
    let mut pending = vec![(root.clone(), None, false)];
    let mut pages = Vec::new();
    while let Some((reference, edge, expanded)) = pending.pop() {
        if expanded {
            pages.push(reference.digest);
            continue;
        }
        if protected.contains(&reference.digest) || !seen.insert(reference.digest) {
            continue;
        }
        match load(store, &reference, edge.as_ref(), None).await {
            Ok((Page::Branch(children), _)) => {
                pending.push((reference, None, true));
                pending.extend(
                    children
                        .into_iter()
                        .map(|edge| (edge.root.clone(), Some(edge), false)),
                );
            }
            Ok((Page::Leaf(_), _)) => pages.push(reference.digest),
            // A prior pass releases descendants before their parent.
            Err(error) if error.category == AnomalyCategory::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(pages)
}

fn invalid(message: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.format/cow-tree", message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, sync::Mutex};

    #[derive(Default)]
    struct Pages(Mutex<BTreeMap<PageDigest, Bytes>>);

    #[async_trait::async_trait]
    impl PageStore for Pages {
        async fn get(&self, digest: PageDigest, _: u64) -> Result<Bytes> {
            self.0.lock().unwrap().get(&digest).cloned().ok_or_else(|| {
                Anomaly::new(AnomalyCategory::NotFound, "test/missing", "Page missing")
            })
        }
        async fn put(&self, digest: PageDigest, bytes: Bytes) -> Result<()> {
            self.0.lock().unwrap().insert(digest, bytes);
            Ok(())
        }
    }

    struct Maximum;
    impl Summary for Maximum {
        fn leaf(&self, rows: &[Row]) -> Vec<u8> {
            rows.iter().map(|row| &row.value).max().unwrap().clone()
        }
        fn branch(&self, children: &[&[u8]]) -> Vec<u8> {
            children.iter().max().unwrap().to_vec()
        }
    }
    fn key(i: usize) -> Vec<u8> {
        (i as u32).to_be_bytes().to_vec()
    }
    fn put(i: usize) -> Edit {
        Edit::Put(Row {
            key: key(i),
            value: key(i),
        })
    }

    #[tokio::test]
    async fn range_pruning_and_updates_reuse_untouched_pages() {
        let store = Pages::default();
        let memory = MemoryBudget::new(16 << 20);
        let count = 3 * MAX_LEAF_ROWS;
        let (root, written) = apply(
            &store,
            None,
            (0..count).map(put).collect(),
            &Maximum,
            &memory,
        )
        .await
        .unwrap();
        let root = root.unwrap();
        assert_eq!(root, RootRef::decode(&root.encode()).unwrap());
        assert_eq!(written, reachable(&store, &root).await.unwrap());
        let cutoff = key(count - MAX_LEAF_ROWS);
        let exact = scan(&store, &root, cutoff.clone()..key(count), &memory)
            .await
            .unwrap();
        assert_eq!(exact.len(), MAX_LEAF_ROWS);
        assert_eq!(
            scan_where(
                &store,
                &root,
                key(0)..key(count),
                &|summary| summary >= cutoff.as_slice(),
                &memory
            )
            .await
            .unwrap(),
            exact
        );
        let (next, written) = apply(
            &store,
            Some(&root),
            vec![Edit::Put(Row {
                key: key(0),
                value: key(11),
            })],
            &Maximum,
            &memory,
        )
        .await
        .unwrap();
        let next = next.unwrap();
        let old_pages = reachable(&store, &root).await.unwrap();
        let new_pages = reachable(&store, &next).await.unwrap();
        assert_eq!(written.len(), 2);
        assert_eq!(
            new_pages
                .iter()
                .filter(|page| old_pages.contains(page))
                .count(),
            2
        );
        assert_eq!(
            scan(&store, &root, key(0)..key(1), &memory).await.unwrap()[0].value,
            key(0)
        );
        assert_eq!(
            scan(&store, &next, key(0)..key(1), &memory).await.unwrap()[0].value,
            key(11)
        );
    }

    #[tokio::test]
    async fn deletes_merge_leaves_and_remove_an_empty_root() {
        let store = Pages::default();
        let memory = MemoryBudget::new(16 << 20);
        let (root, _) = apply(
            &store,
            None,
            (0..MAX_LEAF_ROWS + 1).map(put).collect(),
            &NoSummary,
            &memory,
        )
        .await
        .unwrap();
        let root = root.unwrap();
        assert_eq!(root.height, 1);
        let (root, _) = apply(
            &store,
            Some(&root),
            vec![Edit::Delete(key(0)), Edit::Delete(key(1))],
            &NoSummary,
            &memory,
        )
        .await
        .unwrap();
        let root = root.unwrap();
        assert_eq!(root.height, 0);
        let (root, _) = apply(
            &store,
            Some(&root),
            (2..MAX_LEAF_ROWS + 1)
                .map(|i| Edit::Delete(key(i)))
                .collect(),
            &NoSummary,
            &memory,
        )
        .await
        .unwrap();
        assert!(root.is_none());
    }

    #[tokio::test]
    async fn a_tampered_child_is_rejected_when_walked() {
        let store = Pages::default();
        let memory = MemoryBudget::new(16 << 20);
        let (root, pages) = apply(
            &store,
            None,
            (0..MAX_LEAF_ROWS + 1).map(put).collect(),
            &NoSummary,
            &memory,
        )
        .await
        .unwrap();
        let root = root.unwrap();
        let child = pages.into_iter().find(|page| *page != root.digest).unwrap();
        let mut held = store.0.lock().unwrap().get(&child).unwrap().to_vec();
        held[0] ^= 1;
        store.0.lock().unwrap().insert(child, held.into());
        assert!(reachable(&store, &root).await.is_err());
        assert!(
            scan(&store, &root, key(0)..key(MAX_LEAF_ROWS + 1), &memory)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn retirement_skips_shared_pages_and_can_resume_after_a_leaf_is_released() {
        let store = Pages::default();
        let memory = MemoryBudget::new(16 << 20);
        let (root, _) = apply_with_generation(
            &store,
            None,
            (0..MAX_LEAF_ROWS + 1).map(put).collect(),
            &NoSummary,
            &memory,
            Some(1),
        )
        .await
        .unwrap();
        let root = root.unwrap();
        let (next, _) = apply_with_generation(
            &store,
            Some(&root),
            vec![Edit::Put(Row {
                key: key(0),
                value: key(42),
            })],
            &NoSummary,
            &memory,
            Some(2),
        )
        .await
        .unwrap();
        let next = next.unwrap();
        let held = reachable(&store, &next)
            .await
            .unwrap()
            .into_iter()
            .collect();
        let order = retirement_order(&store, &root, &held).await.unwrap();
        assert_eq!(order.len(), 2);
        assert_eq!(*order.last().unwrap(), root.digest);
        store.0.lock().unwrap().remove(&order[0]);
        assert!(reachable(&store, &root).await.is_err());
        assert_eq!(
            retirement_order(&store, &root, &held).await.unwrap(),
            [root.digest]
        );
        assert_eq!(reachable(&store, &next).await.unwrap().len(), 3);
    }
}
