use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use bytes::Bytes;
use parking_lot::Mutex;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_storage::{IoRegion, ObjectKey, cache::BufferPool, object_store::IndexPageScope};
use rtdb_types::{Iid, Interval, Result, Timestamp};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::{
    correction, format_error, ipc,
    key::{Atom, AtomRef, Bucket},
    maintenance::cancelled,
    postings::{KeyRow, Postings, Tuple},
    references, seek,
};

const FANOUT: usize = 64;
const PAGE_CACHE_LIMIT: usize = 8 << 20;
type Work<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Address {
    pub iid: Iid,
    pub key: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct AddressRef<'a> {
    pub iid: Iid,
    pub key: &'a [u8],
}

impl AddressRef<'_> {
    pub fn compare(self, other: Self) -> Ordering {
        self.key.cmp(other.key).then_with(|| {
            if self.key.is_empty() {
                self.iid.cmp(&other.iid)
            } else {
                Ordering::Equal
            }
        })
    }
    pub fn owned(self) -> Address {
        Address {
            iid: self.iid,
            key: self.key.to_vec(),
        }
    }
}

impl Address {
    pub fn view(&self) -> AddressRef<'_> {
        AddressRef {
            iid: self.iid,
            key: &self.key,
        }
    }
}

impl PartialEq for Address {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Address {}
impl PartialOrd for Address {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Address {
    fn cmp(&self, other: &Self) -> Ordering {
        self.view().compare(other.view())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) enum Key {
    Posting {
        bucket: Bucket,
        tuple: KeyRow,
        address: Address,
    },
    Row(Address),
    Revision(Timestamp),
}

impl Key {
    pub(super) fn view(&self) -> KeyRef<'_> {
        match self {
            Self::Posting {
                bucket,
                tuple,
                address,
            } => KeyRef::Posting(*bucket, tuple.view(), address.view()),
            Self::Row(address) => KeyRef::Row(address.view()),
            Self::Revision(time) => KeyRef::Revision(*time),
        }
    }

    fn bytes(&self) -> usize {
        size_of::<Self>()
            + match self {
                Self::Posting { tuple, address, .. } => tuple.bytes() + address.key.len(),
                Self::Row(address) => address.key.len(),
                Self::Revision(_) => 0,
            }
    }
}

#[derive(Clone, Copy)]
pub(super) enum KeyRef<'a> {
    Posting(Bucket, Tuple<'a>, AddressRef<'a>),
    Row(AddressRef<'a>),
    Revision(Timestamp),
}

impl KeyRef<'_> {
    fn compare(self, other: KeyRef<'_>) -> Ordering {
        use KeyRef::*;
        match (self, other) {
            (Posting(bucket, tuple, iid), Posting(other_bucket, other_tuple, other_iid)) => bucket
                .cmp(&other_bucket)
                .then_with(|| tuple.compare(other_tuple))
                .then_with(|| iid.compare(other_iid)),
            (Row(address), Row(other)) => address.compare(other),
            (Revision(time), Revision(other)) => time.cmp(&other),
            (Posting(..), _) | (Row(_), Revision(_)) => Ordering::Less,
            _ => Ordering::Greater,
        }
    }

    fn owned(self) -> Key {
        match self {
            Self::Posting(bucket, tuple, iid) => Key::Posting {
                bucket,
                address: iid.owned(),
                tuple: KeyRow::Fence(tuple.iter().map(|atom| atom.owned()).collect()),
            },
            Self::Row(address) => Key::Row(address.owned()),
            Self::Revision(time) => Key::Revision(time),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Span {
    pub valid: Interval,
    pub tuple: KeyRow,
}

#[derive(Clone, Debug)]
pub(super) enum Item {
    Posting,
    Row(Arc<[Span]>),
    Revision(Option<Arc<Pointer>>),
}

static POSTING_ITEM: Item = Item::Posting;

#[derive(Clone, Debug)]
pub(super) struct Entry {
    pub key: Key,
    pub item: Item,
}

#[derive(Clone, Copy)]
struct EntryView<'a> {
    key: KeyRef<'a>,
    item: &'a Item,
}

impl EntryView<'_> {
    fn bytes(self) -> usize {
        let key = size_of::<Key>()
            + match self.key {
                KeyRef::Posting(_, tuple, address) => {
                    address.key.len()
                        + tuple
                            .iter()
                            .map(|cell| size_of::<Atom>() + cell.retained_bytes())
                            .sum::<usize>()
                }
                KeyRef::Row(address) => address.key.len(),
                KeyRef::Revision(_) => 0,
            };
        key + match self.item {
            Item::Posting | Item::Revision(None) => 0,
            Item::Row(spans) => spans
                .iter()
                .map(|span| size_of::<Span>() + span.tuple.bytes())
                .sum(),
            Item::Revision(Some(pointer)) => pointer.first.bytes() + pointer.last.bytes() + 128,
        }
    }

    fn owned(self) -> Entry {
        Entry {
            key: self.key.owned(),
            item: self.item.clone(),
        }
    }
}

impl Entry {
    fn view(&self) -> EntryView<'_> {
        EntryView {
            key: self.key.view(),
            item: &self.item,
        }
    }

    fn references_at_or_before(&self, generation: i64) -> bool {
        match &self.item {
            Item::Revision(Some(root)) => root.exists_by(generation),
            Item::Revision(None) | Item::Posting | Item::Row(_) => true,
        }
    }

    fn is_typed(&self) -> bool {
        matches!(
            (&self.key, &self.item),
            (Key::Posting { .. }, Item::Posting)
                | (Key::Row(_), Item::Row(_))
                | (Key::Revision(_), Item::Revision(_))
        )
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum MeasureDomain {
    #[default]
    Empty,
    Int64,
    Decimal128(u8, i8),
    Mixed,
}

impl MeasureDomain {
    fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Empty, domain) | (domain, Self::Empty) => domain,
            (left, right) if left == right => left,
            _ => Self::Mixed,
        }
    }
}

/// Exact per-subtree coefficient weight for a single declared domain.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Measure {
    pub count: u64,
    pub(super) domain: MeasureDomain,
    pub(super) sum: [u8; 32],
}

impl Measure {
    fn sum(self) -> arrow_buffer::i256 { arrow_buffer::i256::from_be_bytes(self.sum) }

    pub fn coefficient(self, expected: MeasureDomain) -> Result<arrow_buffer::i256> {
        if self.domain == expected || (self.count == 0 && self.domain == MeasureDomain::Empty) {
            Ok(self.sum())
        } else {
            Err(super::uncovered_owned_range())
        }
    }

    pub fn checked_add(self, other: Self) -> Result<Self> {
        let domain = self.domain.combine(other.domain);
        let count = self.count.checked_add(other.count).ok_or_else(cardinality_overflow)?;
        let sum = if domain == MeasureDomain::Mixed {
            [0; 32]
        } else {
            self.sum().checked_add(other.sum()).ok_or_else(cardinality_overflow)?.to_be_bytes()
        };
        Ok(Self { count, domain, sum })
    }

    pub fn checked_sub(self, other: Self) -> Result<Self> {
        let domain = self.domain.combine(other.domain);
        let count = self.count.checked_sub(other.count).ok_or_else(cardinality_overflow)?;
        let sum = if domain == MeasureDomain::Mixed {
            [0; 32]
        } else {
            self.sum().checked_sub(other.sum()).ok_or_else(cardinality_overflow)?.to_be_bytes()
        };
        Ok(Self { count, domain, sum })
    }

    fn at(key: KeyRef<'_>) -> Self {
        let KeyRef::Posting(_, tuple, _) = key else { return Self::default(); };
        let Some(last) = tuple.len().checked_sub(2) else { return Self::default(); };
        let (domain, value) = match tuple.get(last) {
            Some(AtomRef::Integer(value)) if i64::try_from(value).is_ok() => (MeasureDomain::Int64, value),
            Some(AtomRef::Decimal128(precision, scale, value)) => (MeasureDomain::Decimal128(precision, scale), value),
            _ => return Self::default(),
        };
        Self { count: 1, domain, sum: arrow_buffer::i256::from_i128(value).to_be_bytes() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Pointer {
    pub digest: String,
    pub(super) generation: i64,
    pub(super) size: usize,
    pub(super) entries: u64,
    pub(super) measure: Measure,
    pub(super) first: Key,
    pub(super) last: Key,
}

impl Pointer {
    pub(super) fn exists_by(&self, block: i64) -> bool {
        self.entries > 0 && (0..=block).contains(&self.generation)
    }
}

#[derive(Clone, Debug)]
pub(super) enum Link {
    Stored(Arc<Pointer>),
    New(Arc<Node>),
}

impl Link {
    fn first(&self) -> &Key {
        match self {
            Self::Stored(pointer) => &pointer.first,
            Self::New(node) => node.page.first(),
        }
    }

    fn entries(&self) -> u64 {
        match self {
            Self::Stored(pointer) => pointer.entries,
            Self::New(node) => node.entries,
        }
    }

    fn measure(&self) -> Measure {
        match self {
            Self::Stored(pointer) => pointer.measure,
            Self::New(node) => node.measure,
        }
    }

    fn last(&self) -> &Key {
        match self {
            Self::Stored(pointer) => &pointer.last,
            Self::New(node) => node.page.last(),
        }
    }
}

#[derive(Debug)]
pub(super) struct Node {
    page: Page,
    entries: u64,
    measure: Measure,
    _memory: Arc<MemoryReservation>,
}

#[derive(Debug)]
enum Page {
    Leaf(Leaf),
    Branch(Vec<Link>),
}

impl Page {
    fn first(&self) -> &Key {
        match self {
            Self::Leaf(leaf) => &leaf.first,
            Self::Branch(children) => children[0].first(),
        }
    }

    fn last(&self) -> &Key {
        match self {
            Self::Leaf(leaf) => &leaf.last,
            Self::Branch(children) => children[children.len() - 1].last(),
        }
    }

    fn entry_count(&self) -> Result<u64> {
        match self {
            Self::Leaf(leaf) => Ok(leaf.len() as u64),
            Self::Branch(children) => children.iter().try_fold(0u64, |sum, child| {
                sum.checked_add(child.entries())
                    .ok_or_else(cardinality_overflow)
            }),
        }
    }

    fn measure(&self) -> Result<Measure> {
        match self {
            Self::Leaf(leaf) => leaf.measure(leaf.posting_count()),
            Self::Branch(children) => children.iter().try_fold(
                Measure::default(), |total, child| total.checked_add(child.measure()),
            ),
        }
    }

    fn bytes(&self) -> usize {
        match self {
            Self::Leaf(leaf) => leaf.bytes(),
            Self::Branch(children) => children
                .iter()
                .map(|child| child.first().bytes() + child.last().bytes() + 128)
                .sum(),
        }
    }
}

#[derive(Debug)]
struct Leaf {
    postings: Option<Postings>,
    metadata: Vec<Entry>,
    first: Key,
    last: Key,
}

impl Leaf {
    fn from_entries(entries: Vec<EntryView<'_>>) -> Result<Self> {
        let count = entries.partition_point(|entry| matches!(entry.key, KeyRef::Posting(..)));
        let (keys, metadata) = entries.split_at(count);
        let postings = if keys.is_empty() {
            None
        } else {
            let keys = keys
                .iter()
                .map(|entry| match entry.key {
                    KeyRef::Posting(bucket, tuple, iid) if matches!(entry.item, Item::Posting) => {
                        Ok((bucket, tuple, iid))
                    }
                    _ => Err(format_error("Invalid ordered index posting")),
                })
                .collect::<Result<Vec<_>>>()?;
            Some(Postings::build(&keys)?)
        };
        Self::new(
            postings,
            metadata.iter().map(|entry| entry.owned()).collect(),
        )
    }

    fn new(postings: Option<Postings>, metadata: Vec<Entry>) -> Result<Self> {
        let mut leaf = Self {
            postings,
            metadata,
            first: Key::Revision(Timestamp::MIN),
            last: Key::Revision(Timestamp::MIN),
        };
        if leaf.len() == 0
            || leaf.len() > FANOUT
            || leaf
                .metadata
                .iter()
                .any(|entry| !entry.is_typed() || matches!(entry.key, Key::Posting { .. }))
            || (1..leaf.len()).any(|row| leaf.key(row - 1).compare(leaf.key(row)) != Ordering::Less)
        {
            return Err(format_error("Invalid ordered index leaf"));
        }
        leaf.first = leaf.key(0).owned();
        leaf.last = leaf.key(leaf.len() - 1).owned();
        Ok(leaf)
    }

    fn posting_count(&self) -> usize {
        self.postings.as_ref().map_or(0, Postings::len)
    }

    fn len(&self) -> usize {
        self.posting_count() + self.metadata.len()
    }

    fn key(&self, row: usize) -> KeyRef<'_> {
        if let Some(postings) = &self.postings
            && row < postings.len()
        {
            let (bucket, tuple, iid) = postings.at(row);
            KeyRef::Posting(bucket, tuple, iid)
        } else {
            self.metadata[row - self.posting_count()].key.view()
        }
    }

    fn measure(&self, entries: usize) -> Result<Measure> {
        (0..entries.min(self.posting_count())).try_fold(
            Measure::default(), |total, slot| total.checked_add(Measure::at(self.key(slot))),
        )
    }

    fn entry(&self, row: usize) -> EntryView<'_> {
        if row < self.posting_count() {
            EntryView {
                key: self.key(row),
                item: &POSTING_ITEM,
            }
        } else {
            self.metadata[row - self.posting_count()].view()
        }
    }

    fn bytes(&self) -> usize {
        let metadata = self
            .metadata
            .iter()
            .map(|entry| match &entry.item {
                Item::Row(spans) => {
                    size_of::<Entry>() + spans.len().saturating_mul(size_of::<Span>())
                }
                _ => entry.view().bytes(),
            })
            .sum();
        self.postings
            .as_ref()
            .map_or(0, Postings::bytes)
            .saturating_add(metadata)
            .saturating_add(self.first.bytes())
            .saturating_add(self.last.bytes())
    }

    fn encoding_bytes(&self) -> usize {
        self.postings
            .as_ref()
            .map_or(0, Postings::bytes)
            .saturating_add(self.metadata.iter().map(|entry| entry.view().bytes()).sum())
    }
}

const ARROW_LEAF_HEADER: [u8; 16] = {
    let mut header = [0; 16];
    header[0] = super::PAGE_LAYOUT_VERSION;
    header
};

#[derive(Serialize, Deserialize)]
enum StoredPage {
    Revisions(Vec<(Timestamp, Option<Arc<Pointer>>)>),
    Branch(Vec<Arc<Pointer>>),
}

pub(super) type Root = Option<Link>;

#[derive(Default)]
struct DecodedPages {
    bytes: usize,
    entries: BTreeMap<ObjectKey, Arc<Node>>,
}

#[derive(Default)]
pub(super) struct ReachablePages {
    pub keys: BTreeSet<ObjectKey>,
    pub bytes: u64,
    allocations: Vec<Arc<MemoryReservation>>,
}

pub(super) struct Tree {
    pub storage: Arc<BufferPool>,
    pub directory: ObjectKey,
    pub scope: u64,
    pub memory: Arc<MemoryBudget>,
    pub arrow_references: bool,
    decoded: Mutex<DecodedPages>,
    // Drop this after the decoded cache.
    permit: Option<Arc<dyn Send + Sync>>,
}

impl Tree {
    pub async fn new(
        storage: &Arc<BufferPool>,
        directory: &ObjectKey,
        memory: &Arc<MemoryBudget>,
        scope: u64,
    ) -> Result<Self> {
        let permit = match storage.object_store().index_page_lifecycle() {
            Some(lifecycle) => Some(
                lifecycle
                    .retain(IndexPageScope {
                        index: directory,
                        generation: scope,
                    })
                    .await?,
            ),
            None => None,
        };
        Ok(Self {
            storage: storage.clone(),
            directory: directory.clone(),
            scope,
            memory: memory.clone(),
            arrow_references: false,
            decoded: Mutex::default(),
            permit,
        })
    }

    pub fn shares_result_buffers(&self) -> bool {
        self.permit.is_none() || rtdb_query::encoding_ordered_batch()
    }

    pub fn retain_result_buffers(
        &self,
        reservation: &Arc<MemoryReservation>,
    ) -> Arc<dyn Send + Sync> {
        match &self.permit {
            Some(permit) => Arc::new((reservation.clone(), permit.clone())),
            None => reservation.clone(),
        }
    }

    pub fn page_scope(&self) -> IndexPageScope<'_> {
        IndexPageScope {
            index: &self.directory,
            generation: self.scope,
        }
    }

    async fn admitted<T>(&self, work: impl Future<Output = T>) -> T {
        match &self.permit {
            Some(permit) => IoRegion::retaining(permit.clone(), work).await,
            None => work.await,
        }
    }

    pub fn pages(&self) -> Result<ObjectKey> {
        Ok(ObjectKey::new(format!(
            "{}/generations/{:016x}/pages",
            self.directory, self.scope
        ))?)
    }

    fn page_key(&self, generation: i64, digest: &str) -> Result<ObjectKey> {
        if generation < 0
            || digest.len() != 64
            || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(format_error("Invalid ordered index page address"));
        }
        Ok(ObjectKey::new(format!(
            "{}/generations/{:016x}/pages/{generation:016x}/{digest}",
            self.directory, self.scope,
        ))?)
    }

    fn leaf_link(&self, entries: Vec<EntryView<'_>>) -> Result<Link> {
        let bytes = entries.iter().map(|entry| entry.bytes()).sum::<usize>();
        let width = match entries.first().map(|entry| entry.key) {
            Some(KeyRef::Posting(_, tuple, _)) => tuple.len(),
            _ => 0,
        };
        let _scratch = self.memory.reserve(
            bytes
                .saturating_mul(4)
                .saturating_add(width.saturating_mul(1024))
                .saturating_add(4096),
        )?;
        self.link(Page::Leaf(Leaf::from_entries(entries)?))
    }

    fn link(&self, page: Page) -> Result<Link> {
        let entries = page.entry_count()?;
        let measure = page.measure()?;
        let memory = self
            .memory
            .reserve(page.bytes().saturating_mul(2).saturating_add(256))?;
        Ok(Link::New(Arc::new(Node {
            page,
            entries,
            measure,
            _memory: memory,
        })))
    }

    async fn open(&self, link: &Link) -> Result<Arc<Node>> {
        let pointer = match link {
            Link::New(node) => return Ok(Arc::clone(node)),
            Link::Stored(pointer) => pointer,
        };
        let object = self.page_key(pointer.generation, &pointer.digest)?;
        let cached = self.decoded.lock().entries.get(&object).cloned();
        if let Some(node) = cached {
            if node.entries != pointer.entries
                || node.measure != pointer.measure
                || node.page.first() != &pointer.first
                || node.page.last() != &pointer.last
            {
                return Err(format_error(
                    "Ordered index page count or fences differ from its reference",
                ));
            }
            return Ok(node);
        }
        let bound = pointer.size.saturating_mul(8).saturating_add(4096);
        let mut memory = self.memory.reserve(bound)?;
        // Derived index pages are rebuildable, and must never make a durable
        // replica-local copy outside the object owner's erasure inventory.
        let store = self.storage.object_store();
        let bytes = IoRegion::retaining(
            memory.clone(),
            self.admitted(store.get_fresh_exact(&object, pointer.size)),
        )
        .await?;
        if bytes.len() != pointer.size
            || format!("{:x}", Sha256::digest(bytes.as_ref())) != pointer.digest
        {
            return Err(format_error("Ordered index page checksum mismatch"));
        }
        let page = decode(bytes, pointer.generation, &memory)?;
        let entries = page.entry_count()?;
        let measure = page.measure()?;
        if entries != pointer.entries
            || measure != pointer.measure
            || page.first() != &pointer.first
            || page.last() != &pointer.last
        {
            return Err(format_error(
                "Ordered index page count or fences differ from its reference",
            ));
        }
        let retained = page.bytes().saturating_add(256);
        if retained > bound {
            return Err(format_error(
                "Ordered index page exceeded its memory admission",
            ));
        }
        if let Some(memory) = Arc::get_mut(&mut memory) {
            memory.shrink_to(retained)?;
        }
        let retained = memory.bytes();
        let node = Arc::new(Node {
            page,
            entries,
            measure,
            _memory: memory,
        });
        // The read or maintenance operation owns the decoded pages. An idle
        // replica has no independently retained copy after it releases the tree.
        let mut decoded = self.decoded.lock();
        let limit = PAGE_CACHE_LIMIT.min(self.memory.limit_bytes() / 16);
        if retained <= limit.saturating_sub(decoded.bytes) && !decoded.entries.contains_key(&object)
        {
            decoded.entries.insert(object, Arc::clone(&node));
            decoded.bytes += retained;
        }
        Ok(node)
    }

    /// Counts the monotone prefix using the immutable cardinalities of
    /// children entirely before the boundary.
    #[cfg(test)]
    pub async fn rank(
        &self,
        root: &Root,
        before: impl for<'key> Fn(KeyRef<'key>) -> bool,
        cancellation: &CancellationToken,
    ) -> Result<u64> {
        self.rank_and_measure(root, before, cancellation, false).await.map(|(rank, _)| rank)
    }

    pub async fn rank_and_measure(
        &self,
        root: &Root,
        before: impl for<'key> Fn(KeyRef<'key>) -> bool,
        cancellation: &CancellationToken,
        measured: bool,
    ) -> Result<(u64, Measure)> {
        let mut measure = Measure::default();
        let Some(mut link) = root.clone() else {
            return Ok((0, measure));
        };
        let mut rank = 0u64;
        loop {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let node = self.open(&link).await?;
            match &node.page {
                Page::Leaf(leaf) => {
                    let prefix = seek::partition(leaf.len(), |slot| before(leaf.key(slot)));
                    if measured {
                        measure = measure.checked_add(leaf.measure(prefix)?)?;
                    }
                    return Ok((
                        rank.checked_add(prefix as u64).ok_or_else(cardinality_overflow)?,
                        measure,
                    ));
                }
                Page::Branch(children) => {
                    let prefix = seek::partition(children.len(), |slot| {
                        before(children[slot].last().view())
                    });
                    for child in &children[..prefix] {
                        if measured {
                            measure = measure.checked_add(child.measure())?;
                        }
                        rank = rank
                            .checked_add(child.entries())
                            .ok_or_else(cardinality_overflow)?;
                    }
                    let Some(child) = children.get(prefix) else {
                        return Ok((rank, measure));
                    };
                    if !before(child.first().view()) {
                        return Ok((rank, measure));
                    }
                    link = child.clone();
                }
            }
        }
    }

    pub async fn spans(&self, root: &Root, address: &Address) -> Result<Arc<[Span]>> {
        let key = Key::Row(address.clone());
        let mut link = root.clone();
        while let Some(page) = link.take() {
            if &key < page.first() || &key > page.last() {
                break;
            }
            let node = self.open(&page).await?;
            match &node.page {
                Page::Leaf(leaf) => {
                    if let Ok(slot) = leaf.metadata.binary_search_by(|entry| entry.key.cmp(&key)) {
                        let Item::Row(spans) = &leaf.metadata[slot].item else {
                            return Err(format_error("Ordered index row has another item type"));
                        };
                        return Ok(Arc::clone(spans));
                    }
                    break;
                }
                Page::Branch(children) => {
                    let slot = children.partition_point(|child| child.last() < &key);
                    link = children.get(slot).cloned();
                }
            }
        }
        Ok(Arc::from([]))
    }

    pub async fn edit(&self, root: Root, edits: Vec<(Key, Option<Item>)>) -> Result<Root> {
        if edits.is_empty() {
            return Ok(root);
        }
        let mut pages = match root {
            Some(root) => self.edit_page(root, &edits).await?,
            None => self.pack(
                edits
                    .iter()
                    .filter_map(|(key, item)| {
                        item.as_ref().map(|item| EntryView {
                            key: key.view(),
                            item,
                        })
                    })
                    .collect(),
                |entries| self.leaf_link(entries),
            )?,
        };
        while pages.len() > 1 {
            pages = self.pack(pages, |children| self.link(Page::Branch(children)))?;
        }
        let Some(mut root) = pages.pop() else {
            return Ok(None);
        };
        loop {
            let Link::New(node) = &root else {
                break;
            };
            let Page::Branch(children) = &node.page else {
                break;
            };
            let [child] = children.as_slice() else {
                break;
            };
            root = child.clone();
        }
        Ok(Some(root))
    }

    fn edit_page<'a>(
        &'a self,
        link: Link,
        edits: &'a [(Key, Option<Item>)],
    ) -> Work<'a, Vec<Link>> {
        Box::pin(async move {
            let node = self.open(&link).await?;
            match &node.page {
                Page::Leaf(leaf) => {
                    let _scratch = self
                        .memory
                        .reserve(leaf.bytes().saturating_mul(2).saturating_add(4096))?;
                    let mut changed = Vec::with_capacity(leaf.len() + edits.len());
                    let mut row = 0;
                    for (key, item) in edits {
                        while row < leaf.len()
                            && leaf.key(row).compare(key.view()) == Ordering::Less
                        {
                            changed.push(leaf.entry(row));
                            row += 1;
                        }
                        if row < leaf.len() && leaf.key(row).compare(key.view()) == Ordering::Equal
                        {
                            row += 1;
                        }
                        if let Some(item) = item {
                            changed.push(EntryView {
                                key: key.view(),
                                item,
                            });
                        }
                    }
                    changed.extend((row..leaf.len()).map(|row| leaf.entry(row)));
                    self.pack(changed, |entries| self.leaf_link(entries))
                }
                Page::Branch(children) => {
                    let mut changed = Vec::with_capacity(children.len());
                    let mut remaining = edits;
                    for (index, child) in children.iter().enumerate() {
                        let count = if index + 1 == children.len() {
                            remaining.len()
                        } else {
                            remaining.partition_point(|(key, _)| key <= child.last())
                        };
                        let (here, rest) = remaining.split_at(count);
                        remaining = rest;
                        if here.is_empty() {
                            changed.push(child.clone());
                        } else {
                            changed.extend(self.edit_page(child.clone(), here).await?);
                        }
                    }
                    self.pack(self.merge_sparse(changed).await?, |children| {
                        self.link(Page::Branch(children))
                    })
                }
            }
        })
    }

    async fn merge_sparse(&self, children: Vec<Link>) -> Result<Vec<Link>> {
        let mut packed: Vec<Link> = Vec::with_capacity(children.len());
        for child in children {
            if let Some(left) = packed.last()
                && (sparse(left) || sparse(&child))
            {
                let (left, right) = (self.open(left).await?, self.open(&child).await?);
                let page = match (&left.page, &right.page) {
                    (Page::Leaf(left), Page::Leaf(right)) if left.len() + right.len() <= FANOUT => {
                        let _scratch = self.memory.reserve(
                            left.bytes()
                                .saturating_add(right.bytes())
                                .saturating_mul(2)
                                .saturating_add(4096),
                        )?;
                        Some(
                            self.leaf_link(
                                (0..left.len())
                                    .map(|row| left.entry(row))
                                    .chain((0..right.len()).map(|row| right.entry(row)))
                                    .collect(),
                            )?,
                        )
                    }
                    (Page::Branch(left), Page::Branch(right))
                        if left.len() + right.len() <= FANOUT =>
                    {
                        Some(self.link(Page::Branch(left.iter().chain(right).cloned().collect()))?)
                    }
                    _ => None,
                };
                if let Some(page) = page {
                    let last = packed.last_mut().expect("a sibling was read");
                    *last = page;
                    continue;
                }
            }
            packed.push(child);
        }
        Ok(packed)
    }

    fn pack<T>(&self, slots: Vec<T>, link: impl Fn(Vec<T>) -> Result<Link>) -> Result<Vec<Link>> {
        if slots.is_empty() {
            return Ok(Vec::new());
        }
        if slots.len() <= FANOUT {
            return Ok(vec![link(slots)?]);
        }
        let num_pages = slots.len().div_ceil(FANOUT);
        let mut slots = slots.into_iter();
        let mut links = Vec::with_capacity(num_pages);
        for remaining in (1..=num_pages).rev() {
            let size = slots.len().div_ceil(remaining);
            links.push(link(slots.by_ref().take(size).collect())?);
        }
        Ok(links)
    }

    pub fn persist<'a>(
        &'a self,
        link: &'a Link,
        generation: i64,
        written: &'a mut BTreeMap<usize, Arc<Pointer>>,
        cancellation: &'a CancellationToken,
    ) -> Work<'a, Arc<Pointer>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let node = match link {
                Link::Stored(pointer) if pointer.exists_by(generation) => {
                    return Ok(Arc::clone(pointer));
                }
                Link::Stored(_) => {
                    return Err(format_error("Ordered index page references a later block"));
                }
                Link::New(node) => node,
            };
            let identity = Arc::as_ptr(node) as usize;
            if let Some(pointer) = written.get(&identity) {
                return Ok(Arc::clone(pointer));
            }
            let bytes = match &node.page {
                Page::Leaf(leaf) => leaf.encoding_bytes(),
                Page::Branch(_) => node.page.bytes(),
            };
            let _scratch = self
                .memory
                .reserve(bytes.saturating_mul(8).saturating_add(4096))?;
            let encoded = match &node.page {
                Page::Leaf(leaf) => {
                    if !leaf
                        .metadata
                        .iter()
                        .all(|entry| entry.references_at_or_before(generation))
                    {
                        return Err(format_error(
                            "Ordered index revision refers to a later block",
                        ));
                    }
                    if let Some(postings) = &leaf.postings {
                        postings.encode(&ARROW_LEAF_HEADER, &correction::encode(&leaf.metadata)?)?
                    } else if matches!(leaf.metadata[0].key, Key::Row(_)) {
                        correction::encode(&leaf.metadata)?
                    } else {
                        let revisions = leaf
                            .metadata
                            .iter()
                            .map(|entry| match (&entry.key, &entry.item) {
                                (Key::Revision(time), Item::Revision(root)) => Ok((*time, root.clone())),
                                _ => Err(format_error("Ordered index revision leaf contains other entries")),
                            })
                            .collect::<Result<Vec<_>>>()?;
                        if self.arrow_references {
                            let references = revisions.iter().map(|(time, root)| (Some(*time), root.as_deref())).collect::<Vec<_>>();
                            ipc::encode(&references::encode(&references)?)?
                        } else {
                            serde_json::to_vec(&StoredPage::Revisions(revisions)).map_err(format_error)?
                        }
                    }
                }
                Page::Branch(children) => {
                    let mut pointers = Vec::with_capacity(children.len());
                    for child in children {
                        pointers.push(
                            self.persist(child, generation, written, cancellation)
                                .await?,
                        );
                    }
                    if self.arrow_references {
                        let references = pointers.iter().map(|pointer| (None, Some(pointer.as_ref()))).collect::<Vec<_>>();
                        ipc::encode(&references::encode(&references)?)?
                    } else {
                        serde_json::to_vec(&StoredPage::Branch(pointers)).map_err(format_error)?
                    }
                }
            };
            let digest = format!("{:x}", Sha256::digest(&encoded));
            let size = encoded.len();
            let object = self.page_key(generation, &digest)?;
            self.admitted(self.storage.object_store().put(&object, encoded.into()))
                .await?;
            let pointer = Arc::new(Pointer {
                digest,
                generation,
                size,
                entries: node.entries,
                measure: node.measure,
                first: link.first().clone(),
                last: link.last().clone(),
            });
            written.insert(identity, Arc::clone(&pointer));
            Ok(pointer)
        })
    }

    pub fn mark<'a>(
        &'a self,
        pointer: Arc<Pointer>,
        reachable: &'a mut ReachablePages,
        cancellation: &'a CancellationToken,
    ) -> Work<'a, ()> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let key = self.page_key(pointer.generation, &pointer.digest)?;
            if reachable.keys.contains(&key) {
                return Ok(());
            }
            let reservation = self
                .memory
                .reserve(key.as_str().len().saturating_add(512))?;
            reachable.keys.insert(key);
            reachable.allocations.push(reservation);
            reachable.bytes = reachable
                .bytes
                .checked_add(pointer.size as u64)
                .ok_or_else(|| format_error("Ordered index file bytes overflow"))?;
            let node = self.open(&Link::Stored(pointer)).await?;
            match &node.page {
                Page::Branch(children) => {
                    for child in children {
                        let Link::Stored(pointer) = child else {
                            return Err(format_error(
                                "A stored page contains an unpublished child",
                            ));
                        };
                        self.mark(Arc::clone(pointer), reachable, cancellation)
                            .await?;
                    }
                }
                Page::Leaf(leaf) => {
                    for entry in &leaf.metadata {
                        if let Item::Revision(Some(pointer)) = &entry.item {
                            self.mark(Arc::clone(pointer), reachable, cancellation)
                                .await?;
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

fn decode(bytes: Bytes, generation: i64, memory: &Arc<MemoryReservation>) -> Result<Page> {
    let (postings, metadata) = if bytes.starts_with(&ARROW_LEAF_HEADER) {
        let (postings, suffix) = Postings::decode(bytes.slice(ARROW_LEAF_HEADER.len()..), FANOUT)?;
        let metadata = if suffix.is_empty() {
            Vec::new()
        } else {
            correction::decode(suffix, FANOUT - postings.len(), memory)?
        };
        (Some(postings), metadata)
    } else if bytes.starts_with(&correction::HEADER) {
        (None, correction::decode(bytes, FANOUT, memory)?)
    } else if bytes.starts_with(&ipc::CONTINUATION) {
        let parents = references::decode(ipc::decode(bytes, FANOUT * 2)?, FANOUT, memory)?;
        if parents.first().is_some_and(|(time, _)| time.is_some()) {
            (
                None,
                parents
                    .into_iter()
                    .map(|(time, root)| {
                        Ok(Entry {
                            key: Key::Revision(
                                time.ok_or_else(|| {
                                    format_error("Revision entry omitted its time")
                                })?,
                            ),
                            item: Item::Revision(root),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
        } else {
            let children = parents
                .into_iter()
                .map(|(time, root)| match (time, root) {
                    (None, Some(child))
                        if child.exists_by(generation) && child.first <= child.last =>
                    {
                        Ok(child)
                    }
                    _ => Err(format_error("Invalid ordered index branch reference")),
                })
                .collect::<Result<Vec<_>>>()?;
            if children.is_empty()
                || children
                    .windows(2)
                    .any(|pair| pair[0].last >= pair[1].first)
            {
                return Err(format_error("Invalid ordered index branch order"));
            }
            return Ok(Page::Branch(
                children.into_iter().map(Link::Stored).collect(),
            ));
        }
    } else {
        match serde_json::from_slice(&bytes).map_err(format_error)? {
            StoredPage::Revisions(revisions) => (
                None,
                revisions
                    .into_iter()
                    .map(|(time, root)| Entry {
                        key: Key::Revision(time),
                        item: Item::Revision(root),
                    })
                    .collect(),
            ),
            StoredPage::Branch(children)
                if !children.is_empty()
                    && children.len() <= FANOUT
                    && children
                        .iter()
                        .all(|child| child.exists_by(generation) && child.first <= child.last)
                    && children.windows(2).all(|pair| pair[0].last < pair[1].first) =>
            {
                return Ok(Page::Branch(
                    children.into_iter().map(Link::Stored).collect(),
                ));
            }
            _ => return Err(format_error("Invalid ordered index branch")),
        }
    };
    if metadata
        .iter()
        .any(|entry| !entry.references_at_or_before(generation))
    {
        return Err(format_error(
            "Ordered index revision refers to a later block",
        ));
    }
    Ok(Page::Leaf(Leaf::new(postings, metadata)?))
}

pub(super) fn cardinality_overflow() -> rtdb_types::Anomaly {
    format_error("Ordered index entry count overflow")
}

fn sparse(link: &Link) -> bool {
    let Link::New(node) = link else {
        return false;
    };
    match &node.page {
        Page::Leaf(leaf) => leaf.len() <= FANOUT / 4,
        Page::Branch(children) => children.len() <= FANOUT / 4,
    }
}

/// A directed tree walk starting at the first leaf whose fence can meet the seek.
pub(super) struct Cursor {
    pages: Vec<(Arc<Node>, usize)>,
    root: Root,
    reverse: bool,
}

#[derive(Clone)]
pub(super) struct Hit {
    node: Arc<Node>,
    row: usize,
}

impl Hit {
    fn leaf(&self) -> &Leaf {
        let Page::Leaf(leaf) = &self.node.page else {
            unreachable!("only leaves produce hits")
        };
        leaf
    }

    pub fn key(&self) -> KeyRef<'_> {
        self.leaf().key(self.row)
    }

    pub fn follows(&self, first: &Self, distance: usize) -> bool {
        Arc::ptr_eq(&self.node, &first.node) && first.row.checked_add(distance) == Some(self.row)
    }

    pub fn plain_column(
        &self,
        slot: usize,
        length: usize,
    ) -> Option<(&arrow_array::ArrayRef, usize, &Arc<MemoryReservation>)> {
        let postings = self.leaf().postings.as_ref()?;
        if self.row.checked_add(length)? > postings.len() {
            return None;
        }
        Some((postings.plain_column(slot)?, self.row, &self.node._memory))
    }

    pub fn into_entry(self) -> Entry {
        self.leaf().entry(self.row).owned()
    }
}

impl Cursor {
    pub fn new(root: Root, reverse: bool) -> Self {
        Self {
            pages: Vec::new(),
            root,
            reverse,
        }
    }

    pub async fn next(
        &mut self,
        tree: &Tree,
        position: impl Fn(KeyRef<'_>) -> Ordering,
    ) -> Result<Option<Hit>> {
        let before = if self.reverse {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        let after = before.reverse();
        if let Some(root) = self.root.take()
            && self.descend(tree, root, &position, before).await?.is_none()
        {
            return Ok(None);
        }
        while let Some((node, offset)) = self.pages.last_mut() {
            let size = match &node.page {
                Page::Leaf(leaf) => leaf.len(),
                Page::Branch(children) => children.len(),
            };
            let Some(slot) = seek::slot(size, *offset, self.reverse) else {
                self.pages.pop();
                continue;
            };
            *offset += 1;
            match &node.page {
                Page::Leaf(leaf) => {
                    let found = position(leaf.key(slot));
                    if found == after {
                        self.pages.clear();
                        return Ok(None);
                    }
                    if found == before {
                        return Err(format_error("Ordered index seek lost its lower boundary"));
                    }
                    return Ok(Some(Hit {
                        node: Arc::clone(node),
                        row: slot,
                    }));
                }
                Page::Branch(children) => {
                    let child = children[slot].clone();
                    if position(
                        if self.reverse {
                            child.last()
                        } else {
                            child.first()
                        }
                        .view(),
                    ) == after
                    {
                        self.pages.clear();
                        return Ok(None);
                    }
                    self.descend(tree, child, &position, before).await?;
                }
            }
        }
        Ok(None)
    }

    async fn descend(
        &mut self,
        tree: &Tree,
        mut link: Link,
        position: &impl Fn(KeyRef<'_>) -> Ordering,
        before: Ordering,
    ) -> Result<Option<()>> {
        loop {
            if position(
                if self.reverse {
                    link.last()
                } else {
                    link.first()
                }
                .view(),
            ) == before.reverse()
                || position(
                    if self.reverse {
                        link.first()
                    } else {
                        link.last()
                    }
                    .view(),
                ) == before
            {
                return Ok(None);
            }
            let node = tree.open(&link).await?;
            match &node.page {
                Page::Leaf(leaf) => {
                    let offset = if self.reverse {
                        leaf.len()
                            - seek::partition(leaf.len(), |i| position(leaf.key(i)) != before)
                    } else {
                        seek::partition(leaf.len(), |i| position(leaf.key(i)) == before)
                    };
                    self.pages.push((node, offset));
                    return Ok(Some(()));
                }
                Page::Branch(children) => {
                    let skipped = if self.reverse {
                        children.len()
                            - seek::partition(children.len(), |i| {
                                position(children[i].first().view()) != before
                            })
                    } else {
                        seek::partition(children.len(), |i| {
                            position(children[i].last().view()) == before
                        })
                    };
                    let slot =
                        seek::slot(children.len(), skipped, self.reverse).ok_or_else(|| {
                            format_error("Ordered index seek contradicts its page fences")
                        })?;
                    link = children[slot].clone();
                    self.pages.push((node, skipped + 1));
                }
            }
        }
    }
}

#[cfg(test)]
mod count_tests {
    use super::*;
    use rtdb_storage::MemoryStore;

    #[tokio::test]
    async fn rank_reads_persisted_cardinalities_across_leaf_boundaries() -> Result<()> {
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let directory = ObjectKey::new("count-test")?;
        let memory = MemoryBudget::new(16 << 20);
        let cancel = CancellationToken::new();
        let tree = Tree::new(&storage, &directory, &memory, 0).await?;
        let edits = (0..150)
            .map(|n| (Key::Revision(Timestamp(n)), Some(Item::Revision(None))))
            .collect();
        let root = tree.edit(None, edits).await?.expect("must create a tree");
        let pointer = tree
            .persist(&root, 0, &mut BTreeMap::new(), &cancel)
            .await?;
        assert_eq!(pointer.entries, 150);
        let reopened = Tree::new(&storage, &directory, &memory, 0).await?;
        let root = Some(Link::Stored(pointer));
        for n in [0, 1, 63, 64, 127, 128, 149, 150, 200] {
            let before =
                |key: KeyRef<'_>| matches!(key, KeyRef::Revision(time) if time < Timestamp(n));
            assert_eq!(
                reopened.rank(&root, before, &cancel).await?,
                n.min(150) as u64
            );
        }
        Ok(())
    }
}
