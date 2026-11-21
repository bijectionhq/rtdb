use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_storage::{
    ObjectKey,
    cache::{BufferPool, Cached, View},
};
use rtdb_types::{Iid, Interval, Result, Timestamp};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::{
    format_error,
    key::{Atom, Bucket},
    maintenance::cancelled,
    seek,
};

const FANOUT: usize = 64;
type Work<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) enum Key {
    Posting {
        bucket: Bucket,
        tuple: Arc<[Atom]>,
        iid: Iid,
    },
    Row(Iid),
    Revision(Timestamp),
}

impl Key {
    fn bytes(&self) -> usize {
        size_of::<Self>()
            + match self {
                Self::Posting { tuple, .. } => tuple_bytes(tuple),
                Self::Row(_) | Self::Revision(_) => 0,
            }
    }
}

fn tuple_bytes(tuple: &[Atom]) -> usize {
    tuple
        .iter()
        .map(|atom| size_of::<Atom>() + atom.retained_bytes())
        .sum()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Span {
    pub valid: Interval,
    pub tuple: Arc<[Atom]>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Item {
    Posting,
    Row(Arc<[Span]>),
    Revision(Option<Arc<Pointer>>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Entry {
    pub key: Key,
    pub item: Item,
}

impl Entry {
    fn bytes(&self) -> usize {
        self.key.bytes()
            + match &self.item {
                Item::Posting | Item::Revision(None) => 0,
                Item::Row(spans) => spans
                    .iter()
                    .map(|span| size_of::<Span>() + tuple_bytes(&span.tuple))
                    .sum(),
                Item::Revision(Some(pointer)) => pointer.first.bytes() + pointer.last.bytes() + 128,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Pointer {
    pub digest: String,
    generation: i64,
    size: usize,
    first: Key,
    last: Key,
}

impl Pointer {
    pub(super) fn exists_by(&self, block: i64) -> bool {
        (0..=block).contains(&self.generation)
    }
}

#[derive(Clone, Debug)]
pub(super) enum Link {
    Stored(Arc<Pointer>),
    New(Arc<Node>),
}

impl Link {
    pub fn dirty_bytes(&self) -> usize {
        match self {
            Self::Stored(_) => 0,
            Self::New(node) => node.dirty_bytes,
        }
    }

    fn first(&self) -> &Key {
        match self {
            Self::Stored(pointer) => &pointer.first,
            Self::New(node) => node.page.first(),
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
    _memory: Arc<MemoryReservation>,
    pub(super) dirty_bytes: usize,
}

impl Cached for Node {}

#[derive(Debug)]
enum Page {
    Leaf(Vec<Entry>),
    Branch(Vec<Link>),
}

impl Page {
    fn first(&self) -> &Key {
        match self {
            Self::Leaf(entries) => &entries[0].key,
            Self::Branch(children) => children[0].first(),
        }
    }

    fn last(&self) -> &Key {
        match self {
            Self::Leaf(entries) => &entries[entries.len() - 1].key,
            Self::Branch(children) => children[children.len() - 1].last(),
        }
    }

    fn bytes(&self) -> usize {
        match self {
            Self::Leaf(entries) => entries.iter().map(Entry::bytes).sum(),
            Self::Branch(children) => children
                .iter()
                .map(|child| child.first().bytes() + child.last().bytes() + 128)
                .sum(),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum StoredPage {
    Leaf(Vec<Entry>),
    Branch(Vec<Arc<Pointer>>),
}

pub(super) type Root = Option<Link>;

pub(super) struct Tree<'a> {
    pub storage: &'a BufferPool,
    pub directory: &'a ObjectKey,
    pub memory: &'a Arc<MemoryBudget>,
}

impl Tree<'_> {
    fn page_key(&self, generation: i64, digest: &str) -> Result<ObjectKey> {
        if generation < 0
            || digest.len() != 64
            || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(format_error("Invalid ordered index page address"));
        }
        Ok(ObjectKey::new(format!(
            "{}/pages/{generation:016x}/{digest}",
            self.directory.as_str()
        ))?)
    }

    fn link(&self, page: Page) -> Result<Link> {
        let memory = self
            .memory
            .reserve(page.bytes().saturating_mul(2).saturating_add(256))?;
        let children = match &page {
            Page::Branch(children) => children.iter().fold(0usize, |bytes, child| {
                bytes.saturating_add(child.dirty_bytes())
            }),
            Page::Leaf(_) => 0,
        };
        let dirty_bytes = memory.bytes().saturating_add(children);
        Ok(Link::New(Arc::new(Node {
            page,
            _memory: memory,
            dirty_bytes,
        })))
    }

    async fn open(&self, link: &Link) -> Result<Arc<Node>> {
        let pointer = match link {
            Link::New(node) => return Ok(Arc::clone(node)),
            Link::Stored(pointer) => pointer,
        };
        let object = self.page_key(pointer.generation, &pointer.digest)?;
        if let Some(node) = self.storage.view::<Node>(&object, View::Layout) {
            if node.page.first() != &pointer.first || node.page.last() != &pointer.last {
                return Err(format_error(
                    "Ordered index page fences differ from its reference",
                ));
            }
            return Ok(node);
        }
        let bound = pointer.size.saturating_mul(8).saturating_add(4096);
        let cached = self.storage.admit(bound);
        let memory = match &cached {
            Some(memory) => Arc::clone(memory),
            None => self.memory.reserve(bound)?,
        };
        // Derived index pages are rebuildable, and must never make a durable
        // replica-local copy outside the object owner's erasure inventory.
        let store = self.storage.object_store();
        if store.head_fresh(&object).await?.size != pointer.size as u64 {
            return Err(format_error("Ordered index page has the wrong length"));
        }
        let bytes = store.get_fresh(&object).await?;
        if bytes.as_slice().len() != pointer.size
            || format!("{:x}", Sha256::digest(bytes.as_slice())) != pointer.digest
        {
            return Err(format_error("Ordered index page checksum mismatch"));
        }
        let page = match serde_json::from_slice(bytes.as_slice()).map_err(format_error)? {
            StoredPage::Leaf(entries)
                if !entries.is_empty()
                    && entries.len() <= FANOUT
                    && entries.iter().all(|entry| {
                        entry.is_typed() && entry.references_at_or_before(pointer.generation)
                    })
                    && entries.windows(2).all(|pair| pair[0].key < pair[1].key) =>
            {
                Page::Leaf(entries)
            }
            StoredPage::Branch(children)
                if !children.is_empty()
                    && children.len() <= FANOUT
                    && children.iter().all(|child| {
                        child.exists_by(pointer.generation) && child.first <= child.last
                    })
                    && children.windows(2).all(|pair| pair[0].last < pair[1].first) =>
            {
                Page::Branch(children.into_iter().map(Link::Stored).collect())
            }
            _ => return Err(format_error("Invalid ordered index page")),
        };
        if page.first() != &pointer.first || page.last() != &pointer.last {
            return Err(format_error(
                "Ordered index page fences differ from its reference",
            ));
        }
        let node = Arc::new(Node {
            page,
            _memory: memory,
            dirty_bytes: 0,
        });
        if cached.is_some() {
            self.storage
                .insert_view(&object, View::Layout, Arc::clone(&node));
        }
        Ok(node)
    }

    pub async fn spans(&self, root: &Root, iid: Iid) -> Result<Arc<[Span]>> {
        let key = Key::Row(iid);
        let mut link = root.clone();
        while let Some(page) = link.take() {
            if &key < page.first() || &key > page.last() {
                break;
            }
            let node = self.open(&page).await?;
            match &node.page {
                Page::Leaf(entries) => {
                    if let Ok(slot) = entries.binary_search_by(|entry| entry.key.cmp(&key)) {
                        let Item::Row(spans) = &entries[slot].item else {
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
                    .into_iter()
                    .filter_map(|(key, item)| item.map(|item| Entry { key, item }))
                    .collect(),
                Page::Leaf,
            )?,
        };
        while pages.len() > 1 {
            pages = self.pack(pages, Page::Branch)?;
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
                Page::Leaf(entries) => {
                    let mut changed = Vec::with_capacity(entries.len() + edits.len());
                    let mut existing = entries.iter().peekable();
                    for (key, item) in edits {
                        while existing.peek().is_some_and(|entry| &entry.key < key) {
                            changed.extend(existing.next().cloned());
                        }
                        if existing.peek().is_some_and(|entry| &entry.key == key) {
                            existing.next();
                        }
                        if let Some(item) = item {
                            changed.push(Entry {
                                key: key.clone(),
                                item: item.clone(),
                            });
                        }
                    }
                    changed.extend(existing.cloned());
                    self.pack(changed, Page::Leaf)
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
                    self.pack(self.merge_sparse(changed).await?, Page::Branch)
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
                        Some(Page::Leaf(left.iter().chain(right).cloned().collect()))
                    }
                    (Page::Branch(left), Page::Branch(right))
                        if left.len() + right.len() <= FANOUT =>
                    {
                        Some(Page::Branch(left.iter().chain(right).cloned().collect()))
                    }
                    _ => None,
                };
                if let Some(page) = page {
                    let last = packed.last_mut().expect("a sibling was read");
                    *last = self.link(page)?;
                    continue;
                }
            }
            packed.push(child);
        }
        Ok(packed)
    }

    fn pack<T>(&self, slots: Vec<T>, page: impl Fn(Vec<T>) -> Page) -> Result<Vec<Link>> {
        if slots.is_empty() {
            return Ok(Vec::new());
        }
        if slots.len() <= FANOUT {
            return Ok(vec![self.link(page(slots))?]);
        }
        let num_pages = slots.len().div_ceil(FANOUT);
        let mut slots = slots.into_iter();
        let mut links = Vec::with_capacity(num_pages);
        for remaining in (1..=num_pages).rev() {
            let size = slots.len().div_ceil(remaining);
            links.push(self.link(page(slots.by_ref().take(size).collect()))?);
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
            let _scratch = self
                .memory
                .reserve(node.page.bytes().saturating_mul(8).saturating_add(4096))?;
            let encoded = match &node.page {
                Page::Leaf(entries) => {
                    if !entries
                        .iter()
                        .all(|entry| entry.references_at_or_before(generation))
                    {
                        return Err(format_error(
                            "Ordered index revision refers to a later block",
                        ));
                    }
                    serde_json::to_vec(&StoredPage::Leaf(entries.clone()))
                }
                Page::Branch(children) => {
                    let mut pointers = Vec::with_capacity(children.len());
                    for child in children {
                        pointers.push(
                            self.persist(child, generation, written, cancellation)
                                .await?,
                        );
                    }
                    serde_json::to_vec(&StoredPage::Branch(pointers))
                }
            }
            .map_err(format_error)?;
            let digest = format!("{:x}", Sha256::digest(&encoded));
            let size = encoded.len();
            self.storage
                .object_store()
                .put(&self.page_key(generation, &digest)?, encoded.into())
                .await?;
            let pointer = Arc::new(Pointer {
                digest,
                generation,
                size,
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
        reachable: &'a mut BTreeSet<ObjectKey>,
    ) -> Work<'a, ()> {
        Box::pin(async move {
            if !reachable.insert(self.page_key(pointer.generation, &pointer.digest)?) {
                return Ok(());
            }
            let node = self.open(&Link::Stored(pointer)).await?;
            match &node.page {
                Page::Branch(children) => {
                    for child in children {
                        let Link::Stored(pointer) = child else {
                            return Err(format_error(
                                "A stored page contains an unpublished child",
                            ));
                        };
                        self.mark(Arc::clone(pointer), reachable).await?;
                    }
                }
                Page::Leaf(entries) => {
                    for entry in entries {
                        if let Item::Revision(Some(pointer)) = &entry.item {
                            self.mark(Arc::clone(pointer), reachable).await?;
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

fn sparse(link: &Link) -> bool {
    let Link::New(node) = link else {
        return false;
    };
    match &node.page {
        Page::Leaf(entries) => entries.len() <= FANOUT / 4,
        Page::Branch(children) => children.len() <= FANOUT / 4,
    }
}

/// A directed tree walk starting at the first leaf whose fence can meet the seek.
pub(super) struct Cursor {
    pages: Vec<(Arc<Node>, usize)>,
    root: Root,
    reverse: bool,
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
        tree: &Tree<'_>,
        position: impl Fn(&Key) -> Ordering,
    ) -> Result<Option<Entry>> {
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
                Page::Leaf(entries) => entries.len(),
                Page::Branch(children) => children.len(),
            };
            let Some(slot) = seek::slot(size, *offset, self.reverse) else {
                self.pages.pop();
                continue;
            };
            *offset += 1;
            match &node.page {
                Page::Leaf(entries) => {
                    let key = &entries[slot].key;
                    let found = position(key);
                    if found == after {
                        self.pages.clear();
                        return Ok(None);
                    }
                    if found == before {
                        return Err(format_error("Ordered index seek lost its lower boundary"));
                    }
                    return Ok(Some(entries[slot].clone()));
                }
                Page::Branch(children) => {
                    let child = children[slot].clone();
                    if position(if self.reverse {
                        child.last()
                    } else {
                        child.first()
                    }) == after
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
        tree: &Tree<'_>,
        mut link: Link,
        position: &impl Fn(&Key) -> Ordering,
        before: Ordering,
    ) -> Result<Option<()>> {
        loop {
            if position(if self.reverse {
                link.last()
            } else {
                link.first()
            }) == before.reverse()
                || position(if self.reverse {
                    link.first()
                } else {
                    link.last()
                }) == before
            {
                return Ok(None);
            }
            let node = tree.open(&link).await?;
            match &node.page {
                Page::Leaf(entries) => {
                    let offset = if self.reverse {
                        entries.len()
                            - seek::partition(entries.len(), |i| {
                                position(&entries[i].key) != before
                            })
                    } else {
                        seek::partition(entries.len(), |i| position(&entries[i].key) == before)
                    };
                    self.pages.push((node, offset));
                    return Ok(Some(()));
                }
                Page::Branch(children) => {
                    let skipped = if self.reverse {
                        children.len()
                            - seek::partition(children.len(), |i| {
                                position(children[i].first()) != before
                            })
                    } else {
                        seek::partition(children.len(), |i| position(children[i].last()) == before)
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
