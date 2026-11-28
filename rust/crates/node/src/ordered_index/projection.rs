use std::{
    cmp::Ordering,
    collections::BTreeMap,
    sync::Arc,
};

use bytes::Bytes;
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation};
use rtdb_format::data::{EncodedEvent, EventBatch, PageEvent};
use rtdb_temporal::HistoryResolver;
use rtdb_types::{Anomaly, AnomalyKind, EventHeader, EventOp, Iid, Interval, LogicalType, Result, TableRef, TemporalBounds, Timestamp, Value, WorldShadow};
use tokio_util::sync::CancellationToken;

use super::{
    format_error,
    key::{AtomRef, Bucket},
    maintenance::cancelled,
    postings::{BatchKind, KeyBatch, KeyColumn, KeyRow, ProjectedKeys, Tuple},
    tree::{Address, AddressRef, Cursor, Hit, IndexRevision, Item, Key, KeyRef, Link, Root, Span, Spans, Tree},
};

/// The roots for the partition's ordered history, with erasures applied to every affected root.
#[derive(Default)]
pub(super) struct RevisionRoots {
    pub archived: Root,
    pub changes: BTreeMap<IndexRevision, Root>,
    pub(super) allocation: Option<Arc<MemoryReservation>>,
}

impl RevisionRoots {
    pub fn clear_changes(&mut self) {
        self.changes.clear();
        self.allocation = None;
    }

    fn insert(&mut self, revision: IndexRevision, root: Root, memory: &Arc<MemoryBudget>) -> Result<()> {
        if !self.changes.contains_key(&revision) {
            if self.allocation.is_none() {
                self.allocation = Some(memory.reserve(0)?);
            }
            let grant = Arc::get_mut(self.allocation.as_mut().unwrap()).expect("revision roots own the directory");
            grant.grow_to(grant.bytes().saturating_add(256))?;
        }
        self.changes.insert(revision, root);
        Ok(())
    }

    pub async fn at(
        &self,
        tree: &Tree,
        through: IndexRevision,
    ) -> Result<Option<(IndexRevision, Root)>> {
        if let Some((&time, root)) = self.changes.range(..=through).next_back() {
            return Ok(Some((time, root.clone())));
        }
        let mut cursor = Cursor::new(self.archived.clone(), true);
        cursor
            .next(tree, |key| match key {
                KeyRef::Revision(revision) if revision <= through => Ordering::Equal,
                KeyRef::Revision(_) => Ordering::Greater,
                KeyRef::Posting(..) | KeyRef::Row(_) => Ordering::Less,
            })
            .await?
            .map(archived_revision)
            .transpose()
    }

    async fn stage_archived(
        &mut self,
        tree: &Tree,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let mut cursor = Cursor::new(self.archived.take(), false);
        while let Some(entry) = cursor.next(tree, |_| Ordering::Equal).await? {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let (time, root) = archived_revision(entry)?;
            if !self.changes.contains_key(&time) {
                self.insert(time, root, &tree.memory)?;
            }
        }
        Ok(())
    }

    pub async fn apply(
        &mut self,
        tree: &Tree,
        changes: Changes,
        order: EventOrder,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if changes.revisions.is_empty() {
            return Ok(());
        }
        let (mut last, mut root) = self
            .at(tree, IndexRevision::MAX)
            .await?
            .map_or((None, None), |(revision, root)| (Some(revision), root));
        let Changes { revisions, addresses } = changes;
        drop(addresses);
        for (revision, rows) in revisions {
            if last.is_some_and(|last| last > revision) {
                return Err(format_error(
                    "Ordered index replay crossed its revision frontier",
                ));
            }
            let mut edits = EditBatch::new(&tree.memory)?;
            let mut cut_at_revision = false;
            for (address, mut events) in rows {
                if cancellation.is_cancelled() {
                    return Err(cancelled());
                }
                if matches!(order, EventOrder::OldestFirst) {
                    events.reverse();
                }
                if events.iter().any(|event| event.header.position.is_some() || event.header.commit_position != 0) {
                    let _order = tree.memory.reserve(events.len().saturating_mul(size_of::<Event>()))?;
                    events.sort_by_key(|event| std::cmp::Reverse(event.header.precedence()));
                }
                let before = tree.spans(&root, &address).await?;
                let cut = events.iter().any(|event| event.header.op == EventOp::Erase);
                let after = resolve(address.iid, revision.time, &before, &events, &tree.memory)?;
                if cut {
                    cut_at_revision = true;
                    self.stage_archived(tree, cancellation).await?;
                    for (_, historical) in self.changes.range_mut(..revision) {
                        let spans = tree.spans(historical, &address).await?;
                        if !spans.is_empty() {
                            let mut removals = EditBatch::new(&tree.memory)?;
                            removals.project(&address, &spans, Spans::default())?;
                            *historical = removals.apply(tree, historical.clone()).await?;
                        }
                    }
                }
                if before.as_ref() != after.as_ref() {
                    edits.project(&address, &before, after)?;
                }
            }
            if cut_at_revision || !edits.entries.is_empty() {
                root = edits.apply(tree, root).await?;
                self.insert(revision, root.clone(), &tree.memory)?;
            }
            last = Some(revision);
        }
        Ok(())
    }
}

fn archived_revision(hit: Hit) -> Result<(IndexRevision, Root)> {
    let entry = hit.into_entry();
    match (entry.key, entry.item) {
        (Key::Revision(time), Item::Revision(pointer)) => Ok((time, pointer.map(Link::Stored))),
        _ => Err(format_error(
            "Ordered index revision directory contains a row or posting",
        )),
    }
}

fn resolve<'a>(
    iid: Iid,
    time: Timestamp,
    before: &[Span],
    events: impl IntoIterator<Item = &'a Event>,
    memory: &Arc<MemoryBudget>,
) -> Result<Spans> {
    let bounds = TemporalBounds {
        valid_time: Interval::ALL,
        system_time: Interval::at(time)?,
    };
    let mut resolver = HistoryResolver::new_in(bounds, false, Arc::clone(memory));
    let mut grant = memory.reserve(Spans::allocation_bytes(0))?;
    let workspace = Arc::get_mut(&mut grant).expect("span construction owns its reservation");
    let mut after = Vec::new();
    for event in events {
        resolver.resolve_fragments(&event.header, |valid, _| {
            if let Some(tuple) = &event.tuple {
                push_reserved(&mut after, Span { valid, tuple: tuple.clone() }, workspace)?;
            }
            Ok(())
        })?;
    }
    // The previous projection contains exactly the older intervals that can still win.
    for span in before {
        let header = EventHeader { position: None,
            commit_position: 0,
            iid,
            system_from: Timestamp::MIN,
            valid_time: span.valid,
            op: EventOp::Put,
        };
        resolver.resolve_fragments(&header, |valid, _| {
            push_reserved(&mut after, Span { valid, tuple: span.tuple.clone() }, workspace)?;
            Ok(())
        })?;
    }
    after.sort_unstable_by_key(|span| span.valid);
    let mut written = 0usize;
    for row in 0..after.len() {
        if written > 0
            && after[written - 1].valid.upper() == after[row].valid.lower()
            && after[written - 1].tuple == after[row].tuple
        {
            after[written - 1].valid =
                Interval::new(after[written - 1].valid.lower(), after[row].valid.upper())?;
        } else {
            after.swap(written, row);
            written += 1;
        }
    }
    after.truncate(written);
    Ok(Spans::new(after, grant))
}

fn push_reserved<T>(values: &mut Vec<T>, value: T, grant: &mut MemoryReservation) -> Result<()> {
    let capacity = values.capacity();
    if values.len() == capacity {
        let next = capacity.saturating_mul(2).max(1);
        let previous = grant.bytes();
        let requested = previous.saturating_add(next.saturating_mul(size_of::<T>()));
        grant.grow_to(requested)?;
        values.try_reserve_exact(next - capacity).map_err(format_error)?;
        grant.shrink_to(requested - capacity * size_of::<T>())?;
    }
    values.push(value);
    Ok(())
}

struct EditBatch {
    entries: Vec<(Key, Option<Item>)>,
    allocation: Arc<MemoryReservation>,
}

impl EditBatch {
    fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { entries: Vec::new(), allocation: memory.reserve(0)? })
    }

    fn project_replayed(
        &mut self,
        address: &Address,
        events: &[Event],
        revision: IndexRevision,
        through: IndexRevision,
        order: EventOrder,
        before: &[Span],
    ) -> Result<()> {
        let memory = self.allocation.budget();
        let after = if revision > through {
            Spans::default()
        } else if events.iter().any(|event| event.header.position.is_some() || event.header.commit_position != 0) {
            let _scratch = memory.reserve(events.len().saturating_mul(2 * size_of::<&Event>()))?;
            let mut ordered: Vec<_> = events.iter().collect();
            if matches!(order, EventOrder::OldestFirst) {
                ordered.reverse();
            }
            ordered.sort_by_key(|event| std::cmp::Reverse(event.header.precedence()));
            resolve(address.iid, revision.time, before, ordered, memory)?
        } else if matches!(order, EventOrder::OldestFirst) {
            resolve(address.iid, revision.time, before, events.iter().rev(), memory)?
        } else {
            resolve(address.iid, revision.time, before, events, memory)?
        };
        if before != after.as_ref() {
            self.project(address, before, after)?;
        }
        Ok(())
    }

    fn project(&mut self, address: &Address, before: &[Span], after: Spans) -> Result<()> {
        let start = self.entries.len();
        let grant = Arc::get_mut(&mut self.allocation).expect("edit staging owns its reservation");
        for (spans, replacement) in [(before, None), (after.as_ref(), Some(Item::Posting))] {
            for span in spans {
                for bucket in Bucket::cover(span.valid) {
                    let key = Key::Posting { bucket, tuple: span.tuple.clone(), address: address.clone() };
                    push_reserved(&mut self.entries, (key, replacement.clone()), grant)?;
                }
            }
        }

        let entries = &mut self.entries[start..];
        entries.sort_unstable_by(|(left, removed), (right, inserted)| {
            left.cmp(right).then_with(|| removed.is_some().cmp(&inserted.is_some()))
        });
        let (mut row, mut written) = (0, 0);
        while row < entries.len() {
            let next = row + 1 + entries[row + 1..].partition_point(|entry| entry.0 == entries[row].0);
            if entries[row].1.is_some() == entries[next - 1].1.is_some() {
                entries.swap(written, row);
                written += 1;
            }
            row = next;
        }
        self.entries.truncate(start + written);
        let spans = (!after.is_empty()).then_some(Item::Row(after));
        push_reserved(&mut self.entries, (Key::Row(address.clone()), spans), grant)
    }

    async fn apply(self, tree: &Tree, root: Root) -> Result<Root> {
        let Self { mut entries, allocation } = self;
        entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let result = tree.edit(root, entries).await;
        drop(allocation);
        result
    }
}

#[derive(Clone, Copy)]
pub(super) enum EventOrder {
    OldestFirst,
    NewestFirst,
}

#[derive(Clone, Copy)]
pub(super) enum KeyLayout {
    Plain,
    Governed,
    World,
}

impl KeyLayout {
    pub fn for_table(table: &TableRef) -> Self {
        if table.owned_world_root_table().is_some() {
            Self::World
        } else if table.is_owned_event_data() {
            Self::Governed
        } else {
            Self::Plain
        }
    }
}

struct Event {
    header: EventHeader,
    tuple: Option<KeyRow>,
    _allocation: Arc<MemoryReservation>,
}

#[derive(Default)]
pub(super) struct Changes {
    revisions: BTreeMap<IndexRevision, BTreeMap<Address, Vec<Event>>>,
    addresses: BTreeMap<Bytes, Arc<MemoryReservation>>,
}

impl Changes {
    pub fn is_empty(&self) -> bool { self.revisions.is_empty() }

    /// A read keeps only its selected root. Later erasures still remove older facts from it.
    pub async fn read(
        &self,
        tree: &Tree,
        root: Root,
        through: IndexRevision,
        order: EventOrder,
        cancellation: &CancellationToken,
    ) -> Result<Root> {
        const GROUP_SIZE: usize = 128;
        const GROUP_WORKSPACE: usize = 32 << 10;
        if tree.memory.snapshot_budget().is_some()
            && self.revisions.values().any(|rows| rows.len() >= 8)
            && let Ok(workspace) = tree.memory.reserve(GROUP_WORKSPACE)
        {
            let result = self.read_grouped(tree, root.clone(), through, order, cancellation, GROUP_SIZE).await;
            drop(workspace);
            match result {
                Err(error) if error.is(AnomalyKind::ResourceExhausted) => (),
                result => return result,
            }
        }
        self.read_grouped(tree, root, through, order, cancellation, 1).await
    }

    async fn read_grouped(
        &self,
        tree: &Tree,
        mut root: Root,
        through: IndexRevision,
        order: EventOrder,
        cancellation: &CancellationToken,
        group_size: usize,
    ) -> Result<Root> {
        let mut group = Vec::with_capacity(group_size);
        let mut addresses = Vec::with_capacity(group_size);
        for (&revision, rows) in &self.revisions {
            let mut edits = EditBatch::new(&tree.memory)?;
            let mut rows = rows.iter();
            loop {
                while group.len() < group_size {
                    let Some((address, events)) = rows.next() else { break; };
                    if cancellation.is_cancelled() { return Err(cancelled()); }
                    if revision > through && !events.iter().any(|event| event.header.op == EventOp::Erase) {
                        continue;
                    }
                    group.push((address, events));
                }
                match group.as_slice() {
                    [] => break,
                    [(address, events)] => {
                        let before = tree.spans(&root, address).await?;
                        edits.project_replayed(address, events, revision, through, order, &before)?;
                    }
                    _ => {
                        addresses.extend(group.iter().map(|&(address, _)| address));
                        let spans = tree.spans_in_order(&root, &addresses, cancellation).await?;
                        for (&(address, events), before) in group.iter().zip(spans) {
                            if cancellation.is_cancelled() { return Err(cancelled()); }
                            edits.project_replayed(address, events, revision, through, order, &before)?;
                        }
                        addresses.clear();
                    }
                }
                group.clear();
            }
            root = edits.apply(tree, root).await?;
        }
        Ok(root)
    }

    pub fn add_batch(
        &mut self,
        batch: &EventBatch,
        skip: usize,
        columns: &[String],
        memory: &Arc<MemoryBudget>,
        cancellation: &CancellationToken,
        layout: KeyLayout,
        retain: impl Fn(EventHeader, AddressRef<'_>) -> bool,
    ) -> Result<()> {
        let keyed = !matches!(layout, KeyLayout::Plain);
        if batch.has_entity_keys() != keyed {
            return Err(format_error("Ordered source events have the wrong physical identity layout"));
        }
        let _headers = memory.reserve(4096usize.saturating_add(columns.len().saturating_mul(size_of::<KeyColumn<'_>>())))?;
        let event_at = batch.prepare_page_events()?;
        (0..batch.len()).try_for_each(|row| event_at(row).map(drop))?;
        let document = batch.put_document()?;
        let column = |name: &str| document.and_then(|document| document.fields().iter().zip(document.columns())
            .find(|(field, _)| field.name() == name))
            .map(|(field, array)| (field.as_ref(), array.as_ref()));
        let projection: Vec<_> = columns.iter().map(|name| KeyColumn::new(column(name))).collect();
        let identity = keyed.then(|| column("_id")).flatten();
        let marker = matches!(layout, KeyLayout::World).then(|| column(WorldShadow::FIELDS)).flatten();
        for start in (skip.min(batch.len())..batch.len()).step_by(128) {
            let end = start.saturating_add(128).min(batch.len());
            let len = end - start;
            let _scratch = memory.reserve(
                len
                    .saturating_mul(columns.len())
                    .saturating_mul(size_of::<AtomRef<'_>>())
                    .saturating_add(8192)
                    .saturating_add(if keyed {
                        len.saturating_mul(128)
                    } else {
                        0
                    }),
            )?;
            let mut retained = Vec::new();
            let mut rows = Vec::new();
            for index in start..end {
                if cancellation.is_cancelled() {
                    return Err(cancelled());
                }
                let event = event_at(index)?;
                let header = event.header();
                let address = AddressRef { iid: header.iid, key: batch.entity_key(index)?.unwrap_or_default() };
                let id = if keyed {
                    Some(rtdb_columnar::EntityIdRef::from_handle_bytes(address.key)?)
                } else {
                    None
                };
                if !retain(header, address) { continue; }
                if header.op == EventOp::Redacted {
                    return Err(Anomaly::unsupported("rtdb.owned/redaction-uncovered", "Ordered indexes require coverage for redacted assertions"));
                }
                let row = if header.op == EventOp::Put {
                    let offset = event.put_offset().ok_or_else(|| format_error("An indexed put has no Arrow document"))?;
                    if let Some(id) = id {
                        let Some((field, array)) = identity else {
                            return Err(format_error("Governed ordered put omitted its identity"));
                        };
                        let _id = memory.reserve(rtdb_columnar::estimate_copy_cell_bytes(field, array, offset)?)?;
                        let matches = match rtdb_columnar::read_owned_entity_id(field, array, offset) {
                            Ok(borrowed) => borrowed == id,
                            Err(_) => event.put_cell("_id", &LogicalType::Null)?.owned_entity_id()? == id.into_owned(),
                        };
                        if !matches {
                            return Err(format_error("Governed ordered put differs from its physical identity"));
                        }
                    }
                    if matches!(layout, KeyLayout::World) && world_tombstone(&event, marker, offset, memory)? {
                        None
                    } else {
                        let row = rows.len();
                        rows.push(u32::try_from(offset).map_err(format_error)?);
                        Some(row)
                    }
                } else {
                    None
                };
                retained.push((header, address, row));
            }
            let keys = if rows.is_empty() {
                None
            } else {
                let selected = projection.iter().map(|column| column.select(&rows)).collect::<Result<Vec<_>>>()?;
                let payload = selected.iter().map(|column| column.payload()).fold(0usize, usize::saturating_add);
                let mut allocation = memory.reserve(
                    payload
                        .saturating_mul(3)
                        .saturating_add(columns.len().saturating_mul(4096))
                        .saturating_add(
                            rows.len().saturating_mul(columns.len()).saturating_mul(64),
                        ),
                )?;
                let keys = KeyBatch::project(selected, rows)?;
                Arc::get_mut(&mut allocation)
                    .expect("key builder owns its reservation")
                    .shrink_to(keys.retained_bytes())?;
                Some(ProjectedKeys::new(keys, allocation))
            };
            for (header, address, row) in retained {
                let allocation = memory.reserve(256)?;
                let key = if address.key.is_empty() {
                    Bytes::new()
                } else if let Some((key, _)) = self.addresses.get_key_value(address.key) {
                    key.clone()
                } else {
                    let slot = memory.reserve(128)?;
                    let backing = memory.reserve(address.key.len().saturating_add(128))?;
                    let key = Address::retain_bytes(address.key.to_vec(), backing);
                    self.addresses.insert(key.clone(), slot);
                    key
                };
                self.revisions
                    .entry(IndexRevision::of_event(header))
                    .or_default()
                    .entry(Address::from_retained(address.iid, key))
                    .or_default()
                    .push(Event {
                        header,
                        tuple: row
                            .map(|row| keys.as_ref().expect("a put has projected keys").row(row)),
                        _allocation: allocation,
                    });
            }
        }
        Ok(())
    }
}

fn world_tombstone(
    event: &PageEvent,
    marker: Option<(&arrow_schema::Field, &dyn arrow_array::Array)>,
    row: usize,
    memory: &Arc<MemoryBudget>,
) -> Result<bool> {
    use arrow_schema::DataType;
    let Some((field, array)) = marker else {
        return Err(format_error("Ordered world shadow omitted its field marker"));
    };
    let _charge = memory.reserve(rtdb_columnar::estimate_copy_cell_bytes(field, array, row)?)?;
    let cell = ArrowCell { field, array, row }.physical()?;
    if !cell.is_undefined()? && cell.is_null()? { return Ok(true); }
    if cell.field.extension_type_name().is_none()
        && let DataType::List(item) = cell.field.data_type()
        && item.extension_type_name().is_none()
        && matches!(item.data_type(), DataType::Utf8 | DataType::Null)
    {
        return Ok(false);
    }
    match event.put_cell(WorldShadow::FIELDS, &LogicalType::Null)?.as_ref() {
        Value::Null => Ok(true),
        Value::List(_) => Ok(false),
        _ => Err(format_error("Invalid ordered world shadow field marker")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_index::{IndexBatch, StoredEvent, StoredOperation};
    use rtdb_storage::{MemoryStore, ObjectKey, cache::BufferPool};
    use rtdb_types::{StructFields, Value};

    fn put(id: i64, system: i64, valid: Interval, key: i64) -> StoredEvent {
        StoredEvent { position: None,
            origin: None,
            commit_position: 0,
            entity_key: None,
            iid: Value::I64(id).entity_id().unwrap().iid(),
            system_from: Timestamp(system),
            valid_time: valid,
            operation: StoredOperation::Put(StructFields::from([
                ("_id".into(), Value::I64(id)),
                ("key".into(), Value::I64(key)),
            ])),
            recency: None,
        }
    }

    fn changes(
        events: &[StoredEvent],
        memory: &Arc<MemoryBudget>,
        cancellation: &CancellationToken,
    ) -> Changes {
        let batch = IndexBatch::from_events(events, &StructFields::new(), memory).unwrap();
        let mut changes = Changes::default();
        changes
            .add_batch(
                batch.view(),
                0,
                &["key".into()],
                memory,
                cancellation,
                if events.first().is_some_and(|event| event.entity_key.is_some()) {
                    KeyLayout::Governed
                } else {
                    KeyLayout::Plain
                },
                |_, _| true,
            )
            .unwrap();
        changes
    }

    #[test]
    fn correction_staging_keeps_unchanged_buckets_out_of_the_edit_batch() -> Result<()> {
        use super::super::key::Atom;
        let memory = MemoryBudget::new(1 << 20);
        let address = Address::new(Iid::MIN, Bytes::new());
        let tuple = KeyRow::Fence(Arc::from([Atom::Integer(7)]));
        let span = |from, to| Span { valid: Interval::from_micros(from, to).unwrap(), tuple: tuple.clone() };
        let unchanged = span(0, 4);
        let before = [unchanged.clone(), span(8, 10)];
        let after = Spans::new(vec![unchanged, span(8, 12)], memory.reserve(Spans::allocation_bytes(2))?);
        let mut edits = EditBatch::new(&memory)?;
        edits.project(&address, &before, after)?;
        assert_eq!(edits.entries.len(), 3);
        assert_eq!(edits.entries.iter().filter(|(_, item)| item.is_none()).count(), 1);
        assert!(matches!(edits.entries.last(), Some((Key::Row(_), Some(Item::Row(_))))));
        drop(edits);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    async fn keys(tree: &Tree, root: Root, valid: Timestamp) -> Vec<(i128, Iid)> {
        let mut keys = Vec::new();
        for bucket in Bucket::at(valid) {
            let mut cursor = Cursor::new(root.clone(), false);
            while let Some(entry) = cursor
                .next(tree, |key| match key {
                    KeyRef::Posting(candidate, _, _) => candidate.cmp(&bucket),
                    KeyRef::Row(_) | KeyRef::Revision(_) => Ordering::Greater,
                })
                .await
                .unwrap()
            {
                let Key::Posting { tuple, address, .. } = entry.into_entry().key else {
                    panic!("Expected posting");
                };
                let AtomRef::Integer(value) = tuple.view().cell(0) else {
                    panic!("Expected integer");
                };
                keys.push((value, address.iid));
            }
        }
        keys.sort_unstable();
        keys
    }

    #[tokio::test]
    async fn ordered_replay_grouping_keeps_the_cut_and_applies_later_erases() -> Result<()> {
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 8 << 20));
        let directory = ObjectKey::new("ordered-read-ahead")?;
        let memory = MemoryBudget::new(32 << 20);
        let tree = Tree::new(&storage, &directory, &memory, 0).await?;
        let token = CancellationToken::new();
        let original: Vec<_> = (0..100).map(|id| put(id, 0, Interval::ALL, id)).collect();
        let through = IndexRevision::through_time(Timestamp(1));
        let root = changes(&original, &memory, &token)
            .read(&tree, None, through, EventOrder::OldestFirst, &token).await?.unwrap();
        let pointer = tree.persist(&root, 0, &mut BTreeMap::new(), &token).await?;
        let mut live: Vec<_> = (0..100).map(|id| put(id, 1, Interval::ALL, 100 + id)).collect();
        let mut erased = put(10, 2, Interval::ALL, 0);
        erased.operation = StoredOperation::Erase;
        live.push(erased);
        live.push(put(20, 2, Interval::ALL, 1000));
        let replay = changes(&live, &memory, &token);
        let tree = Tree::new(&storage, &directory, &memory, 0).await?;
        let replayed = replay.read(&tree, Some(Link::Stored(pointer)), through, EventOrder::OldestFirst, &token).await?;
        let expected: Vec<_> = original.iter().enumerate().filter(|(id, _)| *id != 10)
            .map(|(id, event)| (100 + id as i128, event.iid)).collect();
        assert_eq!(keys(&tree, replayed, Timestamp(0)).await, expected);
        Ok(())
    }

    #[test]
    fn projection_releases_unknown_source_owners_and_reuses_complete_address_bytes() -> Result<()>
    {
        use arrow_array::StringArray;
        use rtdb_types::EntityId;

        let events = [(0, "one"), (1, "two")].map(|(system, key)| {
            let mut event = put(1, system, Interval::ALL, 0);
            event.entity_key = Some(EntityId::Integer(1).handle_bytes().into());
            let StoredOperation::Put(row) = &mut event.operation else {
                unreachable!()
            };
            row.insert("key".into(), Value::Utf8(key.into()));
            event
        });
        let source_memory = MemoryBudget::new(32 << 20);
        let projection_memory = MemoryBudget::new(32 << 20);
        let batch = IndexBatch::from_events(&events, &StructFields::new(), &source_memory)?;
        let source = batch
            .view()
            .put_document()?
            .unwrap()
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .as_ptr();
        let mut changes = Changes::default();
        changes.add_batch(
            batch.view(),
            0,
            &["key".into()],
            &projection_memory,
            &CancellationToken::new(),
            KeyLayout::Governed,
            |_, _| true,
        )?;
        drop(batch);
        let mut later = events[1].clone();
        later.system_from = Timestamp(2);
        let batch = IndexBatch::from_events(&[later], &StructFields::new(), &source_memory)?;
        changes.add_batch(
            batch.view(), 0, &["key".into()], &projection_memory,
            &CancellationToken::new(), KeyLayout::Governed, |_, _| true,
        )?;
        drop(batch);

        let mut rows = changes
            .revisions
            .values()
            .map(|revision| revision.first_key_value().unwrap());
        let (first, events) = rows.next().unwrap();
        let (second, _) = rows.next().unwrap();
        let AtomRef::Text(value) = events[0].tuple.as_ref().unwrap().view().cell(0) else {
            unreachable!()
        };
        assert_eq!(value, "one");
        assert_ne!(value.as_ptr(), source);
        assert_eq!(first.view().key.as_ptr(), second.view().key.as_ptr());
        assert_eq!(first.view().key.as_ptr(), rows.next().unwrap().0.view().key.as_ptr());
        assert_eq!(source_memory.used_bytes(), 0);
        assert!(projection_memory.used_bytes() > 0);
        drop(changes);
        assert_eq!(projection_memory.used_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn world_projection_keeps_selected_puts_and_shadows_older_keys() -> Result<()> {
        use rtdb_types::EntityId;

        let world_put = |id, system, valid, shadow: WorldShadow| -> Result<_> {
            let mut event = put(id, system, valid, 0);
            let id = EntityId::Integer(id);
            event.entity_key = Some(id.handle_bytes().into());
            event.operation = StoredOperation::Put(shadow.record(&id)?);
            Ok(event)
        };
        let present = |id, key, label: Option<Value>| {
            let mut row = StructFields::from([("_id".into(), Value::I64(id)), ("key".into(), key)]);
            if let Some(label) = label {
                row.insert("label".into(), label);
            }
            WorldShadow::Present(row)
        };
        let events = [
            world_put(7, 0, Interval::ALL, present(7, Value::I64(7), None))?,
            world_put(1, 0, Interval::ALL, present(1, Value::U32(u32::MAX), None))?,
            world_put(1, 1, Interval::from_micros(5, 10)?, WorldShadow::Deleted)?,
            world_put(8, 1, Interval::ALL, present(8, Value::I64(8), None))?,
            world_put(2, 1, Interval::ALL, present(2, Value::I64(9), Some(Value::Binary(vec![1, 2]))))?,
        ];
        let memory = MemoryBudget::new(32 << 20);
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 8 << 20));
        let tree = Tree::new(&storage, &ObjectKey::new("world-index")?, &memory, 0).await?;
        let token = CancellationToken::new();
        let batch = IndexBatch::from_events(&events, &StructFields::new(), &memory)?;
        let mut changes = Changes::default();
        changes.add_batch(
            batch.view(), 1, &["key".into(), "label".into(), "absent".into()],
            &memory, &token, KeyLayout::World, |_, address| address.iid != events[3].iid,
        )?;
        let mut roots = RevisionRoots::default();
        roots.apply(&tree, changes, EventOrder::OldestFirst, &token).await?;
        let (_, before) = roots.at(&tree, IndexRevision::through_time(Timestamp(0))).await?.unwrap();
        let (_, after) = roots.at(&tree, IndexRevision::through_time(Timestamp(1))).await?.unwrap();
        assert_eq!(keys(&tree, before, Timestamp(6)).await, [(-1, events[1].iid)]);
        assert_eq!(keys(&tree, after.clone(), Timestamp(6)).await, [(9, events[4].iid)]);
        assert_eq!(
            keys(&tree, after.clone(), Timestamp(4)).await,
            [(-1, events[1].iid), (9, events[4].iid)],
        );
        let address = AddressRef {
            iid: events[4].iid,
            key: events[4].entity_key.as_deref().unwrap(),
        }
        .owned();
        let spans = tree.spans(&after, &address).await?;
        assert_eq!(spans[0].tuple.view().cell(1), AtomRef::Binary(&[1, 2]));
        assert_eq!(spans[0].tuple.view().cell(2), AtomRef::Null);
        Ok(())
    }

    #[tokio::test]
    async fn a_correction_retracts_the_original_bucket_and_a_pin_keeps_its_root() {
        let directory = ObjectKey::new("index").unwrap();
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 8 << 20));
        let memory = MemoryBudget::new(32 << 20);
        let tree = Tree::new(&storage, &directory, &memory, 0).await.unwrap();
        let token = CancellationToken::new();
        let mut roots = RevisionRoots::default();
        let original = put(1, 0, Interval::from_micros(0, 16).unwrap(), 90);
        let iid = original.iid;
        roots
            .apply(
                &tree,
                changes(&[original], &memory, &token),
                EventOrder::OldestFirst,
                &token,
            )
            .await
            .unwrap();
        let (_, pin) = roots.at(&tree, IndexRevision::through_time(Timestamp(0))).await.unwrap().unwrap();
        let revised = put(1, 1, Interval::from_micros(4, 8).unwrap(), 5);
        roots
            .apply(
                &tree,
                changes(&[revised], &memory, &token),
                EventOrder::OldestFirst,
                &token,
            )
            .await
            .unwrap();
        let (_, root) = roots.at(&tree, IndexRevision::through_time(Timestamp(1))).await.unwrap().unwrap();
        for (valid, expected) in [(3, 90), (4, 5), (7, 5), (8, 90), (15, 90)] {
            assert_eq!(
                keys(&tree, root.clone(), Timestamp(valid)).await,
                vec![(expected, iid)]
            );
        }
        assert!(keys(&tree, root, Timestamp(16)).await.is_empty());
        assert_eq!(keys(&tree, pin, Timestamp(4)).await, vec![(90, iid)]);
    }

    #[tokio::test]
    async fn an_ordered_cut_selects_one_of_two_tied_transactions() {
        use rtdb_types::{EventPosition, TxId};

        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 8 << 20));
        let memory = MemoryBudget::new(32 << 20);
        let directory = ObjectKey::new("tied-revisions").unwrap();
        let tree = Tree::new(&storage, &directory, &memory, 0).await.unwrap();
        let token = CancellationToken::new();
        let mut roots = RevisionRoots::default();
        let mut first = put(1, 40, Interval::ALL, 2);
        let mut second = put(1, 40, Interval::ALL, 9);
        first.position = Some(EventPosition { transaction: TxId(10), ordinal: 0 });
        second.position = Some(EventPosition { transaction: TxId(11), ordinal: 0 });
        let iid = first.iid;

        roots.apply(&tree, changes(&[second, first], &memory, &token), EventOrder::NewestFirst, &token)
            .await.unwrap();
        roots.archived = super::super::checkpoint::store_roots(&tree, 0, &roots, &token).await.unwrap();
        roots.clear_changes();
        let captured = IndexRevision { time: Timestamp(40), transaction: 10 };
        let (_, early) = roots.at(&tree, captured).await.unwrap().unwrap();
        let (_, late) = roots.at(&tree, IndexRevision::through_time(Timestamp(40))).await.unwrap().unwrap();
        assert_eq!(keys(&tree, early, Timestamp(0)).await, vec![(2, iid)]);
        assert_eq!(keys(&tree, late, Timestamp(0)).await, vec![(9, iid)]);
    }

    #[tokio::test]
    async fn an_erase_removes_only_the_complete_key_at_a_shared_iid() {
        use rtdb_types::EntityId;

        let directory = ObjectKey::new("shared-iid-index").unwrap();
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 8 << 20));
        let memory = MemoryBudget::new(32 << 20);
        let tree = Tree::new(&storage, &directory, &memory, 0).await.unwrap();
        let token = CancellationToken::new();
        let mut roots = RevisionRoots::default();
        let mut first = put(1, 0, Interval::ALL, 2);
        let iid = first.iid;
        first.entity_key = Some(EntityId::Integer(1).handle_bytes().into());
        let peer_id = EntityId::Uuid(iid.0);
        let mut peer = put(1, 0, Interval::ALL, 9);
        let StoredOperation::Put(row) = &mut peer.operation else { unreachable!(); };
        row.insert("_id".into(), peer_id.as_value());
        peer.entity_key = Some(peer_id.handle_bytes().into());
        let erase = StoredEvent { position: None,
            origin: None,
            commit_position: 0,
            entity_key: first.entity_key.clone(), iid, system_from: Timestamp(1),
            valid_time: Interval::ALL, operation: StoredOperation::Erase, recency: None,
        };
        roots.apply(&tree, changes(&[first, peer], &memory, &token), EventOrder::OldestFirst, &token)
            .await.unwrap();
        let (_, original) = roots.at(&tree, IndexRevision::through_time(Timestamp(0))).await.unwrap().unwrap();
        assert_eq!(keys(&tree, original, Timestamp(0)).await, vec![(2, iid), (9, iid)]);
        roots.apply(&tree, changes(&[erase], &memory, &token), EventOrder::OldestFirst, &token)
            .await.unwrap();
        for at in [0, 1] {
            let (_, root) = roots.at(&tree, IndexRevision::through_time(Timestamp(at))).await.unwrap().unwrap();
            assert_eq!(keys(&tree, root, Timestamp(0)).await, vec![(9, iid)]);
        }
    }

    #[tokio::test]
    async fn an_erasure_scrubs_older_revisions_and_respects_commit_order() {
        let directory = ObjectKey::new("index").unwrap();
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 8 << 20));
        let memory = MemoryBudget::new(32 << 20);
        let tree = Tree::new(&storage, &directory, &memory, 0).await.unwrap();
        let token = CancellationToken::new();
        let mut roots = RevisionRoots::default();
        let original = put(1, 0, Interval::ALL, 2);
        let iid = original.iid;
        roots
            .apply(
                &tree,
                changes(&[original], &memory, &token),
                EventOrder::OldestFirst,
                &token,
            )
            .await
            .unwrap();
        let erase = StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None,
            iid,
            system_from: Timestamp(1),
            valid_time: Interval::ALL,
            operation: StoredOperation::Erase,
            recency: None,
        };
        let after = put(1, 1, Interval::from_micros(5, 10).unwrap(), 3);
        roots
            .apply(
                &tree,
                changes(&[erase, after], &memory, &token),
                EventOrder::OldestFirst,
                &token,
            )
            .await
            .unwrap();
        let (_, old) = roots.at(&tree, IndexRevision::through_time(Timestamp(0))).await.unwrap().unwrap();
        let (_, new) = roots.at(&tree, IndexRevision::through_time(Timestamp(1))).await.unwrap().unwrap();
        assert!(keys(&tree, old, Timestamp(6)).await.is_empty());
        assert!(keys(&tree, new.clone(), Timestamp(4)).await.is_empty());
        assert_eq!(keys(&tree, new, Timestamp(6)).await, vec![(3, iid)]);
    }
}
