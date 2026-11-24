use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use bytes::Bytes;
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation};
use rtdb_format::data::{EncodedEvent, EventBatch, PageEvent};
use rtdb_temporal::HistoryResolver;
use rtdb_types::{Anomaly, EventHeader, EventOp, Iid, Interval, LogicalType, Result, TableRef, TemporalBounds, Timestamp, Value, WorldShadow};
use tokio_util::sync::CancellationToken;

use super::{
    format_error,
    key::{AtomRef, Bucket},
    maintenance::cancelled,
    postings::{BatchKind, KeyBatch, KeyColumn, KeyRow, ProjectedKeys, Tuple},
    tree::{Address, AddressRef, Cursor, Hit, IndexRevision, Item, Key, KeyRef, Link, Root, Span, Tree},
};

/// The roots for the partition's ordered history, with erasures applied to every affected root.
#[derive(Default)]
pub(super) struct RevisionRoots {
    pub archived: Root,
    pub changes: BTreeMap<IndexRevision, Root>,
}

impl RevisionRoots {
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
            self.changes.entry(time).or_insert(root);
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
        for (revision, rows) in changes.revisions {
            if last.is_some_and(|last| last > revision) {
                return Err(format_error(
                    "Ordered index replay crossed its revision frontier",
                ));
            }
            let mut edits = BTreeMap::new();
            let mut cut_at_revision = false;
            for (address, mut events) in rows {
                if cancellation.is_cancelled() {
                    return Err(cancelled());
                }
                if matches!(order, EventOrder::OldestFirst) {
                    events.reverse();
                }
                if events.iter().any(|event| event.header.position.is_some() || event.header.commit_position != 0) {
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
                            let mut removals = BTreeMap::new();
                            project(&mut removals, &address, &spans, Arc::from([]));
                            *historical = tree
                                .edit(historical.clone(), removals.into_iter().collect())
                                .await?;
                        }
                    }
                }
                if before.as_ref() != after.as_slice() {
                    project(&mut edits, &address, &before, after.into());
                }
            }
            if cut_at_revision || !edits.is_empty() {
                root = tree.edit(root, edits.into_iter().collect()).await?;
                self.changes.insert(revision, root.clone());
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
) -> Result<Vec<Span>> {
    let bounds = TemporalBounds {
        valid_time: Interval::ALL,
        system_time: Interval::at(time)?,
    };
    let mut resolver = HistoryResolver::new_in(bounds, false, Arc::clone(memory));
    let mut after = Vec::new();
    for event in events {
        resolver.resolve_fragments(&event.header, |valid, _| {
            if let Some(tuple) = &event.tuple {
                after.push(Span {
                    valid,
                    tuple: tuple.clone(),
                });
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
            after.push(Span {
                valid,
                tuple: span.tuple.clone(),
            });
            Ok(())
        })?;
    }
    after.sort_unstable_by_key(|span| span.valid);
    let mut coalesced: Vec<Span> = Vec::new();
    for span in after {
        if let Some(previous) = coalesced.last_mut()
            && previous.valid.upper() == span.valid.lower()
            && previous.tuple == span.tuple
        {
            previous.valid = Interval::new(previous.valid.lower(), span.valid.upper())?;
        } else {
            coalesced.push(span);
        }
    }
    Ok(coalesced)
}

fn project(edits: &mut BTreeMap<Key, Option<Item>>, address: &Address, before: &[Span], after: Arc<[Span]>) {
    let postings = |spans: &[Span]| -> BTreeSet<Key> {
        spans
            .iter()
            .flat_map(|span| {
                Bucket::cover(span.valid).map(|bucket| Key::Posting {
                    bucket,
                    tuple: span.tuple.clone(),
                    address: address.clone(),
                })
            })
            .collect()
    };
    let old = postings(before);
    let new = postings(&after);
    for key in old.difference(&new) {
        edits.insert(key.clone(), None);
    }
    for key in new.difference(&old) {
        edits.insert(key.clone(), Some(Item::Posting));
    }
    let spans = (!after.is_empty()).then_some(Item::Row(after));
    edits.insert(Key::Row(address.clone()), spans);
}

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
}

impl Changes {
    pub fn is_empty(&self) -> bool { self.revisions.is_empty() }

    /// A read keeps only its selected root. Later erasures still remove older facts from it.
    pub async fn read(
        &self,
        tree: &Tree,
        mut root: Root,
        through: IndexRevision,
        order: EventOrder,
        cancellation: &CancellationToken,
    ) -> Result<Root> {
        for (&revision, rows) in &self.revisions {
            let mut edits = BTreeMap::new();
            for (address, events) in rows {
                if cancellation.is_cancelled() {
                    return Err(cancelled());
                }
                if revision > through && !events.iter().any(|event| event.header.op == EventOp::Erase)
                {
                    continue;
                }
                let before = tree.spans(&root, address).await?;
                let after = if revision > through {
                    Vec::new()
                } else if events.iter().any(|event| event.header.position.is_some() || event.header.commit_position != 0) {
                    let _scratch = tree.memory.reserve(events.len().saturating_mul(2 * size_of::<&Event>()))?;
                    let mut ordered: Vec<_> = events.iter().collect();
                    if matches!(order, EventOrder::OldestFirst) {
                        ordered.reverse();
                    }
                    ordered.sort_by_key(|event| std::cmp::Reverse(event.header.precedence()));
                    resolve(address.iid, revision.time, &before, ordered, &tree.memory)?
                } else if matches!(order, EventOrder::OldestFirst) {
                    resolve(address.iid, revision.time, &before, events.iter().rev(), &tree.memory)?
                } else {
                    resolve(address.iid, revision.time, &before, events, &tree.memory)?
                };
                if before.as_ref() != after.as_slice() {
                    project(&mut edits, address, &before, after.into());
                }
            }
            root = tree.edit(root, edits.into_iter().collect()).await?;
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
        let _headers = memory.reserve(batch.len().saturating_mul(256))?;
        let events = batch.page_events()?;
        let document = batch.put_document()?;
        let column = |name: &str| document.and_then(|document| document.fields().iter().zip(document.columns())
            .find(|(field, _)| field.name() == name))
            .map(|(field, array)| (field.as_ref(), array.as_ref()));
        let projection: Vec<_> = columns.iter().map(|name| KeyColumn::new(column(name))).collect();
        let identity = keyed.then(|| column("_id")).flatten();
        let marker = matches!(layout, KeyLayout::World).then(|| column(WorldShadow::FIELDS)).flatten();
        for events in events[skip.min(events.len())..].chunks(128) {
            let _scratch = memory.reserve(
                events
                    .len()
                    .saturating_mul(columns.len())
                    .saturating_mul(size_of::<AtomRef<'_>>())
                    .saturating_add(8192)
                    .saturating_add(if keyed {
                        events.len().saturating_mul(128)
                    } else {
                        0
                    }),
            )?;
            let mut retained = Vec::new();
            let mut rows = Vec::new();
            for event in events {
                if cancellation.is_cancelled() {
                    return Err(cancelled());
                }
                let header = event.header();
                let address = AddressRef { iid: header.iid, key: event.entity_key().unwrap_or_default() };
                let _key = memory.reserve(address.key.len().saturating_add(128))?;
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
                        let matches = match rtdb_columnar::read_entity_id(field, array, offset) {
                            Ok(borrowed) => borrowed == id,
                            Err(_) => event.put_cell("_id", &LogicalType::Null)?.entity_id()? == id.into_owned(),
                        };
                        if !matches {
                            return Err(format_error("Governed ordered put differs from its physical identity"));
                        }
                    }
                    if matches!(layout, KeyLayout::World) && world_tombstone(event, marker, offset, memory)? {
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
            let mut address_keys = BTreeMap::new();
            for (header, address, row) in retained {
                let allocation = memory.reserve(address.key.len().saturating_add(256))?;
                let key = if address.key.is_empty() {
                    Bytes::new()
                } else {
                    address_keys
                        .entry(address.key)
                        .or_insert_with(|| Bytes::copy_from_slice(address.key))
                        .clone()
                };
                self.revisions
                    .entry(IndexRevision::of_event(header))
                    .or_default()
                    .entry(Address::new(address.iid, key))
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
        roots.changes.clear();
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
