use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use rtdb_columnar::{MemoryBudget, MemoryReservation, estimate_read_cell_bytes};
use rtdb_format::data::{EncodedEvent, EventBatch, PutColumn};
use rtdb_temporal::HistoryResolver;
use rtdb_types::{
    EventHeader, EventOp, Iid, Interval, LogicalType, Result, TemporalBounds, Timestamp,
};
use tokio_util::sync::CancellationToken;

use super::{
    format_error,
    key::{Atom, Bucket},
    maintenance::cancelled,
    tree::{Cursor, Entry, Item, Key, Link, Root, Span, Tree},
};

/// The roots for the partition's system-time points, with erasures applied to every affected root.
#[derive(Default)]
pub(super) struct RevisionRoots {
    pub archived: Root,
    pub changes: BTreeMap<Timestamp, Root>,
}

impl RevisionRoots {
    pub async fn at(
        &self,
        tree: &Tree<'_>,
        system_from: Timestamp,
    ) -> Result<Option<(Timestamp, Root)>> {
        if let Some((&time, root)) = self.changes.range(..=system_from).next_back() {
            return Ok(Some((time, root.clone())));
        }
        let mut cursor = Cursor::new(self.archived.clone(), true);
        cursor
            .next(tree, |key| match key {
                Key::Revision(time) if *time <= system_from => Ordering::Equal,
                Key::Revision(_) => Ordering::Greater,
                Key::Posting { .. } | Key::Row(_) => Ordering::Less,
            })
            .await?
            .map(archived_revision)
            .transpose()
    }

    async fn stage_archived(
        &mut self,
        tree: &Tree<'_>,
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
        tree: &Tree<'_>,
        changes: Changes,
        order: EventOrder,
        cancellation: &CancellationToken,
    ) -> Result<bool> {
        if changes.revisions.is_empty() {
            return Ok(false);
        }
        let (mut last, mut root) = self
            .at(tree, Timestamp::MAX)
            .await?
            .map_or((Timestamp::MIN, None), |(time, root)| (time, root));
        let mut erased = false;
        for (time, rows) in changes.revisions {
            if last > time {
                return Err(format_error(
                    "Ordered index replay crossed its revision frontier",
                ));
            }
            let mut edits = BTreeMap::new();
            let mut cut_at_revision = false;
            for (iid, mut events) in rows {
                if cancellation.is_cancelled() {
                    return Err(cancelled());
                }
                if matches!(order, EventOrder::OldestFirst) {
                    events.reverse();
                }
                let before = tree.spans(&root, iid).await?;
                let cut = events.iter().any(|event| event.header.op == EventOp::Erase);
                let after = resolve(iid, time, &before, &events, tree.memory)?;
                if cut {
                    erased = true;
                    cut_at_revision = true;
                    self.stage_archived(tree, cancellation).await?;
                    for (_, historical) in self.changes.range_mut(..time) {
                        let spans = tree.spans(historical, iid).await?;
                        if !spans.is_empty() {
                            let mut removals = BTreeMap::new();
                            project(&mut removals, iid, &spans, Arc::from([]));
                            *historical = tree
                                .edit(historical.clone(), removals.into_iter().collect())
                                .await?;
                        }
                    }
                }
                if before.as_ref() != after.as_slice() {
                    project(&mut edits, iid, &before, after.into());
                }
            }
            if cut_at_revision || !edits.is_empty() {
                root = tree.edit(root, edits.into_iter().collect()).await?;
                self.changes.insert(time, root.clone());
            }
            last = time;
        }
        Ok(erased)
    }
}

fn archived_revision(entry: Entry) -> Result<(Timestamp, Root)> {
    match (entry.key, entry.item) {
        (Key::Revision(time), Item::Revision(pointer)) => Ok((time, pointer.map(Link::Stored))),
        _ => Err(format_error(
            "Ordered index revision directory contains a row or posting",
        )),
    }
}

fn resolve(
    iid: Iid,
    time: Timestamp,
    before: &[Span],
    events: &[Event],
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
                    tuple: Arc::clone(tuple),
                });
            }
            Ok(())
        })?;
    }
    // The previous projection contains exactly the older intervals that can still win.
    for span in before {
        let header = EventHeader {
            iid,
            system_from: Timestamp::MIN,
            valid_time: span.valid,
            op: EventOp::Put,
        };
        resolver.resolve_fragments(&header, |valid, _| {
            after.push(Span {
                valid,
                tuple: Arc::clone(&span.tuple),
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

fn project(edits: &mut BTreeMap<Key, Option<Item>>, iid: Iid, before: &[Span], after: Arc<[Span]>) {
    let postings = |spans: &[Span]| -> BTreeSet<Key> {
        spans
            .iter()
            .flat_map(|span| {
                Bucket::cover(span.valid).map(|bucket| Key::Posting {
                    bucket,
                    tuple: Arc::clone(&span.tuple),
                    iid,
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
    edits.insert(Key::Row(iid), spans);
}

pub(super) enum EventOrder {
    OldestFirst,
    NewestFirst,
}

struct Event {
    header: EventHeader,
    tuple: Option<Arc<[Atom]>>,
    _allocation: Arc<MemoryReservation>,
}

#[derive(Default)]
pub(super) struct Changes {
    revisions: BTreeMap<Timestamp, BTreeMap<Iid, Vec<Event>>>,
}

impl Changes {
    /// A read keeps only its selected root. Later erasures still remove older facts from it.
    pub async fn read(
        self,
        tree: &Tree<'_>,
        mut root: Root,
        system_at: Timestamp,
        order: EventOrder,
        cancellation: &CancellationToken,
    ) -> Result<Root> {
        for (time, rows) in self.revisions {
            let mut edits = BTreeMap::new();
            for (iid, mut events) in rows {
                if cancellation.is_cancelled() {
                    return Err(cancelled());
                }
                if time > system_at && !events.iter().any(|event| event.header.op == EventOp::Erase)
                {
                    continue;
                }
                let before = tree.spans(&root, iid).await?;
                let after = if time > system_at {
                    Vec::new()
                } else {
                    if matches!(order, EventOrder::OldestFirst) {
                        events.reverse();
                    }
                    resolve(iid, time, &before, &events, tree.memory)?
                };
                if before.as_ref() != after.as_slice() {
                    project(&mut edits, iid, &before, after.into());
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
    ) -> Result<()> {
        let _headers = memory.reserve(batch.len().saturating_mul(256))?;
        for event in batch.page_events()?.into_iter().skip(skip) {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let header = event.header();
            let bytes = if header.op == EventOp::Put {
                columns.iter().try_fold(256usize, |bytes, name| {
                    let cell = match event.put_column(name) {
                        Some(PutColumn::Stored(field, array, row)) => {
                            estimate_read_cell_bytes(field, array, row)?
                        }
                        Some(PutColumn::Absent) | None => 0,
                    };
                    Ok::<_, rtdb_types::Anomaly>(
                        bytes
                            .saturating_add(cell.saturating_mul(3))
                            .saturating_add(size_of::<Atom>()),
                    )
                })?
            } else {
                256
            };
            let allocation = memory.reserve(bytes)?;
            let tuple = if header.op == EventOp::Put {
                let values = columns
                    .iter()
                    .map(|name| {
                        let value = event.put_cell(name, &LogicalType::Null)?;
                        Ok(Atom::of(&value).unwrap_or(Atom::Unordered))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Some(values.into())
            } else {
                None
            };
            self.revisions
                .entry(header.system_from)
                .or_default()
                .entry(header.iid)
                .or_default()
                .push(Event {
                    header,
                    tuple,
                    _allocation: allocation,
                });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_index::{IndexBatch, StoredEvent, StoredOperation};
    use rtdb_storage::{MemoryStore, ObjectKey, cache::BufferPool};
    use rtdb_types::{StructFields, Value};

    fn put(id: i64, system: i64, valid: Interval, key: i64) -> StoredEvent {
        StoredEvent {
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
            .add_batch(batch.view(), 0, &["key".into()], memory, cancellation)
            .unwrap();
        changes
    }

    async fn keys(tree: &Tree<'_>, root: Root, valid: Timestamp) -> Vec<(i128, Iid)> {
        let mut keys = Vec::new();
        for bucket in Bucket::at(valid) {
            let mut cursor = Cursor::new(root.clone(), false);
            while let Some(entry) = cursor
                .next(tree, |key| match key {
                    Key::Posting {
                        bucket: candidate, ..
                    } => candidate.cmp(&bucket),
                    Key::Row(_) | Key::Revision(_) => Ordering::Greater,
                })
                .await
                .unwrap()
            {
                let Key::Posting { tuple, iid, .. } = entry.key else {
                    panic!("Expected posting");
                };
                let [Atom::Integer(value)] = tuple.as_ref() else {
                    panic!("Expected integer");
                };
                keys.push((*value, iid));
            }
        }
        keys.sort_unstable();
        keys
    }

    #[tokio::test]
    async fn a_correction_retracts_the_original_bucket_and_a_pin_keeps_its_root() {
        let directory = ObjectKey::new("index").unwrap();
        let storage = BufferPool::new(Arc::new(MemoryStore::default()), 8 << 20);
        let memory = MemoryBudget::new(32 << 20);
        let tree = Tree {
            storage: &storage,
            directory: &directory,
            memory: &memory,
        };
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
        let (_, pin) = roots.at(&tree, Timestamp(0)).await.unwrap().unwrap();
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
        let (_, root) = roots.at(&tree, Timestamp(1)).await.unwrap().unwrap();
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
    async fn an_erasure_scrubs_older_revisions_and_respects_commit_order() {
        let directory = ObjectKey::new("index").unwrap();
        let storage = BufferPool::new(Arc::new(MemoryStore::default()), 8 << 20);
        let memory = MemoryBudget::new(32 << 20);
        let tree = Tree {
            storage: &storage,
            directory: &directory,
            memory: &memory,
        };
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
        let erase = StoredEvent {
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
        let (_, old) = roots.at(&tree, Timestamp(0)).await.unwrap().unwrap();
        let (_, new) = roots.at(&tree, Timestamp(1)).await.unwrap().unwrap();
        assert!(keys(&tree, old, Timestamp(6)).await.is_empty());
        assert!(keys(&tree, new.clone(), Timestamp(4)).await.is_empty());
        assert_eq!(keys(&tree, new, Timestamp(6)).await, vec![(3, iid)]);
    }
}
