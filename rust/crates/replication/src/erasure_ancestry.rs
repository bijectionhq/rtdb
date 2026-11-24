//! Event identities carried by a verified fork checkpoint and its copied suffix.

use std::{collections::{BTreeMap, BTreeSet}, io::Cursor, sync::Arc};

use bytes::{BufMut, Bytes, BytesMut};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{data::EventBatch, ipc::{ArrowFile, inspect_file_reserved}, log::proto::ResolvedTx};
use rtdb_types::{Anomaly, BlockIndex, EventOrigin, EventPosition, Iid, Result, TableRef};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventCoordinate {
    pub subject: [u8; 32],
    pub position: EventPosition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForkCoordinates {
    pub parent: EventCoordinate,
    pub child: EventCoordinate,
}

#[derive(Clone, Copy)]
pub enum ForkSide { Parent, Child }

pub type InheritedEvents = BTreeMap<EventOrigin, ForkCoordinates>;
const COORDINATE: usize = EventPosition::WIDTH as usize + 32;
const ENTRY: usize = EventOrigin::WIDTH as usize + 2 * COORDINATE;

pub fn erasure_subject(table: &TableRef, iid: Iid) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"rtdb/erasure-subject/v1\0");
    for name in [&table.schema, &table.table] {
        hash.update((name.len() as u64).to_be_bytes());
        hash.update(name.as_bytes());
    }
    hash.update(iid.0);
    hash.finalize().into()
}

pub struct ForkEvents {
    entries: InheritedEvents,
    budget: Arc<MemoryBudget>,
    reservations: Vec<Arc<MemoryReservation>>,
}

impl ForkEvents {
    pub fn new(budget: Arc<MemoryBudget>) -> Self {
        Self { entries: BTreeMap::new(), budget, reservations: Vec::new() }
    }

    pub fn budget(&self) -> &Arc<MemoryBudget> { &self.budget }

    pub fn add_trie(&mut self, table: &TableRef, body: Bytes) -> Result<()> {
        let mut file = Cursor::new(body);
        let layout = inspect_file_reserved(&mut file, |bytes| self.budget.reserve(bytes))?;
        let _metadata = self.budget.reserve(layout.opening_allocation_bound()?)?;
        let mut file = ArrowFile::from_inspected_reader(file, None, layout)?;
        for page in 0..file.page_count() {
            let _decode = self.budget.reserve(file.page_allocation_bound(page)?)?;
            self.add_batch(table, &EventBatch::new(file.read_page(page)?)?)?;
        }
        Ok(())
    }

    pub fn add_replica(&mut self, message: &ResolvedTx) -> Result<()> {
        let decoded = rtdb_tx::decode_replica(message, None, BlockIndex(0), &self.budget)?;
        for (table, write) in decoded.writes.tables() {
            if table == &TableRef::new("xt", rtdb_types::PLACED_ATTEMPT_OUTCOME_TABLE) {
                continue;
            }
            if let Some(batch) = write.batch() { self.add_batch(table, batch.view())?; }
        }
        Ok(())
    }

    fn add_batch(&mut self, table: &TableRef, batch: &EventBatch) -> Result<()> {
        for row in 0..batch.len() {
            let origin = batch.origin(row)?.ok_or_else(missing)?;
            let header = batch.header(row)?;
            let coordinate = EventCoordinate {
                subject: erasure_subject(table, header.iid),
                position: header.position.ok_or_else(missing)?,
            };
            let coordinates = ForkCoordinates { parent: coordinate, child: coordinate };
            match self.entries.entry(origin) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    self.reservations.push(self.budget.reserve(ENTRY * 4)?);
                    entry.insert(coordinates);
                }
                std::collections::btree_map::Entry::Occupied(entry) if *entry.get() == coordinates => (),
                std::collections::btree_map::Entry::Occupied(_) => return Err(missing()),
            }
        }
        Ok(())
    }

    pub fn pair_with_parent(mut self, parent: &Self) -> Result<Self> {
        for (origin, pair) in &mut self.entries {
            pair.parent = parent.entries.get(origin).ok_or_else(missing)?.parent;
        }
        Ok(self)
    }

    pub fn encode(self, maximum: usize) -> Result<Bytes> {
        let length = fork_event_size(self.entries.len(), maximum)?;
        let charge = self.budget.reserve(length)?;
        let bytes = encode_fork_events(&self.entries, maximum)?;
        Ok(Bytes::from_owner(ChargedEvents { bytes, _charge: charge }))
    }
}

struct ChargedEvents {
    bytes: Bytes,
    _charge: Arc<MemoryReservation>,
}

impl AsRef<[u8]> for ChargedEvents {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}

fn fork_event_size(count: usize, maximum: usize) -> Result<usize> {
    count.checked_mul(ENTRY).and_then(|length| length.checked_add(8))
        .filter(|length| *length <= maximum).ok_or_else(missing)
}

pub fn encode_fork_events(entries: &InheritedEvents, maximum: usize) -> Result<Bytes> {
    let mut body = BytesMut::with_capacity(fork_event_size(entries.len(), maximum)?);
    body.put_u64(entries.len() as u64);
    for (origin, coordinates) in entries {
        body.extend_from_slice(&origin.0);
        for coordinate in [coordinates.parent, coordinates.child] {
            body.extend_from_slice(&coordinate.subject);
            body.extend_from_slice(&coordinate.position.to_bytes());
        }
    }
    Ok(body.freeze())
}

pub fn decode_fork_events(bytes: &[u8]) -> Result<InheritedEvents> {
    let (count, entries) = bytes.split_first_chunk::<8>().ok_or_else(missing)?;
    let count = usize::try_from(u64::from_be_bytes(*count)).map_err(|_| missing())?;
    if count.checked_mul(ENTRY) != Some(entries.len()) { return Err(missing()); }
    let mut map = BTreeMap::new();
    let mut previous = None;
    for entry in entries.chunks_exact(ENTRY) {
        let (origin, coordinates) = entry.split_first_chunk::<32>().ok_or_else(missing)?;
        let (parent, child) = coordinates.split_at(COORDINATE);
        let coordinate = |bytes: &[u8]| {
            let (subject, position) = bytes.split_first_chunk::<32>().ok_or_else(missing)?;
            Ok::<_, Anomaly>(EventCoordinate {
                subject: *subject, position: EventPosition::from_bytes(position.try_into().map_err(|_| missing())?),
            })
        };
        let origin = EventOrigin(*origin);
        if previous.is_some_and(|prior| prior >= origin) { return Err(missing()); }
        map.insert(origin, ForkCoordinates { parent: coordinate(parent)?, child: coordinate(child)? });
        previous = Some(origin);
    }
    Ok(map)
}

/// Selects the origins that crossed both roots' authenticated fork boundaries.
pub fn project_erasure_origins<'a>(
    targets: &BTreeMap<[u8; 32], EventPosition>,
    source: impl IntoIterator<Item = (&'a InheritedEvents, ForkSide)>,
    dependent: impl IntoIterator<Item = &'a InheritedEvents>,
) -> BTreeSet<EventOrigin> {
    let removed: BTreeSet<_> = source.into_iter().flat_map(|(edges, side)| edges.iter().filter_map(move |(origin, event)| {
        let event = match side { ForkSide::Parent => event.parent, ForkSide::Child => event.child };
        targets.get(&event.subject).is_some_and(|cut| event.position < *cut).then_some(*origin)
    })).collect();
    dependent.into_iter().flatten().filter_map(|(origin, _)| removed.contains(origin).then_some(*origin)).collect()
}

fn missing() -> Anomaly {
    Anomaly::incorrect("rtdb.multi/fork-event-ancestry", "The inherited event inventory is incomplete or inconsistent")
}


#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::TxId;

    #[test]
    fn erasure_joins_origins_across_histories_and_preserves_branch_writes() {
        let before = EventPosition { transaction: TxId(5), ordinal: 1 };
        let after = EventPosition { transaction: TxId(9), ordinal: 0 };
        let boundary = EventPosition { transaction: TxId(7), ordinal: 0 };
        let subject = [1; 32];
        let inherited = EventOrigin([4; 32]);
        let own_write = EventOrigin([5; 32]);
        let later = EventOrigin([6; 32]);
        let same_coordinate = EventCoordinate { subject, position: before };
        let later_coordinate = EventCoordinate { subject, position: after };
        let source = BTreeMap::from([
            (inherited, ForkCoordinates { parent: later_coordinate, child: same_coordinate }),
            (later, ForkCoordinates { parent: same_coordinate, child: later_coordinate }),
        ]);
        let destination = BTreeMap::from([
            (inherited, ForkCoordinates { parent: same_coordinate, child: later_coordinate }),
            (later, ForkCoordinates { parent: same_coordinate, child: same_coordinate }),
            (own_write, ForkCoordinates { parent: same_coordinate, child: same_coordinate }),
        ]);
        let body = encode_fork_events(&source, 4096).unwrap();
        assert_eq!(decode_fork_events(&body).unwrap(), source);
        let result = project_erasure_origins(&BTreeMap::from([(subject, boundary)]), [(&source, ForkSide::Child)], [&destination]);
        assert_eq!(result, BTreeSet::from([inherited]));
    }
}
