use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::data::{StoredEvent, StoredOperation};
use rtdb_index::branches::BranchView;
use rtdb_types::{
    EntityId, EventHeader, Iid, Result, StructFields, TableRef, Timestamp, TransactionKey, TxId,
    Value,
};
use serde::{Deserialize, Serialize};

use super::{inconsistent, unavailable};
use crate::archive::encode;

const MAX_POINTS: usize = 2;
const MAX_POINT_BYTES: usize = 1 << 20;
const MAX_POINT_EVENTS: usize = 4096;

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PointSelector {
    pub(crate) table: TableRef,
    pub(crate) id: EntityId,
}

impl PointSelector {
    pub(super) fn valid(&self) -> Result<bool> {
        Ok(valid_table(&self.table) && encode(&self.id)?.len() <= 1024)
    }
}

pub(super) fn valid_table(table: &TableRef) -> bool {
    table.schema == "public"
        && !table.table.is_empty()
        && table.table.len() <= 255
        && !table.stores_expiring_transaction_metadata()
}

pub(crate) struct PointHistoryRead {
    valid_at: Timestamp,
    points: Vec<History>,
}

struct History {
    point: PointSelector,
    iid: Option<Iid>,
    events: Vec<PointEvent>,
    bytes: usize,
    charges: Vec<Arc<MemoryReservation>>,
}

#[derive(Serialize)]
pub(crate) struct PointHistories {
    pub(crate) valid_at: Timestamp,
    pub(crate) points: Vec<PointHistory>,
}

#[derive(Serialize)]
pub(crate) struct PointHistory {
    table: TableRef,
    id: EntityId,
    events: Vec<PointEvent>,
    effective_event: usize,
    #[serde(skip)]
    _charges: Vec<Arc<MemoryReservation>>,
}

#[derive(Serialize)]
struct PointEvent {
    written: TransactionKey,
    header: EventHeader,
    document: Option<StructFields<Value>>,
}

impl PointHistoryRead {
    pub(crate) fn new(valid_at: Timestamp, points: Vec<PointSelector>) -> Result<Self> {
        if !(1..=MAX_POINTS).contains(&points.len())
            || matches!(valid_at, Timestamp::MIN | Timestamp::MAX)
            || (points.len() == MAX_POINTS && points[0] == points[1])
        {
            return Err(invalid());
        }
        for point in &points {
            if !point.valid()? {
                return Err(invalid());
            }
        }
        Ok(Self {
            valid_at,
            points: points
                .into_iter()
                .map(|point| History {
                    point,
                    iid: None,
                    events: Vec::new(),
                    bytes: 0,
                    charges: Vec::new(),
                })
                .collect(),
        })
    }

    pub(crate) fn selects(&self, table: &TableRef) -> bool {
        self.points
            .iter()
            .any(|history| &history.point.table == table)
    }

    pub(super) fn selects_only(&self, first: &PointSelector, second: &PointSelector) -> bool {
        self.points
            .iter()
            .all(|history| &history.point == first || &history.point == second)
    }

    pub(super) fn observe(
        &mut self,
        table: &TableRef,
        tx: TxId,
        event: &StoredEvent,
        memory: &Arc<MemoryBudget>,
    ) -> Result<()> {
        for history in self
            .points
            .iter_mut()
            .filter(|history| &history.point.table == table)
        {
            history.observe(tx, event, memory)?;
        }
        Ok(())
    }

    pub(super) fn finish(
        self,
        view: &BranchView<'_>,
        cut: TransactionKey,
    ) -> Result<PointHistories> {
        let points = self
            .points
            .into_iter()
            .map(|history| history.finish(view, cut, self.valid_at))
            .collect::<Result<Vec<_>>>()?;
        if let [first, second] = points.as_slice()
            && first.events[first.effective_event].header.position
                == second.events[second.effective_event].header.position
        {
            return Err(invalid());
        }
        Ok(PointHistories {
            valid_at: self.valid_at,
            points,
        })
    }
}

impl History {
    fn observe(&mut self, tx: TxId, event: &StoredEvent, memory: &Arc<MemoryBudget>) -> Result<()> {
        let document = match &event.operation {
            StoredOperation::Put(row)
                if row
                    .get("_id")
                    .and_then(|value| value.entity_id().ok())
                    .as_ref()
                    == Some(&self.point.id) =>
            {
                if self.iid.is_some_and(|iid| iid != event.iid) {
                    return Err(invalid());
                }
                self.iid = Some(event.iid);
                Some(row.clone())
            }
            StoredOperation::Delete if self.iid == Some(event.iid) => None,
            _ => return Ok(()),
        };
        let entry = PointEvent {
            written: TransactionKey {
                tx_id: tx,
                system_time: event.system_from,
            },
            header: event.header(),
            document,
        };
        let length = encode(&entry)?.len();
        self.bytes = self
            .bytes
            .checked_add(length)
            .filter(|size| *size <= MAX_POINT_BYTES)
            .ok_or_else(oversized)?;
        if self.events.len() >= MAX_POINT_EVENTS {
            return Err(oversized());
        }
        self.charges
            .push(memory.reserve(length.checked_mul(4).ok_or_else(oversized)? + 256)?);
        self.events.push(entry);
        Ok(())
    }

    fn finish(
        mut self,
        view: &BranchView<'_>,
        cut: TransactionKey,
        valid_at: Timestamp,
    ) -> Result<PointHistory> {
        self.events.sort_by_key(|event| event.header.precedence());
        let effective_event = self
            .events
            .iter()
            .rposition(|event| event.header.valid_time.contains(valid_at))
            .ok_or_else(invalid)?;
        let effective = &self.events[effective_event];
        let row = view
            .row(&self.point.table, &self.point.id, valid_at)
            .ok_or_else(invalid)?;
        let image = row.image_at(valid_at).ok_or_else(invalid)?;
        if effective.written != cut
            || effective
                .header
                .position
                .is_none_or(|position| position.transaction != cut.tx_id)
            || row.cut != view.cut()
            || row.valid != effective.header.valid_time
            || image.custody_group.is_some()
            || image.custody_payload.is_some()
            || effective.document.as_ref() != Some(&image.cells)
        {
            return Err(invalid());
        }
        Ok(PointHistory {
            table: self.point.table,
            id: self.point.id,
            events: self.events,
            effective_event,
            _charges: self.charges,
        })
    }
}

fn invalid() -> rtdb_types::Anomaly {
    inconsistent(
        "The certified Classic replay does not prove this exact positioned application point",
    )
}

fn oversized() -> rtdb_types::Anomaly {
    unavailable("The certified Classic point history exceeds its reply bound")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_db::classic_import_basis;
    use rtdb_index::branches::{BranchCut, BranchIndex};
    use rtdb_types::{
        EventPosition, Interval,
        rtdb_v1::{At, Layer, MAIN_BRANCH},
    };

    #[test]
    fn effective_value_selects_the_positioned_put_covering_the_valid_instant() -> Result<()> {
        let table = TableRef::new("public", "controls");
        let id = EntityId::String("exact".into());
        let point = PointSelector {
            table: table.clone(),
            id: id.clone(),
        };
        let mut read = PointHistoryRead::new(Timestamp(15), vec![point.clone()])?;
        assert!(PointHistoryRead::new(Timestamp(15), vec![point.clone(), point]).is_err());
        let cut = TransactionKey {
            tx_id: TxId(3),
            system_time: Timestamp(100),
        };
        let memory = MemoryBudget::new(1 << 20);
        let mut index = BranchIndex::new([3; 16]);
        let basis = classic_import_basis([3; 16], cut.tx_id)?;
        index.advance_classic(BranchCut {
            basis,
            system_time: cut.system_time,
        })?;
        for (ordinal, lower, upper, binding) in [(1, 0, 20, 8), (6, 10, 20, 9), (7, 20, 40, 10)] {
            let raw = StoredEvent {
                position: Some(EventPosition {
                    transaction: cut.tx_id,
                    ordinal,
                }),
                origin: None,
                entity_key: None,
                iid: id.iid(),
                system_from: cut.system_time,
                commit_position: 0,
                valid_time: Interval::from_micros(lower, upper)?,
                recency: None,
                operation: StoredOperation::Put(StructFields::from([
                    ("_id".into(), id.as_value()),
                    ("binding".into(), Value::Binary(vec![binding])),
                ])),
            };
            read.observe(&table, cut.tx_id, &raw, &memory)?;
            index.apply_classic_event(&table, raw, Some(cut.tx_id))?;
        }
        let view = index.view(
            MAIN_BRANCH,
            At::Exactly(basis),
            Layer::Local,
            cut.system_time,
        )?;
        let observed = read.finish(&view, cut)?;
        let history = &observed.points[0];
        assert_eq!(history.events.len(), 3);
        assert_eq!(history.effective_event, 1);
        let selected = &history.events[history.effective_event];
        assert_eq!(selected.written, cut);
        assert_eq!(selected.header.position.unwrap().ordinal, 6);
        assert_eq!(
            selected.document.as_ref().unwrap().get("binding"),
            Some(&Value::Binary(vec![9]))
        );
        Ok(())
    }
}
