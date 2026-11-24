use std::sync::Arc;

use rtdb_format::data::{StoredEvent, StoredOperation};
use rtdb_types::{
    EntityId, EventHeader, Iid, MemoryReservation, Result, StructFields, TableRef, Value,
};
use serde::Serialize;

use super::{RelationInventory, inconsistent, unavailable};

const MAX_RECEIPT_HISTORY_BYTES: usize = 1 << 20;

pub(crate) struct ReceiptLookup {
    table: TableRef,
    id: EntityId,
    iid: Iid,
    history: Vec<ReceiptEvent>,
    table_sha256: Option<[u8; 32]>,
    bytes: usize,
    charges: Vec<Arc<MemoryReservation>>,
}

#[derive(Serialize)]
pub(crate) struct ReceiptHistory {
    table: TableRef,
    id: EntityId,
    table_sha256: Option<[u8; 32]>,
    events: Vec<ReceiptEvent>,
    #[serde(skip)]
    _charges: Vec<Arc<MemoryReservation>>,
}

#[derive(Serialize)]
struct ReceiptEvent {
    header: EventHeader,
    origin: Option<[u8; 32]>,
    entity_key: Option<Vec<u8>>,
    sha256: [u8; 32],
    document: Option<StructFields<Value>>,
}

pub(crate) struct ReceiptLookups {
    receipt: ReceiptLookup,
    coverage: Option<ReceiptLookup>,
}

pub(crate) struct ReceiptReadback {
    pub(crate) receipt: ReceiptHistory,
    pub(crate) coverage: Option<ReceiptHistory>,
}

impl ReceiptLookups {
    pub(crate) fn new(receipt: ReceiptLookup, coverage: Option<ReceiptLookup>) -> Result<Self> {
        if coverage
            .as_ref()
            .is_some_and(|coverage| coverage.table == receipt.table && coverage.iid == receipt.iid)
        {
            return Err(inconsistent(
                "Receipt coverage must select a distinct Original control",
            ));
        }
        Ok(Self { receipt, coverage })
    }

    pub(super) fn covers(&self, table: &TableRef) -> bool {
        &self.receipt.table == table
            || self
                .coverage
                .as_ref()
                .is_some_and(|coverage| &coverage.table == table)
    }

    pub(super) fn observe(
        &mut self,
        table: &TableRef,
        event: StoredEvent,
        sha256: [u8; 32],
        charge: Arc<MemoryReservation>,
    ) -> Result<()> {
        let iid = event.iid;
        if &self.receipt.table == table && self.receipt.iid == iid {
            self.receipt.observe(event, sha256, charge)?;
        } else if let Some(coverage) = &mut self.coverage
            && &coverage.table == table
            && coverage.iid == iid
        {
            coverage.observe(event, sha256, charge)?;
        }
        Ok(())
    }

    pub(super) fn observe_relation(&mut self, relation: &RelationInventory) {
        self.receipt.observe_relation(relation);
        if let Some(coverage) = &mut self.coverage {
            coverage.observe_relation(relation);
        }
    }

    pub(super) fn finish(self) -> ReceiptReadback {
        ReceiptReadback {
            receipt: self.receipt.finish(),
            coverage: self.coverage.map(ReceiptLookup::finish),
        }
    }
}

impl ReceiptLookup {
    pub(crate) fn new(table: TableRef, id: EntityId) -> Self {
        Self {
            table,
            iid: id.iid(),
            id,
            history: Vec::new(),
            table_sha256: None,
            bytes: 0,
            charges: Vec::new(),
        }
    }

    fn observe(
        &mut self,
        event: StoredEvent,
        sha256: [u8; 32],
        charge: Arc<MemoryReservation>,
    ) -> Result<()> {
        let header = event.header();
        let document = match event.operation {
            StoredOperation::Put(document) => {
                if document
                    .get("_id")
                    .and_then(|value| value.entity_id().ok())
                    .as_ref()
                    != Some(&self.id)
                {
                    return Err(inconsistent(
                        "The retained outcome has another original identity",
                    ));
                }
                Some(document)
            }
            StoredOperation::Delete | StoredOperation::Erase | StoredOperation::Redacted => None,
        };
        let event = ReceiptEvent {
            header,
            origin: event.origin.map(|origin| origin.0),
            entity_key: event.entity_key.as_deref().map(<[u8]>::to_vec),
            sha256,
            document,
        };
        let bytes = crate::archive::encode(&event)?.len();
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|size| *size <= MAX_RECEIPT_HISTORY_BYTES)
            .ok_or_else(|| {
                unavailable("The exact retained receipt history exceeds the reply bound")
            })?;
        self.history.push(event);
        self.charges.push(charge);
        Ok(())
    }

    fn observe_relation(&mut self, relation: &RelationInventory) {
        if self.table == relation.table {
            self.table_sha256 = Some(relation.sha256);
        }
    }

    fn finish(mut self) -> ReceiptHistory {
        self.history.sort_by_key(|event| event.header.precedence());
        ReceiptHistory {
            table: self.table,
            id: self.id,
            table_sha256: self.table_sha256,
            events: self.history,
            _charges: self.charges,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_columnar::MemoryBudget;
    use rtdb_types::{EventPosition, Interval, Timestamp, TxId};

    #[test]
    fn paired_original_readback_keeps_each_key_and_its_own_ordered_stamp() -> Result<()> {
        let receipt_table = TableRef::new("public", "receipts");
        let coverage_table = TableRef::new("public", "admission");
        let receipt_id = EntityId::String("retained-key".into());
        let coverage_id = EntityId::String("registered-coverage".into());
        let mut read = ReceiptLookups::new(
            ReceiptLookup::new(receipt_table.clone(), receipt_id.clone()),
            Some(ReceiptLookup::new(
                coverage_table.clone(),
                coverage_id.clone(),
            )),
        )?;
        let memory = MemoryBudget::new(1 << 20);
        let event = |id: &EntityId, transaction, time, field: (&str, Value)| StoredEvent {
            position: Some(EventPosition {
                transaction: TxId(transaction),
                ordinal: 4,
            }),
            origin: None,
            entity_key: None,
            iid: id.iid(),
            system_from: Timestamp(time),
            commit_position: 0,
            valid_time: Interval::ALL,
            recency: None,
            operation: StoredOperation::Put(StructFields::from([
                ("_id".into(), id.as_value()),
                (field.0.into(), field.1),
            ])),
        };
        let retained = Value::Binary(vec![8, 9, 10]);
        let receipt = event(&receipt_id, 9, 55, ("native_transaction", retained.clone()));
        let coverage = event(&coverage_id, 4, 28, ("version", Value::U8(2)));
        read.observe(&coverage_table, coverage, [1; 32], memory.reserve(512)?)?;
        read.observe(
            &coverage_table,
            receipt.clone(),
            [2; 32],
            memory.reserve(512)?,
        )?;
        read.observe(&receipt_table, receipt, [2; 32], memory.reserve(512)?)?;
        let readback = read.finish();
        let [receipt] = readback.receipt.events.as_slice() else {
            panic!("expected one retained receipt")
        };
        let coverage_readback = readback.coverage.unwrap();
        let [coverage] = coverage_readback.events.as_slice() else {
            panic!("expected one retained coverage row")
        };
        assert_eq!(readback.receipt.id, receipt_id);
        assert_eq!(coverage_readback.id, coverage_id);
        assert_eq!(receipt.header.system_from, Timestamp(55));
        assert_eq!(coverage.header.system_from, Timestamp(28));
        assert_eq!(
            receipt.document.as_ref().unwrap().get("native_transaction"),
            Some(&retained)
        );
        assert_eq!(
            coverage.document.as_ref().unwrap().get("version"),
            Some(&Value::U8(2))
        );
        assert!(
            ReceiptLookups::new(
                ReceiptLookup::new(receipt_table.clone(), receipt_id.clone()),
                Some(ReceiptLookup::new(receipt_table, receipt_id)),
            )
            .is_err()
        );
        Ok(())
    }
}
