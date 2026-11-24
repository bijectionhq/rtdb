use std::sync::Arc;

use rtdb_format::data::{StoredEvent, StoredOperation};
use rtdb_types::{EntityId, EventHeader, MemoryReservation, Result, StructFields, TableRef, Value};
use serde::Serialize;

use super::{RelationInventory, inconsistent, unavailable};

const MAX_RECEIPT_HISTORY_BYTES: usize = 1 << 20;

pub(crate) struct ReceiptLookup {
    pub(crate) table: TableRef,
    pub(crate) id: EntityId,
    pub(crate) history: Vec<ReceiptEvent>,
    pub(crate) table_sha256: Option<[u8; 32]>,
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
pub(crate) struct ReceiptEvent {
    header: EventHeader,
    origin: Option<[u8; 32]>,
    entity_key: Option<Vec<u8>>,
    sha256: [u8; 32],
    document: Option<StructFields<Value>>,
}

impl ReceiptLookup {
    pub(crate) fn new(table: TableRef, id: EntityId) -> Self {
        Self {
            table,
            id,
            history: Vec::new(),
            table_sha256: None,
            bytes: 0,
            charges: Vec::new(),
        }
    }

    pub(super) fn observe(
        &mut self,
        event: StoredEvent,
        sha256: [u8; 32],
        charge: Arc<MemoryReservation>,
    ) -> Result<()> {
        if event.iid != self.id.iid() {
            return Ok(());
        }
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

    pub(super) fn observe_relation(&mut self, relation: &RelationInventory) {
        if self.table == relation.table {
            self.table_sha256 = Some(relation.sha256);
        }
    }

    pub(super) fn finish(mut self) -> ReceiptHistory {
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
