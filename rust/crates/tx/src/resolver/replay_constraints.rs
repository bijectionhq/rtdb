//! Canonical evidence for derived constraint effects in held replay.

use std::{collections::BTreeSet, sync::Arc};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, EntityId, Result};
use sha2::{Digest, Sha256};

pub(super) enum ConstraintMember<'a> {
    Removed,
    Holder(&'a EntityId),
    Count(i64),
}

#[derive(Default)]
pub(super) struct ConstraintMembers {
    events: BTreeSet<[u8; 32]>,
    reservations: Vec<Arc<MemoryReservation>>,
}

impl ConstraintMembers {
    pub(super) fn insert(
        &mut self,
        id: &EntityId,
        member: ConstraintMember<'_>,
        memory: &Arc<MemoryBudget>,
    ) -> Result<()> {
        let EntityId::String(id) = id else {
            return Err(invalid());
        };
        let mut event = Sha256::new();
        event.update(b"rtdb.hosted.constraint-member.v1\0");
        frame(&mut event, id);
        match member {
            ConstraintMember::Removed => event.update([0]),
            ConstraintMember::Holder(EntityId::String(holder)) => {
                event.update([1]);
                frame(&mut event, holder);
            }
            ConstraintMember::Holder(_) => return Err(invalid()),
            ConstraintMember::Count(count) => {
                event.update([2]);
                event.update(count.to_be_bytes());
            }
        }
        self.reservations.push(memory.reserve(128)?);
        if !self.events.insert(event.finalize().into()) {
            return Err(invalid());
        }
        Ok(())
    }

    pub(super) fn finish(self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"rtdb.hosted.constraint-members.v1\0");
        for event in self.events {
            digest.update(event);
        }
        digest.finalize().into()
    }
}

fn frame(image: &mut Sha256, value: &str) {
    image.update((value.len() as u64).to_be_bytes());
    image.update(value.as_bytes());
}

fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.hosted/constraint-proof",
        "A copied constraint member is outside its exact native identity",
    )
}
