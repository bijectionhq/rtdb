use async_trait::async_trait;
use rtdb_crypt::Generation;
use rtdb_format::log::proto;
use rtdb_types::{
    BlockIndex, ReplicaPosition, Result, SourcePosition, Term, Timestamp, rtdb_v1::Basis,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{BlockStore, NativeServingAuthority};

use super::{ErasureRetirement, MAX_CONTROL_BYTES, invalid, unsupported};

const COMPLETED_FRAME: u8 = 9;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DefaultErasureReadCut {
    pub database: Uuid,
    pub block: BlockIndex,
    pub source: SourcePosition,
    pub basis: Basis,
    pub erase_replica: ReplicaPosition,
    pub erase_term: Term,
    pub boundary: ReplicaPosition,
    pub root_sha256: [u8; 32],
    pub inventory_sha256: [u8; 32],
    pub predecessor: u64,
    pub successor: u64,
    pub unreadable_by: Timestamp,
}

/// Authority over every held view, plaintext cache and export that can serve the prior state.
#[async_trait]
pub trait DefaultErasureReadFence: Send + Sync {
    /// Atomically closes prior admission and returns a durable witness that admitted copies cannot deliver beyond the original deadline.
    /// The witness also establishes exclusive retirement authority for the predecessor and coverage of earlier generations.
    async fn close(&self, cut: DefaultErasureReadCut) -> Result<Option<[u8; 32]>>;
    /// Verifies the same durable closure during ordered read-back or restart.
    async fn verify(&self, cut: DefaultErasureReadCut, witness: [u8; 32]) -> Result<bool>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ErasureCompleted {
    pub intent: ErasureRetirement,
    pub ordered_intent: ReplicaPosition,
    pub read_closure: [u8; 32],
}

impl ErasureRetirement {
    pub fn read_cut(self) -> Result<DefaultErasureReadCut> {
        let predecessor = self.evidence.prior_generation.ok_or_else(unsupported)?;
        let successor = predecessor.checked_add(1).ok_or_else(invalid)?;
        Ok(DefaultErasureReadCut {
            database: Uuid::from_bytes(self.evidence.cut.basis.incarnation()),
            block: self.block,
            source: self.evidence.cut.source,
            basis: self.evidence.cut.basis,
            erase_replica: self.evidence.cut.replica,
            erase_term: self.evidence.cut.term,
            boundary: self.evidence.boundary,
            root_sha256: self.root_sha256,
            inventory_sha256: self.evidence.inventory_sha256.ok_or_else(unsupported)?,
            predecessor,
            successor,
            unreadable_by: self.unreadable_by,
        })
    }

    pub async fn destroy_predecessor(
        self,
        authority: &NativeServingAuthority,
        now_lower: Timestamp,
    ) -> Result<bool> {
        let cut = self.read_cut()?;
        if now_lower < cut.unreadable_by {
            return Ok(false);
        }
        let kms = authority
            .checkpoints
            .as_ref()
            .ok_or_else(unsupported)?
            .kms
            .as_ref();
        if !kms.supports_durable_generation_retirement()
            || cut.database.as_bytes() != &authority.incarnation
        {
            return Err(unsupported());
        }
        kms.check_readable(Generation {
            database: cut.database,
            number: cut.successor,
        })
        .await?;
        let predecessor = Generation {
            database: cut.database,
            number: cut.predecessor,
        };
        kms.retire(predecessor, cut.unreadable_by.0).await?;
        kms.destroy(predecessor).await?;
        kms.generation_destroyed(predecessor).await
    }
}

impl ErasureCompleted {
    pub fn require_order(
        self,
        intent: ErasureRetirement,
        ordered_intent: ReplicaPosition,
        position: ReplicaPosition,
    ) -> Result<()> {
        if self.intent != intent
            || self.ordered_intent != ordered_intent
            || position.epoch() != ordered_intent.epoch()
            || position <= ordered_intent
        {
            return Err(invalid());
        }
        Ok(())
    }

    pub async fn verify(
        self,
        blocks: &BlockStore,
        authority: &NativeServingAuthority,
        reader_fence: &dyn DefaultErasureReadFence,
    ) -> Result<()> {
        let cut = self.intent.read_cut()?;
        if cut.database.as_bytes() != &authority.incarnation
            || !reader_fence.verify(cut, self.read_closure).await?
            || !self.intent.objects_are_settled(blocks).await?
        {
            return Err(invalid());
        }
        let kms = authority
            .checkpoints
            .as_ref()
            .ok_or_else(unsupported)?
            .kms
            .as_ref();
        if !kms
            .generation_destroyed(Generation {
                database: cut.database,
                number: cut.predecessor,
            })
            .await?
        {
            return Err(invalid());
        }
        kms.check_readable(Generation {
            database: cut.database,
            number: cut.successor,
        })
        .await
    }

    pub fn message(self) -> Result<proto::NoOp> {
        let mut frame = vec![COMPLETED_FRAME];
        serde_json::to_writer(&mut frame, &self).map_err(|_| invalid())?;
        if frame.len() > MAX_CONTROL_BYTES + 1 {
            return Err(invalid());
        }
        Ok(proto::NoOp {
            serving_record: Some(frame.into()),
            ..Default::default()
        })
    }

    pub fn decode(message: &proto::NoOp) -> Result<Option<Self>> {
        let Some(record) = message
            .serving_record
            .as_deref()
            .and_then(|frame| frame.strip_prefix(&[COMPLETED_FRAME]))
        else {
            return Ok(None);
        };
        if message.src_msg_id.is_some()
            || message.placed_slot_reservation.is_some()
            || record.len() > MAX_CONTROL_BYTES
        {
            return Err(invalid());
        }
        serde_json::from_slice(record)
            .map(Some)
            .map_err(|_| invalid())
    }
}
