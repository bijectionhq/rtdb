use std::{fmt, sync::Arc};

use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::envelope::{ReplicaBody, ReplicaEnvelope, ReplicaFrame};
use rtdb_types::{PhysicalObjectVersion, StorageEpoch, StorageVersion};

use crate::{Error, HeldView, IoRegion, ObjectKey, ObjectStore, Result};

use super::LogOffset;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaObjectScope {
    pub deployment: uuid::Uuid,
    pub incarnation: uuid::Uuid,
    pub group: uuid::Uuid,
    pub generation: u64,
    pub writer_epoch: u64,
}

/// Authority from one admitted, authenticated delivery. It authorizes only
/// the object named at that position and retains admission through provider I/O.
#[derive(Clone)]
pub struct ReplicaObjectCustody {
    authority: ReplicaPayloadAuthority,
    position: LogOffset,
    key: ObjectKey,
    digest: [u8; 32],
    publication: Option<PhysicalObjectVersion>,
    _hold: Arc<dyn Send + Sync>,
}

#[derive(Clone)]
enum ReplicaPayloadAuthority {
    Installed(ReplicaObjectScope),
    Brokered(Arc<dyn ReplicaPayloadBroker>),
}

#[async_trait::async_trait]
pub trait ReplicaPayloadBroker: Send + Sync {
    async fn read(&self, custody: &ReplicaObjectCustody, memory: &Arc<MemoryBudget>) -> Result<ReplicaPayload>;
}

impl ReplicaObjectCustody {
    pub fn from_delivery(
        scope: ReplicaObjectScope, position: LogOffset, frame: &[u8],
        version: StorageVersion, epoch: StorageEpoch, hold: Arc<dyn Send + Sync>,
    ) -> Result<Option<Self>> {
        Self::new(ReplicaPayloadAuthority::Installed(scope), position, frame, version, epoch, hold)
    }

    pub fn from_brokered_delivery(
        broker: Arc<dyn ReplicaPayloadBroker>, position: LogOffset, frame: &[u8],
        version: StorageVersion, epoch: StorageEpoch, hold: Arc<dyn Send + Sync>,
    ) -> Result<Option<Self>> {
        Self::new(ReplicaPayloadAuthority::Brokered(broker), position, frame, version, epoch, hold)
    }

    fn new(
        authority: ReplicaPayloadAuthority, position: LogOffset, frame: &[u8],
        version: StorageVersion, epoch: StorageEpoch, hold: Arc<dyn Send + Sync>,
    ) -> Result<Option<Self>> {
        let invalid = |error: rtdb_types::Anomaly| Error::Task(error.to_string());
        if !ReplicaFrame::has_external_payload(frame).map_err(invalid)? { return Ok(None); }
        let envelope = ReplicaEnvelope::decode(frame).map_err(invalid)?
            .ok_or_else(|| Error::Task("A delivered replica pointer has no envelope".into()))?;
        let ReplicaBody::OversizedMessage(reference) = &envelope.message else {
            return Err(Error::Task("A delivered replica pointer has no exact body".into()));
        };
        let digest = reference.payload_sha256.as_deref().and_then(|digest| digest.try_into().ok())
            .ok_or_else(|| Error::Task("A delivered replica pointer has no exact payload digest".into()))?;
        let key = envelope.oversized_payload(version, epoch).map_err(invalid)?
            .ok_or_else(|| Error::Task("A delivered replica pointer has no object".into()))?;
        let publication = envelope.offload_publication().map_err(invalid)?.map(|(_, version)| version);
        Ok(Some(Self { authority, position, key: ObjectKey::new(key)?, digest, publication, _hold: hold }))
    }

    pub fn installed_scope(&self) -> Option<ReplicaObjectScope> {
        match &self.authority {
            ReplicaPayloadAuthority::Installed(scope) => Some(*scope),
            ReplicaPayloadAuthority::Brokered(_) => None,
        }
    }

    pub fn position(&self) -> LogOffset { self.position }

    pub async fn redeem(&self, store: &dyn ObjectStore, memory: &Arc<MemoryBudget>) -> Result<ReplicaPayload> {
        let view = HeldView::recovery(Arc::new(self.clone()));
        IoRegion::reading(view, async {
            match &self.authority {
                ReplicaPayloadAuthority::Installed(_) => store.read_replica_payload(self, memory).await,
                ReplicaPayloadAuthority::Brokered(broker) => broker.read(self, memory).await,
            }
        }).await
    }

    pub fn matches(&self, position: LogOffset, key: &ObjectKey) -> bool {
        self.position == position && &self.key == key
    }

    /// The storage broker calls this only after validating the original scope.
    pub async fn read_from(&self, store: &dyn ObjectStore, memory: &Arc<MemoryBudget>) -> Result<ReplicaPayload> {
        self.read_publication(store, memory, self.publication.as_ref()).await
    }

    /// An authenticated transfer binds its own copy by the original body digest.
    pub async fn read_transferred_copy(&self, store: &dyn ObjectStore, memory: &Arc<MemoryBudget>) -> Result<ReplicaPayload> {
        self.read_publication(store, memory, None).await
    }

    async fn read_publication(
        &self, store: &dyn ObjectStore, memory: &Arc<MemoryBudget>, publication: Option<&PhysicalObjectVersion>,
    ) -> Result<ReplicaPayload> {
        let view = HeldView::recovery(Arc::new(self.clone()));
        IoRegion::reading(view, async {
            let length = usize::try_from(store.head_fresh(&self.key).await?.size)
                .map_err(|_| payload_size())?;
            let allocation = store.exact_object_allocation(&self.key, length)?;
            let charge = allocation.retained_bytes.checked_add(allocation.scratch_bytes).ok_or_else(payload_size)?;
            let charge = memory.reserve(charge).map_err(|error| Error::Task(error.to_string()))?;
            let observed = IoRegion::retaining(charge.clone(), store.observe_fresh_exact(&self.key, length)).await?;
            if publication.is_some_and(|expected| observed.version.as_ref() != Some(expected))
                || ring::digest::digest(&ring::digest::SHA256, &observed.value).as_ref() != self.digest.as_slice()
            {
                return Err(Error::ConflictingObject(self.key.to_string()));
            }
            Ok(ReplicaPayload { bytes: observed.value, _charge: charge })
        }).await
    }
}

fn payload_size() -> Error {
    Error::Task("A replica payload exceeds addressable storage".into())
}

impl fmt::Debug for ReplicaObjectCustody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ReplicaObjectCustody").field("installed_scope", &self.installed_scope())
            .field("position", &self.position).finish_non_exhaustive()
    }
}

impl PartialEq for ReplicaObjectCustody {
    fn eq(&self, other: &Self) -> bool {
        let same_owner = match (&self.authority, &other.authority) {
            (ReplicaPayloadAuthority::Installed(left), ReplicaPayloadAuthority::Installed(right)) => left == right,
            (ReplicaPayloadAuthority::Brokered(left), ReplicaPayloadAuthority::Brokered(right)) => Arc::ptr_eq(left, right),
            _ => false,
        };
        same_owner && self.position == other.position && self.key == other.key
            && self.digest == other.digest && self.publication == other.publication
    }
}
impl Eq for ReplicaObjectCustody {}

pub struct ReplicaPayload {
    bytes: Bytes,
    _charge: Arc<MemoryReservation>,
}

impl AsRef<[u8]> for ReplicaPayload {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::{keys::CURRENT_STORAGE_VERSION, log::proto::NoOp};
    use rtdb_types::{BlockIndex, Term, TermSequence};

    #[tokio::test]
    async fn a_replica_permit_binds_the_position_and_the_payload() {
        let body = ReplicaEnvelope {
            term: Term(1),
            term_sequence: Some(TermSequence(0)),
            message: ReplicaBody::NoOp(NoOp { src_msg_id: None, ..Default::default() }),
        };
        let bytes = Bytes::from(body.encode());
        let epoch = StorageEpoch(0);
        let frame = body.offloaded(CURRENT_STORAGE_VERSION, epoch, BlockIndex(2), "payload".into(), &bytes);
        let key = ObjectKey::new(frame.oversized_payload(CURRENT_STORAGE_VERSION, epoch).unwrap().unwrap()).unwrap();
        let scope = ReplicaObjectScope {
            deployment: uuid::Uuid::new_v4(),
            incarnation: uuid::Uuid::new_v4(),
            group: uuid::Uuid::new_v4(),
            generation: 1,
            writer_epoch: 2,
        };
        let permit = ReplicaObjectCustody::from_delivery(
            scope, LogOffset(4), &frame.encode(), CURRENT_STORAGE_VERSION, epoch, Arc::new(()),
        ).unwrap().unwrap();
        let store = crate::MemoryStore::default();
        let memory = MemoryBudget::new(1 << 20);
        assert!(permit.matches(LogOffset(4), &key));
        assert!(!permit.matches(LogOffset(5), &key));
        assert!(!permit.matches(LogOffset(4), &ObjectKey::new("oversized/other.binpb").unwrap()));
        store.put(&key, bytes.clone()).await.unwrap();
        assert_eq!(permit.read_from(&store, &memory).await.unwrap().as_ref(), bytes.as_ref());

        store.delete(&key).await.unwrap();
        store.put(&key, Bytes::from(vec![0; bytes.len()])).await.unwrap();
        assert!(matches!(permit.read_from(&store, &memory).await, Err(Error::ConflictingObject(_))));
    }
}
