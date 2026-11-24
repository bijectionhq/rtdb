use std::{collections::{BTreeMap, VecDeque}, sync::Arc};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use rtdb_crypt::{Kms, tokens::VerifiedClaims};
use rtdb_format::journal::ObjectId;
use rtdb_format::log::proto;
use rtdb_index::branches::{BranchCut, BranchIndex, BranchWrite};
use rtdb_tx::transact::{Decision, LINEARIZABLE, Receipt, TRANSACT_REQUIRES, Transact, TransactPipeline, TransactState, TransactionRecord};
use rtdb_storage::ObjectStore;
use rtdb_store::{Reachability, WriterObjects};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Term, Timestamp, rtdb_v1::{At, Basis}};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, oneshot, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::ReplicationHandle;

const MAX_IN_FLIGHT: usize = 64;
const MAX_CHANGE_GROUPS: usize = 4096;
const FRAME_VERSION: u8 = 1;
const FRAME_HEADER_BYTES: usize = 65;

pub const NATIVE_WRITE_FEATURES: &[&str] = &[
    "query", "query.track", "query.page", "query.linearizable", LINEARIZABLE, "branches",
    "changes", "changes.progress-only", "transact", TRANSACT_REQUIRES, "tx-status", "sql.history",
];
pub const NATIVE_READ_FEATURES: &[&str] = &[
    "query", "query.track", "query.page", "query.linearizable", "branches",
    "changes", "changes.progress-only", "tx-status", "sql.history",
];

pub struct TransactSubmission {
    pub transaction: Transact,
    pub attempt: Uuid,
    pub submitted_at: Timestamp,
    pub source: Bytes,
}

#[derive(Clone)]
pub struct NativeServingAuthority {
    pub incarnation: [u8; 16],
    pub checkpoints: Option<NativeCheckpointAuthority>,
}

#[derive(Clone)]
pub struct NativeCheckpointAuthority {
    pub kms: Arc<dyn Kms>,
    pub generation: u64,
}

impl NativeServingAuthority {
    pub(crate) async fn read_checkpoint(&self, store: &dyn ObjectStore, inline: Option<&[u8]>, object: Option<&str>) -> Result<Option<Bytes>> {
        match (inline, object) {
            (Some(bytes), None) => Ok(Some(Bytes::copy_from_slice(bytes))),
            (None, None) => Ok(None),
            (None, Some(key)) => {
                let id: ObjectId = key.parse()?;
                if id.database.as_bytes() != &self.incarnation { return Err(invalid()); }
                let checkpoints = self.checkpoints.as_ref().ok_or_else(unavailable)?;
                let plaintext = rtdb_store::read_file(store, checkpoints.kms.as_ref(), id).await?;
                Ok(Some(Bytes::copy_from_slice(&plaintext.bytes)))
            }
            (Some(_), Some(_)) => Err(invalid()),
        }
    }
}

#[derive(Clone)]
pub struct NativeServingView {
    pub index: BranchIndex,
    pub floor: Basis,
    pub changes: Arc<VecDeque<(BranchCut, Vec<BranchWrite>)>>,
    compatible: bool,
}

#[derive(Clone)]
pub struct NativeServingHandle {
    pub(crate) replication: ReplicationHandle,
    pub(crate) view: watch::Receiver<NativeServingView>,
    pub(crate) authority: NativeServingAuthority,
}

impl NativeServingHandle {
    pub fn authority(&self) -> &NativeServingAuthority { &self.authority }

    pub async fn view(&self, at: At, cancellation: &CancellationToken) -> Result<NativeServingView> {
        if at == At::Linearizable {
            self.replication.serving_barrier(cancellation).await?;
        }
        let mut view = self.view.clone();
        view.has_changed().map_err(|_| unavailable())?;
        if let At::AtLeast(basis) = at {
            if basis.incarnation() != self.authority.incarnation { return Err(foreign_basis()); }
            cancellation.run_until_cancelled(view.wait_for(|applied| !applied.compatible || applied.index.head().basis >= basis))
                .await.ok_or_else(interrupted)?.map_err(|_| unavailable())?;
        }
        let snapshot = view.borrow().clone();
        if !snapshot.compatible { return Err(incompatible()); }
        Ok(snapshot)
    }

    pub async fn after(&self, basis: Basis, cancellation: &CancellationToken) -> Result<()> {
        if basis.incarnation() != self.authority.incarnation { return Err(foreign_basis()); }
        let mut view = self.view.clone();
        let applied = cancellation.run_until_cancelled(view.wait_for(|applied| !applied.compatible || applied.index.head().basis > basis))
            .await.ok_or_else(interrupted)?.map_err(|_| unavailable())?;
        if !applied.compatible { return Err(incompatible()); }
        Ok(())
    }

    pub async fn transact(&self, request: TransactSubmission, claims: VerifiedClaims, cancellation: &CancellationToken) -> Result<Receipt> {
        if request.transaction.ops.is_empty() && request.transaction.otherwise.is_empty() {
            self.replication.serving_barrier(cancellation).await?;
        }
        self.replication.serving_transact(request, claims, cancellation).await
    }

    pub async fn tx_status(&self, key: Vec<u8>, cancellation: &CancellationToken) -> Result<Receipt> {
        self.replication.serving_barrier(cancellation).await?;
        self.replication.serving_status(key, cancellation).await
    }
}

pub(crate) type Completion = oneshot::Sender<Result<Receipt>>;

pub(crate) struct NativeServing {
    authority: NativeServingAuthority,
    fold: Option<TransactState>,
    position: u64,
    pipeline: Option<TransactPipeline>,
    deciding: bool,
    collect_checkpoints: bool,
    writer: Option<(Term, Arc<Mutex<WriterObjects>>)>,
    pending: BTreeMap<u64, Pending>,
    view: watch::Sender<NativeServingView>,
}

struct Pending {
    frame: Bytes,
    completion: Completion,
}

pub(crate) struct ServingCheckpoint {
    image: Bytes,
    writer: Option<CheckpointWriter>,
}

struct CheckpointWriter {
    allocator: Arc<Mutex<WriterObjects>>,
    authority: NativeCheckpointAuthority,
    database: Uuid,
    epoch: u64,
    collect: bool,
}

impl ServingCheckpoint {
    pub(crate) async fn verify(&self, store: &dyn ObjectStore, inline: Option<&[u8]>, object: Option<&str>) -> Result<()> {
        let same = match (inline, object) {
            (Some(inline), None) => self.image.as_ref() == inline && self.writer.is_none(),
            (None, Some(key)) => {
                let writer = self.writer.as_ref().ok_or_else(invalid)?;
                let id: ObjectId = key.parse()?;
                if id.database != writer.database || id.epoch > writer.epoch { return Err(invalid()); }
                let plaintext = rtdb_store::read_file(store, writer.authority.kms.as_ref(), id).await?;
                self.image.as_ref() == plaintext.bytes.as_slice()
            }
            _ => false,
        };
        if same { Ok(()) } else { Err(invalid()) }
    }

    pub(crate) async fn store(self, blocks: &crate::BlockStore, cancellation: &CancellationToken) -> Result<(Option<Vec<u8>>, Option<String>)> {
        let Some(writer) = self.writer else { return Ok((Some(self.image.to_vec()), None)); };
        let store = blocks.object_store();
        let prepared = writer.allocator.lock().await.file(writer.authority.kms.as_ref(), writer.authority.generation, &self.image).await?;
        prepared.put(store).await?;
        let reachable = Reachability::from_roots(writer.database, writer.epoch, [])?;
        reachable.validate_publication([prepared.id()])?;
        if writer.collect {
            blocks.collect_serving_checkpoints(writer.database, writer.epoch, cancellation).await?;
        }
        Ok((None, Some(prepared.id().to_string())))
    }
}

impl NativeServing {
    pub(crate) fn open(authority: NativeServingAuthority, checkpoint: Option<&[u8]>, collect_checkpoints: bool) -> Result<(Self, watch::Receiver<NativeServingView>)> {
        let (fold, position) = match checkpoint {
            Some(checkpoint) => {
                let mut bytes = checkpoint;
                let Some((cut_bytes, state)) = bytes.split_at_checked(40) else { return Err(invalid()); };
                bytes = cut_bytes;
                let position = bytes.get_u64();
                let basis = Basis::from_bytes(&bytes[..24])?;
                bytes.advance(24);
                let cut = BranchCut { basis, system_time: Timestamp(bytes.get_i64()) };
                if basis.incarnation() != authority.incarnation || position < basis.position() { return Err(invalid()); }
                (TransactState::decode_checkpoint(state, cut)?, position)
            }
            None => (TransactState::new(authority.incarnation), 0),
        };
        let (view, receiver) = watch::channel(NativeServingView {
            index: fold.index().clone(),
            floor: fold.index().head().basis,
            changes: Arc::default(), compatible: true,
        });
        Ok((Self { authority, fold: Some(fold), position, pipeline: None, deciding: false, collect_checkpoints, writer: None, pending: BTreeMap::new(), view }, receiver))
    }

    pub(crate) fn checkpoint(&mut self, term: Term, confirmed: bool) -> Result<ServingCheckpoint> {
        let fold = self.durable();
        let cut = fold.index().head();
        let checkpoint = fold.encode_checkpoint()?;
        let mut frame = BytesMut::with_capacity(40 + checkpoint.len());
        frame.put_u64(self.position);
        frame.extend_from_slice(&cut.basis.to_bytes());
        frame.put_i64(cut.system_time.0);
        frame.extend_from_slice(&checkpoint);
        let writer = if let Some(authority) = &self.authority.checkpoints {
            let epoch = u64::try_from(term.0).map_err(|_| invalid())?;
            let database = Uuid::from_bytes(self.authority.incarnation);
            if self.writer.as_ref().is_none_or(|(opened, _)| *opened != term) {
                self.writer = Some((term, Arc::new(Mutex::new(WriterObjects::new(database, epoch)))));
            }
            let (_, writer) = self.writer.as_ref().expect("writer installed for this term");
            Some(CheckpointWriter { allocator: Arc::clone(writer), authority: authority.clone(), database, epoch, collect: confirmed && self.collect_checkpoints })
        } else { None };
        Ok(ServingCheckpoint { image: frame.freeze(), writer })
    }

    pub(crate) fn decide_batch(
        &mut self, now: Timestamp, requests: Vec<(TransactSubmission, VerifiedClaims, Completion)>,
    ) -> Result<Vec<proto::NoOp>> {
        if !self.view.borrow().compatible {
            for (_, _, completion) in requests { let _ = completion.send(Err(incompatible())); }
            return Ok(Vec::new());
        }
        if !self.deciding {
            let next = self.next_position()?;
            if let Some(pipeline) = &mut self.pipeline {
                pipeline.resume(next)?;
            } else {
                let fold = self.fold.take().expect("replay fold before first decision");
                self.pipeline = Some(TransactPipeline::new(fold, next, MAX_IN_FLIGHT)?);
            }
            self.deciding = true;
        }
        let pipeline = self.pipeline.as_mut().expect("initialized serving pipeline");
        let decisions = requests.iter().map(|(request, claims, _)| {
            pipeline.decide(&request.transaction, now, NATIVE_WRITE_FEATURES, claims)
        }).collect::<Vec<_>>();
        let mut messages = Vec::new();
        for ((request, _, completion), decision) in requests.into_iter().zip(decisions) {
            match decision {
                Ok(Decision::Answer(receipt)) => { let _ = completion.send(Ok(receipt)); }
                Err(error) => { let _ = completion.send(Err(error)); }
                Ok(Decision::Append { position, record }) => {
                    let frame = frame(position, &request, &record);
                    self.pending.insert(position, Pending { frame: frame.clone(), completion });
                    messages.push(proto::NoOp { serving_record: Some(frame), ..Default::default() });
                }
            }
        }
        Ok(messages)
    }

    pub(crate) fn apply(&mut self, frame: &[u8], own: bool) -> Result<()> {
        let (position, bytes) = decode(frame)?;
        if position != self.next_position()? { return Err(invalid()); }
        if own && self.pending.get(&position).is_none_or(|entry| entry.frame.as_ref() != frame) { return Err(invalid()); }
        let receipts = match &mut self.pipeline {
            Some(pipeline) if own => pipeline.acknowledge(position)?,
            Some(pipeline) => pipeline.recover(position, bytes)?.into_iter().collect(),
            None => self.fold.as_mut().ok_or_else(invalid)?.apply(position, TransactionRecord::decode(bytes)?)?.into_iter().collect(),
        };
        if receipts.len() != 1 || receipts[0].basis.position() != position { return Err(invalid()); }
        self.position = position;
        let index = self.durable().index();
        let cut = index.head();
        let changes = index.writes_at(cut.basis);
        self.view.send_modify(|view| {
            view.index = index.clone();
            if changes.is_empty() { return; }
            let retained = Arc::make_mut(&mut view.changes);
            if retained.len() == MAX_CHANGE_GROUPS {
                view.floor = retained.pop_front().expect("full retained groups").0.basis;
            }
            retained.push_back((cut, changes));
        });
        if own {
            let receipt = receipts.into_iter().next().expect("one ordinary receipt");
            let pending = self.pending.remove(&position).ok_or_else(invalid)?;
            let _ = pending.completion.send(Ok(receipt));
        }
        Ok(())
    }

    pub(crate) fn status(&self, key: &[u8]) -> Result<Receipt> {
        if !self.view.borrow().compatible { return Err(incompatible()); }
        match &self.pipeline {
            Some(pipeline) => pipeline.tx_status(key),
            None => self.durable().tx_status(key),
        }
    }

    pub(crate) fn withdraw(&mut self) {
        if let Some(pipeline) = &mut self.pipeline {
            let _ = pipeline.withdraw();
        }
        self.deciding = false;
        for (_, entry) in std::mem::take(&mut self.pending) {
            let _ = entry.completion.send(Err(interrupted()));
        }
    }

    fn next_position(&self) -> Result<u64> {
        self.position.checked_add(1).ok_or_else(invalid)
    }

    fn durable(&self) -> &TransactState {
        match &self.pipeline {
            Some(pipeline) => pipeline.durable(),
            None => self.fold.as_ref().expect("replay fold before first decision"),
        }
    }

    pub(crate) fn mark_classic_writes(&self) {
        self.view.send_if_modified(|view| std::mem::replace(&mut view.compatible, false));
    }
}

impl Drop for NativeServing {
    fn drop(&mut self) { self.withdraw(); }
}

fn frame(position: u64, request: &TransactSubmission, record: &[u8]) -> Bytes {
    let mut frame = BytesMut::with_capacity(FRAME_HEADER_BYTES + record.len());
    frame.put_u8(FRAME_VERSION);
    frame.put_u64(position);
    frame.extend_from_slice(request.attempt.as_bytes());
    frame.put_i64(request.submitted_at.0);
    frame.extend_from_slice(&Sha256::digest(&request.source));
    frame.extend_from_slice(record);
    frame.freeze()
}

fn decode(frame: &[u8]) -> Result<(u64, &[u8])> {
    let Some((mut header, record)) = frame.split_at_checked(FRAME_HEADER_BYTES) else { return Err(invalid()); };
    if header.get_u8() != FRAME_VERSION { return Err(invalid()); }
    let position = header.get_u64();
    if position == 0 || record.is_empty() || header[..16] == [0; 16] { return Err(invalid()); }
    Ok((position, record))
}

pub(crate) fn unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb/default-serving-unavailable", "The default serving writer or applied prefix is unavailable")
}
fn incompatible() -> Anomaly {
    Anomaly::unsupported("rtdb/default-serving-classic-history", "The database has Classic writes outside the applied branch history")
}
pub(crate) fn interrupted() -> Anomaly {
    Anomaly::new(AnomalyCategory::Interrupted, "rtdb/transaction-outcome-unknown", "Recover the transaction outcome by key")
}
fn foreign_basis() -> Anomaly {
    Anomaly::new(AnomalyCategory::NotFound, "rtdb/unknown-basis", "The basis belongs to another database incarnation")
}
fn invalid() -> Anomaly {
    Anomaly::fault("rtdb/default-serving-record", "The serving record does not extend the applied prefix")
}

#[cfg(test)]
mod tests {
    use rtdb_columnar::MemoryBudget;
    use rtdb_crypt::{Generation, LocalKms};
    use rtdb_format::{block::proto as block_pb, catalog::BlockCatalog, keys::block_key};
    use rtdb_storage::{LocalStore, ObjectKey, cache::BufferPool};
    use rtdb_tx::transact::{Cell, Op, Row, Window};
    use rtdb_types::{BlockIndex, EntityId, StorageEpoch, StructFields, TableRef, Value};

    use super::*;

    const INCARNATION: [u8; 16] = [7; 16];

    fn authority() -> NativeServingAuthority {
        NativeServingAuthority { incarnation: INCARNATION, checkpoints: None }
    }

    fn put(key: u8) -> TransactSubmission {
        let id = EntityId::Integer(i64::from(key));
        TransactSubmission {
            attempt: Uuid::from_u128(key.into()), submitted_at: Timestamp(31),
            source: Bytes::from_static(b"\x0a\x02\x03\x04\xf8\x07\x01"),
            transaction: Transact {
                key: vec![key; 16], require: Vec::new(), window: Window::default(), guards: Vec::new(), otherwise: Vec::new(),
                requires: Vec::new(),
                ops: vec![Op::Put { branch: "main".into(), table: TableRef::new("public", "items"), if_absent: false,
                    rows: vec![Row { id, valid: None, cells: StructFields::from([("value".into(), Cell::Value(Value::Utf8("row".into())))]) }] }],
            },
        }
    }

    #[tokio::test]
    async fn serving_withdrawal_recovers_only_the_committed_prefix_before_resuming() {
        let (mut serving, view) = NativeServing::open(authority(), None, false).unwrap();
        let first = put(1);
        let (first_reply, mut first_result) = oneshot::channel();
        let (second_reply, mut second_result) = oneshot::channel();
        let mut messages = serving.decide_batch(Timestamp(41), vec![
            (first, VerifiedClaims::default(), first_reply),
            (put(2), VerifiedClaims::default(), second_reply),
        ]).unwrap();
        assert_eq!(messages.len(), 2);
        let excluded = messages.pop().unwrap().serving_record.unwrap();
        let committed = messages.pop().unwrap().serving_record.unwrap();
        assert_eq!(&committed[9..25], put(1).attempt.as_bytes());
        assert_eq!(&committed[25..33], &Timestamp(31).0.to_be_bytes());
        let digest: [u8; 32] = Sha256::digest(&put(1).source).into();
        assert_eq!(&committed[33..65], &digest);
        assert!(first_result.try_recv().is_err());
        assert!(second_result.try_recv().is_err());
        assert_eq!(serving.status(&[1; 16]).unwrap_err().code, "rtdb/transaction-pending");
        assert_eq!(view.borrow().index.head().basis.position(), 0);

        serving.withdraw();
        assert_eq!(first_result.await.unwrap().unwrap_err().category, AnomalyCategory::Interrupted);
        assert_eq!(second_result.await.unwrap().unwrap_err().category, AnomalyCategory::Interrupted);
        serving.apply(&committed, false).unwrap();
        assert!(serving.status(&[1; 16]).unwrap().replayed);
        assert_eq!(serving.status(&[2; 16]).unwrap_err().code, "rtdb/unknown-transaction");
        let (reply, result) = oneshot::channel();
        let message = serving.decide_batch(Timestamp(42), vec![(put(3), VerifiedClaims::default(), reply)])
            .unwrap().remove(0).serving_record.unwrap();
        assert!(serving.apply(&excluded, true).is_err());
        serving.apply(&message, true).unwrap();
        assert_eq!(result.await.unwrap().unwrap().basis.position(), 2);
        assert_eq!(view.borrow().changes.iter().map(|(cut, _)| cut.basis.position()).collect::<Vec<_>>(), [1, 2]);

        let image = serving.checkpoint(Term(2), true).unwrap().image;
        let (recovered, recovered_view) = NativeServing::open(authority(), Some(&image), false).unwrap();
        assert_eq!(recovered.status(&[3; 16]).unwrap().basis.position(), 2);
        assert_eq!(recovered_view.borrow().floor.position(), 2);
        assert!(recovered_view.borrow().changes.is_empty());
    }

    #[tokio::test]
    async fn serving_prefix_replay_matches_same_term_acknowledgement_across_batch_boundaries() {
        for batch_size in [1, 2, 3, 5, 8] {
            let (mut leader, leader_view) = NativeServing::open(authority(), None, false).unwrap();
            let (mut follower, follower_view) = NativeServing::open(authority(), None, false).unwrap();
            for batch in (1..=24).collect::<Vec<_>>().chunks(batch_size) {
                let mut replies = Vec::new();
                let requests = batch.iter().map(|&key| {
                    let (completion, reply) = oneshot::channel();
                    replies.push((key, reply));
                    (put(key), completion)
                }).collect();
                let messages = leader.decide_batch(Timestamp(50), requests).unwrap();
                for (message, (key, reply)) in messages.iter().zip(replies) {
                    let frame = message.serving_record.as_deref().unwrap();
                    leader.apply(frame, true).unwrap();
                    follower.apply(frame, false).unwrap();
                    let receipt = reply.await.unwrap().unwrap();
                    assert_eq!(receipt.basis.position(), u64::from(key));
                    assert_eq!(leader.status(&[key; 16]).unwrap(), follower.status(&[key; 16]).unwrap());
                }
                assert_eq!(leader_view.borrow().changes.as_ref(), follower_view.borrow().changes.as_ref());
            }
            let leader_image = leader.checkpoint(Term(1), true).unwrap();
            let follower_image = follower.checkpoint(Term(1), true).unwrap();
            assert_eq!(leader_image.image, follower_image.image);
        }
    }

    #[tokio::test]
    async fn serving_object_collection_preserves_retained_and_current_epoch_images() {
        let directory = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalStore::open(directory.path()).unwrap().with_catalog_ledger(StorageEpoch(0)));
        let store: Arc<dyn ObjectStore> = local.clone();
        let blocks = crate::BlockStore::new(store.clone(), Arc::new(BufferPool::new(store.clone(), 1 << 20)),
            MemoryBudget::new(1 << 24), 100_000_000).unwrap();
        let database = Uuid::from_bytes(INCARNATION);
        let kms = LocalKms::new(|| 0);
        kms.create(Generation { database, number: 1 }).await.unwrap();
        let mut old = WriterObjects::new(database, 1);
        let mut current = WriterObjects::new(database, 2);
        let retained = old.file(&kms, 1, b"retained").await.unwrap();
        let orphan = old.file(&kms, 1, b"orphan").await.unwrap();
        let inflight = current.file(&kms, 1, b"in flight").await.unwrap();
        for object in [&retained, &orphan, &inflight] { object.put(store.as_ref()).await.unwrap(); }
        let mut root = BlockCatalog::from_proto(block_pb::Block { block_index: Some(1),
            serving_checkpoint_object: Some(retained.id().to_string()), ..Default::default() }).unwrap();
        let root_key = ObjectKey::new(block_key(BlockIndex(1)).unwrap()).unwrap();
        store.put(&root_key, Bytes::from(root.encode_for_publication())).await.unwrap();

        let cancellation = CancellationToken::new();
        let recovery = local.hold_recovery_open().await.unwrap();
        blocks.collect_serving_checkpoints(database, 2, &cancellation).await.unwrap();
        let orphan_key = ObjectKey::new(orphan.id().to_string()).unwrap();
        assert!(store.head(&orphan_key).await.is_ok());
        drop(recovery);
        blocks.collect_serving_checkpoints(database, 2, &cancellation).await.unwrap();
        assert!(matches!(store.head(&orphan_key).await, Err(rtdb_storage::Error::MissingObject(_))));
        assert_eq!(rtdb_store::read_file(store.as_ref(), &kms, retained.id()).await.unwrap().bytes.as_slice(), b"retained");
        assert_eq!(rtdb_store::read_file(store.as_ref(), &kms, inflight.id()).await.unwrap().bytes.as_slice(), b"in flight");
    }
}
