use std::{collections::{BTreeMap, VecDeque}, sync::Arc};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use rtdb_crypt::{Kms, tokens::VerifiedClaims};
use rtdb_format::journal::ObjectId;
use rtdb_format::log::proto;
use rtdb_index::branches::{BranchCut, BranchIndex, BranchWrite};
use rtdb_tx::transact::{Decision, LINEARIZABLE, RELATIVE_VALID_TIME, Receipt, ScopedTransact, TIMED_TRANSACT, TRANSACT_REQUIRES, Transact, TransactPipeline, TransactState, TransactionRecord};
use rtdb_storage::ObjectStore;
use rtdb_store::{Reachability, WriterObjects};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Term, Timestamp, rtdb_v1::{At, Basis, LabelScope, QUERY_WRITTEN_BASIS}};
use tokio::sync::{Mutex, oneshot, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::ReplicationHandle;

const MAX_IN_FLIGHT: usize = 64;
const MAX_CHANGE_GROUPS: usize = 4096;
const FRAME_VERSION: u8 = 2;
const FRAME_HEADER_BYTES: usize = 9;
const LEGACY_FRAME_HEADER_BYTES: usize = 65;

pub const NATIVE_WRITE_FEATURES: &[&str] = &[
    "query", "query.track", "query.page", "query.linearizable", QUERY_WRITTEN_BASIS, LINEARIZABLE, "branches", "branch.local-layer",
    "changes", "changes.progress-only", "transact", TRANSACT_REQUIRES, RELATIVE_VALID_TIME, TIMED_TRANSACT, "row-visibility", "tx-status", "sql.history",
];
pub const NATIVE_READ_FEATURES: &[&str] = &[
    "query", "query.track", "query.page", "query.linearizable", QUERY_WRITTEN_BASIS, "branches", "branch.local-layer",
    "changes", "changes.progress-only", "tx-status", "sql.history",
];
const SCOPED_FEATURES: &[&str] = &[LINEARIZABLE, TRANSACT_REQUIRES, TIMED_TRANSACT, "row-visibility"];

pub struct NativeServingScope {
    pub labels: LabelScope,
    pub receipt_namespace: [u8; 32],
}

impl NativeServingScope {
    fn authority(&self) -> ScopedTransact<'_> {
        ScopedTransact { labels: &self.labels, receipt_namespace: self.receipt_namespace }
    }
}

pub(crate) struct NativeRequest {
    transaction: Transact,
    claims: VerifiedClaims,
    scope: Option<NativeServingScope>,
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
    pub fn is_compatible(&self) -> bool { self.view.borrow().compatible }

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

    pub async fn transact(&self, transaction: Transact, claims: VerifiedClaims, scope: Option<NativeServingScope>, cancellation: &CancellationToken) -> Result<Receipt> {
        if transaction.ops.is_empty() && transaction.otherwise.is_empty() {
            self.replication.serving_barrier(cancellation).await?;
        }
        self.replication.serving_transact(NativeRequest { transaction, claims, scope }, cancellation).await
    }

    pub async fn tx_status(&self, key: Vec<u8>, scope: Option<NativeServingScope>, cancellation: &CancellationToken) -> Result<Receipt> {
        self.replication.serving_barrier(cancellation).await?;
        self.replication.serving_status(key, scope, cancellation).await
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
    enqueued_through: u64,
    terminal: Option<(u64, Bytes)>,
    view: watch::Sender<NativeServingView>,
}

struct Pending {
    frame: Bytes,
    completion: Completion,
    scope: Option<NativeServingScope>,
    reconsider: Option<(Transact, VerifiedClaims)>,
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
        Ok((Self {
            authority, fold: Some(fold), position, pipeline: None, deciding: false,
            collect_checkpoints, writer: None, pending: BTreeMap::new(),
            enqueued_through: position, terminal: None, view,
        }, receiver))
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
        &mut self, now: Timestamp, requests: Vec<(NativeRequest, Completion)>,
    ) -> Result<Vec<proto::NoOp>> {
        if !self.view.borrow().compatible {
            for (_, completion) in requests { let _ = completion.send(Err(incompatible())); }
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
        let decisions = requests.iter().map(|(request, _)| {
            let transaction = &request.transaction;
            match &request.scope {
                Some(scope) => pipeline.decide_scoped(transaction, now, SCOPED_FEATURES, &request.claims, scope.authority()),
                None => pipeline.decide(transaction, now, NATIVE_WRITE_FEATURES, &request.claims),
            }
        }).collect::<Vec<_>>();
        let mut messages = Vec::new();
        for ((request, completion), decision) in requests.into_iter().zip(decisions) {
            match decision {
                Ok(Decision::Answer(receipt)) => {
                    let _ = completion.send(self.project_receipt(receipt, request.scope.as_ref()));
                }
                Err(error) => { let _ = completion.send(Err(error)); }
                Ok(Decision::Append { position, record }) => {
                    let frame = frame(position, &record);
                    self.pending.insert(position, Pending {
                        frame, completion, scope: request.scope,
                        reconsider: Some((request.transaction, request.claims)),
                    });
                }
            }
        }
        self.enqueue_ready(&mut messages)?;
        Ok(messages)
    }

    fn enqueue_ready(&mut self, messages: &mut Vec<proto::NoOp>) -> Result<()> {
        loop {
            let position = self.enqueued_through.checked_add(1).ok_or_else(invalid)?;
            let Some(pending) = self.pending.get_mut(&position) else { return Ok(()); };
            self.enqueued_through = position;
            pending.reconsider = None;
            messages.push(proto::NoOp { serving_record: Some(pending.frame.clone()), ..Default::default() });
        }
    }

    pub(crate) fn holds_checkpoint(&self) -> bool {
        self.durable().recover_pending().is_some()
            || self.pipeline.as_ref().is_some_and(|pipeline| pipeline.awaiting_terminal().next().is_some())
    }

    pub(crate) fn recovering_timed(&self) -> bool {
        self.pipeline.is_none() && self.durable().recover_pending().is_some()
    }

    pub(crate) fn progress_timed(&mut self, now: Timestamp, clock_upper: Timestamp) -> Result<Vec<proto::NoOp>> {
        let Some(refusal @ TransactionRecord::Refuse { tx_lsn }) = self.durable().recover_pending() else {
            return Ok(Vec::new());
        };
        if self.terminal.is_some() { return Ok(Vec::new()); }
        let position = self.next_position()?;
        let mut reconsider = Vec::new();
        let record = match &mut self.pipeline {
            Some(pipeline) => {
                let terminal = pipeline.terminal(tx_lsn, clock_upper)?;
                if terminal.position != position { return Err(invalid()); }
                if !terminal.redecide.is_empty() {
                    let dropped = self.pending.split_off(&position);
                    if dropped.len() != terminal.redecide.len() { return Err(invalid()); }
                    for pending in dropped.into_values() {
                        let (transaction, claims) = pending.reconsider.ok_or_else(invalid)?;
                        reconsider.push((NativeRequest { transaction, claims, scope: pending.scope }, pending.completion));
                    }
                }
                terminal.record
            }
            None => refusal.encode()?,
        };
        let frame = frame(position, &record);
        self.terminal = Some((position, frame.clone()));
        self.enqueued_through = position;
        let mut messages = vec![proto::NoOp { serving_record: Some(frame), ..Default::default() }];
        if reconsider.is_empty() {
            self.enqueue_ready(&mut messages)?;
        } else {
            messages.extend(self.decide_batch(now, reconsider)?);
        }
        Ok(messages)
    }

    pub(crate) fn apply(&mut self, frame: &[u8], own: bool) -> Result<()> {
        let (position, bytes) = decode(frame)?;
        if position != self.next_position()? { return Err(invalid()); }
        let own_terminal = own && self.terminal.as_ref().is_some_and(|(at, expected)| *at == position && expected.as_ref() == frame);
        if own && !own_terminal && self.pending.get(&position).is_none_or(|entry| entry.frame.as_ref() != frame) {
            return Err(invalid());
        }
        let receipts = match &mut self.pipeline {
            Some(pipeline) if own => pipeline.acknowledge(position)?,
            Some(pipeline) => pipeline.recover(position, bytes)?.into_iter().collect(),
            None => self.fold.as_mut().ok_or_else(invalid)?.apply(position, TransactionRecord::decode(bytes)?)?.into_iter().collect(),
        };
        self.position = position;
        self.enqueued_through = self.enqueued_through.max(position);
        if own_terminal { self.terminal = None; }
        let mut receipts = receipts.into_iter();
        let Some(receipt) = receipts.next() else {
            return if self.durable().recover_pending().is_some() { Ok(()) } else { Err(invalid()) };
        };
        if receipts.next().is_some() || receipt.basis.position() != position { return Err(invalid()); }
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
            let original = if own_terminal { position - 1 } else { position };
            if let Some(pending) = self.pending.remove(&original) {
                let _ = pending.completion.send(self.project_receipt(receipt, pending.scope.as_ref()));
            } else if !own_terminal {
                return Err(invalid());
            }
        }
        Ok(())
    }

    fn project_receipt(&self, receipt: Receipt, scope: Option<&NativeServingScope>) -> Result<Receipt> {
        match scope {
            Some(scope) => self.durable().project_receipt(receipt, &scope.labels),
            None => Ok(receipt),
        }
    }

    pub(crate) fn status(&self, key: &[u8], scope: Option<&NativeServingScope>) -> Result<Receipt> {
        if !self.view.borrow().compatible { return Err(incompatible()); }
        let receipt = match (&self.pipeline, scope) {
            (Some(pipeline), Some(scope)) => pipeline.tx_status_scoped(key, scope.authority()),
            (Some(pipeline), None) => pipeline.tx_status(key),
            (None, Some(scope)) => self.durable().tx_status_scoped(key, scope.authority()),
            (None, None) => self.durable().tx_status(key),
        }?;
        self.project_receipt(receipt, scope)
    }

    pub(crate) fn withdraw(&mut self) {
        if let Some(pipeline) = self.pipeline.take() {
            self.fold = Some(pipeline.into_durable());
        }
        self.enqueued_through = self.position;
        self.terminal = None;
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

fn frame(position: u64, record: &[u8]) -> Bytes {
    let mut frame = BytesMut::with_capacity(FRAME_HEADER_BYTES + record.len());
    frame.put_u8(FRAME_VERSION);
    frame.put_u64(position);
    frame.extend_from_slice(record);
    frame.freeze()
}

fn decode(frame: &[u8]) -> Result<(u64, &[u8])> {
    let header_len = match frame.first().copied() {
        Some(FRAME_VERSION) => FRAME_HEADER_BYTES,
        Some(1) => LEGACY_FRAME_HEADER_BYTES,
        _ => return Err(invalid()),
    };
    let (header, record) = frame.split_at_checked(header_len).ok_or_else(invalid)?;
    let position = u64::from_be_bytes(header[1..FRAME_HEADER_BYTES].try_into().expect("serving position has fixed width"));
    if position == 0 || record.is_empty() || header_len == LEGACY_FRAME_HEADER_BYTES && header[9..25] == [0; 16] {
        return Err(invalid());
    }
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

    fn put(key: u8) -> Transact {
        let id = EntityId::Integer(i64::from(key));
        Transact {
            custody_request_digest: None, request_memory: None,
            key: vec![key; 16], require: Vec::new(), window: Window::default(), guards: Vec::new(), otherwise: Vec::new(),
            requires: Vec::new(),
            ops: vec![Op::Put { branch: "main".into(), table: TableRef::new("public", "items"), if_absent: false,
                rows: vec![Row { id, valid: None, cells: StructFields::from([("value".into(), Cell::Value(Value::Utf8("row".into())))]), custody_group: None, protected_columns: Vec::new(), custody_payload: None }] }],
        }
    }

    fn unscoped(transaction: Transact) -> NativeRequest {
        NativeRequest { transaction, claims: VerifiedClaims::default(), scope: None }
    }

    #[tokio::test]
    async fn timed_terminal_gates_replies_and_the_speculative_suffix() {
        for (upper, settled) in [(Timestamp(55), true), (Timestamp(80), false)] {
            let (mut leader, view) = NativeServing::open(authority(), None, false).unwrap();
            let (mut follower, _) = NativeServing::open(authority(), None, false).unwrap();
            let mut request = put(1);
            request.window.before = Some(Timestamp(80));
            let (done, mut result) = oneshot::channel();
            let (later_done, mut later_result) = oneshot::channel();
            let mut messages = leader.decide_batch(Timestamp(50), vec![
                (unscoped(request), done),
                (unscoped(put(2)), later_done),
            ]).unwrap();
            assert_eq!(messages.len(), 1);
            assert!(leader.holds_checkpoint());
            let original = messages.pop().unwrap().serving_record.unwrap();
            leader.apply(&original, true).unwrap();
            follower.apply(&original, false).unwrap();
            assert!(result.try_recv().is_err());
            assert!(later_result.try_recv().is_err());
            assert_eq!(view.borrow().index.head().basis.position(), 0);
            assert_eq!(leader.status(&[1; 16], None).unwrap_err().code, "rtdb/transaction-pending");

            let messages = leader.progress_timed(Timestamp(55), upper).unwrap();
            assert_eq!(messages.len(), 2);
            assert!(result.try_recv().is_err());
            let terminal = messages[0].serving_record.as_deref().unwrap();
            leader.apply(terminal, true).unwrap();
            follower.apply(terminal, false).unwrap();
            let receipt = result.await.unwrap().unwrap();
            assert_eq!(receipt.basis.position(), 2);
            assert_eq!(matches!(receipt.outcome, rtdb_tx::transact::Outcome::Then), settled);
            assert_eq!(leader.status(&[1; 16], None).unwrap(), follower.status(&[1; 16], None).unwrap());
            assert!(later_result.try_recv().is_err());
            let later = messages[1].serving_record.as_deref().unwrap();
            leader.apply(later, true).unwrap();
            follower.apply(later, false).unwrap();
            assert_eq!(later_result.await.unwrap().unwrap().basis.position(), 3);
            assert!(!leader.holds_checkpoint());
        }
    }

    #[tokio::test]
    async fn takeover_refuses_the_replayed_original_even_inside_the_window() {
        let (mut serving, _) = NativeServing::open(authority(), None, false).unwrap();
        let mut request = put(1);
        request.window.before = Some(Timestamp(80));
        let (done, result) = oneshot::channel();
        let message = serving.decide_batch(Timestamp(50), vec![(unscoped(request), done)])
            .unwrap().remove(0).serving_record.unwrap();
        serving.withdraw();
        assert_eq!(result.await.unwrap().unwrap_err().category, AnomalyCategory::Interrupted);
        serving.apply(&message, false).unwrap();
        assert!(serving.recovering_timed());
        let terminal = serving.progress_timed(Timestamp(51), Timestamp(52)).unwrap()
            .remove(0).serving_record.unwrap();
        serving.apply(&terminal, true).unwrap();
        assert!(!serving.recovering_timed());
        assert!(matches!(serving.status(&[1; 16], None).unwrap().outcome,
            rtdb_tx::transact::Outcome::Refused { error } if error.code == "rtdb/timed-transaction-refused"));
    }

    #[tokio::test]
    async fn serving_withdrawal_recovers_only_the_committed_prefix_before_resuming() {
        let (mut serving, view) = NativeServing::open(authority(), None, false).unwrap();
        let first = put(1);
        let (first_reply, mut first_result) = oneshot::channel();
        let (second_reply, mut second_result) = oneshot::channel();
        let mut messages = serving.decide_batch(Timestamp(41), vec![
            (unscoped(first), first_reply),
            (unscoped(put(2)), second_reply),
        ]).unwrap();
        assert_eq!(messages.len(), 2);
        let excluded = messages.pop().unwrap().serving_record.unwrap();
        let committed = messages.pop().unwrap().serving_record.unwrap();
        let (position, bytes) = decode(&committed).unwrap();
        let TransactionRecord::Tx(record) = TransactionRecord::decode(bytes).unwrap() else { panic!() };
        assert_eq!(position, 1);
        assert_eq!(record.admission().0, &[1; 16]);
        assert_eq!(record.admission().1, Timestamp(41));
        assert_eq!(record.canonical_request().unwrap().get(), put(1).canonical_request().unwrap().get());
        assert!(first_result.try_recv().is_err());
        assert!(second_result.try_recv().is_err());
        assert_eq!(serving.status(&[1; 16], None).unwrap_err().code, "rtdb/transaction-pending");
        assert_eq!(view.borrow().index.head().basis.position(), 0);

        serving.withdraw();
        assert_eq!(first_result.await.unwrap().unwrap_err().category, AnomalyCategory::Interrupted);
        assert_eq!(second_result.await.unwrap().unwrap_err().category, AnomalyCategory::Interrupted);
        serving.apply(&committed, false).unwrap();
        assert!(serving.status(&[1; 16], None).unwrap().replayed);
        assert_eq!(serving.status(&[2; 16], None).unwrap_err().code, "rtdb/unknown-transaction");
        let (reply, result) = oneshot::channel();
        let message = serving.decide_batch(Timestamp(42), vec![(unscoped(put(3)), reply)])
            .unwrap().remove(0).serving_record.unwrap();
        assert!(serving.apply(&excluded, true).is_err());
        serving.apply(&message, true).unwrap();
        assert_eq!(result.await.unwrap().unwrap().basis.position(), 2);
        assert_eq!(view.borrow().changes.iter().map(|(cut, _)| cut.basis.position()).collect::<Vec<_>>(), [1, 2]);

        let image = serving.checkpoint(Term(2), true).unwrap().image;
        let (recovered, recovered_view) = NativeServing::open(authority(), Some(&image), false).unwrap();
        assert_eq!(recovered.status(&[3; 16], None).unwrap().basis.position(), 2);
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
                    (unscoped(put(key)), completion)
                }).collect();
                let messages = leader.decide_batch(Timestamp(50), requests).unwrap();
                for (message, (key, reply)) in messages.iter().zip(replies) {
                    let frame = message.serving_record.as_deref().unwrap();
                    leader.apply(frame, true).unwrap();
                    follower.apply(frame, false).unwrap();
                    let receipt = reply.await.unwrap().unwrap();
                    assert_eq!(receipt.basis.position(), u64::from(key));
                    assert_eq!(leader.status(&[key; 16], None).unwrap(), follower.status(&[key; 16], None).unwrap());
                }
                assert_eq!(leader_view.borrow().changes.as_ref(), follower_view.borrow().changes.as_ref());
            }
            let leader_image = leader.checkpoint(Term(1), true).unwrap();
            let follower_image = follower.checkpoint(Term(1), true).unwrap();
            assert_eq!(leader_image.image, follower_image.image);
        }
    }

    #[tokio::test]
    async fn serving_scoped_receipts_are_projected_and_recover_in_their_namespace() {
        use rtdb_tx::transact::{Declaration, Outcome, Visibility};

        let (mut serving, _) = NativeServing::open(authority(), None, false).unwrap();
        let mut declaration = put(1);
        declaration.ops = vec![Op::Declare {
            table: TableRef::new("public", "items"),
            change: Declaration::Visibility(Visibility { column: "label".into() }),
        }];
        let (completion, reply) = oneshot::channel();
        let mut records = serving.decide_batch(Timestamp(40), vec![(unscoped(declaration), completion)]).unwrap();
        serving.apply(records.remove(0).serving_record.as_deref().unwrap(), true).unwrap();
        assert_eq!(reply.await.unwrap().unwrap().outcome, Outcome::Then);

        let scope = || NativeServingScope {
            labels: LabelScope { read: [vec![7]].into_iter().collect(), write: [vec![7]].into_iter().collect() },
            receipt_namespace: [7; 32],
        };
        let mut submission = put(2);
        let Op::Put { rows, .. } = &mut submission.ops[0] else { unreachable!() };
        rows[0].valid = Some(rtdb_types::Interval::ALL.into());
        rows[0].cells.insert("label".into(), Cell::Value(Value::Binary(vec![7])));
        let request = NativeRequest { submission, claims: VerifiedClaims::default(), scope: Some(scope()) };
        let (completion, reply) = oneshot::channel();
        let mut records = serving.decide_batch(Timestamp(41), vec![(request, completion)]).unwrap();
        serving.apply(records.remove(0).serving_record.as_deref().unwrap(), true).unwrap();
        let receipt = reply.await.unwrap().unwrap();
        assert_eq!(receipt.outcome, Outcome::Then);
        assert_eq!(receipt.basis.incarnation(), scope().labels.main_read_incarnation(INCARNATION));

        let checkpoint = serving.checkpoint(Term(1), true).unwrap();
        let (reopened, _) = NativeServing::open(authority(), Some(&checkpoint.image), false).unwrap();
        assert_eq!(reopened.status(&[2; 16], Some(&scope())).unwrap().basis, receipt.basis);
        assert_eq!(reopened.status(&[2; 16], None).unwrap_err().code, "rtdb/unknown-transaction");
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
