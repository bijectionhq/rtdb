use std::{collections::{BTreeMap, VecDeque}, sync::Arc};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use rtdb_crypt::{Kms, tokens::VerifiedClaims};
use rtdb_format::journal::ObjectId;
use rtdb_format::{envelope::{SourceBody, SourceMessage}, log::proto};
use rtdb_index::branches::{BranchCut, BranchIndex, BranchWrite};
use rtdb_tx::transact::{Decision, LINEARIZABLE, RELATIVE_VALID_TIME, Receipt, Resolution, ScopedTransact, TIMED_TRANSACT, TRANSACT_REQUIRES, Transact, TransactPipeline, TransactState, TransactionRecord};
use rtdb_storage::ObjectStore;
use rtdb_store::{Reachability, WriterObjects};
use rtdb_types::{Anomaly, AnomalyCategory, Result, SourcePosition, Term, Timestamp, rtdb_v1::{At, Basis, LabelScope, QUERY_WRITTEN_BASIS}};
use parking_lot::Mutex as ReplyLock;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, oneshot, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::ReplicationHandle;

mod source;
use source::SourceAuthority;
pub(crate) use source::SourceRequest;

const MAX_IN_FLIGHT: usize = 64;
const MAX_CHANGE_GROUPS: usize = 4096;
const FRAME_VERSION: u8 = 2;
const SOURCE_DECISION_FRAME: u8 = 3;
const SOURCE_REPLY_FRAME: u8 = 4;
const SOURCE_DECISION_HEADER_BYTES: usize = FRAME_HEADER_BYTES + 8;
const MAX_SOURCE_REPLIES: usize = 4096;
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

#[derive(Clone, Serialize, Deserialize)]
pub struct NativeServingScope {
    pub labels: LabelScope,
    pub receipt_namespace: [u8; 32],
}

impl NativeServingScope {
    fn authority(&self) -> ScopedTransact<'_> {
        ScopedTransact { labels: &self.labels, receipt_namespace: self.receipt_namespace }
    }
}

pub(crate) struct NativeRequest<C = VerifiedClaims> {
    pub(crate) transaction: Transact,
    claims: C,
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
    pub(crate) source_authority: Arc<SourceAuthority>,
    pub(crate) source_replies: Arc<SourceReplies>,
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
        if !self.is_compatible() { return Err(incompatible()); }
        let request = NativeRequest { transaction, claims, scope };
        if request.is_validation() {
            self.replication.serving_barrier(cancellation).await?;
            return self.replication.serving_validate(request, cancellation).await;
        }
        request.check_source_custody()?;
        let scope = request.scope.clone();
        let bytes = cancellation.run_until_cancelled(self.source_authority.seal(request)).await
            .ok_or_else(interrupted)??;
        let message = SourceMessage::Protobuf(SourceBody::ServingTransact(bytes));
        let position = self.replication.submit_serving_source(&message, cancellation).await?;
        self.replication.await_source(position, cancellation).await.map_err(|error| {
            if error.category == AnomalyCategory::Interrupted { interrupted() } else { error }
        })?;
        let receipt = self.source_replies.get(position).ok_or_else(interrupted)??;
        project_receipt(&self.view.borrow().index, receipt, scope.as_ref())
    }

    pub async fn tx_status(&self, key: Vec<u8>, scope: Option<NativeServingScope>, cancellation: &CancellationToken) -> Result<Receipt> {
        self.replication.serving_barrier(cancellation).await?;
        self.replication.serving_status(key, scope, cancellation).await
    }
}

impl<C> NativeRequest<C> {
    fn is_validation(&self) -> bool {
        self.transaction.ops.is_empty() && self.transaction.otherwise.is_empty()
    }

    fn check_source_custody(&self) -> Result<()> {
        let features = if self.scope.is_some() { SCOPED_FEATURES } else { NATIVE_WRITE_FEATURES };
        self.transaction.require_sealed_custody(features)
    }
}

pub(crate) type Completion = oneshot::Sender<Result<Receipt>>;

#[derive(Default)]
pub(crate) struct SourceReplies(ReplyLock<BTreeMap<SourcePosition, Result<Receipt>>>);

impl SourceReplies {
    pub(crate) fn publish(&self, position: SourcePosition, reply: Result<Receipt>) {
        let mut replies = self.0.lock();
        replies.insert(position, reply);
        if replies.len() > MAX_SOURCE_REPLIES { replies.pop_first(); }
    }

    fn get(&self, position: SourcePosition) -> Option<Result<Receipt>> {
        self.0.lock().get(&position).cloned()
    }
}

pub(crate) struct NativeServing {
    authority: NativeServingAuthority,
    fold: Option<TransactState>,
    position: u64,
    pipeline: Option<TransactPipeline>,
    collect_checkpoints: bool,
    writer: Option<(Term, Arc<Mutex<WriterObjects>>)>,
    pending: BTreeMap<u64, proto::NoOp>,
    timed_source: Option<SourcePosition>,
    pending_replies: usize,
    view: watch::Sender<NativeServingView>,
    pub(crate) source_authority: Arc<SourceAuthority>,
    pub(crate) source_replies: Arc<SourceReplies>,
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
    pub(crate) fn open(authority: NativeServingAuthority, store: Arc<dyn ObjectStore>, checkpoint: Option<&[u8]>, collect_checkpoints: bool) -> Result<(Self, watch::Receiver<NativeServingView>)> {
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
        let source_authority = Arc::new(SourceAuthority::new(store, authority.incarnation));
        Ok((Self {
            authority, fold: Some(fold), position, pipeline: None,
            collect_checkpoints, writer: None, pending: BTreeMap::new(),
            timed_source: None, pending_replies: 0, view, source_authority, source_replies: Arc::default(),
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

    pub(crate) fn source_ready(&self) -> bool {
        !self.holds_checkpoint() && self.pending.len() + self.pending_replies < MAX_IN_FLIGHT
    }

    pub(crate) fn request_pending<C>(&self, request: &NativeRequest<C>) -> bool {
        self.pipeline.as_ref().is_some_and(|pipeline| {
            pipeline.is_deciding(&request.transaction.key, request.scope.as_ref().map(|scope| scope.receipt_namespace))
        })
    }

    pub(crate) fn decide_source(&mut self, source: SourcePosition, now: Timestamp, request: Result<SourceRequest>) -> Result<proto::NoOp> {
        if !self.source_ready() { return Err(invalid()); }
        let request = match request {
            Ok(_) if !self.view.borrow().compatible => return self.source_reply(source, Err(incompatible())),
            Ok(request) => request,
            Err(error) => return self.source_reply(source, Err(error)),
        };
        if request.is_validation() { return self.source_reply(source, Err(invalid_source_request())); }
        if let Err(error) = request.check_source_custody() { return self.source_reply(source, Err(error)); }
        if self.request_pending(&request) { return Err(invalid()); }
        if self.pipeline.is_none() {
            let next = self.next_position()?;
            let fold = self.fold.take().ok_or_else(invalid)?;
            self.pipeline = Some(TransactPipeline::new(fold, next, MAX_IN_FLIGHT)?);
        }
        let pipeline = self.pipeline.as_mut().expect("initialized serving pipeline");
        let decision = match &request.scope {
            Some(scope) => pipeline.decide_scoped(&request.transaction, now, SCOPED_FEATURES, &request.claims, scope.authority()),
            None => pipeline.decide(&request.transaction, now, NATIVE_WRITE_FEATURES, &request.claims),
        };
        match decision {
            Ok(Decision::Answer(receipt)) => self.source_reply(source, Ok(receipt)),
            Err(error) if error.is_caller_error() => self.source_reply(source, Err(error)),
            Err(error) => Err(error),
            Ok(Decision::Append { position, record }) => {
                let installs_source = request.transaction.window.before.is_none();
                let message = decision_message(position, &record, Some(source), installs_source);
                self.pending.insert(position, message.clone());
                Ok(message)
            }
        }
    }

    fn source_reply(&mut self, source: SourcePosition, reply: Result<Receipt>) -> Result<proto::NoOp> {
        let mut record = vec![SOURCE_REPLY_FRAME];
        serde_json::to_writer(&mut record, &reply).map_err(encoding_error)?;
        self.pending_replies += 1;
        Ok(proto::NoOp { src_msg_id: Some(source.0), serving_record: Some(record.into()), ..Default::default() })
    }

    pub(crate) fn validate(&self, request: NativeRequest, now: Timestamp) -> Result<Receipt> {
        if !self.view.borrow().compatible { return Err(incompatible()); }
        if !request.is_validation() { return Err(invalid_source_request()); }
        if self.request_pending(&request) { return self.status(&request.transaction.key, request.scope.as_ref()); }
        let durable = self.durable();
        let position = durable.index().head().basis.position();
        let resolution = match &request.scope {
            Some(scope) => durable.resolve_scoped(&request.transaction, position, now, SCOPED_FEATURES, &request.claims, scope.authority()),
            None => durable.resolve(&request.transaction, position, now, NATIVE_WRITE_FEATURES, &request.claims),
        }?;
        let Resolution::Answer(receipt) = resolution else { return Err(invalid()); };
        project_receipt(durable.index(), receipt, request.scope.as_ref())
    }

    pub(crate) fn holds_checkpoint(&self) -> bool {
        self.durable().recover_pending().is_some()
            || self.pipeline.as_ref().is_some_and(|pipeline| pipeline.awaiting_terminal().next().is_some())
    }

    pub(crate) fn progress_timed(&mut self, clock_upper: Timestamp) -> Result<Option<proto::NoOp>> {
        let Some(refusal @ TransactionRecord::Refuse { tx_lsn }) = self.durable().recover_pending() else {
            return Ok(None);
        };
        let position = self.next_position()?;
        if self.pending.contains_key(&position) { return Ok(None); }
        let record = match &mut self.pipeline {
            Some(pipeline) => {
                let terminal = pipeline.terminal(tx_lsn, clock_upper)?;
                if terminal.position != position || !terminal.redecide.is_empty() { return Err(invalid()); }
                terminal.record
            }
            None => refusal.encode()?,
        };
        let message = decision_message(position, &record, self.timed_source, true);
        self.pending.insert(position, message.clone());
        Ok(Some(message))
    }

    pub(crate) fn apply(&mut self, message: &proto::NoOp, own: bool) -> Result<Option<(SourcePosition, Result<Receipt>)>> {
        let frame = message.serving_record.as_deref().ok_or_else(invalid)?;
        let installed = message.src_msg_id.map(SourcePosition);
        if frame.first() == Some(&SOURCE_REPLY_FRAME) {
            let source = installed.ok_or_else(invalid)?;
            if self.durable().recover_pending().is_some() { return Err(invalid()); }
            let reply = serde_json::from_slice(&frame[1..]).map_err(|_| invalid())?;
            if own { self.pending_replies = self.pending_replies.checked_sub(1).ok_or_else(invalid)?; }
            return Ok(Some((source, reply)));
        }

        let (position, source, bytes) = decode(frame)?;
        if position != self.next_position()? || own && self.pending.get(&position) != Some(message) {
            return Err(invalid());
        }
        let terminal = self.durable().recover_pending().is_some();
        if terminal && source != self.timed_source { return Err(invalid()); }
        let receipts = match &mut self.pipeline {
            Some(pipeline) if own => pipeline.acknowledge(position)?,
            Some(pipeline) => pipeline.recover(position, bytes)?.into_iter().collect(),
            None => self.fold.as_mut().ok_or_else(invalid)?.apply(position, TransactionRecord::decode(bytes)?)?.into_iter().collect(),
        };
        self.position = position;
        if own { self.pending.remove(&position); }
        let mut receipts = receipts.into_iter();
        let Some(receipt) = receipts.next() else {
            if installed.is_some() || terminal || self.durable().recover_pending().is_none() { return Err(invalid()); }
            self.timed_source = source;
            return Ok(None);
        };
        self.timed_source = None;
        if installed != source || receipts.next().is_some() || receipt.basis.position() != position { return Err(invalid()); }
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
        Ok(source.map(|source| (source, Ok(receipt))))
    }

    pub(crate) fn status(&self, key: &[u8], scope: Option<&NativeServingScope>) -> Result<Receipt> {
        if !self.view.borrow().compatible { return Err(incompatible()); }
        let receipt = match (&self.pipeline, scope) {
            (Some(pipeline), Some(scope)) => pipeline.tx_status_scoped(key, scope.authority()),
            (Some(pipeline), None) => pipeline.tx_status(key),
            (None, Some(scope)) => self.durable().tx_status_scoped(key, scope.authority()),
            (None, None) => self.durable().tx_status(key),
        }?;
        project_receipt(self.durable().index(), receipt, scope)
    }

    pub(crate) fn withdraw(&mut self) {
        if let Some(pipeline) = self.pipeline.take() {
            self.fold = Some(pipeline.into_durable());
        }
        self.pending.clear();
        self.pending_replies = 0;
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

fn project_receipt(index: &BranchIndex, mut receipt: Receipt, scope: Option<&NativeServingScope>) -> Result<Receipt> {
    if let Some(scope) = scope {
        let cut = index.project_main_visibility(&scope.labels)?.projected_cut(receipt.basis)?;
        receipt.basis = cut.basis;
        receipt.system_time = cut.system_time;
    }
    Ok(receipt)
}

fn decision_message(position: u64, record: &[u8], source: Option<SourcePosition>, installs_source: bool) -> proto::NoOp {
    let mut frame = BytesMut::with_capacity(SOURCE_DECISION_HEADER_BYTES + record.len());
    frame.put_u8(if source.is_some() { SOURCE_DECISION_FRAME } else { FRAME_VERSION });
    frame.put_u64(position);
    if let Some(source) = source { frame.put_i64(source.0); }
    frame.extend_from_slice(record);
    proto::NoOp {
        src_msg_id: source.filter(|_| installs_source).map(|source| source.0),
        serving_record: Some(frame.freeze()),
        ..Default::default()
    }
}

fn encoding_error(error: serde_json::Error) -> Anomaly {
    Anomaly::incorrect("rtdb/transaction-encoding", error.to_string())
}

fn invalid_source_request() -> Anomaly {
    Anomaly::incorrect("rtdb/source-admission", "Source intake requires a transaction with operations")
}

fn decode(frame: &[u8]) -> Result<(u64, Option<SourcePosition>, &[u8])> {
    let header_len = match frame.first().copied() {
        Some(SOURCE_DECISION_FRAME) => SOURCE_DECISION_HEADER_BYTES,
        Some(FRAME_VERSION) => FRAME_HEADER_BYTES,
        Some(1) => LEGACY_FRAME_HEADER_BYTES,
        _ => return Err(invalid()),
    };
    let (header, record) = frame.split_at_checked(header_len).ok_or_else(invalid)?;
    let mut fields = &header[1..];
    let position = fields.get_u64();
    let source = if header_len == SOURCE_DECISION_HEADER_BYTES {
        let source = fields.get_i64();
        if source < 0 { return Err(invalid()); }
        Some(SourcePosition(source))
    } else { None };
    if position == 0 || record.is_empty() || header_len == LEGACY_FRAME_HEADER_BYTES && header[9..25] == [0; 16] {
        return Err(invalid());
    }
    Ok((position, source, record))
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
    use rtdb_storage::{LocalStore, MemoryStore, ObjectKey, cache::BufferPool};
    use rtdb_tx::transact::{Cell, Op, Row, Window};
    use rtdb_types::{BlockIndex, EntityId, StorageEpoch, StructFields, TableRef, Value};

    use super::*;

    const INCARNATION: [u8; 16] = [7; 16];

    fn authority() -> NativeServingAuthority {
        NativeServingAuthority { incarnation: INCARNATION, checkpoints: None }
    }

    fn open(checkpoint: Option<&[u8]>) -> (NativeServing, watch::Receiver<NativeServingView>) {
        NativeServing::open(authority(), Arc::new(MemoryStore::default()), checkpoint, false).unwrap()
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

    fn unscoped(transaction: Transact) -> SourceRequest {
        NativeRequest { transaction, claims: source::SourceClaims::default(), scope: None }
    }

    #[test]
    fn timed_terminal_alone_installs_the_source_position() {
        for (upper, settled) in [(Timestamp(55), true), (Timestamp(80), false)] {
            let (mut leader, view) = open(None);
            let (mut follower, _) = open(None);
            let mut request = put(1);
            request.window.before = Some(Timestamp(80));
            let source = SourcePosition(8);
            let original = leader.decide_source(source, Timestamp(50), Ok(unscoped(request))).unwrap();
            assert_eq!(original.src_msg_id, None);
            assert!(!leader.source_ready());
            assert!(leader.apply(&original, true).unwrap().is_none());
            assert!(follower.apply(&original, false).unwrap().is_none());
            assert_eq!(view.borrow().index.head().basis.position(), 0);
            assert_eq!(leader.status(&[1; 16], None).unwrap_err().code, "rtdb/transaction-pending");

            let terminal = leader.progress_timed(upper).unwrap().unwrap();
            assert!(!leader.source_ready());
            let (applied, result) = leader.apply(&terminal, true).unwrap().unwrap();
            assert_eq!(applied, source);
            assert_eq!(follower.apply(&terminal, false).unwrap(), Some((applied, result.clone())));
            let receipt = result.unwrap();
            assert_eq!(receipt.basis.position(), 2);
            assert_eq!(matches!(receipt.outcome, rtdb_tx::transact::Outcome::Then), settled);
            assert!(leader.source_ready());
        }
    }

    #[test]
    fn takeover_refuses_the_replayed_timed_source_even_inside_the_window() {
        let (mut original, _) = open(None);
        let mut request = put(1);
        request.window.before = Some(Timestamp(80));
        let source = SourcePosition(9);
        let message = original.decide_source(source, Timestamp(50), Ok(unscoped(request.clone()))).unwrap();
        let (mut successor, _) = open(None);
        assert!(successor.apply(&message, false).unwrap().is_none());
        successor.withdraw();
        let terminal = successor.progress_timed(Timestamp(52)).unwrap().unwrap();
        let (completed, receipt) = successor.apply(&terminal, true).unwrap().unwrap();
        assert_eq!(completed, source);
        let receipt = receipt.unwrap();
        assert!(matches!(&receipt.outcome,
            rtdb_tx::transact::Outcome::Refused { error } if error.code == "rtdb/timed-transaction-refused"));
        let next = SourcePosition(10);
        let retry = successor.decide_source(next, Timestamp(53), Ok(unscoped(request))).unwrap();
        let (retried_at, retry) = successor.apply(&retry, true).unwrap().unwrap();
        assert_eq!(retried_at, next);
        assert!(retry.as_ref().unwrap().replayed);
        assert_eq!(retry.unwrap().basis, receipt.basis);
    }

    #[test]
    fn serving_source_takes_over_from_the_read_back_prefix() {
        let (mut serving, view) = open(None);
        let first = serving.decide_source(SourcePosition(2), Timestamp(41), Ok(unscoped(put(1)))).unwrap();
        let excluded = serving.decide_source(SourcePosition(3), Timestamp(41), Ok(unscoped(put(2)))).unwrap();
        let (position, source, bytes) = decode(first.serving_record.as_deref().unwrap()).unwrap();
        let TransactionRecord::Tx(record) = TransactionRecord::decode(bytes).unwrap() else { panic!() };
        assert_eq!((position, source), (1, Some(SourcePosition(2))));
        assert_eq!(record.admission().0, &[1; 16]);
        assert_eq!(record.admission().1, Timestamp(41));
        assert_eq!(record.canonical_request().unwrap().get(), put(1).canonical_request().unwrap().get());
        assert_eq!(serving.status(&[1; 16], None).unwrap_err().code, "rtdb/transaction-pending");
        assert_eq!(view.borrow().index.head().basis.position(), 0);

        serving.withdraw();
        assert_eq!(serving.apply(&first, false).unwrap().unwrap().0, SourcePosition(2));
        assert!(serving.status(&[1; 16], None).unwrap().replayed);
        assert_eq!(serving.status(&[2; 16], None).unwrap_err().code, "rtdb/unknown-transaction");
        let reread = serving.decide_source(SourcePosition(3), Timestamp(42), Ok(unscoped(put(2)))).unwrap();
        assert!(serving.apply(&excluded, true).is_err());
        assert_eq!(serving.apply(&reread, true).unwrap().unwrap().1.unwrap().basis.position(), 2);

        let image = serving.checkpoint(Term(2), true).unwrap().image;
        let (recovered, _) = open(Some(&image));
        assert_eq!(recovered.status(&[2; 16], None).unwrap().basis.position(), 2);
    }

    #[test]
    fn source_order_matches_replica_replay_across_concurrent_decisions() {
        for batch_size in [1, 2, 3, 5, 8] {
            let (mut leader, _) = open(None);
            let (mut follower, _) = open(None);
            for batch in (1..=24).collect::<Vec<u8>>().chunks(batch_size) {
                let messages = batch.iter().map(|&key| {
                    leader.decide_source(SourcePosition(i64::from(key) * 2), Timestamp(50), Ok(unscoped(put(key)))).unwrap()
                }).collect::<Vec<_>>();
                for (&key, message) in batch.iter().zip(messages) {
                    let (position, reply) = leader.apply(&message, true).unwrap().unwrap();
                    assert_eq!(position, SourcePosition(i64::from(key) * 2));
                    assert_eq!(follower.apply(&message, false).unwrap(), Some((position, reply.clone())));
                    assert_eq!(reply.unwrap().basis.position(), u64::from(key));
                }
            }
            assert_eq!(leader.checkpoint(Term(1), true).unwrap().image, follower.checkpoint(Term(1), true).unwrap().image);
        }
    }

    #[test]
    fn serving_scoped_receipts_are_projected_and_recover_in_their_namespace() {
        use rtdb_tx::transact::{Declaration, Outcome, Visibility};

        let (mut serving, _) = open(None);
        let mut declaration = put(1);
        declaration.ops = vec![Op::Declare {
            table: TableRef::new("public", "items"),
            change: Declaration::Visibility(Visibility { column: "label".into() }),
        }];
        let declared = serving.decide_source(SourcePosition(1), Timestamp(40), Ok(unscoped(declaration))).unwrap();
        assert_eq!(serving.apply(&declared, true).unwrap().unwrap().1.unwrap().outcome, Outcome::Then);

        let scope = NativeServingScope {
            labels: LabelScope { read: [vec![7]].into_iter().collect(), write: [vec![7]].into_iter().collect() },
            receipt_namespace: [7; 32],
        };
        let mut submission = put(2);
        let Op::Put { rows, .. } = &mut submission.ops[0] else { unreachable!() };
        rows[0].valid = Some(rtdb_types::Interval::ALL.into());
        rows[0].cells.insert("label".into(), Cell::Value(Value::Binary(vec![7])));
        let request = NativeRequest { transaction: submission, claims: source::SourceClaims::default(), scope: Some(scope.clone()) };
        let message = serving.decide_source(SourcePosition(2), Timestamp(41), Ok(request)).unwrap();
        let receipt = serving.apply(&message, true).unwrap().unwrap().1.unwrap();
        let receipt = project_receipt(serving.durable().index(), receipt, Some(&scope)).unwrap();
        assert_eq!(receipt.outcome, Outcome::Then);
        assert_eq!(receipt.basis.incarnation(), scope.labels.main_read_incarnation(INCARNATION));

        let checkpoint = serving.checkpoint(Term(1), true).unwrap();
        let (reopened, _) = open(Some(&checkpoint.image));
        assert_eq!(reopened.status(&[2; 16], Some(&scope)).unwrap().basis, receipt.basis);
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
