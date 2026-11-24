use std::{collections::{BTreeMap, VecDeque}, sync::Arc};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_crypt::{Generation, Kms, tokens::VerifiedClaims};
use rtdb_format::journal::ObjectId;
use rtdb_format::{envelope::{SourceBody, SourceMessage}, log::proto};
use rtdb_index::{Snapshot, StagedWrite, branches::{BranchCut, BranchEffect, BranchIndex, BranchWrite}};
use rtdb_tx::transact::{
    COUNT_RANGE_CONSTRAINT, Decision, Erasure, GROUPED_COUNT_CONSTRAINT, InstalledReceipt, LINEARIZABLE, RELATIVE_VALID_TIME, ROW_CUSTODY, PHYSICAL_ERASURE, Receipt, Resolution, ScopedTransact,
    TIMED_TRANSACT, TRANSACT_REQUIRES, Transact, TransactPipeline, TransactState,
    TransactionRecord, TrustedCustodyCommand,
};
use rtdb_storage::ObjectStore;
use rtdb_store::{Reachability, WriterObjects};
use rtdb_types::{Anomaly, AnomalyCategory, CatalogCut, ReplicaPosition, Result, SourcePosition, Term, Timestamp, rtdb_v1::{At, Basis, LabelScope, QUERY_WRITTEN_BASIS}};
use parking_lot::Mutex as ReplyLock;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, oneshot, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{ReplicationHandle, default_erasure::{self, RewriteEvidence}};

pub(crate) mod classic;
pub(crate) mod retained;
mod source;
#[cfg(test)]
mod checkpoint_delivery_tests;
use source::SourceAuthority;
pub(crate) use source::SourceRequest;

const MAX_IN_FLIGHT: usize = 64;
const MAX_CHANGE_GROUPS: usize = 4096;
const FRAME_VERSION: u8 = 2;
pub(crate) const SHARED_CHECKPOINT: u8 = b'S';
const SOURCE_DECISION_FRAME: u8 = 3;
const SOURCE_REPLY_FRAME: u8 = 4;
const SHARED_DECISION_FRAME: u8 = 5;
const SOURCE_DECISION_HEADER_BYTES: usize = FRAME_HEADER_BYTES + 8;
const MAX_SOURCE_REPLIES: usize = 4096;
const FRAME_HEADER_BYTES: usize = 9;
const LEGACY_FRAME_HEADER_BYTES: usize = 65;
const CHECKPOINT_HEADER_BYTES: usize = 40;

pub const NATIVE_WRITE_FEATURES: &[&str] = &[
    "query", "query.track", "query.page", "query.linearizable", QUERY_WRITTEN_BASIS, LINEARIZABLE, "branches", "branch.local-layer",
    "changes", "changes.progress-only", "transact", TRANSACT_REQUIRES, RELATIVE_VALID_TIME, TIMED_TRANSACT, COUNT_RANGE_CONSTRAINT, GROUPED_COUNT_CONSTRAINT, "row-visibility", "tx-status", "sql.history",
    "sql.search", "sql.search-table", "sql.nearest-table", "sql.cosine-distance",
    rtdb_types::rtdb_v1::CLEAR_RANK_INPUT,
    rtdb_types::rtdb_v1::RANKED_ARTEFACT_READINESS,
    rtdb_types::rtdb_v1::RANKED_INDEX_DECLARATIONS,
];
pub const NATIVE_READ_FEATURES: &[&str] = &[
    "query", "query.track", "query.page", "query.linearizable", QUERY_WRITTEN_BASIS, "branches", "branch.local-layer",
    "changes", "changes.progress-only", "tx-status", "sql.history",
    "sql.search", "sql.search-table", "sql.nearest-table", "sql.cosine-distance",
    rtdb_types::rtdb_v1::CLEAR_RANK_INPUT,
    rtdb_types::rtdb_v1::RANKED_ARTEFACT_READINESS,
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

pub(crate) enum ReceiptLookup {
    Status(Option<NativeServingScope>),
    ExactRequest { scope: Option<NativeServingScope>, fingerprint: [u8; 32] },
    InstalledCustody(Basis),
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

pub(crate) enum NativeCheckpointImage {
    Inline(Bytes),
    Sealed { id: ObjectId, opened: rtdb_crypt::Plaintext },
}

impl NativeCheckpointImage {
    pub(crate) fn bytes(&self) -> &[u8] {
        match self { Self::Inline(bytes) => bytes, Self::Sealed { opened, .. } => &opened.bytes }
    }

    pub(crate) fn key_generation(&self) -> Option<u64> {
        self.generation().map(|generation| generation.number)
    }

    pub(crate) fn generation(&self) -> Option<Generation> {
        match self { Self::Inline(_) => None, Self::Sealed { opened, .. } => Some(opened.generation) }
    }
}

impl NativeServingAuthority {
    pub(crate) async fn read_checkpoint(&self, store: &dyn ObjectStore, inline: Option<&[u8]>, object: Option<&str>) -> Result<Option<NativeCheckpointImage>> {
        Self::read_selected_checkpoint(self.incarnation, self.checkpoints.as_ref().map(|authority| authority.kms.as_ref()), store, inline, object).await
    }

    pub(crate) async fn read_selected_checkpoint(incarnation: [u8; 16], kms: Option<&dyn Kms>, store: &dyn ObjectStore,
        inline: Option<&[u8]>, object: Option<&str>,
    ) -> Result<Option<NativeCheckpointImage>> {
        match (inline, object) {
            (Some(bytes), None) => Ok(Some(NativeCheckpointImage::Inline(Bytes::copy_from_slice(bytes)))),
            (None, None) => Ok(None),
            (None, Some(key)) => {
                let id: ObjectId = key.parse()?;
                if id.database.as_bytes() != &incarnation { return Err(invalid()); }
                let opened = rtdb_store::read_file(store, kms.ok_or_else(unavailable)?, id).await?;
                Ok(Some(NativeCheckpointImage::Sealed { id, opened }))
            }
            (Some(_), Some(_)) => Err(invalid()),
        }
    }

    /// Rechecks the generation captured by the view before releasing its contents.
    pub async fn check_checkpoint_generation(&self, generation: u64) -> Result<()> {
        let checkpoints = self.checkpoints.as_ref().ok_or_else(unavailable)?;
        checkpoints.kms.check_readable(Generation { database: Uuid::from_bytes(self.incarnation), number: generation }).await
    }
}

#[derive(Clone)]
pub struct NativeServingView {
    pub index: BranchIndex,
    pub floor: Basis,
    pub changes: Arc<VecDeque<(BranchCut, Vec<BranchWrite>)>>,
    pub key_generation: Option<u64>,
    pub read_custody: Option<Arc<crate::LocalErasureRead>>,
    compatible: bool,
    classic_head: Option<ReplicaPosition>,
    ranked_catalog: Option<(BranchCut, CatalogCut)>,
}

#[derive(Clone)]
pub struct NativeServingHandle {
    pub(crate) replication: ReplicationHandle,
    pub(crate) view: watch::Receiver<NativeServingView>,
    pub(crate) authority: NativeServingAuthority,
    pub(crate) source_authority: Arc<SourceAuthority>,
    pub(crate) source_replies: Arc<SourceReplies>,
    pub(crate) local_read_fence: Option<Arc<crate::LocalDefaultErasureReadFence>>,
}

impl NativeServingHandle {
    pub fn authority(&self) -> &NativeServingAuthority { &self.authority }
    pub fn has_local_erasure_fence(&self) -> bool { self.local_read_fence.is_some() }
    pub fn supports_row_custody(&self) -> bool {
        self.authority.checkpoints.is_some() && !self.has_local_erasure_fence()
    }

    pub fn supports_ranked_catalog(&self) -> bool {
        let view = self.view.borrow();
        view.compatible && view.ranked_catalog.is_some() && !self.has_local_erasure_fence()
    }

    pub fn ranked_catalog(&self, selected: &NativeServingView, cut: BranchCut) -> Option<Snapshot> {
        if self.has_local_erasure_fence() { return None; }
        let (native, physical) = selected.ranked_catalog?;
        if native != cut || selected.index.head() != cut { return None; }
        let snapshot = self.replication.serving_partition()?;
        (snapshot.catalog_cut() == Some(physical)
            && snapshot.tx_basis().is_some_and(|basis| basis.system_time == cut.system_time)).then_some(snapshot)
    }

    pub async fn check_read_generation(&self, generation: u64) -> Result<()> {
        let _admission = match &self.local_read_fence {
            Some(fence) => Some(fence.admit(generation).await?),
            None => None,
        };
        self.authority.check_checkpoint_generation(generation).await
    }

    pub fn is_compatible(&self) -> bool { self.view.borrow().compatible }

    pub fn has_classic_read(&self) -> bool {
        let view = self.view.borrow();
        !view.compatible && view.index.head().basis.position() == 0 && view.classic_head.is_some()
    }

    pub async fn classic_read(&self, at: At, cancellation: &CancellationToken) -> Result<(Snapshot, BranchCut)> {
        if at == At::Linearizable { self.replication.serving_barrier(cancellation).await?; }
        let mut status = self.replication.watch_status();
        loop {
            let (role, published) = {
                let status = status.borrow_and_update();
                (status.role, status.installed.is_some())
            };
            if role == crate::Role::Stopped { return Err(unavailable()); }
            let classic = {
                let view = self.view.borrow();
                if view.compatible || view.index.head().basis.position() != 0 { return Err(incompatible()); }
                view.classic_head.ok_or_else(unavailable)?
            };
            if published {
                let snapshot = self.replication.serving_partition().ok_or_else(unavailable)?;
                let through = snapshot.catalog_cut().ok_or_else(unavailable)?.through;
                if through >= classic {
                    let position = replica_basis_position(through)?;
                    let basis = Basis::new(self.authority.incarnation, position);
                    let system_time = snapshot.tx_basis().ok_or_else(invalid)?.system_time;
                    let ready = match at {
                        At::Exactly(requested) | At::AtLeast(requested) if requested.incarnation() != basis.incarnation() => return Err(foreign_basis()),
                        At::Exactly(requested) if requested < basis => return Err(Anomaly::new(
                            AnomalyCategory::NotFound, "rtdb/cursor-expired", "The Classic catalog no longer holds the requested replica cut",
                        )),
                        At::Exactly(requested) if requested > basis => return Err(Anomaly::new(
                            AnomalyCategory::NotFound, "rtdb/unknown-basis", "The Classic catalog has not installed the requested replica cut",
                        )),
                        At::AtLeast(requested) => requested <= basis,
                        At::AsOf(time) if time < system_time => return Err(Anomaly::unsupported(
                            "rtdb/classic-historical-basis", "The Classic catalog has no retained replica cut for the requested time",
                        )),
                        _ => true,
                    };
                    if ready { return Ok((snapshot, BranchCut { basis, system_time })); }
                }
            }
            cancellation.run_until_cancelled(status.changed()).await.ok_or_else(interrupted)?.map_err(|_| unavailable())?;
        }
    }

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
        let mut snapshot = view.borrow().clone();
        if !snapshot.compatible { return Err(incompatible()); }
        if let Some(fence) = &self.local_read_fence {
            let generation = snapshot.key_generation.ok_or_else(default_erasure::unsupported)?;
            snapshot.read_custody = Some(fence.admit(generation).await?);
        }
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
        if let Err(error) = request.check_source_admission(self.has_local_erasure_fence(), self.supports_row_custody()) {
            if !request.transaction.uses_erasure()
                && let Some(fingerprint) = request.transaction.custody_request_digest
            {
                self.replication.serving_barrier(cancellation).await?;
                return self.replication.serving_status(
                    request.transaction.key, ReceiptLookup::ExactRequest { scope: request.scope, fingerprint }, cancellation,
                ).await.map_err(|status| if status.code == "rtdb/unknown-transaction" { error } else { status });
            }
            return Err(error);
        }
        let scope = request.scope.clone();
        let bytes = cancellation.run_until_cancelled(self.source_authority.seal(request)).await
            .ok_or_else(interrupted)??;
        self.submit_admitted(bytes, scope, cancellation).await
    }

    pub async fn submit_custody_release(&self, command: TrustedCustodyCommand, cancellation: &CancellationToken) -> Result<Receipt> {
        if !self.is_compatible() { return Err(incompatible()); }
        let bytes = cancellation.run_until_cancelled(self.source_authority.seal_release(command)).await
            .ok_or_else(interrupted)??;
        self.submit_admitted(bytes, None, cancellation).await
    }

    async fn submit_admitted(&self, bytes: Bytes, scope: Option<NativeServingScope>, cancellation: &CancellationToken) -> Result<Receipt> {
        let message = SourceMessage::Protobuf(SourceBody::ServingTransact(bytes));
        let position = self.replication.submit_serving_source(&message, cancellation).await?;
        self.replication.await_source(position, cancellation).await.map_err(|error| {
            if error.category == AnomalyCategory::Interrupted { interrupted() } else { error }
        })?;
        let receipt = self.source_replies.get(position).ok_or_else(interrupted)??;
        let Some(scope) = &scope else {
            require_armed_erase_receipt(&receipt)?;
            return Ok(receipt);
        };
        let index = self.view.borrow().index.clone();
        project_receipt(&index, receipt, Some(scope))
    }

    pub async fn tx_status(&self, key: Vec<u8>, scope: Option<NativeServingScope>, cancellation: &CancellationToken) -> Result<Receipt> {
        self.replication.serving_barrier(cancellation).await?;
        self.replication.serving_status(key, ReceiptLookup::Status(scope), cancellation).await
    }

    pub async fn original_custody_receipt(&self, key: Vec<u8>, basis: Basis, cancellation: &CancellationToken) -> Result<Receipt> {
        self.replication.serving_barrier(cancellation).await?;
        self.replication.serving_status(key, ReceiptLookup::InstalledCustody(basis), cancellation).await
    }
}

impl<C> NativeRequest<C> {
    fn is_validation(&self) -> bool {
        self.transaction.ops.is_empty() && self.transaction.otherwise.is_empty()
    }

    fn check_source_admission(&self, physical_erasure: bool, row_custody: bool) -> Result<()> {
        if self.transaction.uses_erasure() && (!physical_erasure || self.scope.is_some()) {
            return Err(Anomaly::unsupported("rtdb/erasure-required", "Default erasure needs ordered retirement of the source and replica keys"));
        }
        let row_custody = row_custody && self.scope.is_none();
        if self.transaction.uses_custody() && !self.transaction.is_response_release() && !row_custody {
            return Err(Anomaly::unsupported("rtdb/feature-required", "Default row custody requires an installed delivery fence"));
        }
        self.transaction.require_sealed_custody(if row_custody { &[ROW_CUSTODY] } else { &[] })
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
    erasure_in_flight: bool,
    pub(crate) physical_erasure: bool,
    pending_replies: usize,
    shares_main: bool,
    classic_charges: Vec<Arc<MemoryReservation>>,
    view: watch::Sender<NativeServingView>,
    pub(crate) source_authority: Arc<SourceAuthority>,
    pub(crate) source_replies: Arc<SourceReplies>,
}

pub(crate) struct ServingCheckpoint {
    image: Bytes,
    publication: CheckpointPublication,
}

enum CheckpointPublication {
    Inline,
    Writer(CheckpointWriter),
    Retained { id: ObjectId, generation: Generation, kms: Arc<dyn Kms> },
}

struct CheckpointWriter {
    allocator: Arc<Mutex<WriterObjects>>,
    authority: NativeCheckpointAuthority,
    database: Uuid,
    epoch: u64,
    collect: bool,
}

impl ServingCheckpoint {
    pub(crate) fn write_generation(&self) -> Result<u64> {
        let CheckpointPublication::Writer(writer) = &self.publication else {
            return Err(default_erasure::unsupported());
        };
        Ok(writer.authority.generation)
    }

    pub(crate) fn bind_rewrite(&mut self, evidence: RewriteEvidence) -> Result<()> {
        let (prefix, state) = self.image.split_at_checked(CHECKPOINT_HEADER_BYTES + 1).ok_or_else(default_erasure::invalid)?;
        if prefix.last() != Some(&SHARED_CHECKPOINT) || RewriteEvidence::read(state)?.0.is_some() {
            return Err(default_erasure::invalid());
        }
        let CheckpointPublication::Writer(writer) = &mut self.publication else {
            return Err(default_erasure::unsupported());
        };
        if evidence.prior_generation.is_some_and(|prior| prior != writer.authority.generation) {
            return Err(default_erasure::invalid());
        }
        writer.authority.generation = writer.authority.generation.checked_add(1).ok_or_else(default_erasure::invalid)?;
        writer.collect = false;
        self.image = evidence.frame(prefix, state)?;
        Ok(())
    }

    pub(crate) async fn store_rewrite(self, blocks: &crate::BlockStore, cancellation: &CancellationToken) -> Result<String> {
        let CheckpointPublication::Writer(writer) = &self.publication else {
            return Err(default_erasure::unsupported());
        };
        writer.authority.kms.create(Generation { database: writer.database, number: writer.authority.generation }).await?;
        self.store(blocks, cancellation).await?.1.ok_or_else(default_erasure::invalid)
    }

    pub(crate) async fn verify(&self, store: &dyn ObjectStore, inline: Option<&[u8]>, object: Option<&str>) -> Result<()> {
        let same = match (&self.publication, inline, object) {
            (CheckpointPublication::Inline, Some(inline), None) => self.image.as_ref() == inline,
            (CheckpointPublication::Writer(writer), None, Some(key)) => {
                let id: ObjectId = key.parse()?;
                if id.database != writer.database || id.epoch > writer.epoch { return Err(invalid()); }
                let plaintext = rtdb_store::read_file(store, writer.authority.kms.as_ref(), id).await?;
                self.image.as_ref() == plaintext.bytes.as_slice()
                    && plaintext.generation.number == writer.authority.generation
            }
            (CheckpointPublication::Retained { id, generation, kms }, None, Some(key)) if key.parse::<ObjectId>()? == *id => {
                let plaintext = rtdb_store::read_file(store, kms.as_ref(), *id).await?;
                plaintext.generation == *generation && self.image.as_ref() == plaintext.bytes.as_slice()
            }
            _ => false,
        };
        if same { Ok(()) } else { Err(invalid()) }
    }

    pub(crate) async fn store(self, blocks: &crate::BlockStore, cancellation: &CancellationToken) -> Result<(Option<Vec<u8>>, Option<String>)> {
        let store = blocks.object_store();
        let writer = match &self.publication {
            CheckpointPublication::Inline => return Ok((Some(self.image.to_vec()), None)),
            CheckpointPublication::Retained { id, .. } => {
                let key = id.to_string();
                self.verify(store, None, Some(&key)).await?;
                return Ok((None, Some(key)));
            }
            CheckpointPublication::Writer(writer) => writer,
        };
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
    fn source_features<C>(&self, request: &NativeRequest<C>) -> std::borrow::Cow<'static, [&'static str]> {
        use std::borrow::Cow;
        if request.scope.is_some() {
            return Cow::Borrowed(SCOPED_FEATURES);
        }
        let extra = if self.physical_erasure && self.shares_main &&
            (request.transaction.uses_erasure() || request.transaction.require.iter().any(|feature| feature == PHYSICAL_ERASURE))
        {
            Some(PHYSICAL_ERASURE)
        } else if self.authority.checkpoints.is_some() && !self.physical_erasure &&
            (request.transaction.uses_custody() || request.transaction.require.iter().any(|feature| feature == ROW_CUSTODY))
        {
            Some(ROW_CUSTODY)
        } else {
            None
        };
        match extra {
            Some(feature) => Cow::Owned([NATIVE_WRITE_FEATURES, &[feature]].concat()),
            None => Cow::Borrowed(NATIVE_WRITE_FEATURES),
        }
    }

    pub(crate) fn open(mut authority: NativeServingAuthority, store: Arc<dyn ObjectStore>, checkpoint: Option<&NativeCheckpointImage>, collect_checkpoints: bool) -> Result<(Self, watch::Receiver<NativeServingView>)> {
        let (fold, position, shared_checkpoint) = match checkpoint {
            Some(checkpoint) => decode_checkpoint_image(checkpoint.bytes(), authority.incarnation)?,
            None => (TransactState::new(authority.incarnation), 0, false),
        };
        let key_generation = checkpoint.and_then(NativeCheckpointImage::key_generation)
            .or_else(|| authority.checkpoints.as_ref().map(|keys| keys.generation));
        if let Some(generation) = checkpoint.and_then(NativeCheckpointImage::key_generation)
            && let Some(keys) = &mut authority.checkpoints
        {
            keys.generation = keys.generation.max(generation);
        }
        let (view, receiver) = watch::channel(NativeServingView {
            index: fold.index().clone(),
            floor: fold.index().head().basis,
            key_generation,
            read_custody: None,
            changes: Arc::default(), compatible: true, classic_head: None, ranked_catalog: None,
        });
        let source_authority = Arc::new(SourceAuthority::new(store, authority.incarnation));
        Ok((Self {
            authority, fold: Some(fold), position, pipeline: None,
            collect_checkpoints, writer: None, pending: BTreeMap::new(),
            timed_source: None, erasure_in_flight: false, physical_erasure: false, pending_replies: 0,
            shares_main: shared_checkpoint || position == 0, classic_charges: Vec::new(),
            view, source_authority, source_replies: Arc::default(),
        }, receiver))
    }

    pub(crate) fn checkpoint(&mut self, term: Term, confirmed: bool) -> Result<ServingCheckpoint> {
        let fold = self.durable();
        let cut = fold.index().head();
        let checkpoint = fold.encode_checkpoint()?;
        let mut frame = BytesMut::with_capacity(CHECKPOINT_HEADER_BYTES + usize::from(self.shares_main) + checkpoint.len());
        frame.put_u64(self.position);
        frame.extend_from_slice(&cut.basis.to_bytes());
        frame.put_i64(cut.system_time.0);
        if self.shares_main { frame.put_u8(SHARED_CHECKPOINT); }
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
        let publication = writer.map_or(CheckpointPublication::Inline, CheckpointPublication::Writer);
        Ok(ServingCheckpoint { image: frame.freeze(), publication })
    }

    pub(crate) fn repair_checkpoint(&self, selected: &NativeCheckpointImage, kms: Option<Arc<dyn Kms>>) -> Result<ServingCheckpoint> {
        let (expected, position, shared) = decode_checkpoint_image(selected.bytes(), self.authority.incarnation)?;
        if self.position != position || self.shares_main != shared { return Err(invalid()); }
        self.durable().verify_repaired_checkpoint(&expected)?;
        let publication = match selected {
            NativeCheckpointImage::Inline(_) => CheckpointPublication::Inline,
            NativeCheckpointImage::Sealed { id, opened } => CheckpointPublication::Retained {
                id: *id, generation: opened.generation, kms: kms.ok_or_else(unavailable)?,
            },
        };
        Ok(ServingCheckpoint { image: Bytes::copy_from_slice(selected.bytes()), publication })
    }

    pub(crate) fn checkpoint_authority(&self) -> &NativeServingAuthority { &self.authority }

    pub(crate) fn pending_erasure(&self) -> Option<Basis> { self.durable().pending_erasure() }

    pub(crate) fn pending_erasure_installation(&self) -> Option<(Basis, InstalledReceipt)> {
        self.durable().pending_erasure_installation()
    }

    pub(crate) fn adopt_erasure_generation(&self, generation: u64) {
        self.view.send_modify(|view| {
            view.key_generation = Some(generation);
            view.floor = view.index.head().basis;
            view.changes = Arc::default();
        });
    }

    pub(crate) fn complete_erasure(&mut self, cut: default_erasure::ErasureCut, generation: u64, unreadable_by: Timestamp) -> Result<()> {
        if self.pending_erasure_installation() != Some(cut.installation()) { return Err(default_erasure::invalid()); }
        let authority = self.authority.checkpoints.as_mut().ok_or_else(default_erasure::unsupported)?;
        if !(authority.generation == generation || authority.generation.checked_add(1) == Some(generation)) {
            return Err(default_erasure::invalid());
        }
        let fold = match &mut self.pipeline {
            Some(pipeline) => pipeline.durable_mut(),
            None => self.fold.as_mut().ok_or_else(invalid)?,
        };
        fold.arm_erasure(cut.basis, unreadable_by)?;
        authority.generation = generation;
        self.adopt_erasure_generation(generation);
        Ok(())
    }

    pub(crate) fn source_ready(&self) -> bool {
        !self.holds_checkpoint() && self.pending.len() + self.pending_replies < MAX_IN_FLIGHT
    }

    pub(crate) fn request_pending<C>(&self, request: &NativeRequest<C>) -> bool {
        self.pipeline.as_ref().is_some_and(|pipeline| {
            pipeline.is_deciding(&request.transaction.key,
                request.transaction.receipt_namespace(request.scope.as_ref().map(|scope| scope.receipt_namespace)))
        })
    }

    pub(crate) fn needs_erasure_history(&self, request: &SourceRequest) -> bool {
        self.physical_erasure && self.shares_main && request.scope.is_none()
            && request.transaction.uses_erasure()
            && self.durable().tx_status(&request.transaction.key).is_err()
    }

    pub(crate) fn decide_source(&mut self, source: SourcePosition, now: Timestamp, time_floor: Option<Timestamp>, request: Result<SourceRequest>) -> Result<proto::NoOp> {
        if !self.source_ready() { return Err(invalid()); }
        let request = match request {
            Ok(_) if !self.view.borrow().compatible => return self.source_reply(source, Err(incompatible())),
            Ok(request) => request,
            Err(error) => return self.source_reply(source, Err(error)),
        };
        if request.is_validation() { return self.source_reply(source, Err(invalid_source_request())); }
        let row_custody = self.authority.checkpoints.is_some() && !self.physical_erasure;
        if let Err(error) = request.check_source_admission(self.physical_erasure && self.shares_main, row_custody) {
            return self.source_reply(source, Err(error));
        }
        if self.request_pending(&request) { return Err(invalid()); }
        let features = self.source_features(&request);
        if self.pipeline.is_none() {
            let next = self.next_position()?;
            let fold = self.fold.take().ok_or_else(invalid)?;
            self.pipeline = Some(TransactPipeline::new(fold, next, MAX_IN_FLIGHT)?);
        }
        let pipeline = self.pipeline.as_mut().expect("initialized serving pipeline");
        let scope = request.scope.as_ref().map(NativeServingScope::authority);
        let decision = pipeline.decide_after(&request.transaction, now, &features, &request.claims, scope, time_floor);
        match decision {
            Ok(Decision::Answer(receipt)) => self.source_reply(source, Ok(receipt)),
            Err(error) if error.is_caller_error() => self.source_reply(source, Err(error)),
            Err(error) => Err(error),
            Ok(Decision::Append { position, record }) => {
                self.erasure_in_flight = request.transaction.uses_erasure();
                let installs_source = request.transaction.window.before.is_none();
                let message = decision_message(position, &record, Some(source), installs_source, self.shares_main);
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
        let features = self.source_features(&request);
        let resolution = match &request.scope {
            Some(scope) => durable.resolve_scoped(&request.transaction, position, now, &features, &request.claims, scope.authority()),
            None => durable.resolve(&request.transaction, position, now, &features, &request.claims),
        }?;
        let Resolution::Answer(receipt) = resolution else { return Err(invalid()); };
        project_receipt(durable.index(), receipt, request.scope.as_ref())
    }

    pub(crate) fn holds_checkpoint(&self) -> bool {
        self.erasure_in_flight || self.durable().recover_pending().is_some()
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
        let message = decision_message(position, &record, self.timed_source, true, self.shares_main);
        self.pending.insert(position, message.clone());
        Ok(Some(message))
    }

    #[cfg(test)]
    fn apply(&mut self, message: &proto::NoOp, own: bool,
        apply_main: impl FnOnce(SourcePosition, &Receipt, &BranchIndex, &[BranchEffect]) -> Result<()>,
    ) -> Result<Option<(SourcePosition, Result<Receipt>)>> {
        self.apply_ordered(message, own, None, apply_main)
    }

    pub(crate) fn apply_ordered(&mut self, message: &proto::NoOp, own: bool, ordered: Option<(ReplicaPosition, Term)>,
        apply_main: impl FnOnce(SourcePosition, &Receipt, &BranchIndex, &[BranchEffect]) -> Result<()>,
    ) -> Result<Option<(SourcePosition, Result<Receipt>)>> {
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
        let shared = frame.first() == Some(&SHARED_DECISION_FRAME);
        if !shared && !terminal { self.shares_main = false; }
        if shared != self.shares_main { return Err(invalid()); }
        let projection = if self.shares_main && installed.is_some() {
            let record = TransactionRecord::decode(bytes)?;
            let (effects, namespace) = match record {
                TransactionRecord::Tx(tx) => (tx.effects().to_vec(), tx.receipt_namespace()),
                TransactionRecord::Settle { .. } => (self.durable().pending_effects().ok_or_else(invalid)?.to_vec(), self.durable().pending_receipt_namespace()),
                TransactionRecord::Refuse { .. } => (Vec::new(), self.durable().pending_receipt_namespace()),
            };
            Some((self.durable().index().clone(), effects, namespace))
        } else { None };
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
        self.erasure_in_flight = false;
        if installed != source || receipts.next().is_some() || receipt.basis.position() != position { return Err(invalid()); }
        if let Some((prior, effects, namespace)) = projection {
            let source = installed.ok_or_else(invalid)?;
            apply_main(source, &receipt, &prior, &effects)?;
            if let Some((replica, term)) = ordered {
                let fold = match &mut self.pipeline {
                    Some(pipeline) => pipeline.durable_mut(),
                    None => self.fold.as_mut().ok_or_else(invalid)?,
                };
                fold.install_receipt_position(namespace, &receipt, InstalledReceipt { source, replica, term })?;
            }
        }
        self.publish_index();
        Ok(source.map(|source| (source, Ok(receipt))))
    }

    fn publish_index(&self) {
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
    }

    pub(crate) fn publish_ranked_catalog(&self, installed: CatalogCut) {
        let cut = self.durable().index().head();
        self.view.send_if_modified(|view| {
            let selected = (self.shares_main && view.compatible && view.index.head() == cut)
                .then_some((cut, installed));
            if view.ranked_catalog == selected { return false; }
            view.ranked_catalog = selected;
            true
        });
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

    pub(crate) fn installed_custody_receipt(&self, key: &[u8], basis: Basis) -> Result<Receipt> {
        let receipt = self.status(key, None)?;
        if receipt.basis != basis || self.durable().installed_receipt(None, key, basis).is_none() {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb/custody-release-receipt",
                "The original custody receipt is not an exact installed v1 decision"));
        }
        Ok(receipt)
    }

    pub(crate) fn recover_exact(&self, key: &[u8], scope: Option<&NativeServingScope>, fingerprint: [u8; 32]) -> Result<Receipt> {
        if !self.view.borrow().compatible { return Err(incompatible()); }
        let namespace = scope.map(|scope| scope.receipt_namespace);
        if self.pipeline.as_ref().is_some_and(|pipeline| pipeline.is_deciding(key, namespace)) {
            return self.status(key, scope);
        }
        let durable = self.durable();
        let receipt = durable.recover_exact(key, namespace, fingerprint)?;
        project_receipt(durable.index(), receipt, scope)
    }

    pub(crate) fn withdraw(&mut self) {
        if let Some(pipeline) = self.pipeline.take() {
            self.fold = Some(pipeline.into_durable());
        }
        self.pending.clear();
        self.erasure_in_flight = false;
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

    pub(crate) async fn recover_classic(&mut self, snapshot: Snapshot, replica: Option<ReplicaPosition>,
        storage: &Arc<rtdb_storage::cache::BufferPool>, memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
        eligible: bool, projected: bool,
    ) -> Result<()> {
        self.shares_main &= eligible;
        if snapshot.tx_basis().is_none() || !snapshot.table_names().any(classic::data_table) { return Ok(()); }
        if self.shares_main && projected { return Ok(()); }
        if self.shares_main && self.position == 0 && let Some(replica) = replica {
            let mut index = self.durable().index().clone();
            if let Ok(charges) = classic::recover(&mut index, snapshot, storage, memory, cancellation).await {
                let current = index.head();
                let covered = replica_basis_position(replica)?;
                if covered >= current.basis.position() {
                    if covered > current.basis.position() {
                        index.advance_classic(BranchCut { basis: Basis::new(current.basis.incarnation(), covered), system_time: current.system_time })?;
                    }
                    index.seal_classic_checkpoint();
                    self.install_classic_index(index, charges)?;
                    let floor = self.durable().index().head().basis;
                    self.view.send_modify(|view| {
                        view.floor = floor;
                        view.changes = Arc::default();
                    });
                    return Ok(());
                }
            }
        }
        self.observe_unprojected_classic(replica);
        Ok(())
    }

    pub(crate) fn shares_main(&self) -> bool { self.shares_main }

    pub(crate) fn incarnation(&self) -> [u8; 16] { self.authority.incarnation }

    pub(crate) fn classic_write_authority(&self) -> &BranchIndex { self.durable().index() }

    pub(crate) fn require_classic_effects(&self, write: &StagedWrite) -> Result<()> {
        write.tables().keys().try_for_each(|table| self.classic_write_authority().require_classic_write(table))
    }

    pub(crate) fn main_write_pending(&self) -> bool { self.shares_main && !self.pending.is_empty() }

    pub(crate) fn is_shared_checkpoint(image: &NativeCheckpointImage) -> bool {
        image.bytes().get(CHECKPOINT_HEADER_BYTES) == Some(&SHARED_CHECKPOINT)
    }

    pub(crate) fn verify_checkpoint_prefix(&self, block: &rtdb_format::catalog::BlockCatalog) -> Result<()> {
        self.durable().validate_installed_receipts(block.latest_source_position(), block.boundary_replica_position(), block.boundary_term())
    }

    pub(crate) fn replay_classic_anchor(&mut self, message: &proto::NoOp, boundary: ReplicaPosition, system_time: Timestamp) -> Result<()> {
        let Some(frame) = message.serving_record.as_deref().filter(|frame| frame.first() == Some(&SHARED_DECISION_FRAME)) else {
            return Ok(());
        };
        let (position, _, _) = decode(frame)?;
        if position <= self.next_position()? { return Ok(()); }
        let anchor = replica_basis_position(boundary)?;
        if !self.shares_main || self.holds_checkpoint() || self.durable().has_receipts() || anchor.checked_add(1) != Some(position) {
            return Err(invalid());
        }
        let mut index = self.durable().index().clone();
        index.advance_classic(BranchCut { basis: Basis::new(self.authority.incarnation, anchor), system_time })?;
        index.seal_classic_checkpoint();
        self.install_classic_index(index, Vec::new())
    }

    pub(crate) fn observe_classic(&mut self, write: &StagedWrite, replica: ReplicaPosition, memory: &Arc<MemoryBudget>) -> Result<()> {
        if !classic::touches_data(write) { return Ok(()); }
        self.require_classic_effects(write)?;
        if self.shares_main {
            if self.main_write_pending() || self.holds_checkpoint() { return Err(invalid()); }
            let mut index = self.durable().index().clone();
            let mut charges = Vec::new();
            let position = self.next_position()?.max(replica_basis_position(replica)?);
            if classic::apply_live(&mut index, position, write, memory, &mut charges).is_ok() {
                return self.install_classic_index(index, charges);
            }
        }
        self.observe_unprojected_classic(Some(replica));
        Ok(())
    }

    fn install_classic_index(&mut self, index: BranchIndex, charges: Vec<Arc<MemoryReservation>>) -> Result<()> {
        if let Some(pipeline) = self.pipeline.take() { self.fold = Some(pipeline.into_durable()); }
        self.position = index.head().basis.position();
        self.fold.as_mut().ok_or_else(invalid)?.replace_applied_index(index)?;
        self.classic_charges.extend(charges);
        self.publish_index();
        Ok(())
    }

    fn observe_unprojected_classic(&mut self, replica: Option<ReplicaPosition>) {
        self.shares_main = false;
        self.view.send_modify(|view| {
            view.compatible = false;
            view.classic_head = replica;
        });
    }
}

pub(crate) fn require_physical_replay_support(message: &proto::NoOp) -> Result<()> {
    if message.serving_record.as_deref().and_then(|frame| frame.first()) == Some(&SHARED_DECISION_FRAME)
        || default_erasure::ErasureRetirement::decode(message)?.is_some()
        || default_erasure::ErasureCompleted::decode(message)?.is_some() {
        return Err(Anomaly::unsupported("rtdb/shared-suffix-replay", "This replay path requires the shared transaction fold"));
    }
    Ok(())
}

fn replica_basis_position(replica: ReplicaPosition) -> Result<u64> {
    let position = replica.0.checked_add(1).ok_or_else(invalid)?;
    u64::try_from(position).map_err(|_| invalid())
}

pub(crate) fn decode_checkpoint_image(image: &[u8], incarnation: [u8; 16]) -> Result<(TransactState, u64, bool)> {
    let (mut header, state) = image.split_at_checked(CHECKPOINT_HEADER_BYTES).ok_or_else(invalid)?;
    let position = header.get_u64();
    let basis = Basis::from_bytes(&header[..24])?;
    header.advance(24);
    let cut = BranchCut { basis, system_time: Timestamp(header.get_i64()) };
    if basis.incarnation() != incarnation || position < basis.position() { return Err(invalid()); }
    let shared = state.first() == Some(&SHARED_CHECKPOINT);
    let (rewrite, state) = if shared { RewriteEvidence::read(&state[1..])? } else { (None, state) };
    let fold = TransactState::decode_checkpoint(state, cut)?;
    if rewrite.is_some_and(|evidence| fold.pending_erasure_installation() != Some(evidence.cut.installation())
        || cut.basis != evidence.cut.basis) {
        return Err(default_erasure::invalid());
    }
    Ok((fold, position, shared))
}

pub(crate) fn restore_plain_checkpoint_image(image: &[u8], source: [u8; 16], destination: [u8; 16]) -> Result<Bytes> {
    if source == destination { return Err(invalid()); }
    let (mut fold, position, shared) = decode_checkpoint_image(image, source)?;
    if !shared { return Err(invalid()); }
    fold.restore_in(destination)?;
    let cut = fold.index().head();
    let state = fold.encode_checkpoint()?;
    let mut frame = BytesMut::with_capacity(CHECKPOINT_HEADER_BYTES + 1 + state.len());
    frame.put_u64(position);
    frame.extend_from_slice(&cut.basis.to_bytes());
    frame.put_i64(cut.system_time.0);
    frame.put_u8(SHARED_CHECKPOINT);
    frame.extend_from_slice(&state);
    Ok(frame.freeze())
}

pub(crate) fn checkpoint_rewrite(image: &[u8]) -> Result<Option<RewriteEvidence>> {
    if image.get(CHECKPOINT_HEADER_BYTES) != Some(&SHARED_CHECKPOINT) { return Ok(None); }
    RewriteEvidence::read(&image[CHECKPOINT_HEADER_BYTES + 1..]).map(|(evidence, _)| evidence)
}

fn project_receipt(index: &BranchIndex, mut receipt: Receipt, scope: Option<&NativeServingScope>) -> Result<Receipt> {
    require_armed_erase_receipt(&receipt)?;
    if let Some(scope) = scope {
        let cut = index.project_main_visibility(&scope.labels)?.projected_cut(receipt.basis)?;
        receipt.basis = cut.basis;
        receipt.system_time = cut.system_time;
    }
    Ok(receipt)
}

fn require_armed_erase_receipt(receipt: &Receipt) -> Result<()> {
    if receipt.erasure == Some(Erasure::Pending) {
        return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/erasure-pending", "The erase receipt is waiting for durable key retirement"));
    }
    Ok(())
}

fn decision_message(position: u64, record: &[u8], source: Option<SourcePosition>, installs_source: bool, shared: bool) -> proto::NoOp {
    let mut frame = BytesMut::with_capacity(SOURCE_DECISION_HEADER_BYTES + record.len());
    let version = match (source, shared) {
        (Some(_), true) => SHARED_DECISION_FRAME,
        (Some(_), false) => SOURCE_DECISION_FRAME,
        (None, _) => FRAME_VERSION,
    };
    frame.put_u8(version);
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
        Some(SOURCE_DECISION_FRAME | SHARED_DECISION_FRAME) => SOURCE_DECISION_HEADER_BYTES,
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
        let checkpoint = checkpoint.map(|bytes| NativeCheckpointImage::Inline(Bytes::copy_from_slice(bytes)));
        NativeServing::open(authority(), Arc::new(MemoryStore::default()), checkpoint.as_ref(), false).unwrap()
    }

    #[test]
    fn physical_receipt_follows_the_original_source_and_the_settled_replica() {
        let memory = MemoryBudget::new(1 << 24);
        let partition = rtdb_index::PartitionState::empty(Default::default()).unwrap();
        let (mut writer, _) = open(None);
        let (mut retained, _) = open(None);
        let mut transaction = put(1);
        transaction.window.before = Some(Timestamp(90));
        let source = SourcePosition(3);
        let original = writer.decide_source(source, Timestamp(40), None, Ok(unscoped(transaction.clone()))).unwrap();
        assert!(writer.apply_ordered(&original, true, Some((ReplicaPosition(8), Term(2))), |_, _, _, _| unreachable!()).unwrap().is_none());
        assert!(super::retained::apply(Some(&mut retained), &original, partition.snapshot(), SourcePosition(2), ReplicaPosition(8), Term(2), &memory).unwrap().is_none());
        let terminal = writer.progress_timed(Timestamp(41)).unwrap().unwrap();
        let (_, receipt) = writer.apply_ordered(&terminal, true, Some((ReplicaPosition(11), Term(2))), |source, receipt, prior, effects| {
            let write = classic::stage_source(partition.snapshot(), prior, source, receipt, effects, &memory)?;
            assert_eq!(write.tx_key().tx_id.0, source.0);
            Ok(())
        }).unwrap().unwrap();
        let receipt = receipt.unwrap();
        let replay = super::retained::apply(Some(&mut retained), &terminal, partition.snapshot(), SourcePosition(2), ReplicaPosition(11), Term(2), &memory).unwrap().unwrap();
        assert_eq!(replay.source, source);
        assert_eq!(replay.write.unwrap().0.tx_key().tx_id.0, source.0);
        let witness = InstalledReceipt { source, replica: ReplicaPosition(11), term: Term(2) };
        let retry = writer.decide_source(SourcePosition(4), Timestamp(43), None, Ok(unscoped(transaction))).unwrap();
        assert!(writer.apply_ordered(&retry, true, Some((ReplicaPosition(12), Term(2))), |_, _, _, _| unreachable!()).unwrap().unwrap().1.unwrap().replayed);
        for serving in [&mut writer, &mut retained] {
            let image = serving.checkpoint(Term(2), true).unwrap().image;
            let (reopened, _) = open(Some(&image));
            assert_eq!(reopened.durable().installed_receipt(None, &receipt.key, receipt.basis), Some(witness));
            assert_eq!(reopened.durable().installed_receipt(Some([0; 32]), &receipt.key, receipt.basis), None);
            assert_eq!(reopened.installed_custody_receipt(&receipt.key, receipt.basis).unwrap().basis, receipt.basis);
            assert!(reopened.installed_custody_receipt(&receipt.key, Basis::new(INCARNATION, 1)).is_err());
            assert!(reopened.durable().validate_installed_receipts(Some(source), Some(ReplicaPosition(11)), Term(2)).is_ok());
            assert!(reopened.durable().validate_installed_receipts(Some(source), Some(ReplicaPosition(8)), Term(2)).is_err());
        }
    }

    #[test]
    fn previously_persisted_native_rows_keep_their_exact_basis_without_a_physical_witness() {
        use rtdb_types::rtdb_v1::{Layer, MAIN_BRANCH};

        let (mut writer, _) = open(None);
        let source = SourcePosition(3);
        let fresh = writer.decide_source(source, Timestamp(40), None, Ok(unscoped(put(1)))).unwrap();
        let (position, _, bytes) = decode(fresh.serving_record.as_deref().unwrap()).unwrap();
        let old_record = decision_message(position, bytes, Some(source), true, false);
        let (mut legacy, _) = open(None);
        let (_, receipt) = legacy.apply_ordered(&old_record, false, Some((ReplicaPosition(4), Term(2))), |_, _, _, _| unreachable!()).unwrap().unwrap();
        let receipt = receipt.unwrap();
        let image = legacy.checkpoint(Term(2), true).unwrap().image;
        let (reopened, _) = open(Some(&image));
        let index = reopened.durable().index();
        let view = index.view(MAIN_BRANCH, At::Exactly(receipt.basis), Layer::Merged, receipt.system_time).unwrap();
        let row = view.row(&TableRef::new("public", "items"), &EntityId::Integer(1), receipt.system_time).unwrap();
        assert_eq!(row.written_basis(), Some(receipt.basis));
        assert_eq!(reopened.status(&receipt.key, None).unwrap().basis, receipt.basis);
        assert_eq!(reopened.durable().installed_receipt(None, &receipt.key, receipt.basis), None);
        assert!(reopened.installed_custody_receipt(&receipt.key, receipt.basis).is_err());
        assert!(!reopened.shares_main());
    }

    #[tokio::test]
    async fn authenticated_sealed_rows_are_refused_before_a_default_decision_is_appended() {
        let (mut serving, _) = open(None);
        let mut transaction = put(1);
        let Op::Put { rows, .. } = &mut transaction.ops[0] else { unreachable!() };
        rows[0].custody_group = Some([1; 16]);
        rows[0].protected_columns = vec!["protected".into()];
        rows[0].custody_payload = Some(rtdb_tx::transact::CustodyPayload { binding: [2; 32], envelope: vec![3] });
        let source = SourcePosition(5);
        let envelope = serving.source_authority.seal(NativeRequest { transaction, claims: VerifiedClaims::default(), scope: None }).await.unwrap();
        let request = serving.source_authority.open(&envelope).await.unwrap();
        let message = serving.decide_source(source, Timestamp(40), None, request).unwrap();
        assert_eq!(message.serving_record.as_deref().unwrap()[0], SOURCE_REPLY_FRAME);
        let (_, reply) = serving.apply(&message, true, |_, _, _, _| unreachable!()).unwrap().unwrap();
        assert_eq!(reply.unwrap_err().code, "rtdb/feature-required");
        assert!(!serving.durable().has_receipts());
    }

    #[test]
    fn retained_protected_decisions_and_revocation_have_no_classic_rows() {
        use rtdb_types::rtdb_v1::{Layer, MAIN_BRANCH};

        let memory = MemoryBudget::new(1 << 24);
        let partition = rtdb_index::PartitionState::empty(Default::default()).unwrap();
        let group = [1; 16];
        let (mut serving, _) = open(None);
        let mut request = put(1);
        let Op::Put { rows, .. } = &mut request.ops[0] else { unreachable!() };
        rows[0].custody_group = Some(group);
        rows[0].protected_columns = vec!["sealed".into()];
        rows[0].custody_payload = Some(rtdb_tx::transact::CustodyPayload { binding: [3; 32], envelope: vec![4] });
        request.ops.insert(0, Op::CreateCustodyGroup { group });
        let source_request = unscoped(request.clone());
        assert!(source_request.check_source_admission(false, false).is_err());
        assert!(source_request.check_source_admission(false, true).is_ok());
        let apply = |serving: &mut NativeServing, request: &Transact, position: u64, source: i64, prior_source: i64| {
            let Resolution::Append(prepared) = serving.durable().resolve(request, position, Timestamp(40), &[ROW_CUSTODY], &VerifiedClaims::default()).unwrap() else { panic!() };
            let message = decision_message(position, &prepared.record().encode().unwrap(), Some(SourcePosition(source)), true, true);
            let replay = super::retained::apply(Some(serving), &message, partition.snapshot(), SourcePosition(prior_source),
                ReplicaPosition(source + 7), Term(2), &memory).unwrap().unwrap();
            let (write, committed) = replay.write.unwrap();
            assert!(committed);
            assert!(write.tables().is_empty());
            partition.apply(&write).unwrap();
        };
        let table = TableRef::new("public", "items");
        let id = EntityId::Integer(1);
        apply(&mut serving, &request, 1, 3, 2);
        let index = serving.durable().index();
        let view = index.view(MAIN_BRANCH, At::Latest, Layer::Merged, index.head().system_time).unwrap();
        assert!(view.row(&table, &id, Timestamp(40)).unwrap().image.as_ref().unwrap().custody_payload.is_some());
        let mut revoke = put(2);
        revoke.ops = vec![Op::RevokeCustodyGroup { group, inventory_sha256: index.custody_inventory(group).unwrap().sha256 }];
        apply(&mut serving, &revoke, 2, 4, 3);
        let index = serving.durable().index();
        let view = index.view(MAIN_BRANCH, At::Latest, Layer::Merged, index.head().system_time).unwrap();
        assert!(view.row(&table, &id, Timestamp(40)).is_none());
    }

    #[tokio::test]
    async fn detached_repair_preserves_the_selected_ciphertext_and_receipt_on_a_retired_generation() {
        use rtdb_storage::ObjectStore;

        let directory = tempfile::tempdir().unwrap();
        let source = Arc::new(LocalStore::open(directory.path().join("source")).unwrap());
        let destination = Arc::new(LocalStore::open(directory.path().join("destination")).unwrap());
        let kms: Arc<dyn Kms> = Arc::new(LocalKms::new(|| 0));
        let database = Uuid::from_bytes(INCARNATION);
        let generation = Generation { database, number: 3 };
        kms.create(generation).await.unwrap();
        let (mut writer, _) = open(None);
        let offset = SourcePosition(7);
        let replica = ReplicaPosition(10);
        let message = writer.decide_source(offset, Timestamp(40), None, Ok(unscoped(put(1)))).unwrap();
        let (_, receipt) = writer.apply_ordered(&message, true, Some((replica, Term(2))), |_, _, _, _| Ok(())).unwrap().unwrap();
        let receipt = receipt.unwrap();
        let image = writer.checkpoint(Term(2), true).unwrap().image;
        let sealed = WriterObjects::new(database, 2).file(kms.as_ref(), generation.number, &image).await.unwrap();
        sealed.put(source.as_ref()).await.unwrap();
        kms.retire(generation, 100).await.unwrap();
        let authority = NativeServingAuthority { incarnation: INCARNATION,
            checkpoints: Some(NativeCheckpointAuthority { kms: Arc::clone(&kms), generation: 9 }) };
        let object = sealed.id().to_string();
        let selected = authority.read_checkpoint(source.as_ref(), None, Some(&object)).await.unwrap().unwrap();
        assert_eq!(selected.generation(), Some(generation));
        let (mut different, _) = open(None);
        different.apply_ordered(&message, false, Some((ReplicaPosition(replica.0 + 1), Term(2))), |_, _, _, _| Ok(())).unwrap();
        assert!(different.repair_checkpoint(&selected, Some(Arc::clone(&kms))).is_err());
        let checkpoint = writer.repair_checkpoint(&selected, Some(Arc::clone(&kms))).unwrap();
        let key = ObjectKey::new(object.clone()).unwrap();
        source.copy_to(&key, destination.as_ref(), &key).await.unwrap();
        let store: Arc<dyn ObjectStore> = destination.clone();
        let blocks = crate::BlockStore::new(Arc::clone(&store), Arc::new(BufferPool::new(Arc::clone(&store), 1 << 20)),
            MemoryBudget::new(1 << 24), 100_000_000).unwrap();
        let (inline, restored_object) = checkpoint.store(&blocks, &CancellationToken::new()).await.unwrap();
        assert!(inline.is_none());
        assert_eq!(restored_object, Some(object.clone()));
        assert_eq!(source.get(&key).await.unwrap(), destination.get(&key).await.unwrap());
        let copied = authority.read_checkpoint(store.as_ref(), None, Some(&object)).await.unwrap();
        let (reopened, view) = NativeServing::open(authority, store, copied.as_ref(), false).unwrap();
        assert_eq!(view.borrow().key_generation, Some(generation.number));
        assert_eq!(reopened.durable().installed_receipt(None, &receipt.key, receipt.basis), Some(InstalledReceipt { source: offset, replica, term: Term(2) }));
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
            let original = leader.decide_source(source, Timestamp(50), None, Ok(unscoped(request))).unwrap();
            assert_eq!(original.src_msg_id, None);
            assert!(!leader.source_ready());
            assert!(leader.apply(&original, true, |_, _, _, _| Ok(())).unwrap().is_none());
            assert!(follower.apply(&original, false, |_, _, _, _| Ok(())).unwrap().is_none());
            assert_eq!(view.borrow().index.head().basis.position(), 0);
            assert_eq!(leader.status(&[1; 16], None).unwrap_err().code, "rtdb/transaction-pending");

            let terminal = leader.progress_timed(upper).unwrap().unwrap();
            assert!(!leader.source_ready());
            let (applied, result) = leader.apply(&terminal, true, |_, _, _, _| Ok(())).unwrap().unwrap();
            assert_eq!(applied, source);
            assert_eq!(follower.apply(&terminal, false, |_, _, _, _| Ok(())).unwrap(), Some((applied, result.clone())));
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
        let message = original.decide_source(source, Timestamp(50), None, Ok(unscoped(request.clone()))).unwrap();
        let (mut successor, _) = open(None);
        assert!(successor.apply(&message, false, |_, _, _, _| Ok(())).unwrap().is_none());
        successor.withdraw();
        let terminal = successor.progress_timed(Timestamp(52)).unwrap().unwrap();
        let (completed, receipt) = successor.apply(&terminal, true, |_, _, _, _| Ok(())).unwrap().unwrap();
        assert_eq!(completed, source);
        let receipt = receipt.unwrap();
        assert!(matches!(&receipt.outcome,
            rtdb_tx::transact::Outcome::Refused { error } if error.code == "rtdb/timed-transaction-refused"));
        let next = SourcePosition(10);
        let retry = successor.decide_source(next, Timestamp(53), None, Ok(unscoped(request))).unwrap();
        let (retried_at, retry) = successor.apply(&retry, true, |_, _, _, _| Ok(())).unwrap().unwrap();
        assert_eq!(retried_at, next);
        assert!(retry.as_ref().unwrap().replayed);
        assert_eq!(retry.unwrap().basis, receipt.basis);
    }

    #[test]
    fn serving_source_takes_over_from_the_read_back_prefix() {
        let (mut serving, view) = open(None);
        let first = serving.decide_source(SourcePosition(2), Timestamp(41), None, Ok(unscoped(put(1)))).unwrap();
        let excluded = serving.decide_source(SourcePosition(3), Timestamp(41), None, Ok(unscoped(put(2)))).unwrap();
        let (position, source, bytes) = decode(first.serving_record.as_deref().unwrap()).unwrap();
        let TransactionRecord::Tx(record) = TransactionRecord::decode(bytes).unwrap() else { panic!() };
        assert_eq!((position, source), (1, Some(SourcePosition(2))));
        assert_eq!(record.admission().0, &[1; 16]);
        assert_eq!(record.admission().1, Timestamp(41));
        assert_eq!(record.canonical_request().unwrap().get(), put(1).canonical_request().unwrap().get());
        assert_eq!(serving.status(&[1; 16], None).unwrap_err().code, "rtdb/transaction-pending");
        assert_eq!(view.borrow().index.head().basis.position(), 0);

        serving.withdraw();
        assert_eq!(serving.apply(&first, false, |_, _, _, _| Ok(())).unwrap().unwrap().0, SourcePosition(2));
        assert!(serving.status(&[1; 16], None).unwrap().replayed);
        assert_eq!(serving.status(&[2; 16], None).unwrap_err().code, "rtdb/unknown-transaction");
        let reread = serving.decide_source(SourcePosition(3), Timestamp(42), None, Ok(unscoped(put(2)))).unwrap();
        assert!(serving.apply(&excluded, true, |_, _, _, _| Ok(())).is_err());
        assert_eq!(serving.apply(&reread, true, |_, _, _, _| Ok(())).unwrap().unwrap().1.unwrap().basis.position(), 2);

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
                    leader.decide_source(SourcePosition(i64::from(key) * 2), Timestamp(50), None, Ok(unscoped(put(key)))).unwrap()
                }).collect::<Vec<_>>();
                for (&key, message) in batch.iter().zip(messages) {
                    let (position, reply) = leader.apply(&message, true, |_, _, _, _| Ok(())).unwrap().unwrap();
                    assert_eq!(position, SourcePosition(i64::from(key) * 2));
                    assert_eq!(follower.apply(&message, false, |_, _, _, _| Ok(())).unwrap(), Some((position, reply.clone())));
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
        let declared = serving.decide_source(SourcePosition(1), Timestamp(40), None, Ok(unscoped(declaration))).unwrap();
        assert_eq!(serving.apply(&declared, true, |_, _, _, _| Ok(())).unwrap().unwrap().1.unwrap().outcome, Outcome::Then);

        let scope = NativeServingScope {
            labels: LabelScope { read: [vec![7]].into_iter().collect(), write: [vec![7]].into_iter().collect() },
            receipt_namespace: [7; 32],
        };
        let mut submission = put(2);
        let Op::Put { rows, .. } = &mut submission.ops[0] else { unreachable!() };
        rows[0].valid = Some(rtdb_types::Interval::ALL.into());
        rows[0].cells.insert("label".into(), Cell::Value(Value::Binary(vec![7])));
        let fingerprint = submission.logical_sha256().unwrap();
        let request = NativeRequest { transaction: submission, claims: source::SourceClaims::default(), scope: Some(scope.clone()) };
        let message = serving.decide_source(SourcePosition(2), Timestamp(41), None, Ok(request)).unwrap();
        let receipt = serving.apply(&message, true, |_, _, _, _| Ok(())).unwrap().unwrap().1.unwrap();
        let receipt = project_receipt(serving.durable().index(), receipt, Some(&scope)).unwrap();
        assert_eq!(receipt.outcome, Outcome::Then);
        assert_eq!(receipt.basis.incarnation(), scope.labels.main_read_incarnation(INCARNATION));

        let checkpoint = serving.checkpoint(Term(1), true).unwrap();
        let (reopened, _) = open(Some(&checkpoint.image));
        assert_eq!(reopened.status(&[2; 16], Some(&scope)).unwrap().basis, receipt.basis);
        assert_eq!(reopened.status(&[2; 16], None).unwrap_err().code, "rtdb/unknown-transaction");
        assert_eq!(reopened.recover_exact(&[2; 16], Some(&scope), fingerprint).unwrap().basis, receipt.basis);
        assert_eq!(reopened.recover_exact(&[2; 16], Some(&scope), [0; 32]).unwrap_err().code, "rtdb/idempotency-key");
        assert_eq!(reopened.recover_exact(&[2; 16], None, fingerprint).unwrap_err().code, "rtdb/unknown-transaction");
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
