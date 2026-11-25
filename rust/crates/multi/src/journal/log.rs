//! RTDB storage-log projection of the installed journal's ordered grants
//! and exact replay intervals.

use super::{InstalledJournal, ReadFailure, WriterAdmission, adapter_command, append_selection, latest, replay_cursor};
use crate::{cipher::SEALED_OVERHEAD, keys::{JournalKey, KeyService}};
use async_trait::async_trait;
use bytes::Bytes;
use ordered_logkeeper::ordered::{
    APPEND_RECORD_CHARGE_BYTES, AppendClass, Command, Cut, Grant, Hold, MAX_OBJECT_REFERENCES,
    ObjectName, Request, RequestId,
    service::Route,
};
use rtdb_node::{ProtectedReadIndex, ReplicaReadIndex};
use rtdb_format::{catalog::BlockCatalog, envelope::{ReplicaBody, ReplicaFrame}};
use rtdb_storage::{
    Error, IoRegion, Result,
    log::{
        AppendCapacity, AppendReceipt, GuardedAppend, Log, LogMetadata, LogOffset, LogRange,
        LogRecord, LogReplayTail, LogTail, ReplayHead, ReplicaObjectCustody, ReplicaObjectScope,
        ReplicaPayloadReference, ReplicaReferenceSource,
    },
    object_store::ReadReservation,
};
use rtdb_types::{Anomaly, BlockIndex, Timestamp};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicI64, Ordering},
};
use std::time::Duration;
use tokio::sync::{Mutex, oneshot};
use tokio_util::task::TaskTracker;

const POLL_INTERVAL: Duration = Duration::from_millis(20);
const RELEASE_RETRY: Duration = Duration::from_secs(1);
const DELIVERED_RECORD_OVERHEAD: usize = 256;
const MAX_RANGE_RECORDS: usize = 4096;
const MAX_RANGE_BYTES: usize = 32 << 20;
/// Reads whose outcome is unknown and whose release has not yet settled.
const MAX_UNSETTLED_READS: usize = 8;

pub(crate) struct JournalLog {
    reads: Arc<Reads>,
    maximum_data_message: usize,
    writer: Option<Mutex<Writer>>,
    recovery: Mutex<Option<Arc<Held>>>,
    reference_source: OnceLock<ReferenceSource>,
}

struct Writer {
    grant: Grant,
    head: Cut,
    pending: Option<PendingAppend>,
    checkpoint: Option<BlockCatalog>,
}

struct PendingAppend {
    request: Request,
    checkpoint: Option<BlockCatalog>,
}

struct ReferenceSource {
    source: ReplicaReferenceSource,
    blocks: Arc<rtdb_replication::BlockStore>,
}

struct Reads {
    journal: Arc<InstalledJournal>,
    keys: Arc<dyn KeyService>,
    io: Arc<IoRegion>,
    tip: AtomicI64,
    tasks: TaskTracker,
    maximum: usize,
    unsettled: Mutex<Vec<RequestId>>,
    reader_epoch: Option<u64>,
}

pub(crate) struct SnapshotFiles {
    pub(crate) journal: Arc<InstalledJournal>,
    pub(crate) held: Arc<Held>,
    pub(crate) epoch: u64,
    budget: Arc<rtdb_columnar::MemoryBudget>,
    files: parking_lot::Mutex<SnapshotFileReferences>,
}

#[derive(Default)]
struct SnapshotFileReferences {
    pending: Option<DeliveredReferences>,
    epochs: BTreeMap<u64, BTreeSet<ObjectName>>,
    charges: Vec<Arc<rtdb_columnar::MemoryReservation>>,
}

struct DeliveredReferences {
    position: LogOffset,
    epoch: u64,
    names: Vec<ObjectName>,
    charge: Arc<rtdb_columnar::MemoryReservation>,
}

impl SnapshotFiles {
    fn reserve_delivered(&self, position: LogOffset, epoch: u64, names: Vec<ObjectName>) -> Result<DeliveredReferences> {
        let charge = self.budget.reserve(256 + names.len() * 96).map_err(storage)?;
        Ok(DeliveredReferences { position, epoch, names, charge })
    }

    fn admit(&self, position: LogOffset) -> Result<()> {
        let mut files = self.files.lock();
        let record = files.pending.take().filter(|record| record.position == position)
            .ok_or_else(|| Error::Closed("Snapshot replay did not admit its own delivered record".into()))?;
        if !record.names.is_empty() {
            files.epochs.entry(record.epoch).or_default().extend(record.names);
            files.charges.push(record.charge);
        }
        Ok(())
    }

    pub(crate) fn origin(
        &self, mut name: impl FnMut(u64) -> Result<Option<ObjectName>>,
    ) -> Result<Option<u64>> {
        for (epoch, references) in self.files.lock().epochs.iter().rev() {
            if name(*epoch)?.is_some_and(|name| references.contains(&name)) {
                return Ok(Some(*epoch));
            }
        }
        Ok(None)
    }
}

pub(crate) struct Held {
    hold: Hold,
    _release: oneshot::Sender<()>,
}

impl Held {
    pub(crate) fn scope(&self) -> Hold {
        self.hold
    }
    pub(crate) async fn acquire(
        journal: &Arc<InstalledJournal>,
        request: Request,
        tasks: &TaskTracker,
    ) -> rtdb_types::Result<Arc<Self>> {
        let (send, receive) = oneshot::channel();
        let journal = Arc::clone(journal);
        let worker = tasks.clone();
        tasks.spawn(async move {
            let admitted = loop {
                match journal.hold(&request).await {
                    Err(error) if error.code == "rtdb.multi/ordered-journal" => {
                        tokio::time::sleep(RELEASE_RETRY).await
                    }
                    result => break result,
                }
            };
            let result = admitted.map(|hold| Self::pin(journal, hold, &worker));
            let _ = send.send(result);
        });
        receive.await.map_err(|_| {
            Anomaly::fault(
                "rtdb.multi/ordered-hold-ended",
                "Ordered replay admission ended",
            )
        })?
    }

    pub(crate) fn pin(
        journal: Arc<InstalledJournal>,
        hold: Hold,
        tasks: &TaskTracker,
    ) -> Arc<Self> {
        let (release, closed) = oneshot::channel();
        tasks.spawn(async move {
            let _ = closed.await;
            loop {
                match journal.release(hold).await {
                    Ok(()) => break,
                    Err(error) => eprintln!(
                        "Ordered replay release {} remains pending: {}",
                        hold.id.nonce, error.code
                    ),
                }
                tokio::time::sleep(RELEASE_RETRY).await;
            }
        });
        Arc::new(Self {
            hold,
            _release: release,
        })
    }
}

impl JournalLog {
    pub async fn open(
        journal: Arc<InstalledJournal>,
        keys: Arc<dyn KeyService>,
        admission: WriterAdmission,
        recovery: Arc<Held>,
        maximum_message_bytes: usize,
        configured_window: Option<u64>,
        tasks: TaskTracker,
        custody: Option<Arc<dyn Send + Sync>>,
    ) -> Result<Arc<Self>> {
        let current = journal.route().await.map_err(storage)?;
        if current.writer.grant != Some(admission.grant)
            || current.head != admission.recovery.through
            || current.position < admission.position
        {
            return Err(deposed(admission.grant, &current));
        }
        let paid_window = configured_window
            .unwrap_or(current.ordinary_limit)
            .min(current.ordinary_limit);
        let maximum_data_message =
            usize::try_from(paid_window.saturating_sub(APPEND_RECORD_CHARGE_BYTES))
                .unwrap_or(usize::MAX)
                .min(maximum_message_bytes)
                .saturating_sub(SEALED_OVERHEAD);
        let reads = Arc::new(Reads {
            journal,
            keys,
            io: IoRegion::new().child_holding(custody),
            tip: AtomicI64::new(latest(current.head).map_err(storage)?.0),
            tasks,
            maximum: maximum_message_bytes,
            unsettled: Mutex::new(Vec::new()),
            reader_epoch: None,
        });
        let writer = Mutex::new(Writer {
            grant: admission.grant,
            head: admission.recovery.through,
            pending: None,
            checkpoint: None,
        });
        Ok(Arc::new(Self {
            reads,
            maximum_data_message,
            writer: Some(writer),
            recovery: Mutex::new(Some(recovery)),
            reference_source: OnceLock::new(),
        }))
    }

    pub(super) async fn open_reader(
        journal: Arc<InstalledJournal>,
        keys: Arc<dyn KeyService>,
        epoch: u64,
        recovery: Arc<Held>,
        maximum: usize,
        tasks: TaskTracker,
        custody: Option<Arc<dyn Send + Sync>>,
    ) -> Result<Arc<Self>> {
        let reads = Arc::new(Reads {
            journal, keys, io: IoRegion::new().child_holding(custody), maximum, tasks,
            tip: AtomicI64::new(latest(recovery.scope().through).map_err(storage)?.0),
            unsettled: Mutex::new(Vec::new()), reader_epoch: Some(epoch),
        });
        reads.route().await?;
        Ok(Arc::new(Self { reads, maximum_data_message: 0, writer: None, recovery: Mutex::new(Some(recovery)), reference_source: OnceLock::new() }))
    }

    fn writer(&self) -> Result<&Mutex<Writer>> {
        self.writer.as_ref().ok_or_else(|| Error::ReadOnlyLog(self.reads.journal.deployment().to_string().into()))
    }

    async fn enqueue_at(
        &self,
        class: AppendClass,
        payload: Bytes,
        sample: impl FnOnce() -> Result<Timestamp> + Send,
    ) -> Result<AppendReceipt> {
        let writer = self.writer()?;
        let maximum = match class {
            AppendClass::Data => self.maximum_data_message,
            AppendClass::Maintenance => self.reads.maximum.saturating_sub(SEALED_OVERHEAD),
        };
        if payload.len() > maximum {
            return Err(Error::MessageTooLarge {
                size: payload.len(),
                maximum,
            });
        }
        let mut writer = writer.lock().await;
        if writer.pending.is_some() {
            self.finish_append(&mut writer).await?;
        }
        let current = self.reads.route().await?;
        if !adapter_command::same_writer(current.writer.grant, writer.grant) || current.head != writer.head {
            return Err(deposed(writer.grant, &current));
        }
        let outcome = crate::residence_trace::AttemptOutcome::from_replica(&payload);
        let (references, checkpoint) = self.references(&writer, &payload).await?;
        let key = JournalKey::for_grant(current.placement, writer.grant);
        let payload = self.reads.keys.seal_journal_payload(key, &self.reads.io, payload).await?;
        let timestamp = sample()?;
        let command = adapter_command::append_command(
            class, writer.grant, writer.head.next_lsn, timestamp.0, payload, references,
        );
        let request = self.reads.journal.request(&current, command).map_err(storage)?;
        writer.pending = Some(PendingAppend { request, checkpoint });
        let committed = self.finish_append(&mut writer).await?;
        if let Some(outcome) = outcome { outcome.observe(committed); }
        Ok(AppendReceipt::observed(async move { Ok(committed) }))
    }

    async fn finish_append(&self, writer: &mut Writer) -> Result<LogMetadata> {
        let pending = writer
            .pending
            .as_ref()
            .ok_or_else(|| Error::Task("No pending ordered append".into()))?;
        let request = &pending.request;
        let Command::Append {
            class,
            expected_next,
            timestamp,
            payload,
            ..
        } = &request.command
        else {
            return Err(Error::Task("Pending ordered work is not an append".into()));
        };
        let result = match self.reads.journal.append(request).await {
            Ok(result) => result,
            Err(error) if error.code == "rtdb.multi/ordered-expired" => self
                .reads
                .journal
                .reconcile(writer.grant, request.clone())
                .await
                .map_err(storage)?,
            Err(error) if error.code == "rtdb.multi/ordered-capacity" => {
                let capacity = (*class == AppendClass::Data).then(|| Error::AppendCapacity {
                    required_bytes: (payload.len() as u64)
                        .saturating_add(APPEND_RECORD_CHARGE_BYTES),
                });
                writer.pending = None;
                return Err(capacity.unwrap_or_else(|| storage(error)));
            }
            Err(error) if matches!(error.code.as_str(),
                "rtdb.multi/ordered-conflict" | "rtdb.multi/ordered-replay-unavailable")
            => {
                writer.pending = None;
                return Err(storage(error));
            }
            Err(error) => return Err(storage(error)),
        };
        if !append_selection::matches_pending_append(
            result.class, *class, result.lsn, *expected_next, writer.head.next_lsn,
        ) {
            return Err(Error::Task(
                "The ordered append receipt addresses a different record".into(),
            ));
        }
        let metadata = LogMetadata {
            offset: latest(result.cut).map_err(storage)?,
            timestamp: Timestamp(*timestamp),
        };
        writer.head = result.cut;
        if let Some(checkpoint) = writer.pending.take().and_then(|pending| pending.checkpoint) {
            writer.checkpoint = Some(checkpoint);
        }
        self.reads
            .tip
            .fetch_max(metadata.offset.0, Ordering::Release);
        Ok(metadata)
    }

    async fn references(
        &self, writer: &Writer, payload: &[u8],
    ) -> Result<(Option<Vec<ObjectName>>, Option<BlockCatalog>)> {
        if let Some(names) = inline_references(payload)? {
            return Ok((Some(names), None));
        }
        let Some(attached) = self.reference_source.get() else { return Ok((None, None)); };
        let Some(mut envelope) = ReplicaFrame::decode(payload).map_err(storage)?.into_envelope() else {
            return Ok((None, None));
        };
        let source = &attached.source;
        let detached = ReplicaPayloadReference::from_frame(payload, source.version, source.epoch)?;
        let detached_body;
        if let Some(detached) = &detached {
            detached_body = detached.read_at(source.store.as_ref(), &source.memory).await?;
            envelope = envelope.unwrap_payload(detached_body.as_ref()).map_err(storage)?;
        }
        let base = writer.checkpoint.as_ref().or(source.checkpoint.as_ref());
        let replay = rtdb_replication::ReplayBase {
            storage_version: source.version, storage_epoch: source.epoch, checkpoint: base,
        };
        let mut dependencies = rtdb_replication::tail_dependencies(replay, [&envelope], attached.blocks.as_ref()).await.map_err(storage)?;
        if let Some(detached) = detached {
            dependencies.insert(detached.object().clone());
        }
        let checkpoint = match &envelope.message {
            ReplicaBody::BlockUploaded(upload)
                if upload.storage_version == Some(source.version.0)
                    && upload.storage_epoch.unwrap_or_default() == source.epoch.0
                    && upload.block_index.unwrap_or_default() > base.map_or(-1, |block| block.block_index().0) =>
            {
                let (checkpoint, _) = attached.blocks.load(BlockIndex(upload.block_index.unwrap_or_default())).await.map_err(storage)?;
                Some(checkpoint)
            }
            _ => None,
        };
        let mut names = Vec::with_capacity(dependencies.len());
        for object in dependencies {
            let Some((namespace, key)) = source.store.replica_publication_address(&object).await? else {
                return Ok((None, checkpoint));
            };
            names.push(crate::store::exact_retirement::installed_object_name(&namespace, &key).map_err(storage)?);
        }
        names.sort_unstable();
        names.dedup();
        if names.len() > MAX_OBJECT_REFERENCES {
            return Err(Error::Task("The installed append exceeds the journal object reference bound".into()));
        }
        Ok((Some(names), checkpoint))
    }

    async fn capture(&self, from: LogOffset, to: LogOffset) -> Result<Cursor> {
        let next =
            u64::try_from(from.0).map_err(|_| Error::Task("Invalid ordered range start".into()))?;
        let end = u64::try_from(to.0.max(from.0))
            .map_err(|_| Error::Task("Invalid ordered range end".into()))?;
        let current = self.reads.route().await?;
        let end = next.max(end.min(current.head.next_lsn));
        let mut cursor = Cursor::new(Arc::clone(&self.reads), next, Some(end));
        cursor.bind(current).await?;
        Ok(cursor)
    }

    /// Releases reads whose outcome was lost. A release settles a read once it is
    /// decided at or after the read's retry horizon, after which a delayed copy of
    /// that barrier can only expire. Failures keep the identity for a later attempt.
    async fn settle_reads(&self, reads: &mut Vec<RequestId>) {
        let mut unsettled = Vec::with_capacity(reads.len());
        for read in reads.drain(..) {
            match self.reads.journal.release_read(read).await {
                Ok(position) if super::read_selection::read_release_settled(position, read.expires_at) => {}
                Ok(_) | Err(_) => unsettled.push(read),
            }
        }
        *reads = unsettled;
    }
}

fn inline_references(payload: &[u8]) -> Result<Option<Vec<ObjectName>>> {
    let frame = ReplicaFrame::decode(payload).map_err(storage)?;
    Ok(frame.into_envelope().and_then(|envelope| match envelope.message {
        ReplicaBody::ResolvedTx(_) | ReplicaBody::NoOp(_) | ReplicaBody::BlockBoundary(_)
        | ReplicaBody::TriesDeleted(_) | ReplicaBody::TablesRetired(_) | ReplicaBody::ArtefactsRemoved(_) => Some(Vec::new()),
        ReplicaBody::BlockUploaded(_) | ReplicaBody::TriesAdded(_) | ReplicaBody::ArtefactsAdded(_)
        | ReplicaBody::OversizedMessage(_) => None,
    }))
}

#[async_trait]
impl ReplicaReadIndex for JournalLog {
    async fn confirm(&self) -> rtdb_types::Result<LogOffset> {
        Ok(self.protect(LogOffset(-1)).await?.offset)
    }

    async fn protect(&self, _applied: LogOffset) -> rtdb_types::Result<ProtectedReadIndex> {
        let mut reads = self.reads.unsettled.lock().await;
        let mut retried = false;
        loop {
            self.settle_reads(&mut reads).await;
            if reads.len() >= MAX_UNSETTLED_READS {
                return Err(reads_unsettled());
            }
            let current = self.reads.route().await?;
            let invocation = self.reads.journal.request(&current, adapter_command::barrier_command())?;
            // Record the identity first so a cancelled read is still released later.
            reads.push(invocation.id);
            match self.reads.journal.barrier(&invocation).await {
                Ok(proof) => {
                    reads.retain(|read| *read != invocation.id);
                    let hold = proof.claim.read();
                    let retention = self.reads.retain(hold);
                    if self.reads.reader_epoch.is_some() {
                        self.reads.route().await?;
                    }
                    let offset = latest(super::read_selection::barrier_cut(hold))?;
                    crate::residence_trace::record("ordered-read-index", || serde_json::json!({
                        "deployment": current.placement.deployment, "incarnation": current.placement.incarnation,
                        "group": current.placement.group, "generation": current.placement.generation,
                        "epoch": current.writer.epoch, "invocation": hold.id.nonce, "through": offset.0,
                    }));
                    return Ok(ProtectedReadIndex {
                        offset,
                        wait: Duration::from_secs(2),
                        retention: Some(Arc::new((proof, retention))),
                        certified_read: None,
                    });
                }
                Err(ReadFailure::Refused(error)) => {
                    reads.retain(|read| *read != invocation.id);
                    return Err(error);
                }
                Err(ReadFailure::Uncertain(error)) if retried => {
                    self.settle_reads(&mut reads).await;
                    return Err(error);
                }
                Err(ReadFailure::Uncertain(_)) => retried = true,
            }
        }
    }
}

impl Reads {
    async fn route(&self) -> Result<Route> {
        let view = self.journal.route().await.map_err(storage)?;
        if self.reader_epoch.is_some_and(|epoch| epoch != view.writer.epoch) {
            return Err(Error::Closed("The ordered reader attachment changed".into()));
        }
        self.tip
            .fetch_max(latest(view.head).map_err(storage)?.0, Ordering::Release);
        Ok(view)
    }

    fn retain(self: &Arc<Self>, hold: Hold) -> Arc<Held> {
        Held::pin(Arc::clone(&self.journal), hold, &self.tasks)
    }

    async fn acquire(self: &Arc<Self>, view: &Route, from: u64, through: u64) -> Result<Arc<Held>> {
        let command = Command::ProtectRange { from, through };
        let request = self.journal.request(view, command).map_err(storage)?;
        Held::acquire(&self.journal, request, &self.tasks)
            .await
            .map_err(storage)
    }
}

struct Prepared {
    metadata: ReplayHead,
    read: replay_cursor::PreparedHead<Cut>,
}

struct Cursor {
    reads: Arc<Reads>,
    position: replay_cursor::ReplayCursor<Cut>,
    end: Option<u64>,
    held: Option<Arc<Held>>,
    prepared: Option<Prepared>,
    snapshot: Option<Arc<SnapshotFiles>>,
}

impl Cursor {
    fn new(reads: Arc<Reads>, next: u64, end: Option<u64>) -> Self {
        Self {
            reads,
            position: replay_cursor::new_replay_cursor(next),
            end,
            held: None,
            prepared: None,
            snapshot: None,
        }
    }

    async fn bind(&mut self, view: Route) -> Result<()> {
        let next = self.position.next();
        if self.end == Some(next) {
            return Ok(());
        }
        if next < view.erasure_floor {
            return Err(Error::ErasedUnderView);
        }
        if next < view.floor.next_lsn {
            return Err(Error::UnavailableLogRange {
                requested: i64::try_from(next).unwrap_or(i64::MAX),
                first_retained: i64::try_from(view.floor.next_lsn).unwrap_or(i64::MAX),
            });
        }
        let through = self
            .end
            .map_or(view.head.next_lsn, |end| end.min(view.head.next_lsn));
        if next < through {
            let held = match self.reads.acquire(&view, next, through).await {
                Ok(held) => held,
                Err(error) => {
                    if next < self.reads.route().await?.erasure_floor {
                        return Err(Error::ErasedUnderView);
                    }
                    return Err(error);
                }
            };
            self.attach(held, through)?;
        }
        Ok(())
    }

    fn attach(&mut self, held: Arc<Held>, through: u64) -> Result<()> {
        let start = held.hold.start;
        if !self.position.bind(
            start, start.next_lsn, held.hold.through.next_lsn, through,
            self.position.from() == Some(start),
        ) {
            return Err(Error::Closed("The ordered replay hold differs from the requested interval".into()));
        }
        self.held = Some(held);
        Ok(())
    }

    async fn prepare(&mut self, wait: Duration) -> Result<Option<ReplayHead>> {
        if let Some(prepared) = &self.prepared {
            return Ok(Some(prepared.metadata));
        }
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            if let (Some(held), Some(from)) = (&self.held, self.position.from())
                && from.next_lsn < held.hold.through.next_lsn
            {
                let item = self
                    .reads
                    .journal
                    .replay_head(held.hold, from)
                    .await
                    .map_err(storage)?
                    .ok_or_else(|| {
                        Error::Task("A protected replay interval lost its next head".into())
                    })?;
                let payload_len = usize::try_from(item.payload_bytes)
                    .map_err(|_| Error::Task("The ordered payload cannot be represented".into()))?;
                if payload_len > self.reads.maximum {
                    return Err(Error::MessageTooLarge {
                        size: payload_len,
                        maximum: self.reads.maximum,
                    });
                }
                let read = self.position.prepare(
                    item.next, item.next.next_lsn, held.hold.through, held.hold.through.next_lsn,
                    item.next == held.hold.through, item.timestamp, payload_len, item.record_bytes,
                ).ok_or_else(|| Error::Closed("The ordered replay head is outside its exact successor cut".into()))?;
                let offset = latest(item.next).map_err(storage)?;
                let metadata = ReplayHead {
                    metadata: LogMetadata {
                        offset,
                        timestamp: Timestamp(item.timestamp),
                    },
                    payload_len: payload_len.checked_sub(SEALED_OVERHEAD)
                        .ok_or_else(|| Error::Task("The ordered record is shorter than its seal".into()))?,
                    allocation_bound: payload_len.saturating_add(DELIVERED_RECORD_OVERHEAD)
                        .max(usize::try_from(item.record_bytes).map_err(|_| Error::Task("The ordered record is too large".into()))?),
                };
                self.prepared = Some(Prepared { metadata, read });
                return Ok(Some(metadata));
            }
            self.held = None;
            if self.end == Some(self.position.next()) {
                return Ok(None);
            }
            let current = self.reads.route().await?;
            self.bind(current).await?;
            if self.held.is_some() {
                continue;
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            tokio::time::sleep_until(deadline.min(now + POLL_INTERVAL)).await;
        }
    }

    async fn deliver(
        &mut self,
        head: ReplayHead,
        reservation: Option<ReadReservation>,
    ) -> Result<LogRecord> {
        let Some(prepared) = &self.prepared else {
            return Err(Error::Task("No ordered replay head was prepared".into()));
        };
        if prepared.metadata != head
            || reservation
                .as_ref()
                .is_some_and(|reservation| reservation.length() < head.allocation_bound)
        {
            return Err(Error::Task(
                "The ordered delivery does not match the prepared head or allocation".into(),
            ));
        }
        let held = self
            .held
            .as_ref()
            .ok_or_else(|| Error::Task("No ordered replay hold".into()))?;
        let page = self
            .reads
            .journal
            .replay(held.hold, prepared.read.from(), prepared.read.record_bytes)
            .await
            .map_err(storage)?;
        let first = page
            .records
            .into_iter()
            .next()
            .ok_or_else(|| Error::Task("No record in the protected interval".into()))?;
        let exact_record_cut = first.result.cut == prepared.read.end();
        let observed_time = first.timestamp;
        let observed_bytes = first.payload.len();
        let (cut, first) = prepared.read.select(
            first,
            page.checkpoint == held.hold.checkpoint,
            page.through == prepared.read.through(),
            exact_record_cut,
            observed_time,
            observed_bytes,
        ).ok_or_else(|| Error::Closed("The ordered replay record differs from its prepared head or hold".into()))?;
        let delivered: Result<_> = async {
            let placement = self.reads.journal.placement();
            let scope = ReplicaObjectScope {
                deployment: placement.deployment,
                incarnation: placement.incarnation,
                group: placement.group,
                generation: placement.generation,
                writer_epoch: first.grant.epoch,
            };
            let snapshot_references = self.snapshot.as_ref().map(|snapshot| {
                snapshot.reserve_delivered(head.metadata.offset, first.grant.epoch, first.references.unwrap_or_default())
            }).transpose()?;
            let key = JournalKey::for_grant(placement, first.grant);
            let open = IoRegion::retaining(
                Arc::clone(held) as Arc<dyn Send + Sync>,
                key.open(self.reads.keys.as_ref(), &self.reads.io, first.payload),
            );
            let payload = match &reservation {
                Some(reservation) => reservation.retaining_io(open).await?,
                None => open.await?,
            };
            let replica_objects = ReplicaObjectCustody::from_delivery(
                scope, head.metadata.offset, &payload, rtdb_format::keys::CURRENT_STORAGE_VERSION,
                rtdb_types::StorageEpoch(0), Arc::clone(held) as Arc<dyn Send + Sync>,
            )?;
            let record = LogRecord {
                metadata: head.metadata,
                replica_objects,
                timed_original: None,
                payload: Bytes::from_owner(Delivery {
                    bytes: payload,
                    _held: Arc::clone(held),
                }),
            };
            let record = match reservation {
                Some(reservation) => record.retain(reservation)?,
                None => record,
            };
            Ok((record, snapshot_references))
        }.await;
        let same_predecessor = self.position.from() == Some(cut.from());
        let (record, references) = self.position.finish(cut, same_predecessor, delivered)?
            .ok_or_else(|| Error::Task("The selected replay record lost its predecessor".into()))?;
        if let (Some(snapshot), Some(references)) = (&self.snapshot, references) {
            snapshot.files.lock().pending = Some(references);
        }
        self.prepared = None;
        Ok(record)
    }
}

struct Delivery {
    bytes: Bytes,
    _held: Arc<Held>,
}

impl AsRef<[u8]> for Delivery {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[async_trait]
impl Log for JournalLog {
    fn requires_replica_payload_permit(&self) -> bool { true }
    fn needs_published_recovery_checkpoint(&self) -> bool { true }
    fn install_replica_reference_source(&self, source: ReplicaReferenceSource) -> Result<()> {
        let blocks = rtdb_replication::BlockStore::new(
            Arc::clone(&source.store), Arc::clone(&source.cache), Arc::clone(&source.memory), source.file_size_target,
        ).map_err(storage)?;
        self.reference_source.set(ReferenceSource { source, blocks })
            .map_err(|_| Error::Task("The installed journal already has a replay reference source".into()))
    }
    async fn publish_replica_payload(
        &self, store: &dyn rtdb_storage::ObjectStore, key: &rtdb_storage::ObjectKey, body: Bytes,
    ) -> Result<Option<rtdb_types::PhysicalObjectVersion>> {
        Ok(store.put_with_publication(key, body).await?.and_then(rtdb_types::PhysicalObjectPublication::into_version))
    }

    async fn confirms_recovery_checkpoint(&self, block: i64, through: LogOffset, root: [u8; 32]) -> Result<bool> {
        let Some(writer) = &self.writer else { return Ok(false); };
        let grant = writer.lock().await.grant;
        let route = self.reads.route().await?;
        let checkpoint = route.checkpoint;
        if route.writer.grant != Some(grant) || checkpoint.root != root || latest(checkpoint.cut).map_err(storage)? != through {
            return Ok(false);
        }
        crate::residence_trace::record("ordered-recovery-closure", || serde_json::json!({
            "deployment": route.placement.deployment, "incarnation": route.placement.incarnation,
            "group": route.placement.group, "generation": route.placement.generation,
            "epoch": grant.epoch, "revision": checkpoint.custody.revision,
            "block": block, "through": through.0, "root": root,
        }));
        Ok(true)
    }

    fn requires_checkpoint_digest(&self) -> bool {
        true
    }

    fn fences_writer_terms(&self) -> bool {
        true
    }

    async fn holds_writer_grant(&self) -> Result<bool> {
        let Some(writer) = &self.writer else { return Ok(false); };
        let grant = writer.lock().await.grant;
        Ok(self.reads.route().await?.writer.grant == Some(grant))
    }

    fn retains_synced_history(&self) -> bool {
        true
    }
    fn maximum_append_bytes(&self) -> Option<usize> {
        Some(self.maximum_data_message)
    }
    fn maximum_guarded_append_bytes(&self) -> Option<usize> {
        self.maximum_append_bytes()
    }

    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        self.enqueue_at(AppendClass::Data, payload, || Ok(timestamp))
            .await
    }

    async fn enqueue_maintenance(
        &self,
        payload: Bytes,
        timestamp: Timestamp,
    ) -> Result<AppendReceipt> {
        self.enqueue_at(AppendClass::Maintenance, payload, || Ok(timestamp))
            .await
    }

    async fn enqueue_guarded(
        &self,
        payload: Bytes,
        guard: GuardedAppend<'_>,
    ) -> Result<AppendReceipt> {
        if guard.journal_before().is_some() {
            return Err(Error::GuardedAppendUnsupported);
        }
        self.enqueue_at(AppendClass::Data, payload, || guard.sample())
            .await
    }

    async fn available_append_capacity(&self) -> Result<Option<AppendCapacity>> {
        let current = self.reads.route().await?;
        let grant = self.writer()?.lock().await.grant;
        if current.writer.grant != Some(grant) {
            return Err(deposed(grant, &current));
        }
        Ok(Some(AppendCapacity {
            available_bytes: current.window.bytes - current.window_spent,
            published_through: latest(current.checkpoint.cut).map_err(storage)?,
            last_data: current.last_data.map(latest).transpose().map_err(storage)?,
        }))
    }

    fn latest(&self) -> Result<LogOffset> {
        Ok(LogOffset(self.reads.tip.load(Ordering::Acquire)))
    }

    async fn retained_floor(&self) -> Result<Option<LogOffset>> {
        let view = self.reads.route().await?;
        i64::try_from(view.floor.next_lsn)
            .map(|floor| Some(LogOffset(floor)))
            .map_err(|_| Error::Task("The ordered replay floor exceeds the RTDB log range".into()))
    }

    async fn recovery_tail(&self, after: LogOffset) -> Result<Option<Box<dyn LogTail>>> {
        let next = next_offset(after)?;
        let Some(held) = self.recovery.lock().await.take() else {
            return self.tail(after).await.map(Some);
        };
        if !replay_cursor::starts_at(next, held.hold.start.next_lsn, false, false) {
            return Err(Error::UnavailableLogRange {
                requested: next as i64,
                first_retained: held.hold.start.next_lsn as i64,
            });
        }
        let mut cursor = Cursor::new(Arc::clone(&self.reads), next, None);
        let through = held.hold.through.next_lsn;
        cursor.attach(held, through)?;
        Ok(Some(Box::new(cursor)))
    }

    async fn tail(&self, after: LogOffset) -> Result<Box<dyn LogTail>> {
        let mut cursor = Cursor::new(Arc::clone(&self.reads), next_offset(after)?, None);
        cursor.bind(self.reads.route().await?).await?;
        Ok(Box::new(cursor))
    }

    async fn range(&self, from: LogOffset, to: LogOffset) -> Result<Box<dyn LogRange>> {
        Ok(Box::new(self.capture(from, to).await?))
    }

    async fn protect_snapshot(
        &self, from: LogOffset, to: LogOffset, root: [u8; 32],
    ) -> Result<rtdb_storage::log::ProtectedLogSnapshot> {
        let unavailable = || Error::Closed("The exact ordered checkpoint and suffix are unavailable".into());
        let start = u64::try_from(from.0).map_err(|_| unavailable())?;
        let end = u64::try_from(to.0).map_err(|_| unavailable())?;
        let view = self.reads.route().await?;
        if start < view.erasure_floor { return Err(Error::ErasedUnderView); }
        if start > end || end > view.head.next_lsn { return Err(unavailable()); }
        let source = self.reference_source.get().ok_or_else(unavailable)?;
        let command = Command::ProtectCheckpoint { root, from: start, through: end };
        let request = self.reads.journal.request(&view, command).map_err(storage)?;
        let held = match Held::acquire(&self.reads.journal, request, &self.reads.tasks).await {
            Ok(held) => held,
            Err(error) => {
                if start < self.reads.route().await?.erasure_floor { return Err(Error::ErasedUnderView); }
                return Err(storage(error));
            }
        };
        let scope = held.scope();
        if scope.checkpoint.root != root || scope.checkpoint.cut != scope.start
            || scope.start.next_lsn != start || scope.through.next_lsn != end
        {
            return Err(unavailable());
        }
        let files = Arc::new(SnapshotFiles {
            journal: Arc::clone(&self.reads.journal),
            held: Arc::clone(&held),
            epoch: view.writer.epoch,
            budget: Arc::clone(&source.source.memory),
            files: parking_lot::Mutex::default(),
        });
        let suffix = if start < end {
            let mut range = Cursor::new(Arc::clone(&self.reads), start, Some(end));
            range.attach(Arc::clone(&held), end)?;
            range.snapshot = Some(Arc::clone(&files));
            let authenticated_head = super::head_identity(self.reads.journal.placement(), scope.through);
            Some(rtdb_storage::log::ProtectedLogRange {
                authenticated_head: Some(authenticated_head), range: Box::new(range), hold: held,
            })
        } else {
            None
        };
        Ok(rtdb_storage::log::ProtectedLogSnapshot { suffix, provider: Some(files) })
    }

    async fn protect_range(&self, from: LogOffset, to: LogOffset) -> Result<rtdb_storage::log::ProtectedLogRange> {
        let range = self.capture(from, to).await?;
        if range.end != u64::try_from(to.0).ok() || from >= to {
            return Err(Error::Closed("The exact protected replica interval is unavailable".into()));
        }
        let hold = range.held.as_ref()
            .ok_or_else(|| Error::Closed("The replica interval has no ordered custody hold".into()))?
            .clone();
        if Some(hold.scope().through.next_lsn) != range.end {
            return Err(Error::Closed("The protected terminal coordinate differs from the requested interval".into()));
        }
        let authenticated_head = super::head_identity(self.reads.journal.placement(), hold.scope().through);
        Ok(rtdb_storage::log::ProtectedLogRange { authenticated_head: Some(authenticated_head), range: Box::new(range), hold })
    }

    async fn read_range(&self, from: LogOffset, to: LogOffset) -> Result<Vec<LogRecord>> {
        let mut cursor = self.capture(from, to).await?;
        let mut records = Vec::new();
        let mut bytes = 0usize;
        while let Some(head) = cursor.prepare(Duration::ZERO).await? {
            bytes = bytes.saturating_add(head.payload_len);
            if records.len() == MAX_RANGE_RECORDS || bytes > MAX_RANGE_BYTES {
                return Err(Error::LogReadLimit {
                    maximum_records: MAX_RANGE_RECORDS,
                    maximum_payload_bytes: MAX_RANGE_BYTES,
                });
            }
            records.push(cursor.deliver(head, None).await?);
        }
        Ok(records)
    }
}

#[async_trait]
impl LogTail for Cursor {
    async fn poll(&mut self, wait: Duration) -> Result<Vec<LogRecord>> {
        let Some(head) = self.prepare(wait).await? else {
            return Ok(Vec::new());
        };
        Ok(vec![self.deliver(head, None).await?])
    }

    fn replay(&mut self) -> Option<&mut dyn LogReplayTail> {
        Some(self)
    }
}

#[async_trait]
impl LogReplayTail for Cursor {
    async fn poll_head(&mut self, wait: Duration) -> Result<Option<ReplayHead>> {
        self.prepare(wait).await
    }

    async fn read_head(
        &mut self,
        head: ReplayHead,
        reservation: ReadReservation,
    ) -> Result<LogRecord> {
        self.deliver(head, Some(reservation)).await
    }
}

#[async_trait]
impl LogRange for Cursor {
    fn admit_snapshot_files(&mut self, position: LogOffset) -> Result<()> {
        self.snapshot.as_ref().map_or(Ok(()), |snapshot| snapshot.admit(position))
    }

    fn allocation_bound(&self) -> usize {
        self.reads.maximum.saturating_add(DELIVERED_RECORD_OVERHEAD + 32 * MAX_OBJECT_REFERENCES)
    }

    async fn next(&mut self, reservation: ReadReservation) -> Result<Option<LogRecord>> {
        let Some(head) = self.prepare(Duration::ZERO).await? else {
            return Ok(None);
        };
        self.deliver(head, Some(reservation)).await.map(Some)
    }
}

fn next_offset(after: LogOffset) -> Result<u64> {
    after
        .0
        .checked_add(1)
        .and_then(|next| u64::try_from(next).ok())
        .ok_or_else(|| Error::Task("Invalid ordered replay offset".into()))
}

fn storage(error: Anomaly) -> Error {
    Error::Remote {
        key: "installed journal".into(),
        message: error.to_string(),
    }
}

fn deposed(grant: Grant, current: &Route) -> Error {
    Error::Deposed {
        epoch: grant.epoch,
        promised: current.writer.epoch,
    }
}

fn reads_unsettled() -> Anomaly {
    Anomaly::new(
        rtdb_types::AnomalyCategory::Unavailable,
        "rtdb.multi/ordered-read-unsettled",
        "Earlier installed reads with lost outcomes are not yet released",
    )
}

#[cfg(test)]
mod reference_tests {
    use super::*;
    use rtdb_format::{envelope::ReplicaEnvelope, keys::CURRENT_STORAGE_VERSION, log::proto::NoOp};
    use rtdb_types::{StorageEpoch, Term, TermSequence};

    #[test]
    fn a_detached_body_needs_a_reference_even_when_its_inline_record_has_none() {
        let inline = ReplicaEnvelope {
            term: Term(1),
            term_sequence: Some(TermSequence(1)),
            message: ReplicaBody::NoOp(NoOp::default()),
        };
        let bytes = inline.encode();
        let detached = inline.offloaded(
            CURRENT_STORAGE_VERSION, StorageEpoch(0), BlockIndex(0), "body".into(), &bytes,
        );
        assert_eq!(inline_references(&bytes).unwrap(), Some(Vec::new()));
        assert!(inline_references(&detached.encode()).unwrap().is_none());
    }
}
