//! RTDB storage-log projection of the installed journal's ordered grants
//! and exact replay intervals.

use super::{InstalledJournal, WriterAdmission, latest};
use async_trait::async_trait;
use bytes::Bytes;
use ordered_logkeeper::ordered::{
    APPEND_RECORD_CHARGE_BYTES, AppendClass, Command, Cut, Grant, Hold, Outcome, Refusal, Request,
    service::Route,
};
use rtdb_node::{ProtectedReadIndex, ReplicaReadIndex};
use rtdb_storage::{
    Error, Result,
    log::{
        AppendCapacity, AppendReceipt, GuardedAppend, Log, LogMetadata, LogOffset, LogRange,
        LogRecord, LogReplayTail, LogTail, ReplayHead,
    },
    object_store::ReadReservation,
};
use rtdb_types::{Anomaly, Timestamp};
use std::num::NonZeroU64;
use std::sync::{
    Arc,
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

pub(crate) struct JournalLog {
    reads: Arc<Reads>,
    maximum_data_message: usize,
    writer: Mutex<Writer>,
    recovery: Mutex<Option<Arc<Held>>>,
}

struct Writer {
    grant: Grant,
    head: Cut,
    pending: Option<Request>,
    pending_read: Option<Request>,
    next_read: Option<NonZeroU64>,
}

struct Reads {
    journal: Arc<InstalledJournal>,
    tip: AtomicI64,
    tasks: TaskTracker,
    maximum: usize,
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
        admission: WriterAdmission,
        recovery: Arc<Held>,
        maximum_message_bytes: usize,
        configured_window: Option<u64>,
        tasks: TaskTracker,
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
                .min(maximum_message_bytes);
        let reads = Arc::new(Reads {
            journal,
            tip: AtomicI64::new(latest(current.head).map_err(storage)?.0),
            tasks,
            maximum: maximum_message_bytes,
        });
        let writer = Mutex::new(Writer {
            grant: admission.grant,
            head: admission.recovery.through,
            pending: None,
            pending_read: None,
            next_read: Some(NonZeroU64::MIN),
        });
        Ok(Arc::new(Self {
            reads,
            maximum_data_message,
            writer,
            recovery: Mutex::new(Some(recovery)),
        }))
    }

    async fn enqueue_at(
        &self,
        class: AppendClass,
        payload: Bytes,
        sample: impl FnOnce() -> Result<Timestamp> + Send,
    ) -> Result<AppendReceipt> {
        let maximum = match class {
            AppendClass::Data => self.maximum_data_message,
            AppendClass::Maintenance => self.reads.maximum,
        };
        if payload.len() > maximum {
            return Err(Error::MessageTooLarge {
                size: payload.len(),
                maximum,
            });
        }
        let mut writer = self.writer.lock().await;
        if writer.pending.is_some() {
            self.finish_append(&mut writer).await?;
        }
        let current = self.reads.route().await?;
        if current.writer.grant != Some(writer.grant) || current.head != writer.head {
            return Err(deposed(writer.grant, &current));
        }
        let timestamp = sample()?;
        let command = Command::Append {
            class,
            grant: writer.grant,
            expected_next: writer.head.next_lsn,
            timestamp: timestamp.0,
            payload,
        };
        writer.pending = Some(
            self.reads
                .journal
                .request(&current, command)
                .map_err(storage)?,
        );
        let committed = self.finish_append(&mut writer).await?;
        Ok(AppendReceipt::observed(async move { Ok(committed) }))
    }

    async fn finish_append(&self, writer: &mut Writer) -> Result<LogMetadata> {
        let request = writer
            .pending
            .as_ref()
            .ok_or_else(|| Error::Task("No pending ordered append".into()))?;
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
            Err(error) => return Err(storage(error)),
        };
        if result.class != *class
            || result.lsn != *expected_next
            || result.lsn != writer.head.next_lsn
        {
            return Err(Error::Task(
                "The ordered append receipt addresses a different record".into(),
            ));
        }
        let metadata = LogMetadata {
            offset: latest(result.cut).map_err(storage)?,
            timestamp: Timestamp(*timestamp),
        };
        writer.head = result.cut;
        writer.pending = None;
        self.reads
            .tip
            .fetch_max(metadata.offset.0, Ordering::Release);
        Ok(metadata)
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

    async fn resolve_old_read(&self, writer: &mut Writer) -> rtdb_types::Result<()> {
        let Some(request) = writer.pending_read.as_ref() else {
            return Ok(());
        };
        match self.reads.journal.barrier(request).await {
            Ok(proof) => drop(self.reads.retain(proof.claim.read())),
            Err(error)
                if matches!(
                    error.code.as_ref(),
                    "rtdb.multi/ordered-capacity"
                        | "rtdb.multi/ordered-expired"
                        | "rtdb.multi/ordered-read-closed"
                        | "rtdb.multi/ordered-stale-grant"
                ) =>
            {
                let route = self.reads.journal.route().await?;
                if route.writer.grant != Some(writer.grant) {
                    return Err(deposed(writer.grant, &route).into());
                }
                let nonce = writer.next_read.ok_or_else(read_exhausted)?;
                let abandon = self.reads.journal.request(
                    &route,
                    Command::AbandonRead {
                        grant: writer.grant,
                        nonce,
                    },
                )?;
                let decision = self.reads.journal.execute(&abandon).await?;
                match decision.receipt.outcome {
                    Ok(Outcome::ReadAbandoned) => {}
                    Err(Refusal::Stale)
                        if self.reads.journal.route().await?.writer.grant == Some(writer.grant) => {
                    }
                    Err(reason) => return Err(super::refused(reason)),
                    Ok(_) => return Err(read_exhausted()),
                }
            }
            Err(error) => return Err(error),
        }
        writer.read_finished();
        Ok(())
    }
}

impl Writer {
    fn read_finished(&mut self) {
        self.next_read = self
            .next_read
            .and_then(|nonce| nonce.get().checked_add(1))
            .and_then(NonZeroU64::new);
        self.pending_read = None;
    }
}

#[async_trait]
impl ReplicaReadIndex for JournalLog {
    async fn confirm(&self) -> rtdb_types::Result<LogOffset> {
        Ok(self.protect().await?.offset)
    }

    async fn protect(&self) -> rtdb_types::Result<ProtectedReadIndex> {
        let mut writer = self.writer.lock().await;
        self.resolve_old_read(&mut writer).await?;
        let current = self.reads.journal.route().await?;
        if current.writer.grant != Some(writer.grant) {
            return Err(deposed(writer.grant, &current).into());
        }
        let nonce = writer.next_read.ok_or_else(read_exhausted)?;
        let invocation = self.reads.journal.request(
            &current,
            Command::Barrier {
                grant: writer.grant,
                nonce,
            },
        )?;
        writer.pending_read = Some(invocation.clone());
        let proof = self.reads.journal.barrier(&invocation).await?;
        let hold = proof.claim.read();
        let retention = self.reads.retain(hold);
        writer.read_finished();
        Ok(ProtectedReadIndex {
            offset: latest(hold.through)?,
            wait: Duration::from_secs(2),
            retention: Some(retention),
        })
    }
}

impl Reads {
    async fn route(&self) -> Result<Route> {
        let view = self.journal.route().await.map_err(storage)?;
        self.tip
            .fetch_max(latest(view.head).map_err(storage)?.0, Ordering::Release);
        Ok(view)
    }

    fn retain(self: &Arc<Self>, hold: Hold) -> Arc<Held> {
        Held::pin(Arc::clone(&self.journal), hold, &self.tasks)
    }

    async fn acquire(self: &Arc<Self>, view: &Route, from: u64, through: u64) -> Result<Arc<Held>> {
        let grant = view
            .writer
            .grant
            .ok_or_else(|| Error::Closed("There is no installed journal writer".into()))?;
        let command = Command::ProtectRange {
            grant,
            from,
            through,
        };
        let request = self.journal.request(view, command).map_err(storage)?;
        Held::acquire(&self.journal, request, &self.tasks)
            .await
            .map_err(storage)
    }
}

struct Prepared {
    metadata: ReplayHead,
    end: Cut,
}

struct Cursor {
    reads: Arc<Reads>,
    next: u64,
    end: Option<u64>,
    held: Option<Arc<Held>>,
    from: Option<Cut>,
    prepared: Option<Prepared>,
}

impl Cursor {
    fn new(reads: Arc<Reads>, next: u64, end: Option<u64>) -> Self {
        Self {
            reads,
            next,
            end,
            held: None,
            from: None,
            prepared: None,
        }
    }

    async fn bind(&mut self, view: Route) -> Result<()> {
        if self.end == Some(self.next) {
            return Ok(());
        }
        if self.next < view.floor.next_lsn {
            return Err(Error::UnavailableLogRange {
                requested: i64::try_from(self.next).unwrap_or(i64::MAX),
                first_retained: i64::try_from(view.floor.next_lsn).unwrap_or(i64::MAX),
            });
        }
        let through = self
            .end
            .map_or(view.head.next_lsn, |end| end.min(view.head.next_lsn));
        if self.next < through {
            let held = self.reads.acquire(&view, self.next, through).await?;
            self.from = Some(held.hold.start);
            self.held = Some(held);
        }
        Ok(())
    }

    async fn prepare(&mut self, wait: Duration) -> Result<Option<ReplayHead>> {
        if let Some(prepared) = &self.prepared {
            return Ok(Some(prepared.metadata));
        }
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            if let (Some(held), Some(from)) = (&self.held, self.from)
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
                let offset = latest(item.next).map_err(storage)?;
                let metadata = ReplayHead {
                    metadata: LogMetadata {
                        offset,
                        timestamp: Timestamp(item.timestamp),
                    },
                    payload_len,
                    allocation_bound: payload_len.saturating_add(DELIVERED_RECORD_OVERHEAD),
                };
                self.prepared = Some(Prepared {
                    metadata,
                    end: item.next,
                });
                return Ok(Some(metadata));
            }
            self.held = None;
            self.from = None;
            if self.end == Some(self.next) {
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
        let from = self
            .from
            .ok_or_else(|| Error::Task("No ordered replay position".into()))?;
        let page = self
            .reads
            .journal
            .replay(held.hold, from, head.allocation_bound as u64)
            .await
            .map_err(storage)?;
        let first = page
            .records
            .into_iter()
            .next()
            .ok_or_else(|| Error::Task("No record in the protected interval".into()))?;
        if first.result.cut != prepared.end
            || first.timestamp != head.metadata.timestamp.0
            || first.payload.len() != head.payload_len
        {
            return Err(Error::Task(
                "The ordered record differs from the prepared head".into(),
            ));
        }
        let record = LogRecord {
            metadata: head.metadata,
            payload: Bytes::from_owner(Delivery {
                bytes: first.payload,
                _held: Arc::clone(held),
            }),
        };
        let record = match reservation {
            Some(reservation) => record.retain(reservation)?,
            None => record,
        };
        self.next = first.result.cut.next_lsn;
        self.from = Some(first.result.cut);
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
    fn requires_checkpoint_digest(&self) -> bool {
        true
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
        let grant = self.writer.lock().await.grant;
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
        if next != held.hold.start.next_lsn {
            return Err(Error::UnavailableLogRange {
                requested: next as i64,
                first_retained: held.hold.start.next_lsn as i64,
            });
        }
        let mut cursor = Cursor::new(Arc::clone(&self.reads), next, None);
        cursor.from = Some(held.hold.start);
        cursor.held = Some(held);
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
    fn allocation_bound(&self) -> usize {
        self.reads.maximum.saturating_add(DELIVERED_RECORD_OVERHEAD)
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

fn read_exhausted() -> Anomaly {
    Anomaly::new(
        rtdb_types::AnomalyCategory::Unavailable,
        "rtdb.multi/ordered-read-sequence",
        "The installed writer cannot issue another read nonce",
    )
}
