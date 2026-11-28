use std::{sync::{Arc, RwLock}, time::Duration};

use bytes::Bytes;
use journal_contract::{BoxFuture, CheckpointAnchor, CheckpointInstallation, CheckpointReceipt, Epoch, Journal, JournalError, Lsn, Page, RecoveryEntry, RecoveryFence, StreamId, WriterFence, WriterSessionId};
use rtdb_crypt::{Context, Generation, Kms};
use rtdb_format::journal::Record;
use rtdb_tx::transact::TransactionRecord;
use rtdb_types::{Anomaly, AnomalyCategory, Result, TimedJournalCutoff, Timestamp};
use sha2::{Digest, Sha256};
use stream_logkeeper::stream::consensus::ConsensusJournal;
use uuid::Uuid;

use crate::{ActivationScope, commissioning};

const PAGE_SIZE: usize = 128;
const KEEPER_BATCH_RECORDS: usize = 256;
const KEEPER_BATCH_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimedOriginal {
    pub stream: StreamId,
    pub epoch: Epoch,
    pub at: Lsn,
    pub payload_sha256: [u8; 32],
    pub cutoff: TimedJournalCutoff,
}

/// The immutable journal bytes named by a timed append commitment.
pub(crate) struct PreparedTimedAppend {
    original: TimedOriginal,
    payload: Bytes,
}

impl PreparedTimedAppend {
    pub(crate) fn commitment(&self) -> TimedOriginal {
        self.original
    }

    pub(crate) fn len(&self) -> usize {
        self.payload.len()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimedOriginalProof {
    pub original: TimedOriginal,
    pub quorum_sync_at: Timestamp,
    pub evidence_sha256: [u8; 32],
}

impl TimedOriginalProof {
    pub(crate) fn verify_exact(self, original: TimedOriginal) -> Result<()> {
        if self.original != original || self.evidence_sha256 == [0; 32]
            || self.quorum_sync_at.0 <= 0 || self.quorum_sync_at >= original.cutoff.before()
        {
            return Err(Anomaly::fault("rtdb/timed-original-proof", "The provider returned a different timed original quorum proof"));
        }
        Ok(())
    }
}

/// Commits a contiguous append group through its returned head.
/// An error may leave a committed prefix of the group.
pub trait JournalBatchAppender: Send + Sync {
    fn append_batch(
        &self,
        stream: StreamId,
        epoch: Epoch,
        records: Vec<(Lsn, Bytes)>,
    ) -> BoxFuture<'_, journal_contract::Result<Lsn>>;

    fn maximum_timed_original_bytes(&self) -> Option<usize> { None }

    /// Returns verified original quorum evidence for the exact sealed bytes.
    fn append_timed_original(
        &self,
        _original: TimedOriginal,
        _payload: Bytes,
    ) -> BoxFuture<'_, journal_contract::Result<TimedOriginalProof>> {
        Box::pin(async { Err(JournalError::Unavailable) })
    }

    fn append_session_timed_original(
        &self,
        _fence: WriterFence,
        _original: TimedOriginal,
        _payload: Bytes,
    ) -> BoxFuture<'_, journal_contract::Result<TimedOriginalProof>> {
        Box::pin(async { Err(JournalError::Unsupported) })
    }

    /// Reopens retained proofs under the original keeper identities after writer replacement.
    fn prove_timed_original(
        &self,
        _original: TimedOriginal,
    ) -> BoxFuture<'_, journal_contract::Result<TimedOriginalProof>> {
        Box::pin(async { Err(JournalError::Unavailable) })
    }
}

#[derive(Clone)]
pub(crate) struct Stream {
    pub(crate) database: Uuid,
    journal: Arc<dyn Journal>,
    pub(crate) authenticated_journal: Option<Arc<ConsensusJournal>>,
    create_authenticated: bool,
    pub(crate) serving_authority: Option<Arc<dyn crate::ServingAuthority>>,
    pub(crate) activation_scope: Option<ActivationScope>,
    pub(crate) batch: Option<Arc<dyn JournalBatchAppender>>,
    pub(crate) kms: Arc<dyn Kms>,
    pub(crate) manifests: Arc<crate::replay::ManifestCache>,
    tail: Arc<TailPoll>,
    session: Option<Arc<WriterSession>>,
}

struct WriterSession {
    id: WriterSessionId,
    fence: RwLock<Option<WriterFence>>,
    verified_history: tokio::sync::OnceCell<()>,
}

/// A trim bound to this stream residence and an exact checkpoint announcement.
pub(crate) struct CertifiedCheckpoint {
    stream_id: StreamId,
    writer_session_id: Option<WriterSessionId>,
    trim: CheckpointTrim,
}

impl CertifiedCheckpoint {
    pub(crate) fn immutable_anchor(&self) -> Option<CheckpointAnchor> {
        match self.trim {
            CheckpointTrim::Immutable(anchor) => Some(anchor),
            CheckpointTrim::Local(_) => None,
        }
    }
}

enum CheckpointTrim {
    Local(Lsn),
    Immutable(CheckpointAnchor),
}

/// One journal long poll shared by every waiter of a stream: the first waiter
/// polls, the others wait for what it finds, and one of them takes over when
/// it ends without news.
#[derive(Default)]
struct TailPoll {
    polling: tokio::sync::Mutex<()>,
    /// A position the last poll found committed, published to the waiters.
    found: tokio::sync::watch::Sender<u64>,
    ended: tokio::sync::Notify,
}

/// The running poll. Ending it, including by dropping its waiter, frees the
/// poll before waking the waiters, so a woken waiter can take it over.
struct Polling<'a> {
    guard: Option<tokio::sync::MutexGuard<'a, ()>>,
    ended: &'a tokio::sync::Notify,
}

impl Drop for Polling<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        self.ended.notify_waiters();
    }
}

pub(crate) struct Entry {
    pub(crate) epoch: Epoch,
    pub(crate) generation: u64,
    pub(crate) record: Record,
}

impl Stream {
    pub(crate) fn new(database: Uuid, journal: Arc<dyn Journal>, kms: Arc<dyn Kms>) -> Self {
        Self {
            database,
            journal,
            authenticated_journal: None,
            create_authenticated: false,
            serving_authority: None,
            activation_scope: None,
            batch: None,
            kms,
            manifests: Arc::default(),
            tail: Arc::default(),
            session: None,
        }
    }

    pub(crate) fn with_immutable_history(mut self) -> Self {
        self.session = Some(Arc::new(WriterSession {
            id: WriterSessionId(Uuid::new_v4().as_u128()),
            fence: RwLock::new(None),
            verified_history: tokio::sync::OnceCell::new(),
        }));
        self
    }

    pub(crate) fn with_authenticated_journal(mut self, journal: Arc<ConsensusJournal>, create: bool) -> Self {
        self.journal = journal.clone();
        self.authenticated_journal = Some(journal);
        self.create_authenticated = create;
        self
    }

    pub(crate) fn immutable_history(&self) -> bool {
        self.session.is_some()
    }

    fn id(&self) -> StreamId {
        StreamId(u128::from_be_bytes(*self.database.as_bytes()))
    }

    fn timed_original(&self, at: Lsn, epoch: Epoch, payload: &[u8], cutoff: TimedJournalCutoff) -> TimedOriginal {
        TimedOriginal { stream: self.id(), epoch, at, payload_sha256: Sha256::digest(payload).into(), cutoff }
    }

    pub(crate) async fn create(&self) -> Result<()> {
        self.kms
            .create(Generation {
                database: self.database,
                number: 1,
            })
            .await?;
        if let Some(session) = &self.session {
            if let Some(authenticated) = self.authenticated_journal.as_ref().filter(|_| self.create_authenticated) {
                match authenticated.require_authenticated(self.id()).await {
                    Ok(()) => (),
                    Err(JournalError::NotFound) => authenticated.create_authenticated(self.id()).await.map_err(journal_error)?,
                    Err(error) => return Err(journal_error(error)),
                }
            } else {
                self.journal.create_immutable(self.id()).await.map_err(journal_error)?;
            }
            let _ = session.verified_history.set(());
            Ok(())
        } else {
            self.journal.create(self.id()).await.map_err(journal_error)
        }
    }

    async fn verify_history(&self) -> journal_contract::Result<()> {
        if let Some(session) = &self.session {
            session.verified_history.get_or_try_init(|| self.journal.require_immutable(self.id())).await?;
        }
        Ok(())
    }

    pub(crate) async fn head(&self) -> Result<Lsn> {
        self.verify_history().await.map_err(journal_error)?;
        self.journal.head(self.id()).await.map_err(journal_error)
    }

    pub(crate) async fn check_commissioning(&self, head: Lsn) -> Result<()> {
        let mut head = head;
        for _ in 0..4 {
            let page = match self.page(head, head, 1, Duration::ZERO).await {
                Ok(page) => page,
                Err(JournalError::Trimmed { .. }) => {
                    head = self.head().await?;
                    continue;
                }
                Err(error) => return Err(journal_error(error)),
            };
            let Some((at, bytes)) = page.records.first().filter(|(at, _)| *at == head) else {
                return Err(journal_error(JournalError::Unavailable));
            };
            // The journal's recovery entry holds no RTDB record of its own.
            if page.recovery(*at).is_none() {
                self.decode_unproven(*at, bytes).await?;
            }
            return Ok(());
        }
        Err(journal_error(JournalError::Unavailable))
    }

    /// Commit the scope before a new incarnation can acquire an application writer.
    pub(crate) async fn commission(&self) -> Result<()> {
        if self.activation_scope.is_none() {
            return Ok(());
        }
        let session = self.session.as_ref().ok_or_else(commissioning::scope_mismatch)?;
        let mut expected_writer_epoch = Epoch(0);
        for _ in 0..8 {
            let head = self.head().await?;
            if head != Lsn(0) {
                return self.check_commissioning(head).await;
            }
            let seal = match self.journal.seal_session(self.id(), expected_writer_epoch, session.id).await {
                Ok(seal) => seal,
                Err(JournalError::Fenced { current }) => {
                    expected_writer_epoch = current;
                    continue;
                }
                Err(error) => return Err(journal_error(error)),
            };
            if seal.sealed_head_lsn != Lsn(0) {
                return self.check_commissioning(seal.sealed_head_lsn).await;
            }
            let first_lsn = Lsn(1);
            let entry = Entry {
                epoch: seal.fence.writer_epoch,
                generation: 1,
                record: Record::Commissioned,
            };
            let bytes = self.encode(first_lsn, &entry).await?;
            match self.journal.append_session_batch(seal.fence, first_lsn, vec![bytes]).await {
                Ok(at) if at == first_lsn => return Ok(()),
                Ok(_) => return Err(journal_error(JournalError::Unavailable)),
                Err(JournalError::Fenced { current }) => expected_writer_epoch = current,
                Err(JournalError::Conflict { head }) if head != Lsn(0) => {
                    return self.check_commissioning(head).await;
                }
                Err(error) => return Err(journal_error(error)),
            }
        }
        Err(journal_error(JournalError::Unavailable))
    }

    pub(crate) async fn require_serving(&self) -> Result<()> {
        if let Some(authority) = &self.serving_authority {
            authority.acquire().await?.check()?;
        }
        Ok(())
    }

    async fn authorize_mutation(&self) -> journal_contract::Result<()> {
        self.require_serving().await.map_err(|_| JournalError::Unavailable)
    }

    pub(crate) async fn seal(&self, epoch: Epoch) -> journal_contract::Result<Lsn> {
        let Some(session) = &self.session else {
            self.authorize_mutation().await?;
            return self.journal.seal(self.id(), epoch).await;
        };
        self.verify_history().await?;
        let expected_epoch = epoch.0.checked_sub(1).map(Epoch).ok_or(JournalError::Unavailable)?;
        self.authorize_mutation().await?;
        let receipt = self.journal.seal_session(self.id(), expected_epoch, session.id).await?;
        let fence = WriterFence { stream: self.id(), writer_epoch: epoch, writer_session_id: session.id };
        if receipt.fence != fence {
            return Err(JournalError::Unavailable);
        }
        let mut current = session.fence.write().expect("writer session");
        if let Some(previous) = *current
            && previous.writer_epoch > epoch
        {
            return Err(JournalError::Fenced { current: previous.writer_epoch });
        }
        *current = Some(fence);
        Ok(receipt.sealed_head_lsn)
    }

    fn session_fence(&self, epoch: Epoch) -> journal_contract::Result<Option<WriterFence>> {
        let Some(session) = &self.session else {
            return Ok(None);
        };
        let fence = session.fence.read().expect("writer session").ok_or(JournalError::Unavailable)?;
        if fence.writer_epoch != epoch {
            return Err(JournalError::Fenced { current: fence.writer_epoch });
        }
        Ok(Some(fence))
    }

    /// Lifts an operator recovery's fence and returns the journal head.
    pub(crate) async fn acknowledge_recovery(&self) -> journal_contract::Result<Lsn> {
        self.authorize_mutation().await?;
        self.journal.acknowledge_recovery(self.id()).await
    }

    /// The pending operator recovery's fence, `None` once it is acknowledged.
    /// A journal without atomic recovery markers returns `Unsupported`.
    pub(crate) async fn recovery_fence(&self) -> journal_contract::Result<Option<RecoveryFence>> {
        self.journal.recovery_fence(self.id()).await
    }

    /// Appends `marker` right after the recovered head under the recovery's
    /// epoch and lifts exactly that fence, in one journal decision.
    pub(crate) async fn acknowledge_recovery_with_marker(&self, fence: RecoveryFence, marker: Bytes) -> journal_contract::Result<Lsn> {
        self.authorize_mutation().await?;
        self.journal.acknowledge_recovery_with_marker(self.id(), fence, marker).await
    }

    pub(crate) async fn read(&self, from: Lsn) -> journal_contract::Result<Page> {
        self.read_through(from, Lsn(0)).await
    }

    pub(crate) async fn read_through(&self, from: Lsn, head: Lsn) -> journal_contract::Result<Page> {
        self.page(from, head, PAGE_SIZE, Duration::ZERO).await
    }

    async fn page(&self, from: Lsn, head: Lsn, limit: usize, wait: Duration) -> journal_contract::Result<Page> {
        if self.immutable_history() {
            self.verify_history().await?;
            self.journal.read_page(self.id(), from, head, limit, wait).await
        } else {
            self.journal.read(self.id(), from, limit, wait).await
        }
    }

    /// Waits until `until` for a committed record after `applied`, sharing one
    /// journal long poll among this stream's concurrent waiters.
    pub(crate) async fn wait_after(&self, applied: Lsn, until: tokio::time::Instant) -> Result<()> {
        let tail = &*self.tail;
        let mut found = tail.found.subscribe();
        // Only a poll that ends after this wait began counts: an older find
        // may predate a recovery that shortened the journal.
        found.mark_unchanged();
        loop {
            let ended = tail.ended.notified();
            tokio::pin!(ended);
            ended.as_mut().enable();
            if let Ok(guard) = tail.polling.try_lock() {
                let _polling = Polling { guard: Some(guard), ended: &tail.ended };
                let wait = until.saturating_duration_since(tokio::time::Instant::now());
                if self.holds_after(applied, wait).await? {
                    tail.found.send_replace(applied.0 + 1);
                }
                return Ok(());
            }
            tokio::select! {
                _ = found.changed() => {
                    if *found.borrow_and_update() > applied.0 {
                        return Ok(());
                    }
                }
                _ = &mut ended => {}
                _ = tokio::time::sleep_until(until) => return Ok(()),
            }
        }
    }

    /// Waits up to `wait` for a committed record after `applied`.
    /// A reader below the retained floor, or behind an operator recovery, also has a record to apply.
    async fn holds_after(&self, applied: Lsn, wait: Duration) -> Result<bool> {
        match self.page(next(applied)?, applied, 1, wait).await {
            Ok(page) => Ok(!page.records.is_empty()),
            Err(JournalError::Trimmed { .. } | JournalError::Recovered { .. }) => Ok(true),
            Err(error) => Err(journal_error(error)),
        }
    }

    pub(crate) async fn certify_checkpoint(
        &self,
        base: &crate::replay::ReplayBase,
        minimum_head: Lsn,
    ) -> Result<CertifiedCheckpoint> {
        let through = Lsn(base.checkpoint.through);
        let invalid = || {
            Anomaly::fault("rtdb/checkpoint-trim-proof", "The trim does not name an exact committed checkpoint")
        };
        if through >= base.announced
            || base.announced > minimum_head
            || base.checkpoint.manifest.database != self.database
        {
            return Err(invalid());
        }
        let trim = if self.immutable_history() {
            let page = self
                .page(base.announced, minimum_head, 1, Duration::ZERO)
                .await
                .map_err(journal_error)?;
            let [(announced, bytes)] = page.records.as_slice() else {
                return Err(invalid());
            };
            if *announced != base.announced || page.head < minimum_head || page.floor > *announced {
                return Err(invalid());
            }
            let entry = self.decode_unproven(*announced, bytes).await?;
            if entry.generation != base.checkpoint.key_generation
                || !matches!(entry.record, Record::Checkpoint(checkpoint) if checkpoint == base.checkpoint)
            {
                return Err(invalid());
            }
            CheckpointTrim::Immutable(CheckpointAnchor {
                announced_at: *announced,
                covered_through: through,
                record_sha256: Sha256::digest(bytes).into(),
            })
        } else {
            CheckpointTrim::Local(through)
        };
        Ok(CertifiedCheckpoint {
            stream_id: self.id(),
            writer_session_id: self.session.as_ref().map(|session| session.id),
            trim,
        })
    }

    pub(crate) async fn install_checkpoint(
        &self,
        epoch: Epoch,
        checkpoint: CheckpointInstallation,
    ) -> journal_contract::Result<Option<CheckpointReceipt>> {
        let Some(fence) = self.session_fence(epoch)? else {
            return Ok(None);
        };
        self.authorize_mutation().await?;
        match self.journal.install_checkpoint(fence, checkpoint).await {
            Ok(receipt) => Ok(Some(receipt)),
            // Before the cluster finalizes atomic installation, publish the
            // record and select it with the verified compatibility request.
            Err(JournalError::Unsupported) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn selected_checkpoint(&self) -> Result<Option<CheckpointReceipt>> {
        if !self.immutable_history() {
            return Ok(None);
        }
        self.verify_history().await.map_err(journal_error)?;
        match self.journal.selected_checkpoint(self.id()).await {
            Ok(receipt) => Ok(receipt),
            Err(JournalError::Unsupported) => Ok(None),
            Err(error) => Err(journal_error(error)),
        }
    }

    pub(crate) async fn read_checkpoint_record(&self, at: Lsn) -> journal_contract::Result<Page> {
        self.page(at, at, 1, Duration::ZERO).await
    }

    pub(crate) async fn trim(
        &self,
        epoch: Epoch,
        checkpoint: CertifiedCheckpoint,
    ) -> journal_contract::Result<()> {
        if checkpoint.stream_id != self.id()
            || checkpoint.writer_session_id != self.session.as_ref().map(|session| session.id)
        {
            return Err(JournalError::Unsupported);
        }
        self.authorize_mutation().await?;
        match (self.session_fence(epoch)?, checkpoint.trim) {
            (Some(fence), CheckpointTrim::Immutable(anchor)) => {
                self.journal.trim_checkpoint(fence, anchor).await
            }
            (None, CheckpointTrim::Local(through)) => self.journal.trim(self.id(), epoch, through).await,
            _ => Err(JournalError::Unsupported),
        }
    }

    pub(crate) async fn encode(&self, lsn: Lsn, entry: &Entry) -> Result<Bytes> {
        let record = entry.record.encode()?;
        let plaintext = commissioning::encode_entry(self.activation_scope, entry.epoch.0, &record);
        let generation = Generation {
            database: self.database,
            number: entry.generation,
        };
        let context = Context::Journal { lsn: lsn.0 };
        Ok(
            rtdb_crypt::seal(self.kms.as_ref(), generation, context, &plaintext)
                .await?
                .into(),
        )
    }

    pub(crate) async fn prepare_timed_append(&self, lsn: Lsn, entry: &Entry, cutoff: TimedJournalCutoff) -> Result<PreparedTimedAppend> {
        let payload = self.encode(lsn, entry).await?;
        let original = self.timed_original(lsn, entry.epoch, &payload, cutoff);
        Ok(PreparedTimedAppend { original, payload })
    }

    pub(crate) async fn decode(&self, lsn: Lsn, bytes: &[u8]) -> Result<Entry> {
        let entry = self.decode_unproven(lsn, bytes).await?;
        if let Record::Tx { payload, .. } = &entry.record
            && let Some(cutoff) = TransactionRecord::timed_journal_cutoff(payload)?
        {
            let batch = self.batch.as_ref().ok_or_else(rtdb_tx::transact::funding_unsupported)?;
            let original = self.timed_original(lsn, entry.epoch, bytes, cutoff);
            batch.prove_timed_original(original).await.map_err(journal_error)?.verify_exact(original)?;
        }
        Ok(entry)
    }

    /// The entry the journal's typed recovery entry at `lsn` records. The
    /// writer's marker inside it is its `Recovered` record, sealed at `lsn`
    /// like any entry, and must name the same fence. Without one, as after
    /// an administrator's acknowledgement, the entry is the journal's alone,
    /// under the key generation `generation` the history has there.
    pub(crate) async fn decode_recovery(&self, lsn: Lsn, typed: &RecoveryEntry, generation: u64) -> Result<Entry> {
        let fence = typed.fence;
        if typed.at() != lsn {
            return Err(recovery_entry_invalid("The journal's recovery entry is not right after the recovered head"));
        }
        let Some(marker) = &typed.marker else {
            return Ok(Entry {
                epoch: fence.epoch,
                generation,
                record: Record::Recovered {
                    head: fence.head.0,
                    epoch: fence.epoch.0,
                    time: i64::try_from(typed.time_unix_ms).unwrap_or(i64::MAX / 1_000).saturating_mul(1_000),
                },
            });
        };
        let entry = self.decode_unproven(lsn, marker).await?;
        match entry.record {
            Record::Recovered { head, epoch, .. } if head == fence.head.0 && epoch == fence.epoch.0 && entry.epoch == fence.epoch => Ok(entry),
            _ => Err(recovery_entry_invalid("The writer's recovery marker does not name the journal's recovery fence")),
        }
    }

    /// Decodes an entry without proving a funded transaction's timed
    /// original, for comparing it with an entry already applied.
    pub(crate) async fn decode_unproven(&self, lsn: Lsn, bytes: &[u8]) -> Result<Entry> {
        let context = Context::Journal { lsn: lsn.0 };
        let plaintext = rtdb_crypt::open(self.kms.as_ref(), self.database, context, bytes).await?;
        let (epoch, record) = commissioning::decode_entry(self.activation_scope, &plaintext.bytes)?;
        let record = Record::decode(record)?;
        if matches!(record, Record::Commissioned) && self.activation_scope.is_none() {
            return Err(commissioning::scope_mismatch());
        }
        Ok(Entry { epoch: Epoch(epoch), generation: plaintext.generation.number, record })
    }

    pub(crate) async fn append(
        &self,
        epoch: Epoch,
        at: Lsn,
        payload: Bytes,
    ) -> journal_contract::Result<Lsn> {
        self.authorize_mutation().await?;
        match self.session_fence(epoch)? {
            Some(fence) => self.journal.append_session_batch(fence, at, vec![payload]).await,
            None => self.journal.append(self.id(), epoch, at, payload).await,
        }
    }

    pub(crate) async fn append_group(
        &self,
        epoch: Epoch,
        mut records: Vec<(Lsn, Bytes)>,
    ) -> journal_contract::Result<Lsn> {
        if let Some(fence) = self.session_fence(epoch)? {
            return self.append_session_group(fence, records).await;
        }
        if let Some(batch) = &self.batch {
            self.authorize_mutation().await?;
            return batch.append_batch(self.id(), epoch, records).await;
        }
        if records.len() != 1 {
            return Err(JournalError::Unavailable);
        }
        let (at, payload) = records.pop().expect("one record");
        self.append(epoch, at, payload).await
    }
    async fn append_session_group(
        &self,
        fence: WriterFence,
        records: Vec<(Lsn, Bytes)>,
    ) -> journal_contract::Result<Lsn> {
        let first_lsn = records.first().ok_or(JournalError::Unavailable)?.0;
        for (index, (lsn, payload)) in records.iter().enumerate() {
            if first_lsn.0.checked_add(index as u64) != Some(lsn.0) {
                return Err(JournalError::Unavailable);
            }
            if payload.len() > KEEPER_BATCH_BYTES {
                return Err(JournalError::TooLarge { max: KEEPER_BATCH_BYTES });
            }
        }
        let mut records = records.into_iter().peekable();
        let mut head = Lsn(0);
        while let Some((first, _)) = records.peek() {
            let first = *first;
            let mut bytes = 0;
            let mut batch = Vec::new();
            while let Some((_, payload)) = records.peek() {
                if batch.len() == KEEPER_BATCH_RECORDS || bytes + payload.len() > KEEPER_BATCH_BYTES {
                    break;
                }
                bytes += payload.len();
                batch.push(records.next().expect("peeked record").1);
            }
            let expected_head = Lsn(first.0 + (batch.len() as u64 - 1));
            self.authorize_mutation().await?;
            head = self.journal.append_session_batch(fence, first, batch).await?;
            if head != expected_head {
                return Err(JournalError::Unavailable);
            }
        }
        Ok(head)
    }

    pub(crate) async fn append_timed_original(
        &self, prepared: PreparedTimedAppend, dispatch_is_open: impl FnOnce() -> bool,
    ) -> journal_contract::Result<TimedOriginalProof> {
        let PreparedTimedAppend { original, payload } = prepared;
        if original.stream != self.id() {
            return Err(JournalError::Corrupted);
        }
        self.authorize_mutation().await?;
        if !dispatch_is_open() {
            return Err(JournalError::Unavailable);
        }
        let batch = self.batch.as_ref().ok_or(JournalError::Unsupported)?;
        match self.session_fence(original.epoch)? {
            Some(fence) => batch.append_session_timed_original(fence, original, payload).await,
            None => batch.append_timed_original(original, payload).await,
        }
    }

}

fn recovery_entry_invalid(message: &str) -> Anomaly {
    Anomaly::fault("rtdb/multi-replay", message)
}

pub(crate) fn next(lsn: Lsn) -> Result<Lsn> {
    lsn.0
        .checked_add(1)
        .map(Lsn)
        .ok_or_else(|| Anomaly::fault("rtdb/journal-exhausted", "The journal has no next position"))
}

/// The recovery faults this process reported, in the order of
/// `RECOVERY_FAULT_CODES`.
static RECOVERY_FAULTS: [std::sync::atomic::AtomicU64; 3] = [const { std::sync::atomic::AtomicU64::new(0) }; 3];
const RECOVERY_FAULT_CODES: [&str; 3] = ["rtdb/journal-recovered", "rtdb/journal-shortened", "rtdb/journal-rewritten"];

fn recovery_fault(kind: usize) {
    RECOVERY_FAULTS[kind].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// How many journal recovery faults this process reported, by anomaly code:
/// `rtdb/journal-recovered` when a journal call met an operator recovery that
/// no writer had acknowledged yet, and `rtdb/journal-shortened` or
/// `rtdb/journal-rewritten` when a replica or reader found that a recovery
/// replaced the history it had replayed and rebuilt from the journal.
pub fn journal_recovery_faults() -> [(&'static str, u64); 3] {
    std::array::from_fn(|kind| (RECOVERY_FAULT_CODES[kind], RECOVERY_FAULTS[kind].load(std::sync::atomic::Ordering::Relaxed)))
}

/// The journal head is behind what this process already replayed: the stream
/// was recovered after losing two of its three voters, and the records after
/// `head` through `replayed` are lost.
pub(crate) fn journal_shortened(head: Lsn, replayed: Lsn) -> Anomaly {
    recovery_fault(1);
    Anomaly::fault(
        "rtdb/journal-shortened",
        format!(
            "The journal ends at {} but this process replayed through {}; journal records {} through {} were lost in a recovery",
            head.0,
            replayed.0,
            head.0.saturating_add(1),
            replayed.0
        ),
    )
}

/// The journal record at `at` is not the one this process replayed there, or
/// the journal announces a recovery after records this process replayed: an
/// operator recovery replaced the history this process built its state from.
pub(crate) fn journal_rewritten(at: Lsn) -> Anomaly {
    recovery_fault(2);
    Anomaly::fault(
        "rtdb/journal-rewritten",
        format!(
            "The journal no longer holds the history this process replayed through {}; an operator recovery replaced it",
            at.0
        ),
    )
}

pub(crate) fn journal_error(error: JournalError) -> Anomaly {
    if let JournalError::Recovered { head } = error {
        recovery_fault(0);
        return Anomaly::fault(
            "rtdb/journal-recovered",
            format!(
                "The journal was recovered after losing two of its three voters and ends at {}; commits acknowledged after it may be lost. Writes resume once a writer or an administrator acknowledges the recovery",
                head.0
            ),
        );
    }
    let (category, code) = match error {
        JournalError::Fenced { .. } => (AnomalyCategory::Unavailable, "rtdb/writer-fenced"),
        JournalError::Conflict { .. } => (AnomalyCategory::Conflict, "rtdb/journal-conflict"),
        JournalError::Trimmed { .. } => (AnomalyCategory::NotFound, "rtdb/cursor-expired"),
        JournalError::TooLarge { .. } => {
            (AnomalyCategory::Incorrect, "rtdb/journal-record-too-large")
        }
        JournalError::Full { .. } => (AnomalyCategory::Busy, "rtdb/journal-full"),
        JournalError::NotFound | JournalError::Deleted => {
            (AnomalyCategory::NotFound, "rtdb/unknown-database")
        }
        JournalError::Unavailable => (AnomalyCategory::Unavailable, "rtdb/journal-unavailable"),
        JournalError::Unsupported => (AnomalyCategory::Unsupported, "rtdb/journal-unsupported"),
        JournalError::Corrupted => (AnomalyCategory::Fault, "rtdb/journal-corrupted"),
        JournalError::Recovered { .. } => unreachable!("handled above"),
    };
    let anomaly = Anomaly::new(category, code, error.to_string());
    if matches!(error, JournalError::TooLarge { .. }) { anomaly.with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded) } else { anomaly }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovered_and_shortened_journals_name_the_loss() {
        let recovered = journal_error(JournalError::Recovered { head: Lsn(41) });
        assert_eq!(recovered.code, "rtdb/journal-recovered");
        assert!(recovered.message.unwrap().contains("ends at 41"));
        let shortened = journal_shortened(Lsn(41), Lsn(50));
        assert_eq!(shortened.code, "rtdb/journal-shortened");
        assert!(shortened.message.unwrap().contains("42 through 50"));
        let rewritten = journal_rewritten(Lsn(50));
        assert_eq!(rewritten.code, "rtdb/journal-rewritten");
        assert!(rewritten.message.unwrap().contains("through 50"));
        let counted = journal_recovery_faults();
        assert_eq!(counted.map(|(code, _)| code), ["rtdb/journal-recovered", "rtdb/journal-shortened", "rtdb/journal-rewritten"]);
        assert!(counted.iter().all(|(_, count)| *count > 0));
    }

    #[test]
    fn timed_original_proof_requires_exact_sealed_bytes_and_an_exclusive_quorum_sample() {
        let original = TimedOriginal {
            stream: StreamId(1), epoch: Epoch(2), at: Lsn(3), payload_sha256: [4; 32],
            cutoff: TimedJournalCutoff::new(Timestamp(20), [5; 32]).unwrap(),
        };
        let proof = TimedOriginalProof { original, quorum_sync_at: Timestamp(19), evidence_sha256: [6; 32] };
        assert!(proof.verify_exact(original).is_ok());
        assert!(proof.verify_exact(TimedOriginal { payload_sha256: [7; 32], ..original }).is_err());
        assert!(proof.verify_exact(TimedOriginal { epoch: Epoch(8), ..original }).is_err());
        assert!(TimedOriginalProof { quorum_sync_at: Timestamp(20), ..proof }.verify_exact(original).is_err());
    }
}
