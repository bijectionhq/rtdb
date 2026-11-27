use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, RwLock},
    time::Duration,
};

use bytes::Bytes;
use journal_contract::{
    BoxFuture, CheckpointAnchor, CheckpointInstallation, Epoch, Identity, Journal, JournalError,
    Lsn, StreamId, WriterFence, WriterSessionId,
    reference::{FileJournal, Limits},
};
use rtdb_db::{ServingAuthority, ServingPermit};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

mod clock;
mod journal_custody;
pub(crate) mod operator;
pub(super) mod recovery;
pub use operator::{HELP as ACTIVATION_HELP, Options as ActivationOptions};

use clock::{ElapsedClock, HOLDER_WINDOW};

const ABSOLUTE_GRANT_PROTOCOL: u8 = 1;
const ELAPSED_GRANT_PROTOCOL: u8 = 2;
const CELL_GRANT_PROTOCOL: u8 = 3;
const RECOVERY_INTENT_PROTOCOL: u8 = 4;
const RECOVERY_KEY_CUSTODY_PROTOCOL: u8 = 5;
const RECOVERY_JOURNAL_CUSTODY_PROTOCOL: u8 = 6;
const RENEW_AHEAD: Duration = Duration::from_secs(1);
const ATTEMPTS: usize = 8;

pub struct ActivationAuthority {
    journal: Arc<dyn Journal>,
    identity: Uuid,
    connection_sha256: Option<[u8; 32]>,
    serving_cell: Option<Uuid>,
    access: Access,
    clock: Arc<ElapsedClock>,
}

#[derive(Clone, Copy)]
enum Access {
    LocalOperator,
    ConsensusOperator,
    ConsensusReader,
}

impl Access {
    fn is_local(self) -> bool {
        matches!(self, Self::LocalOperator)
    }

    fn require_operator(self) -> Result<()> {
        if matches!(self, Self::ConsensusReader) {
            return Err(Anomaly::new(
                AnomalyCategory::Forbidden,
                "rtdb/activation-operator",
                "Only the activation operator may change the binding",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub incarnation: Uuid,
    pub revision: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActivationStatus {
    pub binding: Binding,
    pub destination_cell: Option<Uuid>,
    pub grants_closed: bool,
    pub grants_end_micros: Option<i64>,
    pub grant_protocol_version: u8,
}

impl ActivationStatus {
    fn published(state: &State) -> Self {
        Self {
            binding: state.binding,
            destination_cell: state.destination_cell(),
            grants_closed: false,
            grants_end_micros: None,
            grant_protocol_version: state.version,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Publication {
    operation: Uuid,
    predecessor: Binding,
    manifest_digest: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    destination_cell: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recovery: Option<recovery::Completed>,
}

/// Produced by the destination installer after its reconstruction is committed.
#[derive(Clone, Copy)]
pub struct VerifiedSuccessor {
    predecessor: Uuid,
    successor: Uuid,
    manifest_digest: [u8; 32],
    activation_scope: Option<rtdb_db::ActivationScope>,
    destination_cell: Option<Uuid>,
    key_hold: Option<rtdb_crypt::RecoveryKeyHold>,
    journal_hold: Option<journal_custody::RetainedJournal>,
    source_cut: recovery::SourceCut,
}

impl VerifiedSuccessor {
    pub fn from_installation(installed: &rtdb_db::InstalledBackup) -> Self {
        Self {
            predecessor: installed.source_incarnation(),
            successor: Uuid::from_bytes(installed.destination_basis().incarnation()),
            manifest_digest: installed.manifest_sha256(),
            activation_scope: installed.activation_scope(),
            destination_cell: None,
            key_hold: None,
            journal_hold: None,
            source_cut: recovery::SourceCut {
                lsn: installed.source_lsn().0,
                position: installed.source_basis().position(),
                system_time: installed.source_system_time().0,
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct State {
    version: u8,
    binding: Binding,
    grants_closed: bool,
    grants_end: Option<i64>,
    retired: BTreeSet<Uuid>,
    publication: Option<Publication>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recovery: Option<recovery::Pending>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    superseded_recoveries: BTreeSet<Uuid>,
}

impl State {
    fn initial(incarnation: Uuid) -> Self {
        Self {
            version: ELAPSED_GRANT_PROTOCOL,
            binding: Binding {
                incarnation,
                revision: 1,
            },
            grants_closed: false,
            grants_end: None,
            retired: BTreeSet::new(),
            publication: None,
            recovery: None,
            superseded_recoveries: BTreeSet::new(),
        }
    }

    fn destination_cell(&self) -> Option<Uuid> {
        self.publication.and_then(|publication| publication.destination_cell)
    }

    fn require(&self, expected: Binding) -> Result<()> {
        if self.binding != expected {
            return Err(conflict());
        }
        Ok(())
    }

    fn already_published(
        &self,
        publication: Publication,
        successor: Uuid,
    ) -> Result<bool> {
        if let Some(prior) = self.publication.filter(|prior| prior.operation == publication.operation) {
            if prior == publication && self.binding.incarnation == successor {
                return Ok(true);
            }
            return Err(conflict());
        }
        self.require(publication.predecessor)?;
        if self.version >= RECOVERY_INTENT_PROTOCOL && publication.recovery.is_none() {
            return Err(conflict());
        }
        if !self.grants_closed || self.retired.contains(&successor) {
            return Err(conflict());
        }
        Ok(false)
    }
}

impl ActivationAuthority {
    async fn connect(
        url: &str,
        credentials: super::Credentials,
        identity: Uuid,
        access: Access,
    ) -> Result<Arc<Self>> {
        let journal_custody::KeeperJournal { journal, connection_sha256 } = journal_custody::KeeperJournal::connect(url, &credentials)?;
        let mut authority = Self::with_access(journal, identity, access)?;
        Arc::get_mut(&mut authority).expect("just connected").connection_sha256 = Some(connection_sha256);
        Ok(authority)
    }

    pub async fn for_development(
        data_journal: Option<&super::DevelopmentStreamJournal>,
    ) -> Result<Option<Arc<Self>>> {
        let Some((url, identity, credentials, cell)) = environment()? else {
            return Ok(None);
        };
        if let Some(data_journal) = data_journal {
            require_independent_journals(&url, &data_journal.url)?;
        }
        super::DevelopmentStreamJournal::new(&url, credentials.clone())?;
        let mut authority = Self::connect(&url, credentials, identity, Access::ConsensusReader).await?;
        Arc::get_mut(&mut authority).expect("just connected").serving_cell = cell;
        Ok(Some(authority))
    }

    /// All participants in an authority must use its same independent ordered journal.
    pub fn new(journal: Arc<dyn Journal>, identity: Uuid) -> Result<Arc<Self>> {
        Self::with_access(journal, identity, Access::ConsensusOperator)
    }

    fn with_access(journal: Arc<dyn Journal>, identity: Uuid, access: Access) -> Result<Arc<Self>> {
        if identity.is_nil() {
            return Err(conflict());
        }
        let clock: Arc<ElapsedClock> = Arc::new(clock::continuous_now);
        clock().ok_or_else(clock::unavailable)?;
        Ok(Arc::new(Self {
            journal,
            identity,
            connection_sha256: None,
            serving_cell: None,
            access,
            clock,
        }))
    }

    pub fn open_local(path: &Path, identity: Uuid) -> Result<Arc<Self>> {
        if identity.is_nil() {
            return Err(conflict());
        }
        let journal = FileJournal::open(path, Limits::default()).map_err(super::io_error)?;
        let journal = Arc::new(journal.handle(Identity::from_static("urn:rtdb:activation:local")));
        Self::with_access(journal, identity, Access::LocalOperator)
    }

    pub fn identity(&self) -> Uuid {
        self.identity
    }

    #[cfg(feature = "backup-object-holds")]
    pub(super) fn connection_digest(&self) -> Result<[u8; 32]> {
        self.connection_sha256.ok_or_else(configuration)
    }

    #[cfg(any(feature = "s3", feature = "backup-object-holds"))]
    pub(super) async fn connect_reader(url: &str, credentials: super::Credentials, identity: Uuid) -> Result<Arc<Self>> {
        Self::connect(url, credentials, identity, Access::ConsensusReader).await
    }

    pub(super) fn scope(&self, name: &str) -> Result<rtdb_db::ActivationScope> {
        rtdb_db::ActivationScope::for_cell(self.identity, name, self.serving_cell)
    }

    pub fn commissioning_stream_id(&self, name: &str) -> Result<StreamId> {
        self.stream_id(name)
    }

    pub fn successor_incarnation(
        &self, name: &str, expected: Binding, operation: Uuid,
    ) -> Result<Uuid> {
        self.successor_incarnation_in_cell(name, expected, operation, None)
    }

    pub(super) fn successor_incarnation_in_cell(
        &self,
        name: &str,
        expected: Binding,
        operation: Uuid,
        cell: Option<Uuid>,
    ) -> Result<Uuid> {
        successor_incarnation(self.identity, name, expected, operation, cell)
    }

    pub async fn bootstrap(&self, name: &str, incarnation: Uuid) -> Result<Binding> {
        self.access.require_operator()?;
        if incarnation.is_nil() {
            return Err(conflict());
        }
        let stream = self.stream_id(name)?;
        if self.access.is_local() {
            self.journal.create(stream).await
        } else {
            self.journal.create_immutable(stream).await
        }
        .map_err(journal_error)?;
        let state = self
            .change(stream, |state| match state {
                None => Ok(Some(State::initial(incarnation))),
                Some(state) if state.binding == State::initial(incarnation).binding => Ok(None),
                Some(_) => Err(conflict()),
            })
            .await?;
        Ok(state.binding)
    }

    pub async fn binding(&self, name: &str) -> Result<Binding> {
        Ok(self.status(name).await?.binding)
    }

    pub async fn status(&self, name: &str) -> Result<ActivationStatus> {
        let (_, state) = self.read(self.stream_id(name)?).await?;
        let state = state.ok_or_else(unavailable)?;
        Ok(ActivationStatus {
            binding: state.binding,
            destination_cell: state.destination_cell(),
            grants_closed: state.grants_closed,
            grants_end_micros: state.grants_end,
            grant_protocol_version: state.version,
        })
    }

    pub async fn upgrade_grants(&self, name: &str, expected: Binding) -> Result<()> {
        self.change(self.stream_id(name)?, |state| {
            let state = state.ok_or_else(unavailable)?;
            state.require(expected)?;
            if state.version >= ELAPSED_GRANT_PROTOCOL {
                return Ok(None);
            }
            let mut next = state.clone();
            next.version = ELAPSED_GRANT_PROTOCOL;
            Ok(Some(next))
        })
        .await
        .map(drop)
    }

    pub async fn close_grants(&self, name: &str, expected: Binding) -> Result<()> {
        self.change(self.stream_id(name)?, |state| {
            let state = state.ok_or_else(unavailable)?;
            state.require(expected)?;
            if state.version >= RECOVERY_INTENT_PROTOCOL {
                return Err(conflict());
            }
            if state.grants_closed {
                return Ok(None);
            }
            let mut next = state.clone();
            next.version = state.version.max(ELAPSED_GRANT_PROTOCOL);
            next.grants_closed = true;
            Ok(Some(next))
        })
        .await
        .map(drop)
    }

    async fn publication_status(
        &self,
        name: &str,
        expected: Binding,
        operation: Uuid,
        installation: Option<([u8; 32], Uuid)>,
    ) -> Result<Option<ActivationStatus>> {
        if operation.is_nil() {
            return Err(conflict());
        }
        let (_, state) = self.read(self.stream_id(name)?).await?;
        let state = state.ok_or_else(unavailable)?;
        if let Some(publication) = state.publication.filter(|prior| prior.operation == operation) {
            if publication.predecessor != expected
                || installation.is_some_and(|(digest, cell)| {
                    digest != publication.manifest_digest || Some(cell) != publication.destination_cell
                })
            {
                return Err(conflict());
            }
            return Ok(Some(ActivationStatus::published(&state)));
        }
        state.require(expected)?;
        if !state.grants_closed {
            return Err(conflict());
        }
        Ok(None)
    }

    pub async fn publish_successor(
        &self,
        name: &str,
        expected: Binding,
        operation: Uuid,
        successor: VerifiedSuccessor,
    ) -> Result<Binding> {
        Ok(self.publish(name, expected, operation, successor).await?.binding)
    }

    async fn publish(
        &self,
        name: &str,
        expected: Binding,
        operation: Uuid,
        successor: VerifiedSuccessor,
    ) -> Result<ActivationStatus> {
        self.access.require_operator()?;
        if successor.predecessor != expected.incarnation
            || successor.successor != successor_incarnation(self.identity, name, expected, operation, successor.destination_cell)?
            || successor.successor == expected.incarnation
            || successor.activation_scope != Some(rtdb_db::ActivationScope::for_cell(self.identity, name, successor.destination_cell)?)
        {
            return Err(conflict());
        }
        let publication = Publication {
            operation,
            predecessor: expected,
            manifest_digest: successor.manifest_digest,
            destination_cell: successor.destination_cell,
            recovery: None,
        };
        let stream = self.stream_id(name)?;
        let (_, frozen) = self.read(stream).await?;
        let frozen = frozen.ok_or_else(unavailable)?;
        if frozen.already_published(publication, successor.successor)? {
            return Ok(ActivationStatus::published(&frozen));
        }
        clock::wait_out(self.clock.as_ref(), clock::drain_window(frozen.grants_end.is_some())).await?;

        let state = self
            .change(stream, |state| {
                let state = state.ok_or_else(unavailable)?;
                if state.already_published(publication, successor.successor)? {
                    return Ok(None);
                }
                Ok(Some(self.install_binding(name, state, publication, successor.successor)?))
            })
            .await?;
        Ok(ActivationStatus::published(&state))
    }

    fn install_binding(
        &self, name: &str, state: &State, publication: Publication, incarnation: Uuid,
    ) -> Result<State> {
        let mut next = state.clone();
        let version = if publication.destination_cell.is_some() {
            CELL_GRANT_PROTOCOL
        } else {
            ELAPSED_GRANT_PROTOCOL
        };
        next.version = state.version.max(version);
        let predecessor = publication.predecessor;
        // Retain identities that were assigned outside the successor protocol.
        let derived_predecessor = state.publication.and_then(|prior| {
            successor_incarnation(self.identity, name, prior.predecessor, prior.operation, prior.destination_cell).ok()
        });
        if derived_predecessor != Some(predecessor.incarnation) {
            next.retired.insert(predecessor.incarnation);
        }
        next.binding = Binding {
            incarnation,
            revision: predecessor.revision.checked_add(1).ok_or_else(conflict)?,
        };
        next.grants_closed = false;
        next.grants_end = None;
        next.publication = Some(publication);
        next.recovery = None;
        next.superseded_recoveries.clear();
        Ok(next)
    }

    pub(super) fn serving_authority(
        self: &Arc<Self>,
        name: &str,
        incarnation: Uuid,
    ) -> Result<Arc<dyn ServingAuthority>> {
        self.serving_authority_for_cell(name, incarnation, self.serving_cell)
    }

    pub(super) fn serving_authority_for_cell(
        self: &Arc<Self>,
        name: &str,
        incarnation: Uuid,
        cell: Option<Uuid>,
    ) -> Result<Arc<dyn ServingAuthority>> {
        Ok(Arc::new(ServingBinding(Arc::new(BoundAuthority {
            authority: Arc::clone(self),
            stream: self.stream_id(name)?,
            incarnation,
            cell,
            grant: RwLock::new(None),
            renewal: Arc::new(Mutex::new(())),
        }))))
    }

    async fn grant(&self, stream: StreamId, incarnation: Uuid, cell: Option<Uuid>) -> Result<ServingPermit> {
        let requested_at = (self.clock)().ok_or_else(clock::unavailable)?;
        let (_, state) = self.read(stream).await?;
        let state = state.ok_or_else(unavailable)?;
        if state.grants_closed || state.binding.incarnation != incarnation || state.destination_cell() != cell {
            return Err(not_active());
        }
        if state.version < ELAPSED_GRANT_PROTOCOL {
            return Err(Anomaly::unsupported(
                "rtdb/activation-grant-protocol",
                "Upgrade the activation binding before opening this incarnation",
            ));
        }
        ServingPermit::bounded(requested_at, HOLDER_WINDOW, self.clock.clone())
    }

    fn stream_id(&self, name: &str) -> Result<StreamId> {
        activation_stream_id(self.identity, name)
    }

    async fn read(&self, stream: StreamId) -> Result<(Lsn, Option<State>)> {
        if !self.access.is_local() {
            self.journal
                .require_immutable(stream)
                .await
                .map_err(journal_error)?;
        }
        for _ in 0..ATTEMPTS {
            let head = self.journal.head(stream).await.map_err(journal_error)?;
            if head == Lsn(0) {
                return Ok((head, None));
            }
            let page = if self.access.is_local() {
                self.journal.read(stream, head, 1, Duration::ZERO).await
            } else {
                self.journal
                    .read_page(stream, head, head, 1, Duration::ZERO)
                    .await
            };
            let page = match page {
                Ok(page) => page,
                Err(JournalError::Trimmed { .. }) => continue,
                Err(error) => return Err(journal_error(error)),
            };
            let [(at, bytes)] = page.records.as_slice() else {
                continue;
            };
            if *at != head {
                return Err(corrupt());
            }
            let state: State = serde_json::from_slice(bytes).map_err(|_| corrupt())?;
            if !matches!(state.version, ABSOLUTE_GRANT_PROTOCOL | ELAPSED_GRANT_PROTOCOL | CELL_GRANT_PROTOCOL | RECOVERY_INTENT_PROTOCOL | RECOVERY_KEY_CUSTODY_PROTOCOL | RECOVERY_JOURNAL_CUSTODY_PROTOCOL)
                || state.destination_cell().is_some_and(|cell| cell.is_nil() || state.version < CELL_GRANT_PROTOCOL)
                || state.binding.incarnation.is_nil()
                || state.binding.revision == 0
                || state.retired.contains(&state.binding.incarnation)
                || !state.valid_recovery()
            {
                return Err(corrupt());
            }
            return Ok((head, Some(state)));
        }
        Err(unavailable())
    }

    async fn change(
        &self,
        stream: StreamId,
        mut next: impl FnMut(Option<&State>) -> Result<Option<State>>,
    ) -> Result<State> {
        self.access.require_operator()?;
        let local = self.access.is_local();
        let mut epoch = Epoch(u64::from(local));
        for _ in 0..ATTEMPTS {
            let (head, observed) = self.read(stream).await?;
            let Some(state) = next(observed.as_ref())? else {
                return observed.ok_or_else(corrupt);
            };
            let sealed = if local {
                self.journal
                    .seal(stream, epoch)
                    .await
                    .map(|head| (head, Fence::Local(epoch)))
            } else {
                let session = WriterSessionId(Uuid::new_v4().as_u128());
                self.journal
                    .seal_session(stream, epoch, session)
                    .await
                    .map(|receipt| (receipt.sealed_head_lsn, Fence::Immutable(receipt.fence)))
            };
            let fence = match sealed {
                Ok((sealed_head, fence)) if sealed_head == head => fence,
                Ok(_) => continue,
                Err(JournalError::Fenced { current }) => {
                    epoch = Epoch(
                        current
                            .0
                            .checked_add(u64::from(local))
                            .ok_or_else(unavailable)?,
                    );
                    continue;
                }
                Err(error) => return Err(journal_error(error)),
            };
            let bytes = serde_json::to_vec(&state)
                .map(Bytes::from)
                .map_err(|_| corrupt())?;
            let at = Lsn(head.0.checked_add(1).ok_or_else(unavailable)?);
            let anchor = CheckpointAnchor {
                announced_at: at,
                covered_through: head,
                record_sha256: Sha256::digest(&bytes).into(),
            };
            let (append, installed) = match fence {
                Fence::Local(epoch) => (self.journal.append(stream, epoch, at, bytes).await, false),
                Fence::Immutable(fence) => {
                    let checkpoint = CheckpointInstallation {
                        operation_id: Uuid::new_v4().as_u128(),
                        checkpoint_lsn: at,
                        covered_through_lsn: head,
                        record: bytes.clone(),
                    };
                    match self.journal.install_checkpoint(fence, checkpoint).await {
                        Ok(receipt) if receipt.checkpoint_lsn == at
                            && receipt.covered_through_lsn == head
                            && receipt.logical_floor_lsn == at => (Ok(at), true),
                        Ok(_) => return Err(corrupt()),
                        Err(JournalError::Unsupported) => (
                            self.journal.append_session_batch(fence, at, vec![bytes]).await, false,
                        ),
                        Err(error) => (Err(error), false),
                    }
                }
            };
            match append {
                Ok(committed) if committed == at => {
                    if !installed && head != Lsn(0) {
                        let _ = match fence {
                            Fence::Local(epoch) => self.journal.trim(stream, epoch, head).await,
                            Fence::Immutable(fence) => {
                                self.journal.trim_checkpoint(fence, anchor).await
                            }
                        };
                    }
                    return Ok(state);
                }
                Err(JournalError::Conflict { .. } | JournalError::Fenced { .. }) => continue,
                Err(error) => return Err(journal_error(error)),
                Ok(_) => return Err(corrupt()),
            }
        }
        Err(unavailable())
    }
}

enum Fence {
    Local(Epoch),
    Immutable(WriterFence),
}

struct BoundAuthority {
    authority: Arc<ActivationAuthority>,
    stream: StreamId,
    incarnation: Uuid,
    cell: Option<Uuid>,
    grant: RwLock<Option<ServingPermit>>,
    renewal: Arc<Mutex<()>>,
}

impl BoundAuthority {
    fn cached(&self) -> Option<(ServingPermit, Duration)> {
        let cached = self.grant.read().unwrap();
        let permit = cached.as_ref()?;
        Some((permit.clone(), permit.remaining().ok()?))
    }

    async fn renew(&self) -> Result<ServingPermit> {
        let permit = self.authority.grant(self.stream, self.incarnation, self.cell).await?;
        *self.grant.write().unwrap() = Some(permit.clone());
        Ok(permit)
    }
}

struct ServingBinding(Arc<BoundAuthority>);

impl ServingAuthority for ServingBinding {
    fn acquire(&self) -> BoxFuture<'_, Result<ServingPermit>> {
        Box::pin(async move {
            let binding = &self.0;
            if let Some((permit, remaining)) = binding.cached() {
                if remaining <= RENEW_AHEAD
                    && let Ok(renewing) = binding.renewal.clone().try_lock_owned()
                {
                    let binding = binding.clone();
                    tokio::spawn(async move {
                        let _renewing = renewing;
                        let _ = binding.renew().await;
                    });
                }
                return Ok(permit);
            }
            let _renewing = binding.renewal.lock().await;
            if let Some((permit, _)) = binding.cached() {
                return Ok(permit);
            }
            binding.renew().await
        })
    }
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/activation-unavailable",
        "The database serving authority is unavailable",
    )
}

fn not_active() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/activation-fenced",
        "The database incarnation may not serve",
    )
}

fn activation_stream_id(authority: Uuid, name: &str) -> Result<StreamId> {
    super::validate_deployment_name(name)?;
    let mut hash = Sha256::new();
    hash.update(b"rtdb.activation.v1");
    hash.update(authority.as_bytes());
    hash.update(name.as_bytes());
    let digest: [u8; 32] = hash.finalize().into();
    Ok(StreamId(u128::from_be_bytes(digest[..16].try_into().unwrap())))
}

fn successor_incarnation(
    authority: Uuid, name: &str, expected: Binding, operation: Uuid, cell: Option<Uuid>,
) -> Result<Uuid> {
    if operation.is_nil() || cell.is_some_and(|id| id.is_nil()) {
        return Err(conflict());
    }
    let stream = activation_stream_id(authority, name)?;
    let next_revision = expected.revision.checked_add(1).ok_or_else(conflict)?;
    let mut digest = Sha256::new();
    if let Some(cell) = cell {
        digest.update(b"rtdb.activation.cell-successor.v1");
        digest.update(cell.as_bytes());
    } else {
        digest.update(b"rtdb.activation.successor.v1");
    }
    digest.update(stream.0.to_be_bytes());
    digest.update(next_revision.to_be_bytes());
    digest.update(operation.as_bytes());
    Ok(Uuid::from_bytes(digest.finalize()[..16].try_into().unwrap()))
}

fn conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/activation-conflict",
        "The database activation does not match the current incarnation",
    )
}

fn corrupt() -> Anomaly {
    Anomaly::fault(
        "rtdb/activation-state",
        "The activation journal contains an invalid binding",
    )
}

fn journal_error(error: JournalError) -> Anomaly {
    match error {
        JournalError::Recovered { .. } | JournalError::Deleted => corrupt(),
        _ => unavailable(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use rtdb_types::Timestamp;
    use tokio::sync::Notify;

    use super::*;

    fn manual_clock(micros: Arc<AtomicU64>, sampled: Arc<Notify>) -> Arc<ElapsedClock> {
        Arc::new(move || {
            let time = micros.load(Ordering::SeqCst);
            sampled.notify_one();
            Some(Duration::from_micros(time))
        })
    }

    #[test]
    fn retained_client_handle_cannot_serve_after_successor_publication() {
        std::thread::Builder::new()
            .stack_size(rtdb_node::WORKER_STACK_BYTES)
            .spawn(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .thread_stack_size(rtdb_node::WORKER_STACK_BYTES)
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(guarded_client());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    async fn guarded_client() {
        use rtdb_columnar::MemoryBudget;
        use rtdb_crypt::Generation;
        use rtdb_db::DeploymentRetirement;
        use rtdb_tx::transact::{Outcome, PHYSICAL_ERASURE, ROW_CUSTODY, Transact};
        use stream_logkeeper::stream::consensus::{CLUSTER_VERSION_CURRENT, testing::LocalCluster};
        use tokio_util::sync::CancellationToken;

        let data = tempfile::tempdir().unwrap();
        const PRINCIPAL: &str = "urn:logkeeper:client:activation-test";
        let mut keepers = LocalCluster::start_replicated_catalog(&[PRINCIPAL]).await.unwrap();
        let journal_config = keepers.journal_config(64 << 20, 4 << 20);
        let journal = keepers.journal(PRINCIPAL, &journal_config).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match journal.client().finalize_version(CLUSTER_VERSION_CURRENT).await {
                    Ok(version) => break assert_eq!(version, CLUSTER_VERSION_CURRENT),
                    Err(JournalError::Unavailable) => tokio::time::sleep(Duration::from_millis(50)).await,
                    Err(error) => panic!("Unexpected journal finalization error: {error:?}"),
                }
            }
        })
        .await
        .unwrap();
        let journal_path = data.path().join("journal.json");
        let journal_config = serde_json::json!({
            "keepers": journal_config.keepers,
            "catalog": journal_config.catalog,
            "catalog_voters": journal_config.catalog_voters,
            "voters": journal_config.voters,
            "quota_bytes": journal_config.quota_bytes,
            "max_record_bytes": journal_config.max_record_bytes,
        });
        std::fs::write(&journal_path, serde_json::to_vec(&journal_config).unwrap()).unwrap();
        let application_journal = super::super::DevelopmentStreamJournal::new(
            &format!("{}{}", super::super::CONSENSUS_SCHEME, journal_path.display()),
            keepers.credentials(PRINCIPAL).unwrap(),
        )
        .unwrap();
        let authority =
            ActivationAuthority::open_local(&data.path().join("authority"), Uuid::new_v4())
                .unwrap();
        let incumbent = Uuid::new_v4();
        let original = authority.bootstrap("xtdb", incumbent).await.unwrap();
        let operation = Uuid::new_v4();
        let successor = authority.successor_incarnation("xtdb", original, operation).unwrap();
        let limit = 256 << 20;
        let mut config =
            crate::config::NodeConfiguration::from_yaml_with_environment("", &|_| None, false)
                .unwrap();
        config.flight_sql = None;
        config.multi_journal.immutable_streams = true;
        let memory = MemoryBudget::new(config.node_options(limit).unwrap().execution_memory_bytes);
        let runtime = super::super::MultiRuntime::development_with_activation(
            &data.path().join("source"),
            &mut config,
            memory.clone(),
            Some(incumbent),
            Some(application_journal),
            Some(authority.clone()),
        )
        .await
        .unwrap();
        let (handle, driver) =
            crate::ServerDriver::open_with_multi(config, limit, memory, &runtime)
                .await
                .unwrap();
        let server = tokio::spawn(driver.run());
        let stop = CancellationToken::new();
        tokio::select! {
            value = runtime.await_development_writer(handle.node(), incumbent) => { value.unwrap(); }
            result = runtime.drive(handle.node(), &stop) => panic!("Multi stopped before readiness: {result:?}"),
        }
        let attached = handle.node().catalog().database("xtdb").unwrap();
        let retained = attached.serving_driver().unwrap();
        let transaction = |key| Transact {
            custody_request_digest: None,
            request_memory: None,
            key: vec![key; 16],
            require: vec![],
            window: Default::default(),
            guards: vec![],
            requires: vec![],
            otherwise: vec![],
            ops: vec![],
        };
        let receipt = retained.transact(transaction(21), &stop).await.unwrap();
        assert_eq!(receipt.outcome, Outcome::Then);
        assert!(!retained.features().contains(&ROW_CUSTODY));
        assert!(!retained.features().contains(&PHYSICAL_ERASURE));
        assert!(!retained.supports_funded_transact().await);
        assert!(!retained.supports_funded_appointment().await);
        assert_eq!(
            retained.export_checkpoint(Uuid::new_v4(), receipt.basis, None, &stop)
                .await.unwrap_err().code,
            "rtdb/activation-effects",
        );
        assert_eq!(
            retained.reconcile_custody(&stop).await.unwrap_err().code,
            "rtdb/activation-effects",
        );
        assert_eq!(
            runtime.ready_driver("xtdb", incumbent).await.unwrap()
                .retire_deployment().await.unwrap_err().code,
            "rtdb/activation-effects",
        );
        retained
            .check_read_delivery(None, &BTreeSet::new(), &stop)
            .await
            .unwrap();

        let retired_database = Uuid::new_v4();
        let generation = Generation { database: retired_database, number: 1 };
        runtime.kms.create(generation).await.unwrap();
        runtime.catalog.reserve("retiring", retired_database).await.unwrap();
        runtime.catalog.mark_ready("retiring", retired_database).await.unwrap();
        let retiring = runtime.catalog.mark_retiring("retiring", DeploymentRetirement {
            incarnation: retired_database,
            terminal: Lsn(1),
            generation: generation.number,
            lease_until: 0,
        }, Timestamp(0)).await.unwrap();
        runtime.tick(handle.node()).await.unwrap();
        runtime.kms.check_readable(generation).await.unwrap();
        assert!(runtime.catalog.read().await.unwrap()["retiring"].status == retiring.status);

        authority.close_grants("xtdb", original).await.unwrap();
        let ready = VerifiedSuccessor {
            predecessor: incumbent,
            successor,
            manifest_digest: [3; 32],
            destination_cell: None,
            key_hold: None,
            journal_hold: None,
            source_cut: recovery::SourceCut::default(),
            activation_scope: Some(rtdb_db::ActivationScope::new(authority.identity(), "xtdb").unwrap()),
        };
        let published = tokio::time::timeout(
            Duration::from_secs(10),
            authority.publish_successor("xtdb", original, operation, ready),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(published.incarnation, successor);
        assert_eq!(
            retained
                .transact(transaction(22), &stop)
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-fenced",
        );
        assert_eq!(
            retained
                .check_read_delivery(None, &BTreeSet::new(), &stop)
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-fenced",
        );
        drop(attached);
        handle.shutdown();
        server.await.unwrap().unwrap();
        drop(handle);
        runtime.close().await;
        keepers.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn freeze_waits_from_each_publisher_observation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("activation");
        let micros = Arc::new(AtomicU64::new(1_000_000));
        let sampled = Arc::new(Notify::new());
        let identity = Uuid::new_v4();
        let mut authority = ActivationAuthority::open_local(&path, identity).unwrap();
        Arc::get_mut(&mut authority).unwrap().clock = manual_clock(micros.clone(), sampled.clone());
        let predecessor = Uuid::new_v4();
        let incumbent = authority.bootstrap("xtdb", predecessor).await.unwrap();
        let operation = Uuid::new_v4();
        let successor = authority.successor_incarnation("xtdb", incumbent, operation).unwrap();
        let installed = VerifiedSuccessor {
            predecessor,
            successor,
            manifest_digest: [4; 32],
            destination_cell: None,
            key_hold: None,
            journal_hold: None,
            source_cut: recovery::SourceCut::default(),
            activation_scope: Some(rtdb_db::ActivationScope::new(identity, "xtdb").unwrap()),
        };
        let serving = authority.serving_authority("xtdb", predecessor).unwrap();
        let old_grant = serving.acquire().await.unwrap();
        let stream = authority.commissioning_stream_id("xtdb").unwrap();
        assert_eq!(authority.journal.head(stream).await.unwrap(), Lsn(1));
        assert_eq!(
            authority
                .serving_authority("xtdb", successor)
                .unwrap()
                .acquire()
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-fenced"
        );
        authority.close_grants("xtdb", incumbent).await.unwrap();
        assert_eq!(
            authority
                .serving_authority("xtdb", predecessor)
                .unwrap()
                .acquire()
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-fenced"
        );
        old_grant.check().unwrap();

        let started = Arc::new(Notify::new());
        let first_clock = manual_clock(micros.clone(), started.clone());
        drop(serving);
        Arc::get_mut(&mut authority).unwrap().clock = first_clock;
        let publishing = {
            let authority = authority.clone();
            tokio::spawn(async move {
                authority.publish_successor("xtdb", incumbent, operation, installed).await
            })
        };
        started.notified().await;
        let holder_end = 1_000_000 + u64::try_from(HOLDER_WINDOW.as_micros()).unwrap();
        micros.store(holder_end - 1, Ordering::SeqCst);
        old_grant.check().unwrap();
        micros.store(holder_end, Ordering::SeqCst);
        assert_eq!(old_grant.check().unwrap_err().code, "rtdb/activation-fenced");
        assert_eq!(authority.binding("xtdb").await.unwrap(), incumbent);
        assert!(!publishing.is_finished());
        publishing.abort();
        assert!(publishing.await.unwrap_err().is_cancelled());
        drop(authority);

        let started = Arc::new(Notify::new());
        let mut authority = ActivationAuthority::open_local(&path, identity).unwrap();
        Arc::get_mut(&mut authority).unwrap().clock = manual_clock(micros.clone(), started.clone());
        let restarted = {
            let authority = authority.clone();
            tokio::spawn(async move {
                authority.publish_successor("xtdb", incumbent, operation, installed).await
            })
        };
        started.notified().await;
        let drain = u64::try_from(clock::drain_window(false).as_micros()).unwrap();
        micros.store(holder_end + drain - 1, Ordering::SeqCst);
        assert_eq!(authority.binding("xtdb").await.unwrap(), incumbent);
        assert!(!restarted.is_finished());
        micros.store(holder_end + drain, Ordering::SeqCst);
        let active = tokio::time::timeout(Duration::from_secs(1), restarted)
            .await.unwrap().unwrap().unwrap();
        assert_eq!(active, Binding { incarnation: successor, revision: 2 });
        assert_eq!(
            authority.publish_successor("xtdb", incumbent, operation, installed).await.unwrap(),
            active
        );
        assert_eq!(
            authority.close_grants("xtdb", incumbent).await.unwrap_err().code,
            "rtdb/activation-conflict"
        );
        authority.serving_authority("xtdb", successor).unwrap().acquire().await.unwrap();
    }

    #[tokio::test]
    async fn a_delayed_renewal_cannot_refresh_its_request_timestamp() {
        let directory = tempfile::tempdir().unwrap();
        let mut authority = ActivationAuthority::open_local(
            &directory.path().join("activation"), Uuid::new_v4()
        ).unwrap();
        let incarnation = Uuid::new_v4();
        authority.bootstrap("xtdb", incarnation).await.unwrap();
        let stream = authority.commissioning_stream_id("xtdb").unwrap();
        let clock_samples = AtomicU64::new(0);
        Arc::get_mut(&mut authority).unwrap().clock = Arc::new(move || {
            let sample = clock_samples.fetch_add(1, Ordering::SeqCst);
            Some(if sample == 0 { Duration::ZERO } else { HOLDER_WINDOW })
        });
        assert_eq!(authority.grant(stream, incarnation, None).await.unwrap_err().code, "rtdb/activation-fenced");
        assert_eq!(authority.journal.head(stream).await.unwrap(), Lsn(1));
    }

    #[tokio::test]
    async fn legacy_grant_metadata_survives_explicit_upgrade() {
        let directory = tempfile::tempdir().unwrap();
        let authority = ActivationAuthority::open_local(
            &directory.path().join("activation"), Uuid::new_v4()
        ).unwrap();
        let incarnation = Uuid::new_v4();
        let binding = authority.bootstrap("xtdb", incarnation).await.unwrap();
        let stream = authority.commissioning_stream_id("xtdb").unwrap();
        authority.change(stream, |state| {
            let mut state = state.unwrap().clone();
            state.version = ABSOLUTE_GRANT_PROTOCOL;
            state.grants_end = Some(123);
            Ok(Some(state))
        }).await.unwrap();
        assert_eq!(
            authority.grant(stream, incarnation, None).await.unwrap_err().code,
            "rtdb/activation-grant-protocol"
        );
        authority.upgrade_grants("xtdb", binding).await.unwrap();
        authority.upgrade_grants("xtdb", binding).await.unwrap();
        let status = authority.status("xtdb").await.unwrap();
        assert_eq!(status.binding, binding);
        assert_eq!(status.grant_protocol_version, ELAPSED_GRANT_PROTOCOL);
        assert_eq!(status.grants_end_micros, Some(123));
        authority.grant(stream, incarnation, None).await.unwrap();
        assert_eq!(authority.journal.head(stream).await.unwrap(), Lsn(3));
        assert!(clock::drain_window(true) > clock::drain_window(false));
    }

    #[tokio::test]
    async fn runtime_capability_cannot_mutate_an_authority_binding() {
        let directory = tempfile::tempdir().unwrap();
        let operator = ActivationAuthority::open_local(
            &directory.path().join("activation"), Uuid::new_v4()
        ).unwrap();
        let incumbent = operator.bootstrap("xtdb", Uuid::new_v4()).await.unwrap();
        let reader = ActivationAuthority::with_access(
            operator.journal.clone(), operator.identity(), Access::ConsensusReader,
        ).unwrap();
        assert_eq!(
            reader.bootstrap("xtdb", incumbent.incarnation).await.unwrap_err().code,
            "rtdb/activation-operator"
        );
        assert_eq!(
            reader.close_grants("xtdb", incumbent).await.unwrap_err().code,
            "rtdb/activation-operator"
        );
        assert!(!operator.status("xtdb").await.unwrap().grants_closed);
    }

}

#[cfg(any(feature = "s3", feature = "backup-object-holds"))]
pub(super) async fn installed(
    database_url: &str,
    immutable_streams: bool,
) -> Result<Option<Arc<ActivationAuthority>>> {
    let Some((url, identity, credentials, cell)) = environment()? else {
        return Ok(None);
    };
    if !immutable_streams {
        return Err(Anomaly::incorrect(
            "rtdb/activation-config",
            "Installed activation requires multiJournal.immutableStreams",
        ));
    }
    require_independent_journals(&url, database_url)?;
    let mut authority = ActivationAuthority::connect_reader(&url, credentials, identity).await?;
    Arc::get_mut(&mut authority).expect("just connected").serving_cell = cell;
    Ok(Some(authority))
}

fn require_independent_journals(authority_url: &str, database_url: &str) -> Result<()> {
    let read = |url: &str| {
        let path = url.strip_prefix(super::CONSENSUS_SCHEME).ok_or_else(configuration)?;
        super::JournalConfig::read(Path::new(path)).map_err(|_| configuration())
    };
    let authority = read(authority_url)?;
    let database = read(database_url)?;
    if authority.catalog == database.catalog
        || authority.keepers.keys().any(|keeper| database.keepers.contains_key(keeper))
    {
        return Err(configuration());
    }
    Ok(())
}

fn environment() -> Result<Option<(String, Uuid, super::Credentials, Option<Uuid>)>> {
    let url = match std::env::var("RTDB_ACTIVATION_JOURNAL_URL") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(_) => return Err(configuration()),
    };
    let env = |name| std::env::var(name).map_err(|_| configuration());
    let identity =
        Uuid::parse_str(&env("RTDB_ACTIVATION_AUTHORITY_ID")?).map_err(|_| configuration())?;
    let credentials = super::Credentials {
        certificate: env("RTDB_ACTIVATION_READER_CERTIFICATE_PATH")?.into(),
        private_key: env("RTDB_ACTIVATION_READER_PRIVATE_KEY_PATH")?.into(),
        ca: env("RTDB_ACTIVATION_CA_PATH")?.into(),
    };
    let cell = match std::env::var("RTDB_ACTIVATION_CELL_ID") {
        Ok(value) => Some(Uuid::parse_str(&value).ok().filter(|cell| !cell.is_nil()).ok_or_else(configuration)?),
        Err(std::env::VarError::NotPresent) => None,
        Err(_) => return Err(configuration()),
    };
    Ok(Some((url, identity, credentials, cell)))
}

fn configuration() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/activation-configuration",
        "Activation requires an independent consensus journal, authority ID and reader TLS paths",
    )
}
