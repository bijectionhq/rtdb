//! Coordination of the exact-head rotation and the erasure commit.

use async_trait::async_trait;
use ordered_logkeeper::ordered::{
    Checkpoint, CheckpointPreparation, Command, Cut, Grant, Outcome, Placement, Principal, Receipt,
    Request, RequestId,
    replication::RotationSlots,
    service::wire::{Decision, SignedRotationSlots},
};
use rtdb_replication::SqlErasureSource;
use tokio_util::sync::CancellationToken;
use rtdb_types::{Anomaly, Result};
use uuid::Uuid;

use crate::journal::{
    Authority, InstalledJournal, WriterAdmission,
    custody::{CheckpointAuthority, CheckpointCustody, ErasureBinding, Preparation, Reservation},
    refused,
};
use crate::keys::{JournalKey, KeyService};
use crate::replay_broker::{Client as CustodyBroker, domain};

mod decision;
mod drain;
mod evidence;
pub(super) mod installed;
pub use drain::{DrainRegistration, ErasureDrainJournal, SignedDrainJoin};
pub(super) use drain::{
    record_checkpoint_disposition, record_serving_disposition, retirement_admitted,
    serving_disposition, serving_retirement_epoch,
};
use evidence::CustodyEvidence;
pub use evidence::{
    CustodyDrain, CustodyWork, DeadlineReservation, DestructionEvidence, DrainParticipant,
    DrainedWork, ErasureDeadline, ErasureTiming, drain_inventory,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlErasureRequest {
    pub operation: crate::replay_broker::domain::Operation,
    pub transaction: rtdb_types::TransactionKey,
    pub position: rtdb_types::ReplicaPosition,
    pub rotation: RequestId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Erasure {
    pub placement: Placement,
    pub operation: Uuid,
    pub custody_domain: Uuid,
    pub incumbent: Grant,
    pub timing: ErasureTiming,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedErasure {
    pub erasure: Erasure,
    pub checkpoint: Checkpoint,
    pub rotation: RequestId,
}

fn binding(erasure: Erasure, rotation: RequestId) -> ErasureBinding {
    ErasureBinding {
        operation: erasure.operation,
        custody_domain: erasure.custody_domain,
        rotation,
        fixed_before_ms: match erasure.timing {
            ErasureTiming::Fixed { before_ms } => Some(before_ms),
            ErasureTiming::OperationalTarget => None,
        },
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ErasureCommit {
    pub prepared: PreparedErasure,
    pub committed_at_ms: u64,
    pub deadline: ErasureDeadline,
    /// Attests the exact retiring key inventory selected by the custody owner.
    pub retiring_inventory: [u8; 32],
    /// Commits the plaintext, egress and copy participants of the closing custody domain.
    pub draining_inventory: [u8; 32],
    /// Authenticated Logkeeper response used to bind the ordered switch.
    pub switch_evidence: [u8; 32],
}

#[derive(Clone, Copy)]
pub enum ErasureProgress {
    Prepared(PreparedErasure),
    Committed(ErasureCommit),
}

pub struct CleanRecoverySlots {
    prepared: PreparedErasure,
    successor: WriterAdmission,
    receipt: Receipt,
    slots: SignedRotationSlots,
    binding: [u8; 32],
    deadline: Option<DeadlineReservation>,
}

impl CleanRecoverySlots {
    pub fn prepared(&self) -> PreparedErasure {
        self.prepared
    }

    pub fn successor(&self) -> WriterAdmission {
        self.successor
    }

    pub fn binding(&self) -> [u8; 32] {
        self.binding
    }

    pub fn receipt(&self) -> Receipt {
        self.receipt
    }

    pub fn slots(&self) -> &SignedRotationSlots {
        &self.slots
    }

    pub fn deadline(&self) -> Option<DeadlineReservation> {
        self.deadline
    }
}

/// The authenticated switch's complete drain, read back from custody media.
pub struct JoinedCustody {
    closed_at_ms: u64,
    joined: Vec<DrainedWork>,
    journal_key_disposition: [u8; 32],
    journal_key_observed_at_ms: u64,
    checkpoint_disposition: [u8; 32],
    checkpoint_observed_at_ms: u64,
    serving_disposition: [u8; 32],
    serving_observed_at_ms: u64,
}

impl JoinedCustody {
    pub fn closed_at_ms(&self) -> u64 { self.closed_at_ms }
    pub fn observations(&self) -> &[DrainedWork] { &self.joined }
    pub fn journal_key_disposition(&self) -> [u8; 32] { self.journal_key_disposition }
    pub fn journal_key_observed_at_ms(&self) -> u64 { self.journal_key_observed_at_ms }
    pub fn checkpoint_disposition(&self) -> [u8; 32] { self.checkpoint_disposition }
    pub fn checkpoint_observed_at_ms(&self) -> u64 { self.checkpoint_observed_at_ms }
    pub fn serving_disposition(&self) -> [u8; 32] { self.serving_disposition }
    pub fn serving_observed_at_ms(&self) -> u64 { self.serving_observed_at_ms }
}

#[async_trait]
pub trait ErasureCustody: Send + Sync {
    /// Reserve an enforceable deadline for this exact attempt before any
    /// irreversible switch. Retrying the same attempt reuses its reservation.
    async fn reserve_deadline(&self, _prepared: PreparedErasure) -> Result<DeadlineReservation> {
        Err(evidence::authority_unavailable())
    }

    /// Stop incumbent writes and await pending appends and offloaded writes.
    /// Hold the write fence through restart until the attempt is resolved.
    ///
    /// Prepare the exact cut in Logkeeper, stage an independently readable
    /// successor at that cut, and retain the exact prepared tuple before
    /// returning. Its immutable closure must preserve surviving history and
    /// exclude targeted rows, artefacts and old key dependencies, including
    /// any evidence recovery needs to recognize the adopted catalog.
    /// Re-entry returns the original tuple or the exact committed result.
    async fn prepare(&self, erasure: Erasure) -> Result<ErasureProgress>;

    /// The journal switch has closed old-root and replay admission. Use the
    /// signed decision and quorum evidence to establish the custody drain.
    /// Make the result and the retiring keys' irrevocable deadline one
    /// durable decision. The authority owns the clock, all grants and key
    /// operations in the affected domain. A concurrent or retried commit of
    /// the same operation returns the original receipt and expiry.
    async fn commit_and_arm(&self, ready: CleanRecoverySlots) -> Result<ErasureCommit>;

    async fn sign_root_drain(
        &self, _operation: domain::Operation, _commit: ErasureCommit, _drain: domain::RootDrain,
    ) -> Result<Vec<u8>> {
        Err(evidence::authority_unavailable())
    }

    async fn sign_root_destruction(
        &self, _operation: domain::Operation, _commit: ErasureCommit, _result: &domain::RootDestruction,
    ) -> Result<Vec<u8>> {
        Err(evidence::authority_unavailable())
    }

    /// Read back the original owner's physical disposition after the custody
    /// drain. A pending or ambiguous provider result has no completion evidence.
    async fn destruction(
        &self,
        _commit: ErasureCommit,
        _drained: &JoinedCustody,
    ) -> Result<Option<DestructionEvidence>> {
        Err(Anomaly::unsupported(
            "rtdb.multi/erasure-evidence",
            "This custody profile supplies no authenticated destruction evidence",
        ))
    }
}

pub struct ErasureCoordinator<'a> {
    worker: &'a InstalledJournal,
    custodian: &'a InstalledJournal,
    provider: &'a CheckpointCustody,
    keys: &'a dyn KeyService,
    authority: Option<CustodyEvidence>,
    broker: &'a CustodyBroker,
    operation: domain::Operation,
}

impl<'a> ErasureCoordinator<'a> {
    pub fn new(
        worker: &'a InstalledJournal,
        custodian: &'a InstalledJournal,
        provider: &'a CheckpointCustody,
        keys: &'a dyn KeyService,
        broker: &'a CustodyBroker,
        operation: domain::Operation,
    ) -> Result<Self> {
        if !matches!(worker.authority(), Authority::Worker { .. })
            || !matches!(custodian.authority(), Authority::Custody)
            || worker.placement() != custodian.placement()
        {
            return Err(invalid(
                "Erasure requires matching installed worker and custody credentials",
            ));
        }
        Ok(Self {
            worker,
            custodian,
            provider,
            keys,
            authority: None,
            broker,
            operation,
        })
    }

    /// Binds fixed-deadline and destruction evidence to the installed custody
    /// signer. A storage credential or an operational schedule confers no such authority.
    pub fn with_custody_verification_key(mut self, key: [u8; 32]) -> Result<Self> {
        self.authority = Some(CustodyEvidence::new(key)?);
        Ok(self)
    }

    /// Rewrites the SQL survivor closure from the protected selected source
    /// and materializes it in the successor provider allocation.
    pub async fn stage_survivor(
        &self,
        erasure: Erasure,
        rotation: RequestId,
        reservation: Reservation,
        selection: &SqlErasureSource,
        cancellation: &CancellationToken,
    ) -> Result<PreparedErasure> {
        self.require_incumbent(erasure)?;
        self.authorize_root().await?;
        let route = self.custodian.route().await?;
        if !drain::retirement_completed(self.provider, route.placement, route.erasure_floor).await? {
            return Err(invalid("The previous erasure still has unconfirmed destruction"));
        }
        let epoch = self.worker.log_epoch();
        let base_matches = match selection.checkpoint() {
            Some(base) => base.root_sha256 == Some(route.checkpoint.root)
                && base.boundary.after_offset(epoch).checked_add(1)
                    .and_then(|next| u64::try_from(next).ok()) == Some(route.checkpoint.cut.next_lsn),
            None => super::genesis(route.checkpoint),
        };
        let cut = selection.cut();
        if !base_matches
            || selection.authenticated_head() != crate::journal::head_identity(route.placement, route.head)
            || cut.replica_epoch != epoch
            || route.writer.grant != Some(erasure.incumbent)
            || rotation.nonce.is_nil()
            || rotation.expires_at <= route.position.saturating_add(1)
            || route.head.next_lsn <= route.checkpoint.cut.next_lsn
            || !crate::journal::custody::member_declaration::fresh_survivor_allocation(
                reservation.custody.allocation.as_u128(),
                route.checkpoint.custody.allocation.as_u128(),
            )
        {
            return Err(invalid(
                "Erasure staging needs a new root and custody at the exact incumbent head",
            ));
        }
        let survivors = selection.build_survivor(CheckpointCustody::copy_budget(&reservation)?, cancellation).await?;
        let checkpoint = super::checkpoint_for_block(&survivors.catalog, epoch)
            .filter(|candidate| candidate.root_sha256 != Some(route.checkpoint.root))
            .ok_or_else(|| invalid("The survivor rewrite has no new exact catalog"))?;
        let source = survivors.store();
        let origin = Preparation {
            authority: CheckpointAuthority::Erasure(binding(erasure, rotation)),
            writer: erasure.incumbent,
            base: route.checkpoint,
            through: route.head,
            checkpoint,
        };
        let preparation = origin.ordered(reservation)?;
        match route.preparing {
            Some(chosen) if chosen == preparation => (),
            Some(_) => return Err(invalid("The journal is preparing another checkpoint")),
            None => {
                let references = self
                    .provider
                    .preflight(&reservation, origin, source)
                    .await?;
                self.custodian
                    .prepare_checkpoint(&route, preparation, references)
                    .await?;
            }
        }
        self.provider.begin(&reservation, origin).await?;
        let checkpoint = match self.provider.prepared(reservation, origin).await? {
            Some(checkpoint) => checkpoint,
            None => self.provider.prepare(&reservation, origin, source).await?,
        };
        let prepared = PreparedErasure {
            erasure,
            checkpoint,
            rotation,
        };
        self.reopen(prepared).await?;
        Ok(prepared)
    }

    async fn authorize_root(&self) -> Result<()> {
        let placement = self.custodian.placement();
        self.broker.custody_domain(self.operation.domain, domain::Action::AuthorizeRoot {
            operation: self.operation, root: placement.deployment, history: domain::installed_history(placement)?,
        }).await?;
        Ok(())
    }

    fn require_incumbent(&self, erasure: Erasure) -> Result<()> {
        self.require_placement(erasure)?;
        if !matches!(self.worker.authority(), Authority::Worker { enrollment } if enrollment == erasure.incumbent.enrollment) {
            return Err(invalid(
                "Erasure must address the installed credential of the incumbent",
            ));
        }
        Ok(())
    }

    fn require_placement(&self, erasure: Erasure) -> Result<()> {
        if erasure.placement != self.custodian.placement() || erasure.operation != self.operation.erase
            || erasure.custody_domain != self.operation.domain
        {
            return Err(invalid("Erasure must address this installed custodian and operation"));
        }
        if erasure.operation.is_nil()
            || erasure.custody_domain.is_nil()
            || erasure.timing == (ErasureTiming::Fixed { before_ms: 0 })
        {
            return Err(invalid(
                "Erasure requires an exact operation, custody domain and timing profile",
            ));
        }
        if matches!(erasure.timing, ErasureTiming::Fixed { .. }) && self.authority.is_none() {
            return Err(evidence::authority_unavailable());
        }
        Ok(())
    }

    pub async fn commit(
        &self,
        custody: &dyn ErasureCustody,
        erasure: Erasure,
    ) -> Result<ErasureCommit> {
        self.require_placement(erasure)?;
        self.authorize_root().await?;
        coordinate(self, self.keys, custody, erasure, self.authority.as_ref()).await
    }

    /// Register a worker or copy before admitting access to old plaintext.
    /// The caller may start work only after the original writer grant is rechecked.
    pub async fn register_drain_work(&self, incumbent: Grant, registration: DrainRegistration) -> Result<()> {
        let still_incumbent = || async {
            let route = self.custodian.route().await?;
            if route.writer.grant != Some(incumbent) {
                return Err(invalid("The ordered switch has closed admission to the old custody"));
            }
            Ok(())
        };
        still_incumbent().await?;
        drain::register(self.provider, self.worker.placement(), incumbent.key_generation, registration).await?;
        still_incumbent().await
    }

    /// Returns the immutable custody census for the certified decision.
    pub async fn registered_drain_roster(&self, ready: &CleanRecoverySlots) -> Result<Vec<DrainRegistration>> {
        self.require_placement(ready.prepared().erasure)?;
        drain::freeze(self.provider, ready).await
    }

    /// Retain the authoritative roster after the certified switch closes
    /// admission. The returned journal can receive the registered workers'
    /// authenticated observations without retaining their plaintext.
    pub async fn record_closed_drain(
        &self,
        ready: &CleanRecoverySlots,
        commit: ErasureCommit,
        closed_at_ms: u64,
    ) -> Result<ErasureDrainJournal<'_>> {
        self.require_placement(ready.prepared().erasure)?;
        validate_commit(ready.prepared(), commit, Some(ready.binding()), self.authority.as_ref())?;
        if ready.deadline().is_some_and(|reservation| !matches_reservation(reservation, commit)) {
            return Err(invalid("The durable drain differs from the reserved custody roster"));
        }
        let roster = self.registered_drain_roster(ready).await?;
        ErasureDrainJournal::close(self.provider, commit, closed_at_ms, roster).await
    }

    pub async fn resume_drain(&self, commit: ErasureCommit) -> Result<Option<ErasureDrainJournal<'_>>> {
        validate_commit(commit.prepared, commit, None, self.authority.as_ref())?;
        ErasureDrainJournal::reopen(self.provider, commit).await
    }

    pub async fn confirm_destruction(
        &self,
        custody: &dyn ErasureCustody,
        commit: ErasureCommit,
    ) -> Result<Option<DestructionEvidence>> {
        self.require_placement(commit.prepared.erasure)?;
        let authority = self
            .authority
            .as_ref()
            .ok_or_else(evidence::authority_unavailable)?;
        let Some(journal) = self.resume_drain(commit).await? else { return Ok(None); };
        let Some(joined) = journal.joined().await? else { return Ok(None); };
        let already = journal.destruction().await?;
        let checkpoint = commit.prepared.checkpoint;
        let placement = self.custodian.placement();
        if let Some(receipt) = already.as_ref() {
            authority.verify_destruction(commit, receipt)?;
            if drain::retirement_completed(self.provider, placement, checkpoint.cut.next_lsn).await? {
                return Ok(Some(receipt.clone()));
            }
            let view = self.broker.custody_domain(self.operation.domain, domain::Action::Status).await?;
            if view.last_completed.is_some_and(|completion| completion.operation == self.operation) {
                let domain_drain = drain::admitted_domain(self.provider, commit).await?;
                drain::record_cut(self.provider, commit, "destruction-confirmed", domain_drain).await?;
                return Ok(Some(receipt.clone()));
            }
        }
        let route = self.custodian.route().await?;
        if route.erasure_floor != checkpoint.cut.next_lsn {
            return Err(invalid("The destruction receipt has a different ordered erasure cut"));
        }
        let Some(retiring) = route.erasure_retirement else {
            return Err(invalid("The custodian cannot read the ordered erasure drain"));
        };
        if !decision::ordered_holds_drained(retiring.outstanding_holds) {
            return Ok(None);
        }
        let joined_custody = journal.joined_fingerprint(&joined)?;
        let root_drain = domain::RootDrain {
            original_head: crate::journal::head_identity(placement, checkpoint.cut),
            successor: checkpoint.manifest,
            ordered_switch: commit.switch_evidence,
            joined_custody,
        };
        let signature = custody.sign_root_drain(self.operation, commit, root_drain).await?;
        let view = self.broker.custody_domain(self.operation.domain, domain::Action::RootDrain {
            operation: self.operation, root: placement.deployment,
            evidence: domain::SignedRootDrain { drain: root_drain, signature },
        }).await?;
        let Some(domain_drain) = view.drained else { return Ok(None); };
        drain::record_cut(self.provider, commit, "joined-custody", domain_drain).await?;
        if retiring.unsettled_checkpoints != 0 { return Ok(None); }
        let erasure = commit.prepared.erasure;
        let Some(serving) = serving_disposition(
            self.provider, erasure.placement, commit.prepared.checkpoint.cut.next_lsn, erasure.incumbent.epoch,
        ).await? else { return Ok(None); };
        let closed_at_ms = journal.closed_at_ms();
        let latest_join = joined.iter().map(|work| work.observed_at_ms).max().unwrap_or(closed_at_ms);
        let (journal_key_disposition, journal_key_observed_at_ms) =
            journal.settle_journal_key(self.keys, latest_join).await?;
        let (checkpoint_disposition, checkpoint_observed_at_ms) = drain::checkpoint_dispositions(self.provider, commit).await?;
        let drained = JoinedCustody {
            closed_at_ms, joined, journal_key_disposition, journal_key_observed_at_ms,
            checkpoint_disposition, checkpoint_observed_at_ms,
            serving_disposition: serving.disposition, serving_observed_at_ms: serving.observed_at_ms,
        };
        let receipt = match already {
            Some(ref receipt) => receipt.clone(),
            None => {
                let Some(receipt) = custody.destruction(commit, &drained).await? else { return Ok(None); };
                receipt
            }
        };
        authority.verify_destruction(commit, &receipt)?;
        if receipt.drain.closed_at_ms != drained.closed_at_ms || receipt.drain.joined != drained.joined
            || receipt.journal_key_disposition != drained.journal_key_disposition
            || receipt.checkpoint_disposition != drained.checkpoint_disposition
            || receipt.serving_disposition != drained.serving_disposition
            || receipt.destroyed_at_ms < drained.serving_observed_at_ms
            || receipt.destroyed_at_ms < drained.journal_key_observed_at_ms.max(drained.checkpoint_observed_at_ms)
        {
            return Err(invalid("The destruction receipt differs from the durable custody observations"));
        }
        if already.is_none() { journal.record_destruction(&receipt).await?; }
        let mut provider = domain::RootDestruction {
            domain_drain, provider_disposition: receipt.disposition, signature: Vec::new(),
        };
        provider.signature = custody.sign_root_destruction(self.operation, commit, &provider).await?;
        self.broker.custody_domain(self.operation.domain, domain::Action::RootDestruction {
            operation: self.operation, root: placement.deployment, evidence: provider,
        }).await?;
        drain::record_cut(self.provider, commit, "destruction-confirmed", domain_drain).await?;
        Ok(Some(receipt))
    }
}

struct Selection {
    grant: Option<Grant>,
    head: Cut,
    checkpoint: Checkpoint,
    preparing: Option<CheckpointPreparation>,
}

#[async_trait]
trait ErasureJournal: Sync {
    async fn selection(&self) -> Result<Selection>;
    async fn reopen(&self, prepared: PreparedErasure) -> Result<()>;
    async fn execute(&self, request: &Request) -> Result<Decision>;
}

#[async_trait]
impl ErasureJournal for ErasureCoordinator<'_> {
    async fn selection(&self) -> Result<Selection> {
        let route = self.custodian.route().await?;
        Ok(Selection {
            grant: route.writer.grant,
            head: route.head,
            checkpoint: route.checkpoint,
            preparing: route.preparing,
        })
    }

    async fn reopen(&self, prepared: PreparedErasure) -> Result<()> {
        self.provider
            .reopen_survivor(
                prepared.checkpoint,
                binding(prepared.erasure, prepared.rotation),
            )
            .await
    }

    async fn execute(&self, request: &Request) -> Result<Decision> {
        self.custodian.execute(request).await
    }
}

async fn coordinate(
    journal: &dyn ErasureJournal,
    keys: &dyn KeyService,
    custody: &dyn ErasureCustody,
    erasure: Erasure,
    authority: Option<&CustodyEvidence>,
) -> Result<ErasureCommit> {
    if erasure.operation.is_nil() || erasure.custody_domain.is_nil() {
        return Err(invalid(
            "Erasure requires an exact operation and custody domain",
        ));
    }
    if let ErasureTiming::Fixed { before_ms } = erasure.timing {
        if before_ms == 0 {
            return Err(invalid("A fixed erasure deadline must be positive"));
        }
        if authority.is_none() {
            return Err(evidence::authority_unavailable());
        }
    }
    let old = erasure.incumbent;
    let generation = old
        .key_generation
        .checked_add(1)
        .ok_or_else(|| invalid("The journal key generation is exhausted"))?;
    let epoch = old
        .epoch
        .checked_add(1)
        .ok_or_else(|| invalid("The journal writer epoch is exhausted"))?;

    let prepared = match custody.prepare(erasure).await? {
        ErasureProgress::Prepared(prepared) if prepared.erasure == erasure => prepared,
        ErasureProgress::Committed(commit) if commit.prepared.erasure == erasure => {
            return validate_commit(commit.prepared, commit, None, authority);
        }
        _ => return Err(invalid("Custody returned a different erasure operation")),
    };
    let checkpoint = prepared.checkpoint;
    if checkpoint.root == [0; 32] || checkpoint.manifest == [0; 32] {
        return Err(invalid(
            "Erasure requires an exact staged root and provider manifest",
        ));
    }
    keys.provision_journal_key(JournalKey::new(erasure.placement, generation))
        .await?;

    let selected = journal.selection().await?;
    if selected.grant == Some(old) {
        if selected.head != checkpoint.cut {
            return Err(invalid(
                "The clean checkpoint is outside the quiesced journal head",
            ));
        }
        let descriptor = binding(erasure, prepared.rotation).descriptor()?;
        if !selected.preparing.is_some_and(|entry| {
            entry.erasure
                && entry.source == old
                && entry.base == selected.checkpoint
                && entry.checkpoint == checkpoint.cut
                && entry.root == checkpoint.root
                && entry.custody == checkpoint.custody
                && entry.descriptor == descriptor
        }) {
            return Err(invalid(
                "The clean checkpoint lacks its exact ordered erasure preparation",
            ));
        }
    }
    journal.reopen(prepared).await?;
    let deadline = match erasure.timing {
        ErasureTiming::OperationalTarget => None,
        ErasureTiming::Fixed { before_ms } => {
            let reservation = custody.reserve_deadline(prepared).await?;
            if !decision::fixed_deadline_admits(before_ms, reservation.by_ms) {
                return Err(invalid(
                    "The custody reservation exceeds the requested deadline",
                ));
            }
            authority
                .ok_or_else(evidence::authority_unavailable)?
                .verify_reservation(prepared, reservation)?;
            Some(reservation)
        }
    };

    let request = Request {
        id: prepared.rotation,
        placement: erasure.placement,
        principal: Principal::Custodian,
        command: Command::PublishAndRotate {
            grant: old,
            checkpoint,
        },
    };
    let decision = journal.execute(&request).await?;
    let successor = validate_rotation(
        &request,
        checkpoint,
        Grant {
            enrollment: old.enrollment,
            epoch,
            key_generation: generation,
        },
        decision.receipt,
    )?;
    let ready = require_clean_slots(prepared, successor, decision, deadline)?;
    let binding = ready.binding();
    let commit = custody.commit_and_arm(ready).await?;
    if deadline.is_some_and(|reserved| !matches_reservation(reserved, commit)) {
        return Err(invalid("Custody armed a different reservation"));
    }
    validate_commit(prepared, commit, Some(binding), authority)
}

fn validate_rotation(
    request: &Request,
    checkpoint: Checkpoint,
    expected: Grant,
    receipt: Receipt,
) -> Result<WriterAdmission> {
    if receipt.placement != request.placement
        || receipt.principal != request.principal
        || receipt.request != request.id
    {
        return Err(invalid(
            "The rotation receipt addresses a different request",
        ));
    }
    let Outcome::Granted { grant, recovery } = receipt.outcome.map_err(refused)? else {
        return Err(invalid("The journal returned a different rotation outcome"));
    };
    if grant != expected
        || recovery.owner != expected.enrollment
        || recovery.id != request.id
        || recovery.admitted_at != receipt.position
        || recovery.checkpoint != checkpoint
        || recovery.start != checkpoint.cut
        || recovery.through != checkpoint.cut
    {
        return Err(invalid(
            "The rotation grant and its exact clean recovery basis disagree",
        ));
    }
    Ok(WriterAdmission {
        grant,
        recovery,
        position: receipt.position,
    })
}

fn require_clean_slots(
    prepared: PreparedErasure,
    successor: WriterAdmission,
    decision: Decision,
    deadline: Option<DeadlineReservation>,
) -> Result<CleanRecoverySlots> {
    let slots = decision
        .rotation
        .filter(|proof| covers_clean_rotation(&proof.evidence, decision.receipt))
        .ok_or_else(|| invalid("Both recovery slots require authenticated quorum readback"))?;
    // The installed Logkeeper client authenticates the decision and exact
    // readback in this signature before returning it to custody.
    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    digest.update(b"rtdb.multi/erasure-logkeeper-switch/v2\0");
    digest.update(&binding(prepared.erasure, prepared.rotation).descriptor()?);
    digest.update(&prepared.checkpoint.root);
    digest.update(&prepared.checkpoint.manifest);
    digest.update(&prepared.checkpoint.cut.history);
    digest.update(&prepared.checkpoint.cut.next_lsn.to_be_bytes());
    digest.update(&successor.grant.key_generation.to_be_bytes());
    digest.update(&decision.receipt.position.to_be_bytes());
    let binding = digest.finish().as_ref().try_into().expect("SHA-256 width");
    Ok(CleanRecoverySlots {
        prepared,
        successor,
        receipt: decision.receipt,
        slots,
        binding,
        deadline,
    })
}

fn covers_clean_rotation(slots: &RotationSlots, receipt: Receipt) -> bool {
    let Ok(Outcome::Granted { grant, recovery }) = receipt.outcome else {
        return false;
    };
    slots.group != [0; 32]
        && slots.ballot.round != 0
        && !slots.ballot.nonce.is_nil()
        && slots.prefix != [0; 32]
        && receipt.position != 0
        && slots.position >= receipt.position
        && recovery.admitted_at == receipt.position
        && recovery.start == slots.checkpoint.cut
        && recovery.through == slots.checkpoint.cut
        && decision::at_clean_checkpoint(
            slots.checkpoint == recovery.checkpoint,
            recovery.id == receipt.request,
            recovery.owner == grant.enrollment,
            slots.key_generation,
            grant.key_generation,
        )
        && decision::strict_majority(slots.cohort_size, slots.voters.len())
        && slots.voters.iter().all(|voter| {
            let [a, b] = voter.slots.images;
            !voter.voter.is_nil()
                && decision::mirrors_fence(
                    voter.slots.selected,
                    [a.generation, b.generation],
                    [a.digest != [0; 32], b.digest != [0; 32]],
                    a.digest != b.digest,
                )
        })
        && slots
            .voters
            .windows(2)
            .all(|pair| pair[0].voter < pair[1].voter)
}

fn matches_reservation(reserved: DeadlineReservation, commit: ErasureCommit) -> bool {
    decision::same_reservation(
        reserved.by_ms,
        commit.deadline.at_ms(),
        reserved.retiring_inventory == commit.retiring_inventory,
        reserved.draining_inventory == commit.draining_inventory,
    )
}

fn validate_commit(
    prepared: PreparedErasure,
    commit: ErasureCommit,
    switch: Option<[u8; 32]>,
    authority: Option<&CustodyEvidence>,
) -> Result<ErasureCommit> {
    if !decision::same_attempt(
        commit.prepared == prepared,
        switch.is_none_or(|binding| binding == commit.switch_evidence),
    ) || commit.committed_at_ms == 0
        || commit.retiring_inventory == [0; 32]
        || commit.draining_inventory == [0; 32]
        || commit.switch_evidence == [0; 32]
    {
        return Err(invalid(
            "The erasure commit has a different switch or retiring inventory",
        ));
    }
    match (prepared.erasure.timing, commit.deadline) {
        (ErasureTiming::OperationalTarget, ErasureDeadline::Target { at_ms })
            if decision::operational_target_at(commit.committed_at_ms) == Some(at_ms) => {}
        (ErasureTiming::Fixed { before_ms }, ErasureDeadline::Enforced { by_ms, signature })
            if decision::fixed_commit_admits(before_ms, by_ms, commit.committed_at_ms) =>
        {
            authority
                .ok_or_else(evidence::authority_unavailable)?
                .verify_commit(&commit, &signature)?;
        }
        _ => {
            return Err(invalid(
                "The custody authority returned a different deadline profile",
            ));
        }
    }
    Ok(commit)
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::incorrect("rtdb.multi/erasure-custody", message.to_owned())
}

#[cfg(test)]
mod tests;
