//! Coordination of the exact-head rotation and the erasure commit.

use async_trait::async_trait;
use ordered_logkeeper::ordered::{
    Checkpoint, CheckpointPreparation, Command, Cut, Grant, Outcome, Placement, Principal, Receipt,
    Request, RequestId,
    service::wire::{Decision, SignedRotationSlots},
};
use rtdb_types::{Anomaly, Result};
use uuid::Uuid;

use crate::journal::{
    Authority, InstalledJournal, WriterAdmission, custody::CheckpointCustody, refused,
};
use crate::keys::{JournalKey, KeyService};

const MAX_REVOCATION_DELAY_MS: u64 = 120_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Erasure {
    pub placement: Placement,
    pub operation: Uuid,
    pub custody_domain: Uuid,
    pub incumbent: Grant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedErasure {
    pub erasure: Erasure,
    pub checkpoint: Checkpoint,
    pub rotation: RequestId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ErasureCommit {
    pub prepared: PreparedErasure,
    pub committed_at_ms: u64,
    pub deadline_ms: u64,
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
}

impl CleanRecoverySlots {
    pub fn prepared(&self) -> PreparedErasure {
        self.prepared
    }

    pub fn successor(&self) -> WriterAdmission {
        self.successor
    }

    pub fn receipt(&self) -> Receipt {
        self.receipt
    }

    pub fn slots(&self) -> &SignedRotationSlots {
        &self.slots
    }
}

#[async_trait]
pub trait ErasureCustody: Send + Sync {
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
}

pub struct ErasureCoordinator<'a> {
    worker: &'a InstalledJournal,
    custodian: &'a InstalledJournal,
    provider: &'a CheckpointCustody,
    keys: &'a dyn KeyService,
}

impl<'a> ErasureCoordinator<'a> {
    pub fn new(
        worker: &'a InstalledJournal,
        custodian: &'a InstalledJournal,
        provider: &'a CheckpointCustody,
        keys: &'a dyn KeyService,
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
        })
    }

    pub async fn commit(
        &self,
        custody: &dyn ErasureCustody,
        erasure: Erasure,
    ) -> Result<ErasureCommit> {
        if erasure.placement != self.worker.placement()
            || !matches!(self.worker.authority(), Authority::Worker { enrollment } if enrollment == erasure.incumbent.enrollment)
        {
            return Err(invalid(
                "Erasure must address the installed credential of the incumbent",
            ));
        }
        coordinate(self, self.keys, custody, erasure).await
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
    async fn reopen(&self, checkpoint: Checkpoint) -> Result<()>;
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

    async fn reopen(&self, checkpoint: Checkpoint) -> Result<()> {
        self.provider.open(checkpoint).await?;
        Ok(())
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
) -> Result<ErasureCommit> {
    if erasure.operation.is_nil() || erasure.custody_domain.is_nil() {
        return Err(invalid(
            "Erasure requires an exact operation and custody domain",
        ));
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
            return validate_commit(commit.prepared, commit);
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
        if !selected.preparing.is_some_and(|entry| {
            entry.erasure
                && entry.source == old
                && entry.base == selected.checkpoint
                && entry.checkpoint == checkpoint.cut
                && entry.root == checkpoint.root
                && entry.custody == checkpoint.custody
        }) {
            return Err(invalid(
                "The clean checkpoint lacks its exact ordered erasure preparation",
            ));
        }
    }
    journal.reopen(checkpoint).await?;

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
    let ready = require_clean_slots(prepared, successor, decision)?;
    validate_commit(prepared, custody.commit_and_arm(ready).await?)
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
) -> Result<CleanRecoverySlots> {
    let slots = decision
        .rotation
        .filter(|proof| proof.evidence.covers(decision.receipt))
        .ok_or_else(|| invalid("Both recovery slots require authenticated quorum readback"))?;
    Ok(CleanRecoverySlots {
        prepared,
        successor,
        receipt: decision.receipt,
        slots,
    })
}

fn validate_commit(prepared: PreparedErasure, commit: ErasureCommit) -> Result<ErasureCommit> {
    if commit.prepared != prepared
        || !matches!(
            commit.deadline_ms.checked_sub(commit.committed_at_ms),
            Some(1..=MAX_REVOCATION_DELAY_MS)
        )
    {
        return Err(invalid(
            "The erasure commit has a different identity or an invalid deadline",
        ));
    }
    Ok(commit)
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::incorrect("rtdb.multi/erasure-custody", message.to_owned())
}

#[cfg(test)]
mod tests;
