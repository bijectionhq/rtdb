//! Durable evidence for the installed provider's operational-target profile.

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use ordered_logkeeper::ordered::Checkpoint;
use ring::{digest::{Context, SHA256}, signature::{Ed25519KeyPair, KeyPair}};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};

use super::{
    CleanRecoverySlots, CustodyDrain, DestructionEvidence, Erasure, ErasureCommit, ErasureCustody,
    ErasureDeadline, ErasureProgress, ErasureTiming, JoinedCustody, PreparedErasure, decision,
    domain, drain, drain_inventory, invalid,
};
use crate::journal::custody::CheckpointCustody;

pub(in crate::journal::hosted) struct Signer(Ed25519KeyPair);

impl Signer {
    pub(in crate::journal::hosted) fn open(path: &Path) -> Result<Self> {
        let mut bytes = std::fs::read(path)
            .map_err(|_| invalid("The installed erasure signer cannot be read"))?;
        let result = Ed25519KeyPair::from_pkcs8(&bytes)
            .map_err(|_| invalid("The installed erasure signer is invalid"));
        bytes.fill(0);
        result.map(Self)
    }

    pub(in crate::journal::hosted) fn public_key(&self) -> [u8; 32] {
        self.0.public_key().as_ref().try_into().expect("Ed25519 key width")
    }

    pub(in crate::journal::hosted) fn sign(&self, statement: &[u8]) -> [u8; 64] {
        self.0.sign(statement).as_ref().try_into().expect("Ed25519 signature width")
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal::hosted) struct Staged {
    pub prepared: PreparedErasure,
    pub base: Checkpoint,
    pub selection: [u8; 32],
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CommitRecord {
    at_ms: u64,
    retiring: [u8; 32],
    draining: [u8; 32],
    switch: [u8; 32],
}

impl CommitRecord {
    fn read(self, prepared: PreparedErasure) -> Result<ErasureCommit> {
        let at_ms = decision::operational_target_at(self.at_ms)
            .ok_or_else(|| invalid("The operational erasure clock overflowed"))?;
        Ok(ErasureCommit {
            prepared, committed_at_ms: self.at_ms, deadline: ErasureDeadline::Target { at_ms },
            retiring_inventory: self.retiring, draining_inventory: self.draining, switch_evidence: self.switch,
        })
    }
}

pub(in crate::journal::hosted) struct Installed<'a> {
    pub provider: &'a CheckpointCustody,
    pub signer: &'a Signer,
    pub staged: Staged,
}

impl<'a> Installed<'a> {
    pub(in crate::journal::hosted) async fn remember(
        provider: &CheckpointCustody, staged: Staged,
    ) -> Result<()> {
        let erasure = staged.prepared.erasure;
        provider.erasure_records(erasure.placement, erasure.operation)?
            .put("installed-staged", encode(&staged)?).await
    }

    pub(in crate::journal::hosted) async fn load(
        provider: &'a CheckpointCustody, signer: &'a Signer,
        placement: ordered_logkeeper::ordered::Placement, operation: domain::Operation,
    ) -> Result<Option<Self>> {
        let records = provider.erasure_records(placement, operation.erase)?;
        let Some(bytes) = records.get("installed-staged").await? else { return Ok(None); };
        let staged: Staged = serde_json::from_slice(&bytes).map_err(serialization)?;
        let erasure = staged.prepared.erasure;
        if erasure.placement != placement || erasure.operation != operation.erase
            || erasure.custody_domain != operation.domain || erasure.timing != ErasureTiming::OperationalTarget
        {
            return Err(invalid("The staged operation belongs to another installed custody profile"));
        }
        Ok(Some(Self { provider, signer, staged }))
    }

    async fn committed(&self) -> Result<Option<ErasureCommit>> {
        let prepared = self.staged.prepared;
        let records = self.provider.erasure_records(prepared.erasure.placement, prepared.erasure.operation)?;
        let Some(bytes) = records.get("installed-commit").await? else { return Ok(None); };
        let record: CommitRecord = serde_json::from_slice(&bytes).map_err(serialization)?;
        record.read(prepared).map(Some)
    }

    fn check_commit(&self, commit: ErasureCommit, operation: domain::Operation) -> Result<()> {
        let erasure = self.staged.prepared.erasure;
        if commit.prepared != self.staged.prepared
            || operation.erase != erasure.operation || operation.domain != erasure.custody_domain
        {
            return Err(invalid("The evidence belongs to a different installed erasure"));
        }
        Ok(())
    }
}

#[async_trait]
impl ErasureCustody for Installed<'_> {
    async fn prepare(&self, erasure: Erasure) -> Result<ErasureProgress> {
        if erasure != self.staged.prepared.erasure {
            return Err(invalid("The installed staging addresses a different erasure"));
        }
        Ok(match self.committed().await? {
            Some(commit) => ErasureProgress::Committed(commit),
            None => ErasureProgress::Prepared(self.staged.prepared),
        })
    }

    async fn commit_and_arm(&self, ready: CleanRecoverySlots) -> Result<ErasureCommit> {
        let prepared = ready.prepared();
        if prepared != self.staged.prepared
            || !decision::installed_admits(
                prepared.erasure.timing == ErasureTiming::OperationalTarget,
                ready.deadline().is_some(),
            )
        {
            return Err(invalid("This installed provider uses the operational erasure target"));
        }
        if let Some(commit) = self.committed().await? {
            if commit.switch_evidence != ready.binding() {
                return Err(invalid("The authenticated rotation differs from the installed commit"));
            }
            return Ok(commit);
        }
        let erasure = prepared.erasure;
        let records = self.provider.erasure_records(erasure.placement, erasure.operation)?;
        let at_ms = match records.get("installed-switch-clock").await? {
            Some(bytes) => serde_json::from_slice(&bytes).map_err(serialization)?,
            None => {
                let now = u64::try_from(rtdb_directory::now_ms()).map_err(|_| invalid("The custody clock is invalid"))?;
                records.put("installed-switch-clock", encode(&now)?).await?;
                now
            }
        };
        let roster = drain::freeze(self.provider, &ready).await?;
        let participants = roster.iter().map(super::DrainRegistration::participant)
            .collect::<Result<Vec<_>>>()?;
        let mut retiring = Context::new(&SHA256);
        retiring.update(b"rtdb.custody/installed-retirement-selection/v1\0");
        retiring.update(&encode(&self.staged)?);
        let record = CommitRecord {
            at_ms, retiring: retiring.finish().as_ref().try_into().expect("SHA-256 width"),
            draining: drain_inventory(participants)?, switch: ready.binding(),
        };
        let commit = record.read(prepared)?;
        super::ErasureDrainJournal::close(self.provider, commit, at_ms, roster).await?;
        records.put("installed-commit", encode(&record)?).await?;
        Ok(commit)
    }

    async fn sign_root_drain(
        &self, operation: domain::Operation, commit: ErasureCommit, drain: domain::RootDrain,
    ) -> Result<Vec<u8>> {
        self.check_commit(commit, operation)?;
        let placement = commit.prepared.erasure.placement;
        let bytes = drain.signing_bytes(operation, placement.deployment, domain::installed_history(placement)?)?;
        Ok(self.signer.sign(&bytes).to_vec())
    }

    async fn sign_root_destruction(
        &self, operation: domain::Operation, commit: ErasureCommit, result: &domain::RootDestruction,
    ) -> Result<Vec<u8>> {
        self.check_commit(commit, operation)?;
        let placement = commit.prepared.erasure.placement;
        let bytes = result.signing_bytes(operation, placement.deployment, domain::installed_history(placement)?)?;
        Ok(self.signer.sign(&bytes).to_vec())
    }

    async fn destruction(
        &self, commit: ErasureCommit, observed: &JoinedCustody,
    ) -> Result<Option<DestructionEvidence>> {
        let mut drain = CustodyDrain {
            closed_at_ms: observed.closed_at_ms(), joined: observed.observations().to_vec(), signature: [0; 64],
        };
        drain.signature = self.signer.sign(&drain.signing_bytes(&commit));
        let destroyed_at_ms = [
            observed.closed_at_ms(), observed.journal_key_observed_at_ms(), observed.checkpoint_observed_at_ms(),
            observed.serving_observed_at_ms(),
        ].into_iter().chain(observed.observations().iter().map(|work| work.observed_at_ms))
            .max().expect("Provider observations");
        let journal = observed.journal_key_disposition();
        let checkpoint = observed.checkpoint_disposition();
        let serving = observed.serving_disposition();
        let mut aggregate = Context::new(&SHA256);
        aggregate.update(b"rtdb.custody/installed-provider-disposition/v1\0");
        aggregate.update(&commit.switch_evidence);
        aggregate.update(&journal);
        aggregate.update(&checkpoint);
        aggregate.update(&serving);
        let mut evidence = DestructionEvidence {
            drain, destroyed_at_ms, disposition: aggregate.finish().as_ref().try_into().expect("SHA-256 width"),
            journal_key_disposition: journal, checkpoint_disposition: checkpoint, serving_disposition: serving,
            signature: [0; 64],
        };
        evidence.signature = self.signer.sign(&evidence.signing_bytes(&commit));
        Ok(Some(evidence))
    }
}

fn encode<T: Serialize>(record: &T) -> Result<Bytes> {
    serde_json::to_vec(record).map(Bytes::from).map_err(serialization)
}

fn serialization(error: serde_json::Error) -> Anomaly {
    Anomaly::fault("rtdb.multi/installed-erasure-evidence", error.to_string())
}
