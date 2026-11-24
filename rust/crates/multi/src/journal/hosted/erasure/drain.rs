//! Durable readback of the original custody roster and signed drain observations.

use bytes::Bytes;
use ring::{
    digest::{Context, SHA256, digest},
    signature::{ED25519, UnparsedPublicKey},
};
use rtdb_types::Result;
use serde::{Deserialize, Serialize};

use crate::journal::custody::{CheckpointCustody, ErasureRecords};

use super::{
    CleanRecoverySlots, CustodyWork, DrainParticipant, DrainedWork, ErasureCommit, decision,
    drain_inventory, invalid,
};
use ordered_logkeeper::ordered::{Checkpoint, Placement};

/// Binds one named duty to the credential that can attest its terminal drain.
/// A worker can register multiple independently tracked duties.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrainRegistration {
    pub kind: CustodyWork,
    pub duty: [u8; 32],
    pub verification_key: [u8; 32],
}

impl DrainRegistration {
    pub fn participant(&self) -> Result<DrainParticipant> {
        if self.duty == [0; 32] || self.verification_key == [0; 32] {
            return Err(invalid(
                "A drain duty and its signer must have exact identities",
            ));
        }
        let mut hash = Context::new(&SHA256);
        hash.update(b"rtdb.multi/drain-registration/v1\0");
        hash.update(&[self.kind as u8]);
        hash.update(&self.duty);
        hash.update(&self.verification_key);
        Ok(DrainParticipant {
            kind: self.kind,
            identity: hash.finish().as_ref().try_into().expect("SHA-256 width"),
        })
    }
}

pub(super) async fn register(
    provider: &CheckpointCustody,
    placement: Placement,
    generation: u64,
    registration: DrainRegistration,
) -> Result<()> {
    let records = provider.erasure_generation_records(placement, generation)?;
    let participant = registration.participant()?;
    let name = registration_name(participant);
    records.put(&name, encode(&registration)?).await
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisteredRoster {
    switch: [u8; 32],
    registrations: Vec<DrainRegistration>,
}

pub(super) async fn freeze(
    provider: &CheckpointCustody,
    ready: &CleanRecoverySlots,
) -> Result<Vec<DrainRegistration>> {
    use std::collections::BTreeMap;

    let erasure = ready.prepared().erasure;
    let records = provider.erasure_records(erasure.placement, erasure.operation)?;
    let switch = ready.binding();
    if let Some(bytes) = records.get("registered-roster").await? {
        let roster: RegisteredRoster = serde_json::from_slice(&bytes).map_err(serialization)?;
        if roster.switch != switch {
            return Err(invalid(
                "A different ordered decision froze the custody roster",
            ));
        }
        return Ok(roster.registrations);
    }

    // Work is allowed only after its registration is durable and the actor
    // has rechecked the incumbent grant. Any later registration cannot have
    // started work and cannot invalidate this first census.
    let mut registrations = BTreeMap::new();
    let generation =
        provider.erasure_generation_records(erasure.placement, erasure.incumbent.key_generation)?;
    for name in generation.names("registered").await? {
        let bytes = generation
            .get(&name)
            .await?
            .ok_or_else(|| invalid("An admitted drain registration disappeared"))?;
        let registration: DrainRegistration =
            serde_json::from_slice(&bytes).map_err(serialization)?;
        let participant = registration.participant()?;
        if registration_name(participant) != name
            || registrations.insert(participant, registration).is_some()
        {
            return Err(invalid(
                "The drain registration has a different durable identity",
            ));
        }
    }
    let roster = RegisteredRoster {
        switch,
        registrations: registrations.into_values().collect(),
    };
    records.put("registered-roster", encode(&roster)?).await?;
    Ok(roster.registrations)
}

fn registration_name(participant: DrainParticipant) -> String {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    format!(
        "registered/{}/{}",
        participant.kind as u8,
        URL_SAFE_NO_PAD.encode(participant.identity)
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedDrainJoin {
    pub work: DrainedWork,
    pub signature: Vec<u8>,
}

impl SignedDrainJoin {
    pub fn signing_bytes(commit: &ErasureCommit, closed_at_ms: u64, work: DrainedWork) -> Vec<u8> {
        let mut bytes = b"rtdb.multi/drain-worker-joined/v1\0".to_vec();
        bytes.extend_from_slice(digest(&SHA256, &commit.signing_bytes()).as_ref());
        bytes.extend_from_slice(&closed_at_ms.to_be_bytes());
        bytes.push(work.participant.kind as u8);
        bytes.extend_from_slice(&work.participant.identity);
        bytes.extend_from_slice(&work.observed_at_ms.to_be_bytes());
        bytes.extend_from_slice(&work.observation);
        bytes
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Closure {
    commit: [u8; 32],
    closed_at_ms: u64,
    roster: Vec<DrainRegistration>,
}

impl Closure {
    fn registrations(&self, commit: ErasureCommit) -> Result<Vec<DrainParticipant>> {
        let participants = self
            .roster
            .iter()
            .map(DrainRegistration::participant)
            .collect::<Result<Vec<_>>>()?;
        if self.commit.as_slice() != digest(&SHA256, &commit.signing_bytes()).as_ref()
            || self.closed_at_ms < commit.committed_at_ms
            || drain_inventory(participants.iter().copied())? != commit.draining_inventory
        {
            return Err(invalid(
                "The durable drain closure differs from the ordered erasure commit",
            ));
        }
        Ok(participants)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetirementAdmission {
    operation: uuid::Uuid,
    switch: [u8; 32],
    through_epoch: u64,
    domain_drain: [u8; 32],
}

pub(in crate::journal::hosted) async fn retirement_admitted(
    provider: &CheckpointCustody,
    placement: Placement,
    erasure_floor: u64,
) -> Result<bool> {
    recorded_cut(provider, placement, erasure_floor, "joined-custody").await
}

pub(super) async fn retirement_completed(
    provider: &CheckpointCustody,
    placement: Placement,
    erasure_floor: u64,
) -> Result<bool> {
    recorded_cut(provider, placement, erasure_floor, "destruction-confirmed").await
}

async fn recorded_cut(provider: &CheckpointCustody, placement: Placement, floor: u64, name: &str) -> Result<bool> {
    Ok(!crate::reclamation_decision::erasure_requires_custody_join(floor)
        || cut_record(provider, placement, floor, name).await?.is_some())
}

async fn cut_record(provider: &CheckpointCustody, placement: Placement, floor: u64, name: &str) -> Result<Option<RetirementAdmission>> {
    let cut = provider.erasure_cut_records(placement, floor)?;
    let Some(bytes) = cut.get(name).await? else { return Ok(None); };
    let admission: RetirementAdmission = serde_json::from_slice(&bytes).map_err(serialization)?;
    if admission.operation.is_nil() || admission.switch == [0; 32] || admission.through_epoch == 0 || admission.domain_drain == [0; 32] {
        return Err(invalid("The recorded custody decision has no exact identity"));
    }
    Ok(Some(admission))
}

pub(in crate::journal::hosted) async fn serving_retirement_epoch(
    provider: &CheckpointCustody, placement: Placement, floor: u64,
) -> Result<Option<u64>> {
    Ok(cut_record(provider, placement, floor, "joined-custody").await?.map(|admission| admission.through_epoch))
}

pub(super) async fn admitted_domain(
    provider: &CheckpointCustody, commit: ErasureCommit,
) -> Result<[u8; 32]> {
    let erasure = commit.prepared.erasure;
    let admission = cut_record(provider, erasure.placement, commit.prepared.checkpoint.cut.next_lsn, "joined-custody").await?
        .ok_or_else(|| invalid("The domain drain has no durable admission record"))?;
    if admission.operation != erasure.operation || admission.switch != commit.switch_evidence {
        return Err(invalid("The domain drain belongs to a different ordered switch"));
    }
    Ok(admission.domain_drain)
}

pub(super) async fn record_cut(provider: &CheckpointCustody, commit: ErasureCommit, name: &str, domain_drain: [u8; 32]) -> Result<()> {
    let erasure = commit.prepared.erasure;
    provider.erasure_cut_records(erasure.placement, commit.prepared.checkpoint.cut.next_lsn)?
        .put(name, encode(&RetirementAdmission {
            operation: erasure.operation,
            switch: commit.switch_evidence,
            through_epoch: erasure.incumbent.epoch,
            domain_drain,
        })?).await
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal::hosted) struct ServingDisposition {
    through_epoch: u64,
    pub(in crate::journal::hosted) disposition: [u8; 32],
    pub(in crate::journal::hosted) observed_at_ms: u64,
}

fn read_serving(bytes: Bytes, through_epoch: u64) -> Result<ServingDisposition> {
    let evidence: ServingDisposition = serde_json::from_slice(&bytes).map_err(serialization)?;
    if evidence.through_epoch != through_epoch || evidence.disposition == [0; 32] {
        return Err(invalid("The serving disposition belongs to different writer grants"));
    }
    Ok(evidence)
}

pub(in crate::journal::hosted) async fn serving_disposition(
    provider: &CheckpointCustody, placement: Placement, floor: u64, through_epoch: u64,
) -> Result<Option<ServingDisposition>> {
    provider.erasure_cut_records(placement, floor)?.get("serving").await?
        .map(|bytes| read_serving(bytes, through_epoch)).transpose()
}

pub(in crate::journal::hosted) async fn record_serving_disposition(
    provider: &CheckpointCustody, placement: Placement, floor: u64, through_epoch: u64, disposition: [u8; 32],
) -> Result<()> {
    let observed_at_ms = u64::try_from(rtdb_directory::now_ms()).map_err(|_| invalid("The custody clock is invalid"))?;
    provider.erasure_cut_records(placement, floor)?
        .put("serving", encode(&ServingDisposition { through_epoch, disposition, observed_at_ms })?).await
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointDisposition {
    checkpoint: Checkpoint,
    disposition: [u8; 32],
    observed_at_ms: u64,
}

fn checkpoint_record(checkpoint: Checkpoint) -> String {
    format!("checkpoints/{}", checkpoint.custody.allocation)
}

pub(in crate::journal::hosted) async fn record_checkpoint_disposition(
    provider: &CheckpointCustody,
    placement: Placement,
    erasure_floor: u64,
    checkpoint: Checkpoint,
    disposition: [u8; 32],
) -> Result<()> {
    let records = provider.erasure_cut_records(placement, erasure_floor)?;
    let name = checkpoint_record(checkpoint);
    if checkpoint.cut.next_lsn >= erasure_floor || disposition == [0; 32] {
        return Err(invalid("The checkpoint is outside the retiring key inventory"));
    }
    if let Some(existing) = records.get(&name).await? {
        let prior: CheckpointDisposition = serde_json::from_slice(&existing).map_err(serialization)?;
        return if prior.checkpoint == checkpoint && prior.disposition == disposition {
            Ok(())
        } else {
            Err(invalid("The checkpoint already has a different provider disposition"))
        };
    }
    let observed_at_ms = u64::try_from(rtdb_directory::now_ms()).map_err(|_| invalid("The custody clock is invalid"))?;
    records.put(&name, encode(&CheckpointDisposition { checkpoint, disposition, observed_at_ms })?).await
}

pub(super) async fn checkpoint_dispositions(
    provider: &CheckpointCustody,
    commit: ErasureCommit,
) -> Result<([u8; 32], u64)> {
    let floor = commit.prepared.checkpoint.cut.next_lsn;
    let records = provider.erasure_cut_records(commit.prepared.erasure.placement, floor)?;
    let mut names = records.names("checkpoints").await?;
    names.sort_unstable();
    let mut hash = Context::new(&SHA256);
    hash.update(b"rtdb.multi/erasure-checkpoint-dispositions/v1\0");
    hash.update(&commit.switch_evidence);
    let mut observed_at_ms = commit.committed_at_ms;
    for name in names {
        let bytes = records.get(&name).await?.ok_or_else(|| invalid("The checkpoint disposition disappeared"))?;
        let evidence: CheckpointDisposition = serde_json::from_slice(&bytes).map_err(serialization)?;
        if checkpoint_record(evidence.checkpoint) != name || evidence.checkpoint.cut.next_lsn >= floor
            || evidence.disposition == [0; 32] || evidence.observed_at_ms < commit.committed_at_ms
        {
            return Err(invalid("The checkpoint disposition does not belong to the erasure cut"));
        }
        hash.update(digest(&SHA256, &bytes).as_ref());
        observed_at_ms = observed_at_ms.max(evidence.observed_at_ms);
    }
    Ok((hash.finish().as_ref().try_into().expect("SHA-256 width"), observed_at_ms))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalKeyWithdrawal {
    namespace: [u8; 32],
    key: String,
    operation_authority: bool,
    observed_at_ms: u64,
}

pub struct ErasureDrainJournal<'a> {
    commit: ErasureCommit,
    records: ErasureRecords<'a>,
    closure: Closure,
    participants: Vec<DrainParticipant>,
}

impl<'a> ErasureDrainJournal<'a> {
    fn records(
        provider: &'a CheckpointCustody,
        commit: ErasureCommit,
    ) -> Result<ErasureRecords<'a>> {
        let erasure = commit.prepared.erasure;
        provider.erasure_records(erasure.placement, erasure.operation)
    }

    pub(super) async fn close(
        provider: &'a CheckpointCustody,
        commit: ErasureCommit,
        closed_at_ms: u64,
        roster: Vec<DrainRegistration>,
    ) -> Result<Self> {
        let records = Self::records(provider, commit)?;
        let closure = Closure {
            commit: digest(&SHA256, &commit.signing_bytes())
                .as_ref()
                .try_into()
                .expect("SHA-256 width"),
            closed_at_ms,
            roster,
        };
        let participants = closure.registrations(commit)?;
        records.put("closed", encode(&closure)?).await?;
        Ok(Self {
            commit,
            records,
            closure,
            participants,
        })
    }

    pub(super) async fn reopen(
        provider: &'a CheckpointCustody,
        commit: ErasureCommit,
    ) -> Result<Option<Self>> {
        let records = Self::records(provider, commit)?;
        let Some(bytes) = records.get("closed").await? else {
            return Ok(None);
        };
        let closure: Closure = serde_json::from_slice(&bytes).map_err(serialization)?;
        let participants = closure.registrations(commit)?;
        Ok(Some(Self {
            commit,
            records,
            closure,
            participants,
        }))
    }

    pub fn closed_at_ms(&self) -> u64 {
        self.closure.closed_at_ms
    }

    pub async fn record(&self, join: &SignedDrainJoin) -> Result<()> {
        let index = self
            .participants
            .binary_search(&join.work.participant)
            .map_err(|_| invalid("The drain observation has no duty in the closed roster"))?;
        self.verify(index, join)?;
        self.records
            .put(&format!("joined/{index}"), encode(join)?)
            .await
    }

    /// Reads back every registered duty. A missing observation leaves the
    /// drain pending even if the other workers have joined.
    pub async fn joined(&self) -> Result<Option<Vec<DrainedWork>>> {
        let mut joined = Vec::with_capacity(self.participants.len());
        for index in 0..self.participants.len() {
            let Some(bytes) = self.records.get(&format!("joined/{index}")).await? else {
                return Ok(None);
            };
            let join: SignedDrainJoin = serde_json::from_slice(&bytes).map_err(serialization)?;
            self.verify(index, &join)?;
            joined.push(join.work);
        }
        Ok(Some(joined))
    }

    pub(super) fn joined_fingerprint(&self, joined: &[DrainedWork]) -> Result<[u8; 32]> {
        let mut hash = Context::new(&SHA256);
        hash.update(b"rtdb.custody/installed-root-joined/v1\0");
        hash.update(&self.closure.commit);
        hash.update(&encode(&(self.closure.closed_at_ms, joined))?);
        Ok(hash.finish().as_ref().try_into().expect("SHA-256 width"))
    }

    /// Settle only the incumbent journal key selected by this switch.
    /// Provider success is retained before any aggregate destruction can be attested.
    pub(super) async fn settle_journal_key(
        &self,
        keys: &dyn crate::keys::KeyService,
        latest_join: u64,
    ) -> Result<([u8; 32], u64)> {
        use crate::keys::{ArchiveKeyWithdrawal, JournalKey};

        let erasure = self.commit.prepared.erasure;
        let namespace = keys.namespace_id().await?;
        let key = JournalKey::new(erasure.placement, erasure.incumbent.key_generation).id();
        let record = match self.records.get("journal-key").await? {
            Some(bytes) => {
                serde_json::from_slice::<JournalKeyWithdrawal>(&bytes).map_err(serialization)?
            }
            None => {
                let before = u64::try_from(rtdb_directory::now_ms())
                    .map_err(|_| invalid("The custody clock is invalid"))?;
                if !decision::withdrawal_follows_join(before, latest_join) {
                    return Err(invalid(
                        "The custody clock has not reached the joined observation",
                    ));
                }
                let operation_authority = match keys.withdraw_archive_key(&key).await? {
                    ArchiveKeyWithdrawal::Vault => false,
                    ArchiveKeyWithdrawal::OperationAuthority => true,
                };
                let observed_at_ms = u64::try_from(rtdb_directory::now_ms())
                    .map_err(|_| invalid("The custody clock is invalid"))?;
                if observed_at_ms < before {
                    return Err(invalid(
                        "The custody clock regressed during provider settlement",
                    ));
                }
                let record = JournalKeyWithdrawal {
                    namespace,
                    key: key.clone(),
                    operation_authority,
                    observed_at_ms,
                };
                self.records.put("journal-key", encode(&record)?).await?;
                record
            }
        };
        if record.namespace != namespace || record.key != key || record.observed_at_ms < latest_join
        {
            return Err(invalid(
                "The journal key disposition belongs to different custody",
            ));
        }
        let mut hash = Context::new(&SHA256);
        hash.update(b"rtdb.multi/erasure-journal-disposition/v1\0");
        hash.update(&self.closure.commit);
        hash.update(&encode(&record)?);
        Ok((
            hash.finish().as_ref().try_into().expect("SHA-256 width"),
            record.observed_at_ms,
        ))
    }

    pub(super) async fn destruction(&self) -> Result<Option<super::DestructionEvidence>> {
        self.records
            .get("destroyed")
            .await?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(serialization))
            .transpose()
    }

    pub(super) async fn record_destruction(
        &self,
        receipt: &super::DestructionEvidence,
    ) -> Result<()> {
        self.records.put("destroyed", encode(receipt)?).await
    }

    fn verify(&self, index: usize, join: &SignedDrainJoin) -> Result<()> {
        let work = join.work;
        if work.participant != self.participants[index]
            || work.observed_at_ms < self.closure.closed_at_ms
            || work.observation == [0; 32]
        {
            return Err(invalid(
                "The observation does not establish a post-closure drain of this duty",
            ));
        }
        UnparsedPublicKey::new(&ED25519, self.closure.roster[index].verification_key)
            .verify(
                &SignedDrainJoin::signing_bytes(&self.commit, self.closure.closed_at_ms, work),
                &join.signature,
            )
            .map_err(|_| {
                invalid("The original registered worker did not sign the drain observation")
            })
    }
}

fn encode(value: &impl Serialize) -> Result<Bytes> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(serialization)
}

fn serialization(error: serde_json::Error) -> rtdb_types::Anomaly {
    invalid(&error.to_string())
}
