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
    CleanRecoverySlots, CustodyWork, DrainParticipant, DrainedWork, ErasureCommit, drain_inventory,
    invalid,
};
use ordered_logkeeper::ordered::Placement;

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
}

pub(in crate::journal::hosted) async fn retirement_admitted(
    provider: &CheckpointCustody,
    placement: Placement,
    erasure_floor: u64,
) -> Result<bool> {
    if erasure_floor == 0 {
        return Ok(true);
    }
    let cut = provider.erasure_cut_records(placement, erasure_floor)?;
    let Some(bytes) = cut.get("joined-custody").await? else { return Ok(false); };
    let admission: RetirementAdmission = serde_json::from_slice(&bytes).map_err(serialization)?;
    Ok(!admission.operation.is_nil() && admission.switch != [0; 32])
}

pub(super) async fn admit_retirement(
    provider: &CheckpointCustody,
    commit: ErasureCommit,
) -> Result<()> {
    let erasure = commit.prepared.erasure;
    provider.erasure_cut_records(erasure.placement, commit.prepared.checkpoint.cut.next_lsn)?
        .put("joined-custody", encode(&RetirementAdmission {
            operation: erasure.operation,
            switch: commit.switch_evidence,
        })?).await
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
                if before < latest_join {
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
