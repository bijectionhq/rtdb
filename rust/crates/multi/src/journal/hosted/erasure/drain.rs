//! Durable readback of the original custody roster and signed drain observations.

use bytes::Bytes;
use ring::{
    digest::{Context, SHA256, digest},
    signature::{ED25519, UnparsedPublicKey},
};
use rtdb_types::Result;
use serde::{Deserialize, Serialize};

use crate::journal::custody::{CheckpointCustody, ErasureRecords};

use super::{CustodyWork, DrainParticipant, DrainedWork, ErasureCommit, drain_inventory, invalid};

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
