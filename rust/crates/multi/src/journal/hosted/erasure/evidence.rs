//! Evidence issued by the original custody authority for one ordered switch.

use ring::{
    digest::{Context, SHA256, digest},
    signature::{ED25519, UnparsedPublicKey},
};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};

use super::{ErasureCommit, PreparedErasure, decision, invalid};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum ErasureTiming {
    OperationalTarget,
    Fixed { before_ms: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErasureDeadline {
    Target { at_ms: u64 },
    Enforced { by_ms: u64, signature: [u8; 64] },
}

impl ErasureDeadline {
    pub fn at_ms(self) -> u64 {
        match self {
            Self::Target { at_ms } => at_ms,
            Self::Enforced { by_ms, .. } => by_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DestructionEvidence {
    pub drain: CustodyDrain,
    pub destroyed_at_ms: u64,
    pub disposition: [u8; 32],
    pub journal_key_disposition: [u8; 32],
    pub checkpoint_disposition: [u8; 32],
    pub serving_disposition: [u8; 32],
    #[serde(with = "signature_bytes")]
    pub signature: [u8; 64],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeadlineReservation {
    pub by_ms: u64,
    pub retiring_inventory: [u8; 32],
    pub draining_inventory: [u8; 32],
    pub signature: [u8; 64],
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[repr(u8)]
pub enum CustodyWork {
    Plaintext = 1,
    Egress = 2,
    Copy = 3,
}

/// An authority registered one custody obligation under this identity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct DrainParticipant {
    pub kind: CustodyWork,
    pub identity: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct DrainedWork {
    pub participant: DrainParticipant,
    pub observed_at_ms: u64,
    pub observation: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CustodyDrain {
    pub closed_at_ms: u64,
    pub joined: Vec<DrainedWork>,
    #[serde(with = "signature_bytes")]
    pub signature: [u8; 64],
}

/// Commits the custody roster in strict participant order. The empty roster
/// has its own digest and still requires a signed admission closure.
pub fn drain_inventory(
    participants: impl IntoIterator<Item = DrainParticipant>,
) -> Result<[u8; 32]> {
    let mut hash = Context::new(&SHA256);
    hash.update(b"rtdb.multi/custody-drain-inventory/v1\0");
    let mut previous = None;
    for participant in participants {
        if participant.identity == [0; 32] || previous.is_some_and(|prior| prior >= participant) {
            return Err(invalid("The drain roster is not an exact ordered set"));
        }
        hash.update(&[participant.kind as u8]);
        hash.update(&participant.identity);
        previous = Some(participant);
    }
    Ok(hash.finish().as_ref().try_into().expect("SHA-256 width"))
}

impl CustodyDrain {
    pub fn signing_bytes(&self, commit: &ErasureCommit) -> Vec<u8> {
        let mut bytes = b"rtdb.multi/custody-drained/v1\0".to_vec();
        bytes.extend_from_slice(digest(&SHA256, &commit.signing_bytes()).as_ref());
        bytes.extend_from_slice(&self.closed_at_ms.to_be_bytes());
        for work in &self.joined {
            bytes.push(work.participant.kind as u8);
            bytes.extend_from_slice(&work.participant.identity);
            bytes.extend_from_slice(&work.observed_at_ms.to_be_bytes());
            bytes.extend_from_slice(&work.observation);
        }
        bytes
    }
}

fn append_preparation(bytes: &mut Vec<u8>, prepared: PreparedErasure) {
    let erasure = prepared.erasure;
    let checkpoint = prepared.checkpoint;
    for id in [
        erasure.placement.deployment,
        erasure.placement.incarnation,
        erasure.placement.group,
        erasure.operation,
        erasure.custody_domain,
        erasure.incumbent.enrollment.worker,
        prepared.rotation.nonce,
        checkpoint.custody.allocation,
    ] {
        bytes.extend_from_slice(id.as_bytes());
    }
    for value in [
        erasure.placement.generation,
        erasure.incumbent.enrollment.serial,
        erasure.incumbent.epoch,
        erasure.incumbent.key_generation,
        prepared.rotation.expires_at,
        checkpoint.cut.next_lsn,
        checkpoint.custody.revision,
    ] {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
    match erasure.timing {
        ErasureTiming::OperationalTarget => bytes.push(0),
        ErasureTiming::Fixed { before_ms } => {
            bytes.push(1);
            bytes.extend_from_slice(&before_ms.to_be_bytes());
        }
    }
    for identity in [
        checkpoint.cut.history,
        checkpoint.root,
        checkpoint.manifest,
        checkpoint.custody.provider,
    ] {
        bytes.extend_from_slice(&identity);
    }
}

impl DeadlineReservation {
    pub fn signing_bytes(&self, prepared: PreparedErasure) -> Vec<u8> {
        let mut bytes = b"rtdb.multi/erasure-reservation/v2\0".to_vec();
        append_preparation(&mut bytes, prepared);
        bytes.extend_from_slice(&self.by_ms.to_be_bytes());
        bytes.extend_from_slice(&self.retiring_inventory);
        bytes.extend_from_slice(&self.draining_inventory);
        bytes
    }
}

impl ErasureCommit {
    /// The authority signs after irrevocably arming the reserved fixed policy
    /// for the authenticated ordered switch.
    pub fn signing_bytes(&self) -> Vec<u8> {
        let mut bytes = b"rtdb.multi/erasure-commit/v2\0".to_vec();
        append_preparation(&mut bytes, self.prepared);
        bytes.extend_from_slice(&self.committed_at_ms.to_be_bytes());
        bytes.extend_from_slice(&self.deadline.at_ms().to_be_bytes());
        bytes.push(u8::from(matches!(
            self.deadline,
            ErasureDeadline::Enforced { .. }
        )));
        bytes.extend_from_slice(&self.retiring_inventory);
        bytes.extend_from_slice(&self.draining_inventory);
        bytes.extend_from_slice(&self.switch_evidence);
        bytes
    }
}

impl DestructionEvidence {
    pub fn signing_bytes(&self, commit: &ErasureCommit) -> Vec<u8> {
        let mut bytes = b"rtdb.multi/erasure-destruction/v4\0".to_vec();
        bytes.extend_from_slice(digest(&SHA256, &commit.signing_bytes()).as_ref());
        bytes.extend_from_slice(digest(&SHA256, &self.drain.signing_bytes(commit)).as_ref());
        bytes.extend_from_slice(&self.drain.signature);
        bytes.extend_from_slice(&self.destroyed_at_ms.to_be_bytes());
        bytes.extend_from_slice(&self.disposition);
        bytes.extend_from_slice(&self.journal_key_disposition);
        bytes.extend_from_slice(&self.checkpoint_disposition);
        bytes.extend_from_slice(&self.serving_disposition);
        bytes
    }

    /// An operational target supplies no deadline obligation.
    pub fn fixed_deadline_met(&self, commit: &ErasureCommit) -> Option<bool> {
        let enforced_deadline = match commit.deadline {
            ErasureDeadline::Target { .. } => None,
            ErasureDeadline::Enforced { by_ms, .. } => Some(by_ms),
        };
        decision::completion_status(enforced_deadline, self.destroyed_at_ms)
    }
}

pub(super) struct CustodyEvidence([u8; 32]);

impl CustodyEvidence {
    pub(super) fn new(key: [u8; 32]) -> Result<Self> {
        if key == [0; 32] {
            return Err(invalid("The custody evidence signer has no identity"));
        }
        Ok(Self(key))
    }

    pub(super) fn verify_commit(&self, commit: &ErasureCommit, signature: &[u8; 64]) -> Result<()> {
        self.verify(&commit.signing_bytes(), signature)
    }

    pub(super) fn verify_reservation(
        &self,
        prepared: PreparedErasure,
        reservation: DeadlineReservation,
    ) -> Result<()> {
        if reservation.retiring_inventory == [0; 32] || reservation.draining_inventory == [0; 32] {
            return Err(invalid(
                "The deadline requires exact retiring-key and custody-work inventories",
            ));
        }
        self.verify(&reservation.signing_bytes(prepared), &reservation.signature)
    }

    pub(super) fn verify_destruction(
        &self,
        commit: ErasureCommit,
        receipt: &DestructionEvidence,
    ) -> Result<()> {
        let drain = &receipt.drain;
        if !decision::destruction_follows_drain(
            commit.committed_at_ms,
            drain.closed_at_ms,
            receipt.destroyed_at_ms,
        )
            || receipt.disposition == [0; 32]
            || receipt.journal_key_disposition == [0; 32]
            || receipt.checkpoint_disposition == [0; 32]
            || receipt.serving_disposition == [0; 32]
            || drain.joined.iter().any(|work| {
                !decision::observed_during_drain(
                    drain.closed_at_ms,
                    work.observed_at_ms,
                    receipt.destroyed_at_ms,
                )
                    || work.observation == [0; 32]
            })
            || drain_inventory(drain.joined.iter().map(|work| work.participant))?
                != commit.draining_inventory
        {
            return Err(invalid(
                "Provider destruction lacks ordered drain and disposition evidence",
            ));
        }
        self.verify(&drain.signing_bytes(&commit), &drain.signature)?;
        self.verify(&receipt.signing_bytes(&commit), &receipt.signature)
    }

    fn verify(&self, bytes: &[u8], signature: &[u8; 64]) -> Result<()> {
        UnparsedPublicKey::new(&ED25519, self.0)
            .verify(bytes, signature)
            .map_err(|_| invalid("The original custody signer did not authenticate the evidence"))
    }
}

pub(super) fn authority_unavailable() -> Anomaly {
    Anomaly::unsupported(
        "rtdb.multi/fixed-erasure-authority",
        "The installation has no authenticated enforcing erasure authority",
    )
}

mod signature_bytes {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<S: Serializer>(value: &[u8; 64], serializer: S) -> Result<S::Ok, S::Error> {
        value.as_slice().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 64], D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        bytes.try_into().map_err(|_| serde::de::Error::custom("Invalid erasure signature width"))
    }
}
