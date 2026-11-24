//! Evidence issued by the original custody authority for one ordered switch.

use ring::{
    digest::{SHA256, digest},
    signature::{ED25519, UnparsedPublicKey},
};
use rtdb_types::{Anomaly, Result};

use super::{ErasureCommit, PreparedErasure, invalid};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DestructionEvidence {
    pub drain_observed_at_ms: u64,
    pub destroyed_at_ms: u64,
    pub disposition: [u8; 32],
    pub signature: [u8; 64],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeadlineReservation {
    pub by_ms: u64,
    pub retiring_inventory: [u8; 32],
    pub signature: [u8; 64],
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
        let mut bytes = b"rtdb.multi/erasure-reservation/v1\0".to_vec();
        append_preparation(&mut bytes, prepared);
        bytes.extend_from_slice(&self.by_ms.to_be_bytes());
        bytes.extend_from_slice(&self.retiring_inventory);
        bytes
    }
}

impl ErasureCommit {
    /// The authority signs after irrevocably arming the reserved fixed policy
    /// for the authenticated ordered switch.
    pub fn signing_bytes(&self) -> Vec<u8> {
        let mut bytes = b"rtdb.multi/erasure-commit/v1\0".to_vec();
        append_preparation(&mut bytes, self.prepared);
        bytes.extend_from_slice(&self.committed_at_ms.to_be_bytes());
        bytes.extend_from_slice(&self.deadline.at_ms().to_be_bytes());
        bytes.push(u8::from(matches!(
            self.deadline,
            ErasureDeadline::Enforced { .. }
        )));
        bytes.extend_from_slice(&self.retiring_inventory);
        bytes.extend_from_slice(&self.switch_evidence);
        bytes
    }
}

impl DestructionEvidence {
    pub fn signing_bytes(&self, commit: &ErasureCommit) -> Vec<u8> {
        let mut bytes = b"rtdb.multi/erasure-destruction/v1\0".to_vec();
        bytes.extend_from_slice(digest(&SHA256, &commit.signing_bytes()).as_ref());
        bytes.extend_from_slice(&self.drain_observed_at_ms.to_be_bytes());
        bytes.extend_from_slice(&self.destroyed_at_ms.to_be_bytes());
        bytes.extend_from_slice(&self.disposition);
        bytes
    }

    /// An operational target supplies no deadline obligation.
    pub fn fixed_deadline_met(&self, commit: &ErasureCommit) -> Option<bool> {
        match commit.deadline {
            ErasureDeadline::Target { .. } => None,
            ErasureDeadline::Enforced { by_ms, .. } => Some(self.destroyed_at_ms <= by_ms),
        }
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
        if reservation.retiring_inventory == [0; 32] {
            return Err(invalid("The deadline requires an exact retiring inventory"));
        }
        self.verify(&reservation.signing_bytes(prepared), &reservation.signature)
    }

    pub(super) fn verify_destruction(
        &self,
        commit: ErasureCommit,
        receipt: &DestructionEvidence,
    ) -> Result<()> {
        if receipt.drain_observed_at_ms < commit.committed_at_ms
            || receipt.destroyed_at_ms < receipt.drain_observed_at_ms
            || receipt.disposition == [0; 32]
        {
            return Err(invalid(
                "Provider destruction lacks ordered drain and disposition evidence",
            ));
        }
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
