use std::num::NonZeroU64;

use sha2::{Digest, Sha256};

use crate::{Anomaly, Result, SourcePosition, TransactionKey};

const SCOPE_BYTES: usize = 40;
const SEED_BYTES: usize = SCOPE_BYTES + 16;
const ATTEMPT_BYTES: usize = SEED_BYTES + 8 + 32;
pub const PLACED_ATTEMPT_OUTCOME_TABLE: &str = "placed_attempt_outcomes";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlacedAttemptScope {
    history: [u8; 32],
    writer_epoch: NonZeroU64,
}

impl PlacedAttemptScope {
    pub fn history_fingerprint(self) -> [u8; 32] { self.history }
    pub fn writer_epoch(self) -> u64 { self.writer_epoch.get() }
    pub fn same_history(self, other: Self) -> bool { self.history == other.history }

    pub fn new(history_stream: &str, writer_epoch: u64) -> Result<Self> {
        let writer_epoch = NonZeroU64::new(writer_epoch).ok_or_else(invalid)?;
        if history_stream.is_empty() { return Err(invalid()); }
        let mut digest = Sha256::new();
        digest.update(b"rtdb/placed-history/v1\0");
        digest.update(history_stream.as_bytes());
        Ok(Self { history: digest.finalize().into(), writer_epoch })
    }

    pub fn encode(self) -> [u8; SCOPE_BYTES] {
        let mut bytes = [0; SCOPE_BYTES];
        bytes[..32].copy_from_slice(&self.history);
        bytes[32..].copy_from_slice(&self.writer_epoch.get().to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes: &[u8; SCOPE_BYTES] = bytes.try_into().map_err(|_| invalid())?;
        let mut history = [0; 32];
        let mut epoch = [0; 8];
        history.copy_from_slice(&bytes[..32]);
        epoch.copy_from_slice(&bytes[32..]);
        let writer_epoch = NonZeroU64::new(u64::from_be_bytes(epoch)).ok_or_else(invalid)?;
        Ok(Self { history, writer_epoch })
    }

    pub fn issue(self) -> PlacedAttemptSeed {
        PlacedAttemptSeed { scope: self, nonce: *uuid::Uuid::new_v4().as_bytes() }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlacedAttemptSeed {
    scope: PlacedAttemptScope,
    nonce: [u8; 16],
}

impl PlacedAttemptSeed {
    pub fn encode(self) -> [u8; SEED_BYTES] {
        let mut bytes = [0; SEED_BYTES];
        bytes[..SCOPE_BYTES].copy_from_slice(&self.scope.encode());
        bytes[SCOPE_BYTES..].copy_from_slice(&self.nonce);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes: &[u8; SEED_BYTES] = bytes.try_into().map_err(|_| invalid())?;
        let scope = PlacedAttemptScope::decode(&bytes[..SCOPE_BYTES])?;
        let mut nonce = [0; 16];
        nonce.copy_from_slice(&bytes[SCOPE_BYTES..]);
        if nonce == [0; 16] { return Err(invalid()); }
        Ok(Self { scope, nonce })
    }

    pub fn identify(self, position: SourcePosition, principal: Option<&str>) -> Result<PlacedAttempt> {
        if position.0 < 0 { return Err(invalid()); }
        let mut digest = Sha256::new();
        digest.update(b"rtdb/attempt-principal/v1\0");
        if let Some(principal) = principal {
            digest.update([1]);
            digest.update(principal.as_bytes());
        } else {
            digest.update([0]);
        }
        Ok(PlacedAttempt { seed: self, position, principal: digest.finalize().into() })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlacedAttempt {
    seed: PlacedAttemptSeed,
    position: SourcePosition,
    principal: [u8; 32],
}

impl PlacedAttempt {
    pub fn position(self) -> SourcePosition { self.position }
    pub fn scope(self) -> PlacedAttemptScope { self.seed.scope }
    pub fn belongs_to(self, principal: Option<&str>) -> bool {
        self.seed.identify(self.position, principal).is_ok_and(|expected| expected == self)
    }
    pub fn matches(self, scope: PlacedAttemptScope, position: SourcePosition, principal: Option<&str>) -> bool {
        self.seed.scope == scope && self.seed.identify(position, principal).is_ok_and(|expected| expected == self)
    }

    pub fn encode(self) -> [u8; ATTEMPT_BYTES] {
        let mut bytes = [0; ATTEMPT_BYTES];
        bytes[..SEED_BYTES].copy_from_slice(&self.seed.encode());
        bytes[SEED_BYTES..SEED_BYTES + 8].copy_from_slice(&self.position.0.to_be_bytes());
        bytes[SEED_BYTES + 8..].copy_from_slice(&self.principal);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes: &[u8; ATTEMPT_BYTES] = bytes.try_into().map_err(|_| invalid())?;
        let seed = PlacedAttemptSeed::decode(&bytes[..SEED_BYTES])?;
        let mut position = [0; 8];
        position.copy_from_slice(&bytes[SEED_BYTES..SEED_BYTES + 8]);
        let position = SourcePosition(i64::from_be_bytes(position));
        if position.0 < 0 { return Err(invalid()); }
        let mut principal = [0; 32];
        principal.copy_from_slice(&bytes[SEED_BYTES + 8..]);
        Ok(Self { seed, position, principal })
    }

    pub fn outcome_key(self) -> String {
        use base64::Engine;
        let mut digest = Sha256::new();
        digest.update(b"rtdb/placed-attempt-outcome/v1\0");
        digest.update(self.encode());
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.finalize())
    }
}

/// Original disposition at the deciding receiver, before retry remapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlacedDecision {
    pub key: TransactionKey,
    pub committed: bool,
}

fn invalid() -> Anomaly {
    Anomaly::incorrect("rtdb/invalid-placed-attempt", "Placed attempt needs a history, nonzero writer epoch and valid source position")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_outcome_key_distinguishes_reused_positions_and_principals() {
        let identify = |stream, epoch, position, principal| {
            PlacedAttemptSeed { scope: PlacedAttemptScope::new(stream, epoch).unwrap(), nonce: [13; 16] }
                .identify(SourcePosition(position), principal).unwrap()
        };
        let original = identify("same-history", 7, 3, Some("alice"));
        assert_eq!(PlacedAttempt::decode(&original.encode()).unwrap(), original);
        for other in [
            identify("same-history", 8, 3, Some("alice")),
            identify("replacement-history", 7, 3, Some("alice")),
            identify("same-history", 7, 3, Some("bob")),
            identify("same-history", 7, 4, Some("alice")),
        ] {
            assert_ne!(original.outcome_key(), other.outcome_key());
        }
        assert!(PlacedAttempt::decode(&original.encode()[..ATTEMPT_BYTES - 1]).is_err());
        assert!(PlacedAttemptScope::new("same-history", 0).is_err());
        let scope = PlacedAttemptScope::new("same-history", 7).unwrap();
        assert_ne!(scope.issue().identify(SourcePosition(3), Some("alice")).unwrap().outcome_key(), original.outcome_key());
    }
}
