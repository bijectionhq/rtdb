use rtdb_tx::{DecisionObservation, HostedControlWrite, OwnedClockSlot};
use rtdb_types::{Anomaly, EntityId, HOSTED_ROOT_RECEIPTS_TABLE, PlacedAttemptScope, Result, Timestamp, TxId, Value};

use super::NativeSession;

const DOMAIN: &[u8] = b"rtdb.owned/funded-mutation-clock/v1\0";
pub const MAX_FUNDED_MUTATION_CLOCK_TOKEN_BYTES: usize = 1024;

/// An authenticated offer for exactly one registered receipt coordinate.
/// The host has to retain this offer in its Original reservation before preparing work.
pub struct FundedMutationClock {
    pub slot: OwnedClockSlot,
    receipt: String,
    fingerprint: [u8; 32],
}

impl FundedMutationClock {
    pub fn admits_terminal(&self, controls: &[HostedControlWrite]) -> bool {
        let mut written = controls.iter().filter(|control| {
            control.table().table == HOSTED_ROOT_RECEIPTS_TABLE && !control.is_check()
        });
        let Some(terminal) = written.next() else { return false; };
        written.next().is_none() && terminal.expected().is_none()
            && matches!(terminal.id(), EntityId::String(id) if id == &self.receipt)
            && matches!(terminal.replacement().and_then(|row| row.get("fingerprint")),
                Some(Value::Binary(fingerprint)) if fingerprint.as_slice() == self.fingerprint.as_slice())
    }
}

impl NativeSession {
    pub fn seal_funded_mutation_clock(
        &self, slot: OwnedClockSlot, receipt: &str, fingerprint: [u8; 32],
    ) -> Result<String> {
        let owner = self.state.user().filter(|owner| !owner.is_empty() && owner.len() <= 128)
            .ok_or_else(invalid)?;
        if !valid_receipt(receipt) || fingerprint == [0; 32] { return Err(invalid()); }
        let database = self.service.database()?;
        let codec = database.await_authority.codec().ok_or_else(invalid)?;
        let mut bytes = DOMAIN.to_vec();
        bytes.extend(codec.scope().encode());
        bytes.extend(slot.through.0.to_be_bytes());
        bytes.extend(slot.reading.0.to_be_bytes());
        bytes.extend(slot.issued.encode());
        bytes.extend(fingerprint);
        bytes.push(owner.len() as u8);
        bytes.extend(owner.as_bytes());
        bytes.extend(receipt.as_bytes());
        let token = codec.seal_range(&bytes)?;
        if token.len() > MAX_FUNDED_MUTATION_CLOCK_TOKEN_BYTES { return Err(invalid()); }
        Ok(token)
    }

    pub fn open_funded_mutation_clock(&self, token: &str) -> Result<FundedMutationClock> {
        if token.len() > MAX_FUNDED_MUTATION_CLOCK_TOKEN_BYTES { return Err(invalid()); }
        let database = self.service.database()?;
        let codec = database.await_authority.codec().ok_or_else(invalid)?;
        let bytes = codec.open_range(token)?;
        let body = bytes.strip_prefix(DOMAIN).ok_or_else(invalid)?;
        let (scope, body) = body.split_first_chunk::<40>().ok_or_else(invalid)?;
        let (through, body) = body.split_first_chunk::<8>().ok_or_else(invalid)?;
        let (reading, body) = body.split_first_chunk::<8>().ok_or_else(invalid)?;
        let (evidence, body) = body.split_first_chunk::<40>().ok_or_else(invalid)?;
        let (fingerprint, body) = body.split_first_chunk::<32>().ok_or_else(invalid)?;
        let (&length, body) = body.split_first().ok_or_else(invalid)?;
        let (owner, receipt) = body.split_at_checked(usize::from(length)).ok_or_else(invalid)?;
        let owner = std::str::from_utf8(owner).map_err(|_| invalid())?;
        let receipt = std::str::from_utf8(receipt).map_err(|_| invalid())?;
        let original = PlacedAttemptScope::decode(scope)?;
        let current = codec.scope();
        let through = TxId(i64::from_be_bytes(*through));
        let reading = Timestamp(i64::from_be_bytes(*reading));
        let issued = DecisionObservation::decode(evidence)?;
        if !current.same_history(original) || original.writer_epoch() > current.writer_epoch()
            || self.state.user() != Some(owner) || *fingerprint == [0; 32]
            || !valid_receipt(receipt) || through.0 < 0
            || rtdb_tx::clock_slot_reading(issued.instant) != Some(reading)
        {
            return Err(invalid());
        }
        Ok(FundedMutationClock { slot: OwnedClockSlot { through, reading, issued, funded: true },
            receipt: receipt.to_owned(), fingerprint: *fingerprint })
    }
}

fn valid_receipt(receipt: &str) -> bool {
    !receipt.is_empty() && receipt.len() <= 256 && receipt.is_ascii() && !receipt.bytes().any(|b| b.is_ascii_control())
}

fn invalid() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/funded-mutation-clock", "The funded mutation clock offer does not match its original request")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::{OwnedRelation, StructFields};

    #[test]
    fn requires_exactly_the_original_create_only_terminal() -> Result<()> {
        let issued = DecisionObservation { domain: [7; 16], epoch: 1, sequence: 1, instant: Timestamp(300_000) };
        let slot = OwnedClockSlot { through: TxId(4), reading: Timestamp(1_000_000), issued, funded: true };
        let offer = FundedMutationClock { slot, receipt: "original".into(), fingerprint: [9; 32] };
        let scope = PlacedAttemptScope::new("history", 1)?;
        let row = |id: &str, digest: [u8; 32]| StructFields::from([
            ("_id".into(), Value::Utf8(id.into())),
            ("fingerprint".into(), Value::Binary(digest.to_vec())),
        ]);
        let relation = OwnedRelation::new(HOSTED_ROOT_RECEIPTS_TABLE)?;
        let original = HostedControlWrite::new(relation.clone(), scope, None, row("original", [9; 32]))?;
        let replacement = HostedControlWrite::new(relation.clone(), scope, Some(Timestamp(1)), row("original", [9; 32]))?;
        let substituted = HostedControlWrite::new(relation, scope, None, row("original", [8; 32]))?;
        assert!(offer.admits_terminal(std::slice::from_ref(&original)));
        assert!(!offer.admits_terminal(&[replacement]));
        assert!(!offer.admits_terminal(std::slice::from_ref(&substituted)));
        assert!(!offer.admits_terminal(&[original, substituted]));
        Ok(())
    }
}
