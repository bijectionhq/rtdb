use ring::signature::{ED25519, UnparsedPublicKey};
use rtdb_types::{Anomaly, AnomalyCategory, Result, TimedJournalCutoff, Timestamp};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAX_CERTIFICATE_BYTES: usize = 4096;

pub struct ReceiverCertificateBinding<'a> {
    pub issuer: &'a [u8; 32],
    pub purpose: &'a str,
    pub deployment: Uuid,
    pub source: Uuid,
    pub cell: &'a str,
    pub image: &'a str,
    pub service_scope: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    statement: String,
    signature: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Statement {
    purpose: String,
    deployment: String,
    source: String,
    cell: String,
    rtdb_image: String,
    service_scope: String,
    max_stage_to_durable_lag_ns: u64,
    max_journal_clock_error_ns: u64,
    expires_at_ns: u64,
    evidence_sha256: String,
}

pub struct VerifiedReceiverCertificate {
    source: [u8; 16],
    digest: [u8; 32],
    lag_ns: u64,
    journal_error_ns: u64,
    expires_ns: u64,
}

impl VerifiedReceiverCertificate {
    pub fn verify(certificate: &[u8], installed: ReceiverCertificateBinding<'_>) -> Result<Self> {
        if certificate.len() > MAX_CERTIFICATE_BYTES {
            return Err(invalid());
        }
        let envelope: Envelope = serde_json::from_slice(certificate).map_err(|_| invalid())?;
        let signature = decode_hex::<64>(&envelope.signature).ok_or_else(invalid)?;
        UnparsedPublicKey::new(&ED25519, installed.issuer)
            .verify(envelope.statement.as_bytes(), &signature)
            .map_err(|_| invalid())?;
        let statement: Statement = serde_json::from_str(&envelope.statement).map_err(|_| invalid())?;
        if installed.source.is_nil() || installed.deployment.is_nil()
            || installed.purpose.is_empty() || installed.cell.is_empty() || installed.image.is_empty()
            || decode_hex::<32>(installed.service_scope).is_none_or(|scope| scope == [0; 32])
            || statement.purpose != installed.purpose
            || statement.source != installed.source.to_string()
            || statement.deployment != installed.deployment.to_string()
            || statement.cell != installed.cell
            || statement.rtdb_image != installed.image
            || statement.service_scope != installed.service_scope
            || decode_hex::<32>(&statement.evidence_sha256).is_none_or(|hash| hash == [0; 32])
            || statement.max_journal_clock_error_ns == 0
            || statement.max_journal_clock_error_ns > statement.max_stage_to_durable_lag_ns
            || statement.max_stage_to_durable_lag_ns >= statement.expires_at_ns
            || statement.expires_at_ns > i64::MAX as u64
        {
            return Err(invalid());
        }
        Ok(Self {
            source: *installed.source.as_bytes(),
            digest: Sha256::digest(certificate).into(),
            lag_ns: statement.max_stage_to_durable_lag_ns,
            journal_error_ns: statement.max_journal_clock_error_ns,
            expires_ns: statement.expires_at_ns,
        })
    }

    pub fn source(&self) -> [u8; 16] { self.source }
    pub fn digest(&self) -> [u8; 32] { self.digest }

    pub fn live_at(&self, now: Timestamp) -> bool {
        micros_to_nanos(now).is_some_and(|now| now < self.expires_ns)
    }

    pub fn cutoffs(&self, observed: Timestamp, before: Timestamp) -> Result<(Timestamp, TimedJournalCutoff)> {
        let limit = micros_to_nanos(before).ok_or_else(invalid)?.min(self.expires_ns);
        let decision = limit.checked_sub(self.lag_ns).ok_or_else(invalid)?;
        let journal = limit.checked_sub(self.journal_error_ns).ok_or_else(invalid)?;
        // Flooring preserves both exclusive bounds when the signed margins are sub-microsecond.
        let decision = Timestamp((decision / 1000) as i64);
        let journal = Timestamp((journal / 1000) as i64);
        if observed >= decision || !self.live_at(observed) || decision.0 <= 0 { return Err(invalid()); }
        Ok((decision, TimedJournalCutoff::new(journal, self.digest)?))
    }
}

fn micros_to_nanos(time: Timestamp) -> Option<u64> {
    u64::try_from(time.0).ok()?.checked_mul(1000)
}

fn decode_hex<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2 { return None; }
    let mut decoded = [0; N];
    for (byte, pair) in decoded.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let high = char::from(pair[0]).to_digit(16)? as u8;
        let low = char::from(pair[1]).to_digit(16)? as u8;
        *byte = (high << 4) | low;
    }
    Some(decoded)
}

fn invalid() -> Anomaly {
    Anomaly::new(AnomalyCategory::Forbidden, "rtdb.clock/certificate",
        "The receiver clock certificate does not cover the installed authority and exclusive bounds")
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{Ed25519KeyPair, KeyPair};

    #[test]
    fn original_certificate_binds_exact_source_and_rounds_exclusive_cutoffs_inward() {
        let signer = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
        let issuer = signer.public_key().as_ref().try_into().unwrap();
        let source = Uuid::from_u128(1);
        let deployment = Uuid::from_u128(2);
        let service_scope = "ab".repeat(32);
        let binding = |source| ReceiverCertificateBinding {
            issuer, source, deployment, service_scope: &service_scope,
            purpose: "clock-evidence", cell: "cell", image: "image",
        };
        let statement = serde_json::json!({
            "purpose": "clock-evidence", "source": source, "deployment": deployment,
            "serviceScope": service_scope.clone(), "cell": "cell", "rtdbImage": "image",
            "maxStageToDurableLagNs": 1501, "maxJournalClockErrorNs": 999,
            "expiresAtNs": 2_000_000_500u64, "evidenceSha256": "cd".repeat(32),
        }).to_string();
        let signature: String = signer.sign(statement.as_bytes()).as_ref().iter()
            .map(|byte| format!("{byte:02x}")).collect();
        let envelope = serde_json::to_vec(&serde_json::json!({"statement": statement, "signature": signature})).unwrap();
        let verified = VerifiedReceiverCertificate::verify(&envelope, binding(source)).unwrap();
        assert!(VerifiedReceiverCertificate::verify(&envelope, binding(Uuid::from_u128(3))).is_err());
        let digest: [u8; 32] = Sha256::digest(&envelope).into();
        let (decision, journal) = verified.cutoffs(Timestamp(1_000_000), Timestamp(2_500_000)).unwrap();
        assert_eq!(decision, Timestamp(1_999_998));
        assert_eq!(journal.before(), Timestamp(1_999_999));
        assert_eq!(journal.certificate_sha256(), digest);
        assert!(verified.cutoffs(decision, Timestamp(2_500_000)).is_err());
        assert!(!verified.live_at(Timestamp(2_000_001)));
    }
}
