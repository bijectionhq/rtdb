use std::collections::BTreeSet;

use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::LabelScope};
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub(super) const CLAIM: &str = "rtdb_label_scope";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeClaim {
    database: String,
    incarnation: [u8; 16],
    read: BTreeSet<Vec<u8>>,
    write: BTreeSet<Vec<u8>>,
    #[serde(default)]
    protected_reads: bool,
}

pub struct VerifiedLabelScope {
    database: String,
    issuer: String,
    subject: String,
    incarnation: [u8; 16],
    labels: LabelScope,
    receipt_namespace: [u8; 32],
    expires_at: Timestamp,
    protected_reads: bool,
}

impl VerifiedLabelScope {
    pub(super) fn from_userinfo(
        claim: &serde_json::Value,
        issuer: &str,
        subject: &str,
        expires_at: Option<Timestamp>,
        now: Timestamp,
    ) -> Result<Self> {
        let claim = ScopeClaim::deserialize(claim).map_err(|_| invalid_scope())?;
        let expires_at = expires_at
            .filter(|expiry| now < *expiry)
            .ok_or_else(invalid_scope)?;
        if claim.database.is_empty()
            || claim.database.len() > 255
            || subject.is_empty()
            || [&claim.read, &claim.write].into_iter().any(|labels| {
                labels.len() > 256
                    || labels
                        .iter()
                        .any(|label| label.is_empty() || label.len() > 1024)
            })
        {
            return Err(invalid_scope());
        }
        let labels = LabelScope { read: claim.read, write: claim.write };
        let receipt_namespace = receipt_namespace(claim.incarnation, issuer, subject, &claim.database, &labels);
        Ok(Self {
            database: claim.database,
            issuer: issuer.to_owned(),
            subject: subject.to_owned(),
            incarnation: claim.incarnation,
            labels,
            receipt_namespace,
            expires_at,
            protected_reads: claim.protected_reads,
        })
    }

    pub fn labels(&self) -> &LabelScope {
        &self.labels
    }

    pub fn permits_protected_reads(&self) -> bool { self.protected_reads }

    pub fn receipt_namespace(&self) -> [u8; 32] {
        self.receipt_namespace
    }

    /// Reconstructs this identity's namespace for a retained funded receipt.
    /// The call must separately admit this credential at the current incarnation.
    pub fn funded_receipt_namespace_at(&self, incarnation: [u8; 16]) -> [u8; 32] {
        receipt_namespace(incarnation, &self.issuer, &self.subject, &self.database, &self.labels)
    }

    pub fn admit(&self, database: &str, incarnation: [u8; 16]) -> Result<()> {
        self.admit_database_name(database)?;
        if incarnation != self.incarnation {
            return Err(invalid_scope());
        }
        Ok(())
    }

    pub fn admit_database_name(&self, database: &str) -> Result<()> {
        if database != self.database { return Err(invalid_scope()); }
        Ok(())
    }

    pub fn revalidate(&self, now: Timestamp) -> Result<()> {
        if now >= self.expires_at {
            return Err(invalid_scope());
        }
        Ok(())
    }

    pub(super) fn memory_bytes(&self) -> usize {
        self.database.capacity()
            + self.issuer.capacity()
            + self.subject.capacity()
            + [&self.labels.read, &self.labels.write]
                .into_iter()
                .flatten()
                .map(|label| label.capacity() + 64)
                .sum::<usize>()
    }
}

fn receipt_namespace(
    incarnation: [u8; 16], issuer: &str, subject: &str, database: &str, labels: &LabelScope,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"rtdb.v1/scoped-receipt");
    hash.update(incarnation);
    for identity in [issuer, subject, database] {
        hash.update((identity.len() as u64).to_be_bytes());
        hash.update(identity.as_bytes());
    }
    for labels in [&labels.read, &labels.write] {
        hash.update((labels.len() as u64).to_be_bytes());
        for label in labels {
            hash.update((label.len() as u64).to_be_bytes());
            hash.update(label);
        }
    }
    hash.finalize().into()
}

pub(super) fn invalid_scope() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Forbidden,
        "rtdb.auth/label-scope",
        "The credential cannot be admitted for scoped access",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn funded_receipt_namespace_reconstructs_only_the_same_verified_scope() {
        let original = [7; 16];
        let successor = [8; 16];
        let credential = |incarnation, issuer, subject, database, read| {
            VerifiedLabelScope::from_userinfo(
                &serde_json::json!({
                    "database": database,
                    "incarnation": incarnation,
                    "read": [[read]],
                    "write": [[3]],
                }),
                issuer, subject, Some(Timestamp(10)), Timestamp(1),
            ).unwrap()
        };
        let retired = credential(original, "issuer", "subject", "database", 2);
        let active = credential(successor, "issuer", "subject", "database", 2);
        assert!(retired.admit("database", successor).is_err());
        assert!(active.admit("database", successor).is_ok());
        assert_ne!(active.receipt_namespace(), retired.receipt_namespace());
        assert_eq!(active.funded_receipt_namespace_at(original), retired.receipt_namespace());
        for different in [
            credential(successor, "different", "subject", "database", 2),
            credential(successor, "issuer", "different", "database", 2),
            credential(successor, "issuer", "subject", "different", 2),
            credential(successor, "issuer", "subject", "database", 4),
        ] {
            assert_ne!(different.funded_receipt_namespace_at(original), retired.receipt_namespace());
        }
    }
}
