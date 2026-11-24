use std::collections::BTreeSet;

use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::LabelScope};
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeClaim {
    database: String,
    incarnation: [u8; 16],
    read: BTreeSet<Vec<u8>>,
    write: BTreeSet<Vec<u8>>,
}

pub struct VerifiedLabelScope {
    database: String,
    incarnation: [u8; 16],
    labels: LabelScope,
    receipt_namespace: [u8; 32],
    expires_at: Timestamp,
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
        let mut hash = Sha256::new();
        hash.update(b"rtdb.v1/scoped-receipt");
        hash.update(claim.incarnation);
        for identity in [issuer, subject, claim.database.as_str()] {
            hash.update((identity.len() as u64).to_be_bytes());
            hash.update(identity.as_bytes());
        }
        for labels in [&claim.read, &claim.write] {
            hash.update((labels.len() as u64).to_be_bytes());
            for label in labels {
                hash.update((label.len() as u64).to_be_bytes());
                hash.update(label);
            }
        }
        Ok(Self {
            database: claim.database,
            incarnation: claim.incarnation,
            labels: LabelScope {
                read: claim.read,
                write: claim.write,
            },
            receipt_namespace: hash.finalize().into(),
            expires_at,
        })
    }

    pub fn labels(&self) -> &LabelScope {
        &self.labels
    }

    pub fn receipt_namespace(&self) -> [u8; 32] {
        self.receipt_namespace
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
            + [&self.labels.read, &self.labels.write]
                .into_iter()
                .flatten()
                .map(|label| label.capacity() + 64)
                .sum::<usize>()
    }
}

fn invalid_scope() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Forbidden,
        "rtdb.auth/label-scope",
        "The credential cannot be admitted for scoped access",
    )
}
