use rtdb_types::{Anomaly, Result, Timestamp};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::forbidden;

pub(super) const CLAIM: &str = "rtdb_workload_grant";
pub(super) const MAX_LIFETIME_MICROS: i64 = 300_000_000;

#[derive(Clone, Debug)]
pub struct WorkloadGrantConfig {
    audience: String,
    client_id_prefix: String,
}

impl WorkloadGrantConfig {
    pub fn new(audience: String, client_id_prefix: String) -> Result<Self> {
        if !valid_identity(&audience)
            || client_id_prefix.is_empty()
            || client_id_prefix.len() > 128
            || !client_id_prefix
                .bytes()
                .all(|byte| byte.is_ascii_graphic() && byte != b':')
        {
            return Err(Anomaly::incorrect(
                "rtdb.auth/workload-grant-config",
                "Workload grants require a bounded audience and a nonempty client ID prefix",
            ));
        }
        Ok(Self {
            audience,
            client_id_prefix,
        })
    }

    pub fn audience(&self) -> &str {
        &self.audience
    }
    pub fn client_id_prefix(&self) -> &str {
        &self.client_id_prefix
    }
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum WorkloadOperation {
    CreateDeployment,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Claim<'a> {
    version: u32,
    audience: &'a str,
    database: &'a str,
    incarnation: [u8; 16],
    grant_id: [u8; 16],
    expires_at_micros: i64,
    operations: [WorkloadOperation; 1],
}

pub struct VerifiedWorkloadGrant {
    database: String,
    incarnation: [u8; 16],
    operation: WorkloadOperation,
    receipt_namespace: [u8; 32],
    expires_at: Timestamp,
}

impl VerifiedWorkloadGrant {
    pub(super) fn from_userinfo(
        claim: &serde_json::Value,
        issuer: &str,
        audience: &str,
        subject: &str,
        token_expiry: Timestamp,
        now: Timestamp,
    ) -> Result<Self> {
        let claim = Claim::deserialize(claim).map_err(|_| invalid_grant())?;
        let lifetime = claim.expires_at_micros.checked_sub(now.0);
        if claim.version != 1
            || claim.audience != audience
            || !valid_identity(subject)
            || !valid_identity(claim.database)
            || claim.database == "*"
            || claim.incarnation == [0; 16]
            || claim.grant_id == [0; 16]
            || !lifetime.is_some_and(|micros| (1..=MAX_LIFETIME_MICROS).contains(&micros))
            || token_expiry <= now
        {
            return Err(invalid_grant());
        }
        let [operation] = claim.operations;
        let mut hash = Sha256::new();
        hash.update(b"rtdb.v1/workload-receipt\0");
        hash.update(claim.incarnation);
        hash.update(claim.grant_id);
        for identity in [issuer, audience, subject, claim.database] {
            hash.update((identity.len() as u64).to_be_bytes());
            hash.update(identity.as_bytes());
        }
        Ok(Self {
            database: claim.database.into(),
            incarnation: claim.incarnation,
            operation,
            receipt_namespace: hash.finalize().into(),
            expires_at: Timestamp(claim.expires_at_micros).min(token_expiry),
        })
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn receipt_namespace(&self) -> [u8; 32] {
        self.receipt_namespace
    }

    pub fn admit(
        &self,
        operation: WorkloadOperation,
        database: &str,
        incarnation: [u8; 16],
        now: Timestamp,
    ) -> Result<()> {
        if self.operation != operation
            || self.database != database
            || self.incarnation != incarnation
        {
            return Err(invalid_grant());
        }
        self.revalidate(now)
    }

    pub(super) fn revalidate(&self, now: Timestamp) -> Result<()> {
        if now >= self.expires_at {
            return Err(invalid_grant());
        }
        Ok(())
    }

    pub(super) fn memory_bytes(&self) -> usize {
        self.database.capacity()
    }
}

fn valid_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

pub(super) fn invalid_grant() -> Anomaly {
    forbidden(
        "rtdb.auth/workload-grant",
        "The credential cannot be admitted for managed workload access",
    )
}
