use rtdb_node::serving::custody_release::CustodyCarrierAuthority;
use rtdb_types::{Anomaly, Result, Timestamp};
use rtdb_tx::transact::workload::{Authority, Point};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{Method, MethodRule, forbidden};

pub(super) const CLAIM: &str = "rtdb_workload_grant";
pub(super) const MAX_LIFETIME_MICROS: i64 = 300_000_000;

#[derive(Clone, Debug)]
pub struct WorkloadGrantConfig {
    audience: String,
    client_id_prefix: String,
    assignment: Option<Point>,
}

impl WorkloadGrantConfig {
    pub fn new(audience: String, client_id_prefix: String) -> Result<Self> {
        if !valid_identity(&audience) || !valid_client_id_prefix(&client_id_prefix) {
            return Err(Anomaly::incorrect(
                "rtdb.auth/workload-grant-config",
                "Workload grants require a bounded audience and a nonempty client ID prefix",
            ));
        }
        Ok(Self {
            audience,
            client_id_prefix,
            assignment: None,
        })
    }

    pub fn with_assignment(mut self, point: Point) -> Self {
        self.assignment = Some(point);
        self
    }

    pub fn assignment(&self) -> Option<&Point> { self.assignment.as_ref() }

    pub fn validate_rules(&self, rules: &[MethodRule]) -> Result<()> {
        if self.assignment.is_some()
            && (rules.is_empty() || rules.iter().any(|rule| rule.method != Method::ClientCredentials))
        {
            return Err(Anomaly::incorrect("rtdb.auth/workload-grant-config",
                "Managed assignment requires a dedicated client-credentials authentication profile"));
        }
        Ok(())
    }

    pub fn audience(&self) -> &str {
        &self.audience
    }
    pub fn client_id_prefix(&self) -> &str {
        &self.client_id_prefix
    }
}

pub(super) fn valid_client_id_prefix(prefix: &str) -> bool {
    (1..=128).contains(&prefix.len())
        && prefix.bytes().all(|byte| byte.is_ascii_graphic() && byte != b':')
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum WorkloadOperation {
    CreateDeployment,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum OriginalCustodyOperation {
    Release,
    Dispose,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginalCustodyControl {
    operation: OriginalCustodyOperation,
    authority: CustodyCarrierAuthority,
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
    operations: Option<[WorkloadOperation; 1]>,
    authority: Option<Authority>,
    original_custody: Option<OriginalCustodyControl>,
}

pub struct VerifiedWorkloadGrant {
    database: String,
    incarnation: [u8; 16],
    permission: Permission,
    receipt_namespace: [u8; 32],
    expires_at: Timestamp,
}

enum Permission {
    Create(WorkloadOperation),
    Assignment { authority: Box<Authority>, point: Point },
    OriginalCustody(OriginalCustodyControl),
}

impl VerifiedWorkloadGrant {
    pub(super) fn from_userinfo(
        raw: &serde_json::Value,
        issuer: &str,
        config: &WorkloadGrantConfig,
        subject: &str,
        token_expiry: Timestamp,
        now: Timestamp,
    ) -> Result<Self> {
        let claim = Claim::deserialize(raw).map_err(|_| invalid_grant())?;
        let lifetime = claim.expires_at_micros.checked_sub(now.0);
        let audience = config.audience();
        if claim.audience != audience
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
        let permission = match (claim.version, claim.operations, claim.authority, claim.original_custody, config.assignment()) {
            (1, Some([operation]), None, None, _) if claim_field_absent(raw, "authority")
                && claim_field_absent(raw, "original_custody") => Permission::Create(operation),
            (2, None, Some(authority), None, Some(point)) if claim_field_absent(raw, "operations")
                && claim_field_absent(raw, "original_custody") && authority.valid(claim.incarnation) =>
                Permission::Assignment { authority: Box::new(authority), point: point.clone() },
            (3, None, None, Some(control), Some(_)) if claim_field_absent(raw, "operations")
                && claim_field_absent(raw, "authority") && control.authority.incarnation == claim.incarnation
                && control.authority.installation != [0; 16] && control.authority.source_history != [0; 32]
                && control.authority.registry != [0; 32] && control.authority.original_writer != [0; 32] =>
                Permission::OriginalCustody(control),
            _ => return Err(invalid_grant()),
        };
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
            permission,
            receipt_namespace: hash.finalize().into(),
            expires_at: Timestamp(claim.expires_at_micros).min(token_expiry),
        })
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn is_assignment(&self) -> bool { matches!(self.permission, Permission::Assignment { .. }) }

    pub(super) fn is_assignment_issued(&self) -> bool {
        self.is_assignment() || self.is_original_custody_control()
    }

    pub fn is_original_custody_control(&self) -> bool {
        matches!(self.permission, Permission::OriginalCustody(_))
    }

    pub fn admit_original_custody(
        &self, database: &str, authority: CustodyCarrierAuthority, operation: OriginalCustodyOperation,
        now: Timestamp,
    ) -> Result<Timestamp> {
        if self.database != database || self.incarnation != authority.incarnation
            || !matches!(&self.permission, Permission::OriginalCustody(control)
                if control.operation == operation && control.authority == authority)
        {
            return Err(invalid_grant());
        }
        self.revalidate(now)?;
        Ok(self.expires_at)
    }

    pub(super) fn is_create(&self) -> bool {
        matches!(self.permission, Permission::Create(WorkloadOperation::CreateDeployment))
    }

    pub fn is_custody_reader(&self) -> bool {
        matches!(&self.permission, Permission::Assignment { authority, .. }
            if matches!(authority.as_ref(), Authority::ReadCustody { .. }))
    }

    pub fn is_root_reader(&self) -> bool {
        matches!(&self.permission, Permission::Assignment { authority, .. }
            if matches!(authority.as_ref(), Authority::ReadRoot { .. }))
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
        if !matches!(self.permission, Permission::Create(granted) if granted == operation)
            || self.database != database
            || self.incarnation != incarnation
        {
            return Err(invalid_grant());
        }
        self.revalidate(now)
    }

    pub fn assignment(&self, database: &str, incarnation: [u8; 16], now: Timestamp)
        -> Result<(&Authority, &Point, Timestamp)>
    {
        if self.database != database || self.incarnation != incarnation {
            return Err(invalid_grant());
        }
        self.revalidate(now)?;
        match &self.permission {
            Permission::Assignment { authority, point } => Ok((authority, point, self.expires_at)),
            Permission::Create(_) | Permission::OriginalCustody(_) => Err(invalid_grant()),
        }
    }

    pub(super) fn revalidate(&self, now: Timestamp) -> Result<()> {
        if now >= self.expires_at {
            return Err(invalid_grant());
        }
        Ok(())
    }

    pub(super) fn memory_bytes(&self) -> usize {
        self.database.capacity() + match &self.permission {
            Permission::Create(_) | Permission::OriginalCustody(_) => 0,
            Permission::Assignment { .. } => 8192,
        }
    }
}

fn valid_identity(value: &str) -> bool { rtdb_tx::transact::workload::bounded(value) }

fn claim_field_absent(raw: &serde_json::Value, key: &str) -> bool { raw.get(key).is_none() }

pub(super) fn invalid_grant() -> Anomaly {
    forbidden(
        "rtdb.auth/workload-grant",
        "The credential cannot be admitted for managed workload access",
    )
}
