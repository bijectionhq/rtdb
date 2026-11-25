use rtdb_node::serving::custody_release::CustodyCarrierAuthority;
use rtdb_types::{Anomaly, Result, Timestamp, rtdb_v1::{Basis, MAIN_BRANCH, valid_branch_name}};
use rtdb_tx::transact::workload::{Applied, Authority, Fence, FundedAppointmentScope, ManagedAppointment, Point};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Method, MethodRule, forbidden};

pub(super) const CLAIM: &str = "rtdb_workload_grant";
pub(super) const MAX_LIFETIME_MICROS: i64 = 300_000_000;

#[derive(Clone, Debug)]
pub struct WorkloadGrantConfig {
    audience: String,
    client_id_prefix: String,
    assignment: Option<Point>,
    original_family: bool,
    funded_appointment_client_prefix: Option<String>,
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
            original_family: false,
            funded_appointment_client_prefix: None,
        })
    }

    pub fn with_assignment(mut self, point: Point) -> Self {
        self.assignment = Some(point);
        self
    }

    pub fn assignment(&self) -> Option<&Point> { self.assignment.as_ref() }

    pub fn with_funded_appointment_client_prefix(mut self, prefix: String) -> Result<Self> {
        if self.assignment.is_none() || !valid_client_id_prefix(&prefix)
            || !prefix.starts_with(self.client_id_prefix.as_str()) || prefix == self.client_id_prefix
        {
            return Err(Anomaly::incorrect("rtdb.auth/workload-grant-config",
                "Funded appointments require a dedicated client ID sub-prefix of the assignment issuer"));
        }
        self.funded_appointment_client_prefix = Some(prefix);
        Ok(self)
    }

    pub(crate) fn funded_appointment_client(&self, client_id: &str) -> bool {
        self.funded_appointment_client_prefix.as_deref().is_some_and(|prefix| client_id.starts_with(prefix))
    }

    pub(crate) fn for_original_family(mut self, point: Point) -> Self {
        self.assignment = Some(point);
        self.original_family = true;
        self
    }

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

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OriginalFamilyGrant {
    pub authority: CustodyCarrierAuthority,
    pub fence: Fence,
    pub writer: Applied,
    pub branch: String,
    pub created: [u8; Basis::WIDTH],
    pub family: [u8; 16],
    pub group: [u8; 16],
}

impl OriginalFamilyGrant {
    fn valid(&self, incarnation: [u8; 16]) -> bool {
        self.authority.incarnation == incarnation
            && self.authority.installation != [0; 16]
            && self.authority.source_history != [0; 32]
            && self.authority.registry != [0; 32]
            && self.authority.original_writer != [0; 32]
            && self.family != [0; 16] && self.group != [0; 16]
            && self.branch != MAIN_BRANCH && valid_branch_name(&self.branch)
            && Basis::from_bytes(&self.created).is_ok_and(|basis| basis.incarnation() == incarnation && basis.position() > 0)
            && Authority::ReadRoot { fence: self.fence.clone(), writer: self.writer.clone() }.valid(incarnation)
    }
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
    original_family: Option<OriginalFamilyGrant>,
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
    OriginalFamily { scope: Box<OriginalFamilyGrant>, point: Point },
    FundedAppointment { scope: Box<FundedAppointmentScope>, point: Point },
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
        let permission = if config.original_family {
            match (claim.version, claim.original_family, config.assignment()) {
                (4, Some(scope), Some(point)) if ["operations", "authority", "original_custody"].iter()
                    .all(|field| claim_field_absent(raw, field)) && scope.valid(claim.incarnation) =>
                    Permission::OriginalFamily { scope: Box::new(scope), point: point.clone() },
                _ => return Err(invalid_grant()),
            }
        } else {
            if !claim_field_absent(raw, "original_family") {
                return Err(invalid_grant());
            }
            match (claim.version, claim.operations, claim.authority, claim.original_custody, config.assignment()) {
            (1, Some([operation]), None, None, _) if claim_field_absent(raw, "authority")
                && claim_field_absent(raw, "original_custody") => Permission::Create(operation),
            (2, None, Some(Authority::FundedAppointment(scope)), None, Some(point))
                if config.funded_appointment_client_prefix.is_some()
                    && claim_field_absent(raw, "operations") && claim_field_absent(raw, "original_custody")
                    && point.table != scope.receipt.table
                    && Authority::FundedAppointment(scope.clone()).valid(claim.incarnation) =>
                Permission::FundedAppointment { scope: Box::new(scope), point: point.clone() },
            (2, None, Some(authority), None, Some(point)) if claim_field_absent(raw, "operations")
                && claim_field_absent(raw, "original_custody") && authority.valid(claim.incarnation)
                && !matches!(authority, Authority::OriginalFamilyWrite { .. } | Authority::FundedAppointment(_)) =>
                Permission::Assignment { authority: Box::new(authority), point: point.clone() },
            (3, None, None, Some(control), Some(_)) if claim_field_absent(raw, "operations")
                && claim_field_absent(raw, "authority") && control.authority.incarnation == claim.incarnation
                && control.authority.installation != [0; 16] && control.authority.source_history != [0; 32]
                && control.authority.registry != [0; 32] && control.authority.original_writer != [0; 32] =>
                Permission::OriginalCustody(control),
            _ => return Err(invalid_grant()),
            }
        };
        let mut hash = Sha256::new();
        hash.update(b"rtdb.v1/workload-receipt\0");
        hash.update(claim.incarnation);
        hash.update(claim.grant_id);
        for identity in [issuer, audience, subject, claim.database] {
            hash.update((identity.len() as u64).to_be_bytes());
            hash.update(identity.as_bytes());
        }
        if let Permission::OriginalFamily { scope, .. } = &permission {
            hash.update(b"rtdb.v1/original-family\0");
            hash.update(serde_json::to_vec(scope.as_ref()).map_err(|_| invalid_grant())?);
        }
        if let Permission::FundedAppointment { scope, point } = &permission {
            hash.update(b"rtdb.v1/managed-funded-appointment\0");
            hash.update(serde_json::to_vec(&(scope, point)).map_err(|_| invalid_grant())?);
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
        self.is_assignment() || self.is_original_custody_control() || self.is_funded_appointment()
    }

    pub fn is_original_custody_control(&self) -> bool {
        matches!(self.permission, Permission::OriginalCustody(_))
    }

    pub fn is_original_family(&self) -> bool {
        matches!(self.permission, Permission::OriginalFamily { .. })
    }

    pub fn is_funded_appointment(&self) -> bool {
        matches!(self.permission, Permission::FundedAppointment { .. })
    }

    pub fn funded_appointment(&self, database: &str, incarnation: [u8; 16], now: Timestamp) -> Result<ManagedAppointment> {
        self.revalidate(now)?;
        match &self.permission {
            Permission::FundedAppointment { scope, point } if self.database == database && self.incarnation == incarnation =>
                ManagedAppointment::new(point.clone(), scope.as_ref().clone(), self.receipt_namespace, self.expires_at),
            _ => Err(invalid_grant()),
        }
    }

    pub fn original_family(
        &self, database: &str, authority: CustodyCarrierAuthority, now: Timestamp,
    ) -> Result<(&OriginalFamilyGrant, &Point, Timestamp)> {
        self.revalidate(now)?;
        match &self.permission {
            Permission::OriginalFamily { scope, point }
                if self.database == database && scope.authority == authority =>
                Ok((scope, point, self.expires_at)),
            _ => Err(invalid_grant()),
        }
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
            _ => Err(invalid_grant()),
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
            Permission::Assignment { .. } | Permission::OriginalFamily { .. } | Permission::FundedAppointment { .. } => 8192,
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

#[cfg(test)]
mod tests {
    use rtdb_tx::transact::workload::{Holder, Placement};
    use serde_json::json;

    use super::*;

    #[test]
    fn funded_appointment_v2_requires_enrollment_and_preserves_the_exact_scope_on_renewal() {
        let incarnation = [1; 16];
        let point = Point::new("public.writer", "authority".into(), Timestamp(0)).unwrap();
        let assignment = WorkloadGrantConfig::new("assignment".into(), "assignment-".into())
            .unwrap().with_assignment(point);
        let enrolled = assignment.clone().with_funded_appointment_client_prefix("assignment-funded-".into()).unwrap();
        assert!(enrolled.funded_appointment_client("assignment-funded-booking"));
        assert!(!enrolled.funded_appointment_client("assignment-writer"));
        let applied = |namespace, position| Applied {
            namespace, key: vec![position as u8; 16], basis: Basis::new(incarnation, position).to_bytes(),
            system_time_micros: position as i64,
        };
        let scope = FundedAppointmentScope {
            fence: Fence { applied: applied([2; 32], 1), placement: Placement { epoch: 1,
                assignment: Some(Holder { assignment_id: [3; 16], holder: "holder".into() }),
            } },
            writer: applied([4; 32], 2), key: [5; 32],
            receipt: Point::new("public.receipts", "receipt".into(), Timestamp(0)).unwrap(),
            intent_sha256: [6; 32], funding_policy_sha256: [7; 32],
        };
        let authority = json!(Authority::FundedAppointment(scope));
        assert_eq!(authority["role"], "funded-appointment");
        let raw = |expires_at_micros| json!({
            "version": 2, "audience": "assignment", "database": "exact", "incarnation": incarnation,
            "grant_id": [8; 16], "expires_at_micros": expires_at_micros, "authority": authority,
        });
        let verify = |raw: &serde_json::Value, config: &WorkloadGrantConfig| VerifiedWorkloadGrant::from_userinfo(
            raw, "https://assignment.example.invalid/", config, "funded", Timestamp(100), Timestamp(0),
        );
        assert!(verify(&raw(80), &assignment).is_err());
        let first = verify(&raw(80), &enrolled).unwrap();
        let renewed = verify(&raw(90), &enrolled).unwrap();
        assert_eq!(first.receipt_namespace(), renewed.receipt_namespace());
        assert!(first.assignment("exact", incarnation, Timestamp(0)).is_err());
        assert!(first.funded_appointment("other", incarnation, Timestamp(0)).is_err());
        assert!(first.funded_appointment("exact", [9; 16], Timestamp(0)).is_err());
        assert!(first.funded_appointment("exact", incarnation, Timestamp(80)).is_err());
        let context = first.funded_appointment("exact", incarnation, Timestamp(0)).unwrap();
        assert!(context.check_key(&[5; 32]).is_ok());
        assert!(context.check_key(&[6; 32]).is_err());
        let mut changed = raw(90);
        changed["authority"]["writer"]["namespace"] = json!([9; 32]);
        assert_ne!(first.receipt_namespace(), verify(&changed, &enrolled).unwrap().receipt_namespace());
    }

    #[test]
    fn original_family_grant_requires_its_issuer_and_binds_line_and_epoch() {
        let incarnation = [1; 16];
        let authority = CustodyCarrierAuthority {
            incarnation, installation: [2; 16], source_history: [3; 32], registry: [4; 32], original_writer: [5; 32],
        };
        let applied = |position| Applied {
            namespace: [6; 32], key: vec![7; 16], basis: Basis::new(incarnation, position).to_bytes(), system_time_micros: 10,
        };
        let mut scope = OriginalFamilyGrant {
            authority, branch: "draft".into(), created: Basis::new(incarnation, 3).to_bytes(),
            family: [8; 16], group: [9; 16], writer: applied(2),
            fence: Fence { applied: applied(1), placement: Placement { epoch: 1,
                assignment: Some(Holder { assignment_id: [10; 16], holder: "holder".into() }),
            }},
        };
        let raw = |scope: &OriginalFamilyGrant| json!({
            "version": 4, "audience": "families", "database": "exact", "incarnation": incarnation,
            "grant_id": [11; 16], "expires_at_micros": 100, "original_family": scope,
        });
        let verify = |raw: &serde_json::Value, config: &WorkloadGrantConfig| VerifiedWorkloadGrant::from_userinfo(
            raw, "https://family.example.invalid/", config, "owner", Timestamp(90), Timestamp(0),
        );
        let point = Point::new("public.writer", "owner".into(), Timestamp(0)).unwrap();
        let assignment = WorkloadGrantConfig::new("families".into(), "assignment-".into()).unwrap().with_assignment(point.clone());
        let family = WorkloadGrantConfig::new("families".into(), "family-".into()).unwrap().for_original_family(point);
        assert!(verify(&raw(&scope), &assignment).is_err());
        let first = verify(&raw(&scope), &family).unwrap();
        assert!(first.is_original_family());
        assert!(first.assignment("exact", incarnation, Timestamp(0)).is_err());
        assert!(first.original_family("other", authority, Timestamp(0)).is_err());
        assert!(first.original_family("exact", authority, Timestamp(90)).is_err());
        assert!(verify(&json!({
            "version": 2, "audience": "families", "database": "exact", "incarnation": incarnation,
            "grant_id": [11; 16], "expires_at_micros": 100,
            "authority": { "role": "receipt-recovery", "original_namespace": first.receipt_namespace(),
                "family": "original-family-write" },
        }), &assignment).is_err());
        scope.fence.placement.epoch = 2;
        let successor = verify(&raw(&scope), &family).unwrap();
        assert_ne!(first.receipt_namespace(), successor.receipt_namespace());
        scope.created = Basis::new(incarnation, 4).to_bytes();
        assert_ne!(successor.receipt_namespace(), verify(&raw(&scope), &family).unwrap().receipt_namespace());
        scope.branch = MAIN_BRANCH.into();
        assert!(verify(&raw(&scope), &family).is_err());
    }
}
