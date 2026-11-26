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
    original_family_readback: bool,
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
            original_family_readback: false,
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

    pub(crate) fn for_original_family_readback(mut self, point: Point) -> Self {
        self.assignment = Some(point);
        self.original_family_readback = true;
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
    PrivateReplay,
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OriginalFamilyReadbackGrant {
    pub line: OriginalFamilyGrant,
    pub generation: [u8; 16],
    pub creation_key: Vec<u8>,
    pub carrier: Option<Point>,
    pub observation: Option<[u8; Basis::WIDTH]>,
    pub witness: Option<OriginalFamilyReadbackWitness>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OriginalFamilyReadbackWitness {
    pub receipt: Applied,
    pub placement: [u8; 16],
    pub projection: OriginalFamilyReadbackProjection,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum OriginalFamilyReadbackProjection {
    Author {
        mapping_sha256: Option<[u8; 32]>,
        settlement: Option<OriginalFamilyAuthorSettlement>,
    },
    Reserve {},
    Landing {
        mapping_sha256: Option<[u8; 32]>,
        settlement: Option<OriginalFamilyLandingSettlement>,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OriginalFamilyAuthorSettlement {
    pub command: String,
    pub head: String,
    pub dispatch: String,
    pub pending: String,
    pub business_receipt: String,
    pub execution_slot: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OriginalFamilyLandingSettlement {
    pub receipt: String,
    pub proof: String,
    pub body_sha256: [u8; 32],
}

impl OriginalFamilyReadbackWitness {
    fn valid(&self, created: [u8; Basis::WIDTH]) -> bool {
        self.placement != [0; 16]
            && self.receipt.namespace != [0; 32]
            && (16..=64).contains(&self.receipt.key.len())
            && self.receipt.basis[..16] == created[..16]
            && self.receipt.basis > created
            && match &self.projection {
                OriginalFamilyReadbackProjection::Author { settlement, .. } =>
                    settlement.as_ref().is_none_or(|witness| {
                        let ids = [&witness.command, &witness.head, &witness.dispatch, &witness.pending];
                        ids.iter().enumerate().all(|(idx, id)| bounded_readback_id(id)
                            && !ids[..idx].contains(id))
                            && bounded_readback_id(&witness.business_receipt)
                            && bounded_readback_id(&witness.execution_slot)
                    }),
                OriginalFamilyReadbackProjection::Reserve {} => true,
                OriginalFamilyReadbackProjection::Landing { settlement, .. } =>
                    settlement.as_ref().is_none_or(|witness| {
                        witness.receipt != witness.proof && bounded_readback_id(&witness.receipt)
                            && bounded_readback_id(&witness.proof) && witness.body_sha256 != [0; 32]
                    }),
            }
    }
}

impl OriginalFamilyReadbackGrant {
    fn valid(&self, incarnation: [u8; 16]) -> bool {
        self.line.valid(incarnation)
            && self.line.created > self.line.writer.basis
            && self.generation != [0; 16]
            && (16..=64).contains(&self.creation_key.len())
            && self.observation.is_none_or(|basis| {
                Basis::from_bytes(&basis).is_ok_and(|at| at.incarnation() == incarnation)
                    && basis >= self.line.created
            })
            && self.witness.as_ref().is_none_or(|witness| witness.valid(self.line.created))
            && self.carrier.as_ref().is_none_or(|point| {
                let bounded_identifier = |name: &str| valid_identity(name) && !name.contains(['.', '/']);
                point.valid_at != Timestamp::MAX
                    && bounded_identifier(&point.table.schema)
                    && bounded_identifier(&point.table.table)
                    && matches!(&point.id, rtdb_types::EntityId::String(id) if bounded_readback_id(id))
            })
    }
}

fn bounded_readback_id(id: &str) -> bool {
    (1..=1024).contains(&id.len()) && !id.chars().any(char::is_control)
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
    original_family_readback: Option<OriginalFamilyReadbackGrant>,
    store_fence: Option<StoreFence>,
}

/// Bijection's managed placement fence (grant version 6): ordinary
/// `rtdb.store.v1` access to the database, for closing its writer row at
/// one placement epoch. Bijection owns the fence's meaning; RTDB only
/// admits the store calls.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "role", rename_all = "kebab-case", deny_unknown_fields)]
pub enum StoreFence {
    FencePlacement { epoch: u64 },
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
    OriginalFamilyReadback { scope: Box<OriginalFamilyReadbackGrant>, point: Point },
    FundedAppointment { scope: Box<FundedAppointmentScope>, point: Point },
    StoreFence(StoreFence),
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
        let store_fence_fields = ["operations", "authority", "original_custody", "original_family", "original_family_readback"];
        let permission = if claim.version == 6 {
            match (claim.store_fence, config.assignment()) {
                (Some(fence @ StoreFence::FencePlacement { epoch }), Some(_))
                    if epoch > 0 && store_fence_fields.iter().all(|field| claim_field_absent(raw, field)) =>
                    Permission::StoreFence(fence),
                _ => return Err(invalid_grant()),
            }
        } else if !claim_field_absent(raw, "store_fence") {
            return Err(invalid_grant());
        } else if config.original_family_readback {
            match (claim.version, claim.original_family_readback, config.assignment()) {
                (5, Some(scope), Some(point)) if ["operations", "authority", "original_custody", "original_family"]
                    .iter().all(|field| claim_field_absent(raw, field)) && scope.valid(claim.incarnation) =>
                    Permission::OriginalFamilyReadback { scope: Box::new(scope), point: point.clone() },
                _ => return Err(invalid_grant()),
            }
        } else if !claim_field_absent(raw, "original_family_readback") {
            return Err(invalid_grant());
        } else if config.original_family {
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
        if let Permission::OriginalFamilyReadback { scope, point } = &permission {
            hash.update(b"rtdb.v1/original-family-readback\0");
            hash.update(serde_json::to_vec(&(scope, point)).map_err(|_| invalid_grant())?);
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
            || self.is_store_fence()
    }

    /// A Bijection placement fence credential: `rtdb.store.v1` calls only.
    pub fn is_store_fence(&self) -> bool {
        matches!(self.permission, Permission::StoreFence(_))
    }

    /// Admit a store call of a placement fence credential on `database` at
    /// its current `incarnation`, while the grant lives.
    pub fn admit_store_fence(&self, database: &str, incarnation: [u8; 16], now: Timestamp) -> Result<()> {
        if !self.is_store_fence() || self.database != database || self.incarnation != incarnation {
            return Err(invalid_grant());
        }
        self.revalidate(now)
    }

    pub fn is_original_custody_control(&self) -> bool {
        matches!(self.permission, Permission::OriginalCustody(_))
    }

    pub fn is_original_private_replay(&self) -> bool {
        matches!(&self.permission, Permission::OriginalCustody(control)
            if control.operation == OriginalCustodyOperation::PrivateReplay)
    }

    pub fn is_original_family(&self) -> bool {
        matches!(self.permission, Permission::OriginalFamily { .. })
    }

    pub fn is_original_family_readback(&self) -> bool {
        matches!(self.permission, Permission::OriginalFamilyReadback { .. })
    }

    pub fn original_family_readback(
        &self, database: &str, authority: CustodyCarrierAuthority, now: Timestamp,
    ) -> Result<(&OriginalFamilyReadbackGrant, &Point, Timestamp)> {
        self.revalidate(now)?;
        match &self.permission {
            Permission::OriginalFamilyReadback { scope, point }
                if self.database == database && scope.line.authority == authority =>
                Ok((scope, point, self.expires_at)),
            _ => Err(invalid_grant()),
        }
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
            Permission::Create(_) | Permission::OriginalCustody(_) | Permission::StoreFence(_) => 0,
            Permission::Assignment { .. } | Permission::OriginalFamily { .. } | Permission::FundedAppointment { .. } => 8192,
            Permission::OriginalFamilyReadback { .. } => 16 * 1024,
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
            "grant_id": ([8; 16]), "expires_at_micros": expires_at_micros, "authority": authority,
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
        changed["authority"]["writer"]["namespace"] = json!(([9; 32]));
        assert_ne!(first.receipt_namespace(), verify(&changed, &enrolled).unwrap().receipt_namespace());
    }

    #[test]
    fn a_store_fence_grant_admits_only_its_database_and_incarnation_from_the_assignment_issuer() {
        let incarnation = [1; 16];
        let raw = |fence: serde_json::Value| json!({
            "version": 6, "audience": "assignment", "database": "exact", "incarnation": incarnation,
            "grant_id": ([2; 16]), "expires_at_micros": 80, "store_fence": fence,
        });
        let verify = |raw: &serde_json::Value, config: &WorkloadGrantConfig| VerifiedWorkloadGrant::from_userinfo(
            raw, "https://assignment.example.invalid/", config, "fence", Timestamp(100), Timestamp(0),
        );
        let point = Point::new("public.writer", "authority".into(), Timestamp(0)).unwrap();
        let unassigned = WorkloadGrantConfig::new("assignment".into(), "assignment-".into()).unwrap();
        let assignment = unassigned.clone().with_assignment(point);
        let fence = json!({ "role": "fence-placement", "epoch": 2 });
        assert!(verify(&raw(fence.clone()), &unassigned).is_err());
        let grant = verify(&raw(fence.clone()), &assignment).unwrap();
        assert!(grant.is_store_fence() && grant.is_assignment_issued() && !grant.is_assignment());
        assert!(grant.admit_store_fence("exact", incarnation, Timestamp(0)).is_ok());
        assert!(grant.admit_store_fence("other", incarnation, Timestamp(0)).is_err());
        assert!(grant.admit_store_fence("exact", [3; 16], Timestamp(0)).is_err());
        assert!(grant.admit_store_fence("exact", incarnation, Timestamp(80)).is_err());
        assert!(grant.assignment("exact", incarnation, Timestamp(0)).is_err());
        assert!(verify(&raw(json!({ "role": "fence-placement", "epoch": 0 })), &assignment).is_err());
        let mut mixed = raw(fence.clone());
        mixed["operations"] = json!(["create-deployment"]);
        assert!(verify(&mixed, &assignment).is_err());
        let mut older = raw(fence);
        older["version"] = json!(2);
        assert!(verify(&older, &assignment).is_err());
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
            "grant_id": ([11; 16]), "expires_at_micros": 100, "original_family": scope,
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
            "grant_id": ([11; 16]), "expires_at_micros": 100,
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
    #[test]
    fn original_family_readback_credential_is_disjoint_and_pins_literal_selectors() {
        let incarnation = [1; 16];
        let authority = CustodyCarrierAuthority {
            incarnation, installation: [2; 16], source_history: [3; 32], registry: [4; 32], original_writer: [5; 32],
        };
        let applied = |position| Applied {
            namespace: [6; 32], key: vec![7; 16], basis: Basis::new(incarnation, position).to_bytes(),
            system_time_micros: position as i64,
        };
        let line = OriginalFamilyGrant {
            authority, branch: "draft".into(), created: Basis::new(incarnation, 3).to_bytes(),
            family: [8; 16], group: [9; 16], writer: applied(2),
            fence: Fence { applied: applied(1), placement: Placement { epoch: 1,
                assignment: Some(Holder { assignment_id: [10; 16], holder: "holder".into() }),
            }},
        };
        let point = Point::new("public.writer", "authority".into(), Timestamp(0)).unwrap();
        let issuer = WorkloadGrantConfig::new("readback".into(), "readback-".into()).unwrap();
        let v4 = issuer.clone().for_original_family(point.clone());
        let v5 = issuer.clone().for_original_family_readback(point);
        let mut raw = json!({
            "version": 5, "audience": "readback", "database": "exact", "incarnation": incarnation,
            "grant_id": ([11; 16]), "expires_at_micros": 100,
            "original_family_readback": {
                "line": line, "generation": ([12; 16]), "creation_key": ([13; 16]),
                "carrier": Point::new("app.protected_carriers", "selected".into(), Timestamp(20)).unwrap(),
                "observation": Basis::new(incarnation, 4).to_bytes(),
                "witness": { "receipt": applied(5), "placement": ([14; 16]), "projection": { "kind": "reserve" } },
            },
        });
        let verify = |raw: &serde_json::Value, config: &WorkloadGrantConfig| VerifiedWorkloadGrant::from_userinfo(
            raw, "https://readback.example.invalid/", config, "owner", Timestamp(90), Timestamp(0),
        );
        assert!(verify(&raw, &v4).is_err());
        assert!(verify(&raw, &issuer).is_err());
        let grant = verify(&raw, &v5).unwrap();
        assert!(grant.is_original_family_readback());
        assert!(!grant.is_original_family());
        assert!(grant.assignment("exact", incarnation, Timestamp(0)).is_err());
        assert!(grant.original_family("exact", authority, Timestamp(0)).is_err());
        assert!(grant.original_family_readback("other", authority, Timestamp(0)).is_err());
        assert!(grant.original_family_readback("exact", authority, Timestamp(90)).is_err());
        let mut v4_raw = raw.clone();
        let family = v4_raw.as_object_mut().unwrap().remove("original_family_readback").unwrap();
        v4_raw["version"] = json!(4);
        v4_raw["original_family"] = family["line"].clone();
        assert!(verify(&v4_raw, &v5).is_err());
        assert!(verify(&v4_raw, &v4).is_ok());
        let mut mixed = raw.clone();
        mixed["authority"] = serde_json::Value::Null;
        assert!(verify(&mixed, &v5).is_err());
        let mut foreign = raw.clone();
        foreign["original_family_readback"]["witness"]["receipt"]["basis"] = json!(Basis::new([2; 16], 5).to_bytes());
        assert!(verify(&foreign, &v5).is_err());
        raw["original_family_readback"]["carrier"]["table"]["table"] = json!("another_carrier");
        assert!(verify(&raw, &v5).is_ok());
        raw["original_family_readback"]["carrier"]["table"]["table"] = json!("app.another_carrier");
        assert!(verify(&raw, &v5).is_err());
        raw["original_family_readback"]["carrier"]["table"]["table"] = json!("another_carrier");
        raw["original_family_readback"]["carrier"]["id"] = json!({ "String": "x".repeat(1025) });
        assert!(verify(&raw, &v5).is_err());
        raw["original_family_readback"]["carrier"]["id"] = json!({ "Integer": 1 });
        assert!(verify(&raw, &v5).is_err());
    }

}
