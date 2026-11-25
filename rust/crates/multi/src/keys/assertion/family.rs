//! Private current custody for a Proposal occurrence and its landed copies.

mod release;
mod root_landing;

pub use release::{CarrierJournalIdentity, FamilyReleaseAuthority, FamilyReleaseInput, FamilyReleaseObservation, FamilyReleaseRegistration, FamilyResponsePart};

use std::{collections::BTreeSet, fmt, fs, io::ErrorKind, sync::Arc};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use ring::digest::{Context, SHA256, digest};
use rtdb_node::{AssertionFenceStatus, OriginalAssertionFamilyReader};
use rtdb_storage::IoRegion;
use tokio_util::sync::CancellationToken;
use rtdb_types::{Anomaly, AnomalyCategory, AssertionFamilyFenceMarker, AssertionId, EntityId, OwnedRelation, PlacedAttempt, PlacementBinding, Position, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{AssertionAuthoring, AssertionEnvelope, AssertionKeyService, AssertionStage, LocalKeyService, io};
use super::issuer::AssertionIssuerScope;
use crate::keys::{assertion_deadline::AssertionDeadline, malformed};

pub(super) const LOCAL_PREFIX: &str = "assertion-family-";
pub(super) const LOCAL_ISSUER_PREFIX: &str = "assertion-issuer-";
pub(in crate::keys) const MAX_RECORD_BYTES: usize = 1024 * 1024;
const MAX_DERIVED_ROWS: usize = 2048;
const ATTEMPTS: usize = 8;
const DENIED: &[u8] = b"RTFD\x01";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FamilyOccurrence {
    #[serde(with = "position")]
    pub position: Position,
    #[serde(with = "assertion_id")]
    pub assertion: AssertionId,
    pub capability: AssertionStage,
}

/// The ordered review owner supplies the admitted slot commitment. A separate
/// capability protects the link control so its ciphertext can outlive the link.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LandingReservation {
    #[serde(with = "position")]
    pub source: Position,
    #[serde(with = "assertion_id")]
    pub destination: AssertionId,
    pub capability: AssertionStage,
    pub link_capability: AssertionStage,
    pub review_commitment: [u8; 32],
}

/// The private control body for a reviewed source slot and its new placement.
/// The ordered batch publishes only its separately sealed envelope.
pub struct LandingLink {
    pub intent: [u8; 32],
    pub source: Position,
    pub destination: Position,
    pub family: Uuid,
    pub review_slot: u64,
}

impl LandingLink {
    const HEADER: &[u8] = b"RTLL\x01";
    const CODEC: u32 = u32::from_be_bytes(*b"LNK1");

    pub async fn seal(
        &self, reservation: LandingReservation, destination: PlacementBinding,
        destination_scope: [u8; 32], keys: &dyn AssertionKeyService, region: &Arc<IoRegion>,
    ) -> Result<AssertionEnvelope> {
        if self.family.is_nil() || self.intent == [0; 32]
            || self.source != reservation.source || self.destination != destination.position
            || destination.assertion != reservation.destination
            || destination.stage != *reservation.capability.stage.as_bytes()
            || reservation.link_capability == reservation.capability
            || reservation.link_capability.deployment != reservation.capability.deployment
            || reservation.link_capability.incarnation != reservation.capability.incarnation
        {
            return Err(conflict());
        }
        let mut body = Vec::with_capacity(Self::HEADER.len() + 32 + 2 * Position::WIDTH + 16 + 8);
        body.extend_from_slice(Self::HEADER);
        body.extend_from_slice(&self.intent);
        body.extend_from_slice(&self.source.to_bytes());
        body.extend_from_slice(&self.destination.to_bytes());
        body.extend_from_slice(self.family.as_bytes());
        body.extend_from_slice(&self.review_slot.to_be_bytes());
        let authoring = AssertionAuthoring {
            assertion_id: reservation.destination,
            scope_commitment: destination_scope,
            codec: Self::CODEC,
        };
        keys.seal_assertion_stage(reservation.link_capability, authoring, region, body.into()).await
    }
}

/// A member's row in a derived search relation. A world row has its own
/// enrolled capability; a Root row is sealed under its relation's slots.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivedRow {
    #[serde(with = "assertion_id")]
    pub member: AssertionId,
    pub world: Option<Uuid>,
    pub relation: String,
    pub key: Vec<u8>,
    pub capability: Option<AssertionStage>,
}

impl DerivedRow {
    pub(crate) const PROPOSAL_REPLAY_RELATION: &str = "bj_fact_root_admission";

    fn is_proposal_replay(&self) -> bool {
        self.relation == Self::PROPOSAL_REPLAY_RELATION && self.world.is_some()
            && self.capability.is_some() && self.key.len() == 64
            && self.key.iter().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    }

    fn same_row(&self, other: &Self) -> bool {
        self.world == other.world && self.relation == other.relation && self.key == other.key
    }

    fn well_formed(&self) -> bool {
        !self.relation.is_empty() && self.relation.len() <= 255 && !self.key.is_empty() && self.key.len() <= 512
            && self.world.is_some() == self.capability.is_some() && self.world.is_none_or(|world| !world.is_nil())
    }
}

/// The exact world overlay row an enrolled capability seals or opens.
#[derive(Clone, Copy, Debug)]
pub struct WorldRowSlot<'a> {
    pub world: Uuid,
    pub relation: &'a str,
    pub key: &'a [u8],
}

pub(crate) fn assertion_root_line(deployment: Uuid, log_epoch: i32) -> [u8; 16] {
    let mut line = Context::new(&SHA256);
    line.update(b"rtdb.owned/assertion-line/v1\0");
    line.update(deployment.as_bytes());
    line.update(&log_epoch.to_be_bytes());
    let mut root_line = [0; 16];
    root_line.copy_from_slice(&line.finish().as_ref()[..16]);
    root_line
}

pub(crate) fn family_vault_address(deployment: Uuid, family: Uuid) -> Uuid {
    let mut address = Context::new(&SHA256);
    address.update(b"rtdb.owned/assertion-family/v1\0");
    address.update(deployment.as_bytes());
    address.update(family.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&address.finish().as_ref()[..16]);
    Uuid::from_bytes(bytes)
}

pub(crate) fn world_row_scope(slot: WorldRowSlot<'_>, disclosure: &[u8; 32]) -> [u8; 32] {
    let mut scope = world_row_commitment(slot, b"rtdb.owned/world-overlay-row/v1\0");
    scope.update(disclosure);
    scope.finish().as_ref().try_into().expect("SHA-256 commitment")
}

pub(crate) fn world_row_slot_commitment(slot: WorldRowSlot<'_>) -> [u8; 32] {
    world_row_commitment(slot, b"rtdb.owned/world-overlay-slot/v1\0")
        .finish().as_ref().try_into().expect("SHA-256 commitment")
}

fn world_row_commitment(slot: WorldRowSlot<'_>, domain: &[u8]) -> Context {
    let mut scope = Context::new(&SHA256);
    scope.update(domain);
    scope.update(slot.world.as_bytes());
    for field in [slot.relation.as_bytes(), slot.key] {
        scope.update(&(field.len() as u16).to_be_bytes());
        scope.update(field);
    }
    scope
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FamilyFence {
    pub order: Uuid,
    pub positions: BTreeSet<Position>,
    /// Includes the capabilities of pending landings and derived world rows.
    pub assertion_capabilities: BTreeSet<AssertionStage>,
    pub link_capabilities: BTreeSet<AssertionStage>,
    pub derived: BTreeSet<DerivedRow>,
}

impl FamilyFence {
    pub fn vault_inventory(&self) -> [u8; 32] {
        let mut inventory = Context::new(&SHA256);
        inventory.update(b"rtdb.multi/family-vault-inventory/v1\0");
        inventory.update(self.order.as_bytes());
        for (role, stages) in [(0, &self.assertion_capabilities), (1, &self.link_capabilities)] {
            for stage in stages {
                inventory.update(&[role]);
                inventory.update(stage.deployment.as_bytes());
                inventory.update(&stage.incarnation.get().to_be_bytes());
                inventory.update(stage.stage.as_bytes());
            }
        }
        for position in &self.positions {
            inventory.update(&[2]);
            inventory.update(&position.to_bytes());
        }
        for row in &self.derived {
            inventory.update(&[3]);
            inventory.update(&row.member.to_bytes());
            match row.world {
                Some(world) => { inventory.update(&[1]); inventory.update(world.as_bytes()); }
                None => inventory.update(&[0]),
            }
            inventory.update(&(row.relation.len() as u16).to_be_bytes());
            inventory.update(row.relation.as_bytes());
            inventory.update(&(row.key.len() as u16).to_be_bytes());
            inventory.update(&row.key);
            match row.capability {
                Some(stage) => {
                    inventory.update(&[1]);
                    inventory.update(stage.deployment.as_bytes());
                    inventory.update(&stage.incarnation.get().to_be_bytes());
                    inventory.update(stage.stage.as_bytes());
                }
                None => inventory.update(&[0]),
            }
        }
        let mut result = [0; 32];
        result.copy_from_slice(inventory.finish().as_ref());
        result
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Landing {
    reservation: LandingReservation,
    accepted: Option<AcceptedLanding>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcceptedLanding {
    #[serde(with = "position")]
    position: Position,
    placement_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrolledWorldRow {
    pub world: Uuid,
    #[serde(with = "assertion_id")]
    pub assertion: AssertionId,
    pub capability: AssertionStage,
    pub scope_commitment: [u8; 32],
    pub slot_commitment: [u8; 32],
    pub codec: u32,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
struct OriginalPosition(#[serde(with = "position")] Position);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorldRewrite {
    #[serde(with = "assertion_id")]
    member: AssertionId,
    row: EnrolledWorldRow,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingFence {
    client_family: Uuid,
    attempt: Vec<u8>,
    marker: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Family {
    original: EnrolledWorldRow,
    original_position: Option<OriginalPosition>,
    rewrites: Vec<WorldRewrite>,
    landings: Vec<Landing>,
    derived: Vec<DerivedRow>,
    fenced: Option<Uuid>,
    pending_fence: Option<PendingFence>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    release_holds: Vec<release::ReleaseHold>,
    #[serde(default)]
    release_sequence: u64,
}

impl Family {
    fn original_at(&self, position: Position) -> bool {
        self.original_position.is_some_and(|original| original.0 == position)
    }

    fn contains(&self, position: Position) -> bool {
        self.original_at(position)
            || self.landings.iter().any(|landing| landing.accepted.as_ref().is_some_and(|accepted| accepted.position == position))
    }

    fn admitted_member(&self, assertion: AssertionId) -> Option<FamilyOccurrence> {
        if self.original.assertion == assertion {
            return Some(FamilyOccurrence {
                position: self.original_position?.0, assertion, capability: self.original.capability,
            });
        }
        let landing = self.landings.iter().find(|landing| landing.reservation.destination == assertion)?;
        Some(FamilyOccurrence {
            position: landing.accepted.as_ref()?.position, assertion, capability: landing.reservation.capability,
        })
    }

    fn has_member(&self, assertion: AssertionId) -> bool {
        self.original.assertion == assertion
            || self.landings.iter().any(|landing| landing.reservation.destination == assertion)
    }

    fn stage_used(&self, stage: AssertionStage) -> bool {
        self.original.capability == stage || self.landings.iter().any(|landing| {
            landing.reservation.capability == stage || landing.reservation.link_capability == stage
        }) || self.rewrites.iter().any(|rewrite| rewrite.row.capability == stage)
            || self.derived.iter().any(|row| row.capability == Some(stage))
    }

    fn derived_capability(&self, stage: AssertionStage, assertion: AssertionId, slot: WorldRowSlot<'_>) -> bool {
        self.derived.iter().any(|row| {
            row.capability == Some(stage) && row.member == assertion && row.world == Some(slot.world)
                && row.relation == slot.relation && row.key == slot.key
        })
    }

    fn fence_inventory(&self, order: Uuid) -> FamilyFence {
        let mut positions = self.original_position.map(|position| position.0).into_iter().collect::<BTreeSet<_>>();
        let mut assertion_capabilities = BTreeSet::from([self.original.capability]);
        let mut link_capabilities = BTreeSet::new();
        assertion_capabilities.extend(self.rewrites.iter().map(|rewrite| rewrite.row.capability));
        for landing in &self.landings {
            positions.extend(landing.accepted.as_ref().map(|accepted| accepted.position));
            assertion_capabilities.insert(landing.reservation.capability);
            link_capabilities.insert(landing.reservation.link_capability);
        }
        assertion_capabilities.extend(self.derived.iter().filter_map(|row| row.capability));
        let derived = self.derived.iter().cloned().collect();
        FamilyFence { order, positions, assertion_capabilities, link_capabilities, derived }
    }

    fn require_unclaimed(&self) -> Result<()> {
        if self.pending_fence.is_some() {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/assertion-family-fence-pending",
                "An original source attempt holds the family inventory"));
        }
        Ok(())
    }

    fn require_live(&self) -> Result<()> {
        if self.fenced.is_some() {
            return Err(rtdb_storage::Error::ErasedUnderView.into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(in crate::keys) enum AssertionVaultRecord {
    Family(Uuid),
    Denial(Position),
    Issuer(AssertionIssuerScope),
    ReleaseTerminal { family: Uuid, attempt: [u8; 32] },
}

impl AssertionVaultRecord {
    fn local_prefix(self) -> &'static str {
        match self {
            Self::Family(_) | Self::Denial(_) | Self::ReleaseTerminal { .. } => LOCAL_PREFIX,
            Self::Issuer(_) => LOCAL_ISSUER_PREFIX,
        }
    }
}

impl fmt::Display for AssertionVaultRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Family(id) => write!(formatter, "{id}"),
            Self::Denial(position) => {
                let address = URL_SAFE_NO_PAD.encode(digest(&SHA256, &position.to_bytes()));
                write!(formatter, "denied-{address}")
            }
            Self::Issuer(scope) => formatter.write_str(&URL_SAFE_NO_PAD.encode(scope.address())),
            Self::ReleaseTerminal { family, attempt } =>
                write!(formatter, "release-{family}-{}", URL_SAFE_NO_PAD.encode(attempt)),
        }
    }
}

#[async_trait::async_trait]
pub(in crate::keys) trait AssertionVaultJournal: Send + Sync {
    async fn read_record(&self, record: AssertionVaultRecord, region: &Arc<IoRegion>) -> Result<Option<(Bytes, String)>>;
    async fn write_record(
        &self, record: AssertionVaultRecord, region: &Arc<IoRegion>, previous: Option<&str>, bytes: Bytes,
    ) -> Result<bool>;
}

/// Original vault operations for the trusted publication and reaper owners.
pub struct AssertionFamilies<'a> {
    journal: &'a dyn AssertionVaultJournal,
    region: Arc<IoRegion>,
    original: Option<OriginalFenceRead<'a>>,
    release_authority: Option<&'a dyn FamilyReleaseAuthority>,
}

struct OriginalFenceRead<'a> {
    reader: &'a OriginalAssertionFamilyReader,
    deployment: Uuid,
    cancellation: &'a CancellationToken,
}

impl<'a> AssertionFamilies<'a> {
    pub(in crate::keys) fn new(journal: &'a dyn AssertionVaultJournal, region: Arc<IoRegion>) -> Self {
        Self { journal, region, original: None, release_authority: None }
    }

    pub fn with_original_reader(
        mut self, reader: &'a OriginalAssertionFamilyReader, deployment: Uuid, cancellation: &'a CancellationToken,
    ) -> Self {
        self.original = Some(OriginalFenceRead { reader, deployment, cancellation });
        self
    }

    pub async fn enroll_original(&self, family: Uuid, original: EnrolledWorldRow) -> Result<()> {
        for _ in 0..ATTEMPTS {
            if let Some((prior, _)) = self.read_current(family, true).await? {
                prior.require_live()?;
                prior.require_unclaimed()?;
                return if prior.original == original { Ok(()) } else { Err(conflict()) };
            }
            let record = Family {
                original, original_position: None, rewrites: Vec::new(),
                landings: Vec::new(), derived: Vec::new(), fenced: None, pending_fence: None, release_holds: Vec::new(), release_sequence: 0,
            };
            if self.write(family, None, &record).await? {
                return Ok(());
            }
        }
        Err(contention())
    }

    pub async fn register(&self, family: Uuid, original: FamilyOccurrence) -> Result<()> {
        self.require_position_open(original.position).await?;
        self.update(family, |record| {
            record.require_live()?;
            if record.original.assertion != original.assertion || record.original.capability != original.capability
                || record.original.world.as_bytes() != &original.position.line_generation
            {
                return Err(conflict());
            }
            if record.original_position.is_some() {
                return if record.original_at(original.position) { Ok(((), false)) } else { Err(conflict()) };
            }
            if record.contains(original.position) {
                return Err(conflict());
            }
            record.original_position = Some(OriginalPosition(original.position));
            Ok(((), true))
        }).await
    }

    pub async fn enroll_rewrite(&self, family: Uuid, member: AssertionId, row: EnrolledWorldRow) -> Result<()> {
        self.update(family, |record| {
            record.require_live()?;
            if let Some(prior) = record.rewrites.iter().find(|prior| prior.row.capability == row.capability) {
                return if prior.row == row && prior.member == member { Ok(((), false)) } else { Err(conflict()) };
            }
            if !record.has_member(member) || record.stage_used(row.capability) {
                return Err(conflict());
            }
            record.rewrites.push(WorldRewrite { member, row });
            Ok(((), true))
        }).await
    }

    /// Enroll both destination and link stages before either can be provisioned.
    pub async fn reserve_landing(&self, family: Uuid, reservation: LandingReservation) -> Result<()> {
        self.update(family, |record| {
            record.require_live()?;
            if !record.contains(reservation.source) || reservation.review_commitment == [0; 32]
                || reservation.capability == reservation.link_capability
                || reservation.capability.deployment != reservation.link_capability.deployment
                || reservation.capability.incarnation != reservation.link_capability.incarnation
            {
                return Err(conflict());
            }
            if let Some(prior) = record.landings.iter().find(|entry| {
                entry.reservation.destination == reservation.destination
            }) {
                return if prior.reservation == reservation { Ok(((), false)) } else { Err(conflict()) };
            }
            if record.original.assertion == reservation.destination
                || record.stage_used(reservation.capability)
                || record.stage_used(reservation.link_capability)
            {
                return Err(conflict());
            }
            record.landings.push(Landing { reservation, accepted: None });
            Ok(((), true))
        }).await
    }

    /// Called after the owner has independently authenticated the same ordered
    /// destination binding and private link. This operation serializes against
    /// the current fence and does not verify publication evidence itself.
    pub async fn activate_ordered_landing(
        &self, family: Uuid, placement: PlacementBinding, reservation: LandingReservation,
    ) -> Result<()> {
        self.require_position_open(placement.position).await?;
        self.update(family, |record| {
            record.require_live()?;
            if placement.assertion != reservation.destination
                || placement.stage != *reservation.capability.stage.as_bytes()
            {
                return Err(conflict());
            }
            let index = record.landings.iter().position(|landing| landing.reservation == reservation)
                .ok_or_else(conflict)?;
            let mut placement_digest = [0; 32];
            placement_digest.copy_from_slice(digest(&SHA256, &placement.to_bytes()).as_ref());
            if let Some(prior) = &record.landings[index].accepted {
                return if prior.placement_digest == placement_digest {
                    Ok(((), false))
                } else {
                    Err(conflict())
                };
            }
            if record.contains(placement.position) {
                return Err(conflict());
            }
            record.landings[index].accepted = Some(AcceptedLanding {
                position: placement.position, placement_digest,
            });
            Ok(((), true))
        }).await
    }

    /// Locate the admitted member of a reviewed world rewrite.
    pub async fn resolve_member(&self, family: Uuid, assertion: AssertionId) -> Result<FamilyOccurrence> {
        let (record, _) = self.read_current(family, true).await?.ok_or_else(conflict)?;
        record.require_live()?;
        let occurrence = record.admitted_member(assertion).ok_or_else(conflict)?;
        self.require_position_open(occurrence.position).await?;
        Ok(occurrence)
    }

    /// Current check for the exact occurrence already admitted for release.
    /// The caller owns the final release fence and draining previously admitted work.
    pub async fn require_live_occurrence(&self, family: Uuid, occurrence: FamilyOccurrence) -> Result<()> {
        self.require_position_open(occurrence.position).await?;
        let (record, _) = self.read_current(family, true).await?.ok_or_else(conflict)?;
        record.require_live()?;
        if (record.original_at(occurrence.position) && record.original.assertion == occurrence.assertion
            && record.original.capability == occurrence.capability) || record.landings.iter().any(|landing| {
            landing.reservation.destination == occurrence.assertion
                && landing.reservation.capability == occurrence.capability
                && landing.accepted.as_ref().is_some_and(|accepted| accepted.position == occurrence.position)
        }) {
            Ok(())
        } else {
            Err(conflict())
        }
    }

    /// Enrolls derived search rows of a member. Each world row's capability is
    /// fresh within the family and later seals exactly that row.
    pub async fn register_derived(&self, family: Uuid, rows: &[DerivedRow]) -> Result<()> {
        if rows.is_empty() {
            return Err(conflict());
        }
        self.update(family, |record| {
            record.require_live()?;
            let mut changed = false;
            for row in rows {
                if !row.well_formed() || !(row.relation.ends_with("_search") || row.is_proposal_replay())
                    || !record.has_member(row.member)
                {
                    return Err(conflict());
                }
                if row.world.is_none() {
                    let selector = rtdb_types::RootFamilyDerivativeId::parse(&row.key).ok().flatten()
                        .ok_or_else(conflict)?;
                    if selector.member != row.member
                        || !record.landings.iter().any(|landing| landing.reservation.destination == row.member)
                    {
                        return Err(conflict());
                    }
                }
                if record.derived.contains(row) {
                    continue;
                }
                if record.derived.iter().any(|prior| prior.same_row(row)
                    && (row.world.is_none() || prior.member != row.member))
                {
                    return Err(conflict());
                }
                if row.capability.is_some_and(|stage| record.stage_used(stage)) {
                    return Err(conflict());
                }
                if record.derived.len() >= MAX_DERIVED_ROWS {
                    return Err(too_large());
                }
                record.derived.push(row.clone());
                changed = true;
            }
            Ok(((), changed))
        }).await
    }

    /// Authenticate the exact enrolled Root slot before its first publication.
    /// The ordered transaction separately checks the absent family marker.
    pub async fn require_reserved_root_derivative(
        &self, family: Uuid, member: AssertionId, relation: &str, key: &[u8],
    ) -> Result<()> {
        let (record, _) = self.read_current(family, true).await?.ok_or_else(conflict)?;
        record.require_live()?;
        if !record.landings.iter().any(|landing| landing.reservation.destination == member)
            || !record.derived.iter().any(|row| row.member == member && row.world.is_none()
                && row.relation == relation && row.key == key)
        {
            return Err(conflict());
        }
        Ok(())
    }

    pub async fn require_row_capability(
        &self, family: Uuid, row: EnrolledWorldRow, slot: WorldRowSlot<'_>,
    ) -> Result<()> {
        let (record, _) = self.read_current(family, true).await?.ok_or_else(conflict)?;
        record.require_live()?;
        if record.original == row || record.rewrites.iter().any(|rewrite| rewrite.row == row)
            || record.landings.iter().any(|landing| {
                landing.reservation.capability == row.capability && landing.reservation.destination == row.assertion
            })
            || record.derived_capability(row.capability, row.assertion, slot)
        {
            Ok(())
        } else {
            Err(conflict())
        }
    }

    pub async fn require_derived_capability(
        &self, family: Uuid, stage: AssertionStage, member: AssertionId, slot: WorldRowSlot<'_>,
    ) -> Result<()> {
        let (record, _) = self.read_current(family, true).await?.ok_or_else(conflict)?;
        record.require_live()?;
        if record.derived_capability(stage, member, slot) { Ok(()) } else { Err(conflict()) }
    }

    pub(crate) async fn require_world_rank_occurrence(
        &self, family: Uuid, stage: AssertionStage, assertion: AssertionId, slot: WorldRowSlot<'_>,
    ) -> Result<FamilyOccurrence> {
        let (record, _) = self.read_current(family, true).await?.ok_or_else(conflict)?;
        record.require_live()?;
        let occurrence = record.admitted_member(assertion).ok_or_else(conflict)?;
        if !record.derived_capability(stage, assertion, slot)
            || occurrence.capability.deployment() != stage.deployment()
            || occurrence.capability.incarnation() != stage.incarnation()
            || occurrence.capability == stage
        {
            return Err(conflict());
        }
        self.require_position_open(occurrence.position).await?;
        Ok(occurrence)
    }

    /// Admits sealing the private link of an enrolled reservation.
    pub async fn require_reserved(&self, family: Uuid, reservation: LandingReservation) -> Result<()> {
        let (record, _) = self.read_current(family, true).await?.ok_or_else(conflict)?;
        record.require_live()?;
        if record.landings.iter().any(|landing| landing.reservation == reservation) { Ok(()) } else { Err(conflict()) }
    }

    pub async fn plan_fence(&self, family: Uuid, selected: Position, order: Uuid) -> Result<FamilyFence> {
        let (record, _) = self.read_current(family, false).await?.ok_or_else(conflict)?;
        if order.is_nil() || !record.contains(selected) {
            return Err(conflict());
        }
        let pending = record.pending_fence.as_ref().map(|pending| {
            AssertionFamilyFenceMarker::decode(&pending.marker).map_err(|_| malformed())
        }).transpose()?;
        Ok(record.fence_inventory(record.fenced.or(pending.map(|marker| marker.order)).unwrap_or(order)))
    }

    pub async fn claim_fence(
        &self, family: Uuid, client_family: Uuid, marker: AssertionFamilyFenceMarker, attempt: PlacedAttempt,
        source_predecessor: Option<rtdb_types::TransactionKey>, root_erases: &BTreeSet<(&OwnedRelation, &EntityId)>,
        winner: Option<(rtdb_node::AssertionFenceWorldWinner<'_>, &dyn AssertionKeyService)>,
    ) -> Result<()> {
        let pending = PendingFence {
            client_family, attempt: attempt.encode().to_vec(), marker: marker.to_bytes().to_vec(),
        };
        for _ in 0..ATTEMPTS {
            let (mut record, etag) = self.read(family).await?.ok_or_else(conflict)?;
            record.require_live()?;
            if let Some(prior) = &record.pending_fence {
                if prior == &pending { return Ok(()); }
                let previous = PlacedAttempt::decode(&prior.attempt).map_err(|_| malformed())?;
                if !attempt.scope().same_history(previous.scope())
                    || previous.position() >= attempt.position()
                    || source_predecessor.is_none_or(|predecessor| predecessor.tx_id.0 < previous.position().0)
                {
                    record.require_unclaimed()?;
                }
                // The resolver checked the insert's absence at this durable predecessor.
            }
            if !record.contains(marker.selected) {
                return Err(conflict());
            }
            if let Some((winner, keys)) = &winner {
                self.authenticate_fence_winner(&record, client_family, marker, winner, *keys).await?;
            } else if marker.selected != marker.winner {
                return Err(conflict());
            }
            let fence = record.fence_inventory(marker.order);
            if marker.inventory != fence.vault_inventory()
                || fence.derived.iter().filter(|row| row.world.is_none()).count() != root_erases.len()
            {
                return Err(conflict());
            }
            for row in fence.derived.into_iter().filter(|row| row.world.is_none()) {
                let relation = OwnedRelation::new(row.relation)?;
                let id = EntityId::Binary(row.key);
                if !root_erases.contains(&(&relation, &id)) {
                    return Err(conflict());
                }
            }
            record.pending_fence = Some(pending.clone());
            if self.write(family, Some(&etag), &record).await? {
                return Ok(());
            }
        }
        Err(contention())
    }

    async fn authenticate_fence_winner(
        &self, record: &Family, family: Uuid, marker: AssertionFamilyFenceMarker,
        winner: &rtdb_node::AssertionFenceWorldWinner<'_>, keys: &dyn AssertionKeyService,
    ) -> Result<()> {
        let carrier = winner.carrier.strip_prefix(b"BJWC1").ok_or_else(conflict)?;
        let (carrier_family, carrier) = carrier.split_first_chunk::<16>().ok_or_else(conflict)?;
        let (stage, carrier) = carrier.split_first_chunk::<40>().ok_or_else(conflict)?;
        let (assertion, carrier) = carrier.split_first_chunk::<32>().ok_or_else(conflict)?;
        let (member, envelope) = carrier.split_first_chunk::<32>().ok_or_else(conflict)?;
        let assertion = AssertionId::from_bytes(*assertion);
        let member = AssertionId::from_bytes(*member);
        let world = Uuid::from_bytes(winner.world.0);
        let slot = WorldRowSlot { world, relation: "fact_unit_column_payloads", key: winner.key.as_bytes() };
        if carrier_family != family.as_bytes() || winner.key.len() > usize::from(u16::MAX)
            || record.admitted_member(member).is_none_or(|occurrence| occurrence.position != marker.selected)
        {
            return Err(conflict());
        }

        let enrolled = if assertion == record.original.assertion {
            if member != assertion || marker.winner != marker.selected { return Err(conflict()); }
            &record.original
        } else {
            if marker.winner == marker.selected { return Err(conflict()); }
            &record.rewrites.iter().find(|rewrite| rewrite.member == member && rewrite.row.assertion == assertion)
                .ok_or_else(conflict)?.row
        };
        let capability = enrolled.capability;
        let stage_matches = &stage[..16] == capability.deployment().as_bytes()
            && stage[16..24] == capability.incarnation().get().to_be_bytes()
            && &stage[24..] == capability.identity().as_bytes();
        if enrolled.world != world || !stage_matches || enrolled.slot_commitment != world_row_slot_commitment(slot)
            || enrolled.codec != u32::from_be_bytes(*b"BJWR")
        {
            return Err(conflict());
        }
        let form = AssertionAuthoring { assertion_id: assertion, scope_commitment: enrolled.scope_commitment, codec: enrolled.codec };
        keys.open_assertion_for_resolution(capability, form, &self.region, Bytes::copy_from_slice(envelope).try_into()?).await?;
        Ok(())
    }

    /// The selected original, or any active landed copy, closes the same
    /// family. The custody roster includes enrolled stages even before order.
    pub async fn fence(&self, family: Uuid, selected: Position, order: Uuid) -> Result<FamilyFence> {
        self.update(family, |record| {
            if order.is_nil() || !record.contains(selected) {
                return Err(conflict());
            }
            let first_order = record.fenced.unwrap_or(order);
            let changed = record.fenced.is_none();
            record.fenced = Some(first_order);
            Ok((record.fence_inventory(first_order), changed))
        }).await
    }

    /// Remove private membership only after signed deadline settlement and
    /// after every original position has its own durable denial.
    pub async fn settle(
        &self, family: Uuid, deadline: &AssertionDeadline<'_>, custody_key: &[u8; 32],
    ) -> Result<()> {
        for _ in 0..ATTEMPTS {
            let (bytes, etag) = self.read_record_bytes(family).await?.ok_or_else(conflict)?;
            if bytes.as_ref() == DENIED {
                return Ok(());
            }
            let record = decode_family(&bytes)?;
            if !record.release_holds.is_empty() { return Err(conflict()); }
            let fence = record.fence_inventory(record.fenced.ok_or_else(conflict)?);
            deadline.require_settled_family(&fence, custody_key).await?;
            for position in fence.positions {
                let address = AssertionVaultRecord::Denial(position);
                if !self.journal.write_record(address, &self.region, None, Bytes::from_static(DENIED)).await? {
                    self.require_denial(address).await?;
                }
            }
            if self.journal.write_record(AssertionVaultRecord::Family(family), &self.region,
                Some(&etag), Bytes::from_static(DENIED)).await?
            {
                return Ok(());
            }
        }
        Err(contention())
    }

    pub async fn require_position_open(&self, position: Position) -> Result<()> {
        if let Some((bytes, _)) = self.journal.read_record(AssertionVaultRecord::Denial(position), &self.region).await? {
            if bytes.as_ref() != DENIED {
                return Err(malformed());
            }
            return Err(rtdb_storage::Error::ErasedUnderView.into());
        }
        Ok(())
    }

    async fn require_denial(&self, address: AssertionVaultRecord) -> Result<()> {
        match self.journal.read_record(address, &self.region).await? {
            Some((bytes, _)) if bytes.as_ref() == DENIED => Ok(()),
            _ => Err(conflict()),
        }
    }

    async fn read_record_bytes(&self, family: Uuid) -> Result<Option<(Bytes, String)>> {
        if family.is_nil() {
            return Err(conflict());
        }
        self.journal.read_record(AssertionVaultRecord::Family(family), &self.region).await
    }

    async fn read(&self, family: Uuid) -> Result<Option<(Family, String)>> {
        let Some((bytes, etag)) = self.read_record_bytes(family).await? else {
            return Ok(None);
        };
        if bytes.as_ref() == DENIED {
            return Err(rtdb_storage::Error::ErasedUnderView.into());
        }
        Ok(Some((decode_family(&bytes)?, etag)))
    }

    async fn read_current(&self, family: Uuid, recover: bool) -> Result<Option<(Family, String)>> {
        for _ in 0..ATTEMPTS {
            let Some((mut record, etag)) = self.read(family).await? else { return Ok(None); };
            let Some(pending) = record.pending_fence.as_ref() else { return Ok(Some((record, etag))); };
            let original = self.original.as_ref().ok_or_else(|| Anomaly::new(
                AnomalyCategory::Unavailable, "rtdb.multi/assertion-family-fence-source",
                "The pending family fence requires the original ordered marker",
            ))?;
            let attempt = PlacedAttempt::decode(&pending.attempt).map_err(|_| malformed())?;
            let marker = AssertionFamilyFenceMarker::decode(&pending.marker).map_err(|_| malformed())?;
            match original.reader.fence_status(
                original.deployment, pending.client_family, attempt, marker, original.cancellation,
            ).await? {
                AssertionFenceStatus::Pending => return Ok(Some((record, etag))),
                AssertionFenceStatus::Aborted => (),
                AssertionFenceStatus::Ordered => record.fenced = Some(marker.order),
            }
            record.pending_fence = None;
            if !recover {
                return Ok(Some((record, etag)));
            }
            self.write(family, Some(&etag), &record).await?;
        }
        Err(contention())
    }

    async fn write(&self, family: Uuid, previous: Option<&str>, record: &Family) -> Result<bool> {
        let bytes = serde_json::to_vec(record)
            .map_err(|_| Anomaly::fault("rtdb.multi/assertion-family-record", "Cannot encode family custody record"))?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(too_large());
        }
        self.journal.write_record(AssertionVaultRecord::Family(family), &self.region, previous, bytes.into()).await
    }

    async fn update<T>(
        &self, family: Uuid, edit: impl Fn(&mut Family) -> Result<(T, bool)>,
    ) -> Result<T> {
        for _ in 0..ATTEMPTS {
            let (mut record, etag) = self.read_current(family, true).await?.ok_or_else(conflict)?;
            record.require_unclaimed()?;
            let (result, changed) = edit(&mut record)?;
            if !changed || self.write(family, Some(&etag), &record).await? {
                return Ok(result);
            }
        }
        Err(contention())
    }
}

fn decode_family(bytes: &[u8]) -> Result<Family> {
    serde_json::from_slice(bytes)
        .map_err(|_| Anomaly::fault("rtdb.multi/assertion-family-record", "Invalid family custody record"))
}

fn conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict, "rtdb.multi/assertion-family-mismatch",
        "The assertion does not match the original family custody",
    )
}

fn too_large() -> Anomaly {
    Anomaly::incorrect("rtdb.multi/assertion-family-size", "The assertion family exceeds its custody inventory limit")
}

fn contention() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict, "rtdb.multi/assertion-family-contention",
        "The original family custody changed concurrently",
    )
}

fn local_record(vault: &LocalKeyService, path: &std::path::Path) -> Result<Option<(Bytes, String)>> {
    match fs::read(path) {
        Ok(bytes) if bytes.len() <= MAX_RECORD_BYTES => {
            vault.sync_directory()?;
            let etag = URL_SAFE_NO_PAD.encode(digest(&SHA256, &bytes));
            Ok(Some((bytes.into(), etag)))
        }
        Ok(_) => Err(malformed()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io(error)),
    }
}

#[async_trait::async_trait]
impl AssertionVaultJournal for LocalKeyService {
    async fn read_record(&self, record: AssertionVaultRecord, region: &Arc<IoRegion>) -> Result<Option<(Bytes, String)>> {
        self.require_assertion_broker()?;
        let owner = self.clone();
        region.run_blocking(move || Ok((|| {
            let vault = owner.archive_vault()?;
            let path = vault.path(&format!("{}{record}", record.local_prefix()))?;
            let _lock = vault.lock_key_shared(&path)?;
            local_record(&vault, &path)
        })())).await?
    }

    async fn write_record(
        &self, record: AssertionVaultRecord, region: &Arc<IoRegion>, previous: Option<&str>, bytes: Bytes,
    ) -> Result<bool> {
        self.require_assertion_broker()?;
        let owner = self.clone();
        let previous = previous.map(str::to_owned);
        region.run_blocking(move || Ok((|| {
            let vault = owner.archive_vault()?;
            let path = vault.path(&format!("{}{record}", record.local_prefix()))?;
            let _lock = vault.lock_key(&path)?;
            if local_record(&vault, &path)?.as_ref().map(|(_, etag)| etag) != previous.as_ref() {
                return Ok(false);
            }
            vault.publish(&path, &bytes)?;
            Ok(true)
        })())).await?
    }
}

mod position {
    use rtdb_types::Position;
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

    pub fn serialize<S: Serializer>(position: &Position, serializer: S) -> Result<S::Ok, S::Error> {
        position.to_bytes().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Position, D::Error> {
        Position::from_bytes(<[u8; Position::WIDTH]>::deserialize(deserializer)?).map_err(Error::custom)
    }
}

mod assertion_id {
    use rtdb_types::AssertionId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(assertion: &AssertionId, serializer: S) -> Result<S::Ok, S::Error> {
        assertion.to_bytes().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<AssertionId, D::Error> {
        Ok(AssertionId::from_bytes(<[u8; AssertionId::WIDTH]>::deserialize(deserializer)?))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::keys::KeyService;

    fn position(publication: u64) -> Position {
        Position { line_generation: [1; 16], publication, ordinal: NonZeroU64::MIN }
    }

    fn stage(realm: Uuid) -> AssertionStage {
        AssertionStage::new(realm, NonZeroU64::MIN, Uuid::new_v4()).unwrap()
    }

    fn enrolment(original: FamilyOccurrence) -> EnrolledWorldRow {
        EnrolledWorldRow {
            world: Uuid::from_bytes(original.position.line_generation),
            assertion: original.assertion, capability: original.capability,
            scope_commitment: [9; 32], slot_commitment: [8; 32], codec: 1,
        }
    }

    fn placement(reservation: LandingReservation) -> PlacementBinding {
        PlacementBinding {
            assertion: reservation.destination,
            stage: *reservation.capability.stage.as_bytes(),
            capsule_digest: [3; 32],
            position: position(2),
            clock_commitment: [4; 32],
            address_extent_commitment: [5; 32],
            disclosure_scope: [6; 32],
            membership_commitment: [7; 32],
        }
    }

    #[tokio::test]
    async fn either_active_end_closes_the_family_and_fences_enrolled_stages() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let region = IoRegion::new();
        let families = vault.assertion_families(Arc::clone(&region)).unwrap();
        let proposal = Uuid::new_v4();
        let destination = Uuid::new_v4();
        for close_from_copy in [false, true] {
            let family = Uuid::new_v4();
            let original = FamilyOccurrence {
                position: position(1),
                assertion: AssertionId::from_bytes([1; 32]),
                capability: stage(proposal),
            };
            let first = LandingReservation {
                source: original.position,
                destination: AssertionId::from_bytes([2; 32]),
                capability: stage(destination),
                link_capability: stage(destination),
                review_commitment: [2; 32],
            };
            let pending = LandingReservation {
                destination: AssertionId::from_bytes([8; 32]),
                capability: stage(destination),
                link_capability: stage(destination),
                ..first
            };
            let binding = placement(first);
            let copy = FamilyOccurrence {
                position: binding.position, assertion: first.destination, capability: first.capability,
            };
            families.enroll_original(family, enrolment(original)).await?;
            families.register(family, original).await?;
            families.reserve_landing(family, first).await?;
            assert_eq!(families.resolve_member(family, original.assertion).await?, original);
            assert!(families.resolve_member(family, copy.assertion).await.is_err());
            assert!(families.require_live_occurrence(family, copy).await.is_err());
            families.activate_ordered_landing(family, binding, first).await?;
            families.activate_ordered_landing(family, binding, first).await?;
            let changed = PlacementBinding { address_extent_commitment: [9; 32], ..binding };
            assert!(families.activate_ordered_landing(family, changed, first).await.is_err());
            families.require_live_occurrence(family, copy).await?;
            assert_eq!(families.resolve_member(family, copy.assertion).await?, copy);
            families.reserve_landing(family, pending).await?;

            let closed = if close_from_copy { copy.position } else { original.position };
            let order = Uuid::new_v4();
            let plan = families.plan_fence(family, closed, order).await?;
            families.require_live_occurrence(family, original).await?;
            let fence = families.fence(family, closed, order).await?;
            assert_eq!(plan, fence);
            assert_eq!(fence.positions, BTreeSet::from([original.position, copy.position]));
            assert_eq!(fence.assertion_capabilities, BTreeSet::from([
                original.capability, first.capability, pending.capability,
            ]));
            assert_eq!(fence.link_capabilities, BTreeSet::from([
                first.link_capability, pending.link_capability,
            ]));

            let reopened = LocalKeyService::with_archive_broker(directory.path())?;
            let reopened = reopened.assertion_families(Arc::clone(&region)).unwrap();
            assert_eq!(reopened.fence(family, original.position, Uuid::new_v4()).await?, fence);
            assert_eq!(reopened.fence(family, copy.position, Uuid::new_v4()).await?, fence);
            assert!(reopened.register(family, original).await.is_err());
            assert!(reopened.require_live_occurrence(family, original).await.is_err());
            assert!(reopened.require_live_occurrence(family, copy).await.is_err());
            assert!(reopened.activate_ordered_landing(family, placement(pending), pending).await.is_err());
            assert!(reopened.reserve_landing(family, pending).await.is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn derived_world_rows_join_the_fence_and_close_with_their_family() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let region = IoRegion::new();
        let families = vault.assertion_families(Arc::clone(&region)).unwrap();
        let realm = Uuid::new_v4();
        let family = Uuid::new_v4();
        let original = FamilyOccurrence {
            position: position(1), assertion: AssertionId::from_bytes([1; 32]), capability: stage(realm),
        };
        let enrolled = enrolment(original);
        families.enroll_original(family, enrolled).await?;
        assert!(families.require_live_occurrence(family, original).await.is_err());
        let world = enrolled.world;
        let derived = DerivedRow {
            member: original.assertion, world: Some(world), relation: "fact_l1_search".into(),
            key: b"row".to_vec(), capability: Some(stage(realm)),
        };
        let raw_root = DerivedRow {
            member: original.assertion, world: None, relation: "fact_l1_search".into(),
            key: b"01".to_vec(), capability: None,
        };
        assert!(families.register_derived(family, &[raw_root.clone()]).await.is_err());
        families.register_derived(family, &[derived.clone()]).await?;
        families.register_derived(family, &[derived.clone()]).await?;
        let moved = DerivedRow { key: b"other".to_vec(), ..derived.clone() };
        assert!(families.register_derived(family, &[moved]).await.is_err());
        let stranger = DerivedRow { member: AssertionId::from_bytes([9; 32]), key: b"x".to_vec(), capability: Some(stage(realm)), ..derived.clone() };
        assert!(families.register_derived(family, &[stranger]).await.is_err());

        let slot = WorldRowSlot { world, relation: &derived.relation, key: &derived.key };
        let capability = derived.capability.unwrap();
        families.require_derived_capability(family, capability, original.assertion, slot).await?;
        assert!(families.require_world_rank_occurrence(family, capability, original.assertion, slot).await.is_err());
        families.require_row_capability(family, enrolled, slot).await?;
        let elsewhere = WorldRowSlot { world: Uuid::new_v4(), ..slot };
        assert!(families.require_derived_capability(family, capability, original.assertion, elsewhere).await.is_err());

        let rewrite = EnrolledWorldRow {
            assertion: AssertionId::from_bytes([2; 32]), capability: stage(realm), ..enrolled
        };
        families.enroll_rewrite(family, original.assertion, rewrite).await?;
        families.enroll_rewrite(family, original.assertion, rewrite).await?;
        families.require_row_capability(family, rewrite, slot).await?;
        let other_slot = EnrolledWorldRow { scope_commitment: [10; 32], ..rewrite };
        assert!(families.require_row_capability(family, other_slot, slot).await.is_err());

        let rewritten = DerivedRow { capability: Some(stage(realm)), ..derived.clone() };
        families.register_derived(family, std::slice::from_ref(&rewritten)).await?;
        let forged = FamilyOccurrence { position: Position { line_generation: [7; 16], ..original.position }, ..original };
        assert!(families.register(family, forged).await.is_err());
        families.register(family, original).await?;
        families.register(family, original).await?;
        families.require_live_occurrence(family, original).await?;
        assert_eq!(families.require_world_rank_occurrence(family, capability, original.assertion, slot).await?, original);
        assert!(families.require_world_rank_occurrence(family, capability, original.assertion, elsewhere).await.is_err());
        let different_incarnation = AssertionStage::new(
            capability.deployment(), NonZeroU64::new(capability.incarnation().get() + 1).unwrap(),
            capability.identity(),
        )?;
        assert!(families.require_world_rank_occurrence(family, different_incarnation, original.assertion, slot).await.is_err());
        let reservation = LandingReservation {
            source: original.position,
            destination: AssertionId::from_bytes([3; 32]),
            capability: stage(realm), link_capability: stage(realm), review_commitment: [7; 32],
        };
        families.reserve_landing(family, reservation).await?;
        let mut address = rtdb_types::RootRowCarrier::table_prefix(&[1; 16]).to_vec();
        address.extend_from_slice(&[0, 255, 0, 255]);
        address.extend_from_slice(&[2; 64]);
        address.extend_from_slice(&[0, 1]);
        let root = DerivedRow {
            member: reservation.destination,
            key: [rtdb_types::RootFamilyDerivativeId::PREFIX, &[1; 16], &[3; 32], &address].concat(),
            ..raw_root
        };
        assert!(families.require_reserved_root_derivative(family, root.member, &root.relation, &root.key).await.is_err());
        families.register_derived(family, &[root.clone()]).await?;
        families.require_reserved_root_derivative(family, root.member, &root.relation, &root.key).await?;
        assert!(families.resolve_member(family, root.member).await.is_err());
        let fence = families.fence(family, original.position, Uuid::new_v4()).await?;
        assert!(fence.assertion_capabilities.contains(&capability));
        assert!(fence.assertion_capabilities.contains(&rewrite.capability));
        assert_eq!(fence.derived, BTreeSet::from([derived.clone(), rewritten, root]));
        assert!(families.require_derived_capability(family, capability, original.assertion, slot).await.is_err());
        assert!(families.require_world_rank_occurrence(family, capability, original.assertion, slot).await.is_err());
        assert!(families.require_row_capability(family, rewrite, slot).await.is_err());
        assert!(families.register_derived(family, &[derived]).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn a_pending_fence_claim_freezes_the_exact_inventory_and_requires_its_source() -> Result<()> {
        use rtdb_types::{PlacedAttemptScope, SourcePosition};

        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let families = vault.assertion_families(IoRegion::new()).unwrap();
        let family = Uuid::new_v4();
        let original = FamilyOccurrence {
            position: position(1), assertion: AssertionId::from_bytes([1; 32]), capability: stage(Uuid::new_v4()),
        };
        families.enroll_original(family, enrolment(original)).await?;
        families.register(family, original).await?;
        let order = Uuid::new_v4();
        let plan = families.plan_fence(family, original.position, order).await?;
        let marker = AssertionFamilyFenceMarker { selected: original.position, winner: original.position, order, inventory: plan.vault_inventory() };
        let attempt = PlacedAttemptScope::new("assertion-fence", 1)?.issue().identify(SourcePosition(2), Some("owner"))?;
        let roots = BTreeSet::new();
        let stale = AssertionFamilyFenceMarker { inventory: [0; 32], ..marker };
        assert!(families.claim_fence(family, family, stale, attempt, None, &roots, None).await.is_err());
        families.require_live_occurrence(family, original).await?;
        families.claim_fence(family, family, marker, attempt, None, &roots, None).await?;
        families.claim_fence(family, family, marker, attempt, None, &roots, None).await?;
        assert!(families.require_live_occurrence(family, original).await.is_err());
        assert!(families.fence(family, original.position, order).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn a_rewrite_fence_binds_the_registered_member_and_the_exact_local_slot() -> Result<()> {
        use rtdb_types::{Iid, PlacedAttemptScope, SourcePosition};

        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let region = IoRegion::new();
        let families = vault.assertion_families(Arc::clone(&region)).unwrap();
        let family = Uuid::new_v4();
        let realm = Uuid::new_v4();
        let original = FamilyOccurrence {
            position: position(1), assertion: AssertionId::from_bytes([1; 32]), capability: stage(realm),
        };
        families.enroll_original(family, enrolment(original)).await?;
        families.register(family, original).await?;
        let world = Uuid::new_v4();
        let slot = WorldRowSlot { world, relation: "fact_unit_column_payloads", key: b"exact-slot" };
        let rewrite = EnrolledWorldRow {
            world, assertion: AssertionId::from_bytes([2; 32]), capability: stage(realm),
            scope_commitment: world_row_scope(slot, &[3; 32]),
            slot_commitment: world_row_slot_commitment(slot),
            codec: u32::from_be_bytes(*b"BJWR"),
        };
        families.enroll_rewrite(family, original.assertion, rewrite).await?;
        let authoring = AssertionAuthoring {
            assertion_id: rewrite.assertion, scope_commitment: rewrite.scope_commitment, codec: rewrite.codec,
        };
        let envelope = Bytes::from(vault.seal_assertion_stage(
            rewrite.capability, authoring, &region, Bytes::from_static(b"rewritten row"),
        ).await?);
        let mut carrier = b"BJWC1".to_vec();
        carrier.extend_from_slice(family.as_bytes());
        carrier.extend_from_slice(realm.as_bytes());
        carrier.extend_from_slice(&rewrite.capability.incarnation().get().to_be_bytes());
        carrier.extend_from_slice(rewrite.capability.identity().as_bytes());
        carrier.extend_from_slice(&rewrite.assertion.to_bytes());
        carrier.extend_from_slice(&original.assertion.to_bytes());
        carrier.extend_from_slice(&envelope);
        let plan = families.plan_fence(family, original.position, Uuid::new_v4()).await?;
        let marker = AssertionFamilyFenceMarker {
            selected: original.position, winner: Position { line_generation: *world.as_bytes(), ..position(2) },
            order: plan.order, inventory: plan.vault_inventory(),
        };
        let attempt = PlacedAttemptScope::new("assertion-fence", 1)?.issue().identify(SourcePosition(2), Some("owner"))?;
        let proof = |key| rtdb_node::AssertionFenceWorldWinner { world: Iid(*world.as_bytes()), key, carrier: &carrier };
        let roots = BTreeSet::new();
        assert!(families.claim_fence(family, family, marker, attempt, None, &roots, Some((proof("wrong-slot"), &vault))).await.is_err());
        families.claim_fence(family, family, marker, attempt, None, &roots, Some((proof("exact-slot"), &vault))).await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_link_can_be_retired_without_destroying_the_destination() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let region = IoRegion::new();
        let realm = Uuid::new_v4();
        let reservation = LandingReservation {
            source: position(1),
            destination: AssertionId::from_bytes([2; 32]),
            capability: stage(realm),
            link_capability: stage(realm),
            review_commitment: [2; 32],
        };
        let scope = [3; 32];
        let binding = placement(reservation);
        let link = LandingLink {
            intent: [4; 32], source: reservation.source, destination: binding.position,
            family: Uuid::new_v4(), review_slot: 0,
        };
        let protected = link.seal(reservation, binding, scope, &vault, &region).await?;
        assert_eq!(link.seal(reservation, binding, scope, &vault, &region).await?, protected);

        let authoring = AssertionAuthoring {
            assertion_id: reservation.destination, scope_commitment: scope, codec: 1,
        };
        let body = Bytes::from_static(b"reviewed assertion");
        let destination = vault.seal_assertion_stage(reservation.capability, authoring, &region, body.clone()).await?;
        vault.retire_assertion_stage(reservation.link_capability, &region).await?;
        assert!(link.seal(reservation, binding, scope, &vault, &region).await.is_err());
        assert_eq!(
            vault.open_assertion_for_resolution(reservation.capability, authoring, &region, destination).await?,
            body,
        );
        Ok(())
    }
}
