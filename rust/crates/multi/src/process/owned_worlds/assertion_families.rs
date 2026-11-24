//! The placed writer's assertion-family custody (dev/doc/rtdb-assertion-families.md).
//! Families are addressed within the attached deployment, and every capability
//! must name that deployment's current incarnation before the vault is touched.

use std::sync::Arc;

use bytes::Bytes;
use ring::digest::{Context, SHA256};
use rtdb_flight_sql::{
    FamilyDerivedRow, FamilyFenceReply, FamilyOccurrenceWire, FamilyReply, FamilyRequest, FamilyReservationWire,
    FamilyStage, WorldRowAuthoring,
};
use rtdb_storage::IoRegion;
use rtdb_types::{Anomaly, AnomalyCategory, OwnedWorld, Result};
use uuid::Uuid;

use crate::keys::{
    AssertionAuthoring, AssertionEnvelope, AssertionKeyService, AssertionStage, DerivedRow, FamilyOccurrence, KeyService, LandingLink,
    LandingReservation, WorldRowSlot,
};

struct Scope {
    deployment: Uuid,
    incarnation: u64,
}

impl Scope {
    fn family(&self, family: Uuid) -> Uuid {
        let mut address = Context::new(&SHA256);
        address.update(b"rtdb.owned/assertion-family/v1\0");
        address.update(self.deployment.as_bytes());
        address.update(family.as_bytes());
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&address.finish().as_ref()[..16]);
        Uuid::from_bytes(bytes)
    }

    fn stage(&self, stage: FamilyStage) -> Result<AssertionStage> {
        if stage.deployment != self.deployment || stage.incarnation.get() != self.incarnation {
            return Err(Anomaly::new(
                AnomalyCategory::Forbidden, "rtdb.owned/assertion-family-scope",
                "The assertion capability belongs to another deployment or incarnation",
            ));
        }
        AssertionStage::new(stage.deployment, stage.incarnation, stage.stage)
    }

    fn occurrence(&self, occurrence: FamilyOccurrenceWire) -> Result<FamilyOccurrence> {
        Ok(FamilyOccurrence {
            position: occurrence.position, assertion: occurrence.assertion, capability: self.stage(occurrence.capability)?,
        })
    }

    fn reservation(&self, reservation: FamilyReservationWire) -> Result<LandingReservation> {
        Ok(LandingReservation {
            source: reservation.source,
            destination: reservation.destination,
            capability: self.stage(reservation.capability)?,
            link_capability: self.stage(reservation.link_capability)?,
            review_commitment: reservation.review_commitment,
        })
    }

    fn derived(&self, row: FamilyDerivedRow) -> Result<DerivedRow> {
        Ok(DerivedRow {
            member: row.member, world: row.world, relation: row.relation, key: row.key,
            capability: row.capability.map(|stage| self.stage(stage)).transpose()?,
        })
    }
}

fn wire_stage(stage: AssertionStage) -> FamilyStage {
    FamilyStage { deployment: stage.deployment(), incarnation: stage.incarnation(), stage: stage.identity() }
}

fn wire_derived(row: DerivedRow) -> FamilyDerivedRow {
    FamilyDerivedRow {
        member: row.member, world: row.world, relation: row.relation, key: row.key,
        capability: row.capability.map(wire_stage),
    }
}

/// Binds a sealed body to exactly one world overlay row and disclosure scope.
fn row_scope(row: &WorldRowAuthoring) -> [u8; 32] {
    let mut scope = Context::new(&SHA256);
    scope.update(b"rtdb.owned/world-overlay-row/v1\0");
    scope.update(row.world.as_bytes());
    for field in [row.relation.as_bytes(), row.key.as_slice()] {
        scope.update(&(field.len() as u16).to_be_bytes());
        scope.update(field);
    }
    scope.update(&row.disclosure);
    let mut commitment = [0; 32];
    commitment.copy_from_slice(scope.finish().as_ref());
    commitment
}

pub(super) async fn serve(
    keys: &dyn KeyService, deployment: Uuid, incarnation: u64, request: FamilyRequest,
) -> Result<FamilyReply> {
    let scope = Scope { deployment, incarnation };
    let region = IoRegion::new();
    let families = keys.assertion_families(Arc::clone(&region)).ok_or_else(OwnedWorld::unavailable)?;
    let vault = || keys.assertion_keys().ok_or_else(OwnedWorld::unavailable);
    Ok(match request {
        FamilyRequest::Register { family, original } => {
            families.register(scope.family(family), scope.occurrence(original)?).await?;
            FamilyReply::Done
        }
        FamilyRequest::ReserveLanding { family, reservation } => {
            families.reserve_landing(scope.family(family), scope.reservation(reservation)?).await?;
            FamilyReply::Done
        }
        FamilyRequest::ActivateLanding { family, placement, reservation } => {
            families.activate_ordered_landing(scope.family(family), placement, scope.reservation(reservation)?).await?;
            FamilyReply::Done
        }
        FamilyRequest::SealLandingLink { family, reservation, placement, destination_scope, intent, review_slot } => {
            let family = scope.family(family);
            let reservation = scope.reservation(reservation)?;
            families.require_reserved(family, reservation).await?;
            let link = LandingLink {
                intent, source: reservation.source, destination: placement.position, family, review_slot,
            };
            let envelope = link.seal(reservation, placement, destination_scope, vault()?, &region).await?;
            FamilyReply::Sealed(Bytes::from(envelope))
        }
        FamilyRequest::RequireLiveOccurrence { family, occurrence } => {
            families.require_live_occurrence(scope.family(family), scope.occurrence(occurrence)?).await?;
            FamilyReply::Done
        }
        FamilyRequest::RequirePositionOpen(position) => {
            families.require_position_open(position).await?;
            FamilyReply::Done
        }
        FamilyRequest::RegisterDerived { family, rows } => {
            let rows = rows.into_iter().map(|row| scope.derived(row)).collect::<Result<Vec<_>>>()?;
            families.register_derived(scope.family(family), &rows).await?;
            FamilyReply::Done
        }
        FamilyRequest::SealWorldRow { row, plaintext } => {
            let (stage, authoring) = admit_row(&scope, &families, &row).await?;
            FamilyReply::Sealed(Bytes::from(vault()?.seal_assertion_stage(stage, authoring, &region, plaintext).await?))
        }
        FamilyRequest::OpenWorldRow { row, envelope } => {
            let envelope = AssertionEnvelope::try_from(envelope)?;
            let (stage, authoring) = admit_row(&scope, &families, &row).await?;
            FamilyReply::Opened(vault()?.open_assertion_for_resolution(stage, authoring, &region, envelope).await?)
        }
        FamilyRequest::Fence { family, selected, order } => {
            let fence = families.fence(scope.family(family), selected, order).await?;
            FamilyReply::Fence(FamilyFenceReply {
                order: fence.order,
                inventory: fence.vault_inventory(),
                positions: fence.positions.into_iter().collect(),
                assertion_capabilities: fence.assertion_capabilities.into_iter().map(wire_stage).collect(),
                link_capabilities: fence.link_capabilities.into_iter().map(wire_stage).collect(),
                derived: fence.derived.into_iter().map(wire_derived).collect(),
            })
        }
        FamilyRequest::Settle { .. } => return Err(Anomaly::unsupported(
            "rtdb.owned/assertion-profile-uninstalled",
            "Family settlement requires the fixed assertion profile, which this cell does not install",
        )),
    })
}

async fn admit_row(
    scope: &Scope, families: &crate::keys::AssertionFamilies<'_>, row: &WorldRowAuthoring,
) -> Result<(AssertionStage, AssertionAuthoring)> {
    let stage = scope.stage(row.stage)?;
    let slot = WorldRowSlot { world: row.world, relation: &row.relation, key: &row.key };
    families.require_row_capability(scope.family(row.family), stage, row.assertion, slot).await?;
    Ok((stage, AssertionAuthoring { assertion_id: row.assertion, scope_commitment: row_scope(row), codec: row.codec }))
}
