//! The placed writer's assertion-family custody (dev/doc/rtdb-assertion-families.md).
//! Families are addressed within the attached deployment, and every capability
//! must name that deployment's current incarnation before the vault is touched.

use std::{num::NonZeroU64, sync::Arc};

use bytes::Bytes;
use rtdb_flight_sql::{
    FamilyDerivedRow, FamilyFenceReply, FamilyOccurrenceWire, FamilyReply, FamilyRequest, FamilyReservationWire,
    FamilyStage, WorldRowAuthoring,
};
use rtdb_directory::DeploymentRecord;
use rtdb_node::NativeSession;
use rtdb_storage::IoRegion;
use rtdb_types::{Anomaly, AnomalyCategory, Iid, OwnedWorld, PlacedAttemptScope, Result};
use uuid::Uuid;

use crate::keys::{
    AssertionAuthoring, AssertionEnvelope, AssertionKeyService, AssertionStage, DerivedRow, EnrolledWorldRow, FamilyFence, FamilyOccurrence, KeyService, LandingLink,
    LandingReservation, WorldRowSlot, assertion_root_line, family_vault_address, world_row_scope,
};

struct Scope {
    deployment: Uuid,
    incarnation: NonZeroU64,
    root_line: [u8; 16],
}

impl Scope {
    fn new(deployment: Uuid, record: &DeploymentRecord) -> Result<Self> {
        let incarnation = NonZeroU64::new(record.incarnation).ok_or_else(OwnedWorld::unavailable)?;
        Ok(Self { deployment, incarnation, root_line: assertion_root_line(deployment, record.log_epoch) })
    }

    async fn admit_lines(&self, session: &mut NativeSession, request: &FamilyRequest) -> Result<()> {
        let mut positions = [None; 2];
        let world = match request {
            FamilyRequest::Register { original, .. } => {
                positions[0] = Some(original.position);
                None
            }
            FamilyRequest::RequireLiveOccurrence { occurrence, .. } => {
                positions[0] = Some(occurrence.position);
                None
            }
            FamilyRequest::RequirePositionOpen(position) | FamilyRequest::PlanFence { selected: position, .. } => {
                positions[0] = Some(*position);
                None
            }
            FamilyRequest::ReserveLanding { reservation, .. } => {
                positions[0] = Some(reservation.source);
                None
            }
            FamilyRequest::ActivateLanding { placement, reservation, .. }
            | FamilyRequest::SealLandingLink { placement, reservation, .. } => {
                positions = [Some(reservation.source), Some(placement.position)];
                None
            }
            FamilyRequest::ActivateRootLanding { binding, .. } | FamilyRequest::OpenRootLandingPayload { binding, .. } => {
                // The switch can have retired the source world. Activation
                // authenticates its previously admitted reservation in the vault.
                if binding.position.line_generation != self.root_line {
                    return Err(scope_mismatch("The Root landing belongs to another line"));
                }
                None
            }
            FamilyRequest::SealRootLandingPayload { reservation, .. } => {
                positions[0] = Some(reservation.source);
                None
            }
            FamilyRequest::SealRootLandingLink { reservation, reviewed_source, .. } => {
                positions = [Some(reservation.source), Some(*reviewed_source)];
                None
            }
            FamilyRequest::EnrollOriginal(row) | FamilyRequest::EnrollWorldRow { row, .. } => Some((row.world, true)),
            FamilyRequest::Scope | FamilyRequest::RegisterDerived { .. } | FamilyRequest::SealWorldRow { .. }
            | FamilyRequest::ResolveMember { .. } | FamilyRequest::OpenWorldRow { .. } | FamilyRequest::Settle { .. } => None,
        };
        let mut worlds = positions.into_iter().flatten()
            .filter(|position| position.line_generation != self.root_line)
            .map(|position| (Uuid::from_bytes(position.line_generation), false))
            .chain(world);
        let Some(first) = worlds.next() else { return Ok(()); };
        let current = session.current_assertion_family_read(self.deployment).await?;
        for (world, enrolling) in std::iter::once(first).chain(worlds) {
            if world.is_nil() || current.world_registration(Iid(*world.as_bytes())).await?
                .is_none_or(|record| !record.mode.is_writable() || (enrolling && record.dropped))
            {
                return Err(scope_mismatch("The assertion position or authoring world belongs to another line"));
            }
        }
        Ok(())
    }

    fn family(&self, family: Uuid) -> Uuid {
        family_vault_address(self.deployment, family)
    }

    fn stage(&self, stage: FamilyStage) -> Result<AssertionStage> {
        if stage.deployment != self.deployment || stage.incarnation != self.incarnation {
            return Err(scope_mismatch("The assertion capability belongs to another deployment or incarnation"));
        }
        AssertionStage::new(stage.deployment, stage.incarnation, stage.stage)
    }

    fn enrolled_row(&self, row: &WorldRowAuthoring) -> Result<EnrolledWorldRow> {
        Ok(EnrolledWorldRow {
            world: row.world, assertion: row.assertion, capability: self.stage(row.stage)?,
            scope_commitment: world_row_scope(row_slot(row), &row.disclosure), codec: row.codec,
        })
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

fn scope_mismatch(message: &'static str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Forbidden, "rtdb.owned/assertion-family-scope", message)
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

fn row_slot(row: &WorldRowAuthoring) -> WorldRowSlot<'_> {
    WorldRowSlot { world: row.world, relation: &row.relation, key: &row.key }
}

pub(super) async fn serve(
    keys: &dyn KeyService, session: &mut NativeSession, deployment: Uuid,
    record: &DeploymentRecord, request: FamilyRequest,
) -> Result<FamilyReply> {
    let expected = PlacedAttemptScope::new(&record.stream("replica"), record.epoch)?;
    if session.placed_history()? != Some(expected) || record.writer_pool().is_none() {
        return Err(OwnedWorld::unavailable());
    }
    let scope = Scope::new(deployment, record)?;
    scope.admit_lines(session, &request).await?;
    let region = IoRegion::new();
    let original = session.assertion_family_reader()?;
    let families = keys.assertion_families(Arc::clone(&region)).ok_or_else(OwnedWorld::unavailable)?
        .with_original_reader(&original, deployment, session.state().cancellation());
    let vault = || keys.assertion_keys().ok_or_else(OwnedWorld::unavailable);
    Ok(match request {
        FamilyRequest::Scope => FamilyReply::Scope {
            deployment, incarnation: scope.incarnation, root_line: scope.root_line,
        },
        FamilyRequest::EnrollOriginal(row) => {
            families.enroll_original(scope.family(row.family), scope.enrolled_row(&row)?).await?;
            FamilyReply::Done
        }
        FamilyRequest::EnrollWorldRow { row, member } => {
            families.enroll_rewrite(scope.family(row.family), member, scope.enrolled_row(&row)?).await?;
            FamilyReply::Done
        }
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
        FamilyRequest::ActivateRootLanding { family, binding, reservation, link } => {
            families.activate_root_landing(
                scope.family(family), scope.reservation(reservation)?, binding, link, vault()?,
            ).await?;
            FamilyReply::Done
        }
        FamilyRequest::SealRootLandingLink { family, reservation, target, reviewed_source, payload_digest } => {
            let envelope = families.seal_root_landing_link(
                scope.family(family), scope.reservation(reservation)?, target, reviewed_source, payload_digest, vault()?,
            ).await?;
            FamilyReply::Sealed(Bytes::from(envelope))
        }
        FamilyRequest::SealRootLandingPayload { family, reservation, target, carrier } => {
            let envelope = families.seal_root_landing_payload(
                scope.family(family), scope.reservation(reservation)?, target, vault()?, carrier,
            ).await?;
            FamilyReply::Sealed(Bytes::from(envelope))
        }
        FamilyRequest::OpenRootLandingPayload { family, stage, binding, envelope } => {
            FamilyReply::Opened(families.open_root_landing_payload(
                scope.family(family), scope.stage(stage)?, binding, envelope, vault()?,
            ).await?)
        }
        FamilyRequest::ResolveMember { family, assertion } => {
            let member = families.resolve_member(scope.family(family), assertion).await?;
            let capability = wire_stage(member.capability);
            scope.stage(capability)?;
            FamilyReply::Member(FamilyOccurrenceWire { position: member.position, assertion, capability })
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
            let opened = vault()?.open_assertion_for_resolution(stage, authoring, &region, envelope).await?;
            admit_row(&scope, &families, &row).await?;
            FamilyReply::Opened(opened)
        }
        FamilyRequest::PlanFence { family, selected, order } => {
            fence_reply(families.plan_fence(scope.family(family), selected, order).await?)
        }
        FamilyRequest::Settle { .. } => return Err(Anomaly::unsupported(
            "rtdb.owned/assertion-profile-uninstalled",
            "Family settlement requires the fixed assertion profile, which this cell does not install",
        )),
    })
}

fn fence_reply(fence: FamilyFence) -> FamilyReply {
    FamilyReply::Fence(FamilyFenceReply {
        order: fence.order,
        inventory: fence.vault_inventory(),
        positions: fence.positions.into_iter().collect(),
        assertion_capabilities: fence.assertion_capabilities.into_iter().map(wire_stage).collect(),
        link_capabilities: fence.link_capabilities.into_iter().map(wire_stage).collect(),
        derived: fence.derived.into_iter().map(wire_derived).collect(),
    })
}

async fn admit_row(
    scope: &Scope, families: &crate::keys::AssertionFamilies<'_>, row: &WorldRowAuthoring,
) -> Result<(AssertionStage, AssertionAuthoring)> {
    let enrolled = scope.enrolled_row(row)?;
    families.require_row_capability(scope.family(row.family), enrolled, row_slot(row)).await?;
    Ok((enrolled.capability, AssertionAuthoring {
        assertion_id: enrolled.assertion, scope_commitment: enrolled.scope_commitment, codec: enrolled.codec,
    }))
}
