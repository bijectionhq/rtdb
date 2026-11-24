//! The placed writer's assertion-family custody (dev/doc/rtdb-assertion-families.md).
//! Families are addressed within the attached deployment, and every capability
//! must name that deployment's current incarnation before the vault is touched.

use std::{num::NonZeroU64, sync::Arc};

use bytes::Bytes;
use ring::digest::{Context, SHA256};
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
    AssertionAuthoring, AssertionEnvelope, AssertionKeyService, AssertionStage, DerivedRow, EnrolledWorldRow, FamilyOccurrence, KeyService, LandingLink,
    LandingReservation, WorldRowSlot,
};

struct Scope {
    deployment: Uuid,
    incarnation: NonZeroU64,
    root_line: [u8; 16],
}

impl Scope {
    fn new(deployment: Uuid, record: &DeploymentRecord) -> Result<Self> {
        let incarnation = NonZeroU64::new(record.incarnation).ok_or_else(OwnedWorld::unavailable)?;
        let mut line = Context::new(&SHA256);
        line.update(b"rtdb.owned/assertion-line/v1\0");
        line.update(deployment.as_bytes());
        line.update(&record.log_epoch.to_be_bytes());
        let mut root_line = [0; 16];
        root_line.copy_from_slice(&line.finish().as_ref()[..16]);
        Ok(Self { deployment, incarnation, root_line })
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
            FamilyRequest::RequirePositionOpen(position) | FamilyRequest::Fence { selected: position, .. } => {
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
            FamilyRequest::EnrollOriginal(row) | FamilyRequest::EnrollWorldRow { row, .. } => Some((row.world, true)),
            FamilyRequest::Scope | FamilyRequest::RegisterDerived { .. } | FamilyRequest::SealWorldRow { .. }
            | FamilyRequest::OpenWorldRow { .. } | FamilyRequest::Settle { .. } => None,
        };
        let mut worlds = positions.into_iter().flatten()
            .filter(|position| position.line_generation != self.root_line)
            .map(|position| (Uuid::from_bytes(position.line_generation), false))
            .chain(world);
        let Some(first) = worlds.next() else { return Ok(()); };
        let snapshot = session.open_owned_read().await?;
        for (world, enrolling) in std::iter::once(first).chain(worlds) {
            if world.is_nil() || snapshot.world_registration(Iid(*world.as_bytes())).await?
                .is_none_or(|(record, _)| !record.mode.is_writable() || (enrolling && record.dropped))
            {
                return Err(scope_mismatch("The assertion position or authoring world belongs to another line"));
            }
        }
        Ok(())
    }

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
        if stage.deployment != self.deployment || stage.incarnation != self.incarnation {
            return Err(scope_mismatch("The assertion capability belongs to another deployment or incarnation"));
        }
        AssertionStage::new(stage.deployment, stage.incarnation, stage.stage)
    }

    fn enrolled_row(&self, row: &WorldRowAuthoring) -> Result<EnrolledWorldRow> {
        Ok(EnrolledWorldRow {
            world: row.world, assertion: row.assertion, capability: self.stage(row.stage)?,
            scope_commitment: row_scope(row), codec: row.codec,
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
    let families = keys.assertion_families(Arc::clone(&region)).ok_or_else(OwnedWorld::unavailable)?;
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
    let enrolled = scope.enrolled_row(row)?;
    let slot = WorldRowSlot { world: row.world, relation: &row.relation, key: &row.key };
    families.require_row_capability(scope.family(row.family), enrolled, slot).await?;
    Ok((enrolled.capability, AssertionAuthoring {
        assertion_id: enrolled.assertion, scope_commitment: enrolled.scope_commitment, codec: enrolled.codec,
    }))
}
