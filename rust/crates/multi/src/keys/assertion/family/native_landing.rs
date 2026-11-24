use std::num::NonZeroU64;

use rtdb_directory::DeploymentRecord;
use rtdb_index::branches::CustodyOccurrence;
use rtdb_node::{
    NativeSession,
    serving::custody_release::{
        CustodyCarrierAuthority, MountedResponseRelease, NativeLandingOrder,
    },
};
use rtdb_types::{
    OwnedWorld, RootLandingBinding, RootLandingTarget, Timestamp,
    rtdb_v1::{Basis, MAIN_BRANCH},
};

use crate::keys::KeyService;

use super::{
    root_landing::{
        ROOT_LINK_CODEC, ROOT_PAYLOAD_CODEC, link_prefix, root_authoring, root_binding_digest,
    },
    *,
};

pub struct NativeFamilyLanding<'a> {
    families: AssertionFamilies<'a>,
    keys: &'a dyn AssertionKeyService,
    native: &'a MountedResponseRelease<'a>,
    root_line: [u8; 16],
    incarnation: NonZeroU64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeLanding {
    authority: CustodyCarrierAuthority,
    order: NativeLandingOrder,
    #[serde(with = "position")]
    winner: Position,
    target: Vec<u8>,
    source_carriers: [[u8; 32]; 2],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body_sha256: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    drained_at: Option<Basis>,
}

impl<'a> NativeFamilyLanding<'a> {
    pub fn from_placed_original(
        keys: &'a dyn KeyService,
        region: Arc<IoRegion>,
        original: &'a OriginalAssertionFamilyReader,
        session: &NativeSession,
        record: &DeploymentRecord,
        native: &'a MountedResponseRelease<'a>,
        cancellation: &'a CancellationToken,
    ) -> Result<Self> {
        let unavailable = OwnedWorld::unavailable;
        let deployment = Uuid::parse_str(&record.id).map_err(|_| unavailable())?;
        let placed = PlacedAttemptScope::new(&record.stream("replica"), record.epoch)?;
        let authority = native.native_landing_authority();
        if record.writer_pool().is_none()
            || session.placed_history()? != Some(placed)
            || original.history_fingerprint() != placed.history_fingerprint()
            || session.state().user() != Some(original.owner())
            || authority.incarnation != *deployment.as_bytes()
        {
            return Err(unavailable());
        }
        let families = keys
            .assertion_families(region)
            .ok_or_else(unavailable)?
            .with_original_reader(original, deployment, cancellation)
            .with_native_landing(native);
        Ok(Self {
            families,
            keys: keys.assertion_keys().ok_or_else(unavailable)?,
            native,
            root_line: assertion_root_line(deployment, record.log_epoch),
            incarnation: NonZeroU64::new(record.incarnation).ok_or_else(unavailable)?,
        })
    }

    pub async fn reserve(
        &self,
        family: Uuid,
        member: FamilyOccurrence,
        reservation: LandingReservation,
        order: NativeLandingOrder,
        target: RootLandingTarget,
        original: &[u8],
        winner: &[u8],
    ) -> Result<()> {
        let authority = self.native.native_landing_authority();
        let family_id = self.family_id(family)?;
        self.require_original_live(family).await?;
        if reservation.source != member.position
            || reservation.review_commitment != order.review_sha256
            || reservation.review_commitment == [0; 32]
            || reservation.capability == reservation.link_capability
            || reservation.capability.identity().as_bytes() != &order.destination
            || reservation.link_capability.identity().as_bytes() != &order.link
            || order.target.branch != MAIN_BRANCH
            || order.review.cut.branch == MAIN_BRANCH
            || order.review.cut.basis.incarnation() != authority.incarnation
            || target.receipt != order.request_sha256
            || order.receipt.same_cell(&order.proof)
            || !(16..=64).contains(&order.reserve_key.len())
            || !(16..=64).contains(&order.accept_key.len())
            || order.reserve_key == order.accept_key
        {
            return Err(conflict());
        }
        for stage in [
            member.capability,
            reservation.capability,
            reservation.link_capability,
        ] {
            if stage.deployment().as_bytes() != &authority.incarnation
                || stage.incarnation() != self.incarnation
            {
                return Err(conflict());
            }
        }
        RootLandingTarget::from_bytes(target.to_bytes()).map_err(|_| conflict())?;

        let original_carrier = WorldCarrier::decode(family, original)?;
        let winning_carrier = WorldCarrier::decode(family, winner)?;
        let (record, _) = self
            .families
            .read_current(family_id, true)
            .await?
            .ok_or_else(conflict)?;
        record.require_live()?;
        record.require_unclaimed()?;
        if record.original.assertion != member.assertion
            || record.admitted_member(member.assertion) != Some(member)
        {
            return Err(conflict());
        }
        if let Some(prior) = record
            .landings
            .iter()
            .find(|entry| entry.reservation.destination == reservation.destination)
        {
            let native = prior.native.as_ref().ok_or_else(conflict)?;
            if prior.reservation != reservation
                || native.authority != authority
                || native.order != order
                || native.target != target.to_bytes()
                || native.source_carriers != [sha256(original), sha256(winner)]
            {
                return Err(conflict());
            }
            return self
                .native
                .verify_native_landing_live(native.order.review.group)
                .await;
        }
        let [
            (original_occurrence, original_clock),
            (winning_occurrence, winning_clock),
        ] = self
            .native
            .verify_native_landing_review(&order.review, original, winner)
            .await?;
        if original_carrier.assertion != member.assertion
            || original_carrier.member != member.assertion
            || !original_carrier.matches_stage(member.capability)
            || position(
                member.position.line_generation,
                &original_occurrence,
                original_clock,
            )? != member.position
            || winning_carrier.member != member.assertion
        {
            return Err(conflict());
        }
        let enrolled = if winning_carrier.assertion == record.original.assertion {
            &record.original
        } else {
            &record
                .rewrites
                .iter()
                .find(|rewrite| {
                    rewrite.member == member.assertion
                        && rewrite.row.assertion == winning_carrier.assertion
                })
                .ok_or_else(conflict)?
                .row
        };
        if !winning_carrier.matches_stage(enrolled.capability) {
            return Err(conflict());
        }
        for (carrier, authoring) in [
            (&original_carrier, &record.original),
            (&winning_carrier, enrolled),
        ] {
            self.keys
                .open_assertion_for_resolution(
                    authoring.capability,
                    AssertionAuthoring {
                        assertion_id: authoring.assertion,
                        scope_commitment: authoring.scope_commitment,
                        codec: authoring.codec,
                    },
                    &self.families.region,
                    AssertionEnvelope::try_from(Bytes::copy_from_slice(carrier.payload))?,
                )
                .await?;
        }
        let source_carriers = [sha256(original), sha256(winner)];
        let winner = position(
            *enrolled.world.as_bytes(),
            &winning_occurrence,
            winning_clock,
        )?;
        let native = NativeLanding {
            authority,
            order,
            winner,
            target: target.to_bytes().to_vec(),
            source_carriers,
            body_sha256: None,
            drained_at: None,
        };
        self.require_original_live(family).await?;
        self.families
            .update(family_id, |record| {
                record.require_live()?;
                if let Some(prior) = record
                    .landings
                    .iter()
                    .find(|entry| entry.reservation.destination == reservation.destination)
                {
                    return if prior.reservation == reservation
                        && prior
                            .native
                            .as_ref()
                            .is_some_and(|prior| prior.same_reservation(&native))
                    {
                        Ok(((), false))
                    } else {
                        Err(conflict())
                    };
                }
                if record.admitted_member(member.assertion) != Some(member)
                    || record.has_member(reservation.destination)
                    || record.stage_used(reservation.capability)
                    || record.stage_used(reservation.link_capability)
                {
                    return Err(conflict());
                }
                record.landings.push(Landing {
                    reservation,
                    accepted: None,
                    native: Some(native.clone()),
                });
                Ok(((), true))
            })
            .await
    }

    pub async fn seal_payload(
        &self,
        family: Uuid,
        destination: AssertionId,
        carrier: Bytes,
    ) -> Result<AssertionEnvelope> {
        let (family_id, reservation, native) = self.selected_live(family, destination).await?;
        let authoring =
            root_authoring(family_id, destination, native.target()?, ROOT_PAYLOAD_CODEC);
        self.keys
            .seal_assertion_stage(
                reservation.capability,
                authoring,
                &self.families.region,
                carrier,
            )
            .await
    }

    pub async fn seal_link(
        &self,
        family: Uuid,
        destination: AssertionId,
        payload_digest: [u8; 32],
    ) -> Result<AssertionEnvelope> {
        let (family_id, reservation, native) = self.selected_live(family, destination).await?;
        let authoring = root_authoring(family_id, destination, native.target()?, ROOT_LINK_CODEC);
        let mut body = link_prefix(reservation, payload_digest);
        body.extend_from_slice(&native.winner.to_bytes());
        self.keys
            .seal_assertion_stage(
                reservation.link_capability,
                authoring,
                &self.families.region,
                body.into(),
            )
            .await
    }

    pub async fn bind_acceptance_body(
        &self,
        family: Uuid,
        destination: AssertionId,
        accept_key: &[u8],
        body_sha256: [u8; 32],
    ) -> Result<()> {
        let (family_id, reservation, native) = self.selected_live(family, destination).await?;
        if native.order.accept_key != accept_key || body_sha256 == [0; 32] {
            return Err(conflict());
        }
        self.families
            .update(family_id, |record| {
                record.require_live()?;
                let selected = record
                    .landings
                    .iter_mut()
                    .find(|entry| entry.reservation == reservation)
                    .ok_or_else(conflict)?;
                let stored = selected.native.as_mut().ok_or_else(conflict)?;
                if !stored.same_reservation(&native) {
                    return Err(conflict());
                }
                match stored.body_sha256 {
                    Some(previous) if previous == body_sha256 => Ok(((), false)),
                    None => {
                        stored.body_sha256 = Some(body_sha256);
                        Ok(((), true))
                    }
                    Some(_) => Err(conflict()),
                }
            })
            .await
    }

    pub async fn activate(
        &self,
        family: Uuid,
        destination: AssertionId,
        accept_key: &[u8],
    ) -> Result<RootLandingBinding> {
        let (family_id, reservation, native) = self.selected_live(family, destination).await?;
        if native.order.accept_key != accept_key {
            return Err(conflict());
        }
        let body_sha256 = native.body_sha256.ok_or_else(conflict)?;
        let verified = self
            .native
            .verify_native_landing_acceptance(&native.order, body_sha256)
            .await?;
        let carrier_parts = RootCarrier::decode(family, verified.carrier())?;
        let link = verified.link();
        let target = native.target()?;
        if carrier_parts.assertion != destination
            || carrier_parts.target != target
            || carrier_parts.stage != stage_bytes(reservation.capability)
            || carrier_parts.link != sha256(link)
        {
            return Err(conflict());
        }
        let binding = RootLandingBinding {
            position: position(
                self.root_line,
                verified.occurrence(),
                verified.receipt().system_time,
            )?,
            assertion: destination,
            stage: *reservation.capability.identity().as_bytes(),
            target,
            payload_digest: sha256(carrier_parts.payload),
            link_digest: carrier_parts.link,
        };
        let binding_digest = root_binding_digest(binding);
        let (record, _) = self
            .families
            .read_current(family_id, true)
            .await?
            .ok_or_else(conflict)?;
        let selected = native_landing(&record, destination, native.authority)?;
        if let Some(accepted) = &selected.accepted {
            self.require_original_live(family).await?;
            self.families
                .require_native_landing_live(&record, destination)
                .await?;
            return same_activation(accepted, binding).map(|()| binding);
        }
        let authoring = root_authoring(family_id, destination, target, ROOT_LINK_CODEC);
        let opened = self
            .keys
            .open_assertion_for_resolution(
                reservation.link_capability,
                authoring,
                &self.families.region,
                AssertionEnvelope::try_from(link.clone())?,
            )
            .await?;
        let mut expected = link_prefix(reservation, binding.payload_digest);
        expected.extend_from_slice(&native.winner.to_bytes());
        if opened.as_ref() != expected.as_slice() {
            return Err(conflict());
        }
        let authoring = root_authoring(family_id, destination, target, ROOT_PAYLOAD_CODEC);
        self.keys
            .open_assertion_for_resolution(
                reservation.capability,
                authoring,
                &self.families.region,
                AssertionEnvelope::try_from(Bytes::copy_from_slice(carrier_parts.payload))?,
            )
            .await?;
        self.require_original_live(family).await?;
        self.families
            .require_position_open(binding.position)
            .await?;
        self.native
            .verify_native_landing_live(native.order.review.group)
            .await?;
        let binding = self
            .families
            .update(family_id, |record| {
                record.require_live()?;
                let selected = native_landing(record, destination, native.authority)?;
                if selected.native.as_ref() != Some(&native) {
                    return Err(conflict());
                }
                if let Some(accepted) = &selected.accepted {
                    return same_activation(accepted, binding).map(|()| (binding, false));
                }
                if record.contains(binding.position) {
                    return Err(conflict());
                }
                let selected = record
                    .landings
                    .iter_mut()
                    .find(|entry| entry.reservation == reservation)
                    .ok_or_else(conflict)?;
                selected.accepted = Some(AcceptedLanding {
                    position: binding.position,
                    placement_digest: binding_digest,
                });
                Ok((binding, true))
            })
            .await?;
        self.require_original_live(family).await?;
        self.native
            .verify_native_landing_live(native.order.review.group)
            .await?;
        Ok(binding)
    }

    pub async fn record_drain(
        &self,
        family: Uuid,
        destination: AssertionId,
        revoked_at: Basis,
        inventory_sha256: [u8; 32],
    ) -> Result<()> {
        let authority = self.native.native_landing_authority();
        let family_id = self.family_id(family)?;
        let (record, _) = self
            .families
            .read_current(family_id, true)
            .await?
            .ok_or_else(conflict)?;
        let native = native_landing(&record, destination, authority)?
            .native
            .as_ref()
            .ok_or_else(conflict)?;
        let group = native.order.review.group;
        let original = self.families.original.as_ref().ok_or_else(conflict)?;
        let current = original
            .reader
            .current(original.deployment, original.cancellation)
            .await?;
        let marker = current
            .assertion_family_fence(family)
            .await?
            .ok_or_else(conflict)?;
        if record.fenced != Some(marker.order)
            || record.fence_inventory(marker.order).vault_inventory() != marker.inventory
        {
            return Err(conflict());
        }
        self.native
            .verify_native_landing_drain(group, revoked_at, inventory_sha256)
            .await?;
        self.families
            .update(family_id, |record| {
                if record.fenced != Some(marker.order)
                    || record.fence_inventory(marker.order).vault_inventory() != marker.inventory
                {
                    return Err(conflict());
                }
                let mut changed = false;
                for native in record
                    .landings
                    .iter_mut()
                    .filter_map(|entry| entry.native.as_mut())
                    .filter(|native| {
                        native.authority == authority && native.order.review.group == group
                    })
                {
                    match native.drained_at {
                        Some(previous) if previous != revoked_at => return Err(conflict()),
                        Some(_) => (),
                        None => {
                            native.drained_at = Some(revoked_at);
                            changed = true;
                        }
                    }
                }
                Ok(((), changed))
            })
            .await
    }

    pub fn with_release_authority(
        self,
        authority: &'a dyn FamilyReleaseAuthority,
    ) -> AssertionFamilies<'a> {
        self.families.with_release_authority(authority)
    }

    async fn selected_live(
        &self,
        family: Uuid,
        destination: AssertionId,
    ) -> Result<(Uuid, LandingReservation, NativeLanding)> {
        self.require_original_live(family).await?;
        let authority = self.native.native_landing_authority();
        let family_id = self.family_id(family)?;
        let (record, _) = self
            .families
            .read_current(family_id, true)
            .await?
            .ok_or_else(conflict)?;
        record.require_live()?;
        record.require_unclaimed()?;
        let landing = native_landing(&record, destination, authority)?;
        let native = landing.native.as_ref().ok_or_else(conflict)?;
        self.native
            .verify_native_landing_live(native.order.review.group)
            .await?;
        Ok((family_id, landing.reservation, native.clone()))
    }

    fn family_id(&self, family: Uuid) -> Result<Uuid> {
        if family.is_nil() {
            return Err(conflict());
        }
        let original = self.families.original.as_ref().ok_or_else(conflict)?;
        Ok(family_vault_address(original.deployment, family))
    }

    async fn require_original_live(&self, family: Uuid) -> Result<()> {
        let original = self.families.original.as_ref().ok_or_else(conflict)?;
        let current = original
            .reader
            .current(original.deployment, original.cancellation)
            .await?;
        if current.assertion_family_fence(family).await?.is_some() {
            return Err(conflict());
        }
        Ok(())
    }
}

impl<'a> AssertionFamilies<'a> {
    pub fn with_native_landing(mut self, native: &'a MountedResponseRelease<'a>) -> Self {
        self.native_landing = Some(native);
        self
    }

    pub(super) async fn require_native_landing_live(
        &self,
        record: &Family,
        member: AssertionId,
    ) -> Result<()> {
        if let Some(landing) = record
            .landings
            .iter()
            .find(|entry| entry.reservation.destination == member)
            && let Some(native) = &landing.native
        {
            record.require_unclaimed()?;
            if landing.accepted.is_none() {
                return Err(conflict());
            }
            let mounted = self.native_landing.ok_or_else(conflict)?;
            if native.authority != mounted.native_landing_authority() {
                return Err(conflict());
            }
            mounted
                .verify_native_landing_live(native.order.review.group)
                .await?;
        }
        Ok(())
    }
}

impl Family {
    pub(super) fn native_landings_drained(&self) -> bool {
        self.landings
            .iter()
            .filter_map(|landing| landing.native.as_ref())
            .all(|native| native.drained_at.is_some())
    }
}

impl NativeLanding {
    fn same_reservation(&self, other: &Self) -> bool {
        self.authority == other.authority
            && self.order == other.order
            && self.winner == other.winner
            && self.target == other.target
            && self.source_carriers == other.source_carriers
    }

    fn target(&self) -> Result<RootLandingTarget> {
        RootLandingTarget::from_bytes(self.target.as_slice().try_into().map_err(|_| malformed())?)
            .map_err(|_| malformed())
    }
}

fn native_landing(
    record: &Family,
    destination: AssertionId,
    authority: CustodyCarrierAuthority,
) -> Result<&Landing> {
    record
        .landings
        .iter()
        .find(|landing| {
            landing.reservation.destination == destination
                && landing
                    .native
                    .as_ref()
                    .is_some_and(|native| native.authority == authority)
        })
        .ok_or_else(conflict)
}

fn same_activation(accepted: &AcceptedLanding, binding: RootLandingBinding) -> Result<()> {
    if accepted.position != binding.position
        || accepted.placement_digest != root_binding_digest(binding)
    {
        return Err(conflict());
    }
    Ok(())
}

fn position(
    line_generation: [u8; 16],
    occurrence: &CustodyOccurrence,
    clock: Timestamp,
) -> Result<Position> {
    let publication = u64::try_from(clock.0).map_err(|_| conflict())?;
    let ordinal = NonZeroU64::new(occurrence.ordinal).ok_or_else(conflict)?;
    Ok(Position {
        line_generation,
        publication,
        ordinal,
    })
}

fn stage_bytes(stage: AssertionStage) -> [u8; 40] {
    let mut bytes = [0; 40];
    bytes[..16].copy_from_slice(stage.deployment().as_bytes());
    bytes[16..24].copy_from_slice(&stage.incarnation().get().to_be_bytes());
    bytes[24..].copy_from_slice(stage.identity().as_bytes());
    bytes
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    digest(&SHA256, bytes)
        .as_ref()
        .try_into()
        .expect("SHA-256 commitment")
}

fn exact_family<'a>(bytes: &'a [u8], tag: &[u8], family: Uuid) -> Result<&'a [u8]> {
    let bytes = bytes.strip_prefix(tag).ok_or_else(conflict)?;
    let (id, bytes) = bytes.split_first_chunk::<16>().ok_or_else(conflict)?;
    if id != family.as_bytes() {
        return Err(conflict());
    }
    Ok(bytes)
}

struct WorldCarrier<'a> {
    stage: [u8; 40],
    assertion: AssertionId,
    member: AssertionId,
    payload: &'a [u8],
}

impl<'a> WorldCarrier<'a> {
    fn decode(family: Uuid, carrier: &'a [u8]) -> Result<Self> {
        let bytes = exact_family(carrier, b"BJWC1", family)?;
        let (stage, bytes) = bytes.split_first_chunk::<40>().ok_or_else(conflict)?;
        let (assertion, bytes) = bytes.split_first_chunk::<32>().ok_or_else(conflict)?;
        let (member, payload) = bytes.split_first_chunk::<32>().ok_or_else(conflict)?;
        if payload.is_empty() {
            return Err(conflict());
        }
        Ok(Self {
            stage: *stage,
            assertion: AssertionId::from_bytes(*assertion),
            member: AssertionId::from_bytes(*member),
            payload,
        })
    }

    fn matches_stage(&self, stage: AssertionStage) -> bool {
        self.stage == stage_bytes(stage)
    }
}

struct RootCarrier<'a> {
    stage: [u8; 40],
    assertion: AssertionId,
    target: RootLandingTarget,
    link: [u8; 32],
    payload: &'a [u8],
}

impl<'a> RootCarrier<'a> {
    fn decode(family: Uuid, carrier: &'a [u8]) -> Result<Self> {
        let bytes = exact_family(carrier, b"BJRL1", family)?;
        let (stage, bytes) = bytes.split_first_chunk::<40>().ok_or_else(conflict)?;
        let (assertion, bytes) = bytes.split_first_chunk::<32>().ok_or_else(conflict)?;
        let (target, bytes) = bytes
            .split_first_chunk::<{ RootLandingTarget::WIDTH }>()
            .ok_or_else(conflict)?;
        let (link, payload) = bytes.split_first_chunk::<32>().ok_or_else(conflict)?;
        if payload.is_empty() {
            return Err(conflict());
        }
        Ok(Self {
            stage: *stage,
            assertion: AssertionId::from_bytes(*assertion),
            target: RootLandingTarget::from_bytes(*target).map_err(|_| conflict())?,
            link: *link,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use rtdb_index::branches::{CustodyReview, CustodyTarget};
    use rtdb_node::serving::custody_release::{NativeLandingReview, NativeLandingWitness};
    use rtdb_types::{EntityId, Interval, TableRef};

    use super::*;

    #[tokio::test]
    async fn native_family_landing_requires_its_bridge_to_open_and_drain() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::with_archive_broker(directory.path())?;
        let families = keys.assertion_families(IoRegion::new()).unwrap();
        let realm = Uuid::from_bytes([1; 16]);
        let family = Uuid::from_bytes([2; 16]);
        let stage = |tag| AssertionStage::new(realm, NonZeroU64::MIN, Uuid::from_bytes([tag; 16]));
        let original_position = Position {
            line_generation: [3; 16],
            publication: 1,
            ordinal: NonZeroU64::MIN,
        };
        let original = FamilyOccurrence {
            position: original_position,
            assertion: AssertionId::from_bytes([4; 32]),
            capability: stage(5)?,
        };
        let reservation = LandingReservation {
            source: original_position,
            destination: AssertionId::from_bytes([6; 32]),
            capability: stage(7)?,
            link_capability: stage(8)?,
            review_commitment: [9; 32],
        };
        let authority = CustodyCarrierAuthority {
            incarnation: *realm.as_bytes(),
            source_history: [10; 32],
            installation: [11; 16],
            registry: [12; 32],
            original_writer: [13; 32],
        };
        let target = RootLandingTarget {
            receipt: [14; 32],
            review_slot: 3,
            address_extent: [15; 32],
            disclosure: [16; 32],
        };
        let point = |id| NativeLandingWitness {
            table: TableRef::new("public", "receipts"),
            id: EntityId::Integer(id),
            valid_at: Timestamp(1),
            column: "body".into(),
        };
        let native = NativeLanding {
            authority,
            order: NativeLandingOrder {
                review: NativeLandingReview {
                    group: [17; 16],
                    cut: CustodyReview {
                        branch: "proposal".into(),
                        basis: Basis::new(authority.incarnation, 1),
                        valid_at: Timestamp(1),
                        inventory_sha256: [18; 32],
                    },
                    original: [19; 32],
                    selected: [20; 32],
                },
                target: CustodyTarget {
                    branch: MAIN_BRANCH.into(),
                    table: TableRef::new("public", "docs"),
                    id: EntityId::Integer(1),
                    valid: Interval::ALL.into(),
                },
                reserve_key: vec![21; 16],
                accept_key: vec![22; 16],
                destination: *reservation.capability.identity().as_bytes(),
                link: *reservation.link_capability.identity().as_bytes(),
                review_sha256: reservation.review_commitment,
                request_sha256: target.receipt,
                receipt: point(1),
                proof: point(2),
            },
            winner: original_position,
            target: target.to_bytes().to_vec(),
            source_carriers: [[23; 32], [24; 32]],
            body_sha256: None,
            drained_at: None,
        };
        families
            .enroll_original(
                family,
                EnrolledWorldRow {
                    world: Uuid::from_bytes(original_position.line_generation),
                    assertion: original.assertion,
                    capability: original.capability,
                    scope_commitment: [25; 32],
                    slot_commitment: [26; 32],
                    codec: 1,
                },
            )
            .await?;
        families.register(family, original).await?;
        families
            .update(family, |record| {
                record.landings.push(Landing {
                    reservation,
                    accepted: None,
                    native: Some(native.clone()),
                });
                Ok(((), true))
            })
            .await?;
        assert!(families.reserve_landing(family, reservation).await.is_err());
        assert!(
            families
                .require_reserved(family, reservation)
                .await
                .is_err()
        );
        let (mut record, _) = families.read(family).await?.unwrap();
        assert!(!record.native_landings_drained());
        assert!(
            families
                .require_native_landing_live(&record, reservation.destination)
                .await
                .is_err()
        );
        let landing = &mut record.landings[0];
        landing.accepted = Some(AcceptedLanding {
            position: original_position,
            placement_digest: [27; 32],
        });
        let stored = landing.native.as_mut().unwrap();
        stored.body_sha256 = Some([28; 32]);
        assert!(stored.same_reservation(&native));
        assert_ne!(stored.body_sha256, Some(stored.order.request_sha256));
        assert!(
            families
                .require_native_landing_live(&record, reservation.destination)
                .await
                .is_err()
        );
        Ok(())
    }
}
