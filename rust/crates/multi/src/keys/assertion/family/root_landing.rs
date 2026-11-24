use rtdb_types::{RootLandingBinding, RootLandingTarget};

use super::*;

const ROOT_PAYLOAD_CODEC: u32 = u32::from_be_bytes(*b"RLP1");
const ROOT_LINK_CODEC: u32 = u32::from_be_bytes(*b"RLL1");

impl AssertionFamilies<'_> {
    pub async fn seal_root_landing_payload(
        &self, family: Uuid, reservation: LandingReservation, target: RootLandingTarget,
        keys: &dyn AssertionKeyService, carrier: Bytes,
    ) -> Result<AssertionEnvelope> {
        self.require_reserved(family, reservation).await?;
        let authoring = root_authoring(family, reservation.destination, target, ROOT_PAYLOAD_CODEC);
        keys.seal_assertion_stage(reservation.capability, authoring, &self.region, carrier).await
    }

    pub async fn seal_root_landing_link(
        &self, family: Uuid, reservation: LandingReservation, target: RootLandingTarget,
        reviewed_source: Position, payload_digest: [u8; 32], keys: &dyn AssertionKeyService,
    ) -> Result<AssertionEnvelope> {
        self.require_reserved(family, reservation).await?;
        let authoring = root_authoring(family, reservation.destination, target, ROOT_LINK_CODEC);
        if reviewed_source.line_generation == [0; 16] {
            return Err(conflict());
        }
        let mut body = link_prefix(reservation, payload_digest);
        body.extend_from_slice(&reviewed_source.to_bytes());
        keys.seal_assertion_stage(reservation.link_capability, authoring, &self.region, body.into()).await
    }

    /// The publication owner authenticates the receipt and original position.
    /// The vault authenticates the enrolled link and serializes against erasure.
    pub async fn activate_root_landing(
        &self, family: Uuid, reservation: LandingReservation, binding: RootLandingBinding,
        link: Bytes, keys: &dyn AssertionKeyService,
    ) -> Result<()> {
        if binding.assertion != reservation.destination
            || binding.stage != *reservation.capability.stage.as_bytes()
            || !matches_digest(binding.link_digest, &link)
        {
            return Err(conflict());
        }
        self.require_position_open(binding.position).await?;
        let binding_digest = root_binding_digest(binding);
        let (record, _) = self.read(family).await?.ok_or_else(conflict)?;
        let landing = reserved(&record, reservation)?;
        if let Some(prior) = &landing.accepted {
            return require_binding(prior, binding.position, binding_digest);
        }

        let authoring = root_authoring(family, binding.assertion, binding.target, ROOT_LINK_CODEC);
        let link = keys.open_assertion_for_resolution(
            reservation.link_capability, authoring, &self.region, AssertionEnvelope::try_from(link)?,
        ).await?;
        let prefix = link_prefix(reservation, binding.payload_digest);
        let reviewed_source = link.strip_prefix(prefix.as_slice())
            .and_then(|bytes| bytes.try_into().ok())
            .and_then(|bytes| Position::from_bytes(bytes).ok())
            .filter(|position| position.line_generation != [0; 16]);
        if reviewed_source.is_none() {
            return Err(conflict());
        }

        self.update(family, |record| {
            let landing = reserved(record, reservation)?;
            if let Some(prior) = &landing.accepted {
                return require_binding(prior, binding.position, binding_digest).map(|()| ((), false));
            }
            if record.contains(binding.position) {
                return Err(conflict());
            }
            let landing = record.landings.iter_mut()
                .find(|entry| entry.reservation == reservation)
                .ok_or_else(conflict)?;
            landing.accepted = Some(AcceptedLanding { position: binding.position, placement_digest: binding_digest });
            Ok(((), true))
        }).await
    }

    pub async fn open_root_landing_payload(
        &self, family: Uuid, stage: AssertionStage, binding: RootLandingBinding, envelope: Bytes,
        keys: &dyn AssertionKeyService,
    ) -> Result<Bytes> {
        if binding.stage != *stage.stage.as_bytes() || !matches_digest(binding.payload_digest, &envelope) {
            return Err(conflict());
        }
        self.require_live_root_landing(family, stage, binding).await?;
        let authoring = root_authoring(family, binding.assertion, binding.target, ROOT_PAYLOAD_CODEC);
        let body = keys.open_assertion_for_resolution(
            stage, authoring, &self.region, AssertionEnvelope::try_from(envelope)?,
        ).await?;
        self.require_live_root_landing(family, stage, binding).await?;
        Ok(body)
    }

    async fn require_live_root_landing(
        &self, family: Uuid, stage: AssertionStage, binding: RootLandingBinding,
    ) -> Result<()> {
        self.require_position_open(binding.position).await?;
        let (record, _) = self.read(family).await?.ok_or_else(conflict)?;
        record.require_live()?;
        let accepted = record.landings.iter().find(|entry| {
            entry.reservation.destination == binding.assertion && entry.reservation.capability == stage
        }).and_then(|entry| entry.accepted.as_ref()).ok_or_else(conflict)?;
        require_binding(accepted, binding.position, root_binding_digest(binding))
    }
}

fn reserved(record: &Family, reservation: LandingReservation) -> Result<&Landing> {
    record.require_live()?;
    record.landings.iter().find(|entry| entry.reservation == reservation).ok_or_else(conflict)
}

fn require_binding(prior: &AcceptedLanding, position: Position, digest: [u8; 32]) -> Result<()> {
    if prior.position != position || prior.placement_digest != digest {
        return Err(conflict());
    }
    Ok(())
}

fn matches_digest(expected: [u8; 32], input: &[u8]) -> bool {
    expected.as_slice() == digest(&SHA256, input).as_ref()
}

fn root_binding_digest(binding: RootLandingBinding) -> [u8; 32] {
    let mut hash = Context::new(&SHA256);
    hash.update(b"rtdb.multi/root-landing-binding/v1\0");
    hash.update(&binding.to_bytes());
    let mut commitment = [0; 32];
    commitment.copy_from_slice(hash.finish().as_ref());
    commitment
}

fn root_authoring(
    family: Uuid, assertion_id: AssertionId, target: RootLandingTarget, codec: u32,
) -> AssertionAuthoring {
    let mut hash = Context::new(&SHA256);
    hash.update(b"rtdb.multi/root-landing-target/v1\0");
    hash.update(family.as_bytes());
    hash.update(&target.to_bytes());
    let mut scope_commitment = [0; 32];
    scope_commitment.copy_from_slice(hash.finish().as_ref());
    AssertionAuthoring { assertion_id, scope_commitment, codec }
}

fn link_prefix(reservation: LandingReservation, payload_digest: [u8; 32]) -> Vec<u8> {
    let mut body = Vec::with_capacity(128);
    body.extend_from_slice(&reservation.source.to_bytes());
    body.extend_from_slice(&reservation.review_commitment);
    body.extend_from_slice(&payload_digest);
    body
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::keys::KeyService;

    fn position(publication: u64) -> Position {
        Position { line_generation: [1; 16], publication, ordinal: NonZeroU64::MIN }
    }

    fn hash(bytes: &[u8]) -> [u8; 32] {
        let mut result = [0; 32];
        result.copy_from_slice(digest(&SHA256, bytes).as_ref());
        result
    }

    #[tokio::test]
    async fn root_landing_opens_only_after_the_exact_link_is_activated() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let region = IoRegion::new();
        let families = vault.assertion_families(Arc::clone(&region)).unwrap();
        let realm = Uuid::new_v4();
        let stage = || AssertionStage::new(realm, NonZeroU64::MIN, Uuid::new_v4()).unwrap();
        let family = Uuid::new_v4();
        let original = FamilyOccurrence {
            position: position(1), assertion: AssertionId::from_bytes([1; 32]), capability: stage(),
        };
        let reservation = LandingReservation {
            source: original.position,
            destination: AssertionId::from_bytes([2; 32]),
            capability: stage(),
            link_capability: stage(),
            review_commitment: [3; 32],
        };
        let target = RootLandingTarget { receipt: [4; 32], review_slot: 0, address_extent: [5; 32], disclosure: [6; 32] };
        let carrier = Bytes::from_static(b"unit-sealed carrier");
        families.enroll_original(family, EnrolledWorldRow {
            world: Uuid::from_bytes(original.position.line_generation),
            assertion: original.assertion,
            capability: original.capability,
            scope_commitment: [8; 32],
            codec: 1,
        }).await?;
        families.register(family, original).await?;
        families.reserve_landing(family, reservation).await?;
        let payload = Bytes::from(families.seal_root_landing_payload(family, reservation, target, &vault, carrier.clone()).await?);
        let link = Bytes::from(families.seal_root_landing_link(family, reservation, target, position(2), hash(&payload), &vault).await?);
        assert!(families.seal_root_landing_link(family, reservation, target, original.position, hash(&payload), &vault).await.is_err());
        let binding = RootLandingBinding {
            position: position(3), assertion: reservation.destination, stage: *reservation.capability.stage.as_bytes(), target,
            payload_digest: hash(&payload), link_digest: hash(&link),
        };
        let open = |binding| families.open_root_landing_payload(family, reservation.capability, binding, payload.clone(), &vault);
        assert!(open(binding).await.is_err());
        let substituted = RootLandingBinding { payload_digest: [7; 32], ..binding };
        assert!(families.activate_root_landing(family, reservation, substituted, link.clone(), &vault).await.is_err());
        families.activate_root_landing(family, reservation, binding, link.clone(), &vault).await?;
        vault.retire_assertion_stage(reservation.link_capability, &region).await?;
        families.activate_root_landing(family, reservation, binding, link.clone(), &vault).await?;
        assert!(open(RootLandingBinding { position: position(4), ..binding }).await.is_err());
        assert_eq!(open(binding).await?, carrier);
        let fence = families.fence(family, binding.position, Uuid::new_v4()).await?;
        assert_eq!(fence.positions, BTreeSet::from([original.position, binding.position]));
        assert!(open(binding).await.is_err());
        Ok(())
    }
}
