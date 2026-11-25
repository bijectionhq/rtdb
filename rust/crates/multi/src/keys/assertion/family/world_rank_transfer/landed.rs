use std::sync::Arc;

use bytes::Bytes;
use ring::digest::{SHA256, digest};
use rtdb_storage::IoRegion;
use rtdb_types::{Position, Result, RootFamilyDerivativeId, RootLandingBinding};
use uuid::Uuid;

use super::super::{
    AssertionEnvelope, AssertionKeyService, AssertionStage, Family, FamilyOccurrence, FamilyReleaseInput,
    family_vault_address, root_landing::{ROOT_PAYLOAD_CODEC, root_authoring},
};
use super::{
    MAX_CARRIER_BYTES, MAX_RANK_KEY_BYTES, MAX_RECORD_BYTES, WorldRankTransferDestination, WorldRankTransferSource,
    conflict, framed_digest, is_rank_relation, scoped, too_large, validate_header,
    wire::{Frame, put_header, put_stage, read_header},
};

pub(super) const AGREEMENT: &[u8; 4] = b"WRT2";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LandedRankTransfer {
    pub(crate) source: WorldRankTransferSource,
    pub(crate) destination: WorldRankTransferDestination,
    pub(crate) member: LandedRankTransferContributor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LandedRankTransferContributor {
    pub(crate) family: Uuid,
    pub(crate) stage: AssertionStage,
    pub(crate) landing: RootLandingBinding,
    pub(crate) carrier_position: Position,
    pub(crate) rank_position: Position,
    pub(crate) relation: String,
    pub(crate) key: Vec<u8>,
    pub(crate) carrier_sha256: [u8; 32],
    pub(crate) rank_payload_sha256: [u8; 32],
    // The capture authority commits the frozen journals and applied landing controls.
    pub(super) frozen_effects_sha256: [u8; 32],
}

impl LandedRankTransfer {
    pub(super) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_RECORD_BYTES { return Err(too_large()); }
        let mut frame = Frame(bytes);
        if &frame.take::<4>()? != AGREEMENT { return Err(conflict()); }
        let (source, destination) = read_header(&mut frame)?;
        if u16::from_be_bytes(frame.take()?) != 1 { return Err(conflict()); }
        let family = frame.uuid()?;
        let stage = frame.stage()?;
        let landing = RootLandingBinding::from_bytes(frame.take()?).map_err(|_| conflict())?;
        let carrier_position = frame.position()?;
        let rank_position = frame.position()?;
        let valid_to = i64::from_be_bytes(frame.take()?);
        let membership_until = i64::from_be_bytes(frame.take()?);
        if valid_to != i64::MAX || membership_until != i64::MAX { return Err(conflict()); }
        let relation = frame.counted()?;
        let key = frame.counted()?;
        if relation.len() > 255 || key.len() > MAX_RANK_KEY_BYTES { return Err(conflict()); }
        let member = LandedRankTransferContributor {
            family, stage, landing, carrier_position, rank_position,
            relation: std::str::from_utf8(relation).map_err(|_| conflict())?.to_owned(),
            key: key.to_vec(),
            carrier_sha256: frame.take()?,
            rank_payload_sha256: frame.take()?,
            frozen_effects_sha256: frame.take()?,
        };
        frame.finish()?;
        let agreement = Self { source, destination, member };
        agreement.validate()?;
        Ok(agreement)
    }

    pub(super) fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut out = AGREEMENT.to_vec();
        put_header(&mut out, self.source, self.destination);
        self.put_inventory(&mut out);
        Ok(out)
    }

    pub(super) fn inventory(&self) -> Result<[u8; 32]> {
        self.validate()?;
        let mut inventory = Vec::new();
        self.put_inventory(&mut inventory);
        Ok(framed_digest(b"rtdb.multi/landed-rank-transfer-inventory/v1\0", &[&inventory]))
    }

    fn put_inventory(&self, out: &mut Vec<u8>) {
        let member = &self.member;
        out.extend_from_slice(&1u16.to_be_bytes());
        out.extend_from_slice(member.family.as_bytes());
        put_stage(out, member.stage);
        out.extend_from_slice(&member.landing.to_bytes());
        out.extend_from_slice(&member.carrier_position.to_bytes());
        out.extend_from_slice(&member.rank_position.to_bytes());
        out.extend_from_slice(&i64::MAX.to_be_bytes());
        out.extend_from_slice(&i64::MAX.to_be_bytes());
        for field in [member.relation.as_bytes(), member.key.as_slice()] {
            out.extend_from_slice(&(field.len() as u16).to_be_bytes());
            out.extend_from_slice(field);
        }
        out.extend_from_slice(&member.carrier_sha256);
        out.extend_from_slice(&member.rank_payload_sha256);
        out.extend_from_slice(&member.frozen_effects_sha256);
    }

    fn validate(&self) -> Result<()> {
        validate_header(self.source, self.destination)?;
        let member = &self.member;
        let binding = member.landing;
        RootLandingBinding::from_bytes(binding.to_bytes()).map_err(|_| conflict())?;
        let selector = RootFamilyDerivativeId::parse(&member.key)
            .map_err(|_| conflict())?.ok_or_else(conflict)?;
        if selector.family != *member.family.as_bytes() || selector.member != binding.assertion
            || member.key.len() > MAX_RANK_KEY_BYTES || !is_rank_relation(&member.relation)
            || !scoped(member.stage, self.source) || binding.stage != *member.stage.identity().as_bytes()
            || binding.position.publication != self.source.original.carrier.system_time.0 as u64
            || [member.carrier_sha256, member.rank_payload_sha256, member.frozen_effects_sha256].contains(&[0; 32])
        {
            return Err(conflict());
        }
        let positions = [binding.position, member.carrier_position, member.rank_position];
        if positions.iter().any(|position| {
            position.line_generation != binding.position.line_generation
                || position.publication != binding.position.publication
                || u32::try_from(position.ordinal.get()).is_err()
        }) || positions[0] == positions[1] || positions[0] == positions[2] || positions[1] == positions[2] {
            return Err(conflict());
        }
        Ok(())
    }
}

impl LandedRankTransferContributor {
    pub(crate) fn release_input(&self) -> FamilyReleaseInput<'_> {
        FamilyReleaseInput::RootDerived {
            member: FamilyOccurrence {
                position: self.landing.position, assertion: self.landing.assertion, capability: self.stage,
            },
            relation: &self.relation,
            key: &self.key,
        }
    }

    pub(super) fn require_member(&self, family: &Family) -> Result<()> {
        family.require_unclaimed()?;
        // Classic archive evidence cannot attest the native group's live admission.
        if family.landings.iter().any(|landing| {
            landing.reservation.destination == self.landing.assertion && landing.native.is_some()
        }) {
            return Err(conflict());
        }
        family.require_root_landing(self.stage, self.landing)?;
        self.release_input().require_enrolled(family)
    }

    pub(crate) fn require_payload(&self, payload: &[u8]) -> Result<()> {
        if payload.is_empty() || payload.len() > MAX_CARRIER_BYTES
            || digest(&SHA256, payload).as_ref() != self.rank_payload_sha256.as_slice()
        {
            return Err(conflict());
        }
        Ok(())
    }

    pub(crate) async fn authenticate_carrier(
        &self, carrier: &[u8], keys: &dyn AssertionKeyService, region: &Arc<IoRegion>,
    ) -> Result<()> {
        if carrier.len() > MAX_CARRIER_BYTES
            || digest(&SHA256, carrier).as_ref() != self.carrier_sha256.as_slice()
        {
            return Err(conflict());
        }
        let mut prefix = Vec::with_capacity(229);
        prefix.extend_from_slice(b"BJRL1");
        prefix.extend_from_slice(self.family.as_bytes());
        put_stage(&mut prefix, self.stage);
        prefix.extend_from_slice(&self.landing.assertion.to_bytes());
        prefix.extend_from_slice(&self.landing.target.to_bytes());
        prefix.extend_from_slice(&self.landing.link_digest);
        let envelope = carrier.strip_prefix(prefix.as_slice()).ok_or_else(conflict)?;
        if digest(&SHA256, envelope).as_ref() != self.landing.payload_digest.as_slice() {
            return Err(conflict());
        }
        let family = family_vault_address(self.stage.deployment(), self.family);
        keys.verify_assertion_envelope(
            self.stage, root_authoring(family, self.landing.assertion, self.landing.target, ROOT_PAYLOAD_CODEC),
            region, AssertionEnvelope::try_from(Bytes::copy_from_slice(envelope))?,
        ).await
    }
}
