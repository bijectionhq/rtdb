//! Flight decoding for assertion-family custody (dev/doc/rtdb-assertion-families.md).
//! The placed world host owns the vault; this module only frames its requests.

use std::num::NonZeroU64;

use arrow_flight::Result as ActionResult;
use bytes::{Bytes, BytesMut};
use rtdb_types::{AssertionId, PlacementBinding, Position, RootLandingBinding, RootLandingTarget, Timestamp, TransactionKey, TxId};
use tonic::Status;
use uuid::Uuid;

use crate::{error::{invalid, status, unsupported}, service::{Call, Service}, state::Owner, wire};

pub(crate) const ASSERTION_FAMILY_SCOPE_V1: &str = "rtdb.owned.AssertionFamilyScope.v1";
pub(crate) const ENROLL_ASSERTION_FAMILY_V1: &str = "rtdb.owned.EnrollAssertionFamily.v1";
pub(crate) const ENROLL_FAMILY_WORLD_ROW_V1: &str = "rtdb.owned.EnrollFamilyWorldRow.v1";
pub(crate) const REGISTER_ASSERTION_FAMILY_V1: &str = "rtdb.owned.RegisterAssertionFamily.v1";
pub(crate) const RESERVE_FAMILY_LANDING_V1: &str = "rtdb.owned.ReserveFamilyLanding.v1";
pub(crate) const ACTIVATE_FAMILY_LANDING_V2: &str = "rtdb.owned.ActivateFamilyLanding.v2";
pub(crate) const SEAL_FAMILY_LANDING_LINK_V2: &str = "rtdb.owned.SealFamilyLandingLink.v2";
pub(crate) const SEAL_FAMILY_LANDING_PAYLOAD_V1: &str = "rtdb.owned.SealFamilyLandingPayload.v1";
pub(crate) const OPEN_FAMILY_LANDING_PAYLOAD_V1: &str = "rtdb.owned.OpenFamilyLandingPayload.v1";
pub(crate) const RESOLVE_FAMILY_MEMBER_V1: &str = "rtdb.owned.ResolveFamilyMember.v1";
pub(crate) const REQUIRE_LIVE_FAMILY_OCCURRENCE_V1: &str = "rtdb.owned.RequireLiveFamilyOccurrence.v1";
pub(crate) const REQUIRE_FAMILY_POSITION_OPEN_V1: &str = "rtdb.owned.RequireFamilyPositionOpen.v1";
pub(crate) const REGISTER_FAMILY_DERIVED_V1: &str = "rtdb.owned.RegisterFamilyDerived.v1";
pub(crate) const SEAL_WORLD_OVERLAY_ROW_V1: &str = "rtdb.owned.SealWorldOverlayRow.v1";
pub(crate) const OPEN_WORLD_OVERLAY_ROW_V1: &str = "rtdb.owned.OpenWorldOverlayRow.v1";
pub(crate) const PLAN_ASSERTION_FAMILY_FENCE_V1: &str = "rtdb.owned.PlanAssertionFamilyFence.v1";
pub(crate) const FAMILY_RESPONSE_RELEASE_V2: &str = "rtdb.owned.FamilyResponseRelease.v2";
pub(crate) const RETIRED_ORIGINAL_FAMILY_V1: &str = "rtdb.owned.RetiredOriginalFamily.v1";
pub(crate) const RETIRED_ORIGINAL_WORLD_RANK_V1: &str = "rtdb.owned.RetiredOriginalWorldRank.v1";
pub(crate) const SETTLE_ASSERTION_FAMILY_V1: &str = "rtdb.owned.SettleAssertionFamily.v1";

pub(crate) fn is_family_action(action: &str) -> bool {
    matches!(action, ASSERTION_FAMILY_SCOPE_V1 | ENROLL_ASSERTION_FAMILY_V1 | ENROLL_FAMILY_WORLD_ROW_V1
        | REGISTER_ASSERTION_FAMILY_V1 | RESERVE_FAMILY_LANDING_V1 | ACTIVATE_FAMILY_LANDING_V2
        | SEAL_FAMILY_LANDING_LINK_V2 | SEAL_FAMILY_LANDING_PAYLOAD_V1 | OPEN_FAMILY_LANDING_PAYLOAD_V1
        | RESOLVE_FAMILY_MEMBER_V1 | REQUIRE_LIVE_FAMILY_OCCURRENCE_V1 | REQUIRE_FAMILY_POSITION_OPEN_V1
        | REGISTER_FAMILY_DERIVED_V1 | SEAL_WORLD_OVERLAY_ROW_V1 | OPEN_WORLD_OVERLAY_ROW_V1
        | PLAN_ASSERTION_FAMILY_FENCE_V1 | SETTLE_ASSERTION_FAMILY_V1 | FAMILY_RESPONSE_RELEASE_V2
        | RETIRED_ORIGINAL_FAMILY_V1 | RETIRED_ORIGINAL_WORLD_RANK_V1)
}

const MAX_DERIVED_PER_REQUEST: usize = 256;
const MAX_RELATION_BYTES: usize = 255;
const MAX_KEY_BYTES: usize = 512;
/// The ordered log's message limit bounds a sealed row body.
const MAX_ROW_BYTES: usize = 4 * 1024 * 1024;
const MAX_ROOT_CARRIER_BYTES: usize = MAX_ROW_BYTES - 128;
const MAX_ROOT_LINK_BYTES: usize = 512;

/// An `AssertionStage` on the wire: deployment, incarnation and one-use slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct FamilyStage {
    pub deployment: Uuid,
    pub incarnation: NonZeroU64,
    pub stage: Uuid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FamilyOccurrenceWire {
    pub position: Position,
    pub assertion: AssertionId,
    pub capability: FamilyStage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FamilyReservationWire {
    pub source: Position,
    pub destination: AssertionId,
    pub capability: FamilyStage,
    pub link_capability: FamilyStage,
    pub review_commitment: [u8; 32],
}

/// A member's row in a derived search relation; `world` is absent for the Root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FamilyDerivedRow {
    pub member: AssertionId,
    pub world: Option<Uuid>,
    pub relation: String,
    pub key: Vec<u8>,
    pub capability: Option<FamilyStage>,
}

/// The exact world overlay row and authoring context of a seal or open.
#[derive(Clone, Debug)]
pub struct WorldRowAuthoring {
    pub family: Uuid,
    pub stage: FamilyStage,
    pub assertion: AssertionId,
    pub codec: u32,
    pub disclosure: [u8; 32],
    pub world: Uuid,
    pub relation: String,
    pub key: Vec<u8>,
}

pub enum FamilyReleaseInputWire {
    Member(FamilyOccurrenceWire),
    World { member: FamilyOccurrenceWire, winner: Position, row: WorldRowAuthoring },
    WorldDerived { member: FamilyOccurrenceWire, stage: FamilyStage, world: Uuid, relation: String, key: Vec<u8> },
    RootDerived { member: FamilyOccurrenceWire, relation: String, key: Vec<u8> },
    RootLanding { member: FamilyOccurrenceWire, binding: RootLandingBinding },
}

pub struct FamilyReleaseRegistrationWire {
    pub origin: [u8; 32],
    pub attempt: [u8; 32],
    pub part: [u8; 72],
    pub address: [u8; 188],
}

pub enum FamilyReleaseRequest {
    Prepare { family: Uuid, part: [u8; 72], cutoff: u64, input: FamilyReleaseInputWire },
    Stage { registration: FamilyReleaseRegistrationWire, input: FamilyReleaseInputWire },
    Promote(FamilyReleaseRegistrationWire),
    Check(FamilyReleaseRegistrationWire),
    Cancel(FamilyReleaseRegistrationWire),
    Dispose(FamilyReleaseRegistrationWire),
}

pub struct RetiredFamilyRequest {
    pub grant: [u8; 32],
    pub signature: [u8; 64],
    pub signed_body: Bytes,
}

pub enum RetiredFamilyOperation {
    Probe,
    Release(FamilyReleaseRequest),
    Open { registration: FamilyReleaseRegistrationWire, envelope: Bytes },
    PlanFence(Uuid),
    RecoverPreFence { registered: TransactionKey, registration: FamilyReleaseRegistrationWire },
}

impl RetiredFamilyRequest {
    pub fn operation(&self) -> Result<RetiredFamilyOperation, Status> {
        let mut frame = Frame(&self.signed_body);
        let operation = match frame.u8("retired family operation")? {
            0 => RetiredFamilyOperation::Probe,
            1 => RetiredFamilyOperation::Release(frame.family_release()?),
            2 => RetiredFamilyOperation::Open {
                registration: frame.release_registration()?,
                envelope: frame.tail(&self.signed_body, MAX_ROW_BYTES)?,
            },
            4 => RetiredFamilyOperation::PlanFence(frame.uuid("retired family order")?),
            5 => {
                let tx_id = i64::from_be_bytes(frame.take("historical registration transaction")?);
                let micros = i64::from_be_bytes(frame.take("historical registration time")?);
                if tx_id < 0 || micros <= 0 { return Err(invalid("Invalid historical registration transaction")); }
                RetiredFamilyOperation::RecoverPreFence {
                    registered: TransactionKey { tx_id: TxId(tx_id), system_time: Timestamp(micros) },
                    registration: frame.release_registration()?,
                }
            }
            _ => return Err(invalid("Invalid retired family operation")),
        };
        frame.finish()?;
        Ok(operation)
    }
}

pub struct RetiredWorldRankRequest {
    pub address: [u8; 32],
    pub signature: [u8; 64],
    pub signed_body: Bytes,
}

impl RetiredWorldRankRequest {
    pub fn release(tail: &Bytes) -> Result<FamilyReleaseRequest, Status> {
        let mut frame = Frame(tail);
        let request = frame.family_release()?;
        frame.finish()?;
        Ok(request)
    }

    pub fn open(tail: &Bytes) -> Result<(FamilyReleaseRegistrationWire, Bytes), Status> {
        let mut frame = Frame(tail);
        let registration = frame.release_registration()?;
        let carrier = frame.tail(tail, 1 << 20)?;
        Ok((registration, carrier))
    }
}

pub enum FamilyRequest {
    Scope,
    EnrollOriginal(WorldRowAuthoring),
    EnrollWorldRow { row: WorldRowAuthoring, member: AssertionId },
    Register { family: Uuid, original: FamilyOccurrenceWire },
    ReserveLanding { family: Uuid, reservation: FamilyReservationWire },
    ActivateLanding { family: Uuid, placement: PlacementBinding, reservation: FamilyReservationWire },
    SealLandingLink {
        family: Uuid, reservation: FamilyReservationWire, placement: PlacementBinding,
        destination_scope: [u8; 32], intent: [u8; 32], review_slot: u64,
    },
    ActivateRootLanding {
        family: Uuid, binding: RootLandingBinding, reservation: FamilyReservationWire, link: Bytes,
    },
    SealRootLandingLink {
        family: Uuid, reservation: FamilyReservationWire, target: RootLandingTarget,
        reviewed_source: Position, payload_digest: [u8; 32],
    },
    SealRootLandingPayload {
        family: Uuid, reservation: FamilyReservationWire, target: RootLandingTarget, carrier: Bytes,
    },
    OpenRootLandingPayload {
        family: Uuid, stage: FamilyStage, binding: RootLandingBinding, envelope: Bytes,
    },
    ResolveMember { family: Uuid, assertion: AssertionId },
    RequireLiveOccurrence { family: Uuid, occurrence: FamilyOccurrenceWire },
    RequirePositionOpen(Position),
    RegisterDerived { family: Uuid, rows: Vec<FamilyDerivedRow> },
    SealWorldRow { row: WorldRowAuthoring, plaintext: Bytes },
    OpenWorldRow { row: WorldRowAuthoring, envelope: Bytes },
    PlanFence { family: Uuid, selected: Position, order: Uuid },
    Settle { family: Uuid },
    Release(FamilyReleaseRequest),
    Retired(RetiredFamilyRequest),
    RetiredWorldRank(RetiredWorldRankRequest),
}

pub struct FamilyFenceReply {
    pub order: Uuid,
    pub inventory: [u8; 32],
    pub positions: Vec<Position>,
    pub assertion_capabilities: Vec<FamilyStage>,
    pub link_capabilities: Vec<FamilyStage>,
    pub derived: Vec<FamilyDerivedRow>,
}

pub enum FamilyReply {
    Scope { deployment: Uuid, incarnation: NonZeroU64, root_line: [u8; 16] },
    Done,
    Member(FamilyOccurrenceWire),
    Sealed(Bytes),
    Opened(Bytes),
    Fence(FamilyFenceReply),
    ReleaseRegistration(Bytes),
    RankTransferPrepared(Bytes),
}

struct Frame<'a>(&'a [u8]);

impl<'a> Frame<'a> {
    fn take<const N: usize>(&mut self, what: &str) -> Result<[u8; N], Status> {
        let (bytes, rest) = self.0.split_first_chunk::<N>().ok_or_else(|| invalid(format!("Assertion family request has no {what}")))?;
        self.0 = rest;
        Ok(*bytes)
    }

    fn bytes(&mut self, length: usize, what: &str) -> Result<&'a [u8], Status> {
        let (bytes, rest) = self.0.split_at_checked(length).ok_or_else(|| invalid(format!("Assertion family request truncates its {what}")))?;
        self.0 = rest;
        Ok(bytes)
    }

    fn uuid(&mut self, what: &str) -> Result<Uuid, Status> {
        let id = Uuid::from_bytes(self.take::<16>(what)?);
        if id.is_nil() { return Err(invalid(format!("Assertion family {what} is nil"))); }
        Ok(id)
    }

    fn u8(&mut self, what: &str) -> Result<u8, Status> { Ok(self.take::<1>(what)?[0]) }
    fn u16(&mut self, what: &str) -> Result<u16, Status> { Ok(u16::from_be_bytes(self.take(what)?)) }
    fn u32(&mut self, what: &str) -> Result<u32, Status> { Ok(u32::from_be_bytes(self.take(what)?)) }
    fn u64(&mut self, what: &str) -> Result<u64, Status> { Ok(u64::from_be_bytes(self.take(what)?)) }

    fn position(&mut self) -> Result<Position, Status> {
        Position::from_bytes(self.take("position")?).map_err(|_| invalid("Invalid assertion position"))
    }

    fn assertion(&mut self) -> Result<AssertionId, Status> {
        Ok(AssertionId::from_bytes(self.take("assertion")?))
    }

    fn stage(&mut self) -> Result<FamilyStage, Status> {
        let deployment = self.uuid("capability deployment")?;
        let incarnation = NonZeroU64::new(self.u64("capability incarnation")?)
            .ok_or_else(|| invalid("Assertion capability incarnation is zero"))?;
        let stage = self.uuid("capability")?;
        Ok(FamilyStage { deployment, incarnation, stage })
    }

    fn occurrence(&mut self) -> Result<FamilyOccurrenceWire, Status> {
        Ok(FamilyOccurrenceWire { position: self.position()?, assertion: self.assertion()?, capability: self.stage()? })
    }

    fn reservation(&mut self) -> Result<FamilyReservationWire, Status> {
        let source = self.position()?;
        let destination = self.assertion()?;
        let capability = self.stage()?;
        let link_capability = self.stage()?;
        let review_commitment = self.take::<32>("review commitment")?;
        if review_commitment == [0; 32] { return Err(invalid("Assertion landing review commitment is zero")); }
        Ok(FamilyReservationWire { source, destination, capability, link_capability, review_commitment })
    }

    fn placement(&mut self) -> Result<PlacementBinding, Status> {
        PlacementBinding::from_bytes(self.take("placement")?).map_err(|_| invalid("Invalid assertion placement"))
    }

    fn text(&mut self, what: &str) -> Result<String, Status> {
        let length = usize::from(self.u16(what)?);
        if length == 0 || length > MAX_RELATION_BYTES { return Err(invalid(format!("Invalid assertion family {what} length"))); }
        let bytes = self.bytes(length, what)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| invalid(format!("Assertion family {what} is not UTF-8")))
    }

    fn key(&mut self) -> Result<Vec<u8>, Status> {
        let length = usize::from(self.u16("row key")?);
        if length == 0 || length > MAX_KEY_BYTES { return Err(invalid("Invalid assertion family row key length")); }
        Ok(self.bytes(length, "row key")?.to_vec())
    }

    fn derived(&mut self) -> Result<FamilyDerivedRow, Status> {
        let member = self.assertion()?;
        let world = match self.u8("derived scope")? {
            0 => None,
            1 => Some(self.uuid("derived world")?),
            _ => return Err(invalid("Invalid derived row scope")),
        };
        let relation = self.text("derived relation")?;
        let key = self.key()?;
        let capability = match self.u8("derived capability flag")? {
            0 => None,
            1 => Some(self.stage()?),
            _ => return Err(invalid("Invalid derived capability flag")),
        };
        if world.is_some() != capability.is_some() {
            return Err(invalid("A world derived row carries its capability and a Root derived row carries none"));
        }
        Ok(FamilyDerivedRow { member, world, relation, key, capability })
    }

    fn world_row(&mut self) -> Result<WorldRowAuthoring, Status> {
        Ok(WorldRowAuthoring {
            family: self.uuid("family")?,
            stage: self.stage()?,
            assertion: self.assertion()?,
            codec: self.u32("codec")?,
            disclosure: self.take("disclosure scope")?,
            world: self.uuid("world")?,
            relation: self.text("relation")?,
            key: self.key()?,
        })
    }

    fn release_input(&mut self) -> Result<FamilyReleaseInputWire, Status> {
        let tag = self.u8("release input kind")?;
        let member = self.occurrence()?;
        match tag {
            0 => Ok(FamilyReleaseInputWire::Member(member)),
            1 => Ok(FamilyReleaseInputWire::World { member, winner: self.position()?, row: self.world_row()? }),
            2 => Ok(FamilyReleaseInputWire::WorldDerived {
                member, stage: self.stage()?, world: self.uuid("derived world")?,
                relation: self.text("derived relation")?, key: self.key()?,
            }),
            3 => Ok(FamilyReleaseInputWire::RootDerived {
                member, relation: self.text("derived relation")?, key: self.key()?,
            }),
            4 => Ok(FamilyReleaseInputWire::RootLanding { member, binding: self.root_binding()? }),
            _ => Err(invalid("Invalid family release input kind")),
        }
    }

    fn release_registration(&mut self) -> Result<FamilyReleaseRegistrationWire, Status> {
        Ok(FamilyReleaseRegistrationWire {
            origin: self.take("release origin")?, attempt: self.take("release attempt")?,
            part: self.take("response part")?, address: self.take("recovery address")?,
        })
    }

    fn family_release(&mut self) -> Result<FamilyReleaseRequest, Status> {
        Ok(match self.u8("release operation")? {
            0 => FamilyReleaseRequest::Prepare {
                family: self.uuid("family")?, part: self.take("response part")?,
                cutoff: self.u64("release cutoff")?, input: self.release_input()?,
            },
            1 => FamilyReleaseRequest::Stage { registration: self.release_registration()?, input: self.release_input()? },
            2 => FamilyReleaseRequest::Promote(self.release_registration()?),
            3 => FamilyReleaseRequest::Check(self.release_registration()?),
            4 => FamilyReleaseRequest::Cancel(self.release_registration()?),
            5 => FamilyReleaseRequest::Dispose(self.release_registration()?),
            _ => return Err(invalid("Invalid family release operation")),
        })
    }

    fn root_target(&mut self) -> Result<RootLandingTarget, Status> {
        RootLandingTarget::from_bytes(self.take("Root landing target")?)
            .map_err(|_| invalid("Invalid Root landing target"))
    }

    fn root_binding(&mut self) -> Result<RootLandingBinding, Status> {
        RootLandingBinding::from_bytes(self.take("Root landing binding")?)
            .map_err(|_| invalid("Invalid Root landing binding"))
    }

    fn tail(&mut self, body: &Bytes, max: usize) -> Result<Bytes, Status> {
        if self.0.is_empty() || self.0.len() > max {
            return Err(invalid("Invalid assertion family body length"));
        }
        let rest = body.slice(body.len() - self.0.len()..);
        self.0 = &[];
        Ok(rest)
    }

    fn finish(self) -> Result<(), Status> {
        if self.0.is_empty() { Ok(()) } else { Err(invalid("Unexpected input after the assertion family request")) }
    }
}

fn decode(action: &str, body: &Bytes) -> Result<FamilyRequest, Status> {
    let mut frame = Frame(body.as_ref());
    let request = match action {
        ASSERTION_FAMILY_SCOPE_V1 => FamilyRequest::Scope,
        ENROLL_ASSERTION_FAMILY_V1 => FamilyRequest::EnrollOriginal(frame.world_row()?),
        ENROLL_FAMILY_WORLD_ROW_V1 => FamilyRequest::EnrollWorldRow {
            row: frame.world_row()?, member: frame.assertion()?,
        },
        REGISTER_ASSERTION_FAMILY_V1 => FamilyRequest::Register { family: frame.uuid("family")?, original: frame.occurrence()? },
        RESERVE_FAMILY_LANDING_V1 => FamilyRequest::ReserveLanding { family: frame.uuid("family")?, reservation: frame.reservation()? },
        ACTIVATE_FAMILY_LANDING_V2 => match frame.u8("landing binding kind")? {
            0 => FamilyRequest::ActivateLanding {
                family: frame.uuid("family")?, placement: frame.placement()?, reservation: frame.reservation()?,
            },
            1 => FamilyRequest::ActivateRootLanding {
                family: frame.uuid("family")?, binding: frame.root_binding()?,
                reservation: frame.reservation()?, link: frame.tail(body, MAX_ROOT_LINK_BYTES)?,
            },
            _ => return Err(invalid("Invalid landing binding kind")),
        },
        SEAL_FAMILY_LANDING_LINK_V2 => match frame.u8("landing binding kind")? {
            0 => {
                let family = frame.uuid("family")?;
                let reservation = frame.reservation()?;
                let placement = frame.placement()?;
                let destination_scope = frame.take("destination scope")?;
                let intent = frame.take::<32>("intent")?;
                if intent == [0; 32] { return Err(invalid("Assertion landing intent is zero")); }
                FamilyRequest::SealLandingLink { family, reservation, placement, destination_scope, intent, review_slot: frame.u64("review slot")? }
            }
            1 => {
                let family = frame.uuid("family")?;
                let reservation = frame.reservation()?;
                let target = frame.root_target()?;
                let reviewed_source = frame.position()?;
                let payload_digest = frame.take::<32>("destination envelope digest")?;
                if payload_digest == [0; 32] { return Err(invalid("Invalid destination envelope digest")); }
                FamilyRequest::SealRootLandingLink { family, reservation, target, reviewed_source, payload_digest }
            }
            _ => return Err(invalid("Invalid landing binding kind")),
        },
        SEAL_FAMILY_LANDING_PAYLOAD_V1 => FamilyRequest::SealRootLandingPayload {
            family: frame.uuid("family")?, reservation: frame.reservation()?,
            target: frame.root_target()?, carrier: frame.tail(body, MAX_ROOT_CARRIER_BYTES)?,
        },
        OPEN_FAMILY_LANDING_PAYLOAD_V1 => FamilyRequest::OpenRootLandingPayload {
            family: frame.uuid("family")?, stage: frame.stage()?,
            binding: frame.root_binding()?, envelope: frame.tail(body, MAX_ROW_BYTES)?,
        },
        RESOLVE_FAMILY_MEMBER_V1 => FamilyRequest::ResolveMember { family: frame.uuid("family")?, assertion: frame.assertion()? },
        REQUIRE_LIVE_FAMILY_OCCURRENCE_V1 => FamilyRequest::RequireLiveOccurrence { family: frame.uuid("family")?, occurrence: frame.occurrence()? },
        REQUIRE_FAMILY_POSITION_OPEN_V1 => FamilyRequest::RequirePositionOpen(frame.position()?),
        REGISTER_FAMILY_DERIVED_V1 => {
            let family = frame.uuid("family")?;
            let count = usize::from(frame.u16("derived count")?);
            if count == 0 || count > MAX_DERIVED_PER_REQUEST { return Err(invalid("Invalid derived row count")); }
            let rows = (0..count).map(|_| frame.derived()).collect::<Result<Vec<_>, _>>()?;
            FamilyRequest::RegisterDerived { family, rows }
        }
        SEAL_WORLD_OVERLAY_ROW_V1 | OPEN_WORLD_OVERLAY_ROW_V1 => {
            let row = frame.world_row()?;
            let rest = body.slice(body.len() - frame.0.len()..);
            frame.0 = &[];
            if rest.is_empty() || rest.len() > MAX_ROW_BYTES { return Err(invalid("Invalid world overlay row body length")); }
            if action == SEAL_WORLD_OVERLAY_ROW_V1 {
                FamilyRequest::SealWorldRow { row, plaintext: rest }
            } else {
                FamilyRequest::OpenWorldRow { row, envelope: rest }
            }
        }
        PLAN_ASSERTION_FAMILY_FENCE_V1 => FamilyRequest::PlanFence {
            family: frame.uuid("family")?, selected: frame.position()?, order: frame.uuid("order")?,
        },
        FAMILY_RESPONSE_RELEASE_V2 => FamilyRequest::Release(frame.family_release()?),
        RETIRED_ORIGINAL_FAMILY_V1 => FamilyRequest::Retired(RetiredFamilyRequest {
            grant: frame.take("custody grant")?,
            signature: frame.take("custody reader signature")?,
            signed_body: frame.tail(body, 325 + MAX_ROW_BYTES)?,
        }),
        RETIRED_ORIGINAL_WORLD_RANK_V1 => FamilyRequest::RetiredWorldRank(RetiredWorldRankRequest {
            address: frame.take("World rank transfer address")?,
            signature: frame.take("World rank transfer signature")?,
            signed_body: frame.tail(body, MAX_ROW_BYTES - 128)?,
        }),
        SETTLE_ASSERTION_FAMILY_V1 => FamilyRequest::Settle { family: frame.uuid("family")? },
        _ => return Err(unsupported()),
    };
    frame.finish()?;
    Ok(request)
}

fn put_stage(out: &mut BytesMut, stage: FamilyStage) {
    out.extend_from_slice(stage.deployment.as_bytes());
    out.extend_from_slice(&stage.incarnation.get().to_be_bytes());
    out.extend_from_slice(stage.stage.as_bytes());
}

fn put_count(out: &mut BytesMut, count: usize) -> Result<(), Status> {
    out.extend_from_slice(&u32::try_from(count).map_err(|_| Status::internal("Assertion fence inventory overflow"))?.to_be_bytes());
    Ok(())
}

fn put_derived(out: &mut BytesMut, row: &FamilyDerivedRow) -> Result<(), Status> {
    out.extend_from_slice(&row.member.to_bytes());
    match row.world {
        Some(world) => { out.extend_from_slice(&[1]); out.extend_from_slice(world.as_bytes()); }
        None => out.extend_from_slice(&[0]),
    }
    for field in [row.relation.as_bytes(), row.key.as_slice()] {
        let length = u16::try_from(field.len()).map_err(|_| Status::internal("Assertion derived row overflow"))?;
        out.extend_from_slice(&length.to_be_bytes());
        out.extend_from_slice(field);
    }
    match row.capability {
        Some(stage) => { out.extend_from_slice(&[1]); put_stage(out, stage); }
        None => out.extend_from_slice(&[0]),
    }
    Ok(())
}

fn encode_fence(fence: &FamilyFenceReply) -> Result<BytesMut, Status> {
    let mut out = BytesMut::with_capacity(48 + 16 + fence.positions.len() * Position::WIDTH
        + (fence.assertion_capabilities.len() + fence.link_capabilities.len()) * 40 + fence.derived.len() * 128);
    out.extend_from_slice(fence.order.as_bytes());
    out.extend_from_slice(&fence.inventory);
    put_count(&mut out, fence.positions.len())?;
    for position in &fence.positions { out.extend_from_slice(&position.to_bytes()); }
    for stages in [&fence.assertion_capabilities, &fence.link_capabilities] {
        put_count(&mut out, stages.len())?;
        for &stage in stages { put_stage(&mut out, stage); }
    }
    put_count(&mut out, fence.derived.len())?;
    for row in &fence.derived { put_derived(&mut out, row)?; }
    Ok(out)
}

fn is_authenticated_original_attachment(owner: Owner, placement: Option<&str>, database: &str) -> bool {
    matches!(owner, Owner::Authenticated(_)) && placement == Some(database)
}

impl Service {
    pub(crate) fn supports_retired_original_family(&self, call: &Call) -> bool {
        self.0.config.owned_native_actions && self.0.config.owned_world_host.as_ref()
            .is_some_and(|host| host.supports_retired_original_family())
            && !call.session.fresh_replica
            && is_authenticated_original_attachment(call.session.caller.owner, call.session.placed_database(), &call.database)
    }

    pub(crate) async fn assertion_family_action(&self, call: &Call, action: &str, body: &Bytes) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        if (action == FAMILY_RESPONSE_RELEASE_V2 && !self.supports_hosted_upload(call))
            || (matches!(action, RETIRED_ORIGINAL_FAMILY_V1 | RETIRED_ORIGINAL_WORLD_RANK_V1)
                && !self.supports_retired_original_family(call))
        {
            return Err(unsupported());
        }
        let request = decode(action, body)?;
        let _world_rank_charge = if let FamilyRequest::RetiredWorldRank(request) = &request {
            Some(call.reserve(wire::mul(request.signed_body.len(), 4)?)?)
        } else {
            None
        };
        if let FamilyRequest::Retired(request) = &request
            && matches!(request.signed_body.first(), Some(3 | 5))
            && !self.supports_hosted_upload(call)
        {
            return Err(unsupported());
        }
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        crate::owned_actions::require_idle(&session)?;
        if let FamilyRequest::Retired(request) = &request && request.signed_body.first() == Some(&3) {
            let enrollment = host.retired_family_fence(
                crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
                request,
            ).await.map_err(status)?;
            drop(session);
            let reply = self.decide_retired_family(call, &request.signed_body.slice(1..), &enrollment).await?;
            return Ok(reply);
        }
        let reply = host.assertion_family(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            request,
        ).await.map_err(status)?;
        let body = match reply {
            FamilyReply::Scope { deployment, incarnation, root_line } => {
                let mut out = BytesMut::with_capacity(40);
                out.extend_from_slice(deployment.as_bytes());
                out.extend_from_slice(&incarnation.get().to_be_bytes());
                out.extend_from_slice(&root_line);
                out.freeze()
            }
            FamilyReply::Done => Bytes::from_static(b"\x01"),
            FamilyReply::Member(member) => {
                let mut out = BytesMut::with_capacity(104);
                out.extend_from_slice(&member.position.to_bytes());
                out.extend_from_slice(&member.assertion.to_bytes());
                put_stage(&mut out, member.capability);
                out.freeze()
            },
            FamilyReply::Sealed(bytes) | FamilyReply::Opened(bytes) | FamilyReply::ReleaseRegistration(bytes)
            | FamilyReply::RankTransferPrepared(bytes) => {
                let charge = call.reserve(bytes.len())?;
                wire::retain(bytes, charge)
            }
            FamilyReply::Fence(fence) => {
                let encoded = encode_fence(&fence)?;
                let charge = call.reserve(encoded.len())?;
                wire::retain(encoded.freeze(), charge)
            }
        };
        Ok(ActionResult { body })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_world_rank_preserves_the_raw_signed_body_and_exact_carrier() {
        let mut bytes = vec![2; 32];
        bytes.extend_from_slice(&[3; 64]);
        bytes.extend_from_slice(&[2, 0, 255, 0]);
        let body = Bytes::from(bytes);
        let FamilyRequest::RetiredWorldRank(request) = decode(RETIRED_ORIGINAL_WORLD_RANK_V1, &body).unwrap()
            else { panic!("expected retired World rank custody"); };
        assert_eq!(request.address, [2; 32]);
        assert_eq!(request.signature, [3; 64]);
        assert_eq!(request.signed_body, body.slice(96..));
        assert!(decode(RETIRED_ORIGINAL_WORLD_RANK_V1, &body.slice(..96)).is_err());

        let mut bytes = vec![4; 324];
        let carrier = b"BJWP1\0\xff\0";
        bytes.extend_from_slice(carrier);
        let body = Bytes::from(bytes);
        let (registration, decoded) = RetiredWorldRankRequest::open(&body).unwrap();
        assert_eq!(registration.address, [4; 188]);
        assert_eq!(decoded.as_ref(), carrier);
        assert!(RetiredWorldRankRequest::open(&body.slice(..324)).is_err());
    }

    #[test]
    fn retired_family_transport_accepts_only_a_named_authenticated_attachment() {
        let reader = Owner::Authenticated(Uuid::from_u128(1));
        assert!(is_authenticated_original_attachment(reader, Some("original"), "original"));
        assert!(!is_authenticated_original_attachment(reader, Some("successor"), "original"));
        assert!(!is_authenticated_original_attachment(reader, None, "original"));
        assert!(!is_authenticated_original_attachment(Owner::Anonymous, Some("original"), "original"));
    }

    #[test]
    fn retired_family_frames_keep_the_signed_operation_and_exact_payload() {
        let mut body = vec![6; 96];
        body.push(0);
        let decoded = decode(RETIRED_ORIGINAL_FAMILY_V1, &Bytes::from(body.clone())).unwrap();
        let FamilyRequest::Retired(request) = decoded else { panic!("a retired family frame") };
        assert_eq!(request.grant, [6; 32]);
        assert_eq!(request.signature, [6; 64]);
        assert!(matches!(request.operation().unwrap(), RetiredFamilyOperation::Probe));
        body.push(0);
        let decoded = decode(RETIRED_ORIGINAL_FAMILY_V1, &Bytes::from(body)).unwrap();
        let FamilyRequest::Retired(request) = decoded else { panic!("a retired family frame") };
        assert!(request.operation().is_err());
    }

    #[test]
    fn historical_recovery_requires_the_pre_fence_transaction_and_exact_registration_frame() {
        let mut body = vec![5];
        body.extend_from_slice(&12_i64.to_be_bytes());
        body.extend_from_slice(&34_i64.to_be_bytes());
        body.extend_from_slice(&[6; 324]);
        let request = |body: &[u8]| RetiredFamilyRequest {
            grant: [1; 32], signature: [2; 64], signed_body: Bytes::copy_from_slice(body),
        };
        let RetiredFamilyOperation::RecoverPreFence { registered, registration } = request(&body).operation().unwrap()
            else { panic!("a historical disposition request") };
        assert_eq!(registered, TransactionKey { tx_id: TxId(12), system_time: Timestamp(34) });
        assert_eq!(registration.address, [6; 188]);
        assert!(request(&body[..body.len() - 1]).operation().is_err());
        body[9..17].fill(0);
        assert!(request(&body).operation().is_err());
    }

    #[test]
    fn release_prepare_decodes_an_exact_root_landing_binding() {
        let member = FamilyOccurrenceWire {
            position: Position { line_generation: [1; 16], publication: 2, ordinal: NonZeroU64::MIN },
            assertion: AssertionId::from_bytes([3; 32]),
            capability: FamilyStage {
                deployment: Uuid::from_bytes([4; 16]), incarnation: NonZeroU64::MIN, stage: Uuid::from_bytes([5; 16]),
            },
        };
        let binding = RootLandingBinding {
            position: member.position, assertion: member.assertion, stage: *member.capability.stage.as_bytes(),
            target: RootLandingTarget { receipt: [6; 32], review_slot: 0, address_extent: [7; 32], disclosure: [8; 32] },
            payload_digest: [9; 32], link_digest: [10; 32],
        };
        let mut request = BytesMut::new();
        request.extend_from_slice(&[0]);
        request.extend_from_slice(&[11; 16]);
        request.extend_from_slice(&[12; 72]);
        request.extend_from_slice(&13_u64.to_be_bytes());
        request.extend_from_slice(&[4]);
        request.extend_from_slice(&member.position.to_bytes());
        request.extend_from_slice(&member.assertion.to_bytes());
        put_stage(&mut request, member.capability);
        request.extend_from_slice(&binding.to_bytes());
        let request = request.freeze();
        let decoded = decode(FAMILY_RESPONSE_RELEASE_V2, &request).unwrap();
        assert!(matches!(decoded, FamilyRequest::Release(FamilyReleaseRequest::Prepare {
            input: FamilyReleaseInputWire::RootLanding { member: decoded_member, binding: decoded_binding }, ..
        }) if decoded_member == member && decoded_binding == binding));
        assert!(decode(FAMILY_RESPONSE_RELEASE_V2, &request.slice(..request.len() - 1)).is_err());
        let mut extended = request.to_vec();
        extended.push(0);
        assert!(decode(FAMILY_RESPONSE_RELEASE_V2, &extended.into()).is_err());
    }
}
