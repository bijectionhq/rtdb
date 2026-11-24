//! Flight decoding for assertion-family custody (dev/doc/rtdb-assertion-families.md).
//! The placed world host owns the vault; this module only frames its requests.

use std::num::NonZeroU64;

use arrow_flight::Result as ActionResult;
use bytes::{Bytes, BytesMut};
use rtdb_types::{AssertionId, PlacementBinding, Position};
use tonic::Status;
use uuid::Uuid;

use crate::{error::{invalid, status, unsupported}, service::{Call, Service}, wire};

pub(crate) const REGISTER_ASSERTION_FAMILY_V1: &str = "rtdb.owned.RegisterAssertionFamily.v1";
pub(crate) const RESERVE_FAMILY_LANDING_V1: &str = "rtdb.owned.ReserveFamilyLanding.v1";
pub(crate) const ACTIVATE_FAMILY_LANDING_V1: &str = "rtdb.owned.ActivateFamilyLanding.v1";
pub(crate) const SEAL_FAMILY_LANDING_LINK_V1: &str = "rtdb.owned.SealFamilyLandingLink.v1";
pub(crate) const REQUIRE_LIVE_FAMILY_OCCURRENCE_V1: &str = "rtdb.owned.RequireLiveFamilyOccurrence.v1";
pub(crate) const REQUIRE_FAMILY_POSITION_OPEN_V1: &str = "rtdb.owned.RequireFamilyPositionOpen.v1";
pub(crate) const REGISTER_FAMILY_DERIVED_V1: &str = "rtdb.owned.RegisterFamilyDerived.v1";
pub(crate) const SEAL_WORLD_OVERLAY_ROW_V1: &str = "rtdb.owned.SealWorldOverlayRow.v1";
pub(crate) const OPEN_WORLD_OVERLAY_ROW_V1: &str = "rtdb.owned.OpenWorldOverlayRow.v1";
pub(crate) const FENCE_ASSERTION_FAMILY_V1: &str = "rtdb.owned.FenceAssertionFamily.v1";
pub(crate) const SETTLE_ASSERTION_FAMILY_V1: &str = "rtdb.owned.SettleAssertionFamily.v1";

pub(crate) fn is_family_action(action: &str) -> bool {
    matches!(action, REGISTER_ASSERTION_FAMILY_V1 | RESERVE_FAMILY_LANDING_V1 | ACTIVATE_FAMILY_LANDING_V1
        | SEAL_FAMILY_LANDING_LINK_V1 | REQUIRE_LIVE_FAMILY_OCCURRENCE_V1 | REQUIRE_FAMILY_POSITION_OPEN_V1
        | REGISTER_FAMILY_DERIVED_V1 | SEAL_WORLD_OVERLAY_ROW_V1 | OPEN_WORLD_OVERLAY_ROW_V1
        | FENCE_ASSERTION_FAMILY_V1 | SETTLE_ASSERTION_FAMILY_V1)
}

const MAX_DERIVED_PER_REQUEST: usize = 256;
const MAX_RELATION_BYTES: usize = 255;
const MAX_KEY_BYTES: usize = 512;
/// The ordered log's message limit bounds a sealed row body.
const MAX_ROW_BYTES: usize = 4 * 1024 * 1024;

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

pub enum FamilyRequest {
    Register { family: Uuid, original: FamilyOccurrenceWire },
    ReserveLanding { family: Uuid, reservation: FamilyReservationWire },
    ActivateLanding { family: Uuid, placement: PlacementBinding, reservation: FamilyReservationWire },
    SealLandingLink {
        family: Uuid, reservation: FamilyReservationWire, placement: PlacementBinding,
        destination_scope: [u8; 32], intent: [u8; 32], review_slot: u64,
    },
    RequireLiveOccurrence { family: Uuid, occurrence: FamilyOccurrenceWire },
    RequirePositionOpen(Position),
    RegisterDerived { family: Uuid, rows: Vec<FamilyDerivedRow> },
    SealWorldRow { row: WorldRowAuthoring, plaintext: Bytes },
    OpenWorldRow { row: WorldRowAuthoring, envelope: Bytes },
    Fence { family: Uuid, selected: Position, order: Uuid },
    Settle { family: Uuid },
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
    Done,
    Sealed(Bytes),
    Opened(Bytes),
    Fence(FamilyFenceReply),
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

    fn finish(self) -> Result<(), Status> {
        if self.0.is_empty() { Ok(()) } else { Err(invalid("Unexpected input after the assertion family request")) }
    }
}

fn decode(action: &str, body: &Bytes) -> Result<FamilyRequest, Status> {
    let mut frame = Frame(body.as_ref());
    let request = match action {
        REGISTER_ASSERTION_FAMILY_V1 => FamilyRequest::Register { family: frame.uuid("family")?, original: frame.occurrence()? },
        RESERVE_FAMILY_LANDING_V1 => FamilyRequest::ReserveLanding { family: frame.uuid("family")?, reservation: frame.reservation()? },
        ACTIVATE_FAMILY_LANDING_V1 => FamilyRequest::ActivateLanding {
            family: frame.uuid("family")?, placement: frame.placement()?, reservation: frame.reservation()?,
        },
        SEAL_FAMILY_LANDING_LINK_V1 => {
            let family = frame.uuid("family")?;
            let reservation = frame.reservation()?;
            let placement = frame.placement()?;
            let destination_scope = frame.take("destination scope")?;
            let intent = frame.take::<32>("intent")?;
            if intent == [0; 32] { return Err(invalid("Assertion landing intent is zero")); }
            FamilyRequest::SealLandingLink { family, reservation, placement, destination_scope, intent, review_slot: frame.u64("review slot")? }
        }
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
        FENCE_ASSERTION_FAMILY_V1 => FamilyRequest::Fence { family: frame.uuid("family")?, selected: frame.position()?, order: frame.uuid("order")? },
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

impl Service {
    pub(crate) async fn assertion_family_action(&self, call: &Call, action: &str, body: &Bytes) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions || call.session.audience_source().is_some() { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let request = decode(action, body)?;
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        crate::owned_actions::require_idle(&session)?;
        let reply = host.assertion_family(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            request,
        ).await.map_err(status)?;
        let body = match reply {
            FamilyReply::Done => Bytes::from_static(b"\x01"),
            FamilyReply::Sealed(bytes) | FamilyReply::Opened(bytes) => {
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
