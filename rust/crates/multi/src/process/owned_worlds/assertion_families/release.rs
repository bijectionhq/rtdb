use std::{fmt::Write, num::NonZeroU64};

use rtdb_columnar::{ArrowCell, OwnedBatch};
use rtdb_flight_sql::{FamilyReleaseInputWire, FamilyReleaseRegistrationWire, FamilyReleaseRequest};
use rtdb_node::{AppliedReadFloor, CurrentAssertionFamilyRead, OriginalAssertionFamilyReader};
use rtdb_types::{Timestamp, Value};
use tokio_util::sync::CancellationToken;

use crate::keys::{
    AssertionFamilies, CarrierJournalIdentity, FamilyReleaseAuthority, FamilyReleaseInput,
    FamilyReleaseObservation, FamilyReleaseRegistration, FamilyResponsePart,
};

use super::*;

struct CarrierJournal<'a> {
    original: &'a OriginalAssertionFamilyReader,
    cancellation: &'a CancellationToken,
    floor: AppliedReadFloor,
    identity: CarrierJournalIdentity,
    writer: String,
}

impl<'a> CarrierJournal<'a> {
    async fn open(original: &'a OriginalAssertionFamilyReader, cancellation: &'a CancellationToken, deployment: Uuid) -> Result<Self> {
        let current = original.current(deployment, cancellation).await?;
        let applied = current.applied_source().ok_or_else(OwnedWorld::unavailable)?;
        let floor = AppliedReadFloor::new(deployment, vec![applied])?;
        let (installation, writer) = Self::installation(&current).await?;
        let identity = carrier_identity(deployment, original.history_fingerprint(), installation, &writer);
        Ok(Self { original, cancellation, floor, identity, writer })
    }

    async fn installation(current: &CurrentAssertionFamilyRead) -> Result<([u8; 16], String)> {
        let point = current.carrier_journal_installation().await?;
        let (row, _) = point.row.ok_or_else(OwnedWorld::unavailable)?;
        let row = row.batch();
        let cell = |name| ArrowCell::named(row, name, 0);
        if cell("kind").and_then(ArrowCell::utf8) != Some("hosted") {
            return Err(OwnedWorld::unavailable());
        }
        let installation = cell("incarnation").and_then(ArrowCell::binary)
            .and_then(|bytes| bytes.try_into().ok()).filter(|bytes| *bytes != [0; 16]).ok_or_else(OwnedWorld::unavailable)?;
        let writer = cell("writer").and_then(ArrowCell::utf8)
            .filter(|writer| !writer.is_empty() && writer.len() <= 128).ok_or_else(OwnedWorld::unavailable)?.to_owned();
        Ok((installation, writer))
    }
}

pub(super) fn carrier_identity(deployment: Uuid, source_history: [u8; 32], installation: [u8; 16], writer: &str) -> CarrierJournalIdentity {
    use crate::family_custody::framed_digest;

    CarrierJournalIdentity {
        deployment, source_history, installation,
        registry: framed_digest(b"rtdb.multi/carrier-registry/v1\0", &[&source_history, &installation, b"public.bj_external_key_recovery"]),
        original_writer: framed_digest(b"rtdb.multi/carrier-writer/v1\0", &[&installation, writer.as_bytes()]),
    }
}

pub(super) fn lower_hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes { write!(text, "{byte:02x}").expect("writing to a string"); }
    text
}

#[async_trait::async_trait]
impl FamilyReleaseAuthority for CarrierJournal<'_> {
    fn identity(&self) -> CarrierJournalIdentity { self.identity }

    async fn observe(&self, registration: FamilyReleaseRegistration) -> Result<FamilyReleaseObservation> {
        let unavailable = OwnedWorld::unavailable;
        if registration.owner() != self.identity { return Err(unavailable()); }
        let current = self.original.current_covering(&self.floor, self.cancellation).await?;
        let (installation, writer) = Self::installation(&current).await?;
        if installation != self.identity.installation || writer != self.writer { return Err(unavailable()); }
        Ok(observe_registry(&current, &self.writer, registration).await?.0)
    }
}

pub(super) async fn observe_registry(
    current: &CurrentAssertionFamilyRead, creator: &str, registration: FamilyReleaseRegistration,
) -> Result<(FamilyReleaseObservation, (OwnedBatch, Timestamp))> {
    let unavailable = OwnedWorld::unavailable;
    let part = registration.part();
    let cell_history = lower_hex(&part.cell_history);
    let part_key = [cell_history.clone(), lower_hex(&part.usher), lower_hex(&part.conductor),
        lower_hex(&part.response), lower_hex(&part.ordinal.get().to_be_bytes())].join("/");
    let origin = lower_hex(&registration.origin());
    let key = format!("{origin}/{}/{part_key}", lower_hex(&registration.attempt()));
    let point = current.carrier_journal_registration(&key).await?;
    let (row, revision) = point.row.ok_or_else(unavailable)?;
    let cell = |name| ArrowCell::named(row.batch(), name, 0).ok_or_else(unavailable);
    let value = |name| {
        let cell = cell(name)?;
        rtdb_columnar::read_value(cell.field, cell.array, cell.row)
    };
    if cell("creator")?.utf8() != Some(creator)
        || cell("origin")?.utf8() != Some(origin.as_str())
        || cell("cell_history")?.utf8() != Some(cell_history.as_str())
        || cell("part")?.utf8() != Some(part_key.as_str())
        || cell("address")?.binary() != Some(registration.address().as_slice())
        || !matches!(value("managed_until")?, Value::Null | Value::Undefined)
    {
        return Err(unavailable());
    }
    let observation = match value("disposed")? {
        Value::Bool(false) if value("producer_done")? == Value::Bool(false) => FamilyReleaseObservation::Live,
        Value::Bool(true) => FamilyReleaseObservation::Disposed,
        _ => return Err(unavailable()),
    };
    Ok((observation, (row, revision)))
}

pub(super) fn part(bytes: [u8; 72]) -> Result<FamilyResponsePart> {
    let field = |start| bytes[start..start + 16].try_into().expect("response field width");
    let ordinal = NonZeroU64::new(u64::from_be_bytes(bytes[64..].try_into().expect("ordinal width")))
        .ok_or_else(OwnedWorld::unavailable)?;
    Ok(FamilyResponsePart {
        cell_history: field(0), usher: field(16), conductor: field(32), response: field(48), ordinal,
    })
}

fn input<'a>(scope: &Scope, family: Uuid, wire: &'a FamilyReleaseInputWire) -> Result<FamilyReleaseInput<'a>> {
    Ok(match wire {
        FamilyReleaseInputWire::Member(member) => FamilyReleaseInput::Member(scope.occurrence(*member)?),
        FamilyReleaseInputWire::World { member, winner, row } if row.family == family =>
            FamilyReleaseInput::WorldRow { member: scope.occurrence(*member)?, winner: *winner, row: scope.enrolled_row(row)? },
        FamilyReleaseInputWire::WorldDerived { member, stage, world, relation, key } => {
            rtdb_types::OwnedRelation::new(relation.as_str())?;
            FamilyReleaseInput::WorldDerived {
                member: scope.occurrence(*member)?, stage: scope.stage(*stage)?,
                slot: WorldRowSlot { world: *world, relation, key },
            }
        }
        FamilyReleaseInputWire::RootDerived { member, relation, key } if member.position.line_generation == scope.root_line => {
            rtdb_types::OwnedRelation::new(relation.as_str())?;
            FamilyReleaseInput::RootDerived { member: scope.occurrence(*member)?, relation, key }
        }
        FamilyReleaseInputWire::RootLanding { member, binding } if member.position.line_generation == scope.root_line =>
            FamilyReleaseInput::RootLanding { member: scope.occurrence(*member)?, binding: *binding },
        FamilyReleaseInputWire::World { .. } | FamilyReleaseInputWire::RootDerived { .. }
        | FamilyReleaseInputWire::RootLanding { .. } =>
            return Err(OwnedWorld::unavailable()),
    })
}

pub(super) fn registration(wire: FamilyReleaseRegistrationWire) -> Result<FamilyReleaseRegistration> {
    FamilyReleaseRegistration::from_address(wire.origin, wire.attempt, part(wire.part)?, &wire.address)
}

pub(super) async fn serve(
    scope: &Scope, original: &OriginalAssertionFamilyReader, cancellation: &CancellationToken,
    families: AssertionFamilies<'_>, request: FamilyReleaseRequest,
) -> Result<FamilyReply> {
    let authority = CarrierJournal::open(original, cancellation, scope.deployment).await?;
    let families = families.with_release_authority(&authority);
    match request {
        FamilyReleaseRequest::Prepare { family, part: coordinates, cutoff, input: selected } => {
            let registration = families.prepare_family_release(family, input(scope, family, &selected)?, part(coordinates)?, cutoff).await?;
            let mut body = Vec::with_capacity(252);
            body.extend_from_slice(&registration.origin());
            body.extend_from_slice(&registration.attempt());
            body.extend_from_slice(&registration.address());
            return Ok(FamilyReply::ReleaseRegistration(body.into()));
        }
        FamilyReleaseRequest::Stage { registration: wire, input: selected } => {
            let registration = registration(wire)?;
            families.stage_family_release(registration, input(scope, registration.client_family(), &selected)?).await?;
        }
        FamilyReleaseRequest::Promote(wire) => families.promote_family_release(registration(wire)?).await?,
        FamilyReleaseRequest::Check(wire) => families.require_family_release(registration(wire)?).await?,
        FamilyReleaseRequest::Cancel(wire) => families.cancel_private_family_release(registration(wire)?).await?,
        FamilyReleaseRequest::Dispose(wire) => families.settle_family_release(registration(wire)?).await?,
    }
    Ok(FamilyReply::Done)
}
