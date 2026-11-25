//! Founding World-rank succession at the authenticated retired Original.

use std::sync::Arc;

use bytes::Bytes;
use rtdb_columnar::ArrowCell;
use rtdb_directory::{DeploymentRecord, Directory};
use rtdb_flight_sql::{FamilyReleaseInputWire, FamilyReleaseRequest, FamilyReply, RetiredWorldRankRequest};
use rtdb_node::{NativeSession, OriginalAssertionFamilyReader, RetiredOriginalFamilyRead};
use rtdb_query::HostedControlPoint;
use rtdb_storage::{IoRegion, sealing::{PlaintextReleaseCheck, PlaintextView}};
use rtdb_types::{OwnedWorld, PlacedAttemptScope, Result, TransactionKey, TxId};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    capability::CellSecret,
    family_custody::{WORLD_RANK_CUSTODY_GRANT_BYTES, WorldRankCustodyGrant, framed_digest, grant_id},
    keys::{
        AssertionFamilies, CarrierJournalIdentity, FamilyReleaseAuthority, FamilyReleaseInput, KeyService, WorldRowSlot,
        FamilyReleaseObservation, FamilyReleaseRegistration, WorldRankDestinationCustody, WorldRankFrame,
        WorldRankSourceCensus, WorldRankSourceDrain, WorldRankTransfer, WorldRankTransferAuthority,
        WorldRankTransferBinding, WorldRankTransferContributor, WorldRankTransferDestination,
        WorldRankTransferRead, WorldRankTransferSource, WorldRankTransferState,
    },
};

use super::{Scope, release};

const ENROLLMENT_BYTES: usize = WORLD_RANK_CUSTODY_GRANT_BYTES + 32;

struct OriginalRankJournal {
    original: OriginalAssertionFamilyReader,
    cancellation: CancellationToken,
    directory: Arc<dyn Directory>,
    source: WorldRankTransferSource,
    destination: WorldRankTransferDestination,
    grant: WorldRankCustodyGrant,
    grant_id: [u8; 32],
    enrollment: [u8; ENROLLMENT_BYTES],
    identity: CarrierJournalIdentity,
    creator: String,
}

fn ordered_binding(point: &HostedControlPoint) -> Result<(TransactionKey, &[u8])> {
    let unavailable = OwnedWorld::unavailable;
    let transaction = TxId(point.receipt_tx_id.ok_or_else(unavailable)?);
    let (row, system_time) = point.row.as_ref().ok_or_else(unavailable)?;
    if rtdb_query::owned_row_event_position(row.batch(), 0)?.transaction != transaction {
        return Err(unavailable());
    }
    let binding = ArrowCell::named(row.batch(), "binding", 0)
        .and_then(ArrowCell::binary).ok_or_else(unavailable)?;
    Ok((TransactionKey { tx_id: transaction, system_time: *system_time }, binding))
}

struct RankState {
    custody: WorldRankDestinationCustody,
    source_drain: Option<[u8; 32]>,
}

impl RankState {
    fn decode(body: &[u8], grant: &WorldRankCustodyGrant, id: &[u8; 32],
        destination: WorldRankTransferDestination,
    ) -> Result<Self> {
        let unavailable = OwnedWorld::unavailable;
        if !matches!(body.len(), 97 | 161 | 241) {
            return Err(unavailable());
        }
        let (proof, payload) = body.split_first_chunk::<64>().ok_or_else(unavailable)?;
        let hash = framed_digest(b"rtdb.owned/retired-world-rank-state/v1\0", &[id, payload]);
        grant.verify(true, &hash, proof)?;
        let mut frame = WorldRankFrame(payload);
        let phase = frame.take::<1>()?[0];
        let generation = frame.take()?;
        let state = frame.state(phase)?;
        let source_drain: Option<[u8; 32]> = if phase == 0 { None } else { Some(frame.take()?) };
        frame.finish()?;
        if generation == [0; 32] || source_drain == Some([0; 32]) || (phase != 0 && !state.terminal()) {
            return Err(unavailable());
        }
        Ok(Self {
            custody: WorldRankDestinationCustody { destination, binding: grant.binding, state, generation },
            source_drain,
        })
    }
}

impl OriginalRankJournal {
    async fn current(&self) -> Result<(RetiredOriginalFamilyRead, TransactionKey)> {
        let unavailable = OwnedWorld::unavailable;
        let current = self.original.current_retired(&self.source.original, &self.cancellation).await?;
        if current.handoff_capture() != self.grant.capture {
            return Err(unavailable());
        }
        let point = current.current().retired_world_rank_custody(self.grant.binding.address, false).await?;
        let (ordered, binding) = ordered_binding(&point)?;
        if binding != self.enrollment.as_slice() || !after(ordered, self.source.original.captured) {
            return Err(unavailable());
        }
        Ok((current, ordered))
    }

    async fn state(&self) -> Result<RankState> {
        let unavailable = OwnedWorld::unavailable;
        let (current, enrolled) = self.current().await?;
        let point = current.current().retired_world_rank_custody(self.grant.binding.address, true).await?;
        let (ordered, body) = ordered_binding(&point)?;
        if !after(ordered, enrolled) {
            return Err(unavailable());
        }
        let state = RankState::decode(body, &self.grant, &self.grant_id, self.destination)?;
        if !matches!(state.custody.state, WorldRankTransferState::Discarded { .. }) {
            let record = self.directory.deployment(&self.destination.deployment.to_string()).await?
                .ok_or_else(unavailable)?;
            if Uuid::parse_str(&record.id).ok() != Some(self.destination.installation)
                || record.incarnation != self.destination.custody_incarnation.get()
                || record.parent.is_some() || record.writer_pool().is_none()
            {
                return Err(unavailable());
            }
        }
        Ok(state)
    }

    fn families<'a>(&'a self, keys: &'a dyn KeyService) -> Result<AssertionFamilies<'a>> {
        Ok(keys.assertion_families(IoRegion::new()).ok_or_else(OwnedWorld::unavailable)?
            .with_original_reader(&self.original, self.source.original.deployment, &self.cancellation)
            .with_release_authority(self))
    }

    async fn release(
        &self, scope: &Scope, families: &AssertionFamilies<'_>, agreement: &WorldRankTransfer,
        request: FamilyReleaseRequest,
    ) -> Result<FamilyReply> {
        let registration = |wire| {
            let registration = release::registration(wire)?;
            self.require_registration(agreement, registration)?;
            Ok::<_, rtdb_types::Anomaly>(registration)
        };
        match request {
            FamilyReleaseRequest::Prepare { family, part, cutoff, input } => {
                let member = member(agreement, family)?;
                let selected = selection(member)?;
                require_wire_input(scope, member, &input)?;
                let r = families.prepare_family_release(family, selected, release::part(part)?, cutoff).await?;
                let mut body = Vec::with_capacity(252);
                body.extend_from_slice(&r.origin());
                body.extend_from_slice(&r.attempt());
                body.extend_from_slice(&r.address());
                return Ok(FamilyReply::ReleaseRegistration(body.into()));
            }
            FamilyReleaseRequest::Stage { registration: wire, input } => {
                let r = registration(wire)?;
                let member = member(agreement, r.client_family())?;
                require_wire_input(scope, member, &input)?;
                families.stage_family_release(r, selection(member)?).await?;
            }
            FamilyReleaseRequest::Promote(wire) => families.promote_family_release(registration(wire)?).await?,
            FamilyReleaseRequest::Check(wire) => families.require_family_release(registration(wire)?).await?,
            FamilyReleaseRequest::Cancel(wire) => families.cancel_private_family_release(registration(wire)?).await?,
            FamilyReleaseRequest::Dispose(wire) => families.settle_family_release(registration(wire)?).await?,
        }
        Ok(FamilyReply::Done)
    }

    fn require_registration(&self, agreement: &WorldRankTransfer, registration: FamilyReleaseRegistration) -> Result<()> {
        if registration.owner() != self.identity {
            return Err(OwnedWorld::unavailable());
        }
        let family = registration.client_family();
        registration.require_input(family, selection(member(agreement, family)?)?)
    }
}

#[async_trait::async_trait]
impl WorldRankTransferAuthority for OriginalRankJournal {
    async fn source_census(
        &self, source: WorldRankTransferSource, _: &CancellationToken,
    ) -> Result<WorldRankSourceCensus> {
        if source != self.source { return Err(OwnedWorld::unavailable()); }
        self.current().await?;
        Ok(WorldRankSourceCensus { source, binding: self.grant.binding, inventory: self.grant.inventory })
    }

    async fn destination_custody(
        &self, destination: WorldRankTransferDestination, _: &CancellationToken,
    ) -> Result<WorldRankDestinationCustody> {
        if destination != self.destination { return Err(OwnedWorld::unavailable()); }
        Ok(self.state().await?.custody)
    }

    async fn source_copy_drain(
        &self, source: WorldRankTransferSource, _: &CancellationToken,
    ) -> Result<WorldRankSourceDrain> {
        let unavailable = OwnedWorld::unavailable;
        if source != self.source { return Err(unavailable()); }
        let observed = self.state().await?;
        Ok(WorldRankSourceDrain {
            source, binding: self.grant.binding, destination: observed.custody.state,
            generation: observed.custody.generation,
            copy_drain: observed.source_drain.ok_or_else(unavailable)?,
        })
    }
}

#[async_trait::async_trait]
impl FamilyReleaseAuthority for OriginalRankJournal {
    fn identity(&self) -> CarrierJournalIdentity { self.identity }

    async fn observe(&self, registration: FamilyReleaseRegistration) -> Result<FamilyReleaseObservation> {
        let unavailable = OwnedWorld::unavailable;
        if registration.owner() != self.identity { return Err(unavailable()); }
        let (current, enrolled) = self.current().await?;
        let (phase, (row, system_time)) = release::observe_registry(current.current(), &self.creator, registration).await?;
        let tx_id = rtdb_query::owned_row_event_position(row.batch(), 0)?.transaction;
        if !after(TransactionKey { tx_id, system_time }, enrolled) {
            return Err(unavailable());
        }
        let proof = ArrowCell::named(row.batch(), "family_custody_proof", 0)
            .and_then(ArrowCell::binary).ok_or_else(unavailable)?;
        let tag = match phase { FamilyReleaseObservation::Live => 0, FamilyReleaseObservation::Disposed => 1 };
        let digest = framed_digest(b"rtdb.owned/retired-family-registry/v1\0", &[
            &self.grant_id, &[tag], &registration.origin(), &registration.attempt(),
            &registration.part().to_bytes(), &registration.address(),
        ]);
        self.grant.verify(true, &digest, proof)?;
        Ok(phase)
    }
}

struct RankReleaseDelivery {
    journal: Arc<OriginalRankJournal>,
    keys: Arc<dyn KeyService>,
    registration: FamilyReleaseRegistration,
}

#[async_trait::async_trait]
impl PlaintextReleaseCheck for RankReleaseDelivery {
    async fn require_current(&self) -> Result<()> {
        self.journal.families(self.keys.as_ref())?.require_family_release(self.registration).await
    }
}

fn after(key: TransactionKey, floor: TransactionKey) -> bool {
    key.tx_id > floor.tx_id && key.system_time > floor.system_time
}

fn member(agreement: &WorldRankTransfer, family: Uuid) -> Result<&WorldRankTransferContributor> {
    let index = agreement.contributors.binary_search_by_key(&family, |member| member.family)
        .map_err(|_| OwnedWorld::unavailable())?;
    Ok(&agreement.contributors[index])
}

fn selection(member: &WorldRankTransferContributor) -> Result<FamilyReleaseInput<'_>> {
    let unavailable = OwnedWorld::unavailable;
    Ok(FamilyReleaseInput::WorldDerived {
        member: member.occurrence,
        stage: member.rank.capability.ok_or_else(unavailable)?,
        slot: WorldRowSlot {
            world: member.rank.world.ok_or_else(unavailable)?,
            relation: &member.rank.relation,
            key: &member.rank.key,
        },
    })
}

fn require_wire_input(scope: &Scope, member: &WorldRankTransferContributor, wire: &FamilyReleaseInputWire) -> Result<()> {
    let unavailable = OwnedWorld::unavailable;
    let FamilyReleaseInputWire::WorldDerived { member: occurrence, stage, world, relation, key } = wire
        else { return Err(unavailable()); };
    if scope.occurrence(*occurrence)? != member.occurrence
        || Some(scope.stage(*stage)?) != member.rank.capability || Some(*world) != member.rank.world
        || *relation != member.rank.relation || *key != member.rank.key
    {
        return Err(unavailable());
    }
    Ok(())
}

pub(in crate::process::owned_worlds) async fn serve(
    keys: Arc<dyn KeyService>, directory: Arc<dyn Directory>, secret: &CellSecret, session: &NativeSession,
    record: &DeploymentRecord, request: RetiredWorldRankRequest,
) -> Result<FamilyReply> {
    let unavailable = OwnedWorld::unavailable;
    let deployment = Uuid::parse_str(&record.id).map_err(|_| unavailable())?;
    let history = PlacedAttemptScope::new(&record.stream("replica"), record.epoch)?;
    if session.placed_history()? != Some(history) { return Err(unavailable()); }
    let original = session.assertion_family_reader()?;
    let cancellation = session.state().cancellation().clone();
    let current = original.current(deployment, &cancellation).await?;
    let point = current.retired_world_rank_custody(request.address, false).await?;
    let (_, enrollment) = ordered_binding(&point)?;
    let enrollment: [u8; ENROLLMENT_BYTES] = enrollment.try_into().map_err(|_| unavailable())?;
    let grant = WorldRankCustodyGrant::authenticate(&enrollment, secret, record)?;
    if grant.binding.address != request.address { return Err(unavailable()); }
    let id = grant_id(&enrollment[..WORLD_RANK_CUSTODY_GRANT_BYTES]);
    let mut frame = WorldRankFrame(&request.signed_body);
    let operation = frame.take::<1>()?[0];
    let controller = match operation {
        0 | 1 | 4 => true,
        2 | 3 => false,
        _ => return Err(unavailable()),
    };
    let digest = framed_digest(b"rtdb.owned/retired-world-rank-action/v1\0", &[&request.address, &request.signed_body]);
    grant.verify(controller, &digest, &request.signature)?;

    let region = IoRegion::new();
    let families = keys.assertion_families(region).ok_or_else(unavailable)?
        .with_original_reader(&original, deployment, &cancellation);
    let agreement = Arc::new(if operation == 0 {
        WorldRankTransfer::from_bytes(frame.payload(1 << 20)?)?
    } else {
        if operation != 3 && frame.take::<32>()? != grant.binding.agreement {
            return Err(unavailable());
        }
        families.world_rank_transfer_agreement(grant.binding).await?
    });
    let source = agreement.source;
    if source.original.deployment != deployment || source.original.installation != deployment
        || source.custody_incarnation.get() != record.incarnation || source.original.captured != grant.captured
        || source.archive != grant.archive || agreement.destination.deployment != agreement.destination.installation
        || agreement.binding()? != grant.binding || agreement.inventory()? != grant.inventory
    {
        return Err(unavailable());
    }
    drop(families);
    let creator = format!("fenced-world-rank-custody/{}", release::lower_hex(&request.address));
    let identity = release::carrier_identity(deployment, source.original.history, *source.original.installation.as_bytes(), &creator);
    let journal = Arc::new(OriginalRankJournal {
        original, cancellation, directory, source, destination: agreement.destination,
        grant, grant_id: id, enrollment, identity, creator,
    });
    journal.current().await?;
    let families = journal.families(keys.as_ref())?;
    let scope = Scope::new(deployment, record)?;
    match operation {
        0 => {
            let key_service = keys.assertion_keys().ok_or_else(unavailable)?;
            let verify = IoRegion::new();
            for member in &agreement.contributors {
                let founder = frame.payload(1 << 20)?;
                let rank = frame.payload(1 << 20)?;
                member.authenticate_carriers(founder, rank, key_service, &verify).await?;
            }
            frame.finish()?;
            let prepared = families.prepare_world_rank_transfer(&agreement, journal.as_ref()).await?;
            let mut reply = Vec::with_capacity(64);
            reply.extend_from_slice(&prepared.address);
            reply.extend_from_slice(&prepared.agreement);
            Ok(FamilyReply::RankTransferPrepared(reply.into()))
        }
        1 => {
            frame.finish()?;
            families.record_world_rank_transfer_drain(&agreement, journal.as_ref()).await?;
            Ok(FamilyReply::Done)
        }
        2 => {
            let family = frame.uuid()?;
            let replay = frame.replay()?;
            let generation = frame.take()?;
            let tail = request.signed_body.slice(request.signed_body.len() - frame.0.len()..);
            let (registration, carrier) = RetiredWorldRankRequest::open(&tail).map_err(|_| unavailable())?;
            let registration = release::registration(registration)?;
            journal.require_registration(&agreement, registration)?;
            if registration.client_family() != family { return Err(unavailable()); }
            let selected = member(&agreement, family)?;
            let FamilyReleaseInput::WorldDerived { slot, .. } = selection(selected)?
                else { return Err(unavailable()); };
            families.require_family_release(registration).await?;
            let body = families.open_transferred_world_rank(
                Arc::clone(&agreement),
                WorldRankTransferRead { contributor: selected, carrier: &carrier, slot, replay, generation },
                carrier.clone(), Arc::clone(&keys), journal.clone(),
            ).await?;
            families.require_family_release(registration).await?;
            let check = framed_digest(b"rtdb.multi/retired-world-rank-part/v1\0", &[
                &journal.grant_id, &registration.attempt(), &registration.address(),
            ]);
            PlaintextView::track_release_check(check, Arc::new(RankReleaseDelivery {
                journal: Arc::clone(&journal), keys: Arc::clone(&keys), registration,
            }))?;
            Ok(FamilyReply::Opened(body))
        }
        3 => {
            let tail = request.signed_body.slice(1..);
            let request = RetiredWorldRankRequest::release(&tail).map_err(|_| unavailable())?;
            journal.release(&scope, &families, &agreement, request).await
        }
        4 => {
            frame.finish()?;
            families.revoke_world_rank_transfer_delivery(&agreement).await?;
            Ok(FamilyReply::Done)
        }
        _ => Err(unavailable()),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use ring::signature::{Ed25519KeyPair, KeyPair};
    use rtdb_directory::{Checkpoint, StorageNamespaces};
    use rtdb_types::{SourcePosition, StorageEpoch};

    use super::*;

    #[test]
    fn ordered_state_requires_the_controller_and_both_positive_drains() -> Result<()> {
        let secret = CellSecret::new(&[1; 32])?;
        let mut record = DeploymentRecord::new("original", "example", vec![], 0);
        record.epoch = 1;
        record.storage_namespaces = Some(StorageNamespaces { objects: Uuid::new_v4(), keys: [2; 32] });
        let history = PlacedAttemptScope::new(&record.stream("replica"), record.epoch)?.history_fingerprint();
        let capture = Uuid::from_bytes([3; 16]);
        let archive = [4; 32];
        let address = WorldRankTransferBinding::archive_address(history, archive);
        let checkpoint = Checkpoint { block: 1, boundary: 2, source: 2, system_time: 20, root_sha256: Some([5; 32]) };
        let captured = SourcePosition::from_epoch_offset(StorageEpoch(record.log_epoch), checkpoint.source)?;
        let controller = Ed25519KeyPair::from_seed_unchecked(&[6; 32]).unwrap();
        let reader = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
        let mut body = b"WRC1".to_vec();
        body.extend_from_slice(capture.as_bytes());
        body.extend_from_slice(&archive);
        body.extend_from_slice(&captured.0.to_be_bytes());
        body.extend_from_slice(&checkpoint.system_time.to_be_bytes());
        body.extend_from_slice(&address);
        body.extend_from_slice(&[8; 32]);
        body.extend_from_slice(&[9; 32]);
        body.extend_from_slice(reader.public_key().as_ref());
        body.extend_from_slice(controller.public_key().as_ref());
        let id = grant_id(&body);
        let proof = WorldRankCustodyGrant::endorse(&secret, &record, capture, archive, checkpoint, &body)?;
        body.extend_from_slice(&proof);
        let grant = WorldRankCustodyGrant::authenticate(&body, &secret, &record)?;

        let target = Uuid::from_bytes([10; 16]);
        let destination = WorldRankTransferDestination {
            deployment: target, installation: target, custody_incarnation: NonZeroU64::MIN,
            hold: [11; 32], preparation: [12; 32],
        };
        let encode = |payload: &[u8], signer: &Ed25519KeyPair| {
            let hash = framed_digest(b"rtdb.owned/retired-world-rank-state/v1\0", &[&id, payload]);
            [signer.sign(&hash).as_ref(), payload].concat()
        };
        let read = |payload: &[u8], signer: &Ed25519KeyPair| {
            RankState::decode(&encode(payload, signer), &grant, &id, destination)
        };
        let mut payload = vec![0];
        payload.extend_from_slice(&[13; 32]);
        assert_eq!(read(&payload, &controller)?.custody.state, WorldRankTransferState::Prepared);
        assert!(read(&payload, &reader).is_err());
        payload[0] = 1;
        payload.extend_from_slice(&3_i64.to_be_bytes());
        payload.extend_from_slice(&30_i64.to_be_bytes());
        for digest in [[14; 32], [15; 32], [16; 32], [17; 32]] {
            payload.extend_from_slice(&digest);
        }
        let state = read(&payload, &controller)?;
        assert!(matches!(state.custody.state, WorldRankTransferState::Installed { .. }));
        assert_eq!(state.source_drain, Some([17; 32]));
        let source_drain = payload.len() - 32;
        payload[source_drain..].fill(0);
        assert!(read(&payload, &controller).is_err());
        payload[source_drain..].fill(17);
        payload[source_drain - 32..source_drain].fill(0);
        assert!(read(&payload, &controller).is_err());
        assert!(read(&payload[..payload.len() - 1], &controller).is_err());
        Ok(())
    }
}
