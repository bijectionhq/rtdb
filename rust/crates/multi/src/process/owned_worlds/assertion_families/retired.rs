use rtdb_columnar::ArrowCell;
use rtdb_flight_sql::{FamilyReleaseInputWire, FamilyReleaseRegistrationWire, FamilyReleaseRequest, RetiredFamilyOperation, RetiredFamilyRequest};
use rtdb_node::{CurrentAssertionFamilyRead, OriginalAssertionFamilyReader, RetiredOriginalFamilyRead};
use rtdb_storage::sealing::{PlaintextReleaseCheck, PlaintextView};
use rtdb_types::{TransactionKey, TxId};
use tokio_util::sync::CancellationToken;

use crate::{
    capability::CellSecret,
    family_custody::{FAMILY_CUSTODY_GRANT_BYTES, FamilyCustodyGrant, framed_digest},
    keys::{AssertionFamilies, CarrierJournalIdentity, FamilyReleaseAuthority, FamilyReleaseInput, FamilyReleaseObservation, FamilyReleaseRegistration},
};

use super::*;

struct RetiredJournal {
    original: OriginalAssertionFamilyReader,
    cancellation: CancellationToken,
    grant: FamilyCustodyGrant,
    id: [u8; 32],
    enrollment: [u8; FAMILY_CUSTODY_GRANT_BYTES + 32],
    creator: String,
    identity: CarrierJournalIdentity,
}

fn enrollment_bytes(point: &rtdb_query::HostedControlPoint) -> Result<&[u8]> {
    let (row, _) = point.row.as_ref().ok_or_else(OwnedWorld::unavailable)?;
    ArrowCell::named(row.batch(), "binding", 0).and_then(ArrowCell::binary).ok_or_else(OwnedWorld::unavailable)
}

impl RetiredJournal {
    async fn open(
        original: OriginalAssertionFamilyReader, cancellation: CancellationToken, secret: &CellSecret,
        record: &DeploymentRecord, request: &RetiredFamilyRequest,
    ) -> Result<Self> {
        let unavailable = OwnedWorld::unavailable;
        let id = request.grant;
        let deployment = Uuid::parse_str(&record.id).map_err(|_| unavailable())?;
        let current = original.current(deployment, &cancellation).await?;
        let creator = format!("fenced-family-custody/{}", release::lower_hex(&id));
        let point = current.retired_family_custody_enrollment(&creator).await?;
        let enrollment: [u8; FAMILY_CUSTODY_GRANT_BYTES + 32] = enrollment_bytes(&point)?.try_into().map_err(|_| unavailable())?;
        let grant = FamilyCustodyGrant::authenticate(&enrollment, id, secret, record)?;
        let signed = framed_digest(
            b"rtdb.owned/retired-family-action/v1\0", &[&id, &request.signed_body],
        );
        if matches!(request.signed_body.first(), Some(3 | 4 | 5)) {
            grant.verify_controller(&signed, &request.signature)?;
        } else {
            grant.verify_reader(&signed, &request.signature)?;
        }
        let identity = release::carrier_identity(deployment, grant.source.history, *grant.source.installation.as_bytes(), &creator);
        Ok(Self { original, cancellation, grant, id, enrollment, creator, identity })
    }

    async fn current(&self) -> Result<(RetiredOriginalFamilyRead, TransactionKey)> {
        let current = self.original.current_retired(&self.grant.source, &self.cancellation).await?;
        let point = current.current().retired_family_custody_enrollment(&self.creator).await?;
        let replay = format!("fenced-family-replay/{}", release::lower_hex(&self.grant.replay));
        let replay = current.current().retired_family_custody_enrollment(&replay).await?;
        let scope_tx = point.receipt_tx_id.ok_or_else(OwnedWorld::unavailable)?;
        let (_, ordered) = point.row.as_ref().ok_or_else(OwnedWorld::unavailable)?;
        if scope_tx <= self.grant.source.through.tx_id.0 || *ordered <= self.grant.source.through.system_time
            || replay.receipt_tx_id != Some(scope_tx) || replay.row.as_ref().map(|(_, revision)| revision) != Some(ordered)
            || enrollment_bytes(&point)? != &self.enrollment || enrollment_bytes(&replay)? != &self.enrollment
        {
            return Err(OwnedWorld::unavailable());
        }
        Ok((current, TransactionKey { tx_id: TxId(scope_tx), system_time: *ordered }))
    }

    async fn require_live(&self, current: &CurrentAssertionFamilyRead) -> Result<()> {
        if current.assertion_family_fence(self.grant.family).await?.is_some() {
            return Err(rtdb_storage::Error::ErasedUnderView.into());
        }
        Ok(())
    }

    fn input(&self, scope: &Scope) -> Result<FamilyReleaseInput<'static>> {
        Ok(FamilyReleaseInput::RootLanding {
            member: scope.occurrence(self.grant.occurrence())?,
            binding: self.grant.root,
        })
    }

    fn require_wire_input(&self, wire: &FamilyReleaseInputWire) -> Result<()> {
        if !matches!(wire, FamilyReleaseInputWire::RootLanding { member, binding }
            if *member == self.grant.occurrence() && *binding == self.grant.root)
        {
            return Err(OwnedWorld::unavailable());
        }
        Ok(())
    }

    fn registration(&self, scope: &Scope, wire: FamilyReleaseRegistrationWire) -> Result<FamilyReleaseRegistration> {
        let registration = release::registration(wire)?;
        registration.require_input(self.grant.family, self.input(scope)?)?;
        if registration.owner() != self.identity {
            return Err(OwnedWorld::unavailable());
        }
        Ok(registration)
    }

    fn families<'a>(&'a self, keys: &'a dyn KeyService, region: Arc<IoRegion>) -> Result<AssertionFamilies<'a>> {
        Ok(keys.assertion_families(region).ok_or_else(OwnedWorld::unavailable)?
            .with_original_reader(&self.original, self.grant.source.deployment, &self.cancellation)
            .with_release_authority(self))
    }

    async fn release(&self, scope: &Scope, families: &AssertionFamilies<'_>, request: FamilyReleaseRequest) -> Result<FamilyReply> {
        match request {
            FamilyReleaseRequest::Prepare { family, part, cutoff, input } => {
                if family != self.grant.family { return Err(OwnedWorld::unavailable()); }
                self.require_wire_input(&input)?;
                let registration = families.prepare_family_release(family, self.input(scope)?, release::part(part)?, cutoff).await?;
                let mut body = Vec::with_capacity(252);
                body.extend_from_slice(&registration.origin());
                body.extend_from_slice(&registration.attempt());
                body.extend_from_slice(&registration.address());
                return Ok(FamilyReply::ReleaseRegistration(body.into()));
            }
            FamilyReleaseRequest::Stage { registration, input } => {
                self.require_wire_input(&input)?;
                families.stage_family_release(self.registration(scope, registration)?, self.input(scope)?).await?;
            }
            FamilyReleaseRequest::Promote(wire) => families.promote_family_release(self.registration(scope, wire)?).await?,
            FamilyReleaseRequest::Check(wire) => families.require_family_release(self.registration(scope, wire)?).await?,
            FamilyReleaseRequest::Cancel(wire) => families.cancel_private_family_release(self.registration(scope, wire)?).await?,
            FamilyReleaseRequest::Dispose(wire) => families.settle_family_release(self.registration(scope, wire)?).await?,
        }
        Ok(FamilyReply::Done)
    }
}

#[async_trait::async_trait]
impl FamilyReleaseAuthority for RetiredJournal {
    fn identity(&self) -> CarrierJournalIdentity { self.identity }

    async fn observe(&self, registration: FamilyReleaseRegistration) -> Result<FamilyReleaseObservation> {
        if registration.owner() != self.identity { return Err(OwnedWorld::unavailable()); }
        let (current, enrollment) = self.current().await?;
        let (phase, (row, revision)) = release::observe_registry(current.current(), &self.creator, registration).await?;
        let transaction = rtdb_query::owned_row_event_position(row.batch(), 0)?.transaction;
        if transaction <= enrollment.tx_id || revision <= enrollment.system_time {
            return Err(OwnedWorld::unavailable());
        }
        let proof = ArrowCell::named(row.batch(), "family_custody_proof", 0).and_then(ArrowCell::binary)
            .ok_or_else(OwnedWorld::unavailable)?;
        let tag = match phase { FamilyReleaseObservation::Live => 0, FamilyReleaseObservation::Disposed => 1 };
        let digest = framed_digest(b"rtdb.owned/retired-family-registry/v1\0", &[
            &self.id, &[tag], &registration.origin(), &registration.attempt(),
            &registration.part().to_bytes(), &registration.address(),
        ]);
        self.grant.verify_controller(&digest, proof)?;
        Ok(phase)
    }
}

struct HistoricalDisposition<'a> {
    journal: &'a RetiredJournal,
    registered: TransactionKey,
    registration: FamilyReleaseRegistration,
}

#[async_trait::async_trait]
impl FamilyReleaseAuthority for HistoricalDisposition<'_> {
    fn identity(&self) -> CarrierJournalIdentity { self.registration.owner() }

    async fn observe(&self, registration: FamilyReleaseRegistration) -> Result<FamilyReleaseObservation> {
        let unavailable = OwnedWorld::unavailable;
        if registration != self.registration { return Err(unavailable()); }
        let (current, _) = self.journal.current().await?;
        let recorded_creator = current.carrier_creator_commitment_at(
            &release::registry_locator(registration), self.registered, &registration.address(),
        ).await?;
        let (phase, (row, _)) = release::read_registry(current.current(), registration).await?;
        let source = &self.journal.grant.source;
        let owner = release::carrier_identity(
            source.deployment, source.history, *source.installation.as_bytes(), release::registry_creator(&row)?,
        );
        if owner != self.identity() || owner.original_writer != recorded_creator || phase != FamilyReleaseObservation::Disposed {
            return Err(unavailable());
        }
        Ok(phase)
    }
}

struct RetiredDelivery {
    journal: Arc<RetiredJournal>,
    keys: Arc<dyn KeyService>,
    registration: FamilyReleaseRegistration,
}

#[async_trait::async_trait]
impl PlaintextReleaseCheck for RetiredDelivery {
    async fn require_current(&self) -> Result<()> {
        self.journal.families(self.keys.as_ref(), IoRegion::new())?.require_family_release(self.registration).await
    }
}

pub(in crate::process::owned_worlds) async fn serve(
    keys: Arc<dyn KeyService>, secret: &CellSecret, session: &NativeSession,
    record: &DeploymentRecord, request: RetiredFamilyRequest,
) -> Result<FamilyReply> {
    let unavailable = OwnedWorld::unavailable;
    let journal = Arc::new(open_on_placement(secret, session, record, &request).await?);
    let operation = request.operation().map_err(|_| unavailable())?;
    let (current, _) = journal.current().await?;
    let scope = Scope::new(journal.grant.source.deployment, record)?;
    let stage = scope.stage(journal.grant.stage)?;
    let family = scope.family(journal.grant.family);
    let families = journal.families(keys.as_ref(), IoRegion::new())?;
    match operation {
        RetiredFamilyOperation::Probe => {
            journal.require_live(current.current()).await?;
            families.require_root_landing_activation(family, stage, journal.grant.root).await?;
            Ok(FamilyReply::Done)
        }
        RetiredFamilyOperation::Release(request) => journal.release(&scope, &families, request).await,
        RetiredFamilyOperation::PlanFence(order) => {
            let fence = families.plan_fence(family, journal.grant.root.position, order).await?;
            Ok(fence_reply(fence))
        }
        RetiredFamilyOperation::Open { registration, envelope } => {
            let registration = journal.registration(&scope, registration)?;
            families.require_family_release(registration).await?;
            let vault = keys.assertion_keys().ok_or_else(unavailable)?;
            let opened = families.open_root_landing_payload(family, stage, journal.grant.root, envelope, vault).await?;
            families.require_family_release(registration).await?;
            let key = framed_digest(b"rtdb.multi/retired-family-delivery/v1\0", &[
                &journal.id, &registration.attempt(), &registration.address(),
            ]);
            PlaintextView::track_release_check(key, Arc::new(RetiredDelivery {
                journal: Arc::clone(&journal), keys: Arc::clone(&keys), registration,
            }))?;
            Ok(FamilyReply::Opened(opened))
        }
        RetiredFamilyOperation::RecoverPreFence { registered, registration } => {
            let registration = release::registration(registration)?;
            registration.require_input(journal.grant.family, journal.input(&scope)?)?;
            let authority = HistoricalDisposition { journal: &journal, registered, registration };
            authority.observe(registration).await?;
            families.with_release_authority(&authority).settle_family_release(registration).await?;
            Ok(FamilyReply::Done)
        }
    }
}

async fn open_on_placement(
    secret: &CellSecret, session: &NativeSession, record: &DeploymentRecord, request: &RetiredFamilyRequest,
) -> Result<RetiredJournal> {
    let expected = PlacedAttemptScope::new(&record.stream("replica"), record.epoch)?;
    if session.placed_history()? != Some(expected) { return Err(OwnedWorld::unavailable()); }
    RetiredJournal::open(session.assertion_family_reader()?, session.state().cancellation().clone(), secret, record, request).await
}

pub(in crate::process::owned_worlds) async fn authorize_fence(
    secret: &CellSecret, session: &NativeSession, record: &DeploymentRecord, request: &RetiredFamilyRequest,
) -> Result<Bytes> {
    if request.signed_body.first() != Some(&3) { return Err(OwnedWorld::unavailable()); }
    let journal = open_on_placement(secret, session, record, request).await?;
    journal.current().await?;
    Ok(Bytes::copy_from_slice(&journal.enrollment))
}
