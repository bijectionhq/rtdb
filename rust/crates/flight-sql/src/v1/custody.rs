use std::{collections::HashMap, fmt, sync::Arc};

use rtdb_crypt::tokens::CustodyReadProof;
use rtdb_node::{
    Database, DatabaseAccess, SystemClock,
    serving::ServingDriver,
    serving::custody_release::{
        CustodyCarrierAuthority, CustodyContributors, CustodyProviderInstallation,
        CustodyProviderMount, CustodyReleasePhase, CustodyReleaseRegistration,
        CustodyResponseAdmission, CustodyResponseOwner, CustodyResponsePart,
    },
};
use rtdb_tx::transact::{CustodyGroupId, CustodyOccurrence, Op, Outcome, Transact};
use rtdb_types::{Timestamp, rtdb_v1::Basis};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tonic::Status;
use uuid::Uuid;

use super::{error, proto};
use crate::service::{Call, Service};

pub(crate) const RELEASE: &str = "rtdb.custody.v1.Release";
const MAX_RELEASE_BYTES: usize = 2 * 1024 * 1024;
const MAX_READ_PROOFS: usize = 8;
const MAX_SCOPED_READS: usize = 1024;
const MAX_SCOPED_READ_MICROS: i64 = 120_000_000;
pub(super) const SCOPED_CUSTODY: &str = "row-visibility.custody-release";

#[derive(Default)]
pub(super) struct ScopedReads(Mutex<HashMap<Uuid, ScopedRead>>);

#[derive(Clone)]
struct ScopedRead {
    owner: [u8; 32],
    scope: [u8; 32],
    projected: Basis,
    source: Basis,
    generation: Option<u64>,
    expires_at: Timestamp,
    contributors: CustodyContributors,
}

impl ScopedReads {
    async fn register(&self, now: Timestamp, read: ScopedRead) -> Result<Vec<u8>, Status> {
        let mut reads = self.0.lock().await;
        reads.retain(|_, read| read.expires_at > now);
        if reads.len() >= MAX_SCOPED_READS {
            return Err(Status::resource_exhausted("The scoped custody read limit is reached"));
        }
        let handle = Uuid::new_v4();
        reads.insert(handle, read);
        Ok(handle.as_bytes().to_vec())
    }

    async fn resolve(&self, now: Timestamp, owner: [u8; 32], proofs: &[Vec<u8>]) -> Result<ScopedRead, Status> {
        if !(1..=MAX_READ_PROOFS).contains(&proofs.len()) {
            return Err(unavailable());
        }
        let reads = self.0.lock().await;
        let get = |proof: &[u8]| {
            Uuid::from_slice(proof).ok().and_then(|handle| reads.get(&handle))
                .filter(|read| read.owner == owner && read.expires_at > now)
                .ok_or_else(unavailable)
        };
        let mut selected = get(&proofs[0])?.clone();
        for proof in &proofs[1..] {
            let read = get(proof)?;
            if (read.scope, read.projected, read.generation) != (selected.scope, selected.projected, selected.generation) {
                return Err(unavailable());
            }
            selected.source = selected.source.max(read.source);
            selected.expires_at = selected.expires_at.min(read.expires_at);
            selected.contributors.extend(&read.contributors);
            if selected.contributors.len() > 512 { return Err(unavailable()); }
        }
        Ok(selected)
    }
}

pub struct CustodyProviderRoute {
    installation: CustodyProviderInstallation,
    owner: Option<String>,
    mount: Mutex<Option<CustodyProviderMount>>,
}
impl fmt::Debug for CustodyProviderRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustodyProviderRoute")
            .finish_non_exhaustive()
    }
}
impl CustodyProviderRoute {
    pub fn new(installation: CustodyProviderInstallation) -> Self {
        Self {
            installation,
            owner: None,
            mount: Mutex::new(None),
        }
    }

    pub fn with_owner(mut self, owner: String) -> Self {
        self.owner = Some(owner);
        self
    }

    pub(crate) fn owner(&self) -> Option<&str> {
        self.owner.as_deref().filter(|owner| !owner.is_empty())
    }

    async fn acquire(
        &self,
        service: &Service,
        call: &Call,
        database: &Arc<Database>,
    ) -> Result<CustodyResponseOwner, Status> {
        let mut active = call
            .cancellation
            .run_until_cancelled(self.mount.lock())
            .await
            .ok_or_else(unavailable)?;
        service.check_attachment(call, database)?;
        let catalog = service.0.node.catalog();
        if active.as_ref().is_none_or(|mount| !mount.is_for(database)) {
            active.take();
            let mount = catalog
                .mount_original_custody_provider(
                    &call.database,
                    self.installation.clone(),
                    &call.cancellation,
                )
                .await
                .map_err(error::anomaly)?;
            service.check_attachment(call, database)?;
            if !mount.is_for(database) {
                return Err(unavailable());
            }
            *active = Some(mount);
        }
        let owner = catalog
            .original_custody_response_owner(&call.database)
            .map_err(error::anomaly)?;
        service.check_attachment(call, database)?;
        if !owner.is_for(database, self.installation.authority) {
            return Err(unavailable());
        }
        Ok(owner)
    }

    fn proof_owner(&self, owner: &CustodyResponseOwner) -> [u8; 32] {
        let authority = self.installation.authority;
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/custody-read-owner\0");
        hash.update(authority.incarnation);
        hash.update(authority.source_history);
        hash.update(authority.installation);
        hash.update(authority.registry);
        hash.update(authority.original_writer);
        hash.update(owner.mount_id().as_bytes());
        hash.finalize().into()
    }
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Prepare {
        registration: CustodyReleaseRegistration,
    },
    Stage {
        registration: CustodyReleaseRegistration,
    },
    Promote {
        registration: CustodyReleaseRegistration,
    },
    Check(ResponseRequest),
    Admit(ResponseRequest),
    ResolveScoped { read_proofs: Vec<Vec<u8>> },
    CheckScoped(ScopedResponseRequest),
    AdmitScoped(ScopedResponseRequest),
    Cancel {
        registration: CustodyReleaseRegistration,
    },
    Dispose {
        registration: CustodyReleaseRegistration,
    },
    Drained {
        authority: CustodyCarrierAuthority,
        group: [u8; 16],
        revoked_at: Vec<u8>,
        inventory_sha256: [u8; 32],
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseRequest {
    registrations: Vec<CustodyReleaseRegistration>,
    frame_sha256: [u8; 32],
    read_proofs: Vec<Vec<u8>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopedResponseRequest {
    part: CustodyResponsePart,
    frame_sha256: [u8; 32],
    read_proofs: Vec<Vec<u8>>,
}

#[derive(Serialize)]
struct OriginalContributor {
    group: [u8; 16],
    original: [u8; 32],
    occurrence: [u8; 32],
    receipt_key: Vec<u8>,
    written_basis: Vec<u8>,
    system_time_micros: i64,
}

#[derive(Serialize)]
#[serde(untagged)]
enum Reply {
    Prepared {
        registration: CustodyReleaseRegistration,
    },
    Ack {
        ok: bool,
    },
    Admission {
        state: &'static str,
        basis: Vec<u8>,
    },
    ScopedAdmission { state: &'static str },
    ResolvedScoped { contributors: Vec<OriginalContributor> },
    Cancelled {
        state: &'static str,
    },
    Disposed {
        state: &'static str,
        evidence_sha256: [u8; 32],
    },
    Drain {
        drained: bool,
    },
}

fn terminal(phase: CustodyReleasePhase) -> Result<Reply, Status> {
    match phase {
        CustodyReleasePhase::Cancelled => Ok(Reply::Cancelled { state: "cancelled" }),
        CustodyReleasePhase::Disposed { evidence_sha256 } => Ok(Reply::Disposed {
            state: "disposed",
            evidence_sha256,
        }),
        _ => Err(Status::internal("Invalid custody release terminal")),
    }
}
fn unavailable() -> Status {
    error::unavailable("The original custody provider is unavailable")
}
fn session_binding(call: &Call) -> [u8; 32] {
    Sha256::digest(call.session.id.as_bytes()).into()
}

fn read_proofs_commitment(proofs: &[Vec<u8>]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"rtdb/v1/custody-read-proofs\0");
    hash.update((proofs.len() as u16).to_be_bytes());
    for proof in proofs {
        hash.update((proof.len() as u64).to_be_bytes());
        hash.update(proof);
    }
    hash.finalize().into()
}

impl Service {
    pub(super) fn custody_route(&self, call: &Call) -> Result<&CustodyProviderRoute, Status> {
        let config = &self.0.config;
        let route = config.custody_providers.get(&call.database).map(Arc::as_ref);
        let owner = route.and_then(CustodyProviderRoute::owner)
            .or_else(|| config.hosted_controls.as_ref().map(|policy| policy.owner.as_str()))
            .or(self.standalone_custody_owner());
        let caller = &call.session.caller;
        if owner.is_none()
            || caller.principal.user_id() != owner
            || !caller.verified_credential
            || caller.principal.label_scope().is_some()
            || call.session.audience_source().is_some()
            || call.session.fresh_replica
            || call
                .session
                .placed_database()
                .is_some_and(|original| original != call.database.as_str())
        {
            return Err(Status::permission_denied(
                "Response release requires the configured original custody owner",
            ));
        }
        caller
            .principal
            .authorize_database(&call.database, DatabaseAccess::Write)
            .map_err(error::anomaly)?;
        route.ok_or_else(unavailable)
    }

    pub(super) async fn custody_read_proof(
        &self,
        call: &Call,
        database: &Arc<Database>,
        issuer: &rtdb_crypt::tokens::TokenIssuer,
        basis: Basis,
        generation: Option<u64>,
        contributors: CustodyContributors,
        review: Option<(&str, &proto::CustodyReviewSelection)>,
    ) -> Result<(Vec<u8>, proto::CustodyReadInventory), Status> {
        if !contributors.is_empty() && generation.is_none() {
            return Err(unavailable());
        }
        let route = self.custody_route(call)?;
        let owner = route.acquire(self, call, database).await?;
        let release = owner
            .response_release(&SystemClock, &call.cancellation)
            .map_err(error::anomaly)?;
        release
            .require_original_writer()
            .await
            .map_err(error::anomaly)?;
        let expires_at = self.0.node.now().0.checked_add(120_000_000)
            .map(Timestamp).ok_or_else(unavailable)?;
        let mut inventory = proto::CustodyReadInventory::default();
        let mut reviewed_source_sha256 = None;
        if !contributors.is_empty() || review.is_some() {
            let driver = database.serving_driver().ok_or_else(unavailable)?;
            let index = driver.current_custody(&call.cancellation).await.map_err(error::anomaly)?;
            inventory.contributors.reserve(contributors.len());
            for &(group, commitment) in &contributors {
                let (original, occurrence) = index.custody_read_occurrence(basis, group, commitment).map_err(error::anomaly)?;
                let written_at = index.exact(occurrence.written).map_err(error::anomaly)?.system_time;
                inventory.contributors.push(confirmed_occurrence(call, driver, group, original, occurrence, written_at).await?);
            }
            if let Some((branch, selection)) = review {
                let group = selection.group.as_slice().try_into().map_err(|_| unavailable())?;
                let selected: [u8; 32] = selection.selected_occurrence_sha256.as_slice().try_into().map_err(|_| unavailable())?;
                if !contributors.contains(&(group, selected)) { return Err(unavailable()); }
                let (review, original, _) = index.custody_review_at(
                    branch, basis, Timestamp(selection.valid_at_micros), group, selected,
                ).map_err(error::anomaly)?;
                let original_commitment = original.commitment(group);
                let selected_evidence = inventory.contributors.iter().find(|entry| {
                    entry.group.as_slice() == group.as_slice() && entry.occurrence.as_slice() == selected.as_slice()
                }).ok_or_else(unavailable)?.clone();
                let original = if original_commitment == selected {
                    selected_evidence.clone()
                } else {
                    let written_at = index.exact(original.written).map_err(error::anomaly)?.system_time;
                    confirmed_occurrence(call, driver, group, original_commitment, original, written_at).await?
                };
                reviewed_source_sha256 = Some(review.commitment(group, original_commitment, selected));
                inventory.reviewed_source = Some(proto::CustodyReviewedSource {
                    cut: Some(proto::CustodyReviewCut {
                        branch: review.branch, basis: Some(review.basis.into()), valid_at_micros: review.valid_at.0,
                        inventory_sha256: review.inventory_sha256.to_vec(),
                    }),
                    original: Some(original), selected: Some(selected_evidence), proof_expires_at_micros: expires_at.0,
                });
            }
        }
        let proof = CustodyReadProof {
            basis,
            key_generation: generation,
            expires_at,
            contributors: contributors.into_iter().collect(),
            reviewed_source_sha256,
        };
        let sealed = issuer
            .seal_custody_read(&proof, route.proof_owner(&owner), session_binding(call))
            .await
            .map_err(error::anomaly)?;
        Ok((sealed, inventory))
    }

    pub(super) async fn reviewed_custody_writer(
        &self, call: &Call, database: &Arc<Database>,
    ) -> Result<(&CustodyProviderRoute, CustodyResponseOwner, rtdb_tx::transact::CustodyWriterFence), Status> {
        let route = self.custody_route(call)?;
        let owner = route.acquire(self, call, database).await?;
        let writer = owner.response_release(&SystemClock, &call.cancellation).map_err(error::anomaly)?
            .original_writer_fence().await.map_err(error::anomaly)?;
        Ok((route, owner, writer))
    }

    pub(super) async fn authenticate_reviewed_custody(
        &self, call: &Call, database: &Arc<Database>, request: &Transact,
    ) -> Result<(), Status> {
        let (route, owner, writer) = self.reviewed_custody_writer(call, database).await?;
        if !request.requires.first().is_some_and(|guard| writer.authenticates(guard)) {
            return Err(unavailable());
        }
        for op in &request.ops {
            let Op::ReserveReviewedCustodyPlacement { group, reservation, source, read_proof } = op else { continue; };
            let cutoff = request.window.before.ok_or_else(|| error::incorrect("A reviewed reservation needs the read proof cutoff"))?;
            let driver = database.serving_driver().ok_or_else(unavailable)?;
            let tokens = database.token_authority().await.map_err(error::anomaly)?;
            let proof = tokens.open_custody_read(read_proof, route.proof_owner(&owner), session_binding(call)).await.map_err(error::anomaly)?;
            if proof.basis != source.basis || proof.expires_at < cutoff || cutoff <= self.0.node.now()
                || proof.reviewed_source_sha256 != Some(source.commitment(*group, reservation.original, reservation.source))
                || !proof.contributors.contains(&(*group, reservation.source))
            {
                return Err(unavailable());
            }
            let groups = proof.contributors.iter().map(|(group, _)| *group).collect();
            let generation = proof.key_generation.ok_or_else(unavailable)?;
            driver.check_custody_release_delivery(&groups, &writer, generation, &call.cancellation).await.map_err(error::anomaly)?;
        }
        self.check_attachment(call, database)
    }

    pub(super) fn authorize_scoped_custody(&self, call: &Call) -> Result<&CustodyProviderRoute, Status> {
        let caller = &call.session.caller;
        if !caller.verified_credential
            || !caller.principal.label_scope().is_some_and(|scope| scope.permits_protected_reads())
        {
            return Err(Status::permission_denied("The scope does not authorize protected reads"));
        }
        self.0.config.custody_providers.get(&call.database).map(Arc::as_ref).ok_or_else(unavailable)
    }

    pub(super) async fn scoped_custody_read(
        &self, call: &Call, database: &Arc<Database>, projected: Basis, source: Basis,
        generation: Option<u64>, contributors: CustodyContributors,
    ) -> Result<Vec<u8>, Status> {
        let route = self.authorize_scoped_custody(call)?;
        let scope = call.session.caller.principal.label_scope().ok_or_else(unavailable)?;
        let owner = route.acquire(self, call, database).await.map_err(|_| unavailable())?;
        let release = owner.response_release(&SystemClock, &call.cancellation).map_err(|_| unavailable())?;
        release.require_original_writer().await.map_err(|_| unavailable())?;
        if source.incarnation() != route.installation.authority.incarnation
            || projected.incarnation() != scope.labels().main_read_incarnation(source.incarnation())
            || !contributors.is_empty() && generation.is_none()
        {
            return Err(unavailable());
        }
        let now = self.0.node.now();
        let expires_at = now.0.checked_add(MAX_SCOPED_READ_MICROS).map(Timestamp).ok_or_else(unavailable)?;
        let read = ScopedRead {
            owner: route.proof_owner(&owner), scope: scope.receipt_namespace(), projected, source,
            generation, expires_at, contributors,
        };
        if !read.contributors.is_empty() {
            self.resolve_scoped_contributors(call, database, &read).await.map_err(|_| unavailable())?;
        }
        self.0.v1.scoped_reads.register(now, read).await
    }

    async fn resolve_scoped_contributors(
        &self, call: &Call, database: &Database, read: &ScopedRead,
    ) -> Result<Vec<OriginalContributor>, Status> {
        let driver = database.serving_driver().ok_or_else(unavailable)?;
        let index = driver.current_custody(&call.cancellation).await.map_err(error::anomaly)?;
        let mut resolved = Vec::with_capacity(read.contributors.len());
        for &(group, commitment) in &read.contributors {
            let (original, occurrence) = index.custody_read_occurrence(read.source, group, commitment)
                .map_err(error::anomaly)?;
            let written = index.exact(occurrence.written).map_err(error::anomaly)?;
            let receipt = driver.original_custody_receipt(&occurrence.receipt_key, occurrence.written, &call.cancellation)
                .await.map_err(error::anomaly)?;
            if receipt.key != occurrence.receipt_key || receipt.basis != written.basis
                || receipt.system_time != written.system_time || matches!(receipt.outcome, Outcome::Refused { .. })
            {
                return Err(unavailable());
            }
            resolved.push(OriginalContributor {
                group, original, occurrence: commitment, receipt_key: receipt.key,
                written_basis: receipt.basis.to_bytes().to_vec(), system_time_micros: receipt.system_time.0,
            });
        }
        Ok(resolved)
    }

    pub(super) async fn custody_release_action(
        &self,
        call: &Call,
        body: &[u8],
    ) -> Result<Vec<u8>, Status> {
        let route = self.custody_route(call)?;
        if body.len() > MAX_RELEASE_BYTES {
            return Err(Status::resource_exhausted(
                "Custody release request exceeds its bound",
            ));
        }
        let _charge = call.reserve(body.len() * 3 + 4096)?;
        let request = serde_json::from_slice(body)
            .map_err(|_| error::incorrect("Invalid custody release request"))?;
        let database = self.wire_attachment(call, DatabaseAccess::Write).await?;
        let owner = route.acquire(self, call, &database).await?;
        let mounted = owner
            .response_release(&SystemClock, &call.cancellation)
            .map_err(error::anomaly)?;
        let release = mounted.release();
        let admit = matches!(&request, Request::Admit(_) | Request::AdmitScoped(_));
        let reply = match request {
            Request::Prepare { registration } => Reply::Prepared {
                registration: release
                    .prepare(registration)
                    .await
                    .map_err(error::anomaly)?,
            },
            Request::Stage { registration } => {
                release.stage(registration).await.map_err(error::anomaly)?;
                Reply::Ack { ok: true }
            }
            Request::Promote { registration } => {
                release
                    .promote(registration)
                    .await
                    .map_err(error::anomaly)?;
                Reply::Ack { ok: true }
            }
            Request::Check(ResponseRequest {
                registrations,
                frame_sha256,
                read_proofs,
            })
            | Request::Admit(ResponseRequest {
                registrations,
                frame_sha256,
                read_proofs,
            }) => {
                let first = registrations.first().ok_or_else(unavailable)?;
                if registrations.len() > 512
                    || first.authority != route.installation.authority
                    || frame_sha256 == [0; 32]
                {
                    return Err(unavailable());
                }
                let (contributors, selected_basis, key_generation) = self
                    .verify_custody_reads(
                        call,
                        &database,
                        route.proof_owner(&owner),
                        &read_proofs,
                        first.expires_at,
                    )
                    .await?;
                let admission = CustodyResponseAdmission {
                    registrations,
                    frame_sha256,
                    selected_basis,
                    key_generation: key_generation.ok_or_else(unavailable)?,
                    read_proofs_sha256: Some(read_proofs_commitment(&read_proofs)),
                };
                if admit {
                    let receipt = mounted
                        .admit_registered_response(&contributors, admission)
                        .await
                        .map_err(error::anomaly)?;
                    Reply::Admission {
                        state: if receipt.replayed {
                            "recorded"
                        } else {
                            "admitted"
                        },
                        basis: receipt.basis.to_bytes().to_vec(),
                    }
                } else {
                    mounted
                        .check_registered_response(&contributors, &admission)
                        .await
                        .map_err(error::anomaly)?;
                    Reply::Ack { ok: true }
                }
            }
            Request::ResolveScoped { read_proofs } => {
                mounted.require_original_writer().await.map_err(error::anomaly)?;
                let read = self.0.v1.scoped_reads.resolve(self.0.node.now(), route.proof_owner(&owner), &read_proofs).await?;
                Reply::ResolvedScoped { contributors: self.resolve_scoped_contributors(call, &database, &read).await? }
            }
            Request::CheckScoped(ScopedResponseRequest { part, frame_sha256, read_proofs })
            | Request::AdmitScoped(ScopedResponseRequest { part, frame_sha256, read_proofs }) => {
                if frame_sha256 == [0; 32] { return Err(unavailable()); }
                let read = self.0.v1.scoped_reads.resolve(self.0.node.now(), route.proof_owner(&owner), &read_proofs).await?;
                let proof_commitment = read_proofs_commitment(&read_proofs);
                if read.contributors.is_empty() {
                    mounted.check_clear_response(part, frame_sha256, proof_commitment, read.expires_at)
                        .await.map_err(error::anomaly)?;
                    Reply::ScopedAdmission { state: "clear" }
                } else {
                    let index = release.driver.current_custody(&call.cancellation).await.map_err(error::anomaly)?;
                    let registrations = index.promoted_custody_registrations(route.installation.authority, part, &read.contributors)
                        .map_err(error::anomaly)?;
                    if registrations.iter().any(|registration| registration.expires_at > read.expires_at) {
                        return Err(unavailable());
                    }
                    let admission = CustodyResponseAdmission {
                        registrations, frame_sha256, selected_basis: read.source,
                        key_generation: read.generation.ok_or_else(unavailable)?,
                        read_proofs_sha256: Some(proof_commitment),
                    };
                    if admit {
                        let receipt = mounted.admit_registered_response(&read.contributors, admission)
                            .await.map_err(error::anomaly)?;
                        Reply::ScopedAdmission { state: if receipt.replayed { "recorded" } else { "admitted" } }
                    } else {
                        mounted.check_registered_response(&read.contributors, &admission).await.map_err(error::anomaly)?;
                        Reply::Ack { ok: true }
                    }
                }
            }
            Request::Cancel { registration } => {
                terminal(release.cancel(registration).await.map_err(error::anomaly)?)?
            }
            Request::Dispose { registration } => terminal(
                release
                    .dispose(registration)
                    .await
                    .map_err(error::anomaly)?,
            )?,
            Request::Drained {
                authority,
                group,
                revoked_at,
                inventory_sha256,
            } => Reply::Drain {
                drained: release
                    .drained(
                        authority,
                        group,
                        Basis::from_bytes(&revoked_at).map_err(error::anomaly)?,
                        inventory_sha256,
                    )
                    .await
                    .map_err(error::anomaly)?,
            },
        };
        self.check_attachment(call, &database)?;
        serde_json::to_vec(&reply)
            .map_err(|_| Status::internal("Cannot encode the custody release response"))
    }

    async fn verify_custody_reads(
        &self,
        call: &Call,
        database: &Database,
        owner: [u8; 32],
        proofs: &[Vec<u8>],
        cutoff: Timestamp,
    ) -> Result<(CustodyContributors, Basis, Option<u64>), Status> {
        if !(1..=MAX_READ_PROOFS).contains(&proofs.len()) {
            return Err(unavailable());
        }
        let tokens = database.token_authority().await.map_err(error::anomaly)?;
        let session = session_binding(call);
        let mut selected = None;
        let mut contributors = CustodyContributors::new();
        for proof in proofs {
            let read = tokens
                .open_custody_read(proof, owner, session)
                .await
                .map_err(error::anomaly)?;
            let cut = (read.basis, read.key_generation);
            if selected.is_some_and(|prior| prior != cut)
                || read.expires_at < cutoff
                || read.expires_at <= self.0.node.now()
            {
                return Err(unavailable());
            }
            selected = Some(cut);
            for contributor in read.contributors {
                if contributors.insert(contributor) && contributors.len() > 512 {
                    return Err(unavailable());
                }
            }
        }
        let (basis, generation) = selected.ok_or_else(unavailable)?;
        Ok((contributors, basis, generation))
    }
}

async fn confirmed_occurrence(
    call: &Call, driver: &dyn ServingDriver, group: CustodyGroupId,
    original: [u8; 32], occurrence: &CustodyOccurrence, written_at: Timestamp,
) -> Result<proto::CustodyReadContributor, Status> {
    let receipt = driver.original_custody_receipt(&occurrence.receipt_key, occurrence.written, &call.cancellation)
        .await.map_err(error::anomaly)?;
    if receipt.key != occurrence.receipt_key || receipt.basis != occurrence.written || receipt.system_time != written_at
        || matches!(receipt.outcome, Outcome::Refused { .. })
    {
        return Err(unavailable());
    }
    let target = &occurrence.target;
    Ok(proto::CustodyReadContributor {
        group: group.to_vec(), original: original.to_vec(), occurrence: occurrence.commitment(group).to_vec(),
        receipt_key: receipt.key, written_basis: Some(receipt.basis.into()), system_time_micros: receipt.system_time.0,
        table: format!("{}.{}", target.table.schema, target.table.table), row_id: target.id.handle_bytes(),
        valid_from_micros: target.valid.lower().0, valid_to_micros: target.valid.upper().0,
        carrier_sha256: occurrence.carrier_sha256.to_vec(), branch: target.branch.clone(), ordinal: occurrence.ordinal,
        placement_id: occurrence.placement_id.to_vec(), reservation_sha256: occurrence.reservation.map(|digest| digest.to_vec()),
        link_sha256: occurrence.link_sha256.map(|digest| digest.to_vec()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scoped_proof_is_opaque_and_binds_mount_scope_and_expiry() {
        let reads = ScopedReads::default();
        let read = ScopedRead {
            owner: [1; 32], scope: [2; 32], projected: Basis::new([3; 16], 1),
            source: Basis::new([4; 16], 900), generation: Some(700), expires_at: Timestamp(50),
            contributors: CustodyContributors::new(),
        };
        let first = reads.register(Timestamp(10), read.clone()).await.unwrap();
        let other_scope = reads.register(Timestamp(10), ScopedRead { scope: [5; 32], ..read }).await.unwrap();
        assert_eq!(first.len(), 16);
        assert_ne!(first, other_scope);
        let resolved = reads.resolve(Timestamp(49), [1; 32], std::slice::from_ref(&first)).await.unwrap();
        assert!(resolved.contributors.is_empty());
        assert!(reads.resolve(Timestamp(49), [8; 32], std::slice::from_ref(&first)).await.is_err());
        assert!(reads.resolve(Timestamp(50), [1; 32], std::slice::from_ref(&first)).await.is_err());
        assert!(reads.resolve(Timestamp(49), [1; 32], &[first, other_scope]).await.is_err());
    }

    #[test]
    fn custody_admit_has_a_distinct_operation_and_reply() {
        let response = serde_json::json!({
            "operation": "admit", "registrations": [], "frame_sha256": [1; 32].to_vec(), "read_proofs": []
        });
        assert!(matches!(
            serde_json::from_value::<Request>(response.clone()).unwrap(),
            Request::Admit(_)
        ));
        let mut check = response;
        check["operation"] = "check".into();
        assert!(matches!(
            serde_json::from_value::<Request>(check.clone()).unwrap(),
            Request::Check(_)
        ));
        check["basis"] = serde_json::json!([]);
        assert!(serde_json::from_value::<Request>(check).is_err());
        let recorded = serde_json::to_value(Reply::Admission {
            state: "recorded",
            basis: vec![2; Basis::WIDTH],
        })
        .unwrap();
        assert_eq!(
            recorded,
            serde_json::json!({ "state": "recorded", "basis": vec![2; Basis::WIDTH] })
        );
    }
}
