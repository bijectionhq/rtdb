use std::num::NonZeroU64;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use prost::Message;
use rtdb_node::{
    DatabaseAccess, SystemClock,
    serving::custody_release::{
        CustodyCarrierAuthority, CustodyReleaseRegistration, MountedResponseRelease, NativeAuthoring,
        NativeAuthoringLine, NativeAuthoringRewrite, NativeLandingOrder, NativeLandingReview,
    },
};
use rtdb_tx::transact::{CustodyReview, ROW_CUSTODY};
use rtdb_types::{
    AssertionId, Position, RootLandingBinding, RootLandingTarget, TableRef,
    rtdb_v1::{Basis, MAIN_BRANCH},
};
use serde_json::{Value, json};
use tonic::Status;

use crate::service::{Call, Service};

use super::{error, proto, transact};

mod vault;
mod wire;
mod managed;
pub(super) use managed::{MANAGED_FEATURE, REGISTERED_AUTHORING, is_managed_family};
pub(super) use vault::NativeFamilyVault;
use vault::{
    Accepted, Author, Authored, Close, Family, Hold, Landing, MAX_HOLDS, Sealed, conflict, full, hash, invalid_record,
    unavailable,
};
use wire::{
    MAX_ACTION_BYTES, MAX_CARRIER_BYTES, Request, basis, blob, contributor, decode_proto, encode_hex, fixed, invalid,
    key, nonzero, stage, stage_scope,
};

pub(crate) const ACTION: &str = "rtdb.v1.OriginalFamilyLanding";
pub(super) const FEATURE: &str = "row-custody.original-family-landing";

impl Service {
    pub(super) async fn original_family_landing(&self, call: &Call, body: &[u8]) -> Result<Vec<u8>, Status> {
        if body.len() > MAX_ACTION_BYTES {
            return Err(invalid());
        }
        if call.session.caller.principal.workload_grant().is_some_and(rtdb_auth::VerifiedWorkloadGrant::is_original_family) {
            return self.managed_family_action(call, body).await;
        }
        let route = self.custody_route(call)?;
        if self.follower_only() || call.session.caller.principal.workload_grant().is_some() {
            return Err(error::unsupported(FEATURE));
        }
        let vault = route.family_vault.as_ref().ok_or_else(|| error::unsupported(FEATURE))?;
        let database = self.wire_attachment(call, DatabaseAccess::Write).await?;
        if !installed_native_authority(&database)
            || !database.serving_driver().is_some_and(|driver| driver.features().contains(&ROW_CUSTODY))
        {
            return Err(error::unsupported(FEATURE));
        }
        let owner = route.acquire(self, call, &database).await?;
        let mounted = owner.response_release(&SystemClock, &call.cancellation).map_err(error::anomaly)?;
        mounted.require_original_writer().await.map_err(error::anomaly)?;
        let authority = mounted.native_landing_authority();
        let request: Request = serde_json::from_slice(body).map_err(|_| invalid())?;
        let result = if let Some(family) = request.family()? {
            let operation = FamilyCall {
                vault,
                mounted: &mounted,
                authority,
                family,
                line_registry: route.family_line_registry.as_ref(),
                managed: None,
            };
            call.cancellation
                .run_until_cancelled(operation.execute(call, request))
                .await
                .ok_or_else(|| error::anomaly(unavailable()))??
        } else {
            json!({ "stage_scope": encode_hex(&stage_scope(authority)) })
        };
        mounted.require_original_writer().await.map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        serde_json::to_vec(&result).map_err(|_| error::anomaly(invalid_record()))
    }
}

pub(super) fn installed_native_authority(database: &rtdb_node::Database) -> bool {
    !database.read_only() && database.placed_writer_scope().is_none()
}

struct FamilyCall<'a> {
    vault: &'a NativeFamilyVault,
    mounted: &'a MountedResponseRelease<'a>,
    authority: CustodyCarrierAuthority,
    family: [u8; 16],
    line_registry: Option<&'a TableRef>,
    managed: Option<&'a rtdb_auth::OriginalFamilyGrant>,
}

impl FamilyCall<'_> {
    async fn execute(&self, call: &Call, request: Request) -> Result<Value, Status> {
        let scope = self.authority;
        let lock = self.vault.lock(scope, self.family).await.map_err(error::anomaly)?;
        if let Request::EnrollAuthoring {
            assertion,
            stage: stage_bytes,
            group,
            native_target_b64,
            branch_created_basis,
            write_key,
            disclosure_sha256,
            line_binding,
            rewrite,
            ..
        } = request
        {
            let assertion = nonzero(&assertion)?;
            let placement = stage(&fixed::<40>(&stage_bytes)?, scope)?;
            let write_key = key(&write_key)?;
            let rewrite = if let Some(rewrite) = rewrite {
                let family = lock.family().map_err(error::anomaly)?;
                let source = nonzero(&rewrite.source_occurrence_sha256)?;
                let original = family
                    .authors
                    .iter()
                    .filter(|author| author.order.has_bound_line())
                    .filter_map(|author| author.confirmed.as_ref())
                    .find(|authored| authored.occurrence == source)
                    .map(|authored| authored.original)
                    .ok_or_else(|| error::anomaly(conflict()))?;
                let link = stage(&fixed::<40>(&rewrite.link_stage)?, scope)?;
                let reserve_key = key(&rewrite.reserve_key)?;
                if link == placement || reserve_key == write_key {
                    return Err(invalid());
                }
                Some(NativeAuthoringRewrite {
                    original,
                    source,
                    link,
                    reserve_key,
                    review_sha256: nonzero(&rewrite.review_sha256)?,
                })
            } else {
                None
            };
            let target = transact::custody_target(decode_proto(&native_target_b64)?, call.memory())?;
            let created = basis(&branch_created_basis)?;
            let group = nonzero(&group)?;
            if self.managed.is_some_and(|scope|
                target.branch != scope.branch || created.to_bytes() != scope.created || group != scope.group)
            {
                return Err(error::anomaly(conflict()));
            }
            let line = match (target.branch.as_str() == MAIN_BRANCH, line_binding) {
                (true, None) => None,
                (false, Some(line)) => {
                    let generation = nonzero(&line.line_generation)?;
                    if generation == root_line_generation(Basis::new(scope.incarnation, 0)) {
                        return Err(invalid());
                    }
                    Some(NativeAuthoringLine {
                        generation,
                        creation_key: key(&line.creation_key)?,
                        registry: self.line_registry.ok_or_else(|| error::unsupported(FEATURE))?.clone(),
                    })
                }
                _ => return Err(error::incorrect("A named authoring branch requires its ordered line binding")),
            };
            let author = Author {
                assertion,
                disclosure: nonzero(&disclosure_sha256)?,
                order: NativeAuthoring {
                    group,
                    placement,
                    write_key,
                    line,
                    rewrite,
                    target,
                    branch_created: created,
                },
                payload: None,
                link: None,
                confirmed: None,
            };
            return self.enroll(lock, author).await.map_err(error::anomaly);
        }

        let record = lock.family().map_err(error::anomaly)?.clone();
        self.check_managed_stage(&request, &record)?;
        let result = match request {
            Request::SealAuthoring { assertion, write_key, carrier_b64, .. } => {
                self.seal_authoring(
                    lock,
                    record,
                    nonzero(&assertion)?,
                    &key(&write_key)?,
                    &blob(&carrier_b64, MAX_CARRIER_BYTES)?,
                )
                .await
            }
            Request::ConfirmAuthoring { assertion, write_key, .. } => {
                self.confirm_authoring(lock, record, nonzero(&assertion)?, &key(&write_key)?).await
            }
            Request::Reserve {
                member,
                winner,
                reservation,
                target,
                reviewed_source_b64,
                original_carrier_b64,
                selected_carrier_b64,
                native_target_b64,
                reserve_key,
                accept_key,
                request_sha256,
                receipt,
                proof,
                ..
            } => {
                let reservation = fixed::<176>(&reservation)?;
                let member = fixed::<104>(&member)?;
                let winner = fixed::<32>(&winner)?;
                Position::from_bytes(winner).map_err(|_| invalid())?;
                let target = fixed::<104>(&target)?;
                let root = RootLandingTarget::from_bytes(target).map_err(|_| invalid())?;
                let request_sha256 = nonzero(&request_sha256)?;
                if request_sha256 != root.receipt || reservation[..32] != member[..32] {
                    return Err(invalid());
                }
                let destination = stage(&reservation[64..104], scope)?;
                let link = stage(&reservation[104..144], scope)?;
                let assertion: [u8; 32] = reservation[32..64].try_into().expect("assertion width");
                let review_sha256: [u8; 32] = reservation[144..].try_into().expect("commitment width");
                let reserve_key = key(&reserve_key)?;
                let accept_key = key(&accept_key)?;
                let target_slot = transact::custody_target(decode_proto(&native_target_b64)?, call.memory())?;
                let receipt = receipt.decode()?;
                let proof = proof.decode()?;
                if destination == link
                    || reserve_key == accept_key
                    || assertion == [0; 32]
                    || review_sha256 == [0; 32]
                    || target_slot.branch != MAIN_BRANCH
                    || receipt.same_cell(&proof)
                {
                    return Err(invalid());
                }
                let source: proto::CustodyReviewedSource = decode_proto(&reviewed_source_b64)?;
                let cut = source.cut.as_ref().ok_or_else(invalid)?;
                let original = source.original.as_ref().ok_or_else(invalid)?;
                let selected = source.selected.as_ref().ok_or_else(invalid)?;
                let group = original.group.as_slice().try_into().map_err(|_| invalid())?;
                let original_commitment = original.occurrence.as_slice().try_into().map_err(|_| invalid())?;
                let selected_commitment = selected.occurrence.as_slice().try_into().map_err(|_| invalid())?;
                let original_carrier = blob(&original_carrier_b64, MAX_CARRIER_BYTES + 4096)?;
                let selected_carrier = blob(&selected_carrier_b64, MAX_CARRIER_BYTES + 4096)?;
                let order = NativeLandingOrder {
                    review: NativeLandingReview {
                        group,
                        original: original_commitment,
                        selected: selected_commitment,
                        cut: CustodyReview {
                            branch: cut.branch.clone(),
                            basis: Basis::from_bytes(&cut.basis.as_ref().ok_or_else(invalid)?.value)
                                .map_err(error::anomaly)?,
                            valid_at: rtdb_types::Timestamp(cut.valid_at_micros),
                            inventory_sha256: cut.inventory_sha256.as_slice().try_into().map_err(|_| invalid())?,
                        },
                    },
                    reserve_key,
                    accept_key,
                    destination,
                    link,
                    target: target_slot,
                    review_sha256,
                    request_sha256,
                    receipt,
                    proof,
                };
                let landing = Landing {
                    assertion,
                    order,
                    target: target.to_vec(),
                    member: member.to_vec(),
                    winner,
                    source_carriers: [hash(&original_carrier), hash(&selected_carrier)],
                    review_evidence: hash(&source.encode_to_vec()),
                    body: None,
                    payload: None,
                    link: None,
                    accepted: None,
                };
                self.reserve(lock, record, landing, &source, &original_carrier, &selected_carrier).await
            }
            Request::SealPayload { destination, carrier_b64, .. } => {
                self.seal_payload(lock, record, nonzero(&destination)?, &blob(&carrier_b64, MAX_CARRIER_BYTES)?).await
            }
            Request::SealLink { destination, payload_sha256, .. } => {
                self.seal_link(lock, record, nonzero(&destination)?, nonzero(&payload_sha256)?).await
            }
            Request::BindAcceptanceBody { destination, accept_key, body_sha256, .. } => {
                self.bind(lock, record, nonzero(&destination)?, &key(&accept_key)?, nonzero(&body_sha256)?).await
            }
            Request::Activate { destination, accept_key, .. } => {
                self.activate(lock, record, nonzero(&destination)?, &key(&accept_key)?).await
            }
            Request::Open { destination, binding, registration, envelope_b64, .. } => {
                let binding = fixed::<248>(&binding)?;
                self.open(
                    lock,
                    record,
                    nonzero(&destination)?,
                    Some(binding),
                    registration,
                    &blob(&envelope_b64, MAX_CARRIER_BYTES + 4096)?,
                )
                .await
            }
            Request::OpenAuthoring { assertion, registration, envelope_b64, .. } => {
                self.open(
                    lock,
                    record,
                    nonzero(&assertion)?,
                    None,
                    registration,
                    &blob(&envelope_b64, MAX_CARRIER_BYTES + 4096)?,
                )
                .await
            }
            Request::Dispose { registration, .. } => self.dispose(lock, record, registration).await,
            Request::PlanClose { .. } => {
                let inventory = match &record.close {
                    Some(close) => close.family_inventory,
                    None => record.inventory().map_err(error::anomaly)?,
                };
                Ok(json!({ "family_inventory_sha256": encode_hex(&inventory) }))
            }
            Request::Close { revoke_key, revoked_at, inventory_sha256, family_inventory_sha256, .. } => {
                self.close(
                    lock,
                    record,
                    key(&revoke_key)?,
                    basis(&revoked_at)?,
                    nonzero(&inventory_sha256)?,
                    nonzero(&family_inventory_sha256)?,
                )
                .await
            }
            Request::RecordDrain { revoked_at, inventory_sha256, .. } => {
                self.drain(lock, record, basis(&revoked_at)?, nonzero(&inventory_sha256)?).await
            }
            Request::Scope {} | Request::EnrollAuthoring { .. }
            | Request::SubmitPlacement { .. } | Request::PlacementReceipt { .. } => return Err(invalid()),
        };
        result.map_err(error::anomaly)
    }

    async fn enroll(&self, lock: vault::LockedFamily, author: Author) -> rtdb_types::Result<Value> {
        let mut family = match &lock.record {
            Some(record) => record.clone(),
            None if author.order.rewrite.is_none() => Family {
                authority: self.authority,
                family: self.family,
                group: author.order.group,
                authors: Vec::new(),
                landings: Vec::new(),
                holds: Vec::new(),
                close: None,
            },
            None => return Err(conflict()),
        };
        family.live()?;
        if family.group != author.order.group {
            return Err(conflict());
        }
        if let Some(prior) = family.authors.iter().find(|prior| prior.assertion == author.assertion) {
            return if prior.same_enrollment(&author) { Ok(ok()) } else { Err(conflict()) };
        }
        if family.landings.iter().any(|prior| prior.assertion == author.assertion)
            || author.order.rewrite.is_none() && !family.authors.is_empty()
        {
            return Err(conflict());
        }
        let stages: Vec<_> = std::iter::once(author.order.placement)
            .chain(author.order.rewrite.as_ref().map(|rewrite| rewrite.link))
            .collect();
        family.check_new_stages(&stages)?;
        self.mounted.verify_native_authoring_scope(&author.order).await?;
        self.vault.bind_line(self.authority, &author.order).await?;
        family.authors.push(author);
        lock.save(family, false).await?;
        Ok(ok())
    }

    async fn seal_authoring(
        &self,
        lock: vault::LockedFamily,
        mut family: Family,
        assertion: [u8; 32],
        key: &[u8],
        carrier: &[u8],
    ) -> rtdb_types::Result<Value> {
        family.live()?;
        let author = family
            .authors
            .iter_mut()
            .find(|entry| entry.assertion == assertion && entry.order.write_key == key)
            .ok_or_else(conflict)?;
        if !author.order.has_bound_line() {
            return Err(conflict());
        }
        if let Some(payload) = &author.payload {
            if payload.input != hash(carrier) {
                return Err(conflict());
            }
            return Ok(authoring_reply(payload, author.link.as_ref()));
        }
        self.mounted.verify_native_authoring_scope(&author.order).await?;
        let payload = self
            .vault
            .seal(self.authority, self.family, author.order.placement, author_binding(author)?, carrier)
            .await?;
        let link = match &author.order.rewrite {
            Some(rewrite) => Some(
                self.vault
                    .seal(
                        self.authority,
                        self.family,
                        rewrite.link,
                        author_binding(author)?,
                        &[rewrite.original.as_slice(), &rewrite.source, &payload.digest, &rewrite.review_sha256]
                            .concat(),
                    )
                    .await?,
            ),
            None => None,
        };
        let reply = authoring_reply(&payload, link.as_ref());
        author.payload = Some(payload);
        author.link = link;
        lock.save(family, false).await?;
        Ok(reply)
    }

    async fn confirm_authoring(
        &self,
        lock: vault::LockedFamily,
        mut family: Family,
        assertion: [u8; 32],
        key: &[u8],
    ) -> rtdb_types::Result<Value> {
        family.live()?;
        let author = family
            .authors
            .iter_mut()
            .find(|entry| entry.assertion == assertion && entry.order.write_key == key)
            .ok_or_else(conflict)?;
        if !author.order.has_bound_line() {
            return Err(conflict());
        }
        self.mounted.verify_native_landing_live(family.group).await?;
        if let Some(confirmed) = &author.confirmed {
            return Ok(confirmation(self.authority, author, confirmed));
        }
        let payload = author.payload.as_ref().ok_or_else(conflict)?;
        let (occurrence, clock, original) = self
            .mounted
            .confirm_native_authoring(&author.order, payload.digest, author.link.as_ref().map(|link| link.digest))
            .await?;
        let line_generation = author
            .order
            .line
            .as_ref()
            .map_or_else(|| root_line_generation(author.order.branch_created), |line| line.generation);
        let authored = Authored {
            position: position(line_generation, clock, occurrence.ordinal)?.to_bytes(),
            original,
            occurrence: occurrence.commitment(family.group),
            evidence: URL_SAFE_NO_PAD.encode(contributor(family.group, original, &occurrence, clock).encode_to_vec()),
        };
        let reply = confirmation(self.authority, author, &authored);
        author.confirmed = Some(authored);
        lock.save(family, false).await?;
        Ok(reply)
    }

    async fn reserve(
        &self,
        lock: vault::LockedFamily,
        mut family: Family,
        landing: Landing,
        evidence: &proto::CustodyReviewedSource,
        original_bytes: &[u8],
        selected_bytes: &[u8],
    ) -> rtdb_types::Result<Value> {
        family.live()?;
        let review = &landing.order.review;
        if family.group != review.group || review.cut.branch == MAIN_BRANCH {
            return Err(conflict());
        }
        if self.managed.is_some_and(|scope| review.cut.branch != scope.branch || review.group != scope.group
            || review.cut.basis.to_bytes()[..16] != scope.created[..16]
            || review.cut.basis.to_bytes() < scope.created)
        {
            return Err(conflict());
        }
        let (member, original) = authored_at(&family, review.original)?;
        let (winner, selected) = authored_at(&family, review.selected)?;
        if !member.order.has_bound_line() || !winner.order.has_bound_line() {
            return Err(conflict());
        }
        if let Some(prior) = family.landings.iter().find(|prior| prior.assertion == landing.assertion) {
            if !prior.same_reservation(&landing) {
                return Err(conflict());
            }
            self.mounted.verify_native_landing_live(family.group).await?;
            return Ok(ok());
        }
        if family.authors.iter().any(|author| author.assertion == landing.assertion) {
            return Err(conflict());
        }
        family.check_new_stages(&[landing.order.destination, landing.order.link])?;
        if member.order.rewrite.is_some()
            || selected.original != original.occurrence
            || member_bytes(self.authority, member, original) != landing.member
            || selected.position != landing.winner
            || member.payload.as_ref().is_none_or(|sealed| sealed.digest != landing.source_carriers[0])
            || winner.payload.as_ref().is_none_or(|sealed| sealed.digest != landing.source_carriers[1])
        {
            return Err(conflict());
        }
        let [(original, original_time), (selected, selected_time)] =
            self.mounted.verify_native_landing_review(review, original_bytes, selected_bytes).await?;
        if evidence.original.as_ref() != Some(&contributor(review.group, review.original, &original, original_time))
            || evidence.selected.as_ref() != Some(&contributor(review.group, review.original, &selected, selected_time))
        {
            return Err(conflict());
        }
        family.landings.push(landing);
        lock.save(family, false).await?;
        Ok(ok())
    }

    async fn seal_payload(
        &self,
        lock: vault::LockedFamily,
        mut family: Family,
        assertion: [u8; 32],
        carrier: &[u8],
    ) -> rtdb_types::Result<Value> {
        family.live()?;
        self.mounted.verify_native_landing_live(family.group).await?;
        let slot = bound_landing_index(&family, assertion)?;
        let landing = &mut family.landings[slot];
        if let Some(payload) = &landing.payload {
            if payload.input != hash(carrier) {
                return Err(conflict());
            }
            return Ok(envelope(payload));
        }
        let payload = self
            .vault
            .seal(self.authority, self.family, landing.order.destination, landing_binding(landing)?, carrier)
            .await?;
        let reply = envelope(&payload);
        landing.payload = Some(payload);
        lock.save(family, false).await?;
        Ok(reply)
    }

    async fn seal_link(
        &self,
        lock: vault::LockedFamily,
        mut family: Family,
        assertion: [u8; 32],
        payload_sha256: [u8; 32],
    ) -> rtdb_types::Result<Value> {
        family.live()?;
        self.mounted.verify_native_landing_live(family.group).await?;
        let slot = bound_landing_index(&family, assertion)?;
        let landing = &mut family.landings[slot];
        if landing.payload.as_ref().is_none_or(|payload| payload.digest != payload_sha256) {
            return Err(conflict());
        }
        if let Some(link) = &landing.link {
            return Ok(envelope(link));
        }
        let body = [landing.winner.as_slice(), &landing.order.review_sha256, &payload_sha256].concat();
        let link =
            self.vault.seal(self.authority, self.family, landing.order.link, landing_binding(landing)?, &body).await?;
        let reply = envelope(&link);
        landing.link = Some(link);
        lock.save(family, false).await?;
        Ok(reply)
    }

    async fn bind(
        &self,
        lock: vault::LockedFamily,
        mut family: Family,
        assertion: [u8; 32],
        key: &[u8],
        body: [u8; 32],
    ) -> rtdb_types::Result<Value> {
        family.live()?;
        self.mounted.verify_native_landing_live(family.group).await?;
        let slot = bound_landing_index(&family, assertion)?;
        let landing = &mut family.landings[slot];
        if landing.order.accept_key != key || landing.payload.is_none() || landing.link.is_none() {
            return Err(conflict());
        }
        if let Some(prior) = landing.body {
            return if prior == body { Ok(ok()) } else { Err(conflict()) };
        }
        landing.body = Some(body);
        lock.save(family, false).await?;
        Ok(ok())
    }

    async fn activate(
        &self,
        lock: vault::LockedFamily,
        mut family: Family,
        assertion: [u8; 32],
        key: &[u8],
    ) -> rtdb_types::Result<Value> {
        family.live()?;
        let group = family.group;
        let slot = bound_landing_index(&family, assertion)?;
        let landing = &mut family.landings[slot];
        if landing.order.accept_key != key {
            return Err(conflict());
        }
        let accepted =
            self.mounted.verify_native_landing_acceptance(&landing.order, landing.body.ok_or_else(conflict)?).await?;
        let payload = landing.payload.as_ref().ok_or_else(conflict)?;
        let link = landing.link.as_ref().ok_or_else(conflict)?;
        if payload.digest != hash(accepted.carrier()) || link.digest != hash(accepted.link()) {
            return Err(conflict());
        }
        let position = position(
            root_line_generation(Basis::new(self.authority.incarnation, 0)),
            accepted.receipt().system_time,
            accepted.occurrence().ordinal,
        )?;
        self.vault.check_readable(self.authority, self.family, landing.order.destination).await?;
        self.vault.check_readable(self.authority, self.family, landing.order.link).await?;
        let binding = RootLandingBinding {
            position,
            assertion: AssertionId::from_bytes(assertion),
            stage: landing.order.destination,
            target: RootLandingTarget::from_bytes(landing.target.as_slice().try_into().map_err(|_| invalid_record())?)
                .map_err(|_| invalid_record())?,
            payload_digest: payload.digest,
            link_digest: link.digest,
        };
        let result =
            Accepted { binding: binding.to_bytes().to_vec(), occurrence: accepted.occurrence().commitment(group) };
        let reply = json!({ "binding": encode_hex(&result.binding) });
        if let Some(prior) = &landing.accepted {
            return if prior == &result { Ok(reply) } else { Err(conflict()) };
        }
        landing.accepted = Some(result);
        lock.save(family, false).await?;
        Ok(reply)
    }

    async fn open(
        &self,
        lock: vault::LockedFamily,
        mut family: Family,
        assertion: [u8; 32],
        binding: Option<[u8; 248]>,
        registration: CustodyReleaseRegistration,
        envelope: &[u8],
    ) -> rtdb_types::Result<Value> {
        family.live()?;
        require_registration(&family, registration, Some(assertion))?;
        let (stage, authoring, digest) = match binding {
            Some(binding) => {
                let landing = &family.landings[bound_landing_index(&family, assertion)?];
                let accepted = landing.accepted.as_ref().ok_or_else(conflict)?;
                if accepted.binding != binding || accepted.occurrence != registration.occurrence {
                    return Err(conflict());
                }
                (
                    landing.order.destination,
                    landing_binding(landing)?,
                    landing.payload.as_ref().ok_or_else(conflict)?.digest,
                )
            }
            None => {
                let (author, _) = authored_at(&family, registration.occurrence)?;
                if !author.order.has_bound_line() {
                    return Err(conflict());
                }
                (author.order.placement, author_binding(author)?, author.payload.as_ref().ok_or_else(conflict)?.digest)
            }
        };
        if hash(envelope) != digest {
            return Err(conflict());
        }
        self.mounted.release().check(registration).await?;
        let held = match family.holds.iter().find(|hold| hold.registration.attempt() == registration.attempt()) {
            Some(hold) if hold.registration == registration && hold.assertion == assertion => true,
            Some(_) => return Err(conflict()),
            None if family.holds.len() >= MAX_HOLDS => return Err(full()),
            None => false,
        };
        if !held {
            family.holds.push(Hold { registration, assertion });
            lock.save(family, false).await?;
        } else {
            drop(lock);
        }
        let lock = self.vault.lock(self.authority, self.family).await?;
        lock.family()?.live()?;
        if !lock.family()?.holds.iter().any(|hold| hold.registration == registration && hold.assertion == assertion) {
            return Err(conflict());
        }
        let opened = self.vault.open_stage(self.authority, self.family, stage, authoring, envelope).await?;
        self.mounted.release().check(registration).await?;
        Ok(json!({ "carrier_b64": URL_SAFE_NO_PAD.encode(opened.bytes.as_slice()) }))
    }

    async fn dispose(
        &self,
        lock: vault::LockedFamily,
        mut family: Family,
        registration: CustodyReleaseRegistration,
    ) -> rtdb_types::Result<Value> {
        require_registration(&family, registration, None)?;
        if family
            .holds
            .iter()
            .any(|hold| hold.registration.attempt() == registration.attempt() && hold.registration != registration)
        {
            return Err(conflict());
        }
        let phase = self.mounted.release().dispose(registration).await?;
        if family.holds.iter().any(|hold| hold.registration == registration) {
            if !matches!(phase, rtdb_node::serving::custody_release::CustodyReleasePhase::Disposed { .. }) {
                return Err(conflict());
            }
            family.holds.retain(|hold| hold.registration != registration);
            lock.save(family, true).await?;
        }
        Ok(ok())
    }

    async fn close(
        &self,
        lock: vault::LockedFamily,
        mut family: Family,
        key: Vec<u8>,
        revoked_at: Basis,
        native_inventory: [u8; 32],
        family_inventory: [u8; 32],
    ) -> rtdb_types::Result<Value> {
        if let Some(prior) = &family.close {
            return if (prior.key.as_slice(), prior.revoked_at, prior.native_inventory, prior.family_inventory)
                == (key.as_slice(), revoked_at, native_inventory, family_inventory)
            {
                Ok(ok())
            } else {
                Err(conflict())
            };
        }
        if family.inventory()? != family_inventory {
            return Err(conflict());
        }
        let closed_at =
            self.mounted.verify_native_group_revocation(family.group, &key, revoked_at, native_inventory).await?;
        family.close = Some(Close { key, revoked_at, native_inventory, family_inventory, closed_at, drained: false });
        lock.save(family, true).await?;
        Ok(ok())
    }

    async fn drain(
        &self,
        lock: vault::LockedFamily,
        mut family: Family,
        revoked_at: Basis,
        inventory: [u8; 32],
    ) -> rtdb_types::Result<Value> {
        let close = family.close.as_ref().ok_or_else(conflict)?;
        if (close.revoked_at, close.native_inventory) != (revoked_at, inventory) || !family.holds.is_empty() {
            return Err(conflict());
        }
        if close.drained {
            return Ok(ok());
        }
        self.mounted.verify_native_landing_drain(family.group, revoked_at, inventory).await?;
        for stage in family.stages() {
            self.vault.destroy_stage(self.authority, self.family, stage, close.closed_at).await?;
        }
        for author in &mut family.authors {
            author.payload = None;
            author.link = None;
        }
        for landing in &mut family.landings {
            landing.payload = None;
            landing.link = None;
        }
        family.close.as_mut().ok_or_else(invalid_record)?.drained = true;
        lock.save(family, true).await?;
        Ok(ok())
    }
}

fn ok() -> Value {
    json!({ "ok": true })
}
fn envelope(sealed: &Sealed) -> Value {
    json!({ "envelope_b64": sealed.envelope })
}
fn authoring_reply(payload: &Sealed, link: Option<&Sealed>) -> Value {
    match link {
        Some(link) => json!({ "carrier_b64": payload.envelope, "carrier_sha256": encode_hex(&payload.digest),
            "link_b64": link.envelope, "link_sha256": encode_hex(&link.digest) }),
        None => json!({ "carrier_b64": payload.envelope, "carrier_sha256": encode_hex(&payload.digest) }),
    }
}

fn author_binding(author: &Author) -> rtdb_types::Result<[u8; 32]> {
    serde_json::to_vec(&("authoring", author.assertion, author.disclosure, &author.order))
        .map(|bytes| hash(&bytes))
        .map_err(|_| invalid_record())
}

fn landing_binding(landing: &Landing) -> rtdb_types::Result<[u8; 32]> {
    serde_json::to_vec(&(
        "landing",
        landing.assertion,
        &landing.order,
        &landing.target,
        &landing.member,
        landing.winner,
    ))
    .map(|bytes| hash(&bytes))
    .map_err(|_| invalid_record())
}

fn bound_landing_index(family: &Family, assertion: [u8; 32]) -> rtdb_types::Result<usize> {
    let slot = family.landings.iter().position(|entry| entry.assertion == assertion).ok_or_else(conflict)?;
    let review = &family.landings[slot].order.review;
    let (member, _) = authored_at(family, review.original)?;
    let (winner, _) = authored_at(family, review.selected)?;
    if !member.order.has_bound_line() || !winner.order.has_bound_line() {
        return Err(conflict());
    }
    Ok(slot)
}

fn authored_at(family: &Family, occurrence: [u8; 32]) -> rtdb_types::Result<(&Author, &Authored)> {
    family
        .authors
        .iter()
        .filter_map(|author| author.confirmed.as_ref().map(|confirmed| (author, confirmed)))
        .find(|(_, confirmed)| confirmed.occurrence == occurrence)
        .ok_or_else(conflict)
}

fn require_registration(
    family: &Family,
    registration: CustodyReleaseRegistration,
    assertion: Option<[u8; 32]>,
) -> rtdb_types::Result<()> {
    if registration.authority != family.authority || registration.group != family.group {
        return Err(conflict());
    }
    let author = authored_at(family, registration.occurrence).is_ok_and(|(author, confirmed)| {
        assertion.is_none_or(|id| id == author.assertion) && confirmed.original == registration.original
    });
    let landing = family.landings.iter().any(|landing| {
        assertion.is_none_or(|id| id == landing.assertion)
            && landing.order.review.original == registration.original
            && landing.accepted.as_ref().is_some_and(|accepted| accepted.occurrence == registration.occurrence)
    });
    if author || landing { Ok(()) } else { Err(conflict()) }
}

fn root_line_generation(created: Basis) -> [u8; 16] {
    let line = hash(
        &[b"rtdb/v1/original-family-authoring-line\0".as_slice(), &created.to_bytes(), MAIN_BRANCH.as_bytes()].concat(),
    );
    line[..16].try_into().expect("line width")
}

fn position(line_generation: [u8; 16], clock: rtdb_types::Timestamp, ordinal: u64) -> rtdb_types::Result<Position> {
    Ok(Position {
        line_generation,
        publication: clock.0.try_into().map_err(|_| conflict())?,
        ordinal: NonZeroU64::new(ordinal).ok_or_else(conflict)?,
    })
}

fn member_bytes(authority: CustodyCarrierAuthority, author: &Author, authored: &Authored) -> Vec<u8> {
    [authored.position.as_slice(), &author.assertion, &stage_scope(authority), &author.order.placement].concat()
}

fn confirmation(authority: CustodyCarrierAuthority, author: &Author, authored: &Authored) -> Value {
    json!({ "member": encode_hex(&member_bytes(authority, author, authored)), "occurrence_b64": authored.evidence })
}
