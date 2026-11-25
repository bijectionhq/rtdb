use std::{collections::BTreeSet, sync::Arc};

use rtdb_auth::{OriginalFamilyGrant, VerifiedWorkloadGrant};
use rtdb_crypt::tokens::{CustodyReadProof, TokenIssuer};
use rtdb_node::{
    Clock, Database,
    serving::{
        FamilyReviewSelection, QuerySource, QueryView,
        custody_release::{CustodyResponseOwner, NativeLandingWitness},
    },
};
use rtdb_tx::transact::{
    Cell, CustodyPlacement, Guard, Op, Row, Transact,
    workload::{self, Authority, Family as ReceiptFamily, Operation, Point, Reply},
};
use rtdb_types::{
    EntityId, Timestamp, Value as CellValue,
    rtdb_v1::{At, Claim, ClaimEvidence, ClaimRegion, ClaimVerifier, Layer},
};

use super::*;
use crate::v1::custody::{CustodyProviderRoute, family_review_commitment, session_binding};
use vault::RetainedReviewQuery;
use wire::AuthorReceipt;

pub(in crate::v1) const MANAGED_FEATURE: &str = "row-custody.managed-original-family-placement";
pub(in crate::v1) const REGISTERED_AUTHORING: &str = "row-custody.managed-registered-authoring";
pub(in crate::v1) const RETAINED_AUTHORING_REVIEW: &str = "row-custody.managed-retained-authoring-review";

struct RetainedAuthoring {
    original: AuthorReceipt,
    selected: AuthorReceipt,
    read_proof: String,
    claim: String,
}

pub(in crate::v1) struct ManagedFamily<'a> {
    pub route: &'a CustodyProviderRoute,
    pub database: Arc<Database>,
    pub owner: CustodyResponseOwner,
    pub scope: &'a OriginalFamilyGrant,
    pub head: Basis,
    pub expires_at: Timestamp,
    namespace: [u8; 32],
    point: &'a Point,
}

impl ManagedFamily<'_> {
    pub async fn seal_review_claim(
        &self, call: &Call, issuer: &TokenIssuer, evidence: &ClaimEvidence, request: [u8; 32],
    ) -> Result<Claim, Status> {
        let authority = self.write_authority(call, [0; 32])?;
        let scope = authority.original_family_claim_scope(self.namespace).map_err(error::anomaly)?;
        issuer.seal_original_family_claim(evidence, &scope, request).await.map_err(error::anomaly)
    }

    fn write_authority(&self, call: &Call, request_digest: [u8; 32]) -> Result<Authority, Status> {
        Ok(Authority::OriginalFamilyWrite {
            fence: self.scope.fence.clone(),
            writer: self.scope.writer.clone(),
            branch: self.scope.branch.clone(),
            created: self.created()?,
            group: self.scope.group,
            session: session_binding(call),
            request_digest,
            authoring_controls: self.route.family_authoring_controls.clone(),
        })
    }

    pub fn created(&self) -> Result<Basis, Status> {
        Basis::from_bytes(&self.scope.created).map_err(error::anomaly)
    }

    async fn order(&self, call: &Call, operation: Operation) -> Result<Reply, Status> {
        self.database
            .serving_driver()
            .ok_or_else(denied)?
            .managed_original_family(
                workload::Request {
                    point: self.point.clone(),
                    namespace: self.namespace,
                    expires_at: self.expires_at,
                    operation,
                },
                &call.cancellation,
            )
            .await
            .map_err(error::anomaly)
    }
}

pub(in crate::v1) fn is_managed_family(call: &Call) -> bool {
    call.session
        .caller
        .principal
        .workload_grant()
        .is_some_and(VerifiedWorkloadGrant::is_original_family)
}

impl Service {
    pub(in crate::v1) async fn managed_family_review(
        &self,
        call: &Call,
        managed: &ManagedFamily<'_>,
        source: QuerySource,
        selection: &proto::CustodyReviewSelection,
    ) -> Result<(QueryView, ClaimRegion), Status> {
        let At::Exactly(basis) = source.at else {
            return Err(denied());
        };
        let selected: [u8; 32] = selection
            .selected_occurrence_sha256
            .as_slice()
            .try_into()
            .map_err(|_| invalid())?;
        if source.branch != managed.scope.branch
            || basis.incarnation() != managed.head.incarnation()
            || basis > managed.head
            || selection.group.as_slice() != managed.scope.group
            || selected == [0; 32]
        {
            return Err(denied());
        }
        let vault = managed.route.family_vault.as_ref().ok_or_else(denied)?;
        let lock = vault
            .lock(managed.scope.authority, managed.scope.family)
            .await
            .map_err(error::anomaly)?;
        let family = lock.family().map_err(error::anomaly)?;
        family.live().map_err(error::anomaly)?;
        if family.group != managed.scope.group {
            return Err(denied());
        }
        let author = family.authors.iter().find(|author| {
            matches_author(managed.scope, author)
                && author.confirmed.as_ref().is_some_and(|authored| authored.occurrence == selected)
        }).ok_or_else(denied)?;
        let target = &author.order.target;
        let region = ClaimRegion::Row { table: target.table.clone(), iid: target.id.iid() };
        let view = managed
            .database
            .serving_driver()
            .ok_or_else(denied)?
            .query_original_family_review(
                source,
                FamilyReviewSelection {
                    created: managed.created()?,
                    group: managed.scope.group,
                    selected,
                    valid_at: Timestamp(selection.valid_at_micros),
                },
                self.0.node.now(),
                &call.cancellation,
            )
            .await
            .map_err(error::anomaly)?;
        Ok((view, region))
    }

    pub(in crate::v1) async fn managed_family<'a>(
        &'a self,
        call: &'a Call,
    ) -> Result<ManagedFamily<'a>, Status> {
        let caller = &call.session.caller;
        let catalog = self.0.node.catalog();
        if call.minted
            || !caller.verified_credential
            || !catalog.is_managed()
            || self.follower_only()
            || call.session.audience_source().is_some()
            || call.session.fresh_replica
        {
            return Err(denied());
        }
        let grant = caller.principal.workload_grant().ok_or_else(denied)?;
        let route = self
            .0
            .config
            .custody_providers
            .get(&call.database)
            .ok_or_else(denied)?
            .as_ref();
        if route.owner().is_none()
            || caller.principal.user_id() != route.owner()
            || route.family_authoring_key.is_none_or(|key| key == [0; 32])
            || route.family_vault.is_none()
            || route.family_line_registry.is_none()
        {
            return Err(denied());
        }
        let (scope, point, expires_at) = grant
            .original_family(&call.database, route.authority(), self.0.node.now())
            .map_err(error::anomaly)?;
        let database = catalog.database(&call.database).map_err(error::anomaly)?;
        call.require_imported_workload(&database)?;
        if !installed_native_authority(&database)
            || !database.wire_tokens_supported()
            || !database.serving_driver().is_some_and(|driver| {
                driver.features().contains(&ROW_CUSTODY) && driver.supports_query_tokens()
            })
        {
            return Err(error::unsupported(MANAGED_FEATURE));
        }
        let owner = route.acquire(self, call, &database).await?;
        let mounted = owner
            .response_release(&SystemClock, &call.cancellation)
            .map_err(error::anomaly)?;
        let writer = mounted
            .original_writer_fence()
            .await
            .map_err(error::anomaly)?;
        let mut family = ManagedFamily {
            route,
            database,
            owner,
            scope,
            head: Basis::new(scope.authority.incarnation, 0),
            expires_at,
            namespace: grant.receipt_namespace(),
            point,
        };
        if writer.table != point.table
            || writer.id != point.id
            || writer.valid_at != point.valid_at
            || writer.written.to_bytes() != scope.writer.basis
        {
            return Err(denied());
        }
        let Reply::ReadCut(cut) = family
            .order(
                call,
                Operation::ReadRoot {
                    fence: scope.fence.clone(),
                    writer: scope.writer.clone(),
                },
            )
            .await?
        else {
            return Err(denied());
        };
        family.head = cut.basis;
        let index = family
            .database
            .serving_driver()
            .ok_or_else(denied)?
            .current_custody(&call.cancellation)
            .await
            .map_err(error::anomaly)?;
        if index.has_visibility_history()
            || !index.lineage(cut.basis).iter().any(|line| {
                line.branch == scope.branch
                    && line.created.to_bytes() == scope.created
                    && line.writable
                    && (line.dropped.is_some()
                        || line
                            .expires_at
                            .is_none_or(|until| self.0.node.now() < until))
            })
        {
            return Err(denied());
        }
        self.check_attachment(call, &family.database)?;
        grant
            .original_family(&call.database, route.authority(), self.0.node.now())
            .map_err(error::anomaly)?;
        Ok(family)
    }

    pub(super) async fn managed_family_action(
        &self,
        call: &Call,
        body: &[u8],
    ) -> Result<Vec<u8>, Status> {
        let request: Request = serde_json::from_slice(body).map_err(|_| invalid())?;
        let managed = self.managed_family(call).await?;
        let mounted = managed
            .owner
            .response_release(&SystemClock, &call.cancellation)
            .map_err(error::anomaly)?;
        let family = request.family()?;
        if family.is_some_and(|family| family != managed.scope.family) {
            return Err(denied());
        }
        let operation = FamilyCall {
            vault: managed.route.family_vault.as_ref().ok_or_else(denied)?,
            mounted: &mounted,
            authority: managed.scope.authority,
            family: managed.scope.family,
            line_registry: managed.route.family_line_registry.as_ref(),
            managed: Some(managed.scope),
        };
        let result = match request {
            Request::Scope {} => json!({
                "stage_scope": encode_hex(&stage_scope(managed.scope.authority)),
                "receipt_namespace": encode_hex(&managed.namespace),
                "expires_at_micros": managed.expires_at.0,
            }),
            Request::SubmitPlacement {
                transaction_b64,
                signature,
                ..
            } => {
                let transaction_bytes = blob(&transaction_b64, MAX_ACTION_BYTES)?;
                let digest = hash(&transaction_bytes);
                let public_key = managed
                    .route
                    .family_authoring_key
                    .as_ref()
                    .ok_or_else(denied)?;
                rtdb_crypt::custody_provider::verify_family_placement(
                    public_key,
                    &managed.namespace,
                    &digest,
                    &fixed::<64>(&signature)?,
                )
                .map_err(error::anomaly)?;
                let request = proto::TransactRequest::decode(transaction_bytes.as_slice())
                    .map_err(|_| invalid())?;
                if request.decision_clock_offer.is_some() {
                    return Err(denied());
                }
                let mut transaction = transact::request(request, call.memory())?;
                crate::v1::check_key(&transaction.key)?;
                transaction.request_memory = Some(call.request_reservation());
                let lock = operation
                    .vault
                    .lock(operation.authority, operation.family)
                    .await
                    .map_err(error::anomaly)?;
                let record = lock.family().map_err(error::anomaly)?;
                operation
                    .check_placement(record, &transaction)
                    .map_err(error::anomaly)?;
                if transaction
                    .ops
                    .iter()
                    .any(|op| matches!(op, Op::PlaceCustodyRow { .. }))
                    && let Some(rewrite) = record
                        .authors
                        .iter()
                        .find(|author| {
                            matches_author(managed.scope, author)
                                && author.order.write_key == transaction.key
                        })
                        .and_then(|author| author.order.rewrite.as_ref())
                {
                    operation
                        .retained_placement_in(&managed, call, record, rewrite.reserve_key.clone())
                        .await?;
                }
                let writer = mounted
                    .original_writer_fence()
                    .await
                    .map_err(error::anomaly)?;
                if !transaction
                    .requires
                    .first()
                    .is_some_and(|requirement| writer.authenticates(requirement))
                {
                    return Err(denied());
                }
                if transaction.uses_reviewed_custody() {
                    self.authenticate_reviewed_custody_as(
                        call,
                        &managed.database,
                        managed.route,
                        &managed.owner,
                        &writer,
                        &transaction,
                    )
                    .await?;
                }
                let Reply::Receipt(receipt) = managed
                    .order(
                        call,
                        Operation::Submit {
                            key: transaction.key.clone(),
                            authority: managed.write_authority(call, digest)?,
                            transaction: Some(transaction),
                            digest,
                        },
                    )
                    .await?
                else {
                    return Err(denied());
                };
                drop(lock);
                receipt_reply(receipt)
            }
            Request::PlacementReceipt {
                key: request_key, ..
            } => receipt_reply(
                operation
                    .retained_placement(&managed, call, key(&request_key)?)
                    .await?,
            ),
            Request::ReserveRetainedAuthoring {
                member, winner, reservation, target, reviewed_source_b64,
                original_author, selected_author, custody_read_proof_b64, claim_b64,
                native_target_b64, reserve_key, accept_key, request_sha256, receipt, proof, ..
            } => {
                let (landing, source) = Reservation {
                    member, winner, reservation, target, reviewed_source_b64,
                    native_target_b64, reserve_key, accept_key, request_sha256, receipt, proof,
                }.decode(call, operation.authority)?;
                let authors = RetainedAuthoring {
                    original: original_author,
                    selected: selected_author,
                    read_proof: custody_read_proof_b64,
                    claim: claim_b64,
                };
                operation.reserve_retained_authoring(&managed, call, landing, &source, authors).await?
            }
            request @ (Request::ConfirmAuthoring { .. } | Request::Activate { .. }) => {
                let exact_key = match &request {
                    Request::ConfirmAuthoring { write_key, .. } => key(write_key)?,
                    Request::Activate { accept_key, .. } => key(accept_key)?,
                    _ => return Err(denied()),
                };
                operation
                    .retained_placement(&managed, call, exact_key)
                    .await?;
                operation.execute(call, request).await?
            }
            request => operation.execute(call, request).await?,
        };
        self.managed_family(call).await?;
        self.check_attachment(call, &managed.database)?;
        serde_json::to_vec(&result).map_err(|_| error::anomaly(invalid_record()))
    }
}

impl FamilyCall<'_> {
    async fn reserve_retained_authoring(
        &self,
        managed: &ManagedFamily<'_>,
        call: &Call,
        mut landing: Landing,
        evidence: &proto::CustodyReviewedSource,
        authors: RetainedAuthoring,
    ) -> Result<Value, Status> {
        let lock = self.vault.lock(self.authority, self.family).await.map_err(error::anomaly)?;
        let record = lock.family().map_err(error::anomaly)?.clone();
        record.live().map_err(error::anomaly)?;
        let review = &landing.order.review;
        if !matches_landing(managed.scope, &landing) || review.cut.basis > managed.head
            || record.group != review.group
        {
            return Err(denied());
        }
        let (first, _) = authored_at(&record, review.original).map_err(error::anomaly)?;
        let (winner, _) = authored_at(&record, review.selected).map_err(error::anomaly)?;
        if !matches_author(managed.scope, first) || !matches_author(managed.scope, winner)
            || first.order.rewrite.is_some()
            || first.order.line.is_none() || first.order.line != winner.order.line
            || !matches!(first.order.target.id, EntityId::String(_))
            || first.order.target.id != winner.order.target.id
            || first.order.target.table != winner.order.target.table
        {
            return Err(denied());
        }
        let original = evidence.original.as_ref().ok_or_else(invalid)?;
        let selected = evidence.selected.as_ref().ok_or_else(invalid)?;
        if review.original == review.selected {
            if authors.original != authors.selected || original != selected {
                return Err(denied());
            }
        } else if winner.order.rewrite.as_ref().is_none_or(|rewrite| rewrite.original != review.original)
            || authors.original.key == authors.selected.key
            || authors.original.system_time_micros >= authors.selected.system_time_micros
        {
            return Err(denied());
        }

        let read_proof = blob(&authors.read_proof, 8192)?;
        let claim: proto::Claim = decode_proto(&authors.claim)?;
        let claim = Claim::from_bytes(claim.value);
        let tokens = managed.database.token_authority().await.map_err(error::anomaly)?;
        let proof = tokens.open_custody_read(
            &read_proof, managed.route.proof_owner(&managed.owner), session_binding(call),
        ).await.map_err(error::anomaly)?;
        let commitment = review.cut.commitment(review.group, review.original, review.selected);
        if proof.basis != review.cut.basis || proof.expires_at.0 != evidence.proof_expires_at_micros
            || proof.expires_at > managed.expires_at
            || proof.contributors.as_slice() != [(review.group, review.selected)]
            || proof.reviewed_source_sha256 != Some(family_review_commitment(commitment, claim.as_bytes()))
        {
            return Err(denied());
        }
        let authority = managed.write_authority(call, [0; 32])?;
        let scope = authority.original_family_claim_scope(managed.namespace).map_err(error::anomaly)?;
        let claim_evidence = tokens.verify_original_family_claims([&claim], &scope).await
            .verify(&claim).map_err(error::anomaly)?;
        let region = ClaimRegion::Row { table: winner.order.target.table.clone(), iid: winner.order.target.id.iid() };
        if claim_evidence.scope.basis != proof.basis || claim_evidence.scope.branch != review.cut.branch
            || claim_evidence.scope.layer != Layer::Merged
            || !claim_evidence.footprint.regions().is_some_and(|regions| regions.len() == 1 && regions.contains(&region))
        {
            return Err(denied());
        }
        self.check_retained_review_delivery(managed, call, &proof).await?;
        self.mounted.verify_native_retained_authoring_line(&first.order).await.map_err(error::anomaly)?;

        let original_frame = self.retained_authoring_frame(managed, call, &record, &authors.original, original).await?;
        let selected_frame = if review.original == review.selected {
            None
        } else {
            Some(self.retained_authoring_frame(managed, call, &record, &authors.selected, selected).await?)
        };
        landing.retained_query = Some(RetainedReviewQuery { proof: hash(&read_proof), claim: hash(claim.as_bytes()) });
        self.reserve(
            lock, record, landing, evidence, &original_frame, selected_frame.as_deref().unwrap_or(&original_frame),
        ).await.map_err(error::anomaly)?;
        self.check_retained_review_delivery(managed, call, &proof).await?;
        Ok(ok())
    }

    async fn check_retained_review_delivery(
        &self, managed: &ManagedFamily<'_>, call: &Call, proof: &CustodyReadProof,
    ) -> Result<(), Status> {
        if proof.expires_at <= SystemClock.now() {
            return Err(denied());
        }
        let driver = managed.database.serving_driver().ok_or_else(denied)?;
        let index = driver.current_custody(&call.cancellation).await.map_err(error::anomaly)?;
        workload::require_family_branch(&index, &managed.scope.branch, managed.created()?, SystemClock.now())
            .map_err(error::anomaly)?;
        let writer = self.mounted.original_writer_fence().await.map_err(error::anomaly)?;
        driver.check_custody_release_delivery(
            &BTreeSet::from([managed.scope.group]),
            &writer, Some(proof.key_generation.ok_or_else(denied)?), &call.cancellation,
        ).await.map_err(error::anomaly)?;
        if proof.expires_at <= SystemClock.now() {
            return Err(denied());
        }
        Ok(())
    }

    async fn retained_authoring_frame(
        &self,
        managed: &ManagedFamily<'_>,
        call: &Call,
        record: &Family,
        selector: &AuthorReceipt,
        evidence: &proto::CustodyReadContributor,
    ) -> Result<Vec<u8>, Status> {
        let commitment = evidence.occurrence.as_slice().try_into().map_err(|_| invalid())?;
        let (author, authored) = authored_at(record, commitment).map_err(error::anomaly)?;
        let key = nonzero::<32>(&selector.key)?;
        let written = basis(&selector.basis)?;
        if author.order.write_key.as_slice() != key || written <= managed.created()?
            || written > managed.head || selector.system_time_micros <= 0
        {
            return Err(denied());
        }
        let receipt = self.retained_placement_in(managed, call, record, key.to_vec()).await?;
        if receipt.key.as_slice() != key || receipt.basis != written || receipt.erasure.is_some()
            || receipt.system_time.0 != selector.system_time_micros
        {
            return Err(unknown_receipt());
        }
        if let Some(rewrite) = &author.order.rewrite {
            let reserve = self.retained_placement_in(managed, call, record, rewrite.reserve_key.clone()).await?;
            if reserve.key != rewrite.reserve_key || reserve.erasure.is_some()
                || reserve.basis <= author.order.branch_created || reserve.basis >= receipt.basis
            {
                return Err(unknown_receipt());
            }
        }

        let payload = author.payload.as_ref().ok_or_else(denied)?;
        let (occurrence, clock, original) = self.mounted.confirm_native_authoring(
            &author.order, payload.digest, author.link.as_ref().map(|link| link.digest),
        ).await.map_err(error::anomaly)?;
        let line = author.order.line.as_ref().ok_or_else(denied)?;
        if occurrence.written != receipt.basis || clock != receipt.system_time
            || original != authored.original
            || position(line.generation, clock, occurrence.ordinal).map_err(error::anomaly)?.to_bytes() != authored.position
            || contributor(record.group, original, &occurrence, clock) != *evidence
            || decode_proto::<proto::CustodyReadContributor>(&authored.evidence)? != *evidence
        {
            return Err(denied());
        }
        self.vault.check_readable(self.authority, self.family, author.order.placement).await.map_err(error::anomaly)?;
        let frame = blob(&payload.envelope, MAX_CARRIER_BYTES + 4096)?;
        if hash(&frame) != occurrence.carrier_sha256 {
            return Err(denied());
        }
        Ok(frame)
    }

    async fn retained_placement(
        &self,
        managed: &ManagedFamily<'_>,
        call: &Call,
        key: Vec<u8>,
    ) -> Result<rtdb_tx::transact::Receipt, Status> {
        let lock = self
            .vault
            .lock(self.authority, self.family)
            .await
            .map_err(error::anomaly)?;
        let record = lock.family().map_err(error::anomaly)?;
        self.retained_placement_in(managed, call, record, key).await
    }

    async fn retained_placement_in(
        &self,
        managed: &ManagedFamily<'_>,
        call: &Call,
        record: &Family,
        key: Vec<u8>,
    ) -> Result<rtdb_tx::transact::Receipt, Status> {
        let basis = self.placement_basis(managed, record, &key, call).await?;
        let Reply::Receipt(receipt) = managed
            .order(
                call,
                Operation::Status {
                    key,
                    family: ReceiptFamily::OriginalFamilyWrite,
                },
            )
            .await?
        else {
            return Err(denied());
        };
        if receipt.basis != basis || receipt.outcome != rtdb_tx::transact::Outcome::Then {
            return Err(unknown_receipt());
        }
        Ok(receipt)
    }

    pub(super) fn check_managed_stage(
        &self,
        request: &Request,
        record: &Family,
    ) -> Result<(), Status> {
        let Some(scope) = self.managed else {
            return Ok(());
        };
        if record.group != scope.group {
            return Err(denied());
        }
        let allowed = match request {
            Request::SealAuthoring { assertion, .. }
            | Request::ConfirmAuthoring { assertion, .. } => {
                let assertion = nonzero(assertion)?;
                record
                    .authors
                    .iter()
                    .any(|author| author.assertion == assertion && matches_author(scope, author))
            }
            Request::Reserve { .. } => true,
            Request::SealPayload { destination, .. }
            | Request::SealLink { destination, .. }
            | Request::BindAcceptanceBody { destination, .. }
            | Request::Activate { destination, .. } => {
                let assertion = nonzero(destination)?;
                record.landings.iter().any(|landing| {
                    landing.assertion == assertion && matches_landing(scope, landing)
                })
            }
            _ => false,
        };
        if allowed { Ok(()) } else { Err(denied()) }
    }

    fn check_placement(&self, family: &Family, tx: &Transact) -> rtdb_types::Result<()> {
        let scope = self.managed.ok_or_else(conflict)?;
        family.live()?;
        if scope.group != family.group || !tx.otherwise.is_empty() || tx.ops.is_empty() {
            return Err(conflict());
        }
        let mut checked = false;
        let mut created_group = false;
        let mut initial_placement = false;
        for op in &tx.ops {
            let matches = match op {
                Op::CreateCustodyGroup { group } if *group == scope.group && !created_group => {
                    created_group = true;
                    continue;
                }
                Op::Put { branch, .. } | Op::Delete { branch, .. }
                    if branch == &scope.branch || branch == MAIN_BRANCH =>
                {
                    continue;
                }
                Op::PlaceCustodyRow {
                    branch,
                    table,
                    row,
                    placement,
                } => family.authors.iter().any(|author| {
                    if !matches_author(scope, author)
                        || author.order.write_key != tx.key
                        || author.order.placement != placement.placement_id
                        || !matches_row(&author.order.target, branch, table, row)
                        || placement.derived.is_some()
                        || author.order.rewrite.is_some() != placement.reservation.is_some()
                        || !matches_carrier(
                            row,
                            placement,
                            scope.group,
                            author.payload.as_ref(),
                            author.link.as_ref(),
                        )
                    {
                        return false;
                    }
                    initial_placement = author.order.rewrite.is_none();
                    true
                }),
                Op::ReserveCustodyPlacement { group, reservation } if *group == scope.group => {
                    family.authors.iter().any(|author| {
                        matches_author(scope, author)
                            && author.order.rewrite.as_ref().is_some_and(|rewrite| {
                                rewrite.reserve_key == tx.key
                                    && rewrite.original == reservation.original
                                    && rewrite.source == reservation.source
                                    && rewrite.link == reservation.link
                                    && rewrite.review_sha256 == reservation.review_sha256
                            })
                            && author.order.placement == reservation.destination
                            && author.order.target == reservation.target
                            && author
                                .payload
                                .as_ref()
                                .is_some_and(|sealed| sealed.digest == reservation.carrier_sha256)
                            && author
                                .link
                                .as_ref()
                                .is_some_and(|sealed| sealed.digest == reservation.link_sha256)
                    })
                }
                Op::ReserveReviewedCustodyPlacement {
                    group,
                    reservation,
                    source,
                    read_proof,
                } if *group == scope.group => family.landings.iter().any(|landing| {
                    matches_landing(scope, landing)
                        && landing.order.reserve_key == tx.key
                        && landing.order.review.cut == *source
                        && landing.body.is_some()
                        && landing.order.review.original == reservation.original
                        && landing.order.review.selected == reservation.source
                        && landing.order.destination == reservation.destination
                        && landing.order.link == reservation.link
                        && landing.order.target == reservation.target
                        && landing.order.review_sha256 == reservation.review_sha256
                        && landing.retained_query.as_ref().is_none_or(|query| {
                            query.proof == hash(read_proof)
                                && tx.requires.iter().any(|guard| matches!(guard,
                                    Guard::Unchanged(claim) if query.claim == hash(claim.as_bytes())))
                        })
                        && landing
                            .payload
                            .as_ref()
                            .is_some_and(|sealed| sealed.digest == reservation.carrier_sha256)
                        && landing
                            .link
                            .as_ref()
                            .is_some_and(|sealed| sealed.digest == reservation.link_sha256)
                }),
                Op::LandCustodyBranch { branch, placements }
                    if branch == &scope.branch && placements.len() == 1 =>
                {
                    let place = &placements[0];
                    family.landings.iter().any(|landing| {
                        matches_landing(scope, landing)
                            && landing.order.accept_key == tx.key
                            && landing.order.reserve_key == place.reserve_key
                            && landing.order.destination == place.placement.placement_id
                            && place.placement.derived.is_none()
                            && place.placement.reservation.is_some()
                            && matches_row(
                                &landing.order.target,
                                MAIN_BRANCH,
                                &place.table,
                                &place.row,
                            )
                            && matches_carrier(
                                &place.row,
                                &place.placement,
                                scope.group,
                                landing.payload.as_ref(),
                                landing.link.as_ref(),
                            )
                            && landing.body.is_some_and(|digest| {
                                matches_witness(tx, &landing.order.receipt, digest)
                                    && matches_witness(tx, &landing.order.proof, digest)
                            })
                    })
                }
                _ => false,
            };
            if !matches || checked {
                return Err(conflict());
            }
            if matches!(
                op,
                Op::ReserveCustodyPlacement { .. } | Op::ReserveReviewedCustodyPlacement { .. }
            ) && tx.ops.len() != 1
            {
                return Err(conflict());
            }
            checked = true;
        }
        if !checked || created_group && !initial_placement {
            return Err(conflict());
        }
        Ok(())
    }

    async fn placement_basis(
        &self,
        managed: &ManagedFamily<'_>,
        family: &Family,
        key: &[u8],
        call: &Call,
    ) -> Result<Basis, Status> {
        family.live().map_err(error::anomaly)?;
        let scope = managed.scope;
        if family.group != scope.group {
            return Err(denied());
        }
        let index = managed
            .database
            .serving_driver()
            .ok_or_else(denied)?
            .current_custody(&call.cancellation)
            .await
            .map_err(error::anomaly)?;
        if !index.custody_group_live(scope.group).unwrap_or(false) {
            return Err(unknown_receipt());
        }
        let view = index
            .view(
                MAIN_BRANCH,
                At::Exactly(managed.head),
                rtdb_types::rtdb_v1::Layer::Merged,
                SystemClock.now(),
            )
            .map_err(error::anomaly)?;
        for entry in view
            .custody_placements()
            .filter(|entry| entry.group == scope.group)
        {
            let written = entry
                .occurrence
                .filter(|occurrence| occurrence.receipt_key == key)
                .map(|occurrence| occurrence.written);
            if let Some(author) = family.authors.iter().find(|author| {
                matches_author(scope, author) && author.order.placement == entry.placement_id
            }) {
                if author.order.write_key == key
                    && let Some(basis) = written
                {
                    return Ok(basis);
                }
                if let Some(rewrite) = &author.order.rewrite
                    && rewrite.reserve_key == key
                {
                    let (_, at) = index
                        .custody_reservation_at(scope.group, entry.placement_id, managed.head)
                        .map_err(error::anomaly)?;
                    return Ok(at);
                }
            }
            if let Some(landing) = family.landings.iter().find(|landing| {
                matches_landing(scope, landing) && landing.order.destination == entry.placement_id
            }) {
                if landing.order.accept_key == key
                    && let Some(basis) = written
                {
                    return Ok(basis);
                }
                if landing.order.reserve_key == key
                    && entry
                        .reviewed
                        .is_some_and(|(reviewed, _)| reviewed.receipt_key == key)
                {
                    return Ok(entry.enrolled);
                }
            }
        }
        Err(unknown_receipt())
    }
}

fn matches_author(scope: &OriginalFamilyGrant, author: &Author) -> bool {
    author.order.target.branch == scope.branch
        && author.order.branch_created.to_bytes() == scope.created
        && author.order.group == scope.group
}

fn matches_landing(scope: &OriginalFamilyGrant, landing: &Landing) -> bool {
    landing.order.review.cut.branch == scope.branch
        && landing.order.review.cut.basis.to_bytes() >= scope.created
        && landing.order.review.cut.basis.incarnation() == scope.authority.incarnation
        && landing.order.review.group == scope.group
}

fn matches_row(
    target: &rtdb_tx::transact::CustodyTarget<rtdb_types::rtdb_v1::ValidTime>,
    branch: &str,
    table: &TableRef,
    row: &Row,
) -> bool {
    target.branch == branch
        && target.table == *table
        && target.id == row.id
        && row.valid == Some(target.valid)
}

fn matches_carrier(
    row: &Row,
    placement: &CustodyPlacement,
    group: [u8; 16],
    payload: Option<&Sealed>,
    link: Option<&Sealed>,
) -> bool {
    let digest = |column: &str| match row.cells.get(column) {
        Some(Cell::Value(CellValue::Binary(bytes) | CellValue::FixedSizeBinary(bytes))) => {
            Some(hash(bytes))
        }
        _ => None,
    };
    row.custody_group == Some(group)
        && row.custody_payload.is_none()
        && payload.is_some_and(|payload| digest(&placement.carrier_column) == Some(payload.digest))
        && placement.link_column.as_deref().and_then(digest) == link.map(|link| link.digest)
        && placement.link_column.is_some() == link.is_some()
}

fn matches_witness(tx: &Transact, witness: &NativeLandingWitness, digest: [u8; 32]) -> bool {
    let mut writes = tx
        .ops
        .iter()
        .filter_map(|op| match op {
            Op::Put {
                branch,
                table,
                rows,
                ..
            } if branch == MAIN_BRANCH && *table == witness.table => Some(rows),
            _ => None,
        })
        .flatten()
        .filter(|row| row.id == witness.id);
    let matching_write = writes.next().is_some_and(|row|
        matches!(row.valid, Some(rtdb_types::rtdb_v1::ValidTime::Literal(valid)) if valid.contains(witness.valid_at))
        && matches!(row.cells.get(&witness.column),
            Some(Cell::Value(CellValue::Binary(bytes) | CellValue::FixedSizeBinary(bytes))) if hash(bytes) == digest));
    matching_write && writes.next().is_none() && !tx.ops.iter().any(|op| matches!(op,
        Op::Delete { branch, table, ids, .. } if branch == MAIN_BRANCH && *table == witness.table && ids.contains(&witness.id)))
}

fn receipt_reply(receipt: rtdb_tx::transact::Receipt) -> Value {
    json!({ "receipt_b64": URL_SAFE_NO_PAD.encode(transact::receipt(receipt).encode_to_vec()) })
}

fn denied() -> Status {
    Status::permission_denied(
        "The credential requires its commissioned Original family and active placement",
    )
}

fn unknown_receipt() -> Status {
    error::anomaly(rtdb_types::Anomaly::new(
        rtdb_types::AnomalyCategory::NotFound,
        "rtdb/unknown-transaction",
        "No transaction has committed with this key",
    ))
}
