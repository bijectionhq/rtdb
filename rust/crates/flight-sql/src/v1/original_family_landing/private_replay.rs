use std::{collections::BTreeSet, path::PathBuf};

use rtdb_auth::OriginalCustodyOperation;
use rtdb_index::branches::BranchIndex;
use rtdb_node::Clock;
use rtdb_tx::transact::workload::{self, Applied, Fence, Point, Reply};
use rtdb_types::Timestamp;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{
    managed::{ManagedFamily, RetainedAuthoring},
    vault::{LockedFamily, RetainedReviewQuery},
    *,
};

pub(crate) const ACTION: &str = "rtdb.v1.OriginalPrivateReplay";
pub(in crate::v1) const FEATURE: &str = "row-custody.managed-original-private-replay";
const MAX_APPROVAL_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug)]
pub(in crate::v1) struct ReplayApprovals {
    pub key: [u8; 32],
    pub directory: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SignedApproval {
    body_b64: String,
    signature: String,
}

impl ReplayApprovals {
    async fn read<T: DeserializeOwned + Send + 'static>(
        &self,
        hold: [u8; 32],
        purpose: &'static str,
    ) -> Result<(T, [u8; 32]), Status> {
        let path = self
            .directory
            .join(format!("{}.{purpose}.json", encode_hex(&hold)));
        let key = self.key;
        tokio::task::spawn_blocking(move || {
            let signed: SignedApproval = vault::read_record(&path, MAX_APPROVAL_BYTES)
                .map_err(error::anomaly)?
                .ok_or_else(denied)?;
            let bytes = blob(&signed.body_b64, MAX_APPROVAL_BYTES)?;
            let digest = hash(&bytes);
            rtdb_crypt::custody_provider::verify_private_replay(
                &key,
                purpose,
                &digest,
                &fixed::<64>(&signed.signature)?,
            )
            .map_err(error::anomaly)?;
            Ok((
                serde_json::from_slice(&bytes).map_err(|_| invalid())?,
                digest,
            ))
        })
        .await
        .map_err(|_| error::anomaly(unavailable()))?
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Authorization {
    hold: String,
    database: String,
    family: String,
    receipt_namespace: String,
    line_generation: String,
    creation_key: String,
    parent_basis: String,
    reviewed_source_sha256: String,
    custody_read_proof_sha256: String,
    claim_sha256: String,
    request_sha256: String,
    reserve_key: String,
    accept_key: String,
    reader_nonce_sha256: String,
    decision_sha256: String,
    owner_version_sha256: String,
    replay_head_version_sha256: String,
    audience_version_sha256: String,
    expires_at_micros: i64,
    review_expires_at_micros: i64,
    original: AuthorEvidence,
    selected: AuthorEvidence,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::v1) struct AuthorEvidence {
    pub(super) receipt: wire::AuthorReceipt,
    pub(in crate::v1) command: String,
    pub(in crate::v1) command_sha256: String,
    pub(in crate::v1) business_receipt: String,
    pub(in crate::v1) business_receipt_sha256: String,
    pub(in crate::v1) execution_slot: String,
    pub(in crate::v1) marker_sha256: String,
    pub(in crate::v1) capsule_sha256: String,
    pub(in crate::v1) mapping_sha256: String,
    sealed_replay_sha256: String,
    request_fingerprint_sha256: String,
    root_position: u64,
    private_label_sha256: String,
    recovery_expires_at_micros: i64,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReplayReference {
    hold: String,
    receipt: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReplayHold {
    reference: ReplayReference,
    authorization_sha256: [u8; 32],
    authorization: Authorization,
    fence: Fence,
    imported: Option<Fence>,
    writer: Applied,
    point: Point,
    created: Basis,
    review: NativeLandingReview,
    query: RetainedReviewQuery,
    session_sha256: [u8; 32],
    key_generation: u64,
    expires_at: Timestamp,
    phase: ReplayPhase,
    disposition: Option<Disposition>,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum ReplayPhase {
    Held {},
    Claimed {
        receipt: [u8; 32],
    },
    Completed {
        receipt: [u8; 32],
        evidence: [u8; 32],
    },
    Consumed {
        receipt: [u8; 32],
        evidence: [u8; 32],
    },
}

impl ReplayPhase {
    fn claim_receipt(self) -> Option<[u8; 32]> {
        match self {
            Self::Held {} => None,
            Self::Claimed { receipt }
            | Self::Completed { receipt, .. }
            | Self::Consumed { receipt, .. } => Some(receipt),
        }
    }

    fn completion(self) -> Option<[u8; 32]> {
        match self {
            Self::Completed { evidence, .. } | Self::Consumed { evidence, .. } => Some(evidence),
            _ => None,
        }
    }

    fn consumed(self) -> bool {
        matches!(self, Self::Consumed { .. })
    }

    fn state(self) -> &'static str {
        match self {
            Self::Held {} => "held",
            Self::Claimed { .. } => "claimed",
            Self::Completed { .. } => "completed",
            Self::Consumed { .. } => "consumed",
        }
    }

    fn claim(&mut self, receipt: [u8; 32]) -> bool {
        if matches!(self, Self::Held {}) {
            *self = Self::Claimed { receipt };
            return true;
        }
        false
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DispositionKind {
    Released,
    Cancelled,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Disposition {
    kind: DispositionKind,
    evidence: [u8; 32],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Completion {
    hold: String,
    receipt: String,
    claim_receipt: String,
    authorization_sha256: String,
    comparison_sha256: String,
    expires_at_micros: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Disposal {
    hold: String,
    receipt: String,
    claim_receipt: Option<String>,
    authorization_sha256: String,
    disposition: DispositionKind,
    disposition_sha256: String,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Control {
    ClaimPrivateReplay {
        family: String,
        hold: String,
        receipt: String,
        nonce: String,
    },
    PrivateReplayStatus {
        family: String,
        hold: String,
        receipt: Option<String>,
        authorization_sha256: Option<String>,
    },
    CompletePrivateReplay {
        family: String,
        hold: String,
        receipt: String,
    },
    DisposePrivateReplay {
        family: String,
        hold: String,
        receipt: String,
    },
}

impl ReplayHold {
    pub(super) fn has_reference(&self, reference: &ReplayReference) -> bool {
        &self.reference == reference
    }

    pub(super) fn blocks_drain(&self) -> bool {
        self.disposition.is_none()
    }

    fn cancelled(&self) -> bool {
        self.disposition
            .as_ref()
            .is_some_and(|disposition| disposition.kind == DispositionKind::Cancelled)
    }

    fn reply(&self, fresh: bool) -> Value {
        let state = if self.cancelled() {
            "cancelled"
        } else {
            self.phase.state()
        };
        json!({
            "hold": self.reference.hold,
            "receipt": self.reference.receipt,
            "state": state,
            "fresh": fresh,
            "claim_receipt": self.phase.claim_receipt().map(|receipt| encode_hex(&receipt)),
            "disposed": self.disposition.is_some(),
            "expires_at_micros": self.expires_at.0,
        })
    }

    fn landing_matches(&self, landing: &Landing) -> bool {
        self.review == landing.order.review
            && landing.retained_query.as_ref() == Some(&self.query)
            && self.authorization.reserve_key == encode_hex(&landing.order.reserve_key)
            && self.authorization.accept_key == encode_hex(&landing.order.accept_key)
            && self.authorization.request_sha256 == encode_hex(&landing.order.request_sha256)
    }

    pub(super) async fn current(
        &self,
        operation: &FamilyCall<'_>,
        route: &super::super::custody::CustodyProviderRoute,
        database: &rtdb_node::Database,
        call: &Call,
        reserved: Option<[u8; 16]>,
    ) -> Result<(), Status> {
        if self.cancelled() || SystemClock.now() >= self.expires_at {
            return Err(denied());
        }
        let driver = database.serving_driver().ok_or_else(denied)?;
        if driver.classic_workload_fence() != self.imported.as_ref() {
            return Err(denied());
        }
        let writer = operation
            .mounted
            .original_writer_fence()
            .await
            .map_err(error::anomaly)?;
        if writer.table != self.point.table
            || writer.id != self.point.id
            || writer.valid_at != self.point.valid_at
            || writer.written.to_bytes() != self.writer.basis
        {
            return Err(denied());
        }
        let namespace = nonzero(&self.authorization.receipt_namespace)?;
        let Reply::ReadCut(head) = driver
            .managed_original_family(
                workload::Request {
                    point: self.point.clone(),
                    namespace,
                    expires_at: self.expires_at,
                    operation: Operation::ReadRoot {
                        fence: self.fence.clone(),
                        writer: self.writer.clone(),
                    },
                },
                &call.cancellation,
            )
            .await
            .map_err(error::anomaly)?
        else {
            return Err(denied());
        };
        let index = driver
            .current_custody(&call.cancellation)
            .await
            .map_err(error::anomaly)?;
        if index.head() != head {
            return Err(denied());
        }
        self.check_current_review(&index, reserved)?;
        let approvals = route.private_replay.as_ref().ok_or_else(denied)?;
        let (_, digest) = approvals
            .read::<Authorization>(nonzero(&self.reference.hold)?, "authorize")
            .await?;
        if digest != self.authorization_sha256 {
            return Err(denied());
        }
        driver
            .check_custody_release_delivery(
                &BTreeSet::from([self.review.group]),
                &writer,
                Some(self.key_generation),
                &call.cancellation,
            )
            .await
            .map_err(error::anomaly)?;
        if SystemClock.now() >= self.expires_at
            || call.session.closed()
            || call.cancellation.is_cancelled()
        {
            return Err(denied());
        }
        Ok(())
    }

    fn check_current_review(
        &self,
        index: &BranchIndex,
        reserved: Option<[u8; 16]>,
    ) -> Result<(), Status> {
        let cut = &self.review.cut;
        let line = index
            .lineage_at(&cut.branch, index.head().basis)
            .ok_or_else(denied)?;
        if line.created != self.created
            || line.base
                != Some((
                    MAIN_BRANCH.to_owned(),
                    basis(&self.authorization.parent_basis)?,
                ))
            || line.expires_at.is_some_and(|until| self.expires_at > until)
        {
            return Err(denied());
        }
        workload::require_family_branch(index, &cut.branch, self.created, SystemClock.now())
            .map_err(error::anomaly)?;
        index
            .check_current_custody_review(
                self.review.group,
                self.review.original,
                self.review.selected,
                cut,
                reserved,
            )
            .map_err(error::anomaly)
    }

    pub(super) fn matches_consumed(&self, landing: &Landing, call: &Call) -> bool {
        self.phase.consumed()
            && !self.cancelled()
            && self.session_sha256 == super::super::custody::session_binding(call)
            && self.landing_matches(landing)
    }

    pub(super) async fn consume(
        &mut self,
        operation: &FamilyCall<'_>,
        managed: &ManagedFamily<'_>,
        call: &Call,
        landing: &Landing,
        prior: Option<&Landing>,
    ) -> Result<(), Status> {
        if self.phase.completion().is_none()
            || self.cancelled()
            || self.session_sha256 != super::super::custody::session_binding(call)
            || !self.landing_matches(landing)
            || self.phase.consumed() != prior.is_some_and(|prior| prior.same_reservation(landing))
        {
            return Err(denied());
        }
        self.current(operation, managed.route, &managed.database, call, None)
            .await?;
        if let ReplayPhase::Completed { receipt, evidence } = self.phase {
            self.phase = ReplayPhase::Consumed { receipt, evidence };
        }
        Ok(())
    }
}

impl Family {
    pub(super) fn private_replay(
        &self,
        reference: &ReplayReference,
    ) -> Result<&ReplayHold, Status> {
        self.private_replays
            .iter()
            .find(|hold| hold.has_reference(reference))
            .ok_or_else(denied)
    }
}

impl FamilyCall<'_> {
    pub(super) async fn acquire_private_replay(
        &self,
        managed: &ManagedFamily<'_>,
        call: &Call,
        hold: String,
        authorization_sha256: String,
        reviewed_source_b64: String,
        authors: RetainedAuthoring,
    ) -> Result<Value, Status> {
        let id = nonzero(&hold)?;
        let authorization_sha256 = nonzero(&authorization_sha256)?;
        let route = managed.route;
        let approvals = route.private_replay.as_ref().ok_or_else(denied)?;
        let layout = route.family_readback_layout.as_ref().ok_or_else(denied)?;
        let lock = self
            .vault
            .lock(self.authority, self.family)
            .await
            .map_err(error::anomaly)?;
        let mut family = lock.family().map_err(error::anomaly)?.clone();
        family.live().map_err(error::anomaly)?;
        let (authorization, digest) = approvals.read::<Authorization>(id, "authorize").await?;
        let source: proto::CustodyReviewedSource = decode_proto(&reviewed_source_b64)?;
        let review = native_review(&source)?;
        let source_bytes = blob(&reviewed_source_b64, 8192)?;
        if digest != authorization_sha256
            || authorization.hold != hold
            || authorization.database != call.database
            || authorization.family != encode_hex(&self.family)
            || authorization.receipt_namespace != encode_hex(&managed.namespace)
            || authorization.reviewed_source_sha256 != encode_hex(&hash(&source_bytes))
            || authorization.original.receipt != authors.original
            || authorization.selected.receipt != authors.selected
        {
            return Err(denied());
        }
        let verified = self
            .verify_retained_review(managed, call, &family, &review, &source, &authors)
            .await?;
        let (first, _) = authored_at(&family, review.original).map_err(error::anomaly)?;
        let (winner, _) = authored_at(&family, review.selected).map_err(error::anomaly)?;
        let line = first.order.line.as_ref().ok_or_else(denied)?;
        if authorization.line_generation != encode_hex(&line.generation)
            || authorization.creation_key != encode_hex(&line.creation_key)
            || review.cut.basis != basis(&authors.selected.basis)?
            || authorization.custody_read_proof_sha256 != encode_hex(&verified.query.proof)
            || authorization.claim_sha256 != encode_hex(&verified.query.claim)
            || key(&authorization.reserve_key)? == key(&authorization.accept_key)?
            || !(if review.original == review.selected {
                authorization.original == authorization.selected
            } else {
                authorization.original.root_position < authorization.selected.root_position
                    && authorization.original.execution_slot
                        != authorization.selected.execution_slot
            })
        {
            return Err(denied());
        }
        for commitment in [
            &authorization.request_sha256,
            &authorization.reader_nonce_sha256,
            &authorization.decision_sha256,
            &authorization.owner_version_sha256,
            &authorization.replay_head_version_sha256,
            &authorization.audience_version_sha256,
        ] {
            nonzero::<32>(commitment)?;
        }
        let driver = managed.database.serving_driver().ok_or_else(denied)?;
        let index = driver
            .current_custody(&call.cancellation)
            .await
            .map_err(error::anomaly)?;
        let line = index
            .lineage_at(&review.cut.branch, index.head().basis)
            .ok_or_else(denied)?;
        let expires_at = Timestamp(authorization.expires_at_micros)
            .min(managed.expires_at)
            .min(verified.proof.expires_at)
            .min(Timestamp(authorization.review_expires_at_micros))
            .min(Timestamp(authorization.original.recovery_expires_at_micros))
            .min(Timestamp(authorization.selected.recovery_expires_at_micros))
            .min(line.expires_at.unwrap_or(Timestamp::MAX));
        if SystemClock.now() >= expires_at || !call.session.holds_attachment(&managed.database) {
            return Err(denied());
        }
        for (author, expected) in [
            (first, &authorization.original),
            (winner, &authorization.selected),
        ] {
            for commitment in [
                &expected.sealed_replay_sha256,
                &expected.request_fingerprint_sha256,
                &expected.private_label_sha256,
            ] {
                nonzero::<32>(commitment)?;
            }
            super::super::original_family_readback::verify_replay_author(
                &index,
                layout,
                &author.order,
                expected,
                basis(&expected.receipt.basis)?,
                expected.receipt.system_time_micros,
                SystemClock.now(),
            )?;
        }
        let key_generation = verified.proof.key_generation.ok_or_else(denied)?;
        let imported = driver.classic_workload_fence().cloned();
        let acquired_at = SystemClock.now();
        let receipt = hash(
            &serde_json::to_vec(&(
                "rtdb/v1/original-private-replay/hold",
                id,
                digest,
                self.authority,
                self.family,
                &imported,
                key_generation,
                expires_at,
                acquired_at,
            ))
            .map_err(|_| error::anomaly(invalid_record()))?,
        );
        let replay = ReplayHold {
            reference: ReplayReference {
                hold,
                receipt: encode_hex(&receipt),
            },
            authorization_sha256,
            authorization,
            fence: managed.scope.fence.clone(),
            imported,
            writer: managed.scope.writer.clone(),
            point: (*managed.point).clone(),
            created: managed.created()?,
            review,
            query: verified.query,
            session_sha256: super::super::custody::session_binding(call),
            key_generation,
            expires_at,
            phase: ReplayPhase::Held {},
            disposition: None,
        };
        if let Some(prior) = family
            .private_replays
            .iter()
            .find(|prior| prior.reference.hold == replay.reference.hold)
        {
            if prior.authorization_sha256 != authorization_sha256 || prior.expires_at != expires_at
            {
                return Err(denied());
            }
            return Ok(prior.reply(false));
        }
        if family.private_replays.iter().any(|prior| {
            prior.authorization.reader_nonce_sha256 == replay.authorization.reader_nonce_sha256
                || (prior.phase.consumed() || !prior.cancelled())
                    && prior.authorization.request_sha256 == replay.authorization.request_sha256
        }) {
            return Err(denied());
        }
        if family.private_replays.len() >= MAX_HOLDS {
            return Err(error::anomaly(full()));
        }
        replay
            .current(self, route, &managed.database, call, None)
            .await?;
        let reply = replay.reply(false);
        family.private_replays.push(replay);
        lock.save(family, false).await.map_err(error::anomaly)?;
        Ok(reply)
    }
}

impl Service {
    pub(in crate::v1) async fn original_private_replay(
        &self,
        call: &Call,
        body: &[u8],
    ) -> Result<Vec<u8>, Status> {
        if body.len() > 1024 {
            return Err(invalid());
        }
        let control: Control = serde_json::from_slice(body).map_err(|_| invalid())?;
        let (family, hold, receipt, approval) = match &control {
            Control::ClaimPrivateReplay {
                family,
                hold,
                receipt,
                ..
            }
            | Control::CompletePrivateReplay {
                family,
                hold,
                receipt,
            }
            | Control::DisposePrivateReplay {
                family,
                hold,
                receipt,
            } => (family, hold, Some(receipt), None),
            Control::PrivateReplayStatus {
                family,
                hold,
                receipt,
                authorization_sha256,
            } => {
                if receipt.is_some() == authorization_sha256.is_some() {
                    return Err(invalid());
                }
                (
                    family,
                    hold,
                    receipt.as_ref(),
                    authorization_sha256.as_deref().map(nonzero).transpose()?,
                )
            }
        };
        let id = nonzero(hold)?;
        if let Some(receipt) = receipt {
            nonzero::<32>(receipt)?;
        }
        let route = self
            .0
            .config
            .custody_providers
            .get(&call.database)
            .ok_or_else(denied)?;
        let caller = &call.session.caller;
        let grant = caller.principal.workload_grant().ok_or_else(denied)?;
        let catalog = self.0.node.catalog();
        if call.minted
            || !caller.verified_credential
            || !catalog.is_managed()
            || self.follower_only()
            || route.owner().is_none()
            || caller.principal.user_id() != route.owner()
            || call.session.audience_source().is_some()
            || call.session.fresh_replica
        {
            return Err(denied());
        }
        let authority = route.authority();
        let cutoff = grant
            .admit_original_custody(
                &call.database,
                authority,
                OriginalCustodyOperation::PrivateReplay,
                SystemClock.now(),
            )
            .map_err(error::anomaly)?;
        let approvals = route.private_replay.as_ref().ok_or_else(denied)?;
        let database = catalog.database(&call.database).map_err(error::anomaly)?;
        if !call.session.holds_attachment(&database) || !installed_native_authority(&database) {
            return Err(denied());
        }
        let owner = route.acquire(self, call, &database).await?;
        let mounted = owner
            .response_release(&SystemClock, &call.cancellation)
            .map_err(error::anomaly)?;
        let operation = FamilyCall {
            vault: route.family_vault.as_ref().ok_or_else(denied)?,
            mounted: &mounted,
            authority,
            family: nonzero(family)?,
            line_registry: route.family_line_registry.as_ref(),
            managed: None,
        };
        let lock = operation
            .vault
            .lock(authority, operation.family)
            .await
            .map_err(error::anomaly)?;
        let mut record = lock.family().map_err(error::anomaly)?.clone();
        let slot = record
            .private_replays
            .iter()
            .position(|replay| {
                replay.reference.hold == *hold
                    && (receipt.is_some_and(|receipt| replay.reference.receipt == *receipt)
                        || approval == Some(replay.authorization_sha256))
            })
            .ok_or_else(denied)?;
        let replay = &mut record.private_replays[slot];
        let mut changed = false;
        let mut fresh = false;
        match control {
            Control::PrivateReplayStatus { .. } => {}
            Control::ClaimPrivateReplay { nonce, .. } => {
                if hash(&nonzero::<32>(&nonce)?)
                    != nonzero(&replay.authorization.reader_nonce_sha256)?
                {
                    return Err(denied());
                }
                if matches!(replay.phase, ReplayPhase::Held {}) {
                    if record.close.is_some()
                        || replay.disposition.is_some()
                        || cutoff < replay.expires_at
                    {
                        return Err(denied());
                    }
                    replay
                        .current(&operation, route, &database, call, None)
                        .await?;
                    fresh = replay.phase.claim(hash(
                        &serde_json::to_vec(&(
                            "rtdb/v1/original-private-replay/claim",
                            &replay.reference.hold,
                            &replay.reference.receipt,
                            nonce,
                        ))
                        .map_err(|_| error::anomaly(invalid_record()))?,
                    ));
                    changed = fresh;
                }
            }
            Control::CompletePrivateReplay { .. } => {
                let (evidence, digest) = approvals.read::<Completion>(id, "complete").await?;
                if evidence.hold != replay.reference.hold
                    || evidence.receipt != replay.reference.receipt
                    || Some(nonzero(&evidence.claim_receipt)?) != replay.phase.claim_receipt()
                    || nonzero::<32>(&evidence.authorization_sha256)? != replay.authorization_sha256
                {
                    return Err(denied());
                }
                nonzero::<32>(&evidence.comparison_sha256)?;
                if let Some(prior) = replay.phase.completion() {
                    if prior != digest {
                        return Err(denied());
                    }
                } else {
                    if record.close.is_some()
                        || replay.disposition.is_some()
                        || cutoff < replay.expires_at
                        || evidence.expires_at_micros < replay.expires_at.0
                    {
                        return Err(denied());
                    }
                    replay
                        .current(&operation, route, &database, call, None)
                        .await?;
                    replay.phase = ReplayPhase::Completed {
                        receipt: nonzero(&evidence.claim_receipt)?,
                        evidence: digest,
                    };
                    changed = true;
                }
            }
            Control::DisposePrivateReplay { .. } => {
                let (evidence, digest) = approvals.read::<Disposal>(id, "dispose").await?;
                if evidence.hold != replay.reference.hold
                    || evidence.receipt != replay.reference.receipt
                    || evidence.claim_receipt.as_deref().map(nonzero).transpose()?
                        != replay.phase.claim_receipt()
                    || nonzero::<32>(&evidence.authorization_sha256)? != replay.authorization_sha256
                    || evidence.disposition == DispositionKind::Released
                        && replay.phase.completion().is_none()
                {
                    return Err(denied());
                }
                nonzero::<32>(&evidence.disposition_sha256)?;
                if let Some(prior) = &replay.disposition {
                    if prior.evidence != digest {
                        return Err(denied());
                    }
                } else {
                    replay.disposition = Some(Disposition {
                        kind: evidence.disposition,
                        evidence: digest,
                    });
                    changed = true;
                }
            }
        }
        mounted
            .require_original_writer()
            .await
            .map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        if SystemClock.now() >= cutoff {
            return Err(denied());
        }
        let reply = replay.reply(fresh);
        if changed {
            lock.save(record, true).await.map_err(error::anomaly)?;
        }
        if fresh {
            let lock = operation
                .vault
                .lock(authority, operation.family)
                .await
                .map_err(error::anomaly)?;
            let family = lock.family().map_err(error::anomaly)?;
            family.live().map_err(error::anomaly)?;
            let replay = family
                .private_replays
                .get(slot)
                .filter(|replay| replay.disposition.is_none())
                .ok_or_else(denied)?;
            replay
                .current(&operation, route, &database, call, None)
                .await?;
            self.check_attachment(call, &database)?;
        }
        serde_json::to_vec(&reply).map_err(|_| error::anomaly(invalid_record()))
    }
}

fn denied() -> Status {
    Status::permission_denied(
        "The request requires the exact mounted Original private replay authority",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restored_claim_cannot_admit_another_reader() {
        let mut phase = ReplayPhase::Held {};
        assert!(phase.claim([1; 32]));
        let bytes = serde_json::to_vec(&phase).unwrap();
        let mut restored: ReplayPhase = serde_json::from_slice(&bytes).unwrap();
        assert!(!restored.claim([1; 32]));
        assert!(!restored.claim([2; 32]));
        assert_eq!(restored.claim_receipt(), Some([1; 32]));
        assert_eq!(restored.completion(), None);
    }

    #[test]
    fn control_refuses_caller_supplied_completion_and_disposition() {
        for (operation, field) in [
            ("complete_private_replay", "comparison_sha256"),
            ("dispose_private_replay", "disposition"),
        ] {
            let mut request =
                json!({"operation": operation, "family": "", "hold": "", "receipt": ""});
            assert!(serde_json::from_value::<Control>(request.clone()).is_ok());
            request[field] = json!("caller asserted");
            assert!(serde_json::from_value::<Control>(request).is_err());
        }
    }
}
