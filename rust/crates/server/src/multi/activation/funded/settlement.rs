use std::{collections::BTreeSet, time::Duration};

use bytes::Bytes;
use journal_contract::{Epoch, JournalError, Lsn, StreamId};
use ring::signature::{ED25519, UnparsedPublicKey};
use rtdb_stream_journal::{KeeperId, TimedCommitment, TimedOriginal, TimedOriginalVerifier};
use rtdb_tx::transact::{FundedOutcome, FundedSettlement, funded_settlement_digest};
use rtdb_types::{Anomaly, AnomalyCategory, Result, rtdb_v1::Basis};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{Closure, Scope};
use crate::multi::activation::{
    ATTEMPTS, ActivationAuthority, Binding, Fence, State, conflict, corrupt, journal_error,
    unavailable,
};

#[cfg(test)]
mod tests;

const BOOKINGS_PER_PAGE: u64 = 32;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::multi::activation) struct JournalTrust {
    catalog: Uuid,
    keepers: Vec<TrustedKeeper>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TrustedKeeper {
    identity: Uuid,
    verification_key: [u8; 32],
}

impl JournalTrust {
    pub(super) fn catalog(&self) -> Uuid {
        self.catalog
    }

    pub(super) fn valid(&self) -> bool {
        if self.catalog.is_nil() || self.keepers.len() < 2 {
            return false;
        }
        let mut identities = BTreeSet::new();
        let mut keys = BTreeSet::new();
        self.keepers.iter().all(|keeper| {
            !keeper.identity.is_nil()
                && keeper.verification_key != [0; 32]
                && identities.insert(keeper.identity)
                && keys.insert(keeper.verification_key)
        })
    }
}

impl TimedOriginalVerifier for JournalTrust {
    fn verify(&self, keeper: KeeperId, statement: &[u8], signature: &[u8; 64]) -> bool {
        self.keepers
            .iter()
            .find(|entry| entry.identity.as_u128() == keeper.0)
            .is_some_and(|entry| {
                UnparsedPublicKey::new(&ED25519, entry.verification_key)
                    .verify(statement, signature)
                    .is_ok()
            })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
struct ReceiptKey {
    namespace: Option<[u8; 32]>,
    key: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Kind {
    Transaction,
    Appointment,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Stage {
    Activate,
    Consume,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Authorization {
    stage: Stage,
    writer_epoch: Epoch,
    at: Lsn,
    payload_sha256: [u8; 32],
    decision_before_micros: i64,
    journal_before_micros: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Booking {
    id: Uuid,
    receipt: ReceiptKey,
    owner: [u8; 32],
    predecessor: Basis,
    intent_sha256: [u8; 32],
    kind: Kind,
    authorizations: Vec<Authorization>,
}

impl Booking {
    fn valid(&self, predecessor: Binding) -> bool {
        if self.id.is_nil()
            || self.owner == [0; 32]
            || self.intent_sha256 == [0; 32]
            || !(16..=64).contains(&self.receipt.key.len())
            || self.predecessor.incarnation() == [0; 16]
            || match self.receipt.namespace {
                Some(namespace) => namespace != self.owner,
                None => self.predecessor.incarnation() != *predecessor.incarnation.as_bytes(),
            }
        {
            return false;
        }
        let stages_match = match (self.kind, self.authorizations.as_slice()) {
            (_, []) => true,
            (Kind::Transaction, [consume]) => consume.stage == Stage::Consume,
            (Kind::Appointment, [activate]) => activate.stage == Stage::Activate,
            (Kind::Appointment, [activate, consume]) => {
                activate.stage == Stage::Activate
                    && consume.stage == Stage::Consume
                    && activate.at < consume.at
                    && activate.writer_epoch <= consume.writer_epoch
            }
            _ => false,
        };
        stages_match
            && self.authorizations.iter().all(|authorization| {
                authorization.writer_epoch != Epoch(0)
                    && (1..u64::MAX).contains(&authorization.at.0)
                    && authorization.payload_sha256 != [0; 32]
                    && authorization.decision_before_micros > 0
                    && authorization.decision_before_micros <= authorization.journal_before_micros
            })
    }

    fn digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"rtdb.activation.funded-booking/v1\0");
        digest.update(self.id.as_bytes());
        match self.receipt.namespace {
            None => digest.update([0]),
            Some(namespace) => {
                digest.update([1]);
                digest.update(namespace);
            }
        }
        digest.update((self.receipt.key.len() as u64).to_be_bytes());
        digest.update(&self.receipt.key);
        digest.update(self.owner);
        digest.update(self.predecessor.to_bytes());
        digest.update(self.intent_sha256);
        digest.update([match self.kind {
            Kind::Transaction => 0,
            Kind::Appointment => 1,
        }]);
        digest.update((self.authorizations.len() as u64).to_be_bytes());
        for authorization in &self.authorizations {
            digest.update([match authorization.stage {
                Stage::Activate => 0,
                Stage::Consume => 1,
            }]);
            digest.update(authorization.writer_epoch.0.to_be_bytes());
            digest.update(authorization.at.0.to_be_bytes());
            digest.update(authorization.payload_sha256);
            digest.update(authorization.decision_before_micros.to_be_bytes());
            digest.update(authorization.journal_before_micros.to_be_bytes());
        }
        digest.finalize().into()
    }
}

fn next_prefix(previous: [u8; 32], booking: &Booking) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"rtdb.activation.funded-inventory/v1\0");
    digest.update(previous);
    digest.update(booking.digest());
    digest.finalize().into()
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "kebab-case", deny_unknown_fields)]
enum Evidence {
    Committed { commitment: Vec<u8> },
    ExcludedBySeal,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Disposition {
    Accepted,
    Refused,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::multi::activation) struct Resolution {
    scope_sha256: [u8; 32],
    index: u64,
    booking: Booking,
    disposition: Disposition,
    stages: Vec<Evidence>,
    issuer_signature: Vec<u8>,
}

impl Resolution {
    pub(super) fn matches(&self, scope_sha256: [u8; 32], index: u64) -> bool {
        self.scope_sha256 == scope_sha256 && self.index == index
    }

    fn statement(&self, closure: &Closure, prefix: [u8; 32]) -> Vec<u8> {
        let mut bytes = b"rtdb.activation.funded-resolution/v1\0".to_vec();
        bytes.extend_from_slice(&self.scope_sha256);
        bytes.extend_from_slice(&closure.inventory_sha256);
        bytes.extend_from_slice(&self.index.to_be_bytes());
        bytes.extend_from_slice(&prefix);
        bytes.push(match self.disposition {
            Disposition::Accepted => 0,
            Disposition::Refused => 1,
        });
        bytes.extend_from_slice(&(self.stages.len() as u64).to_be_bytes());
        for stage in &self.stages {
            match stage {
                Evidence::Committed { commitment } => {
                    bytes.push(0);
                    bytes.extend_from_slice(&Sha256::digest(commitment));
                }
                Evidence::ExcludedBySeal => bytes.push(1),
            }
        }
        bytes
    }

    fn verify(
        &self,
        predecessor: Binding,
        scope: &Scope,
        closure: &Closure,
        previous: Option<&VerifiedBooking>,
        source_fence: Option<(Epoch, Lsn)>,
    ) -> Result<VerifiedBooking> {
        if self.scope_sha256 != scope.scope_sha256
            || self.index == 0
            || self.index > closure.bookings
            || !self.booking.valid(predecessor)
            || self.booking.authorizations.len() != self.stages.len()
            || previous
                .is_some_and(|prior| prior.resolution.booking.receipt >= self.booking.receipt)
        {
            return Err(invalid());
        }
        let prefix = next_prefix(
            previous.map_or([0; 32], |prior| prior.prefix),
            &self.booking,
        );
        if (self.index == closure.bookings && prefix != closure.inventory_sha256)
            || UnparsedPublicKey::new(&ED25519, scope.enrollment.verification_key)
                .verify(&self.statement(closure, prefix), &self.issuer_signature)
                .is_err()
        {
            return Err(invalid());
        }
        let mut accepted_through = previous.map_or(Lsn(0), |prior| prior.accepted_through);
        let mut accepted = false;
        let mut excluded = false;
        let stream = StreamId(predecessor.incarnation.as_u128());
        for (authorization, evidence) in self.booking.authorizations.iter().zip(&self.stages) {
            match evidence {
                Evidence::Committed { commitment } if !excluded => {
                    let journal = scope.journal.as_ref().ok_or_else(invalid)?;
                    let original = TimedOriginal {
                        stream,
                        epoch: authorization.writer_epoch,
                        at: authorization.at,
                        payload_sha256: authorization.payload_sha256,
                        exclusive_journal_cutoff_micros: authorization.journal_before_micros,
                        original_receiver_certificate_sha256: scope
                            .enrollment
                            .receiver_certificate_sha256,
                    };
                    TimedCommitment::decode(commitment)
                        .and_then(|proof| proof.verify(journal.catalog, original, journal))
                        .ok_or_else(invalid)?;
                    accepted_through = accepted_through.max(authorization.at);
                    accepted = authorization.stage == Stage::Consume;
                }
                Evidence::ExcludedBySeal
                    if scope.journal.is_some()
                        && source_fence.is_some_and(|(epoch, head)| {
                            authorization.writer_epoch < epoch && authorization.at > head
                        }) =>
                {
                    excluded = true;
                }
                _ => return Err(invalid()),
            }
        }
        if accepted != (self.disposition == Disposition::Accepted) {
            return Err(invalid());
        }
        Ok(VerifiedBooking {
            resolution: self.clone(),
            prefix,
            accepted_through,
            exclusion_fence: source_fence.filter(|_| excluded),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::multi::activation) struct VerifiedBooking {
    resolution: Resolution,
    prefix: [u8; 32],
    accepted_through: Lsn,
    exclusion_fence: Option<(Epoch, Lsn)>,
}

impl VerifiedBooking {
    fn portable(&self) -> Result<FundedSettlement> {
        let booking = &self.resolution.booking;
        let committed = |stage| {
            booking
                .authorizations
                .iter()
                .zip(&self.resolution.stages)
                .find_map(|(authorization, evidence)| {
                    (authorization.stage == stage && matches!(evidence, Evidence::Committed { .. }))
                        .then_some(authorization.at.0)
                })
        };
        let encoded = serde_json::to_vec(self).map_err(|_| corrupt())?;
        Ok(FundedSettlement {
            namespace: booking.receipt.namespace,
            key: booking.receipt.key.clone(),
            outcome: FundedOutcome {
                booking: *booking.id.as_bytes(),
                owner: booking.owner,
                predecessor: booking.predecessor,
                intent_sha256: booking.intent_sha256,
                appointment: booking.kind == Kind::Appointment,
                activation_lsn: committed(Stage::Activate),
                consumption_lsn: committed(Stage::Consume),
                evidence_sha256: Sha256::digest(encoded).into(),
            },
        })
    }
}

#[derive(Clone, Copy)]
struct OutcomeLocation {
    stream: StreamId,
    at: Lsn,
    index: u64,
}

impl Scope {
    fn outcome(&self, closure: &Closure, index: u64) -> OutcomeLocation {
        let offset = index - 1;
        let mut digest = Sha256::new();
        digest.update(b"rtdb.activation.funded-outcomes/v1\0");
        digest.update(self.scope_sha256);
        digest.update(closure.inventory_sha256);
        digest.update((offset / BOOKINGS_PER_PAGE).to_be_bytes());
        OutcomeLocation {
            stream: StreamId(u128::from_be_bytes(
                digest.finalize()[..16].try_into().unwrap(),
            )),
            at: Lsn(offset % BOOKINGS_PER_PAGE + 1),
            index,
        }
    }
}

impl ActivationAuthority {
    pub(super) async fn closed_funded_scope(
        &self,
        name: &str,
        predecessor: Binding,
        scope_sha256: [u8; 32],
    ) -> Result<(Scope, Option<(Epoch, Lsn)>)> {
        let (_, state) = self.read(self.stream_id(name)?).await?;
        let state = state.ok_or_else(unavailable)?;
        if state.binding == predecessor {
            if !state.grants_closed {
                return Err(conflict());
            }
            let scope = state
                .funded_scopes()
                .iter()
                .find(|scope| scope.scope_sha256 == scope_sha256)
                .cloned()
                .ok_or_else(conflict)?;
            return Ok((
                scope,
                state.recovery.and_then(|recovery| recovery.source_fence()),
            ));
        }
        let completed = self
            .archived_funded_recovery(name, &state, predecessor)
            .await?;
        let source_fence = completed.source_fence();
        let scope = completed
            .funded_scopes
            .into_iter()
            .find(|scope| scope.scope_sha256 == scope_sha256)
            .ok_or_else(conflict)?;
        Ok((scope, source_fence))
    }

    pub(super) async fn next_funded_index(&self, scope: &Scope, closure: &Closure) -> Result<Option<u64>> {
        // An outcome cannot be admitted until its predecessor exists, including across pages.
        let mut recorded = 0;
        let mut possible = closure.bookings;
        while recorded < possible {
            let index = recorded + (possible - recorded) / 2 + 1;
            let outcome = scope.outcome(closure, index);
            if self.outcome_head(outcome.stream).await? >= outcome.at {
                recorded = index;
            } else {
                possible = index - 1;
            }
        }
        Ok(recorded.checked_add(1).filter(|next| *next <= closure.bookings))
    }

    pub(in crate::multi::activation) async fn record_funded_outcome(
        &self,
        name: &str,
        predecessor: Binding,
        resolution: Resolution,
    ) -> Result<VerifiedBooking> {
        self.access.require_operator()?;
        let (scope, source_fence) = self
            .closed_funded_scope(name, predecessor, resolution.scope_sha256)
            .await?;
        let closure = scope.closure.as_ref().ok_or_else(unresolved)?;
        if !(1..=closure.bookings).contains(&resolution.index) {
            return Err(invalid());
        }

        let local = self.access.is_local();
        let outcome = scope.outcome(closure, resolution.index);
        let stream = outcome.stream;
        let mut epoch = Epoch(u64::from(local));
        for _ in 0..ATTEMPTS {
            let head = self.outcome_head(stream).await?;
            if head >= outcome.at {
                let prior = self
                    .read_funded_outcome(outcome)
                    .await?
                    .ok_or_else(corrupt)?;
                return if prior.resolution == resolution {
                    Ok(prior)
                } else {
                    Err(conflict())
                };
            }
            if head.0 + 1 != outcome.at.0 {
                return Err(unresolved());
            }
            let previous = if resolution.index == 1 {
                None
            } else {
                let previous = scope.outcome(closure, resolution.index - 1);
                Some(
                    self.read_funded_outcome(previous)
                        .await?
                        .ok_or_else(unresolved)?,
                )
            };
            let verified = resolution.verify(
                predecessor,
                &scope,
                closure,
                previous.as_ref(),
                source_fence,
            )?;
            if head == Lsn(0) {
                if local {
                    self.journal.create(stream).await
                } else {
                    self.journal.create_immutable(stream).await
                }
                .map_err(journal_error)?;
            }
            let fence = match self.seal_fence(stream, epoch).await {
                Ok((sealed, fence)) if sealed == head => fence,
                Ok(_) => continue,
                Err(JournalError::Fenced { current }) => {
                    epoch = Epoch(
                        current
                            .0
                            .checked_add(u64::from(local))
                            .ok_or_else(unavailable)?,
                    );
                    continue;
                }
                Err(error) => return Err(journal_error(error)),
            };
            let bytes = Bytes::from(serde_json::to_vec(&verified).map_err(|_| corrupt())?);
            let append = match fence {
                Fence::Local(epoch) => self.journal.append(stream, epoch, outcome.at, bytes).await,
                Fence::Immutable(fence) => {
                    self.journal
                        .append_session_batch(fence, outcome.at, vec![bytes])
                        .await
                }
            };
            match append {
                Ok(at) if at == outcome.at => return Ok(verified),
                Ok(_) => return Err(corrupt()),
                Err(JournalError::Conflict { .. } | JournalError::Fenced { .. }) => continue,
                Err(error) => return Err(journal_error(error)),
            }
        }
        Err(unavailable())
    }

    pub(in crate::multi::activation) async fn funded_outcome(
        &self,
        name: &str,
        predecessor: Binding,
        scope_sha256: [u8; 32],
        index: u64,
    ) -> Result<Option<VerifiedBooking>> {
        let (scope, _) = self
            .closed_funded_scope(name, predecessor, scope_sha256)
            .await?;
        let closure = scope.closure.as_ref().ok_or_else(unresolved)?;
        if !(1..=closure.bookings).contains(&index) {
            return Err(invalid());
        }
        self.read_funded_outcome(scope.outcome(closure, index))
            .await
    }

    pub(in crate::multi) async fn funded_backup_settlements(
        &self,
        name: &str,
        predecessor: Binding,
    ) -> Result<(Option<Lsn>, Vec<FundedSettlement>)> {
        let (_, state) = self.read(self.stream_id(name)?).await?;
        let state = state.ok_or_else(unavailable)?;
        state.require(predecessor)?;
        let pending = state
            .recovery
            .filter(|_| state.grants_closed)
            .ok_or_else(conflict)?;
        let sealed = pending.source_lsn_for_backup()?;
        let settlements = self
            .funded_settlements(&state, sealed.unwrap_or(Lsn(u64::MAX)))
            .await?;
        Ok((sealed, settlements))
    }

    pub(in crate::multi::activation) async fn funded_settlements(
        &self,
        state: &State,
        source_cut: Lsn,
    ) -> Result<Vec<FundedSettlement>> {
        let mut settlements = Vec::new();
        for scope in state.require_funded_closures()? {
            let closure = scope.closure.as_ref().ok_or_else(corrupt)?;
            if closure.bookings == 0 {
                continue;
            }
            let last = self
                .read_funded_outcome(scope.outcome(closure, closure.bookings))
                .await?
                .ok_or_else(unresolved)?;
            if last.prefix != closure.inventory_sha256 {
                return Err(corrupt());
            }
            if last.accepted_through > source_cut {
                return Err(Anomaly::new(
                    AnomalyCategory::Conflict,
                    "rtdb/activation-funded-source-cut",
                    "The installed recovery cut does not cover every committed funded stage",
                ));
            }
            for index in (1..closure.bookings).step_by(BOOKINGS_PER_PAGE as usize) {
                let count = (closure.bookings - index).min(BOOKINGS_PER_PAGE);
                let page = self
                    .read_funded_page(scope.outcome(closure, index), count)
                    .await?
                    .ok_or_else(corrupt)?;
                for booking in page {
                    settlements.push(booking.portable()?);
                }
            }
            settlements.push(last.portable()?);
        }
        settlements
            .sort_by(|left, right| (left.namespace, &left.key).cmp(&(right.namespace, &right.key)));
        funded_settlement_digest(*state.binding.incarnation.as_bytes(), &settlements)?;
        Ok(settlements)
    }

    async fn outcome_head(&self, stream: StreamId) -> Result<Lsn> {
        if !self.access.is_local() {
            match self.journal.require_immutable(stream).await {
                Ok(()) => (),
                Err(JournalError::NotFound) => return Ok(Lsn(0)),
                Err(error) => return Err(journal_error(error)),
            }
        }
        match self.journal.head(stream).await {
            Ok(head) => Ok(head),
            Err(JournalError::NotFound) => Ok(Lsn(0)),
            Err(error) => Err(journal_error(error)),
        }
    }

    async fn read_funded_outcome(
        &self,
        outcome: OutcomeLocation,
    ) -> Result<Option<VerifiedBooking>> {
        Ok(self
            .read_funded_page(outcome, 1)
            .await?
            .and_then(|mut page| page.pop()))
    }

    async fn read_funded_page(
        &self,
        outcome: OutcomeLocation,
        count: u64,
    ) -> Result<Option<Vec<VerifiedBooking>>> {
        let OutcomeLocation { stream, at, index } = outcome;
        let last = Lsn(at.0 + count - 1);
        if last > self.outcome_head(stream).await? {
            return Ok(None);
        }
        let page = if self.access.is_local() {
            self.journal
                .read(stream, at, count as usize, Duration::ZERO)
                .await
        } else {
            self.journal
                .read_page(stream, at, last, count as usize, Duration::ZERO)
                .await
        }
        .map_err(journal_error)?;
        if page.records.len() != count as usize {
            return Err(corrupt());
        }
        page.records
            .into_iter()
            .enumerate()
            .map(|(offset, (position, bytes))| {
                let record: VerifiedBooking =
                    serde_json::from_slice(&bytes).map_err(|_| corrupt())?;
                if position.0 != at.0 + offset as u64
                    || record.resolution.index != index + offset as u64
                {
                    return Err(corrupt());
                }
                Ok(record)
            })
            .collect::<Result<Vec<_>>>()
            .map(Some)
    }
}

fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/activation-funded-resolution",
        "The issuer outcome does not match the closed inventory and original journal evidence",
    )
}

fn unresolved() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/activation-funded-reconciliation-required",
        "Recovery requires a verified outcome for every booking in the closed funded inventory",
    )
}
