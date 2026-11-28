//! Order a Kernel-owned authorization before its exact timed Store effect.

use std::{collections::BTreeSet, time::Duration};

use journal_contract::BoxFuture;
use rtdb_format::journal::Record;
use rtdb_index::branches::BranchEffect;
use rtdb_tx::transact::{Outcome, Receipt, Transact, TransactionRecord};
use rtdb_types::{Anomaly, AnomalyCategory, Result, TimedJournalCutoff, Timestamp};
use stream_logkeeper::stream::consensus::authenticated::CatalogRoot;
use uuid::Uuid;

use super::{MultiDatabase, journal_error, next, no_writer};
use super::pipeline::{Append, AttemptError, Planned, plan, receipt_for};
use crate::{TimedOriginal, TimedOriginalProof};

#[derive(Clone, Copy, Debug)]
pub struct TimedStoreCommitment {
    pub catalog: Uuid,
    pub original: TimedOriginal,
}

/// Receives the exact prepared effect and returns the Kernel's final Store
/// append recording its authorization. It runs within the current writer turn.
pub type TimedStoreAuthorizer =
    Box<dyn FnOnce(TimedStoreCommitment) -> BoxFuture<'static, Result<Transact>> + Send>;

pub struct TimedStoreAppend {
    /// The Kernel's stable authorization key and prospective writes, using a
    /// placeholder commitment. This request will never be sent to the journal.
    pub preview: Transact,
    pub effect: Transact,
    pub application_root: CatalogRoot,
    pub decision_before: Timestamp,
    pub journal_cutoff: TimedJournalCutoff,
    pub authorize: TimedStoreAuthorizer,
}

pub struct TimedStoreReceipt {
    pub receipt: Receipt,
    pub evidence: TimedOriginalProof,
}

impl MultiDatabase {
    pub fn supports_timed_store_append(&self) -> bool {
        self.authenticated_store_history()
            && self.stream.batch.as_ref().and_then(|batch| batch.maximum_timed_original_bytes()).is_some()
            && matches!(self.role(), super::Role::Writer(_))
    }

    fn authenticated_store_history(&self) -> bool {
        self.stream.authenticated_journal.is_some()
            && self.stream.immutable_history()
            && self.stream.activation_scope.is_some()
            && self.stream.serving_authority.is_some()
    }

    pub(super) async fn append_timed_store_serial(&mut self, request: TimedStoreAppend) -> Result<TimedStoreReceipt> {
        let result = self.order_timed_store(request).await;
        if result.is_err() && self.writer.as_ref().is_some_and(|writer| writer.recovering) {
            self.recover_after_failure().await;
        }
        result
    }

    async fn order_timed_store(&mut self, request: TimedStoreAppend) -> Result<TimedStoreReceipt> {
        let TimedStoreAppend { preview, effect, application_root, decision_before, journal_cutoff, authorize } = request;
        if decision_before.0 <= 0 || decision_before > journal_cutoff.before() {
            return Err(invalid("The dispatch deadline must be within the certified journal cutoff"));
        }
        let journal = self.stream.authenticated_journal.as_ref()
            .filter(|_| self.authenticated_store_history()).cloned().ok_or_else(funding_unavailable)?;
        let limit = self.stream.batch.as_ref().and_then(|batch| batch.maximum_timed_original_bytes())
            .ok_or_else(funding_unavailable)?;
        self.refresh().await?;
        self.require_armed_rotation()?;
        let lease = self.authority()?;
        let authority = self.append_authority(lease.epoch)?;
        authority.check_timed_window(decision_before, journal_cutoff)?;
        let remaining = decision_before.0.saturating_sub(authority.upper().0).max(0) as u64;
        let local_deadline = tokio::time::Instant::now().checked_add(Duration::from_micros(remaining))
            .ok_or_else(authorization_expired)?;
        let history = tokio::time::timeout_at(local_deadline, async {
            journal.require_authenticated(self.journal_stream()).await?;
            journal.catalog_history().await
        }).await.map_err(|_| authorization_expired())?.map_err(journal_error)?;
        history.verify(application_root).map_err(|_| invalid("The writer serves a different application catalog"))?;

        let claims = self.tokens.verify_claims_in(std::iter::empty(), None).await;
        let now = (self.clock)();
        let features = self.write_features();
        let generation = self.state().generation;
        let coordinates = self.state().coordinates;
        let (companion, candidate) = self.writer.as_ref().ok_or_else(no_writer)?.pipeline
            .preview_timed_store_pair(&preview, &effect, now, features, &claims, journal_cutoff)?;
        let companion = append(plan(companion, lease, generation, coordinates)?)?;
        let candidate = append(plan(candidate, lease, generation, coordinates)?)?;
        if candidate.at != next(companion.at)? {
            return Err(invalid("The timed effect must immediately follow its authorization"));
        }
        let prepared = self.stream.prepare_timed_append(candidate.at, &candidate.entry, journal_cutoff).await?;
        if prepared.len() > limit {
            return Err(Anomaly::incorrect("rtdb/timed-store-size", "The sealed timed Store record exceeds the journal limit")
                .with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded));
        }
        authority.check_timed_window(decision_before, journal_cutoff)?;
        let confirmed = tokio::time::timeout_at(local_deadline, authorize(TimedStoreCommitment {
            catalog: history.genesis.catalog,
            original: prepared.commitment(),
        })).await.map_err(|_| authorization_expired())??;
        if confirmed.key != preview.key {
            return Err(invalid("The Kernel changed its original authorization key"));
        }
        let (confirmed_companion, confirmed_effect) = self.writer.as_ref().ok_or_else(no_writer)?.pipeline
            .preview_timed_store_pair(&confirmed, &effect, now, features, &claims, journal_cutoff)?;
        let confirmed_companion = append(plan(confirmed_companion, lease, generation, coordinates)?)?;
        let confirmed_effect = append(plan(confirmed_effect, lease, generation, coordinates)?)?;
        require_same_append(&candidate, &confirmed_effect)?;
        require_separate_writes(&confirmed_companion, &confirmed_effect)?;
        authority.check_timed_window(decision_before, journal_cutoff)?;

        let decision = self.writer.as_mut().ok_or_else(no_writer)?.pipeline.decide(&confirmed, now, features, &claims)?;
        let mut write = plan(decision, lease, generation, coordinates).and_then(append)
            .map_err(|error| self.begin_write_recovery(AttemptError::Local(error)))?;
        require_same_append(&confirmed_companion, &write)
            .map_err(|error| self.begin_write_recovery(AttemptError::Local(error)))?;
        let companion_position = coordinates.position(write.at)?;
        write.admit_before = Some(decision_before);
        let (_, receipts) = self.commit_one(lease, write).await?;
        let authorized = receipt_for(receipts, companion_position, &confirmed.key)?;
        if !matches!(authorized.outcome, Outcome::Then) {
            return Err(invalid("The Kernel's authorization did not apply"));
        }
        // Only after the Kernel authorization is durable can the writer send
        // the actual effect. No other command has access to either position.
        let authority = self.append_authority(lease.epoch)?;
        authority.check_timed_window(decision_before, journal_cutoff)?;
        let decision = self.writer.as_mut().ok_or_else(no_writer)?.pipeline
            .decide_timed_store(&effect, now, features, &claims, journal_cutoff)?;
        let write = plan(decision, lease, generation, coordinates).and_then(append)
            .map_err(|error| self.begin_write_recovery(AttemptError::Local(error)))?;
        require_same_append(&candidate, &write)
            .map_err(|error| self.begin_write_recovery(AttemptError::Local(error)))?;
        let evidence = authority.dispatch_timed(prepared, decision_before).await
            .map_err(|error| self.begin_write_recovery(error))?;
        let receipts = self.publish_staged(lease, vec![write], evidence.original.payload_sha256).await
            .map_err(|error| self.begin_write_recovery(AttemptError::Local(error)))?;
        Ok(TimedStoreReceipt {
            receipt: receipt_for(receipts, coordinates.position(candidate.at)?, &effect.key)?,
            evidence,
        })
    }
}

fn append(planned: Planned) -> Result<Append> {
    match planned {
        Planned::Append(write) => Ok(write),
        Planned::Answer(_) => Err(invalid("A durable attempt must recover its original authorization and receipt")),
    }
}

fn require_same_append(expected: &Append, actual: &Append) -> Result<()> {
    if expected.at != actual.at || expected.entry.epoch != actual.entry.epoch
        || expected.entry.generation != actual.entry.generation || expected.entry.record != actual.entry.record
    {
        return Err(invalid("The authorized Store decision changed during preparation"));
    }
    Ok(())
}

fn require_separate_writes(companion: &Append, effect: &Append) -> Result<()> {
    fn transaction(write: &Append) -> Result<rtdb_tx::transact::ResolvedTransact> {
        if let Record::Tx { payload, .. } = &write.entry.record
            && let TransactionRecord::Tx(tx) = TransactionRecord::decode(payload)?
        {
            return Ok(tx);
        }
        Err(invalid("A timed Store pair contains only resolved transactions"))
    }
    let companion = transaction(companion)?;
    let effect = transaction(effect)?;
    let authorized: BTreeSet<_> = companion.effects().iter().filter_map(|effect| match effect {
        BranchEffect::Write { branch, table, event } => Some((branch, table, &event.id)),
        _ => None,
    }).collect();
    if effect.effects().iter().any(|effect| match effect {
        BranchEffect::Write { branch, table, event } => authorized.contains(&(branch, table, &event.id)),
        _ => true,
    }) {
        return Err(invalid("The Store effect cannot overwrite its authorization"));
    }
    Ok(())
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::incorrect("rtdb/timed-store-authorization", message)
}

fn funding_unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb/timed-store-unavailable", "Timed Store writes require a commissioned authenticated writer and a reachable Kernel")
}

fn authorization_expired() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb/window-expired", "The Kernel did not confirm authorization before the dispatch cutoff")
}
