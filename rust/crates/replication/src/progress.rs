use std::sync::Arc;

use rtdb_tx::TransactionResult;
use rtdb_types::{Anomaly, AnomalyCategory, PlacedAttempt, PlacedDecision, PlacedReservation, Result, SourcePosition, Timestamp, TransactionKey, TxId};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedTransaction {
    result: Arc<TransactionResult>,
    attempt: Option<PlacedAttempt>,
}

impl AppliedTransaction {
    pub(crate) fn new(result: Arc<TransactionResult>, attempt: Option<PlacedAttempt>) -> Self {
        Self { result, attempt }
    }

    pub fn result(&self) -> &TransactionResult { self.result.as_ref() }
}

impl From<Arc<TransactionResult>> for AppliedTransaction {
    fn from(result: Arc<TransactionResult>) -> Self { Self::new(result, None) }
}

#[derive(Clone, Debug)]
pub struct AppliedProgress {
    pub source_position: SourcePosition,
    pub tx_key: Option<TransactionKey>,
    pub latest_result: Option<AppliedTransaction>,
    pub external_token: Option<Arc<[u8]>>,
}
impl AppliedProgress {
    pub fn tx_id(&self) -> TxId { self.tx_key.map_or(TxId(-1), |key| key.tx_id) }
    pub fn placed_outcome(&self, attempt: PlacedAttempt) -> Option<PlacedDecision> {
        self.latest_result.as_ref().filter(|completed| completed.attempt == Some(attempt))
            .map(|completed| PlacedDecision {
                key: completed.result.tx_key(),
                committed: completed.result.committed(),
            })
    }
}

#[derive(Debug)]
pub(crate) struct DriverRestart;

#[derive(Clone, Debug)]
struct ProgressState {
    applied: AppliedProgress,
    placed: Option<PlacedReservation>,
    fault: Option<Anomaly>,
    stopped: bool,
    restart: Option<Arc<DriverRestart>>,
}

impl ProgressState {
    fn clear_terminal(&mut self) {
        self.fault = None;
        self.stopped = false;
        self.restart = None;
    }
}

pub struct ProgressMonitor { state: watch::Sender<ProgressState> }
impl ProgressMonitor {
    pub fn new(applied: AppliedProgress) -> Arc<Self> {
        Arc::new(Self { state: watch::channel(ProgressState { applied, placed: None, fault: None, stopped: false, restart: None }).0 })
    }
    pub fn current(&self) -> AppliedProgress { self.state.borrow().applied.clone() }
    pub fn fault(&self) -> Option<Anomaly> { self.state.borrow().fault.clone() }

    pub(crate) fn reserved(&self, reservation: PlacedReservation) -> Result<()> {
        let mut conflict = false;
        self.state.send_if_modified(|state| {
            if state.placed == Some(reservation) { return false; }
            if state.placed.is_some() {
                conflict = true;
                return false;
            }
            state.placed = Some(reservation);
            true
        });
        if conflict {
            return Err(Anomaly::fault("rtdb.replication/placed-slot-conflict", "A different receiver slot is still open"));
        }
        Ok(())
    }

    /// Publishes source, transaction, result, and resume token together after their effects apply.
    pub fn applied(&self, source: Option<SourcePosition>, result: Option<AppliedTransaction>, token: Option<Vec<u8>>) -> Result<()> {
        let mut failure = None;
        self.state.send_modify(|state| {
            if let Some(error) = &state.fault { failure = Some(error.clone()); return; }
            if state.stopped { failure = Some(stopped()); return; }
            if source.is_some_and(|source| source < state.applied.source_position) {
                failure = Some(Anomaly::fault("rtdb.replication/source-regression", "Applied source watermark moved backwards"));
                return;
            }
            if result.as_ref().is_some_and(|result| result.result.tx_key().tx_id <= state.applied.tx_id()) {
                failure = Some(Anomaly::fault("rtdb.replication/tx-regression", "Applied transaction ID did not advance"));
                return;
            }
            if let Some(placed) = state.placed
                && source.is_some_and(|source| source >= placed.attempt.position())
            {
                if result.as_ref().is_none_or(|result| result.attempt != Some(placed.attempt)) {
                    failure = Some(Anomaly::fault("rtdb.replication/placed-slot-bypass", "Applied source progress bypassed its receiver slot"));
                    return;
                }
                state.placed = None;
            }
            if let Some(source) = source { state.applied.source_position = source; }
            if let Some(result) = result { state.applied.tx_key = Some(result.result.tx_key()); state.applied.latest_result = Some(result); }
            if let Some(token) = token { state.applied.external_token = Some(token.into()); }
        });
        failure.map_or(Ok(()), Err)
    }

    pub fn ingestion_failed(&self, failure: Anomaly) {
        self.state.send_modify(|state| {
            state.restart = None;
            if state.fault.is_none() { state.fault = Some(failure); }
        });
    }

    /// Explicitly reopens progress after the caller has initialized a replacement driver.
    /// The caller must ensure the reset cannot clear a newer fault or stop report.
    pub fn driver_restarted(&self) { self.state.send_modify(ProgressState::clear_terminal); }
    pub fn stopped(&self) { self.driver_stopped(None); }

    pub(crate) fn prepare_driver_restart(&self) -> Option<Arc<DriverRestart>> {
        let mut restart = None;
        self.state.send_if_modified(|state| {
            if state.fault.is_some() || state.stopped {
                let permit = Arc::new(DriverRestart);
                state.restart = Some(Arc::clone(&permit));
                restart = Some(permit);
                return true;
            }
            false
        });
        restart
    }

    pub(crate) fn driver_initialized(&self, restart: Option<Arc<DriverRestart>>) -> Result<()> {
        let mut outcome = Ok(());
        self.state.send_if_modified(|state| {
            let can_restart = state.restart.as_ref().zip(restart.as_ref())
                .is_some_and(|(armed, provided)| Arc::ptr_eq(armed, provided));
            if can_restart {
                state.clear_terminal();
                return true;
            }
            outcome = match &state.fault {
                Some(fault) => Err(fault.clone()),
                None if state.stopped => Err(stopped()),
                None => Ok(()),
            };
            false
        });
        outcome
    }

    pub(crate) fn driver_stopped(&self, failure: Option<Anomaly>) {
        self.state.send_modify(|state| {
            state.restart = None;
            if state.fault.is_none() { state.fault = failure; }
            state.stopped = true;
        });
    }

    async fn until(&self, ready: impl Fn(&ProgressState) -> bool + Send, cancellation: &CancellationToken) -> Result<AppliedProgress> {
        let mut state = self.state.subscribe();
        loop {
            {
                let state = state.borrow_and_update();
                if ready(&state) { return Ok(state.applied.clone()); }
                if let Some(error) = &state.fault { return Err(error.clone()); }
                if state.stopped { return Err(stopped()); }
            }
            tokio::select! {
                _ = cancellation.cancelled() => return Err(cancelled()),
                result = state.changed() => if result.is_err() { return Err(stopped()); },
            }
        }
    }

    /// Returns the result only when the requested ID is still the most recent applied transaction.
    pub async fn await_tx(&self, tx_id: TxId, cancellation: &CancellationToken) -> Result<Option<Arc<TransactionResult>>> {
        let progress = self.until(|state| state.applied.tx_id() >= tx_id, cancellation).await?;
        Ok(progress.latest_result.map(|completed| completed.result).filter(|result| result.tx_key().tx_id == tx_id))
    }
    pub async fn await_source(&self, source: SourcePosition, cancellation: &CancellationToken) -> Result<()> {
        self.until(|state| state.applied.source_position >= source, cancellation).await.map(|_| ())
    }
    /// Waits until this exact slot is admitted or progress has moved beyond it.
    /// In the latter case the caller can redeem the retained decision.
    pub async fn await_placed_admission(&self, attempt: PlacedAttempt, submitted_at: Timestamp, cancellation: &CancellationToken) -> Result<bool> {
        let mut receiver = self.state.subscribe();
        loop {
            {
                let state = receiver.borrow_and_update();
                if state.placed.is_some_and(|slot| slot.attempt == attempt && slot.submitted_at == submitted_at)
                    || state.applied.placed_outcome(attempt).is_some()
                {
                    return Ok(true);
                }
                if state.applied.source_position >= attempt.position() { return Ok(false); }
                if let Some(error) = &state.fault { return Err(error.clone()); }
                if state.stopped { return Err(stopped()); }
            }
            tokio::select! {
                _ = cancellation.cancelled() => return Err(cancelled()),
                result = receiver.changed() => if result.is_err() { return Err(stopped()); },
            }
        }
    }
    pub async fn await_system_time(&self, time: Timestamp, cancellation: &CancellationToken) -> Result<AppliedProgress> {
        self.until(|state| state.applied.tx_key.is_some_and(|key| key.system_time >= time), cancellation).await
    }
}

pub(crate) fn cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/ingestion-interrupted", "Replication operation was cancelled") }
pub(crate) fn stopped() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/ingestion-stopped", "The replication driver has stopped") }
pub(crate) fn term_ended() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/leader-term-ended", "The leader term ended before this operation was applied") }
