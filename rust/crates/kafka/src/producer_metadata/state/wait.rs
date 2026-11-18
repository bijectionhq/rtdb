use rtdb_types::{Anomaly, Result};

use super::RefreshState;

/// The next action for one invocation of ProducerMetadata.awaitUpdate.
#[derive(Debug, PartialEq, Eq)]
pub enum WaitStep {
    /// A strictly newer signed update version is available and metadata is open.
    Updated,
    /// Metadata is closed when this invocation checks its state.
    Closed,
    /// The elapsed timer reached its deadline, with the original timeout in milliseconds.
    TimedOut(i64),
    /// The caller waits for a notification or up to these positive milliseconds.
    Wait(i64),
}

/// Failure in constructing or advancing a source wait.
#[derive(Debug)]
pub enum WaitFailure {
    /// The source rejects this negative timeout after the initial clock observation.
    InvalidTimeout(i64),
    /// The source fatal check found an error before testing the update version and close state.
    Fatal(Anomaly),
    /// A required signed millisecond clock observation was unavailable.
    Clock(Anomaly),
}

/// Owns one waiting caller's requested version and source Timer state.
pub struct MetadataWait {
    last_version: i32,
    timeout_ms: i64,
    current_ms: i64,
    deadline_ms: i64,
}

impl MetadataWait {
    /// Samples the source clock before validating the nonnegative timeout.
    /// Negative clock samples become zero and deadline addition saturates at `i64::MAX`.
    pub fn new(
        last_version: i32, timeout_ms: i64, now_ms: impl FnOnce() -> Result<i64>,
    ) -> std::result::Result<Self, WaitFailure> {
        let current_ms = now_ms().map_err(WaitFailure::Clock)?.max(0);
        if timeout_ms < 0 {
            return Err(WaitFailure::InvalidTimeout(timeout_ms));
        }
        let deadline_ms = current_ms.saturating_add(timeout_ms);
        Ok(Self { last_version, timeout_ms, current_ms, deadline_ms })
    }

    /// Checks fatal errors, closed metadata, then a newer signed version before sampling the clock for expiry.
    /// The supplied fatal check preserves bootstrap errors and consumes ordinary fatal errors.
    /// Clock samples only advance the cached time.
    /// For `Wait(ms)`, the caller atomically registers its wait with the metadata check and reevaluates after waking.
    pub fn step(
        &mut self,
        refresh: &RefreshState,
        fatal: impl FnOnce() -> Result<()>,
        now_ms: impl FnOnce() -> Result<i64>,
    ) -> std::result::Result<WaitStep, WaitFailure> {
        fatal().map_err(WaitFailure::Fatal)?;
        self.advance(refresh, now_ms).map_err(WaitFailure::Clock)
    }

    pub(super) fn advance(
        &mut self, refresh: &RefreshState, now_ms: impl FnOnce() -> Result<i64>,
    ) -> Result<WaitStep> {
        if refresh.is_closed() {
            return Ok(WaitStep::Closed);
        }
        if refresh.update_version() > self.last_version {
            return Ok(WaitStep::Updated);
        }
        self.current_ms = self.current_ms.max(now_ms()?);
        if self.current_ms >= self.deadline_ms {
            return Ok(WaitStep::TimedOut(self.timeout_ms));
        }
        Ok(WaitStep::Wait(self.deadline_ms - self.current_ms))
    }
}
