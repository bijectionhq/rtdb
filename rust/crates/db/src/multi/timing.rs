//! Phase timings of a checkpoint, emitted as a debug event on the
//! `rtdb_db::checkpoint` target (`XTDB_LOGGING_LEVEL_DB_CHECKPOINT=debug`).

use std::time::Instant;

use journal_contract::Lsn;
use uuid::Uuid;

const TARGET: &str = "rtdb_db::checkpoint";

pub(super) struct Phases {
    started: Option<Instant>,
    last: Option<Instant>,
    phases: Vec<(&'static str, f64)>,
}

impl Phases {
    pub(super) fn start() -> Self {
        let now = tracing::enabled!(target: TARGET, tracing::Level::DEBUG).then(Instant::now);
        Self { started: now, last: now, phases: Vec::new() }
    }

    pub(super) fn mark(&mut self, phase: &'static str) {
        let Some(last) = self.last else { return };
        let now = Instant::now();
        self.phases.push((phase, now.duration_since(last).as_secs_f64() * 1e3));
        self.last = Some(now);
    }

    pub(super) fn report(&self, database: Uuid, payload: usize, through: Lsn) {
        self.report_phase("checkpoint", database, payload, through);
    }

    /// `kind` names the step: a whole `checkpoint`, or the `deferred_build`
    /// and `deferred_announce` halves of a deferred one.
    pub(super) fn report_phase(&self, kind: &str, database: Uuid, payload: usize, through: Lsn) {
        let Some(started) = self.started else { return };
        let phases = self.phases.iter().map(|(name, ms)| format!("\"{name}\":{ms:.3}")).collect::<Vec<_>>().join(",");
        tracing::debug!(
            target: TARGET,
            kind,
            %database,
            through = through.0,
            payload_bytes = payload,
            total_ms = started.elapsed().as_secs_f64() * 1e3,
            phases = %format_args!("{{{phases}}}"),
            "Checkpoint timing",
        );
    }
}
