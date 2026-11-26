//! Phase timings of a checkpoint, printed when `RTDB_CHECKPOINT_TIMING` is set.

use std::{sync::OnceLock, time::Instant};

use journal_contract::Lsn;
use uuid::Uuid;

fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("RTDB_CHECKPOINT_TIMING").is_some())
}

pub(super) struct Phases {
    started: Option<Instant>,
    last: Option<Instant>,
    phases: Vec<(&'static str, f64)>,
}

impl Phases {
    pub(super) fn start() -> Self {
        let now = enabled().then(Instant::now);
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
        eprintln!(
            "{{\"checkpoint_timing\":{{\"kind\":\"{kind}\",\"database\":\"{database}\",\"through\":{},\"payload_bytes\":{payload},\"total_ms\":{:.3},{phases}}}}}",
            through.0, started.elapsed().as_secs_f64() * 1e3,
        );
    }
}
