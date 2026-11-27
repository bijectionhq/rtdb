use std::{fmt, sync::Arc, time::Duration};

use journal_contract::BoxFuture;
use rtdb_types::{Anomaly, AnomalyCategory, Result};

/// The host commissions a database incarnation against its publishing authority.
pub trait ServingAuthority: Send + Sync {
    fn acquire(&self) -> BoxFuture<'_, Result<ServingPermit>>;
}

#[derive(Clone)]
pub struct ServingPermit {
    requested_at: Duration,
    window: Duration,
    elapsed_clock: Arc<dyn Fn() -> Option<Duration> + Send + Sync>,
}

impl ServingPermit {
    pub fn bounded(
        requested_at: Duration,
        window: Duration,
        elapsed_clock: Arc<dyn Fn() -> Option<Duration> + Send + Sync>,
    ) -> Result<Self> {
        let permit = Self {
            requested_at,
            window,
            elapsed_clock,
        };
        permit.check()?;
        Ok(permit)
    }

    pub fn check(&self) -> Result<()> {
        self.remaining().map(drop)
    }

    pub fn remaining(&self) -> Result<Duration> {
        (self.elapsed_clock)()
            .and_then(|now| now.checked_sub(self.requested_at))
            .and_then(|elapsed| self.window.checked_sub(elapsed))
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(expired)
    }
}

impl fmt::Debug for ServingPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServingPermit")
            .field("requested_at", &self.requested_at)
            .field("window", &self.window)
            .finish_non_exhaustive()
    }
}

fn expired() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/activation-fenced",
        "The database serving grant expired",
    )
}
