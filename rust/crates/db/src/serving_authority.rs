use std::{fmt, sync::Arc, time::Duration};

use journal_contract::BoxFuture;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio::time::Instant;

/// The host commissions a database incarnation against its publishing authority.
pub trait ServingAuthority: Send + Sync {
    fn acquire(&self) -> BoxFuture<'_, Result<ServingPermit>>;
}

#[derive(Clone)]
pub struct ServingPermit {
    deadline: Instant,
    expires_micros: i64,
    upper_clock_micros: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl ServingPermit {
    pub fn until(
        expires_micros: i64,
        upper_clock_micros: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<Self> {
        let observed = Instant::now();
        let remaining = expires_micros
            .checked_sub(upper_clock_micros())
            .and_then(|remaining| u64::try_from(remaining).ok())
            .ok_or_else(expired)?;
        let deadline = observed
            .checked_add(Duration::from_micros(remaining))
            .ok_or_else(expired)?;
        let permit = Self {
            deadline,
            expires_micros,
            upper_clock_micros,
        };
        permit.check()?;
        Ok(permit)
    }

    pub fn check(&self) -> Result<()> {
        if Instant::now() < self.deadline && (self.upper_clock_micros)() < self.expires_micros {
            Ok(())
        } else {
            Err(expired())
        }
    }
}

impl fmt::Debug for ServingPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServingPermit")
            .field("expires_micros", &self.expires_micros)
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
