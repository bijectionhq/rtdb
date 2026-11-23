use parking_lot::Mutex;
use rtdb_storage::ReadLease;
use rtdb_types::{Anomaly, AnomalyCategory, ReplicaPosition, Result};

mod step;

/// When, in monotonic microseconds, this database's catalog was last confirmed to include every replica record then published.
/// Passive: its callers supply the clock reading, the published tip and the applied position.
#[derive(Debug, Default)]
pub(crate) struct ReadCurrency(Mutex<Currency>);

#[derive(Debug, Default)]
struct Currency {
    anchor: Option<i64>,
    /// A tip read at an instant, confirmed once the applied position reaches it.
    pending: Option<(i64, i64)>,
}

impl Currency {
    fn confirm(&mut self, installed: Option<ReplicaPosition>) {
        (self.anchor, self.pending) = step::confirm(self.anchor, self.pending, installed.map(|position| position.0));
    }
}

impl ReadCurrency {
    /// Whether a fresh tip observation is due: none is pending and the anchor is absent or older than `refresh` micros.
    pub fn needs_observation(&self, installed: Option<ReplicaPosition>, now: i64, refresh: i64) -> bool {
        let mut currency = self.0.lock();
        currency.confirm(installed);
        currency.pending.is_none() && currency.anchor.is_none_or(|anchor| now.saturating_sub(anchor) >= refresh)
    }
    /// Records the tip published no earlier than `observed_at`; the caller reads the clock before the tip.
    pub fn observe(&self, observed_at: i64, tip: ReplicaPosition) {
        let mut currency = self.0.lock();
        currency.pending = step::observe(currency.pending, observed_at, tip.0);
    }
    /// Confirms a pending observation once the catalog has applied its tip, and returns the anchor.
    /// The caller observes installed progress before capturing its catalog.
    pub fn anchor(&self, installed: Option<ReplicaPosition>) -> Option<i64> {
        let mut currency = self.0.lock();
        currency.confirm(installed);
        currency.anchor
    }
    pub fn pending_tip(&self) -> Option<ReplicaPosition> { self.0.lock().pending.map(|(_, tip)| ReplicaPosition(tip)) }
}

/// Rejects a replicated query before exposing its snapshot without a current lease.
pub(crate) fn require_current(database: &str, lease: Option<&ReadLease>) -> Result<()> {
    if let Some(lease) = lease {
        lease.check().map_err(|_| {
            Anomaly::new(AnomalyCategory::Unavailable, "rtdb.node/catalog-not-current",
                "This node could not confirm the catalogs required by the query")
                .with_context("db-name", database.into())
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position(offset: i64) -> ReplicaPosition { ReplicaPosition(offset) }

    #[test]
    fn an_observation_anchors_only_after_its_tip_is_applied() {
        let currency = ReadCurrency::default();
        assert!(currency.needs_observation(None, 10, 5));
        currency.observe(10, position(4));
        assert!(!currency.needs_observation(None, 100, 5));
        assert_eq!(currency.anchor(None), None);
        assert_eq!(currency.anchor(Some(position(3))), None);
        assert_eq!(currency.anchor(Some(position(4))), Some(10));
        assert!(!currency.needs_observation(Some(position(4)), 14, 5));
        assert!(currency.needs_observation(Some(position(4)), 15, 5));
    }

    #[test]
    fn an_applied_old_observation_leaves_the_same_request_eligible_to_refresh() {
        let currency = ReadCurrency::default();
        currency.observe(10, position(4));
        assert!(!currency.needs_observation(Some(position(3)), 100, 5));
        assert!(currency.needs_observation(Some(position(4)), 100, 5));
        currency.observe(100, position(5));
        assert_eq!(currency.anchor(Some(position(4))), Some(10));
        assert_eq!(currency.anchor(Some(position(5))), Some(100));
    }

    #[test]
    fn an_out_of_order_tip_cannot_replace_the_pending_observation() {
        let currency = ReadCurrency::default();
        currency.observe(10, position(5));
        currency.observe(12, position(4));
        currency.observe(13, position(5));
        assert_eq!(currency.pending_tip(), Some(position(5)));
        assert_eq!(currency.anchor(Some(position(4))), None);
        assert_eq!(currency.anchor(Some(position(5))), Some(10));
    }

    #[test]
    fn query_admission_requires_an_unexpired_anchor_for_a_replicated_snapshot() {
        use std::sync::Arc;

        require_current("offline", None).unwrap();
        let current = ReadLease::new(Some(11), Arc::new(|| 10));
        require_current("shop", Some(&current)).unwrap();

        for deadline in [None, Some(9), Some(10)] {
            let lease = ReadLease::new(deadline, Arc::new(|| 10));
            let error = require_current("shop", Some(&lease)).unwrap_err();
            assert_eq!(error.category, AnomalyCategory::Unavailable);
            assert_eq!(error.code, "rtdb.node/catalog-not-current");
        }
    }

    #[test]
    fn the_anchor_never_moves_back() {
        let currency = ReadCurrency::default();
        currency.observe(20, position(2));
        assert_eq!(currency.anchor(Some(position(2))), Some(20));
        currency.observe(15, position(3));
        assert_eq!(currency.anchor(Some(position(3))), Some(20));
    }
}
