use parking_lot::Mutex;
use rtdb_types::ReplicaPosition;

/// When, in monotonic microseconds, this database's catalog was last confirmed to include every replica record then published.
/// Passive: its callers supply the clock reading, the published tip and the applied position.
#[derive(Debug, Default)]
pub(crate) struct ReadCurrency(Mutex<Currency>);

#[derive(Debug, Default)]
struct Currency {
    anchor: Option<i64>,
    /// A tip read at an instant, confirmed once the applied position reaches it.
    pending: Option<(i64, ReplicaPosition)>,
}

impl ReadCurrency {
    /// Whether a fresh tip observation is due: none is pending and the anchor is absent or older than `refresh` micros.
    pub fn needs_observation(&self, now: i64, refresh: i64) -> bool {
        let currency = self.0.lock();
        currency.pending.is_none() && currency.anchor.is_none_or(|anchor| now.saturating_sub(anchor) >= refresh)
    }
    /// Records the tip published no earlier than `observed_at`; the caller reads the clock before the tip.
    pub fn observe(&self, observed_at: i64, tip: ReplicaPosition) {
        let mut currency = self.0.lock();
        if currency.pending.is_none_or(|(_, pending)| tip > pending) { currency.pending = Some((observed_at, tip)); }
    }
    /// Confirms a pending observation once the catalog has applied its tip, and returns the anchor.
    /// The caller reads `applied` after the catalog it will capture has published it.
    pub fn anchor(&self, applied: ReplicaPosition) -> Option<i64> {
        let mut currency = self.0.lock();
        if let Some((observed_at, tip)) = currency.pending {
            if applied >= tip {
                currency.pending = None;
                currency.anchor = Some(currency.anchor.map_or(observed_at, |anchor| anchor.max(observed_at)));
            }
        }
        currency.anchor
    }
    pub fn pending_tip(&self) -> Option<ReplicaPosition> { self.0.lock().pending.map(|(_, tip)| tip) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position(offset: i64) -> ReplicaPosition { ReplicaPosition(offset) }

    #[test]
    fn an_observation_anchors_only_after_its_tip_is_applied() {
        let currency = ReadCurrency::default();
        assert!(currency.needs_observation(10, 5));
        currency.observe(10, position(4));
        assert!(!currency.needs_observation(100, 5));
        assert_eq!(currency.anchor(position(3)), None);
        assert_eq!(currency.anchor(position(4)), Some(10));
        assert!(!currency.needs_observation(14, 5));
        assert!(currency.needs_observation(15, 5));
    }

    #[test]
    fn the_anchor_never_moves_back() {
        let currency = ReadCurrency::default();
        currency.observe(20, position(2));
        assert_eq!(currency.anchor(position(2)), Some(20));
        currency.observe(15, position(3));
        assert_eq!(currency.anchor(position(3)), Some(20));
    }
}
