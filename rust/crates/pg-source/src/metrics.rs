use std::sync::Arc;
use rtdb_types::Result;
use crate::SourceObserver;

pub trait SourceAssignmentMetrics: Send {}
pub trait SourceMetrics: Send + Sync {
    /// The guard ends this assignment's gauges; the source's counters and summary survive it.
    fn assigned(&self, observer: SourceObserver) -> Result<Box<dyn SourceAssignmentMetrics>>;
    /// Called only after admitted apply, with a millisecond timestamp difference expressed in seconds.
    /// Callbacks must not panic, retain transactions, or spawn independent work.
    fn applied(&self, events: usize, lag_seconds: Option<f64>);
}
pub trait SourceMetricsFactory: Send + Sync {
    /// The returned registration belongs to the database source and is released after its final assignment joins.
    fn register_source(&self, database: &str, slot: &str) -> Result<Arc<dyn SourceMetrics>>;
}
