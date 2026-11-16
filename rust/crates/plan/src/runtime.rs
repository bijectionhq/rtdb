use async_trait::async_trait;
use rtdb_types::Result;
use std::collections::BTreeMap;
use std::sync::Arc;

struct CatalogAllocation<T> { rows: Vec<T>, _lease: Arc<dyn Send + Sync> }

pub struct CatalogRows<T>(Arc<CatalogAllocation<T>>);
impl<T> Clone for CatalogRows<T> { fn clone(&self) -> Self { Self(Arc::clone(&self.0)) } }
impl<T> CatalogRows<T> {
    /// The lease must retain the allocation charge for every row and its owned payloads.
    pub fn new(rows: Vec<T>, lease: Arc<dyn Send + Sync>) -> Self { Self(Arc::new(CatalogAllocation { rows, _lease: lease })) }
    pub fn rows(&self) -> &[T] { &self.0.rows }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogUser { pub name: String, pub superuser: bool }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogMetricUnit { Bytes, Seconds }

#[derive(Clone, Debug, PartialEq)]
pub enum CatalogMetric {
    /// Percentiles are nanoseconds at p75, p85, p95, p98, p99 and p999, in that order.
    Timer { name: String, tags: BTreeMap<String, String>, count: i64, total_ns: f64, percentiles_ns: [f64; 6], mean_ns: Option<i64>, max_ns: Option<i64>, p75_ns: Option<i64>, p95_ns: Option<i64>, p99_ns: Option<i64>, p999_ns: Option<i64> },
    /// Values use the meter's native unit; percentiles are p50, p95 and p99, in that order.
    DistributionSummary { name: String, tags: BTreeMap<String, String>, unit: Option<CatalogMetricUnit>, count: i64, total: f64, maximum: f64, percentiles: [f64; 3] },
    Gauge { name: String, tags: BTreeMap<String, String>, unit: Option<CatalogMetricUnit>, value: f64 },
    Counter { name: String, tags: BTreeMap<String, String>, count: f64 },
}
impl CatalogMetric {
    pub fn name(&self) -> &str { match self { Self::Timer { name, .. } | Self::DistributionSummary { name, .. } | Self::Gauge { name, .. } | Self::Counter { name, .. } => name } }
    pub fn tags(&self) -> &BTreeMap<String, String> { match self { Self::Timer { tags, .. } | Self::DistributionSummary { tags, .. } | Self::Gauge { tags, .. } | Self::Counter { tags, .. } => tags } }
}

/// Captures configured users without exposing credentials.
pub trait CatalogUsers: Send + Sync {
    fn users(&self) -> Result<CatalogRows<CatalogUser>>;
    fn is_superuser(&self, user: &str) -> bool;
}
/// Captures one coherent telemetry observation, retaining its allocation ownership.
#[async_trait]
pub trait CatalogMetrics: Send + Sync {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>>;
    /// Performs caller-owned live sampling; dropping this future cancels any in-flight provider I/O.
    async fn sample(&self) -> Result<CatalogRows<CatalogMetric>> { self.metrics() }
    /// Advances owned sixty-second accumulators; one exporter owner calls the final rollover once at shutdown.
    fn roll_step(&self, _now_ms: i64, _closing: bool) {}
    /// Returns the preceding completed step while sampling live gauges on the caller's task.
    /// Providers with cumulative meters implement their own rollover; the default accepts gauges only.
    async fn sample_step(&self) -> Result<CatalogRows<CatalogMetric>> {
        let rows = self.sample().await?;
        if rows.rows().iter().any(|row| !matches!(row, CatalogMetric::Gauge { .. })) {
            return Err(rtdb_types::Anomaly::unsupported("rtdb.metrics/missing-step-provider", "Cumulative metric provider has no step accumulation implementation"));
        }
        Ok(rows)
    }
}

#[async_trait]
pub trait CatalogRuntime: CatalogUsers + CatalogMetrics {
    /// Memberships use the current primary-database watermark, independently of the outer query basis.
    /// All children and resources complete within this future, which query cancellation may drop.
    async fn memberships(&self) -> Result<CatalogRows<(String, String)>>;
    async fn open_inspection(&self, _database: &str, _request: crate::InspectionRequest) -> Result<Box<dyn crate::CatalogInspection>> {
        Err(rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.query/catalog-inspection-unavailable", "Catalog runtime does not provide storage or log inspection"))
    }
}
