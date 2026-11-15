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

#[derive(Clone, Debug, PartialEq)]
pub enum CatalogMetric {
    Timer { name: String, tags: BTreeMap<String, String>, count: i64, mean_ns: Option<i64>, max_ns: Option<i64>, p75_ns: Option<i64>, p95_ns: Option<i64>, p99_ns: Option<i64>, p999_ns: Option<i64> },
    Gauge { name: String, tags: BTreeMap<String, String>, value: f64 },
    Counter { name: String, tags: BTreeMap<String, String>, count: f64 },
}
impl CatalogMetric {
    pub fn name(&self) -> &str { match self { Self::Timer { name, .. } | Self::Gauge { name, .. } | Self::Counter { name, .. } => name } }
    pub fn tags(&self) -> &BTreeMap<String, String> { match self { Self::Timer { tags, .. } | Self::Gauge { tags, .. } | Self::Counter { tags, .. } => tags } }
}

/// Captures configured users without exposing credentials.
pub trait CatalogUsers: Send + Sync {
    fn users(&self) -> Result<CatalogRows<CatalogUser>>;
    fn is_superuser(&self, user: &str) -> bool;
}
/// Captures one coherent telemetry observation, retaining its allocation ownership.
pub trait CatalogMetrics: Send + Sync { fn metrics(&self) -> Result<CatalogRows<CatalogMetric>>; }

#[async_trait]
pub trait CatalogRuntime: CatalogUsers + CatalogMetrics {
    /// Memberships use the current primary-database watermark, independently of the outer query basis.
    /// All children and resources complete within this future, which query cancellation may drop.
    async fn memberships(&self) -> Result<CatalogRows<(String, String)>>;
}
