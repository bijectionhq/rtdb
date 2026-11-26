use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_columnar::{MemoryReservation, estimate_read_value_bytes, read_value};
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows, CatalogRuntime, CatalogUser, CatalogUsers};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions};
use rtdb_types::{Anomaly, Result, TableRef, Value};
use tokio_util::sync::CancellationToken;

use crate::{DatabaseCatalog, database::DatabaseFactory, statement::prepare_query};

/// What a query's catalog runtime reads from its node, shared by every query on the node.
pub(crate) struct RuntimeProviders {
    pub users: Arc<dyn CatalogUsers>,
    pub metrics: Arc<dyn CatalogMetrics>,
    pub catalog: Arc<DatabaseCatalog>,
    pub factory: Arc<DatabaseFactory>,
    /// The node's own cancellation.
    pub owner: CancellationToken,
}
pub(crate) struct NodeCatalogRuntime { node: Arc<RuntimeProviders>, cancellation: CancellationToken }
impl NodeCatalogRuntime {
    pub fn new(node: Arc<RuntimeProviders>, cancellation: CancellationToken) -> Self { Self { node, cancellation } }
}
impl CatalogUsers for NodeCatalogRuntime {
    fn users(&self) -> Result<CatalogRows<CatalogUser>> { self.node.users.users() }
    fn is_superuser(&self, user: &str) -> bool { self.node.users.is_superuser(user) }
}
#[async_trait]
impl CatalogMetrics for NodeCatalogRuntime {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { self.node.metrics.metrics() }
    async fn sample(&self) -> Result<CatalogRows<CatalogMetric>> { self.node.metrics.sample().await }
}
#[async_trait]
impl CatalogRuntime for NodeCatalogRuntime {
    async fn open_inspection(&self, database: &str, request: rtdb_plan::InspectionRequest) -> Result<Box<dyn rtdb_plan::CatalogInspection>> {
        crate::inspection::open(self.node.catalog.database(database)?, request, Arc::clone(&self.node.factory.memory)).await
    }
    async fn memberships(&self) -> Result<CatalogRows<(String, String)>> {
        let empty = || Ok(CatalogRows::new(Vec::new(), self.node.factory.memory.reserve(0)?));
        let Ok(database) = self.node.catalog.database("xtdb") else { return empty(); };
        let lease = self.node.factory.catalog_read_lease(&database).await?;
        let snapshot = database.partition.snapshot();
        crate::read_currency::require_current(database.name(), lease.as_deref())?;
        if !snapshot.contains_table(&TableRef::new("xt", "role_membership")) { return empty(); }
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), DatabaseSnapshot { partitions: vec![snapshot], storage: Arc::clone(&database.storage) })]), false)?;
        let catalog = match lease { Some(lease) => catalog.with_read_lease(lease), None => catalog };
        let context = QueryContext::with_owner(catalog, Arc::clone(&self.node.factory.memory), self.node.factory.clock.now(), "UTC", QueryOptions::default(), &self.cancellation, &self.node.owner)?;
        let mut cursor = prepare_query("SELECT \"user\", \"role\" FROM xt.role_membership", &context, &Default::default())?
            .execute(context, Vec::new()).await?;
        let mut rows = Vec::new();
        let mut reservations = Vec::<Arc<MemoryReservation>>::new();
        while let Some(batch) = cursor.next_batch().await? {
            let schema = batch.batch().schema();
            for row in 0..batch.batch().num_rows() {
                let mut bound = std::mem::size_of::<(String, String)>() * 4 + std::mem::size_of::<Arc<MemoryReservation>>() * 4;
                for column in 0..2 { bound = bound.checked_add(estimate_read_value_bytes(schema.field(column), batch.batch().column(column).as_ref(), row)?).ok_or_else(|| Anomaly::incorrect("rtdb.node/role-allocation", "Role membership allocation exceeds addressable memory"))?; }
                let reservation = self.node.factory.memory.reserve(bound)?;
                let user = read_value(schema.field(0), batch.batch().column(0).as_ref(), row)?;
                let role = read_value(schema.field(1), batch.batch().column(1).as_ref(), row)?;
                let (Value::Utf8(user), Value::Utf8(role)) = (user, role) else { return Err(Anomaly::fault("rtdb.node/role-schema", "Role membership contains non-string values")); };
                rows.push((user, role)); reservations.push(reservation);
            }
        }
        Ok(CatalogRows::new(rows, Arc::new(reservations)))
    }
}
