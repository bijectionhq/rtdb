use std::sync::Arc;

use rtdb_query::HostedControlPoint;
use rtdb_tx::HostedControlWrite;
use rtdb_types::{Anomaly, AnomalyCategory, Result};

use super::OwnedReadSession;

impl OwnedReadSession {
    /// Read an admitted ordinary control point on this original placed cut.
    pub async fn hosted_control_point(
        &self,
        target: &HostedControlWrite,
    ) -> Result<HostedControlPoint> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        let unavailable = || {
            Anomaly::new(
                AnomalyCategory::Forbidden,
                "rtdb.hosted/control-scope",
                "Hosted controls require their original live placed hold",
            )
        };
        if Some(target.scope()) != self.witness_scope.or(self.applied_scope) {
            return Err(unavailable());
        }
        let database = self.context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else {
            return Err(unavailable());
        };
        if snapshot.tx_basis() != Some(self.present) {
            return Err(unavailable());
        }
        let point = rtdb_query::read_hosted_control_point(
            Arc::clone(&self.context),
            target.table(),
            target.id(),
            true,
        )
        .await?;
        if point
            .row
            .as_ref()
            .is_some_and(|(_, revision)| *revision > self.present.system_time)
        {
            return Err(unavailable());
        }
        self.check()?;
        Ok(point)
    }
}
