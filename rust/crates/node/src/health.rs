use std::collections::BTreeMap;

use rtdb_types::{ReplicaPosition, Result};

use crate::Node;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HealthResponse { pub status: u16, pub headers: BTreeMap<String, String>, pub body: String }
impl HealthResponse {
    fn text(status: u16, body: impl Into<String>) -> Self { Self { status, headers: BTreeMap::new(), body: body.into() } }
}

#[derive(Clone)]
pub struct HealthChecks { node: Node, startup_targets: BTreeMap<String, ReplicaPosition> }
impl HealthChecks {
    /// Captures startup targets once; later checks use current membership and criticality.
    pub fn new(node: Node) -> Result<Self> {
        let mut startup_targets = BTreeMap::new();
        for (name, database) in node.catalog().open_databases() {
            if let Some(target) = database.startup_replica_target()? { startup_targets.insert(name, target); }
        }
        Ok(Self { node, startup_targets })
    }
    pub fn started(&self) -> HealthResponse {
        let mut catching_up = Vec::new();
        for (name, target) in &self.startup_targets {
            let Ok(database) = self.node.catalog().database(name) else { continue; };
            let current = database.status().replica_position;
            if database.is_critical() && current < *target { catching_up.push(format!("{name} (at: [{}], targets: [{}])", current.0, target.0)); }
        }
        if catching_up.is_empty() { HealthResponse::text(200, "Started.") }
        else {
            let mut response = HealthResponse::text(503, format!("Catching up: {}", catching_up.join(", ")));
            response.headers.insert("X-XTDB-Databases-Catching-Up".into(), catching_up.len().to_string());
            response
        }
    }
    pub async fn alive(&self) -> Result<HealthResponse> {
        let databases = self.node.catalog().open_databases();
        let mut unhealthy = 0usize;
        let mut critical = Vec::new();
        for (name, database) in &databases {
            let error = database.progress().fault();
            let lag = database.block_lag().await?;
            if error.is_some() || lag > 5 {
                unhealthy += 1;
                if database.is_critical() {
                    critical.push(match error { Some(error) => format!("{name} (ingestion error: {error})"), None => format!("{name} (block lag: {lag})") });
                }
            }
        }
        let mut response = if critical.is_empty() { HealthResponse::text(200, "Alive.") }
            else { HealthResponse::text(503, format!("Unhealthy: {}", critical.join(", "))) };
        response.headers.insert("X-XTDB-Databases-Checked".into(), databases.len().to_string());
        response.headers.insert("X-XTDB-Databases-Unhealthy".into(), unhealthy.to_string());
        Ok(response)
    }
    pub fn ready(&self) -> HealthResponse { HealthResponse::text(200, "Ready.") }
    pub async fn finish_block(&self, database: Option<&str>) -> HealthResponse {
        let databases = match database {
            None => self.node.catalog().open_databases().into_values().collect::<Vec<_>>(),
            Some(name) => match self.node.catalog().database(name) {
                Ok(database) => vec![database],
                Err(error) => return HealthResponse::text(500, format!("Exception when calling endpoint - {error}")),
            },
        };
        for database in &databases {
            if let Err(error) = database.flush().await { return HealthResponse::text(500, format!("Error sending flush block message: {error}")); }
        }
        HealthResponse::text(200, format!("Block flush message sent to {} database(s).", databases.len()))
    }
}
