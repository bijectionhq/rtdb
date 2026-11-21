//! Cells (contract `CellMap`): the only global state, which cell each deployment lives in and where each cell's gateway listens.
//! A row changes only when a deployment is created or moves cells.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

#[cfg(feature = "dynamodb")]
mod dynamo;
#[cfg(feature = "dynamodb")]
pub use dynamo::DynamoCellMap;

#[async_trait]
pub trait CellMap: Send + Sync {
    async fn register_cell(&self, cell: &str, gateway: &str) -> Result<()>;
    async fn gateway(&self, cell: &str) -> Result<Option<String>>;
    async fn cells(&self) -> Result<BTreeMap<String, String>>;
    async fn cell_of(&self, deployment: &str) -> Result<Option<String>>;
    /// Assigns a deployment to a cell, only if its current cell is `expected` (absent for a new deployment).
    async fn assign(&self, deployment: &str, cell: &str, expected: Option<&str>) -> Result<bool>;
}

#[derive(Default)]
pub struct MemoryCellMap { state: Mutex<(BTreeMap<String, String>, BTreeMap<String, String>)> }

impl MemoryCellMap { pub fn new() -> Arc<Self> { Arc::new(Self::default()) } }

#[async_trait]
impl CellMap for MemoryCellMap {
    async fn register_cell(&self, cell: &str, gateway: &str) -> Result<()> { self.state.lock().0.insert(cell.into(), gateway.into()); Ok(()) }
    async fn gateway(&self, cell: &str) -> Result<Option<String>> { Ok(self.state.lock().0.get(cell).cloned()) }
    async fn cells(&self) -> Result<BTreeMap<String, String>> { Ok(self.state.lock().0.clone()) }
    async fn cell_of(&self, deployment: &str) -> Result<Option<String>> { Ok(self.state.lock().1.get(deployment).cloned()) }
    async fn assign(&self, deployment: &str, cell: &str, expected: Option<&str>) -> Result<bool> {
        let mut state = self.state.lock();
        if state.1.get(deployment).map(String::as_str) != expected { return Ok(false); }
        state.1.insert(deployment.into(), cell.into());
        Ok(true)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "op")]
enum Request {
    RegisterCell { cell: String, gateway: String },
    Gateway { cell: String },
    Cells,
    CellOf { deployment: String },
    Assign { deployment: String, cell: String, expected: Option<String> },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
enum Response { Done, Value { value: Option<String> }, Cells { cells: BTreeMap<String, String> }, Assigned { applied: bool }, Failed { code: String, message: String } }

/// Serves a cell map over TCP: newline-delimited JSON, one request per connection.
pub async fn serve(listener: TcpListener, map: Arc<dyn CellMap>, cancellation: CancellationToken) -> Result<()> {
    loop {
        let (stream, _) = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            accepted = listener.accept() => accepted.map_err(io)?,
        };
        let map = Arc::clone(&map);
        tokio::spawn(async move {
            let (read, mut write) = stream.into_split();
            let mut line = String::new();
            if BufReader::new(read).read_line(&mut line).await.is_err() { return; }
            let response = match serde_json::from_str::<Request>(&line) {
                Ok(request) => answer(map.as_ref(), request).await.unwrap_or_else(|error| Response::Failed { code: error.code.to_string(), message: error.message.unwrap_or_default() }),
                Err(error) => Response::Failed { code: "rtdb.multi/cells-request".into(), message: error.to_string() },
            };
            let mut bytes = serde_json::to_vec(&response).expect("a response serializes");
            bytes.push(b'\n');
            let _ = write.write_all(&bytes).await;
        });
    }
}

async fn answer(map: &dyn CellMap, request: Request) -> Result<Response> {
    Ok(match request {
        Request::RegisterCell { cell, gateway } => { map.register_cell(&cell, &gateway).await?; Response::Done }
        Request::Gateway { cell } => Response::Value { value: map.gateway(&cell).await? },
        Request::Cells => Response::Cells { cells: map.cells().await? },
        Request::CellOf { deployment } => Response::Value { value: map.cell_of(&deployment).await? },
        Request::Assign { deployment, cell, expected } => Response::Assigned { applied: map.assign(&deployment, &cell, expected.as_deref()).await? },
    })
}

pub struct RemoteCellMap { address: String }

impl RemoteCellMap {
    pub fn new(address: impl Into<String>) -> Arc<Self> { Arc::new(Self { address: address.into() }) }

    async fn call(&self, request: Request) -> Result<Response> {
        let exchange = async {
            let stream = TcpStream::connect(&self.address).await.map_err(io)?;
            let (read, mut write) = stream.into_split();
            let mut bytes = serde_json::to_vec(&request).expect("a request serializes");
            bytes.push(b'\n');
            write.write_all(&bytes).await.map_err(io)?;
            let mut line = String::new();
            BufReader::new(read).read_line(&mut line).await.map_err(io)?;
            match serde_json::from_str::<Response>(&line).map_err(|error| Anomaly::fault("rtdb.multi/cells-response", error.to_string()))? {
                Response::Failed { code, message } => Err(Anomaly::new(AnomalyCategory::Unavailable, code, message)),
                response => Ok(response),
            }
        };
        tokio::time::timeout(Duration::from_secs(10), exchange).await.map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/cells-timeout", "The cell map did not answer"))?
    }
}

#[async_trait]
impl CellMap for RemoteCellMap {
    async fn register_cell(&self, cell: &str, gateway: &str) -> Result<()> { self.call(Request::RegisterCell { cell: cell.into(), gateway: gateway.into() }).await.map(|_| ()) }
    async fn gateway(&self, cell: &str) -> Result<Option<String>> {
        match self.call(Request::Gateway { cell: cell.into() }).await? { Response::Value { value } => Ok(value), _ => Err(unexpected()) }
    }
    async fn cells(&self) -> Result<BTreeMap<String, String>> {
        match self.call(Request::Cells).await? { Response::Cells { cells } => Ok(cells), _ => Err(unexpected()) }
    }
    async fn cell_of(&self, deployment: &str) -> Result<Option<String>> {
        match self.call(Request::CellOf { deployment: deployment.into() }).await? { Response::Value { value } => Ok(value), _ => Err(unexpected()) }
    }
    async fn assign(&self, deployment: &str, cell: &str, expected: Option<&str>) -> Result<bool> {
        match self.call(Request::Assign { deployment: deployment.into(), cell: cell.into(), expected: expected.map(str::to_owned) }).await? { Response::Assigned { applied } => Ok(applied), _ => Err(unexpected()) }
    }
}

fn unexpected() -> Anomaly { Anomaly::fault("rtdb.multi/cells-response", "The cell map answered another request") }
fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/cells-io", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_deployment_moves_cells_only_from_the_cell_it_is_in() {
        let map: Arc<dyn CellMap> = MemoryCellMap::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        tokio::spawn(serve(listener, map, CancellationToken::new()));
        contract(RemoteCellMap::new(address).as_ref()).await;
    }

    pub(super) async fn contract(map: &dyn CellMap) {
        assert_eq!(map.cell_of("eu-1").await.unwrap(), None);
        map.register_cell("eu-1", "127.0.0.1:1").await.unwrap();
        assert_eq!(map.cells().await.unwrap().get("eu-1").map(String::as_str), Some("127.0.0.1:1"));
        assert!(map.assign("eu-1", "eu-1", None).await.unwrap());
        assert!(!map.assign("eu-1", "eu-2", None).await.unwrap());
        assert!(!map.assign("eu-1", "eu-2", Some("us-1")).await.unwrap());
        assert!(map.assign("eu-1", "eu-2", Some("eu-1")).await.unwrap());
        assert_eq!(map.cell_of("eu-1").await.unwrap().as_deref(), Some("eu-2"));
        assert_eq!(map.gateway("eu-1").await.unwrap().as_deref(), Some("127.0.0.1:1"));
    }
}
