//! The control protocol between the cell and a node's agent: newline-delimited JSON, one request per connection.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "op")]
pub enum Request {
    /// Become the deployment's holder, or name who is.
    Activate { name: String },
    /// Flush, release the placement, and stop running the deployment.
    Park { name: String },
    /// Answers Done while the agent is serving: the controller's liveness probe.
    Ping,
    /// A client is using the deployment through a gateway.
    Touch { name: String },
    /// Serve the deployment's reads from a log-fed replica on this node.
    Subscribe { name: String },
    /// Serve the deployment's reads as of its checkpoint, from object storage alone.
    Snapshot { name: String },
    /// Flush the deployment this node holds and wait for the block's adoption, keeping the holding.
    Flush { name: String },
    /// Publish a compaction a worker produced, as the deployment's holder; the notification is an encoded TriesAdded.
    PublishCompaction { name: String, notification: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum Response {
    /// This member holds the deployment under `epoch`; clients reach it at `address`.
    Held { epoch: u64, address: String },
    /// Another live member holds it; clients reach that one at `address`.
    Redirect { holder: String, address: String },
    /// This member serves the deployment's reads from a replica; clients reach it at `address`.
    Subscribed { address: String },
    Done,
    Published { position: i64 },
    Refused { code: String, message: String },
}

impl Response {
    pub fn refused(error: &Anomaly) -> Self { Self::Refused { code: error.code.to_string(), message: error.message.clone().unwrap_or_default() } }
}

#[async_trait]
pub trait Handler: Send + Sync + 'static {
    async fn handle(&self, request: Request) -> Response;
}

pub async fn serve(listener: TcpListener, handler: Arc<dyn Handler>, cancellation: CancellationToken) -> Result<()> {
    loop {
        let (stream, _) = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            accepted = listener.accept() => accepted.map_err(io)?,
        };
        let handler = Arc::clone(&handler);
        tokio::spawn(async move {
            let (read, mut write) = stream.into_split();
            let mut line = String::new();
            if BufReader::new(read).read_line(&mut line).await.is_err() { return; }
            let response = match serde_json::from_str::<Request>(&line) {
                Ok(request) => handler.handle(request).await,
                Err(error) => Response::Refused { code: "rtdb.multi/control-request".into(), message: error.to_string() },
            };
            let mut bytes = serde_json::to_vec(&response).expect("a response serializes");
            bytes.push(b'\n');
            let _ = write.write_all(&bytes).await;
        });
    }
}

pub async fn call(address: &str, request: &Request, timeout: Duration) -> Result<Response> {
    let exchange = async {
        let stream = TcpStream::connect(address).await.map_err(io)?;
        let (read, mut write) = stream.into_split();
        let mut bytes = serde_json::to_vec(request).expect("a request serializes");
        bytes.push(b'\n');
        write.write_all(&bytes).await.map_err(io)?;
        let mut line = String::new();
        BufReader::new(read).read_line(&mut line).await.map_err(io)?;
        serde_json::from_str::<Response>(&line).map_err(|error| Anomaly::fault("rtdb.multi/control-response", error.to_string()))
    };
    tokio::time::timeout(timeout, exchange).await
        .map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/control-timeout", format!("No answer from {address}")))?
}

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/control-io", error.to_string()) }
