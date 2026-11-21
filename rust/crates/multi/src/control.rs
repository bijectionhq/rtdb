//! The control protocol between the cell and a node's agent: newline-delimited JSON, one request per connection.
//! In a cell with a secret, every request carries a grant signed with it, and a node refuses one that does not.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    PublishCompaction { name: String, notification: Vec<u8>, grant: String },
    /// Read a fragment of another node's scan of the deployment, at the fragment's basis (`RefineKeyForLargeDeployments`).
    ScanFragment { name: String, fragment: rtdb_query::ScanFragment, grant: String },
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
    /// A fragment's rows: length-prefixed Arrow IPC streams, in base64.
    Fragment { batches: String },
    Refused { code: String, message: String },
}

/// The secret a cell's members sign their control requests with; a cell without one sends them unsigned, to nodes that take them unsigned.
#[derive(Clone, Default)]
pub struct ControlKey(pub Option<Arc<crate::capability::CellSecret>>);

impl std::fmt::Debug for ControlKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str(if self.0.is_some() { "ControlKey(signed)" } else { "ControlKey(unsigned)" }) }
}

const CONTROL_SCOPE: &str = "~control";

/// The longest request a node reads.
const MAXIMUM_REQUEST_BYTES: u64 = 1 << 20;
/// The longest response a caller reads: a fragment's rows, in base64, are the largest.
pub const MAXIMUM_RESPONSE_BYTES: u64 = 96 << 20;

impl ControlKey {
    fn grant(&self) -> Option<String> { self.0.as_ref().map(|secret| secret.mint(CONTROL_SCOPE, "~cell-member", false, Duration::from_secs(30))) }
    fn admit(&self, grant: Option<&str>) -> Result<()> {
        let Some(secret) = &self.0 else { return Ok(()) };
        let grant = grant.ok_or_else(|| Anomaly::new(AnomalyCategory::Forbidden, "rtdb.multi/control-unsigned", "This cell's control requests must be signed with its secret"))?;
        use rtdb_pgwire::CapabilityVerifier;
        secret.verify(grant, CONTROL_SCOPE).map(|_| ())
    }
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    /// Named apart from any field of a request, which share its JSON object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    control_grant: Option<String>,
    #[serde(flatten)]
    request: Request,
}

impl Response {
    pub fn refused(error: &Anomaly) -> Self { Self::Refused { code: error.code.to_string(), message: error.message.clone().unwrap_or_default() } }
}

#[async_trait]
pub trait Handler: Send + Sync + 'static {
    async fn handle(&self, request: Request) -> Response;
}

pub async fn serve(listener: TcpListener, handler: Arc<dyn Handler>, key: ControlKey, cancellation: CancellationToken) -> Result<()> {
    loop {
        let (stream, _) = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            accepted = listener.accept() => accepted.map_err(io)?,
        };
        let (handler, key) = (Arc::clone(&handler), key.clone());
        tokio::spawn(async move {
            let (read, mut write) = stream.into_split();
            let mut line = String::new();
            if BufReader::new(read.take(MAXIMUM_REQUEST_BYTES)).read_line(&mut line).await.is_err() { return; }
            let response = match serde_json::from_str::<Envelope>(&line) {
                Ok(envelope) => match key.admit(envelope.control_grant.as_deref()) {
                    Ok(()) => handler.handle(envelope.request).await,
                    Err(error) => Response::refused(&error),
                },
                Err(error) => Response::Refused { code: "rtdb.multi/control-request".into(), message: error.to_string() },
            };
            let mut bytes = serde_json::to_vec(&response).expect("a response serializes");
            bytes.push(b'\n');
            let _ = write.write_all(&bytes).await;
        });
    }
}

pub async fn call(address: &str, request: &Request, timeout: Duration, key: &ControlKey) -> Result<Response> {
    let exchange = async {
        let stream = TcpStream::connect(address).await.map_err(io)?;
        let (read, mut write) = stream.into_split();
        let mut bytes = serde_json::to_vec(&Envelope { control_grant: key.grant(), request: request.clone() }).expect("a request serializes");
        bytes.push(b'\n');
        write.write_all(&bytes).await.map_err(io)?;
        let mut line = String::new();
        BufReader::new(read.take(MAXIMUM_RESPONSE_BYTES)).read_line(&mut line).await.map_err(io)?;
        if !line.ends_with('\n') { return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/control-response-size", format!("{address} answered with more than the largest response a caller reads"))); }
        serde_json::from_str::<Response>(&line).map_err(|error| Anomaly::fault("rtdb.multi/control-response", error.to_string()))
    };
    tokio::time::timeout(timeout, exchange).await
        .map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/control-timeout", format!("No answer from {address}")))?
}

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/control-io", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;

    struct Answers;
    #[async_trait]
    impl Handler for Answers { async fn handle(&self, _: Request) -> Response { Response::Done } }

    #[tokio::test]
    async fn a_node_reads_no_more_of_a_request_than_the_largest_it_takes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, Arc::new(Answers), ControlKey::default(), CancellationToken::new()));
        let mut stream = TcpStream::connect(address).await.unwrap();
        let _ = stream.write_all(&vec![b'x'; 2 << 20]).await;
        let mut answer = String::new();
        BufReader::new(stream).read_line(&mut answer).await.unwrap();
        assert!(matches!(serde_json::from_str::<Response>(&answer).unwrap(), Response::Refused { .. }), "an oversized request is refused, not read whole: {answer}");
        assert!(matches!(call(&address.to_string(), &Request::Ping, Duration::from_secs(5), &ControlKey::default()).await.unwrap(), Response::Done));
    }
}
