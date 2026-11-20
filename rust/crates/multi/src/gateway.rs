//! The gateway: terminates a client's pgwire startup, finds or activates the deployment's holder, and splices the connection through to it (`RouteRead`, `ActivateOnDemand`, `ReclaimOnDemand`).
//!
//! Routing never authorises: a node may refuse or redirect, and the gateway walks on.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rtdb_directory::{DeploymentRecord, Directory, MemberId, MemberRecord, MemberRole, effective_holder, now_ms};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::control::{self, Request, Response};

const SSL_REQUEST: i32 = 80877103;
const GSSENC_REQUEST: i32 = 80877104;
const CANCEL_REQUEST: i32 = 80877102;
const PROTOCOL_3: i32 = 196608;
const MAXIMUM_STARTUP_BYTES: usize = 10_000;

#[derive(Clone, Debug)]
pub struct GatewayOptions {
    pub control_timeout: Duration,
    /// How often an open connection tells its holder it is in use.
    pub touch_interval: Duration,
}

impl Default for GatewayOptions {
    fn default() -> Self { Self { control_timeout: Duration::from_secs(35), touch_interval: Duration::from_secs(10) } }
}

pub async fn serve(listener: TcpListener, directory: Arc<dyn Directory>, options: GatewayOptions, cancellation: CancellationToken) -> Result<()> {
    loop {
        let (client, _) = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            accepted = listener.accept() => accepted.map_err(io)?,
        };
        let directory = Arc::clone(&directory);
        let options = options.clone();
        let cancellation = cancellation.child_token();
        tokio::spawn(async move { let _ = connection(client, directory, options, cancellation).await; });
    }
}

struct Startup { bytes: Vec<u8>, parameters: BTreeMap<String, String> }

async fn connection(mut client: TcpStream, directory: Arc<dyn Directory>, options: GatewayOptions, cancellation: CancellationToken) -> Result<()> {
    let Some(startup) = read_startup(&mut client).await? else { return Ok(()); };
    let Some(name) = startup.parameters.get("database").or_else(|| startup.parameters.get("user")).cloned() else {
        return refuse(&mut client, "3D000", "The startup message names no database").await;
    };
    let (mut server, control) = match route(&directory, &name, &options).await {
        Ok(route) => route,
        Err(error) => return refuse(&mut client, "08006", error.message.as_deref().unwrap_or(error.code.as_ref())).await,
    };
    server.write_all(&startup.bytes).await.map_err(io)?;
    let touching = control.map(|control| {
        let name = name.clone();
        let interval = options.touch_interval;
        let timeout = options.control_timeout;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let _ = control::call(&control, &Request::Touch { name: name.clone() }, timeout).await;
            }
        })
    });
    let spliced = tokio::select! {
        _ = cancellation.cancelled() => Ok(()),
        copied = tokio::io::copy_bidirectional(&mut client, &mut server) => copied.map(|_| ()).map_err(io),
    };
    if let Some(touching) = touching { touching.abort(); }
    spliced
}

/// The open connection to the deployment's holder, and that holder's control address.
async fn route(directory: &Arc<dyn Directory>, name: &str, options: &GatewayOptions) -> Result<(TcpStream, Option<String>)> {
    let record = directory.deployment_by_name(name).await?
        .ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-deployment", format!("No deployment is named {name}")))?;
    let members: BTreeMap<MemberId, MemberRecord> = directory.members().await?.into_iter().map(|member| (member.id.clone(), member)).collect();
    let mut last = None;
    for candidate in candidates(&record, &members) {
        let Some(control_address) = &candidate.control_address else { continue; };
        match control::call(control_address, &Request::Activate { name: name.to_owned() }, options.control_timeout).await {
            Ok(Response::Held { address, .. }) => return Ok((connect(&address).await?, Some(control_address.clone()))),
            Ok(Response::Redirect { holder, address }) => {
                let control = members.get(&MemberId(holder)).and_then(|member| member.control_address.clone());
                return Ok((connect(&address).await?, control));
            }
            Ok(Response::Refused { code, message }) => last = Some(Anomaly::new(AnomalyCategory::Unavailable, code, message)),
            Ok(Response::Done) => {}
            Err(error) => last = Some(error),
        }
    }
    Err(last.unwrap_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/no-node", "No node can serve this deployment")))
}

/// The holder that counts, then every node that may take a placement, in the deployment's affinity order.
fn candidates<'a>(record: &DeploymentRecord, members: &'a BTreeMap<MemberId, MemberRecord>) -> Vec<&'a MemberRecord> {
    let now = now_ms();
    let holder = effective_holder(record, members, now).and_then(|holder| members.get(holder));
    let nodes: Vec<&MemberRecord> = members.values().filter(|member| member.role == MemberRole::Node && member.may_receive_placements(now)).collect();
    let ranked = rtdb_affinity::rank(&record.storage_identity(), &nodes, |member| member.id.as_str());
    holder.into_iter().chain(ranked.into_iter().copied().filter(|member| holder.is_none_or(|holder| holder.id != member.id))).collect()
}

async fn connect(address: &str) -> Result<TcpStream> {
    let stream = TcpStream::connect(address).await.map_err(io)?;
    stream.set_nodelay(true).map_err(io)?;
    Ok(stream)
}

/// Answers SSL and GSS encryption requests with "not supported" until the client sends a startup message.
/// Returns None for a cancel request, which this gateway does not forward.
async fn read_startup(client: &mut TcpStream) -> Result<Option<Startup>> {
    loop {
        let length = client.read_i32().await.map_err(io)?;
        let length = usize::try_from(length).ok().filter(|length| (8..=MAXIMUM_STARTUP_BYTES).contains(length))
            .ok_or_else(|| Anomaly::incorrect("rtdb.multi/startup", "Startup message length is out of range"))?;
        let mut body = vec![0; length - 4];
        client.read_exact(&mut body).await.map_err(io)?;
        let code = i32::from_be_bytes(body[..4].try_into().expect("four bytes"));
        match code {
            SSL_REQUEST | GSSENC_REQUEST => { client.write_all(b"N").await.map_err(io)?; }
            CANCEL_REQUEST => return Ok(None),
            PROTOCOL_3 => {
                let mut parameters = BTreeMap::new();
                let mut fields = body[4..].split(|byte| *byte == 0).map(|field| String::from_utf8_lossy(field).into_owned());
                while let (Some(key), Some(value)) = (fields.next(), fields.next()) {
                    if key.is_empty() { break; }
                    parameters.insert(key, value);
                }
                let mut bytes = (length as i32).to_be_bytes().to_vec();
                bytes.extend_from_slice(&body);
                return Ok(Some(Startup { bytes, parameters }));
            }
            _ => return Err(Anomaly::incorrect("rtdb.multi/startup", format!("Unsupported startup code {code}"))),
        }
    }
}

async fn refuse(client: &mut TcpStream, sqlstate: &str, message: &str) -> Result<()> {
    let mut fields = Vec::new();
    for (tag, value) in [(b'S', "FATAL"), (b'V', "FATAL"), (b'C', sqlstate), (b'M', message)] {
        fields.push(tag);
        fields.extend_from_slice(value.as_bytes());
        fields.push(0);
    }
    fields.push(0);
    let mut frame = vec![b'E'];
    frame.extend_from_slice(&((fields.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(&fields);
    client.write_all(&frame).await.map_err(io)
}

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/gateway-io", error.to_string()) }
