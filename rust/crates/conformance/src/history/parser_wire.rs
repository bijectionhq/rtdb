use super::{
    Error, combine, mixed_iid::wire::{flight_handshake, pg_raw_frame, pg_send, pg_startup, request, with_flight, with_pgwire},
    store::Reads, support::{FixtureClock, RunningNode}, write_new,
};
use arrow_array::{Array, Int64Array};
use arrow_flight::{Action, FlightDescriptor, decode::{DecodedPayload, FlightDataDecoder}, sql::{CommandStatementQuery, ProstMessageExt}};
use arrow_schema::{DataType, Schema};
use futures::TryStreamExt;
use prost::Message;
use serde::Deserialize;
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap, fs::{self, File}, io::Read, net::SocketAddr, path::Path,
    sync::{Arc, atomic::AtomicI64},
};
use tonic::metadata::MetadataMap;

const MAX_FIXTURE_BYTES: u64 = 4096;
const MAX_PG_FRAMES: usize = 16;
const ANOMALY_METADATA_KEY: &str = "xtdb-anomaly-bin";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema_version: u32,
    statement_mismatch: String,
    unwanted_token: String,
    follow_up: String,
}

impl Fixture {
    fn statements(&self) -> [(&str, &str); 2] {
        [("statement_mismatch", &self.statement_mismatch), ("unwanted_token", &self.unwanted_token)]
    }
}

async fn pg_query(stream: &mut tokio::net::TcpStream, sql: &str) -> Result<Json, Error> {
    let mut command = sql.as_bytes().to_vec();
    command.push(0);
    pg_send(stream, b'Q', &command).await?;
    let mut frames = Vec::new();
    for _ in 0..MAX_PG_FRAMES {
        let (tag, body) = pg_raw_frame(stream).await?;
        frames.push(json!({"tag": char::from(tag), "bytes": body}));
        if tag == b'Z' { return Ok(json!({"sql": sql, "frames": frames})); }
    }
    Err("PostgreSQL observation did not reach ReadyForQuery".into())
}

async fn pg_observe(address: SocketAddr, fixture: &Fixture) -> Result<Json, Error> {
    let mut stream = pg_startup(address).await?;
    let mut errors = serde_json::Map::new();
    for (id, sql) in fixture.statements() {
        errors.insert(id.into(), pg_query(&mut stream, sql).await?);
    }
    let follow_up = pg_query(&mut stream, &fixture.follow_up).await?;
    pg_send(&mut stream, b'X', &[]).await?;
    Ok(json!({"errors": errors, "follow_up": follow_up}))
}

fn descriptor(sql: &str) -> FlightDescriptor {
    let command = CommandStatementQuery { query: sql.into(), transaction_id: None };
    FlightDescriptor::new_cmd(command.as_any().encode_to_vec())
}

#[derive(Clone, PartialEq, Message)]
struct SessionOptions {
    #[prost(btree_map = "string, message", tag = "1")]
    values: BTreeMap<String, StringOption>,
}

#[derive(Clone, PartialEq, Message)]
struct StringOption {
    #[prost(string, tag = "1")]
    value: String,
}

fn reject_cookie_change(headers: &MetadataMap) -> Result<(), Error> {
    if headers.contains_key("set-cookie") { return Err("Flight call returned a session cookie after establishment".into()); }
    Ok(())
}

fn established_headers(initial: &MetadataMap, trailers: Option<&MetadataMap>) -> Result<MetadataMap, Error> {
    if let Some(trailers) = trailers { reject_cookie_change(trailers)?; }
    let session_cookies = initial.get_all("set-cookie");
    let mut cookies = session_cookies.iter();
    let cookie = cookies.next().ok_or("Flight handshake omitted its session cookie")?;
    let id = cookie.to_str()?.strip_prefix("arrow_flight_session_id=").ok_or("Flight handshake returned a different cookie")?;
    if id.len() != 36 || uuid::Uuid::parse_str(id).is_err() || cookies.next().is_some() {
        return Err("Flight handshake did not establish exactly one session".into());
    }
    let mut headers = MetadataMap::new();
    headers.insert("cookie", cookie.clone());
    Ok(headers)
}

fn flight_schema(schema: &Schema) -> Result<Json, Error> {
    let [field] = &schema.fields()[..] else { return Err("Flight follow-up schema did not have one field".into()); };
    if field.name() != "answer" || field.data_type() != &DataType::Int64 {
        return Err("Flight follow-up schema omitted bigint answer".into());
    }
    Ok(json!({"fields": [{"name": field.name(), "type": "i64", "nullable": field.is_nullable(), "metadata": field.metadata()}],
        "metadata": schema.metadata()}))
}

async fn flight_observe(address: SocketAddr, fixture: &Fixture) -> Result<Json, Error> {
    let (mut client, response) = flight_handshake(address).await?;
    let initial = response.metadata().clone();
    let mut handshake = response.into_inner();
    if handshake.message().await?.is_some() { return Err("Unexpected Flight fixture handshake payload".into()); }
    let trailers = handshake.trailers().await?;
    let headers = established_headers(&initial, trailers.as_ref())?;
    let options = SessionOptions { values: BTreeMap::from([("catalog".into(), StringOption { value: "xtdb".into() })]) };
    let action = Action { r#type: "SetSessionOptions".into(), body: options.encode_to_vec().into() };
    let response = client.do_action(request(action, &headers)).await?;
    reject_cookie_change(response.metadata())?;
    let mut replies = response.into_inner();
    if replies.message().await?.is_none_or(|result| !result.body.is_empty()) || replies.message().await?.is_some() {
        return Err("Flight server did not apply the requested session catalog".into());
    }
    if let Some(trailers) = replies.trailers().await? { reject_cookie_change(&trailers)?; }

    let mut calls = Vec::new();
    let mut errors = serde_json::Map::new();
    for (id, sql) in fixture.statements() {
        let error = client.get_flight_info(request(descriptor(sql), &headers)).await
            .err().ok_or("Malformed Flight statement returned FlightInfo")?;
        reject_cookie_change(error.metadata())?;
        let wire = error.metadata().get_bin(ANOMALY_METADATA_KEY)
            .ok_or("Flight parser rejection omitted binary anomaly metadata")?.to_bytes()?;
        errors.insert(id.into(), json!({"sql": sql, "status": format!("{:?}", error.code()),
            "message": error.message(), "metadata_bytes": wire.as_ref()}));
        calls.push(id);
    }
    let response = client.get_flight_info(request(descriptor(&fixture.follow_up), &headers)).await?;
    reject_cookie_change(response.metadata())?;
    let info = response.into_inner();
    let [endpoint] = info.endpoint.as_slice() else { return Err("Flight follow-up did not return one endpoint".into()); };
    let ticket = endpoint.ticket.clone().ok_or("Flight follow-up omitted its ticket")?;
    let declared = flight_schema(&info.try_decode_schema()?)?;
    calls.push("follow_up_info");
    let response = client.do_get(request(ticket, &headers)).await?;
    reject_cookie_change(response.metadata())?;
    let mut stream = response.into_inner();
    let mut frames = Vec::new();
    while let Some(frame) = stream.message().await? {
        if frames.len() == 8 { return Err("Flight follow-up returned too many frames".into()); }
        frames.push(Ok(frame));
    }
    if let Some(trailers) = stream.trailers().await? { reject_cookie_change(&trailers)?; }
    let mut decoder = FlightDataDecoder::new(futures::stream::iter(frames));
    let mut stream_schema = None;
    let mut batches = Vec::new();
    while let Some(message) = decoder.try_next().await? {
        match message.payload {
            DecodedPayload::Schema(value) if stream_schema.is_none() => stream_schema = Some(flight_schema(&value)?),
            DecodedPayload::RecordBatch(batch) if stream_schema.is_some() && batches.is_empty() && batch.num_columns() == 1 => {
                let answer = batch.column(0).as_any().downcast_ref::<Int64Array>().ok_or("Flight follow-up was not bigint")?;
                if answer.null_count() != 0 || answer.len() != 1 { return Err("Flight follow-up returned unexpected rows".into()); }
                batches.push(json!({"schema": flight_schema(&batch.schema())?, "rows": [[{"kind":"i64","value":answer.value(0)}]]}));
            }
            _ => return Err("Unexpected Flight follow-up schema or payload".into()),
        }
    }
    calls.push("follow_up_stream");
    let stream_schema = stream_schema.ok_or("Flight follow-up omitted the DoGet stream schema")?;
    Ok(json!({"errors": errors, "session": {"options_applied": true, "cookie_established": true, "calls": calls},
        "follow_up": {"sql": fixture.follow_up, "declared_schema": declared, "stream_schema": stream_schema, "batches": batches}}))
}

/// Observes PostgreSQL and Flight SQL parser responses on a fresh local node.
/// The caller supplies the fixture and a new output directory whose parent exists.
/// Returns the receipt written to `output/execution.json` after both listeners and the node have joined.
pub async fn execute(fixture_path: &Path, output: &Path) -> Result<Json, Error> {
    let mut bytes = Vec::new();
    File::open(fixture_path)?.take(MAX_FIXTURE_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_FIXTURE_BYTES { return Err("Parser wire fixture exceeds its size bound".into()); }
    let fixture: Fixture = serde_json::from_slice(&bytes)?;
    if fixture.schema_version != 1 || fixture.statements() != [("statement_mismatch", "SELEC 1"), ("unwanted_token", "extra SELECT 1")]
        || fixture.follow_up != "SELECT 42::bigint AS answer"
    {
        return Err("Invalid parser wire fixture".into());
    }
    fs::create_dir(output)?;
    let node_dir = output.join("node");
    fs::create_dir(&node_dir)?;
    let clock = Arc::new(FixtureClock(AtomicI64::new(0)));
    let running = RunningNode::open(&node_dir, clock, Arc::new(Reads::default())).await?;
    let observed = async {
        let pgwire = with_pgwire(&running.node, |address| pg_observe(address, &fixture)).await?;
        let flight = with_flight(&running.node, |address| flight_observe(address, &fixture)).await?;
        Ok(json!({"pgwire": pgwire, "flight_sql": flight}))
    }.await;
    let observations = combine(observed, running.close().await)?;
    let execution = json!({"schema_version": 1, "engine": "rust",
        "artifact_revision": env!("RTDB_CONFORMANCE_REVISION"), "source_dirty": env!("RTDB_CONFORMANCE_DIRTY") != "false",
        "fixture_sha256": format!("{:x}", Sha256::digest(&bytes)),
        "binary_sha256": crate::controls::digest_file(&std::env::current_exe()?)?,
        "observations": observations});
    write_new(&output.join("execution.json"), &execution)?;
    Ok(execution)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flight_handshake_requires_exactly_one_cookie_and_later_calls_reject_cookie_headers() {
        let mut initial = MetadataMap::new();
        initial.insert("authorization", "Bearer unrelated".parse().unwrap());
        assert!(established_headers(&initial, None).is_err());
        let cookie = "arrow_flight_session_id=feee95cf-cd47-4d4d-b634-7d4d53ed5441";
        initial.insert("set-cookie", cookie.parse().unwrap());
        let outgoing = established_headers(&initial, None).unwrap();
        assert_eq!(outgoing.get("cookie").unwrap().to_str().unwrap(), cookie);
        assert!(reject_cookie_change(&MetadataMap::new()).is_ok());
        assert!(reject_cookie_change(&initial).is_err());
        assert!(established_headers(&initial, Some(&initial)).is_err());
        initial.append("set-cookie", cookie.parse().unwrap());
        assert!(established_headers(&initial, None).is_err());
    }
}
