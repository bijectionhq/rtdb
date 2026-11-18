use super::Error;
use arrow_array::{Array, FixedSizeBinaryArray};
use arrow_flight::{FlightDescriptor, HandshakeRequest, HandshakeResponse, decode::{DecodedPayload, FlightDataDecoder},
    flight_service_client::FlightServiceClient, sql::{CommandStatementQuery, ProstMessageExt}};
use arrow_schema::{DataType, Schema};
use async_trait::async_trait;
use futures::TryStreamExt;
use prost::Message;
use rtdb_auth::{AuthConfig, Authenticator, HttpRequest, HttpResponse, HttpTransport};
use rtdb_node::{Node, SystemClock};
use rtdb_types::Anomaly;
use serde::Serialize;
use serde_json::{Value as Json, json};
use std::{future::Future, net::{Ipv4Addr, SocketAddr}, sync::Arc, time::Duration};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, task::JoinError};
use tonic::{Request, Response, Streaming, metadata::MetadataMap, transport::Channel};

const QUERY_TIMEOUT: Duration = Duration::from_secs(60);
const PG_FRAME_LIMIT: usize = 64 * 1024;
const FLIGHT_FRAME_LIMIT: usize = 1024 * 1024;
const ROW_LIMIT: usize = 16_384;
const FLIGHT_SQL: &str = "SELECT _iid FROM readings FOR ALL VALID_TIME FOR ALL SYSTEM_TIME";

struct UnusedHttp;

#[async_trait]
impl HttpTransport for UnusedHttp {
    async fn request(&self, _: HttpRequest) -> rtdb_types::Result<HttpResponse> {
        Err(Anomaly::fault("rtdb.conformance/unexpected-http", "Mixed IID root authentication attempted HTTP"))
    }
}

async fn auth(node: &Node) -> Result<Arc<Authenticator>, Error> {
    Ok(Authenticator::open(AuthConfig::SingleRootUser { password: None }, Arc::clone(node.memory()),
        Arc::new(SystemClock), Arc::new(UnusedHttp)).await?)
}

fn listener_result<T>(work: Result<T, Error>, joined: Result<rtdb_types::Result<()>, JoinError>, drained: bool) -> Result<T, Error> {
    let closed = joined.map_err(Error::from).and_then(|result| result.map_err(Error::from)).and_then(|()| {
        if drained { Ok(()) } else { Err("Mixed IID wire listener did not drain".into()) }
    });
    match (work, closed) {
        (Ok(observation), Ok(())) => Ok(observation),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(close)) => Err(format!("{error}; wire listener shutdown also failed: {close}").into()),
    }
}

pub(super) async fn pgwire(node: &Node, sql: &str) -> Result<Json, Error> {
    with_pgwire(node, |address| pg_query(address, sql)).await
}

pub(in crate::history) async fn with_pgwire<T, F>(node: &Node, observe: impl FnOnce(SocketAddr) -> F) -> Result<T, Error>
where F: Future<Output = Result<T, Error>> {
    let (server, driver) = rtdb_pgwire::ServerDriver::bind(rtdb_pgwire::ServerConfig {
        address: (Ipv4Addr::LOCALHOST, 0).into(), ..Default::default()
    }, node.clone(), auth(node).await?).await?;
    let owner = tokio::spawn(driver.run());
    let work = tokio::time::timeout(QUERY_TIMEOUT, observe(server.address())).await
        .map_err(Error::from).and_then(|result| result);
    server.shutdown();
    let joined = owner.await;
    let status = server.status();
    listener_result(work, joined, status.stopped && status.active_connections == 0 && status.pending_connections == 0)
}

pub(super) async fn flight_sql(node: &Node) -> Result<Json, Error> {
    with_flight(node, flight_query).await
}

pub(in crate::history) async fn with_flight<T, F>(node: &Node, observe: impl FnOnce(SocketAddr) -> F) -> Result<T, Error>
where F: Future<Output = Result<T, Error>> {
    let (server, driver) = rtdb_flight_sql::ServerDriver::bind(rtdb_flight_sql::FlightConfig {
        address: (Ipv4Addr::LOCALHOST, 0).into(), maximum_message_bytes: FLIGHT_FRAME_LIMIT, ..Default::default()
    }, node.clone(), auth(node).await?).await?;
    let owner = tokio::spawn(driver.run());
    let work = tokio::time::timeout(QUERY_TIMEOUT, observe(server.address())).await
        .map_err(Error::from).and_then(|result| result);
    server.shutdown();
    let joined = owner.await;
    let status = server.status();
    listener_result(work, joined, status.stopped && status.active_calls == 0)
}

pub(in crate::history) async fn pg_raw_frame(stream: &mut TcpStream) -> Result<(u8, Vec<u8>), Error> {
    let tag = stream.read_u8().await?;
    let length = stream.read_u32().await? as usize;
    if !(4..=PG_FRAME_LIMIT).contains(&length) {
        return Err("Invalid PostgreSQL fixture frame length".into());
    }
    let mut body = vec![0; length - 4];
    stream.read_exact(&mut body).await?;
    Ok((tag, body))
}

async fn pg_frame(stream: &mut TcpStream) -> Result<(u8, Vec<u8>), Error> {
    let (tag, body) = pg_raw_frame(stream).await?;
    if tag == b'E' { return Err(format!("PostgreSQL fixture error: {}", String::from_utf8_lossy(&body)).into()); }
    Ok((tag, body))
}

pub(in crate::history) async fn pg_send(stream: &mut TcpStream, tag: u8, body: &[u8]) -> Result<(), Error> {
    stream.write_u8(tag).await?;
    stream.write_u32(u32::try_from(body.len() + 4)?).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

pub(in crate::history) async fn pg_startup(address: SocketAddr) -> Result<TcpStream, Error> {
    let mut stream = TcpStream::connect(address).await?;
    let parameters = b"user\0xtdb\0database\0xtdb\0\0";
    stream.write_u32(u32::try_from(parameters.len() + 8)?).await?;
    stream.write_u32(196_608).await?;
    stream.write_all(parameters).await?;
    stream.flush().await?;
    let mut authenticated = false;
    let mut process = None;
    for _ in 0..128 {
        let (tag, body) = pg_frame(&mut stream).await?;
        match tag {
            b'R' if !authenticated && body == 0u32.to_be_bytes() => authenticated = true,
            b'K' if authenticated && process.is_none() && body.len() == 8 => {
                let mut body = body.as_slice();
                let id = i32::from_be_bytes(fixed(&mut body)?);
                if id <= 0 { return Err("Invalid PostgreSQL fixture process identifier".into()); }
                process = Some(id);
            }
            b'S' if authenticated => (),
            b'Z' if authenticated && process.is_some() && body == b"I" => return Ok(stream),
            _ => return Err(format!("Unexpected PostgreSQL fixture startup response: {tag}").into()),
        }
    }
    Err("PostgreSQL fixture startup did not become ready".into())
}

fn take<'a>(input: &mut &'a [u8], count: usize) -> Result<&'a [u8], Error> {
    let Some((prefix, remaining)) = input.split_at_checked(count) else {
        return Err("Truncated PostgreSQL fixture value".into());
    };
    *input = remaining;
    Ok(prefix)
}

fn fixed<const N: usize>(input: &mut &[u8]) -> Result<[u8; N], Error> {
    Ok(take(input, N)?.try_into()?)
}

#[derive(Serialize)]
struct PgField {
    name: String,
    table_oid: u32,
    attribute_number: i16,
    type_oid: u32,
    type_size: i16,
    type_modifier: i32,
    format: i16,
}

fn pg_fields(mut body: &[u8]) -> Result<Vec<PgField>, Error> {
    if u16::from_be_bytes(fixed(&mut body)?) != 2 { return Err("Expected two PostgreSQL fixture columns".into()); }
    let mut fields = Vec::new();
    for (expected_name, expected_oid, expected_size) in [("_id", 20, 8), ("_iid", 17, -1)] {
        let end = body.iter().position(|byte| *byte == 0).ok_or("Missing PostgreSQL column name terminator")?;
        let name = std::str::from_utf8(&take(&mut body, end + 1)?[..end])?.to_owned();
        let field = PgField {
            name,
            table_oid: u32::from_be_bytes(fixed(&mut body)?),
            attribute_number: i16::from_be_bytes(fixed(&mut body)?),
            type_oid: u32::from_be_bytes(fixed(&mut body)?),
            type_size: i16::from_be_bytes(fixed(&mut body)?),
            type_modifier: i32::from_be_bytes(fixed(&mut body)?),
            format: i16::from_be_bytes(fixed(&mut body)?),
        };
        if field.name != expected_name || field.type_oid != expected_oid || field.type_size != expected_size || field.format != 0 {
            return Err("Unexpected PostgreSQL IID RowDescription field".into());
        }
        fields.push(field);
    }
    if !body.is_empty() { return Err("Trailing PostgreSQL RowDescription bytes".into()); }
    Ok(fields)
}

fn pg_cell<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], Error> {
    let size = usize::try_from(i32::from_be_bytes(fixed(input)?))?;
    take(input, size)
}

fn signed_iid(value: &[u8]) -> Result<[i8; 16], Error> {
    let bytes: [u8; 16] = value.try_into()?;
    Ok(bytes.map(|byte| byte as i8))
}

fn typed_iid(bytes: [i8; 16]) -> Json {
    json!({"kind":"fixed-size-binary","signed_bytes":bytes})
}

fn pg_row(mut body: &[u8]) -> Result<Json, Error> {
    if u16::from_be_bytes(fixed(&mut body)?) != 2 { return Err("Expected a two-column PostgreSQL DataRow".into()); }
    let id = std::str::from_utf8(pg_cell(&mut body)?)?.parse::<i64>()?;
    let text = pg_cell(&mut body)?.strip_prefix(b"\\x").ok_or("PostgreSQL IID is not hex bytea")?;
    if text.len() != 32 || !text.iter().all(u8::is_ascii_hexdigit) || !body.is_empty() {
        return Err("PostgreSQL IID DataRow is not exactly sixteen bytes".into());
    }
    let mut bytes = [0; 16];
    for (target, hex) in bytes.iter_mut().zip(text.chunks_exact(2)) {
        *target = u8::from_str_radix(std::str::from_utf8(hex)?, 16)?;
    }
    Ok(json!({"xt/id":{"kind":"i64","value":id},"xt/iid":typed_iid(signed_iid(&bytes)?)}))
}

async fn pg_query(address: SocketAddr, sql: &str) -> Result<Json, Error> {
    let mut stream = pg_startup(address).await?;
    let mut query = sql.as_bytes().to_vec();
    query.push(0);
    pg_send(&mut stream, b'Q', &query).await?;
    let (tag, description) = pg_frame(&mut stream).await?;
    if tag != b'T' { return Err("PostgreSQL IID query did not return a RowDescription".into()); }
    let fields = pg_fields(&description)?;
    let mut rows = Vec::new();
    let command = loop {
        let (tag, body) = pg_frame(&mut stream).await?;
        match tag {
            b'D' if rows.len() < ROW_LIMIT => rows.push(pg_row(&body)?),
            b'C' => break body,
            _ => return Err("Unexpected or excessive PostgreSQL IID query responses".into()),
        }
    };
    let expected = format!("SELECT {}\0", rows.len());
    if rows.is_empty() || command != expected.as_bytes() || pg_frame(&mut stream).await? != (b'Z', b"I".to_vec()) {
        return Err("Incomplete PostgreSQL IID query result".into());
    }
    pg_send(&mut stream, b'X', &[]).await?;
    Ok(json!({"sql":sql,"fields":fields,"rows":rows,"command_tag":expected.trim_end_matches('\0')}))
}

fn flight_schema(schema: &Schema) -> Result<Json, Error> {
    let [field] = &schema.fields()[..] else { return Err("Flight IID schema does not contain exactly one field".into()); };
    if field.name() != "_iid" || field.data_type() != &DataType::FixedSizeBinary(16) {
        return Err("Flight IID field is not fixed-size binary(16)".into());
    }
    Ok(json!({"fields":[{"name":field.name(),"type":{"kind":"fixed-size-binary","byte_width":16},
        "nullable":field.is_nullable(),"metadata":field.metadata()}],"metadata":schema.metadata()}))
}

pub(in crate::history) fn request<T>(body: T, headers: &MetadataMap) -> Request<T> {
    let mut request = Request::new(body);
    *request.metadata_mut() = headers.clone();
    request
}

pub(in crate::history) async fn flight_handshake(address: SocketAddr) -> Result<(FlightServiceClient<Channel>, Response<Streaming<HandshakeResponse>>), Error> {
    let channel = Channel::from_shared(format!("http://{address}"))?.connect().await?;
    let mut client = FlightServiceClient::new(channel).max_decoding_message_size(FLIGHT_FRAME_LIMIT);
    let response = client.handshake(futures::stream::iter([HandshakeRequest::default()])).await?;
    Ok((client, response))
}

async fn flight_query(address: SocketAddr) -> Result<Json, Error> {
    let (mut client, response) = flight_handshake(address).await?;
    let mut headers = MetadataMap::new();
    if let Some(cookie) = response.metadata().get("set-cookie") { headers.insert("cookie", cookie.clone()); }
    if let Some(authorization) = response.metadata().get("authorization") { headers.insert("authorization", authorization.clone()); }
    if headers.is_empty() { return Err("Flight fixture handshake did not establish a session".into()); }
    let mut handshake = response.into_inner();
    if handshake.message().await?.is_some() { return Err("Unexpected Flight fixture handshake payload".into()); }
    drop(handshake);

    let command = CommandStatementQuery { query: FLIGHT_SQL.into(), transaction_id: None };
    let descriptor = FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
    let info = client.get_flight_info(request(descriptor, &headers)).await?.into_inner();
    let [endpoint] = info.endpoint.as_slice() else { return Err("Flight IID query did not return one endpoint".into()); };
    let ticket = endpoint.ticket.clone().ok_or("Flight IID endpoint has no ticket")?;
    let declared = info.try_decode_schema()?;
    let declared_fields = flight_schema(&declared)?;
    let stream = client.do_get(request(ticket, &headers)).await?.into_inner();
    let mut decoder = FlightDataDecoder::new(stream.map_err(Into::into));
    let mut stream_schema = None;
    let mut batches = Vec::new();
    let mut iids = Vec::new();
    while let Some(message) = decoder.try_next().await? {
        match message.payload {
            DecodedPayload::Schema(schema) if stream_schema.is_none() && schema.as_ref() == &declared => {
                stream_schema = Some(flight_schema(&schema)?);
            }
            DecodedPayload::RecordBatch(batch) if stream_schema.is_some() && batch.schema().as_ref() == &declared => {
                if batches.len() == ROW_LIMIT || batch.num_rows() > ROW_LIMIT - iids.len() {
                    return Err("Excessive Flight IID fixture response".into());
                }
                let fields = flight_schema(&batch.schema())?;
                let values = batch.column(0).as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or("Invalid Flight IID array")?;
                if values.null_count() != 0 || values.value_length() != 16 {
                    return Err("Flight IID batch has null or non-16-byte values".into());
                }
                for row in 0..values.len() { iids.push(signed_iid(values.value(row))?); }
                batches.push(json!({"schema":fields,"rows":batch.num_rows()}));
            }
            _ => return Err("Unexpected Flight IID stream schema or payload".into()),
        }
    }
    let stream_schema = stream_schema.ok_or("Flight IID stream did not describe a schema")?;
    if iids.is_empty() { return Err("Flight IID query returned no rows".into()); }
    iids.sort_unstable();
    Ok(json!({"sql":FLIGHT_SQL,"declared":declared_fields,"stream_schema":stream_schema,"batches":batches,
              "iids":iids.into_iter().map(typed_iid).collect::<Vec<_>>()}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_description_rejects_incorrect_iid_type_or_format() {
        let mut description = 2u16.to_be_bytes().to_vec();
        for (name, oid, size) in [("_id", 20u32, 8i16), ("_iid", 17, -1)] {
            description.extend_from_slice(name.as_bytes());
            description.push(0);
            description.extend_from_slice(&[0; 6]);
            description.extend_from_slice(&oid.to_be_bytes());
            description.extend_from_slice(&size.to_be_bytes());
            description.extend_from_slice(&(-1i32).to_be_bytes());
            description.extend_from_slice(&[0; 2]);
        }
        assert!(pg_fields(&description).is_ok());
        *description.last_mut().unwrap() = 1;
        assert!(pg_fields(&description).is_err());
    }

    #[test]
    fn text_iid_requires_exact_byte_width_and_preserves_signed_bytes() {
        let mut row = 2u16.to_be_bytes().to_vec();
        row.extend_from_slice(&1i32.to_be_bytes());
        row.push(b'7');
        row.extend_from_slice(&34i32.to_be_bytes());
        row.extend_from_slice(b"\\x80ff0000000000000000000000000000");
        let value = pg_row(&row).unwrap();
        assert_eq!(value["xt/id"]["value"], 7);
        assert_eq!(value["xt/iid"]["signed_bytes"][0], -128);
        assert_eq!(value["xt/iid"]["signed_bytes"][1], -1);
        let index = row.len() - 1;
        row[index] = b'!';
        assert!(pg_row(&row).is_err());
        row.truncate(index);
        assert!(pg_row(&row).is_err());
    }
}
