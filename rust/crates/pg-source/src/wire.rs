use std::{collections::BTreeMap, sync::Arc};
use postgres_protocol::authentication::{md5_hash, sasl::{ChannelBinding, ScramSha256}};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_pg_codec::{Format, source_snapshot_format};
use rtdb_types::{Anomaly, Result};
use tokio::{io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt}, net::TcpStream};
use tokio_rustls::{TlsConnector, rustls::pki_types::ServerName};
use crate::{Remote, SslMode};

trait Socket: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Socket for T {}

pub(crate) struct Frame { pub tag: u8, pub bytes: Vec<u8>, _charge: Arc<MemoryReservation> }
struct Reading { header: [u8; 5], header_read: usize, bytes: Vec<u8>, bytes_read: usize, charge: Option<Arc<MemoryReservation>> }
impl Reading { fn new() -> Self { Self { header: [0; 5], header_read: 0, bytes: Vec::new(), bytes_read: 0, charge: None } } }
pub(crate) struct Connection { socket: Box<dyn Socket>, reading: Reading, memory: Arc<MemoryBudget>, max_message: usize }
#[derive(Clone, Debug)]
pub(crate) struct Column { pub name: String, pub oid: u32, pub typmod: i32, pub format: Format }
pub(crate) struct Row { frame: Frame, cells: Vec<Option<std::ops::Range<usize>>> }
impl Row {
    pub fn cell(&self, index: usize) -> Result<Option<&str>> {
        self.cell_bytes(index)?.map(|bytes| std::str::from_utf8(bytes).map_err(protocol)).transpose()
    }
    pub fn cell_bytes(&self, index: usize) -> Result<Option<&[u8]>> {
        let cell = self.cells.get(index).ok_or_else(|| protocol("DataRow column index is out of bounds"))?;
        Ok(cell.as_ref().map(|range| &self.frame.bytes[range.clone()]))
    }
    pub fn encoded_size(&self) -> usize { self.frame.bytes.len() }
}
pub(crate) struct QueryResult { pub columns: Vec<Column>, pub rows: Vec<Row>, _description: Option<Frame>, _charge: Arc<MemoryReservation> }

pub(crate) struct SnapshotFetch { bind: Vec<u8>, _charge: Arc<MemoryReservation> }

impl Connection {
    pub async fn open(remote: &Remote, replication: bool, memory: Arc<MemoryBudget>) -> Result<Self> {
        remote.validate()?;
        tokio::time::timeout(remote.connect_timeout, Self::connect(remote, replication, memory)).await.map_err(|_| network("PostgreSQL connection timed out"))?
    }
    async fn connect(remote: &Remote, replication: bool, memory: Arc<MemoryBudget>) -> Result<Self> {
        let mut socket = TcpStream::connect((remote.hostname.as_str(), remote.port)).await.map_err(network)?;
        socket.set_nodelay(true).map_err(network)?;
        let socket: Box<dyn Socket> = match &remote.ssl {
            SslMode::Disable => Box::new(socket),
            SslMode::Prefer(config) | SslMode::Require(config) => {
                socket.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await.map_err(network)?;
                socket.flush().await.map_err(network)?;
                match socket.read_u8().await.map_err(network)? {
                    b'S' => {
                        let name = ServerName::try_from(remote.hostname.clone()).map_err(protocol)?;
                        let tls = TlsConnector::from(Arc::clone(config)).connect(name, socket).await.map_err(network)?;
                        Box::new(tls)
                    }
                    b'N' if matches!(&remote.ssl, SslMode::Prefer(_)) => Box::new(socket),
                    b'N' => return Err(network("PostgreSQL server declined required TLS")),
                    _ => return Err(protocol("Unexpected SSL negotiation response")),
                }
            }
        };
        let mut connection = Self { socket, reading: Reading::new(), memory, max_message: remote.maximum_message_bytes };
        let mut startup = Vec::new();
        startup.extend_from_slice(&196608u32.to_be_bytes());
        for (key, value) in [("user", remote.username.as_str()), ("database", remote.database.as_str()), ("client_encoding", "UTF8"), ("application_name", "rtdb-pg-source")] {
            startup.extend_from_slice(key.as_bytes()); startup.push(0); startup.extend_from_slice(value.as_bytes()); startup.push(0);
        }
        if replication { startup.extend_from_slice(b"replication\0database\0"); }
        startup.push(0);
        connection.send_startup(&startup).await?;
        let mut scram = None;
        let mut authenticated = false;
        let mut verified = false;
        loop {
            let frame = connection.read().await?;
            let mut input = Input::new(&frame.bytes);
            match frame.tag {
                b'R' => match input.i32()? {
                    0 => { if scram.is_some() && !verified { return Err(protocol("SCRAM authentication completed without a verified server signature")); } authenticated = true; }
                    3 => { let mut password = remote.password.as_bytes().to_vec(); password.push(0); connection.send(b'p', &password).await?; }
                    5 => { let salt: [u8; 4] = input.take(4)?.try_into().map_err(protocol)?; let mut password = md5_hash(remote.username.as_bytes(), remote.password.as_bytes(), salt).into_bytes(); password.push(0); connection.send(b'p', &password).await?; }
                    10 => {
                        let mut offered = false;
                        while input.remaining() > 0 { let mechanism = input.string()?; if mechanism.is_empty() { break; } offered |= mechanism == "SCRAM-SHA-256"; }
                        if !offered { return Err(protocol("PostgreSQL did not offer supported SCRAM authentication")); }
                        let state = ScramSha256::new(remote.password.as_bytes(), ChannelBinding::unsupported());
                        let mut body = b"SCRAM-SHA-256\0".to_vec(); body.extend_from_slice(&i32::try_from(state.message().len()).map_err(protocol)?.to_be_bytes()); body.extend_from_slice(state.message());
                        connection.send(b'p', &body).await?; scram = Some(state);
                    }
                    11 => { let state = scram.as_mut().ok_or_else(|| protocol("SASL continuation precedes its challenge"))?; state.update(input.rest()).map_err(protocol)?; connection.send(b'p', state.message()).await?; }
                    12 => { scram.as_mut().ok_or_else(|| protocol("SASL final precedes its challenge"))?.finish(input.rest()).map_err(protocol)?; verified = true; }
                    method => return Err(protocol(format!("Unsupported PostgreSQL authentication method {method}"))),
                },
                b'E' => return Err(server_error(&frame.bytes)?),
                b'S' | b'K' | b'N' => {},
                b'Z' if authenticated => break,
                _ => return Err(protocol("Unexpected PostgreSQL startup message")),
            }
        }
        Ok(connection)
    }
    async fn send_startup(&mut self, bytes: &[u8]) -> Result<()> {
        let length = i32::try_from(bytes.len().checked_add(4).ok_or_else(|| protocol("Message length overflow"))?).map_err(protocol)?;
        self.socket.write_all(&length.to_be_bytes()).await.map_err(network)?;
        self.socket.write_all(bytes).await.map_err(network)?;
        self.socket.flush().await.map_err(network)
    }
    pub async fn send(&mut self, tag: u8, bytes: &[u8]) -> Result<()> {
        let length = i32::try_from(bytes.len().checked_add(4).ok_or_else(|| protocol("Message length overflow"))?).map_err(protocol)?;
        self.socket.write_u8(tag).await.map_err(network)?;
        self.socket.write_all(&length.to_be_bytes()).await.map_err(network)?;
        self.socket.write_all(bytes).await.map_err(network)?;
        self.socket.flush().await.map_err(network)
    }
    pub async fn read(&mut self) -> Result<Frame> {
        while self.reading.header_read < 5 {
            let count = self.socket.read(&mut self.reading.header[self.reading.header_read..]).await.map_err(network)?;
            if count == 0 { return Err(network("PostgreSQL connection closed while reading a frame header")); }
            self.reading.header_read += count;
        }
        if self.reading.charge.is_none() {
            let length = i32::from_be_bytes(self.reading.header[1..5].try_into().map_err(protocol)?);
            let length = usize::try_from(length.checked_sub(4).ok_or_else(|| protocol("Invalid backend frame length"))?).map_err(protocol)?;
            if length > self.max_message { return Err(protocol("PostgreSQL frame exceeds the configured message limit")); }
            self.reading.charge = Some(self.memory.reserve(length.checked_mul(32).and_then(|size| size.checked_add(4096)).ok_or_else(|| protocol("Frame allocation size overflow"))?)?);
            self.reading.bytes.resize(length, 0);
        }
        while self.reading.bytes_read < self.reading.bytes.len() {
            let count = self.socket.read(&mut self.reading.bytes[self.reading.bytes_read..]).await.map_err(network)?;
            if count == 0 { return Err(network("PostgreSQL connection closed while reading a frame body")); }
            self.reading.bytes_read += count;
        }
        let reading = std::mem::replace(&mut self.reading, Reading::new());
        Ok(Frame { tag: reading.header[0], bytes: reading.bytes, _charge: reading.charge.ok_or_else(|| protocol("Completed frame has no reservation"))? })
    }
    pub async fn query(&mut self, query: &str) -> Result<QueryResult> {
        if query.contains('\0') { return Err(protocol("SQL query contains a zero byte")); }
        let charge = self.memory.reserve(query.len().checked_add(8192).ok_or_else(|| protocol("Query allocation overflow"))?)?;
        let mut body = query.as_bytes().to_vec(); body.push(0); self.send(b'Q', &body).await?;
        self.read_query(charge, false).await
    }
    pub async fn prepare_snapshot_fetch(&mut self) -> Result<SnapshotFetch> {
        let query = b"\0FETCH FORWARD 1000 FROM rtdb_rows\0\0\0";
        let charge = self.memory.reserve(8192 + query.len())?;
        self.send(b'P', query).await?;
        self.send(b'D', b"S\0").await?;
        self.send(b'S', b"").await?;
        let statement = self.read_query(charge, true).await?;
        let count = u16::try_from(statement.columns.len()).map_err(protocol)?;
        let len = 8 + 2 * usize::from(count);
        let charge = self.memory.reserve(1024 + len)?;
        let mut bind = Vec::with_capacity(len);
        bind.extend_from_slice(&[0; 6]);
        bind.extend_from_slice(&count.to_be_bytes());
        for column in &statement.columns {
            bind.extend_from_slice(&source_snapshot_format(column.oid).code().to_be_bytes());
        }
        Ok(SnapshotFetch { bind, _charge: charge })
    }

    pub async fn snapshot_fetch(&mut self, statement: &SnapshotFetch) -> Result<QueryResult> {
        let charge = self.memory.reserve(8192)?;
        self.send(b'B', &statement.bind).await?;
        self.send(b'D', b"P\0").await?;
        self.send(b'E', b"\0\0\0\0\0").await?;
        self.send(b'S', b"").await?;
        self.read_query(charge, true).await
    }

    async fn read_query(&mut self, charge: Arc<MemoryReservation>, extended: bool) -> Result<QueryResult> {
        let mut columns = Vec::new(); let mut description = None; let mut rows = Vec::new(); let mut error = None;
        loop {
            let frame = self.read().await?;
            match frame.tag {
                b'T' => {
                    columns = row_description(&frame.bytes)?;
                    if !extended && columns.iter().any(|column| column.format != Format::Text) {
                        return Err(protocol("Source query requires text result format"));
                    }
                    description = Some(frame);
                }
                b'D' => {
                    if rows.len() >= 1001 { return Err(protocol("Source query exceeded its bounded result-row limit")); }
                    let mut input = Input::new(&frame.bytes); let count = usize::from(input.u16()?);
                    if count != columns.len() { return Err(protocol("DataRow and RowDescription widths differ")); }
                    let mut cells = Vec::with_capacity(count);
                    for _ in 0..count { let length = input.i32()?; if length == -1 { cells.push(None); } else { let length = usize::try_from(length).map_err(protocol)?; let start = input.position(); input.take(length)?; cells.push(Some(start..start + length)); } }
                    input.end()?; rows.push(Row { frame, cells });
                }
                b'E' => error = Some(server_error(&frame.bytes)?),
                b'C' | b'I' | b'N' | b'S' => {},
                b'1' | b'2' | b't' | b'n' if extended => {},
                b'Z' => return match error { Some(error) => Err(error), None => Ok(QueryResult { columns, rows, _description: description, _charge: charge }) },
                _ => return Err(protocol("Unexpected message during a source SQL query")),
            }
        }
    }
    pub async fn start_copy(&mut self, query: &str) -> Result<()> {
        let mut body = query.as_bytes().to_vec(); body.push(0); self.send(b'Q', &body).await?;
        loop { let frame = self.read().await?; match frame.tag { b'W' => return Ok(()), b'E' => return Err(server_error(&frame.bytes)?), b'N' | b'S' => {}, _ => return Err(protocol("START_REPLICATION did not enter CopyBoth mode")) } }
    }
}

fn row_description(bytes: &[u8]) -> Result<Vec<Column>> {
    let mut input = Input::new(bytes); let count = usize::from(input.u16()?); let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let name = input.string()?.to_owned();
        input.take(6)?;
        let oid = input.u32()?;
        input.take(2)?;
        let typmod = input.i32()?;
        let format = match input.u16()? {
            0 => Format::Text,
            1 => Format::Binary,
            _ => return Err(protocol("Unknown source query result format")),
        };
        fields.push(Column { name, oid, typmod, format });
    }
    input.end()?; Ok(fields)
}

pub(crate) struct Input<'a> { bytes: &'a [u8], position: usize }
impl<'a> Input<'a> {
    pub fn new(bytes: &'a [u8]) -> Self { Self { bytes, position: 0 } }
    pub fn position(&self) -> usize { self.position }
    pub fn remaining(&self) -> usize { self.bytes.len() - self.position }
    pub fn rest(&self) -> &'a [u8] { &self.bytes[self.position..] }
    pub fn take(&mut self, count: usize) -> Result<&'a [u8]> { let end = self.position.checked_add(count).filter(|end| *end <= self.bytes.len()).ok_or_else(|| protocol("Truncated PostgreSQL message"))?; let bytes = &self.bytes[self.position..end]; self.position = end; Ok(bytes) }
    pub fn u8(&mut self) -> Result<u8> { Ok(self.take(1)?[0]) }
    pub fn u16(&mut self) -> Result<u16> { Ok(u16::from_be_bytes(self.take(2)?.try_into().map_err(protocol)?)) }
    pub fn i32(&mut self) -> Result<i32> { Ok(i32::from_be_bytes(self.take(4)?.try_into().map_err(protocol)?)) }
    pub fn u32(&mut self) -> Result<u32> { Ok(u32::from_be_bytes(self.take(4)?.try_into().map_err(protocol)?)) }
    pub fn i64(&mut self) -> Result<i64> { Ok(i64::from_be_bytes(self.take(8)?.try_into().map_err(protocol)?)) }
    pub fn u64(&mut self) -> Result<u64> { Ok(u64::from_be_bytes(self.take(8)?.try_into().map_err(protocol)?)) }
    pub fn string(&mut self) -> Result<&'a str> { let length = self.rest().iter().position(|byte| *byte == 0).ok_or_else(|| protocol("Missing zero-terminated string"))?; let text = std::str::from_utf8(self.take(length)?).map_err(protocol)?; self.take(1)?; Ok(text) }
    pub fn end(&self) -> Result<()> { if self.remaining() == 0 { Ok(()) } else { Err(protocol("Trailing bytes in PostgreSQL message")) } }
}
pub(crate) fn server_error(bytes: &[u8]) -> Result<Anomaly> {
    let mut input = Input::new(bytes); let mut fields = BTreeMap::new();
    loop { let kind = input.u8()?; if kind == 0 { break; } fields.insert(kind, input.string()?.to_owned()); }
    let mut error = Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "xtdb.postgres/server-error", fields.remove(&b'M').unwrap_or_else(|| "PostgreSQL rejected the command".into()));
    if let Some(state) = fields.remove(&b'C') { error = error.with_context("sql-state", state.into()); }
    if let Some(detail) = fields.remove(&b'D') { error = error.with_context("detail", detail.into()); }
    Ok(error)
}
pub(crate) fn protocol(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("xtdb.postgres/protocol-error", error.to_string()) }
pub(crate) fn network(error: impl std::fmt::Display) -> Anomaly { Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "xtdb.postgres/connection-error", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cancellation_preserves_partial_backend_header_and_body() {
        let memory = MemoryBudget::new(1 << 20);
        let (client, mut server) = tokio::io::duplex(128);
        let mut connection = Connection { socket: Box::new(client), reading: Reading::new(), memory: Arc::clone(&memory), max_message: 1024 };
        server.write_all(&[b'd', 0]).await.unwrap();
        assert!(futures::poll!(std::pin::pin!(connection.read())).is_pending());
        assert_eq!(connection.reading.header_read, 2);
        server.write_all(&[0, 0, 7, 1]).await.unwrap();
        assert!(futures::poll!(std::pin::pin!(connection.read())).is_pending());
        assert_eq!(connection.reading.bytes_read, 1);
        server.write_all(&[2, 3]).await.unwrap();
        let frame = connection.read().await.unwrap(); assert_eq!(frame.bytes, [1, 2, 3]);
        assert!(memory.used_bytes() > 0); drop(frame); assert_eq!(memory.used_bytes(), 0);
    }
    #[tokio::test]
    async fn oversized_backend_frame_is_rejected_before_payload_allocation() {
        let memory = MemoryBudget::new(1 << 20); let (client, mut server) = tokio::io::duplex(128);
        let mut connection = Connection { socket: Box::new(client), reading: Reading::new(), memory: Arc::clone(&memory), max_message: 1024 };
        server.write_all(&[b'd', 0, 0, 16, 4]).await.unwrap();
        assert!(connection.read().await.is_err()); assert_eq!(memory.used_bytes(), 0);
    }
}
