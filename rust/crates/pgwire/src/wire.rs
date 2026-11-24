use std::collections::BTreeMap;
use std::sync::Arc;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_pg_codec::{EncodedValue, Format};
use rtdb_types::{Anomaly, PgType, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use crate::error::{network, protocol};

pub(crate) fn delivery_error(error: Anomaly) -> Anomaly {
    if matches!(error.code.as_str(), "rtdb.storage/plaintext-delivery-expired" | "rtdb.storage/read-lease-expired") {
        network(error.message.as_deref().unwrap_or("The plaintext delivery grant expired"))
    } else {
        error
    }
}

pub(crate) const STARTUP_LIMIT: usize = 10_000;
/// ErrorResponse field carrying a placement interruption code in every detail format.
pub const ANOMALY_CODE_FIELD: u8 = b'y';

pub(crate) struct Frame { pub tag: u8, pub body: Vec<u8>, pub charge: Arc<MemoryReservation> }
pub(crate) struct Reader<R> { input: R, memory: Arc<MemoryBudget> }
impl<R: AsyncRead + Unpin> Reader<R> {
    pub fn new(input: R, memory: Arc<MemoryBudget>) -> Self { Self { input, memory } }
    pub async fn read(&mut self, limit: usize) -> Result<Option<Frame>> {
        let mut tag = [0u8; 1]; if self.input.read(&mut tag).await.map_err(network)? == 0 { return Ok(None); }
        self.body(tag[0], limit).await.map(Some)
    }
    pub async fn startup(&mut self, extra: usize) -> Result<Frame> { self.body(0, STARTUP_LIMIT + extra).await }
    async fn body(&mut self, tag: u8, limit: usize) -> Result<Frame> {
        let length = self.input.read_i32().await.map_err(network)?;
        let length = usize::try_from(length).ok().and_then(|n| n.checked_sub(4)).filter(|n| *n <= limit).ok_or_else(|| protocol("Invalid message length"))?;
        let bound = length.checked_mul(16).and_then(|n| n.checked_add(8192)).ok_or_else(|| protocol("Message allocation exceeds addressable memory"))?;
        let charge = self.memory.reserve(bound)?;
        let mut body = vec![0; length]; self.input.read_exact(&mut body).await.map_err(network)?;
        Ok(Frame { tag, body, charge })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Column { pub name: String, pub ty: PgType, pub format: Format }
pub(crate) struct Writer<W> { output: W, memory: Arc<MemoryBudget>, gateway: bool }
impl<W: AsyncWrite + Unpin> Writer<W> {
    pub fn new(output: W, memory: Arc<MemoryBudget>) -> Self { Self { output, memory, gateway: false } }
    pub fn through_gateway(mut self, enabled: bool) -> Self { self.gateway = enabled; self }
    pub async fn raw(&mut self, bytes: &[u8]) -> Result<()> { self.output.write_all(bytes).await.map_err(network)?; self.flush().await }
    pub async fn flush(&mut self) -> Result<()> { self.output.flush().await.map_err(network) }
    pub async fn send(&mut self, tag: u8, body: &[u8]) -> Result<()> {
        let length = i32::try_from(body.len().checked_add(4).ok_or_else(|| protocol("Output message too large"))?).map_err(|_| protocol("Output message too large"))?;
        if self.gateway && let Some(deadline) = rtdb_query::current_delivery_deadline() {
            let cutoff = deadline.wire_nanos().ok_or_else(|| network("The answer deadline cannot be relayed"))?;
            let mut stamp = [0; 9];
            stamp[0] = crate::GATEWAY_ANSWER_DEADLINE_KIND;
            stamp[1..].copy_from_slice(&cutoff.to_be_bytes());
            self.packet(crate::GATEWAY_SESSION_STATE_TAG, 13, &stamp).await?;
        }
        self.packet(tag, length, body).await
    }
    async fn packet(&mut self, tag: u8, length: i32, body: &[u8]) -> Result<()> {
        self.output.write_u8(tag).await.map_err(network)?;
        self.output.write_i32(length).await.map_err(network)?;
        self.output.write_all(body).await.map_err(network)
    }
    pub async fn empty(&mut self, tag: u8) -> Result<()> { self.send(tag, &[]).await }
    pub async fn string(&mut self, tag: u8, text: &str) -> Result<()> { self.record(tag, text.len() + 1, |bytes| cstring(bytes, text)).await }
    pub async fn auth(&mut self, method: i32) -> Result<()> { self.send(b'R', &method.to_be_bytes()).await?; self.flush().await }
    pub async fn parameter(&mut self, key: &str, value: &str) -> Result<()> { self.record(b'S', key.len() + value.len() + 2, |bytes| { cstring(bytes, key)?; cstring(bytes, value) }).await?; self.flush().await }
    pub async fn gateway_await_token(&mut self, token: Option<&str>) -> Result<()> {
        self.record(crate::GATEWAY_SESSION_STATE_TAG, 1 + token.map_or(0, str::len), |body| {
            body.push(u8::from(token.is_some()));
            if let Some(token) = token { body.extend_from_slice(token.as_bytes()); }
            Ok(())
        }).await
    }
    pub async fn gateway_settings(&mut self, encoded: &[u8]) -> Result<()> {
        self.record(crate::GATEWAY_SESSION_STATE_TAG, 1 + encoded.len(), |body| {
            body.push(crate::GATEWAY_SETTINGS_KIND);
            body.extend_from_slice(encoded);
            Ok(())
        }).await
    }
    pub async fn gateway_sql_prepare(&mut self, name: &str, source: &str, search_path: &str, timezone: &str) -> Result<()> {
        let length = [name, source, search_path, timezone].into_iter().try_fold(1usize, |length, field| length.checked_add(field.len())?.checked_add(1))
            .ok_or_else(|| protocol("Gateway PREPARE state is too large"))?;
        self.record(crate::GATEWAY_SESSION_STATE_TAG, length, |body| {
            body.push(crate::GATEWAY_SQL_PREPARE_KIND);
            for field in [name, source, search_path, timezone] { cstring(body, field)?; }
            Ok(())
        }).await
    }
    pub async fn gateway_named_parse(&mut self, name: &str, search_path: &str, timezone: &str, parse: &[u8]) -> Result<()> {
        let length = [name, search_path, timezone].into_iter().try_fold(1usize, |length, field| length.checked_add(field.len())?.checked_add(1))
            .and_then(|length| length.checked_add(parse.len())).ok_or_else(|| protocol("Gateway Parse state is too large"))?;
        self.record(crate::GATEWAY_SESSION_STATE_TAG, length, |body| {
            body.push(crate::GATEWAY_NAMED_PARSE_KIND);
            for field in [name, search_path, timezone] { cstring(body, field)?; }
            body.extend_from_slice(parse);
            Ok(())
        }).await
    }
    pub async fn ready(&mut self, status: u8) -> Result<()> { self.send(b'Z', &[status]).await?; self.flush().await }
    pub async fn backend_key(&mut self, process: i32) -> Result<()> { self.record(b'K', 8, |bytes| { bytes.extend_from_slice(&process.to_be_bytes()); bytes.extend_from_slice(&0i32.to_be_bytes()); Ok(()) }).await }
    pub async fn parameters(&mut self, oids: &[i32]) -> Result<()> { self.record(b't', 2 + oids.len() * 4, |bytes| { count(bytes, oids.len())?; for oid in oids { bytes.extend_from_slice(&(if *oid == 0 { 25 } else { *oid }).to_be_bytes()); } Ok(()) }).await }
    pub async fn columns(&mut self, columns: &[Column], fallback: PgType) -> Result<()> {
        let bound = columns.iter().try_fold(2usize, |n, column| n.checked_add(column.name.len() + 19).ok_or_else(|| protocol("Column description too large")))?;
        self.record(b'T', bound, |bytes| { count(bytes, columns.len())?; for column in columns { let ty = if column.ty == PgType::Default { fallback } else { column.ty }; let descriptor = ty.descriptor(); cstring(bytes, &column.name)?; bytes.extend_from_slice(&0i32.to_be_bytes()); bytes.extend_from_slice(&0i16.to_be_bytes()); bytes.extend_from_slice(&descriptor.oid.to_be_bytes()); bytes.extend_from_slice(&descriptor.length.to_be_bytes()); bytes.extend_from_slice(&(-1i32).to_be_bytes()); bytes.extend_from_slice(&column.format.code().to_be_bytes()); } Ok(()) }).await
    }
    /// Writes a data row whose column count and cells, each a length then its bytes, `body` already holds.
    pub async fn row_body(&mut self, body: &[u8]) -> Result<()> { self.send(b'D', body).await }
    pub async fn row(&mut self, values: &[Option<EncodedValue>]) -> Result<()> {
        let length = values.iter().try_fold(6usize, |n, value| n.checked_add(4 + value.as_ref().map_or(0, |value| value.as_slice().len())).ok_or_else(|| protocol("Row message too large")))?;
        self.output.write_u8(b'D').await.map_err(network)?; self.output.write_i32(i32::try_from(length).map_err(|_| protocol("Row message too large"))?).await.map_err(network)?;
        self.output.write_u16(u16::try_from(values.len()).map_err(|_| protocol("Too many columns"))?).await.map_err(network)?;
        for value in values { match value { None => self.output.write_i32(-1).await.map_err(network)?, Some(value) => { self.output.write_i32(i32::try_from(value.as_slice().len()).map_err(|_| protocol("Value too large"))?).await.map_err(network)?; self.output.write_all(value.as_slice()).await.map_err(network)?; } } }
        Ok(())
    }
    pub async fn error(&mut self, error: &Anomaly, fallback: PgType, fatal: bool) -> Result<()> {
        let retained = error.message.as_ref().map_or(0, String::len).checked_add(error.code.len()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| protocol("Error detail too large"))?;
        let retained = error.context.iter().try_fold(retained, |bytes, (key, value)| bytes.checked_add(key.len()).and_then(|n| n.checked_add(json_bytes(value).ok()?)).ok_or_else(|| protocol("Error detail too large")))?;
        let _scratch = self.memory.reserve(retained.checked_mul(32).and_then(|n| n.checked_add(8192)).ok_or_else(|| protocol("Error detail too large"))?)?;
        let detail = if error.context.contains_key("sql-state") { None } else { rtdb_pg_codec::encode_error(error, fallback).ok().and_then(|bytes| String::from_utf8(bytes).ok()) };
        let severity = if fatal { "FATAL" } else { "ERROR" };
        let mut fields = vec![(b'S', severity), (b'V', severity), (b'C', crate::error::state(error)), (b'M', error.message.as_deref().unwrap_or(""))];
        if let Some(detail) = &detail { fields.push((b'D', detail)); }
        if matches!(error.code.as_str(), rtdb_node::NOT_HELD | "xtdb/ingestion-stopped" | "rtdb.storage/closed") { fields.push((ANOMALY_CODE_FIELD, &error.code)); }
        if matches!(error.code.as_str(), "xtdb/prepared-query-out-of-date" | "prepared-query-out-of-date") { fields.push((b'R', "RevalidateCachedQuery")); }
        self.fields(b'E', &fields).await
    }
    pub async fn notice(&mut self, message: &str) -> Result<()> { self.fields(b'N', &[(b'S', "NOTICE"), (b'V', "NOTICE"), (b'C', "00000"), (b'M', message)]).await }
    pub async fn planning_warning(&mut self, message: &str) -> Result<()> { self.fields(b'N', &[(b'S', "WARNING"), (b'V', "WARNING"), (b'C', "01000"), (b'M', message)]).await }
    async fn fields(&mut self, tag: u8, fields: &[(u8, &str)]) -> Result<()> { let bound = fields.iter().try_fold(1usize, |n, (_, text)| n.checked_add(text.len() + 2).ok_or_else(|| protocol("Error fields too large")))?; self.record(tag, bound, |bytes| { for (tag, value) in fields { bytes.push(*tag); cstring(bytes, value)?; } bytes.push(0); Ok(()) }).await?; self.flush().await }
    async fn record(&mut self, tag: u8, length: usize, fill: impl FnOnce(&mut Vec<u8>) -> Result<()>) -> Result<()> { let _charge = self.memory.reserve(length + 256)?; let mut body = Vec::with_capacity(length); fill(&mut body)?; self.send(tag, &body).await }
    #[cfg(test)] pub fn into_inner(self) -> W { self.output }
}

pub(crate) fn cstring(bytes: &mut Vec<u8>, value: &str) -> Result<()> { if value.contains('\0') { return Err(protocol("PostgreSQL string contains a zero byte")); } bytes.extend_from_slice(value.as_bytes()); bytes.push(0); Ok(()) }
fn count(bytes: &mut Vec<u8>, value: usize) -> Result<()> { bytes.extend_from_slice(&u16::try_from(value).map_err(|_| protocol("PostgreSQL list exceeds u16"))?.to_be_bytes()); Ok(()) }
fn json_bytes(value: &serde_json::Value) -> Result<usize> { match value { serde_json::Value::String(value) => Ok(value.capacity() + 64), serde_json::Value::Array(values) => values.iter().try_fold(values.capacity() * 64, |bytes, value| bytes.checked_add(json_bytes(value)?).ok_or_else(|| protocol("Error detail too large"))), serde_json::Value::Object(values) => values.iter().try_fold(256, |bytes: usize, (key, value)| bytes.checked_add(key.capacity()).and_then(|n| n.checked_add(json_bytes(value).ok()?)).and_then(|n| n.checked_add(256)).ok_or_else(|| protocol("Error detail too large"))), _ => Ok(64) } }

pub(crate) struct Input<'a> { bytes: &'a [u8] }
impl<'a> Input<'a> {
    pub fn new(bytes: &'a [u8]) -> Self { Self { bytes } }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> { let (head, tail) = self.bytes.split_at_checked(n).ok_or_else(|| protocol("Truncated client message"))?; self.bytes = tail; Ok(head) }
    pub fn byte(&mut self) -> Result<u8> { Ok(self.take(1)?[0]) }
    pub fn short(&mut self) -> Result<u16> { Ok(u16::from_be_bytes(self.take(2)?.try_into().map_err(|_| protocol("Truncated int16"))?)) }
    pub fn integer(&mut self) -> Result<i32> { Ok(i32::from_be_bytes(self.take(4)?.try_into().map_err(|_| protocol("Truncated int32"))?)) }
    pub fn string(&mut self) -> Result<&'a str> { let length = self.bytes.iter().position(|byte| *byte == 0).ok_or_else(|| protocol("Unterminated client string"))?; let value = self.take(length + 1)?; std::str::from_utf8(&value[..length]).map_err(|_| protocol("Client string is not UTF-8")) }
    pub fn end(self) -> Result<()> { if self.bytes.is_empty() { Ok(()) } else { Err(protocol("Trailing bytes in client message")) } }
    pub fn remaining(&self) -> &'a [u8] { self.bytes }
    pub fn startup(&mut self) -> Result<BTreeMap<String, String>> { let mut params = BTreeMap::new(); while self.bytes.first() != Some(&0) { let key = self.string()?.to_owned(); let value = self.string()?.to_owned(); params.insert(key, value); } self.byte()?; Ok(params) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn nullable_error_primary_text_and_structured_details_preserve_their_policies() {
        for message in [None, Some(String::new()), Some("detail".into())] {
            let error = Anomaly::new_nullable(rtdb_types::AnomalyCategory::Fault, "test/error", message.clone());
            for fallback in [PgType::Json, PgType::JsonLd, PgType::Transit] {
                let memory = MemoryBudget::new(1 << 20);
                let mut writer = Writer::new(Vec::new(), Arc::clone(&memory));
                writer.error(&error, fallback, false).await.unwrap();
                let bytes = writer.into_inner();
                assert_eq!(bytes[0], b'E');
                assert_eq!(i32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize, bytes.len() - 1);
                let mut input = Input::new(&bytes[5..]);
                let mut fields = BTreeMap::new();
                loop { let tag = input.byte().unwrap(); if tag == 0 { break; } fields.insert(tag, input.string().unwrap()); }
                input.end().unwrap();
                assert_eq!(fields[&b'M'], message.as_deref().unwrap_or(""));
                assert!(!fields.contains_key(&ANOMALY_CODE_FIELD));
                assert_eq!(fields[&b'D'].as_bytes(), rtdb_pg_codec::encode_error(&error, fallback).unwrap());
                assert_eq!(error.message, message);
                assert_eq!(memory.used_bytes(), 0);
            }
        }
    }

    #[tokio::test]
    async fn placement_error_codes_are_independent_of_the_detail_format() {
        for code in [rtdb_node::NOT_HELD, "xtdb/ingestion-stopped", "rtdb.storage/closed"] {
            let error = Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, code, "holder stopped");
            for fallback in [PgType::Json, PgType::JsonLd, PgType::Transit] {
                let mut writer = Writer::new(Vec::new(), MemoryBudget::new(1 << 20));
                writer.error(&error, fallback, false).await.unwrap();
                let bytes = writer.into_inner();
                assert!(bytes[5..].split(|byte| *byte == 0)
                    .any(|field| field.strip_prefix(&[ANOMALY_CODE_FIELD]) == Some(code.as_bytes())));
            }
        }
    }

    #[tokio::test]
    async fn declared_length_is_rejected_before_payload_reservation() {
        let memory = MemoryBudget::new(1 << 20);
        for length in [-1i32, 0, 3, 10_005] {
            let bytes = length.to_be_bytes();
            let mut reader = Reader::new(bytes.as_slice(), Arc::clone(&memory));
            assert!(reader.startup(0).await.is_err()); assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn frame_ownership_keeps_reservation_until_last_retained_owner_drops() {
        let memory = MemoryBudget::new(1 << 20); let bytes = b"Q\0\0\0\x06x\0";
        let mut reader = Reader::new(bytes.as_slice(), Arc::clone(&memory));
        let frame = reader.read(100).await.unwrap().unwrap(); assert_eq!(frame.body, b"x\0");
        let owner = Arc::clone(&frame.charge); drop(frame); assert!(memory.used_bytes() > 0);
        drop(owner); assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn malformed_cstrings_and_payload_tails_are_protocol_errors() {
        assert!(Input::new(b"unterminated").string().is_err());
        assert!(Input::new(b"\xff\0").string().is_err());
        assert!(Input::new(b"\0").end().is_err());
        assert!(Input::new(&[]).integer().is_err());
        assert!(Input::new(b"name\0value\0").startup().is_err());
    }
}
