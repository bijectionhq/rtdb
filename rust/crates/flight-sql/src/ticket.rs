use std::sync::Arc;
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::{Buf, Bytes};
use rtdb_columnar::MemoryBudget;
use rtdb_node::QueryBasis;
use rtdb_types::Timestamp;
use serde::{Serialize, Deserialize};
use tonic::Status;
use crate::{error::{invalid, status}, wire};

const VERSION_1: &[u8] = b"RTDBFT\x01";

pub(crate) enum Ticket {
    Run { database: String, sql: String, params: Option<Bytes>, basis: Basis },
    Empty {},
}

// Rolling upgrades and the JVM producer can leave already issued JSON tickets in flight.
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum LegacyTicket {
    #[serde(rename = "run")]
    Run { #[serde(rename = "dbName")] database: String, sql: String, #[serde(deserialize_with = "required_nullable")] params: Option<String>, basis: Basis },
    #[serde(rename = "empty")]
    Empty {},
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Basis { current_time: String, default_tz: String, snapshot_token: String, #[serde(deserialize_with = "required_nullable")] snapshot_time: Option<String> }
fn required_nullable<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(deserializer: D) -> Result<Option<T>, D::Error> { Option::<T>::deserialize(deserializer) }
impl Basis {
    pub fn capture(basis: &QueryBasis) -> Result<Self, Status> { Ok(Self { current_time: basis.current_time.to_iso_string().map_err(status)?, default_tz: basis.timezone.clone(), snapshot_token: basis.snapshot_token(), snapshot_time: basis.snapshot_time.map(timestamp).transpose()? }) }
    pub fn read(&self) -> Result<QueryBasis, Status> { Ok(QueryBasis { snapshot: rtdb_format::basis::decode_time_basis(&self.snapshot_token).map_err(status)?, snapshot_time: self.snapshot_time.as_deref().map(parse_time).transpose()?, current_time: rtdb_types::Instant::parse_iso(&self.current_time).map_err(|_| invalid("invalid ticket timestamp"))?, timezone: rtdb_expr::normalize_timezone(&self.default_tz).map_err(status)? }) }
}
impl Ticket {
    pub fn run(database: String, sql: String, params: Option<Bytes>, basis: &QueryBasis) -> Result<Self, Status> {
        Ok(Self::Run { database, sql, params, basis: Basis::capture(basis)? })
    }

    pub fn encode(&self, memory: &Arc<MemoryBudget>) -> Result<Bytes, Status> {
        let Self::Run { database, sql, params, basis } = self else {
            let reserve = memory.reserve(64).map_err(status)?;
            return Ok(wire::retain(Bytes::from_static(br#"{"type":"empty"}"#), reserve));
        };
        let metadata = [database, sql, &basis.current_time, &basis.default_tz, &basis.snapshot_token];
        let mut size = metadata.iter().try_fold(VERSION_1.len() + 1, |size, item| {
            wire::add(size, wire::add(4, item.len())?)
        })?;
        if let Some(time) = &basis.snapshot_time { size = wire::add(size, wire::add(4, time.len())?)?; }
        size = wire::add(size, params.as_ref().map_or(0, Bytes::len))?;
        let charge = memory.reserve(wire::add(size, 128)?).map_err(status)?;
        let mut bytes = Vec::with_capacity(size);
        bytes.extend_from_slice(VERSION_1);
        bytes.push(u8::from(basis.snapshot_time.is_some()) | (u8::from(params.is_some()) << 1));
        for value in metadata { put_string(&mut bytes, value)?; }
        if let Some(time) = &basis.snapshot_time { put_string(&mut bytes, time)?; }
        if let Some(params) = params { bytes.extend_from_slice(params); }
        Ok(wire::retain(bytes, charge))
    }

    pub fn decode(mut bytes: Bytes) -> Result<Self, Status> {
        if !bytes.starts_with(VERSION_1) {
            let ticket = serde_json::from_slice::<LegacyTicket>(&bytes).map_err(|_| invalid("invalid ticket"))?;
            return Ok(match ticket {
                LegacyTicket::Empty {} => Self::Empty {},
                LegacyTicket::Run { database, sql, params, basis } => {
                    let params = params.map(|value| STANDARD.decode(value).map(Bytes::from).map_err(|_| invalid("invalid ticket parameters"))).transpose()?;
                    Self::Run { database, sql, params, basis }
                }
            });
        }
        bytes.advance(VERSION_1.len());
        if bytes.is_empty() { return Err(invalid("truncated ticket")); }
        let flags = bytes.get_u8();
        if flags & !0b11 != 0 { return Err(invalid("invalid ticket flags")); }
        let database = take_string(&mut bytes)?;
        let sql = take_string(&mut bytes)?;
        let current_time = take_string(&mut bytes)?;
        let default_tz = take_string(&mut bytes)?;
        let snapshot_token = take_string(&mut bytes)?;
        let snapshot_time = (flags & 1 != 0).then(|| take_string(&mut bytes)).transpose()?;
        let basis = Basis { current_time, default_tz, snapshot_token, snapshot_time };
        if flags & 2 == 0 && !bytes.is_empty() { return Err(invalid("unexpected ticket parameters")); }
        Ok(Self::Run { database, sql, params: (flags & 2 != 0).then_some(bytes), basis })
    }
}

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<(), Status> {
    let length = u32::try_from(value.len()).map_err(|_| invalid("ticket field is too large"))?;
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take_string(bytes: &mut Bytes) -> Result<String, Status> {
    if bytes.len() < 4 { return Err(invalid("truncated ticket length")); }
    let length = bytes.get_u32() as usize;
    let value = bytes.get(..length).ok_or_else(|| invalid("truncated ticket field"))?;
    let value = std::str::from_utf8(value).map_err(|_| invalid("invalid ticket text"))?.to_owned();
    bytes.advance(length);
    Ok(value)
}

pub(crate) fn timestamp(time: Timestamp) -> Result<String, Status> { rtdb_types::Instant::from(time).to_iso_string().map_err(status) }
fn parse_time(value: &str) -> Result<Timestamp, Status> { rtdb_types::Instant::parse_iso(value).and_then(rtdb_types::Instant::as_micros).map_err(|_| invalid("invalid ticket timestamp")) }

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    #[test]
    fn binary_ticket_borrows_ipc_and_reads_legacy_json() {
        let memory = MemoryBudget::new(1 << 20);
        let basis = QueryBasis { snapshot: rtdb_types::TimeBasis { databases: BTreeMap::new() }, snapshot_time: None, current_time: Timestamp(1_234_567).into(), timezone: "UTC".into() };
        let ipc = Bytes::from_static(b"\xff\x00\x83\x59");
        let bytes = Ticket::run("xtdb".into(), "SELECT ?".into(), Some(ipc.clone()), &basis).unwrap().encode(&memory).unwrap();
        let Ticket::Run { basis: decoded, params: Some(params), .. } = Ticket::decode(bytes.clone()).unwrap() else { panic!("run ticket"); };
        assert_eq!(decoded.read().unwrap(), basis);
        assert_eq!(params, ipc);
        assert_eq!(params.as_ptr(), bytes[bytes.len() - ipc.len()..].as_ptr());
        drop(params);
        let value = serde_json::to_value(LegacyTicket::Run { database: "xtdb".into(), sql: "SELECT ?".into(), params: Some(STANDARD.encode(&ipc)), basis: Basis::capture(&basis).unwrap() }).unwrap();
        let Ticket::Run { basis: decoded, params: Some(params), .. } = Ticket::decode(serde_json::to_vec(&value).unwrap().into()).unwrap() else { panic!("legacy ticket"); };
        assert_eq!(decoded.read().unwrap(), basis);
        assert_eq!(params, ipc);
        let mut missing_params = value.clone(); missing_params.as_object_mut().unwrap().remove("params"); assert!(Ticket::decode(serde_json::to_vec(&missing_params).unwrap().into()).is_err());
        let mut missing_snapshot_time = value; missing_snapshot_time["basis"].as_object_mut().unwrap().remove("snapshotTime"); assert!(Ticket::decode(serde_json::to_vec(&missing_snapshot_time).unwrap().into()).is_err());
        assert!(Ticket::decode(bytes.slice(..VERSION_1.len())).is_err());
        drop(bytes); assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn ticket_current_time_retains_nanoseconds_while_snapshot_time_keeps_microseconds() {
        let memory = MemoryBudget::new(1 << 20);
        for time in [rtdb_types::Instant::new(0,999).unwrap(),rtdb_types::Instant::new(-1,999_999_999).unwrap()] {
            let basis = QueryBasis { snapshot: rtdb_types::TimeBasis { databases: BTreeMap::new() },snapshot_time: Some(Timestamp(-1)),current_time: time,timezone: "UTC".into() };
            let bytes = Ticket::run("xtdb".into(),"SELECT CURRENT_TIMESTAMP(9)".into(),None,&basis).unwrap().encode(&memory).unwrap();
            let Ticket::Run { basis: decoded,.. } = Ticket::decode(bytes).unwrap() else { panic!() };
            assert_eq!(decoded.read().unwrap(),basis);
        }
        assert_eq!(memory.used_bytes(),0);
    }
    #[test]
    fn empty_ticket_and_unknown_fields_match_kotlin_json_configuration() {
        let memory = MemoryBudget::new(4096); assert_eq!(Ticket::Empty {}.encode(&memory).unwrap().as_ref(), br#"{"type":"empty"}"#);
        assert!(Ticket::decode(Bytes::from_static(br#"{"type":"empty","unknown":1}"#)).is_err());
        assert!(Ticket::decode(Bytes::from_static(b"not-json")).is_err());
    }
    #[test]
    fn ticket_instant_text_preserves_full_microsecond_domain() {
        for time in [Timestamp::MIN,Timestamp(-1),Timestamp(0),Timestamp::MAX] {
            assert_eq!(parse_time(&timestamp(time).unwrap()).unwrap(),time);
        }
        assert_eq!(parse_time("1970-01-01T00:00:00.000000999Z").unwrap(),Timestamp(0));
        assert!(parse_time("1970-01-01T00:00Z").is_err());
    }
}
