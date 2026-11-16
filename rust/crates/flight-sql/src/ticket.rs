use std::sync::Arc;
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use rtdb_columnar::MemoryBudget;
use rtdb_node::QueryBasis;
use rtdb_types::Timestamp;
use serde::{Serialize, Deserialize};
use tonic::Status;
use crate::{error::{arrow, invalid, status}, wire};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub(crate) enum Ticket {
    #[serde(rename = "run")]
    Run { #[serde(rename = "dbName")] database: String, sql: String, #[serde(deserialize_with = "required_nullable")] params: Option<String>, basis: Basis },
    #[serde(rename = "empty")]
    Empty,
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
    pub fn run(database: String, sql: String, params: Option<&Bytes>, basis: &QueryBasis) -> Result<Self, Status> { Ok(Self::Run { database, sql, params: params.map(|bytes| STANDARD.encode(bytes)), basis: Basis::capture(basis)? }) }
    pub fn encode(&self, memory: &Arc<MemoryBudget>) -> Result<Bytes, Status> {
        let size = match self { Self::Empty => 128, Self::Run { database, sql, params, basis } => wire::add(1024, wire::add(database.len(), wire::add(sql.len(), wire::add(params.as_ref().map_or(0, String::len), wire::add(basis.snapshot_token.len(), basis.default_tz.len())?)?)?)?)? };
        let charge = memory.reserve(wire::mul(size, 6)?).map_err(status)?;
        Ok(wire::retain(serde_json::to_vec(self).map_err(arrow)?, charge))
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Status> { serde_json::from_slice(bytes).map_err(|_| invalid("invalid ticket")) }
}
pub(crate) fn timestamp(time: Timestamp) -> Result<String, Status> { rtdb_types::Instant::from(time).to_iso_string().map_err(status) }
fn parse_time(value: &str) -> Result<Timestamp, Status> { rtdb_types::Instant::parse_iso(value).and_then(rtdb_types::Instant::as_micros).map_err(|_| invalid("invalid ticket timestamp")) }

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    #[test]
    fn ticket_wire_has_kotlin_discriminator_camelcase_basis_and_explicit_nulls() {
        let memory = MemoryBudget::new(1 << 20);
        let basis = QueryBasis { snapshot: rtdb_types::TimeBasis { databases: BTreeMap::new() }, snapshot_time: None, current_time: Timestamp(1_234_567).into(), timezone: "UTC".into() };
        let bytes = Ticket::run("xtdb".into(), "SELECT 1".into(), None, &basis).unwrap().encode(&memory).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["type"], "run"); assert_eq!(value["dbName"], "xtdb"); assert!(value["params"].is_null()); assert_eq!(value["basis"]["currentTime"], "1970-01-01T00:00:01.234567Z"); assert!(value["basis"]["snapshotTime"].is_null());
        let Ticket::Run { basis: decoded, .. } = Ticket::decode(&bytes).unwrap() else { panic!("run ticket"); }; assert_eq!(decoded.read().unwrap(), basis);
        let mut missing_params = value.clone(); missing_params.as_object_mut().unwrap().remove("params"); assert!(Ticket::decode(&serde_json::to_vec(&missing_params).unwrap()).is_err());
        let mut missing_snapshot_time = value; missing_snapshot_time["basis"].as_object_mut().unwrap().remove("snapshotTime"); assert!(Ticket::decode(&serde_json::to_vec(&missing_snapshot_time).unwrap()).is_err());
        drop(bytes); assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn ticket_current_time_retains_nanoseconds_while_snapshot_time_keeps_microseconds() {
        let memory = MemoryBudget::new(1 << 20);
        for time in [rtdb_types::Instant::new(0,999).unwrap(),rtdb_types::Instant::new(-1,999_999_999).unwrap()] {
            let basis = QueryBasis { snapshot: rtdb_types::TimeBasis { databases: BTreeMap::new() },snapshot_time: Some(Timestamp(-1)),current_time: time,timezone: "UTC".into() };
            let bytes = Ticket::run("xtdb".into(),"SELECT CURRENT_TIMESTAMP(9)".into(),None,&basis).unwrap().encode(&memory).unwrap();
            let Ticket::Run { basis: decoded,.. } = Ticket::decode(&bytes).unwrap() else { panic!() };
            assert_eq!(decoded.read().unwrap(),basis);
        }
        assert_eq!(memory.used_bytes(),0);
    }
    #[test]
    fn empty_ticket_and_unknown_fields_match_kotlin_json_configuration() {
        let memory = MemoryBudget::new(4096); assert_eq!(Ticket::Empty.encode(&memory).unwrap().as_ref(), br#"{"type":"empty"}"#);
        assert!(Ticket::decode(br#"{"type":"empty","unknown":1}"#).is_err()); assert!(Ticket::decode(b"not-json").is_err());
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
