use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use rtdb_columnar::MemoryBudget;
use rtdb_node::QueryBasis;
use rtdb_types::{Instant, TimeBasis, Timestamp};
use serde_json::{Map, Value};
use sha2::Sha256;
use tonic::Status;

use crate::{error::{invalid, status}, state::Owner, wire};

const MAC_BYTES: usize = 32;
const NONCE_BYTES: usize = 16;

/// A self-contained query ticket in the reference's `QueryTicket` JSON; nothing about an issued ticket is retained by the server.
/// A run ticket is unsigned and redeemable by any session, and after a restart, as the reference's `QueryTicket.Run` is: it names its databases
/// by name and basis alone, the basis as the reference's `QueryBasis` with its XTDB snapshot token. An inspection, which the reference
/// does not have, is signed for its session, and so dies with it.
pub(crate) enum Ticket {
    Run(QueryRun),
    Inspect(Inspection),
    Empty,
}

/// A statement query and the read basis it was described at.
pub(crate) struct QueryRun {
    pub database: String,
    pub sql: String,
    pub search_path: Vec<String>,
    pub basis: TicketBasis,
    pub params: Option<Bytes>,
}

/// A control inspection such as `SHOW`, executed again on each redemption on the connection that described it.
pub(crate) struct Inspection {
    pub database: String,
    pub transaction: Option<Bytes>,
    pub sql: String,
    pub search_path: Vec<String>,
}

pub(crate) struct TicketBasis {
    pub current_time: Instant,
    pub timezone: String,
    pub snapshot_time: Option<Timestamp>,
    pub has_await_floor: bool,
    pub cut: Cut,
}

pub(crate) enum Cut {
    /// A restored SQL snapshot, which the token names.
    Snapshot(String),
    /// Each database's last visible system time, by name; `None`, or a database absent from the map, sees no transaction.
    Visible(BTreeMap<String, Option<Timestamp>>),
}

impl TicketBasis {
    /// `databases` are those the node now holds, so that one attached after the cut was captured stays empty.
    pub fn query_basis(&self, databases: BTreeSet<String>) -> QueryBasis {
        let (snapshot, resume_token) = match &self.cut {
            Cut::Snapshot(token) => (TimeBasis::default(), Some(token.clone())),
            Cut::Visible(visible) => (TimeBasis { databases: databases.into_iter().chain(visible.keys().cloned()).map(|name| {
                let time = visible.get(&name).copied().flatten().unwrap_or(Timestamp::MIN);
                (name, vec![Some(Instant::from(time))])
            }).collect() }, None),
        };
        QueryBasis {
            snapshot, replica_capture: None, resume_token, has_await_floor: self.has_await_floor,
            snapshot_time: self.snapshot_time, current_time: self.current_time, timezone: self.timezone.clone(),
        }
    }
}

/// Binds an inspection ticket to the session that described it, under the service's ticket key.
pub(crate) struct Binding<'a> { pub mac: Hmac<Sha256>, pub session: &'a str, pub owner: Owner }

const SIGN: u8 = 0;
const MASK: u8 = 1;

impl Binding<'_> {
    fn keyed(&self, purpose: u8) -> Hmac<Sha256> {
        let mut mac = self.mac.clone();
        mac.update(&[purpose]);
        mac.update(&(self.session.len() as u64).to_be_bytes());
        mac.update(self.session.as_bytes());
        match self.owner {
            Owner::Anonymous => mac.update(&[0]),
            Owner::Authenticated(id) => { mac.update(&[1]); mac.update(id.as_bytes()); }
        }
        mac
    }

    fn sign(&self, payload: &[u8]) -> Hmac<Sha256> {
        let mut mac = self.keyed(SIGN);
        mac.update(payload);
        mac
    }

    /// XORs `value` with a keystream bound to this session and the ticket's `nonce`, so that a ticket discloses neither the transaction handle it names nor how it relates to another ticket's.
    fn mask(&self, nonce: &[u8; NONCE_BYTES], value: &[u8]) -> Vec<u8> {
        value.chunks(MAC_BYTES).enumerate().flat_map(|(block, chunk)| {
            let mut mac = self.keyed(MASK);
            mac.update(nonce);
            mac.update(&(block as u64).to_be_bytes());
            let stream = mac.finalize().into_bytes();
            chunk.iter().zip(stream).map(|(byte, key)| byte ^ key).collect::<Vec<_>>()
        }).collect()
    }
}

impl Ticket {
    pub fn empty(memory: &Arc<MemoryBudget>) -> Result<Bytes, Status> {
        let charge = memory.reserve(64).map_err(status)?;
        Ok(wire::retain(EMPTY.as_bytes().to_vec(), charge))
    }

    /// An upper bound on the encoded ticket: JSON escapes a byte to at most six, and base64 grows by a third.
    pub fn encoded_bound(&self) -> Result<usize, Status> {
        let text = |length: usize| wire::add(wire::mul(length, 6)?, 8);
        let base64 = |length: usize| wire::add(wire::mul(length.div_ceil(3), 4)?, 8);
        let strings = |values: &[String]| values.iter().try_fold(8usize, |bytes, value| wire::add(bytes, text(value.len())?));
        let body = match self {
            Self::Empty => 0,
            Self::Inspect(inspection) => [text(inspection.database.len())?, text(inspection.sql.len())?, strings(&inspection.search_path)?,
                base64(inspection.transaction.as_ref().map_or(0, Bytes::len))?, base64(NONCE_BYTES)?, base64(MAC_BYTES)?]
                .into_iter().try_fold(0usize, wire::add)?,
            Self::Run(run) => {
                let cut = match &run.basis.cut {
                    Cut::Snapshot(token) => text(token.len())?,
                    Cut::Visible(databases) => base64(databases.keys().try_fold(0usize, |bytes, name| wire::add(bytes, wire::add(name.len(), 32)?))?)?,
                };
                [text(run.database.len())?, text(run.sql.len())?, strings(&run.search_path)?, text(run.basis.timezone.len())?, cut,
                    base64(run.params.as_ref().map_or(0, Bytes::len))?]
                    .into_iter().try_fold(0usize, wire::add)?
            }
        };
        wire::add(body, 256)
    }

    pub fn encode(&self, binding: &Binding<'_>, memory: &Arc<MemoryBudget>) -> Result<Bytes, Status> {
        let bound = self.encoded_bound()?;
        let charge = memory.reserve(wire::add(wire::mul(bound, 2)?, 128)?).map_err(status)?;
        let mut object = Map::new();
        match self {
            Self::Empty => return Self::empty(memory),
            Self::Inspect(inspection) => {
                let nonce = *uuid::Uuid::new_v4().as_bytes();
                let transaction = inspection.transaction.as_deref().map(|handle| binding.mask(&nonce, handle));
                let tag = binding.sign(&inspection_payload(&inspection.database, &nonce, transaction.as_deref(), &inspection.sql, &inspection.search_path)?).finalize().into_bytes();
                object.insert("type".into(), "inspect".into());
                object.insert("dbName".into(), inspection.database.clone().into());
                object.insert("sql".into(), inspection.sql.clone().into());
                object.insert("searchPath".into(), inspection.search_path.clone().into());
                object.insert("nonce".into(), STANDARD.encode(nonce).into());
                object.insert("transaction".into(), transaction.map_or(Value::Null, |handle| STANDARD.encode(handle).into()));
                object.insert("mac".into(), STANDARD.encode(tag).into());
            }
            Self::Run(run) => {
                let basis = &run.basis;
                let snapshot_token = match &basis.cut {
                    Cut::Snapshot(token) => token.clone(),
                    Cut::Visible(databases) => rtdb_format::basis::encode_time_basis(&TimeBasis {
                        databases: databases.iter().map(|(name, through)| (name.clone(), vec![through.map(Instant::from)])).collect(),
                    }),
                };
                let mut query_basis = Map::new();
                query_basis.insert("currentTime".into(), basis.current_time.to_iso_string().map_err(status)?.into());
                query_basis.insert("defaultTz".into(), basis.timezone.clone().into());
                query_basis.insert("snapshotToken".into(), snapshot_token.into());
                query_basis.insert("snapshotTime".into(), basis.snapshot_time.map(timestamp).transpose()?.map_or(Value::Null, Value::from));
                object.insert("type".into(), "run".into());
                object.insert("dbName".into(), run.database.clone().into());
                object.insert("sql".into(), run.sql.clone().into());
                object.insert("params".into(), run.params.as_ref().map_or(Value::Null, |params| STANDARD.encode(params).into()));
                object.insert("basis".into(), query_basis.into());
                // RTDB-only, and written only when they differ from what a ticket without them means, so that the reference's
                // decoder, which refuses unknown keys, reads every ticket that does not depend on them.
                if run.search_path != default_search_path() { object.insert("searchPath".into(), run.search_path.clone().into()); }
                if basis.has_await_floor { object.insert("awaitFloor".into(), true.into()); }
            }
        }
        let mut bytes = Vec::with_capacity(bound);
        serde_json::to_writer(&mut bytes, &Value::Object(object)).map_err(|error| Status::internal(error.to_string()))?;
        if bytes.len() > bound { return Err(Status::internal("Query ticket exceeds its encoding reservation")); }
        Ok(wire::retain(bytes, charge))
    }

    /// An inspection signed for another session or by another service reads as unknown; a run ticket decodes for any session.
    /// Anything else, including a ticket an earlier RTDB issued in its binary format, is an invalid ticket, as the reference reports one.
    pub fn decode(bytes: &Bytes, binding: &Binding<'_>) -> Result<Self, Status> {
        let Ok(Value::Object(mut object)) = serde_json::from_slice::<Value>(bytes) else { return Err(invalid_ticket()); };
        let ticket = match take_string(&mut object, "type")?.as_str() {
            "empty" => Self::Empty,
            "run" => {
                let database = take_string(&mut object, "dbName")?;
                let sql = take_string(&mut object, "sql")?;
                let params = take_base64(&mut object, "params")?.map(Bytes::from);
                let Some(Value::Object(mut basis)) = object.remove("basis") else { return Err(invalid_ticket()); };
                let current_time = Instant::parse_iso(&take_string(&mut basis, "currentTime")?).map_err(|_| invalid_ticket())?;
                let timezone = take_string(&mut basis, "defaultTz")?;
                let snapshot_token = take_string(&mut basis, "snapshotToken")?;
                let snapshot_time = match basis.remove("snapshotTime") {
                    Some(Value::Null) => None,
                    Some(Value::String(time)) => Some(micros(&time)?),
                    _ => return Err(invalid_ticket()),
                };
                if !basis.is_empty() { return Err(invalid_ticket()); }
                let search_path = match object.remove("searchPath") { None => default_search_path(), Some(value) => strings(value)? };
                let has_await_floor = match object.remove("awaitFloor") { None => false, Some(Value::Bool(flag)) => flag, Some(_) => return Err(invalid_ticket()) };
                let cut = match rtdb_format::basis::decode_time_basis(&snapshot_token) {
                    Ok(time_basis) => Cut::Visible(time_basis.databases.into_iter().map(|(name, times)| match times.as_slice() {
                        [through] => Ok((name, through.map(Instant::as_micros).transpose().map_err(|_| invalid_ticket())?)),
                        _ => Err(invalid_ticket()),
                    }).collect::<Result<_, Status>>()?),
                    Err(_) => Cut::Snapshot(snapshot_token),
                };
                Self::Run(QueryRun { database, sql, search_path, basis: TicketBasis { current_time, timezone, snapshot_time, has_await_floor, cut }, params })
            }
            "inspect" => {
                let database = take_string(&mut object, "dbName")?;
                let sql = take_string(&mut object, "sql")?;
                let search_path = strings(object.remove("searchPath").ok_or_else(invalid_ticket)?)?;
                let nonce: [u8; NONCE_BYTES] = take_base64(&mut object, "nonce")?.and_then(|nonce| nonce.try_into().ok()).ok_or_else(invalid_ticket)?;
                let transaction = take_base64(&mut object, "transaction")?;
                let tag = take_base64(&mut object, "mac")?.ok_or_else(invalid_ticket)?;
                binding.sign(&inspection_payload(&database, &nonce, transaction.as_deref(), &sql, &search_path)?).verify_slice(&tag)
                    .map_err(|_| Status::not_found("unknown query ticket"))?;
                let transaction = transaction.map(|handle| Bytes::from(binding.mask(&nonce, &handle)));
                Self::Inspect(Inspection { database, transaction, sql, search_path })
            }
            _ => return Err(invalid_ticket()),
        };
        if !object.is_empty() { return Err(invalid_ticket()); }
        Ok(ticket)
    }
}

/// The reference's `QueryTicket.Empty`.
const EMPTY: &str = r#"{"type":"empty"}"#;

fn invalid_ticket() -> Status { invalid("invalid ticket") }

fn default_search_path() -> Vec<String> { rtdb_sql::PrepareOptions::default().search_path }

/// What an inspection's tag signs: every field, each length-prefixed, so that no two tickets share a payload.
fn inspection_payload(database: &str, nonce: &[u8; NONCE_BYTES], transaction: Option<&[u8]>, sql: &str, search_path: &[String]) -> Result<Vec<u8>, Status> {
    let mut payload = Vec::new();
    let mut put = |value: &[u8]| -> Result<(), Status> {
        payload.extend_from_slice(&u32::try_from(value.len()).map_err(|_| invalid("ticket field is too large"))?.to_be_bytes());
        payload.extend_from_slice(value);
        Ok(())
    };
    put(b"inspect")?;
    put(database.as_bytes())?;
    put(nonce)?;
    match transaction { Some(handle) => { put(b"\x01")?; put(handle)?; } None => put(b"\x00")? }
    put(sql.as_bytes())?;
    put(&u32::try_from(search_path.len()).map_err(|_| invalid("ticket field is too large"))?.to_be_bytes())?;
    for schema in search_path { put(schema.as_bytes())?; }
    Ok(payload)
}

fn take_string(object: &mut Map<String, Value>, key: &str) -> Result<String, Status> {
    match object.remove(key) { Some(Value::String(value)) => Ok(value), _ => Err(invalid_ticket()) }
}

/// A key that must be present, holding base64 or null.
fn take_base64(object: &mut Map<String, Value>, key: &str) -> Result<Option<Vec<u8>>, Status> {
    match object.remove(key) {
        Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => STANDARD.decode(text).map(Some).map_err(|_| invalid_ticket()),
        _ => Err(invalid_ticket()),
    }
}

fn strings(value: Value) -> Result<Vec<String>, Status> {
    let Value::Array(values) = value else { return Err(invalid_ticket()); };
    values.into_iter().map(|value| match value { Value::String(value) => Ok(value), _ => Err(invalid_ticket()) }).collect()
}

fn micros(text: &str) -> Result<Timestamp, Status> {
    Instant::parse_iso(text).and_then(Instant::as_micros).map_err(|_| invalid_ticket())
}

pub(crate) fn timestamp(time: Timestamp) -> Result<String, Status> {
    rtdb_types::Instant::from(time).to_iso_string().map_err(status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn binding<'a>(session: &'a str, owner: Owner, key: &[u8]) -> Binding<'a> {
        Binding { mac: Hmac::<Sha256>::new_from_slice(key).unwrap(), session, owner }
    }

    fn run(cut: Cut, params: Option<Bytes>) -> Ticket {
        Ticket::Run(QueryRun {
            database: "xtdb".into(), sql: "SELECT ?".into(), search_path: vec!["public".into(), "other".into()],
            basis: TicketBasis { current_time: Instant::new(-1, 999_999_999).unwrap(), timezone: "Europe/Paris".into(), snapshot_time: Some(Timestamp(-1)), has_await_floor: true, cut },
            params,
        })
    }

    #[test]
    fn run_ticket_round_trips_its_basis_cut_and_parameters() {
        let memory = MemoryBudget::new(1 << 20);
        let owner = Owner::Authenticated(Uuid::from_bytes([7; 16]));
        let signer = binding("session", owner, b"key");
        let cut = Cut::Visible(BTreeMap::from([("empty".to_owned(), None), ("xtdb".to_owned(), Some(Timestamp(1_234_567)))]));
        let params = Bytes::from_static(b"\xff\x00\x83\x59");
        {
            let encoded = run(cut, Some(params.clone())).encode(&signer, &memory).unwrap();
            let Ok(Ticket::Run(decoded)) = Ticket::decode(&encoded, &signer) else { panic!("run ticket") };
            assert_eq!((decoded.database.as_str(), decoded.sql.as_str()), ("xtdb", "SELECT ?"));
            assert_eq!(decoded.search_path, ["public", "other"]);
            assert_eq!(decoded.params, Some(params));
            let Cut::Visible(cut) = &decoded.basis.cut else { panic!("visible cut") };
            assert_eq!((cut["empty"], cut["xtdb"]), (None, Some(Timestamp(1_234_567))));
            let basis = decoded.basis.query_basis(BTreeSet::from(["attached".to_owned()]));
            assert_eq!(basis.current_time, Instant::new(-1, 999_999_999).unwrap());
            assert_eq!((basis.timezone.as_str(), basis.snapshot_time, basis.has_await_floor, basis.resume_token), ("Europe/Paris", Some(Timestamp(-1)), true, None));
            assert_eq!(basis.snapshot.databases["empty"], [Some(Instant::from(Timestamp::MIN))]);
            assert_eq!(basis.snapshot.databases["xtdb"], [Some(Instant::from(Timestamp(1_234_567)))]);
            assert_eq!(basis.snapshot.databases["attached"], [Some(Instant::from(Timestamp::MIN))]);

            let snapshot = run(Cut::Snapshot("token".into()), None).encode(&signer, &memory).unwrap();
            let Ok(Ticket::Run(decoded)) = Ticket::decode(&snapshot, &signer) else { panic!("snapshot ticket") };
            let basis = decoded.basis.query_basis(BTreeSet::from(["attached".to_owned()]));
            assert_eq!((basis.resume_token.as_deref(), basis.snapshot.databases.len(), decoded.params), (Some("token"), 0, None));
        }
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn run_ticket_is_redeemable_by_another_session_owner_or_service() {
        let memory = MemoryBudget::new(1 << 20);
        let owner = Owner::Authenticated(Uuid::from_bytes([7; 16]));
        let signer = binding("session", owner, b"key");
        let encoded = run(Cut::Visible(BTreeMap::new()), None).encode(&signer, &memory).unwrap();
        for other in [binding("other", owner, b"key"), binding("session", Owner::Anonymous, b"key"), binding("session", owner, b"restarted")] {
            let Ok(Ticket::Run(decoded)) = Ticket::decode(&encoded, &other) else { panic!("run ticket for another session") };
            assert_eq!((decoded.database.as_str(), decoded.sql.as_str()), ("xtdb", "SELECT ?"));
        }
        assert!(Ticket::decode(&encoded.slice(..encoded.len() - 1), &signer).is_err());
        for refused in [&b"RTDBFT\x05\x01"[..], br#"{"type":"run"}"#, br#"{"type":"empty","extra":1}"#, br#"{"type":"other"}"#] {
            let error = Ticket::decode(&Bytes::from_static(refused), &signer).err().unwrap();
            assert_eq!((error.code(), error.message()), (tonic::Code::InvalidArgument, "invalid ticket"), "{refused:?}");
        }
    }

    #[test]
    fn run_ticket_is_the_references_query_ticket_json() {
        let memory = MemoryBudget::new(1 << 20);
        let signer = binding("session", Owner::Anonymous, b"key");
        let ticket = Ticket::Run(QueryRun {
            database: "xtdb".into(), sql: "SELECT 1".into(), search_path: vec!["public".into()],
            basis: TicketBasis { current_time: Instant::new(1_577_836_800, 0).unwrap(), timezone: "Europe/London".into(), snapshot_time: None, has_await_floor: false,
                cut: Cut::Visible(BTreeMap::from([("xtdb".to_owned(), Some(Timestamp(1_577_836_800_000_000)))])) },
            params: Some(Bytes::from_static(b"\x01\x02\x03")),
        });
        let encoded = ticket.encode(&signer, &memory).unwrap();
        // `xtdb.api/q` documents ChAKBHh0ZGISCAoGCIDCr/AF as database "xtdb" at 2020-01-01T00:00:00Z.
        assert_eq!(std::str::from_utf8(&encoded).unwrap(), concat!(
            r#"{"type":"run","dbName":"xtdb","sql":"SELECT 1","params":"AQID","#,
            r#""basis":{"currentTime":"2020-01-01T00:00:00Z","defaultTz":"Europe/London","snapshotToken":"ChAKBHh0ZGISCAoGCIDCr/AF","snapshotTime":null}}"#));
        assert!(matches!(Ticket::decode(&Ticket::empty(&memory).unwrap(), &signer), Ok(Ticket::Empty)));
        assert_eq!(&Ticket::empty(&memory).unwrap()[..], br#"{"type":"empty"}"#);
        // A ticket the reference issued redeems here.
        let upstream = Bytes::from_static(br#"{"type":"run","dbName":"xtdb","sql":"SELECT 1","params":null,"basis":{"currentTime":"2020-01-01T00:00:00.123Z","defaultTz":"Z","snapshotToken":"ChAKBHh0ZGISCAoGCIDCr/AF","snapshotTime":"2020-01-01T00:00:00Z"}}"#);
        let Ok(Ticket::Run(decoded)) = Ticket::decode(&upstream, &signer) else { panic!("reference ticket") };
        assert_eq!(decoded.search_path, ["public"]);
        assert_eq!(decoded.basis.snapshot_time, Some(Timestamp(1_577_836_800_000_000)));
        assert_eq!(decoded.basis.current_time, Instant::new(1_577_836_800, 123_000_000).unwrap());
        assert!(!decoded.basis.has_await_floor && decoded.params.is_none());
        let Cut::Visible(cut) = decoded.basis.cut else { panic!("visible cut") };
        assert_eq!(cut["xtdb"], Some(Timestamp(1_577_836_800_000_000)));
    }

    #[test]
    fn inspection_ticket_is_unknown_to_another_session_owner_or_service_and_rejects_tampering() {
        let memory = MemoryBudget::new(1 << 20);
        let owner = Owner::Authenticated(Uuid::from_bytes([7; 16]));
        let signer = binding("session", owner, b"key");
        let encoded = Ticket::Inspect(Inspection { database: "xtdb".into(), transaction: None, sql: "SHOW search_path".into(), search_path: Vec::new() })
            .encode(&signer, &memory).unwrap();
        for other in [binding("other", owner, b"key"), binding("session", Owner::Anonymous, b"key"), binding("session", owner, b"restarted")] {
            assert_eq!(Ticket::decode(&encoded, &other).err().unwrap().code(), tonic::Code::NotFound);
        }
        let tampered = std::str::from_utf8(&encoded).unwrap().replace("SHOW search_path", "SHOW timezone");
        assert_eq!(Ticket::decode(&Bytes::from(tampered), &signer).err().unwrap().code(), tonic::Code::NotFound);
        assert!(Ticket::decode(&encoded.slice(..8), &signer).is_err());
    }

    #[test]
    fn empty_and_inspection_tickets_round_trip() {
        let memory = MemoryBudget::new(4096);
        let signer = binding("session", Owner::Anonymous, b"key");
        assert!(matches!(Ticket::decode(&Ticket::empty(&memory).unwrap(), &binding("other", Owner::Anonymous, b"x")), Ok(Ticket::Empty)));
        let inspection = Ticket::Inspect(Inspection { database: "xtdb".into(), transaction: Some(Bytes::from_static(b"tx")), sql: "SHOW search_path".into(), search_path: vec!["public".into()] });
        let encoded = inspection.encode(&signer, &memory).unwrap();
        let Ok(Ticket::Inspect(decoded)) = Ticket::decode(&encoded, &signer) else { panic!("inspection ticket") };
        assert_eq!((decoded.database.as_str(), decoded.sql.as_str(), decoded.search_path.as_slice()), ("xtdb", "SHOW search_path", &["public".to_owned()][..]));
        assert_eq!(decoded.transaction.as_deref(), Some(&b"tx"[..]));
        let handle = [9u8; 40];
        let inspect = || Ticket::Inspect(Inspection { database: "xtdb".into(), transaction: Some(Bytes::copy_from_slice(&handle)), sql: "SHOW search_path".into(), search_path: Vec::new() })
            .encode(&signer, &memory).unwrap();
        let (masked, again) = (inspect(), inspect());
        assert!(!masked.windows(8).any(|window| window == &handle[..8]));
        assert_ne!(masked, again);
        let Ok(Ticket::Inspect(decoded)) = Ticket::decode(&masked, &signer) else { panic!("masked inspection ticket") };
        assert_eq!(decoded.transaction.as_deref(), Some(&handle[..]));
    }
}
