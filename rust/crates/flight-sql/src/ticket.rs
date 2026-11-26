use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use bytes::{Buf, Bytes};
use hmac::{Hmac, Mac};
use rtdb_columnar::MemoryBudget;
use rtdb_node::QueryBasis;
use rtdb_types::{Instant, TimeBasis, Timestamp};
use sha2::Sha256;
use tonic::Status;

use crate::{error::{invalid, status}, state::Owner, wire};

const VERSION: &[u8] = b"RTDBFT\x03";
const MAC_BYTES: usize = 32;
const RUN: u8 = 1;
const INSPECT: u8 = 2;

/// A self-contained query ticket; nothing about an issued ticket is retained by the server.
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
    /// Each database's last visible system time; `None`, or a database absent from the map, sees no transaction.
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

/// Binds a ticket to the session that described it, under the service's ticket key.
pub(crate) struct Binding<'a> { pub mac: Hmac<Sha256>, pub session: &'a str, pub owner: Owner }

impl Binding<'_> {
    fn sign(&self, payload: &[u8]) -> Hmac<Sha256> {
        let mut mac = self.mac.clone();
        mac.update(&(self.session.len() as u64).to_be_bytes());
        mac.update(self.session.as_bytes());
        match self.owner {
            Owner::Anonymous => mac.update(&[0]),
            Owner::Authenticated(id) => { mac.update(&[1]); mac.update(id.as_bytes()); }
        }
        mac.update(payload);
        mac
    }
}

impl Ticket {
    pub fn empty(memory: &Arc<MemoryBudget>) -> Result<Bytes, Status> {
        let charge = memory.reserve(64).map_err(status)?;
        Ok(wire::retain(VERSION.to_vec(), charge))
    }

    fn encoded_bound(&self) -> Result<usize, Status> {
        let strings = |values: &[String]| values.iter().try_fold(4usize, |bytes, value| wire::add(bytes, wire::add(4, value.len())?));
        let body = match self {
            Self::Empty => 0,
            Self::Inspect(inspection) => wire::add(wire::add(inspection.database.len(), inspection.sql.len())?,
                wire::add(strings(&inspection.search_path)?, inspection.transaction.as_ref().map_or(0, Bytes::len))?)?,
            Self::Run(run) => {
                let cut = match &run.basis.cut {
                    Cut::Snapshot(token) => token.len(),
                    Cut::Visible(databases) => databases.keys().try_fold(0usize, |bytes, name| wire::add(bytes, wire::add(name.len(), 16)?))?,
                };
                [run.database.len(), run.sql.len(), strings(&run.search_path)?, run.basis.timezone.len(), cut, run.params.as_ref().map_or(0, Bytes::len)]
                    .into_iter().try_fold(0usize, wire::add)?
            }
        };
        wire::add(body, 128)
    }

    pub fn encode(&self, binding: &Binding<'_>, memory: &Arc<MemoryBudget>) -> Result<Bytes, Status> {
        let bound = self.encoded_bound()?;
        let charge = memory.reserve(wire::add(bound, 128)?).map_err(status)?;
        let mut bytes = Vec::with_capacity(bound);
        bytes.extend_from_slice(VERSION);
        match self {
            Self::Empty => return Self::empty(memory),
            Self::Inspect(inspection) => {
                bytes.push(INSPECT);
                put_string(&mut bytes, &inspection.database)?;
                put_bytes(&mut bytes, inspection.transaction.as_deref())?;
                put_string(&mut bytes, &inspection.sql)?;
                put_strings(&mut bytes, &inspection.search_path)?;
            }
            Self::Run(run) => {
                let basis = &run.basis;
                bytes.push(RUN);
                put_string(&mut bytes, &run.database)?;
                put_string(&mut bytes, &run.sql)?;
                put_strings(&mut bytes, &run.search_path)?;
                put_string(&mut bytes, &basis.timezone)?;
                bytes.extend_from_slice(&basis.current_time.seconds().to_be_bytes());
                bytes.extend_from_slice(&basis.current_time.nanos().to_be_bytes());
                put_time(&mut bytes, basis.snapshot_time);
                bytes.push(u8::from(basis.has_await_floor));
                match &basis.cut {
                    Cut::Snapshot(token) => { bytes.push(0); put_string(&mut bytes, token)?; }
                    Cut::Visible(databases) => {
                        bytes.push(1);
                        put_len(&mut bytes, databases.len())?;
                        for (name, visible) in databases { put_string(&mut bytes, name)?; put_time(&mut bytes, *visible); }
                    }
                }
                put_bytes(&mut bytes, run.params.as_deref())?;
            }
        }
        let tag = binding.sign(&bytes).finalize().into_bytes();
        bytes.extend_from_slice(&tag);
        if bytes.capacity() > bound { return Err(Status::internal("Query ticket exceeds its encoding reservation")); }
        Ok(wire::retain(bytes, charge))
    }

    /// A ticket signed for another session or by another service reads as unknown.
    pub fn decode(bytes: &Bytes, binding: &Binding<'_>) -> Result<Self, Status> {
        if !bytes.starts_with(VERSION) { return Err(invalid("invalid ticket version")); }
        if bytes.len() == VERSION.len() { return Ok(Self::Empty); }
        let split = bytes.len().checked_sub(MAC_BYTES).filter(|split| *split > VERSION.len())
            .ok_or_else(|| invalid("truncated ticket"))?;
        binding.sign(&bytes[..split]).verify_slice(&bytes[split..]).map_err(|_| Status::not_found("unknown query ticket"))?;
        let mut body = bytes.slice(VERSION.len()..split);
        let ticket = match body.get_u8() {
            INSPECT => Self::Inspect(Inspection {
                database: take_string(&mut body)?, transaction: take_bytes(&mut body)?, sql: take_string(&mut body)?, search_path: take_strings(&mut body)?,
            }),
            RUN => {
                let database = take_string(&mut body)?;
                let sql = take_string(&mut body)?;
                let search_path = take_strings(&mut body)?;
                let timezone = take_string(&mut body)?;
                need(&body, 12)?;
                let current_time = Instant::new(body.get_i64(), body.get_u32()).map_err(status)?;
                let snapshot_time = take_time(&mut body)?;
                let has_await_floor = take_flag(&mut body)?;
                let cut = if take_flag(&mut body)? {
                    let count = take_len(&mut body)?;
                    let mut databases = BTreeMap::new();
                    for _ in 0..count { databases.insert(take_string(&mut body)?, take_time(&mut body)?); }
                    Cut::Visible(databases)
                } else {
                    Cut::Snapshot(take_string(&mut body)?)
                };
                let params = take_bytes(&mut body)?;
                Self::Run(QueryRun { database, sql, search_path, basis: TicketBasis { current_time, timezone, snapshot_time, has_await_floor, cut }, params })
            }
            _ => return Err(invalid("invalid ticket kind")),
        };
        if !body.is_empty() { return Err(invalid("trailing ticket bytes")); }
        Ok(ticket)
    }
}

fn put_len(out: &mut Vec<u8>, length: usize) -> Result<(), Status> {
    out.extend_from_slice(&u32::try_from(length).map_err(|_| invalid("ticket field is too large"))?.to_be_bytes());
    Ok(())
}

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<(), Status> {
    put_len(out, value.len())?;
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_strings(out: &mut Vec<u8>, values: &[String]) -> Result<(), Status> {
    put_len(out, values.len())?;
    values.iter().try_for_each(|value| put_string(out, value))
}

fn put_bytes(out: &mut Vec<u8>, value: Option<&[u8]>) -> Result<(), Status> {
    match value {
        Some(value) => { out.push(1); put_len(out, value.len())?; out.extend_from_slice(value); }
        None => out.push(0),
    }
    Ok(())
}

fn take_bytes(bytes: &mut Bytes) -> Result<Option<Bytes>, Status> {
    if !take_flag(bytes)? { return Ok(None); }
    let length = take_len(bytes)?;
    need(bytes, length)?;
    Ok(Some(bytes.split_to(length)))
}

fn put_time(out: &mut Vec<u8>, time: Option<Timestamp>) {
    match time {
        Some(time) => { out.push(1); out.extend_from_slice(&time.0.to_be_bytes()); }
        None => out.push(0),
    }
}

fn need(bytes: &Bytes, length: usize) -> Result<(), Status> {
    if bytes.len() < length { Err(invalid("truncated ticket")) } else { Ok(()) }
}

fn take_flag(bytes: &mut Bytes) -> Result<bool, Status> {
    need(bytes, 1)?;
    match bytes.get_u8() { 0 => Ok(false), 1 => Ok(true), _ => Err(invalid("invalid ticket flag")) }
}

fn take_len(bytes: &mut Bytes) -> Result<usize, Status> {
    need(bytes, 4)?;
    Ok(bytes.get_u32() as usize)
}

fn take_string(bytes: &mut Bytes) -> Result<String, Status> {
    let length = take_len(bytes)?;
    need(bytes, length)?;
    String::from_utf8(bytes.split_to(length).to_vec()).map_err(|_| invalid("invalid ticket text"))
}

fn take_strings(bytes: &mut Bytes) -> Result<Vec<String>, Status> {
    let count = take_len(bytes)?;
    // Each string carries at least its four-byte length.
    need(bytes, count.saturating_mul(4))?;
    (0..count).map(|_| take_string(bytes)).collect()
}

fn take_time(bytes: &mut Bytes) -> Result<Option<Timestamp>, Status> {
    if !take_flag(bytes)? { return Ok(None); }
    need(bytes, 8)?;
    Ok(Some(Timestamp(bytes.get_i64())))
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
    fn ticket_is_unknown_to_another_session_owner_or_service_and_rejects_tampering() {
        let memory = MemoryBudget::new(1 << 20);
        let owner = Owner::Authenticated(Uuid::from_bytes([7; 16]));
        let signer = binding("session", owner, b"key");
        let encoded = run(Cut::Visible(BTreeMap::new()), None).encode(&signer, &memory).unwrap();
        for other in [binding("other", owner, b"key"), binding("session", Owner::Anonymous, b"key"), binding("session", owner, b"restarted")] {
            assert_eq!(Ticket::decode(&encoded, &other).err().unwrap().code(), tonic::Code::NotFound);
        }
        let mut tampered = encoded.to_vec();
        tampered[VERSION.len() + 8] ^= 1;
        assert_eq!(Ticket::decode(&Bytes::from(tampered), &signer).err().unwrap().code(), tonic::Code::NotFound);
        assert!(Ticket::decode(&encoded.slice(..encoded.len() - 1), &signer).is_err());
        assert!(Ticket::decode(&Bytes::from_static(b"RTDBFT\x02"), &signer).is_err());
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
    }
}
