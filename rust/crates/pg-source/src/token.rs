use std::fmt;
use prost::Message;
use rtdb_format::postgres::proto::PostgresSourceToken;
use rtdb_types::{Anomaly, Result};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lsn(pub u64);
impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xffff_ffff) }
}
impl std::str::FromStr for Lsn {
    type Err = Anomaly;
    fn from_str(text: &str) -> Result<Self> {
        let invalid = || Anomaly::incorrect("xtdb.postgres/invalid-lsn", "LSN must contain two hexadecimal 32-bit words separated by a slash");
        let (high, low) = text.split_once('/').ok_or_else(invalid)?;
        let high = u32::from_str_radix(high, 16).map_err(|_| invalid())?;
        let low = u32::from_str_radix(low, 16).map_err(|_| invalid())?;
        Ok(Self((u64::from(high) << 32) | u64::from(low)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResumeToken { pub lsn: Lsn, pub snapshot_completed: bool, pub legacy_tx_id: Option<i64> }
impl ResumeToken {
    pub fn new(lsn: Lsn, snapshot_completed: bool) -> Self { Self { lsn, snapshot_completed, legacy_tx_id: None } }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let token = PostgresSourceToken::decode(bytes).map_err(|error| Anomaly::incorrect("xtdb.postgres/invalid-source-token", error.to_string()))?;
        Ok(Self { lsn: Lsn(token.latest_committed_lsn.unwrap_or_default() as u64), snapshot_completed: token.snapshot_completed.unwrap_or_default(), legacy_tx_id: token.latest_tx_id })
    }
    pub fn encode(&self) -> Vec<u8> {
        PostgresSourceToken { latest_committed_lsn: Some(self.lsn.0 as i64), latest_tx_id: self.legacy_tx_id, snapshot_completed: Some(self.snapshot_completed) }.encode_to_vec()
    }
    pub fn require_complete(&self) -> Result<Lsn> {
        if self.snapshot_completed { Ok(self.lsn) }
        else { Err(Anomaly::fault("xtdb.postgres/incomplete-snapshot", "The initial PostgreSQL snapshot did not complete; clear this database's logs and object storage before importing it again")) }
    }
}

#[derive(Clone, Debug)]
pub struct ConfirmationState { held: Lsn, received: Lsn, durable: Option<Lsn>, replayable: Option<Lsn>, confirmed: Lsn }
impl ConfirmationState {
    pub fn new(resume: Lsn, durable: Option<Lsn>) -> Self { Self { held: resume, received: resume, durable, replayable: None, confirmed: Lsn(0) } }
    pub fn held(&self) -> Lsn { self.held }
    pub fn received(&self) -> Lsn { self.received }
    pub fn confirmed(&self) -> Lsn { self.confirmed }
    pub fn received_through(&mut self, lsn: Lsn) { self.received = self.received.max(lsn); }
    pub fn submitted(&mut self, lsn: Lsn) { self.held = self.held.max(lsn); }
    pub fn adopted(&mut self, lsn: Option<Lsn>) { self.durable = self.durable.max(lsn); }
    pub fn recovered_by_replay(&mut self, lsn: Lsn) {
        if lsn <= self.held { self.replayable = self.replayable.max(Some(lsn)); }
    }
    pub fn confirmable(&self) -> Lsn {
        if self.durable.is_some_and(|durable| durable >= self.held) { self.received }
        else { self.durable.max(self.replayable).unwrap_or_default() }
    }
    pub fn confirmed_through(&mut self, lsn: Lsn) { self.confirmed = self.confirmed.max(lsn); }
}
