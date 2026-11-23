use std::{collections::BTreeSet, net::{IpAddr, Ipv4Addr, SocketAddr}, sync::Arc, time::Duration};
use rtdb_node::Database;
use rtdb_types::{Anomaly, Result};

pub const FORWARDED_PEER_HEADER: &str = "x-rtdb-flight-peer";
pub const AUDIENCE_FLIGHT_HEADER: &str = "x-rtdb-audience-flight-v15";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AudienceFlightConfig {
    source: uuid::Uuid,
    turns: usize,
}

impl std::fmt::Display for AudienceFlightConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}", self.source, self.turns)
    }
}

impl AudienceFlightConfig {
    pub fn parse(value: &str) -> Result<Self> {
        let invalid = || Anomaly::incorrect("rtdb.flight-sql/audience", "Expected an audience source UUID and one to 64 observation turns as UUID/TURNS");
        let (source, turns) = value.split_once('/').ok_or_else(invalid)?;
        let source = uuid::Uuid::parse_str(source).ok().filter(|source| !source.is_nil()).ok_or_else(invalid)?;
        let turns = turns.parse::<usize>().ok().filter(|turns| (1..=64).contains(turns)).ok_or_else(invalid)?;
        Ok(Self { source, turns })
    }

    pub fn source(self) -> uuid::Uuid { self.source }
    pub fn permits(self) -> usize { 2 * (self.turns + 1) }

    /// The installed byte pool includes both the active round and its replacement.
    pub fn execution_bytes(self) -> usize { self.permits() * (16 << 20) }
    pub fn cache_bytes(self) -> usize { self.permits() * (16 << 20) }
    pub fn prepaid_bytes(self) -> usize { self.execution_bytes() + self.cache_bytes() }
}

pub trait FlightPeerVerifier: std::fmt::Debug + Send + Sync {
    fn verify(&self, proof: &str, attachment: &Database) -> Result<IpAddr>;
}

/// The installed hosted principal and its finite ordinary control table allowlist.
#[derive(Clone, Debug)]
pub struct HostedControlConfig {
    pub owner: String,
    pub relations: BTreeSet<String>,
}

#[derive(Clone, Debug)]
pub struct FlightConfig {
    pub address: SocketAddr,
    pub transaction_idle_timeout: Duration,
    pub prepared_statement_idle_timeout: Duration,
    pub session_idle_timeout: Duration,
    pub session_unclaimed_timeout: Duration,
    pub sweep_interval: Duration,
    pub maximum_message_bytes: usize,
    pub owned_native_actions: bool,
    pub hosted_controls: Option<HostedControlConfig>,
    /// An exclusive listener for this installed audience.
    pub audience: Option<AudienceFlightConfig>,
    /// The ordinary listener cannot mint applied holds for this installed audience.
    pub reserved_applied_source: Option<uuid::Uuid>,
    pub maximum_concurrent_calls: usize,
    pub playground: bool,
    pub forwarded_peer: Option<Arc<dyn FlightPeerVerifier>>,
    pub owned_world_host: Option<Arc<dyn crate::OwnedWorldHost>>,
}
impl Default for FlightConfig {
    fn default() -> Self {
        Self { address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9839), transaction_idle_timeout: Duration::from_secs(1800), prepared_statement_idle_timeout: Duration::from_secs(1800), session_idle_timeout: Duration::from_secs(1800), session_unclaimed_timeout: Duration::from_secs(60), sweep_interval: Duration::from_secs(10), maximum_message_bytes: 64 * 1024 * 1024, owned_native_actions: false, hosted_controls: None, audience: None, reserved_applied_source: None, maximum_concurrent_calls: 256, playground: false, forwarded_peer: None, owned_world_host: None }
    }
}
impl FlightConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.audience.is_some() && (!self.owned_native_actions || self.hosted_controls.is_none() || self.forwarded_peer.is_none()
            || self.reserved_applied_source.is_some()) {
            return Err(Anomaly::incorrect("rtdb.flight-sql/audience", "An audience reservation requires placed hosted controls and authenticated forwarding"));
        }
        if [self.transaction_idle_timeout, self.prepared_statement_idle_timeout, self.session_idle_timeout, self.session_unclaimed_timeout, self.sweep_interval].iter().any(Duration::is_zero) || self.maximum_message_bytes == 0 || self.maximum_concurrent_calls == 0 {
            return Err(Anomaly::incorrect("rtdb.flight-sql/configuration", "Flight timeouts and transport limits must be positive"));
        }
        if let Some(controls) = &self.hosted_controls
            && (!self.owned_native_actions || controls.owner.is_empty()
                || !(1..=32).contains(&controls.relations.len())
                || controls.relations.iter().any(|name| !name.starts_with("bj_") || name.contains('-')
                    || rtdb_types::OwnedRelation::new(name.as_str()).is_err()))
        {
            return Err(Anomaly::incorrect("rtdb.flight-sql/configuration", "Invalid hosted control principal or relation allowlist"));
        }
        Ok(())
    }
}
