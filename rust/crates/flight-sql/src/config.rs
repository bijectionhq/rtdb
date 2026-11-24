use std::{collections::BTreeSet, net::{IpAddr, Ipv4Addr, SocketAddr}, sync::Arc, time::Duration};
use rtdb_node::Database;
use rtdb_types::{Anomaly, Result};
use crate::audience_seat::{session_seat_count, session_seat_index};

mod seat_uuid;
mod seat_wire;
pub use seat_wire::{AUDIENCE_FLIGHT_HEADER, AUDIENCE_SEAT_HEADER};
pub(crate) use seat_wire::accepts_seat_header;
use seat_wire::parse_seat_fields;

pub const FORWARDED_PEER_HEADER: &str = "x-rtdb-flight-peer";
pub const AUDIENCE_H2_STREAMS: u32 = 4;
pub const AUDIENCE_H2_STREAM_WINDOW: u32 = 128 << 10;
pub const AUDIENCE_H2_CONNECTION_WINDOW: u32 = 512 << 10;
pub const AUDIENCE_GATEWAY_TRANSPORT_BYTES: usize = 2 << 20;
pub(crate) const AUDIENCE_HOLDER_TRANSPORT_BYTES: usize = 12 << 20;
pub(crate) const AUDIENCE_REQUEST_BYTES: usize = 512 << 10;
pub(crate) const AUDIENCE_REPLY_BYTES: usize = 2 << 20;

mod audience_limits;
pub(crate) use audience_limits::AUDIENCE_READ_LIMITS;

pub(crate) const AUDIENCE_TURN_ACTIONS: usize = 64;
pub(crate) const AUDIENCE_TURN_LIMITS: rtdb_storage::audience_work::Limits = rtdb_storage::audience_work::Limits {
    provider_requests: 4096,
    provider_bytes: 512 << 20,
    provider_metadata_bytes: 64 << 20,
    scan_items: 1 << 20,
    decode_bytes: 1 << 30,
};

// One catalog preparation, one readiness hold, one confirmation and at
// most eight layout points. This allowance lives with the replacement seat.
pub(crate) const AUDIENCE_RECOVERY_ACTIONS: usize = 11;
pub(crate) const AUDIENCE_RECOVERY_LIMITS: rtdb_storage::audience_work::Limits = rtdb_storage::audience_work::Limits {
    provider_requests: 2048,
    provider_bytes: 256 << 20,
    provider_metadata_bytes: 32 << 20,
    scan_items: 1 << 19,
    decode_bytes: 512 << 20,
};

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

/// One of two provider-owned session positions for a prepaid observation turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AudienceSeat {
    turn: usize,
    alternate: bool,
    attempt: uuid::Uuid,
}

impl std::fmt::Display for AudienceSeat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}/{}", self.turn, u8::from(self.alternate), self.attempt)
    }
}

impl AudienceSeat {
    pub(crate) fn index(self) -> usize { session_seat_index(self.turn, self.alternate) }
}

impl AudienceFlightConfig {
    pub fn parse(value: &str) -> Result<Self> {
        let invalid = || Anomaly::incorrect("rtdb.flight-sql/audience", "Expected an audience source UUID and one to 64 observation turns as UUID/TURNS");
        let (source, turns) = value.split_once('/').ok_or_else(invalid)?;
        let source = uuid::Uuid::parse_str(source).ok().filter(|source| !source.is_nil()).ok_or_else(invalid)?;
        let turns = turns.parse::<usize>().ok().filter(|turns| (1..=64).contains(turns)).ok_or_else(invalid)?;
        Ok(Self { source, turns })
    }

    pub fn parse_seat(self, value: &str) -> Result<AudienceSeat> {
        let invalid = || Anomaly::incorrect("rtdb.flight-sql/audience-seat", "Invalid audience reader seat");
        let (turn, alternate, attempt) = parse_seat_fields(self.turns, value.as_bytes()).ok_or_else(invalid)?;
        Ok(AudienceSeat { turn, alternate, attempt })
    }

    pub(crate) fn session_seats(self) -> usize { session_seat_count(self.turns) }

    pub fn source(self) -> uuid::Uuid { self.source }
    pub fn permits(self) -> usize { 2 * (self.turns + 1) }

    /// The installed byte pool includes both the active round and its replacement.
    pub fn execution_bytes(self) -> usize { self.permits() * (16 << 20) }
    pub fn cache_bytes(self) -> usize { self.permits() * (16 << 20) }
    pub(crate) fn transport_bytes(self) -> usize { self.permits() * AUDIENCE_HOLDER_TRANSPORT_BYTES }
    pub fn prepaid_bytes(self) -> usize { self.execution_bytes() + self.cache_bytes() + self.transport_bytes() }
}

pub trait FlightPeerVerifier: std::fmt::Debug + Send + Sync {
    /// Checks the forwarded peer and reports whether the route is restricted to fresh reads.
    fn verify(&self, proof: &str, attachment: &Database) -> Result<(IpAddr, bool)>;
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
    pub custody_providers: std::collections::BTreeMap<String, Arc<crate::CustodyProviderRoute>>,
    /// An exclusive listener for this installed audience.
    pub audience: Option<AudienceFlightConfig>,
    /// The ordinary listener cannot mint applied holds for this installed audience.
    pub reserved_applied_source: Option<uuid::Uuid>,
    pub maximum_concurrent_calls: usize,
    pub playground: bool,
    pub forwarded_peer: Option<Arc<dyn FlightPeerVerifier>>,
    pub owned_world_host: Option<Arc<dyn crate::OwnedWorldHost>>,
    /// Installed by the trusted deployment host for checkpoint administration.
    #[cfg(feature = "multi")]
    pub deployment_admin: Option<Arc<dyn crate::DeploymentAdminAuthority>>,
    /// Trusted process directory for independent Default Local deployments.
    #[cfg(feature = "multi")]
    pub default_checkpoint_restore_directory: Option<std::path::PathBuf>,
}
impl Default for FlightConfig {
    fn default() -> Self {
        Self { address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9839), transaction_idle_timeout: Duration::from_secs(1800), prepared_statement_idle_timeout: Duration::from_secs(1800), session_idle_timeout: Duration::from_secs(1800), session_unclaimed_timeout: Duration::from_secs(60), sweep_interval: Duration::from_secs(10), maximum_message_bytes: 64 * 1024 * 1024, owned_native_actions: false, hosted_controls: None, custody_providers: Default::default(), audience: None, reserved_applied_source: None, maximum_concurrent_calls: 256, playground: false, forwarded_peer: None, owned_world_host: None,
            #[cfg(feature = "multi")]
            deployment_admin: None,
            #[cfg(feature = "multi")]
            default_checkpoint_restore_directory: None,
        }
    }
}
impl FlightConfig {
    pub(crate) fn validate(&self, standalone_custody_owner: Option<&str>) -> Result<()> {
        let owns_unassigned_providers = self.hosted_controls.is_some() || standalone_custody_owner.is_some();
        if !self.custody_providers.is_empty()
            && (self.audience.is_some()
                || !owns_unassigned_providers && self.custody_providers.values().any(|route| route.owner().is_none()))
        {
            return Err(Anomaly::incorrect("rtdb.flight-sql/custody-provider", "Custody providers require the installed owner on the original Flight listener"));
        }
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

#[cfg(test)]
mod tests {
    use super::AudienceFlightConfig;

    #[test]
    fn reader_seats_only_address_the_two_installed_sessions_per_turn() {
        let attempt = "01234567-89ab-4def-8000-000000000001";
        let profile = AudienceFlightConfig::parse(&format!("{attempt}/2")).unwrap();
        assert_eq!(profile.session_seats(), 4);
        for (position, index) in [("0/0", 0), ("0/1", 1), ("1/0", 2), ("1/1", 3)] {
            let requested = format!("{position}/{attempt}");
            let seat = profile.parse_seat(&requested).unwrap();
            assert_eq!(seat.index(), index);
            assert_eq!(seat.to_string(), requested);
        }
        for position in ["2/0", "0/2", "00/0"] {
            assert!(profile.parse_seat(&format!("{position}/{attempt}")).is_err());
        }
    }

    #[test]
    fn flight_seat_text_has_one_spelling_and_consumes_the_entire_header() {
        let attempt = "01234567-89ab-4def-8000-000000000001";
        let profile = AudienceFlightConfig::parse(&format!("{attempt}/64")).unwrap();
        let canonical = format!("63/1/{attempt}");
        assert_eq!(profile.parse_seat(&canonical).unwrap().to_string(), canonical);
        for value in [
            format!("063/1/{attempt}"),
            format!("+1/0/{attempt}"),
            format!("63/1/{}", attempt.to_ascii_uppercase()),
            format!("63/1/{attempt}/"),
            format!("64/1/{attempt}"),
            "1/0/00000000-0000-0000-0000-000000000000".into(),
        ] {
            assert!(profile.parse_seat(&value).is_err(), "{value}");
        }
    }
}
