use std::{collections::BTreeSet, net::{IpAddr, Ipv4Addr, SocketAddr}, sync::Arc, time::Duration};
use rtdb_node::Database;
use rtdb_types::{Anomaly, Result};

pub const FORWARDED_PEER_HEADER: &str = "x-rtdb-flight-peer";
/// The largest unary point reply a `ReadSelection` encodes.
pub(crate) const POINT_REPLY_BYTES: usize = 2 << 20;

pub trait FlightPeerVerifier: std::fmt::Debug + Send + Sync {
    /// Checks the forwarded peer and reports whether the route is restricted to fresh reads.
    fn verify(&self, proof: &str, attachment: &Database) -> Result<(IpAddr, bool)>;
}

#[async_trait::async_trait]
pub trait ClassicFlightExclusion: std::fmt::Debug + Send + Sync {
    async fn excludes_unclassified(&self, attachment: &Database) -> Result<bool>;
    async fn verify_retained(
        &self, attachment: &Database, request: ClassicRetainedRead<'_>,
    ) -> Result<ClassicRetainedAdmission>;
}

pub struct ClassicRetainedRead<'a> {
    pub principal: &'a str,
    pub action: &'a str,
    pub body_sha256: [u8; 32],
    pub proof: &'a str,
}

#[derive(Clone, Copy)]
pub struct ClassicRetainedAdmission {
    pub scope_sha256: [u8; 32],
    pub snapshot: rtdb_types::TransactionKey,
    pub deadline: rtdb_storage::sealing::DeliveryDeadline,
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
    /// Serve only `rtdb.store.v1` and `rtdb.admin.v1` of the v1 surface: the
    /// standalone installation's one contract with Bijection.
    pub store_only: bool,
    pub hosted_controls: Option<HostedControlConfig>,
    pub custody_providers: std::collections::BTreeMap<String, Arc<crate::CustodyProviderRoute>>,
    pub maximum_concurrent_calls: usize,
    pub playground: bool,
    pub forwarded_peer: Option<Arc<dyn FlightPeerVerifier>>,
    pub classic_flight_exclusion: Option<Arc<dyn ClassicFlightExclusion>>,
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
        Self { address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9839), transaction_idle_timeout: Duration::from_secs(1800), prepared_statement_idle_timeout: Duration::from_secs(1800), session_idle_timeout: Duration::from_secs(1800), session_unclaimed_timeout: Duration::from_secs(60), sweep_interval: Duration::from_secs(10), maximum_message_bytes: 64 * 1024 * 1024, owned_native_actions: false, store_only: false, hosted_controls: None, custody_providers: Default::default(), maximum_concurrent_calls: 256, playground: false, forwarded_peer: None, classic_flight_exclusion: None, owned_world_host: None,
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
        if !owns_unassigned_providers && self.custody_providers.values().any(|route| route.owner().is_none()) {
            return Err(Anomaly::incorrect("rtdb.flight-sql/custody-provider", "Custody providers require the installed owner on the original Flight listener"));
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
