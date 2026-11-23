//! Capabilities (value `Capability`): short-lived grants a gateway signs for one session's principal on one deployment.
//! A node accepts one in place of the principal's password, so a gateway can reconnect an authenticated session to a new holder.

use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};
use rtdb_format::database::proto::database_config;
use rtdb_pgwire::{CapabilityVerifier, VerifiedCapability};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};

/// The key a cell's gateways sign capabilities with and its nodes verify them with.
pub struct CellSecret { key: hmac::Key }

#[derive(Debug, Serialize, Deserialize)]
struct Claims { deployment: String, principal: String, read_only: bool, expires_at_ms: i64 }

impl CellSecret {
    pub fn new(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 32 { return Err(Anomaly::incorrect("rtdb.multi/cell-secret", "A cell secret needs at least 32 bytes")); }
        Ok(Self { key: hmac::Key::new(hmac::HMAC_SHA256, bytes) })
    }
    pub fn generate() -> Result<Self> {
        let mut bytes = [0u8; 32];
        SystemRandom::new().fill(&mut bytes).map_err(|_| Anomaly::fault("rtdb.multi/random", "No randomness for a cell secret"))?;
        Self::new(&bytes)
    }
    pub fn from_file(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).map_err(|error| Anomaly::incorrect("rtdb.multi/cell-secret", format!("{}: {error}", path.display())))?;
        Self::new(&bytes)
    }

    /// A grant by which one of the cell's nodes asks another for a deployment's data or to publish on its behalf, which no client can make.
    pub fn node_grant(&self, deployment: &str) -> String { self.mint(deployment, NODE_PRINCIPAL, false, Duration::from_secs(30)) }

    /// Refuses unless `grant` is a node grant for `deployment` under this cell's secret.
    pub fn verify_node_grant(&self, grant: &str, deployment: &str) -> Result<()> {
        if self.verify(grant, deployment)?.principal == NODE_PRINCIPAL { Ok(()) } else { Err(refused("The grant is not a node's")) }
    }

    pub(crate) fn flight_session_cipher(&self) -> Result<crate::cipher::Cipher> {
        crate::cipher::Cipher::new(hmac::sign(&self.key, b"rtdb.multi/flight-sql/session-route/v1").as_ref())
    }

    pub(crate) fn sign_route(&self, binding: &[u8]) -> [u8; 32] {
        hmac::sign(&self.key, binding).as_ref().try_into().expect("a SHA-256 MAC is 32 bytes")
    }

    pub(crate) fn verify_route(&self, binding: &[u8], proof: &[u8; 32]) -> bool {
        hmac::verify(&self.key, binding, proof).is_ok()
    }

    pub(crate) fn flight_peer(&self, deployment: &str, epoch: u64, peer: IpAddr) -> String {
        self.mint(&flight_peer_scope(deployment, epoch), &peer.to_string(), true, Duration::from_secs(60))
    }

    /// A capability for `principal` on `deployment`, valid for `ttl`.
    pub fn mint(&self, deployment: &str, principal: &str, read_only: bool, ttl: Duration) -> String {
        let claims = Claims { deployment: deployment.into(), principal: principal.into(), read_only, expires_at_ms: rtdb_directory::now_ms() + ttl.as_millis() as i64 };
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).expect("claims serialize"));
        let tag = URL_SAFE_NO_PAD.encode(hmac::sign(&self.key, payload.as_bytes()));
        format!("{payload}.{tag}")
    }
}

impl CapabilityVerifier for CellSecret {
    fn verify(&self, token: &str, database: &str) -> Result<VerifiedCapability> {
        let (payload, tag) = token.split_once('.').ok_or_else(|| refused("The capability is malformed"))?;
        let tag = URL_SAFE_NO_PAD.decode(tag).map_err(|_| refused("The capability is malformed"))?;
        hmac::verify(&self.key, payload.as_bytes(), &tag).map_err(|_| refused("The capability's signature is not the cell's"))?;
        let claims: Claims = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).map_err(|_| refused("The capability is malformed"))?)
            .map_err(|_| refused("The capability is malformed"))?;
        if claims.expires_at_ms <= rtdb_directory::now_ms() { return Err(refused("The capability has expired")); }
        if claims.deployment != database { return Err(refused("The capability names another deployment")); }
        Ok(VerifiedCapability { principal: claims.principal, read_only: claims.read_only })
    }

    fn require_installation(&self, installation: &str, database: &rtdb_node::Database) -> Result<()> {
        if attachment_identity(database)?.0 == installation { Ok(()) }
        else { Err(refused("The attachment belongs to another installation")) }
    }
}

impl std::fmt::Debug for CellSecret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("CellSecret") }
}

impl rtdb_flight_sql::FlightPeerVerifier for CellSecret {
    fn verify(&self, proof: &str, attachment: &rtdb_node::Database) -> Result<IpAddr> {
        let (deployment, epoch) = attachment_identity(attachment)?;
        let epoch = epoch.ok_or_else(|| refused("The Flight SQL route requires a writer attachment"))?;
        let peer = <Self as CapabilityVerifier>::verify(self, proof, &flight_peer_scope(&deployment, epoch))?;
        peer.principal.parse().map_err(|_| refused("The Flight SQL peer address is invalid"))
    }
}

fn attachment_identity(attachment: &rtdb_node::Database) -> Result<(String, Option<u64>)> {
    if let Some(database_config::Storage::RemoteStorage(remote)) = &attachment.configuration().storage
        && let Some(value) = remote.object_store.as_ref().filter(|value|
            value.type_url.rsplit('/').next() == Some(crate::journal::hosted::STORE_TYPE))
    {
        let (deployment, epoch) = crate::journal::hosted::attachment_identity(value)?;
        return Ok((deployment, Some(epoch)));
    }
    let config = attachment_storage(attachment)?;
    Ok((config.deployment, config.writer_epoch))
}

pub(crate) fn attachment_storage(attachment: &rtdb_node::Database) -> Result<crate::store::CellStoreConfig> {
    let Some(database_config::Storage::RemoteStorage(remote)) = &attachment.configuration().storage else {
        return Err(refused("The attachment has no cell storage identity"));
    };
    let config = remote.object_store.as_ref().filter(|config| config.type_url.rsplit('/').next() == Some(crate::store::CELL_STORAGE_TYPE))
        .ok_or_else(|| refused("The attachment has no cell storage identity"))?;
    serde_json::from_slice(&config.value).map_err(|_| refused("The attachment has an invalid cell storage identity"))
}

fn flight_peer_scope(deployment: &str, epoch: u64) -> String { format!("~flight/peer/{deployment}/{epoch}") }

const NODE_PRINCIPAL: &str = "~cell-node";

fn refused(message: &str) -> Anomaly { Anomaly::new(AnomalyCategory::Forbidden, "rtdb.multi/capability-refused", message) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_capability_grants_only_its_deployment_until_it_expires_and_only_under_its_secret() {
        let secret = CellSecret::generate().unwrap();
        let token = secret.mint("shop", "alice", true, Duration::from_secs(60));
        assert_eq!(secret.verify(&token, "shop").unwrap(), VerifiedCapability { principal: "alice".into(), read_only: true });
        assert!(secret.verify(&token, "blog").is_err(), "another deployment refuses it");
        assert!(CellSecret::generate().unwrap().verify(&token, "shop").is_err(), "another cell's secret refuses it");
        let (payload, tag) = token.split_once('.').unwrap();
        let forged = URL_SAFE_NO_PAD.encode(String::from_utf8(URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap().replace("alice", "admin"));
        assert!(secret.verify(&format!("{forged}.{tag}"), "shop").is_err(), "an altered grant refuses");
        assert!(secret.verify(&secret.mint("shop", "alice", false, Duration::ZERO), "shop").is_err(), "an expired grant refuses");
    }
}
