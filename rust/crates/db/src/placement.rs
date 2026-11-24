use std::{collections::BTreeSet, net::IpAddr};

use rtdb_types::{Anomaly, AnomalyCategory, Result};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub async fn discover_nodes(host: &str) -> Result<BTreeSet<IpAddr>> {
    let addresses = tokio::net::lookup_host((host, 0)).await.map_err(|err| {
        Anomaly::new(
            AnomalyCategory::Unavailable,
            "rtdb/node-discovery",
            err.to_string(),
        )
    })?;
    Ok(addresses.map(|addr| addr.ip()).collect())
}

pub fn preferred(database: Uuid, nodes: &BTreeSet<IpAddr>) -> Option<IpAddr> {
    nodes.iter().copied().max_by_key(|address| {
        let mut hash = Sha256::new();
        hash.update(database.as_bytes());
        match address {
            IpAddr::V4(ip) => {
                hash.update([4]);
                hash.update(ip.octets());
            }
            IpAddr::V6(ip) => {
                hash.update([6]);
                hash.update(ip.octets());
            }
        }
        (hash.finalize(), *address)
    })
}
