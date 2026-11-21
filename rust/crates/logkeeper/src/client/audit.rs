use std::collections::BTreeMap;
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use sha2::{Digest, Sha256};

use super::ensemble::{Ensemble, MAXIMUM_BATCH_ENTRIES};
use super::{StreamConfig, validate};

/// Fingerprints of the same committed range on all responding members of an ensemble.
#[derive(Debug, Default)]
pub struct RetainedAudit {
    pub interval: Option<[i64; 2]>,
    pub digests: BTreeMap<String, String>,
    pub unavailable: BTreeMap<String, String>,
}

/// Compares independently read replicas without appending, sealing or advancing their commit positions.
pub async fn audit_retained(config: &StreamConfig) -> Result<RetainedAudit> {
    validate(config)?;
    let ensemble = Ensemble::new(config.clone());
    let probes = (0..ensemble.len()).map(|server| {
        let ensemble = &ensemble;
        async move { (server, ensemble.read(server, 0, 0, Duration::ZERO).await) }
    }).collect::<FuturesUnordered<_>>();
    let mut audit = RetainedAudit::default();
    let mut responders = Vec::new();
    let (mut first, mut through) = (0, i64::MAX);
    for (server, outcome) in probes.collect::<Vec<_>>().await {
        match outcome {
            Ok(bounds) => {
                first = first.max(bounds.trim_lsn.saturating_add(1));
                through = through.min(bounds.commit_lsn);
                responders.push(server);
            }
            Err(error) => { audit.unavailable.insert(config.servers[server].clone(), error.to_string()); }
        }
    }
    if responders.len() < config.write_quorum || first > through { return Ok(audit); }
    audit.interval = Some([first, through]);

    let mut proofs = responders.into_iter().map(|server| {
        let ensemble = &ensemble;
        async move { (server, chain(ensemble, server, first, through).await) }
    }).collect::<FuturesUnordered<_>>();
    while let Some((server, proof)) = proofs.next().await {
        audit.digests.insert(config.servers[server].clone(), proof?);
    }
    Ok(audit)
}

async fn chain(ensemble: &Ensemble, server: usize, first: i64, through: i64) -> Result<String> {
    let stream = &ensemble.config.stream;
    let mut root = Sha256::new();
    root.update(b"rtdb.logkeeper.retained-chain.v1\0");
    root.update((stream.len() as u64).to_be_bytes());
    root.update(stream.as_bytes());
    root.update(first.to_be_bytes());
    let mut digest = root.finalize();
    let mut next = first;
    loop {
        let page = ensemble.read(server, next, MAXIMUM_BATCH_ENTRIES, Duration::ZERO).await?;
        if next <= page.trim_lsn {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.logkeeper/audit-trimmed", format!(
                "Replica {} trimmed position {next} during the audit", ensemble.config.servers[server])));
        }
        if page.entries.is_empty() {
            return Err(incomplete(ensemble, server, next));
        }
        for entry in page.entries {
            if entry.lsn != next { return Err(incomplete(ensemble, server, next)); }
            let mut link = Sha256::new();
            link.update(digest);
            link.update(entry.lsn.to_be_bytes());
            link.update(entry.epoch.to_be_bytes());
            link.update(entry.timestamp.to_be_bytes());
            link.update((entry.payload.len() as u64).to_be_bytes());
            link.update(entry.payload);
            digest = link.finalize();
            if next == through { return Ok(format!("{digest:x}")); }
            next += 1;
        }
    }
}

fn incomplete(ensemble: &Ensemble, server: usize, position: i64) -> Anomaly {
    Anomaly::fault("rtdb.logkeeper/audit-gap", format!("Replica {} omitted committed position {position}", ensemble.config.servers[server]))
}
