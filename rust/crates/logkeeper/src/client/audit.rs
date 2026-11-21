use std::collections::BTreeMap;
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

use super::ensemble::{Ensemble, MAXIMUM_BATCH_ENTRIES};
use super::{StreamConfig, validate};
use crate::history::RetainedChain;
use crate::server::JournalDigest;

/// Fingerprints of the same committed range on all responding members of an ensemble.
#[derive(Debug, Default)]
pub struct RetainedAudit {
    pub interval: Option<[i64; 2]>,
    pub digests: BTreeMap<String, String>,
    pub windows: BTreeMap<String, Option<[i64; 2]>>,
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
    for (server, outcome) in probes.collect::<Vec<_>>().await {
        match outcome {
            Ok(bounds) => {
                let first = bounds.trim_lsn.saturating_add(1).max(0);
                let window = (first <= bounds.commit_lsn).then_some([first, bounds.commit_lsn]);
                if let Some(window) = window { responders.push((server, window)); }
                audit.windows.insert(config.servers[server].clone(), window);
            }
            Err(error) => { audit.unavailable.insert(config.servers[server].clone(), error.to_string()); }
        }
    }
    let Some([first, through]) = quorum_overlap(&responders, config.write_quorum) else { return Ok(audit) };
    audit.interval = Some([first, through]);

    let mut proofs = responders.into_iter().filter(|(_, [start, end])| *start <= first && through <= *end).map(|(server, _)| {
        let ensemble = &ensemble;
        async move { (server, chain(ensemble, server, first, through).await) }
    }).collect::<FuturesUnordered<_>>();
    while let Some((server, proof)) = proofs.next().await {
        match proof {
            Ok(digest) => { audit.digests.insert(config.servers[server].clone(), digest); }
            Err(error) if error.category == AnomalyCategory::Unavailable => { audit.unavailable.insert(config.servers[server].clone(), error.to_string()); }
            Err(error) => return Err(error),
        }
    }
    Ok(audit)
}

/// Obtains agreement from a durable write quorum for the committed interval ending at `through`.
/// A supplied prefix must belong to the same stream and adjoin the interval to hash.
pub async fn certify_committed_prefix(config: &StreamConfig, through: i64, prefix: Option<&JournalDigest>) -> Result<JournalDigest> {
    validate(config)?;
    if through < 0 { return Err(Anomaly::incorrect("rtdb.logkeeper/audit-range", "A journal position must be nonnegative")); }
    let ensemble = Ensemble::new(config.clone());
    let probes = (0..ensemble.len()).map(|server| {
        let ensemble = &ensemble;
        async move { (server, ensemble.read(server, 0, 0, Duration::ZERO).await) }
    }).collect::<FuturesUnordered<_>>();
    let mut windows: Vec<(usize, i64)> = probes.collect::<Vec<_>>().await.into_iter().filter_map(|(server, probe)| {
        let bounds = probe.ok()?;
        (bounds.commit_lsn >= through && bounds.trim_lsn < through)
            .then_some((server, bounds.trim_lsn.saturating_add(1).max(0)))
    }).collect();
    windows.sort_unstable_by_key(|(_, first)| *first);

    let insufficient = || Anomaly::new(AnomalyCategory::Unavailable, "rtdb.logkeeper/audit-quorum", format!(
        "Fewer than {} durable replicas could prove the prefix through {through}", config.write_quorum));
    let (start, from, digest) = if let Some(prefix) = prefix {
        let [start, end] = prefix.interval;
        if prefix.stream != config.stream || start < 0 || end < start || end >= through {
            return Err(Anomaly::incorrect("rtdb.logkeeper/audit-prefix", "The prefix must name this stream and precede the requested position"));
        }
        let digest = RetainedChain::continue_from(&prefix.digest)
            .ok_or_else(|| Anomaly::incorrect("rtdb.logkeeper/audit-prefix", "The prefix digest must be a lowercase SHA-256 value"))?;
        (start, end + 1, Some(digest.bytes()))
    } else {
        let first = windows.get(config.write_quorum - 1).ok_or_else(&insufficient)?.1;
        (first, first, None)
    };
    let mut proofs = windows.into_iter().filter(|(_, first)| *first <= from).map(|(server, _)| {
        let ensemble = &ensemble;
        async move { durable_chain(ensemble, server, from, through, digest).await }
    }).collect::<FuturesUnordered<_>>();
    let mut accepted = None;
    let mut agreeing = 0;
    while let Some(proof) = proofs.next().await {
        let digest = match proof {
            Ok(digest) => digest,
            Err(error) if error.category == AnomalyCategory::Unavailable => continue,
            Err(error) => return Err(error),
        };
        if accepted.as_ref().is_some_and(|first| *first != digest) {
            return Err(Anomaly::fault("rtdb.logkeeper/audit-divergence", format!("Replicas disagree on committed positions {from} through {through}")));
        }
        accepted = Some(digest);
        agreeing += 1;
    }
    let digest = accepted.filter(|_| agreeing >= config.write_quorum).ok_or_else(insufficient)?;
    Ok(JournalDigest { stream: config.stream.clone(), interval: [start, through], digest })
}

async fn durable_chain(ensemble: &Ensemble, server: usize, mut from: i64, through: i64, mut preceding_digest: Option<[u8; 32]>) -> Result<String> {
    loop {
        let page = ensemble.digest_page(server, from, through, preceding_digest).await?;
        let Ok(digest) = <[u8; 32]>::try_from(page.digest.as_ref()) else {
            return Err(Anomaly::fault("rtdb.logkeeper/audit-proof", "A replica returned an invalid digest"));
        };
        if page.through_lsn < from || page.through_lsn > through {
            return Err(Anomaly::fault("rtdb.logkeeper/audit-proof", "A replica returned an invalid digest interval"));
        }
        if page.through_lsn == through { return Ok(RetainedChain::from_bytes(digest).digest()); }
        preceding_digest = Some(digest);
        from = page.through_lsn + 1;
    }
}

fn quorum_overlap(windows: &[(usize, [i64; 2])], quorum: usize) -> Option<[i64; 2]> {
    let index = quorum.checked_sub(1)?;
    let mut ends: Vec<_> = windows.iter().map(|(_, [_, end])| *end).collect();
    ends.sort_unstable_by(|a, b| b.cmp(a));
    ends.dedup();
    for through in ends {
        let mut starts: Vec<_> = windows.iter().filter_map(|(_, [start, end])| (*start <= through && through <= *end).then_some(*start)).collect();
        if starts.len() >= quorum {
            starts.sort_unstable();
            return Some([starts[index], through]);
        }
    }
    None
}

async fn chain(ensemble: &Ensemble, server: usize, first: i64, through: i64) -> Result<String> {
    let mut chain = RetainedChain::new(&ensemble.config.stream, first);
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
            chain.append(&entry);
            if next == through { return Ok(chain.digest()); }
            next += 1;
        }
    }
}

fn incomplete(ensemble: &Ensemble, server: usize, position: i64) -> Anomaly {
    Anomaly::fault("rtdb.logkeeper/audit-gap", format!("Replica {} omitted committed position {position}", ensemble.config.servers[server]))
}

#[cfg(test)]
mod tests {
    use super::quorum_overlap;

    #[test]
    fn the_latest_comparison_range_falls_back_when_retained_windows_do_not_overlap_at_the_tip() {
        let windows = [(0, [3, 6]), (1, [4, 8]), (2, [9, 11])];
        assert_eq!(quorum_overlap(&windows, 2), Some([4, 6]));
        assert_eq!(quorum_overlap(&windows, 3), None);
    }
}
