use std::collections::BTreeMap;
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

use super::ensemble::{Ensemble, MAXIMUM_BATCH_ENTRIES};
use super::{StreamConfig, validate};
use crate::history::RetainedChain;

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
