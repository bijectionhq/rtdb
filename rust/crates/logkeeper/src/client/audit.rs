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

#[derive(Clone, Debug)]
pub struct JournalSyncObservation {
    pub journal: String,
    pub synced_clock_upper_micros: i64,
}

/// Verify one committed entry and collect the original append's retained sync observations.
pub async fn certify_observed_entry(
    config: &StreamConfig, offset: i64,
) -> Result<(JournalDigest, Vec<JournalSyncObservation>)> {
    validate(config)?;
    if offset < 0 || config.servers.iter().any(|server| server.journal().is_none()) {
        return Err(Anomaly::incorrect("rtdb.logkeeper/sync-observation-scope",
            "A sync observation requires an exact position and identified journals"));
    }
    let ensemble = Ensemble::new(config.clone());
    let mut responses = (0..ensemble.len()).map(|server| {
        let ensemble = &ensemble;
        async move {
            (server, ensemble.digest_page(server, offset, offset, None).await.map_err(Anomaly::from))
        }
    }).collect::<FuturesUnordered<_>>();
    let mut agreed = None;
    let mut agreeing = 0;
    let mut observations = Vec::new();
    while let Some((server, response)) = responses.next().await {
        let page = match response {
            Ok(page) => page,
            Err(error) if error.category == AnomalyCategory::Unavailable => continue,
            Err(error) => return Err(error),
        };
        let digest: [u8; 32] = page.digest.as_ref().try_into()
            .map_err(|_| Anomaly::fault("rtdb.logkeeper/audit-proof", "A replica returned an invalid digest"))?;
        if page.through_lsn != offset || agreed.is_some_and(|expected| expected != digest) {
            return Err(Anomaly::fault("rtdb.logkeeper/audit-divergence", "Replicas disagree on the exact committed entry"));
        }
        agreed = Some(digest);
        agreeing += 1;
        if let Some(synced_clock_upper_micros) = page.synced_clock_upper_micros {
            if synced_clock_upper_micros <= 0 {
                return Err(Anomaly::fault("rtdb.logkeeper/sync-observation-clock", "A journal returned an invalid sync sample"));
            }
            observations.push(JournalSyncObservation {
                journal: config.servers[server].journal().expect("validated journal identities").to_owned(),
                synced_clock_upper_micros,
            });
        }
    }
    let digest = agreed.filter(|_| agreeing >= config.write_quorum).ok_or_else(range_unavailable)?;
    observations.sort_unstable_by_key(|observation| observation.synced_clock_upper_micros);
    Ok((JournalDigest::from_digest_bytes(config.stream.clone(), [offset, offset], digest), observations))
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
                audit.windows.insert(config.servers[server].address().to_owned(), window);
            }
            Err(error) => { audit.unavailable.insert(config.servers[server].address().to_owned(), error.to_string()); }
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
            Ok(digest) => { audit.digests.insert(config.servers[server].address().to_owned(), digest); }
            Err(error) if error.category == AnomalyCategory::Unavailable => { audit.unavailable.insert(config.servers[server].address().to_owned(), error.to_string()); }
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

/// Certifies exactly this retained interval, seeded at its first offset.
pub async fn certify_committed_range(config: &StreamConfig, first: i64, through: i64) -> Result<JournalDigest> {
    validate(config)?;
    if first < 0 || through < first { return Err(range_unavailable()); }
    let ensemble = Ensemble::new(config.clone());
    let mut proofs = (0..ensemble.len()).map(|server| {
        let ensemble = &ensemble;
        async move {
            let status = ensemble.read(server, 0, 0, Duration::ZERO).await?;
            if status.trim_lsn >= first || status.commit_lsn < through { return Err(range_unavailable()); }
            durable_chain(ensemble, server, first, through, None).await
        }
    }).collect::<FuturesUnordered<_>>();
    let mut accepted = None;
    let mut agreeing = 0;
    while let Some(result) = proofs.next().await {
        let digest = match result {
            Ok(digest) => digest,
            Err(error) if error.category == AnomalyCategory::Unavailable => continue,
            Err(error) => return Err(error),
        };
        if accepted.as_ref().is_some_and(|previous| previous != &digest) {
            return Err(Anomaly::fault("rtdb.logkeeper/audit-divergence", "Replicas disagree on the retained interval"));
        }
        accepted = Some(digest);
        agreeing += 1;
    }
    let digest = accepted.filter(|_| agreeing >= config.write_quorum).ok_or_else(range_unavailable)?;
    Ok(JournalDigest { stream: config.stream.clone(), interval: [first, through], digest })
}

fn range_unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.logkeeper/retained-range-unavailable", "The exact committed interval is unavailable")
}

/// A bounded cursor through an immutable committed interval. Pages are
/// provisional until the final raw history digest matches the certificate.
pub struct CertifiedRangeReader {
    ensemble: Ensemble,
    certificate: JournalDigest,
    chain: RetainedChain,
    next: i64,
    preferred: usize,
}

impl CertifiedRangeReader {
    pub fn new(config: &StreamConfig, certificate: &JournalDigest) -> Result<Self> {
        validate(config)?;
        let [first, through] = certificate.interval;
        if certificate.stream != config.stream || first < 0 || through < first
            || through == i64::MAX || certificate.digest_bytes().is_none()
        {
            return Err(range_unavailable());
        }
        Ok(Self {
            ensemble: Ensemble::new(config.clone()),
            certificate: certificate.clone(),
            chain: RetainedChain::new(&config.stream, first),
            next: first,
            preferred: 0,
        })
    }

    /// Returns at most one bounded protocol page. Observe completion before
    /// publishing anything that depends on the replayed records.
    pub async fn next_page(&mut self) -> Result<Option<Vec<crate::protocol::Entry>>> {
        let through = self.certificate.interval[1];
        if self.next > through {
            if self.chain.digest() != self.certificate.digest { return Err(range_unavailable()); }
            return Ok(None);
        }
        let max_entries = u32::try_from(through - self.next + 1).unwrap_or(u32::MAX)
            .min(MAXIMUM_BATCH_ENTRIES);
        for offset in 0..self.ensemble.len() {
            let server = (self.preferred + offset) % self.ensemble.len();
            let Ok(page) = self.ensemble.read(server, self.next, max_entries, Duration::ZERO).await else {
                continue;
            };
            if page.trim_lsn >= self.next || page.commit_lsn < through || page.entries.is_empty()
                || page.entries.len() > max_entries as usize
                || page.entries.iter().enumerate().any(|(index, entry)| entry.lsn != self.next + index as i64)
            {
                continue;
            }
            for entry in &page.entries { self.chain.append(entry); }
            self.next += page.entries.len() as i64;
            self.preferred = server;
            if self.next > through && self.chain.digest() != self.certificate.digest {
                return Err(range_unavailable());
            }
            return Ok(Some(page.entries));
        }
        Err(range_unavailable())
    }
}

/// Reads one complete retained copy whose raw bytes match the exact-range certificate.
pub async fn copy_certified_range(
    config: &StreamConfig, certificate: &JournalDigest, maximum_bytes: usize, maximum_entries: usize,
) -> Result<Vec<crate::protocol::Entry>> {
    validate(config)?;
    let [first, through] = certificate.interval;
    let count = through.checked_sub(first).and_then(|difference| difference.checked_add(1))
        .and_then(|count| usize::try_from(count).ok()).filter(|count| *count > 0 && *count <= maximum_entries)
        .ok_or_else(range_unavailable)?;
    if certificate.stream != config.stream || first < 0 { return Err(range_unavailable()); }
    let ensemble = Ensemble::new(config.clone());
    for server in 0..ensemble.len() {
        let copy = async {
            let mut retained = Vec::with_capacity(count);
            let mut remaining = maximum_bytes;
            let mut next = first;
            let mut chain = RetainedChain::new(&config.stream, first);
            while retained.len() < count {
                let limit = u32::try_from(count - retained.len()).unwrap_or(u32::MAX).min(MAXIMUM_BATCH_ENTRIES);
                let page = ensemble.read(server, next, limit, Duration::ZERO).await?;
                if next <= page.trim_lsn || page.commit_lsn < through || page.entries.is_empty() {
                    return Err(range_unavailable());
                }
                for entry in page.entries {
                    if entry.lsn != next || entry.lsn > through { return Err(range_unavailable()); }
                    remaining = remaining.checked_sub(entry.payload.len().saturating_add(64)).ok_or_else(range_unavailable)?;
                    chain.append(&entry);
                    retained.push(entry);
                    if next == through { break; }
                    next += 1;
                }
            }
            if chain.digest() != certificate.digest { return Err(range_unavailable()); }
            Ok::<_, Anomaly>(retained)
        }.await;
        if let Ok(entries) = copy { return Ok(entries); }
    }
    Err(range_unavailable())
}

/// Rechecks archived records against their exact interval in the original replica stream.
pub fn verify_certified_range(certificate: &JournalDigest, entries: &[crate::protocol::Entry]) -> Result<()> {
    let [first, through] = certificate.interval;
    if first < 0 || entries.first().map(|entry| entry.lsn) != Some(first)
        || entries.last().map(|entry| entry.lsn) != Some(through)
    {
        return Err(range_unavailable());
    }
    let mut chain = RetainedChain::new(&certificate.stream, first);
    for (index, entry) in entries.iter().enumerate() {
        if first.checked_add(i64::try_from(index).map_err(|_| range_unavailable())?) != Some(entry.lsn) {
            return Err(range_unavailable());
        }
        chain.append(entry);
    }
    if chain.digest() != certificate.digest { return Err(range_unavailable()); }
    Ok(())
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
                "Replica {} trimmed position {next} during the audit", ensemble.config.servers[server].address())));
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
        Anomaly::fault("rtdb.logkeeper/audit-gap", format!("Replica {} omitted committed position {position}", ensemble.config.servers[server].address()))
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
