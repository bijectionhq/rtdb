#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::{multiset::Multiset, relations::{sorted_by, total_ordering}, seq_lib::lemma_sorted_unique};

#[cfg(not(verus_keep_ghost))]
use std::collections::BTreeMap;
#[cfg(not(verus_keep_ghost))]
use std::time::Duration;

#[cfg(not(verus_keep_ghost))]
use futures::stream::{FuturesUnordered, StreamExt};
#[cfg(not(verus_keep_ghost))]
use rtdb_types::{Anomaly, AnomalyCategory, Result};

#[cfg(not(verus_keep_ghost))]
use super::ensemble::{Ensemble, MAXIMUM_BATCH_ENTRIES};
#[cfg(not(verus_keep_ghost))]
use super::{JournalEndpoint, StreamConfig, validate};
#[cfg(not(verus_keep_ghost))]
use crate::history::RetainedChain;
#[cfg(not(verus_keep_ghost))]
use crate::protocol::Entry;
#[cfg(not(verus_keep_ghost))]
use crate::server::JournalDigest;

#[cfg(not(verus_keep_ghost))]
/// Fingerprints of the same committed range on all responding members of an ensemble.
#[derive(Debug, Default)]
pub struct RetainedAudit {
    pub interval: Option<[i64; 2]>,
    pub digests: BTreeMap<String, String>,
    pub windows: BTreeMap<String, Option<[i64; 2]>>,
    pub unavailable: BTreeMap<String, String>,
}

#[cfg_attr(not(verus_keep_ghost), derive(Clone, Debug))]
pub struct JournalSyncObservation {
    pub journal: String,
    pub synced_clock_upper_micros: i64,
}

#[cfg_attr(verus_keep_ghost, verus_spec(matches =>
    ensures matches == (through == offset && digest@ =~= expected_digest@),
))]
fn exact_audit_page(offset: i64, through: i64, digest: &[u8], expected_digest: &[u8]) -> bool {
    through == offset && digest == expected_digest
}

#[cfg_attr(verus_keep_ghost, verus_spec(matches =>
    ensures matches == (reported_origin@ =~= origin@ && reported_cutoff == cutoff),
))]
fn matching_original_origin(
    reported_origin: &[u8], origin: &[u8], reported_cutoff: Option<i64>, cutoff: Option<i64>,
) -> bool {
    reported_origin == origin && reported_cutoff == cutoff
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    ensures result == match (original_append, sample) {
        (Some(true), Some(upper)) => if upper > 0 { Ok(Some(upper)) } else { Err(()) },
        _ => Ok(None),
    },
))]
fn original_page_sample(original_append: Option<bool>, sample: Option<i64>) -> std::result::Result<Option<i64>, ()> {
    if original_append != Some(true) { return Ok(None); }
    match sample {
        Some(upper) if upper <= 0 => Err(()),
        _ => Ok(sample),
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    requires rehash.requires(()),
    ensures match result {
        Some(digest) => if carried@.len() == 0 { rehash.ensures((), digest) }
            else { carried@.len() == 32 && digest@ == carried@ },
        None => carried@.len() != 0 && carried@.len() != 32,
    },
))]
fn original_digest_to_probe<F: FnOnce() -> [u8; 32]>(carried: &[u8], rehash: F) -> Option<[u8; 32]> {
    if carried.is_empty() { return Some(rehash()); }
    if carried.len() != 32 { return None; }
    let mut digest = [0; 32];
    digest.copy_from_slice(carried);
    Some(digest)
}

#[cfg(verus_keep_ghost)]
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

spec fn qualifies(journal: Seq<char>, observation: JournalSyncObservation, cutoff: i64) -> bool {
    observation.journal@ == journal && 0 < observation.synced_clock_upper_micros < cutoff
}

spec fn is_earliest_sample(
    journal: Seq<char>, observations: Seq<JournalSyncObservation>, cutoff: i64, seen: int,
    earliest: Option<i64>,
) -> bool {
    &&& forall |i: int| 0 <= i < seen && #[trigger] qualifies(journal, observations[i], cutoff)
        ==> earliest.is_some() && earliest.unwrap() <= observations[i].synced_clock_upper_micros
    &&& match earliest {
        None => true,
        Some(best) => exists |i: int| 0 <= i < seen && #[trigger] qualifies(journal, observations[i], cutoff)
            && observations[i].synced_clock_upper_micros == best,
    }
}

spec fn earliest_vote(journal: Seq<char>, observations: Seq<JournalSyncObservation>, cutoff: i64) -> Option<i64> {
    choose |earliest: Option<i64>| is_earliest_sample(journal, observations, cutoff, observations.len() as int, earliest)
}

spec fn recorded_votes(
    roster: Seq<String>, observations: Seq<JournalSyncObservation>, cutoff: i64, seen: nat,
) -> Multiset<i64>
    decreases seen,
{
    if seen == 0 {
        Multiset::empty()
    } else {
        let preceding = recorded_votes(roster, observations, cutoff, (seen - 1) as nat);
        match earliest_vote(roster[seen - 1]@, observations, cutoff) {
            Some(sample) => preceding.insert(sample),
            None => preceding,
        }
    }
}

spec fn ordered_recorded_votes(
    ordered: Seq<i64>, roster: Seq<String>, observations: Seq<JournalSyncObservation>, cutoff: i64,
) -> bool {
    sorted_by(ordered, |a: i64, b: i64| a <= b)
        && ordered.to_multiset() == recorded_votes(roster, observations, cutoff, roster.len())
}

spec fn quorum_sample(ordered: Seq<i64>, write_quorum: usize) -> Option<i64> {
    if 0 < write_quorum <= ordered.len() { Some(ordered[write_quorum - 1]) } else { None }
}
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    ensures result == if qualifies(journal@, *observation, cutoff) {
        Some(observation.synced_clock_upper_micros)
    } else { None },
))]
fn qualifying_sample(journal: &str, observation: &JournalSyncObservation, cutoff: i64) -> Option<i64> {
    let upper = observation.synced_clock_upper_micros;
    if observation.journal.as_str() == journal && 0 < upper && upper < cutoff { Some(upper) } else { None }
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    ensures result == earliest_vote(journal@, observations@, cutoff),
))]
fn earliest_sample(journal: &str, observations: &[JournalSyncObservation], cutoff: i64) -> Option<i64> {
    let mut earliest = None;
    let mut i = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            0 <= i <= observations@.len(),
            is_earliest_sample(journal@, observations@, cutoff, i as int, earliest),
        decreases observations@.len() - i,
    ))]
    while i < observations.len() {
        if let Some(sample) = qualifying_sample(journal, &observations[i], cutoff) {
            if match earliest { None => true, Some(best) => sample < best } {
                earliest = Some(sample);
            }
        }
        i += 1;
    }
    #[cfg(verus_keep_ghost)]
    proof! {
        let chosen = earliest_vote(journal@, observations@, cutoff);
        assert(is_earliest_sample(journal@, observations@, cutoff, observations@.len() as int, chosen));
        if let Some(sample) = earliest {
            let i = choose |i: int| 0 <= i < observations@.len()
                && #[trigger] qualifies(journal@, observations@[i], cutoff)
                && observations@[i].synced_clock_upper_micros == sample;
            assert(qualifies(journal@, observations@[i], cutoff));
            assert(chosen.is_some() && chosen.unwrap() <= sample);
        }
        if let Some(sample) = chosen {
            let i = choose |i: int| 0 <= i < observations@.len()
                && #[trigger] qualifies(journal@, observations@[i], cutoff)
                && observations@[i].synced_clock_upper_micros == sample;
            assert(qualifies(journal@, observations@[i], cutoff));
            assert(earliest.is_some() && earliest.unwrap() <= sample);
        }
    }
    earliest
}

#[cfg_attr(verus_keep_ghost, verus_spec(result =>
    requires
        roster@.map_values(|journal: String| journal@).no_duplicates(),
        0 < write_quorum <= roster@.len(),
    ensures
        exists |ordered: Seq<i64>| #[trigger] ordered_recorded_votes(ordered, roster@, observations@, cutoff)
            && result == quorum_sample(ordered, write_quorum),
        forall |ordered: Seq<i64>| #[trigger] ordered_recorded_votes(ordered, roster@, observations@, cutoff)
            ==> result == quorum_sample(ordered, write_quorum),
))]
fn recorded_sync_bound(
    roster: &[String], observations: &[JournalSyncObservation], cutoff: i64, write_quorum: usize,
) -> Option<i64> {
    let mut samples = Vec::new();
    let mut i = 0;
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(samples@ =~= Seq::<i64>::empty());
        assert(recorded_votes(roster@, observations@, cutoff, 0) == Multiset::<i64>::empty());
    }
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            0 <= i <= roster@.len(),
            samples@.to_multiset() == recorded_votes(roster@, observations@, cutoff, i as nat),
        decreases roster@.len() - i,
    ))]
    while i < roster.len() {
        if let Some(sample) = earliest_sample(roster[i].as_str(), observations, cutoff) {
            samples.push(sample);
        }
        i += 1;
    }
    samples.sort_unstable();
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(ordered_recorded_votes(samples@, roster@, observations@, cutoff));
        let precedes = |a: i64, b: i64| a <= b;
        assert(total_ordering(precedes));
        assert forall |ordered: Seq<i64>| #[trigger] ordered_recorded_votes(ordered, roster@, observations@, cutoff)
            implies ordered == samples@ by {
            lemma_sorted_unique(ordered, samples@, precedes);
        }
    }
    if samples.len() < write_quorum { None } else { Some(samples[write_quorum - 1]) }
}

#[cfg(not(verus_keep_ghost))]
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
    let mut agreed: Option<[u8; 32]> = None;
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
        if !exact_audit_page(offset, page.through_lsn, &digest, agreed.as_ref().unwrap_or(&digest)) {
            return Err(Anomaly::fault("rtdb.logkeeper/audit-divergence", "Replicas disagree on the exact committed entry"));
        }
        agreed = Some(digest);
        agreeing += 1;
        let sample = original_page_sample(page.original_append, page.synced_clock_upper_micros)
            .map_err(|()| Anomaly::fault("rtdb.logkeeper/sync-observation-clock", "A journal returned an invalid sync sample"))?;
        if let Some(synced_clock_upper_micros) = sample {
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

#[cfg(not(verus_keep_ghost))]
/// Read the retained observations of the original voters for an already certified timed entry.
pub async fn original_sync_observations(
    certificate: &JournalDigest, entry: &Entry, endpoints: Vec<JournalEndpoint>, maximum_message_bytes: usize,
) -> Result<Vec<JournalSyncObservation>> {
    verify_certified_range(certificate, std::slice::from_ref(entry))?;
    let Some(origin) = entry.timed_origin.as_ref().filter(|_| entry.valid_guard()) else {
        return Err(range_unavailable());
    };
    let offset = entry.lsn;
    let digest = original_entry_digest(entry).ok_or_else(range_unavailable)?;
    let origin_identity = origin.fingerprint();
    // An address can have been reused by a different journal. Each probe binds the expected
    // identity on the connection, and at most one result per original voter can contribute.
    let mut by_journal = BTreeMap::new();
    for endpoint in endpoints {
        if let Some(journal) = endpoint.journal()
            && origin.journals.binary_search_by(|id| id.as_str().cmp(journal)).is_ok()
        {
            by_journal.entry(journal.to_owned()).or_insert(endpoint);
        }
    }
    if by_journal.is_empty() { return Ok(Vec::new()); }
    let ensemble = Ensemble::new(StreamConfig {
        servers: by_journal.into_values().collect(), stream: origin.stream.clone(),
        write_quorum: 1, maximum_message_bytes,
    });
    let mut pending = (0..ensemble.len()).map(|server| {
        let (ensemble, digest) = (&ensemble, &digest);
        async move { (server, ensemble.original_digest(server, offset, digest).await) }
    }).collect::<FuturesUnordered<_>>();
    let mut observations = Vec::new();
    while let Some((server, reply)) = pending.next().await {
        let Ok(page) = reply else { continue };
        if exact_audit_page(offset, page.through_lsn, &page.digest, &digest)
            && matching_original_origin(&page.timed_origin_identity, &origin_identity, page.guarded_before, entry.guarded_before)
            && let Ok(Some(synced_clock_upper_micros)) = original_page_sample(page.original_append, page.synced_clock_upper_micros)
        {
            observations.push(JournalSyncObservation {
                journal: ensemble.config.servers[server].journal().expect("filtered original identities").to_owned(),
                synced_clock_upper_micros,
            });
        }
    }
    Ok(observations)
}

#[cfg(not(verus_keep_ghost))]
/// The original stream's digest of this timed entry: computed from the original append's
/// exact bytes, or carried by a copy whose payload was resealed.
fn original_entry_digest(entry: &Entry) -> Option<[u8; 32]> {
    let origin = entry.timed_origin.as_ref()?;
    original_digest_to_probe(&origin.original_entry_sha256, || {
        let mut original = RetainedChain::new(&origin.stream, entry.lsn);
        original.append(entry);
        original.bytes()
    })
}

#[cfg(not(verus_keep_ghost))]
/// Before replacing a timed entry's payload, retain the digest its original voters hold.
/// Call it on the exact bytes read from the installation's replica stream.
pub fn retain_original_entry_digest(entry: &mut Entry) {
    if let Some(digest) = original_entry_digest(entry)
        && let Some(origin) = entry.timed_origin.as_mut()
    {
        origin.original_entry_sha256 = bytes::Bytes::copy_from_slice(&digest);
    }
}

#[cfg(not(verus_keep_ghost))]
/// The original quorum's earliest qualifying upper bound, if it can be recovered.
pub fn timely_original_sync_upper(entry: &Entry, observations: &[JournalSyncObservation]) -> Option<i64> {
    let cutoff = entry.guarded_before?;
    let origin = entry.timed_origin.as_ref().filter(|_| entry.valid_guard())?;
    recorded_sync_bound(&origin.journals, observations, cutoff, origin.write_quorum as usize)
}

#[cfg(not(verus_keep_ghost))]
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

#[cfg(not(verus_keep_ghost))]
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

#[cfg(not(verus_keep_ghost))]
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

#[cfg(not(verus_keep_ghost))]
fn range_unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.logkeeper/retained-range-unavailable", "The exact committed interval is unavailable")
}

#[cfg(not(verus_keep_ghost))]
/// A bounded cursor through an immutable committed interval. Pages are
/// provisional until the final raw history digest matches the certificate.
pub struct CertifiedRangeReader {
    ensemble: Ensemble,
    certificate: JournalDigest,
    chain: RetainedChain,
    next: i64,
    preferred: usize,
}

#[cfg(not(verus_keep_ghost))]
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

#[cfg(not(verus_keep_ghost))]
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

#[cfg(not(verus_keep_ghost))]
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
        if !entry.valid_guard() || first.checked_add(i64::try_from(index).map_err(|_| range_unavailable())?) != Some(entry.lsn) {
            return Err(range_unavailable());
        }
        chain.append(entry);
    }
    if chain.digest() != certificate.digest { return Err(range_unavailable()); }
    Ok(())
}

#[cfg(not(verus_keep_ghost))]
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

#[cfg(not(verus_keep_ghost))]
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

#[cfg(not(verus_keep_ghost))]
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

#[cfg(not(verus_keep_ghost))]
fn incomplete(ensemble: &Ensemble, server: usize, position: i64) -> Anomaly {
        Anomaly::fault("rtdb.logkeeper/audit-gap", format!("Replica {} omitted committed position {position}", ensemble.config.servers[server].address()))
}

#[cfg(all(test, not(verus_keep_ghost)))]
mod tests {
    use super::{JournalDigest, JournalSyncObservation, RetainedChain, quorum_overlap, timely_original_sync_upper, verify_certified_range};
    use crate::protocol::{Entry, TimedJournalOrigin};

    #[test]
    fn timed_audit_requires_distinct_original_votes_strictly_before_the_cutoff() {
        let journal = |id| uuid::Uuid::from_u128(id).to_string();
        let entry = Entry {
            lsn: 0, epoch: 1, guarded: true, guarded_before: Some(100),
            timed_origin: Some(TimedJournalOrigin {
                journals: (1..=3).map(journal).collect(), write_quorum: 2, stream: "original".to_owned(),
                clock_certificate_sha256: bytes::Bytes::from_static(&[7; 32]), original_entry_sha256: bytes::Bytes::new(),
            }),
            ..Default::default()
        };
        let sample = |id, synced_clock_upper_micros| JournalSyncObservation {
            journal: journal(id), synced_clock_upper_micros,
        };
        let mut observations = vec![sample(1, 75), sample(1, 80), sample(2, 100), sample(3, 110), sample(4, 10)];
        assert_eq!(timely_original_sync_upper(&entry, &observations), None);
        observations.push(sample(3, 99));
        assert_eq!(timely_original_sync_upper(&entry, &observations), Some(99));
    }

    #[test]
    fn certified_timed_record_rejects_a_different_clock_certificate_at_the_same_cutoff() {
        let mut entry = Entry {
            lsn: 0, epoch: 1, timestamp: 70, guarded: true, guarded_before: Some(100),
            timed_origin: Some(TimedJournalOrigin {
                journals: (1..=3).map(|id| uuid::Uuid::from_u128(id).to_string()).collect(),
                write_quorum: 2,
                stream: "original".to_owned(),
                clock_certificate_sha256: bytes::Bytes::from_static(&[7; 32]), original_entry_sha256: bytes::Bytes::new(),
            }),
            ..Default::default()
        };
        let mut chain = RetainedChain::new("copy", 0);
        chain.append(&entry);
        let proof = JournalDigest { stream: "copy".to_owned(), interval: [0, 0], digest: chain.digest() };
        assert!(verify_certified_range(&proof, std::slice::from_ref(&entry)).is_ok());

        entry.timed_origin.as_mut().unwrap().clock_certificate_sha256 = bytes::Bytes::from_static(&[8; 32]);
        assert!(entry.valid_guard());
        assert!(verify_certified_range(&proof, &[entry]).is_err());
    }

    #[test]
    fn a_resealed_copy_keeps_the_digest_its_original_voters_hold() {
        let mut entry = Entry {
            lsn: 4, epoch: 1, timestamp: 70, guarded: true, guarded_before: Some(100),
            payload: bytes::Bytes::from_static(b"source-sealed"),
            timed_origin: Some(TimedJournalOrigin {
                journals: (1..=3).map(|id| uuid::Uuid::from_u128(id).to_string()).collect(),
                write_quorum: 2,
                stream: "original".to_owned(),
                clock_certificate_sha256: bytes::Bytes::from_static(&[7; 32]), original_entry_sha256: bytes::Bytes::new(),
            }),
            ..Default::default()
        };
        let original = super::original_entry_digest(&entry).unwrap();
        let mut copied = entry.clone();
        super::retain_original_entry_digest(&mut copied);
        copied.payload = bytes::Bytes::from_static(b"target-sealed");
        assert!(copied.valid_guard());
        assert_eq!(super::original_entry_digest(&copied), Some(original));
        super::retain_original_entry_digest(&mut copied);
        assert_eq!(super::original_entry_digest(&copied), Some(original));

        let mut chain = RetainedChain::new("copy", 4);
        chain.append(&copied);
        let proof = JournalDigest { stream: "copy".to_owned(), interval: [4, 4], digest: chain.digest() };
        assert!(verify_certified_range(&proof, std::slice::from_ref(&copied)).is_ok());
        copied.timed_origin.as_mut().unwrap().original_entry_sha256 = bytes::Bytes::from_static(&[9; 32]);
        assert!(verify_certified_range(&proof, &[copied]).is_err());
        entry.payload = bytes::Bytes::from_static(b"target-sealed");
        assert_ne!(super::original_entry_digest(&entry), Some(original));
    }

    #[test]
    fn the_latest_comparison_range_falls_back_when_retained_windows_do_not_overlap_at_the_tip() {
        let windows = [(0, [3, 6]), (1, [4, 8]), (2, [9, 11])];
        assert_eq!(quorum_overlap(&windows, 2), Some([4, 6]));
        assert_eq!(quorum_overlap(&windows, 3), None);
    }
}
