use std::any::Any;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, AnomalyCategory, Result, RowVersion};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{ResolvedSearchRow, columnar::ColumnBatch, rank_cutoff::RankKey};

/// A partition's task and its ceiling in the query's SQL rank order.
pub struct RankWork {
    bound: RankKey,
    task: Box<dyn Any + Send>,
}

impl RankWork {
    pub fn new<T: Any + Send>(bound: RankKey, task: T) -> Self { Self { bound, task: Box::new(task) } }

    pub fn take<T: Any + Send>(self) -> Result<T> {
        self.task.downcast().map(|task| *task)
            .map_err(|_| Anomaly::fault("rtdb.query/rank-work", "Rank task belongs to a different source"))
    }
}

/// One visible payload whose score and SQL predicates were evaluated by the verifier.
pub struct VerifiedRankRow {
    pub(crate) key: RankKey,
    pub(crate) row: ResolvedSearchRow,
    pub(crate) projection: Option<VerifiedRankProjection>,
}

pub(crate) struct VerifiedRankProjection {
    pub(crate) columns: Arc<[rtdb_plan::ColumnId]>,
    pub(crate) batch: ColumnBatch,
}

impl VerifiedRankRow {
    pub fn version(&self) -> RowVersion { self.row.version }
    pub fn score(&self) -> f64 { self.key.score }
}

#[derive(Default)]
pub struct RankOutcome {
    pub more: Vec<RankWork>,
    pub verified: Vec<VerifiedRankRow>,
}

/// Expands work for one pinned partition, with all scheduling left to the query.
#[async_trait]
pub trait RankSource: Send {
    fn seed(&mut self) -> Result<Vec<RankWork>>;
    async fn execute(&mut self, work: RankWork) -> Result<Option<RankOutcome>>;
}

#[async_trait]
impl<S: RankSource + ?Sized> RankSource for Box<S> {
    fn seed(&mut self) -> Result<Vec<RankWork>> { (**self).seed() }
    async fn execute(&mut self, work: RankWork) -> Result<Option<RankOutcome>> { (**self).execute(work).await }
}

enum Work {
    Open,
    Source(RankWork),
}

struct Queued {
    bound: RankKey,
    partition: usize,
    sequence: usize,
    work: Work,
}

impl Ord for Queued {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bound.compare(other.bound).then_with(|| other.sequence.cmp(&self.sequence))
    }
}
impl PartialOrd for Queued { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
impl PartialEq for Queued { fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() } }
impl Eq for Queued {}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct WinnerId {
    partition: usize,
    version: RowVersion,
}

#[derive(Clone, Copy)]
struct WinnerKey {
    key: RankKey,
    id: WinnerId,
}

impl Ord for WinnerKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.compare(other.key).then_with(|| other.id.cmp(&self.id))
    }
}
impl PartialOrd for WinnerKey { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
impl PartialEq for WinnerKey { fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() } }
impl Eq for WinnerKey {}

struct VerifiedTopK {
    limit: NonZeroUsize,
    best: BinaryHeap<Reverse<WinnerKey>>,
    rows: BTreeMap<WinnerId, VerifiedRankRow>,
    ties: Vec<WinnerId>,
}

impl VerifiedTopK {
    fn kth(&self) -> Option<RankKey> {
        (self.best.len() == self.limit.get()).then(|| self.best.peek().map(|winner| winner.0.key)).flatten()
    }

    fn admit(&mut self, partition: usize, rows: Vec<VerifiedRankRow>) {
        for row in rows {
            let id = WinnerId { partition, version: row.row.version };
            if self.rows.contains_key(&id) { continue; }
            let key = WinnerKey { key: row.key, id };
            match self.kth() {
                Some(kth) if kth.beats(row.key) => continue,
                Some(kth) if row.key.beats(kth) => {
                    let displaced = self.best.pop().expect("a cutoff has a last winner").0;
                    self.best.push(Reverse(key));
                    let kth = self.kth().expect("the winner fills the vacated slot");
                    if kth.beats(displaced.key) {
                        self.rows.remove(&displaced.id);
                        for tied in self.ties.drain(..) { self.rows.remove(&tied); }
                    } else {
                        self.ties.push(displaced.id);
                    }
                }
                Some(_) => self.ties.push(id),
                None => self.best.push(Reverse(key)),
            }
            self.rows.insert(id, row);
        }
    }
}

struct RankingState {
    ready: BinaryHeap<Queued>,
    next: usize,
    in_flight: Vec<Option<RankKey>>,
    winners: VerifiedTopK,
    abandoned: bool,
    charge: Arc<MemoryReservation>,
}

impl RankingState {
    fn can_finish(&self) -> bool {
        let kth = self.winners.kth();
        let loses = |bound| kth.is_some_and(|kth| kth.beats(bound));
        self.ready.peek().is_none_or(|entry| loses(entry.bound))
            && self.in_flight.iter().flatten().copied().all(loses)
            && self.ready.iter().all(|entry| loses(entry.bound))
    }

    fn reserve(&mut self, work: usize, rows: usize) -> Result<()> {
        let work = self.ready.len().saturating_add(self.in_flight.len()).saturating_add(work);
        let rows = self.winners.rows.len().saturating_add(rows);
        let needed = 4096usize.saturating_add(work.saturating_mul(256)).saturating_add(rows.saturating_mul(512));
        let charge = Arc::get_mut(&mut self.charge).expect("the monitor owns its reservation");
        if needed > charge.bytes() { charge.grow_to(needed)?; }
        Ok(())
    }
}

/// The ranked query's work admission, outstanding bounds and verified SQL prefix.
pub struct QueryRanking {
    state: Mutex<RankingState>,
    ready: Notify,
}

impl QueryRanking {
    pub fn new(limit: NonZeroUsize, partitions: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let mut state = RankingState {
            ready: BinaryHeap::new(), next: partitions, in_flight: Vec::new(),
            winners: VerifiedTopK { limit, best: BinaryHeap::new(), rows: BTreeMap::new(), ties: Vec::new() },
            abandoned: false, charge: memory.reserve(4096)?,
        };
        state.reserve(partitions.saturating_mul(2), 0)?;
        state.in_flight.resize(partitions, None);
        state.ready.extend((0..partitions).map(|partition| Queued {
            partition, bound: RankKey::unknown(), sequence: partition, work: Work::Open,
        }));
        Ok(Self { state: Mutex::new(state), ready: Notify::new() })
    }

    async fn claim(&self, partition: usize, cancellation: &CancellationToken) -> Result<Option<(RankClaim<'_>, Work)>> {
        const MAX_IN_FLIGHT: usize = 4;
        loop {
            if cancellation.is_cancelled() { return Err(cancelled()); }
            let notified = self.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock();
                if state.abandoned || state.can_finish() { return Ok(None); }
                if state.ready.peek().is_some_and(|entry| entry.partition == partition)
                    && state.in_flight.iter().flatten().count() < MAX_IN_FLIGHT
                {
                    let entry = state.ready.pop().expect("the highest bound was checked");
                    state.in_flight[partition] = Some(entry.bound);
                    self.ready.notify_waiters();
                    return Ok(Some((RankClaim { ranking: self, partition, completed: false }, entry.work)));
                }
            }
            cancellation.run_until_cancelled(notified).await;
        }
    }

    pub fn winners(self) -> Option<QueryWinners> {
        let state = self.state.into_inner();
        if state.abandoned || !state.can_finish() { return None; }
        let RankingState { ready, in_flight, winners, mut charge, .. } = state;
        let mut partitions: Vec<_> = (0..in_flight.len()).map(|_| Vec::new()).collect();
        let retained = 4096usize.saturating_add(winners.rows.len().saturating_mul(512))
            .saturating_add(partitions.len().saturating_mul(256));
        drop(ready);
        drop(winners.best);
        drop(winners.ties);
        for (id, row) in winners.rows { partitions[id.partition].push(row); }
        if retained < charge.bytes() {
            Arc::get_mut(&mut charge).expect("the monitor owns its reservation").shrink_to(retained)
                .expect("the retained winners fit in the frontier reservation");
        }
        Some(QueryWinners { partitions, _reservation: charge })
    }
}

/// Verified rows from a complete frontier, backed by the query's reservation through projection.
pub struct QueryWinners {
    partitions: Vec<Vec<VerifiedRankRow>>,
    _reservation: Arc<MemoryReservation>,
}

impl QueryWinners {
    pub fn take_partition(&mut self, partition: usize) -> Vec<VerifiedRankRow> {
        std::mem::take(&mut self.partitions[partition])
    }
}

struct RankClaim<'a> {
    ranking: &'a QueryRanking,
    partition: usize,
    completed: bool,
}

impl RankClaim<'_> {
    fn complete(mut self, outcome: RankOutcome) -> Result<()> {
        let mut state = self.ranking.state.lock();
        state.reserve(outcome.more.len(), outcome.verified.len())?;
        for work in outcome.more {
            let sequence = state.next;
            state.next += 1;
            state.ready.push(Queued { partition: self.partition, bound: work.bound, sequence, work: Work::Source(work) });
        }
        state.winners.admit(self.partition, outcome.verified);
        state.in_flight[self.partition] = None;
        self.completed = true;
        self.ranking.ready.notify_waiters();
        Ok(())
    }
}

impl Drop for RankClaim<'_> {
    fn drop(&mut self) {
        if self.completed { return; }
        let mut state = self.ranking.state.lock();
        state.in_flight[self.partition] = None;
        state.abandoned = true;
        self.ranking.ready.notify_waiters();
    }
}

fn cancelled() -> Anomaly {
    Anomaly::new(AnomalyCategory::Interrupted, "rtdb.query/cancelled", "Ranked scan cancelled")
}

#[doc(hidden)]
pub async fn run_rank_partition<S: RankSource>(
    ranking: &QueryRanking, partition: usize, prepare: impl Future<Output = Result<Option<S>>>,
    cancellation: &CancellationToken,
) -> Result<Option<S>> {
    let mut prepare = std::pin::pin!(prepare);
    let mut source = None::<S>;
    while let Some((claim, work)) = ranking.claim(partition, cancellation).await? {
        let outcome = match work {
            Work::Open => {
                let opened = cancellation.run_until_cancelled(prepare.as_mut()).await.ok_or_else(cancelled)??;
                let Some(mut opened) = opened else { return Ok(None); };
                let more = opened.seed()?;
                source = Some(opened);
                RankOutcome { more, ..Default::default() }
            }
            Work::Source(work) => {
                let source = source.as_mut().expect("only an opened partition publishes tasks");
                let outcome = cancellation.run_until_cancelled(source.execute(work)).await.ok_or_else(cancelled)??;
                let Some(outcome) = outcome else { return Ok(None); };
                outcome
            }
        };
        claim.complete(outcome)?;
    }
    Ok(source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SearchRowLocation;
    use rtdb_format::keys::TrieKey;
    use rtdb_types::{BlockIndex, Iid, Interval, TemporalBounds, Timestamp};

    fn ranking(limit: usize, partitions: usize) -> QueryRanking {
        QueryRanking::new(NonZeroUsize::new(limit).unwrap(), partitions, &MemoryBudget::new(1 << 20)).unwrap()
    }

    fn row(id: u8, version: i64, key: RankKey) -> VerifiedRankRow {
        VerifiedRankRow { key, row: ResolvedSearchRow {
            version: RowVersion { position: None, iid: Iid([id; 16]), system_from: Timestamp(version), commit_position: 0 },
            origin: None,
            location: SearchRowLocation::Persisted { trie: TrieKey::l0(BlockIndex(0)).unwrap(), page: 0, row: 0 },
            visibility: TemporalBounds { valid_time: Interval::ALL, system_time: Interval::ALL },
        }, projection: None }
    }

    #[tokio::test]
    async fn queued_and_claimed_work_hold_their_bounds_until_children_and_winners_are_published() {
        let ranking = ranking(1, 2);
        let stop = CancellationToken::new();
        let (first, _) = ranking.claim(0, &stop).await.unwrap().unwrap();
        first.complete(RankOutcome {
            more: Vec::new(),
            verified: vec![row(1, 0, RankKey::integer(4.0, None))],
        }).unwrap();
        assert!(!ranking.state.lock().can_finish());

        let (second, _) = ranking.claim(1, &stop).await.unwrap().unwrap();
        assert!(!ranking.state.lock().can_finish());
        second.complete(RankOutcome {
            more: vec![RankWork::new(RankKey::integer(4.0, None), ())],
            verified: Vec::new(),
        }).unwrap();
        assert!(!ranking.state.lock().can_finish());

        let (tie, _) = ranking.claim(1, &stop).await.unwrap().unwrap();
        assert!(!ranking.state.lock().can_finish());
        tie.complete(RankOutcome {
            more: vec![RankWork::new(RankKey::integer(3.0, None), ())],
            verified: vec![row(2, 0, RankKey::integer(5.0, None))],
        }).unwrap();
        assert!(ranking.state.lock().can_finish());
        let mut winners = ranking.winners().unwrap();
        assert!(winners.take_partition(0).is_empty());
        assert_eq!(winners.take_partition(1)[0].version().iid, Iid([2; 16]));
    }

    #[tokio::test]
    async fn rejecting_a_stale_nomination_preserves_the_verified_version_of_that_iid() {
        let ranking = ranking(2, 1);
        let stop = CancellationToken::new();
        let earliest = row(7, 10, RankKey::integer(3.0, Some(7)));
        let latest = row(7, 11, RankKey::integer(2.0, Some(7)));
        let stale = RankWork::new(RankKey::unknown(), earliest.version());
        let (initial, _) = ranking.claim(0, &stop).await.unwrap().unwrap();
        initial.complete(RankOutcome { more: vec![stale], verified: vec![latest] }).unwrap();
        let (rejected, task) = ranking.claim(0, &stop).await.unwrap().unwrap();
        let Work::Source(task) = task else { panic!("missing stale nomination"); };
        assert_eq!(task.take::<RowVersion>().unwrap(), earliest.version());
        rejected.complete(RankOutcome::default()).unwrap();
        let mut winners = ranking.winners().unwrap();
        let rows = winners.take_partition(0);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].version().system_from, Timestamp(11));
    }

    #[tokio::test]
    async fn signed_zero_ties_need_identity_proof_and_nan_bounds_cannot_be_pruned() {
        for (winner, later, same) in [
            (RankKey::integer(-0.0, Some(-1)), RankKey::integer(0.0, Some(0)), RankKey::integer(0.0, Some(-1))),
            (RankKey::uuid(-0.0, [0; 16]), RankKey::uuid(0.0, [1; 16]), RankKey::uuid(0.0, [0; 16])),
        ] {
            for (bound, finish) in [
                (later, true), (same, false), (RankKey::integer(0.0, None), false),
                (RankKey::integer(f64::NAN, None), false), (RankKey::unknown(), false),
            ] {
                let ranking = ranking(1, 1);
                let stop = CancellationToken::new();
                let (claim, _) = ranking.claim(0, &stop).await.unwrap().unwrap();
                claim.complete(RankOutcome {
                    more: vec![RankWork::new(bound, ())],
                    verified: vec![row(0, 0, winner)],
                }).unwrap();
                assert_eq!(ranking.state.lock().can_finish(), finish, "{winner:?} against {bound:?}");
            }
        }
    }

    #[tokio::test]
    async fn tied_rows_survive_until_the_cutoff_moves_above_their_score() {
        let ranking = ranking(2, 1);
        let stop = CancellationToken::new();
        let (initial, _) = ranking.claim(0, &stop).await.unwrap().unwrap();
        initial.complete(RankOutcome {
            more: vec![RankWork::new(RankKey::unknown(), ())],
            verified: (1..=3).map(|id| row(id, 0, RankKey::integer(1.0, None))).collect(),
        }).unwrap();
        assert_eq!(ranking.state.lock().winners.rows.len(), 3);
        let (better, _) = ranking.claim(0, &stop).await.unwrap().unwrap();
        better.complete(RankOutcome {
            more: Vec::new(),
            verified: (4..=5).map(|id| row(id, 0, RankKey::integer(2.0, None))).collect(),
        }).unwrap();
        let mut winners = ranking.winners().unwrap();
        let rows = winners.take_partition(0);
        assert_eq!(rows.iter().map(|row| row.version().iid).collect::<Vec<_>>(), [Iid([4; 16]), Iid([5; 16])]);
    }

    #[tokio::test]
    async fn cancelled_claim_prevents_publication_of_a_partial_result() {
        let ranking = ranking(1, 1);
        let stop = CancellationToken::new();
        let (claim, _) = ranking.claim(0, &stop).await.unwrap().unwrap();
        drop(claim);
        assert!(ranking.claim(0, &stop).await.unwrap().is_none());
        assert!(ranking.winners().is_none());
    }
}
