//! One router per pool per dispatching process.
//!
//! The router replaces per-tenant pools: it holds one worker value per member
//! (one channel and one pressure feed, for a transport pool), counts in-flight
//! work per (key, worker) and removes a count when it returns to zero, and
//! memoizes each key's preference order for the current membership epoch.
//!
//! Routing never authorizes. A worker the router picks may still refuse the
//! work (its own directory, admission or a kernel fence decides); the caller
//! excludes it and selects again, which walks on to the next rank.
use std::{
    collections::{
        BTreeSet,
        HashMap,
    },
    fmt,
    sync::{
        Arc,
        Mutex,
        PoisonError,
        RwLock,
    },
};

use crate::{
    rank,
    select_ranked,
    Busy,
    Candidate,
    Policy,
    Pressure,
    Reason,
    Skip,
};

/// Keys whose order the memo keeps for one epoch before it starts over.
const MEMO_KEYS: usize = 4096;

/// One member of a routed pool, as the router holds it.
pub trait Worker: Send + Sync + 'static {
    /// The lifetime of the worker's caches; unique within the pool.
    fn id(&self) -> &str;
    /// How much work one key may have in flight on this worker.
    fn limit(&self) -> usize;
    /// Advisory; Unknown keeps affinity.
    fn pressure(&self) -> Pressure;
}

/// A pool's member set. Every member is eligible: the membership source
/// (lease or catalog) already decided who serves.
pub struct Membership<W> {
    /// The source revision that last changed the member set. It never
    /// decreases; a renewal with the same members keeps the installed epoch.
    pub epoch: u64,
    /// Only the first `width` ranks may serve one key.
    pub width: usize,
    pub workers: Vec<Arc<W>>,
}

/// Why a membership was not installed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MembershipRefused {
    /// Two members share an identity.
    Duplicate,
    /// The member set changed without a later epoch.
    Rewound,
}

impl fmt::Display for MembershipRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Duplicate => "Pool membership repeats a worker identity",
            Self::Rewound => "Pool membership changed without a later epoch",
        })
    }
}

impl std::error::Error for MembershipRefused {}

/// In-flight work per key per worker. Entries exist only while non-zero, so
/// the map is bounded by the work in flight, not by the keys ever routed.
#[derive(Default)]
struct Occupancy(Mutex<HashMap<String, HashMap<String, usize>>>);

impl Occupancy {
    fn reserve(&self, key: &str, worker: &str, limit: usize) -> bool {
        let mut keys = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let count = keys
            .entry(key.to_owned())
            .or_default()
            .entry(worker.to_owned())
            .or_default();
        if *count >= limit {
            if *count == 0 {
                Self::forget(&mut keys, key, worker);
            }
            return false;
        }
        *count += 1;
        true
    }

    fn release(&self, key: &str, worker: &str) {
        let mut keys = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(count) = keys
            .get_mut(key)
            .and_then(|workers| workers.get_mut(worker))
        else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            Self::forget(&mut keys, key, worker);
        }
    }

    fn forget(keys: &mut HashMap<String, HashMap<String, usize>>, key: &str, worker: &str) {
        if let Some(workers) = keys.get_mut(key) {
            workers.remove(worker);
            if workers.is_empty() {
                keys.remove(key);
            }
        }
    }

    fn count(&self, key: &str, worker: &str) -> usize {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
            .and_then(|workers| workers.get(worker))
            .copied()
            .unwrap_or(0)
    }

    fn keys(&self) -> usize {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).len()
    }
}

/// Each key's order, as indices into the epoch's sorted members.
#[derive(Default)]
struct Memo {
    epoch: u64,
    orders: HashMap<String, Arc<[usize]>>,
}

pub struct Router<W> {
    membership: RwLock<Option<Arc<Membership<W>>>>,
    occupancy: Arc<Occupancy>,
    memo: Mutex<Memo>,
}

impl<W: Worker> Default for Router<W> {
    fn default() -> Self {
        Self {
            membership: RwLock::new(None),
            occupancy: Arc::default(),
            memo: Mutex::default(),
        }
    }
}

impl<W: Worker> Router<W> {
    /// The installed membership; None before the first install.
    pub fn membership(&self) -> Option<Arc<Membership<W>>> {
        self.membership
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replace the membership. Work already selected keeps its worker and its
    /// count; a member that leaves and returns under the same identity finds
    /// its outstanding count again.
    pub fn install(&self, mut membership: Membership<W>) -> Result<(), MembershipRefused> {
        membership
            .workers
            .sort_by(|left, right| left.id().cmp(right.id()));
        if membership
            .workers
            .windows(2)
            .any(|pair| pair[0].id() == pair[1].id())
        {
            return Err(MembershipRefused::Duplicate);
        }
        let mut current = self
            .membership
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(current) = current.as_ref() {
            let unchanged = current.workers.len() == membership.workers.len()
                && current
                    .workers
                    .iter()
                    .zip(&membership.workers)
                    .all(|(left, right)| left.id() == right.id());
            if membership.epoch < current.epoch
                || membership.epoch == current.epoch && !unchanged
            {
                return Err(MembershipRefused::Rewound);
            }
            // Same members, same sorted order: memoized orders stay valid.
            if unchanged {
                membership.epoch = current.epoch;
            }
        }
        *current = Some(Arc::new(membership));
        Ok(())
    }

    /// Select a worker for `key` with the kernel's walk over the memoized
    /// order. `excluded` names workers that refused this request.
    pub fn select(
        &self,
        key: &str,
        excluded: &BTreeSet<String>,
        mut on_skip: impl FnMut(&W, usize, Skip),
    ) -> Result<Lease<W>, Busy> {
        let membership = self.membership().ok_or(Busy)?;
        let order = self.order(key, &membership);
        let candidates: Vec<_> = order
            .iter()
            .map(|&index| Entry {
                worker: &membership.workers[index],
                key,
                occupancy: &self.occupancy,
            })
            .collect();
        let selected = select_ranked(
            &candidates,
            excluded,
            Policy {
                width: membership.width,
            },
            |entry, rank, skip| on_skip(&**entry.worker, rank, skip),
        )?;
        Ok(Lease {
            slot: selected.slot,
            rank: selected.rank,
            reason: selected.reason,
        })
    }

    /// Work in flight for `key` on `worker`.
    pub fn in_flight(&self, key: &str, worker: &str) -> usize {
        self.occupancy.count(key, worker)
    }

    /// Keys with work in flight.
    pub fn busy_keys(&self) -> usize {
        self.occupancy.keys()
    }

    fn order(&self, key: &str, membership: &Membership<W>) -> Arc<[usize]> {
        {
            let memo = self.memo.lock().unwrap_or_else(PoisonError::into_inner);
            if memo.epoch == membership.epoch
                && let Some(order) = memo.orders.get(key)
            {
                return order.clone();
            }
        }
        let indexed: Vec<_> = membership.workers.iter().enumerate().collect();
        let order: Arc<[usize]> = rank(key, &indexed, |(_, worker)| worker.id())
            .into_iter()
            .map(|(index, _)| *index)
            .collect();
        let mut memo = self.memo.lock().unwrap_or_else(PoisonError::into_inner);
        if membership.epoch > memo.epoch {
            memo.epoch = membership.epoch;
            memo.orders.clear();
        }
        if memo.epoch == membership.epoch {
            if memo.orders.len() >= MEMO_KEYS {
                memo.orders.clear();
            }
            memo.orders.insert(key.to_owned(), order.clone());
        }
        order
    }
}

/// One member as the kernel's walk sees it for one key.
struct Entry<'a, W> {
    worker: &'a Arc<W>,
    key: &'a str,
    occupancy: &'a Arc<Occupancy>,
}

impl<W: Worker> Candidate for Entry<'_, W> {
    type Slot = Slot<W>;

    fn id(&self) -> &str {
        self.worker.id()
    }

    fn eligible(&self) -> bool {
        true
    }

    fn pressure(&self) -> Pressure {
        self.worker.pressure()
    }

    fn try_reserve(&self) -> Option<Slot<W>> {
        self.occupancy
            .reserve(self.key, self.worker.id(), self.worker.limit())
            .then(|| Slot {
                occupancy: self.occupancy.clone(),
                key: self.key.into(),
                worker: self.worker.clone(),
            })
    }
}

/// One unit of the key's bound on one worker, released when dropped.
struct Slot<W: Worker> {
    occupancy: Arc<Occupancy>,
    key: Box<str>,
    worker: Arc<W>,
}

impl<W: Worker> Drop for Slot<W> {
    fn drop(&mut self) {
        self.occupancy.release(&self.key, self.worker.id());
    }
}

/// A selected worker and its reservation. Dropping it releases the
/// reservation; the worker value lives as long as the lease, whatever the
/// membership does meanwhile.
pub struct Lease<W: Worker> {
    slot: Slot<W>,
    rank: usize,
    reason: Reason,
}

impl<W: Worker> Lease<W> {
    pub fn worker(&self) -> &Arc<W> {
        &self.slot.worker
    }

    /// One-based preference rank of the worker for this key.
    pub fn rank(&self) -> usize {
        self.rank
    }

    pub fn reason(&self) -> Reason {
        self.reason
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{
        AtomicBool,
        Ordering,
    };

    use super::*;

    struct Fake {
        id: &'static str,
        pressured: AtomicBool,
    }

    impl Worker for Fake {
        fn id(&self) -> &str {
            self.id
        }

        fn limit(&self) -> usize {
            1
        }

        fn pressure(&self) -> Pressure {
            if self.pressured.load(Ordering::Relaxed) {
                Pressure::Pressured
            } else {
                Pressure::Normal
            }
        }
    }

    fn fake(id: &'static str) -> Arc<Fake> {
        Arc::new(Fake {
            id,
            pressured: AtomicBool::new(false),
        })
    }

    fn membership(epoch: u64, width: usize, workers: &[&Arc<Fake>]) -> Membership<Fake> {
        Membership {
            epoch,
            width,
            workers: workers.iter().map(|worker| Arc::clone(worker)).collect(),
        }
    }

    fn order(key: &str, ids: &[&'static str]) -> Vec<&'static str> {
        rank(key, ids, |id| *id).into_iter().copied().collect()
    }

    fn pick(router: &Router<Fake>, key: &str) -> Result<Lease<Fake>, Busy> {
        router.select(key, &BTreeSet::new(), |_, _, _| {})
    }

    #[test]
    fn keys_overflow_in_rank_order_and_counts_vanish_at_zero() {
        let (a, b, c) = (fake("a"), fake("b"), fake("c"));
        let router = Router::default();
        assert_eq!(pick(&router, "k").err(), Some(Busy));
        router.install(membership(1, 2, &[&c, &a, &b])).unwrap();
        let ranked = order("k", &["a", "b", "c"]);
        let first = pick(&router, "k").unwrap();
        assert_eq!((first.worker().id, first.rank()), (ranked[0], 1));
        assert_eq!(first.reason(), Reason::Preferred);
        let second = pick(&router, "k").unwrap();
        assert_eq!(
            (second.worker().id, second.reason()),
            (ranked[1], Reason::CapacityOverflow)
        );
        // Width two: the third rank never serves this key.
        assert_eq!(pick(&router, "k").err(), Some(Busy));
        // Another key's bound is its own.
        let other = pick(&router, "other").unwrap();
        assert_eq!(other.rank(), 1);
        assert_eq!(router.in_flight("k", ranked[0]), 1);
        drop((first, second, other));
        assert_eq!(router.in_flight("k", ranked[0]), 0);
        assert_eq!(router.busy_keys(), 0);
        // Returned capacity returns the key to its preferred worker.
        assert_eq!(pick(&router, "k").unwrap().worker().id, ranked[0]);
    }

    #[test]
    fn epochs_never_rewind_and_renewals_keep_the_memo() {
        let (a, b) = (fake("a"), fake("b"));
        let router = Router::default();
        router.install(membership(5, 2, &[&a, &b])).unwrap();
        drop(pick(&router, "k").unwrap());
        // A renewal listing the same members keeps the epoch and its memo.
        router.install(membership(9, 2, &[&b, &a])).unwrap();
        assert_eq!(router.membership().unwrap().epoch, 5);
        assert_eq!(router.memo.lock().unwrap().orders.len(), 1);
        assert_eq!(
            router.install(membership(5, 2, &[&a])),
            Err(MembershipRefused::Rewound)
        );
        assert_eq!(
            router.install(membership(4, 2, &[&a, &b])),
            Err(MembershipRefused::Rewound)
        );
        assert_eq!(
            router.install(membership(10, 2, &[&a, &a])),
            Err(MembershipRefused::Duplicate)
        );
        router.install(membership(10, 2, &[&a])).unwrap();
        assert_eq!(pick(&router, "k").unwrap().worker().id, "a");
        assert_eq!(router.memo.lock().unwrap().epoch, 10);
    }

    #[test]
    fn withdrawn_workers_keep_accepted_work_and_their_counts() {
        let (a, b) = (fake("a"), fake("b"));
        let router = Router::default();
        router.install(membership(1, 2, &[&a, &b])).unwrap();
        let ranked = order("k", &["a", "b"]);
        let preferred = if ranked[0] == "a" { &a } else { &b };
        let other = if ranked[0] == "a" { &b } else { &a };
        let accepted = pick(&router, "k").unwrap();
        router.install(membership(2, 2, &[other])).unwrap();
        // The withdrawn worker keeps its accepted work and gets no new work.
        let next = pick(&router, "k").unwrap();
        assert_eq!(next.worker().id, other.id);
        drop(next);
        // Returning under the same identity finds the outstanding count.
        router.install(membership(3, 2, &[&a, &b])).unwrap();
        assert!(Arc::ptr_eq(accepted.worker(), preferred));
        let spill = pick(&router, "k").unwrap();
        assert_eq!(spill.worker().id, other.id);
        drop(accepted);
        assert_eq!(pick(&router, "k").unwrap().worker().id, preferred.id);
    }

    #[test]
    fn pressure_and_refusals_move_down_the_order() {
        let (a, b, c) = (fake("a"), fake("b"), fake("c"));
        let router = Router::default();
        router.install(membership(1, 3, &[&a, &b, &c])).unwrap();
        let ranked = order("k", &["a", "b", "c"]);
        let by_id = |id: &str| [&a, &b, &c].into_iter().find(|w| w.id == id).unwrap();
        by_id(ranked[0]).pressured.store(true, Ordering::Relaxed);
        let excluded = BTreeSet::from([ranked[1].to_owned()]);
        let selected = router.select("k", &excluded, |_, _, _| {}).unwrap();
        assert_eq!(
            (selected.worker().id, selected.reason()),
            (ranked[2], Reason::Alternate)
        );
        let fallback = router.select("k", &excluded, |_, _, _| {}).unwrap();
        assert_eq!(
            (fallback.worker().id, fallback.reason()),
            (ranked[0], Reason::PressureFallback)
        );
    }
}
