//! Cache-affinity routing policy shared by every pool.
//!
//! A router sends work for one key to the worker whose caches are most likely
//! warm for it, without ever depending on that worker for correctness. Every
//! pool uses the same three pieces:
//!
//! - [`score`]: a specified rendezvous (highest random weight) hash of a key
//!   and a worker identity. Its bytes are fixed by `fixtures/vectors.json`,
//!   which the Go controller mirror checks too, so prewarm hints and dispatch
//!   agree across languages and toolchains.
//! - [`rank`] and [`warm_set`]: every worker in preference order, and its first
//!   `k`. Removing or adding a worker never reorders the others, so membership
//!   changes move only the keys that preferred the changed worker.
//! - [`select`]: walk the order, skip ineligible and excluded workers, set
//!   pressured ones aside, take the first that grants a slot under its per-key
//!   bound, then fall back to pressured workers in rank order, else refuse.
//!   Occupancy and pressure gate preference; they never reorder ranks, so a hot
//!   key's overflow always warms the same second and third workers.
//!
//! The key is the prefix of the cache key the worker uses: a deployment for
//! execution, a physical storage namespace for remote search reads, the
//! operation for maintenance (which has no reusable cache, so this spreads
//! work and keeps a retry on the worker that saw it). The worker identity is
//! the lifetime of the worker's caches, never an address it can be reached at
//! when a lease names it.
//!
//! [`Router`] holds one pool's membership for a whole dispatching process:
//! one worker value per member, per-(key, worker) in-flight counts and a rank
//! memo keyed by the membership epoch.
//!
//! This crate is pure policy: no transport, clock, membership source or
//! metrics. Writer placement is not affinity: [`score`] may rank the kernel
//! members that may claim an unplaced namespace, but the catalog record and
//! its placement epoch are the authority, never this order.
use std::collections::BTreeSet;

use sha2::{
    Digest,
    Sha256,
};

mod router;

pub use router::{
    Lease,
    Membership,
    MembershipRefused,
    Router,
    Worker,
};

/// Domain separator of the score. The historical `funrun` name is part of the
/// specified bytes; renaming it means regenerating every vector.
const DOMAIN: &[u8] = b"bijection.funrun.affinity.v1\0";

/// Rendezvous score of `worker` for `key`; higher is preferred.
///
/// `SHA-256(DOMAIN || be32(len key) || key || be32(len worker) || worker)`,
/// over UTF-8 bytes. Length prefixes keep `("ab", "c")` and `("a", "bc")`
/// distinct; the result is independent of Rust's `Hash`, request inputs and
/// candidate order.
pub fn score(key: &str, worker: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(DOMAIN);
    for identity in [key, worker] {
        let length = u32::try_from(identity.len()).expect("bounded affinity identity");
        hasher.update(length.to_be_bytes());
        hasher.update(identity.as_bytes());
    }
    hasher.finalize().into()
}

/// Every worker, most preferred first. Ties (never expected) break on the
/// worker identity, so the order is total and deterministic.
pub fn rank<'a, W>(key: &str, workers: &'a [W], id: impl Fn(&W) -> &str) -> Vec<&'a W> {
    let mut ranked: Vec<_> = workers.iter().collect();
    ranked.sort_by_cached_key(|worker| {
        (
            std::cmp::Reverse(score(key, id(worker))),
            id(worker).to_owned(),
        )
    });
    ranked
}

/// The first `k` workers of [`rank`]: the workers that should be warm for
/// `key`.
pub fn warm_set<'a, W>(
    key: &str,
    workers: &'a [W],
    id: impl Fn(&W) -> &str,
    k: usize,
) -> Vec<&'a W> {
    let mut ranked = rank(key, workers, id);
    ranked.truncate(k);
    ranked
}

/// A worker's advisory load signal. Unknown keeps affinity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pressure {
    Unknown,
    Normal,
    Pressured,
}

/// What a pool tells the walk about one worker. The walk never sees
/// channels, leases or wire types.
pub trait Candidate {
    /// A reservation under the per-key bound, released when dropped.
    type Slot;
    /// The worker's identity: the lifetime of its caches.
    fn id(&self) -> &str;
    /// Membership current, endpoint present, not draining.
    fn eligible(&self) -> bool;
    /// Advisory; consulted only for eligible, non-excluded workers.
    fn pressure(&self) -> Pressure;
    /// Atomically take one slot under this key's bound on this worker.
    fn try_reserve(&self) -> Option<Self::Slot>;
}

/// Why the selected worker was chosen, from the first reason a higher rank
/// was passed over. One vocabulary for every pool's metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    /// The highest-ranked worker took the work.
    Preferred,
    /// A higher rank had no free slot under the per-key bound.
    CapacityOverflow,
    /// A higher rank was not a current member.
    Ineligible,
    /// The caller excluded a worker after a proven refusal.
    Alternate,
    /// A higher rank was skipped because it reported pressure.
    Pressured,
    /// Every eligible worker with capacity was pressured.
    PressureFallback,
}

impl Reason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preferred => "preferred",
            Self::CapacityOverflow => "capacity_overflow",
            Self::Ineligible => "ineligible",
            Self::Alternate => "alternate",
            Self::Pressured => "pressured",
            Self::PressureFallback => "pressure_fallback",
        }
    }
}

/// Why a rank was passed over, for the caller's diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Skip {
    Ineligible,
    Excluded,
    Pressured,
    CapacityOverflow,
    /// Beyond the policy's width.
    Width,
}

impl Skip {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ineligible => "ineligible",
            Self::Excluded => "server_refusal",
            Self::Pressured => "pressured",
            Self::CapacityOverflow => "capacity_overflow",
            Self::Width => "width",
        }
    }
}

/// Routing limits that are not per worker.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    /// Only the first `width` ranks may serve the key. It bounds how many
    /// workers one key can make itself resident on; `usize::MAX` when the
    /// membership is already bounded upstream.
    pub width: usize,
}

impl Policy {
    pub const UNBOUNDED: Self = Self { width: usize::MAX };
}

pub struct Selected<'a, C: Candidate> {
    pub worker: &'a C,
    pub slot: C::Slot,
    /// One-based preference rank of the selected worker.
    pub rank: usize,
    pub reason: Reason,
}

/// Every eligible worker within width is excluded or out of slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Busy;

/// The bounded selection walk. `on_skip` receives every passed-over worker
/// with its one-based rank, for diagnostics only.
pub fn select<'a, C: Candidate>(
    key: &str,
    workers: &'a [C],
    excluded: &BTreeSet<String>,
    policy: Policy,
    on_skip: impl FnMut(&C, usize, Skip),
) -> Result<Selected<'a, C>, Busy> {
    // Rank the whole known membership so expired preferences stay visible in
    // diagnostics instead of silently making an alternate rank first.
    select_ranked(
        rank(key, workers, |worker| worker.id()),
        excluded,
        policy,
        on_skip,
    )
}

/// The same walk over workers already in [`rank`] order for the key, such as
/// a router's memoized order.
pub fn select_ranked<'a, C: Candidate>(
    ranked: impl IntoIterator<Item = &'a C>,
    excluded: &BTreeSet<String>,
    policy: Policy,
    mut on_skip: impl FnMut(&C, usize, Skip),
) -> Result<Selected<'a, C>, Busy> {
    let mut reason = (!excluded.is_empty()).then_some(Reason::Alternate);
    let mut pressured = Vec::new();
    for (index, worker) in ranked.into_iter().enumerate() {
        let rank = index + 1;
        if index >= policy.width {
            on_skip(worker, rank, Skip::Width);
            continue;
        }
        if !worker.eligible() {
            reason.get_or_insert(Reason::Ineligible);
            on_skip(worker, rank, Skip::Ineligible);
            continue;
        }
        if excluded.contains(worker.id()) {
            on_skip(worker, rank, Skip::Excluded);
            continue;
        }
        if worker.pressure() == Pressure::Pressured {
            reason.get_or_insert(Reason::Pressured);
            on_skip(worker, rank, Skip::Pressured);
            pressured.push((rank, worker));
            continue;
        }
        if let Some(slot) = worker.try_reserve() {
            return Ok(Selected {
                worker,
                slot,
                rank,
                reason: reason.unwrap_or(Reason::Preferred),
            });
        }
        reason.get_or_insert(Reason::CapacityOverflow);
        on_skip(worker, rank, Skip::CapacityOverflow);
    }
    // Every eligible worker with capacity is pressured: pressure only orders
    // preference, so keep using them in rank order.
    for (rank, worker) in pressured {
        if let Some(slot) = worker.try_reserve() {
            return Ok(Selected {
                worker,
                slot,
                rank,
                reason: Reason::PressureFallback,
            });
        }
    }
    Err(Busy)
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        collections::BTreeSet,
    };

    use super::*;

    #[test]
    fn score_matches_shared_vectors() {
        #[derive(serde::Deserialize)]
        struct Vector {
            deployment: String,
            worker: String,
            score: String,
        }
        let vectors: Vec<Vector> =
            serde_json::from_str(include_str!("../fixtures/vectors.json")).unwrap();
        assert!(!vectors.is_empty());
        for vector in vectors {
            let hex: String = score(&vector.deployment, &vector.worker)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            assert_eq!(hex, vector.score);
        }
    }

    #[test]
    fn membership_changes_preserve_existing_order() {
        fn ids<'a>(workers: &[&'a str]) -> Vec<&'a str> {
            rank("tenant", workers, |id| *id)
                .into_iter()
                .copied()
                .collect()
        }
        let original = ids(&["a", "b", "c"]);
        assert_eq!(original, ids(&["c", "a", "b"]));
        let expanded = ids(&["d", "c", "b", "a"]);
        assert_eq!(
            original,
            expanded
                .into_iter()
                .filter(|id| *id != "d")
                .collect::<Vec<_>>()
        );
        assert_eq!(
            ids(&["a", "c"]),
            original
                .into_iter()
                .filter(|id| *id != "b")
                .collect::<Vec<_>>()
        );
        assert_eq!(
            warm_set("tenant", &["a", "b", "c"], |id| *id, 2),
            rank("tenant", &["a", "b", "c"], |id| *id)[..2]
        );
    }

    struct Fake {
        id: &'static str,
        eligible: bool,
        pressure: Pressure,
        free: Cell<usize>,
    }

    impl Candidate for Fake {
        type Slot = &'static str;

        fn id(&self) -> &str {
            self.id
        }

        fn eligible(&self) -> bool {
            self.eligible
        }

        fn pressure(&self) -> Pressure {
            self.pressure
        }

        fn try_reserve(&self) -> Option<&'static str> {
            let free = self.free.get();
            (free > 0).then(|| {
                self.free.set(free - 1);
                self.id
            })
        }
    }

    fn fake(id: &'static str) -> Fake {
        Fake {
            id,
            eligible: true,
            pressure: Pressure::Normal,
            free: Cell::new(1),
        }
    }

    fn order(workers: &[Fake]) -> Vec<&'static str> {
        rank("k", workers, |worker| worker.id)
            .into_iter()
            .map(|worker| worker.id)
            .collect()
    }

    #[test]
    fn walk_prefers_rank_then_overflows_in_order() {
        let workers = [fake("a"), fake("b"), fake("c")];
        let order = order(&workers);
        let first = select(
            "k",
            &workers,
            &BTreeSet::new(),
            Policy::UNBOUNDED,
            |_, _, _| {},
        )
        .unwrap();
        assert_eq!(
            (first.slot, first.rank, first.reason),
            (order[0], 1, Reason::Preferred)
        );
        let second = select(
            "k",
            &workers,
            &BTreeSet::new(),
            Policy::UNBOUNDED,
            |_, _, _| {},
        )
        .unwrap();
        assert_eq!(
            (second.slot, second.rank, second.reason),
            (order[1], 2, Reason::CapacityOverflow)
        );
        let narrow = Policy { width: 2 };
        assert_eq!(
            select("k", &workers, &BTreeSet::new(), narrow, |_, _, _| {}).err(),
            Some(Busy)
        );
    }

    #[test]
    fn pressure_and_exclusion_never_reorder() {
        let mut workers = [fake("a"), fake("b"), fake("c")];
        let order = order(&workers);
        for worker in &mut workers {
            if worker.id == order[0] {
                worker.pressure = Pressure::Pressured;
            }
        }
        let excluded = BTreeSet::from([order[1].to_owned()]);
        let selected = select("k", &workers, &excluded, Policy::UNBOUNDED, |_, _, _| {}).unwrap();
        // The exclusion reason wins: it was decided before the walk.
        assert_eq!(
            (selected.slot, selected.reason),
            (order[2], Reason::Alternate)
        );
        let fallback = select("k", &workers, &excluded, Policy::UNBOUNDED, |_, _, _| {}).unwrap();
        assert_eq!(
            (fallback.slot, fallback.rank, fallback.reason),
            (order[0], 1, Reason::PressureFallback)
        );
    }
}
