//! The store's judgement of one append and the choices of one read, as pure
//! functions over what the store holds (MULTI-PLACE §4.1, §8 V9; Lean
//! `Bijection.Endgame.PersistenceContract` P1, P3, P4 and P7).
//!
//! [`super::memory::MemoryPersistence`] decides through these and nothing
//! else. After an append's reads ([`super::conflicts`]), [`admit`] judges the
//! clock reading against the last stamp and the window and assigns the
//! stamp; a read picks each key's visible version with [`latest_at`] and
//! resumes a page with [`skips`]. [`super::Window::outside`] is [`outside`].
//!
//! V9, proved over these exact bytes (Verus area `owned_multi_place`):
//!
//!   * [`admit`] is Lean's `stampedOutcome` past its conflict check (P1,
//!     P3): outside exactly when the reading misses the window, then
//!     validated at the head when write-free, else committed with stamp
//!     `⟨reading, head + 1⟩`. The reading is never clamped: one below the
//!     last stamp's micros is refused, so committed micros never decrease
//!     (P4);
//!   * [`latest_at`] is the newest version at or before the read's (Lean's
//!     `Store.read`), and [`skips`] resumes strictly past `after` in the
//!     read's direction;
//!   * a read applying one per-key entry to the keys in descending order
//!     returns the ascending read reversed ([`lemma_descending_is_ascending_reversed`],
//!     P7).
//!
//! Outside: the `BTreeMap` and `Mutex` the store keeps these in, that a
//! range enumerates its keys ascending and `rev()` descending, that each
//! key's versions are pushed in commit order, and the receipt table.
//!
//! The module imports only `store_read::order`, so the area includes these
//! exact bytes by `#[path]`. EVERY ANNOTATION IS `cfg`-GATED ON
//! `verus_keep_ghost`: an ordinary build gains no dependency and no code.
#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
use store_read::order::at_most;
use store_read::order::le_bytes;

/// The side of a [`super::Window`] the store's clock fell on.
#[cfg_attr(not(verus_keep_ghost), derive(Clone, Copy, Debug, PartialEq, Eq, Hash))]
pub enum Side {
    /// Before `not_before`: the window has not opened.
    Early,
    /// At or past `before`: the window has closed.
    Late,
}

/// What an append whose reads passed becomes at one clock reading.
#[cfg_attr(not(verus_keep_ghost), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum Admitted {
    /// The reading lies below the last stamp's micros: refused, never raised.
    Regressed,
    /// The reading misses the window: nothing is written, and no receipt.
    Outside(Side),
    /// A write-free append validated its reads through the head.
    Validated(u64),
    /// No version follows the head.
    Exhausted,
    /// Committed with the stamp `{ micros, version }`.
    Committed { micros: i64, version: u64 },
}

#[cfg(verus_keep_ghost)]
verus! {

/// Lean `Window.outside`.
pub open spec fn outside_spec(not_before: Option<i64>, before: Option<i64>, micros: i64) -> Option<Side> {
    if not_before is Some && micros < not_before->Some_0 {
        Some(Side::Early)
    } else if before is Some && before->Some_0 <= micros {
        Some(Side::Late)
    } else {
        None
    }
}

/// Lean `stampedOutcome` past its conflict check, with the reference store's
/// refusal of a regressed reading and of an exhausted version space.
pub open spec fn admitted(
    head: u64, last: Option<i64>, micros: i64, not_before: Option<i64>, before: Option<i64>, write_free: bool,
) -> Admitted {
    if last is Some && micros < last->Some_0 {
        Admitted::Regressed
    } else if outside_spec(not_before, before, micros) is Some {
        Admitted::Outside(outside_spec(not_before, before, micros)->Some_0)
    } else if write_free {
        Admitted::Validated(head)
    } else if head == u64::MAX {
        Admitted::Exhausted
    } else {
        Admitted::Committed { micros, version: (head + 1) as u64 }
    }
}

/// A read over keys in ascending order: each key's entry, if any (Lean
/// `readKeys`).
pub open spec fn ascending<K, E>(keys: Seq<K>, entry: spec_fn(K) -> Option<E>) -> Seq<E>
    decreases keys.len(),
{
    if keys.len() == 0 {
        Seq::empty()
    } else {
        let rest = ascending(keys.drop_last(), entry);
        match entry(keys.last()) {
            Some(e) => rest.push(e),
            None => rest,
        }
    }
}

/// The same read over the keys in descending order (Lean `readKeysReverse`).
pub open spec fn descending<K, E>(keys: Seq<K>, entry: spec_fn(K) -> Option<E>) -> Seq<E>
    decreases keys.len(),
{
    if keys.len() == 0 {
        Seq::empty()
    } else {
        let rest = descending(keys.drop_last(), entry);
        match entry(keys.last()) {
            Some(e) => seq![e] + rest,
            None => rest,
        }
    }
}

pub open spec fn reversed<E>(s: Seq<E>) -> Seq<E>
    decreases s.len(),
{
    if s.len() == 0 { Seq::empty() } else { seq![s.last()] + reversed(s.drop_last()) }
}

/// P7: a descending read is the ascending read reversed.
pub proof fn lemma_descending_is_ascending_reversed<K, E>(keys: Seq<K>, entry: spec_fn(K) -> Option<E>)
    ensures
        descending(keys, entry) == reversed(ascending(keys, entry)),
    decreases keys.len(),
{
    if keys.len() > 0 {
        lemma_descending_is_ascending_reversed(keys.drop_last(), entry);
        let rest = ascending(keys.drop_last(), entry);
        match entry(keys.last()) {
            Some(e) => {
                assert(rest.push(e).drop_last() =~= rest);
                assert(rest.push(e).last() == e);
                assert(reversed(rest.push(e)) == seq![e] + reversed(rest));
            },
            None => {},
        }
    }
}

} // verus!

/// Where `micros` falls against the window `[not_before, before)`: inside
/// (`None`), or the side it missed.
#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures out == outside_spec(not_before, before, micros)))]
pub fn outside(not_before: Option<i64>, before: Option<i64>, micros: i64) -> Option<Side> {
    let early = match not_before {
        Some(start) => micros < start,
        None => false,
    };
    if early {
        return Some(Side::Early);
    }
    let late = match before {
        Some(end) => end <= micros,
        None => false,
    };
    if late { Some(Side::Late) } else { None }
}

/// An append whose reads passed, judged at the clock reading `micros`: the
/// head is the last version and `last` the last stamp's micros.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == admitted(head, last, micros, not_before, before, write_free)
))]
pub fn admit(
    head: u64, last: Option<i64>, micros: i64, not_before: Option<i64>, before: Option<i64>, write_free: bool,
) -> Admitted {
    let regressed = match last {
        Some(last) => micros < last,
        None => false,
    };
    if regressed {
        return Admitted::Regressed;
    }
    match outside(not_before, before, micros) {
        Some(side) => return Admitted::Outside(side),
        None => {},
    }
    if write_free {
        return Admitted::Validated(head);
    }
    if head == u64::MAX {
        return Admitted::Exhausted;
    }
    Admitted::Committed { micros, version: head + 1 }
}

/// The newest of a key's versions at or before `at`: the last one, the
/// versions being in commit order.
#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures
    match out {
        Some(i) => i < versions@.len() && versions@[i as int].0 <= at
            && forall|j: int| i < j < versions@.len() ==> #[trigger] versions@[j].0 > at,
        None => forall|j: int| 0 <= j < versions@.len() ==> #[trigger] versions@[j].0 > at,
    },
))]
pub fn latest_at<T>(versions: &[(u64, T)], at: u64) -> Option<usize> {
    let mut i = versions.len();
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        i <= versions@.len(),
        forall|j: int| i <= j < versions@.len() ==> #[trigger] versions@[j].0 > at,
        decreases i,
    ))]
    while i > 0 {
        i -= 1;
        if versions[i].0 <= at {
            return Some(i);
        }
    }
    None
}

/// Whether a page resuming after `after` skips `key`: a read resumes strictly
/// past `after` in its direction, descending when `reverse`.
#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures
    out == if reverse { at_most(after@, key@) } else { at_most(key@, after@) },
))]
pub fn skips(key: &[u8], after: &[u8], reverse: bool) -> bool {
    if reverse { le_bytes(after, key) } else { le_bytes(key, after) }
}
