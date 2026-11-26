//! V9′ (T7): RTDB's `rtdb.store.v1.Append` decides its clock sample through
//! the vendored bytes of Bijection's `fact_store::persistence::judge`, so
//! Bijection's V9 (Verus area `owned_multi_place`) covers RTDB's decision
//! bytes too.
//!
//! Included by `#[path]`, as shipped:
//!   * `rust/vendor/bijection-store-judge/src/judge.rs`, byte-identical to
//!     Bijection's `judge.rs` (SHA-256 pinned in both repositories);
//!   * `rust/crates/tx/src/transact/store_clock.rs`, the one function
//!     `TransactState::resolve_store` judges the sample with.
//!
//! The law: `judge_store_clock` equals Lean's `stampedOutcome` past its
//! conflict check (the vendored `admitted` spec). In particular a sample
//! equal to the last stamp's micros commits at `⟨sample, head + 1⟩`: no tie
//! refusal, no raise, no wait.
//!
//! Outside: that `resolve_store` makes no other comparison of the sample with
//! the head's time (source inspection: its only other use of the sample is
//! the record's system time, which is the sample itself), and the
//! `store_read::order::le_bytes` shim, whose contract is stated here as the
//! external body Bijection proves (`store_read::order`).
#![feature(proc_macro_hygiene)]
use vstd::prelude::*;

extern crate self as store_read;
extern crate self as store_judge;

/// Bijection's `store_read::order` as `judge.rs` uses it: `[u8]`'s `<=`.
pub mod order {
    use vstd::prelude::*;

    verus! {

    /// `a` sorts at or before `b` (Bijection proves `le_bytes` against its
    /// own definition; here it is the shim's stated contract).
    pub uninterp spec fn at_most(a: Seq<u8>, b: Seq<u8>) -> bool;

    #[verifier::external_body]
    pub fn le_bytes(a: &[u8], b: &[u8]) -> (out: bool)
        ensures out == at_most(a@, b@),
    {
        a <= b
    }

    } // verus!
}

#[path = "../../../rust/vendor/bijection-store-judge/src/judge.rs"]
mod judge;
pub use judge::*;

#[path = "../../../rust/crates/tx/src/transact/store_clock.rs"]
mod store_clock;

verus! {

/// A tie at the last stamp's micros commits, at the next version.
proof fn tie_commits(head: u64, now: i64)
    requires head < u64::MAX,
    ensures judge::admitted(head, Some(now), now, None, None, false)
        == (judge::Admitted::Committed { micros: now, version: (head + 1) as u64 }),
{
}

/// Whether an append is outside depends on its sample and window alone:
/// two stores at one sample whose last stamps both lie at or below it give
/// the same outcome kind and the same micros.
proof fn hidden_last_stamp_is_irrelevant(head: u64, last1: i64, last2: i64, now: i64, nb: Option<i64>, b: Option<i64>, wf: bool)
    requires last1 <= now, last2 <= now,
    ensures judge::admitted(head, Some(last1), now, nb, b, wf) == judge::admitted(head, Some(last2), now, nb, b, wf),
{
}

} // verus!

fn main() {}
