#[path = "../../../rust/crates/directory/src/dynamo/audience_seat.rs"]
mod production;

use production::{SeatCondition, SeatPredecessor, release_condition};
use vstd::prelude::*;

verus! {
    spec fn matches_row(condition: SeatCondition, row: Option<u128>) -> bool {
        match condition {
            SeatCondition::Missing => row is None,
            SeatCondition::Exact(revision) => row == Some(revision),
            SeatCondition::MissingOr(claimed) => row is None || row == Some(claimed),
            SeatCondition::Either(prior, claimed) => row == Some(prior) || row == Some(claimed),
        }
    }

    fn stale_release_excludes_replacement(
        predecessor: SeatPredecessor, original: u128, replacement: u128,
    ) -> (condition: SeatCondition)
        requires
            replacement != original,
            match predecessor {
                SeatPredecessor::Missing => true,
                SeatPredecessor::Exact(prior) => replacement != prior,
            },
        ensures !matches_row(condition, Some(replacement)),
    {
        release_condition(&predecessor, original)
    }
}

fn main() {}
