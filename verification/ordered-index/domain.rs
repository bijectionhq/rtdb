#![feature(proc_macro_hygiene)]

use vstd::prelude::*;

#[path = "../../rust/crates/node/src/ordered_index/domain.rs"]
mod production;

verus! {

proof fn timestamp_window(lower: i64, upper: i64, point: i64)
    requires
        lower <= upper,
        point < i64::MAX,
    ensures
        (lower <= point && point < upper) <==>
            (lower as int - i64::MIN as int <= point as int - i64::MIN as int
                && point as int - i64::MIN as int <
                   (if upper == i64::MAX {
                       u64::MAX as int + 1
                   } else {
                       upper as int - i64::MIN as int
                   })),
{
}

fn production_window(lower: i64, upper: i64, point: i64) -> (out: bool)
    requires lower <= upper, point < i64::MAX,
    ensures out == (lower <= point && point < upper),
{
    let value = production::offset(point);
    let start = production::offset(lower);
    let end = production::upper_bound(upper);
    proof! { timestamp_window(lower, upper, point); }
    start <= value && value < end
}

fn main() {}

}
