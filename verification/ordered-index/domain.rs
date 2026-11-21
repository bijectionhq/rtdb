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
                && (point as int - i64::MIN as int) <
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

proof fn alignment_identifies_the_bucket(start: u64, point: u128, level: u32)
    requires
        point <= u64::MAX as u128,
        level <= 64,
        start as u128 & sub(1_u128 << level, 1) == 0,
    ensures
        (start as u128 <= point && point < start as int + (1_u128 << level) as int)
            <==> start as u128 == point >> level << level,
{
    let width = 1_u128 << level;
    assert(width <= 18_446_744_073_709_551_616_u128) by (bit_vector)
        requires width == 1_u128 << level, level <= 64;
    assert(start as int + width as int <= u128::MAX as int);
    let end = (start as u128 + width) as u128;
    assert(end as int == start as int + width as int);
    assert((start as u128 <= point && point < end)
        <==> start as u128 == point >> level << level) by (bit_vector)
        requires
            point <= u64::MAX as u128,
            level <= 64,
            end == add(start as u128, 1_u128 << level),
            start as u128 & sub(1_u128 << level, 1) == 0;
}

fn main() {}

}
