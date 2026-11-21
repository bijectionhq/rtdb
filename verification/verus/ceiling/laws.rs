use crate::production::{cell_window, covers, meets, overwrite, valid_ceiling};
use crate::Timestamp;
use vstd::prelude::*;

// LAW RTDB-TIME-08.
#[verus_spec(
    requires valid_ceiling(old(bounds)@, old(ceilings)@), lower.0 <= upper.0,
    ensures forall|point: i64, cell: int|
        lower.0 <= point < upper.0 && #[trigger] covers(final(bounds)@, cell, point)
        ==> final(ceilings)@[cell] == system_from,
)]
fn applying_a_write_sets_the_ceiling_at_every_covered_point(
    bounds: &mut Vec<Timestamp>, ceilings: &mut Vec<Timestamp>,
    lower: Timestamp, upper: Timestamp, system_from: Timestamp,
) {
    overwrite(bounds, ceilings, lower, upper, system_from);
}

// LAW RTDB-TIME-09.
#[verus_spec(out =>
    requires valid_ceiling(bounds@, ceilings@), lower.0 < upper.0,
    ensures forall|point: i64, cell: int|
        lower.0 <= point < upper.0 && #[trigger] covers(bounds@, cell, point)
        ==> out.0 <= cell <= out.1,
)]
fn clipping_never_skips_a_queried_point(
    bounds: &[Timestamp], ceilings: &[Timestamp], lower: Timestamp, upper: Timestamp,
) -> (usize, usize) {
    let out = cell_window(bounds, ceilings, lower, upper);
    proof! {
        assert forall|point: i64, cell: int|
            lower.0 <= point < upper.0 && #[trigger] covers(bounds@, cell, point)
            implies out.0 <= cell <= out.1 by {
            assert(meets(bounds@, cell, lower, upper));
        }
    }
    out
}
