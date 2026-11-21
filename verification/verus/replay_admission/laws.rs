use crate::production::{cutoff_micros, is_setup_frame, may_send_simple_replay};
use vstd::prelude::*;

#[verus_spec(out => ensures !out,)]
fn execute_is_not_a_setup_frame() -> bool {
    is_setup_frame(b'E')
}

#[verus_spec(out =>
    ensures !out.0, !out.1, out.2.is_none(),
)]
fn a_permit_requires_support_and_a_finite_unexpired_cutoff() -> (bool, bool, Option<i64>) {
    (may_send_simple_replay(false, true), may_send_simple_replay(true, false), cutoff_micros(i64::MAX - 1, 1))
}
