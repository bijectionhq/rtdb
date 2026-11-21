#![allow(unexpected_cfgs)]
#![cfg(kani)]

use arrow_buffer::{ArrowNativeType, RunEndBuffer, ScalarBuffer};

fn check_two_run_slice<E: ArrowNativeType>(split: E, end: E) {
    kani::assume(E::usize_as(0) < split && split < end);
    let [start, length, row] = kani::any::<[usize; 3]>();
    kani::assume(start <= end.as_usize());
    kani::assume(length <= end.as_usize() - start && row < length);

    let buffer = RunEndBuffer::new(ScalarBuffer::from(vec![split, end]), start, length);
    let expected = usize::from(start + row >= split.as_usize());
    assert_eq!(buffer.get_physical_index(row), expected);
}

#[kani::proof]
#[kani::unwind(8)]
fn arrow_i16_run_indices_respect_slices_and_exclusive_ends() {
    check_two_run_slice(kani::any::<i16>(), kani::any::<i16>());
}

#[kani::proof]
#[kani::unwind(8)]
fn arrow_i32_run_indices_respect_slices_and_exclusive_ends() {
    check_two_run_slice(kani::any::<i32>(), kani::any::<i32>());
}

#[kani::proof]
#[kani::unwind(8)]
fn arrow_i64_run_indices_respect_slices_and_exclusive_ends() {
    check_two_run_slice(kani::any::<i64>(), kani::any::<i64>());
}
