use std::cell::Cell;

use super::*;

#[test]
fn class_clocks_and_platform_threads_use_distinct_source_seed_advances() {
    let order = Cell::new(0);
    let seeds = JavaThreadLocalSeeds::from_class_clocks(
        || { assert_eq!(order.replace(1), 0); Ok(1234) },
        || { assert_eq!(order.replace(2), 1); Ok(-555) },
    ).unwrap();
    assert_eq!(order.get(), 2);
    let mut first = seeds.initialize_platform_thread(19);
    let mut second = seeds.initialize_platform_thread(20);
    assert_eq!(first.next_double(0.8, 1.2).unwrap().to_bits(), 0x3feb_c4f2_46cb_09dd);
    assert_eq!(second.next_double(0.8, 1.2).unwrap().to_bits(), 0x3ff0_48ce_ccb3_7e73);
    assert_eq!(first.next_double(0.8, 1.2).unwrap().to_bits(), 0x3fef_e975_ad51_f7f7);
    assert_eq!(second.next_double(0.8, 1.2).unwrap().to_bits(), 0x3ff2_b71a_af6c_e5c8);
}

#[test]
fn unavailable_class_wall_sample_precedes_monotonic_sample() {
    let result = JavaThreadLocalSeeds::from_class_clocks(
        || Err(Anomaly::unsupported("fixture/wall", "Missing wall sample")),
        || panic!("the first clock failure prevents the second sample"),
    );
    assert!(matches!(result, Err(cause) if cause.code == "fixture/wall"));
}

#[test]
fn invalid_ranges_leave_the_original_thread_sequence_unchanged() {
    let mut random = JavaThreadLocalRandom::from_initialized_thread(42, i64::MAX);
    for (origin, bound) in [(f64::NAN, 1.0), (0.0, f64::INFINITY), (0.0, -0.0), (1.0, 0.0)] {
        assert_eq!(random.next_double(origin, bound).unwrap_err().code, "rtdb/kafka-java-thread-random-range");
    }
    assert_eq!(random.next_double(0.0, 1.0).unwrap().to_bits(), 0x3fe5_b1b3_6226_a071);
    assert_eq!(random.next_double(0.0, 1.0).unwrap().to_bits(), 0x3feb_0b8d_8285_1790);
}

#[test]
fn source_scaling_handles_infinite_width_and_excludes_the_rounded_boundary() {
    let mut wide = JavaThreadLocalRandom::from_initialized_thread(42, i64::MAX);
    assert_eq!(wide.next_double(-f64::MAX, f64::MAX).unwrap().to_bits(), 0x7fd6_c6cd_889a_81c2);

    let mut adjacent = JavaThreadLocalRandom::from_initialized_thread(42, i64::MAX);
    let bound = f64::from_bits(0x3ff0_0000_0000_0001);
    assert_eq!(adjacent.next_double(1.0, bound).unwrap().to_bits(), 1.0f64.to_bits());
    assert_eq!(adjacent.next_double(0.0, 1.0).unwrap().to_bits(), 0x3feb_0b8d_8285_1790);
}
