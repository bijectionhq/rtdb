use super::*;
use std::collections::VecDeque;

#[derive(Debug, PartialEq)]
enum Observation { Log(f64), Pow(f64, f64), Random(f64, f64) }

#[derive(Default)]
struct Observations { calls: Vec<Observation>, factors: VecDeque<f64> }
impl JavaBackoffObservations for Observations {
    fn log(&mut self, value: f64) -> f64 {
        self.calls.push(Observation::Log(value));
        match value { 2.0 => 0.6931471805599453, 4.0 => 1.3862943611198906, _ => panic!("unexpected log {value}") }
    }
    fn pow(&mut self, base: f64, exponent: f64) -> f64 {
        self.calls.push(Observation::Pow(base, exponent));
        assert_eq!(base, 2.0);
        match exponent { 0.0 => 1.0, 1.0 => 2.0, 2.0 => 4.0, _ => panic!("unexpected exponent {exponent}") }
    }
    fn next_double(&mut self, origin: f64, bound: f64) -> f64 {
        self.calls.push(Observation::Random(origin, bound));
        self.factors.pop_front().expect("one supplied observation per draw")
    }
}

fn state() -> (RefreshState, Observations) {
    let mut observations = Observations::default();
    let state = RefreshState::new(10, 40, 1000, &mut observations);
    (state, observations)
}

#[test]
fn constructor_and_dominated_delay_preserve_math_and_random_observation_order() {
    let (mut state, mut observations) = state();
    assert_eq!(observations.calls, [Observation::Log(4.0), Observation::Log(2.0)]);
    observations.factors.push_back(1.0);
    assert_eq!(state.time_to_next_update(0, &mut observations), 1000);
    assert_eq!(observations.calls[2..], [Observation::Pow(2.0, 0.0), Observation::Random(0.8, 1.2)]);
    assert!(observations.factors.is_empty());
    let mut constant_observations = Observations::default();
    let mut constant = RefreshState::new(40, 10, 1000, &mut constant_observations);
    assert_eq!(constant.time_to_allow_update(0, &mut constant_observations), 10);
    assert!(constant_observations.calls.is_empty());
}

#[test]
fn full_and_partial_responses_preserve_request_scope_and_pre_snapshot_mutations() {
    let (mut state, _) = state();
    state.bootstrap();
    assert_eq!(state.update_version(), 1);
    let first = state.new_request(1);
    assert_eq!(first.kind(), RequestKind::Full);
    assert_eq!(state.request_new_topics(), 1);
    state.begin_response(first, 100).unwrap();
    assert_eq!(state.update_version(), 2);
    assert_eq!(state.last_successful_ms(), 100);
    assert!(state.update_requested());
    let partial = state.new_request(101);
    assert_eq!(partial, RequestScope {version: 1, kind: RequestKind::NewTopics});
    state.begin_response(partial, 102).unwrap();
    assert_eq!(state.last_successful_ms(), 100);
    assert_eq!(state.last_refresh_ms, 102);
    assert_eq!(state.update_version(), 3);
    assert!(!state.update_requested());
    assert_eq!(state.equivalent_responses, 2);
    state.close();
    assert!(state.is_closed());
    assert_eq!(state.begin_response(partial, 999), Err(ClosedMetadata));
    assert_eq!((state.update_version(), state.last_refresh_ms, state.equivalent_responses), (3, 102, 2));
}

#[test]
fn attempts_draw_precedes_equivalent_draw_and_expiry_cancels_only_the_second() {
    let (mut state, mut observations) = state();
    state.bootstrap();
    let full = state.new_request(0);
    state.begin_response(full, 100).unwrap();
    state.begin_response(full, 101).unwrap();
    observations.calls.clear();
    observations.factors.extend([0.8, 1.1]);
    assert_eq!(state.time_to_allow_update(101, &mut observations), 22);
    assert_eq!(observations.calls, [Observation::Pow(2.0, 0.0), Observation::Random(0.8, 1.2), Observation::Pow(2.0, 1.0), Observation::Random(0.8, 1.2)]);
    observations.calls.clear();
    observations.factors.push_back(1.0);
    assert_eq!(state.time_to_allow_update(1101, &mut observations), 0);
    assert_eq!(state.equivalent_responses, 0);
    assert_eq!(observations.calls, [Observation::Pow(2.0, 0.0), Observation::Random(0.8, 1.2)]);
}

#[test]
fn progress_new_topics_and_explicit_reset_have_distinct_backoff_effects() {
    let (mut state, _) = state();
    state.equivalent_responses = 3;
    assert_eq!(state.request_full(false), 0);
    assert_eq!(state.equivalent_responses, 3);
    assert_eq!(state.request_full(true), 0);
    assert_eq!(state.equivalent_responses, 0);
    state.equivalent_responses = 3;
    state.metadata_progress();
    assert_eq!(state.equivalent_responses, 0);
    state.last_refresh_ms = 99;
    state.equivalent_responses = 3;
    state.request_new_topics();
    assert_eq!((state.last_refresh_ms, state.equivalent_responses, state.request_version), (0, 0, 1));
    assert_eq!(state.new_request(1).kind(), RequestKind::Full);
}

#[test]
fn failed_fetches_increment_attempts_and_apply_jitter_before_cap_and_truncation() {
    let (mut state, mut observations) = state();
    state.failed_response(90);
    state.failed_response(100);
    observations.factors.push_back(0.99);
    assert_eq!(state.time_to_allow_update(100, &mut observations), 19);
    state.attempts = 20;
    observations.factors.push_back(1.19);
    assert_eq!(state.time_to_allow_update(100, &mut observations), 40);
    state.attempts = i64::MAX;
    state.failed_response(110);
    assert_eq!(state.attempts, i64::MIN);
    observations.factors.push_back(1.0);
    assert_eq!(state.time_to_allow_update(110, &mut observations), 10);
    assert_eq!(state.last_successful_ms(), 0);
}

#[test]
fn signed_request_versions_timestamps_and_equivalent_count_wrap_as_java_values() {
    let (mut state, _) = state();
    state.request_version = i32::MAX;
    state.update_version = i32::MAX;
    let prior = state.new_request(0);
    state.request_new_topics();
    state.equivalent_responses = i64::MAX;
    state.begin_response(prior, 1).unwrap();
    assert_eq!(state.request_version, i32::MIN);
    assert_eq!(state.update_version(), i32::MIN);
    assert_eq!(state.equivalent_responses, i64::MIN);
    assert!(!state.partial_requested);
    state.full_requested = false;
    state.last_successful_ms = i64::MAX - 1;
    assert_eq!(state.new_request(2).kind(), RequestKind::Full);
    assert_eq!(state.expiry_delay(2), 0);
    assert!(java_min(-0.0, 0.0).is_sign_negative());
    assert!(java_min(0.0, -0.0).is_sign_negative());
    assert!(java_min(1.0, f64::NAN).is_nan());
}

#[test]
fn metadata_throttle_gates_versions_and_never_regresses_connection_deadline() {
    let mut throttle = ConnectionThrottle::new();
    let mut increments = Vec::new();
    throttle.metadata_received(5, 50, 100, |value| increments.push(value));
    throttle.metadata_received(6, 0, 100, |value| increments.push(value));
    throttle.metadata_received(6, -1, 100, |value| increments.push(value));
    assert!(increments.is_empty());
    throttle.metadata_received(6, 10, 100, |value| increments.push(value));
    throttle.metadata_received(13, 5, 101, |value| increments.push(value));
    assert_eq!(increments, [10, 5]);
    assert_eq!(throttle.remaining_ms(105), 5);
    assert_eq!(throttle.poll_delay_ms(true, 70, 105), 5);
    assert_eq!(throttle.poll_delay_ms(false, 70, 105), 70);
    assert!(!throttle.allows_send(109));
    assert!(throttle.allows_send(110));
    assert_eq!(throttle.remaining_ms(110), 0);
    assert_eq!(throttle.poll_delay_ms(true, 70, 110), 70);
}

#[test]
fn throttle_long_overflow_preserves_inflight_increment_and_signed_deadline_comparison() {
    let mut throttle = ConnectionThrottle::new();
    let mut total = 0;
    throttle.metadata_received(6, 10, i64::MAX - 2, |value| total += value);
    assert_eq!(total, 10);
    assert_eq!(throttle.until_ms, 0);
    assert_eq!(throttle.remaining_ms(i64::MIN), i64::MIN);
    assert!(!throttle.allows_send(i64::MIN));
    assert_eq!(throttle.poll_delay_ms(true, 7, i64::MIN), 7);
}
