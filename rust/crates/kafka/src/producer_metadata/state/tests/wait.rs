use std::cell::Cell;

use super::*;

fn refresh() -> RefreshState {
    RefreshState::new(10, 10, 1000, &mut Observations)
}

#[test]
fn initial_clock_demand_precedes_negative_timeout_validation() {
    let calls = Cell::new(0);
    let timer = MetadataWait::new(0, -1, || {
        calls.set(calls.get() + 1);
        Ok(50)
    });
    assert!(matches!(timer, Err(WaitFailure::InvalidTimeout(-1))));
    assert_eq!(calls.get(), 1);

    let timer = MetadataWait::new(0, -1, || Err(Anomaly::unsupported("fixture/clock", "Clock unavailable")));
    assert!(matches!(timer, Err(WaitFailure::Clock(error)) if error.code == "fixture/clock"));
}

#[test]
fn fatal_then_close_precede_any_timer_sample_even_for_an_updated_version() {
    let mut refresh = refresh();
    refresh.bootstrap();
    refresh.close();
    let mut wait = MetadataWait::new(0, 0, || Ok(1)).unwrap();
    let failure = wait.step(
        &refresh,
        || Err(Anomaly::unsupported("fixture/bootstrap", "Bootstrap failure")),
        || panic!("fatal error precedes the timer"),
    );
    assert!(matches!(failure, Err(WaitFailure::Fatal(error)) if error.code == "fixture/bootstrap"));
    assert_eq!(wait.step(&refresh, || Ok(()), || panic!("close precedes the timer")).unwrap(), WaitStep::Closed);
}

#[test]
fn newer_version_beats_expiry_and_uses_signed_comparison() {
    let refresh = refresh();
    let mut wait = MetadataWait::new(-1, 0, || Ok(50)).unwrap();
    assert_eq!(wait.step(&refresh, || Ok(()), || panic!("newer version precedes the timer")).unwrap(), WaitStep::Updated);

    let mut wait = MetadataWait::new(i32::MAX, 0, || Ok(50)).unwrap();
    assert_eq!(wait.step(&refresh, || Ok(()), || Ok(50)).unwrap(), WaitStep::TimedOut(0));
}

#[test]
fn negative_or_backward_clock_samples_leave_cached_time_nondecreasing() {
    let refresh = refresh();
    let mut wait = MetadataWait::new(0, 10, || Ok(-3)).unwrap();
    assert_eq!(wait.step(&refresh, || Ok(()), || Ok(-5)).unwrap(), WaitStep::Wait(10));
    assert_eq!(wait.step(&refresh, || Ok(()), || Ok(4)).unwrap(), WaitStep::Wait(6));
    assert_eq!(wait.step(&refresh, || Ok(()), || Ok(1)).unwrap(), WaitStep::Wait(6));
    assert_eq!(wait.step(&refresh, || Ok(()), || Ok(10)).unwrap(), WaitStep::TimedOut(10));
}

#[test]
fn deadline_saturates_while_timeout_error_preserves_the_original_duration() {
    let refresh = refresh();
    let mut wait = MetadataWait::new(0, 30, || Ok(i64::MAX - 12)).unwrap();
    assert_eq!(wait.step(&refresh, || Ok(()), || Ok(i64::MAX - 6)).unwrap(), WaitStep::Wait(6));
    let failure = wait.step(&refresh, || Ok(()), || Err(Anomaly::unsupported("fixture/clock", "Clock unavailable")));
    assert!(matches!(failure, Err(WaitFailure::Clock(error)) if error.code == "fixture/clock"));
    assert_eq!(wait.step(&refresh, || Ok(()), || Ok(i64::MAX - 11)).unwrap(), WaitStep::Wait(6));
    assert_eq!(wait.step(&refresh, || Ok(()), || Ok(i64::MAX)).unwrap(), WaitStep::TimedOut(30));
}
