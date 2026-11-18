use super::*;

#[derive(Debug, PartialEq)]
enum Observation { Log(f64), Power(f64), Jitter(f64, f64) }

#[derive(Default)]
struct History(Vec<Observation>);

impl JavaBackoffObservations for History {
    fn log(&mut self, value: f64) -> f64 {
        self.0.push(Observation::Log(value));
        let exponent = [2.0, 4.0, 8.0].iter().position(|power| *power == value).unwrap() + 1;
        std::f64::consts::LN_2 * exponent as f64
    }

    fn pow(&mut self, base: f64, exponent: f64) -> f64 {
        assert_eq!(base, 2.0);
        self.0.push(Observation::Power(exponent));
        2.0f64.powi(exponent as i32)
    }

    fn next_double(&mut self, origin: f64, bound: f64) -> f64 {
        self.0.push(Observation::Jitter(origin, bound));
        1.0
    }
}

fn policy(history: &mut History) -> ConnectionBackoffs {
    ConnectionBackoffs::new(ConnectionSettings {
        reconnect_ms: 10, reconnect_max_ms: 80, setup_ms: 20, setup_max_ms: 80,
    }, history)
}

fn draws(exponents: &[f64]) -> Vec<Observation> {
    exponents.iter().flat_map(|&exponent| [
        Observation::Power(exponent), Observation::Jitter(0.8, 1.2),
    ]).collect()
}

#[test]
fn new_host_draws_reconnect_then_setup_and_same_host_only_updates_the_attempt() {
    let mut history = History::default();
    let policy = policy(&mut history);
    assert_eq!(history.0, [
        Observation::Log(8.0), Observation::Log(2.0), Observation::Log(4.0), Observation::Log(2.0),
    ]);
    history.0.clear();
    let mut state: ConnectionStatus<()> = policy.connecting(i64::MAX - 5, &mut history);
    assert_eq!(history.0, draws(&[0.0, 0.0]));
    assert_eq!(state.connection_delay_ms(i64::MIN + 14), 20);
    assert_eq!(state.setup_timed_out(i64::MIN + 14), Some(false));
    assert_eq!(state.setup_timed_out(i64::MIN + 15), Some(true));
    assert!(!state.can_connect(i64::MIN + 15));

    state.connect_same_host(40);
    assert_eq!(history.0, draws(&[0.0, 0.0]));
    assert_eq!(state.last_connect_attempt_ms(), 40);
    assert_eq!(state.phase(), ConnectionPhase::Connecting);
    assert_eq!(state.setup_timeout_ms(), 20);
}

#[test]
fn connecting_failure_counts_reconnect_and_setup_differently() {
    let mut history = History::default();
    let policy = policy(&mut history);
    let mut state: ConnectionStatus<()> = policy.connecting(2, &mut history);
    history.0.clear();
    assert_eq!(state.disconnect(10, &policy, &mut history), DisconnectAddresses::Keep);
    assert_eq!(history.0, draws(&[0.0, 1.0]));
    assert_eq!(state.setup_timeout_ms(), 40);
    assert!(!state.can_connect(19));
    assert!(state.can_connect(20));
    assert_eq!(state.connection_delay_ms(16), 4);

    state.connect_same_host(21);
    history.0.clear();
    assert_eq!(state.disconnect(30, &policy, &mut history), DisconnectAddresses::Keep);
    assert_eq!(history.0, draws(&[1.0, 2.0]));
    assert_eq!(state.setup_timeout_ms(), 80);
    assert_eq!(state.connection_delay_ms(32), 18);
}

#[test]
fn auth_failure_survives_same_host_attempt_and_connected_disconnect_until_ready() {
    let mut history = History::default();
    let policy = policy(&mut history);
    let mut state = policy.connecting(2, &mut history);
    history.0.clear();
    state.authentication_failed("credential expired", 10, &policy, &mut history);
    assert_eq!(history.0, draws(&[0.0]));
    assert_eq!(state.phase(), ConnectionPhase::AuthenticationFailed);
    assert_eq!(state.authentication_error(), Some(&"credential expired"));
    assert_eq!(state.last_connect_attempt_ms(), 10);
    assert!(state.can_connect(20));

    state.connect_same_host(21);
    history.0.clear();
    state.checking_api_versions(&policy, &mut history);
    assert_eq!(history.0, draws(&[0.0]));
    assert_eq!(state.authentication_error(), Some(&"credential expired"));
    assert!(state.preparing_connection());
    assert_eq!(state.setup_timed_out(200), None);
    assert_eq!(state.connection_delay_ms(22), i64::MAX);
    history.0.clear();
    assert_eq!(state.disconnect(23, &policy, &mut history), DisconnectAddresses::Clear);
    assert_eq!(history.0, draws(&[1.0, 0.0]));
    assert_eq!(state.authentication_error(), Some(&"credential expired"));

    state.connect_same_host(50);
    history.0.clear();
    state.ready(&policy, &mut history);
    assert_eq!(history.0, draws(&[0.0, 0.0]));
    assert_eq!(state.phase(), ConnectionPhase::Ready);
    assert_eq!(state.authentication_error(), None);
    assert!(!state.preparing_connection());
}

#[test]
fn throttle_survives_same_address_transitions_and_controls_only_connected_poll_delay() {
    let mut history = History::default();
    let policy = policy(&mut history);
    let mut state: ConnectionStatus<()> = policy.connecting(80, &mut history);
    state.checking_api_versions(&policy, &mut history);
    state.metadata_received(5, 50, 100, |_| panic!("pre-version-6 metadata does not throttle"));
    assert_eq!(state.poll_delay_ms(101), i64::MAX);
    let mut increments = Vec::new();
    state.metadata_received(6, 50, 100, |throttle| increments.push(throttle));
    assert_eq!(increments, [50]);
    assert_eq!(state.poll_delay_ms(101), 49);
    assert!(!state.connection_ready(150));
    state.ready(&policy, &mut history);
    assert!(!state.connection_ready(149));
    assert!(state.connection_ready(150));

    assert_eq!(state.disconnect(110, &policy, &mut history), DisconnectAddresses::Clear);
    assert_eq!(state.poll_delay_ms(112), 8);
    state.connect_same_host(120);
    assert_eq!(state.poll_delay_ms(121), 20);
    state.ready(&policy, &mut history);
    assert_eq!(state.poll_delay_ms(121), 29);
    let replacement: ConnectionStatus<()> = policy.connecting(120, &mut history);
    assert_eq!(replacement.poll_delay_ms(121), 20);
}
