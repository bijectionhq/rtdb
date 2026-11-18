#[path = "refresh/jitter.rs"]
mod jitter;
pub use jitter::{JavaThreadLocalRandom, JavaThreadLocalSeeds};

/// Caller-owned observations of the pinned JVM's math and random operations.
/// Implementations must preserve Java Math results and nextDouble's origin-inclusive, bound-exclusive range.
pub trait JavaBackoffObservations {
    /// Observes one Math.log invocation with its exact input.
    fn log(&mut self, value: f64) -> f64;
    /// Observes one Math.pow invocation with its exact inputs.
    fn pow(&mut self, base: f64, exponent: f64) -> f64;
    /// Observes one ThreadLocalRandom.nextDouble invocation with its exact range.
    fn next_double(&mut self, origin: f64, bound: f64) -> f64;
}

#[derive(Debug)]
struct RefreshBackoff { initial_ms: i64, maximum_ms: i64, exponent_maximum: f64 }

impl RefreshBackoff {
    fn new(initial_ms: i64, maximum_ms: i64, observations: &mut impl JavaBackoffObservations) -> Self {
        let exponent_maximum = if maximum_ms > initial_ms {
            observations.log(maximum_ms as f64 / initial_ms.max(1) as f64) / observations.log(2.0)
        } else { 0.0 };
        Self { initial_ms: initial_ms.min(maximum_ms), maximum_ms, exponent_maximum }
    }

    fn delay(&self, attempts: i64, observations: &mut impl JavaBackoffObservations) -> i64 {
        if self.exponent_maximum == 0.0 { return self.initial_ms; }
        let exponent = java_min(attempts as f64, self.exponent_maximum);
        let term = self.initial_ms as f64 * observations.pow(2.0, exponent);
        let factor = observations.next_double(1.0 - 0.2, 1.0 + 0.2);
        ((factor * term) as i64).min(self.maximum_ms)
    }
}

fn java_min(left: f64, right: f64) -> f64 {
    if left.is_nan() { left }
    else if right.is_nan() { right }
    else if left == 0.0 && right == 0.0 { f64::from_bits(left.to_bits() | right.to_bits()) }
    else { left.min(right) }
}

/// The producer request's topic scope, fixed when its builder is created.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestKind { Full, NewTopics }

/// A captured request version and scope carried unchanged to its metadata response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestScope { version: i32, kind: RequestKind }

impl RequestScope {
    /// Returns the signed request version, including wraparound.
    pub fn version(self) -> i32 { self.version }
    /// Returns which producer topic collection supplies the request.
    pub fn kind(self) -> RequestKind { self.kind }
}

/// A metadata response attempted after close, before any refresh-state mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClosedMetadata;

/// Caller-driven Java Metadata refresh state; all times and durations are signed milliseconds.
/// The producer owns serialization, observations, snapshot construction and waiter notification.
#[derive(Debug)]
pub struct RefreshState {
    backoff: RefreshBackoff,
    expire_ms: i64,
    update_version: i32,
    request_version: i32,
    last_refresh_ms: i64,
    last_successful_ms: i64,
    attempts: i64,
    equivalent_responses: i64,
    full_requested: bool,
    partial_requested: bool,
    closed: bool,
}

impl RefreshState {
    /// Constructs the schedule from nonnegative, Kafka-validated configuration durations.
    /// Math observations occur only when maximum_backoff_ms exceeds initial_backoff_ms.
    pub fn new(initial_backoff_ms: i64, maximum_backoff_ms: i64, expire_ms: i64, observations: &mut impl JavaBackoffObservations) -> Self {
        Self {
            backoff: RefreshBackoff::new(initial_backoff_ms, maximum_backoff_ms, observations),
            expire_ms, update_version: 0, request_version: 0,
            last_refresh_ms: 0, last_successful_ms: 0, attempts: 0,
            equivalent_responses: 0, full_requested: false, partial_requested: false, closed: false,
        }
    }

    /// Marks bootstrap before the producer constructs its bootstrap snapshot.
    pub fn bootstrap(&mut self) {
        self.full_requested = true;
        self.update_version = self.update_version.wrapping_add(1);
    }

    /// Requests all active topics and returns the current signed update version.
    pub fn request_full(&mut self, reset_equivalent_backoff: bool) -> i32 {
        self.full_requested = true;
        if reset_equivalent_backoff { self.equivalent_responses = 0; }
        self.update_version
    }

    /// Requests newly added topics, resetting the last-refresh time and equivalent-response backoff.
    /// Returns the current update version after incrementing the request version.
    pub fn request_new_topics(&mut self) -> i32 {
        self.last_refresh_ms = 0;
        self.partial_requested = true;
        self.equivalent_responses = 0;
        self.request_version = self.request_version.wrapping_add(1);
        self.update_version
    }

    /// Reports either outstanding full or partial refresh demand.
    pub fn update_requested(&self) -> bool { self.full_requested || self.partial_requested }

    /// Captures Full when a full update is requested or the last successful refresh has expired; otherwise captures NewTopics.
    /// Expiry uses the wrapping Java long sum of the last-successful timestamp and expiry duration.
    pub fn new_request(&self, now_ms: i64) -> RequestScope {
        let kind = if !self.full_requested && self.last_successful_ms.wrapping_add(self.expire_ms) > now_ms {
            RequestKind::NewTopics
        } else { RequestKind::Full };
        RequestScope { version: self.request_version, kind }
    }

    /// Applies Metadata.update's mutations before the caller constructs or merges its snapshot.
    /// A later snapshot failure leaves these mutations in place; closed state fails before mutation.
    pub fn begin_response(&mut self, scope: RequestScope, now_ms: i64) -> Result<(), ClosedMetadata> {
        if self.closed { return Err(ClosedMetadata); }
        self.partial_requested = scope.version < self.request_version;
        self.last_refresh_ms = now_ms;
        self.attempts = 0;
        self.update_version = self.update_version.wrapping_add(1);
        if scope.kind == RequestKind::Full {
            self.full_requested = false;
            self.last_successful_ms = now_ms;
        }
        self.equivalent_responses = self.equivalent_responses.wrapping_add(1);
        Ok(())
    }

    /// Resets equivalent-response backoff after recording a partition's first reliable epoch, replacing it for a changed non-null topic ID, or advancing it.
    /// Also applies after removing epoch information when it is absent or unreliable.
    pub fn metadata_progress(&mut self) { self.equivalent_responses = 0; }

    /// Records one failed fetch attempt, retaining the last successful full-refresh time.
    pub fn failed_response(&mut self, now_ms: i64) {
        self.last_refresh_ms = now_ms;
        self.attempts = self.attempts.wrapping_add(1);
        self.equivalent_responses = 0;
    }

    /// Returns the retry delay with attempts observed before expiry reset and equivalent-response backoff.
    /// Arithmetic wraps as Java long; observation calls occur even when a later maximum dominates their result.
    pub fn time_to_allow_update(&mut self, now_ms: i64, observations: &mut impl JavaBackoffObservations) -> i64 {
        let attempts = if self.attempts > 0 { self.attempts - 1 } else { 0 };
        let attempt_delay = self.last_refresh_ms.wrapping_add(self.backoff.delay(attempts, observations)).wrapping_sub(now_ms).max(0);
        if self.expiry_delay(now_ms) == 0 { self.equivalent_responses = 0; }
        let equivalent_backoff = if self.equivalent_responses > 0 {
            self.backoff.delay(self.equivalent_responses - 1, observations)
        } else { 0 };
        let equivalent_delay = self.last_refresh_ms.wrapping_add(equivalent_backoff).wrapping_sub(now_ms).max(0);
        attempt_delay.max(equivalent_delay)
    }

    /// Returns the larger of metadata expiry and retry delay; explicit demand makes expiry immediate.
    pub fn time_to_next_update(&mut self, now_ms: i64, observations: &mut impl JavaBackoffObservations) -> i64 {
        let expiry = if self.update_requested() { 0 } else { self.expiry_delay(now_ms) };
        expiry.max(self.time_to_allow_update(now_ms, observations))
    }

    /// Returns the signed update version observed by waiting producer callers.
    pub fn update_version(&self) -> i32 { self.update_version }
    /// Returns the last successful full-refresh timestamp, in milliseconds.
    pub fn last_successful_ms(&self) -> i64 { self.last_successful_ms }
    /// Prevents subsequent begin_response calls; waiter notification belongs to the producer owner.
    pub fn close(&mut self) { self.closed = true; }
    /// Reports whether metadata response application has been closed.
    pub fn is_closed(&self) -> bool { self.closed }

    fn expiry_delay(&self, now_ms: i64) -> i64 {
        self.last_successful_ms.wrapping_add(self.expire_ms).wrapping_sub(now_ms).max(0)
    }
}

/// One existing connection's caller-driven throttle deadline, initially zero milliseconds.
#[derive(Debug)]
pub struct ConnectionThrottle { until_ms: i64 }

impl ConnectionThrottle {
    /// Starts with ClusterConnectionStates' zero throttle deadline.
    pub const fn new() -> Self { Self { until_ms: 0 } }

    /// Applies a Metadata response's positive throttle from protocol v6 onward.
    /// The callback must increment this connection's in-flight request throttle times before the deadline changes.
    pub fn metadata_received(&mut self, version: i16, throttle_ms: i32, now_ms: i64, mut increment_inflight: impl FnMut(i64)) {
        if throttle_ms > 0 && version >= 6 {
            increment_inflight(i64::from(throttle_ms));
            self.until_ms = self.until_ms.max(now_ms.wrapping_add(i64::from(throttle_ms)));
        }
    }

    /// Returns deadline minus now when the deadline is later, using Java long wrapping arithmetic.
    pub fn remaining_ms(&self, now_ms: i64) -> i64 {
        if self.until_ms > now_ms { self.until_ms.wrapping_sub(now_ms) } else { 0 }
    }

    /// Returns whether the throttle deadline permits sending; connection readiness is checked by the caller.
    pub fn allows_send(&self, now_ms: i64) -> bool { self.until_ms <= now_ms }

    /// Returns positive throttle delay for a connected node, otherwise the supplied connection delay.
    pub fn poll_delay_ms(&self, connected: bool, connection_delay_ms: i64, now_ms: i64) -> i64 {
        let throttle = self.remaining_ms(now_ms);
        if connected && throttle > 0 { throttle } else { connection_delay_ms }
    }
}

#[cfg(test)]
#[path = "refresh/tests.rs"]
mod tests;
