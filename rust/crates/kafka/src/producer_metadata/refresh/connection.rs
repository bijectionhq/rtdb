use super::{ConnectionThrottle, JavaBackoffObservations, SourceBackoff};

/// Nonnegative Java reconnect and socket setup settings in signed milliseconds.
pub struct ConnectionSettings {
    /// The source reconnect.backoff.ms base.
    pub reconnect_ms: i64,
    /// The source reconnect.backoff.max.ms limit.
    pub reconnect_max_ms: i64,
    /// The source socket.connection.setup.timeout.ms base.
    pub setup_ms: i64,
    /// The source socket.connection.setup.timeout.max.ms limit.
    pub setup_max_ms: i64,
}

/// The shared reconnect and connection setup schedules of one source NetworkClient.
pub struct ConnectionBackoffs {
    reconnect: SourceBackoff,
    setup: SourceBackoff,
}

impl ConnectionBackoffs {
    /// Builds reconnect before setup and demands their Math observations in source order.
    pub fn new(settings: ConnectionSettings, observations: &mut impl JavaBackoffObservations) -> Self {
        Self {
            reconnect: SourceBackoff::new(settings.reconnect_ms, settings.reconnect_max_ms, observations),
            setup: SourceBackoff::new(settings.setup_ms, settings.setup_max_ms, observations),
        }
    }

    /// Constructs a connecting state for an absent ID or changed source hostname.
    /// Draws the initial reconnect delay before the initial setup timeout.
    /// The caller installs it in the inventory and owns the initially empty address state.
    pub fn connecting<E>(&self, now_ms: i64, observations: &mut impl JavaBackoffObservations) -> ConnectionStatus<E> {
        ConnectionStatus {
            phase: ConnectionPhase::Connecting,
            last_attempt_ms: now_ms,
            failed_attempts: 0,
            failed_setup_attempts: 0,
            reconnect_ms: self.reconnect.delay(0, observations),
            setup_ms: self.setup.delay(0, observations),
            throttle: ConnectionThrottle::new(),
            authentication_error: None,
        }
    }
}

/// The observed source phase of a single connection ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionPhase {
    Disconnected,
    Connecting,
    CheckingApiVersions,
    Ready,
    AuthenticationFailed,
}

impl ConnectionPhase {
    fn disconnected(self) -> bool {
        matches!(self, Self::Disconnected | Self::AuthenticationFailed)
    }

    fn connected(self) -> bool { matches!(self, Self::CheckingApiVersions | Self::Ready) }
}

/// The DNS address obligation after a source disconnection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DisconnectAddresses { Keep, Clear }

/// The source timing and authentication state retained for one existing connection ID.
/// The caller serializes access, owns its address list and tracks source connecting-set membership.
pub struct ConnectionStatus<E> {
    phase: ConnectionPhase,
    last_attempt_ms: i64,
    failed_attempts: i64,
    failed_setup_attempts: i64,
    reconnect_ms: i64,
    setup_ms: i64,
    throttle: ConnectionThrottle,
    authentication_error: Option<E>,
}

impl<E> ConnectionStatus<E> {
    /// Starts another attempt for the same hostname using the existing timing state.
    /// The caller advances the existing address selection without rebuilding this entry.
    pub fn connect_same_host(&mut self, now_ms: i64) {
        self.last_attempt_ms = now_ms;
        self.phase = ConnectionPhase::Connecting;
    }

    /// Records a disconnection and returns its address-list disposition.
    /// Reconnect draws from its prior failure count before any setup draw.
    /// A connecting failure advances the setup count before drawing, and other phases reset it and draw from zero.
    pub fn disconnect(
        &mut self, now_ms: i64, backoffs: &ConnectionBackoffs, observations: &mut impl JavaBackoffObservations,
    ) -> DisconnectAddresses {
        let previous = self.phase;
        self.last_attempt_ms = now_ms;
        self.update_reconnect(backoffs, observations);
        if previous == ConnectionPhase::Connecting {
            self.failed_setup_attempts = self.failed_setup_attempts.wrapping_add(1);
            self.setup_ms = backoffs.setup.delay(self.failed_setup_attempts, observations);
        } else {
            self.reset_setup(backoffs, observations);
        }
        self.phase = ConnectionPhase::Disconnected;
        if previous.connected() { DisconnectAddresses::Clear } else { DisconnectAddresses::Keep }
    }

    /// Enters the established phase awaiting API version negotiation.
    /// Resets setup failures before the enclosing caller removes this ID from the connecting set.
    pub fn checking_api_versions(&mut self, backoffs: &ConnectionBackoffs, observations: &mut impl JavaBackoffObservations) {
        self.phase = ConnectionPhase::CheckingApiVersions;
        self.reset_setup(backoffs, observations);
    }

    /// Marks a connection ready, clears its retained authentication failure and resets both policies.
    /// Draws reconnect before setup.
    /// The caller removes this ID from the connecting set after both resets.
    pub fn ready(&mut self, backoffs: &ConnectionBackoffs, observations: &mut impl JavaBackoffObservations) {
        self.phase = ConnectionPhase::Ready;
        self.authentication_error = None;
        self.failed_attempts = 0;
        self.reconnect_ms = backoffs.reconnect.delay(0, observations);
        self.reset_setup(backoffs, observations);
    }

    /// Retains the source authentication error and records a disconnected phase with reconnect backoff.
    /// The caller supplies the original admitted error owner and handles connecting-set membership.
    /// NetworkClient's disconnect path first applies [`Self::disconnect`], removes version state, then calls this transition.
    pub fn authentication_failed(
        &mut self, error: E, now_ms: i64, backoffs: &ConnectionBackoffs, observations: &mut impl JavaBackoffObservations,
    ) {
        self.authentication_error = Some(error);
        self.phase = ConnectionPhase::AuthenticationFailed;
        self.last_attempt_ms = now_ms;
        self.update_reconnect(backoffs, observations);
    }

    fn update_reconnect(&mut self, backoffs: &ConnectionBackoffs, observations: &mut impl JavaBackoffObservations) {
        self.reconnect_ms = backoffs.reconnect.delay(self.failed_attempts, observations);
        self.failed_attempts = self.failed_attempts.wrapping_add(1);
    }

    fn reset_setup(&mut self, backoffs: &ConnectionBackoffs, observations: &mut impl JavaBackoffObservations) {
        self.failed_setup_attempts = 0;
        self.setup_ms = backoffs.setup.delay(0, observations);
    }

    /// Returns the current source connection phase.
    pub fn phase(&self) -> ConnectionPhase { self.phase }

    /// Borrows the authentication failure until it is replaced or a ready transition clears it.
    pub fn authentication_error(&self) -> Option<&E> { self.authentication_error.as_ref() }

    /// Returns the signed millisecond sample stored by the most recent attempt, disconnect or authentication failure.
    pub fn last_connect_attempt_ms(&self) -> i64 { self.last_attempt_ms }

    /// Reports whether a source disconnect phase has reached its inclusive reconnect boundary.
    /// Elapsed time wraps as a Java long.
    pub fn can_connect(&self, now_ms: i64) -> bool {
        self.phase.disconnected() && now_ms.wrapping_sub(self.last_attempt_ms) >= self.reconnect_ms
    }

    /// Reports active preparation, including the connected phase awaiting API versions.
    pub fn preparing_connection(&self) -> bool {
        matches!(self.phase, ConnectionPhase::Connecting | ConnectionPhase::CheckingApiVersions)
    }

    /// Tests the strict source setup deadline for a Connecting state using wrapping milliseconds.
    /// Other phases return None, representing Kafka's invalid-state failure.
    pub fn setup_timed_out(&self, now_ms: i64) -> Option<bool> {
        (self.phase == ConnectionPhase::Connecting)
            .then(|| now_ms.wrapping_sub(self.last_attempt_ms) > self.setup_ms)
    }

    /// Returns the current source setup timeout in milliseconds.
    pub fn setup_timeout_ms(&self) -> i64 { self.setup_ms }

    /// Reports READY and an elapsed throttle deadline for this existing connection.
    /// Selector readiness and in-flight capacity are separate source observations.
    pub fn connection_ready(&self, now_ms: i64) -> bool {
        self.phase == ConnectionPhase::Ready && self.throttle.allows_send(now_ms)
    }

    /// Applies the version-qualified Metadata throttle before the source updater handles the response.
    /// [`ConnectionThrottle::metadata_received`] gates the callback on a positive delay and version 6 or later.
    /// This callback applies the delay to the remaining in-flight requests first.
    pub fn metadata_received(
        &mut self, version: i16, throttle_ms: i32, now_ms: i64, increment_inflight: impl FnMut(i64),
    ) {
        self.throttle.metadata_received(version, throttle_ms, now_ms, increment_inflight);
    }

    /// Returns the next source connection delay in milliseconds.
    /// Connecting yields the whole setup timeout.
    /// Disconnected phases return the wrapping remainder clamped at zero.
    /// Connected phases return i64::MAX for an external event.
    pub fn connection_delay_ms(&self, now_ms: i64) -> i64 {
        match self.phase {
            ConnectionPhase::Connecting => self.setup_ms,
            ConnectionPhase::Disconnected | ConnectionPhase::AuthenticationFailed => {
                self.reconnect_ms.wrapping_sub(now_ms.wrapping_sub(self.last_attempt_ms)).max(0)
            }
            ConnectionPhase::CheckingApiVersions | ConnectionPhase::Ready => i64::MAX,
        }
    }

    /// Returns a positive connected-state throttle delay or the source connection delay.
    pub fn poll_delay_ms(&self, now_ms: i64) -> i64 {
        self.throttle.poll_delay_ms(self.phase.connected(), self.connection_delay_ms(now_ms), now_ms)
    }
}

#[cfg(test)]
#[path = "connection/tests.rs"]
mod tests;
