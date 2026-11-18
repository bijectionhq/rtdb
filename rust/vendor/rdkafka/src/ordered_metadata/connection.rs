use std::{fmt, mem::MaybeUninit, ptr::NonNull, sync::Arc, time::Duration};
use rdkafka_sys as sys;
use crate::{client::{Client, ClientContext, NativeClient}, native_byte_account::NativeByteAccount};
use super::{MetadataTopic, OrderedMetadataError, OrderedMetadataRequest};

/// Copied native facts, with monotonic microsecond timestamps and native broker state/source codes.
/// Source codes are configured=0 and learned=1.
/// State codes are INIT=0, DOWN=1, TRY_CONNECT=2, CONNECT=3, SSL_HANDSHAKE=4, AUTH_LEGACY=5,
/// UP=6, APIVERSION_QUERY=7, AUTH_HANDSHAKE=8, AUTH_REQ=9 and REAUTH=10.
/// Counts refer to native buffer queues; transport/partial-send flags are zero or one.
/// last_connect_attempt_us is zero until native code records an attempt; reconnect_at_us retains its native zero reset sentinel.
/// Native queue counts and transport presence do not establish Java Selector, throttle, random or in-flight observations.
/// Changing this copy cannot change the immutable observation or its native submission target.
pub type NativeConnectionSnapshot = sys::rd_kafka_connection_snapshot_t;

/// Owns an admitted broker-thread observation while borrowing its originating client.
/// Only the queued control retains a broker reference; completed observations cannot delay broker decommission.
/// Dropping this owner abandons delivery and releases its storage after the queued control finishes or cancels.
/// Source Java connection observations remain a separate caller qualification.
pub struct NativeConnectionObservation<'client> {
    native: NonNull<sys::rd_kafka_connection_observation_t>,
    client: &'client NativeClient,
    account: Arc<NativeByteAccount>,
}

// Native publication/refcounts use a mutex; the borrowed NativeClient is thread safe.
unsafe impl Send for NativeConnectionObservation<'_> {}
unsafe impl Sync for NativeConnectionObservation<'_> {}

impl<C: ClientContext> Client<C> {
    /// Queues a snapshot on the existing learned broker with this nonnegative ID, without connecting or choosing another broker.
    /// Requires a NativeByteAccount; completion and queue control storage are admitted independently.
    /// A missing owner returns BrokerNotReady; the returned owner cannot outlive this client.
    /// A negative ID, denied allocation or unsupported native profile returns Native with the original native error code.
    pub fn observe_native_connection(&self, node_id: i32) -> Result<NativeConnectionObservation<'_>, OrderedMetadataError> {
        self.observe_connection_target(Some(node_id), "", 0)
    }

    /// Observes an exact configured endpoint, requiring enable.ordered.metadata.bootstrap=true.
    /// Host/port validation follows start_ordered_metadata_endpoint; this call neither resolves nor starts a connection.
    /// Admission, missing-owner and client lifetime follow observe_native_connection.
    pub fn observe_native_endpoint(&self, host: &str, port: u16) -> Result<NativeConnectionObservation<'_>, OrderedMetadataError> {
        self.observe_connection_target(None, host, port)
    }

    fn observe_connection_target(&self, id: Option<i32>, host: &str, port: u16) -> Result<NativeConnectionObservation<'_>, OrderedMetadataError> {
        let account = self.native_client().byte_account().ok_or(OrderedMetadataError::AccountRequired)?;
        let mut native = std::ptr::null_mut();
        let error = unsafe {
            match id {
                Some(id) => sys::rd_kafka_connection_observe_at(self.native_ptr(), id, &mut native),
                None => sys::rd_kafka_connection_observe_endpoint(self.native_ptr(), host.as_ptr().cast(), host.len(), port, &mut native),
            }
        };
        if error != sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR { return Err(native_error(error)); }
        Ok(NativeConnectionObservation {
            native: NonNull::new(native).ok_or(OrderedMetadataError::InvalidCompletion)?,
            client: self.native_client(), account: Arc::clone(account),
        })
    }
}

impl NativeConnectionObservation<'_> {
    /// Polls without waiting or allocating; a ready snapshot/error remains unchanged across polls.
    /// Successful facts belong to one native broker actor, not an atomic serialized Java cross-node scan.
    /// Queue cancellation returns Native with the native DESTROY code.
    pub fn poll(&self) -> Option<Result<NativeConnectionSnapshot, OrderedMetadataError>> {
        let mut error = sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR;
        let mut snapshot = MaybeUninit::uninit();
        let ready = unsafe { sys::rd_kafka_connection_observation_poll(self.native.as_ptr(), &mut error, snapshot.as_mut_ptr()) };
        if ready == 0 { return None; }
        if error != sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR { return Some(Err(native_error(error))); }
        Some(Ok(unsafe { snapshot.assume_init() }))
    }

    /// Consumes this observation and submits only to its exact native owner and observed authenticated socket.
    /// A pending result returns native IN_PROGRESS; a replaced/reconnected/unavailable owner returns BrokerNotReady.
    /// No sparse connection or native retry is attempted, and zero/exhausted socket generations are unsupported.
    /// Topic, body-capacity, timeout and completion ownership follow start_ordered_metadata_endpoint.
    /// The timeout rounds up to positive milliseconds fitting i32; an invalid duration returns InvalidTimeout.
    pub fn into_metadata(self, topics: &[MetadataTopic<'_>], body_capacity: usize, timeout: Duration) -> Result<OrderedMetadataRequest, OrderedMetadataError> {
        let millis = timeout.as_millis().checked_add(u128::from(timeout.subsec_nanos() % 1_000_000 != 0)).ok_or(OrderedMetadataError::InvalidTimeout)?;
        let millis = i32::try_from(millis).ok().filter(|value| *value > 0).ok_or(OrderedMetadataError::InvalidTimeout)?;
        let mut result = std::ptr::null_mut();
        let pointer = if topics.is_empty() { std::ptr::null() } else { topics.as_ptr().cast() };
        let error = unsafe {
            sys::rd_kafka_ordered_metadata_start_observed(self.client.ptr(), self.native.as_ptr(),
                pointer, topics.len(), body_capacity, millis, &mut result)
        };
        if error != sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR { return Err(native_error(error)); }
        Ok(OrderedMetadataRequest {
            native: NonNull::new(result).ok_or(OrderedMetadataError::InvalidCompletion)?,
            _account: Arc::clone(&self.account),
        })
    }
}

fn native_error(error: sys::rd_kafka_resp_err_t) -> OrderedMetadataError {
    if error == sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__TRANSPORT { OrderedMetadataError::BrokerNotReady }
    else { OrderedMetadataError::Native(error as i32) }
}

impl Drop for NativeConnectionObservation<'_> {
    fn drop(&mut self) { unsafe { sys::rd_kafka_connection_observation_destroy(self.native.as_ptr()) } }
}
impl fmt::Debug for NativeConnectionObservation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_struct("NativeConnectionObservation").finish_non_exhaustive() }
}
