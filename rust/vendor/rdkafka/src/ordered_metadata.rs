//! Bounded, ordered Metadata responses from an accounted producer.

use std::{fmt, marker::PhantomData, ptr::NonNull, sync::Arc, time::Duration};
use rdkafka_sys as sys;
use crate::{client::{Client, ClientContext}, native_byte_account::NativeByteAccount, util::AsyncRuntime};

mod wire;
pub use wire::{Broker, Event, Int32Array, MetadataView, Partition, ProtocolError, RawBroker, RawEvent, RawMetadataView, RawTopic, Summary, Topic};

/// A topic name borrowed for submission only; no allocation or NUL terminator is required.
/// Submission rejects empty names and names longer than the Kafka signed-short string range.
#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct MetadataTopic<'a> {
    raw: sys::rd_kafka_ordered_metadata_topic_t,
    borrowed: PhantomData<&'a str>,
}
impl<'a> MetadataTopic<'a> {
    /// Borrows the UTF8 bytes without validating broker topic-name rules.
    pub fn new(name: &'a str) -> Self {
        Self { raw: sys::rd_kafka_ordered_metadata_topic_t { name: name.as_ptr().cast(), length: name.len() }, borrowed: PhantomData }
    }
}

/// A submission, transport or borrowed-decoding failure.
/// Native error integers are retained without remapping Kafka error codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderedMetadataError {
    /// The producer was not created with a native byte account.
    AccountRequired,
    /// No authenticated Up broker was ready; no request was accepted.
    BrokerNotReady,
    /// The timeout is zero or exceeds the native positive i32 millisecond range.
    InvalidTimeout,
    /// A librdkafka error, including admission denial and unsupported protocol profiles.
    Native(i32),
    /// The successful native body does not contain a supported versioned field prefix.
    Protocol(ProtocolError),
    /// The native ABI returned an inconsistent successful completion.
    InvalidCompletion,
}
impl fmt::Display for OrderedMetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "ordered Metadata: {self:?}") }
}
impl std::error::Error for OrderedMetadataError {}

/// Owns one bounded native response slot and the producer's byte-account guard.
/// Dropping it abandons delivery; native work retains its independent charge until its deadline or producer teardown.
/// A completed response remains available after producer destruction.
/// Last-account drop retains the existing blocking native drain contract.
pub struct OrderedMetadataRequest {
    native: NonNull<sys::rd_kafka_ordered_metadata_t>,
    _account: Arc<NativeByteAccount>,
}

// C publication and reference release use the completion mutex; published bytes are immutable.
unsafe impl Send for OrderedMetadataRequest {}
unsafe impl Sync for OrderedMetadataRequest {}

enum MetadataTarget<'a> {
    AnyNamed(&'a [MetadataTopic<'a>]),
    AnyBrokers,
    Broker { id: i32, topics: &'a [MetadataTopic<'a>] },
    Endpoint { host: &'a str, port: u16, topics: &'a [MetadataTopic<'a>] },
}

impl<C: ClientContext> Client<C> {
    /// Starts an ordered named-topic request without blocking for broker readiness.
    /// The producer must carry a NativeByteAccount; completion storage and native encoding/network overlap acquire that account before allocation.
    /// `body_capacity` bounds the retained response bytes and must fit i32; topic count must be nonzero and fit i32.
    /// `timeout` is rounded up to milliseconds and bounds accepted request retries; callers bound readiness retries separately.
    /// Uses the negotiated Metadata version in 0..13 and the producer's auto-creation setting; disabling auto creation requires v4 or newer.
    /// Topic inputs are consumed synchronously and need not remain alive afterward.
    /// No cache mutation, result decoding or Rust callback runs on native completion.
    pub fn start_ordered_metadata(&self, topics: &[MetadataTopic<'_>], body_capacity: usize, timeout: Duration) -> Result<OrderedMetadataRequest, OrderedMetadataError> {
        self.start_ordered_metadata_request(MetadataTarget::AnyNamed(topics), body_capacity, timeout)
    }

    /// Starts a broker-only Metadata request using an explicit empty topic list.
    /// Negotiates Metadata versions 1 through 13, or 4 through 13 with auto creation disabled.
    /// Uses the admission, timeout and completion ownership of [`Self::start_ordered_metadata`].
    pub fn start_ordered_metadata_brokers(&self, body_capacity: usize, timeout: Duration) -> Result<OrderedMetadataRequest, OrderedMetadataError> {
        self.start_ordered_metadata_request(MetadataTarget::AnyBrokers, body_capacity, timeout)
    }

    /// Starts ordered Metadata on the existing native broker with this nonnegative node ID.
    /// Missing or unavailable brokers return `BrokerNotReady`; a sparse connection may start for the requested broker.
    /// An empty topic slice uses the version floor and wire semantics of [`Self::start_ordered_metadata_brokers`].
    /// Named topics, resource admission, timeout and completion follow [`Self::start_ordered_metadata`].
    /// The accepted request follows the selected broker owner across its native reconnects.
    pub fn start_ordered_metadata_at(&self, broker_id: i32, topics: &[MetadataTopic<'_>], body_capacity: usize, timeout: Duration) -> Result<OrderedMetadataRequest, OrderedMetadataError> {
        self.start_ordered_metadata_request(MetadataTarget::Broker { id: broker_id, topics }, body_capacity, timeout)
    }

    /// Starts ordered Metadata on the currently authenticated connection of a configured bootstrap endpoint.
    /// Requires `enable.ordered.metadata.bootstrap` on the producer at creation; otherwise submission returns a native unsupported-feature error.
    /// Host must match the native configured hostname or address bytes, including any retained IPv6 brackets.
    /// An empty host, embedded NUL or formatted endpoint exceeding the native node-name capacity returns a native argument error.
    /// A missing or unavailable connection returns `BrokerNotReady` without starting a connection.
    /// An accepted request uses the captured connection and completes on transport failure or timeout without retry.
    /// Topic list, timeout validation and rounding, response admission and ownership follow [`Self::start_ordered_metadata_at`].
    pub fn start_ordered_metadata_endpoint(
        &self, host: &str, port: u16, topics: &[MetadataTopic<'_>], body_capacity: usize, timeout: Duration,
    ) -> Result<OrderedMetadataRequest, OrderedMetadataError> {
        self.start_ordered_metadata_request(MetadataTarget::Endpoint { host, port, topics }, body_capacity, timeout)
    }

    fn start_ordered_metadata_request(&self, target: MetadataTarget<'_>, body_capacity: usize, timeout: Duration) -> Result<OrderedMetadataRequest, OrderedMetadataError> {
        let account = self.native_client().byte_account().ok_or(OrderedMetadataError::AccountRequired)?;
        let millis = timeout.as_millis().checked_add(u128::from(timeout.subsec_nanos() % 1_000_000 != 0)).ok_or(OrderedMetadataError::InvalidTimeout)?;
        let millis = i32::try_from(millis).ok().filter(|value| *value > 0).ok_or(OrderedMetadataError::InvalidTimeout)?;
        let mut result = std::ptr::null_mut();
        let error = unsafe {
            match target {
                MetadataTarget::AnyNamed(topics) => sys::rd_kafka_ordered_metadata_start(self.native_ptr(), topics.as_ptr().cast(), topics.len(), body_capacity, millis, &mut result),
                MetadataTarget::AnyBrokers => sys::rd_kafka_ordered_metadata_start_brokers(self.native_ptr(), body_capacity, millis, &mut result),
                MetadataTarget::Broker { id, topics } => {
                    let pointer = if topics.is_empty() { std::ptr::null() } else { topics.as_ptr().cast() };
                    sys::rd_kafka_ordered_metadata_start_at(self.native_ptr(), id, pointer, topics.len(), body_capacity, millis, &mut result)
                }
                MetadataTarget::Endpoint { host, port, topics } => {
                    let pointer = if topics.is_empty() { std::ptr::null() } else { topics.as_ptr().cast() };
                    sys::rd_kafka_ordered_metadata_start_endpoint(
                        self.native_ptr(), host.as_ptr().cast(), host.len(), port,
                        pointer, topics.len(), body_capacity, millis, &mut result,
                    )
                }
            }
        };
        if error == sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__TRANSPORT {
            return Err(OrderedMetadataError::BrokerNotReady);
        }
        if error != sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            return Err(OrderedMetadataError::Native(error as i32));
        }
        Ok(OrderedMetadataRequest { native: NonNull::new(result).ok_or(OrderedMetadataError::InvalidCompletion)?, _account: Arc::clone(account) })
    }
}

impl OrderedMetadataRequest {
    /// Polls once without waiting or allocating an output value.
    /// None means pending; success validates the versioned fields and tags before yielding a borrowed view.
    /// Unconsumed suffix bytes are retained; see MetadataView::trailing_bytes.
    /// Native failures precede wire validation and preserve their integer codes.
    pub fn poll(&self) -> Option<Result<MetadataView<'_>, OrderedMetadataError>> {
        self.response().map(|result| result.and_then(|(version, bytes)| MetadataView::parse(version, bytes).map_err(OrderedMetadataError::Protocol)))
    }

    /// Polls without waiting or output allocation, preserving malformed UTF8 as raw string bytes.
    /// Native failures precede structural validation; no caller visitor runs before the complete field prefix and tags validate.
    /// The borrowed view and its strings cannot outlive this request; replacement decoding remains caller-owned.
    pub fn poll_raw(&self) -> Option<Result<RawMetadataView<'_>, OrderedMetadataError>> {
        self.response().map(|result| result.and_then(|(version, bytes)| RawMetadataView::parse(version, bytes).map_err(OrderedMetadataError::Protocol)))
    }

    fn response(&self) -> Option<Result<(i16, &[u8]), OrderedMetadataError>> {
        let mut error = sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR;
        let mut version = 0;
        let mut body = std::ptr::null();
        let mut length = 0;
        let ready = unsafe { sys::rd_kafka_ordered_metadata_poll(self.native.as_ptr(), &mut error, &mut version, &mut body, &mut length) };
        if ready == 0 { return None; }
        if error != sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            return Some(Err(OrderedMetadataError::Native(error as i32)));
        }
        if body.is_null() || length > isize::MAX as usize { return Some(Err(OrderedMetadataError::InvalidCompletion)); }
        let bytes = unsafe { std::slice::from_raw_parts(body.cast::<u8>(), length) };
        Some(Ok((version, bytes)))
    }

    /// Waits on the caller's runtime with a 10ms polling interval, without spawning a task.
    /// Cancelling this wait leaves the request pollable; dropping the request abandons delivery.
    /// Runtime timer/task allocations remain the caller's responsibility.
    pub async fn wait<R: AsyncRuntime>(&self) -> Result<MetadataView<'_>, OrderedMetadataError> {
        loop {
            if let Some(result) = self.poll() { return result; }
            R::delay_for(Duration::from_millis(10)).await;
        }
    }

    /// Waits on the caller's runtime for a raw-string view, polling every 10ms without spawning a task.
    /// Cancelling the wait leaves the request pollable; dropping it abandons delivery under the existing native drain contract.
    /// Runtime allocation and replacement decoding remain caller-owned.
    pub async fn wait_raw<R: AsyncRuntime>(&self) -> Result<RawMetadataView<'_>, OrderedMetadataError> {
        loop {
            if let Some(result) = self.poll_raw() { return result; }
            R::delay_for(Duration::from_millis(10)).await;
        }
    }
}
impl Drop for OrderedMetadataRequest {
    fn drop(&mut self) { unsafe { sys::rd_kafka_ordered_metadata_destroy(self.native.as_ptr()) } }
}
impl fmt::Debug for OrderedMetadataRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_struct("OrderedMetadataRequest").finish_non_exhaustive() }
}

#[cfg(test)] mod tests;
