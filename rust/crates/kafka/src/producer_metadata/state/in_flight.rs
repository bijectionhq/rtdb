use std::{collections::VecDeque, sync::Arc};

use rtdb_types::{Anomaly, MemoryBudget, Result, java_hash_map::Admission};

use super::{Budget, Lease, owned::{allocation_failed, bytes}};

/// One accepted request's admitted payload and source send, throttle and timeout state.
/// The caller supplies a payload owner that keeps its own admission through native delivery or cancellation.
pub struct PendingRequest<T> {
    payload: T,
    sent_ms: i64,
    timeout_ms: i32,
    throttle_ms: i64,
    expects_response: bool,
    send_completed: bool,
}

impl<T> PendingRequest<T> {
    /// Creates a freshly submitted request from its original payload and source timing.
    /// The timeout is the caller's validated nonnegative signed milliseconds.
    pub fn new(payload: T, sent_ms: i64, timeout_ms: i32, expects_response: bool) -> Self {
        Self { payload, sent_ms, timeout_ms, throttle_ms: 0, expects_response, send_completed: false }
    }

    /// Borrows the accepted payload without transferring the in-flight slot.
    pub fn payload(&self) -> &T { &self.payload }

    /// Returns the exact original payload after the caller removes this request.
    pub fn into_payload(self) -> T { self.payload }

    fn expired(&self, now_ms: i64) -> bool {
        now_ms.wrapping_sub(self.sent_ms).max(0).wrapping_sub(self.throttle_ms) > i64::from(self.timeout_ms)
    }
}

struct Storage<T> {
    requests: VecDeque<PendingRequest<T>>,
    _lease: Lease,
}

/// Admitted in-flight storage for one source connection, serialized by its network caller.
/// The caller owns the connection map, total request count and exact Selector delivery order.
pub struct InFlightQueue<T> {
    maximum: usize,
    storage: Option<Storage<T>>,
    budget: Budget,
}

impl<T> InFlightQueue<T> {
    /// Creates an empty queue with the source's positive signed in-flight limit.
    /// The limit gates send eligibility, independent of native storage admission.
    pub fn new(maximum: i32, budget: Arc<MemoryBudget>) -> Result<Self> {
        if maximum <= 0 {
            return Err(Anomaly::incorrect("rtdb/kafka-in-flight-limit", "Source in-flight limit must be positive"));
        }
        Ok(Self { maximum: maximum as usize, storage: None, budget: Budget(budget) })
    }

    /// Retains one original submitted request as the newest send.
    /// Storage growth admits the new capacity before releasing the old queue's allocation.
    /// Queue admission covers envelope storage; the network caller enforces [`Self::can_send_more`] before accepting a send.
    /// A denial drops the proposed request and leaves all existing requests intact.
    pub fn add(&mut self, request: PendingRequest<T>) -> Result<()> {
        let replacement = self.reserve_slot()?;
        self.install_request(request, replacement);
        Ok(())
    }

    pub(super) fn submit<E>(
        &mut self, send: impl FnOnce() -> std::result::Result<PendingRequest<T>, E>,
    ) -> Result<std::result::Result<(), E>> {
        if !self.can_send_more() {
            return Err(Anomaly::incorrect("rtdb/kafka-in-flight-capacity", "Connection has no eligible in-flight request slot"));
        }
        let replacement = self.reserve_slot()?;
        match send() {
            Ok(request) => {
                self.install_request(request, replacement);
                Ok(Ok(()))
            }
            Err(rejected) => Ok(Err(rejected)),
        }
    }

    fn reserve_slot(&self) -> Result<Option<Storage<T>>> {
        if self.count() == i32::MAX {
            return Err(Anomaly::unsupported("rtdb/kafka-in-flight-size", "Java request queues require a signed int length"));
        }
        let capacity = self.storage.as_ref().map_or(0, |storage| storage.requests.capacity());
        if self.count() as usize == capacity {
            let next = capacity.saturating_mul(2).clamp(4, i32::MAX as usize);
            let lease = self.budget.reserve(bytes::<PendingRequest<T>>(next)?)?;
            let mut requests = VecDeque::new();
            requests.try_reserve_exact(next).map_err(|_| allocation_failed())?;
            Ok(Some(Storage { requests, _lease: lease }))
        } else {
            Ok(None)
        }
    }

    fn install_request(&mut self, request: PendingRequest<T>, replacement: Option<Storage<T>>) {
        if let Some(mut replacement) = replacement {
            if let Some(mut previous) = self.storage.take() {
                replacement.requests.append(&mut previous.requests);
            }
            self.storage = Some(replacement);
        }
        self.storage.as_mut().expect("queue storage admitted").requests.push_back(request);
    }

    /// Returns the source connection request count in the signed int domain.
    pub fn count(&self) -> i32 {
        self.storage.as_ref().map_or(0, |storage| storage.requests.len() as i32)
    }

    /// Allows sending when empty, or when the newest send is complete and capacity remains below the source limit.
    pub fn can_send_more(&self) -> bool {
        self.storage.as_ref().is_none_or(|storage| {
            storage.requests.back().is_none_or(|last| last.send_completed && storage.requests.len() < self.maximum)
        })
    }

    /// Borrows the newest request, if present, for the source completed-send stage.
    pub fn last_sent(&self) -> Option<&PendingRequest<T>> {
        self.storage.as_ref()?.requests.back()
    }

    /// Borrows the oldest request for receive readiness before transferring its slot.
    pub fn oldest(&self) -> Option<&PendingRequest<T>> {
        self.storage.as_ref()?.requests.front()
    }

    /// Records the Selector's completed send for the newest request.
    /// A send expecting no response completes immediately and transfers its original owner.
    /// An empty queue returns an error, corresponding to Kafka's invalid completion.
    pub fn complete_send(&mut self) -> Result<Option<PendingRequest<T>>> {
        let requests = &mut self.storage.as_mut().ok_or_else(missing_send)?.requests;
        let last = requests.back_mut().ok_or_else(missing_send)?;
        last.send_completed = true;
        Ok(if last.expects_response { None } else { requests.pop_back() })
    }

    /// Completes the oldest request on a matching Selector receive.
    /// The caller preserves source correlation and reports an empty queue as an invalid receive.
    pub fn complete_next(&mut self) -> Option<PendingRequest<T>> {
        self.storage.as_mut()?.requests.pop_front()
    }

    /// Applies the source throttle to each currently queued request in newest-first order.
    /// Signed milliseconds add with Java long wrapping.
    pub fn increment_throttle(&mut self, throttle_ms: i64) {
        if let Some(storage) = &mut self.storage {
            for request in storage.requests.iter_mut().rev() {
                request.throttle_ms = request.throttle_ms.wrapping_add(throttle_ms);
            }
        }
    }

    /// Tests strict expiry after subtracting accumulated throttle from the nonnegative elapsed send time.
    /// Elapsed and throttle subtraction use Java long wrapping.
    pub fn has_expired(&self, now_ms: i64) -> bool {
        self.storage.as_ref().is_some_and(|storage| storage.requests.iter().rev().any(|request| request.expired(now_ms)))
    }

    /// Transfers the deque for disconnect cleanup in oldest-first order.
    /// It retains its native capacity lease until the returned iterator drops.
    pub fn clear_all(self) -> impl Iterator<Item = PendingRequest<T>> {
        let mut storage = self.storage;
        std::iter::from_fn(move || storage.as_mut()?.requests.pop_front())
    }
}

fn missing_send() -> Anomaly {
    Anomaly::incorrect("rtdb/kafka-in-flight-send", "No in-flight request is awaiting a send completion")
}

#[cfg(test)]
#[path = "in_flight/tests.rs"]
mod tests;
