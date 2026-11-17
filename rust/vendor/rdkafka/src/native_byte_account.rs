//! Byte admission for native producer receive frames and accounted requests.

use std::fmt;
use std::ptr::NonNull;
use std::sync::Arc;

use rdkafka_sys as rdsys;

use crate::error::{KafkaError, KafkaResult};

/// A shared native byte ceiling backed by a caller-owned capacity reservation.
/// Dropping the last owner blocks on its Rust caller until attached native clients and charged allocations have completed final release.
/// The supplied reservation is then dropped on that Rust caller.
pub struct NativeByteAccount {
    native: NonNull<rdsys::rd_kafka_byte_account_t>,
    _reservation: Box<dyn Send + Sync>,
}

// All access to the opaque account's live state is synchronized by its native monitor.
unsafe impl Send for NativeByteAccount {}
unsafe impl Sync for NativeByteAccount {}

impl NativeByteAccount {
    /// Takes ownership of an existing reservation covering at least `capacity` bytes.
    /// Each attached producer shares that ceiling.
    /// A zero capacity denies all positive charges.
    /// Returns a client creation error if the native monitor cannot be allocated.
    pub fn new(capacity: usize, reservation: impl Send + Sync + 'static) -> KafkaResult<Arc<Self>> {
        let native = NonNull::new(unsafe { rdsys::rd_kafka_byte_account_new(capacity) })
            .ok_or_else(|| {
                KafkaError::ClientCreation("Could not allocate the native byte account".into())
            })?;
        Ok(Arc::new(Self {
            native,
            _reservation: Box::new(reservation),
        }))
    }

    /// Returns the total charge for native allocations that have not completed final release.
    pub fn used(&self) -> usize {
        unsafe { rdsys::rd_kafka_byte_account_used(self.ptr()) }
    }

    pub(crate) fn ptr(&self) -> *mut rdsys::rd_kafka_byte_account_t {
        self.native.as_ptr()
    }
}

impl fmt::Debug for NativeByteAccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeByteAccount")
            .field("used", &self.used())
            .finish_non_exhaustive()
    }
}

impl Drop for NativeByteAccount {
    fn drop(&mut self) {
        unsafe { rdsys::rd_kafka_byte_account_destroy(self.ptr()) }
    }
}
