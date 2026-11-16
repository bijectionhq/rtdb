mod configuration;
mod workload;
pub use configuration::validate_configuration;
pub use workload::validate_workload;
#[cfg(target_os = "linux")]
mod filesystem;
#[cfg(target_os = "linux")]
pub mod driver;

use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::{Arc, Mutex}};

pub const BOUNDARY: &str = "successful_backing_filesystem_payload_and_requests";

/// Owned configuration for one isolated measurement lifetime.
/// Paths and process privileges are checked by `driver::run` before mounting.
/// Limits count retained inodes, open handles, and bytes per payload request, respectively.
/// The caller owns the empty backing/mount directories and retains them until the driver returns.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub namespace: String,
    pub storage_placement: String,
    pub backing: PathBuf,
    pub mount: PathBuf,
    pub ready: PathBuf,
    pub final_receipt: PathBuf,
    pub expected_binary_sha256: String,
    pub max_inodes: usize,
    pub max_handles: usize,
    pub max_io_bytes: usize,
}
impl Config {
    /// Checks scalar identities and allocation limits without performing I/O.
    /// Path existence, isolation, and mount privileges are checked when the driver starts.
    /// Invalid values return `io::Error`; no configuration is changed.
    pub fn validate(&self) -> std::io::Result<()> {
        if self.namespace.is_empty() || self.namespace.len() > 128 || !self.namespace.bytes().all(|c|c.is_ascii_alphanumeric() || c == b'-')
            || self.storage_placement.is_empty() || self.storage_placement.len() > 1024
            || !(16..=1_000_000).contains(&self.max_inodes) || !(16..=65_536).contains(&self.max_handles)
            || !(4096..=1024*1024).contains(&self.max_io_bytes)
            || self.expected_binary_sha256.len() != 64 || !self.expected_binary_sha256.bytes().all(|c|c.is_ascii_digit() || (b'a'..=b'f').contains(&c)) {
            return Err(std::io::Error::other("invalid bounded filesystem observer configuration"));
        }
        Ok(())
    }
}

#[derive(Clone, Default, Serialize)]
pub(crate) struct Counters {
    pub successful_read_bytes: u64,
    pub successful_read_requests: u64,
    pub successful_write_bytes: u64,
    pub successful_write_requests: u64,
    pub successful_metadata_requests: u64,
    pub failed_requests: u64,
    pub failed_request_bytes: u64,
    pub failed_response_bytes: u64,
    pub rejected_requests: u64,
    pub rejected_connections: u64,
    pub in_flight_requests: u64,
    pub overflowed: bool,
}

#[derive(Clone, Default)]
pub(crate) struct Measurements(Arc<Mutex<Counters>>);
impl Measurements {
    pub fn snapshot(&self) -> Counters { self.0.lock().unwrap_or_else(|e|e.into_inner()).clone() }
    pub fn request(&self) -> RequestGuard {
        self.update(|v| add(&mut v.in_flight_requests, 1, &mut v.overflowed));
        RequestGuard(self.clone())
    }
    pub fn read(&self, bytes: usize) { self.update(|v| { add(&mut v.successful_read_requests,1,&mut v.overflowed); add(&mut v.successful_read_bytes,bytes as u64,&mut v.overflowed); }); }
    pub fn write(&self, bytes: usize) { self.update(|v| { add(&mut v.successful_write_requests,1,&mut v.overflowed); add(&mut v.successful_write_bytes,bytes as u64,&mut v.overflowed); }); }
    pub fn metadata(&self) { self.update(|v|add(&mut v.successful_metadata_requests,1,&mut v.overflowed)); }
    pub fn failed(&self) { self.update(|v|add(&mut v.failed_requests,1,&mut v.overflowed)); }
    pub fn rejected(&self) { self.update(|v|add(&mut v.rejected_requests,1,&mut v.overflowed)); }
    fn update(&self, f: impl FnOnce(&mut Counters)) { f(&mut self.0.lock().unwrap_or_else(|e|e.into_inner())); }
}
fn add(value: &mut u64, n: u64, overflow: &mut bool) { match value.checked_add(n) { Some(next)=>*value=next, None=>*overflow=true } }
pub(crate) struct RequestGuard(Measurements);
impl Drop for RequestGuard {
    fn drop(&mut self) { self.0.update(|v|v.in_flight_requests-=1); }
}
