use std::{ffi::{CStr, c_char, c_void}, fs::File, io::Read, ptr, sync::{Arc, Mutex}};
use openssl_sys::{SSL_CTX, X509, X509_CRL, X509_STORE};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, Result};
use crate::tls::NativeTls;
use super::{DECODE_BYTES_PER_INPUT_BYTE, DECODE_FIXED_BYTES, overflow};

struct DirectorySource { owner: Arc<NativeTls>, metadata: Mutex<Arc<MemoryReservation>> }
struct DecodedFile { _owner: Arc<NativeTls>, _charge: Arc<MemoryReservation> }
struct DirectoryRead { bytes: Vec<u8>, decoded: Arc<DecodedFile>, _raw: Arc<MemoryReservation> }

pub(super) fn metadata_bound(path_length: usize) -> Result<usize> {
    path_length.checked_mul(128).and_then(|bytes| bytes.checked_add(DECODE_FIXED_BYTES)).ok_or_else(overflow)
}

pub(super) unsafe fn install(context: *mut SSL_CTX, owner: Arc<NativeTls>, memory: &Arc<MemoryBudget>, path_length: usize) -> Result<()> {
    let charge = memory.reserve(metadata_bound(path_length)?)?;
    let source = Box::into_raw(Box::new(DirectorySource { owner, metadata: Mutex::new(charge) }));
    let callbacks = NativeSource { arg: source.cast(), open, close, retain, reserve, settle, free_source };
    let installed = unsafe { X509_STORE_set0_hash_dir_source(openssl_sys::SSL_CTX_get_cert_store(context), &callbacks) };
    if installed != 1 { unsafe { drop(Box::from_raw(source)); } }
    if installed < 0 { return Err(invalid("Unable to install native TLS directory admission")); }
    Ok(())
}

impl DirectorySource {
    fn read(&self, path: &CStr) -> std::result::Result<DirectoryRead, ReadError> {
        let memory = Arc::clone(self.metadata.lock().map_err(|_| ReadError::Denied)?.budget());
        #[cfg(unix)]
        let path = { use std::os::unix::ffi::OsStrExt; std::path::Path::new(std::ffi::OsStr::from_bytes(path.to_bytes())) };
        #[cfg(not(unix))]
        let path = std::path::Path::new(path.to_str().map_err(|_| ReadError::Io(libc::EINVAL))?);
        #[cfg(unix)]
        let mut file = { use std::os::unix::fs::OpenOptionsExt; File::options().read(true).custom_flags(libc::O_NONBLOCK).open(path).map_err(ReadError::io)? };
        #[cfg(not(unix))]
        let mut file = File::open(path).map_err(ReadError::io)?;
        let metadata = file.metadata().map_err(ReadError::io)?;
        if !metadata.is_file() { return Err(ReadError::Io(libc::EINVAL)); }
        let length = usize::try_from(metadata.len()).ok().filter(|length| *length <= i32::MAX as usize).ok_or(ReadError::Denied)?;
        let raw = memory.reserve(length.checked_add(512).ok_or(ReadError::Denied)?).map_err(|_| ReadError::Denied)?;
        let decoded = memory.reserve(length.checked_mul(DECODE_BYTES_PER_INPUT_BYTE).and_then(|bytes| bytes.checked_add(DECODE_FIXED_BYTES)).ok_or(ReadError::Denied)?).map_err(|_| ReadError::Denied)?;
        let mut bytes = vec![0; length];
        file.read_exact(&mut bytes).map_err(ReadError::io)?;
        if file.read(&mut [0]).map_err(ReadError::io)? != 0 { return Err(ReadError::Io(libc::EAGAIN)); }
        Ok(DirectoryRead { bytes, decoded: Arc::new(DecodedFile { _owner: Arc::clone(&self.owner), _charge: decoded }), _raw: raw })
    }

    fn reserve(&self, bytes: usize) -> Result<*mut c_void> {
        let mut charge = self.metadata.lock().map_err(|_| invalid("Native TLS directory metadata lock was poisoned"))?;
        let next = charge.bytes().checked_add(bytes).and_then(|size| size.checked_add(64)).ok_or_else(overflow)?;
        Arc::get_mut(&mut charge).expect("metadata reservation is exclusively owned").grow_to(next)?;
        Ok(Box::into_raw(Box::new(bytes)).cast())
    }

    fn settle(&self, bytes: usize, retained: bool) {
        let mut charge = self.metadata.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let released = 64 + if retained { 0 } else { bytes };
        let next = charge.bytes().checked_sub(released).expect("only this grant's admitted bytes are released");
        Arc::get_mut(&mut charge).expect("metadata reservation is exclusively owned").shrink_to(next).expect("settlement only shrinks admission");
    }
}

enum ReadError { Io(i32), Denied }
impl ReadError { fn io(error: std::io::Error) -> Self { Self::Io(error.raw_os_error().unwrap_or(libc::EIO)) } }

unsafe extern "C" fn open(arg: *mut c_void, path: *const c_char, bytes: *mut *const u8, length: *mut usize, read: *mut *mut c_void, os_error: *mut i32) -> i32 {
    unsafe { *bytes = ptr::null(); *length = 0; *read = ptr::null_mut(); *os_error = libc::EIO; }
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let source = unsafe { &*arg.cast::<DirectorySource>() };
        let value = source.read(unsafe { CStr::from_ptr(path) })?;
        let value = Box::into_raw(Box::new(value));
        unsafe { *bytes = (*value).bytes.as_ptr(); *length = (*value).bytes.len(); *read = value.cast(); }
        Ok::<(), ReadError>(())
    })) {
        Ok(Ok(())) => 1,
        Ok(Err(ReadError::Io(error))) => { unsafe { *os_error = error; } 0 },
        _ => -1,
    }
}

unsafe extern "C" fn close(read: *mut c_void) { unsafe { drop(Box::from_raw(read.cast::<DirectoryRead>())); } }
unsafe extern "C" fn free_source(arg: *mut c_void) { unsafe { drop(Box::from_raw(arg.cast::<DirectorySource>())); } }
unsafe extern "C" fn free_decoded(arg: *mut c_void) { unsafe { drop(Arc::from_raw(arg.cast::<DecodedFile>())); } }

unsafe extern "C" fn retain(read: *mut c_void, certificate: *mut X509, crl: *mut X509_CRL) -> i32 {
    let read = unsafe { &*read.cast::<DirectoryRead>() };
    let owner = Arc::into_raw(Arc::clone(&read.decoded)).cast_mut().cast();
    let installed = if certificate.is_null() { unsafe { X509_CRL_set0_retained_owner(crl, owner, free_decoded) } }
        else { unsafe { X509_set0_retained_owner(certificate, owner, free_decoded) } };
    if installed != 1 { unsafe { free_decoded(owner); } }
    installed
}

unsafe extern "C" fn reserve(arg: *mut c_void, bytes: usize) -> *mut c_void {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe { &*arg.cast::<DirectorySource>() }.reserve(bytes)))
        .ok().and_then(std::result::Result::ok).unwrap_or(ptr::null_mut())
}

unsafe extern "C" fn settle(arg: *mut c_void, grant: *mut c_void, retained: i32) {
    let bytes = unsafe { *Box::from_raw(grant.cast::<usize>()) };
    unsafe { &*arg.cast::<DirectorySource>() }.settle(bytes, retained != 0);
}

#[repr(C)]
struct NativeSource {
    arg: *mut c_void,
    open: unsafe extern "C" fn(*mut c_void, *const c_char, *mut *const u8, *mut usize, *mut *mut c_void, *mut i32) -> i32,
    close: unsafe extern "C" fn(*mut c_void),
    retain: unsafe extern "C" fn(*mut c_void, *mut X509, *mut X509_CRL) -> i32,
    reserve: unsafe extern "C" fn(*mut c_void, usize) -> *mut c_void,
    settle: unsafe extern "C" fn(*mut c_void, *mut c_void, i32),
    free_source: unsafe extern "C" fn(*mut c_void),
}
unsafe extern "C" {
    fn X509_STORE_set0_hash_dir_source(store: *mut X509_STORE, source: *const NativeSource) -> i32;
    fn X509_set0_retained_owner(certificate: *mut X509, owner: *mut c_void, free_owner: unsafe extern "C" fn(*mut c_void)) -> i32;
    fn X509_CRL_set0_retained_owner(crl: *mut X509_CRL, owner: *mut c_void, free_owner: unsafe extern "C" fn(*mut c_void)) -> i32;
}
fn invalid(cause: &str) -> Anomaly { Anomaly::incorrect("xtdb.kafka/invalid-tls-config", cause) }

#[cfg(test)]
mod tests;
