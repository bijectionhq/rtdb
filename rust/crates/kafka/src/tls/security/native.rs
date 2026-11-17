use std::{ffi::c_void, sync::OnceLock};
use foreign_types::{ForeignType, ForeignTypeRef};
use openssl::{error::ErrorStack, ex_data::Index, nid::Nid, ssl::{SslContext, SslContextBuilder, SslOptions}, x509::X509Ref};
use rtdb_types::Result;
use super::super::invalid;

type Callback = unsafe extern "C" fn(*const openssl_sys::SSL, *const openssl_sys::SSL_CTX, i32, i32, i32, *mut c_void, *mut c_void) -> i32;
struct Previous { callback: Callback, extra: usize }
const PEER: i32 = 0x1000;
const EE_KEY: i32 = 0x60010;
const CA_KEY: i32 = 0x60011;
const CA_DIGEST: i32 = 0x60012;
const CURVE_SUPPORTED: i32 = 0x20004;
const CURVE_SHARED: i32 = 0x20005;
const CURVE_CHECK: i32 = 0x20006;

pub(crate) fn configure(context: &mut SslContextBuilder) -> Result<()> {
    let index = index()?;
    if context.ex_data(index).is_some() { return Err(invalid("TLS context security callback is already configured")); }
    let previous = unsafe { SSL_CTX_get_security_callback(context.as_ptr()) }.ok_or_else(|| invalid("TLS context has no security callback"))?;
    let extra = unsafe { SSL_CTX_get0_security_ex_data(context.as_ptr()) } as usize;
    context.set_ex_data(index, Previous { callback: previous, extra });
    let previous = context.ex_data(index).expect("installed TLS security callback") as *const Previous;
    context.set_security_level(1);
    context.set_options(SslOptions::NO_COMPRESSION);
    unsafe {
        SSL_CTX_set0_security_ex_data(context.as_ptr(), previous.cast_mut().cast());
        SSL_CTX_set_security_callback(context.as_ptr(), Some(check));
    }
    Ok(())
}

fn index() -> Result<Index<SslContext, Previous>> {
    static INDEX: OnceLock<std::result::Result<Index<SslContext, Previous>, ErrorStack>> = OnceLock::new();
    INDEX.get_or_init(SslContext::new_ex_index).as_ref().copied().map_err(invalid)
}

unsafe extern "C" fn check(ssl: *const openssl_sys::SSL, context: *const openssl_sys::SSL_CTX, operation: i32, bits: i32, nid: i32, other: *mut c_void, opaque: *mut c_void) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let base = operation & !PEER;
        if matches!(base, EE_KEY | CA_KEY) {
            if other.is_null() { return 0; }
            let certificate = unsafe { X509Ref::from_ptr(other.cast()) };
            return i32::from(super::verify_public_key(certificate).is_ok());
        }
        if base == CA_DIGEST && nid == Nid::SHA1.as_raw() { return 1; }
        if matches!(base, CURVE_SUPPORTED | CURVE_SHARED | CURVE_CHECK) && bits < 112 { return 0; }
        let previous = unsafe { &*opaque.cast::<Previous>() };
        unsafe { (previous.callback)(ssl, context, operation, bits, nid, other, previous.extra as *mut c_void) }
    })).unwrap_or(0)
}

unsafe extern "C" {
    fn SSL_CTX_get_security_callback(context: *const openssl_sys::SSL_CTX) -> Option<Callback>;
    fn SSL_CTX_get0_security_ex_data(context: *const openssl_sys::SSL_CTX) -> *mut c_void;
    fn SSL_CTX_set0_security_ex_data(context: *mut openssl_sys::SSL_CTX, extra: *mut c_void);
    fn SSL_CTX_set_security_callback(context: *mut openssl_sys::SSL_CTX, callback: Option<Callback>);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;
    use openssl::{hash::MessageDigest, ssl::SslMethod};

    #[test]
    fn native_security_keeps_handshake_sha1_disabled_and_java_certificate_key_limits() {
        let mut context = SslContextBuilder::new(SslMethod::tls_client()).unwrap();
        configure(&mut context).unwrap();
        assert_eq!(context.security_level(), 1);
        assert!(context.options().contains(SslOptions::NO_COMPRESSION));
        let opaque = unsafe { SSL_CTX_get0_security_ex_data(context.as_ptr()) };
        let call = |operation, bits, nid, other| unsafe { check(ptr::null(), context.as_ptr(), operation, bits, nid, other, opaque) };
        assert_eq!(call(0x60012, 63, Nid::SHA1.as_raw(), ptr::null_mut()), 1);
        assert_eq!(call(0x61012, 63, Nid::SHA1.as_raw(), ptr::null_mut()), 1);
        assert_eq!(call(0x5000d, 63, Nid::SHA1.as_raw(), ptr::null_mut()), 0);
        assert_eq!(call(0x20006, 111, 0, ptr::null_mut()), 0);
        assert_eq!(call(0x20006, 112, 0, ptr::null_mut()), 1);
        for (bits, accepted) in [(512, 0), (1024, 1)] {
            let certificate = super::super::tests::certificate(bits, MessageDigest::sha256(), 1_700_000_000, None);
            assert_eq!(call(0x60010, 0, 0, certificate.as_ptr().cast()), accepted);
        }
    }
}
