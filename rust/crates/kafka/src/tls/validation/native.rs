use std::{ffi::{c_char, c_void}, ptr, sync::{Arc, OnceLock}};
use foreign_types::{ForeignType, ForeignTypeRef};
use openssl::{asn1::Asn1Time, error::ErrorStack, ex_data::Index, ssl::{Ssl, SslContextBuilder, SslRef}, stack::{Stack, StackRef}, x509::{X509, X509Ref}};
use rtdb_columnar::MemoryReservation;
use rtdb_types::Result;
use super::{SimpleTrust, invalid};

impl SimpleTrust {
    // The certificate verifier borrows this value until the context and its SSL objects are destroyed.
    pub(crate) unsafe fn configure_native(&self, context: &mut SslContextBuilder) -> Result<()> {
        charges_index()?;
        unsafe { SSL_CTX_set_cert_verify_callback(context.as_ptr(), Some(verify), (self as *const Self).cast_mut().cast()); }
        Ok(())
    }

    unsafe fn verify_native(&self, context: *mut openssl_sys::X509_STORE_CTX) -> Result<()> {
        let leaf = unsafe { X509_STORE_CTX_get0_cert(context) };
        if leaf.is_null() { return Err(invalid("TLS peer supplied no certificate")); }
        let untrusted = unsafe { X509_STORE_CTX_get0_untrusted(context) };
        let untrusted = if untrusted.is_null() { None } else { Some(unsafe { StackRef::<X509>::from_ptr(untrusted) }) };
        let leaf = unsafe { X509Ref::from_ptr(leaf) };
        let certificates = || std::iter::once(leaf).chain(untrusted.into_iter().flatten().enumerate().filter_map(|(index, certificate)| (index != 0 || certificate.as_ptr() != leaf.as_ptr()).then_some(certificate)));
        let bound = certificates().try_fold(65_536usize, |bytes, certificate| {
            let length = unsafe { openssl_sys::i2d_X509(certificate.as_ptr(), ptr::null_mut()) };
            usize::try_from(length).ok().and_then(|length| length.checked_mul(32)).and_then(|length| bytes.checked_add(length)).ok_or_else(|| invalid("TLS peer chain exceeds allocation range"))
        })?;
        let charge = self._charge.budget().reserve(bound)?;
        let peer = certificates().map(X509Ref::to_owned).collect::<Vec<_>>();
        let now = Asn1Time::days_from_now(0).map_err(invalid)?;
        let verified = self.verify(&peer, &now, crate::tls::security::verify_server_chain)?;
        unsafe { verify_names(context, verified[0])?; }
        let mut chain = Stack::new().map_err(invalid)?;
        for certificate in verified { chain.push(certificate.to_owned()).map_err(invalid)?; }
        let ssl = unsafe { openssl_sys::X509_STORE_CTX_get_ex_data(context, openssl_sys::SSL_get_ex_data_X509_STORE_CTX_idx()) }.cast::<openssl_sys::SSL>();
        if ssl.is_null() { return Err(invalid("TLS verifier has no owning connection")); }
        let ssl = unsafe { SslRef::from_ptr_mut(ssl) };
        let index = charges_index()?;
        if let Some(charges) = ssl.ex_data_mut(index) { charges.push(charge); } else { ssl.set_ex_data(index, vec![charge]); }
        unsafe { X509_STORE_CTX_set0_verified_chain(context, chain.as_ptr()); }
        std::mem::forget(chain);
        unsafe { openssl_sys::X509_STORE_CTX_set_error(context, openssl_sys::X509_V_OK); }
        Ok(())
    }
}

fn charges_index() -> Result<Index<Ssl, Vec<Arc<MemoryReservation>>>> {
    static INDEX: OnceLock<std::result::Result<Index<Ssl, Vec<Arc<MemoryReservation>>>, ErrorStack>> = OnceLock::new();
    INDEX.get_or_init(Ssl::new_ex_index).as_ref().copied().map_err(invalid)
}

unsafe extern "C" fn verify(context: *mut openssl_sys::X509_STORE_CTX, opaque: *mut c_void) -> i32 {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let trust = unsafe { &*opaque.cast::<SimpleTrust>() };
        unsafe { trust.verify_native(context) }
    }));
    match result {
        Ok(Ok(())) => 1,
        _ => { unsafe { openssl_sys::X509_STORE_CTX_set_error(context, openssl_sys::X509_V_ERR_APPLICATION_VERIFICATION); } 0 },
    }
}

unsafe fn verify_names(context: *mut openssl_sys::X509_STORE_CTX, certificate: &X509Ref) -> Result<()> {
    let parameters = unsafe { X509_STORE_CTX_get0_param(context) };
    if parameters.is_null() { return Err(invalid("TLS verification parameters are absent")); }
    let flags = unsafe { X509_VERIFY_PARAM_get_hostflags(parameters) };
    let mut host = unsafe { X509_VERIFY_PARAM_get0_host(parameters, 0) };
    let mut matched = host.is_null();
    let mut index = 0;
    while !host.is_null() {
        matched |= unsafe { openssl_sys::X509_check_host(certificate.as_ptr(), host, 0, flags, ptr::null_mut()) } == 1;
        index += 1;
        host = unsafe { X509_VERIFY_PARAM_get0_host(parameters, index) };
    }
    if !matched { return Err(invalid("TLS server hostname does not match its certificate")); }
    let address = unsafe { X509_VERIFY_PARAM_get1_ip_asc(parameters) };
    if !address.is_null() {
        let matched = unsafe { openssl_sys::X509_check_ip_asc(certificate.as_ptr(), address, 0) } == 1;
        unsafe { openssl_sys::OPENSSL_free(address.cast()); }
        if !matched { return Err(invalid("TLS server IP address does not match its certificate")); }
    }
    Ok(())
}

unsafe extern "C" {
    fn SSL_CTX_set_cert_verify_callback(context: *mut openssl_sys::SSL_CTX, callback: Option<unsafe extern "C" fn(*mut openssl_sys::X509_STORE_CTX, *mut c_void) -> i32>, opaque: *mut c_void);
    fn X509_STORE_CTX_get0_cert(context: *const openssl_sys::X509_STORE_CTX) -> *mut openssl_sys::X509;
    fn X509_STORE_CTX_get0_untrusted(context: *const openssl_sys::X509_STORE_CTX) -> *mut openssl_sys::stack_st_X509;
    fn X509_STORE_CTX_get0_param(context: *const openssl_sys::X509_STORE_CTX) -> *mut openssl_sys::X509_VERIFY_PARAM;
    fn X509_STORE_CTX_set0_verified_chain(context: *mut openssl_sys::X509_STORE_CTX, chain: *mut openssl_sys::stack_st_X509);
    fn X509_VERIFY_PARAM_get_hostflags(parameters: *const openssl_sys::X509_VERIFY_PARAM) -> u32;
    fn X509_VERIFY_PARAM_get0_host(parameters: *mut openssl_sys::X509_VERIFY_PARAM, index: i32) -> *mut c_char;
    fn X509_VERIFY_PARAM_get1_ip_asc(parameters: *mut openssl_sys::X509_VERIFY_PARAM) -> *mut c_char;
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::x509::{X509StoreContext, store::X509StoreBuilder, verify::X509VerifyParamRef};
    use crate::tls::tests::support::Credential;

    #[test]
    fn simple_native_verification_preserves_hostname_and_ip_requirements() {
        let root = Credential::new("issuer", false, None);
        let server = Credential::new("localhost", false, Some(&root));
        let store = X509StoreBuilder::new().unwrap().build(); let chain = Stack::new().unwrap();
        let mut context = X509StoreContext::new().unwrap();
        context.init(&store, &server.certificate, &chain, |context| {
            let parameters = unsafe { X509_STORE_CTX_get0_param(context.as_ptr()) };
            let borrow_parameters = || unsafe { X509VerifyParamRef::from_ptr_mut(parameters) };
            borrow_parameters().set_host("localhost").unwrap();
            assert!(unsafe { verify_names(context.as_ptr(), &server.certificate) }.is_ok());
            borrow_parameters().set_host("wrong.example").unwrap();
            assert!(unsafe { verify_names(context.as_ptr(), &server.certificate) }.is_err());
            unsafe { openssl_sys::X509_VERIFY_PARAM_set1_host(parameters, ptr::null(), 0); }
            borrow_parameters().set_ip("127.0.0.1".parse().unwrap()).unwrap();
            assert!(unsafe { verify_names(context.as_ptr(), &server.certificate) }.is_ok());
            borrow_parameters().set_ip("127.0.0.2".parse().unwrap()).unwrap();
            assert!(unsafe { verify_names(context.as_ptr(), &server.certificate) }.is_err());
            Ok(())
        }).unwrap();
    }
}
