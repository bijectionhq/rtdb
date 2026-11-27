use std::{ffi::c_void, ptr, sync::Arc};
use foreign_types::{ForeignType, ForeignTypeRef};
use openssl::{asn1::{Asn1Time, Asn1TimeRef}, ssl::SslContextBuilder, stack::{Stack, StackRef}, x509::{X509, X509PurposeId, X509Ref, X509StoreContext, X509StoreContextRef, verify::{X509VerifyFlags, X509VerifyParamRef}}};
use rtdb_columnar::MemoryReservation;
use rtdb_types::Result;
use super::super::invalid;
mod path;
mod policy;

#[cfg(test)]
use openssl::x509::store::X509StoreRef;

#[cfg(test)]
pub(crate) struct VerifiedChain { pub(crate) certificates: Vec<X509>, _charge: Arc<MemoryReservation> }

#[cfg(test)]
pub(crate) fn verify_store(store: &X509StoreRef, peer: &[X509], now: &Asn1TimeRef, charge: &Arc<MemoryReservation>) -> Result<VerifiedChain> {
    let leaf = peer.first().ok_or_else(|| invalid("TLS peer supplied no certificate"))?;
    let charge = reserve(peer.iter().map(X509::as_ref), charge)?;
    let mut untrusted = Stack::new().map_err(invalid)?;
    for certificate in peer { untrusted.push(certificate.clone()).map_err(invalid)?; }
    let mut context = X509StoreContext::new().map_err(invalid)?;
    let certificates = context.init(store, leaf, &untrusted, |context| Ok(verify_context(context, now, &charge))).map_err(invalid)??;
    Ok(VerifiedChain { certificates, _charge: charge })
}

fn verify_context(context: &mut X509StoreContextRef, now: &Asn1TimeRef, charge: &Arc<MemoryReservation>) -> Result<Vec<X509>> {
    path::verify(context, now, charge)
}

fn configure_parameters(context: &mut X509StoreContextRef, now: &Asn1TimeRef) -> Result<()> {
    let parameters = unsafe { X509_STORE_CTX_get0_param(context.as_ptr()) };
    if parameters.is_null() { return Err(invalid("TLS PKIX parameters are absent")); }
    let parameters = unsafe { X509VerifyParamRef::from_ptr_mut(parameters) };
    parameters.set_auth_level(0);
    parameters.set_purpose(X509PurposeId::ANY).map_err(invalid)?;
    parameters.clear_flags(X509VerifyFlags::NO_CHECK_TIME | X509VerifyFlags::POLICY_CHECK | X509VerifyFlags::EXPLICIT_POLICY | X509VerifyFlags::INHIBIT_ANY | X509VerifyFlags::INHIBIT_MAP | X509VerifyFlags::NOTIFY_POLICY).map_err(invalid)?;
    let elapsed = Asn1Time::from_unix(0).map_err(invalid)?.diff(now).map_err(invalid)?;
    let seconds = i64::from(elapsed.days) * 86_400 + i64::from(elapsed.secs);
    parameters.set_time(seconds.try_into().map_err(|_| invalid("TLS verification time exceeds the native range"))?);
    parameters.set_flags(X509VerifyFlags::PARTIAL_CHAIN | X509VerifyFlags::TRUSTED_FIRST).map_err(invalid)?;
    parameters.set_depth(i32::MAX);
    Ok(())
}

type VerifyCallback = unsafe extern "C" fn(i32, *mut openssl_sys::X509_STORE_CTX) -> i32;
struct DateCheck<'a> { previous: Option<VerifyCallback>, now: &'a Asn1TimeRef }
struct RestoreCallback { context: *mut openssl_sys::X509_STORE_CTX, previous: Option<VerifyCallback>, data: *mut c_void, index: i32 }
impl Drop for RestoreCallback {
    fn drop(&mut self) {
        unsafe {
            X509_STORE_CTX_set_verify_cb(self.context, self.previous);
            X509_STORE_CTX_set_ex_data(self.context, self.index, self.data);
        }
    }
}

fn date_index() -> Result<i32> {
    static INDEX: std::sync::OnceLock<std::result::Result<i32, openssl::error::ErrorStack>> = std::sync::OnceLock::new();
    INDEX.get_or_init(|| {
        let index = unsafe { openssl_sys::CRYPTO_get_ex_new_index(5, 0, ptr::null_mut(), None, None, None) };
        if index >= 0 { Ok(index) } else { Err(openssl::error::ErrorStack::get()) }
    }).as_ref().copied().map_err(invalid)
}

#[cfg(test)]
fn verify_with_dates(context: &mut X509StoreContextRef, now: &Asn1TimeRef, originals: &StackRef<X509>, self_issued: &[u8]) -> Result<bool> {
    with_dates(context, now, |context| unsafe {
        X509_STORE_CTX_verify_selected_chain(context.as_ptr(), originals.as_ptr(), self_issued.as_ptr(), self_issued.len())
    })
}

fn verify_java_with_dates(context: &mut X509StoreContextRef, now: &Asn1TimeRef, originals: &StackRef<X509>, metadata: &super::super::security::PathMetadata<'_>) -> Result<bool> {
    with_dates(context, now, |context| unsafe {
        X509_STORE_CTX_verify_selected_java_chain(context.as_ptr(), originals.as_ptr(), metadata.facts().as_ptr(), metadata.facts().len())
    })
}

fn with_dates(context: &mut X509StoreContextRef, now: &Asn1TimeRef, verify: impl FnOnce(&mut X509StoreContextRef) -> i32) -> Result<bool> {
    let index = date_index()?;
    let previous = unsafe { X509_STORE_CTX_get_verify_cb(context.as_ptr()) };
    let data = unsafe { openssl_sys::X509_STORE_CTX_get_ex_data(context.as_ptr(), index) };
    let state = DateCheck { previous, now };
    let result = unsafe { X509_STORE_CTX_set_ex_data(context.as_ptr(), index, (&state as *const DateCheck<'_>).cast_mut().cast()) };
    if result != 1 { return Err(invalid(openssl::error::ErrorStack::get())); }
    let _restore = RestoreCallback { context: context.as_ptr(), previous, data, index };
    unsafe { X509_STORE_CTX_set_verify_cb(context.as_ptr(), Some(check_date)); }
    let verified = verify(context);
    if verified < 0 { Err(invalid(openssl::error::ErrorStack::get())) } else { Ok(verified == 1) }
}

// DateCheck is borrowed only during synchronous selected-chain verification; RestoreCallback clears its pointer before the borrow ends.
unsafe extern "C" fn check_date(mut verified: i32, context: *mut openssl_sys::X509_STORE_CTX) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let Ok(index) = date_index() else { return 0; };
        let state = unsafe { openssl_sys::X509_STORE_CTX_get_ex_data(context, index) }.cast::<DateCheck<'_>>();
        if state.is_null() { return 0; }
        let state = unsafe { &*state };
        let context_ref = unsafe { X509StoreContextRef::from_ptr(context) };
        if verified == 0 && matches!(context_ref.error().as_raw(), openssl_sys::X509_V_ERR_CERT_NOT_YET_VALID | openssl_sys::X509_V_ERR_CERT_HAS_EXPIRED) {
            let depth = context_ref.error_depth() as usize;
            let trusted = unsafe { X509_STORE_CTX_get_num_untrusted(context) };
            let anchor = context_ref.chain().is_some_and(|chain| depth + 1 == chain.len() && trusted >= 0 && depth >= trusted as usize);
            let valid = context_ref.current_cert().is_some_and(|certificate| certificate.not_before().compare(state.now).is_ok_and(|order| !order.is_gt()) && certificate.not_after().compare(state.now).is_ok_and(|order| !order.is_lt()));
            if anchor || valid { verified = 1; }
        }
        state.previous.map_or(verified, |previous| unsafe { previous(verified, context) })
    })).unwrap_or(0)
}

fn reserve<'a>(certificates: impl IntoIterator<Item = &'a X509Ref>, charge: &MemoryReservation) -> Result<Arc<MemoryReservation>> {
    reserve_typed(certificates, charge).map_err(super::super::TlsConfigError::into_anomaly)
}

fn reserve_typed<'a>(certificates: impl IntoIterator<Item = &'a X509Ref>, charge: &MemoryReservation) -> super::super::TlsConfigResult<Arc<MemoryReservation>> {
    let bound = certificates.into_iter().try_fold(65_536usize, |bytes, certificate| {
        let length = unsafe { openssl_sys::i2d_X509(certificate.as_ptr(), ptr::null_mut()) };
        usize::try_from(length).ok().and_then(|length| length.checked_mul(32)).and_then(|length| bytes.checked_add(length))
            .ok_or_else(|| super::super::TlsConfigError::invalid("TLS peer chain exceeds allocation range"))
    })?;
    charge.budget().reserve(bound).map_err(super::super::TlsConfigError::Other)
}

// The installed verifier borrows the reservation until this context and its SSL objects are destroyed.
pub(crate) unsafe fn configure_native(context: &mut SslContextBuilder, charge: &Arc<MemoryReservation>) -> Result<()> {
    super::native::charges_index()?;
    unsafe { SSL_CTX_set_cert_verify_callback(context.as_ptr(), Some(verify_native), Arc::as_ptr(charge).cast_mut().cast()); }
    Ok(())
}

unsafe extern "C" fn verify_native(context: *mut openssl_sys::X509_STORE_CTX, opaque: *mut c_void) -> i32 {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
        let charge = unsafe { &*opaque.cast::<MemoryReservation>() };
        let leaf = unsafe { X509_STORE_CTX_get0_cert(context) };
        if leaf.is_null() { return Err(invalid("TLS peer supplied no certificate")); }
        let leaf = unsafe { X509Ref::from_ptr(leaf) };
        let untrusted = unsafe { X509_STORE_CTX_get0_untrusted(context) };
        let untrusted = if untrusted.is_null() { None } else { Some(unsafe { StackRef::<X509>::from_ptr(untrusted) }) };
        let charge = reserve(std::iter::once(leaf).chain(untrusted.into_iter().flatten()), charge)?;
        let now = Asn1Time::days_from_now(0).map_err(invalid)?;
        let verified = verify_context(unsafe { X509StoreContextRef::from_ptr_mut(context) }, &now, &charge)?;
        unsafe { super::native::verify_names(context, &verified[0])?; }
        unsafe { super::native::retain_chain(context, verified.iter().map(X509::as_ref), charge)?; }
        Ok(())
    }));
    match result {
        Ok(Ok(())) => 1,
        _ => { unsafe { openssl_sys::X509_STORE_CTX_set_error(context, openssl_sys::X509_V_ERR_APPLICATION_VERIFICATION); } 0 },
    }
}

unsafe extern "C" {
    #[cfg(test)]
    fn X509_STORE_CTX_verify_selected_chain(context: *mut openssl_sys::X509_STORE_CTX, chain: *mut openssl_sys::stack_st_X509, self_issued: *const u8, count: usize) -> i32;
    fn X509_STORE_CTX_verify_selected_java_chain(context: *mut openssl_sys::X509_STORE_CTX, chain: *mut openssl_sys::stack_st_X509, facts: *const super::super::security::JavaCertificateFacts, count: usize) -> i32;
    fn SSL_CTX_set_cert_verify_callback(context: *mut openssl_sys::SSL_CTX, callback: Option<unsafe extern "C" fn(*mut openssl_sys::X509_STORE_CTX, *mut c_void) -> i32>, opaque: *mut c_void);
    fn X509_STORE_CTX_get0_cert(context: *const openssl_sys::X509_STORE_CTX) -> *mut openssl_sys::X509;
    fn X509_STORE_CTX_get0_untrusted(context: *const openssl_sys::X509_STORE_CTX) -> *mut openssl_sys::stack_st_X509;
    fn X509_STORE_CTX_get0_param(context: *const openssl_sys::X509_STORE_CTX) -> *mut openssl_sys::X509_VERIFY_PARAM;
    fn X509_STORE_CTX_get1_certs(context: *mut openssl_sys::X509_STORE_CTX, name: *const openssl_sys::X509_NAME) -> *mut openssl_sys::stack_st_X509;
    fn X509_STORE_CTX_get_num_untrusted(context: *const openssl_sys::X509_STORE_CTX) -> i32;
    fn X509_STORE_CTX_get_verify_cb(context: *const openssl_sys::X509_STORE_CTX) -> Option<VerifyCallback>;
    fn X509_STORE_CTX_set_verify_cb(context: *mut openssl_sys::X509_STORE_CTX, callback: Option<VerifyCallback>);
    fn X509_STORE_CTX_set_ex_data(context: *mut openssl_sys::X509_STORE_CTX, index: i32, data: *mut c_void) -> i32;
}

#[cfg(test)]
mod tests;
