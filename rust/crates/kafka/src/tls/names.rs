use std::{ptr, sync::Arc};
use foreign_types::ForeignTypeRef;
use openssl::{ssl::SslRef, stack::StackRef, x509::{X509Name, X509NameRef, X509Ref}};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_tls::DistinguishedName;
use rtdb_types::Result;
use super::{TlsConfigError, TlsConfigResult, invalid};

pub(super) fn decode(name: &X509NameRef, budget: &Arc<MemoryBudget>) -> Result<DistinguishedName> {
    decode_typed(name, budget).map_err(TlsConfigError::into_anomaly)
}

pub(super) fn decode_typed(name: &X509NameRef, budget: &Arc<MemoryBudget>) -> TlsConfigResult<DistinguishedName> {
    let length = unsafe { openssl_sys::i2d_X509_NAME(name.as_ptr(), ptr::null_mut()) };
    let length = usize::try_from(length).map_err(|_| TlsConfigError::invalid("TLS distinguished name encoding failed"))?;
    let _charge = budget.reserve(length.checked_add(64).ok_or_else(|| TlsConfigError::invalid("TLS distinguished name exceeds allocation range"))?)
        .map_err(TlsConfigError::Other)?;
    DistinguishedName::decode(&name.to_der().map_err(TlsConfigError::invalid)?, budget)
        .map_err(TlsConfigError::from_tls)
}

pub(super) fn same(left: &X509NameRef, right: &X509NameRef, budget: &Arc<MemoryBudget>) -> Result<bool> {
    same_typed(left, right, budget).map_err(TlsConfigError::into_anomaly)
}

pub(super) fn same_typed(left: &X509NameRef, right: &X509NameRef, budget: &Arc<MemoryBudget>) -> TlsConfigResult<bool> {
    if left.as_ptr() == right.as_ptr() { return Ok(true); }
    Ok(decode_typed(left, budget)? == decode_typed(right, budget)?)
}

pub(super) struct Authorities { names: Vec<DistinguishedName>, _charge: Arc<MemoryReservation> }
impl Authorities {
    fn decode(names: Option<&StackRef<X509Name>>, budget: &Arc<MemoryBudget>) -> Result<Self> {
        let count = names.map_or(0, StackRef::len);
        let bytes = count.checked_mul(std::mem::size_of::<DistinguishedName>()).and_then(|bytes| bytes.checked_add(128))
            .ok_or_else(|| invalid("TLS issuer names exceed allocation range"))?;
        let charge = budget.reserve(bytes)?;
        let mut decoded = Vec::with_capacity(count);
        if let Some(names) = names { for name in names { decoded.push(decode(name, budget)?); } }
        Ok(Self { names: decoded, _charge: charge })
    }
}

pub(super) fn peer_authorities(ssl: &SslRef, budget: &Arc<MemoryBudget>) -> Result<Authorities> {
    let authorities = unsafe { SSL_get0_peer_CA_list(ssl.as_ptr()) };
    let authorities = if authorities.is_null() { None } else { Some(unsafe { StackRef::<X509Name>::from_ptr(authorities.cast_mut()) }) };
    Authorities::decode(authorities, budget)
}

pub(super) fn matches_issuer<'a>(chain: impl IntoIterator<Item = &'a X509Ref>, authorities: &Authorities, budget: &Arc<MemoryBudget>) -> Result<bool> {
    if authorities.names.is_empty() { return Ok(true); }
    for certificate in chain {
        let issuer = decode(certificate.issuer_name(), budget)?;
        if authorities.names.contains(&issuer) { return Ok(true); }
    }
    Ok(false)
}

unsafe extern "C" { fn SSL_get0_peer_CA_list(ssl: *const openssl_sys::SSL) -> *const openssl_sys::stack_st_X509_NAME; }

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::x509::X509NameBuilder;

    fn name(value: &str) -> X509Name {
        let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", value).unwrap(); name.build()
    }

    #[test]
    fn native_names_follow_java_unicode_equivalence_and_release_temporary_charges() {
        let memory = MemoryBudget::new(1 << 20);
        let upper = name("ÉCOLE"); let decomposed = name("e\u{301}cole"); let other = name("other");
        assert!(same(&upper, &decomposed, &memory).unwrap());
        assert!(!same(&upper, &other, &memory).unwrap());
        assert_eq!(memory.used_bytes(), 0);
        let decoded = decode(&upper, &memory).unwrap(); assert!(memory.used_bytes() > 0);
        drop(decoded); assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn name_comparison_propagates_admission_failure() {
        let memory = MemoryBudget::new(1024);
        assert_eq!(same(&name("issuer"), &name("issuer"), &memory).err().unwrap().code, "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[cfg(test)]
mod integration;
