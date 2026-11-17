use std::{cmp::Ordering, collections::BTreeSet, ptr, sync::Arc};
use foreign_types::{ForeignType, ForeignTypeRef};
use openssl::{asn1::{Asn1BitString, Asn1Object, Asn1TimeRef}, nid::Nid, stack::Stack, x509::{X509, X509NameRef, X509Ref}};
use rtdb_columnar::MemoryReservation;
use rtdb_types::Result;
use super::invalid;
mod native;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Algorithm { Pkix, SunX509 }
pub(crate) struct SimpleTrust { anchors: Vec<X509>, _charge: Arc<MemoryReservation> }

impl Algorithm {
    pub(crate) fn parse(name: Option<&str>) -> Result<Self> {
        match name.unwrap_or("PKIX").to_ascii_uppercase().as_str() {
            "PKIX" | "SUNPKIX" | "X509" | "X.509" => Ok(Self::Pkix),
            "SUNX509" => Ok(Self::SunX509),
            _ => Err(invalid("TLS trust-manager algorithm requires a native adapter")),
        }
    }
}

impl SimpleTrust {
    pub(crate) fn new(anchors: Vec<X509>, charge: Arc<MemoryReservation>) -> Result<Self> {
        let encoded = anchors.iter().map(|certificate| certificate.to_der().map_err(invalid)).collect::<Result<Vec<_>>>()?;
        let mut seen = BTreeSet::new();
        let indices = encoded.iter().enumerate().filter_map(|(index, bytes)| seen.insert(bytes.as_slice()).then_some(index)).collect::<Vec<_>>();
        let order = rtdb_types::java_hash_map_order(16, indices.iter(), |index| encoded[*index].iter().fold(1i32, |hash, byte| hash.wrapping_mul(31).wrapping_add(i32::from(*byte as i8))),
            |left, right| (anchors[*left].as_ptr() as usize).cmp(&(anchors[*right].as_ptr() as usize))).expect("constant Java map capacity");
        let mut anchors = anchors.into_iter().map(Some).collect::<Vec<_>>();
        let anchors = order.into_iter().map(|index| anchors[indices[index]].take().expect("deduplicated trust anchor")).collect();
        Ok(Self { anchors, _charge: charge })
    }

    // OpenJDK 25.0.1 SimpleValidator validates the supplied order and replaces reissued trust anchors.
    pub(crate) fn verify<'a>(&'a self, peer: &'a [X509], now: &Asn1TimeRef, constraints: impl FnOnce(&[&X509Ref]) -> Result<()>) -> Result<Vec<&'a X509Ref>> {
        if peer.is_empty() { return Err(invalid("TLS peer supplied no certificate chain")); }
        let mut chain = Vec::with_capacity(peer.len() + 1);
        for certificate in peer {
            if let Some(anchor) = self.trusted(certificate)? {
                chain.push(anchor);
                return self.check(chain, now, constraints);
            }
            chain.push(certificate.as_ref());
        }
        let last = *chain.last().expect("nonempty peer chain");
        let mut candidates = self.anchors.iter().filter(|anchor| same_name(anchor.subject_name(), last.issuer_name()));
        let mut anchor = candidates.next().ok_or_else(|| invalid("TLS certificate chain has no trust anchor"))?;
        if let Some(identifier) = last.authority_key_id() {
            if !anchor.subject_key_id().is_some_and(|subject| subject.as_slice() == identifier.as_slice()) {
                if let Some(matched) = candidates.find(|candidate| candidate.subject_key_id().is_some_and(|subject| subject.as_slice() == identifier.as_slice())) { anchor = matched; }
            }
        }
        chain.push(anchor.as_ref());
        self.check(chain, now, constraints)
    }

    fn trusted(&self, certificate: &X509Ref) -> Result<Option<&X509Ref>> {
        for anchor in &self.anchors {
            if same_name(anchor.subject_name(), certificate.subject_name()) && same_name(anchor.issuer_name(), certificate.issuer_name())
                && super::public_key::same(anchor, certificate)? { return Ok(Some(anchor)); }
        }
        Ok(None)
    }

    fn check<'a>(&self, chain: Vec<&'a X509Ref>, now: &Asn1TimeRef, constraints: impl FnOnce(&[&X509Ref]) -> Result<()>) -> Result<Vec<&'a X509Ref>> {
        constraints(&chain)?;
        let mut maximum_path = chain.len() - 1;
        for index in (0..chain.len() - 1).rev() {
            let certificate = chain[index]; let issuer = chain[index + 1];
            if certificate.not_before().compare(now).map_err(invalid)? == Ordering::Greater || certificate.not_after().compare(now).map_err(invalid)? == Ordering::Less {
                return Err(invalid("TLS certificate is outside its validity interval"));
            }
            if !same_name(certificate.issuer_name(), issuer.subject_name()) { return Err(invalid("TLS certificate issuer does not match the supplied chain")); }
            if !certificate.verify(&issuer.public_key().map_err(invalid)?).map_err(invalid)? { return Err(invalid("TLS certificate signature is invalid")); }
            if index != 0 { maximum_path = ca_extensions(certificate, maximum_path)?; }
        }
        if chain.len() > 1 { server_extensions(chain[0])?; }
        Ok(chain)
    }
}

fn same_name(left: &X509NameRef, right: &X509NameRef) -> bool { left.try_cmp(right).is_ok_and(|order| order == Ordering::Equal) }

fn ca_extensions(certificate: &X509Ref, mut maximum_path: usize) -> Result<usize> {
    let flags = unsafe { openssl_sys::X509_get_extension_flags(certificate.as_ptr()) };
    if flags & (openssl_sys::EXFLAG_BCONS | openssl_sys::EXFLAG_CA) != (openssl_sys::EXFLAG_BCONS | openssl_sys::EXFLAG_CA) {
        return Err(invalid("TLS intermediate certificate is not a certificate authority"));
    }
    if !same_name(certificate.subject_name(), certificate.issuer_name()) { maximum_path = maximum_path.checked_sub(1).ok_or_else(|| invalid("TLS certificate path exceeds its length constraint"))?; }
    let path = unsafe { openssl_sys::X509_get_pathlen(certificate.as_ptr()) };
    if path >= 0 { maximum_path = maximum_path.min(path as usize); }
    if !bit_extension(certificate, Nid::KEY_USAGE, 0x04)? { return Err(invalid("TLS intermediate key usage excludes certificate signing")); }
    if !bit_extension(certificate, Nid::NETSCAPE_CERT_TYPE, 0x04)? { return Err(invalid("TLS intermediate Netscape certificate type excludes SSL authorities")); }
    critical_extensions(certificate, &[Nid::BASIC_CONSTRAINTS, Nid::KEY_USAGE, Nid::EXT_KEY_USAGE, Nid::NETSCAPE_CERT_TYPE])?;
    Ok(maximum_path)
}

fn server_extensions(certificate: &X509Ref) -> Result<()> {
    if !bit_extension(certificate, Nid::KEY_USAGE, 0x80)? { return Err(invalid("TLS server key usage excludes digital signatures")); }
    if !bit_extension(certificate, Nid::NETSCAPE_CERT_TYPE, 0x40)? { return Err(invalid("TLS server Netscape certificate type excludes SSL servers")); }
    let mut critical = -1;
    let usage = unsafe { openssl_sys::X509_get_ext_d2i(certificate.as_ptr(), Nid::EXT_KEY_USAGE.as_raw(), &mut critical, ptr::null_mut()) };
    if usage.is_null() {
        if critical != -1 { return Err(invalid("TLS server extended key usage is malformed")); }
    } else {
        let usage = unsafe { Stack::<Asn1Object>::from_ptr(usage.cast()) };
        if !usage.iter().any(|oid| matches!(oid.nid(), Nid::SERVER_AUTH | Nid::ANYEXTENDEDKEYUSAGE | Nid::MS_SGC | Nid::NS_SGC)) {
            return Err(invalid("TLS server extended key usage excludes server authentication"));
        }
    }
    critical_extensions(certificate, &[Nid::BASIC_CONSTRAINTS, Nid::SUBJECT_ALT_NAME, Nid::KEY_USAGE, Nid::EXT_KEY_USAGE, Nid::NETSCAPE_CERT_TYPE])
}

fn bit_extension(certificate: &X509Ref, extension: Nid, mask: u8) -> Result<bool> {
    let mut critical = -1;
    let value = unsafe { openssl_sys::X509_get_ext_d2i(certificate.as_ptr(), extension.as_raw(), &mut critical, ptr::null_mut()) };
    if value.is_null() { return if critical == -1 { Ok(true) } else { Err(invalid("TLS certificate bit-string extension is malformed")) }; }
    let value = unsafe { Asn1BitString::from_ptr(value.cast()) };
    Ok(value.as_slice().first().is_some_and(|bits| bits & mask != 0))
}

fn critical_extensions(certificate: &X509Ref, recognized: &[Nid]) -> Result<()> {
    let count = unsafe { openssl_sys::X509_get_ext_count(certificate.as_ptr()) };
    for index in 0..count {
        let extension = unsafe { openssl_sys::X509_get_ext(certificate.as_ptr(), index) };
        if extension.is_null() { return Err(invalid("TLS certificate extension is absent")); }
        if unsafe { openssl_sys::X509_EXTENSION_get_critical(extension) } == 0 { continue; }
        let oid = unsafe { openssl_sys::X509_EXTENSION_get_object(extension) };
        if oid.is_null() || !recognized.contains(&Nid::from_raw(unsafe { openssl_sys::OBJ_obj2nid(oid) })) { return Err(invalid("TLS certificate has an unsupported critical extension")); }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
