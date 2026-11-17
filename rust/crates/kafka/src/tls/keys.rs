use std::{cmp::Ordering, ptr};
use foreign_types::{ForeignType, ForeignTypeRef};
use openssl::{asn1::{Asn1BitString, Asn1Object, Asn1TimeRef}, nid::Nid, pkey::Id, stack::Stack, x509::X509Ref};
use rtdb_tls::Identity;
use rtdb_types::Result;
use super::invalid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Algorithm { SunX509, NewSunX509 }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyType { Rsa, RsaPss, Dsa, Ec, EdDsa, Other }
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Status { Valid, Expired, ExtensionMismatch }
pub(crate) struct KeyManager { algorithm: Algorithm, order: Vec<usize> }

impl Algorithm {
    pub(crate) fn parse(name: Option<&str>) -> Result<Self> {
        match name.unwrap_or("SunX509").to_ascii_uppercase().as_str() {
            "SUNX509" => Ok(Self::SunX509),
            "NEWSUNX509" | "PKIX" => Ok(Self::NewSunX509),
            _ => Err(invalid("Kafka key-manager algorithm requires a native adapter")),
        }
    }
}

impl KeyManager {
    pub(crate) fn constrains_certificates(&self) -> bool { self.algorithm == Algorithm::NewSunX509 }

    pub(crate) fn new(algorithm: Algorithm, identities: &[Identity]) -> Self {
        let aliases = identities.iter().map(|identity| identity.alias().unwrap_or(&[])).collect::<Vec<_>>();
        let order = if algorithm == Algorithm::SunX509 {
            rtdb_types::java_hash_map_order(16, aliases.iter(), |alias| alias.iter().fold(0i32, |hash, unit| hash.wrapping_mul(31).wrapping_add(i32::from(*unit))), |left, right| left.cmp(right)).expect("constant Java map capacity")
        } else { (0..identities.len()).collect() };
        Self { algorithm, order }
    }

    // OpenJDK 25.0.1 SunX509KeyManagerImpl.chooseClientAlias and X509KeyManagerImpl.chooseAlias/getAliases.
    pub(crate) fn choose(&self, types: &[KeyType], compatible: impl Fn(usize, KeyType) -> bool, status: impl Fn(usize) -> Status) -> Option<usize> {
        if self.algorithm == Algorithm::SunX509 {
            return types.iter().find_map(|kind| self.order.iter().copied().find(|index| compatible(*index, *kind)));
        }
        let mut valid = None;
        let mut fallback = None;
        for index in self.order.iter().copied() {
            let Some(key_index) = types.iter().position(|kind| compatible(index, *kind)) else { continue; };
            let status = status(index);
            if status == Status::Valid {
                if key_index == 0 { return Some(index); }
                valid.get_or_insert(index);
            } else if fallback.is_none_or(|(previous, previous_key, _)| (status, key_index) < (previous, previous_key)) {
                fallback = Some((status, key_index, index));
            }
        }
        valid.or_else(|| fallback.map(|(_, _, index)| index))
    }
}

impl KeyType {
    pub(crate) fn of(certificate: &X509Ref) -> Result<Self> {
        Ok(match certificate.public_key().map_err(invalid)?.id() {
            Id::RSA => Self::Rsa, Id::RSA_PSS => Self::RsaPss, Id::DSA => Self::Dsa, Id::EC => Self::Ec,
            Id::ED25519 | Id::ED448 => Self::EdDsa, _ => Self::Other,
        })
    }
    pub(crate) fn signature(scheme: u16) -> Option<Self> {
        match scheme {
            0x0804..=0x0806 | 0x0201 | 0x0301 | 0x0401 | 0x0501 | 0x0601 => Some(Self::Rsa),
            0x0809..=0x080b => Some(Self::RsaPss),
            0x0202 | 0x0302 | 0x0402 => Some(Self::Dsa),
            0x0203 | 0x0303 | 0x0403 | 0x0503 | 0x0603 => Some(Self::Ec),
            0x0807 | 0x0808 => Some(Self::EdDsa),
            _ => None,
        }
    }
}

pub(crate) fn requested_types(schemes: impl IntoIterator<Item = u16>) -> Vec<KeyType> {
    let mut types = Vec::new();
    for kind in schemes.into_iter().filter_map(KeyType::signature) { if !types.contains(&kind) { types.push(kind); } }
    types
}

pub(crate) fn client_status(certificate: &X509Ref, now: &Asn1TimeRef) -> Status {
    if !client_extensions(certificate) { return Status::ExtensionMismatch; }
    if !certificate.not_before().compare(now).is_ok_and(|order| order != Ordering::Greater)
        || !certificate.not_after().compare(now).is_ok_and(|order| order != Ordering::Less) { return Status::Expired; }
    Status::Valid
}

fn client_extensions(certificate: &X509Ref) -> bool {
    let mut critical = -1;
    let usage = unsafe { openssl_sys::X509_get_ext_d2i(certificate.as_ptr(), Nid::EXT_KEY_USAGE.as_raw(), &mut critical, ptr::null_mut()) };
    if usage.is_null() {
        if critical != -1 { return false; }
    } else {
        let usage = unsafe { Stack::<Asn1Object>::from_ptr(usage.cast()) };
        if !usage.iter().any(|oid| matches!(oid.nid(), Nid::CLIENT_AUTH | Nid::ANYEXTENDEDKEYUSAGE)) { return false; }
    }
    let usage = unsafe { openssl_sys::X509_get_ext_d2i(certificate.as_ptr(), Nid::KEY_USAGE.as_raw(), &mut critical, ptr::null_mut()) };
    if usage.is_null() { return critical == -1; }
    let usage = unsafe { Asn1BitString::from_ptr(usage.cast()) };
    let bits = usage.as_slice().first().copied().unwrap_or(0);
    match certificate.public_key().map(|key| key.id()) {
        Ok(Id::RSA | Id::DSA | Id::EC) => bits & 0x80 != 0,
        Ok(Id::DH) => bits & 0x08 != 0,
        Ok(_) => true,
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests;
