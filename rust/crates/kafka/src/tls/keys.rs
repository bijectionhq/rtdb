use std::{cmp::Ordering, ptr, sync::Arc};
use foreign_types::ForeignTypeRef;
use openssl::{asn1::Asn1TimeRef, pkey::Id, x509::X509Ref};
use rtdb_columnar::MemoryBudget;
use rtdb_tls::{CertificateExtendedKeyUsage, CertificateKeyUsage, Identity};
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
        Self::from_aliases(algorithm, &aliases)
    }

    pub(crate) fn from_aliases(algorithm: Algorithm, aliases: &[&[u16]]) -> Self {
        let order = if algorithm == Algorithm::SunX509 {
            rtdb_types::java_hash_map_order(16, aliases.iter(), |alias| alias.iter().fold(0i32, |hash, unit| hash.wrapping_mul(31).wrapping_add(i32::from(*unit))), |left, right| left.cmp(right)).expect("constant Java map capacity")
        } else { (0..aliases.len()).collect() };
        Self { algorithm, order }
    }

    #[cfg(test)]
    pub(crate) fn choose(&self, types: &[KeyType], compatible: impl Fn(usize, KeyType) -> bool, status: impl Fn(usize) -> Status) -> Option<usize> {
        self.try_choose(types, compatible, |index| Ok(status(index))).expect("infallible client status")
    }

    // OpenJDK 25.0.1 SunX509KeyManagerImpl.chooseClientAlias and X509KeyManagerImpl.chooseAlias/getAliases.
    pub(crate) fn try_choose(&self, types: &[KeyType], compatible: impl Fn(usize, KeyType) -> bool, status: impl Fn(usize) -> Result<Status>) -> Result<Option<usize>> {
        if self.algorithm == Algorithm::SunX509 {
            return Ok(types.iter().find_map(|kind| self.order.iter().copied().find(|index| compatible(*index, *kind))));
        }
        let mut valid = None;
        let mut fallback = None;
        for index in self.order.iter().copied() {
            let Some(key_index) = types.iter().position(|kind| compatible(index, *kind)) else { continue; };
            let status = status(index)?;
            if status == Status::Valid {
                if key_index == 0 { return Ok(Some(index)); }
                valid.get_or_insert(index);
            } else if fallback.is_none_or(|(previous, previous_key, _)| (status, key_index) < (previous, previous_key)) {
                fallback = Some((status, key_index, index));
            }
        }
        Ok(valid.or_else(|| fallback.map(|(_, _, index)| index)))
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

pub(crate) fn client_status(certificate: &X509Ref, now: &Asn1TimeRef, memory: &Arc<MemoryBudget>) -> Result<Status> {
    Ok(status_with_usage(certificate, now, client_extensions(certificate, memory)?))
}

pub(crate) fn status_with_usage(certificate: &X509Ref, now: &Asn1TimeRef, usage_eligible: bool) -> Status {
    if !usage_eligible { return Status::ExtensionMismatch; }
    if !certificate.not_before().compare(now).is_ok_and(|order| order != Ordering::Greater)
        || !certificate.not_after().compare(now).is_ok_and(|order| order != Ordering::Less) { return Status::Expired; }
    Status::Valid
}

pub(crate) fn client_extensions(certificate: &X509Ref, memory: &Arc<MemoryBudget>) -> Result<bool> {
    let length = unsafe { openssl_sys::i2d_X509(certificate.as_ptr(), ptr::null_mut()) };
    let bound = usize::try_from(length).ok().filter(|length| *length > 0).and_then(|length| length.checked_add(64))
        .ok_or_else(|| invalid("TLS client certificate encoding exceeds allocation range"))?;
    let _encoding = memory.reserve(bound)?;
    let encoded = certificate.to_der().map_err(invalid)?;
    match CertificateExtendedKeyUsage::decode(&encoded, memory).and_then(|usage| usage.permits_client_authentication()) {
        Ok(true) => {},
        Ok(false) => return Ok(false),
        Err(rtdb_tls::TlsError::InvalidKeystore(_)) => return Ok(false),
        Err(error) => return Err(error.into_anomaly()),
    }
    let bits = match CertificateKeyUsage::decode(&encoded, memory) {
        Ok(usage) => usage.permitted_bits(),
        Err(rtdb_tls::TlsError::InvalidKeystore(_)) => return Ok(false),
        Err(error) => return Err(error.into_anomaly()),
    };
    if bits & 0x88 == 0x88 { return Ok(true); }
    Ok(match certificate.public_key().map(|key| key.id()) {
        Ok(Id::RSA | Id::DSA | Id::EC) => bits & 0x80 != 0,
        Ok(Id::DH | Id::DHX) => bits & 0x08 != 0,
        Ok(_) => true,
        Err(_) => false,
    })
}

#[cfg(test)]
mod tests;
