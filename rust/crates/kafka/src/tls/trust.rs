use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::MemoryReservation;
use rtdb_tls::{KeyStore, StoreFormat, StoreUse};
use rtdb_types::Result;
use super::overflow;

const CACERTS: &[u8] = include_bytes!("cacerts/temurin-25.0.1.p12");

pub(super) fn native_override(options: &BTreeMap<String, String>) -> bool {
    options.contains_key("ssl.ca.location") || options.contains_key("ssl.ca.pem")
}

pub(super) fn defaults(reserve: &mut impl FnMut(usize) -> Result<Arc<MemoryReservation>>) -> Result<KeyStore> {
    decode(reserve(allocation_bound()?)?)
}

pub(crate) fn allocation_bound() -> Result<usize> {
    KeyStore::allocation_bound_with_passwords(CACERTS.len(), Some("changeit"), None)?
        .checked_add(CACERTS.len().checked_mul(32).ok_or_else(overflow)?).ok_or_else(overflow)
}

pub(crate) fn decode(reservation: Arc<MemoryReservation>) -> Result<KeyStore> {
    decode_typed(reservation).map_err(super::TlsConfigError::into_anomaly)
}

pub(crate) fn decode_typed(reservation: Arc<MemoryReservation>) -> super::TlsConfigResult<KeyStore> {
    if reservation.bytes() < allocation_bound().map_err(super::TlsConfigError::from_invalid_anomaly)? {
        return Err(super::TlsConfigError::invalid("Default trust store reservation is too small"));
    }
    KeyStore::decode(CACERTS, StoreFormat::Java, StoreUse::Trust, Some("changeit"), None, reservation).map_err(super::TlsConfigError::Other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_columnar::MemoryBudget;
    use openssl::hash::{MessageDigest, hash};

    #[test]
    fn pinned_default_roots_are_decoded_under_the_retained_reservation() {
        let digest = hash(MessageDigest::sha256(), CACERTS).unwrap().iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        assert_eq!(digest, "7a33aaf51d8460f3e57956e90a8fe179188e9c82578555020881f5cd48c4274b");
        let memory = MemoryBudget::new(32 << 20);
        let roots = defaults(&mut |bytes| memory.reserve(bytes)).unwrap();
        assert!(roots.identities().is_empty());
        assert_eq!(roots.trusted_certificates().len(), 146);
        assert!(memory.used_bytes() > CACERTS.len());
        let retained = roots.reservation();
        drop(roots);
        assert!(memory.used_bytes() > 0);
        drop(retained);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn default_root_allocation_is_admitted_before_decoding() {
        let memory = MemoryBudget::new(0);
        let failure = defaults(&mut |bytes| memory.reserve(bytes)).err().unwrap();
        assert_eq!(failure.code, "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), 0);
    }
}
