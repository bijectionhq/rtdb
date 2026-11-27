use std::{collections::BTreeMap, sync::Arc};
use openssl::{pkey::{Id, PKey}, x509::X509};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, Result};
use zeroize::Zeroizing;
use super::{JksEntry, KeyStore, TlsError, TlsResult, invalid, x500::lowercase_utf16};

/// Immutable Java keystore metadata and protected private keys for selection before key recovery.
/// Loading completes on the caller and retains its supplied reservation; no key password is retained.
pub struct ProtectedKeyStore { identities: Vec<ProtectedIdentity>, _reservations: Vec<Arc<MemoryReservation>> }

/// One enumerated Java alias and its resolved private-key entry with a nonempty certificate chain.
/// Several aliases may share an entry; this borrowed value remains valid until its store drops.
pub struct ProtectedIdentity { alias: Vec<u16>, entry: Arc<Entry> }

struct Entry { key: ProtectedKey, certificates: Vec<Vec<u8>> }
enum ProtectedKey { Jks(Zeroizing<Vec<u8>>), Pkcs12(Zeroizing<Vec<u8>>) }

/// Independently reserved PKCS8 key material, zeroized when this result drops.
/// The result may outlive its source store; copies require their own allocation reservation.
pub struct RecoveredKey { private_key: Zeroizing<Vec<u8>>, reservation: Arc<MemoryReservation> }

impl ProtectedKeyStore {
    /// Initial reservation covering store decoding and retained protected entries.
    /// Expanded PKCS12 chains and Java distinguished-name comparisons reserve additional memory from the same budget.
    pub fn allocation_bound(encoded_bytes: usize, store_password: Option<&str>) -> Result<usize> {
        KeyStore::allocation_bound_with_passwords(encoded_bytes, store_password, None)?.checked_add(encoded_bytes.checked_mul(48)
            .ok_or_else(|| invalid("Protected keystore allocation exceeds range"))?).ok_or_else(|| invalid("Protected keystore allocation exceeds range"))
    }

    /// Loads JKS or PKCS12 without decrypting private-key entries; PEM uses the eager `KeyStore` API.
    /// Integrity, encrypted certificate sections and certificate framing are checked during loading.
    /// A missing store password skips integrity and encrypted PKCS12 sections; an empty identity set is valid.
    /// The supplied reservation must cover `allocation_bound` before this call.
    pub fn decode(bytes: &[u8], store_password: Option<&str>, reservation: Arc<MemoryReservation>) -> Result<Self> {
        if reservation.bytes() < Self::allocation_bound(bytes.len(), store_password)? { return Err(invalid("Protected keystore decode reservation is too small")); }
        let mut reservations = vec![Arc::clone(&reservation)];
        let identities = if bytes.starts_with(&0xfeedfeedu32.to_be_bytes()) { jks(bytes, store_password, reservation.budget())? }
            else { super::pkcs12::decode_protected(bytes, store_password, reservation.budget(), &mut reservations)? };
        Ok(Self { identities, _reservations: reservations })
    }

    /// Selectable aliases in Java enumeration order after provider alias lookup and chain availability checks.
    /// JKS retains raw enumeration spellings while resolving them through Java locale-English lowercase.
    /// PKCS12 enumerates its normalized alias keys.
    pub fn identities(&self) -> &[ProtectedIdentity] { &self.identities }

}

impl ProtectedIdentity {
    pub(super) fn pkcs12(alias: Vec<u16>, protected: Zeroizing<Vec<u8>>, certificates: Vec<Vec<u8>>) -> Self {
        Self { alias, entry: Arc::new(Entry { key: ProtectedKey::Pkcs12(protected), certificates }) }
    }

    /// Alias supplied to Java's key-entry lookup, represented as UTF-16 including isolated surrogates.
    pub fn alias(&self) -> &[u16] { &self.alias }

    /// Leaf-first certificate chain available before private-key recovery.
    pub fn certificates(&self) -> &[Vec<u8>] { &self.entry.certificates }

    /// Recovers this selected entry with the supplied key password under a separate retained reservation.
    /// Password, protected-key framing, unsupported algorithms and Java private-key-entry algorithm mismatch errors occur here.
    /// Java compares key algorithms rather than public-key values; handshake proof of possession remains the transport's responsibility.
    /// Recovery does not cache success or failure and does not try other aliases.
    pub fn recover(&self, password: Option<&str>, memory: &Arc<MemoryBudget>) -> TlsResult<RecoveredKey> {
        let password = password.ok_or_else(|| TlsError::invalid_keystore("Java private-key recovery requires a password"))?;
        let protected = match &self.entry.key { ProtectedKey::Jks(bytes) | ProtectedKey::Pkcs12(bytes) => bytes };
        let leaf = self.entry.certificates.first().ok_or_else(|| TlsError::invalid_keystore("Protected private key has no certificate"))?;
        let bytes = protected.len().checked_add(leaf.len()).ok_or_else(|| TlsError::invalid_keystore("Private-key recovery allocation exceeds range"))?;
        let bound = KeyStore::allocation_bound_with_passwords(bytes, None, Some(password)).map_err(TlsError::from_invalid_anomaly)?;
        let reservation = memory.reserve(bound).map_err(TlsError::Other)?;
        let private_key = match &self.entry.key {
            ProtectedKey::Jks(protected) => {
                let password = Zeroizing::new(password.encode_utf16().flat_map(u16::to_be_bytes).collect::<Vec<_>>());
                super::decrypt_jks_key(protected, &password).map_err(TlsError::from_invalid_anomaly)?
            }
            ProtectedKey::Pkcs12(protected) => super::pkcs12::recover_key(protected, Some(password)).map_err(TlsError::from_invalid_anomaly)?,
        };
        let key = PKey::private_key_from_pkcs8(&private_key).map_err(TlsError::invalid_keystore)?;
        let certificate = X509::from_der(leaf).map_err(TlsError::invalid_keystore)?;
        let algorithm = java_algorithm(key.id()).ok_or_else(|| TlsError::invalid_keystore("Unsupported Java private-key algorithm"))?;
        if java_algorithm(certificate.public_key().map_err(TlsError::invalid_keystore)?.id()) != Some(algorithm) {
            return Err(TlsError::invalid_keystore("Private-key algorithm differs from its leaf certificate"));
        }
        Ok(RecoveredKey { private_key, reservation })
    }
}

impl RecoveredKey {
    /// Borrows the validated PKCS8 encoding until this result drops.
    pub fn private_key_pkcs8(&self) -> &[u8] { &self.private_key }

    /// Retain this charge with native key objects that copy the recovered material.
    pub fn reservation(&self) -> Arc<MemoryReservation> { Arc::clone(&self.reservation) }
}

fn jks(bytes: &[u8], password: Option<&str>, memory: &Arc<MemoryBudget>) -> Result<Vec<ProtectedIdentity>> {
    let entries = super::jks_entries(bytes, password, |certificate| super::CertificateIdentifiers::decode(certificate, memory).map(|_| ()).map_err(Anomaly::from))?.into_enumeration().collect::<Vec<_>>();
    let mut material = BTreeMap::new();
    for (alias, entry) in &entries {
        if let JksEntry::Key { protected, certificates } = entry {
            if !certificates.is_empty() {
                material.insert(alias.as_slice(), Arc::new(Entry {
                    key: ProtectedKey::Jks(Zeroizing::new(protected.to_vec())),
                    certificates: certificates.iter().map(|certificate| certificate.to_vec()).collect(),
                }));
            }
        }
    }
    let mut identities = Vec::new();
    for (alias, _) in &entries {
        if let Some(entry) = material.get(lowercase_utf16(alias).as_slice()) {
            identities.push(ProtectedIdentity { alias: alias.clone(), entry: Arc::clone(entry) });
        }
    }
    Ok(identities)
}

fn java_algorithm(id: Id) -> Option<u8> {
    Some(match id {
        Id::RSA => 0, Id::RSA_PSS => 1, Id::DSA => 2, Id::DH | Id::DHX => 3,
        Id::EC => 4, Id::ED25519 | Id::ED448 => 5, Id::X25519 | Id::X448 => 6,
        _ => return None,
    })
}
