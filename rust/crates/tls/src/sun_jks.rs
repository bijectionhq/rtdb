use std::sync::Arc;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::Result;
use zeroize::Zeroizing;
use super::{JksEntry, KeyStore, invalid, x500::lowercase_utf16};
mod recovery;

/// Immutable eager SunX509 JKS credentials, including mismatched private keys and leaf certificates.
/// Aliases share recovered entries; loading completes on the caller without retaining passwords.
pub struct SunX509JksStore { identities: Vec<SunX509JksIdentity>, _reservation: Arc<MemoryReservation> }

/// One raw enumerated alias and its resolved entry with a nonempty leaf-first certificate chain.
/// Borrowed material remains valid until the store drops; copies need separate admission.
pub struct SunX509JksIdentity { alias: Vec<u16>, entry: Arc<RecoveredEntry> }

struct RecoveredEntry { private_key: Zeroizing<Vec<u8>>, certificates: Vec<Vec<u8>>, _reservation: Arc<MemoryReservation> }
enum Entry<'a> { Pending(JksEntry<'a>), Recovered { protected: &'a [u8], entry: Arc<RecoveredEntry> } }

impl SunX509JksStore {
    /// Initial admission for JKS input, alias lookup, certificate parsing and temporary password encodings.
    /// Each key recovery reserves additional scratch and retained entry bytes from the same budget.
    pub fn allocation_bound(encoded_bytes: usize, store_password: Option<&str>, key_password: Option<&str>) -> Result<usize> {
        KeyStore::allocation_bound_with_passwords(encoded_bytes, store_password, key_password)?.checked_add(encoded_bytes.checked_mul(48).ok_or_else(overflow)?).ok_or_else(overflow)
    }

    /// Loads JKS using SunX509's alias lookup and eager recovery order; an empty credential set is valid.
    ///
    /// - A missing store password skips integrity checks; framing, recovery and admission errors abort loading.
    /// - The effective key password is supplied explicitly; absence fails only when a key entry resolves.
    /// - Recovery precedes empty-chain filtering and does not compare key/certificate algorithms or values.
    /// - Java PKCS8 framing is checked without requiring native provider acceptance.
    /// - AlgorithmParameters validation and KeyFactory re-encoding remain outside this decoder.
    /// - The supplied reservation must cover `allocation_bound` before allocating input or calling this method.
    pub fn decode(bytes: &[u8], store_password: Option<&str>, key_password: Option<&str>, reservation: Arc<MemoryReservation>) -> Result<Self> {
        if reservation.bytes() < Self::allocation_bound(bytes.len(), store_password, key_password)? { return Err(invalid("SunX509 JKS decode reservation is too small")); }
        let mut entries = super::jks_entries(bytes, store_password, |_| Ok(()))?.map(Entry::Pending);
        let aliases = entries.aliases().map(<[u16]>::to_vec).collect::<Vec<_>>();
        let password = if let Some(password) = key_password {
            let mut encoded = Zeroizing::new(Vec::with_capacity(password.len().checked_mul(2).ok_or_else(overflow)?));
            encoded.extend(password.encode_utf16().flat_map(u16::to_be_bytes));
            Some(encoded)
        } else { None };
        let mut identities = Vec::new();
        for alias in aliases {
            if let Some(entry) = entries.get_mut(&lowercase_utf16(&alias)) {
                if let Some(entry) = entry.recover(password.as_deref().map(Vec::as_slice), reservation.budget())? {
                    identities.push(SunX509JksIdentity { alias, entry });
                }
            }
        }
        Ok(Self { identities, _reservation: reservation })
    }

    /// Credentials in Java Hashtable enumeration order, retaining raw UTF-16 alias spellings.
    pub fn identities(&self) -> &[SunX509JksIdentity] { &self.identities }
}

impl SunX509JksIdentity {
    /// Raw alias supplied to provider lookup, including isolated UTF-16 surrogates.
    pub fn alias(&self) -> &[u16] { &self.alias }
    /// Borrows zeroized-on-release normalized PKCS8; native key import is the transport's responsibility.
    pub fn private_key_pkcs8(&self) -> &[u8] { &self.entry.private_key }
    /// Borrows the nonempty leaf-first DER certificate chain.
    pub fn certificates(&self) -> &[Vec<u8>] { &self.entry.certificates }
}

impl<'a> Entry<'a> {
    fn recover(&mut self, password: Option<&[u8]>, memory: &Arc<MemoryBudget>) -> Result<Option<Arc<RecoveredEntry>>> {
        let protected = match self {
            Self::Pending(JksEntry::Certificate(_)) => return Ok(None),
            Self::Pending(JksEntry::Key { protected, .. }) | Self::Recovered { protected, .. } => *protected,
        };
        let password = password.ok_or_else(|| invalid("JKS private keys require a password"))?;
        let mut bound = protected.len().checked_mul(512).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(overflow)?;
        if let Self::Pending(JksEntry::Key { certificates, .. }) = self {
            for certificate in certificates { bound = bound.checked_add(certificate.len().checked_mul(16).ok_or_else(overflow)?).ok_or_else(overflow)?; }
        }
        let mut reservation = memory.reserve(bound)?;
        let private_key = recovery::recover(protected, password)?;
        let certificates = match self {
            Self::Recovered { entry, .. } => return Ok(Some(Arc::clone(entry))),
            Self::Pending(JksEntry::Key { certificates, .. }) if certificates.is_empty() => return Ok(None),
            Self::Pending(JksEntry::Key { certificates, .. }) => certificates.iter().map(|certificate| certificate.to_vec()).collect::<Vec<_>>(),
            Self::Pending(JksEntry::Certificate(_)) => unreachable!("certificate entry has no key recovery"),
        };
        let retained = certificates.iter().try_fold(private_key.capacity().checked_add(std::mem::size_of::<RecoveredEntry>()).and_then(|bytes| bytes.checked_add(128)).ok_or_else(overflow)?, |bytes, certificate| bytes.checked_add(certificate.capacity()).ok_or_else(overflow))?
            .checked_add(certificates.capacity().checked_mul(std::mem::size_of::<Vec<u8>>()).ok_or_else(overflow)?).ok_or_else(overflow)?;
        Arc::get_mut(&mut reservation).expect("unpublished recovered entry reservation").shrink_to(retained)?;
        let entry = Arc::new(RecoveredEntry { private_key, certificates, _reservation: reservation });
        *self = Self::Recovered { protected, entry: Arc::clone(&entry) };
        Ok(Some(entry))
    }
}

fn overflow() -> rtdb_types::Anomaly { invalid("SunX509 JKS allocation exceeds range") }

#[cfg(test)]
mod tests;
