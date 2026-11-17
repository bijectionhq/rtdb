use std::sync::Arc;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::Result;
use crate::{KeyStore, invalid, private_factory::PrivateKey};
mod attributes;
mod der;
mod loader;
mod protection;

/// Immutable eager SunX509 PKCS12 credentials in provider alias order.
/// Loading completes on the caller and retains no password; borrowed material lives with this owner.
pub struct SunX509Pkcs12Store { identities: Vec<SunX509Pkcs12Identity>, _reservation: Arc<MemoryReservation> }

/// One normalized alias, admitted private key, and nonempty leaf-first certificate chain.
pub struct SunX509Pkcs12Identity { alias: Vec<u16>, private_key: PrivateKey, certificates: Vec<Vec<u8>>, _reservation: Arc<MemoryReservation> }

impl SunX509Pkcs12Store {
    /// Initial admission for input, parsed provider metadata, certificates and temporary password representations.
    /// Loading additionally reserves 192 times the input length until its construction transient is released.
    /// Section decryption, chain expansion and key recovery reserve separate scratch from the same budget.
    pub fn allocation_bound(encoded_bytes: usize, store_password: Option<&str>, key_password: Option<&str>) -> Result<usize> {
        KeyStore::allocation_bound_with_passwords(encoded_bytes, store_password, key_password)?.checked_add(encoded_bytes.checked_mul(48).ok_or_else(overflow)?).ok_or_else(overflow)
    }

    /// Loads the pinned SunX509 PKCS12 path, recovering keys before secret-key and empty-chain filtering.
    ///
    /// - A missing store password skips MAC verification and encrypted sections.
    /// - Key recovery requires an explicit key password; `None` does not use `store_password`.
    /// - Supported provider admission errors, unsupported services and memory failures abort loading.
    /// - This API does not match private-key algorithms or values against the leaf certificate.
    /// - The reservation must cover `allocation_bound` before input allocation and this call; copies need separate admission.
    pub fn decode(bytes: &[u8], store_password: Option<&str>, key_password: Option<&str>, reservation: Arc<MemoryReservation>) -> Result<Self> {
        if bytes.len() > i32::MAX as usize || reservation.bytes() < Self::allocation_bound(bytes.len(), store_password, key_password)? { return Err(invalid("SunX509 PKCS12 decode reservation is too small")); }
        let _loading = reservation.budget().reserve(bytes.len().checked_mul(192).ok_or_else(overflow)?)?;
        let identities = loader::load(bytes, store_password, reservation.budget())?.recover(key_password, reservation.budget())?;
        Ok(Self { identities, _reservation: reservation })
    }

    /// Credentials after provider alias replacement and eager recovery, including an empty result.
    pub fn identities(&self) -> &[SunX509Pkcs12Identity] { &self.identities }
}

impl SunX509Pkcs12Identity {
    /// Provider-normalized UTF-16 alias supplied to SunX509's credential table.
    pub fn alias(&self) -> &[u16] { &self.alias }
    /// Borrows zeroized-on-release provider-admitted PKCS8; native import remains the transport's responsibility.
    pub fn private_key_pkcs8(&self) -> &[u8] { self.private_key.encoded_pkcs8() }
    /// Borrows the nonempty leaf-first DER certificate chain.
    pub fn certificates(&self) -> &[Vec<u8>] { &self.certificates }
}

fn scratch(length: usize, password: Option<&str>, memory: &Arc<MemoryBudget>) -> Result<Arc<MemoryReservation>> {
    let bound = length.checked_mul(512).and_then(|bytes| password.map_or(Some(0), |password| password.len().checked_mul(16)).and_then(|password| bytes.checked_add(password))).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(overflow)?;
    memory.reserve(bound)
}
fn overflow() -> rtdb_types::Anomaly { invalid("SunX509 PKCS12 allocation exceeds range") }

#[cfg(test)]
mod tests;
