use std::sync::{Arc, OnceLock};
use foreign_types::{ForeignType, ForeignTypeRef};
use openssl::{error::ErrorStack, ex_data::Index, pkey::{PKey, Private}, ssl::{Ssl, SslRef}, stack::Stack, x509::X509};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_tls::{KeyStore, ProtectedKeyStore, StoreFormat, StoreUse, SunX509JksStore, SunX509Pkcs12Store};
use rtdb_types::Result;
use zeroize::Zeroizing;
use super::{Algorithm, KeyManager, KeyType, TlsConfigError, TlsConfigResult, invalid, overflow, random, security};

#[derive(Clone)]
/// Admitted client material; every exposed identity has a nonempty leaf-first certificate chain.
/// Deferred entries retain their protected store and zeroized effective password through the last owner.
pub(crate) enum Material { Eager(Arc<KeyStore>), SunJks(Arc<SunX509JksStore>), SunPkcs12(Arc<SunX509Pkcs12Store>), Deferred(Arc<ProtectedKeys>) }
pub(crate) struct ProtectedKeys { store: Arc<ProtectedKeyStore>, password: Option<Zeroizing<String>>, _password_charge: Arc<MemoryReservation> }
pub(super) struct NativeIdentities { entries: Vec<ClientIdentity>, manager: KeyManager, charge: Arc<MemoryReservation> }
pub(super) struct ClientIdentity { pub certificate: X509, pub chain: Stack<X509>, pub kind: KeyType, pub eligible: bool, key: Key }
enum Key { Eager { key: PKey<Private>, _random: Arc<random::Context> }, Deferred { owner: Arc<ProtectedKeys>, index: usize } }
struct SelectedKey { key: PKey<Private>, _charge: Arc<MemoryReservation>, _random: Arc<random::Context>, _signature_random: Arc<random::Context> }

impl Material {
    /// Initial decode charge; key recovery, deferred passwords and expanded PKCS12 chains reserve additional memory from the same budget.
    pub(crate) fn allocation_bound(length: usize, format: StoreFormat, usage: StoreUse, algorithm: Algorithm, password: Option<&str>, key_password: Option<&str>) -> Result<usize> {
        if format == StoreFormat::Java && usage != StoreUse::Trust && algorithm == Algorithm::NewSunX509 {
            ProtectedKeyStore::allocation_bound(length, password)
        } else if format == StoreFormat::Java && usage != StoreUse::Trust && algorithm == Algorithm::SunX509 {
            SunX509JksStore::allocation_bound(length, password, key_password.or(password))
        } else { KeyStore::allocation_bound_with_passwords(length, password, key_password) }
    }

    /// The supplied charge must cover `allocation_bound` before decoding or allocating the input buffer.
    /// Native provider acceptance is checked when creating native identities.
    pub(crate) fn decode(bytes: &[u8], format: StoreFormat, usage: StoreUse, algorithm: Algorithm, password: Option<&str>, key_password: Option<&str>, charge: Arc<MemoryReservation>) -> Result<Self> {
        if format == StoreFormat::Java && usage != StoreUse::Trust && algorithm == Algorithm::NewSunX509 {
            let password_charge = charge.budget().reserve(key_password.or(password).map_or(0, str::len).checked_add(256).ok_or_else(overflow)?)?;
            let store = Arc::new(ProtectedKeyStore::decode(bytes, password, charge)?);
            if usage == StoreUse::Identity && store.identities().is_empty() { return Err(invalid("Keystore contains no private key and certificate chain")); }
            Ok(Self::Deferred(Arc::new(ProtectedKeys { store, password: key_password.or(password).map(|password| Zeroizing::new(password.to_owned())), _password_charge: password_charge })))
        } else if format == StoreFormat::Java && usage != StoreUse::Trust && algorithm == Algorithm::SunX509 && bytes.starts_with(&0xfeedfeedu32.to_be_bytes()) {
            let store = SunX509JksStore::decode(bytes, password, key_password.or(password), charge)?;
            if usage == StoreUse::Identity && store.identities().is_empty() { return Err(invalid("Keystore contains no private key and certificate chain")); }
            Ok(Self::SunJks(Arc::new(store)))
        } else if format == StoreFormat::Java && usage != StoreUse::Trust && algorithm == Algorithm::SunX509 {
            let store = SunX509Pkcs12Store::decode(bytes, password, key_password.or(password), charge)?;
            if usage == StoreUse::Identity && store.identities().is_empty() { return Err(invalid("Keystore contains no private key and certificate chain")); }
            Ok(Self::SunPkcs12(Arc::new(store)))
        } else { KeyStore::decode(bytes, format, usage, password, key_password, charge).map(|store| Self::Eager(Arc::new(store))) }
    }

    /// The returned owner's reservation must outlive SSL connections retaining its certificates.
    /// Deferred material requires NewSunX509; loading metadata does not attempt private-key recovery.
    pub(super) fn native(&self, algorithm: Algorithm, random: &Arc<random::Context>, charge: &Arc<MemoryReservation>) -> Result<NativeIdentities> {
        self.native_typed(algorithm, random, charge).map_err(TlsConfigError::into_anomaly)
    }

    pub(super) fn native_typed(&self, algorithm: Algorithm, random: &Arc<random::Context>, charge: &Arc<MemoryReservation>) -> TlsConfigResult<NativeIdentities> {
        if matches!(self, Self::Deferred(_)) && algorithm != Algorithm::NewSunX509 { return Err(TlsConfigError::invalid("Deferred private keys require NewSunX509")); }
        if matches!(self, Self::SunJks(_) | Self::SunPkcs12(_)) && algorithm != Algorithm::SunX509 { return Err(TlsConfigError::invalid("Eager SunX509 material requires SunX509")); }
        let mut bound = 65_536usize;
        match self {
            Self::Eager(store) => for identity in store.identities() {
                bound = native_bound(bound, identity.private_key_pkcs8().len()).map_err(TlsConfigError::from_invalid_anomaly)?;
                for certificate in identity.certificates() { bound = native_bound(bound, certificate.len()).map_err(TlsConfigError::from_invalid_anomaly)?; }
            },
            Self::SunJks(store) => for identity in store.identities() {
                bound = native_bound(bound, identity.private_key_pkcs8().len()).map_err(TlsConfigError::from_invalid_anomaly)?;
                for certificate in identity.certificates() { bound = native_bound(bound, certificate.len()).map_err(TlsConfigError::from_invalid_anomaly)?; }
            },
            Self::SunPkcs12(store) => for identity in store.identities() {
                bound = native_bound(bound, identity.private_key_pkcs8().len()).map_err(TlsConfigError::from_invalid_anomaly)?;
                for certificate in identity.certificates() { bound = native_bound(bound, certificate.len()).map_err(TlsConfigError::from_invalid_anomaly)?; }
            },
            Self::Deferred(owner) => for identity in owner.store.identities() {
                for certificate in identity.certificates() { bound = native_bound(bound, certificate.len()).map_err(TlsConfigError::from_invalid_anomaly)?; }
            },
        }
        let charge = charge.budget().reserve(bound).map_err(TlsConfigError::Other)?;
        let (manager, identities) = match self {
            Self::Eager(store) => {
                let manager = KeyManager::new(algorithm, store.identities());
                let identities = store.identities().iter().map(|identity| {
                    let key = unsafe { random.private_key_from_pkcs8(identity.private_key_pkcs8()).map_err(TlsConfigError::from_invalid_anomaly)? };
                    ClientIdentity::new(identity.certificates(), Key::Eager { key, _random: Arc::clone(random) }, algorithm, charge.budget())
                }).collect::<TlsConfigResult<Vec<_>>>()?;
                (manager, identities)
            }
            Self::SunJks(store) => {
                let aliases = store.identities().iter().map(|identity| identity.alias()).collect::<Vec<_>>();
                let manager = KeyManager::from_aliases(algorithm, &aliases);
                let identities = store.identities().iter().map(|identity| {
                    let key = unsafe { random.private_key_from_pkcs8(identity.private_key_pkcs8()).map_err(TlsConfigError::from_invalid_anomaly)? };
                    ClientIdentity::new(identity.certificates(), Key::Eager { key, _random: Arc::clone(random) }, algorithm, charge.budget())
                }).collect::<TlsConfigResult<Vec<_>>>()?;
                (manager, identities)
            }
            Self::SunPkcs12(store) => {
                let aliases = store.identities().iter().map(|identity| identity.alias()).collect::<Vec<_>>();
                let manager = KeyManager::from_aliases(algorithm, &aliases);
                let identities = store.identities().iter().map(|identity| {
                    let key = unsafe { random.private_key_from_pkcs8(identity.private_key_pkcs8()).map_err(TlsConfigError::from_invalid_anomaly)? };
                    ClientIdentity::new(identity.certificates(), Key::Eager { key, _random: Arc::clone(random) }, algorithm, charge.budget())
                }).collect::<TlsConfigResult<Vec<_>>>()?;
                (manager, identities)
            }
            Self::Deferred(owner) => {
                let aliases = owner.store.identities().iter().map(|identity| identity.alias()).collect::<Vec<_>>();
                let manager = KeyManager::from_aliases(algorithm, &aliases);
                let identities = owner.store.identities().iter().enumerate().map(|(index, identity)| {
                    ClientIdentity::new(identity.certificates(), Key::Deferred { owner: Arc::clone(owner), index }, algorithm, charge.budget())
                }).collect::<TlsConfigResult<Vec<_>>>()?;
                (manager, identities)
            }
        };
        Ok(NativeIdentities { entries: identities, manager, charge })
    }
}

impl NativeIdentities {
    pub(super) fn entries(&self) -> &[ClientIdentity] { &self.entries }
    pub(super) fn manager(&self) -> &KeyManager { &self.manager }
    pub(super) fn reservation(&self) -> Arc<MemoryReservation> { Arc::clone(&self.charge) }
}

impl ClientIdentity {
    fn new(certificates: &[Vec<u8>], key: Key, algorithm: Algorithm, memory: &Arc<MemoryBudget>) -> TlsConfigResult<Self> {
        let certificate = X509::from_der(&certificates[0]).map_err(TlsConfigError::invalid)?;
        let kind = KeyType::of(&certificate).map_err(TlsConfigError::from_invalid_anomaly)?;
        let mut chain = Stack::new().map_err(TlsConfigError::invalid)?;
        for certificate in &certificates[1..] { chain.push(X509::from_der(certificate).map_err(TlsConfigError::invalid)?).map_err(TlsConfigError::invalid)?; }
        let eligible = algorithm == Algorithm::SunX509 || security::java_client_chain_eligible(&std::iter::once(certificate.as_ref()).chain(chain.iter()).collect::<Vec<_>>(), memory).map_err(TlsConfigError::Other)?;
        Ok(Self { certificate, chain, kind, eligible, key })
    }

    pub(super) fn eager_compatible(&self, ssl: &SslRef, required: i32) -> bool {
        match &self.key {
            Key::Eager { key, .. } => (unsafe { super::SSL_check_chain(ssl.as_ptr(), self.certificate.as_ptr(), key.as_ptr(), self.chain.as_ptr()) }) & required == required,
            Key::Deferred { .. } => false,
        }
    }

    pub(super) fn install(&self, ssl: &mut SslRef, random: &Arc<random::Context>, signature_random: &Arc<random::Context>) -> Result<bool> {
        let key = match &self.key {
            Key::Eager { key, .. } => key.clone(),
            Key::Deferred { owner, index } => {
                let Some(selected) = owner.recover(*index, random, signature_random)? else { return Ok(false); };
                let key = selected.key.clone();
                let index = selected_index()?;
                if let Some(keys) = ssl.ex_data_mut(index) { keys.push(selected); } else { ssl.set_ex_data(index, vec![selected]); }
                key
            }
        };
        ssl.set_certificate(&self.certificate).map_err(invalid)?;
        ssl.set_private_key(&key).map_err(invalid)?;
        for certificate in &self.chain { ssl.add_chain_cert(certificate.to_owned()).map_err(invalid)?; }
        Ok(true)
    }
}

impl ProtectedKeys {
    fn recover(&self, index: usize, random: &Arc<random::Context>, signature_random: &Arc<random::Context>) -> Result<Option<SelectedKey>> {
        let recovered = match self.store.identities()[index].recover(self.password.as_ref().map(|password| password.as_str()), self._password_charge.budget()) {
            Ok(key) => key,
            Err(rtdb_tls::TlsError::InvalidKeystore(_)) => return Ok(None),
            Err(error) => return Err(error.into_anomaly()),
        };
        let key = unsafe { random.private_key_from_pkcs8(recovered.private_key_pkcs8())? };
        Ok(Some(SelectedKey { key, _charge: recovered.reservation(), _random: Arc::clone(random), _signature_random: Arc::clone(signature_random) }))
    }
}

// SSL_free destroys the TLS connection and its certificate keys before freeing SSL ex-data.
fn selected_index() -> Result<Index<Ssl, Vec<SelectedKey>>> {
    static INDEX: OnceLock<std::result::Result<Index<Ssl, Vec<SelectedKey>>, ErrorStack>> = OnceLock::new();
    INDEX.get_or_init(Ssl::new_ex_index).as_ref().copied().map_err(invalid)
}

fn native_bound(bytes: usize, length: usize) -> Result<usize> {
    length.checked_mul(32).and_then(|length| bytes.checked_add(length)).ok_or_else(overflow)
}

#[cfg(test)]
mod tests;
