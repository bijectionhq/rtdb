use std::{fs::File, io::Read, sync::Arc};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_tls::{KeyStore, StoreFormat, StoreUse};
use rtdb_types::{Anomaly, Result};
use tokio_rustls::rustls::{self, pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, pem::PemObject}, server::{ClientHello, ResolvesServerCert}, sign::CertifiedKey};
use zeroize::Zeroizing;
use crate::config::KeyStoreSettings;
const MAX_KEYSTORE_BYTES: u64 = 16 * 1024 * 1024;

/// Loads JKS or PKCS12 identities using the store password for private keys too.
/// Returned TLS configuration retains its memory reservation until every listener releases it.
pub fn load(settings: &KeyStoreSettings, memory: &Arc<MemoryBudget>) -> Result<Arc<rustls::ServerConfig>> {
    let file = File::open(&settings.path).map_err(invalid)?;
    let length = file.metadata().map_err(invalid)?.len();
    if length > MAX_KEYSTORE_BYTES { return Err(invalid("TLS keystore exceeds 16 MiB")); }
    let reservation = memory.reserve(KeyStore::allocation_bound_with_passwords(length as usize, Some(settings.password.expose()), None).map_err(invalid)?)?;
    let mut bytes = Zeroizing::new(Vec::with_capacity(length as usize + 1));
    file.take(length + 1).read_to_end(&mut bytes).map_err(invalid)?;
    if bytes.len() as u64 != length { return Err(invalid("TLS keystore changed while loading")); }
    let store = KeyStore::decode(&bytes, StoreFormat::Java, StoreUse::Identity, Some(settings.password.expose()), None, reservation).map_err(invalid)?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let keys = store.identities().iter().map(|identity| {
        let certificates = identity.certificates().iter().cloned().map(CertificateDer::from).collect();
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity.private_key_pkcs8().to_vec()));
        CertifiedKey::from_der(certificates, key, &provider).map(Arc::new).map_err(invalid)
    }).collect::<Result<Vec<_>>>()?;
    let configuration = rustls::ServerConfig::builder_with_provider(provider).with_safe_default_protocol_versions().map_err(invalid)?
        .with_no_client_auth().with_cert_resolver(Arc::new(KeyResolver { keys, _reservation: store.reservation() }));
    Ok(Arc::new(configuration))
}
pub fn from_pem_identity(certificate: &[u8], private_key: &[u8]) -> Result<rustls::ServerConfig> {
    let invalid = |message| Anomaly::incorrect("rtdb.server/invalid-tls-identity", message);
    let chain = CertificateDer::pem_slice_iter(certificate)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| invalid("Invalid TLS certificate PEM".to_owned()))?;
    let key = PrivateKeyDer::from_pem_slice(private_key)
        .map_err(|_| invalid("Invalid TLS private key PEM".to_owned()))?;
    rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|error| invalid(error.to_string()))?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|error| invalid(error.to_string()))
}

struct KeyResolver { keys: Vec<Arc<CertifiedKey>>, _reservation: Arc<MemoryReservation> }
impl std::fmt::Debug for KeyResolver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.debug_struct("KeyResolver").field("identities", &self.keys.len()).finish() }
}
impl ResolvesServerCert for KeyResolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.keys.iter().find(|key| key.key.choose_scheme(hello.signature_schemes()).is_some()).cloned()
    }
}

fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.server/invalid-tls-keystore", error.to_string()) }
