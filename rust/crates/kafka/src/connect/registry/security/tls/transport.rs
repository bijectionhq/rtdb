use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::MemoryReservation;
use rtdb_tls::{IssuerNames, KeyStore};
use rtdb_types::Result;
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme, client::{ResolvesClientCert, danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier}}, crypto::{CryptoProvider, WebPkiSupportedAlgorithms}, pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime}, server::ParsedCertificate, sign::CertifiedKey};
use super::error;

pub(super) fn configuration(options: &BTreeMap<&str, &str>, trust: Option<&KeyStore>, identities: Option<&KeyStore>, registry_hostname_policy: bool, charge: Arc<MemoryReservation>) -> Result<ClientConfig> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut versions = rustls::ALL_VERSIONS.to_vec();
    match options.get("protocol").copied().unwrap_or("TLSv1.3") {
        "TLS" | "TLSv1.3" => (), "TLSv1.2" => versions.retain(|version| version.version == rustls::ProtocolVersion::TLSv1_2),
        _ => return Err(error("invalid-plugin-config", "Registry TLS protocol requires TLSv1.2 or TLSv1.3")),
    }
    if let Some(enabled) = options.get("enabled.protocols") {
        let protocols = enabled.split(',').map(str::trim).collect::<Vec<_>>();
        if protocols.iter().any(|protocol| !matches!(*protocol, "TLSv1.2" | "TLSv1.3")) { return Err(error("invalid-plugin-config", "Unsupported registry TLS enabled protocol")); }
        versions.retain(|version| match version.version { rustls::ProtocolVersion::TLSv1_2 => protocols.contains(&"TLSv1.2"), rustls::ProtocolVersion::TLSv1_3 => protocols.contains(&"TLSv1.3"), _ => false });
    }
    let roots = roots(trust, Arc::clone(&charge))?;
    let verify_names = !options.get("endpoint.identification.algorithm").map_or(registry_hostname_policy, |algorithm| matches!(*algorithm, "" | "none"));
    let verifier = ChainVerifier { roots, algorithms: provider.signature_verification_algorithms, verify_names, _charge: Arc::clone(&charge) };
    let resolver = ClientKeys::new(identities, &provider, charge)?;
    Ok(ClientConfig::builder_with_provider(provider).with_protocol_versions(&versions).map_err(|cause| error("invalid-plugin-config", cause))?
        .dangerous().with_custom_certificate_verifier(Arc::new(verifier)).with_client_cert_resolver(Arc::new(resolver)))
}
fn roots(trust: Option<&KeyStore>, charge: Arc<MemoryReservation>) -> Result<RootCertStore> {
    let defaults = if trust.is_none() { Some(crate::tls::trust::decode(charge)?) } else { None };
    let trust = trust.or(defaults.as_ref()).expect("explicit or default trust store");
    let mut roots = RootCertStore::empty();
    for certificate in trust.trusted_certificates() { roots.add(CertificateDer::from(certificate.as_slice())).map_err(|cause| error("invalid-plugin-config", cause))?; }
    Ok(roots)
}
struct ClientIdentity { key: Arc<CertifiedKey>, issuers: IssuerNames }
struct ClientKeys { identities: Vec<ClientIdentity>, _charge: Arc<MemoryReservation> }
impl ClientKeys {
    fn new(store: Option<&KeyStore>, provider: &CryptoProvider, charge: Arc<MemoryReservation>) -> Result<Self> {
        let identities = store.into_iter().flat_map(KeyStore::identities).map(|identity| {
            let certificates = identity.certificates().iter().cloned().map(CertificateDer::from).collect();
            let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity.private_key_pkcs8().to_vec()));
            Ok(ClientIdentity { key: Arc::new(CertifiedKey::from_der(certificates, key, provider).map_err(|cause| error("invalid-plugin-config", cause))?), issuers: identity.issuer_names(charge.budget())? })
        }).collect::<Result<Vec<_>>>()?;
        Ok(Self { identities, _charge: charge })
    }
}
impl std::fmt::Debug for ClientKeys {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.debug_struct("ClientKeys").field("identities", &self.identities.len()).finish() }
}
impl ResolvesClientCert for ClientKeys {
    fn resolve(&self, authorities: &[&[u8]], schemes: &[SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        self.identities.iter().find(|identity| identity.key.key.choose_scheme(schemes).is_some()
            && (authorities.is_empty() || identity.issuers.as_slice().iter().any(|issuer| authorities.contains(&issuer.as_slice())))).map(|identity| Arc::clone(&identity.key))
    }
    fn has_certs(&self) -> bool { !self.identities.is_empty() }
}
struct ChainVerifier { roots: RootCertStore, algorithms: WebPkiSupportedAlgorithms, verify_names: bool, _charge: Arc<MemoryReservation> }
impl std::fmt::Debug for ChainVerifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.debug_struct("ChainVerifier").field("verify_names", &self.verify_names).finish() }
}
impl ServerCertVerifier for ChainVerifier {
    fn verify_server_cert(&self, leaf: &CertificateDer<'_>, intermediates: &[CertificateDer<'_>], server: &ServerName<'_>, _ocsp: &[u8], now: UnixTime) -> std::result::Result<ServerCertVerified, rustls::Error> {
        let certificate = ParsedCertificate::try_from(leaf)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(&certificate, &self.roots, intermediates, now, self.algorithms.all)?;
        if self.verify_names { rustls::client::verify_server_name(&certificate, server)?; }
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(&self, message: &[u8], certificate: &CertificateDer<'_>, signature: &DigitallySignedStruct) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, certificate, signature, &self.algorithms)
    }
    fn verify_tls13_signature(&self, message: &[u8], certificate: &CertificateDer<'_>, signature: &DigitallySignedStruct) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, certificate, signature, &self.algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> { self.algorithms.supported_schemes() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_columnar::MemoryBudget;
    use rtdb_tls::{StoreFormat, StoreUse};
    #[test]
    fn registry_default_roots_match_the_pinned_java_store_and_explicit_empty_trust_wins() {
        let memory = MemoryBudget::new(1 << 25);
        let charge = memory.reserve(crate::tls::trust::allocation_bound().unwrap()).unwrap();
        let defaults = roots(None, Arc::clone(&charge)).unwrap();
        assert_eq!(defaults.len(), 146);
        let empty = include_bytes!("../../../../../../../../rust/crates/tls/tests/fixtures/empty.jks");
        let empty = KeyStore::decode(empty, StoreFormat::Java, StoreUse::Trust, Some("12345678"), None, Arc::clone(&charge)).unwrap();
        assert!(roots(Some(&empty), Arc::clone(&charge)).unwrap().is_empty());
        drop(defaults); drop(empty); drop(charge);
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn client_identity_selection_checks_issuer_hints_and_signature_schemes() {
        let bytes = include_bytes!("../../../../../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.p12");
        let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(KeyStore::allocation_bound_with_passwords(bytes.len(),Some("password123"),None).unwrap()).unwrap();
        let store = KeyStore::decode(bytes,StoreFormat::Java,StoreUse::Identity,Some("password123"),None,Arc::clone(&charge)).unwrap();
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let first = ClientKeys::new(Some(&store),&provider,charge).unwrap();
        let issuer = &first.identities[0].issuers.as_slice()[0];
        let first_key = Arc::clone(&first.identities[0].key);
        assert!(Arc::ptr_eq(&first.resolve(&[issuer.as_slice()],&[SignatureScheme::RSA_PSS_SHA256]).unwrap(),&first_key));
        assert!(first.resolve(&[b"issuer-unknown"],&[SignatureScheme::RSA_PSS_SHA256]).is_none());
        assert!(first.resolve(&[],&[SignatureScheme::ECDSA_NISTP256_SHA256]).is_none());
        assert!(first.resolve(&[],&[SignatureScheme::RSA_PSS_SHA256]).is_some());
        drop(first); drop(first_key); drop(store); assert_eq!(memory.used_bytes(),0);
    }
    fn certificates() -> (Vec<u8>, Vec<u8>) {
        use openssl::{asn1::Asn1Time, bn::BigNum, ec::{EcGroup, EcKey}, hash::MessageDigest, nid::Nid, pkey::PKey, x509::{X509, X509NameBuilder, extension::{BasicConstraints, KeyUsage, SubjectAlternativeName}}};
        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
        let key = PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN","registry.test").unwrap(); let name = name.build();
        let mut root = X509::builder().unwrap(); root.set_version(2).unwrap();
        root.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
        root.set_subject_name(&name).unwrap(); root.set_issuer_name(&name).unwrap(); root.set_pubkey(&key).unwrap();
        root.set_not_before(&Asn1Time::from_unix(1_600_000_000).unwrap()).unwrap(); root.set_not_after(&Asn1Time::from_unix(1_900_000_000).unwrap()).unwrap();
        root.append_extension(BasicConstraints::new().critical().ca().build().unwrap()).unwrap();
        root.append_extension(KeyUsage::new().key_cert_sign().build().unwrap()).unwrap(); root.sign(&key,MessageDigest::sha256()).unwrap(); let root = root.build();
        let mut leaf = X509::builder().unwrap(); leaf.set_version(2).unwrap();
        leaf.set_serial_number(&BigNum::from_u32(2).unwrap().to_asn1_integer().unwrap()).unwrap();
        leaf.set_subject_name(&name).unwrap(); leaf.set_issuer_name(&name).unwrap(); leaf.set_pubkey(&key).unwrap();
        leaf.set_not_before(&Asn1Time::from_unix(1_600_000_000).unwrap()).unwrap(); leaf.set_not_after(&Asn1Time::from_unix(1_900_000_000).unwrap()).unwrap();
        leaf.append_extension(BasicConstraints::new().build().unwrap()).unwrap();
        leaf.append_extension(SubjectAlternativeName::new().dns("registry.test").build(&leaf.x509v3_context(Some(&root),None)).unwrap()).unwrap();
        leaf.sign(&key,MessageDigest::sha256()).unwrap(); (root.to_der().unwrap(),leaf.build().to_der().unwrap())
    }
    #[test]
    fn disabling_hostnames_preserves_chain_and_validity_verification() {
        let (root,leaf) = certificates(); let leaf = CertificateDer::from(leaf); let mut roots = RootCertStore::empty(); roots.add(CertificateDer::from(root)).unwrap();
        let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1 << 20).unwrap();
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let mut verifier = ChainVerifier { roots, algorithms: provider.signature_verification_algorithms, verify_names:false,_charge:charge };
        let wrong_name = ServerName::try_from("wrong.test").unwrap(); let now = UnixTime::since_unix_epoch(std::time::Duration::from_secs(1_700_000_000));
        assert!(verifier.verify_server_cert(&leaf,&[],&wrong_name,&[],now).is_ok());
        verifier.verify_names = true; assert!(verifier.verify_server_cert(&leaf,&[],&wrong_name,&[],now).is_err());
        assert!(verifier.verify_server_cert(&leaf,&[],&ServerName::try_from("registry.test").unwrap(),&[],now).is_ok());
        verifier.verify_names = false;
        assert!(verifier.verify_server_cert(&leaf,&[],&wrong_name,&[],UnixTime::since_unix_epoch(std::time::Duration::from_secs(1_950_000_000))).is_err());
        verifier.roots = RootCertStore::empty(); assert!(verifier.verify_server_cert(&leaf,&[],&wrong_name,&[],now).is_err());
    }
}
