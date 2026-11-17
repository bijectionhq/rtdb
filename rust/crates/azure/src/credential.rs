use std::collections::BTreeMap;
use std::sync::Arc;
use async_trait::async_trait;
use azure_core::credentials::{Secret, SecretBytes, TokenCredential};
use azure_identity::{AzureCliCredential, AzureCliCredentialOptions, AzureDeveloperCliCredential, AzureDeveloperCliCredentialOptions, ClientCertificateCredential, ClientCertificateCredentialOptions, ClientSecretCredential, WorkloadIdentityCredential, WorkloadIdentityCredentialOptions};
use object_store::{CredentialProvider, azure::AzureCredential};
use rtdb_types::{Anomaly, Result};
use tokio::sync::Mutex;

enum FailurePolicy { Stop, Continue, ManagedUnavailable }
impl FailurePolicy {
    fn continues(&self, error: &azure_core::Error) -> bool {
        match self { Self::Stop => false, Self::Continue => true, Self::ManagedUnavailable => crate::managed::is_unavailable(error) }
    }
}
struct Candidate { credential: Arc<dyn TokenCredential>, failure: FailurePolicy }

pub(crate) struct CredentialBridge { sources: Vec<Candidate>, selected: Mutex<Option<usize>> }

impl std::fmt::Debug for CredentialBridge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("AzureCredentialBridge") }
}

impl CredentialBridge {
    pub fn new(managed_identity_client_id: Option<&str>, io: Arc<rtdb_storage::IoRegion>) -> Result<Self> {
        let environment: BTreeMap<_, _> = ["AZURE_TOKEN_CREDENTIALS", "AZURE_TENANT_ID", "AZURE_AUTHORITY_HOST", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_CLIENT_CERTIFICATE_PATH", "AZURE_CLIENT_CERTIFICATE_PASSWORD", "AZURE_USERNAME", "AZURE_PASSWORD", "AZURE_FEDERATED_TOKEN_FILE", "IDENTITY_ENDPOINT", "IDENTITY_HEADER", "IDENTITY_SERVER_THUMBPRINT", "IMDS_ENDPOINT", "MSI_ENDPOINT", "AZURE_POD_IDENTITY_AUTHORITY_HOST", "ProgramData"]
            .into_iter().filter_map(|name| std::env::var(name).ok().map(|value| (name, value))).collect();
        Self::from_environment(&environment, managed_identity_client_id, io)
    }

    fn from_environment(environment: &BTreeMap<&str, String>, managed_identity_client_id: Option<&str>, io: Arc<rtdb_storage::IoRegion>) -> Result<Self> {
        let selection = environment.get("AZURE_TOKEN_CREDENTIALS").map(|value| value.trim().to_ascii_lowercase()).unwrap_or_default();
        if !matches!(selection.as_str(), "" | "prod" | "dev" | "environmentcredential" | "workloadidentitycredential" | "managedidentitycredential" | "intellijcredential" | "visualstudiocodecredential" | "azureclicredential" | "azurepowershellcredential" | "azuredeveloperclicredential") {
            return Err(Anomaly::unsupported("xtdb.azure/credential-selector", "Selected Azure credential has no native adapter"));
        }
        let mut sources = Vec::new();
        if let (Some(tenant), Some(client)) = (environment.get("AZURE_TENANT_ID"), environment.get("AZURE_CLIENT_ID")) {
            if let Some(secret) = environment.get("AZURE_CLIENT_SECRET").filter(|_| matches!(selection.as_str(), "" | "prod" | "environmentcredential")) {
                sources.push(Candidate { credential: ClientSecretCredential::new(tenant, client.clone(), Secret::new(secret.clone()), None).map_err(invalid)?, failure: FailurePolicy::Stop });
            } else if let Some(path) = environment.get("AZURE_CLIENT_CERTIFICATE_PATH").filter(|_| matches!(selection.as_str(), "" | "prod" | "environmentcredential")) {
                let options = ClientCertificateCredentialOptions { password: environment.get("AZURE_CLIENT_CERTIFICATE_PASSWORD").cloned().map(Secret::new), ..Default::default() };
                let certificate = certificate(&std::fs::read(path).map_err(invalid)?, options.password.as_ref())?;
                sources.push(Candidate { credential: ClientCertificateCredential::new(tenant.clone(), client.clone(), certificate, Some(options)).map_err(invalid)?, failure: FailurePolicy::Stop });
            }
            if !sources.is_empty() { return Ok(Self { sources, selected: Mutex::new(None) }); }
        }
        if ["AZURE_CLIENT_ID", "AZURE_USERNAME", "AZURE_PASSWORD"].iter().all(|name| environment.contains_key(name))
            && matches!(selection.as_str(), "" | "prod" | "environmentcredential") {
            sources.push(Candidate { credential: crate::desktop::PasswordCredential::new(environment).map_err(invalid)?, failure: FailurePolicy::Stop });
            return Ok(Self { sources, selected: Mutex::new(None) });
        }
        if ["AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_FEDERATED_TOKEN_FILE"].iter().all(|name| environment.contains_key(name))
            && matches!(selection.as_str(), "" | "prod" | "workloadidentitycredential") {
            sources.push(Candidate { credential: WorkloadIdentityCredential::new(None).map_err(invalid)?, failure: FailurePolicy::Stop });
            return Ok(Self { sources, selected: Mutex::new(None) });
        }
        if matches!(selection.as_str(), "environmentcredential" | "workloadidentitycredential") { return Err(invalid("Selected Azure credential requires complete environment configuration")); }
        let managed_id = managed_identity_client_id.map(str::to_owned).or_else(|| environment.get("AZURE_CLIENT_ID").cloned());
        if matches!(selection.as_str(), "" | "prod" | "managedidentitycredential") {
            let managed: Arc<dyn TokenCredential> = if environment.contains_key("AZURE_TENANT_ID") && environment.contains_key("AZURE_FEDERATED_TOKEN_FILE") {
                WorkloadIdentityCredential::new(Some(WorkloadIdentityCredentialOptions { client_id: managed_id, ..Default::default() })).map_err(invalid)?
            } else { crate::managed::ManagedCredential::new(environment, managed_id, Arc::clone(&io)).map_err(invalid)? };
            sources.push(Candidate { credential: managed, failure: FailurePolicy::ManagedUnavailable });
        }
        if matches!(selection.as_str(), "" | "dev" | "intellijcredential") {
            sources.push(Candidate { credential: crate::desktop::IntelliJCredential::new(environment, io).map_err(invalid)?, failure: FailurePolicy::Continue });
        }
        if matches!(selection.as_str(), "" | "dev" | "visualstudiocodecredential") {
            sources.push(Candidate { credential: Arc::new(crate::desktop::MissingBroker), failure: FailurePolicy::Continue });
        }
        let executor: Arc<dyn azure_identity::Executor> = Arc::new(crate::process::JoinedExecutor);
        if matches!(selection.as_str(), "" | "dev" | "azureclicredential") {
            sources.push(Candidate { credential: AzureCliCredential::new(Some(AzureCliCredentialOptions { executor: Some(Arc::clone(&executor)), tenant_id: environment.get("AZURE_TENANT_ID").cloned(), ..Default::default() })).map_err(invalid)?, failure: if selection != "azureclicredential" { FailurePolicy::Continue } else { FailurePolicy::Stop } });
        }
        if matches!(selection.as_str(), "" | "dev" | "azurepowershellcredential") {
            sources.push(Candidate { credential: crate::powershell::PowerShellCredential::new(Arc::clone(&executor), environment.get("AZURE_TENANT_ID").cloned()).map_err(invalid)?, failure: if selection != "azurepowershellcredential" { FailurePolicy::Continue } else { FailurePolicy::Stop } });
        }
        if matches!(selection.as_str(), "" | "dev" | "azuredeveloperclicredential") {
            sources.push(Candidate { credential: AzureDeveloperCliCredential::new(Some(AzureDeveloperCliCredentialOptions { executor: Some(executor), tenant_id: environment.get("AZURE_TENANT_ID").cloned(), ..Default::default() })).map_err(invalid)?, failure: if selection != "azuredeveloperclicredential" { FailurePolicy::Continue } else { FailurePolicy::Stop } });
        }
        if matches!(selection.as_str(), "" | "dev") {
            sources.push(Candidate { credential: Arc::new(crate::desktop::MissingBroker), failure: FailurePolicy::Continue });
        }
        Ok(Self { sources, selected: Mutex::new(None) })
    }
}

#[async_trait]
impl CredentialProvider for CredentialBridge {
    type Credential = AzureCredential;
    async fn get_credential(&self) -> object_store::Result<Arc<AzureCredential>> {
        let mut selected = self.selected.lock().await;
        if let Some(index) = *selected {
            let token = self.sources[index].credential.get_token(&["https://storage.azure.com/.default"], None).await.map_err(provider_error)?;
            return Ok(Arc::new(AzureCredential::BearerToken(token.token.secret().into())));
        }
        for (index, source) in self.sources.iter().enumerate() {
            match source.credential.get_token(&["https://storage.azure.com/.default"], None).await {
                Ok(token) => {
                    *selected = Some(index);
                    return Ok(Arc::new(AzureCredential::BearerToken(token.token.secret().into())));
                }
                Err(error) if !source.failure.continues(&error) => return Err(provider_error(error)),
                Err(_) => (),
            }
        }
        Err(object_store::Error::Generic { store: "Azure", source: "No Azure credential source authenticated".into() })
    }
}

fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("xtdb.azure/credentials", error.to_string()) }
fn provider_error(error: azure_core::Error) -> object_store::Error { object_store::Error::Generic { store: "Azure", source: Box::new(error) } }

fn certificate(bytes: &[u8], password: Option<&Secret>) -> Result<SecretBytes> {
    if !bytes.starts_with(b"-----BEGIN") { return Ok(SecretBytes::new(bytes.to_vec())); }
    let mut certificates = openssl::x509::X509::stack_from_pem(bytes).map_err(invalid)?;
    let key = match password {
        Some(password) => openssl::pkey::PKey::private_key_from_pem_passphrase(bytes, password.secret().as_bytes()),
        None => openssl::pkey::PKey::private_key_from_pem(bytes),
    }.map_err(invalid)?;
    let index = certificates.iter().position(|certificate| certificate.public_key().is_ok_and(|public| public.public_eq(&key)))
        .ok_or_else(|| invalid("PEM certificate does not match its private key"))?;
    let certificate = certificates.remove(index);
    let mut builder = openssl::pkcs12::Pkcs12::builder();
    builder.name("rtdb").pkey(&key).cert(&certificate);
    if !certificates.is_empty() {
        let mut chain = openssl::stack::Stack::new().map_err(invalid)?;
        for certificate in certificates { chain.push(certificate).map_err(invalid)?; }
        builder.ca(chain);
    }
    let certificate = builder.build2(password.map(Secret::secret).unwrap_or("")).map_err(invalid)?;
    Ok(SecretBytes::new(certificate.to_der().map_err(invalid)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use azure_core::{credentials::{AccessToken, TokenRequestOptions}, error::ErrorKind, time::OffsetDateTime};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct Source { calls: Arc<AtomicUsize>, available: bool }
    #[async_trait]
    impl TokenCredential for Source {
        async fn get_token(&self, scopes: &[&str], _: Option<TokenRequestOptions<'_>>) -> azure_core::Result<AccessToken> {
            assert_eq!(scopes, ["https://storage.azure.com/.default"]);
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.available { return Err(azure_core::Error::with_message(ErrorKind::Credential, "unavailable")); }
            Ok(AccessToken::new(format!("token-{call}"), OffsetDateTime::now_utc()))
        }
    }

    #[tokio::test]
    async fn first_available_source_is_retained_and_asked_to_refresh_each_request() {
        let unavailable = Arc::new(AtomicUsize::new(0));
        let available = Arc::new(AtomicUsize::new(0));
        let bridge = CredentialBridge { sources: vec![
            Candidate { credential: Arc::new(Source { calls: Arc::clone(&unavailable), available: false }), failure: FailurePolicy::Continue },
            Candidate { credential: Arc::new(Source { calls: Arc::clone(&available), available: true }), failure: FailurePolicy::Stop },
        ], selected: Mutex::new(None) };
        assert!(matches!(bridge.get_credential().await.unwrap().as_ref(), AzureCredential::BearerToken(token) if token == "token-0"));
        assert!(matches!(bridge.get_credential().await.unwrap().as_ref(), AzureCredential::BearerToken(token) if token == "token-1"));
        assert_eq!(unavailable.load(Ordering::SeqCst), 1);
        assert_eq!(available.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn desktop_selectors_use_the_reference_default_chain_without_shared_cache_selection() {
        let io = rtdb_storage::IoRegion::new();
        for name in [" IntelliJCredential ", "VISUALSTUDIOCODECREDENTIAL"] {
            let environment = BTreeMap::from([("AZURE_TOKEN_CREDENTIALS", name.into())]);
            let bridge = CredentialBridge::from_environment(&environment, None, Arc::clone(&io)).unwrap();
            assert_eq!(bridge.sources.len(), 1);
        }
        let environment = BTreeMap::from([("AZURE_TOKEN_CREDENTIALS", "dev".into())]);
        let bridge = CredentialBridge::from_environment(&environment, None, Arc::clone(&io)).unwrap();
        assert_eq!(bridge.sources.len(), 6);
        assert!(format!("{:?}", bridge.sources[0].credential).starts_with("IntelliJCredential"));
        assert_eq!(format!("{:?}", bridge.sources[1].credential), "MissingBroker");
        assert_eq!(format!("{:?}", bridge.sources[5].credential), "MissingBroker");
        for name in ["SharedTokenCacheCredential", "BrokerCredential"] {
            let environment = BTreeMap::from([("AZURE_TOKEN_CREDENTIALS", name.into())]);
            assert!(CredentialBridge::from_environment(&environment, None, Arc::clone(&io)).is_err());
        }
        io.close_and_wait().await;
    }

    #[tokio::test]
    async fn broker_free_reference_credentials_allow_developer_fallback() {
        let available = Arc::new(AtomicUsize::new(0));
        let bridge = CredentialBridge { sources: vec![
            Candidate { credential: Arc::new(crate::desktop::MissingBroker), failure: FailurePolicy::Continue },
            Candidate { credential: Arc::new(Source { calls: Arc::clone(&available), available: true }), failure: FailurePolicy::Stop },
        ], selected: Mutex::new(None) };
        assert!(matches!(bridge.get_credential().await.unwrap().as_ref(), AzureCredential::BearerToken(token) if token == "token-0"));
        assert_eq!(available.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn environment_password_precedes_workload_and_does_not_require_a_tenant() {
        let io = rtdb_storage::IoRegion::new();
        for selection in ["", "prod", "EnvironmentCredential"] {
            let mut environment = BTreeMap::from([("AZURE_TOKEN_CREDENTIALS", selection.into()), ("AZURE_CLIENT_ID", "application".into()), ("AZURE_USERNAME", "user".into()), ("AZURE_PASSWORD", "password".into())]);
            for tenant in [None, Some("tenant")] {
                if let Some(tenant) = tenant { environment.insert("AZURE_TENANT_ID", tenant.into()); environment.insert("AZURE_FEDERATED_TOKEN_FILE", "unused-file".into()); }
                let bridge = CredentialBridge::from_environment(&environment, None, Arc::clone(&io)).unwrap();
                assert_eq!(bridge.sources.len(), 1);
                assert_eq!(format!("{:?}", bridge.sources[0].credential), "PasswordCredential([redacted])");
                assert!(matches!(bridge.sources[0].failure, FailurePolicy::Stop));
            }
        }
        io.close_and_wait().await;
    }

    #[tokio::test]
    async fn environment_application_secret_and_certificate_precede_password() {
        let io = rtdb_storage::IoRegion::new();
        let mut environment = BTreeMap::from([("AZURE_TOKEN_CREDENTIALS", "EnvironmentCredential".into()), ("AZURE_CLIENT_ID", "application".into()), ("AZURE_TENANT_ID", "tenant".into()), ("AZURE_CLIENT_SECRET", "application-secret".into()), ("AZURE_USERNAME", "user".into()), ("AZURE_PASSWORD", "password".into())]);
        let bridge = CredentialBridge::from_environment(&environment, None, Arc::clone(&io)).unwrap();
        assert_eq!(bridge.sources.len(), 1);
        assert!(!format!("{:?}", bridge.sources[0].credential).starts_with("PasswordCredential"));
        environment.remove("AZURE_CLIENT_SECRET");
        environment.insert("AZURE_CLIENT_CERTIFICATE_PATH", tempfile::tempdir().unwrap().path().join("absent.pem").to_string_lossy().into_owned());
        assert!(CredentialBridge::from_environment(&environment, None, Arc::clone(&io)).is_err());
        environment.remove("AZURE_TENANT_ID");
        let bridge = CredentialBridge::from_environment(&environment, None, Arc::clone(&io)).unwrap();
        assert_eq!(format!("{:?}", bridge.sources[0].credential), "PasswordCredential([redacted])");
        io.close_and_wait().await;
    }

    #[tokio::test]
    async fn present_empty_password_stops_the_configured_environment_provider() {
        let io = rtdb_storage::IoRegion::new();
        let mut environment = BTreeMap::from([("AZURE_TOKEN_CREDENTIALS", "EnvironmentCredential".into()), ("AZURE_CLIENT_ID", "application".into()), ("AZURE_USERNAME", "user".into()), ("AZURE_PASSWORD", String::new())]);
        let bridge = CredentialBridge::from_environment(&environment, None, Arc::clone(&io)).unwrap();
        assert_eq!(bridge.sources.len(), 1);
        assert!(bridge.get_credential().await.is_err());
        environment.remove("AZURE_PASSWORD");
        assert!(CredentialBridge::from_environment(&environment, None, Arc::clone(&io)).is_err());
        environment.insert("AZURE_TOKEN_CREDENTIALS", "UsernamePasswordCredential".into());
        assert!(CredentialBridge::from_environment(&environment, None, Arc::clone(&io)).is_err());
        io.close_and_wait().await;
    }

    #[tokio::test]
    async fn configured_identity_failure_does_not_fall_through_to_another_identity() {
        let unused = Arc::new(AtomicUsize::new(0));
        let bridge = CredentialBridge { sources: vec![
            Candidate { credential: Arc::new(Source { calls: Arc::new(AtomicUsize::new(0)), available: false }), failure: FailurePolicy::Stop },
            Candidate { credential: Arc::new(Source { calls: Arc::clone(&unused), available: true }), failure: FailurePolicy::Stop },
        ], selected: Mutex::new(None) };
        assert!(bridge.get_credential().await.is_err());
        assert_eq!(unused.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn reachable_managed_identity_authentication_failure_does_not_select_developer_credentials() {
        let unused = Arc::new(AtomicUsize::new(0));
        let bridge = CredentialBridge { sources: vec![
            Candidate { credential: Arc::new(Source { calls: Arc::new(AtomicUsize::new(0)), available: false }), failure: FailurePolicy::ManagedUnavailable },
            Candidate { credential: Arc::new(Source { calls: Arc::clone(&unused), available: true }), failure: FailurePolicy::Continue },
        ], selected: Mutex::new(None) };
        assert!(bridge.get_credential().await.is_err());
        assert_eq!(unused.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unsupported_managed_client_id_allows_default_chain_developer_fallback_without_http() {
        use futures::FutureExt;
        for service_fabric in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
            let environment = if service_fabric {
                BTreeMap::from([("IDENTITY_ENDPOINT", endpoint), ("IDENTITY_HEADER", "secret".into()), ("IDENTITY_SERVER_THUMBPRINT", "thumbprint".into())])
            } else { BTreeMap::from([("MSI_ENDPOINT", endpoint)]) };
            let io = rtdb_storage::IoRegion::new();
            let managed = crate::managed::ManagedCredential::new(&environment, Some("client".into()), Arc::clone(&io)).unwrap();
            let bridge = CredentialBridge { sources: vec![
                Candidate { credential: managed.clone(), failure: FailurePolicy::ManagedUnavailable },
                Candidate { credential: Arc::new(Source { calls: Arc::new(AtomicUsize::new(0)), available: true }), failure: FailurePolicy::Continue },
            ], selected: Mutex::new(None) };
            assert!(matches!(bridge.get_credential().await.unwrap().as_ref(), AzureCredential::BearerToken(token) if token == "token-0"));
            assert!(listener.accept().now_or_never().is_none());
            let explicit = CredentialBridge { sources: vec![Candidate { credential: managed, failure: FailurePolicy::ManagedUnavailable }], selected: Mutex::new(None) };
            assert!(explicit.get_credential().await.is_err());
            assert!(listener.accept().now_or_never().is_none());
            io.close_and_wait().await;
        }
    }

    #[test]
    fn encrypted_pem_identity_keeps_its_key_and_certificate() {
        use openssl::{asn1::Asn1Time, hash::MessageDigest, pkey::PKey, rsa::Rsa, symm::Cipher, x509::{X509, X509NameBuilder}};
        let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", "rtdb-test").unwrap();
        let name = name.build();
        let mut identity = X509::builder().unwrap();
        identity.set_version(2).unwrap();
        identity.set_subject_name(&name).unwrap();
        identity.set_issuer_name(&name).unwrap();
        identity.set_pubkey(&key).unwrap();
        identity.set_not_before(&Asn1Time::days_from_now(0).unwrap()).unwrap();
        identity.set_not_after(&Asn1Time::days_from_now(1).unwrap()).unwrap();
        identity.sign(&key, MessageDigest::sha256()).unwrap();
        let identity = identity.build();
        let mut pem = identity.to_pem().unwrap();
        pem.extend(key.private_key_to_pem_pkcs8_passphrase(Cipher::aes_256_cbc(), b"password").unwrap());
        let result = certificate(&pem, Some(&Secret::new("password"))).unwrap();
        let parsed = openssl::pkcs12::Pkcs12::from_der(result.bytes()).unwrap().parse2("password").unwrap();
        assert_eq!(parsed.cert.unwrap().to_der().unwrap(), identity.to_der().unwrap());
        assert!(parsed.pkey.unwrap().public_eq(&key));
        assert!(certificate(&pem, Some(&Secret::new("wrong"))).is_err());
    }
}
