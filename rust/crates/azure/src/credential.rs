use std::collections::BTreeMap;
use std::sync::Arc;
use async_trait::async_trait;
use azure_core::credentials::{Secret, SecretBytes, TokenCredential};
use azure_identity::{AzureCliCredential, AzureCliCredentialOptions, AzureDeveloperCliCredential, AzureDeveloperCliCredentialOptions, ClientCertificateCredential, ClientCertificateCredentialOptions, ClientSecretCredential, ManagedIdentityCredential, ManagedIdentityCredentialOptions, UserAssignedId, WorkloadIdentityCredential};
use object_store::{CredentialProvider, azure::AzureCredential};
use rtdb_types::{Anomaly, Result};
use tokio::sync::Mutex;

struct Candidate { credential: Arc<dyn TokenCredential>, continue_on_error: bool }

pub(crate) struct CredentialBridge { sources: Vec<Candidate>, selected: Mutex<Option<usize>> }

impl std::fmt::Debug for CredentialBridge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("AzureCredentialBridge") }
}

impl CredentialBridge {
    pub fn new(managed_identity_client_id: Option<&str>) -> Result<Self> {
        let environment: BTreeMap<_, _> = ["AZURE_TOKEN_CREDENTIALS", "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_CLIENT_CERTIFICATE_PATH", "AZURE_CLIENT_CERTIFICATE_PASSWORD", "AZURE_FEDERATED_TOKEN_FILE", "IDENTITY_ENDPOINT", "MSI_ENDPOINT"]
            .into_iter().filter_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()).map(|value| (name, value))).collect();
        let selection = environment.get("AZURE_TOKEN_CREDENTIALS").map(|value| value.trim().to_ascii_lowercase()).unwrap_or_default();
        if !matches!(selection.as_str(), "" | "prod" | "dev" | "environmentcredential" | "workloadidentitycredential" | "managedidentitycredential" | "azureclicredential" | "azurepowershellcredential" | "azuredeveloperclicredential") {
            return Err(Anomaly::unsupported("xtdb.azure/credential-selector", "Selected Azure credential has no native adapter"));
        }
        let mut sources = Vec::new();
        if let (Some(tenant), Some(client)) = (environment.get("AZURE_TENANT_ID"), environment.get("AZURE_CLIENT_ID")) {
            if let Some(secret) = environment.get("AZURE_CLIENT_SECRET").filter(|_| matches!(selection.as_str(), "" | "prod" | "environmentcredential")) {
                sources.push(Candidate { credential: ClientSecretCredential::new(tenant, client.clone(), Secret::new(secret.clone()), None).map_err(invalid)?, continue_on_error: false });
            } else if let Some(path) = environment.get("AZURE_CLIENT_CERTIFICATE_PATH").filter(|_| matches!(selection.as_str(), "" | "prod" | "environmentcredential")) {
                let options = ClientCertificateCredentialOptions { password: environment.get("AZURE_CLIENT_CERTIFICATE_PASSWORD").cloned().map(Secret::new), ..Default::default() };
                let certificate = certificate(&std::fs::read(path).map_err(invalid)?, options.password.as_ref())?;
                sources.push(Candidate { credential: ClientCertificateCredential::new(tenant.clone(), client.clone(), certificate, Some(options)).map_err(invalid)?, continue_on_error: false });
            }
            if !sources.is_empty() { return Ok(Self { sources, selected: Mutex::new(None) }); }
            if environment.contains_key("AZURE_FEDERATED_TOKEN_FILE") && matches!(selection.as_str(), "" | "prod" | "workloadidentitycredential") {
                sources.push(Candidate { credential: WorkloadIdentityCredential::new(None).map_err(invalid)?, continue_on_error: false });
                return Ok(Self { sources, selected: Mutex::new(None) });
            }
        }
        if matches!(selection.as_str(), "environmentcredential" | "workloadidentitycredential") { return Err(invalid("Selected Azure credential requires complete environment configuration")); }
        let managed_id = managed_identity_client_id.map(str::to_owned).or_else(|| environment.get("AZURE_CLIENT_ID").cloned());
        if matches!(selection.as_str(), "" | "prod" | "managedidentitycredential") {
            let managed = ManagedIdentityCredential::new(Some(ManagedIdentityCredentialOptions { user_assigned_id: managed_id.map(UserAssignedId::ClientId), ..Default::default() })).map_err(invalid)?;
            let explicit_managed = environment.contains_key("IDENTITY_ENDPOINT") || environment.contains_key("MSI_ENDPOINT");
            sources.push(Candidate { credential: managed, continue_on_error: !explicit_managed && selection.is_empty() });
        }
        let executor: Arc<dyn azure_identity::Executor> = Arc::new(crate::process::JoinedExecutor);
        if matches!(selection.as_str(), "" | "dev" | "azureclicredential") {
            sources.push(Candidate { credential: AzureCliCredential::new(Some(AzureCliCredentialOptions { executor: Some(Arc::clone(&executor)), tenant_id: environment.get("AZURE_TENANT_ID").cloned(), ..Default::default() })).map_err(invalid)?, continue_on_error: selection != "azureclicredential" });
        }
        if matches!(selection.as_str(), "" | "dev" | "azurepowershellcredential") {
            sources.push(Candidate { credential: crate::powershell::PowerShellCredential::new(Arc::clone(&executor), environment.get("AZURE_TENANT_ID").cloned()).map_err(invalid)?, continue_on_error: selection != "azurepowershellcredential" });
        }
        if matches!(selection.as_str(), "" | "dev" | "azuredeveloperclicredential") {
            sources.push(Candidate { credential: AzureDeveloperCliCredential::new(Some(AzureDeveloperCliCredentialOptions { executor: Some(executor), tenant_id: environment.get("AZURE_TENANT_ID").cloned(), ..Default::default() })).map_err(invalid)?, continue_on_error: false });
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
                Err(error) if !source.continue_on_error => return Err(provider_error(error)),
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
            Candidate { credential: Arc::new(Source { calls: Arc::clone(&unavailable), available: false }), continue_on_error: true },
            Candidate { credential: Arc::new(Source { calls: Arc::clone(&available), available: true }), continue_on_error: false },
        ], selected: Mutex::new(None) };
        assert!(matches!(bridge.get_credential().await.unwrap().as_ref(), AzureCredential::BearerToken(token) if token == "token-0"));
        assert!(matches!(bridge.get_credential().await.unwrap().as_ref(), AzureCredential::BearerToken(token) if token == "token-1"));
        assert_eq!(unavailable.load(Ordering::SeqCst), 1);
        assert_eq!(available.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn configured_identity_failure_does_not_fall_through_to_another_identity() {
        let unused = Arc::new(AtomicUsize::new(0));
        let bridge = CredentialBridge { sources: vec![
            Candidate { credential: Arc::new(Source { calls: Arc::new(AtomicUsize::new(0)), available: false }), continue_on_error: false },
            Candidate { credential: Arc::new(Source { calls: Arc::clone(&unused), available: true }), continue_on_error: false },
        ], selected: Mutex::new(None) };
        assert!(bridge.get_credential().await.is_err());
        assert_eq!(unused.load(Ordering::SeqCst), 0);
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
