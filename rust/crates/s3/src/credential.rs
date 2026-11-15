use std::sync::Arc;
use std::time::{Duration, SystemTime};
use async_trait::async_trait;
use aws_credential_types::{Credentials, provider::{ProvideCredentials, SharedCredentialsProvider}};
use object_store::{CredentialProvider, aws::AwsCredential};
use tokio::sync::Mutex;

pub(crate) struct CredentialBridge { provider: SharedCredentialsProvider, cached: Mutex<Option<Credentials>> }

impl std::fmt::Debug for CredentialBridge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("AwsCredentialBridge") }
}

impl CredentialBridge {
    pub fn new(provider: SharedCredentialsProvider) -> Self { Self { provider, cached: Mutex::new(None) } }
}

#[async_trait]
impl CredentialProvider for CredentialBridge {
    type Credential = AwsCredential;
    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let mut cached = self.cached.lock().await;
        let fresh_until = SystemTime::now().checked_add(Duration::from_secs(60)).unwrap_or(SystemTime::now());
        if cached.as_ref().is_none_or(|credentials| credentials.expiry().is_some_and(|expiry| expiry <= fresh_until)) {
            *cached = Some(self.provider.provide_credentials().await.map_err(|source| object_store::Error::Generic { store: "S3", source: Box::new(source) })?);
        }
        let credentials = cached.as_ref().ok_or_else(|| object_store::Error::Generic { store: "S3", source: "AWS provider returned no credentials".into() })?;
        Ok(Arc::new(AwsCredential { key_id: credentials.access_key_id().into(), secret_key: credentials.secret_access_key().into(), token: credentials.session_token().map(str::to_owned) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use aws_credential_types::provider::future;

    #[derive(Debug)]
    struct Rotating(Arc<AtomicUsize>);
    impl ProvideCredentials for Rotating {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a> where Self: 'a {
            future::ProvideCredentials::new(async move {
                let call = self.0.fetch_add(1, Ordering::SeqCst);
                let expiry = SystemTime::now() + Duration::from_secs(if call == 0 { 30 } else { 3600 });
                Ok(Credentials::new(format!("key-{call}"), "secret", Some(format!("token-{call}")), Some(expiry), "test"))
            })
        }
    }

    #[tokio::test]
    async fn expiring_credentials_refresh_and_unexpired_credentials_share_the_cache() {
        let calls = Arc::new(AtomicUsize::new(0));
        let bridge = CredentialBridge::new(SharedCredentialsProvider::new(Rotating(Arc::clone(&calls))));
        assert_eq!(bridge.get_credential().await.unwrap().token.as_deref(), Some("token-0"));
        assert_eq!(bridge.get_credential().await.unwrap().token.as_deref(), Some("token-1"));
        assert_eq!(bridge.get_credential().await.unwrap().key_id, "key-1");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!format!("{bridge:?}").contains("secret"));
    }
}
