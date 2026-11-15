use std::sync::Arc;
use async_trait::async_trait;
use google_cloud_auth::credentials::{CacheableResource, Credentials};
use object_store::{ClientOptions, CredentialProvider, client::{HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpService, ReqwestConnector}, gcp::GcpCredential};

#[derive(Clone)]
pub(crate) struct CredentialBridge { credentials: Credentials }

impl std::fmt::Debug for CredentialBridge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("GcsCredentialBridge") }
}

impl CredentialBridge {
    pub fn new(credentials: Credentials) -> Self { Self { credentials } }
    async fn headers(&self) -> object_store::Result<http::HeaderMap> {
        match self.credentials.headers(http::Extensions::new()).await.map_err(|source| object_store::Error::Generic { store: "GCS", source: Box::new(source) })? {
            CacheableResource::New { data, .. } => Ok(data),
            CacheableResource::NotModified => Err(object_store::Error::Generic { store: "GCS", source: "Credential provider omitted headers without a cache tag".into() }),
        }
    }
}

#[async_trait]
impl CredentialProvider for CredentialBridge {
    type Credential = GcpCredential;
    async fn get_credential(&self) -> object_store::Result<Arc<GcpCredential>> {
        let headers = self.headers().await?;
        let bearer = headers.get(http::header::AUTHORIZATION).and_then(|value| value.to_str().ok()).and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| object_store::Error::Generic { store: "GCS", source: "ADC did not return a bearer token".into() })?;
        Ok(Arc::new(GcpCredential { bearer: bearer.into() }))
    }
}

impl HttpConnector for CredentialBridge {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(AuthenticatedClient { http: ReqwestConnector::default().connect(options)?, credentials: self.clone() }))
    }
}

#[derive(Debug)]
struct AuthenticatedClient { http: HttpClient, credentials: CredentialBridge }

#[async_trait]
impl HttpService for AuthenticatedClient {
    async fn call(&self, mut request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        let headers = self.credentials.headers().await.map_err(|source| HttpError::new(HttpErrorKind::Unknown, source))?;
        request.headers_mut().extend(headers);
        self.http.execute(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_auth::credentials::{CredentialsProvider as GoogleCredentialsProvider, EntityTag};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct Rotating(Arc<AtomicUsize>);
    impl GoogleCredentialsProvider for Rotating {
        async fn headers(&self, _: http::Extensions) -> std::result::Result<CacheableResource<http::HeaderMap>, google_cloud_auth::errors::CredentialsError> {
            let call = self.0.fetch_add(1, Ordering::SeqCst);
            let mut data = http::HeaderMap::new();
            data.insert(http::header::AUTHORIZATION, format!("Bearer token-{call}").parse().unwrap());
            data.insert("x-goog-user-project", "billing-project".parse().unwrap());
            Ok(CacheableResource::New { data, entity_tag: EntityTag::new() })
        }
        async fn universe_domain(&self) -> Option<String> { None }
    }

    #[tokio::test]
    async fn adc_token_refresh_and_quota_project_headers_are_retained() {
        let calls = Arc::new(AtomicUsize::new(0));
        let bridge = CredentialBridge::new(Credentials::from(Rotating(Arc::clone(&calls))));
        assert_eq!(bridge.get_credential().await.unwrap().bearer, "token-0");
        assert_eq!(bridge.get_credential().await.unwrap().bearer, "token-1");
        let headers = bridge.headers().await.unwrap();
        assert_eq!(headers["authorization"], "Bearer token-2");
        assert_eq!(headers["x-goog-user-project"], "billing-project");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
}
