use async_trait::async_trait;
use bytes::Bytes;
use object_store::{
    ClientOptions, ObjectStore, RetryConfig,
    client::{HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpService},
};
use std::sync::Arc;

pub fn open<C: HttpConnector, P: ObjectStore>(
    connector: C, build: impl FnOnce(CacheConnector<C>, RetryConfig) -> object_store::Result<P>,
) -> object_store::Result<Arc<P>> {
    // The SDK's streaming retry validates only the ETag, which can be shared by distinct versions.
    let retry = RetryConfig { max_retries: 0, ..RetryConfig::default() };
    build(CacheConnector(connector), retry).map(Arc::new)
}

#[derive(Debug)]
pub struct CacheConnector<C>(C);

impl<C: HttpConnector> HttpConnector for CacheConnector<C> {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let options = options.clone().with_http2_max_frame_size(16 * 1024);
        Ok(HttpClient::new(CacheHttp(self.0.connect(&options)?)))
    }
}

#[derive(Debug)]
struct CacheHttp(HttpClient);

#[async_trait]
impl HttpService for CacheHttp {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let response = self.0.execute(request).await?;
        if response.status().as_u16() == 200 { return Ok(response); }
        if response.status().is_success() {
            return Err(HttpError::new(HttpErrorKind::Decode, std::io::Error::other("Expected a full object response")));
        }
        // The SDK otherwise collects the entire failure body to format its error.
        let (parts, _) = response.into_parts();
        Ok(HttpResponse::from_parts(parts, Bytes::new().into()))
    }
}

#[cfg(test)]
mod tests {
    use super::CacheHttp;
    use async_trait::async_trait;
    use bytes::Bytes;
    use object_store::client::{HttpClient, HttpError, HttpErrorKind, HttpRequest, HttpRequestBody, HttpResponse, HttpService};

    #[derive(Debug)]
    struct Reply(u16);

    #[async_trait]
    impl HttpService for Reply {
        async fn call(&self, _: HttpRequest) -> Result<HttpResponse, HttpError> {
            let mut response = HttpResponse::new(Bytes::from_static(b"provider body").into());
            *response.status_mut() = self.0.try_into().unwrap();
            Ok(response)
        }
    }

    #[tokio::test]
    async fn cache_http_exposes_only_full_object_bodies_to_the_sdk() {
        let request = || HttpRequest::new(HttpRequestBody::empty());
        let full = CacheHttp(HttpClient::new(Reply(200))).call(request()).await.unwrap();
        assert_eq!(full.into_body().bytes().await.unwrap().as_ref(), b"provider body");
        let partial = CacheHttp(HttpClient::new(Reply(206))).call(request()).await.unwrap_err();
        assert_eq!(partial.kind(), HttpErrorKind::Decode);
        let forbidden = CacheHttp(HttpClient::new(Reply(403))).call(request()).await.unwrap();
        assert_eq!(forbidden.status().as_u16(), 403);
        assert!(forbidden.into_body().bytes().await.unwrap().is_empty());
    }
}
