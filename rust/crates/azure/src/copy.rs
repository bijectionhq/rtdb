use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use object_store::{
    ClientOptions, RetryConfig,
    azure::{AzureAuthorizer, AzureCredentialProvider},
    client::{HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpRequestBody, HttpResponse, HttpService, ReqwestConnector},
};
use rtdb_storage::remote::CopyOutcomeUnknown;

#[derive(Clone)]
pub(super) struct CopyConnector {
    account: String,
    credentials: AzureCredentialProvider,
}

impl CopyConnector {
    pub(super) fn new(account: String, credentials: AzureCredentialProvider) -> Self {
        Self { account, credentials }
    }
}

impl fmt::Debug for CopyConnector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("CopyConnector").finish_non_exhaustive()
    }
}

impl HttpConnector for CopyConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(CopyService {
            upstream: ReqwestConnector::default().connect(options)?,
            authorization: self.clone(),
        }))
    }
}

#[derive(Debug)]
struct CopyService {
    upstream: HttpClient,
    authorization: CopyConnector,
}

#[async_trait]
impl HttpService for CopyService {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        if request.method() == "HEAD" {
            let response = self.upstream.execute(request).await?;
            if response.status().is_success() && copy_status(&response) == Some(b"pending") {
                return Err(unknown("the destination copy is still pending"));
            }
            return Ok(response);
        }
        if request.method() != "PUT" || !request.headers().contains_key("x-ms-copy-source") {
            return self.upstream.execute(request).await;
        }
        let destination = request.uri().to_string();
        // The copy request already contains any SAS query parameters. The HEAD will be authorized independently.
        let (destination, _) = destination.split_once('?').unwrap_or((&destination, ""));
        let response = self.upstream.execute(request).await
            .map_err(|_| unknown("the copy request did not return a response"))?;
        if response.status().is_server_error() || response.status().as_u16() == 408 {
            return Err(unknown("the provider could not confirm whether copying began"));
        }
        if !response.status().is_success() { return Ok(response); }
        match copy_status(&response) {
            Some(b"success") => Ok(response),
            Some(b"pending") => {
                let id = response.headers().get("x-ms-copy-id")
                    .ok_or_else(|| unknown("the pending copy has no identifier"))?;
                let timeout = RetryConfig::default().retry_timeout;
                tokio::time::timeout(timeout, self.wait_for_copy(destination, id.as_bytes())).await
                    .map_err(|_| unknown("the provider copy is still pending"))??;
                Ok(response)
            }
            _ => Err(unknown("the provider did not report the copy status")),
        }
    }
}

impl CopyService {
    async fn wait_for_copy(&self, destination: &str, copy_id: &[u8]) -> Result<(), HttpError> {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let credential = self.authorization.credentials.get_credential().await
                .map_err(|_| unknown("could not authorize a copy status check"))?;
            let mut request = HttpRequest::new(HttpRequestBody::empty());
            *request.method_mut() = "HEAD".parse().unwrap();
            *request.uri_mut() = destination.parse().map_err(|_| unknown("invalid copy destination"))?;
            AzureAuthorizer::new(credential.as_ref(), &self.authorization.account).authorize(&mut request);
            let response = self.upstream.execute(request).await
                .map_err(|_| unknown("the copy status check did not return a response"))?;
            if !response.status().is_success() {
                return Err(unknown("the provider could not read the copy status"));
            }
            if response.headers().get("x-ms-copy-id").map(|id| id.as_bytes()) != Some(copy_id) {
                return Err(unknown("the provider reported a different copy operation"));
            }
            match copy_status(&response) {
                Some(b"success") => return Ok(()),
                Some(b"pending") => (),
                Some(b"failed" | b"aborted") => {
                    return Err(HttpError::new(HttpErrorKind::Unknown, std::io::Error::other("Azure copy failed")));
                }
                _ => return Err(unknown("the provider did not report the copy status")),
            }
        }
    }
}

fn copy_status(response: &HttpResponse) -> Option<&[u8]> {
    response.headers().get("x-ms-copy-status").map(|value| value.as_bytes())
}

fn unknown(message: &'static str) -> HttpError {
    HttpError::new(HttpErrorKind::Unknown, CopyOutcomeUnknown(message))
}
