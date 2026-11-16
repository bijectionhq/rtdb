use std::{collections::BTreeMap, io::Read, path::{Component, Path, PathBuf}, sync::Arc, time::Duration};
use async_trait::async_trait;
use azure_core::{credentials::{AccessToken, TokenCredential, TokenRequestOptions}, error::ErrorKind, time::OffsetDateTime};
use futures::StreamExt;
use reqwest::{Client, Response, StatusCode, header::{HeaderMap, HeaderValue}};
use rtdb_storage::IoRegion;
use tokio::sync::Mutex;
use url::Url;

const RESOURCE: &str = "https://storage.azure.com";
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_CHALLENGE_BYTES: u64 = 4096;

enum Source {
    Imds { probe: bool },
    AppService { secret: HeaderValue },
    ServiceFabric { secret: HeaderValue },
    CloudShell,
    Arc { token_root: PathBuf },
}

struct Endpoint { url: Url, source: Source, client_id: Option<String> }

impl Endpoint {
    fn select(environment: &BTreeMap<&str, String>, client_id: Option<String>) -> azure_core::Result<Self> {
        let client_id = client_id.filter(|value| !value.is_empty());
        let get = |name| environment.get(name).filter(|value| !value.trim().is_empty()).map(String::as_str);
        let (url, source) = if let (Some(endpoint), Some(secret)) = (get("IDENTITY_ENDPOINT"), get("IDENTITY_HEADER")) {
            let secret = secret_header(secret)?;
            (endpoint.to_owned(), if get("IDENTITY_SERVER_THUMBPRINT").is_some() { Source::ServiceFabric { secret } } else { Source::AppService { secret } })
        } else if let Some(endpoint) = get("MSI_ENDPOINT") {
            (endpoint.to_owned(), Source::CloudShell)
        } else if let (Some(endpoint), Some(_)) = (get("IDENTITY_ENDPOINT"), get("IMDS_ENDPOINT")) {
            (endpoint.to_owned(), Source::Arc { token_root: arc_token_root(environment)? })
        } else {
            let host = get("AZURE_POD_IDENTITY_AUTHORITY_HOST").unwrap_or("http://169.254.169.254");
            (format!("{host}/metadata/identity/oauth2/token"), Source::Imds { probe: !get("AZURE_TOKEN_CREDENTIALS").is_some_and(|value| value.trim().eq_ignore_ascii_case("managedidentitycredential")) })
        };
        let mut url = Url::parse(&url).map_err(|_| invalid("Managed identity endpoint is not a valid URL"))?;
        if !matches!(url.scheme(), "http" | "https") || !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
            return Err(invalid("Managed identity endpoint requires HTTP or HTTPS without user info or a fragment"));
        }
        let version = match source {
            Source::Imds { .. } => Some("2018-02-01"), Source::AppService { .. } => Some("2019-08-01"),
            Source::ServiceFabric { .. } => Some("2019-07-01-preview"), Source::Arc { .. } => Some("2020-06-01"), Source::CloudShell => None,
        };
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("resource", RESOURCE);
            if let Some(version) = version { query.append_pair("api-version", version); }
            if let Some(client_id) = &client_id { query.append_pair("client_id", client_id); }
        }
        Ok(Self { url, source, client_id })
    }

    fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        match &self.source {
            Source::AppService { secret } => { headers.insert("x-identity-header", secret.clone()); }
            Source::ServiceFabric { secret } => { headers.insert("secret", secret.clone()); }
            Source::Imds { .. } | Source::Arc { .. } => { headers.insert("metadata", HeaderValue::from_static("true")); }
            Source::CloudShell => {
                headers.insert("metadata", HeaderValue::from_static("true"));
                headers.insert("contenttype", HeaderValue::from_static("application/x-www-form-urlencoded"));
            }
        }
        headers
    }

    fn retry_delay(&self, status: StatusCode, retry: usize) -> Option<Duration> {
        let status = status.as_u16();
        if matches!(self.source, Source::Imds { .. }) {
            if status == 410 { return (retry < 7).then_some(Duration::from_secs(10)); }
            ((matches!(status, 404 | 408 | 429) || (500..=599).contains(&status)) && retry < 3).then(|| Duration::from_secs(2 << retry))
        } else {
            (matches!(status, 404 | 408 | 429 | 500 | 503 | 504) && retry < 3).then_some(Duration::from_secs(1))
        }
    }
}

struct CachedToken { token: AccessToken, refresh_at: OffsetDateTime }

pub(crate) struct ManagedCredential { endpoint: Endpoint, client: Client, cached: Mutex<Option<CachedToken>>, io: Arc<IoRegion> }

impl std::fmt::Debug for ManagedCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("ManagedCredential([redacted])") }
}

impl ManagedCredential {
    pub fn new(environment: &BTreeMap<&str, String>, client_id: Option<String>, io: Arc<IoRegion>) -> azure_core::Result<Arc<Self>> {
        let endpoint = Endpoint::select(environment, client_id)?;
        let client = Client::builder().no_proxy().redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5)).timeout(Duration::from_secs(30))
            .build().map_err(|_| invalid("Managed identity HTTP client could not be created"))?;
        Ok(Arc::new(Self { endpoint, client, cached: Mutex::new(None), io }))
    }

    async fn request(&self, headers: HeaderMap) -> azure_core::Result<Response> {
        let mut retry = 0;
        loop {
            let response = self.client.get(self.endpoint.url.clone()).headers(headers.clone()).send().await
                .map_err(|_| invalid("Managed identity endpoint could not be reached"))?;
            let Some(delay) = self.endpoint.retry_delay(response.status(), retry) else { return Ok(response); };
            drop(response);
            tokio::time::sleep(delay).await;
            retry += 1;
        }
    }

    async fn acquire(&self) -> azure_core::Result<AccessToken> {
        let mut headers = self.endpoint.headers();
        let mut response = self.request(headers.clone()).await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            if let Source::Arc { token_root } = &self.endpoint.source {
                let challenge = response.headers().get("www-authenticate").and_then(|value| value.to_str().ok())
                    .ok_or_else(|| invalid("Azure Arc response has no valid authentication challenge"))?;
                let challenge = challenge.to_owned();
                let token_root = token_root.clone();
                drop(response);
                let authorization = self.io.run_blocking(move || Ok(read_arc_challenge(&challenge, &token_root))).await
                    .map_err(|_| invalid("Azure Arc challenge I/O could not complete"))??;
                headers.insert("authorization", authorization);
                response = self.request(headers).await?;
            }
        }
        let status = response.status();
        if response.content_length().is_some_and(|length| length > MAX_RESPONSE_BYTES as u64) { return Err(invalid("Managed identity response exceeds its byte limit")); }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| invalid("Managed identity response could not be read"))?;
            if bytes.len().checked_add(chunk.len()).is_none_or(|size| size > MAX_RESPONSE_BYTES) { return Err(invalid("Managed identity response exceeds its byte limit")); }
            bytes.extend_from_slice(&chunk);
        }
        if status != StatusCode::OK {
            if crate::managed_error::is_unavailable(&bytes, matches!(self.endpoint.source, Source::Imds { .. }) && matches!(status.as_u16(), 400 | 502 | 504)) {
                return Err(unavailable());
            }
            return Err(invalid(format!("Managed identity endpoint returned HTTP {}", status.as_u16())));
        }
        parse_token(&bytes, if matches!(self.endpoint.source, Source::Arc { .. }) { self.endpoint.client_id.as_deref() } else { None })
    }
}

#[async_trait]
impl TokenCredential for ManagedCredential {
    async fn get_token(&self, scopes: &[&str], _: Option<TokenRequestOptions<'_>>) -> azure_core::Result<AccessToken> {
        if scopes != ["https://storage.azure.com/.default"] { return Err(invalid("Managed storage identity requires the storage scope")); }
        if self.endpoint.client_id.is_some() && matches!(self.endpoint.source, Source::CloudShell | Source::ServiceFabric { .. }) {
            return Err(unavailable());
        }
        let mut cached = self.cached.lock().await;
        if matches!(self.endpoint.source, Source::Imds { probe: true }) {
            let mut url = self.endpoint.url.clone();
            url.set_query(Some("api-version=2018-02-01"));
            self.client.get(url).timeout(Duration::from_secs(1)).send().await.map_err(|_| unavailable())?;
        }
        let now = OffsetDateTime::now_utc();
        if let Some(cached) = cached.as_ref().filter(|cached| cached.refresh_at > now) { return Ok(cached.token.clone()); }
        let token = self.acquire().await?;
        let seconds = token.expires_on.unix_timestamp().saturating_sub(now.unix_timestamp());
        let refresh_seconds = if seconds > 7200 { seconds / 2 } else { seconds.saturating_sub(300) };
        let refresh_at = now.saturating_add(azure_core::time::Duration::seconds(refresh_seconds));
        *cached = Some(CachedToken { token: token.clone(), refresh_at });
        Ok(token)
    }
}

fn parse_token(bytes: &[u8], expected_client_id: Option<&str>) -> azure_core::Result<AccessToken> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| invalid("Managed identity returned an invalid token response"))?;
    let token = value["access_token"].as_str().filter(|token| !token.is_empty()).ok_or_else(|| invalid("Managed identity response has no access token"))?;
    if let Some(expected) = expected_client_id {
        if !value["client_id"].as_str().is_some_and(|actual| actual.eq_ignore_ascii_case(expected)) {
            return Err(invalid("Azure Arc did not confirm the requested user-assigned identity"));
        }
    }
    let expiry = value["expires_on"].as_i64().or_else(|| value["expires_on"].as_str().and_then(|value| value.parse::<i64>().ok()
        .or_else(|| chrono::DateTime::parse_from_rfc3339(value).ok().map(|value| value.timestamp()))))
        .ok_or_else(|| invalid("Managed identity returned an invalid expiration"))?;
    Ok(AccessToken::new(token.to_owned(), OffsetDateTime::from_unix_timestamp(expiry).map_err(|_| invalid("Managed identity expiration is out of range"))?))
}

fn arc_token_root(environment: &BTreeMap<&str, String>) -> azure_core::Result<PathBuf> {
    if cfg!(target_os = "linux") { return Ok(PathBuf::from("/var/opt/azcmagent/tokens")); }
    if cfg!(windows) {
        return environment.get("ProgramData").map(|root| Path::new(root).join("AzureConnectedMachineAgent/Tokens"))
            .ok_or_else(|| invalid("Azure Arc requires ProgramData on Windows"));
    }
    Err(invalid("Azure Arc supports Linux and Windows hosts"))
}

fn read_arc_challenge(challenge: &str, token_root: &Path) -> azure_core::Result<HeaderValue> {
    let mut parts = challenge.split('=');
    let _scheme = parts.next();
    let path = parts.next().filter(|_| parts.next().is_none()).ok_or_else(|| invalid("Azure Arc authentication challenge is malformed"))?;
    let mut normalized = PathBuf::new();
    for component in Path::new(path).components() {
        match component { Component::CurDir => (), Component::ParentDir => { if !normalized.pop() { return Err(invalid("Azure Arc challenge path escapes its root")); } }, other => normalized.push(other.as_os_str()) }
    }
    if !normalized.is_absolute() || !normalized.starts_with(token_root) || !normalized.extension().and_then(|extension| extension.to_str()).is_some_and(|extension| extension.eq_ignore_ascii_case("key")) {
        return Err(invalid("Azure Arc challenge must name a key within its token directory"));
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(normalized).map_err(|_| invalid("Azure Arc challenge file could not be opened"))?;
    let metadata = file.metadata().map_err(|_| invalid("Azure Arc challenge metadata could not be read"))?;
    if !metadata.is_file() || metadata.len() > MAX_CHALLENGE_BYTES { return Err(invalid("Azure Arc challenge requires a regular file within its byte limit")); }
    let mut bytes = Vec::new();
    file.take(MAX_CHALLENGE_BYTES + 1).read_to_end(&mut bytes).map_err(|_| invalid("Azure Arc challenge file could not be read"))?;
    if bytes.len() as u64 > MAX_CHALLENGE_BYTES { return Err(invalid("Azure Arc challenge exceeds its byte limit")); }
    secret_header(&format!("Basic {}", String::from_utf8_lossy(&bytes)))
}

fn secret_header(secret: &str) -> azure_core::Result<HeaderValue> {
    let mut value = HeaderValue::from_str(secret).map_err(|_| invalid("Managed identity secret is not a valid HTTP header"))?;
    value.set_sensitive(true);
    Ok(value)
}
fn invalid(message: impl Into<String>) -> azure_core::Error { azure_core::Error::with_message(ErrorKind::Credential, message.into()) }

#[derive(Debug)]
struct ManagedUnavailable;
impl std::fmt::Display for ManagedUnavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("Managed identity authentication is unavailable") }
}
impl std::error::Error for ManagedUnavailable {}
fn unavailable() -> azure_core::Error { azure_core::Error::new(ErrorKind::Credential, ManagedUnavailable) }
pub(crate) fn is_unavailable(error: &azure_core::Error) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(error) = current {
        if error.is::<ManagedUnavailable>() { return true; }
        current = error.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn env(values: &[(&'static str, &str)]) -> BTreeMap<&'static str, String> { values.iter().map(|(key, value)| (*key, (*value).into())).collect() }
    fn token_body() -> &'static str { r#"{"access_token":"token-secret","expires_on":"2099-01-01T00:00:00Z","client_id":"CLIENT"}"# }

    async fn receive(listener: &tokio::net::TcpListener, status: &str, extra_headers: &str, body: &str) -> String {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut buffer = [0; 1024];
            let size = stream.read(&mut buffer).await.unwrap();
            assert!(size > 0 && request.len() + size < 16 * 1024);
            request.extend_from_slice(&buffer[..size]);
            if request.windows(4).any(|value| value == b"\r\n\r\n") { break; }
        }
        stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n{body}", body.len()).as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
        String::from_utf8(request).unwrap()
    }

    #[test]
    fn endpoint_selection_follows_msal_precedence_and_cloud_shell_even_with_msi_secret() {
        let mut values = env(&[("IDENTITY_ENDPOINT", "http://identity/token"), ("IDENTITY_HEADER", "header-secret"),
            ("IDENTITY_SERVER_THUMBPRINT", "certificate-thumbprint"), ("MSI_ENDPOINT", "http://shell/token"), ("MSI_SECRET", "legacy-secret"), ("IMDS_ENDPOINT", "http://arc")]);
        let selected = Endpoint::select(&values, None).unwrap();
        assert!(matches!(selected.source, Source::ServiceFabric { .. }));
        assert_eq!(selected.headers()["secret"], "header-secret");
        assert!(Endpoint::select(&values, Some("client".into())).is_ok());
        let empty = Endpoint::select(&values, Some(String::new())).unwrap();
        assert!(empty.client_id.is_none());
        assert!(!empty.url.query_pairs().any(|(key, _)| key == "client_id"));
        values.remove("IDENTITY_SERVER_THUMBPRINT");
        let selected = Endpoint::select(&values, Some("client + /".into())).unwrap();
        assert!(matches!(selected.source, Source::AppService { .. }));
        assert_eq!(selected.headers()["x-identity-header"], "header-secret");
        assert_eq!(selected.url.query_pairs().find(|(key, _)| key == "client_id").unwrap().1, "client + /");
        values.remove("IDENTITY_HEADER");
        let selected = Endpoint::select(&values, None).unwrap();
        assert!(matches!(selected.source, Source::CloudShell));
        assert!(!selected.url.query_pairs().any(|(key, _)| key == "api-version"));
        assert_eq!(selected.headers()["contenttype"], "application/x-www-form-urlencoded");
        assert!(Endpoint::select(&values, Some("client".into())).is_ok());
    }

    #[test]
    fn imds_host_probe_selection_and_retry_limits_match_reference_policy() {
        let mut values = env(&[("AZURE_POD_IDENTITY_AUTHORITY_HOST", "http://pod")]);
        let selected = Endpoint::select(&values, None).unwrap();
        assert!(matches!(selected.source, Source::Imds { probe: true }));
        assert_eq!(selected.url.path(), "/metadata/identity/oauth2/token");
        assert_eq!(selected.retry_delay(StatusCode::GONE, 6), Some(Duration::from_secs(10)));
        assert_eq!(selected.retry_delay(StatusCode::GONE, 7), None);
        assert_eq!(selected.retry_delay(StatusCode::BAD_GATEWAY, 2), Some(Duration::from_secs(8)));
        assert_eq!(selected.retry_delay(StatusCode::BAD_GATEWAY, 3), None);
        assert_eq!(selected.retry_delay(StatusCode::UNAUTHORIZED, 0), None);
        values.insert("AZURE_TOKEN_CREDENTIALS", " ManagedIdentityCredential ".into());
        assert!(matches!(Endpoint::select(&values, None).unwrap().source, Source::Imds { probe: false }));
    }

    #[test]
    fn token_expiration_and_arc_principal_confirmation_are_independent_of_secret_text() {
        assert_eq!(parse_token(token_body().as_bytes(), Some("client")).unwrap().token.secret(), "token-secret");
        assert_eq!(parse_token(br#"{"access_token":"token-secret","expires_on":4070908800}"#, None).unwrap().expires_on.year(), 2099);
        for body in [r#"{"access_token":"token-secret","expires_on":"invalid"}"#, token_body()] {
            let error = parse_token(body.as_bytes(), Some("different-client")).unwrap_err();
            assert!(!error.to_string().contains("token-secret"));
        }
        assert!(parse_token(br#"{"access_token":"token-secret","expires_on":"4070908800"}"#, Some("client")).is_err());
        assert!(parse_token(br#"{"access_token":"token-secret","expires_on":"invalid"}"#, None).is_err());
        assert!(parse_token(br#"{"access_token":"","expires_on":"4070908800"}"#, None).is_err());
        assert!(is_unavailable(&unavailable()));
        assert!(!is_unavailable(&invalid("authentication failed")));
    }

    #[test]
    fn arc_challenge_paths_and_bytes_are_bounded_before_forming_authorization() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("tokens");
        std::fs::create_dir(&root).unwrap();
        let key = root.join("identity.key");
        std::fs::write(&key, b"challenge-secret").unwrap();
        let header = read_arc_challenge(&format!("Basic realm={}", key.display()), &root).unwrap();
        assert_eq!(header, "Basic challenge-secret");
        assert!(header.is_sensitive());
        for path in [root.join("../escape.key"), root.join("identity.txt"), temporary.path().join("tokens-other/key.key")] {
            assert!(read_arc_challenge(&format!("Basic realm={}", path.display()), &root).is_err());
        }
        assert!(read_arc_challenge("Basic realm=one=two", &root).is_err());
        std::fs::write(key, vec![b'a'; MAX_CHALLENGE_BYTES as usize + 1]).unwrap();
        assert!(read_arc_challenge(&format!("Basic realm={}", root.join("identity.key").display()), &root).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn arc_rejects_fifo_and_symlink_to_fifo_without_waiting_for_a_writer() {
        use std::os::unix::{ffi::OsStrExt, fs::symlink};
        let temporary = tempfile::tempdir().unwrap();
        let fifo = temporary.path().join("identity.key");
        let c_path = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        let link = temporary.path().join("linked.key");
        symlink(&fifo, &link).unwrap();
        let io = IoRegion::new();
        for path in [fifo, link] {
            let root = temporary.path().to_owned();
            let result = io.run_blocking(move || Ok(read_arc_challenge(&format!("Basic realm={}", path.display()), &root))).await.unwrap();
            assert!(result.unwrap_err().to_string().contains("regular file"));
        }
        io.close_and_wait().await;
    }

    #[tokio::test]
    async fn arc_challenge_does_not_start_file_io_after_the_database_region_closes() {
        let temporary = tempfile::tempdir().unwrap();
        let key = temporary.path().join("identity.key");
        std::fs::write(&key, b"challenge-secret").unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let io = IoRegion::new();
        io.close_and_wait().await;
        let credential = ManagedCredential { endpoint: Endpoint {
            url: Url::parse(&format!("http://{}/token", listener.local_addr().unwrap())).unwrap(),
            source: Source::Arc { token_root: temporary.path().to_owned() }, client_id: None,
        }, client: Client::builder().no_proxy().build().unwrap(), cached: Mutex::new(None), io };
        let challenge = format!("WWW-Authenticate: Basic realm={}\r\n", key.display());
        let server = receive(&listener, "401 Unauthorized", &challenge, "");
        let caller = credential.get_token(&["https://storage.azure.com/.default"], None);
        let (_, result) = tokio::join!(server, caller);
        let error = result.unwrap_err().to_string();
        assert!(error.contains("I/O could not complete"));
        assert!(!error.contains("challenge-secret") && !error.contains(&key.display().to_string()));
    }

    #[tokio::test]
    async fn app_service_uses_secret_header_and_reuses_a_valid_token() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
        let credential = ManagedCredential::new(&env(&[("IDENTITY_ENDPOINT", &endpoint), ("IDENTITY_HEADER", "header-secret")]), Some("client".into()), IoRegion::new()).unwrap();
        let server = receive(&listener, "200 OK", "", token_body());
        let caller = async {
            for _ in 0..2 { assert_eq!(credential.get_token(&["https://storage.azure.com/.default"], None).await.unwrap().token.secret(), "token-secret"); }
        };
        let (request, ()) = tokio::join!(server, caller);
        assert!(request.starts_with("GET /token?"));
        assert!(request.contains("x-identity-header: header-secret\r\n"));
        assert!(request.contains("api-version=2019-08-01") && request.contains("client_id=client"));
        assert!(!format!("{credential:?}").contains("header-secret"));
    }

    #[tokio::test]
    async fn default_imds_probes_without_metadata_before_cached_and_uncached_tokens() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let credential = ManagedCredential::new(&env(&[("AZURE_POD_IDENTITY_AUTHORITY_HOST", &endpoint)]), None, IoRegion::new()).unwrap();
        let server = async {
            let first = receive(&listener, "400 Bad Request", "", "{}").await;
            let token = receive(&listener, "200 OK", "", token_body()).await;
            let cached = receive(&listener, "400 Bad Request", "", "{}").await;
            (first, token, cached)
        };
        let caller = async { for _ in 0..2 { credential.get_token(&["https://storage.azure.com/.default"], None).await.unwrap(); } };
        let ((first, token, cached), ()) = tokio::join!(server, caller);
        for probe in [first, cached] {
            assert!(probe.starts_with("GET /metadata/identity/oauth2/token?api-version=2018-02-01 HTTP/1.1"));
            assert!(!probe.contains("metadata: true") && !probe.contains("resource="));
        }
        assert!(token.contains("metadata: true\r\n") && token.contains("resource="));
    }

    #[tokio::test]
    async fn arc_challenge_retries_the_same_endpoint_and_confirms_the_returned_identity() {
        let temporary = tempfile::tempdir().unwrap();
        let key = temporary.path().join("identity.key");
        std::fs::write(&key, b"challenge-secret").unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let credential = ManagedCredential { endpoint: Endpoint {
            url: Url::parse(&format!("http://{}/token?api-version=2020-06-01&client_id=client", listener.local_addr().unwrap())).unwrap(),
            source: Source::Arc { token_root: temporary.path().to_owned() }, client_id: Some("client".into()),
        }, client: Client::builder().no_proxy().build().unwrap(), cached: Mutex::new(None), io: IoRegion::new() };
        let server = async {
            let initial = receive(&listener, "401 Unauthorized", &format!("WWW-Authenticate: Basic realm={}\r\n", key.display()), "").await;
            let retry = receive(&listener, "200 OK", "", token_body()).await;
            (initial, retry)
        };
        let caller = credential.get_token(&["https://storage.azure.com/.default"], None);
        let ((initial, retry), token) = tokio::join!(server, caller);
        assert_eq!(token.unwrap().token.secret(), "token-secret");
        assert!(!initial.contains("authorization:"));
        assert!(retry.contains("authorization: Basic challenge-secret\r\n"));
        assert_eq!(initial.lines().next(), retry.lines().next());
    }

    #[tokio::test]
    async fn oversized_http_token_response_is_rejected_from_its_headers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
        let credential = ManagedCredential::new(&env(&[("MSI_ENDPOINT", &endpoint)]), None, IoRegion::new()).unwrap();
        let server = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            assert!(stream.read(&mut buffer).await.unwrap() > 0);
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", MAX_RESPONSE_BYTES + 1).as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        };
        let caller = credential.get_token(&["https://storage.azure.com/.default"], None);
        let ((), token) = tokio::join!(server, caller);
        assert!(token.unwrap_err().to_string().contains("byte limit"));
    }
}
