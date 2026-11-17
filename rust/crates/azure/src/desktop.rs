use std::{collections::BTreeMap, sync::Arc, time::Duration};
use async_trait::async_trait;
use base64::Engine;
use azure_core::{credentials::{AccessToken, Secret, TokenCredential, TokenRequestOptions}, error::ErrorKind, time::OffsetDateTime};
use futures::StreamExt;
use rtdb_storage::IoRegion;
use tokio::sync::Mutex;
#[cfg(test)]
use url::Url;

mod cache;
mod json;
mod authority;
mod federation;
mod password;
pub(crate) use password::PasswordCredential;

const CLIENT_ID: &str = "04b07795-8ddb-461a-bbee-02f9e1bf7b46";
const STORAGE_SCOPE: &str = "https://storage.azure.com/.default";
const GRANTED_SCOPES: &str = "https://storage.azure.com/.default openid profile offline_access";
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

trait RefreshTokenCache: Send + Sync {
    fn read(&self) -> azure_core::Result<Secret>;
}

#[derive(Clone)]
struct Session { token: AccessToken, refresh_token: Option<Secret>, refresh_at: Option<OffsetDateTime>, account: Option<AccountId>, scopes: String, family: Option<String> }

impl Session {
    fn usable_at(&self, now: OffsetDateTime) -> bool { self.token.expires_on.unix_timestamp() >= now.unix_timestamp() + 300 }
    fn proactive_at(&self, now: OffsetDateTime) -> bool { self.refresh_at.is_some_and(|refresh_at| refresh_at.unix_timestamp() < now.unix_timestamp()) }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum AccountId { Value(String), Missing }

impl AccountId { fn bytes(&self) -> usize { match self { Self::Value(value) => value.len(), Self::Missing => 0 } } }

fn account_key(account: Option<&AccountId>) -> &str { match account { Some(AccountId::Value(value)) => value, _ => "null" } }
fn same_account_key(left: Option<&AccountId>, right: Option<&AccountId>) -> bool { account_key(left).to_lowercase() == account_key(right).to_lowercase() }
fn same_refresh_key(client_id: &str, left: &(Option<AccountId>, Option<String>), right: &(Option<AccountId>, Option<String>)) -> bool {
    same_account_key(left.0.as_ref(), right.0.as_ref()) && left.1.as_deref().unwrap_or(client_id).to_lowercase() == right.1.as_deref().unwrap_or(client_id).to_lowercase()
}

struct Sessions { client_id: String, last_account: Option<Option<AccountId>>, tokens: Vec<Session>, refresh_tokens: Vec<((Option<AccountId>, Option<String>), Secret)>, accounts: Vec<AccountId>, family: bool }

impl Default for Sessions { fn default() -> Self { Self::for_client_id(CLIENT_ID.into()) } }

impl Sessions {
    fn for_client_id(client_id: String) -> Self {
        Self { client_id, last_account: None, tokens: Vec::new(), refresh_tokens: Vec::new(), accounts: Vec::new(), family: false }
    }

    fn store(&mut self, session: &Session) -> azure_core::Result<()> {
        let replacing = self.tokens.iter().position(|token| same_account_key(token.account.as_ref(), session.account.as_ref()) && token.scopes.to_lowercase() == session.scopes.to_lowercase());
        let refresh_key = (session.account.clone(), session.family.clone());
        let replacing_refresh = self.refresh_tokens.iter().position(|(key, _)| same_refresh_key(&self.client_id, key, &refresh_key));
        let replacing_account = session.account.as_ref().and_then(|account| self.accounts.iter().position(|cached| same_account_key(Some(cached), Some(account))));
        let token_bytes: usize = self.tokens.iter().enumerate().filter(|(index, _)| Some(*index) != replacing).map(|(_, token)| token.token.token.secret().len() + token.scopes.len() + token.account.as_ref().map_or(0, AccountId::bytes)).sum();
        let refresh_bytes: usize = self.refresh_tokens.iter().enumerate().filter(|(index, _)| session.refresh_token.is_none() || Some(*index) != replacing_refresh).map(|(_, ((account, family), token))| account.as_ref().map_or(0, AccountId::bytes) + family.as_ref().map_or(0, String::len) + token.secret().len()).sum();
        let account_bytes: usize = self.accounts.iter().map(AccountId::bytes).sum();
        let last_account_bytes = self.last_account.as_ref().and_then(Option::as_ref).map_or(0, AccountId::bytes)
            .max(session.account.as_ref().map_or(0, AccountId::bytes));
        let new_bytes = session.token.token.secret().len() + session.scopes.len() + session.refresh_token.as_ref().map_or(0, |token| token.secret().len()) + 3 * session.account.as_ref().map_or(0, AccountId::bytes) + 2 * session.family.as_ref().map_or(0, String::len);
        if self.client_id.len() + token_bytes + refresh_bytes + account_bytes + last_account_bytes + new_bytes > 8 * MAX_RESPONSE_BYTES
            || (replacing.is_none() && self.tokens.len() >= 64)
            || (session.refresh_token.is_some() && replacing_refresh.is_none() && self.refresh_tokens.len() >= 64)
            || (session.account.is_some() && replacing_account.is_none() && self.accounts.len() >= 64) {
            return Err(invalid("Azure public client token cache exceeds its limit"));
        }
        if let Some(account) = &session.account {
            if let Some(index) = replacing_account { self.accounts[index] = account.clone(); } else { self.accounts.push(account.clone()); }
        }
        if session.family.is_some() { self.family = true; }
        if let Some(refresh_token) = &session.refresh_token {
            let entry = (refresh_key, refresh_token.clone());
            if let Some(index) = replacing_refresh { self.refresh_tokens[index] = entry; } else { self.refresh_tokens.push(entry); }
        }
        if !java_blank(session.token.token.secret()) {
            let mut cached = session.clone();
            cached.refresh_token = None;
            cached.family = None;
            if let Some(index) = replacing { self.tokens[index] = cached; } else { self.tokens.push(cached); }
        }
        Ok(())
    }

    fn refresh_token(&self, account: &Option<AccountId>) -> Option<&Secret> {
        if self.family {
            if let Some((_, secret)) = self.refresh_tokens.iter().find(|((owner, family), _)| owner == account && family.is_some()) { return Some(secret); }
        }
        self.refresh_tokens.iter().find(|((owner, _), _)| owner == account).map(|(_, secret)| secret)
    }

    fn lookup(&self, account: &Option<AccountId>, now: OffsetDateTime) -> Option<Session> {
        let mut session = self.tokens.iter().find(|token| {
            token.scopes.split(' ').any(|scope| scope.eq_ignore_ascii_case(STORAGE_SCOPE))
                && match account { Some(account) => token.account.as_ref() == Some(account), None => token.token.expires_on.unix_timestamp() > now.unix_timestamp() + 300 }
        })?.clone();
        session.account = account.clone().or_else(|| self.accounts.first().cloned());
        Some(session)
    }
}

enum ExchangeFailure { Service(azure_core::Error), Other(azure_core::Error) }
impl From<azure_core::Error> for ExchangeFailure { fn from(error: azure_core::Error) -> Self { Self::Other(error) } }
impl ExchangeFailure { fn error(self) -> azure_core::Error { match self { Self::Service(error) | Self::Other(error) => error } } }

pub(crate) struct IntelliJCredential {
    authority: authority::Authority,
    client: reqwest::Client,
    cache: Arc<dyn RefreshTokenCache>,
    session: Mutex<Sessions>,
    io: Arc<IoRegion>,
}

impl std::fmt::Debug for IntelliJCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("IntelliJCredential([redacted])") }
}

impl IntelliJCredential {
    pub fn new(environment: &BTreeMap<&str, String>, io: Arc<IoRegion>) -> azure_core::Result<Arc<Self>> {
        let tenant = environment.get("AZURE_TENANT_ID").map(String::as_str).unwrap_or("common");
        let host = environment.get("AZURE_AUTHORITY_HOST").map(String::as_str).unwrap_or("https://login.microsoftonline.com");
        let client = Self::client()?;
        let authority = authority::Authority::new(host, tenant, client.clone())?;
        Ok(Arc::new(Self { authority, client, cache: Arc::new(cache::NativeCache::new()), session: Mutex::new(Sessions::default()), io }))
    }

    #[cfg(test)]
    fn with_cache(endpoint: Url, cache: Arc<dyn RefreshTokenCache>, io: Arc<IoRegion>) -> azure_core::Result<Arc<Self>> {
        Ok(Arc::new(Self { authority: authority::Authority::static_endpoint(endpoint), client: Self::client()?, cache, session: Mutex::new(Sessions::default()), io }))
    }

    fn client() -> azure_core::Result<reqwest::Client> {
        reqwest::Client::builder().redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5)).timeout(Duration::from_secs(30)).build()
            .map_err(|_| invalid("IntelliJ credential HTTP client could not be created"))
    }

    async fn exchange(&self, refresh_token: &Secret) -> Result<Session, ExchangeFailure> {
        let endpoint = self.authority.token_endpoint().await?;
        let body = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("client_id", CLIENT_ID)
            .append_pair("grant_type", "refresh_token")
            .append_pair("refresh_token", refresh_token.secret())
            .append_pair("scope", GRANTED_SCOPES)
            .append_pair("client_info", "1").finish();
        let response = self.client.post(endpoint.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body).send().await.map_err(|_| invalid("IntelliJ credential token endpoint could not be reached"))?;
        if response.status() != reqwest::StatusCode::OK { return Err(ExchangeFailure::Service(invalid("IntelliJ credential token exchange failed"))); }
        if response.content_length().is_some_and(|bytes| bytes > MAX_RESPONSE_BYTES as u64) {
            return Err(invalid("IntelliJ credential response exceeds its byte limit").into());
        }
        let mut bytes = Vec::new();
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|_| invalid("IntelliJ credential response could not be read"))?;
            if bytes.len().checked_add(chunk.len()).is_none_or(|size| size > MAX_RESPONSE_BYTES) {
                return Err(invalid("IntelliJ credential response exceeds its byte limit").into());
            }
            bytes.extend_from_slice(&chunk);
        }
        parse_token(&bytes, endpoint.path().starts_with("/adfs/"), OffsetDateTime::now_utc()).map_err(ExchangeFailure::Other)
    }

    async fn silent(&self, sessions: &mut Sessions, account: &Option<AccountId>, force: bool) -> azure_core::Result<Session> {
        let now = OffsetDateTime::now_utc();
        let cached = sessions.lookup(account, now);
        if account.is_none() { return cached.ok_or_else(|| invalid("Azure public client has no cached token")); }
        if account == &Some(AccountId::Missing) { return Err(invalid("Azure public client account has no identifier")); }
        let usable = cached.as_ref().is_some_and(|session| session.usable_at(now));
        let proactive = !force && usable && cached.as_ref().is_some_and(|session| session.proactive_at(now));
        if !force && usable && !proactive { return Ok(cached.expect("usable cached token")); }
        let refresh_token = sessions.refresh_token(account).ok_or_else(|| invalid("Azure public client has no cached refresh token"))?;
        match self.exchange(refresh_token).await {
            Ok(next) => { sessions.store(&next)?; Ok(next) }
            Err(ExchangeFailure::Service(_)) if proactive => Ok(cached.expect("usable proactive token")),
            Err(error) => Err(error.error()),
        }
    }
}

#[async_trait]
impl TokenCredential for IntelliJCredential {
    async fn get_token(&self, scopes: &[&str], _: Option<TokenRequestOptions<'_>>) -> azure_core::Result<AccessToken> {
        if scopes != [STORAGE_SCOPE] { return Err(invalid("IntelliJ storage credential requires the storage scope")); }
        let mut sessions = self.session.lock().await;
        if let Some(account) = sessions.last_account.clone() {
            if let Ok(mut next) = self.silent(&mut sessions, &account, false).await {
                if next.token.expires_on <= OffsetDateTime::now_utc() + azure_core::time::Duration::seconds(300) {
                    match self.silent(&mut sessions, &account, true).await { Ok(forced) => next = forced, Err(_) => return self.desktop_token(&mut sessions).await }
                }
                sessions.last_account = Some(next.account);
                return Ok(next.token);
            }
        }
        self.desktop_token(&mut sessions).await
    }
}

impl IntelliJCredential {
    async fn desktop_token(&self, sessions: &mut Sessions) -> azure_core::Result<AccessToken> {
        let cache = Arc::clone(&self.cache);
        let refresh_token = self.io.run_blocking(move || Ok(cache.read())).await
            .map_err(|_| invalid("IntelliJ credential cache I/O could not complete"))??;
        let next = self.exchange(&refresh_token).await.map_err(ExchangeFailure::error)?;
        sessions.store(&next)?;
        sessions.last_account = Some(next.account);
        Ok(next.token)
    }
}

fn parse_token(bytes: &[u8], adfs: bool, received_at: OffsetDateTime) -> azure_core::Result<Session> {
    let text = rtdb_types::decode_java_utf8(bytes);
    let mut reader = json::Reader::new(&text);
    if reader.next()? != json::Token::Object { return Err(invalid("IntelliJ credential token response requires an object")); }
    let mut response = BTreeMap::new();
    while reader.next()? != json::Token::EndObject {
        let field = reader.field().map(str::to_owned);
        reader.next()?;
        response.insert(field, reader.string().map(str::to_owned));
    }
    let text = |field: &str| response.get(&Some(field.to_owned())).and_then(Option::as_deref);
    let access_token = text("access_token").filter(|value| !value.is_empty()).ok_or_else(|| invalid("IntelliJ credential returned an empty access token"))?;
    let seconds = token_seconds(text("expires_in"))?;
    token_seconds(text("ext_expires_in"))?;
    let expires_on = received_at.unix_timestamp().checked_add(seconds)
        .and_then(|seconds| OffsetDateTime::from_unix_timestamp(seconds).ok())
        .ok_or_else(|| invalid("IntelliJ credential token expiration exceeds its supported range"))?;
    let refresh_token = text("refresh_token").filter(|value| !java_blank(value)).map(|value| Secret::new(value.to_owned()));
    let refresh_in = token_seconds(text("refresh_in"))?;
    let refresh_at = if refresh_in > 0 {
        Some(received_at.unix_timestamp().checked_add(refresh_in).and_then(|seconds| OffsetDateTime::from_unix_timestamp(seconds).ok())
            .ok_or_else(|| invalid("IntelliJ credential refresh time exceeds its supported range"))?)
    } else { None };
    let account = token_account(text("id_token"), text("client_info"), adfs)?;
    let scopes = text("scope").filter(|value| !java_blank(value)).unwrap_or(GRANTED_SCOPES).to_owned();
    let family = text("foci").filter(|value| !java_blank(value)).map(str::to_owned);
    Ok(Session { token: AccessToken::new(access_token.to_owned(), expires_on), refresh_token, refresh_at, account, scopes, family })
}

fn java_blank(value: &str) -> bool { value.trim_matches(|character| character <= '\u{20}').is_empty() }

fn decode_identity(value: &str) -> azure_core::Result<String> {
    let engine = base64::engine::general_purpose::GeneralPurpose::new(&base64::alphabet::URL_SAFE,
        base64::engine::general_purpose::GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true).with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent));
    let bytes = engine.decode(value).map_err(|_| invalid("Azure token identity has invalid Base64 encoding"))?;
    Ok(rtdb_types::decode_java_utf8(&bytes))
}

fn token_account(id_token: Option<&str>, client_info: Option<&str>, adfs: bool) -> azure_core::Result<Option<AccountId>> {
    let Some(id_token) = id_token.filter(|value| !java_blank(value)) else { return Ok(None); };
    let payload = decode_identity(id_token.split('.').nth(1).ok_or_else(|| invalid("Azure ID token has no payload"))?)?;
    let mut reader = json::Reader::new(&payload);
    let first = reader.next()?;
    let mut subject = None;
    if first != json::Token::Null {
        if first != json::Token::Object { return Err(invalid("Azure ID token requires an object")); }
        while reader.next()? != json::Token::EndObject {
            let field = reader.field().map(str::to_owned).ok_or_else(|| invalid("Azure ID token field is invalid"))?;
            reader.next()?;
            match field.as_str() {
                "sub" => subject = reader.string().map(str::to_owned),
                "iss" | "aud" | "name" | "preferred_username" | "oid" | "tid" | "upn" | "unique_name" => { reader.string(); }
                "exp" | "iat" | "nbf" => {
                    let value = reader.number().ok_or_else(|| invalid("Azure ID token timestamp is invalid"))?;
                    let valid = if value.contains(['.', 'e', 'E']) { value.parse::<f64>().is_ok_and(|value| value.is_finite() && value >= i64::MIN as f64 && value <= i64::MAX as f64) } else { value.parse::<i64>().is_ok() };
                    if !valid { return Err(invalid("Azure ID token timestamp is invalid")); }
                }
                _ => reader.skip_children()?,
            }
        }
    }
    if let Some(client_info) = client_info.filter(|value| !java_blank(value)) {
        let payload = decode_identity(client_info)?;
        let mut reader = json::Reader::new(&payload);
        if reader.next()? != json::Token::Object { return Err(invalid("Azure client identity requires an object")); }
        let mut uid = None;
        let mut tenant = None;
        while reader.next()? != json::Token::EndObject {
            let field = reader.field().map(str::to_owned).ok_or_else(|| invalid("Azure client identity field is invalid"))?;
            reader.next()?;
            match field.as_str() { "uid" => uid = reader.string().map(str::to_owned), "utid" => tenant = reader.string().map(str::to_owned), _ => reader.skip_children()? }
        }
        return Ok(Some(AccountId::Value(format!("{}.{}", uid.as_deref().unwrap_or("null"), tenant.as_deref().unwrap_or("null")))));
    }
    if adfs {
        if first == json::Token::Null { return Err(invalid("Azure ADFS ID token requires an object")); }
        return Ok(Some(subject.map(AccountId::Value).unwrap_or(AccountId::Missing)));
    }
    Ok(None)
}

fn token_seconds(value: Option<&str>) -> azure_core::Result<i64> {
    let Some(value) = value.filter(|value| !value.trim_matches(|character| character <= '\u{20}').is_empty()) else { return Ok(0); };
    let (negative, digits) = match value.as_bytes().first() { Some(b'-') => (true, &value[1..]), Some(b'+') => (false, &value[1..]), _ => (false, value) };
    if digits.is_empty() { return Err(invalid("IntelliJ credential returned an invalid token duration")); }
    let limit = if negative { i64::MIN } else { -i64::MAX };
    let mut result = 0i64;
    for digit in digits.chars() {
        let digit = rtdb_types::decimal_digit(digit).ok_or_else(|| invalid("IntelliJ credential returned an invalid token duration"))?;
        result = result.checked_mul(10).and_then(|value| value.checked_sub(i64::from(digit))).filter(|value| *value >= limit)
            .ok_or_else(|| invalid("IntelliJ credential returned an invalid token duration"))?;
    }
    Ok(if negative { result } else { -result })
}

#[derive(Debug)]
pub(crate) struct MissingBroker;

#[async_trait]
impl TokenCredential for MissingBroker {
    async fn get_token(&self, _: &[&str], _: Option<TokenRequestOptions<'_>>) -> azure_core::Result<AccessToken> {
        Err(invalid("Visual Studio Code and OS broker authentication require azure-identity-broker, which is absent from the reference distribution"))
    }
}

fn invalid(message: &str) -> azure_core::Error { azure_core::Error::with_message(ErrorKind::Credential, message.to_owned()) }

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct Cache { reads: Arc<AtomicUsize> }
    impl RefreshTokenCache for Cache {
        fn read(&self) -> azure_core::Result<Secret> {
            let read = self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(Secret::new(format!("desktop-refresh-{read}")))
        }
    }

    async fn server(responses: Vec<(u16, String)>) -> (Url, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!("http://{}/common/oauth2/v2.0/token", listener.local_addr().unwrap())).unwrap();
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut chunk = [0; 1024];
                    let count = socket.read(&mut chunk).await.unwrap();
                    assert!(count > 0);
                    request.extend_from_slice(&chunk[..count]);
                    assert!(request.len() <= 16_384);
                    if let Some(headers_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&request[..headers_end]).unwrap();
                        let length: usize = headers.lines().find_map(|line| line.split_once(':').filter(|(name, _)| name.eq_ignore_ascii_case("content-length")).map(|(_, value)| value.trim().parse().unwrap())).unwrap();
                        if request.len() >= headers_end + 4 + length { break; }
                    }
                }
                requests.push(String::from_utf8(request).unwrap());
                let _ = socket.write_all(format!("HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await;
            }
            requests
        });
        (endpoint, task)
    }

    fn body(request: &str) -> BTreeMap<String, String> {
        url::form_urlencoded::parse(request.split_once("\r\n\r\n").unwrap().1.as_bytes()).into_owned().collect()
    }

    fn authenticated(body: &str) -> String {
        let mut response: serde_json::Value = serde_json::from_str(body).unwrap();
        response["id_token"] = "header.eyJzdWIiOiJ1c2VyIn0.signature".into();
        response["client_info"] = "eyJ1aWQiOiJ1c2VyIiwidXRpZCI6InRlbmFudCJ9".into();
        response.to_string()
    }

    #[tokio::test]
    async fn intellij_default_tenant_is_common() {
        let io = IoRegion::new();
        let credential = IntelliJCredential::new(&BTreeMap::new(), Arc::clone(&io)).unwrap();
        assert_eq!(credential.authority.configured_token_endpoint().path(), "/common/oauth2/v2.0/token");
        io.close_and_wait().await;
    }

    #[test]
    fn public_client_refresh_keys_use_the_cache_application_id() {
        let now = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let mut cache = Sessions::for_client_id("application-id".into());
        let mut token = parse_token(authenticated(r#"{"access_token":"first","refresh_token":"family"}"#).as_bytes(), false, now).unwrap();
        token.family = Some(CLIENT_ID.into());
        cache.store(&token).unwrap();
        token.family = None;
        token.refresh_token = Some(Secret::new("application"));
        cache.store(&token).unwrap();
        assert_eq!(cache.refresh_tokens.len(), 2);
        token.family = Some("APPLICATION-ID".into());
        token.refresh_token = Some(Secret::new("same-key"));
        cache.store(&token).unwrap();
        assert_eq!(cache.refresh_tokens.len(), 2);
        assert_eq!(cache.refresh_tokens[1].1.secret(), "same-key");
    }

    #[tokio::test]
    async fn intellij_refresh_uses_the_developer_client_and_retains_its_session() {
        let (endpoint, server) = server(vec![
            (200, authenticated(r#"{"access_token":"first","expires_in":0,"refresh_token":"rotated"}"#)),
            (200, authenticated(r#"{"access_token":"second","expires_in":3600,"refresh_token":"next"}"#)),
        ]).await;
        let reads = Arc::new(AtomicUsize::new(0));
        let io = IoRegion::new();
        let credential = IntelliJCredential::with_cache(endpoint, Arc::new(Cache { reads: Arc::clone(&reads) }), Arc::clone(&io)).unwrap();
        assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "first");
        assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "second");
        assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "second");
        let requests = server.await.unwrap();
        let first = body(&requests[0]);
        assert_eq!(first["client_id"], CLIENT_ID);
        assert_eq!(first["grant_type"], "refresh_token");
        assert_eq!(first["refresh_token"], "desktop-refresh-0");
        assert_eq!(first["scope"], "https://storage.azure.com/.default openid profile offline_access");
        assert_eq!(body(&requests[1])["refresh_token"], "rotated");
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert!(!format!("{credential:?}").contains("rotated"));
        io.close_and_wait().await;
    }

    #[tokio::test]
    async fn intellij_rereads_desktop_credentials_when_a_cached_refresh_fails() {
        let (endpoint, server) = server(vec![
            (200, authenticated(r#"{"access_token":"first","expires_in":0,"refresh_token":"stale"}"#)),
            (400, r#"{"error":"invalid_grant","error_description":"private"}"#.into()),
            (200, r#"{"access_token":"recovered","expires_in":3600}"#.into()),
        ]).await;
        let reads = Arc::new(AtomicUsize::new(0));
        let io = IoRegion::new();
        let credential = IntelliJCredential::with_cache(endpoint, Arc::new(Cache { reads: Arc::clone(&reads) }), Arc::clone(&io)).unwrap();
        credential.get_token(&[STORAGE_SCOPE], None).await.unwrap();
        assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "recovered");
        let requests = server.await.unwrap();
        assert_eq!(body(&requests[1])["refresh_token"], "stale");
        assert_eq!(body(&requests[2])["refresh_token"], "desktop-refresh-1");
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        io.close_and_wait().await;
    }

    #[tokio::test]
    async fn intellij_cached_refresh_forces_one_more_exchange_for_short_lived_results() {
        let (endpoint, server) = server(vec![
            (200, authenticated(r#"{"access_token":"first","expires_in":0,"refresh_token":"rotate-one"}"#)),
            (200, authenticated(r#"{"access_token":"short","expires_in":60,"refresh_token":"rotate-two"}"#)),
            (200, authenticated(r#"{"access_token":"forced","expires_in":0,"refresh_token":"rotate-three"}"#)),
        ]).await;
        let io = IoRegion::new();
        let reads = Arc::new(AtomicUsize::new(0));
        let credential = IntelliJCredential::with_cache(endpoint, Arc::new(Cache { reads: Arc::clone(&reads) }), Arc::clone(&io)).unwrap();
        assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "first");
        assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "forced");
        let requests = server.await.unwrap();
        assert_eq!(body(&requests[1])["refresh_token"], "rotate-one");
        assert_eq!(body(&requests[2])["refresh_token"], "rotate-two");
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        io.close_and_wait().await;
    }

    #[tokio::test]
    async fn intellij_cache_secret_is_not_retained_without_a_returned_refresh_token() {
        let (endpoint, server) = server(vec![
            (200, r#"{"access_token":"first","expires_in":0}"#.into()),
            (200, r#"{"access_token":"second","expires_in":3600}"#.into()),
        ]).await;
        let io = IoRegion::new();
        let reads = Arc::new(AtomicUsize::new(0));
        let credential = IntelliJCredential::with_cache(endpoint, Arc::new(Cache { reads: Arc::clone(&reads) }), Arc::clone(&io)).unwrap();
        credential.get_token(&[STORAGE_SCOPE], None).await.unwrap();
        assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "second");
        let requests = server.await.unwrap();
        assert_eq!(body(&requests[0])["refresh_token"], "desktop-refresh-0");
        assert_eq!(body(&requests[1])["refresh_token"], "desktop-refresh-1");
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        io.close_and_wait().await;
    }

    #[tokio::test]
    async fn intellij_refresh_token_without_an_account_rereads_the_desktop_cache() {
        let (endpoint, server) = server(vec![
            (200, r#"{"access_token":"first","expires_in":0,"refresh_token":"unused"}"#.into()),
            (200, r#"{"access_token":"second","expires_in":3600,"refresh_token":"unused-again"}"#.into()),
        ]).await;
        let io = IoRegion::new();
        let reads = Arc::new(AtomicUsize::new(0));
        let credential = IntelliJCredential::with_cache(endpoint, Arc::new(Cache { reads: Arc::clone(&reads) }), Arc::clone(&io)).unwrap();
        credential.get_token(&[STORAGE_SCOPE], None).await.unwrap();
        assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "second");
        assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "second");
        let requests = server.await.unwrap();
        assert_eq!(body(&requests[1])["refresh_token"], "desktop-refresh-1");
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        io.close_and_wait().await;
    }

    #[tokio::test]
    async fn intellij_scope_mismatch_refreshes_only_an_authenticated_account() {
        for account in [false, true] {
            let response = r#"{"access_token":"different-scope","expires_in":3600,"refresh_token":"rotated","scope":"openid profile"}"#;
            let (endpoint, server) = server(vec![
                (200, if account { authenticated(response) } else { response.into() }),
                (200, r#"{"access_token":"storage","expires_in":3600}"#.into()),
            ]).await;
            let io = IoRegion::new();
            let reads = Arc::new(AtomicUsize::new(0));
            let credential = IntelliJCredential::with_cache(endpoint, Arc::new(Cache { reads: Arc::clone(&reads) }), Arc::clone(&io)).unwrap();
            credential.get_token(&[STORAGE_SCOPE], None).await.unwrap();
            assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "storage");
            let requests = server.await.unwrap();
            assert_eq!(body(&requests[1])["refresh_token"], if account { "rotated" } else { "desktop-refresh-1" });
            assert_eq!(reads.load(Ordering::SeqCst), if account { 1 } else { 2 });
            io.close_and_wait().await;
        }
    }

    #[test]
    fn intellij_identity_claims_are_parsed_before_a_session_is_saved() {
        assert_eq!(token_account(Some("header.e30.signature"), Some("e30"), false).unwrap(), Some(AccountId::Value("null.null".into())));
        assert_eq!(token_account(Some("header.eyJzdWIiOjEyM30.signature"), None, true).unwrap(), Some(AccountId::Value("123".into())));
        assert!(token_account(None, Some("invalid"), false).unwrap().is_none());
        assert!(token_account(Some("header.e30.signature"), None, false).unwrap().is_none());
        for token in ["invalid", "header.bad!", "header.eyJleHAiOiIxMjMifQ.signature", "header.eyJleHAiOm51bGx9.signature"] {
            assert!(token_account(Some(token), Some("e30"), false).is_err());
        }
        assert!(token_account(Some("header.e30.signature"), Some("invalid"), false).is_err());
    }

    #[test]
    fn intellij_cache_retains_refresh_tokens_per_account_and_matches_scope_case_insensitively() {
        let now = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let mut cache = Sessions::default();
        let first = parse_token(authenticated(r#"{"access_token":"first","expires_in":3600,"refresh_token":"retained","scope":"HTTPS://STORAGE.AZURE.COM/.DEFAULT openid"}"#).as_bytes(), false, now).unwrap();
        cache.store(&first).unwrap();
        let second = parse_token(authenticated(r#"{"access_token":"second","expires_in":3600,"refresh_token":" \t\r\n "}"#).as_bytes(), false, now).unwrap();
        cache.store(&second).unwrap();
        assert_eq!(cache.refresh_token(&first.account).unwrap().secret(), "retained");
        assert_eq!(cache.lookup(&first.account, now).unwrap().token.token.secret(), "first");
        assert_eq!(cache.lookup(&None, now).unwrap().account, first.account);
    }

    #[test]
    fn intellij_family_refresh_token_survives_a_nonfamily_response() {
        let now = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let mut cache = Sessions::default();
        let first = parse_token(authenticated(r#"{"access_token":"first","refresh_token":"family","foci":"1"}"#).as_bytes(), false, now).unwrap();
        cache.store(&first).unwrap();
        let second = parse_token(authenticated(r#"{"access_token":"second","refresh_token":"application"}"#).as_bytes(), false, now).unwrap();
        cache.store(&second).unwrap();
        assert_eq!(cache.refresh_token(&first.account).unwrap().secret(), "family");
        assert_eq!(cache.refresh_tokens.len(), 2);
    }

    #[test]
    fn intellij_family_refresh_tokens_preserve_insertion_order_when_replaced() {
        let now = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let mut cache = Sessions::default();
        let first = parse_token(authenticated(r#"{"access_token":"first","refresh_token":"family-two","foci":"2"}"#).as_bytes(), false, now).unwrap();
        cache.store(&first).unwrap();
        let second = parse_token(authenticated(r#"{"access_token":"second","refresh_token":"family-one","foci":"1"}"#).as_bytes(), false, now).unwrap();
        cache.store(&second).unwrap();
        assert_eq!(cache.refresh_token(&first.account).unwrap().secret(), "family-two");
        let replacement = parse_token(authenticated(r#"{"access_token":"replacement","refresh_token":"rotated-two","foci":"2"}"#).as_bytes(), false, now).unwrap();
        cache.store(&replacement).unwrap();
        assert_eq!(cache.refresh_token(&first.account).unwrap().secret(), "rotated-two");
        assert_eq!(cache.refresh_tokens.len(), 2);
    }

    #[test]
    fn intellij_silent_cache_checks_use_epoch_seconds_before_the_azure_refresh_filter() {
        let received = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let now = received + azure_core::time::Duration::milliseconds(900);
        let mut session = parse_token(authenticated(r#"{"access_token":"boundary","expires_in":300,"refresh_in":1}"#).as_bytes(), false, received).unwrap();
        assert!(session.usable_at(now));
        assert!(!session.usable_at(received + azure_core::time::Duration::seconds(1)));
        session.refresh_at = Some(received);
        assert!(!session.proactive_at(now));
        assert!(session.proactive_at(received + azure_core::time::Duration::seconds(1)));
        let mut cache = Sessions::default();
        cache.store(&session).unwrap();
        assert!(cache.lookup(&None, now).is_none());
        session.token.expires_on += azure_core::time::Duration::seconds(1);
        cache.store(&session).unwrap();
        assert!(cache.lookup(&None, now).is_some());
    }

    #[test]
    fn intellij_cache_replaces_case_folded_keys_but_looks_up_exact_account_identifiers() {
        let now = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let mut cache = Sessions::default();
        let mut first = parse_token(authenticated(r#"{"access_token":"first","expires_in":3600,"refresh_token":"first-refresh","foci":"FAMILY"}"#).as_bytes(), false, now).unwrap();
        first.account = Some(AccountId::Value("USER.TENANT".into()));
        cache.store(&first).unwrap();
        let second = parse_token(authenticated(r#"{"access_token":"second","expires_in":3600,"refresh_token":"second-refresh","foci":"family"}"#).as_bytes(), false, now).unwrap();
        cache.store(&second).unwrap();
        assert_eq!((cache.tokens.len(), cache.refresh_tokens.len(), cache.accounts.len()), (1, 1, 1));
        assert!(cache.lookup(&first.account, now).is_none());
        assert!(cache.refresh_token(&first.account).is_none());
        assert_eq!(cache.lookup(&second.account, now).unwrap().token.token.secret(), "second");
        assert_eq!(cache.refresh_token(&second.account).unwrap().secret(), "second-refresh");
        assert_eq!(cache.lookup(&None, now).unwrap().account, second.account);
    }

    #[test]
    fn intellij_missing_response_scope_retains_the_full_grant_scope_cache_key() {
        let now = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let mut cache = Sessions::default();
        let first = parse_token(authenticated(r#"{"access_token":"first","expires_in":3600}"#).as_bytes(), false, now).unwrap();
        assert_eq!(first.scopes, GRANTED_SCOPES);
        cache.store(&first).unwrap();
        let second = parse_token(authenticated(r#"{"access_token":"second","expires_in":3600,"scope":"https://storage.azure.com/.default"}"#).as_bytes(), false, now).unwrap();
        cache.store(&second).unwrap();
        assert_eq!(cache.tokens.len(), 2);
        assert_eq!(cache.lookup(&first.account, now).unwrap().token.token.secret(), "first");
    }

    #[test]
    fn intellij_token_durations_accept_java_bmp_digits_and_signed_long_boundaries() {
        for (value, expected) in [("+٣٦٠٠", 3600), ("-１２", -12), ("9223372036854775807", i64::MAX), ("-9223372036854775808", i64::MIN), (" \t\r\n", 0)] {
            assert_eq!(token_seconds(Some(value)).unwrap(), expected);
        }
        for value in ["9223372036854775808", "-9223372036854775809", "+", " 1", "1 ", "1e2", "𝟙"] {
            assert!(token_seconds(Some(value)).is_err());
        }
    }

    #[test]
    fn intellij_cache_limits_account_and_scope_growth_before_publication() {
        let now = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let mut cache = Sessions::default();
        let mut session = parse_token(br#"{"access_token":"token","refresh_token":"refresh"}"#, false, now).unwrap();
        for index in 0..64 { session.account = Some(AccountId::Value(index.to_string())); cache.store(&session).unwrap(); }
        session.account = Some(AccountId::Value("overflow".into()));
        assert!(cache.store(&session).is_err());
        assert_eq!((cache.tokens.len(), cache.refresh_tokens.len(), cache.accounts.len()), (64, 64, 64));
        session.account = Some(AccountId::Value("0".into()));
        session.token.token = Secret::new("x".repeat(8 * MAX_RESPONSE_BYTES));
        assert!(cache.store(&session).is_err());
        assert_eq!(cache.tokens[0].token.token.secret(), "token");
    }

    #[test]
    fn intellij_token_map_keeps_last_duplicate_and_scalar_string_coercions() {
        let now = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let session = parse_token(br#"{"access_token":"discarded","access_token":123,"refresh_token":true,"expires_in":1,"expires_in":"3600","ext_expires_in":"7200"}"#, false, now).unwrap();
        assert_eq!(session.token.token.secret(), "123");
        assert_eq!(session.refresh_token.as_ref().unwrap().secret(), "true");
        assert_eq!(session.token.expires_on.unix_timestamp(), 4600);
        assert!(parse_token(br#"{"access_token":"access","expires_in":3600,"ext_expires_in":"bad"}"#, false, now).is_err());
        let nested = parse_token(br#"{"access_token":"access","ignored":{"expires_in":3600},BROKEN"#, false, now).unwrap();
        assert_eq!(nested.token.expires_on.unix_timestamp(), 4600);
    }

    #[tokio::test]
    async fn intellij_cache_does_not_open_after_its_io_region_closes() {
        let io = IoRegion::new();
        io.close_and_wait().await;
        let reads = Arc::new(AtomicUsize::new(0));
        let credential = IntelliJCredential::with_cache(Url::parse("http://127.0.0.1:1/token").unwrap(), Arc::new(Cache { reads: Arc::clone(&reads) }), io).unwrap();
        assert!(credential.get_token(&[STORAGE_SCOPE], None).await.is_err());
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert!(credential.get_token(&["wrong-scope"], None).await.is_err());
    }

    #[tokio::test]
    async fn intellij_token_responses_are_bounded_and_hide_private_error_payloads() {
        let (endpoint, server) = server(vec![(200, " ".repeat(MAX_RESPONSE_BYTES + 1))]).await;
        let io = IoRegion::new();
        let credential = IntelliJCredential::with_cache(endpoint, Arc::new(Cache { reads: Arc::new(AtomicUsize::new(0)) }), Arc::clone(&io)).unwrap();
        assert!(credential.get_token(&[STORAGE_SCOPE], None).await.is_err());
        server.await.unwrap();
        let now = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let session = parse_token(br#"{"access_token":"access","expires_in":"3600"}"#, false, now).unwrap();
        assert_eq!(session.token.expires_on.unix_timestamp(), 4600);
        assert!(session.refresh_token.is_none());
        for (bytes, expiry) in [(br#"{"access_token":"access","expires_in":-1}"#.as_slice(), 999), (br#"{"access_token":"access"}"#, 1000)] {
            assert_eq!(parse_token(bytes, false, now).unwrap().token.expires_on.unix_timestamp(), expiry);
        }
        for bytes in [br#"{"access_token":"private","expires_in":9223372036854775807}"#.as_slice(), br#"{"access_token":"private","expires_in":"invalid"}"#, br#"{"access_token":"","expires_in":3600}"#] {
            let error = parse_token(bytes, false, now).err().unwrap();
            assert!(!error.to_string().contains("private"));
            assert!(!error.to_string().contains("refresh"));
        }
        io.close_and_wait().await;
    }

    #[tokio::test]
    async fn intellij_proactive_refresh_service_failure_retains_an_unexpired_token() {
        let (endpoint, server) = server(vec![(400, r#"{"error":"temporarily_unavailable"}"#.into())]).await;
        let io = IoRegion::new();
        let reads = Arc::new(AtomicUsize::new(0));
        let credential = IntelliJCredential::with_cache(endpoint, Arc::new(Cache { reads: Arc::clone(&reads) }), Arc::clone(&io)).unwrap();
        let mut session = parse_token(authenticated(r#"{"access_token":"usable","expires_in":3600,"refresh_in":1,"refresh_token":"refresh"}"#).as_bytes(), false, OffsetDateTime::now_utc()).unwrap();
        session.refresh_at = Some(OffsetDateTime::now_utc() - azure_core::time::Duration::seconds(1));
        { let mut sessions = credential.session.lock().await; sessions.store(&session).unwrap(); sessions.last_account = Some(session.account); }
        assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "usable");
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        server.await.unwrap();
        io.close_and_wait().await;
    }
}
