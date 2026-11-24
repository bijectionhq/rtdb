use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_node::Clock;
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp};
use serde_json::Value;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{Credentials as PrincipalCredentials, HttpRequest, HttpResponse, HttpTransport, MethodRule, Principal, Secret, VerifiedLabelScope, forbidden};

#[derive(Clone, Debug)]
pub struct OidcConfig {
    pub issuer_url: Url,
    pub client_id: String,
    pub client_secret: Secret,
    pub rules: Vec<MethodRule>,
}

pub(crate) struct Provider {
    issuer: Url,
    token_endpoint: Url,
    userinfo_endpoint: Url,
    device_endpoint: Option<Url>,
    client_id: String,
    client_secret: Secret,
    pub rules: Vec<MethodRule>,
    clock: Arc<dyn Clock>,
    http: Arc<dyn HttpTransport>,
    memory: Arc<MemoryBudget>,
}

struct Token {
    access: Secret,
    refresh: Option<Secret>,
    expires_at: Option<Timestamp>,
    _reservation: Arc<MemoryReservation>,
}
impl Token {
    fn parse(body: &Value, now: Timestamp, old_refresh: Option<&Secret>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let access = string(body, "access_token")?;
        let refresh = body.get("refresh_token").and_then(Value::as_str).or_else(|| old_refresh.map(Secret::expose));
        let bytes = access.len().checked_add(refresh.map_or(0, str::len)).and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .ok_or_else(|| invalid("OIDC token is too large"))?;
        let reservation = memory.reserve(bytes)?;
        Ok(Self { access: Secret::new(access), refresh: refresh.map(Secret::new), expires_at: expires_at(body, now)?, _reservation: reservation })
    }
    fn expired(&self, now: Timestamp) -> bool { self.expires_at.is_some_and(|expires| now > expires) }
}

enum RefreshGrant { Password, ClientCredentials { id: String, secret: Secret, _reservation: Arc<MemoryReservation> } }

pub(crate) struct Credentials {
    provider: Arc<Provider>,
    grant: RefreshGrant,
    token: Mutex<Token>,
}
impl Credentials {
    pub async fn revalidate(&self, now: Timestamp, subject: &str) -> Result<()> {
        let mut token = self.token.lock().await;
        if !token.expired(now) { return Ok(()); }
        let fields = match &self.grant {
            RefreshGrant::Password => fields([
                ("grant_type", "refresh_token"), ("refresh_token", token.refresh.as_ref().map_or("", Secret::expose)),
            ]),
            RefreshGrant::ClientCredentials { id, secret, .. } => fields([
                ("grant_type", "client_credentials"), ("scope", "openid"), ("client_id", id.as_str()), ("client_secret", secret.expose()),
            ]),
        };
        let response = self.provider.token(fields).await.map_err(|_| refresh_failed(None))?;
        if response.status != 200 { return Err(refresh_failed(Some(response.status))); }
        let old_refresh = match &self.grant { RefreshGrant::Password => token.refresh.as_ref(), _ => None };
        let next = Token::parse(&response.body, self.provider.clock.now(), old_refresh, &self.provider.memory).map_err(|_| refresh_failed(Some(response.status)))?;
        let userinfo = self.provider.userinfo(&next).await.map_err(|_| refresh_failed(None))?;
        if userinfo.status != 200 || string(&userinfo.body, "sub").ok() != Some(subject)
            || userinfo.body.get("rtdb_label_scope").is_some()
        {
            return Err(refresh_failed(Some(userinfo.status)));
        }
        *token = next;
        Ok(())
    }
}

impl Provider {
    pub async fn open(config: OidcConfig, memory: Arc<MemoryBudget>, clock: Arc<dyn Clock>, http: Arc<dyn HttpTransport>) -> Result<Arc<Self>> {
        let discovery_url = Url::parse(&format!("{}/.well-known/openid-configuration", config.issuer_url.as_str().trim_end_matches('/')))
            .map_err(|_| discovery_failed("Invalid OIDC discovery URL"))?;
        let response = http.request(HttpRequest::Get { url: discovery_url.clone(), bearer: None }).await
            .map_err(|_| discovery_failed(format!("Error thrown when fetching OIDC configuration from {discovery_url}")))?;
        if response.status != 200 { return Err(discovery_failed(format!("Failed to discover OIDC configuration from {discovery_url}")).with_context("status", response.status.into())); }
        let endpoint = |name: &str| -> Result<Url> {
            let value = response.body.get(name).and_then(Value::as_str).ok_or_else(|| discovery_failed(format!("OIDC configuration missing required {name} from {discovery_url}")))?;
            Url::parse(value).map_err(|_| discovery_failed(format!("OIDC configuration contains invalid {name}")))
        };
        let device_endpoint = response.body.get("device_authorization_endpoint").map(|_| endpoint("device_authorization_endpoint")).transpose()?;
        Ok(Arc::new(Self { issuer: config.issuer_url, token_endpoint: endpoint("token_endpoint")?, userinfo_endpoint: endpoint("userinfo_endpoint")?, device_endpoint,
            client_id: config.client_id, client_secret: config.client_secret, rules: config.rules, clock, http, memory }))
    }
    fn base_fields(&self) -> BTreeMap<String, Secret> { fields([("client_id", self.client_id.as_str()), ("client_secret", self.client_secret.expose())]) }
    async fn token(&self, fields: BTreeMap<String, Secret>) -> Result<HttpResponse> {
        let mut body = self.base_fields(); body.extend(fields);
        self.http.request(HttpRequest::Form { url: self.token_endpoint.clone(), fields: body }).await
    }
    async fn userinfo(&self, token: &Token) -> Result<HttpResponse> {
        self.http.request(HttpRequest::Get { url: self.userinfo_endpoint.clone(), bearer: Some(token.access.clone()) }).await
    }
    async fn principal(self: &Arc<Self>, token: Token, grant: RefreshGrant) -> Result<Arc<Principal>> {
        let response = self.userinfo(&token).await?;
        if response.status != 200 { return Err(forbidden("xtdb.authn/failed", format!("Failed to obtain user info from {}", self.userinfo_endpoint)).with_context("status", response.status.into())); }
        let user = string(&response.body, "sub")?;
        let label_scope = response.body.get("rtdb_label_scope").map(|claim| {
            VerifiedLabelScope::from_userinfo(claim, self.issuer.as_str(), user, token.expires_at, self.clock.now())
        }).transpose()?;
        let bytes = user.len().checked_add(std::mem::size_of::<Principal>())
            .and_then(|bytes| bytes.checked_add(label_scope.as_ref().map_or(0, VerifiedLabelScope::memory_bytes)))
            .ok_or_else(|| invalid("Principal identity is too large"))?;
        let reservation = self.memory.reserve(bytes)?;
        Ok(Arc::new(Principal { user: Some(user.to_owned()), credentials: PrincipalCredentials::Oidc(Credentials { provider: Arc::clone(self), grant, token: Mutex::new(token) }), grants: None, label_scope, _reservation: reservation }))
    }
    pub async fn password(self: &Arc<Self>, user: &str, password: &str) -> Result<Arc<Principal>> {
        let response = self.token(fields([("grant_type", "password"), ("scope", "openid"), ("username", user), ("password", password)])).await?;
        if response.status != 200 { return Err(forbidden("xtdb.authn/failed", format!("Password authentication failed for user: {user}")).with_context("status", response.status.into())); }
        self.principal(Token::parse(&response.body, self.clock.now(), None, &self.memory)?, RefreshGrant::Password).await
    }
    pub async fn client_credentials(self: &Arc<Self>, id: &str, secret: &str) -> Result<Arc<Principal>> {
        let response = self.token(fields([("grant_type", "client_credentials"), ("scope", "openid"), ("client_id", id), ("client_secret", secret)])).await?;
        if response.status != 200 { return Err(forbidden("xtdb.authn/failed", format!("Client credentials authentication failed for client: {id}")).with_context("status", response.status.into())); }
        let reservation = self.memory.reserve(id.len().checked_add(secret.len()).and_then(|bytes| bytes.checked_add(std::mem::size_of::<RefreshGrant>())).ok_or_else(|| invalid("Client credentials are too large"))?)?;
        self.principal(Token::parse(&response.body, self.clock.now(), None, &self.memory)?, RefreshGrant::ClientCredentials { id: id.into(), secret: Secret::new(secret), _reservation: reservation }).await
    }
    pub async fn device(self: &Arc<Self>) -> Result<DeviceAuthorization> {
        let endpoint = self.device_endpoint.as_ref().ok_or_else(|| Anomaly::unsupported("xtdb.authn/device-auth-unsupported", "OIDC provider does not support device authorization flow - missing device_authorization_endpoint"))?;
        let mut body = self.base_fields(); body.insert("scope".into(), Secret::new("openid"));
        let response = self.http.request(HttpRequest::Form { url: endpoint.clone(), fields: body }).await?;
        if response.status != 200 { return Err(forbidden("xtdb.authn/failed", format!("Failed to obtain device info from {endpoint}")).with_context("status", response.status.into())); }
        let interval = response.body.get("interval").and_then(Value::as_u64).ok_or_else(|| invalid("OIDC device response has no polling interval"))?;
        let url = string(&response.body, "verification_uri_complete")?;
        let code = string(&response.body, "device_code")?;
        let reservation = self.memory.reserve(url.len().checked_mul(8).and_then(|bytes| bytes.checked_add(code.len())).and_then(|bytes| bytes.checked_add(512 + std::mem::size_of::<DeviceAuthorization>())).ok_or_else(|| invalid("Device authorization is too large"))?)?;
        Ok(DeviceAuthorization { provider: Arc::clone(self), url: Url::parse(url).map_err(|_| invalid("OIDC verification URL is invalid"))?,
            code: Secret::new(code), interval: Duration::from_secs(interval), _reservation: reservation,
            expires_at: expires_at(&response.body, self.clock.now())?.ok_or_else(|| invalid("OIDC device response has no expiry"))? })
    }
}

pub struct DeviceAuthorization {
    provider: Arc<Provider>,
    url: Url,
    code: Secret,
    interval: Duration,
    expires_at: Timestamp,
    _reservation: Arc<MemoryReservation>,
}
impl DeviceAuthorization {
    pub fn url(&self) -> &Url { &self.url }
    /// Polls on the caller's task until authorized, expired, rejected or cancelled.
    pub async fn await_principal(self, cancellation: &CancellationToken) -> Result<Arc<Principal>> {
        cancellation.run_until_cancelled(async {
            loop {
                if self.provider.clock.now() > self.expires_at { return Err(forbidden("xtdb.authn/failed", "Device authorization has expired")); }
                let response = self.provider.token(fields([("grant_type", "urn:ietf:params:oauth:grant-type:device_code"), ("device_code", self.code.expose())])).await?;
                match response.status {
                    200 => return self.provider.principal(Token::parse(&response.body, self.provider.clock.now(), None, &self.provider.memory)?, RefreshGrant::Password).await,
                    400 if response.body.get("error").and_then(Value::as_str) == Some("authorization_pending") => tokio::time::sleep(self.interval).await,
                    status => return Err(forbidden("xtdb.authn/failed", format!("Device authentication failed with status {status}")).with_context("status", status.into())),
                }
            }
        }).await.ok_or_else(|| Anomaly::new(AnomalyCategory::Interrupted, "xtdb.authn/cancelled", "Device authorization was cancelled"))?
    }
}

fn fields<const N: usize>(values: [(&str, &str); N]) -> BTreeMap<String, Secret> { values.into_iter().map(|(key, value)| (key.into(), Secret::new(value))).collect() }
fn string<'a>(body: &'a Value, key: &str) -> Result<&'a str> { body.get(key).and_then(Value::as_str).ok_or_else(|| invalid(format!("OIDC response has no string {key}"))) }
fn expires_at(body: &Value, now: Timestamp) -> Result<Option<Timestamp>> {
    match body.get("expires_in") {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_i64().and_then(|seconds| seconds.checked_mul(1_000_000)).and_then(|micros| now.0.checked_add(micros)).map(Timestamp).map(Some).ok_or_else(|| invalid("OIDC token expiry is outside supported range")),
    }
}
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("rtdb.auth/invalid-response", message) }
fn discovery_failed(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb.authn/oidc-discovery-failed", message) }
fn refresh_failed(status: Option<u16>) -> Anomaly {
    let error = forbidden("xtdb.authn/refresh-failed", "Failed to refresh OAuth token");
    match status { Some(status) => error.with_context("status", status.into()), None => error }
}
