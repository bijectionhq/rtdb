use std::{collections::{BTreeMap, BTreeSet}, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};
use base64::{Engine, engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD}};
use futures::StreamExt;
use reqwest::{Url, header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION}};
use super::http::Client;
use rtdb_columnar::MemoryReservation;
use rtdb_types::Result;
use serde_json::Value as Json;
use super::super::{ConversionContext, error};
mod tls;
const MAXIMUM_TOKEN_RESPONSE: usize = 256 * 1024;

pub(super) struct Security { credentials: Credentials, headers: HeaderMap }
enum Credentials { None, Url, Basic(String), Bearer(String), OAuth(OAuth) }
struct OAuth {
    endpoint: Url, authorization: String, scope: Option<String>, scope_claim: String, subject_claim: String,
    buffer_seconds: i16, cached: Option<CachedToken>, client: Option<(Client, Arc<MemoryReservation>)>, client_options: BTreeMap<String, String>,
}
struct CachedToken { token: String, refresh_millis: i64, charge: Arc<MemoryReservation> }
impl Security {
    pub fn open(options: &BTreeMap<String, String>) -> Result<Self> {
        let options = normalized(options);
        let basic = options.get("basic.auth.credentials.source").filter(|value| !value.is_empty()).map(String::as_str);
        let bearer = options.get("bearer.auth.credentials.source").filter(|value| !value.is_empty()).map(String::as_str);
        if basic.is_some() && bearer.is_some() { return Err(error("invalid-plugin-config", "Only one registry basic or bearer credential source may be configured")); }
        let mut headers = HeaderMap::new();
        let credentials = if let Some(source) = basic { match source {
            "URL" => Credentials::Url,
            "USER_INFO" => Credentials::Basic(basic_header(required_raw(&options, "basic.auth.user.info")?)),
            "SASL_INHERIT" => {
                let (_, jaas) = crate::config::parse_jaas(required(&options, "sasl.jaas.config")?)?;
                Credentials::Basic(basic_header(&format!("{}:{}", required_raw(&jaas, "username")?, required_raw(&jaas, "password")?)))
            }
            _ => return Err(error("invalid-plugin-config", "Registry basic credential source requires a native adapter")),
        } } else if let Some(source) = bearer {
            for (name, config) in [("target-sr-cluster", "bearer.auth.logical.cluster"), ("confluent-identity-pool-id", "bearer.auth.identity.pool.id")] {
                if let Some(value) = options.get(config) { insert_header(&mut headers, name, value.trim())?; }
            }
            match source {
                "STATIC_TOKEN" => Credentials::Bearer(required_raw(&options, "bearer.auth.token")?.to_owned()),
                "OAUTHBEARER" | "SASL_OAUTHBEARER_INHERIT" => {
                    let inherited = if source == "SASL_OAUTHBEARER_INHERIT" { crate::config::parse_jaas(required(&options, "sasl.jaas.config")?)?.1 } else { BTreeMap::new() };
                    if !headers.contains_key("confluent-identity-pool-id") { if let Some(value) = inherited.get("extension_identityPoolId") { insert_header(&mut headers, "confluent-identity-pool-id", value.trim())?; } }
                    let option = |key: &str, inherited_key: &str| options.get(key).or_else(|| inherited.get(inherited_key)).map(String::as_str);
                    let client_id = nonempty(option("bearer.auth.client.id", "clientId"), "OAuth client ID")?;
                    let client_secret = nonempty(option("bearer.auth.client.secret", "clientSecret"), "OAuth client secret")?;
                    let endpoint = options.get("bearer.auth.issuer.endpoint.url").or_else(|| if source == "SASL_OAUTHBEARER_INHERIT" { options.get("sasl.oauthbearer.token.endpoint.url") } else { None });
                    let endpoint = Url::parse(nonempty(endpoint.map(String::as_str), "OAuth token endpoint")?).map_err(|_| error("invalid-plugin-config", "Invalid OAuth token endpoint"))?;
                    if !matches!(endpoint.scheme(), "http" | "https") || !endpoint.username().is_empty() || endpoint.password().is_some() { return Err(error("invalid-plugin-config", "OAuth token endpoint requires HTTP(S) without embedded credentials")); }
                    let claim = |registry: &str, sasl: &str, default: &str| -> Result<String> {
                        let value = if source == "SASL_OAUTHBEARER_INHERIT" { options.get(sasl).or_else(|| options.get(registry)) } else { options.get(registry) };
                        Ok(nonempty(value.map(String::as_str).or(Some(default)), "OAuth claim name")?.into())
                    };
                    let buffer_seconds = options.get("bearer.auth.cache.expiry.buffer.seconds").map(|value| value.parse::<i16>().map_err(|_| error("invalid-plugin-config", "OAuth cache buffer must fit a signed short"))).transpose()?.unwrap_or(300);
                    let tls_options = if source == "SASL_OAUTHBEARER_INHERIT" { &inherited } else { &options };
                    let mut client_options = tls_options.iter().filter(|(name, _)| name.starts_with("ssl.")).map(|(name, value)| (format!("schema.registry.{name}"), value.clone())).collect::<BTreeMap<_, _>>();
                    client_options.insert("http.connect.timeout.ms".into(), "0".into()); client_options.insert("http.read.timeout.ms".into(), "0".into());
                    Credentials::OAuth(OAuth { endpoint, authorization: basic_header(&format!("{client_id}:{client_secret}")), scope: option("bearer.auth.scope", "scope").map(str::trim).filter(|scope| !scope.is_empty()).map(str::to_owned), scope_claim: claim("bearer.auth.scope.claim.name", "sasl.oauthbearer.scope.claim.name", "scope")?, subject_claim: claim("bearer.auth.sub.claim.name", "sasl.oauthbearer.sub.claim.name", "sub")?, buffer_seconds, cached: None, client: None, client_options })
                }
                _ => return Err(error("invalid-plugin-config", "Registry bearer credential source requires a native adapter")),
            }
        } else { Credentials::None };
        Ok(Self { credentials, headers })
    }
    pub async fn headers(&mut self, url: &Url, context: &mut ConversionContext) -> Result<HeaderMap> {
        let credential_bytes = match &self.credentials { Credentials::None => 0, Credentials::Url => url.as_str().len(), Credentials::Basic(value) | Credentials::Bearer(value) => value.len(), Credentials::OAuth(value) => MAXIMUM_TOKEN_RESPONSE.saturating_add(value.authorization.len()).saturating_add(value.scope.as_ref().map_or(0, String::len)) };
        context.reserve(credential_bytes.saturating_mul(4).saturating_add(self.headers.iter().map(|(key, value)| key.as_str().len().saturating_add(value.as_bytes().len())).sum::<usize>()).saturating_add(4096))?;
        let authorization = match &mut self.credentials {
            Credentials::None => None,
            Credentials::Url => if url.username().is_empty() && url.password().is_none() { None } else { Some(basic_header(&match url.password() { Some(password) => format!("{}:{password}", url.username()), None => url.username().to_owned() })) },
            Credentials::Basic(value) => Some(value.clone()), Credentials::Bearer(value) => Some(format!("Bearer {value}")),
            Credentials::OAuth(oauth) => Some(format!("Bearer {}", oauth.token(context).await?)),
        };
        let mut headers = self.headers.clone();
        if let Some(value) = authorization { headers.insert(AUTHORIZATION, HeaderValue::from_str(&value).map_err(|_| error("invalid-plugin-config", "Invalid registry authorization header"))?); }
        Ok(headers)
    }
}
impl OAuth {
    async fn token(&mut self, context: &mut ConversionContext) -> Result<&str> {
        let now = now_millis()?;
        if self.cached.as_ref().is_some_and(|cached| now < cached.refresh_millis) {
            let cached = self.cached.as_ref().expect("cached token checked"); context.retain(Arc::clone(&cached.charge))?; return Ok(&cached.token);
        }
        if self.client.is_none() { self.client = Some(tls::client(&self.client_options, context, false)?); }
        let (client, client_charge) = self.client.as_ref().expect("OAuth client initialized");
        context.retain(Arc::clone(client_charge))?;
        let charge = context.reserve(MAXIMUM_TOKEN_RESPONSE * 8)?;
        let mut form = url::form_urlencoded::Serializer::new(String::new()); form.append_pair("grant_type", "client_credentials");
        if let Some(scope) = &self.scope { form.append_pair("scope", scope); }
        let form = form.finish();
        let started = tokio::time::Instant::now(); let mut backoff = Duration::from_millis(100);
        let response = loop {
            let mut headers = HeaderMap::new();
            for (name, value) in [("authorization", self.authorization.as_str()), ("accept", "application/json"), ("cache-control", "no-cache"), ("content-type", "application/x-www-form-urlencoded")] { insert_header(&mut headers, name, value)?; }
            let response = client.request(::http::Method::POST, self.endpoint.clone(), headers, form.as_bytes().to_vec()).await;
            match response {
                Ok(response) if matches!(response.status().as_u16(), 200 | 201) => break response,
                Ok(response) if matches!(response.status().as_u16(), 400..=407 | 409..=415 | 501 | 505) => return Err(error("schema-registry-auth", format!("OAuth token endpoint returned HTTP {}", response.status()))),
                _ => (),
            }
            let remaining = Duration::from_secs(10).saturating_sub(started.elapsed());
            if remaining.is_zero() { return Err(error("schema-registry-auth", "OAuth token request retries exhausted")); }
            tokio::time::sleep(backoff.min(remaining)).await; backoff = backoff.saturating_mul(2);
        };
        if response.content_length().is_some_and(|bytes| bytes > MAXIMUM_TOKEN_RESPONSE as u64) { return Err(error("schema-registry-auth", "OAuth response exceeds its byte limit")); }
        let mut bytes = Vec::new(); let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|cause| error("schema-registry-auth", cause.without_url()))?;
            if bytes.len().checked_add(chunk.len()).is_none_or(|bytes| bytes > MAXIMUM_TOKEN_RESPONSE) { return Err(error("schema-registry-auth", "OAuth response exceeds its byte limit")); }
            bytes.extend_from_slice(&chunk);
        }
        let json: Json = serde_json::from_slice(&bytes).map_err(|_| error("schema-registry-auth", "OAuth response is not valid JSON"))?;
        let token = nonempty(json.get("access_token").and_then(Json::as_str), "OAuth access token")?.to_owned();
        let (expiration, issued_at) = token_lifetime(&token, &self.scope_claim, &self.subject_claim)?;
        let refresh_millis = refresh_time(now_millis()?, expiration, issued_at, self.buffer_seconds);
        self.cached = Some(CachedToken { token, refresh_millis, charge }); Ok(&self.cached.as_ref().expect("token stored").token)
    }
}
fn token_lifetime(token: &str, scope_claim: &str, subject_claim: &str) -> Result<(i64, Option<i64>)> {
    let mut parts = token.trim().split('.'); let header = parts.next().unwrap_or_default(); let payload = parts.next().unwrap_or_default(); let signature = parts.next();
    if header.trim().is_empty() || payload.trim().is_empty() || signature.is_none_or(|value| value.trim().is_empty()) || parts.next().is_some() { return Err(error("schema-registry-auth", "OAuth access token must contain three JWT parts")); }
    let payload = URL_SAFE_NO_PAD.decode(payload.trim()).or_else(|_| URL_SAFE.decode(payload.trim())).map_err(|_| error("schema-registry-auth", "Invalid OAuth JWT payload encoding"))?;
    let claims: Json = serde_json::from_slice(&payload).map_err(|_| error("schema-registry-auth", "Invalid OAuth JWT claims"))?;
    nonempty(claims.get(subject_claim).and_then(Json::as_str), "OAuth subject claim")?;
    let number = |name: &str| -> Result<Option<i64>> {
        claims.get(name).filter(|value| !value.is_null()).map(|value| value.as_i64().or_else(|| value.as_u64().map(|value| value as i64)).or_else(|| value.as_f64().map(|value| value as i64)).map(|value| value.wrapping_mul(1000)).filter(|value| *value >= 0).ok_or_else(|| error("schema-registry-auth", "OAuth time claim must be a nonnegative number"))).transpose()
    };
    let expiration = number("exp")?.ok_or_else(|| error("schema-registry-auth", "OAuth expiration claim is absent"))?;
    let mut scopes = BTreeSet::new();
    if let Some(scope) = claims.get(scope_claim) {
        let values = match scope { Json::String(value) => vec![value.as_str()], Json::Array(values) => values.iter().map(|value| value.as_str().ok_or_else(|| error("schema-registry-auth", "OAuth scope entries must be strings"))).collect::<Result<Vec<_>>>()?, _ => Vec::new() };
        for scope in values { if !scopes.insert(nonempty(Some(scope), "OAuth scope")?) { return Err(error("schema-registry-auth", "OAuth scope contains duplicates")); } }
    }
    Ok((expiration, number("iat")?))
}
fn refresh_time(now: i64, expiration: i64, issued_at: Option<i64>, buffer_seconds: i16) -> i64 {
    if now > expiration { return expiration; }
    let buffer = i64::from(buffer_seconds) * 1000;
    if now.wrapping_add(buffer) > expiration { return now.wrapping_add(((expiration.wrapping_sub(now) as f32) * 0.8).floor() as i64); }
    let start = issued_at.unwrap_or(now);
    start.wrapping_add(((expiration.wrapping_sub(start) as f32) * 0.8) as i64).min(expiration.wrapping_sub(buffer))
}
fn now_millis() -> Result<i64> { i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| error("schema-registry-auth", "Clock is before the Unix epoch"))?.as_millis()).map_err(|_| error("schema-registry-auth", "Clock exceeds millisecond range")) }
fn basic_header(value: &str) -> String { format!("Basic {}", STANDARD.encode(value)) }
fn required_raw<'a>(options: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str> { options.get(key).map(String::as_str).filter(|value| !value.is_empty()).ok_or_else(|| error("invalid-plugin-config", format!("{key} must be nonempty"))) }
fn required<'a>(options: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str> { nonempty(options.get(key).map(String::as_str), key) }
fn nonempty<'a>(value: Option<&'a str>, name: &str) -> Result<&'a str> { value.map(str::trim).filter(|value| !value.is_empty()).ok_or_else(|| error("invalid-plugin-config", format!("{name} must be nonempty"))) }
fn insert_header(headers: &mut HeaderMap, name: &'static str, value: &str) -> Result<()> { headers.insert(HeaderName::from_static(name), HeaderValue::from_str(value).map_err(|_| error("invalid-plugin-config", "Invalid registry target header"))?); Ok(()) }
fn normalized(options: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut normalized = options.clone();
    for (name, value) in options { if let Some(name) = name.strip_prefix("schema.registry.") { normalized.insert(name.into(), value.clone()); } }
    normalized
}
pub(super) fn client(options: &BTreeMap<String, String>, context: &mut ConversionContext) -> Result<(Client, Arc<MemoryReservation>)> { tls::client(options, context, true) }

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn oauth_refresh_runs_on_the_request_and_cached_tokens_retain_their_charge() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(); let address = listener.local_addr().unwrap();
        let token = format!("e30.{}.signature", URL_SAFE_NO_PAD.encode(serde_json::json!({"sub":"subject","exp":now_millis().unwrap()/1000+3600}).to_string()));
        let body = serde_json::json!({"access_token":token}).to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap(); let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") { let mut byte = [0]; socket.read_exact(&mut byte).await.unwrap(); request.push(byte[0]); assert!(request.len() < 8192); }
            let request = String::from_utf8(request).unwrap();
            let length = request.lines().find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length:").map(|value| value.trim().parse::<usize>().unwrap())).unwrap();
            let mut form = vec![0; length]; socket.read_exact(&mut form).await.unwrap();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            (request, String::from_utf8(form).unwrap())
        });
        let config = BTreeMap::from([("bearer.auth.credentials.source".into(), "OAUTHBEARER".into()), ("bearer.auth.client.id".into(), "client".into()), ("bearer.auth.client.secret".into(), "secret".into()), ("bearer.auth.issuer.endpoint.url".into(), format!("http://{address}/token")), ("bearer.auth.scope".into(), "read write".into()), ("bearer.auth.logical.cluster".into(), "registry".into())]);
        let memory = rtdb_columnar::MemoryBudget::new(1 << 27); let mut context = ConversionContext::new(Arc::clone(&memory));
        let mut security = Security::open(&config).unwrap(); let url = Url::parse("http://registry.invalid").unwrap();
        let first = security.headers(&url, &mut context).await.unwrap();
        let (request, form) = server.await.unwrap(); assert!(request.starts_with("POST /token ")); assert!(request.to_ascii_lowercase().contains("authorization: basic y2xpzw50onnly3jlda==")); assert_eq!(form, "grant_type=client_credentials&scope=read+write");
        drop(context); assert!(memory.used_bytes() > 0);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        assert_eq!(security.headers(&url, &mut context).await.unwrap(), first); assert_eq!(first["target-sr-cluster"], "registry");
        drop(context); drop(security); assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn registry_basic_auth_sources_are_explicit_and_mutually_exclusive() {
        assert!(matches!(Security::open(&BTreeMap::from([("basic.auth.user.info".into(), "u:p".into())])).unwrap().credentials, Credentials::None));
        let config = BTreeMap::from([("basic.auth.credentials.source".into(), "SASL_INHERIT".into()), ("sasl.jaas.config".into(), "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"p\";".into())]);
        assert!(matches!(Security::open(&config).unwrap().credentials, Credentials::Basic(value) if value == "Basic dTpw"));
        let mut both = config; both.insert("bearer.auth.credentials.source".into(), "STATIC_TOKEN".into()); assert!(Security::open(&both).is_err());
    }
    #[test]
    fn oauth_refresh_matches_confluent_short_lifetime_and_end_buffer_rules() {
        assert_eq!(refresh_time(1000, 2000, Some(0), 300), 1800);
        assert_eq!(refresh_time(1000, 10_000, Some(0), 3), 7000);
        assert_eq!(refresh_time(1000, 10_000, Some(0), 0), 8000);
        assert_eq!(refresh_time(3000, 2000, None, 0), 2000);
    }
    #[test]
    fn oauth_claims_keep_optional_issued_at_and_reject_duplicate_scopes() {
        let token = |claims: Json| format!("e30.{}.signature", URL_SAFE_NO_PAD.encode(claims.to_string()));
        assert_eq!(token_lifetime(&token(serde_json::json!({"sub":"subject","exp":100})), "scope", "sub").unwrap(), (100_000, None));
        assert!(token_lifetime(&token(serde_json::json!({"sub":"subject","exp":100,"scope":["a"," a "]})), "scope", "sub").is_err());
    }
}
