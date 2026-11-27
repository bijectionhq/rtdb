use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::CString,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD},
};
use futures::StreamExt;
use http::{
    HeaderMap, HeaderValue, Method,
    header::{ACCEPT, AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE},
};
use rdkafka::{
    ClientConfig,
    client::{ManagedOAuthToken, OAuthTokenProvider},
    util::current_time_millis,
};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, Result};
use serde_json::Value;
use url::Url;
use zeroize::Zeroizing;

use crate::{
    config::{invalid_config, signed_integer, trim_java},
    connect::registry::http::Client,
    tls::NativeTls,
};

const PREFIX: &str = "org.apache.kafka.common.security.oauthbearer.";
pub(crate) const LOGIN_MODULE: &str =
    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";
const CALLBACK: &str = "sasl.login.callback.handler.class";
const ENDPOINT: &str = "sasl.oauthbearer.token.endpoint.url";
const ALLOWED_URLS_ENV: &str = "RTDB_KAFKA_OAUTHBEARER_ALLOWED_URLS";
const CLIENT_ID: &str = "sasl.oauthbearer.client.credentials.client.id";
const CLIENT_SECRET: &str = "sasl.oauthbearer.client.credentials.client.secret";
const NATIVE_ID: &str = "sasl.oauthbearer.client.id";
const NATIVE_SECRET: &str = "sasl.oauthbearer.client.secret";
const SCOPE: &str = "sasl.oauthbearer.scope";
const CONNECT_TIMEOUT: &str = "sasl.login.connect.timeout.ms";
const READ_TIMEOUT: &str = "sasl.login.read.timeout.ms";
const RETRY_BACKOFF: &str = "sasl.login.retry.backoff.ms";
const RETRY_MAX: &str = "sasl.login.retry.backoff.max.ms";
const UNSUPPORTED_AUTH: &str = "xtdb.kafka/unsupported-config";
const MAX_TOKEN_RESPONSE: usize = 256 * 1024;

pub(crate) struct OAuthConfig {
    endpoint: Url,
    endpoint_text: String,
    authorization: HeaderValue,
    form: String,
    extensions: Vec<CString>,
    tls_options: BTreeMap<String, String>,
    connect_timeout: i64,
    read_timeout: i64,
    retry_backoff: Duration,
    retry_max: Duration,
    subject_claim: String,
    scope_claim: String,
    refresh: Refresh,
}

impl fmt::Debug for OAuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Kafka OAuth configuration")
    }
}

struct Refresh {
    window: f64,
    jitter: f64,
    minimum_ms: i64,
    buffer_ms: i64,
}

impl OAuthConfig {
    pub(crate) fn extract(
        properties: &mut BTreeMap<String, String>,
        jaas: Option<(&str, &BTreeMap<String, String>)>,
    ) -> Result<Option<Self>> {
        let callback = properties.get(CALLBACK).map(|value| trim_java(value));
        let (explicit, unsecured) = match callback.and_then(|value| value.strip_prefix(PREFIX)) {
            Some(
                "OAuthBearerLoginCallbackHandler"
                | "internals.secured.OAuthBearerLoginCallbackHandler",
            ) => (true, false),
            Some("internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler") => (false, true),
            _ => (false, false),
        };
        let native_oidc = properties
            .get("sasl.oauthbearer.method")
            .is_some_and(|mode| trim_java(mode).eq_ignore_ascii_case("oidc"));
        let empty = BTreeMap::new();
        let options = jaas
            .filter(|(module, _)| *module == LOGIN_MODULE)
            .map_or(&empty, |(_, options)| options);
        let secured = explicit
            || native_oidc
            || [ENDPOINT, CLIENT_ID, CLIENT_SECRET, NATIVE_ID, NATIVE_SECRET]
                .iter()
                .any(|name| properties.contains_key(*name))
            || options.contains_key("clientId")
            || options.contains_key("clientSecret");
        if !secured {
            return Ok(None);
        }

        let mechanisms = [
            properties.get("sasl.mechanism"),
            properties.get("sasl.mechanisms"),
        ];
        if !mechanisms
            .iter()
            .flatten()
            .any(|mechanism| trim_java(mechanism).eq_ignore_ascii_case("OAUTHBEARER"))
            || mechanisms
                .iter()
                .flatten()
                .any(|mechanism| !trim_java(mechanism).eq_ignore_ascii_case("OAUTHBEARER"))
            || !properties.get("security.protocol").is_some_and(|protocol| {
                let protocol = trim_java(protocol);
                protocol.eq_ignore_ascii_case("SASL_SSL")
                    || protocol.eq_ignore_ascii_case("SASL_PLAINTEXT")
            })
            || jaas.is_some_and(|(module, _)| module != LOGIN_MODULE)
        {
            return Err(invalid_config(
                "Kafka OIDC requires SASL/OAUTHBEARER with the OAuthBearer login module",
            ));
        }
        if unsecured
            || options.keys().any(|key| key.starts_with("unsecuredLogin"))
            || properties.contains_key("sasl.oauthbearer.config")
            || properties
                .get("enable.sasl.oauthbearer.unsecure.jwt")
                .is_some_and(|value| {
                    !["false", "0", "f"]
                        .iter()
                        .any(|disabled| trim_java(value).eq_ignore_ascii_case(disabled))
                })
        {
            return Err(Anomaly::incorrect(
                "xtdb.kafka/conflicting-config",
                "Secured Kafka OAuth conflicts with unsecured token settings",
            ));
        }
        if properties.contains_key("sasl.oauthbearer.method") && !native_oidc {
            return Err(Anomaly::incorrect(
                "xtdb.kafka/conflicting-config",
                "Kafka OIDC conflicts with sasl.oauthbearer.method",
            ));
        }
        if callback.is_some_and(|name| !name.is_empty()) && !explicit {
            return Err(Anomaly::unsupported(
                UNSUPPORTED_AUTH,
                "Custom Kafka OAuth callback handlers require a native adapter",
            ));
        }
        for (name, classes) in [
            (
                "sasl.oauthbearer.jwt.retriever.class",
                &["DefaultJwtRetriever", "ClientCredentialsJwtRetriever"][..],
            ),
            (
                "sasl.oauthbearer.jwt.validator.class",
                &["DefaultJwtValidator", "ClientJwtValidator"][..],
            ),
        ] {
            if properties.remove(name).is_some_and(|class| {
                !trim_java(&class)
                    .strip_prefix(PREFIX)
                    .is_some_and(|class| classes.contains(&class))
            }) {
                return Err(Anomaly::unsupported(
                    UNSUPPORTED_AUTH,
                    format!("Kafka property {name} requires a native adapter"),
                ));
            }
        }
        if properties
            .keys()
            .any(|key| key.starts_with("sasl.oauthbearer.assertion."))
        {
            return Err(Anomaly::unsupported(
                UNSUPPORTED_AUTH,
                "Kafka OAuth client assertions require a native adapter",
            ));
        }
        if explicit {
            properties.remove(CALLBACK);
        }
        if properties.get("sasl.login.class").is_some_and(|class| {
            trim_java(class) == "org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerRefreshingLogin"
        }) {
            properties.remove("sasl.login.class");
        }

        let endpoint_text = properties
            .remove(ENDPOINT)
            .ok_or_else(|| invalid_config("Kafka OAuth token endpoint is required"))?;
        let endpoint_text = trim_java(&endpoint_text).to_owned();
        let endpoint = Url::parse(&endpoint_text)
            .map_err(|_| invalid_config("Invalid Kafka OAuth token endpoint"))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(invalid_config(
                "Kafka OAuth token endpoint requires an HTTP(S) URL without embedded credentials or a fragment",
            ));
        }

        let client_id = secret(properties, options, CLIENT_ID, NATIVE_ID, "clientId")?;
        let client_secret = secret(
            properties,
            options,
            CLIENT_SECRET,
            NATIVE_SECRET,
            "clientSecret",
        )?;
        let scope = properties
            .remove(SCOPE)
            .or_else(|| options.get("scope").cloned());
        let scope = scope
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(trim_java);
        let encode = properties
            .remove("sasl.oauthbearer.header.urlencode")
            .map_or(Ok(false), |value| {
                match trim_java(&value).to_ascii_lowercase().as_str() {
                    "true" => Ok(true),
                    "false" => Ok(false),
                    _ => Err(invalid_config(
                        "sasl.oauthbearer.header.urlencode must be a boolean",
                    )),
                }
            })?;
        let (authorization, form) = request_fields(&client_id, &client_secret, scope, encode)?;
        let mut extensions = BTreeMap::new();
        if let Some(native) = properties.remove("sasl.oauthbearer.extensions") {
            for entry in native.split(',').filter(|value| !value.is_empty()) {
                let (key, value) = entry.split_once('=').ok_or_else(extension_error)?;
                extensions.insert(key.to_owned(), value.to_owned());
            }
        }
        for (key, value) in options {
            if let Some(key) = key.strip_prefix("extension_") {
                if extensions.get(key).is_some_and(|native| native != value) {
                    return Err(Anomaly::incorrect(
                        "xtdb.kafka/conflicting-config",
                        "JAAS and native Kafka OAuth extensions disagree",
                    ));
                }
                extensions.insert(key.to_owned(), value.to_owned());
            }
        }
        let mut wire_extensions = Vec::new();
        for (key, value) in extensions {
            if key.is_empty()
                || !key.bytes().all(|byte| byte.is_ascii_alphabetic())
                || key == "auth"
                || !value
                    .bytes()
                    .all(|byte| matches!(byte, b'\t' | b'\r' | b'\n' | b' '..=b'~'))
            {
                return Err(extension_error());
            }
            wire_extensions.push(CString::new(key).map_err(|_| extension_error())?);
            wire_extensions.push(CString::new(value).map_err(|_| extension_error())?);
        }

        let connect_timeout = take_int(properties, CONNECT_TIMEOUT, 60_000, i64::from(i32::MAX))?;
        let read_timeout = take_int(properties, READ_TIMEOUT, 60_000, i64::from(i32::MAX))?;
        let retry_backoff = take_int(properties, RETRY_BACKOFF, 100, i64::MAX)?;
        let retry_max = take_int(properties, RETRY_MAX, 10_000, i64::MAX)?;
        if retry_backoff > retry_max {
            return Err(invalid_config(
                "Kafka OAuth initial retry backoff exceeds the maximum retry backoff",
            ));
        }
        let refresh = Refresh {
            window: take_fraction(
                properties,
                "sasl.login.refresh.window.factor",
                0.8,
                0.5,
                1.0,
            )?,
            jitter: take_fraction(
                properties,
                "sasl.login.refresh.window.jitter",
                0.05,
                0.0,
                0.25,
            )?,
            minimum_ms: 1000
                * take_int(properties, "sasl.login.refresh.min.period.seconds", 60, 900)?,
            buffer_ms: 1000 * take_int(properties, "sasl.login.refresh.buffer.seconds", 300, 3600)?,
        };
        let subject_claim = claim_name(properties, "sasl.oauthbearer.sub.claim.name", "sub")?;
        let scope_claim = claim_name(properties, "sasl.oauthbearer.scope.claim.name", "scope")?;
        let tls_options = options
            .iter()
            .filter(|(name, _)| name.starts_with("ssl.") && endpoint.scheme() == "https")
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        properties.remove("sasl.oauthbearer.method");
        properties.remove("sasl.mechanisms");
        properties.insert("sasl.mechanism".into(), "OAUTHBEARER".into());

        Ok(Some(Self {
            endpoint,
            endpoint_text,
            authorization,
            form,
            extensions: wire_extensions,
            tls_options,
            connect_timeout,
            read_timeout,
            retry_backoff: Duration::from_millis(retry_backoff as u64),
            retry_max: Duration::from_millis(retry_max as u64),
            subject_claim,
            scope_claim,
            refresh,
        }))
    }

    pub(crate) fn attach(
        self,
        config: &mut ClientConfig,
        memory: &Arc<MemoryBudget>,
    ) -> Result<()> {
        let allowed = std::env::var(ALLOWED_URLS_ENV).unwrap_or_default();
        self.attach_with_allowlist(config, memory, &allowed)
    }

    fn attach_with_allowlist(
        mut self,
        config: &mut ClientConfig,
        memory: &Arc<MemoryBudget>,
        allowed: &str,
    ) -> Result<()> {
        if !allowed
            .split(',')
            .map(trim_java)
            .any(|url| url == self.endpoint_text)
        {
            return Err(invalid_config(format!(
                "Kafka OAuth token endpoint must be allowed by {ALLOWED_URLS_ENV}"
            )));
        }
        let bound = self
            .form
            .len()
            .checked_add(self.authorization.as_bytes().len())
            .and_then(|size| size.checked_add(self.endpoint.as_str().len()))
            .and_then(|size| size.checked_add(self.endpoint_text.len()))
            .and_then(|size| size.checked_add(self.subject_claim.len()))
            .and_then(|size| size.checked_add(self.scope_claim.len()))
            .and_then(|size| {
                self.extensions.iter().try_fold(size, |size, extension| {
                    size.checked_add(extension.as_bytes_with_nul().len() + 32)
                })
            })
            .and_then(|size| size.checked_add(4096))
            .ok_or_else(crate::config::property_allocation)?;
        let charge = memory.reserve(bound)?;
        let mut tls_options = std::mem::take(&mut self.tls_options);
        if tls_options
            .get("ssl.engine.factory.class")
            .is_some_and(|class| {
                trim_java(class) != "org.apache.kafka.common.security.ssl.DefaultSslEngineFactory"
            })
        {
            return Err(Anomaly::unsupported(
                UNSUPPORTED_AUTH,
                "Custom Kafka OAuth SSL engine factories require a native adapter",
            ));
        }
        tls_options.insert("security.protocol".into(), "SSL".into());
        let tls = NativeTls::from_java(&tls_options, &mut |bytes| memory.reserve(bytes))?
            .ok_or_else(|| invalid_config("Missing Kafka OAuth TLS configuration"))?;
        let context = unsafe { tls.registry_context() }
            .map_err(|_| invalid_config("Could not build Kafka OAuth TLS context"))?;
        let options = BTreeMap::from([
            (
                "http.connect.timeout.ms".into(),
                finite_timeout(self.connect_timeout).to_string(),
            ),
            (
                "http.read.timeout.ms".into(),
                finite_timeout(self.read_timeout).to_string(),
            ),
        ]);
        let transport = Client::new(context, tls, true, &options, Arc::clone(&charge))?;
        config.set_oauth_token_provider(Arc::new(Endpoint {
            config: self,
            transport,
            charge,
        }));
        Ok(())
    }
}

struct Endpoint {
    config: OAuthConfig,
    transport: Client,
    charge: Arc<MemoryReservation>,
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Kafka OAuth token provider")
    }
}

impl OAuthTokenProvider for Endpoint {
    fn admit_client(&self) -> std::result::Result<Box<dyn Send + Sync>, String> {
        let bound = self
            .config
            .extensions
            .iter()
            .try_fold(MAX_TOKEN_RESPONSE + 4096, |size, extension| {
                size.checked_add(extension.as_bytes_with_nul().len() + 32)
            })
            .ok_or_else(|| "Kafka OAuth client allocation overflow".to_owned())?;
        let charge = self
            .charge
            .budget()
            .reserve(bound)
            .map_err(|_| "Kafka OAuth client exceeded its memory budget")?;
        Ok(Box::new(charge))
    }

    fn token(&self) -> std::result::Result<ManagedOAuthToken, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| "Could not start the Kafka OAuth HTTP client".to_owned())?;
        let result = runtime.block_on(self.fetch());
        // A started system DNS lookup cannot be cancelled and retains its own memory reservation.
        runtime.shutdown_timeout(Duration::ZERO);
        result
    }

    fn extensions(&self) -> &[CString] {
        &self.config.extensions
    }
}

impl Endpoint {
    async fn fetch(&self) -> std::result::Result<ManagedOAuthToken, String> {
        let _response_charge = self
            .charge
            .budget()
            .reserve(MAX_TOKEN_RESPONSE * 8)
            .map_err(|_| "Kafka OAuth token response exceeded its memory budget")?;
        let start = Instant::now();
        let mut wait = self.config.retry_backoff;
        let mut first_error = None;
        loop {
            let timeout = Duration::from_millis(
                (finite_timeout(self.config.connect_timeout)
                    + finite_timeout(self.config.read_timeout)) as u64,
            );
            let result = tokio::time::timeout(timeout, self.request()).await;
            let (error, retryable) = match result {
                Ok(Ok(body)) => return self.decode(&body).map_err(str::to_owned),
                Ok(Err(failure)) => failure,
                Err(_) => ("Kafka OAuth token request timed out".to_owned(), true),
            };
            first_error.get_or_insert(error);
            let remaining = self.config.retry_max.saturating_sub(start.elapsed());
            if !retryable || remaining.is_zero() || wait.is_zero() {
                return Err(first_error.expect("OAuth retrieval recorded a failure"));
            }
            tokio::time::sleep(wait.min(remaining)).await;
            wait = wait.saturating_mul(2);
        }
    }

    async fn request(&self) -> std::result::Result<Vec<u8>, (String, bool)> {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, self.config.authorization.clone());
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let mut response = self
            .transport
            .request_once(
                Method::POST,
                &self.config.endpoint,
                &headers,
                bytes::Bytes::copy_from_slice(self.config.form.as_bytes()),
                None,
            )
            .await
            .map_err(|_| ("Kafka OAuth token endpoint request failed".into(), true))?;
        let status = response.status().as_u16();
        if !matches!(status, 200 | 201) {
            let retryable = !matches!(status, 300..=399 | 400..=407 | 409..=415 | 501 | 505);
            return Err((
                format!("Kafka OAuth token endpoint returned HTTP {status}"),
                retryable,
            ));
        }
        let oversized = || {
            (
                "Kafka OAuth token response exceeded its byte limit".into(),
                false,
            )
        };
        if response
            .content_length()
            .is_some_and(|bytes| bytes > MAX_TOKEN_RESPONSE as u64)
        {
            return Err(oversized());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.next().await {
            let chunk =
                chunk.map_err(|_| ("Kafka OAuth token response could not be read".into(), true))?;
            if chunk.len() > MAX_TOKEN_RESPONSE - body.len() {
                return Err(oversized());
            }
            body.extend_from_slice(&chunk);
        }
        if body.is_empty() {
            return Err(("Kafka OAuth token response was empty".into(), true));
        }
        Ok(body)
    }

    fn decode(&self, body: &[u8]) -> std::result::Result<ManagedOAuthToken, &'static str> {
        let response: Value = serde_json::from_slice(body)
            .map_err(|_| "Kafka OAuth token response is not valid JSON")?;
        let token = response
            .get("access_token")
            .and_then(Value::as_str)
            .map(str::trim)
            .ok_or("Kafka OAuth token response is missing an access token")?;
        let (principal, expires, issued_at) =
            jwt_claims(token, &self.config.subject_claim, &self.config.scope_claim)?;
        let now = current_time_millis();
        if expires <= now {
            return Err("Kafka OAuth token is already expired");
        }
        let mut random = [0; 4];
        if self.config.refresh.jitter > 0.0 {
            openssl::rand::rand_bytes(&mut random)
                .map_err(|_| "Could not sample Kafka OAuth refresh jitter")?;
        }
        let sample = f64::from(u32::from_ne_bytes(random)) / 4_294_967_296.0;
        Ok(ManagedOAuthToken {
            token: token.to_owned(),
            principal,
            expires_at_ms: expires,
            refresh_at_ms: self.config.refresh.next(now, expires, issued_at, sample),
        })
    }
}

impl Refresh {
    fn next(&self, now: i64, expires: i64, issued_at: Option<i64>, sample: f64) -> i64 {
        let fraction = self.window + self.jitter * sample;
        if expires - now < self.minimum_ms + self.buffer_ms {
            return (now + ((expires - now) as f64 * fraction) as i64).min(expires);
        }
        let issued = issued_at.unwrap_or(now);
        let proposed = issued + ((expires - issued) as f64 * fraction) as i64;
        proposed.clamp(now + self.minimum_ms, expires - self.buffer_ms)
    }
}

fn jwt_claims(
    token: &str,
    subject: &str,
    scope: &str,
) -> std::result::Result<(String, i64, Option<i64>), &'static str> {
    let mut parts = token.split('.');
    let valid = |part: &&str| {
        !part.is_empty()
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'='))
    };
    let (Some(_), Some(payload), Some(_), None) = (
        parts.next().filter(valid),
        parts.next().filter(valid),
        parts.next().filter(valid),
        parts.next(),
    ) else {
        return Err("Kafka OAuth access token must have three JWT parts");
    };
    let claims = URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| URL_SAFE.decode(payload))
        .map_err(|_| "Kafka OAuth access token has an invalid JWT payload")?;
    let claims: Value = serde_json::from_slice(&claims)
        .map_err(|_| "Kafka OAuth access token has invalid JWT claims")?;
    let principal = claims
        .get(subject)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.contains('\0'))
        .ok_or("Kafka OAuth JWT has an invalid subject claim")?;
    let milliseconds = |value: &Value| {
        value
            .as_f64()
            .filter(|seconds| *seconds >= 0.0 && *seconds < (i64::MAX / 1_000_000) as f64)
            .map(|seconds| seconds as i64 * 1000)
            .ok_or("Kafka OAuth JWT has an invalid time claim")
    };
    let expires = milliseconds(
        claims
            .get("exp")
            .ok_or("Kafka OAuth JWT is missing its expiration claim")?,
    )?;
    let issued_at = claims
        .get("iat")
        .filter(|value| !value.is_null())
        .map(milliseconds)
        .transpose()?;
    if issued_at.is_some_and(|issued| issued > expires) {
        return Err("Kafka OAuth JWT issue time exceeds its expiration");
    }

    let mut seen = BTreeSet::new();
    let mut validate_scope = |value: &Value| match value.as_str().map(str::trim) {
        Some(scope) if !scope.is_empty() && seen.insert(scope.to_owned()) => Ok(()),
        _ => Err("Kafka OAuth JWT has invalid or duplicate scope values"),
    };
    match claims.get(scope) {
        Some(Value::Array(values)) => {
            for value in values {
                validate_scope(value)?;
            }
        }
        Some(value @ Value::String(_)) => validate_scope(value)?,
        _ => (),
    }
    Ok((principal.to_owned(), expires, issued_at))
}

fn secret(
    properties: &mut BTreeMap<String, String>,
    options: &BTreeMap<String, String>,
    java: &str,
    native: &str,
    jaas: &str,
) -> Result<Zeroizing<String>> {
    let modern = properties.remove(java).map(Zeroizing::new);
    let native = properties.remove(native).map(Zeroizing::new);
    let legacy = options.get(jaas).map(|value| trim_java(value));
    if let (Some(native), Some(other)) = (
        &native,
        modern.as_ref().map(|value| trim_java(value)).or(legacy),
    ) && trim_java(native) != other
    {
        return Err(Anomaly::incorrect(
            "xtdb.kafka/conflicting-config",
            format!("Java and native Kafka OAuth {jaas} disagree"),
        ));
    }
    modern
        .as_ref()
        .or(native.as_ref())
        .map(|value| trim_java(value))
        .or(legacy)
        .filter(|value| !value.trim().is_empty() && !value.contains('\0'))
        .map(|value| Zeroizing::new(value.to_owned()))
        .ok_or_else(|| invalid_config(format!("Kafka OAuth {jaas} is required")))
}

fn request_fields(
    id: &str,
    secret: &str,
    scope: Option<&str>,
    encode: bool,
) -> Result<(HeaderValue, String)> {
    let encode_component = |value: &str| -> String {
        if encode {
            url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
        } else {
            value.to_owned()
        }
    };
    let id = Zeroizing::new(encode_component(id));
    let secret = Zeroizing::new(encode_component(secret));
    let pair = Zeroizing::new(format!("{}:{}", *id, *secret));
    let basic = Zeroizing::new(format!("Basic {}", STANDARD.encode(pair.as_bytes())));
    let mut authorization = HeaderValue::from_str(&basic)
        .map_err(|_| invalid_config("Invalid Kafka OAuth authorization header"))?;
    authorization.set_sensitive(true);
    let mut form = "grant_type=client_credentials".to_owned();
    if let Some(scope) = scope {
        form.push_str("&scope=");
        form.push_str(&encode_component(scope));
    }
    Ok((authorization, form))
}

fn take_int(
    properties: &mut BTreeMap<String, String>,
    key: &str,
    default: i64,
    max: i64,
) -> Result<i64> {
    properties.remove(key).map_or(Ok(default), |value| {
        signed_integer(&value)
            .filter(|value| (0..=max).contains(value))
            .ok_or_else(|| invalid_config(format!("{key} must be between 0 and {max}")))
    })
}

fn take_fraction(
    properties: &mut BTreeMap<String, String>,
    key: &str,
    default: f64,
    min: f64,
    max: f64,
) -> Result<f64> {
    properties.remove(key).map_or(Ok(default), |value| {
        trim_java(&value)
            .parse::<f64>()
            .ok()
            .filter(|value| (min..=max).contains(value))
            .ok_or_else(|| invalid_config(format!("{key} must be between {min} and {max}")))
    })
}

fn claim_name(
    properties: &mut BTreeMap<String, String>,
    key: &str,
    default: &str,
) -> Result<String> {
    properties.remove(key).map_or_else(
        || Ok(default.to_owned()),
        |value| {
            let name = trim_java(&value);
            if name.is_empty() {
                Err(invalid_config(format!("{key} must be nonempty")))
            } else {
                Ok(name.to_owned())
            }
        },
    )
}

fn extension_error() -> Anomaly {
    invalid_config("Invalid Kafka OAuth SASL extension")
}

fn finite_timeout(millis: i64) -> i64 {
    if millis == 0 { 60_000 } else { millis }
}

#[cfg(test)]
mod tests;
