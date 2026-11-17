use std::{collections::{BTreeMap, BTreeSet}, sync::{Arc, OnceLock}, time::Duration};
use futures::StreamExt;
use tokio::sync::Mutex;
use url::Url;
use super::{invalid, json::{Reader, Token}};

const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_CACHE_BYTES: usize = 4 * 1024 * 1024;
const MAX_CACHE_ALIASES: usize = 8192;
const KNOWN_CLOUDS: &[(&str, &[&str])] = &[
    ("login.microsoftonline.com", &["login.microsoftonline.com", "login.windows.net", "login.microsoft.com", "sts.windows.net"]),
    ("login.partner.microsoftonline.cn", &["login.partner.microsoftonline.cn", "login.chinacloudapi.cn"]),
    ("login.microsoftonline.de", &["login.microsoftonline.de"]),
    ("login.microsoftonline.us", &["login.microsoftonline.us", "login.usgovcloudapi.net"]),
    ("login-us.microsoftonline.com", &["login-us.microsoftonline.com"]),
    ("login.sovcloud-identity.fr", &["login.sovcloud-identity.fr"]),
    ("login.sovcloud-identity.de", &["login.sovcloud-identity.de"]),
    ("login.sovcloud-identity.sg", &["login.sovcloud-identity.sg"]),
];

pub(super) struct Authority { mode: Mode }
enum Mode {
    Configured { endpoint: Url, discovery: Option<Url>, client: reqwest::Client, metadata: Arc<Mutex<MetadataCache>> },
    #[cfg(test)]
    Direct(Url),
}

#[derive(Default)]
struct MetadataCache { aliases: BTreeMap<String, Option<String>>, bytes: usize }

impl MetadataCache {
    fn insert(&mut self, alias: String, preferred_network: Option<String>, overwrite: bool) -> azure_core::Result<()> {
        let previous = self.aliases.get(&alias);
        if previous.is_some() && !overwrite { return Ok(()); }
        let previous_bytes = previous.map_or(0, |host| alias.len() + host.as_ref().map_or(0, String::len) + 64);
        let bytes = self.bytes - previous_bytes + alias.len() + preferred_network.as_ref().map_or(0, String::len) + 64;
        if bytes > MAX_CACHE_BYTES || (previous.is_none() && self.aliases.len() == MAX_CACHE_ALIASES) {
            return Err(invalid("Azure authority metadata exceeds its cache limit"));
        }
        self.aliases.insert(alias, preferred_network);
        self.bytes = bytes;
        Ok(())
    }

    fn fallback(&mut self, host: &str) -> azure_core::Result<()> {
        if let Some((network, aliases)) = known_cloud(host) {
            for alias in *aliases { self.insert((*alias).to_owned(), Some((*network).to_owned()), false)?; }
        } else {
            self.insert(host.to_owned(), Some(host.to_owned()), false)?;
        }
        Ok(())
    }

    fn publish(&mut self, host: &str, response: DiscoveryResponse) -> azure_core::Result<()> {
        for entry in response.metadata.into_iter().flatten() {
            let entry = entry.ok_or_else(|| invalid("Azure authority metadata contains an invalid entry"))?;
            for alias in entry.aliases.ok_or_else(|| invalid("Azure authority metadata has no aliases"))? {
                let alias = alias.ok_or_else(|| invalid("Azure authority metadata contains an invalid alias"))?;
                self.insert(alias, entry.preferred_network.clone(), true)?;
            }
        }
        self.fallback(host)
    }
}

fn known_cloud(host: &str) -> Option<&'static (&'static str, &'static [&'static str])> {
    KNOWN_CLOUDS.iter().find(|(_, aliases)| aliases.iter().any(|alias| alias.eq_ignore_ascii_case(host)))
}

impl Authority {
    pub fn new(authority_host: &str, tenant: &str, client: reqwest::Client) -> azure_core::Result<Self> {
        static METADATA: OnceLock<Arc<Mutex<MetadataCache>>> = OnceLock::new();
        let metadata = METADATA.get_or_init(|| Arc::new(Mutex::new(MetadataCache::default()))).clone();
        Self::with_metadata(authority_host, tenant, client, metadata)
    }

    fn with_metadata(authority_host: &str, tenant: &str, client: reqwest::Client, metadata: Arc<Mutex<MetadataCache>>) -> azure_core::Result<Self> {
        if tenant.is_empty() || !tenant.bytes().all(|byte| byte.is_ascii_alphanumeric() || b".-".contains(&byte)) {
            return Err(invalid("Azure tenant ID contains unsupported characters"));
        }
        let authority = format!("{}/{tenant}/", authority_host.strip_suffix('/').unwrap_or(authority_host)).to_lowercase();
        let mut endpoint = Url::parse(&authority).map_err(|_| invalid("Azure authority host is not a valid URL"))?;
        if endpoint.scheme() != "https" || endpoint.host_str().is_none() || !endpoint.username().is_empty() || endpoint.password().is_some() || endpoint.query().is_some() || endpoint.fragment().is_some() {
            return Err(invalid("Azure authority host requires HTTPS without credentials, query or fragment"));
        }
        let path = endpoint.path().trim_end_matches('/');
        let segments: Vec<_> = path.strip_prefix('/').unwrap_or(path).split('/').collect();
        if segments.iter().any(|segment| segment.is_empty()) { return Err(invalid("Azure authority path contains an empty segment")); }
        let tenant = segments[0].to_owned();
        let host = endpoint.host_str().expect("validated authority host").to_owned();
        if tenant == "tfp" || host.contains("b2clogin.com") { return Err(invalid("Azure public client authority does not accept B2C authorities")); }
        let adfs = tenant == "adfs";
        endpoint.set_path(&format!("/{tenant}/oauth2/{}token", if adfs { "" } else { "v2.0/" }));
        if adfs || host.ends_with(".ciamlogin.com") {
            return Ok(Self { mode: Mode::Configured { endpoint, discovery: None, client, metadata } });
        }
        let mut discovery = endpoint.clone();
        if known_cloud(&host).is_none() {
            discovery.set_host(Some("login.microsoftonline.com")).map_err(|_| invalid("Azure discovery host is invalid"))?;
            discovery.set_port(None).map_err(|_| invalid("Azure discovery port is invalid"))?;
        }
        discovery.set_path("/common/discovery/instance");
        discovery.query_pairs_mut().append_pair("api-version", "1.1")
            .append_pair("authorization_endpoint", &format!("https://{host}/{tenant}/oauth2/v2.0/authorize"));
        Ok(Self { mode: Mode::Configured { endpoint, discovery: Some(discovery), client, metadata } })
    }

    #[cfg(test)]
    pub fn static_endpoint(endpoint: Url) -> Self { Self { mode: Mode::Direct(endpoint) } }

    pub fn user_realm_endpoint(&self, username: &str) -> azure_core::Result<Option<Url>> {
        let Mode::Configured { endpoint, discovery: Some(_), .. } = &self.mode else { return Ok(None); };
        let origin = &endpoint[..url::Position::BeforePath];
        Url::parse(&format!("{origin}/common/userrealm/{username}?api-version=1.0"))
            .map(Some).map_err(|_| invalid("Azure user realm endpoint is invalid"))
    }

    pub async fn token_endpoint(&self) -> azure_core::Result<Url> {
        let (endpoint, discovery, client, metadata) = match &self.mode {
            Mode::Configured { endpoint, discovery, client, metadata } => (endpoint, discovery, client, metadata),
            #[cfg(test)]
            Mode::Direct(endpoint) => return Ok(endpoint.clone()),
        };
        let host = endpoint.host_str().expect("validated authority host");
        let mut metadata = metadata.lock().await;
        if !metadata.aliases.contains_key(host) {
            let result = match discovery { Some(endpoint) => discover(client, endpoint.clone()).await?, None => Discovery::Fallback };
            match result {
                Discovery::Fallback => metadata.fallback(host)?,
                Discovery::Response { status, response } => {
                    if status == 400 && response.as_ref().and_then(|response| response.error.as_deref()) == Some("invalid_instance") {
                        return Err(invalid("Azure authority instance validation failed"));
                    }
                    if status != 200 { metadata.fallback(host)?; }
                    if status != 400 || response.as_ref().and_then(|response| response.error.as_ref()).is_some() {
                        let response = response.ok_or_else(|| invalid("Azure authority discovery returned no metadata"))?;
                        if response.tenant_discovery_endpoint.as_deref().is_none_or(java_blank) {
                            return Err(invalid("Azure authority discovery returned no tenant endpoint"));
                        }
                        metadata.publish(host, response)?;
                    }
                }
            }
        }
        let preferred = metadata.aliases.get(host).and_then(Option::as_deref)
            .ok_or_else(|| invalid("Azure authority metadata has no preferred network host"))?;
        let mut endpoint = endpoint.clone();
        endpoint.set_host(Some(preferred)).map_err(|_| invalid("Azure authority metadata has an invalid preferred network host"))?;
        Ok(endpoint)
    }
}

fn java_blank(value: &str) -> bool { value.trim_matches(|character| character <= '\u{20}').is_empty() }

enum Discovery { Fallback, Response { status: u16, response: Option<DiscoveryResponse> } }

async fn discover(client: &reqwest::Client, endpoint: Url) -> azure_core::Result<Discovery> {
    let Ok(response) = client.get(endpoint).timeout(Duration::from_secs(30)).send().await else { return Ok(Discovery::Fallback); };
    let status = response.status().as_u16();
    if response.content_length().is_some_and(|bytes| bytes > MAX_RESPONSE_BYTES as u64) {
        return Err(invalid("Azure authority response exceeds its byte limit"));
    }
    let mut bytes = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let Ok(chunk) = chunk else { return Ok(Discovery::Fallback); };
        if bytes.len().checked_add(chunk.len()).is_none_or(|size| size > MAX_RESPONSE_BYTES) {
            return Err(invalid("Azure authority response exceeds its byte limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    match parse_response(&bytes) {
        Ok(response) => Ok(Discovery::Response { status, response }),
        Err(_) => Ok(Discovery::Fallback),
    }
}

#[derive(Default)]
struct DiscoveryResponse {
    tenant_discovery_endpoint: Option<String>,
    metadata: Option<Vec<Option<MetadataEntry>>>,
    error: Option<String>,
}

#[derive(Default)]
struct MetadataEntry { preferred_network: Option<String>, aliases: Option<BTreeSet<Option<String>>> }

fn parse_response(bytes: &[u8]) -> azure_core::Result<Option<DiscoveryResponse>> {
    let text = rtdb_types::decode_java_utf8(bytes);
    let mut reader = Reader::new(&text);
    match reader.next()? {
        Token::Null => return Ok(None),
        Token::Object => (),
        _ => return Err(invalid("Azure authority discovery requires an object")),
    }
    let mut response = DiscoveryResponse::default();
    while reader.next()? != Token::EndObject {
        let field = reader.field().map(str::to_owned).ok_or_else(|| invalid("Azure authority discovery field is invalid"))?;
        let token = reader.next()?;
        match field.as_str() {
            "tenant_discovery_endpoint" => response.tenant_discovery_endpoint = reader.string().map(str::to_owned),
            "error" => response.error = reader.string().map(str::to_owned),
            "error_description" | "correlation_id" => { reader.string(); }
            "metadata" => response.metadata = read_array(&mut reader, token, parse_metadata)?,
            "error_codes" => { read_array(&mut reader, token, |reader, _| {
                let value = reader.number().ok_or_else(|| invalid("Azure authority error code is invalid"))?;
                let valid = if value.contains(['.', 'e', 'E']) {
                    value.parse::<f64>().is_ok_and(|value| value.is_finite() && value >= i64::MIN as f64 && value <= i64::MAX as f64)
                } else { value.parse::<i64>().is_ok() };
                if !valid { return Err(invalid("Azure authority error code is invalid")); }
                Ok(())
            })?; }
            _ => reader.skip_children()?,
        }
    }
    Ok(Some(response))
}

fn read_array<T>(reader: &mut Reader<'_>, token: Token, mut item: impl FnMut(&mut Reader<'_>, Token) -> azure_core::Result<T>) -> azure_core::Result<Option<Vec<T>>> {
    if token == Token::Null { return Ok(None); }
    if token != Token::Array { return Err(invalid("Azure authority metadata requires an array")); }
    let mut values = Vec::new();
    loop {
        let token = reader.next()?;
        if token == Token::EndArray { return Ok(Some(values)); }
        values.push(item(reader, token)?);
    }
}

fn parse_metadata(reader: &mut Reader<'_>, token: Token) -> azure_core::Result<Option<MetadataEntry>> {
    if token == Token::Null { return Ok(None); }
    if token != Token::Object { return Err(invalid("Azure authority metadata requires an object")); }
    let mut entry = MetadataEntry::default();
    while reader.next()? != Token::EndObject {
        let field = reader.field().map(str::to_owned).ok_or_else(|| invalid("Azure authority metadata field is invalid"))?;
        let token = reader.next()?;
        match field.as_str() {
            "preferred_network" => entry.preferred_network = reader.string().map(str::to_owned),
            "preferred_cache" => { reader.string(); }
            "aliases" => {
                let aliases = read_array(reader, token, |reader, _| Ok(reader.string().map(str::to_owned)))?
                    .ok_or_else(|| invalid("Azure authority metadata aliases are null"))?;
                entry.aliases = Some(aliases.into_iter().collect());
            }
            _ => reader.skip_children()?,
        }
    }
    Ok(Some(entry))
}

#[cfg(test)]
#[path = "authority_tests.rs"]
mod tests;
