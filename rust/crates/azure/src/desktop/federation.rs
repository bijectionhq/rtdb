use std::{collections::BTreeMap, sync::atomic::{AtomicBool, Ordering}};
use base64::{Engine, engine::general_purpose::URL_SAFE};
use futures::StreamExt;
use url::Url;
use super::{invalid, java_blank, json::{Reader, Token}, MAX_RESPONSE_BYTES};

mod xml;
use xml::Document;
type Result<T> = azure_core::Result<T>;
const WSDL: &str = "http://schemas.xmlsoap.org/wsdl/";
const POLICY: &str = "http://schemas.xmlsoap.org/ws/2004/09/policy";
const SECURITY: &str = "http://docs.oasis-open.org/ws-sx/ws-securitypolicy/200702";
const SECURITY_2005: &str = "http://schemas.xmlsoap.org/ws/2005/07/securitypolicy";
const ADDRESSING: &str = "http://www.w3.org/2005/08/addressing";
const SOAP_BINDING: &str = "http://schemas.xmlsoap.org/wsdl/soap12/";
const SOAP: &str = "http://www.w3.org/2003/05/soap-envelope";
const TRUST: &str = "http://docs.oasis-open.org/ws-sx/ws-trust/200512";
const TRUST_2005: &str = "http://schemas.xmlsoap.org/ws/2005/02/trust";
const SAML1: &str = "urn:oasis:names:tc:SAML:1.0:assertion";
static SECURITY_NAMESPACE_CHANGED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Version { Trust13, Trust2005 }
impl Version {
    fn namespace(self) -> &'static str { match self { Self::Trust13 => TRUST, Self::Trust2005 => TRUST_2005 } }
    fn action(self) -> &'static str { match self { Self::Trust13 => "http://docs.oasis-open.org/ws-sx/ws-trust/200512/RST/Issue", Self::Trust2005 => "http://schemas.xmlsoap.org/ws/2005/02/trust/RST/Issue" } }
}
struct Policy { endpoint: Url, address: String, version: Version }
#[derive(Clone)]
struct Candidate { id: String, endpoint: Option<String>, version: Option<Version> }
struct Realm { federated: bool, metadata: Option<String>, audience: Option<String> }
pub(super) struct Assertion { pub grant_type: &'static str, pub encoded: String }

pub(super) async fn discover(client: &reqwest::Client, realm_endpoint: Url, username: &str, password: &str) -> Result<Option<Assertion>> {
    let response = client.get(realm_endpoint).header(reqwest::header::ACCEPT, "application/json, text/javascript, */*").send().await
        .map_err(|_| invalid("Azure username/password user realm endpoint could not be reached"))?;
    if response.status() != reqwest::StatusCode::OK { return Err(invalid("Azure username/password user realm discovery failed")); }
    let realm = parse_realm(&read_body(response).await?)?;
    if !realm.federated { return Ok(None); }
    let metadata = realm.metadata.ok_or_else(|| invalid("Azure federation metadata endpoint is missing"))?;
    let metadata = Url::parse(&metadata).map_err(|_| invalid("Azure federation metadata endpoint is invalid"))?;
    let response = client.get(metadata).send().await.map_err(|_| invalid("Azure federation metadata endpoint could not be reached"))?;
    if response.status() != reqwest::StatusCode::OK { return Err(invalid("Azure federation metadata discovery failed")); }
    let bytes = read_body(response).await?;
    let text = rtdb_types::decode_java_utf8(&bytes);
    let policy = parse_policy(&text, SECURITY_NAMESPACE_CHANGED.load(Ordering::Acquire), true)?;
    let body = request(&policy, username, password, realm.audience.as_deref(), chrono::Utc::now(), &uuid()?, &uuid()?);
    if body.len() > MAX_RESPONSE_BYTES { return Err(invalid("Azure federation request exceeds its byte limit")); }
    let response = client.post(policy.endpoint.clone())
        .header(reqwest::header::CONTENT_TYPE, "application/soap+xml; charset=utf-8")
        .header("return-client-request-id", "true").header("SOAPAction", policy.version.action())
        .body(body).send().await.map_err(|_| invalid("Azure federation trust endpoint could not be reached"))?;
    let text = rtdb_types::decode_java_utf8(&read_body(response).await?);
    assertion(&text, policy.version).map(Some)
}

pub(super) async fn read_body(response: reqwest::Response) -> Result<Vec<u8>> {
    if response.content_length().is_some_and(|bytes| bytes > MAX_RESPONSE_BYTES as u64) { return Err(invalid("Azure credential response exceeds its byte limit")); }
    let mut bytes = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| invalid("Azure credential response could not be read"))?;
        if bytes.len().checked_add(chunk.len()).is_none_or(|size| size > MAX_RESPONSE_BYTES) { return Err(invalid("Azure credential response exceeds its byte limit")); }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn parse_realm(bytes: &[u8]) -> Result<Realm> {
    let text = rtdb_types::decode_java_utf8(bytes);
    let mut reader = Reader::new(&text);
    if reader.next()? != Token::Object { return Err(invalid("Azure user realm response requires an object")); }
    let mut account = None;
    let mut metadata = None;
    let mut audience = None;
    while reader.next()? != Token::EndObject {
        let field = reader.field().map(str::to_owned).ok_or_else(|| invalid("Azure user realm field is invalid"))?;
        reader.next()?;
        match field.as_str() {
            "account_type" => account = reader.string().map(str::to_owned),
            "federation_metadata_url" => metadata = reader.string().map(str::to_owned),
            "cloud_audience_urn" => audience = reader.string().map(str::to_owned),
            "ver" => if !reader.string().is_some_and(java_float_literal) { return Err(invalid("Azure user realm version is invalid")); },
            "federation_protocol" | "federation_active_auth_url" => { reader.string(); }
            _ => reader.skip_children()?,
        }
    }
    Ok(Realm { federated: account.is_some_and(|value| value.eq_ignore_ascii_case("Federated")), metadata, audience })
}

fn java_float_literal(value: &str) -> bool {
    let value = value.trim_matches(|character| character <= '\u{20}');
    let value = value.strip_prefix(['+', '-']).unwrap_or(value);
    if matches!(value, "NaN" | "Infinity") { return true; }
    let value = value.strip_suffix(['f', 'F', 'd', 'D']).unwrap_or(value);
    let (value, hex) = if let Some(value) = value.strip_prefix("0x").or_else(|| value.strip_prefix("0X")) { (value, true) } else { (value, false) };
    let exponent = value.find(if hex { &['p', 'P'][..] } else { &['e', 'E'][..] });
    if hex && exponent.is_none() { return false; }
    let mantissa = if let Some(index) = exponent {
        let exponent = value[index + 1..].strip_prefix(['+', '-']).unwrap_or(&value[index + 1..]);
        if exponent.is_empty() || !exponent.bytes().all(|byte| byte.is_ascii_digit()) { return false; }
        &value[..index]
    } else { value };
    let mut digits = 0;
    let mut dots = 0;
    for byte in mantissa.bytes() {
        if byte == b'.' { dots += 1; }
        else if if hex { byte.is_ascii_hexdigit() } else { byte.is_ascii_digit() } { digits += 1; }
        else { return false; }
    }
    digits > 0 && dots <= 1
}

fn map_order(candidates: &[Candidate]) -> Result<Vec<usize>> {
    rtdb_types::java_hash_map_order(16, candidates.iter().map(|candidate| &candidate.id),
        |key| key.encode_utf16().fold(0i32, |hash, unit| hash.wrapping_mul(31).wrapping_add(i32::from(unit))),
        |left, right| left.encode_utf16().cmp(right.encode_utf16())).ok_or_else(|| invalid("Azure federation policy map exceeds its capacity"))
}

fn parse_policy(text: &str, legacy_namespace: bool, publish_namespace: bool) -> Result<Policy> {
    let document = Document::parse(text)?;
    let definitions = document.descendants(WSDL, "definitions");
    let policies = document.path(definitions.clone(), &[(POLICY, "Policy")]);
    let mut candidates = select_policies(&document, &policies, if legacy_namespace { SECURITY_2005 } else { SECURITY }, "SignedEncryptedSupportingTokens");
    if publish_namespace { SECURITY_NAMESPACE_CHANGED.store(true, Ordering::Release); }
    let second = select_policies(&document, &policies, SECURITY_2005, "SignedSupportingTokens");
    let second = map_order(&second)?.into_iter().map(|index| second[index].clone()).collect::<Vec<_>>();
    let addition_keys = second.iter().map(|candidate| &candidate.id).collect::<Vec<_>>();
    let order = rtdb_types::java_hash_map_put_all_order(None, candidates.iter().map(|candidate| &candidate.id), &addition_keys,
        |key| key.encode_utf16().fold(0i32, |hash, unit| hash.wrapping_mul(31).wrapping_add(i32::from(unit))),
        |left, right| left.encode_utf16().cmp(right.encode_utf16())).ok_or_else(|| invalid("Azure federation policy map exceeds its capacity"))?;
    candidates.extend(second);
    let mut selected = order.iter().map(|index| (candidates[*index].id.clone(), *index)).collect::<BTreeMap<_, _>>();
    let mut bindings = BTreeMap::new();
    for reference in document.path(definitions.clone(), &[(WSDL, "binding"), (POLICY, "PolicyReference")]) {
        let uri = document.attribute(reference, "URI").ok_or_else(|| invalid("Azure federation binding has no policy reference"))?;
        if !selected.contains_key(uri) { continue; }
        let binding = document.parent(reference).expect("policy reference parent");
        let name = document.attribute(binding, "name").ok_or_else(|| invalid("Azure federation binding has no name"))?;
        let action = document.path(vec![binding], &[(WSDL, "operation"), (SOAP_BINDING, "operation")]).into_iter().find_map(|node| document.attribute(node, "soapAction"));
        let transport = document.children(binding, SOAP_BINDING, "binding").into_iter().find_map(|node| document.attribute(node, "transport"));
        if !transport.is_some_and(|transport| transport.eq_ignore_ascii_case("http://schemas.xmlsoap.org/soap/http")) { continue; }
        let version = if action.is_some_and(|action| action.eq_ignore_ascii_case(Version::Trust13.action())) { Some(Version::Trust13) }
            else if action.is_some_and(|action| action.eq_ignore_ascii_case(Version::Trust2005.action())) { Some(Version::Trust2005) } else { None };
        if let Some(version) = version { bindings.insert(name.to_owned(), (uri.to_owned(), version)); }
    }
    for port in document.path(definitions, &[(WSDL, "service"), (WSDL, "port")]) {
        let binding = document.attribute(port, "binding").ok_or_else(|| invalid("Azure federation port has no binding"))?;
        let local = binding.trim_end_matches(':');
        if local.is_empty() && !binding.is_empty() { return Err(invalid("Azure federation port binding is invalid")); }
        let Some((id, version)) = bindings.get(local.rsplit(':').next().unwrap_or(binding)) else { continue; };
        let Some(index) = selected.get_mut(id) else { continue; };
        let candidate = &mut candidates[*index];
        if candidate.endpoint.as_deref().is_some_and(|endpoint| !java_blank(endpoint)) { continue; }
        candidate.version = Some(*version);
        let address = document.path(vec![port], &[(ADDRESSING, "EndpointReference"), (ADDRESSING, "Address")]).into_iter().next()
            .ok_or_else(|| invalid("Azure federation port has no address"))?;
        let address = document.text_content(address);
        if address.to_ascii_lowercase().starts_with("https://") { candidate.endpoint = Some(address.trim_matches(|character| character <= '\u{20}').to_owned()); }
    }
    let mut trust13 = None;
    let mut trust2005 = None;
    for index in order {
        let candidate = &candidates[index];
        if let Some(endpoint) = &candidate.endpoint { match candidate.version {
            Some(Version::Trust13) => trust13 = Some(endpoint), Some(Version::Trust2005) => trust2005 = Some(endpoint), None => (),
        } }
    }
    let (endpoint, version) = if let Some(endpoint) = trust13 { (endpoint, Version::Trust13) } else if let Some(endpoint) = trust2005 { (endpoint, Version::Trust2005) }
        else { return Err(invalid("Azure federation metadata has no username/password trust endpoint")); };
    let address = endpoint.to_owned();
    let endpoint = Url::parse(endpoint).map_err(|_| invalid("Azure federation trust endpoint is invalid"))?;
    Ok(Policy { endpoint, address, version })
}

fn select_policies(document: &Document, policies: &[usize], namespace: &str, tokens: &str) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    for policy in policies {
        let matches = document.path(vec![*policy], &[(POLICY, "ExactlyOne"), (POLICY, "All"), (namespace, tokens), (POLICY, "Policy"), (namespace, "UsernameToken"), (POLICY, "Policy"), (namespace, "WssUsernameToken10")]);
        for _ in matches {
            let transport = !document.path(vec![*policy], &[(POLICY, "ExactlyOne"), (POLICY, "All"), (namespace, "TransportBinding")]).is_empty()
                || !document.path(vec![*policy], &[(POLICY, "ExactlyOne"), (POLICY, "All"), (SECURITY_2005, "TransportBinding")]).is_empty();
            let id = if transport { document.attribute(*policy, "wsu:Id").unwrap_or("null") } else { "null" };
            candidates.push(Candidate { id: format!("#{id}"), endpoint: None, version: None });
        }
    }
    candidates
}

fn assertion(text: &str, version: Version) -> Result<Assertion> {
    let document = Document::parse(text)?;
    let bodies = document.path(document.descendants(SOAP, "Envelope"), &[(SOAP, "Body")]);
    let fault_values = document.path(bodies.clone(), &[(SOAP, "Fault"), (SOAP, "Code"), (SOAP, "Subcode"), (SOAP, "Value")]);
    if fault_values.len() > 1 { return Err(invalid("Azure federation response contains multiple fault codes")); }
    if let Some(fault) = fault_values.first() {
        let code = document.first_child_text(*fault).ok_or_else(|| invalid("Azure federation fault code is invalid"))?;
        if !code.contains(':') { return Err(invalid("Azure federation fault code is invalid")); }
        return Err(invalid("Azure federation trust service rejected the credentials"));
    }
    let responses = if version == Version::Trust13 {
        document.path(bodies, &[(TRUST, "RequestSecurityTokenResponseCollection"), (TRUST, "RequestSecurityTokenResponse")])
    } else { document.path(bodies, &[(TRUST_2005, "RequestSecurityTokenResponse")]) };
    for token_type in document.path(responses, &[(version.namespace(), "TokenType")]) {
        let parent = document.parent(token_type).expect("token type parent");
        let tokens = document.children(parent, version.namespace(), "RequestedSecurityToken");
        if tokens.len() > 1 { return Err(invalid("Azure federation response contains multiple requested security tokens")); }
        let Some(token) = tokens.first() else { continue; };
        let token = document.inner_xml(*token)?;
        if java_blank(&token) { continue; }
        let saml1 = document.text_content(token_type).eq_ignore_ascii_case(SAML1);
        return Ok(Assertion { grant_type: if saml1 { "urn:ietf:params:oauth:grant-type:saml1_1-bearer" } else { "urn:ietf:params:oauth:grant-type:saml2-bearer" }, encoded: URL_SAFE.encode(token.as_bytes()) });
    }
    Err(invalid("Azure federation response contains no security token"))
}

fn request(policy: &Policy, username: &str, password: &str, audience: Option<&str>, now: chrono::DateTime<chrono::Utc>, message_id: &str, token_id: &str) -> String {
    let expires = now + chrono::Duration::minutes(10);
    let created = now.format("%Y-%m-%dT%H:%M:%SZ");
    let expires = expires.format("%Y-%m-%dT%H:%M:%SZ");
    let audience = audience.filter(|audience| !java_blank(audience)).unwrap_or("urn:federation:MicrosoftOnline");
    let namespace = policy.version.namespace();
    let action = policy.version.action();
    let key = if policy.version == Version::Trust13 { "http://docs.oasis-open.org/ws-sx/ws-trust/200512/Bearer" } else { "http://schemas.xmlsoap.org/ws/2005/05/identity/NoProofKey" };
    let username = escape_credentials(username);
    let password = escape_credentials(password);
    let endpoint = &policy.address;
    format!("<s:Envelope xmlns:s='{SOAP}' xmlns:a='{ADDRESSING}' xmlns:u='http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd'><s:Header><a:Action s:mustUnderstand='1'>{action}</a:Action><a:messageID>urn:uuid:{message_id}</a:messageID><a:ReplyTo><a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address></a:ReplyTo><a:To s:mustUnderstand='1'>{endpoint}</a:To><o:Security s:mustUnderstand='1' xmlns:o='http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd'><u:Timestamp u:Id='_0'><u:Created>{created}</u:Created><u:Expires>{expires}</u:Expires></u:Timestamp><o:UsernameToken u:Id='uuid-{token_id}'><o:Username>{username}</o:Username><o:Password>{password}</o:Password></o:UsernameToken></o:Security></s:Header><s:Body><trust:RequestSecurityToken xmlns:trust='{namespace}'><wsp:AppliesTo xmlns:wsp='{POLICY}'><a:EndpointReference><a:Address>{audience}</a:Address></a:EndpointReference></wsp:AppliesTo><trust:KeyType>{key}</trust:KeyType><trust:RequestType>{namespace}/Issue</trust:RequestType></trust:RequestSecurityToken></s:Body></s:Envelope>")
}

fn escape_credentials(value: &str) -> String {
    let mut escaped = String::new();
    for character in value.chars() { match character {
        '<' => escaped.push_str("&lt;"), '>' => escaped.push_str("&gt;"), '"' => escaped.push_str("&quot;"), '\'' => escaped.push_str("&apos;"), '&' => escaped.push_str("&amp;"), character => escaped.push(character),
    } }
    escaped
}
fn uuid() -> Result<String> {
    let mut bytes = [0u8; 16];
    openssl::rand::rand_bytes(&mut bytes).map_err(|_| invalid("Azure federation request identifier could not be generated"))?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40; bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let mut text = String::with_capacity(36);
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) { text.push('-'); }
        use std::fmt::Write; write!(text, "{byte:02x}").expect("string formatting");
    }
    Ok(text)
}

#[cfg(test)]
#[path = "federation_tests.rs"]
mod tests;
