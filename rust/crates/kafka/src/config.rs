use std::{collections::BTreeMap, fs::File, io::Read, mem::size_of, path::PathBuf, sync::Arc, time::Duration};
use rtdb_format::generated::xtdb::kafka::proto as pb;
use rtdb_types::{AllocationScope, Anomaly, MemoryBudget, Result, jvm_number::Number};
use serde::{Deserialize, Serialize};

pub(crate) mod providers;

pub(crate) fn trim_java(value: &str) -> &str { value.trim_matches(|ch| ch <= '\u{20}') }

pub(crate) fn signed_integer(value: &str) -> Option<i64> {
    let value = trim_java(value);
    if value.chars().any(|ch| ch.len_utf16() == 2) { return None; }
    match Number::parse_long(value).ok()? {
        Number::Long(value) => Some(value),
        _ => unreachable!("signed decimal parser returns a long"),
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct KafkaClusterConfig {
    pub bootstrap_servers: String,
    #[serde(default)]
    pub properties_map: BTreeMap<String, String>,
    pub properties_file: Option<PathBuf>,
    pub schema_registry_url: Option<String>,
}

impl std::fmt::Debug for KafkaClusterConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("KafkaClusterConfig").field("bootstrap_servers", &self.bootstrap_servers)
            .field("property_names", &self.properties_map.keys().collect::<Vec<_>>()).field("properties_file", &self.properties_file).finish_non_exhaustive()
    }
}

impl KafkaClusterConfig {
    pub fn new(bootstrap_servers: impl Into<String>) -> Self { Self { bootstrap_servers: bootstrap_servers.into(), properties_map: BTreeMap::new(), properties_file: None, schema_registry_url: None } }
    pub fn properties(&self) -> Result<BTreeMap<String, String>> {
        let mut properties = self.inline_properties();
        if let Some(path) = &self.properties_file {
            properties.extend(parse_properties(&std::fs::read(path).map_err(|error| Anomaly::incorrect("xtdb.kafka/properties-file", error.to_string()))?)?);
        }
        Ok(properties)
    }

    pub(crate) fn capture_client_properties(&self, memory: &Arc<MemoryBudget>) -> Result<ClientProperties> {
        if self.properties_file.is_none() && !providers::configured(&self.properties_map) {
            return Ok(ClientProperties { values: self.inline_properties(), allocation: None });
        }
        let staging = AllocationScope::new(memory)?;
        staging.reserve(properties_bound(&self.properties_map)?.checked_add(self.bootstrap_servers.capacity())
            .and_then(|bytes| bytes.checked_add(512)).ok_or_else(property_allocation)?)?;
        let mut values = self.inline_properties();
        if let Some(path) = &self.properties_file {
            let input = File::open(path).map_err(property_io)?;
            let bytes = read_allocated(input, &mut |bytes| staging.reserve(bytes), property_io, property_allocation)?;
            let parse = bytes.len().checked_mul(96).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(property_allocation)?;
            staging.reserve(parse)?;
            values.extend(parse_properties(&bytes)?);
        }
        let allocation = AllocationScope::new(memory)?;
        allocation.reserve(properties_bound(&values)?)?;
        Ok(ClientProperties { values, allocation: Some(allocation) })
    }

    fn inline_properties(&self) -> BTreeMap<String, String> {
        let mut properties = BTreeMap::from([("bootstrap.servers".into(), self.bootstrap_servers.clone())]);
        properties.extend(self.properties_map.clone());
        properties
    }
}

pub(crate) struct ClientProperties {
    pub(crate) values: BTreeMap<String, String>,
    pub(crate) allocation: Option<AllocationScope>,
}

pub(crate) fn properties_bound(properties: &BTreeMap<String, String>) -> Result<usize> {
    properties.iter().try_fold(4096usize, |sum, (name, value)| {
        sum.checked_add(512)?.checked_add(name.capacity())?.checked_add(value.capacity())
    }).ok_or_else(property_allocation)
}

fn read_allocated(
    mut input: File,
    reserve: &mut impl FnMut(usize) -> Result<()>,
    io_error: impl Fn(std::io::Error) -> Anomaly,
    allocation_error: fn() -> Anomaly,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut block = [0; 8192];
    loop {
        let length = match input.read(&mut block) {
            Ok(0) => return Ok(bytes),
            Ok(length) => length,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(io_error(error)),
        };
        let needed = bytes.len().checked_add(length).ok_or_else(allocation_error)?;
        if needed > bytes.capacity() {
            let capacity = bytes.capacity().checked_mul(2).ok_or_else(allocation_error)?.max(needed);
            reserve(capacity)?;
            bytes.try_reserve_exact(capacity - bytes.len()).map_err(|_| allocation_error())?;
        }
        bytes.extend_from_slice(&block[..length]);
    }
}

fn property_io(error: std::io::Error) -> Anomaly { Anomaly::incorrect("xtdb.kafka/properties-file", error.to_string()) }
pub(crate) fn property_allocation() -> Anomaly { Anomaly::incorrect("xtdb.kafka/config-allocation", "Kafka configuration exceeds addressable memory") }

pub fn producer_properties(properties: &BTreeMap<String, String>, pipelined: bool) -> BTreeMap<String, String> {
    let mut result = BTreeMap::from([("enable.idempotence".into(), "true".into()), ("compression.type".into(), "snappy".into())]);
    if !pipelined { result.insert("linger.ms".into(), "0".into()); }
    result.extend(properties.clone());
    result.insert("acks".into(), "all".into());
    if result.get("enable.idempotence").is_some_and(|value| value == "false") { result.insert("max.in.flight.requests.per.connection".into(), "1".into()); }
    result
}

/// Java client properties librdkafka has no equivalent for.
/// kafka-clients would apply them; the native client ignores them, and [`native_client_properties`] reports each one it drops.
const IGNORED_JAVA_PROPERTIES: &[&str] = &[
    "auto.include.jmx.reporter", "default.api.timeout.ms", "exclude.internal.topics", "max.block.ms", "max.poll.records",
    "metadata.max.idle.ms", "metadata.recovery.rebootstrap.trigger.ms", "metric.reporters", "metrics.num.samples",
    "metrics.recording.level", "metrics.sample.window.ms", "partitioner.adaptive.partitioning.enable",
    "partitioner.availability.timeout.ms", "partitioner.class", "partitioner.ignore.keys", "rebalance.timeout.ms",
    "sasl.kerberos.ticket.renew.jitter", "sasl.kerberos.ticket.renew.window.factor", "sasl.login.connect.timeout.ms",
    "sasl.login.read.timeout.ms", "sasl.login.refresh.buffer.seconds", "sasl.login.refresh.min.period.seconds",
    "sasl.login.refresh.window.factor", "sasl.login.refresh.window.jitter", "sasl.login.retry.backoff.max.ms",
    "sasl.login.retry.backoff.ms", "sasl.oauthbearer.clock.skew.seconds", "sasl.oauthbearer.expected.audience",
    "sasl.oauthbearer.expected.issuer", "sasl.oauthbearer.header.urlencode", "sasl.oauthbearer.jwks.endpoint.refresh.ms",
    "sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms", "sasl.oauthbearer.jwks.endpoint.retry.backoff.ms",
    "sasl.oauthbearer.jwks.endpoint.url", "sasl.oauthbearer.scope.claim.name", "sasl.oauthbearer.sub.claim.name",
    "security.providers", "socket.connection.setup.timeout.max.ms",
];
const SERIALIZER_PROPERTIES: &[&str] = &["key.serializer", "value.serializer", "key.deserializer", "value.deserializer"];
const UNSUPPORTED_JAVA_PROPERTIES: &[&str] = &["interceptor.classes", "sasl.client.callback.handler.class", "sasl.login.callback.handler.class", "sasl.login.class"];

pub fn native_properties(properties: &BTreeMap<String, String>) -> Result<BTreeMap<String, String>> {
    native_client_properties(properties).map(|(properties, _)| properties)
}

/// Translates Java client properties to their librdkafka equivalents, returning the Java-only names it dropped.
pub(crate) fn native_client_properties(properties: &BTreeMap<String, String>) -> Result<(BTreeMap<String, String>, Vec<String>)> {
    let mut properties = properties.clone();
    let ignored = java_client_properties(&mut properties)?;
    if let Some(wait) = properties.remove("fetch.max.wait.ms") {
        let wait = signed_integer(&wait).and_then(|wait| i32::try_from(wait).ok()).filter(|wait| *wait >= 0)
            .ok_or_else(|| Anomaly::incorrect("xtdb.kafka/invalid-config", "fetch.max.wait.ms must be a nonnegative signed integer"))?;
        if properties.get("fetch.wait.max.ms").is_some_and(|native| signed_integer(native) != Some(i64::from(wait))) {
            return Err(Anomaly::incorrect("xtdb.kafka/conflicting-config", "Java and native maximum fetch wait settings disagree"));
        }
        properties.insert("fetch.wait.max.ms".into(), wait.to_string());
    }
    if let Some(jaas) = properties.remove("sasl.jaas.config") {
        let (module, mut options) = parse_jaas(&jaas)?;
        match module.as_str() {
            "org.apache.kafka.common.security.plain.PlainLoginModule" | "org.apache.kafka.common.security.scram.ScramLoginModule" => {
                for (jaas, native) in [("username", "sasl.username"), ("password", "sasl.password")] {
                    let value = options.remove(jaas).ok_or_else(jaas_error)?;
                    if properties.get(native).is_some_and(|configured| configured != &value) { return Err(Anomaly::incorrect("xtdb.kafka/conflicting-config", format!("JAAS and native {native} disagree"))); }
                    properties.insert(native.into(), value);
                }
                if module == "org.apache.kafka.common.security.scram.ScramLoginModule" {
                    let enabled = options.get("tokenauth").is_some_and(|value| value.eq_ignore_ascii_case("true"));
                    if let Some(configured) = properties.get("sasl.scram.tokenauth") {
                        let configured = if ["true", "t", "1"].iter().any(|value| configured.eq_ignore_ascii_case(value)) { Some(true) }
                            else if ["false", "f", "0"].iter().any(|value| configured.eq_ignore_ascii_case(value)) { Some(false) } else { None };
                        if configured != Some(enabled) { return Err(Anomaly::incorrect("xtdb.kafka/conflicting-config", "JAAS and native sasl.scram.tokenauth disagree")); }
                    }
                    properties.insert("sasl.scram.tokenauth".into(), enabled.to_string());
                }
            }
            "com.sun.security.auth.module.Krb5LoginModule" => {
                for (jaas, native) in [("principal", "sasl.kerberos.principal"), ("serviceName", "sasl.kerberos.service.name")] {
                    if let Some(value) = options.remove(jaas) {
                        if properties.get(native).is_some_and(|configured| configured != &value) {
                            return Err(Anomaly::incorrect("xtdb.kafka/conflicting-config", format!("JAAS and native {native} disagree")));
                        }
                        properties.insert(native.into(), value);
                    }
                }
                let use_keytab = options.get("useKeyTab").is_none_or(|value| value.eq_ignore_ascii_case("true"));
                if use_keytab && let Some(keytab) = options.remove("keyTab") {
                    if properties.get("sasl.kerberos.keytab").is_some_and(|configured| configured != &keytab) {
                        return Err(Anomaly::incorrect("xtdb.kafka/conflicting-config", "JAAS and native sasl.kerberos.keytab disagree"));
                    }
                    properties.insert("sasl.kerberos.keytab".into(), keytab);
                }
                if !properties.contains_key("sasl.kerberos.keytab")
                    && !properties.contains_key("sasl.kerberos.kinit.cmd")
                    && !properties.contains_key("sasl.kerberos.min.time.before.relogin")
                {
                    properties.insert("sasl.kerberos.min.time.before.relogin".into(), "0".into());
                }
            }
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule" => {
                translate_oauthbearer_jaas(&options, &mut properties)?;
            }
            _ => return Err(Anomaly::unsupported("xtdb.kafka/jaas-login-module", "Kafka login module requires a native credential adapter")),
        }
    }
    Ok((properties, ignored))
}

fn translate_oauthbearer_jaas(options: &BTreeMap<String, String>, properties: &mut BTreeMap<String, String>) -> Result<()> {
    if options.contains_key("clientId") || options.contains_key("clientSecret") || properties.contains_key("sasl.oauthbearer.token.endpoint.url") {
        return Err(Anomaly::unsupported("xtdb.kafka/jaas-login-module", "Kafka OAUTHBEARER OIDC login requires a native credential adapter"));
    }
    let principal_claim = options.get("unsecuredLoginPrincipalClaimName").map_or("sub", String::as_str);
    let scope_claim = options.get("unsecuredLoginScopeClaimName").map_or("scope", String::as_str);
    let principal_key = format!("unsecuredLoginStringClaim_{principal_claim}");
    let scope_list_key = format!("unsecuredLoginListClaim_{scope_claim}");
    let scope_string_key = format!("unsecuredLoginStringClaim_{scope_claim}");
    let valid_token = |value: &str| !value.is_empty() && !value.chars().any(char::is_whitespace);
    let mut parts = Vec::new();
    if let Some(claim) = options.get("unsecuredLoginPrincipalClaimName") {
        if !valid_token(claim) { return Err(invalid_config("unsecuredLoginPrincipalClaimName must be a non-empty token without whitespace")); }
        parts.push(format!("principalClaimName={claim}"));
    }
    if let Some(principal) = options.get(&principal_key) {
        if !valid_token(principal) { return Err(invalid_config("OAuthBearer unsecured principal must be a non-empty token without whitespace")); }
        parts.push(format!("principal={principal}"));
    }
    if let Some(claim) = options.get("unsecuredLoginScopeClaimName") {
        if !valid_token(claim) { return Err(invalid_config("unsecuredLoginScopeClaimName must be a non-empty token without whitespace")); }
        parts.push(format!("scopeClaimName={claim}"));
    }
    if let Some(list_value) = options.get(&scope_list_key) {
        let mut chars = list_value.chars();
        let Some(delimiter) = chars.next() else { return Err(invalid_config("OAuthBearer unsecured scope list must start with a delimiter")); };
        let items: Vec<&str> = chars.as_str().split(delimiter).filter(|item| !item.is_empty()).collect();
        if items.is_empty() || items.iter().any(|item| item.contains(',') || item.chars().any(char::is_whitespace)) {
            return Err(invalid_config("OAuthBearer unsecured scope list entries cannot contain commas or whitespace"));
        }
        parts.push(format!("scope={}", items.join(",")));
    } else if let Some(scope) = options.get(&scope_string_key) {
        if !valid_token(scope) { return Err(invalid_config("OAuthBearer unsecured scope must be a non-empty token without whitespace")); }
        parts.push(format!("scope={scope}"));
    }
    if let Some(lifetime) = options.get("unsecuredLoginLifetimeSeconds") {
        let seconds = signed_integer(lifetime).and_then(|value| i32::try_from(value).ok()).filter(|value| *value > 0)
            .ok_or_else(|| invalid_config("unsecuredLoginLifetimeSeconds must be a positive 32-bit integer"))?;
        parts.push(format!("lifeSeconds={seconds}"));
    }
    for (key, value) in options {
        if let Some(ext_key) = key.strip_prefix("unsecuredLoginExtension_") {
            if ext_key.is_empty() || !ext_key.bytes().all(|byte| byte.is_ascii_alphabetic()) || value.contains(' ') {
                return Err(invalid_config("OAuthBearer unsecured extension key must be alphabetic and value cannot contain spaces"));
            }
            parts.push(format!("extension_{ext_key}={value}"));
        } else if key.starts_with("unsecuredLoginNumberClaim_")
            || (key.starts_with("unsecuredLoginStringClaim_") && key != &principal_key && key != &scope_string_key)
            || (key.starts_with("unsecuredLoginListClaim_") && key != &scope_list_key)
        {
            return Err(Anomaly::unsupported("xtdb.kafka/jaas-login-module", "Custom OAuthBearer unsecured claims require a native credential adapter"));
        }
    }
    if !parts.is_empty() {
        let config = parts.join(" ");
        if properties.get("sasl.oauthbearer.config").is_some_and(|configured| configured != &config) {
            return Err(Anomaly::incorrect("xtdb.kafka/conflicting-config", "JAAS and native sasl.oauthbearer.config disagree"));
        }
        properties.insert("sasl.oauthbearer.config".into(), config);
    } else if !properties.contains_key("sasl.oauthbearer.config") {
        return Err(Anomaly::incorrect("xtdb.kafka/invalid-jaas-config", "Kafka OAuthBearerLoginModule requires unsecured login options or native sasl.oauthbearer.config"));
    }
    Ok(())
}

fn java_client_properties(properties: &mut BTreeMap<String, String>) -> Result<Vec<String>> {
    for name in UNSUPPORTED_JAVA_PROPERTIES {
        if properties.remove(*name).is_some_and(|value| {
            let trimmed = trim_java(&value);
            !trimmed.is_empty() && !(*name == "sasl.login.callback.handler.class"
                && trimmed == "org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler")
        }) {
            return Err(Anomaly::unsupported("xtdb.kafka/unsupported-config", format!("Kafka property {name} requires a native adapter")));
        }
    }
    for name in SERIALIZER_PROPERTIES { properties.remove(*name); }
    let mut ignored: Vec<String> = IGNORED_JAVA_PROPERTIES.iter().filter(|name| properties.remove(**name).is_some()).map(|name| (*name).into()).collect();
    for (java, native) in [("send.buffer.bytes", "socket.send.buffer.bytes"), ("receive.buffer.bytes", "socket.receive.buffer.bytes")] {
        if let Some(value) = properties.remove(java) {
            let size = signed_integer(&value).and_then(|size| i32::try_from(size).ok()).filter(|size| *size >= -1)
                .ok_or_else(|| invalid_config(format!("{java} must be -1 or a nonnegative integer")))?;
            translate(properties, java, native, size.max(0).to_string())?;
        }
    }
    if let Some(value) = properties.remove("buffer.memory") {
        let kilobytes = signed_integer(&value).and_then(|bytes| u64::try_from(bytes).ok()).map(|bytes| bytes.div_ceil(1024).max(1))
            .filter(|kilobytes| *kilobytes <= i32::MAX as u64).ok_or_else(|| invalid_config("buffer.memory must be a nonnegative integer within the native queue limit"))?;
        translate(properties, "buffer.memory", "queue.buffering.max.kbytes", kilobytes.to_string())?;
    }
    if let Some(value) = properties.remove("max.request.size") {
        translate(properties, "max.request.size", "message.max.bytes", trim_java(&value).into())?;
    }
    let codec = properties.get("compression.type").map(|codec| trim_java(codec).to_ascii_lowercase());
    for java_codec in ["gzip", "lz4", "zstd"] {
        let name = format!("compression.{java_codec}.level");
        if let Some(level) = properties.remove(&name) {
            if codec.as_deref() == Some(java_codec) { translate(properties, &name, "compression.level", trim_java(&level).into())?; }
        }
    }
    if let Some(reset) = properties.get_mut("auto.offset.reset") {
        if trim_java(reset) == "none" { *reset = "error".into(); }
    }
    if let Some(strategy) = properties.remove("partition.assignment.strategy") {
        let native = strategy.split(',').map(trim_java).filter(|assignor| !assignor.is_empty()).map(|assignor| match assignor {
            "org.apache.kafka.clients.consumer.RangeAssignor" => Some("range"),
            "org.apache.kafka.clients.consumer.RoundRobinAssignor" => Some("roundrobin"),
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor" => Some("cooperative-sticky"),
            native if !native.contains('.') => Some(native),
            _ => None,
        }).collect::<Option<Vec<_>>>();
        match native {
            Some(native) if !native.is_empty() => { properties.insert("partition.assignment.strategy".into(), native.join(",")); }
            _ => ignored.push("partition.assignment.strategy".into()),
        }
    }
    Ok(ignored)
}

fn translate(properties: &mut BTreeMap<String, String>, java: &str, native: &str, value: String) -> Result<()> {
    if properties.get(native).is_some_and(|configured| trim_java(configured) != value) {
        return Err(Anomaly::incorrect("xtdb.kafka/conflicting-config", format!("Java {java} and native {native} disagree")));
    }
    properties.insert(native.into(), value);
    Ok(())
}

fn invalid_config(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb.kafka/invalid-config", message.into()) }

pub(crate) fn parse_jaas(jaas: &str) -> Result<(String, BTreeMap<String, String>)> {
    let mut tokens = jaas_tokens(jaas).into_iter();
    let Some(JaasToken::Value(module)) = tokens.next() else { return Err(jaas_error()); };
    if !matches!(tokens.next(), Some(JaasToken::Value(flag)) if jaas_flag(&flag)) { return Err(jaas_error()); }
    let mut options = BTreeMap::new();
    loop {
        let key = match tokens.next() { Some(JaasToken::End) => break, Some(JaasToken::Value(key)) => Some(key), Some(_) => None, None => return Err(jaas_error()) };
        if !matches!(tokens.next(), Some(JaasToken::Equals)) { return Err(jaas_error()); }
        let Some(JaasToken::Value(value)) = tokens.next() else { return Err(jaas_error()); };
        if let Some(key) = key { options.insert(key, value); }
    }
    if tokens.next().is_some() { return Err(jaas_error()); }
    Ok((module, options))
}

fn jaas_flag(flag: &str) -> bool {
    let mut upper = String::with_capacity(flag.len());
    // Java25 Locale.ROOT uppercase mappings whose entire result is ASCII (Unicode16).
    for character in flag.chars() {
        match character {
            '\u{131}' => upper.push('I'), '\u{17f}' => upper.push('S'), '\u{df}' => upper.push_str("SS"),
            '\u{fb00}' => upper.push_str("FF"), '\u{fb01}' => upper.push_str("FI"), '\u{fb02}' => upper.push_str("FL"),
            '\u{fb03}' => upper.push_str("FFI"), '\u{fb04}' => upper.push_str("FFL"), '\u{fb05}' | '\u{fb06}' => upper.push_str("ST"),
            character if character.is_ascii() => upper.push(character.to_ascii_uppercase()),
            _ => return false,
        }
    }
    matches!(upper.as_str(), "REQUIRED" | "REQUISITE" | "SUFFICIENT" | "OPTIONAL")
}

fn jaas_error() -> Anomaly { Anomaly::incorrect("xtdb.kafka/invalid-jaas-config", "Kafka JAAS requires one supported login module, a valid control flag, and username/password options") }

enum JaasToken { Value(String), Equals, End, Other }

fn jaas_word(character: char) -> bool { character >= '\u{a0}' || character.is_ascii_alphabetic() || matches!(character, '_' | '$' | '-') }

fn jaas_tokens(input: &str) -> Vec<JaasToken> {
    let mut characters = input.chars().peekable();
    let mut tokens = Vec::new();
    while let Some(character) = characters.next() {
        if character <= '\u{20}' { continue; }
        if character == '=' { tokens.push(JaasToken::Equals); continue; }
        if character == ';' { tokens.push(JaasToken::End); continue; }
        if character == '/' {
            if characters.next() == Some('*') {
                let mut previous = None;
                for next in characters.by_ref() {
                    if previous == Some('*') && next == '/' { break; }
                    previous = Some(next);
                }
            } else {
                while characters.next().is_some_and(|next| !matches!(next, '\r' | '\n')) {}
            }
            continue;
        }
        if character.is_ascii_digit() || matches!(character, '.' | '-') {
            if character != '-' || characters.peek().is_some_and(|next| next.is_ascii_digit() || *next == '.') {
                let mut decimal = character == '.';
                while let Some(next) = characters.peek() {
                    if *next == '.' && !decimal { decimal = true; }
                    else if !next.is_ascii_digit() { break; }
                    characters.next();
                }
            }
            tokens.push(JaasToken::Other);
            continue;
        }
        let mut value = String::new();
        if matches!(character, '\'' | '"') {
            while let Some(next) = characters.peek().copied() {
                if matches!(next, '\r' | '\n') { break; }
                characters.next();
                if next == character { break; }
                if next != '\\' { value.push(next); continue; }
                let escaped = characters.next().unwrap_or('\u{ffff}');
                if matches!(escaped, '0'..='7') {
                    let mut octal = escaped as u32 - '0' as u32;
                    for _ in 0..if escaped <= '3' { 2 } else { 1 } {
                        let Some(next @ '0'..='7') = characters.peek().copied() else { break; };
                        characters.next(); octal = octal * 8 + next as u32 - '0' as u32;
                    }
                    value.push(char::from_u32(octal).expect("octal byte"));
                } else {
                    value.push(match escaped {
                        'a' => '\u{7}', 'b' => '\u{8}', 'f' => '\u{c}', 'n' => '\n',
                        'r' => '\r', 't' => '\t', 'v' => '\u{b}', other => other,
                    });
                }
            }
        } else if jaas_word(character) {
            value.push(character);
            while characters.peek().is_some_and(|next| jaas_word(*next) || next.is_ascii_digit() || *next == '.') {
                value.push(characters.next().expect("peeked word character"));
            }
        } else { tokens.push(JaasToken::Other); continue; }
        tokens.push(JaasToken::Value(value));
    }
    tokens
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KafkaLogOptions {
    pub topic: String,
    pub read_only: bool,
    pub auto_create_topic: bool,
    pub pipelined: bool,
    pub queue_capacity: usize,
    pub queued_bytes: usize,
    pub maximum_message_bytes: usize,
    pub maximum_poll_records: usize,
    pub maximum_poll_bytes: usize,
    pub poll_interval: Duration,
    pub request_timeout: Duration,
    pub shutdown_timeout: Duration,
}

impl KafkaLogOptions {
    pub fn new(topic: impl Into<String>) -> Self {
        Self { topic: topic.into(), read_only: false, auto_create_topic: true, pipelined: false, queue_capacity: 1024,
            queued_bytes: 64 * 1024 * 1024, maximum_message_bytes: 1024 * 1024, maximum_poll_records: 1024, maximum_poll_bytes: 16 * 1024 * 1024,
            poll_interval: Duration::from_millis(5), request_timeout: Duration::from_secs(30), shutdown_timeout: Duration::from_secs(5) }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.topic.is_empty() || self.topic.len() > 249 || self.topic == "." || self.topic == ".." || !self.topic.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte)) {
            return Err(Anomaly::incorrect("xtdb.kafka/invalid-config", "Kafka topic name is invalid"));
        }
        if self.queue_capacity == 0 || self.queue_capacity > tokio::sync::Semaphore::MAX_PERMITS || self.queued_bytes == 0 || self.queued_bytes > u32::MAX as usize || self.queued_bytes > tokio::sync::Semaphore::MAX_PERMITS
            || self.maximum_message_bytes == 0 || self.maximum_message_bytes.checked_mul(2).is_none_or(|bytes| bytes > self.queued_bytes) || self.maximum_message_bytes > i32::MAX as usize
            || self.maximum_poll_records == 0 || self.maximum_poll_bytes < self.maximum_message_bytes || self.poll_interval.is_zero() || self.request_timeout.is_zero() || self.shutdown_timeout.is_zero() {
            return Err(Anomaly::incorrect("xtdb.kafka/invalid-config", "Kafka capacities and timeouts must be positive and hold a maximum-size message"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaLogConfig {
    pub cluster: String,
    pub topic: String,
    pub replica_cluster: Option<String>,
    pub replica_topic: Option<String>,
    pub epoch: i32,
}

impl KafkaLogConfig {
    pub fn new(cluster: impl Into<String>, topic: impl Into<String>) -> Self { Self { cluster: cluster.into(), topic: topic.into(), replica_cluster: None, replica_topic: None, epoch: 0 } }
    pub fn replica_cluster(&self) -> &str { self.replica_cluster.as_deref().unwrap_or(&self.cluster) }
    pub fn replica_topic(&self) -> String { self.replica_topic.clone().unwrap_or_else(|| format!("{}-replica", self.topic)) }
    pub fn to_proto(&self) -> pb::KafkaLogConfig {
        pb::KafkaLogConfig { log_cluster_alias: Some(self.cluster.clone()), topic: Some(self.topic.clone()), epoch: Some(self.epoch),
            replica_cluster_alias: Some(self.replica_cluster().to_owned()), replica_topic: Some(self.replica_topic()) }
    }
    pub fn from_proto(config: &pb::KafkaLogConfig) -> Self {
        Self { cluster: config.log_cluster_alias.clone().unwrap_or_default(), topic: config.topic.clone().unwrap_or_default(), epoch: config.epoch.unwrap_or_default(),
            replica_cluster: config.replica_cluster_alias.clone(), replica_topic: config.replica_topic.clone() }
    }
}

pub fn parse_properties(bytes: &[u8]) -> Result<BTreeMap<String, String>> {
    let text: String = bytes.iter().map(|byte| char::from(*byte)).collect();
    parse_properties_text(&text)
}

fn parse_properties_text(text: &str) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    let mut input = text.encode_utf16();
    visit_properties_units(|| Ok(input.next()), &mut |_| Ok(()), |key, value, _| {
        values.insert(unescape(key)?, unescape(value)?);
        Ok(())
    })?;
    Ok(values)
}

fn visit_properties_units<R: FnMut(usize) -> Result<()>>(
    mut input: impl FnMut() -> Result<Option<u16>>, reserve: &mut R,
    mut entry: impl FnMut(&[u16], &[u16], &mut R) -> Result<()>,
) -> Result<()> {
    let mut line = Vec::new();
    let mut leading_space = true;
    let mut continued = false;
    let mut trailing_backslash = false;
    let mut pending = None;
    loop {
        let current = match pending.take() { Some(unit) => Some(unit), None => input()? };
        let Some(unit) = current else { break; };
        let newline = matches!(unit, 0x0a | 0x0d);
        if leading_space {
            if property_space(unit) || (newline && !continued) { continue; }
            leading_space = false;
            continued = false;
        }
        if line.is_empty() && matches!(unit, 0x23 | 0x21) {
            while let Some(comment) = input()? {
                if matches!(comment, 0x0a | 0x0d) { break; }
            }
            leading_space = true;
            continue;
        }
        if !newline {
            if line.len() == line.capacity() {
                let capacity = line.capacity().checked_mul(2).ok_or_else(property_allocation)?.max(128);
                reserve(capacity.checked_mul(size_of::<u16>()).ok_or_else(property_allocation)?)?;
                line.try_reserve_exact(capacity - line.len()).map_err(|_| property_allocation())?;
            }
            line.push(unit);
            trailing_backslash = unit == 0x5c && !trailing_backslash;
            continue;
        }
        leading_space = true;
        if line.is_empty() { continue; }
        let Some(next) = input()? else { break; };
        if trailing_backslash {
            line.pop();
            trailing_backslash = false;
            continued = true;
            if unit != 0x0d || next != 0x0a { pending = Some(next); }
        } else {
            pending = Some(next);
            let (key, value) = property_parts(&line);
            entry(key, value, reserve)?;
            line.clear();
        }
    }
    if !line.is_empty() {
        if trailing_backslash { line.pop(); }
        let (key, value) = property_parts(&line);
        entry(key, value, reserve)?;
    }
    Ok(())
}

fn property_space(unit: u16) -> bool { matches!(unit, 0x09 | 0x0c | 0x20) }

fn property_parts(line: &[u16]) -> (&[u16], &[u16]) {
    let mut escaped = false;
    let mut separator = line.len();
    for (index, &unit) in line.iter().enumerate() {
        if !escaped && (matches!(unit, 0x3d | 0x3a) || property_space(unit)) { separator = index; break; }
        escaped = !escaped && unit == 0x5c;
    }
    let mut value = skip_property_space(&line[separator..]);
    if matches!(value.first().copied(), Some(0x3d | 0x3a)) { value = &value[1..]; }
    (&line[..separator], skip_property_space(value))
}

fn skip_property_space(mut units: &[u16]) -> &[u16] {
    while units.first().is_some_and(|&unit| property_space(unit)) { units = &units[1..]; }
    units
}

fn unescape(input: &[u16]) -> Result<String> {
    String::from_utf16(&unescape_units(input)?).map_err(|_| Anomaly::incorrect("xtdb.kafka/properties-file", "Unpaired surrogate in Java properties value"))
}

fn unescape_units(input: &[u16]) -> Result<Vec<u16>> {
    let mut result = Vec::<u16>::new();
    let mut units = input.iter().copied();
    while let Some(unit) = units.next() {
        let unit = if unit == u16::from(b'\\') {
            let Some(escaped) = units.next() else { break; };
            match char::from_u32(u32::from(escaped)) {
                Some('u') => {
                    let digits = [units.next(), units.next(), units.next(), units.next()];
                    if digits[3].is_none() { return Err(Anomaly::incorrect("xtdb.kafka/properties-file", "Incomplete Java properties Unicode escape")); }
                    digits.into_iter().flatten().try_fold(0u16, |value, digit| {
                        char::from_u32(u32::from(digit))?.to_digit(16).map(|digit| (value << 4) | digit as u16)
                    }).ok_or_else(|| Anomaly::incorrect("xtdb.kafka/properties-file", "Invalid Java properties Unicode escape"))?
                }
                Some('t') => u16::from(b'\t'),
                Some('r') => u16::from(b'\r'),
                Some('n') => u16::from(b'\n'),
                Some('f') => 0x0c,
                _ => escaped,
            }
        } else { unit };
        result.push(unit);
    }
    Ok(result)
}
