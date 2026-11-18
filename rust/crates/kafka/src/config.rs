use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;
use rtdb_format::generated::xtdb::kafka::proto as pb;
use rtdb_types::{Anomaly, Result, jvm_number::Number};
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
        let mut properties = BTreeMap::from([("bootstrap.servers".into(), self.bootstrap_servers.clone())]);
        properties.extend(self.properties_map.clone());
        if let Some(path) = &self.properties_file {
            properties.extend(parse_properties(&std::fs::read(path).map_err(|error| Anomaly::incorrect("xtdb.kafka/properties-file", error.to_string()))?)?);
        }
        Ok(properties)
    }
}

pub fn producer_properties(properties: &BTreeMap<String, String>, pipelined: bool) -> BTreeMap<String, String> {
    let mut result = BTreeMap::from([("enable.idempotence".into(), "true".into()), ("compression.type".into(), "snappy".into())]);
    if !pipelined { result.insert("linger.ms".into(), "0".into()); }
    result.extend(properties.clone());
    result.insert("acks".into(), "all".into());
    if result.get("enable.idempotence").is_some_and(|value| value == "false") { result.insert("max.in.flight.requests.per.connection".into(), "1".into()); }
    result
}

pub fn native_properties(properties: &BTreeMap<String, String>) -> Result<BTreeMap<String, String>> {
    let mut properties = properties.clone();
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
            _ => return Err(Anomaly::unsupported("xtdb.kafka/jaas-login-module", "Kafka login module requires a native credential adapter")),
        }
    }
    Ok(properties)
}

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
    visit_properties_text(text, |key, value| {
        values.insert(unescape(key)?, unescape(value)?);
        Ok(())
    })?;
    Ok(values)
}

fn visit_properties_text(text: &str, mut entry: impl FnMut(&str, &str) -> Result<()>) -> Result<()> {
    let mut logical = Vec::new();
    let mut current = String::new();
    let mut continuation = false;
    let text = text.replace("\r\n", "\n").replace('\r', "\n");
    for line in text.split('\n') {
        let line = line.trim_start_matches([' ', '\t', '\u{c}']);
        if !continuation && (line.is_empty() || line.starts_with(['#', '!'])) { continue; }
        current.push_str(line);
        continuation = current.chars().rev().take_while(|character| *character == '\\').count() % 2 == 1;
        if continuation { current.pop(); } else { logical.push(std::mem::take(&mut current)); }
    }
    if !current.is_empty() { logical.push(current); }
    for line in logical {
        let mut escaped = false;
        let mut separator = line.len();
        for (index, character) in line.char_indices() {
            if !escaped && matches!(character, '=' | ':' | ' ' | '\t' | '\u{c}') { separator = index; break; }
            escaped = !escaped && character == '\\';
        }
        let mut value = line[separator..].trim_start_matches([' ', '\t', '\u{c}']);
        if value.starts_with(['=', ':']) { value = &value[1..]; }
        value = value.trim_start_matches([' ', '\t', '\u{c}']);
        entry(&line[..separator], value)?;
    }
    Ok(())
}

fn unescape(input: &str) -> Result<String> {
    String::from_utf16(&unescape_units(input)?).map_err(|_| Anomaly::incorrect("xtdb.kafka/properties-file", "Unpaired surrogate in Java properties value"))
}

fn unescape_units(input: &str) -> Result<Vec<u16>> {
    let mut result = Vec::<u16>::new();
    let mut characters = input.chars();
    while let Some(character) = characters.next() {
        let character = if character == '\\' {
            match characters.next() {
                Some('u') => {
                    let digits = [characters.next(), characters.next(), characters.next(), characters.next()];
                    if digits[3].is_none() { return Err(Anomaly::incorrect("xtdb.kafka/properties-file", "Incomplete Java properties Unicode escape")); }
                    let unit = digits.into_iter().flatten().try_fold(0u16, |unit, digit| {
                        digit.to_digit(16).map(|digit| (unit << 4) | digit as u16)
                    }).ok_or_else(|| Anomaly::incorrect("xtdb.kafka/properties-file", "Invalid Java properties Unicode escape"))?;
                    result.push(unit);
                    continue;
                }
                Some('t') => '\t', Some('r') => '\r', Some('n') => '\n', Some('f') => '\u{c}', Some(character) => character, None => break,
            }
        } else { character };
        let mut units = [0; 2];
        result.extend(character.encode_utf16(&mut units).iter().copied());
    }
    Ok(result)
}
