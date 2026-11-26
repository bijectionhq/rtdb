use std::{collections::BTreeMap, sync::Arc};
use rdkafka::ClientConfig;
use rtdb_columnar::MemoryBudget;
use rtdb_types::{Anomaly, Result};
use crate::tls::NativeTls;
pub(crate) mod material;

pub(crate) fn prepare(options: &BTreeMap<String, String>, memory: &Arc<MemoryBudget>) -> Result<ClientConfig> {
    let (mut properties, ignored) = crate::config::native_client_properties(options)?;
    for name in ignored {
        tracing::warn!("The configuration '{name}' was supplied but isn't a known config.");
    }
    for name in ["security.protocol", "ssl.protocol", "ssl.provider", "ssl.keystore.type", "ssl.keystore.location", "ssl.truststore.type", "ssl.truststore.location",
        "ssl.keymanager.algorithm", "ssl.trustmanager.algorithm", "ssl.endpoint.identification.algorithm", "ssl.secure.random.implementation", "ssl.engine.factory.class"] {
        if let Some(value) = properties.get_mut(name) { *value = value.trim_matches(|character| character <= '\u{20}').to_owned(); }
    }
    if properties.get("ssl.engine.factory.class").is_some_and(|factory| factory != "org.apache.kafka.common.security.ssl.DefaultSslEngineFactory") {
        return Err(invalid("ssl.engine.factory.class requires a native adapter"));
    }
    let java_identity = properties.contains_key("ssl.keystore.location") || properties.contains_key("ssl.keystore.key");
    let java_trust = properties.contains_key("ssl.truststore.location") || properties.contains_key("ssl.truststore.certificates");
    if properties.get("security.protocol").is_some_and(|protocol| protocol.eq_ignore_ascii_case("SSL") || protocol.eq_ignore_ascii_case("SASL_SSL")) {
        if java_identity && ["ssl.key.location", "ssl.key.pem", "ssl.certificate.location", "ssl.certificate.pem"].iter().any(|name| properties.contains_key(*name)) {
            return Err(invalid("Java and native Kafka identities are mutually exclusive"));
        }
        if java_trust && ["ssl.ca.location", "ssl.ca.pem"].iter().any(|name| properties.contains_key(*name)) {
            return Err(invalid("Java and native Kafka trust stores are mutually exclusive"));
        }
    }
    let tls = NativeTls::from_java(&properties, &mut |bytes| memory.reserve(bytes))?;
    for name in ["ssl.protocol", "ssl.provider", "ssl.cipher.suites", "ssl.enabled.protocols", "ssl.keystore.type", "ssl.keystore.location", "ssl.keystore.password",
        "ssl.keystore.key", "ssl.keystore.certificate.chain", "ssl.truststore.type", "ssl.truststore.location", "ssl.truststore.password", "ssl.truststore.certificates",
        "ssl.keymanager.algorithm", "ssl.trustmanager.algorithm", "ssl.secure.random.implementation", "ssl.engine.factory.class"] {
        properties.remove(name);
    }
    if java_identity { properties.remove("ssl.key.password"); }
    if let Some(value) = properties.get_mut("ssl.endpoint.identification.algorithm") { *value = if value.is_empty() { "none".into() } else { value.to_ascii_lowercase() }; }
    let mut configuration = crate::client_config(&properties);
    if let Some(tls) = tls { configuration.set_ssl_context_configurer(material::Configuration::capture(tls, &properties, &mut |bytes| memory.reserve(bytes))?); }
    Ok(configuration)
}

fn invalid(message: &str) -> Anomaly { Anomaly::incorrect("xtdb.kafka/invalid-tls-config", message) }

#[cfg(test)]
mod tests;
