use std::{collections::BTreeMap, io::Read, sync::Arc, time::Duration};
use reqwest::Client;
mod transport;
use rtdb_tls::{KeyStore, StoreFormat, StoreUse};
use zeroize::Zeroizing;
use rtdb_columnar::MemoryReservation;
use rtdb_types::Result;
use super::{ConversionContext, error};
const MAXIMUM_STORE_FILE: usize = 4 * 1024 * 1024;
pub(super) fn client(options: &BTreeMap<String, String>, context: &mut ConversionContext, registry_hostname_policy: bool) -> Result<(Client, Arc<MemoryReservation>)> {
    let normalized = super::normalized(options);
    let tls = options.iter().filter_map(|(name, value)| name.strip_prefix("schema.registry.ssl.").map(|name| (name, value.as_str()))).collect::<BTreeMap<_, _>>();
    for name in ["provider", "trustmanager.algorithm", "secure.random.implementation", "cipher.suites"] {
        if tls.get(name).is_some_and(|value| !value.is_empty()) { return Err(error("invalid-plugin-config", format!("Registry TLS option '{name}' requires a native provider adapter"))); }
    }
    let mut files = BTreeMap::new();
    let default_roots = !tls.contains_key("truststore.location") && !tls.contains_key("truststore.certificates");
    let root_bound = if default_roots { crate::tls::trust::allocation_bound()? } else { 0 };
    let root_bound = root_bound.checked_add(65_536).ok_or_else(|| error("allocation-overflow", "Registry TLS configuration overflow"))?;
    let mut bound = options.iter().try_fold(root_bound, |bytes, (name, value)| bytes.checked_add(name.len()).and_then(|bytes| bytes.checked_add(value.len().checked_mul(16)?)).ok_or_else(|| error("allocation-overflow", "Registry TLS configuration overflow")))?;
    for name in ["truststore.location", "keystore.location"] {
        if let Some(path) = tls.get(name) {
            let length = usize::try_from(std::fs::metadata(path).map_err(|cause| error("invalid-plugin-config", cause))?.len()).map_err(|_| error("invalid-plugin-config", "Registry keystore file is too large"))?;
            if length > MAXIMUM_STORE_FILE { return Err(error("invalid-plugin-config", "Registry keystore file exceeds its byte limit")); }
            bound = bound.checked_add(length.checked_add(1).and_then(|bytes| bytes.checked_mul(16)).ok_or_else(|| error("allocation-overflow", "Registry TLS allocation overflow"))?).ok_or_else(|| error("allocation-overflow", "Registry TLS allocation overflow"))?;
            files.insert(name, length);
        }
    }
    let charge = context.reserve(bound)?;
    let mut builder = Client::builder().no_proxy().redirect(reqwest::redirect::Policy::none());
    for (name, connect) in [("http.connect.timeout.ms", true), ("http.read.timeout.ms", false)] {
        let timeout = normalized.get(name).map(|value| value.parse::<u64>().map_err(|_| error("invalid-plugin-config", "Registry timeout must be nonnegative milliseconds"))).transpose()?.unwrap_or(60_000);
        if timeout > 0 { builder = if connect { builder.connect_timeout(Duration::from_millis(timeout)) } else { builder.read_timeout(Duration::from_millis(timeout)) }; }
    }
    if let (Some(host), Some(port)) = (normalized.get("proxy.host"), normalized.get("proxy.port")) {
        let port = port.parse::<u16>().map_err(|_| error("invalid-plugin-config", "Registry proxy port is invalid"))?;
        if !host.is_empty() && port > 0 { builder = builder.proxy(reqwest::Proxy::all(format!("http://{host}:{port}")).map_err(|_| error("invalid-plugin-config", "Registry proxy host is invalid"))?); }
    }
    let trust = material(&tls, &files, "truststore", StoreUse::Trust, Arc::clone(&charge))?;
    let identity = material(&tls, &files, "keystore", StoreUse::OptionalIdentity, Arc::clone(&charge))?;
    builder = builder.tls_backend_preconfigured(transport::configuration(&tls, trust.as_ref(), identity.as_ref(), registry_hostname_policy, Arc::clone(&charge))?);
    Ok((builder.build().map_err(|cause| error("invalid-plugin-config", cause.without_url()))?, charge))
}
fn material(tls: &BTreeMap<&str, &str>, files: &BTreeMap<&str, usize>, store: &str, usage: StoreUse, charge: Arc<MemoryReservation>) -> Result<Option<KeyStore>> {
    let location_key = format!("{store}.location");
    let inline_key = format!("{store}.{}", if usage == StoreUse::Trust { "certificates" } else { "key" });
    let location = tls.get(location_key.as_str()); let inline = tls.get(inline_key.as_str());
    let password = tls.get(format!("{store}.password").as_str()).copied();
    let key_password = tls.get("key.password").copied();
    let chain = if usage != StoreUse::Trust { tls.get("keystore.certificate.chain") } else { None };
    if location.is_none() && inline.is_none() {
        if password.is_some() || chain.is_some() { return Err(error("invalid-plugin-config", "Registry store password or chain requires a store")); }
        return Ok(None);
    }
    let format = match tls.get(format!("{store}.type").as_str()).copied().unwrap_or("JKS") {
        "PEM" => StoreFormat::Pem, "JKS" | "PKCS12" => StoreFormat::Java,
        _ => return Err(error("invalid-plugin-config", "Unknown registry keystore format")),
    };
    if inline.is_some() && format != StoreFormat::Pem { return Err(error("invalid-plugin-config", "Inline registry material requires PEM format")); }
    if format == StoreFormat::Pem && password.is_some() { return Err(error("invalid-plugin-config", "Registry PEM stores accept a private-key password, not a store password")); }
    if location.is_some() && (inline.is_some() || chain.is_some()) { return Err(error("invalid-plugin-config", "Registry keystore location and inline content are mutually exclusive")); }
    if format == StoreFormat::Java && usage != StoreUse::Trust && password.is_none() { return Err(error("invalid-plugin-config", "Registry private-key stores require a store password")); }
    let mut bytes = if let Some(inline) = inline {
        let capacity = inline.len().checked_add(chain.map_or(0, |chain| chain.len())).and_then(|length| length.checked_add(1)).ok_or_else(|| error("allocation-overflow", "Registry inline key size overflow"))?;
        let mut bytes = Zeroizing::new(Vec::with_capacity(capacity)); bytes.extend_from_slice(inline.as_bytes()); bytes
    } else {
        let path = location.expect("one keystore source is present"); let length = files[location_key.as_str()];
        let mut bytes = Zeroizing::new(Vec::with_capacity(length + 1)); std::fs::File::open(path).map_err(|cause| error("invalid-plugin-config", cause))?.take(length as u64 + 1).read_to_end(&mut bytes).map_err(|cause| error("invalid-plugin-config", cause))?;
        if bytes.len() != length { return Err(error("invalid-plugin-config", "Registry keystore file changed while opening")); } bytes
    };
    if inline.is_some() && usage != StoreUse::Trust {
        let chain = chain.ok_or_else(|| error("invalid-plugin-config", "Inline registry private key requires a certificate chain"))?;
        bytes.push(b'\n'); bytes.extend_from_slice(chain.as_bytes());
    }
    KeyStore::decode(&bytes, format, usage, password, key_password, charge).map(Some)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registry_default_roots_remain_charged_until_the_client_closes() {
        let memory = rtdb_columnar::MemoryBudget::new(1 << 26);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let (client, charge) = client(&BTreeMap::new(), &mut context, true).unwrap();
        drop(context); drop(charge);
        assert!(memory.used_bytes() >= crate::tls::trust::allocation_bound().unwrap());
        drop(client);
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn explicit_registry_trust_does_not_reserve_the_default_bundle() {
        let path = format!("{}/../tls/tests/fixtures/empty.jks", env!("CARGO_MANIFEST_DIR"));
        let options = BTreeMap::from([
            ("schema.registry.ssl.truststore.location".into(), path),
            ("schema.registry.ssl.truststore.password".into(), "12345678".into()),
        ]);
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let (client, charge) = client(&options, &mut context, true).unwrap();
        assert!(charge.bytes() < crate::tls::trust::allocation_bound().unwrap());
        drop(context); drop(charge); drop(client);
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn empty_java_registry_stores_are_retained_until_the_client_closes() {
        let path = format!("{}/../tls/tests/fixtures/empty.jks", env!("CARGO_MANIFEST_DIR"));
        let mut options = BTreeMap::new();
        for store in ["keystore", "truststore"] {
            options.insert(format!("schema.registry.ssl.{store}.location"), path.clone());
            options.insert(format!("schema.registry.ssl.{store}.password"), "12345678".into());
        }
        let memory = rtdb_columnar::MemoryBudget::new(1 << 26);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let (client, charge) = client(&options, &mut context, true).unwrap();
        drop(context); drop(charge);
        assert!(memory.used_bytes() > 0);
        drop(client);
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn registry_tls_rejects_ambiguous_or_unimplemented_keystores() {
        let memory = rtdb_columnar::MemoryBudget::new(1 << 24); let mut context = ConversionContext::new(memory);
        assert!(client(&BTreeMap::from([("schema.registry.ssl.keystore.key".into(), "secret".into())]), &mut context, true).is_err());
        assert!(client(&BTreeMap::from([("schema.registry.ssl.truststore.type".into(), "PEM".into()), ("schema.registry.ssl.truststore.certificates".into(), "invalid".into())]), &mut context, true).is_err());
        assert!(client(&BTreeMap::from([("schema.registry.ssl.provider".into(), "java-only".into())]), &mut context, true).is_err());
    }
}
