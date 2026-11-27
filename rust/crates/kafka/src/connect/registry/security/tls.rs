use std::{collections::BTreeMap, io::Read, sync::Arc};
use super::super::http::Client;
#[cfg(test)]
mod transport;
mod material;
use material::Material;
use rtdb_tls::{KeyStore, StoreFormat, StoreUse};
use zeroize::Zeroizing;
use rtdb_columnar::MemoryReservation;
use rtdb_types::Result;
use super::{ConversionContext, error};
const MAXIMUM_STORE_FILE: usize = 4 * 1024 * 1024;

pub(super) fn client(options: &BTreeMap<String, String>, context: &mut ConversionContext, registry_hostname_policy: bool) -> Result<(Client, Arc<MemoryReservation>)> {
    let preflight = options.iter().try_fold(65_536usize, |bytes, (name, value)| bytes.checked_add(name.len()).and_then(|bytes| bytes.checked_add(value.len().checked_mul(16)?)).ok_or_else(|| error("allocation-overflow", "Registry TLS configuration overflow")))?;
    let _preflight = context.reserve(preflight)?;
    let normalized = super::normalized(options);
    let tls = options.iter().filter_map(|(name, value)| name.strip_prefix("schema.registry.ssl.").map(|name| (name, value.as_str()))).collect::<BTreeMap<_, _>>();
    let algorithm = crate::tls::keys::Algorithm::parse(tls.get("keymanager.algorithm").copied().filter(|name| !name.is_empty())).unwrap_or(crate::tls::keys::Algorithm::SunX509);
    let trust = Material::parse(&tls, "truststore", StoreUse::Trust)?;
    let identity = Material::parse(&tls, "keystore", StoreUse::OptionalIdentity)?;
    let root_bound = if trust.is_none() { crate::tls::trust::allocation_bound()? } else { 0 };
    let root_bound = root_bound.checked_add(65_536).ok_or_else(|| error("allocation-overflow", "Registry TLS configuration overflow"))?;
    let mut bound = options.iter().try_fold(root_bound, |bytes, (name, value)| bytes.checked_add(name.len()).and_then(|bytes| bytes.checked_add(value.len().checked_mul(16)?)).ok_or_else(|| error("allocation-overflow", "Registry TLS configuration overflow")))?;
    let trust_length = trust.as_ref().map(Material::length).transpose()?;
    let identity_length = identity.as_ref().map(Material::length).transpose()?;
    for (material, length) in trust.as_ref().zip(trust_length).into_iter().chain(identity.as_ref().zip(identity_length)) {
        bound = bound.checked_add(material.allocation_bound(length.checked_add(1).ok_or_else(|| error("allocation-overflow", "Registry TLS allocation overflow"))?, algorithm)?).ok_or_else(|| error("allocation-overflow", "Registry TLS allocation overflow"))?;
    }
    let charge = context.reserve(bound)?;
    let trust = trust.as_ref().zip(trust_length).map(|(material, length)| material.load(length, Arc::clone(&charge))).transpose()?;
    let identity = identity.as_ref().zip(identity_length).map(|(material, length)| material.load_identity(length, algorithm, Arc::clone(&charge))).transpose()?;
    let owner = crate::tls::NativeTls::from_registry(&tls, trust.as_ref(), identity.as_ref(), Arc::clone(&charge)).map_err(tls_error)?;
    let tls_context = unsafe { owner.registry_context().map_err(tls_error)? };
    let client = Client::new(tls_context, owner, verifies_names(&tls, registry_hostname_policy), &normalized, Arc::clone(&charge))?;
    Ok((client, charge))
}

fn verifies_names(tls: &BTreeMap<&str, &str>, registry_hostname_policy: bool) -> bool {
    !registry_hostname_policy || !matches!(tls.get("endpoint.identification.algorithm").copied(), None | Some("" | "none"))
}

fn tls_error(cause: crate::tls::TlsConfigError) -> rtdb_types::Anomaly {
    match cause {
        crate::tls::TlsConfigError::Invalid(message) => rtdb_types::Anomaly::new_nullable(
            rtdb_types::AnomalyCategory::Incorrect,
            "xtdb.kafka-connect-source/invalid-plugin-config",
            message,
        ),
        crate::tls::TlsConfigError::Other(cause) => cause,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registry_tls_reclassification_preserves_optional_messages_and_other_errors() {
        for message in [None, Some(String::new()), Some("invalid TLS configuration".into())] {
            let error = tls_error(crate::tls::TlsConfigError::Invalid(message.clone()));
            assert_eq!(error.category, rtdb_types::AnomalyCategory::Incorrect);
            assert_eq!(error.code, "xtdb.kafka-connect-source/invalid-plugin-config");
            assert_eq!(error.message, message);
            assert!(error.context.is_empty());
            let cause = rtdb_types::Anomaly::new_nullable(rtdb_types::AnomalyCategory::Busy, "rtdb/memory-budget-exceeded", message).with_context("source", "budget".into());
            assert_eq!(tls_error(crate::tls::TlsConfigError::Other(cause.clone())), cause);
        }
    }
    #[test]
    fn registry_trust_manager_names_preserve_jca_aliases_and_raw_strings() {
        let memory = rtdb_columnar::MemoryBudget::new(1 << 26);
        for algorithm in ["PKIX", "SunPKIX", "X509", "X.509", "SunX509", "sunx509", ""] {
            let mut context = ConversionContext::new(Arc::clone(&memory));
            let options = BTreeMap::from([("schema.registry.ssl.trustmanager.algorithm".into(), algorithm.into())]);
            let (client, charge) = client(&options, &mut context, true).unwrap();
            drop(client); drop(charge); drop(context); assert_eq!(memory.used_bytes(), 0);
        }
        for algorithm in [" SunX509 ", "NewSunX509", "custom.TrustManager"] {
            let mut context = ConversionContext::new(Arc::clone(&memory));
            let options = BTreeMap::from([("schema.registry.ssl.trustmanager.algorithm".into(), algorithm.into())]);
            assert_eq!(client(&options, &mut context, true).err().unwrap().code, "xtdb.kafka-connect-source/invalid-plugin-config");
            drop(context); assert_eq!(memory.used_bytes(), 0);
        }
    }
    #[test]
    fn registry_sunjsse_provider_and_empty_default_preserve_raw_provider_names() {
        let memory = rtdb_columnar::MemoryBudget::new(1 << 26);
        for provider in ["SunJSSE", ""] {
            let mut context = ConversionContext::new(Arc::clone(&memory));
            let options = BTreeMap::from([("schema.registry.ssl.provider".into(), provider.into())]);
            let (client, charge) = client(&options, &mut context, true).unwrap();
            drop(client); drop(charge); drop(context); assert_eq!(memory.used_bytes(), 0);
        }
        for provider in ["sunjsse", " SunJSSE ", "\u{a0}SunJSSE\u{a0}", "SUN", "custom.Provider"] {
            let mut context = ConversionContext::new(Arc::clone(&memory));
            let options = BTreeMap::from([("schema.registry.ssl.provider".into(), provider.into())]);
            assert_eq!(client(&options, &mut context, true).err().unwrap().code, "xtdb.kafka-connect-source/invalid-plugin-config");
            drop(context); assert_eq!(memory.used_bytes(), 0);
        }
    }
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
    fn registry_http_option_failure_releases_native_tls_owners() {
        let memory = rtdb_columnar::MemoryBudget::new(1 << 26);
        for (name, value) in [("http.connect.timeout.ms", "-1"), ("http.read.timeout.ms", "2147483648"), ("proxy.port", "invalid")] {
            let mut context = ConversionContext::new(Arc::clone(&memory));
            let options = BTreeMap::from([(format!("schema.registry.{name}"), value.into())]);
            assert_eq!(client(&options, &mut context, true).err().unwrap().code, "xtdb.kafka-connect-source/invalid-plugin-config");
            drop(context);
            assert_eq!(memory.used_bytes(), 0);
        }
    }
    #[test]
    fn registry_tls_preflight_preserves_memory_admission_failures() {
        let memory = rtdb_columnar::MemoryBudget::new(0);
        let mut context = ConversionContext::with_limit(Arc::clone(&memory), 1 << 20);
        let options = BTreeMap::from([("schema.registry.ssl.truststore.location".into(), "absent-store".into())]);
        assert_eq!(client(&options, &mut context, true).err().unwrap().code, "rtdb/memory-budget-exceeded");
        drop(context);
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

    #[test]
    fn registry_new_sunx509_defers_java_key_password_errors_after_loading_metadata() {
        let path = format!("{}/../tls/tests/fixtures/distinct-passwords.jks", env!("CARGO_MANIFEST_DIR"));
        let memory = rtdb_columnar::MemoryBudget::new(32 << 20);
        for (algorithm, succeeds) in [("SunX509", false), ("NewSunX509", true), ("PKIX", true)] {
            let options = BTreeMap::from([
                ("schema.registry.ssl.keystore.location".into(), path.clone()),
                ("schema.registry.ssl.keystore.password".into(), "store_password".into()),
                ("schema.registry.ssl.key.password".into(), "incorrect-key-password".into()),
                ("schema.registry.ssl.keymanager.algorithm".into(), algorithm.into()),
            ]);
            let mut context = ConversionContext::new(Arc::clone(&memory));
            let loaded = client(&options, &mut context, true);
            assert_eq!(loaded.is_ok(), succeeds);
            drop(loaded); drop(context);
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}

#[cfg(test)]
mod hostname_policy_tests {
    use super::*;
    #[test]
    fn registry_hostname_policy_preserves_exact_disabled_strings_and_oauth_defaults() {
        for (algorithm, registry) in [(None, false), (Some(""), false), (Some("none"), false), (Some("NONE"), true), (Some("HTTPS"), true), (Some(" none "), true)] {
            let options = algorithm.map(|algorithm| BTreeMap::from([("endpoint.identification.algorithm", algorithm)])).unwrap_or_default();
            assert_eq!(verifies_names(&options, true), registry);
            assert!(verifies_names(&options, false));
        }
    }
}
