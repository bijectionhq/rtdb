use std::{collections::BTreeMap, sync::Arc};
use rtdb_types::Result;
use super::ConversionContext;
use crate::tls::NativeTls;

pub(super) fn configure(options: &BTreeMap<String, String>, properties: &mut BTreeMap<String, String>, context: &mut ConversionContext) -> Result<Option<Arc<NativeTls>>> {
    let material = NativeTls::from_java(options, &mut |bytes| context.reserve(bytes))?;
    if material.is_some() {
        properties.remove("ssl.cipher.suites");
        if options.contains_key("ssl.keystore.location") || options.contains_key("ssl.keystore.key") {
            for name in ["ssl.key.location", "ssl.key.pem", "ssl.certificate.location", "ssl.certificate.pem"] {
                if properties.contains_key(name) { return Err(super::error("dlq", "Java and native producer identities are mutually exclusive")); }
            }
            properties.remove("ssl.key.password");
        }
        if options.contains_key("ssl.truststore.location") || options.contains_key("ssl.truststore.certificates") {
            for name in ["ssl.ca.location", "ssl.ca.pem"] {
                if properties.contains_key(name) { return Err(super::error("dlq", "Java and native producer trust stores are mutually exclusive")); }
            }
        }
    }
    Ok(material)
}
