use std::collections::BTreeMap;
use openssl::ssl::{SslContextBuilder, SslMethod, SslOptions, cipher_name};
use rtdb_types::Result;
use super::invalid;

include!("cipher_names.rs");

pub(super) struct Policy {
    tls12: bool,
    tls13: bool,
    ciphers: Ciphers,
}
struct Ciphers { legacy: String, tls13: String }

impl Policy {
    pub(super) fn from_java(options: &BTreeMap<String, String>) -> Result<Self> {
        let defaults = match options.get("ssl.protocol").map(String::as_str).unwrap_or("TLSv1.3").trim().to_ascii_uppercase().as_str() {
            "TLS" | "SSL" | "TLSV1.3" => (true, true),
            "TLSV1.2" => (true, false),
            "SSLV3" | "TLSV1" | "TLSV1.1" => (false, false),
            _ => return Err(invalid("Unknown Kafka TLS context protocol")),
        };
        let enabled = options.get("ssl.enabled.protocols").map(String::as_str).unwrap_or("TLSv1.2,TLSv1.3");
        let (mut tls12, mut tls13) = if enabled.trim().is_empty() { defaults } else { (false, false) };
        for protocol in enabled.split(',').map(str::trim).filter(|_| !enabled.trim().is_empty()) {
            match protocol {
                "TLSv1.2" => tls12 = true,
                "TLSv1.3" => tls13 = true,
                "SSLv2Hello" | "SSLv3" | "TLSv1" | "TLSv1.1" => (),
                _ => return Err(invalid("Unknown Kafka TLS enabled protocol")),
            }
        }
        let ciphers = match options.get("ssl.cipher.suites").filter(|value| !value.trim().is_empty()) {
            Some(value) => Ciphers::parse(value.split(',').map(str::trim))?,
            None => Ciphers::parse(JAVA_CIPHERS.iter().filter(|(_, name, enabled)| *enabled && if is_tls13(name) { defaults.1 } else { defaults.0 }).map(|(name, _, _)| *name))?,
        };
        tls12 &= !ciphers.legacy.is_empty();
        tls13 &= !ciphers.tls13.is_empty();
        Ok(Self { tls12, tls13, ciphers })
    }

    pub(super) fn apply(&self, context: &mut SslContextBuilder) -> Result<()> {
        if !self.tls12 { context.set_options(SslOptions::NO_TLSV1_2); }
        if !self.tls13 { context.set_options(SslOptions::NO_TLSV1_3); }
        if self.tls12 { context.set_cipher_list(&self.ciphers.legacy).map_err(invalid)?; }
        context.set_ciphersuites(&self.ciphers.tls13).map_err(invalid)?;
        Ok(())
    }
}

impl Ciphers {
    fn parse<'a>(names: impl Iterator<Item = &'a str>) -> Result<Self> {
        let mut legacy = Vec::new();
        let mut tls13 = Vec::new();
        let mut validation = SslContextBuilder::new(SslMethod::tls_client()).map_err(invalid)?;
        for name in names {
            let (_, iana, _) = JAVA_CIPHERS.iter().find(|(java, iana, _)| name == *java || name == *iana)
                .ok_or_else(|| invalid(format!("Unsupported Java Kafka cipher suite '{name}'")))?;
            if *iana == "TLS_EMPTY_RENEGOTIATION_INFO_SCSV" || disabled(iana) { continue; }
            let native = cipher_name(iana);
            if native == "(NONE)" { return Err(invalid(format!("Kafka cipher suite '{name}' is unavailable in native TLS"))); }
            if is_tls13(iana) {
                validation.set_ciphersuites(native).map_err(invalid)?;
                tls13.push(native);
            } else {
                validation.set_cipher_list(native).map_err(invalid)?;
                legacy.push(native);
            }
        }
        Ok(Self { legacy: legacy.join(":"), tls13: tls13.join(":") })
    }
}
fn is_tls13(name: &str) -> bool { name.starts_with("TLS_AES_") || name == "TLS_CHACHA20_POLY1305_SHA256" }
fn disabled(name: &str) -> bool {
    name.starts_with("TLS_RSA_") || name.starts_with("TLS_ECDH_") || ["_anon_", "_RC4_", "_DES_", "_DES40_", "_3DES_", "_NULL_"].iter().any(|part| name.contains(part))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn options() -> BTreeMap<String, String> { BTreeMap::new() }
    #[test]
    fn protocol_context_defaults_and_disabled_java_suites_do_not_broaden_native_policy() {
        let policy = |options: BTreeMap<String, String>| Policy::from_java(&options).unwrap();
        let default = policy(options()); assert!(default.tls12 && default.tls13);
        let mut options = options(); options.insert("ssl.protocol".into(), "TLSv1.2".into());
        let defaults = policy(options.clone()); assert!(defaults.tls12 && !defaults.tls13);
        options.insert("ssl.enabled.protocols".into(), "TLSv1.3".into()); options.insert("ssl.cipher.suites".into(), "TLS_AES_128_GCM_SHA256".into());
        let explicit = policy(options.clone()); assert!(!explicit.tls12 && explicit.tls13);
        options.insert("ssl.cipher.suites".into(), "TLS_RSA_WITH_AES_128_GCM_SHA256".into());
        let disabled = policy(options); assert!(!disabled.tls12 && !disabled.tls13);
    }
}
