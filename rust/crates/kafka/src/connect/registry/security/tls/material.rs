use super::*;

#[derive(Clone, Copy)]
enum Source<'a> { File(&'a str), Inline { key: &'a str, chain: Option<&'a str> } }
pub(super) struct Material<'a> {
    source: Source<'a>,
    format: StoreFormat,
    usage: StoreUse,
    password: Option<&'a str>,
    key_password: Option<&'a str>,
}

impl<'a> Material<'a> {
    pub(super) fn parse(options: &BTreeMap<&str, &'a str>, store: &str, usage: StoreUse) -> Result<Option<Self>> {
        let value = |suffix: &str| options.get(format!("{store}.{suffix}").as_str()).copied();
        let password = nonempty_password(value("password"));
        let key_password = nonempty_password(options.get("key.password").copied());
        let path = value("location");
        let kind = value("type").unwrap_or("JKS");
        let inline = nonempty_password(value(if usage == StoreUse::Trust { "certificates" } else { "key" }));
        let chain = if usage == StoreUse::Trust { None } else { nonempty_password(value("certificate.chain")) };
        let source = if let Some(key) = inline {
            if kind != "PEM" { return Err(error("invalid-plugin-config", "Inline Registry material requires PEM format")); }
            if usage != StoreUse::Trust && chain.is_none() { return Err(error("invalid-plugin-config", "Inline Registry key requires a certificate chain")); }
            if path.is_some_and(|path| usage != StoreUse::Trust || !path.is_empty()) { return Err(error("invalid-plugin-config", "Registry store file and inline material are mutually exclusive")); }
            if password.is_some() { return Err(error("invalid-plugin-config", "Registry PEM stores accept a key password, not a store password")); }
            Source::Inline { key, chain }
        } else {
            if chain.is_some() { return Err(error("invalid-plugin-config", "Registry certificate chain requires a private key")); }
            if kind == "PEM" && path.is_some_and(|path| usage != StoreUse::Trust || !path.is_empty()) {
                if password.is_some() { return Err(error("invalid-plugin-config", "Registry PEM stores accept a key password, not a store password")); }
                Source::File(path.expect("present PEM path"))
            } else if path.is_none() && password.is_some() {
                return Err(error("invalid-plugin-config", "Registry store password requires a store"));
            } else if let Some(path) = path.filter(|path| !path.is_empty()) {
                if usage != StoreUse::Trust && password.is_none() { return Err(error("invalid-plugin-config", "Registry private-key stores require a store password")); }
                Source::File(path)
            } else { return Ok(None); }
        };
        let format = if kind == "PEM" { StoreFormat::Pem }
            else if kind.eq_ignore_ascii_case("JKS") || kind.eq_ignore_ascii_case("PKCS12") { StoreFormat::Java }
            else { return Err(error("invalid-plugin-config", "Unknown Registry keystore format")); };
        Ok(Some(Self { source, format, usage, password, key_password }))
    }

    pub(super) fn length(&self) -> Result<usize> {
        match self.source {
            Source::File(path) => {
                let length = usize::try_from(std::fs::metadata(path).map_err(|cause| error("invalid-plugin-config", cause))?.len()).map_err(|_| error("invalid-plugin-config", "Registry keystore file is too large"))?;
                if length > MAXIMUM_STORE_FILE { return Err(error("invalid-plugin-config", "Registry keystore file exceeds its byte limit")); }
                Ok(length)
            }
            Source::Inline { key, chain } => key.len().checked_add(chain.map_or(0, str::len)).and_then(|length| length.checked_add(1)).ok_or_else(|| error("allocation-overflow", "Registry inline key size overflow")),
        }
    }

    pub(super) fn load(&self, length: usize, charge: Arc<MemoryReservation>) -> Result<KeyStore> {
        let bytes = self.bytes(length)?;
        KeyStore::decode(&bytes, self.format, self.usage, self.password, self.key_password, charge)
    }

    pub(super) fn allocation_bound(&self, length: usize, algorithm: crate::tls::keys::Algorithm) -> Result<usize> {
        crate::tls::identities::Material::allocation_bound(length, self.format, self.usage, algorithm, self.password, self.key_password)
    }

    pub(super) fn load_identity(&self, length: usize, algorithm: crate::tls::keys::Algorithm, charge: Arc<MemoryReservation>) -> Result<crate::tls::identities::Material> {
        let bytes = self.bytes(length)?;
        crate::tls::identities::Material::decode(&bytes, self.format, self.usage, algorithm, self.password, self.key_password, charge)
    }

    fn bytes(&self, length: usize) -> Result<Zeroizing<Vec<u8>>> {
        let mut bytes = Zeroizing::new(Vec::with_capacity(length.checked_add(1).ok_or_else(|| error("allocation-overflow", "Registry store size overflow"))?));
        match self.source {
            Source::Inline { key, chain } => {
                bytes.extend_from_slice(key.as_bytes());
                if let Some(chain) = chain { bytes.push(b'\n'); bytes.extend_from_slice(chain.as_bytes()); }
            }
            Source::File(path) => {
                std::fs::File::open(path).map_err(|cause| error("invalid-plugin-config", cause))?.take(length as u64 + 1).read_to_end(&mut bytes).map_err(|cause| error("invalid-plugin-config", cause))?;
                if bytes.len() != length { return Err(error("invalid-plugin-config", "Registry keystore file changed while opening")); }
            }
        }
        Ok(bytes)
    }
}

fn nonempty_password(value: Option<&str>) -> Option<&str> { value.filter(|value| !value.trim_matches(|character| character <= '\u{20}').is_empty()) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_empty_paths_and_passwords_follow_store_kind_and_role() {
        for usage in [StoreUse::Trust, StoreUse::OptionalIdentity] {
            for kind in ["JKS", "custom.unused.Type"] {
                let options = BTreeMap::from([("store.location", ""), ("store.type", kind), ("store.password", "password")]);
                assert!(Material::parse(&options, "store", usage).unwrap().is_none());
            }
            assert!(Material::parse(&BTreeMap::from([("store.password", "\u{1} \t")]), "store", usage).unwrap().is_none());
            assert!(Material::parse(&BTreeMap::from([("store.password", "\u{a0}")]), "store", usage).is_err());
        }
        let options = BTreeMap::from([("store.location", ""), ("store.type", "PEM")]);
        assert!(Material::parse(&options, "store", StoreUse::Trust).unwrap().is_none());
        assert!(matches!(Material::parse(&options, "store", StoreUse::OptionalIdentity).unwrap().unwrap().source, Source::File("")));
    }

    #[test]
    fn registry_java_store_names_are_case_insensitive_and_password_bytes_are_preserved() {
        for kind in ["jks", "pKcS12"] {
            let options = BTreeMap::from([("store.location", "input"), ("store.type", kind), ("store.password", " password ")]);
            let material = Material::parse(&options, "store", StoreUse::OptionalIdentity).unwrap().unwrap();
            assert_eq!(material.format, StoreFormat::Java);
            assert_eq!(material.password, Some(" password "));
        }
    }

    #[test]
    fn registry_inline_key_and_trust_differ_on_an_empty_location() {
        let trust = BTreeMap::from([("store.type", "PEM"), ("store.location", ""), ("store.certificates", "certificates")]);
        assert!(Material::parse(&trust, "store", StoreUse::Trust).unwrap().is_some());
        let key = BTreeMap::from([("store.type", "PEM"), ("store.location", ""), ("store.key", "key"), ("store.certificate.chain", "chain")]);
        assert!(Material::parse(&key, "store", StoreUse::OptionalIdentity).is_err());
    }
}
