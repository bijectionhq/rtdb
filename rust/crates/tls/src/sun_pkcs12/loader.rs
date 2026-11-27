use std::sync::Arc;
use openssl::x509::X509;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::Result;
use zeroize::Zeroizing;
use crate::{CertificateIdentifiers, DistinguishedName, TlsError, TlsResult, invalid, java_der::{Der, valid_oid}, private_factory::{FactoryInput, PrivateKey}, secret_der::{self, SecretBytes, Value, next}, x500::{equals_ignore_case_utf16, lowercase_utf16}};
use super::{SunX509Pkcs12Identity, attributes::{self, Attributes}, der::{expect, fields, integer, malformed, octets, oid, values}, overflow, protection::{self, Protection}, scratch};

const DATA: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x07\x01";
const ENCRYPTED_DATA: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x07\x06";
const BAG_PREFIX: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x0c\x0a\x01";

struct Certificate { der: Vec<u8>, native: X509, attributes: Attributes }
struct PrivateEntry { protected: Zeroizing<Vec<u8>>, alias: Vec<u16>, key_id: Vec<u8> }
enum Entry { Private(usize), Secret(Zeroizing<Vec<u8>>), Trusted }
struct NamedEntry { alias: Vec<u16>, entry: Entry }
struct Chain { certificates: Vec<Arc<Certificate>>, _reservation: Arc<MemoryReservation> }
#[derive(Default)]
pub(super) struct LoadedPkcs12 { entries: Vec<NamedEntry>, keys: Vec<PrivateEntry>, certificates: Vec<Arc<Certificate>>, private_keys: usize, unnamed: usize }
struct ContentInfo { oid: Zeroizing<Vec<u8>>, content: Option<Value<'static>> }

pub(super) fn load(bytes: &[u8], password: Option<&str>, memory: &Arc<MemoryBudget>) -> Result<LoadedPkcs12> {
    let outer = next(&mut Der(bytes))?;
    let mut pfx = fields(&outer)?;
    if integer(&mut pfx)? != 3 { return Err(invalid("Unsupported PKCS12 version")); }
    let auth_safe = content_info(next(&mut pfx)?)?;
    if auth_safe.oid.as_slice() != DATA { return Err(invalid("Public-key protected PKCS12 is unsupported")); }
    let authenticated = octets(auth_safe.content.ok_or_else(malformed)?)?;
    let contents = expect(&mut Der(&authenticated), 0x30)?;
    let mut loaded = LoadedPkcs12::default();
    for content in values(&contents)? {
        let content = content_info(content)?;
        match content.oid.as_slice() {
            DATA => loaded.read(&octets(content.content.ok_or_else(malformed)?)?, memory)?,
            ENCRYPTED_DATA => if let Some(password) = password {
                let content = content.content.ok_or_else(malformed)?;
                let _scratch = scratch(content.content.len(), Some(password), memory)?;
                let mut encrypted = fields(&content)?;
                integer(&mut encrypted)?;
                let info = expect(&mut encrypted, 0x30)?;
                let [kind, algorithm, mut ciphertext]: [Value<'_>; 3] = values(&info)?.try_into().map_err(|_| malformed())?;
                if kind.tag != 6 { return Err(malformed()); } valid_oid(&kind.content)?;
                if ciphertext.tag & 0xdf != 0x80 { return Err(malformed()); }
                ciphertext.tag = 4 | (ciphertext.tag & 0x20);
                let ciphertext = octets(ciphertext)?;
                let protection = Protection::parse(algorithm)?;
                protection::retry(Some(password), |password| loaded.read(&protection.decrypt(&ciphertext, password).map_err(TlsError::from_invalid_anomaly)?, memory))?;
            },
            _ => return Err(invalid("Public-key protected PKCS12 content is unsupported")),
        }
    }
    if !pfx.0.is_empty() { if let Some(password) = password { protection::verify_mac(next(&mut pfx)?, &authenticated, password)?; } }
    Ok(loaded)
}

fn content_info(value: Value<'_>) -> Result<ContentInfo> {
    if value.tag != 0x30 { return Err(malformed()); }
    let mut fields = Der(&value.content);
    let oid = oid(&mut fields)?;
    let content = if fields.0.is_empty() { None } else {
        let wrapper = next(&mut fields)?;
        let [value]: [Value<'_>; 1] = values(&wrapper.content)?.try_into().map_err(|_| malformed())?;
        Some(Value { tag: value.tag, content: SecretBytes::Owned(value.content.into_owned()) })
    };
    fields.end()?;
    Ok(ContentInfo { oid, content })
}

impl LoadedPkcs12 {
    fn read(&mut self, bytes: &[u8], memory: &Arc<MemoryBudget>) -> TlsResult<()> {
        let inv = TlsError::from_invalid_anomaly;
        let contents = expect(&mut Der(bytes), 0x30).map_err(inv)?;
        for (index, bag) in values(&contents).map_err(inv)?.into_iter().enumerate() {
            let mut bag = fields(&bag).map_err(inv)?;
            let kind = oid(&mut bag).map_err(inv)?;
            let wrapper = next(&mut bag).map_err(inv)?;
            if wrapper.tag & 0xdf != 0x80 { return Err(inv(malformed())); }
            let value = next(&mut Der(&wrapper.content)).map_err(inv)?;
            let kind = kind.strip_prefix(BAG_PREFIX).and_then(|suffix| if let [kind] = suffix { Some(*kind) } else { None });
            enum Item { Private(Zeroizing<Vec<u8>>), Secret(Zeroizing<Vec<u8>>), Certificate(Vec<u8>, X509), Ignored }
            let item = match kind {
                Some(2) => {
                    self.private_keys = self.private_keys.checked_add(1).ok_or_else(|| inv(overflow()))?;
                    Item::Private(secret_der::encode(value.tag, &value.content))
                }
                Some(3 | 5) => {
                    if value.tag != 0x30 { return Err(inv(malformed())); }
                    let [id, wrapper]: [Value<'_>; 2] = values(&value.content).map_err(inv)?.try_into().map_err(|_| inv(malformed()))?;
                    if id.tag != 6 { return Err(inv(malformed())); } valid_oid(&id.content).map_err(inv)?;
                    if wrapper.tag & 0xdf != 0x80 { return Err(inv(malformed())); }
                    let bytes = octets(next(&mut Der(&wrapper.content)).map_err(inv)?).map_err(inv)?;
                    if kind == Some(3) {
                        let native = X509::from_der(&bytes).or_else(|_| X509::from_pem(&bytes)).map_err(TlsError::invalid_keystore)?;
                        let der = native.to_der().map_err(TlsError::invalid_keystore)?;
                        CertificateIdentifiers::decode(&der, memory)?;
                        Item::Certificate(der, native)
                    } else { Item::Secret(bytes) }
                }
                _ => Item::Ignored,
            };
            let mut attributes = attributes::parse(&mut bag).map_err(inv)?;
            match item {
                Item::Private(protected) => {
                    let key_id = match attributes.key_id.take() {
                        Some(key_id) => key_id,
                        None if self.private_keys == 1 => b"01".to_vec(),
                        None => continue,
                    };
                    let alias = self.name(attributes.alias.take()).map_err(inv)?;
                    let index = self.keys.len();
                    self.keys.push(PrivateEntry { protected, alias: alias.clone(), key_id });
                    self.insert(alias, Entry::Private(index));
                }
                Item::Secret(protected) => {
                    let alias = self.name(attributes.alias.take()).map_err(inv)?;
                    self.insert(alias, Entry::Secret(protected));
                }
                Item::Certificate(der, native) => {
                    if attributes.key_id.is_none() && self.private_keys == 1 && index == 0 { attributes.key_id = Some(b"01".to_vec()); }
                    if attributes.trusted {
                        let alias = self.name(attributes.alias.take()).map_err(inv)?;
                        self.insert(alias.clone(), Entry::Trusted);
                        attributes.alias = Some(alias);
                    }
                    self.certificates.push(Arc::new(Certificate { der, native, attributes }));
                }
                Item::Ignored => {}
            }
        }
        Ok(())
    }

    fn name(&mut self, name: Option<Vec<u16>>) -> Result<Vec<u16>> {
        if let Some(name) = name { return Ok(name); }
        self.unnamed = self.unnamed.checked_add(1).ok_or_else(overflow)?;
        Ok(self.unnamed.to_string().encode_utf16().collect())
    }

    fn insert(&mut self, alias: Vec<u16>, entry: Entry) {
        let alias = lowercase_utf16(&alias);
        if let Some(previous) = self.entries.iter_mut().find(|previous| previous.alias == alias) { previous.entry = entry; }
        else { self.entries.push(NamedEntry { alias, entry }); }
    }

    fn matched(&self, key: &PrivateEntry) -> Option<Arc<Certificate>> {
        let mut key_id_match = None; let mut alias_match = None;
        for certificate in self.certificates.iter().filter(|certificate| !certificate.attributes.trusted) {
            let alias_matches = certificate.attributes.alias.as_deref().is_some_and(|alias| equals_ignore_case_utf16(&key.alias, alias));
            if certificate.attributes.key_id.as_deref() == Some(key.key_id.as_slice()) {
                key_id_match = Some(Arc::clone(certificate));
                if alias_matches { return key_id_match; }
            } else if alias_matches { alias_match = Some(Arc::clone(certificate)); }
        }
        key_id_match.or(alias_match)
    }

    fn chain(&self, key: &PrivateEntry, memory: &Arc<MemoryBudget>) -> Result<Chain> {
        let bound = self.certificates.len().checked_mul(std::mem::size_of::<Arc<Certificate>>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?;
        let reservation = memory.reserve(bound)?;
        let mut chain: Vec<Arc<Certificate>> = Vec::with_capacity(self.certificates.len());
        let mut next = self.matched(key);
        while let Some(certificate) = next {
            if chain.iter().any(|previous| previous.der == certificate.der) { break; }
            chain.push(Arc::clone(&certificate));
            let issuer = DistinguishedName::decode(&certificate.native.issuer_name().to_der().map_err(invalid)?, memory)?;
            let subject = DistinguishedName::decode(&certificate.native.subject_name().to_der().map_err(invalid)?, memory)?;
            if issuer == subject && certificate.native.public_key().and_then(|key| certificate.native.verify(&key)).unwrap_or(false) { break; }
            drop(subject);
            let identifiers = CertificateIdentifiers::decode(&certificate.der, memory)?;
            let authority = identifiers.authority_key_identifier();
            next = None; let mut fallback = None;
            for candidate in &self.certificates {
                let subject = DistinguishedName::decode(&candidate.native.subject_name().to_der().map_err(invalid)?, memory)?;
                if subject != issuer { continue; }
                let Some(authority) = authority else { next = Some(Arc::clone(candidate)); break; };
                let identifiers = CertificateIdentifiers::decode(&candidate.der, memory)?;
                match identifiers.subject_key_identifier() {
                    Some(subject) if subject == authority => { next = Some(Arc::clone(candidate)); break; }
                    Some(_) => {}
                    None => fallback = Some(Arc::clone(candidate)),
                }
            }
            if next.is_none() { next = fallback; }
        }
        Ok(Chain { certificates: chain, _reservation: reservation })
    }

    pub(super) fn recover(self, password: Option<&str>, memory: &Arc<MemoryBudget>) -> Result<Vec<SunX509Pkcs12Identity>> {
        let chains = self.keys.iter().map(|key| self.chain(key, memory)).collect::<Result<Vec<_>>>()?;
        let mut identities = Vec::new();
        for binding in self.entries {
            let (protected, private) = match binding.entry {
                Entry::Trusted => continue,
                Entry::Private(index) => (self.keys[index].protected.as_slice(), Some(index)),
                Entry::Secret(ref protected) => (protected.as_slice(), None),
            };
            let _scratch = scratch(protected.len(), password, memory)?;
            let mut encoded = Der(protected); let outer = next(&mut encoded)?; encoded.end()?;
            let mut fields = Der(&outer.content);
            let protection = Protection::parse(next(&mut fields)?)?;
            let ciphertext = octets(next(&mut fields)?)?; fields.end()?;
            let key = protection::retry(password, |password| {
                let plaintext = protection.decrypt(&ciphertext, password).map_err(TlsError::from_invalid_anomaly)?;
                if private.is_some() {
                    let input = FactoryInput::pkcs12(&plaintext).map_err(TlsError::from_invalid_anomaly)?;
                    PrivateKey::from_pkcs12(input, memory).map(Some)
                } else { protection::secret_key(&plaintext).map_err(TlsError::from_invalid_anomaly)?; Ok(None) }
            })?;
            let Some((key, index)) = key.zip(private) else { continue; };
            let chain = &chains[index].certificates;
            if chain.is_empty() { continue; }
            let bound = chain.iter().try_fold(65_536usize, |bytes, certificate| certificate.der.len().checked_mul(16).and_then(|length| length.checked_add(64)).and_then(|length| bytes.checked_add(length)).ok_or_else(overflow))?;
            let mut reservation = memory.reserve(bound)?;
            let certificates = chain.iter().map(|certificate| certificate.der.clone()).collect::<Vec<_>>();
            let retained = certificates.iter().try_fold(std::mem::size_of::<SunX509Pkcs12Identity>() + 128, |bytes, certificate| bytes.checked_add(certificate.capacity()).ok_or_else(overflow))?
                .checked_add(certificates.capacity().checked_mul(std::mem::size_of::<Vec<u8>>()).ok_or_else(overflow)?).ok_or_else(overflow)?;
            Arc::get_mut(&mut reservation).expect("unpublished PKCS12 identity reservation").shrink_to(retained)?;
            identities.push(SunX509Pkcs12Identity { alias: binding.alias, private_key: key, certificates, _reservation: reservation });
        }
        Ok(identities)
    }
}

#[cfg(test)]
mod tests;
