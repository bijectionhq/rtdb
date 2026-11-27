use std::sync::Arc;
use base64::{Engine, engine::general_purpose::STANDARD};
use openssl::{pkey::PKey, x509::X509};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, Result};
use sha1::{Digest, Sha1};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;
mod pkcs12;
mod aliases;
mod x500;
pub use x500::DistinguishedName;
mod protected;
pub use protected::{ProtectedIdentity, ProtectedKeyStore, RecoveredKey};
mod sun_jks;
pub use sun_jks::{SunX509JksIdentity, SunX509JksStore};
mod java_der;
mod secret_der;
mod private_factory;
mod sun_pkcs12;
pub use sun_pkcs12::{SunX509Pkcs12Identity, SunX509Pkcs12Store};
mod identifiers;
pub use identifiers::{CertificateAlternativeNames, CertificateBasicConstraints, CertificateExtendedKeyUsage, CertificateIdentifiers, CertificateKeyUsage, CertificatePublicKeyEncoding};

pub type TlsResult<T> = std::result::Result<T, TlsError>;

/// Typed failure from `rtdb_tls` parsing and recovery, keeping wire codes unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TlsError {
    InvalidKeystore(String),
    UnsupportedIdentifier(String),
    Other(Anomaly),
}

impl TlsError {
    pub fn invalid_keystore(error: impl std::fmt::Display) -> Self {
        Self::InvalidKeystore(error.to_string())
    }

    pub fn unsupported_identifier(message: impl Into<String>) -> Self {
        Self::UnsupportedIdentifier(message.into())
    }

    pub(crate) fn from_invalid_anomaly(error: Anomaly) -> Self {
        Self::InvalidKeystore(error.message.unwrap_or_default())
    }

    pub fn code(&self) -> &str {
        match self {
            Self::InvalidKeystore(_) => "rtdb.tls/invalid-keystore",
            Self::UnsupportedIdentifier(_) => "rtdb.tls/unsupported-identifier",
            Self::Other(anomaly) => &anomaly.code,
        }
    }

    pub fn into_anomaly(self) -> Anomaly {
        self.into()
    }
}

impl From<TlsError> for Anomaly {
    fn from(error: TlsError) -> Self {
        match error {
            TlsError::InvalidKeystore(message) => Anomaly::incorrect("rtdb.tls/invalid-keystore", message),
            TlsError::UnsupportedIdentifier(message) => Anomaly::unsupported("rtdb.tls/unsupported-identifier", message),
            TlsError::Other(anomaly) => anomaly,
        }
    }
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidKeystore(message) => write!(f, "rtdb.tls/invalid-keystore: {message}"),
            Self::UnsupportedIdentifier(message) => write!(f, "rtdb.tls/unsupported-identifier: {message}"),
            Self::Other(anomaly) => std::fmt::Display::fmt(anomaly, f),
        }
    }
}

impl std::error::Error for TlsError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreFormat { Java, Pem }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreUse {
    /// Decodes private keys and requires at least one usable identity.
    Identity,
    /// Decodes available private keys; an empty Java store supplies no client certificate.
    OptionalIdentity,
    /// Reads trusted certificates without decrypting private keys.
    Trust,
}
pub struct KeyStore { identities: Vec<Identity>, trusted: Vec<Vec<u8>>, reservation: Arc<MemoryReservation> }
pub struct Identity { alias: Option<Vec<u16>>, private_key: Zeroizing<Vec<u8>>, certificates: Vec<Vec<u8>> }
pub struct IssuerNames { names: Vec<Vec<u8>>, _reservation: Arc<MemoryReservation> }
impl IssuerNames {
    pub fn as_slice(&self) -> &[Vec<u8>] { &self.names }
}
pub struct PemIdentity { bytes: Zeroizing<Vec<u8>>, _reservation: Arc<MemoryReservation> }
impl PemIdentity {
    /// Borrows zeroized-on-release PEM; any copies need a separate allocation reservation.
    pub fn bytes(&self) -> &[u8] { &self.bytes }
}
struct Parts { identities: Vec<Identity>, trusted: Vec<Vec<u8>> }
impl KeyStore {
    /// Bytes reserved before decoding, including native crypto and retained key material.
    pub fn allocation_bound(encoded_bytes: usize) -> Result<usize> { encoded_bytes.checked_mul(16).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| invalid("Keystore allocation exceeds range")) }
    /// Decode bound including temporary password encodings and derivation buffers.
    pub fn allocation_bound_with_passwords(encoded_bytes: usize, store_password: Option<&str>, key_password: Option<&str>) -> Result<usize> {
        let passwords = store_password.into_iter().chain(key_password).try_fold(0usize, |bytes, password| bytes.checked_add(password.len()).ok_or_else(|| invalid("Keystore password allocation exceeds range")))?;
        Self::allocation_bound(encoded_bytes)?.checked_add(passwords.checked_mul(16).ok_or_else(|| invalid("Keystore password allocation exceeds range"))?).ok_or_else(|| invalid("Keystore allocation exceeds range"))
    }
    /// Decodes one store and retains its reservation through all borrowed identities and certificates.
    /// Java format accepts JKS and PKCS12, including multiple identities and distinct key passwords.
    /// A missing store password skips integrity checks and encrypted PKCS12 certificate sections.
    /// The reservation must cover `allocation_bound_with_passwords` for these inputs before this call.
    pub fn decode(bytes: &[u8], format: StoreFormat, usage: StoreUse, store_password: Option<&str>, key_password: Option<&str>, reservation: Arc<MemoryReservation>) -> Result<Self> {
        if reservation.bytes() < Self::allocation_bound_with_passwords(bytes.len(), store_password, key_password)? { return Err(invalid("Keystore decode reservation is too small")); }
        let private_keys = usage != StoreUse::Trust;
        let parts = match format {
            StoreFormat::Java if bytes.starts_with(&0xfeedfeedu32.to_be_bytes()) => jks(bytes, store_password, key_password, private_keys)?,
            StoreFormat::Java => pkcs12::decode(bytes, store_password, key_password, private_keys)?,
            StoreFormat::Pem => {
                if store_password.is_some() { return Err(invalid("PEM stores accept a private-key password, not a store password")); }
                pem(bytes, key_password, private_keys)?
            }
        };
        for certificate in &parts.trusted { X509::from_der(certificate).map_err(invalid)?; }
        for identity in &parts.identities { for certificate in &identity.certificates { X509::from_der(certificate).map_err(invalid)?; } }
        if usage == StoreUse::Identity && parts.identities.is_empty() { return Err(invalid("Keystore contains no private key and certificate chain")); }
        Ok(Self { identities: parts.identities, trusted: parts.trusted, reservation })
    }
    /// Private-key entries in the Java store's alias-enumeration order, or the single PEM identity.
    pub fn identities(&self) -> &[Identity] { &self.identities }
    pub fn trusted_certificates(&self) -> &[Vec<u8>] { &self.trusted }
    /// Retain this charge with TLS configuration objects that copy material from this store.
    pub fn reservation(&self) -> Arc<MemoryReservation> { Arc::clone(&self.reservation) }
}
impl Identity {
    fn new(alias: Option<Vec<u16>>, private_key: Zeroizing<Vec<u8>>, certificates: Vec<Vec<u8>>) -> Result<Self> {
        let key = PKey::private_key_from_pkcs8(&private_key).map_err(invalid)?;
        let certificate = X509::from_der(certificates.first().ok_or_else(|| invalid("Private key has no certificate"))?).map_err(invalid)?;
        if !certificate.public_key().map_err(invalid)?.public_eq(&key) { return Err(invalid("Private key does not match its leaf certificate")); }
        Ok(Self { alias, private_key, certificates })
    }
    /// Java store alias as UTF-16 code units; PEM identities have no stored alias.
    pub fn alias(&self) -> Option<&[u16]> { self.alias.as_deref() }
    pub fn private_key_pkcs8(&self) -> &[u8] { &self.private_key }
    pub fn certificates(&self) -> &[Vec<u8>] { &self.certificates }
    /// DER issuer names in chain order, for matching a peer's acceptable certificate-authority hints.
    /// Each result owns a separate reservation covering decoding and retained names.
    pub fn issuer_names(&self, memory: &Arc<MemoryBudget>) -> Result<IssuerNames> {
        let bound = self.certificates.iter().try_fold(65_536usize, |bytes, certificate| certificate.len().checked_mul(16).and_then(|length| length.checked_add(std::mem::size_of::<Vec<u8>>())).and_then(|length| bytes.checked_add(length)).ok_or_else(|| invalid("Issuer names exceed allocation range")))?;
        let reservation = memory.reserve(bound)?;
        let names = self.certificates.iter().map(|certificate| X509::from_der(certificate).and_then(|certificate| certificate.issuer_name().to_der()).map_err(invalid)).collect::<Result<Vec<_>>>()?;
        Ok(IssuerNames { names, _reservation: reservation })
    }
    /// Copies an identity to PEM under a separate reservation retained by the result.
    pub fn to_pem(&self, memory: &Arc<MemoryBudget>) -> Result<PemIdentity> {
        let capacity = std::iter::once(self.private_key.len()).chain(self.certificates.iter().map(Vec::len)).try_fold(0usize, |size, length| length.checked_mul(2).and_then(|length| length.checked_add(128)).and_then(|length| size.checked_add(length)).ok_or_else(|| invalid("PEM output exceeds range")))?;
        let reservation = memory.reserve(capacity.checked_mul(2).ok_or_else(|| invalid("PEM allocation exceeds range"))?)?;
        let mut output = Zeroizing::new(Vec::with_capacity(capacity));
        pem_block(&mut output, "PRIVATE KEY", &self.private_key);
        for certificate in &self.certificates { pem_block(&mut output, "CERTIFICATE", certificate); }
        Ok(PemIdentity { bytes: output, _reservation: reservation })
    }
}
fn pem_block(output: &mut Vec<u8>, label: &str, bytes: &[u8]) {
    output.extend_from_slice(format!("-----BEGIN {label}-----\n").as_bytes());
    let encoded = Zeroizing::new(STANDARD.encode(bytes));
    for line in encoded.as_bytes().chunks(64) { output.extend_from_slice(line); output.push(b'\n'); }
    output.extend_from_slice(format!("-----END {label}-----\n").as_bytes());
}
fn pem(bytes: &[u8], key_password: Option<&str>, private_keys: bool) -> Result<Parts> {
    let certificates = X509::stack_from_pem(bytes).map_err(invalid)?.into_iter().map(|certificate| certificate.to_der().map_err(invalid)).collect::<Result<Vec<_>>>()?;
    if certificates.is_empty() { return Err(invalid("PEM store contains no certificates")); }
    let identities = if private_keys {
        let text = std::str::from_utf8(bytes).map_err(|_| invalid("PEM material must be UTF-8"))?;
        let plain = text.matches("-----BEGIN PRIVATE KEY-----").count();
        let encrypted = text.matches("-----BEGIN ENCRYPTED PRIVATE KEY-----").count();
        if plain + encrypted != 1 { return Err(invalid("PEM identity requires exactly one PKCS8 private key")); }
        if (encrypted == 1) != key_password.is_some() { return Err(invalid("Encrypted PEM keys require a key password; plain keys require no password")); }
        let key = match key_password { Some(password) => PKey::private_key_from_pem_passphrase(bytes, password.as_bytes()), None => PKey::private_key_from_pem(bytes) }.map_err(invalid)?;
        vec![Identity::new(None, Zeroizing::new(key.private_key_to_pkcs8().map_err(invalid)?), certificates.clone())?]
    } else { Vec::new() };
    Ok(Parts { identities, trusted: certificates })
}
// JKS framing and key protection: OpenJDK JavaKeyStore and KeyProtector, jdk-21+35.
// https://github.com/openjdk/jdk/tree/jdk-21%2B35/src/java.base/share/classes/sun/security/provider
fn jks(bytes: &[u8], password: Option<&str>, key_password: Option<&str>, private_keys: bool) -> Result<Parts> {
    let entries = jks_entries(bytes, password, |_| Ok(()))?;
    let key_password = key_password.or(password).map(|password| Zeroizing::new(password.encode_utf16().flat_map(u16::to_be_bytes).collect::<Vec<_>>()));
    let mut identities = Vec::new(); let mut trusted = Vec::new();
    for (alias, entry) in entries.into_enumeration() {
        match entry {
            JksEntry::Key { protected, certificates } => {
                if let Some(certificate) = certificates.first() { trusted.push(certificate.to_vec()); }
                if private_keys {
                    let password = key_password.as_ref().ok_or_else(|| invalid("JKS private keys require a password"))?;
                    let key = decrypt_jks_key(protected, password)?;
                    if !certificates.is_empty() { identities.push(Identity::new(Some(alias), key, certificates.into_iter().map(<[u8]>::to_vec).collect())?); }
                }
            }
            JksEntry::Certificate(certificate) => trusted.push(certificate.to_vec()),
        }
    }
    Ok(Parts { identities, trusted })
}

fn jks_entries<'a>(bytes: &'a [u8], password: Option<&str>, validate_certificate: impl Fn(&[u8]) -> Result<()>) -> Result<aliases::JksEntries<JksEntry<'a>>> {
    let split = bytes.len().checked_sub(20).ok_or_else(|| invalid("Truncated JKS digest"))?;
    let (body, digest) = bytes.split_at(split);
    if let Some(password) = password {
        let mut verifier = Sha1::new();
        for unit in password.encode_utf16() { verifier.update(unit.to_be_bytes()); }
        verifier.update(b"Mighty Aphrodite"); verifier.update(body);
        if !bool::from(verifier.finalize().as_slice().ct_eq(digest)) { return Err(invalid("JKS integrity check failed")); }
    }
    let mut cursor = Input(body);
    if cursor.u32()? != 0xfeedfeed { return Err(invalid("Invalid JKS magic")); }
    let version = cursor.u32()?;
    if !matches!(version, 1 | 2) { return Err(invalid("Unsupported JKS version")); }
    let count = cursor.u32()? as usize;
    if count > cursor.0.len() / 14 { return Err(invalid("JKS entry count exceeds input")); }
    let mut entries = aliases::JksEntries::new();
    for _ in 0..count {
        let tag = cursor.u32()?;
        let alias = modified_utf8(cursor.short_bytes()?)?;
        cursor.take(8)?;
        let entry = match tag {
            1 => {
                let protected = cursor.long_bytes()?;
                let chain_count = cursor.u32()? as usize;
                if chain_count > cursor.0.len() / 4 { return Err(invalid("JKS certificate count exceeds input")); }
                let mut certificates = Vec::with_capacity(chain_count);
                for _ in 0..chain_count {
                    let certificate = certificate(&mut cursor, version)?;
                    validate_certificate(certificate)?;
                    certificates.push(certificate);
                }
                JksEntry::Key { protected, certificates }
            }
            2 => {
                let certificate = certificate(&mut cursor, version)?;
                validate_certificate(certificate)?;
                JksEntry::Certificate(certificate)
            }
            _ => return Err(invalid("Unknown JKS entry type")),
        };
        entries.insert(alias, entry)?;
    }
    if !cursor.0.is_empty() { return Err(invalid("Trailing JKS content")); }
    Ok(entries)
}

enum JksEntry<'a> { Key { protected: &'a [u8], certificates: Vec<&'a [u8]> }, Certificate(&'a [u8]) }

// DataInputStream.readUTF, jdk-21+35: aliases compare as UTF-16, including isolated surrogates.
fn modified_utf8(bytes: &[u8]) -> Result<Vec<u16>> {
    let mut input = Input(bytes);
    let mut units = Vec::with_capacity(bytes.len());
    while !input.0.is_empty() {
        let first = input.take(1)?[0];
        let unit = match first {
            0..=0x7f => u16::from(first),
            0xc0..=0xdf => {
                let second = input.take(1)?[0];
                if second & 0xc0 != 0x80 { return Err(invalid("Invalid modified UTF-8 alias")); }
                (u16::from(first & 0x1f) << 6) | u16::from(second & 0x3f)
            }
            0xe0..=0xef => {
                let rest = input.take(2)?;
                if rest.iter().any(|byte| byte & 0xc0 != 0x80) { return Err(invalid("Invalid modified UTF-8 alias")); }
                (u16::from(first & 0x0f) << 12) | (u16::from(rest[0] & 0x3f) << 6) | u16::from(rest[1] & 0x3f)
            }
            _ => return Err(invalid("Invalid modified UTF-8 alias")),
        };
        units.push(unit);
    }
    Ok(units)
}

fn certificate<'a>(cursor: &mut Input<'a>, version: u32) -> Result<&'a [u8]> {
    if version == 2 && !matches!(cursor.short_bytes()?, b"X.509" | b"X509") { return Err(invalid("TLS requires X.509 certificates")); }
    let certificate = cursor.long_bytes()?;
    X509::from_der(certificate).map_err(invalid)?;
    Ok(certificate)
}

fn decrypt_jks_key(bytes: &[u8], password: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    let mut outer = Input(bytes);
    let mut sequence = Input(outer.der(0x30)?);
    let mut algorithm = Input(sequence.der(0x30)?);
    if algorithm.der(0x06)? != [0x2b, 6, 1, 4, 1, 0x2a, 2, 0x11, 1, 1] { return Err(invalid("Unsupported JKS private key protection algorithm")); }
    if !algorithm.0.is_empty() && !algorithm.der(0x05)?.is_empty() { return Err(invalid("Invalid JKS key algorithm parameters")); }
    let encrypted = sequence.der(0x04)?;
    if !outer.0.is_empty() || !sequence.0.is_empty() || !algorithm.0.is_empty() || encrypted.len() < 40 { return Err(invalid("Invalid JKS protected private key")); }
    let (salt, rest) = encrypted.split_at(20);
    let (encrypted, expected) = rest.split_at(rest.len() - 20);
    let mut previous: [u8; 20] = salt.try_into().map_err(invalid)?;
    let mut plaintext = Zeroizing::new(Vec::with_capacity(encrypted.len()));
    for block in encrypted.chunks(20) {
        let mut hash = Sha1::new();
        hash.update(password);
        hash.update(previous);
        previous = hash.finalize().into();
        plaintext.extend(block.iter().zip(previous).map(|(byte, mask)| byte ^ mask));
    }
    let mut verifier = Sha1::new();
    verifier.update(password);
    verifier.update(plaintext.as_slice());
    if !bool::from(verifier.finalize().as_slice().ct_eq(expected)) { return Err(invalid("JKS private key password is incorrect")); }
    Ok(plaintext)
}

struct Input<'a>(&'a [u8]);
impl<'a> Input<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        if count > self.0.len() { return Err(invalid("Truncated keystore")); }
        let (value, rest) = self.0.split_at(count);
        self.0 = rest;
        Ok(value)
    }
    fn u32(&mut self) -> Result<u32> { Ok(u32::from_be_bytes(self.take(4)?.try_into().map_err(invalid)?)) }
    fn short_bytes(&mut self) -> Result<&'a [u8]> { let length = u16::from_be_bytes(self.take(2)?.try_into().map_err(invalid)?) as usize; self.take(length) }
    fn long_bytes(&mut self) -> Result<&'a [u8]> { let length = self.u32()? as usize; self.take(length) }
    fn der(&mut self, tag: u8) -> Result<&'a [u8]> {
        if self.take(1)?[0] != tag { return Err(invalid("Unexpected DER tag in protected key")); }
        let first = self.take(1)?[0];
        let length = if first < 0x80 { usize::from(first) } else {
            let count = usize::from(first & 0x7f);
            if count == 0 || count > std::mem::size_of::<usize>() { return Err(invalid("Invalid DER length")); }
            self.take(count)?.iter().try_fold(0usize, |length, byte| length.checked_mul(256).and_then(|length| length.checked_add(usize::from(*byte))).ok_or_else(|| invalid("DER length exceeds range")))?
        };
        self.take(length)
    }
}
fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.tls/invalid-keystore", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn der_lengths_are_bounded_by_the_input() {
        assert!(Input(&[4, 0x80]).der(4).is_err());
        assert!(Input(&[4, 0x84, 0xff, 0xff, 0xff, 0xff]).der(4).is_err());
        assert!(Input(&[4, 3, 1, 2]).der(4).is_err());
        assert_eq!(Input(&[4, 2, 1, 2]).der(4).unwrap(), &[1, 2]);
    }
}
