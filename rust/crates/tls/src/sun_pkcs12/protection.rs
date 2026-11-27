use openssl::{hash::{Hasher, MessageDigest}, pkey::PKey, provider::Provider, sign::Signer, symm::{Cipher, Crypter, Mode}};
use rtdb_types::Result;
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, Zeroizing};
use crate::{TlsError, TlsResult, invalid, java_der::Der, secret_der::{self, Value, next}};
use super::der::{expect, integer, malformed, octets, oid};

const PKCS12_PBE: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x0c\x01";
const PBES2: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x05\x0d";
const PBKDF2: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x05\x0c";
const MD5_DES: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x05\x03";

enum Scheme { Pkcs12(u8), Md5Des, Pbes2 { cipher: Cipher, hash: MessageDigest, iv: Zeroizing<Vec<u8>> } }
pub(super) struct Protection { scheme: Scheme, salt: Zeroizing<Vec<u8>>, count: usize }

impl Protection {
    pub(super) fn parse(value: Value<'_>) -> Result<Self> {
        let encoded = secret_der::encode(value.tag, &value.content);
        let algorithm = secret_der::algorithm(&mut Der(&encoded), malformed)?;
        let content = expect(&mut Der(&algorithm.encoded), 0x30)?;
        let mut fields = Der(&content); oid(&mut fields)?;
        let parameters = expect(&mut fields, 0x30)?;
        fields.end()?;
        if algorithm.oid.as_slice() == PBES2 { return Self::pbes2(&parameters); }
        let scheme = if algorithm.oid.as_slice() == MD5_DES { Scheme::Md5Des }
        else {
            match algorithm.oid.strip_prefix(PKCS12_PBE) {
                Some([kind @ (1 | 2 | 3 | 5 | 6)]) => Scheme::Pkcs12(*kind),
                _ => return Err(invalid("Unsupported SunJCE PKCS12 protection service")),
            }
        };
        let mut parameters = Der(&parameters);
        let salt = expect(&mut parameters, 4)?.into_owned();
        let count = count(integer(&mut parameters)?, false)?;
        parameters.end()?;
        if salt.len() < 8 || (matches!(scheme, Scheme::Md5Des) && salt.len() != 8) { return Err(invalid("Invalid SunJCE PBE salt length")); }
        Ok(Self { scheme, salt, count })
    }

    fn pbes2(parameters: &[u8]) -> Result<Self> {
        let mut fields = Der(parameters);
        let first = next(&mut fields)?;
        if first.tag == 6 {
            let nested = expect(&mut fields, 0x30)?;
            return Self::pbes2_fields(&nested);
        }
        Self::pbes2_parts(first, &mut fields)
    }

    fn pbes2_fields(parameters: &[u8]) -> Result<Self> {
        let mut fields = Der(parameters); let kdf = next(&mut fields)?;
        Self::pbes2_parts(kdf, &mut fields)
    }

    fn pbes2_parts(kdf: Value<'_>, fields: &mut Der<'_>) -> Result<Self> {
        if kdf.tag != 0x30 { return Err(malformed()); }
        let mut kdf = Der(&kdf.content);
        if oid(&mut kdf)?.as_slice() != PBKDF2 { return Err(invalid("Unsupported SunJCE PBES2 derivation")); }
        let parameters = expect(&mut kdf, 0x30)?;
        let mut parameters = Der(&parameters);
        let salt = expect(&mut parameters, 4)?.into_owned();
        let count = count(integer(&mut parameters)?, true)?;
        if parameters.0.first() == Some(&2) { integer(&mut parameters)?; }
        let hash = if parameters.0.first() == Some(&0x30) {
            let prf = expect(&mut parameters, 0x30)?; let mut prf = Der(&prf);
            let hash = hmac(&oid(&mut prf)?)?;
            if prf.0.first() == Some(&5) { next(&mut prf)?; }
            prf.end()?; hash
        } else { MessageDigest::sha1() };
        let encryption = next(fields)?;
        let mut encryption = Der(&encryption.content);
        let cipher = match oid(&mut encryption)?.as_slice() {
            b"\x60\x86\x48\x01\x65\x03\x04\x01\x02" => Cipher::aes_128_cbc(),
            b"\x60\x86\x48\x01\x65\x03\x04\x01\x2a" => Cipher::aes_256_cbc(),
            _ => return Err(invalid("Unsupported SunJCE PBES2 cipher")),
        };
        let iv = expect(&mut encryption, 4)?.into_owned();
        if salt.len() < 8 || iv.len() != 16 { return Err(invalid("Invalid SunJCE PBES2 salt or IV length")); }
        Ok(Self { scheme: Scheme::Pbes2 { cipher, hash, iv }, salt, count })
    }

    pub(super) fn decrypt(&self, ciphertext: &[u8], password: &str) -> Result<Zeroizing<Vec<u8>>> {
        let _default = Provider::load(None, "default").map_err(invalid)?;
        match &self.scheme {
            Scheme::Pbes2 { cipher, hash, iv } => {
                if password.len() > i32::MAX as usize || self.salt.len() > i32::MAX as usize { return Err(malformed()); }
                let mut key = Zeroizing::new(vec![0; cipher.key_len()]);
                openssl::pkcs5::pbkdf2_hmac(password.as_bytes(), &self.salt, self.count, *hash, &mut key).map_err(invalid)?;
                decrypt_cipher(*cipher, &key, iv, ciphertext)
            }
            Scheme::Md5Des => {
                let _legacy = Provider::load(None, "legacy").map_err(invalid)?;
                let mut digest = hash(MessageDigest::md5(), &[password.as_bytes(), &self.salt])?;
                for _ in 1..self.count { digest = hash(MessageDigest::md5(), &[&digest])?; }
                decrypt_cipher(Cipher::des_cbc(), &digest[..8], &digest[8..], ciphertext)
            }
            Scheme::Pkcs12(kind) => {
                let _legacy = if *kind == 3 { None } else { Some(Provider::load(None, "legacy").map_err(invalid)?) };
                let (cipher, length) = match kind { 1 => (Cipher::rc4(), 16), 2 => (Cipher::rc4(), 5), 3 => (Cipher::des_ede3_cbc(), 24), 5 => (Cipher::rc2_cbc(), 16), 6 => (Cipher::rc2_40_cbc(), 5), _ => unreachable!("admitted PKCS12 cipher") };
                let password = password_bytes(password, true)?;
                let key = derive(MessageDigest::sha1(), &password, &self.salt, self.count, 1, length)?;
                let iv = derive(MessageDigest::sha1(), &password, &self.salt, self.count, 2, cipher.iv_len().unwrap_or(0))?;
                decrypt_cipher(cipher, &key, &iv, ciphertext)
            }
        }
    }
}

pub(super) fn retry<T>(password: Option<&str>, mut attempt: impl FnMut(&str) -> TlsResult<T>) -> TlsResult<T> {
    let password = password.ok_or_else(|| TlsError::invalid_keystore("PKCS12 key recovery requires a password"))?;
    match attempt(password) {
        Err(TlsError::InvalidKeystore(_)) if password.is_empty() => attempt("\0"),
        result => result,
    }
}

pub(super) fn verify_mac(value: Value<'_>, authenticated: &[u8], password: &str) -> Result<()> {
    if value.tag != 0x30 { return Err(malformed()); }
    let mut fields = Der(&value.content);
    let digest = expect(&mut fields, 0x30)?;
    let mut digest = Der(&digest);
    let algorithm = secret_der::algorithm(&mut digest, malformed)?;
    let hash = digest_algorithm(&algorithm.oid)?;
    let expected = octets(next(&mut digest)?)?; digest.end()?;
    let salt = octets(next(&mut fields)?)?;
    let count = if fields.0.is_empty() { 1 } else { count(integer(&mut fields)?, false)? };
    fields.end()?;
    if salt.len() < 8 { return Err(invalid("Invalid SunJCE MAC salt length")); }
    retry(Some(password), |password| {
        let password = password_bytes(password, false).map_err(TlsError::from_invalid_anomaly)?;
        let key = derive(hash, &password, &salt, count, 3, hash.size()).map_err(TlsError::from_invalid_anomaly)?;
        let key = PKey::hmac(&key).map_err(TlsError::invalid_keystore)?;
        let mut signer = Signer::new(hash, &key).map_err(TlsError::invalid_keystore)?;
        signer.update(authenticated).map_err(TlsError::invalid_keystore)?;
        let actual = Zeroizing::new(signer.sign_to_vec().map_err(TlsError::invalid_keystore)?);
        if bool::from(actual.as_slice().ct_eq(&expected)) { Ok(()) } else { Err(TlsError::invalid_keystore("PKCS12 integrity check failed")) }
    })?;
    Ok(())
}

pub(super) fn secret_key(encoded: &[u8]) -> Result<()> {
    let mut input = Der(encoded); let value = next(&mut input)?; input.end()?;
    let mut fields = super::der::fields(&value)?;
    integer(&mut fields)?;
    let algorithm = expect(&mut fields, 0x30)?; let mut algorithm = Der(&algorithm);
    let oid = oid(&mut algorithm)?;
    if !algorithm.0.is_empty() { next(&mut algorithm)?; }
    algorithm.end()?;
    let key = expect(&mut fields, 4)?;
    if key.is_empty() { return Err(invalid("Empty PKCS12 secret key")); }
    if matches!(oid.as_slice(), [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 6 | 10 | 11 | 13]) {
        return Err(invalid("Unsupported SunJCE PBE secret-key factory"));
    }
    Ok(())
}

fn count(count: i32, default_zero: bool) -> Result<usize> {
    if !(0..=5_000_000).contains(&count) || (count == 0 && !default_zero) { return Err(invalid("PKCS12 PBE iteration count is outside the Java limit")); }
    Ok(if count == 0 { 4096 } else { count as usize })
}

fn digest_algorithm(oid: &[u8]) -> Result<MessageDigest> {
    if oid == b"\x2b\x0e\x03\x02\x1a" { return Ok(MessageDigest::sha1()); }
    match oid.strip_prefix(b"\x60\x86\x48\x01\x65\x03\x04\x02") {
        Some([1]) => Ok(MessageDigest::sha256()), Some([2]) => Ok(MessageDigest::sha384()), Some([3]) => Ok(MessageDigest::sha512()), Some([4]) => Ok(MessageDigest::sha224()),
        Some([5]) => MessageDigest::from_name("SHA512-224").ok_or_else(malformed), Some([6]) => MessageDigest::from_name("SHA512-256").ok_or_else(malformed),
        _ => Err(invalid("Unsupported SunJCE PKCS12 MAC digest")),
    }
}
fn hmac(oid: &[u8]) -> Result<MessageDigest> {
    match oid.strip_prefix(b"\x2a\x86\x48\x86\xf7\x0d\x02") {
        Some([7]) => Ok(MessageDigest::sha1()), Some([8]) => Ok(MessageDigest::sha224()), Some([9]) => Ok(MessageDigest::sha256()), Some([10]) => Ok(MessageDigest::sha384()), Some([11]) => Ok(MessageDigest::sha512()),
        Some([12]) => MessageDigest::from_name("SHA512-224").ok_or_else(malformed), Some([13]) => MessageDigest::from_name("SHA512-256").ok_or_else(malformed),
        _ => Err(invalid("Unsupported SunJCE PBKDF2 digest")),
    }
}
fn password_bytes(password: &str, legacy_cipher: bool) -> Result<Zeroizing<Vec<u8>>> {
    let capacity = password.len().checked_mul(2).and_then(|bytes| bytes.checked_add(2)).ok_or_else(malformed)?;
    let mut bytes = Zeroizing::new(Vec::with_capacity(capacity));
    if password == "\0" { return Ok(bytes); }
    if legacy_cipher { bytes.extend(password.bytes().flat_map(|byte| [0, byte & 0x7f])); }
    else { bytes.extend(password.encode_utf16().flat_map(u16::to_be_bytes)); }
    bytes.extend_from_slice(&[0, 0]); Ok(bytes)
}

fn hash(hash: MessageDigest, parts: &[&[u8]]) -> Result<Zeroizing<Vec<u8>>> {
    let mut state = Hasher::new(hash).map_err(invalid)?;
    for part in parts { state.update(part).map_err(invalid)?; }
    let mut digest = state.finish().map_err(invalid)?;
    let output = Zeroizing::new(digest.to_vec()); digest[..].zeroize(); Ok(output)
}

// RFC 7292 Appendix B.2; password bytes have already passed the selected SunJCE service's conversion.
fn derive(digest: MessageDigest, password: &[u8], salt: &[u8], count: usize, purpose: u8, length: usize) -> Result<Zeroizing<Vec<u8>>> {
    let block = digest.block_size();
    let padded = |length: usize| length.checked_add(block - 1).map(|length| length / block * block).ok_or_else(malformed);
    let capacity = padded(salt.len())?.checked_add(padded(password.len())?).ok_or_else(malformed)?;
    let mut input = Zeroizing::new(Vec::with_capacity(capacity));
    for bytes in [salt, password] { if !bytes.is_empty() { input.extend(bytes.iter().copied().cycle().take(padded(bytes.len())?)); } }
    let diversifier = vec![purpose; block];
    let mut output = Zeroizing::new(Vec::with_capacity(length));
    while output.len() < length {
        let mut value = hash(digest, &[&diversifier, &input])?;
        for _ in 1..count { value = hash(digest, &[&value])?; }
        let remaining = (length - output.len()).min(value.len());
        output.extend_from_slice(&value[..remaining]);
        for chunk in input.chunks_exact_mut(block) {
            let mut carry = 1u16;
            for (index, byte) in chunk.iter_mut().enumerate().rev() {
                let sum = u16::from(*byte) + u16::from(value[index % value.len()]) + carry;
                *byte = sum as u8; carry = sum >> 8;
            }
        }
    }
    Ok(output)
}
fn decrypt_cipher(cipher: Cipher, key: &[u8], iv: &[u8], ciphertext: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    if ciphertext.len() > i32::MAX as usize { return Err(malformed()); }
    let mut state = Crypter::new(cipher, Mode::Decrypt, key, if iv.is_empty() { None } else { Some(iv) }).map_err(invalid)?;
    let capacity = ciphertext.len().checked_add(cipher.block_size()).ok_or_else(malformed)?;
    let mut output = Zeroizing::new(vec![0; capacity]);
    let count = state.update(ciphertext, &mut output).map_err(invalid)?;
    let last = state.finalize(&mut output[count..]).map_err(invalid)?;
    output[count + last..].zeroize(); output.truncate(count + last); Ok(output)
}

#[cfg(test)]
mod tests;
