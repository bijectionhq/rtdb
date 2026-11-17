use openssl::{hash::{Hasher, MessageDigest}, pkey::PKey, provider::Provider, sign::Signer, symm::{Cipher, Crypter, Mode}, x509::X509};
use rtdb_types::Result;
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, Zeroizing};
use super::{Identity, Parts, invalid};
mod ber;
use ber::{Reader, Value};
const MAXIMUM_ITERATIONS: usize = 5_000_000;
const DATA: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x07\x01";
const ENCRYPTED_DATA: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x07\x06";
const BAG_PREFIX: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x0c\x0a\x01";
const PKCS12_PBE: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x0c\x01";
const PBES2: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x05\x0d";
const PBKDF2: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x05\x0c";
#[derive(Default)]
struct Attributes { alias: Option<String>, key_id: Option<Vec<u8>>, trusted: bool }
struct KeyBag { encrypted: Zeroizing<Vec<u8>>, attributes: Attributes, order: usize }
struct CertificateBag { der: Vec<u8>, certificate: X509, attributes: Attributes, order: usize }
#[derive(Default)]
struct Bags { keys: Vec<KeyBag>, certificates: Vec<CertificateBag>, private_keys_seen: usize, unfriendly_name: usize, next_order: usize }

// RFC 7292 §§4–5; OpenJDK PKCS12KeyStore at jdk-21+35 supplies the Java password rules.
pub(super) fn decode(bytes: &[u8], password: Option<&str>, key_password: Option<&str>, private_keys: bool) -> Result<Parts> {
    let mut outer = Reader(bytes);
    let mut pfx = outer.expect(0x30)?.reader(); outer.end()?;
    if pfx.integer()? != 3 { return Err(invalid("Unsupported PKCS12 version")); }
    let mut auth_safe = pfx.expect(0x30)?.reader();
    if auth_safe.expect(6)?.content != DATA { return Err(invalid("Public-key protected PKCS12 is unsupported")); }
    let authenticated = explicit_octets(auth_safe.expect(0xa0)?)?; auth_safe.end()?;
    if !pfx.is_empty() {
        let mac = pfx.expect(0x30)?;
        if let Some(password) = password { verify_mac(mac, &authenticated, password)?; }
    }
    pfx.end()?;
    let mut authenticated_outer = Reader(&authenticated);
    let mut contents = authenticated_outer.expect(0x30)?.reader(); authenticated_outer.end()?;
    let mut bags = Bags::default();
    while !contents.is_empty() {
        let mut info = contents.expect(0x30)?.reader();
        let kind = info.expect(6)?.content;
        let content = info.expect(0xa0)?; info.end()?;
        if kind == DATA { bags.read(&explicit_octets(content)?, 0)?; }
        else if kind == ENCRYPTED_DATA {
            if let Some(password) = password {
                let mut content = content.reader();
                let mut encrypted = content.expect(0x30)?.reader(); content.end()?;
                if encrypted.integer()? != 0 { return Err(invalid("Unsupported encrypted PKCS12 content version")); }
                let mut info = encrypted.expect(0x30)?.reader(); encrypted.end()?;
                if info.expect(6)?.content != DATA { return Err(invalid("Unsupported encrypted PKCS12 content")); }
                let algorithm = info.expect(0x30)?;
                let ciphertext = info.next()?.octets(true)?; info.end()?;
                bags.read(&decrypt(algorithm, &ciphertext, password)?, 0)?;
            }
        } else { return Err(invalid("Public-key protected PKCS12 content is unsupported")); }
    }
    bags.finish(key_password.or(password), private_keys, bytes.len().checked_mul(4).ok_or_else(|| invalid("PKCS12 retained material exceeds range"))?)
}
fn explicit_octets(value: Value<'_>) -> Result<Zeroizing<Vec<u8>>> {
    let mut content = value.reader(); let value = content.next()?.octets(false)?; content.end()?; Ok(value)
}
impl Bags {
    fn read(&mut self, bytes: &[u8], depth: usize) -> Result<()> {
        if depth > 64 { return Err(invalid("PKCS12 safe contents exceed nesting limit")); }
        let mut outer = Reader(bytes); let mut bags = outer.expect(0x30)?.reader(); outer.end()?;
        let mut index = 0;
        while !bags.is_empty() {
            let order = self.next_order; self.next_order += 1;
            let mut bag = bags.expect(0x30)?.reader();
            let oid = bag.expect(6)?.content;
            let mut value = bag.expect(0xa0)?.reader(); let value = value.next_and_end()?;
            let mut attributes = if bag.is_empty() { Attributes::default() } else { attributes(bag.expect(0x31)?)? }; bag.end()?;
            let kind = oid.strip_prefix(BAG_PREFIX).and_then(|suffix| if suffix.len() == 1 { Some(suffix[0]) } else { None });
            match kind {
                Some(2) => {
                    self.private_keys_seen += 1;
                    if attributes.key_id.is_none() {
                        if self.private_keys_seen == 1 { attributes.key_id = Some(b"01".to_vec()); } else { index += 1; continue; }
                    }
                    self.name_entry(&mut attributes);
                    self.keys.push(KeyBag { encrypted: Zeroizing::new(value.encoded.to_vec()), attributes, order });
                }
                Some(3) => {
                    let mut value = value.sequence()?;
                    if value.expect(6)?.content != b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x16\x01" { return Err(invalid("PKCS12 requires X.509 certificates")); }
                    let der = explicit_octets(value.expect(0xa0)?)?.to_vec(); value.end()?;
                    if attributes.key_id.is_none() && self.private_keys_seen == 1 && index == 0 { attributes.key_id = Some(b"01".to_vec()); }
                    let certificate = X509::from_der(&der).map_err(invalid)?;
                    if attributes.trusted { self.name_entry(&mut attributes); }
                    self.certificates.push(CertificateBag { der, certificate, attributes, order });
                }
                Some(6) => self.read(value.encoded, depth + 1)?,
                _ => (),
            }
            index += 1;
        }
        Ok(())
    }
    fn name_entry(&mut self, attributes: &mut Attributes) {
        if attributes.alias.is_none() { self.unfriendly_name += 1; attributes.alias = Some(self.unfriendly_name.to_string()); }
    }
    fn finish(self, password: Option<&str>, private_keys: bool, mut remaining: usize) -> Result<Parts> {
        let mut entries = std::collections::BTreeMap::new();
        for (attributes, order) in self.keys.iter().map(|key| (&key.attributes,key.order)).chain(self.certificates.iter().filter(|certificate| certificate.attributes.trusted).map(|certificate| (&certificate.attributes,certificate.order))) {
            if let Some(alias) = attributes.alias.as_deref() {
                entries.entry(alias).and_modify(|previous: &mut (usize, usize)| { previous.0 = previous.0.min(order); previous.1 = previous.1.max(order); }).or_insert((order, order));
            }
        }
        let current = |attributes: &Attributes, order: usize| attributes.alias.as_deref().is_some_and(|alias| entries.get(alias).is_some_and(|(_, latest)| *latest == order));
        let mut identities = Vec::new();
        let mut trusted = self.certificates.iter().filter(|certificate| certificate.attributes.trusted && current(&certificate.attributes,certificate.order)).map(|certificate| certificate.der.clone()).collect::<Vec<_>>();
        let mut keys = self.keys.iter().filter(|key| current(&key.attributes, key.order)).collect::<Vec<_>>();
        keys.sort_by_key(|key| entries[key.attributes.alias.as_deref().expect("named key entry")].0);
        for key in keys {
            let same_alias = |certificate: &&CertificateBag| key.attributes.alias.as_ref().zip(certificate.attributes.alias.as_ref()).is_some_and(|(a,b)| a == b);
            let same_id = |certificate: &&CertificateBag| key.attributes.key_id.is_some() && key.attributes.key_id == certificate.attributes.key_id;
            let certificate = self.certificates.iter().filter(|certificate| !certificate.attributes.trusted).find(|certificate| same_id(certificate) && same_alias(certificate))
                .or_else(|| self.certificates.iter().filter(|certificate| !certificate.attributes.trusted).rfind(same_id))
                .or_else(|| self.certificates.iter().filter(|certificate| !certificate.attributes.trusted).rfind(same_alias));
            let Some(certificate) = certificate else { continue; };
            if !trusted.contains(&certificate.der) { trusted.push(certificate.der.clone()); }
            if !private_keys { continue; }
            let mut encrypted = Reader(&key.encrypted); let mut encrypted_key = encrypted.expect(0x30)?.reader(); encrypted.end()?;
            let algorithm = encrypted_key.expect(0x30)?; let ciphertext = encrypted_key.expect(4)?.content; encrypted_key.end()?;
            let private_key = decrypt(algorithm, ciphertext, password.ok_or_else(|| invalid("PKCS12 private keys require a password"))?)?;
            let mut chain = Vec::new(); let mut next = Some(certificate);
            while let Some(certificate) = next {
                if chain.contains(&certificate.der) { break; }
                remaining = remaining.checked_sub(certificate.der.len()).ok_or_else(|| invalid("PKCS12 certificate chains exceed the reserved material bound"))?;
                chain.push(certificate.der.clone());
                let issuer = certificate.certificate.issuer_name().to_der().map_err(invalid)?;
                if issuer == certificate.certificate.subject_name().to_der().map_err(invalid)? && certificate.certificate.verify(&certificate.certificate.public_key().map_err(invalid)?).map_err(invalid)? { break; }
                let authority = certificate.certificate.authority_key_id().map(|id| id.as_slice());
                let mut fallback = None; next = None;
                for candidate in &self.certificates {
                    if candidate.certificate.subject_name().to_der().map_err(invalid)? != issuer { continue; }
                    match (authority, candidate.certificate.subject_key_id()) {
                        (Some(authority), Some(subject)) if authority == subject.as_slice() => { next = Some(candidate); break; }
                        (Some(_), Some(_)) => (),
                        _ => fallback = Some(candidate),
                    }
                }
                if next.is_none() { next = fallback; }
            }
            identities.push(Identity::new(key.attributes.alias.as_deref().map(|alias| alias.encode_utf16().collect()), private_key, chain)?);
        }
        Ok(Parts { identities, trusted })
    }
}
fn attributes(value: Value<'_>) -> Result<Attributes> {
    let mut result = Attributes::default(); let mut attributes = value.reader();
    while !attributes.is_empty() {
        let mut attribute = attributes.expect(0x30)?.reader(); let oid = attribute.expect(6)?.content;
        let mut values = attribute.expect(0x31)?.reader(); attribute.end()?;
        match oid {
            b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x14" => {
                let bytes = values.expect(0x1e)?.content;
                if bytes.len() % 2 != 0 { return Err(invalid("Invalid PKCS12 friendly name")); }
                let units = bytes.chunks_exact(2).map(|pair| u16::from_be_bytes([pair[0],pair[1]])).collect::<Vec<_>>();
                result.alias = Some(String::from_utf16(&units).map_err(invalid)?.to_lowercase());
            }
            b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x15" => result.key_id = Some(values.expect(4)?.content.to_vec()),
            b"\x60\x86\x48\x01\x86\xf9\x66\xad\xca\x7b\x01\x01" => { while !values.is_empty() { values.expect(6)?; } result.trusted = true; }
            _ => (),
        }
    }
    Ok(result)
}
fn verify_mac(value: Value<'_>, bytes: &[u8], password: &str) -> Result<()> {
    let mut mac = value.reader(); let mut digest = mac.expect(0x30)?.reader();
    let mut algorithm = digest.expect(0x30)?.reader(); let hash = digest_algorithm(algorithm.expect(6)?.content)?; algorithm.optional_null()?;
    let expected = digest.expect(4)?.content; digest.end()?;
    let salt = mac.expect(4)?.content; let count = if mac.is_empty() { 1 } else { iterations(mac.integer()?)? }; mac.end()?;
    let verify = |empty_legacy| -> Result<bool> {
        let key = derive(hash, password, salt, count, 3, hash.size(), empty_legacy)?;
        let key = PKey::hmac(&key).map_err(invalid)?; let mut signer = Signer::new(hash, &key).map_err(invalid)?;
        signer.update(bytes).map_err(invalid)?; let actual = Zeroizing::new(signer.sign_to_vec().map_err(invalid)?);
        Ok(bool::from(actual.as_slice().ct_eq(expected)))
    };
    if verify(false)? || (password.is_empty() && verify(true)?) { Ok(()) } else { Err(invalid("PKCS12 integrity check failed")) }
}
fn decrypt(algorithm: Value<'_>, ciphertext: &[u8], password: &str) -> Result<Zeroizing<Vec<u8>>> {
    let decrypt = |empty_legacy| decrypt_once(algorithm, ciphertext, password, empty_legacy);
    match decrypt(false) { Err(_) if password.is_empty() => decrypt(true), result => result }
}
fn decrypt_once(algorithm: Value<'_>, ciphertext: &[u8], password: &str, empty_legacy: bool) -> Result<Zeroizing<Vec<u8>>> {
    let mut algorithm = algorithm.reader(); let oid = algorithm.expect(6)?.content; let parameters = algorithm.expect(0x30)?; algorithm.end()?;
    if oid == PBES2 { return decrypt_pbes2(parameters, ciphertext, password); }
    let kind = oid.strip_prefix(PKCS12_PBE).and_then(|suffix| if suffix.len() == 1 { Some(suffix[0]) } else { None }).ok_or_else(|| invalid("Unsupported PKCS12 encryption algorithm"))?;
    let mut parameters = parameters.reader(); let salt = parameters.expect(4)?.content; let count = iterations(parameters.integer()?)?; parameters.end()?;
    let _default = Provider::load(None, "default").map_err(invalid)?;
    let _legacy = if matches!(kind, 1 | 2 | 5 | 6) { Some(Provider::load(None, "legacy").map_err(invalid)?) } else { None };
    let (cipher, length) = match kind {
        1 => (Cipher::rc4(), 16), 2 => (Cipher::rc4(), 5), 3 => (Cipher::des_ede3_cbc(), 24), 4 => (Cipher::des_ede3_cbc(), 24),
        5 => (Cipher::rc2_cbc(), 16), 6 => (Cipher::rc2_40_cbc(), 5), _ => return Err(invalid("Unsupported PKCS12 encryption algorithm")),
    };
    let mut key = derive(MessageDigest::sha1(), password, salt, count, 1, length, empty_legacy)?;
    if kind == 4 { key.copy_within(..8,16); }
    let iv = derive(MessageDigest::sha1(), password, salt, count, 2, cipher.iv_len().unwrap_or(0), empty_legacy)?;
    decrypt_cipher(cipher, &key, &iv, ciphertext)
}
fn decrypt_pbes2(parameters: Value<'_>, ciphertext: &[u8], password: &str) -> Result<Zeroizing<Vec<u8>>> {
    let mut parameters = parameters.reader(); let mut kdf = parameters.expect(0x30)?.reader();
    if kdf.expect(6)?.content != PBKDF2 { return Err(invalid("Unsupported PKCS12 key derivation")); }
    let mut derivation = kdf.expect(0x30)?.reader(); kdf.end()?;
    let salt = derivation.expect(4)?.content; let count = iterations(derivation.integer()?)?;
    let key_length = if derivation.peek() == Some(2) { Some(derivation.integer()?) } else { None };
    let hash = if derivation.is_empty() { MessageDigest::sha1() } else { let mut prf = derivation.expect(0x30)?.reader(); let hash = hmac_algorithm(prf.expect(6)?.content)?; prf.optional_null()?; hash }; derivation.end()?;
    let mut encryption = parameters.expect(0x30)?.reader(); parameters.end()?;
    let cipher = match encryption.expect(6)?.content {
        b"\x60\x86\x48\x01\x65\x03\x04\x01\x02" => Cipher::aes_128_cbc(),
        b"\x60\x86\x48\x01\x65\x03\x04\x01\x16" => Cipher::aes_192_cbc(),
        b"\x60\x86\x48\x01\x65\x03\x04\x01\x2a" => Cipher::aes_256_cbc(),
        b"\x2a\x86\x48\x86\xf7\x0d\x03\x07" => Cipher::des_ede3_cbc(),
        _ => return Err(invalid("Unsupported PKCS12 PBES2 cipher")),
    };
    let iv = encryption.expect(4)?.content; encryption.end()?;
    if key_length.is_some_and(|length| length != cipher.key_len()) { return Err(invalid("PKCS12 derived-key length differs from cipher")); }
    if password.len() > i32::MAX as usize || salt.len() > i32::MAX as usize { return Err(invalid("PKCS12 PBKDF2 input exceeds native limit")); }
    let mut key = Zeroizing::new(vec![0; cipher.key_len()]);
    openssl::pkcs5::pbkdf2_hmac(password.as_bytes(), salt, count, hash, &mut key).map_err(invalid)?;
    decrypt_cipher(cipher, &key, iv, ciphertext)
}
fn decrypt_cipher(cipher: Cipher, key: &[u8], iv: &[u8], ciphertext: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    if cipher.iv_len().unwrap_or(0) != iv.len() { return Err(invalid("PKCS12 cipher IV length is invalid")); }
    let mut cipher_state = Crypter::new(cipher, Mode::Decrypt, key, if iv.is_empty() { None } else { Some(iv) }).map_err(invalid)?;
    let size = ciphertext.len().checked_add(cipher.block_size()).ok_or_else(|| invalid("PKCS12 plaintext size exceeds range"))?;
    let mut output = Zeroizing::new(vec![0; size]); let count = cipher_state.update(ciphertext, &mut output).map_err(invalid)?;
    let final_count = cipher_state.finalize(&mut output[count..]).map_err(invalid)?; output[count + final_count..].zeroize(); output.truncate(count + final_count); Ok(output)
}
fn iterations(count: usize) -> Result<usize> { if count == 0 || count > MAXIMUM_ITERATIONS { Err(invalid("PKCS12 iteration count is outside the Java limit")) } else { Ok(count) } }
fn digest_algorithm(oid: &[u8]) -> Result<MessageDigest> {
    Ok(match oid {
        b"\x2b\x0e\x03\x02\x1a" => MessageDigest::sha1(),
        b"\x60\x86\x48\x01\x65\x03\x04\x02\x04" => MessageDigest::sha224(),
        b"\x60\x86\x48\x01\x65\x03\x04\x02\x01" => MessageDigest::sha256(),
        b"\x60\x86\x48\x01\x65\x03\x04\x02\x02" => MessageDigest::sha384(),
        b"\x60\x86\x48\x01\x65\x03\x04\x02\x03" => MessageDigest::sha512(),
        _ => return Err(invalid("Unsupported PKCS12 MAC digest")),
    })
}
fn hmac_algorithm(oid: &[u8]) -> Result<MessageDigest> {
    let suffix = oid.strip_prefix(b"\x2a\x86\x48\x86\xf7\x0d\x02");
    Ok(match suffix { Some([7]) => MessageDigest::sha1(), Some([8]) => MessageDigest::sha224(), Some([9]) => MessageDigest::sha256(), Some([10]) => MessageDigest::sha384(), Some([11]) => MessageDigest::sha512(), _ => return Err(invalid("Unsupported PKCS12 PBKDF2 digest")) })
}
// RFC 7292 Appendix B: https://www.rfc-editor.org/rfc/rfc7292#appendix-B.2
fn derive(hash: MessageDigest, password: &str, salt: &[u8], iterations: usize, purpose: u8, length: usize, empty_legacy: bool) -> Result<Zeroizing<Vec<u8>>> {
    let capacity = password.len().checked_mul(2).and_then(|n| n.checked_add(2)).ok_or_else(|| invalid("PKCS12 password exceeds range"))?;
    let mut password_bytes = Zeroizing::new(Vec::with_capacity(capacity));
    password_bytes.extend(password.encode_utf16().flat_map(u16::to_be_bytes));
    if (password.is_empty() && empty_legacy) || password == "\0" { password_bytes[..].zeroize(); password_bytes.clear(); } else { password_bytes.extend_from_slice(&[0,0]); }
    let block = hash.block_size(); let mut input = Zeroizing::new(Vec::new());
    for bytes in [salt, password_bytes.as_slice()] {
        if bytes.is_empty() { continue; }
        let padded = bytes.len().checked_add(block - 1).map(|n| n / block * block).ok_or_else(|| invalid("PKCS12 derivation input exceeds range"))?;
        input.extend(bytes.iter().copied().cycle().take(padded));
    }
    let diversifier = vec![purpose; block]; let mut output = Zeroizing::new(Vec::with_capacity(length));
    while output.len() < length {
        let mut state = Hasher::new(hash).map_err(invalid)?; state.update(&diversifier).map_err(invalid)?; state.update(&input).map_err(invalid)?;
        let mut digest = state.finish().map_err(invalid)?;
        for _ in 1..iterations {
            let mut state = Hasher::new(hash).map_err(invalid)?; state.update(&digest).map_err(invalid)?;
            digest[..].zeroize(); digest = state.finish().map_err(invalid)?;
        }
        output.extend_from_slice(&digest[..(length - output.len()).min(digest.len())]);
        for chunk in input.chunks_exact_mut(block) {
            let mut carry = 1u16;
            for (index, byte) in chunk.iter_mut().enumerate().rev() {
                let sum = u16::from(*byte) + u16::from(digest[index % digest.len()]) + carry;
                *byte = sum as u8; carry = sum >> 8;
            }
        }
        digest[..].zeroize();
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn hex(value: &str) -> Vec<u8> { value.as_bytes().chunks_exact(2).map(|bytes| u8::from_str_radix(std::str::from_utf8(bytes).unwrap(),16).unwrap()).collect() }
    #[test]
    fn published_pkcs12_sha1_key_iv_and_mac_vectors() {
        for (password, salt, iterations, purpose, expected) in [
            ("smeg", "0A58CF64530D823F", 1, 1, "8AAAE6297B6CB04642AB5B077851284EB7128F1A2A7FBCA3"),
            ("smeg", "0A58CF64530D823F", 1, 2, "79993DFE048D3B76"),
            ("smeg", "3D83C0E4546AC140", 1, 3, "8D967D88F6CAA9D714800AB3D48051D63F73A312"),
            ("queeg", "05DEC959ACFF72F7", 1000, 1, "ED2034E36328830FF09DF1E1A07DD357185DAC0D4F9EB3D4"),
            ("queeg", "05DEC959ACFF72F7", 1000, 2, "11DEDAD7758D4860"),
            ("queeg", "263216FCC2FAB31C", 1000, 3, "5EC4C7A80DF652294C3925B6489A7AB857C83476"),
        ] {
            let expected = hex(expected);
            assert_eq!(&*derive(MessageDigest::sha1(), password, &hex(salt), iterations, purpose, expected.len(), false).unwrap(), &expected);
        }
    }
    fn der(tag: u8, bytes: &[u8]) -> Vec<u8> {
        let mut result = vec![tag];
        if bytes.len() < 128 { result.push(bytes.len() as u8); }
        else { let length = bytes.len().to_be_bytes(); let first = length.iter().position(|byte| *byte != 0).unwrap(); result.push(0x80 | (length.len() - first) as u8); result.extend_from_slice(&length[first..]); }
        result.extend_from_slice(bytes); result
    }
    fn sequence(values: &[Vec<u8>]) -> Vec<u8> { der(0x30, &values.concat()) }
    fn safe_bag(kind: u8, value: Vec<u8>, id: u8, alias: Option<&str>) -> Vec<u8> {
        let mut oid = BAG_PREFIX.to_vec(); oid.push(kind);
        let mut attribute = sequence(&[der(6,b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x15"),der(0x31,&der(4,&[id]))]);
        if let Some(alias) = alias {
            let name = alias.encode_utf16().flat_map(u16::to_be_bytes).collect::<Vec<_>>();
            attribute.extend_from_slice(&sequence(&[der(6,b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x14"),der(0x31,&der(0x1e,&name))]));
        }
        sequence(&[der(6,&oid),der(0xa0,&value),der(0x31,&attribute)])
    }
    fn store(duplicate_alias: bool) -> Vec<u8> {
        let key = PKey::private_key_from_pem(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.key")).unwrap();
        let certificate = X509::from_pem(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.crt")).unwrap().to_der().unwrap();
        let certificate = sequence(&[der(6,b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x16\x01"),der(0xa0,&der(4,&certificate))]);
        let encrypted = key.private_key_to_pkcs8_passphrase(Cipher::aes_256_cbc(),b"key-password").unwrap();
        let first_key = if duplicate_alias { key.private_key_to_pkcs8_passphrase(Cipher::aes_256_cbc(),b"ignored-key-password").unwrap() } else { encrypted.clone() };
        let bags = sequence(&[safe_bag(2,first_key,1,duplicate_alias.then_some("SHARED")),safe_bag(2,encrypted,2,duplicate_alias.then_some("shared")),safe_bag(3,certificate.clone(),1,None),safe_bag(3,certificate,2,None)]);
        let authenticated = sequence(&[sequence(&[der(6,DATA),der(0xa0,&der(4,&bags))])]);
        let salt = b"eightbyt";
        let key = derive(MessageDigest::sha256(),"store-password",salt,2048,3,32,false).unwrap();
        let key = PKey::hmac(&key).unwrap(); let mut signer = Signer::new(MessageDigest::sha256(),&key).unwrap(); signer.update(&authenticated).unwrap();
        let digest = sequence(&[sequence(&[der(6,b"\x60\x86\x48\x01\x65\x03\x04\x02\x01"),der(5,&[])]),der(4,&signer.sign_to_vec().unwrap())]);
        sequence(&[der(2,&[3]),sequence(&[der(6,DATA),der(0xa0,&der(4,&authenticated))]),sequence(&[digest,der(4,salt),der(2,&[8,0])])])
    }
    #[test]
    fn multiple_private_keys_use_a_separate_password_and_store_mac() {
        let store = store(false);
        let decoded = decode(&store,Some("store-password"),Some("key-password"),true).unwrap();
        assert_eq!(decoded.identities.len(),2);
        assert!(decode(&store,Some("wrong-store"),Some("key-password"),true).is_err());
        assert!(decode(&store,Some("store-password"),Some("wrong-key"),true).is_err());
        assert_eq!(decode(&store,Some("store-password"),None,false).unwrap().trusted.len(),1);
        assert_eq!(decode(&store,None,None,false).unwrap().trusted.len(),1);
        let mut damaged = store; let last = damaged.len()-1; damaged[last] ^= 1;
        assert!(decode(&damaged,Some("store-password"),Some("key-password"),true).is_err());
    }
    #[test]
    fn duplicate_aliases_replace_earlier_keys_case_insensitively() {
        let decoded = decode(&store(true),Some("store-password"),Some("key-password"),true).unwrap();
        assert_eq!(decoded.identities.len(),1);
        assert_eq!(decoded.identities[0].alias(), Some("shared".encode_utf16().collect::<Vec<_>>().as_slice()));
    }
    #[test]
    fn pkcs12_replacement_retains_the_original_alias_enumeration_position() {
        let key = PKey::private_key_from_pem(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.key")).unwrap();
        let certificate = X509::from_pem(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.crt")).unwrap().to_der().unwrap();
        let certificate = sequence(&[der(6,b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x16\x01"),der(0xa0,&der(4,&certificate))]);
        let encrypted = key.private_key_to_pkcs8_passphrase(Cipher::aes_256_cbc(),b"key-password").unwrap();
        let bags = sequence(&[
            safe_bag(2, encrypted.clone(), 1, Some("first")),
            safe_bag(2, encrypted.clone(), 2, Some("second")),
            safe_bag(2, encrypted, 3, Some("FIRST")),
            safe_bag(3, certificate.clone(), 1, None),
            safe_bag(3, certificate.clone(), 2, None),
            safe_bag(3, certificate, 3, None),
        ]);
        let authenticated = sequence(&[sequence(&[der(6,DATA),der(0xa0,&der(4,&bags))])]);
        let store = sequence(&[der(2,&[3]),sequence(&[der(6,DATA),der(0xa0,&der(4,&authenticated))])]);
        let decoded = decode(&store, None, Some("key-password"), true).unwrap();
        let aliases = decoded.identities.iter().map(|identity| String::from_utf16(identity.alias().unwrap()).unwrap()).collect::<Vec<_>>();
        assert_eq!(aliases, ["first", "second"]);
    }
    #[test]
    fn iteration_limits_are_enforced_before_derivation() {
        assert!(iterations(0).is_err()); assert!(iterations(MAXIMUM_ITERATIONS + 1).is_err()); assert_eq!(iterations(MAXIMUM_ITERATIONS).unwrap(),MAXIMUM_ITERATIONS);
    }
}
