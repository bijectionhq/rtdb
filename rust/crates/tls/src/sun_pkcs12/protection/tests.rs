use super::*;
use crate::java_der::encoded;
use rtdb_columnar::MemoryBudget;

fn seq(parts: &[Vec<u8>]) -> Vec<u8> { encoded(0x30, &parts.concat()) }
fn hex(value: &str) -> Vec<u8> { value.as_bytes().chunks_exact(2).map(|bytes| u8::from_str_radix(std::str::from_utf8(bytes).unwrap(), 16).unwrap()).collect() }
fn algorithm(oid: &[u8], parameters: &[u8]) -> Vec<u8> { seq(&[encoded(6, oid), parameters.to_vec()]) }
fn parse(bytes: &[u8]) -> Result<Protection> { Protection::parse(next(&mut Der(bytes))?) }
fn legacy(kind: u8, salt: &[u8], count: i32) -> Vec<u8> {
    let mut oid = PKCS12_PBE.to_vec(); oid.push(kind);
    algorithm(&oid, &seq(&[encoded(4, salt), encoded(2, &count.to_be_bytes())]))
}
fn pbes2(cipher: u8, count: i32, salt: &[u8], key_length: Option<i32>, prf: u8, null: &[u8]) -> Vec<u8> {
    let mut derivation = vec![encoded(4, salt), encoded(2, &count.to_be_bytes())];
    if let Some(key_length) = key_length { derivation.push(encoded(2, &key_length.to_be_bytes())); }
    let mut oid = b"\x2a\x86\x48\x86\xf7\x0d\x02".to_vec(); oid.push(prf);
    derivation.push(seq(&[encoded(6, &oid), encoded(5, null)]));
    let mut oid = b"\x60\x86\x48\x01\x65\x03\x04\x01".to_vec(); oid.push(cipher);
    algorithm(PBES2, &seq(&[seq(&[encoded(6, PBKDF2), seq(&derivation)]), seq(&[encoded(6, &oid), encoded(4, &[0x43; 16])])]))
}

#[test]
fn sun_legacy_ciphers_decrypt_with_published_pkcs12_key_and_iv_vectors() {
    let _default = Provider::load(None, "default").unwrap(); let _legacy = Provider::load(None, "legacy").unwrap();
    let salt = hex("0A58CF64530D823F"); let key = hex("8AAAE6297B6CB04642AB5B077851284EB7128F1A2A7FBCA3"); let iv = hex("79993DFE048D3B76");
    let plaintext = b"SunJCE legacy PKCS12 fixture";
    for (kind, cipher, length) in [(1, Cipher::rc4(), 16), (2, Cipher::rc4(), 5), (3, Cipher::des_ede3_cbc(), 24), (5, Cipher::rc2_cbc(), 16), (6, Cipher::rc2_40_cbc(), 5)] {
        let iv = cipher.iv_len().map(|_| iv.as_slice());
        let ciphertext = openssl::symm::encrypt(cipher, &key[..length], iv, plaintext).unwrap();
        assert_eq!(parse(&legacy(kind, &salt, 1)).unwrap().decrypt(&ciphertext, "smeg").unwrap().as_slice(), plaintext);
    }
    assert!(parse(&legacy(4, &salt, 1)).is_err());
    assert!(parse(&legacy(3, b"short", 1)).is_err());
    for count in [-1, 0, 5_000_001] { assert!(parse(&legacy(3, &salt, count)).is_err()); }
}

#[test]
fn sun_pbes2_cipher_size_overwrites_key_length_and_zero_uses_provider_default() {
    let plaintext = b"PBES2 plaintext"; let salt = b"eightbyt";
    for (encoded_count, actual_count) in [(0, 4096), (2, 2)] {
        for (cipher_id, cipher) in [(2, Cipher::aes_128_cbc()), (42, Cipher::aes_256_cbc())] {
            let mut key = vec![0; cipher.key_len()];
            openssl::pkcs5::pbkdf2_hmac("é".as_bytes(), salt, actual_count, MessageDigest::sha256(), &mut key).unwrap();
            let ciphertext = openssl::symm::encrypt(cipher, &key, Some(&[0x43; 16]), plaintext).unwrap();
            for length in [None, Some(-1), Some(1), Some(1024)] {
                let protection = parse(&pbes2(cipher_id, encoded_count, salt, length, 9, &[0x51])).unwrap();
                assert_eq!(protection.decrypt(&ciphertext, "é").unwrap().as_slice(), plaintext);
            }
        }
    }
    assert!(parse(&pbes2(22, 2, salt, None, 9, &[])).is_err());
    assert!(parse(&pbes2(2, -1, salt, None, 9, &[])).is_err());
    assert!(parse(&pbes2(2, 5_000_001, salt, None, 9, &[])).is_err());
    assert!(parse(&pbes2(2, 2, b"short", None, 9, &[])).is_err());
}

#[test]
fn sun_pbes2_admits_truncated_sha512_prfs_and_legacy_nested_parameters() {
    for (prf, hash) in [(12, "SHA512-224"), (13, "SHA512-256")] {
        let encoded_algorithm = pbes2(2, 2, b"eightbyt", None, prf, &[]);
        let fields = expect(&mut Der(&encoded_algorithm), 0x30).unwrap(); let mut fields = Der(&fields); oid(&mut fields).unwrap();
        let parameters = next(&mut fields).unwrap();
        let legacy = algorithm(PBES2, &seq(&[encoded(6, PBES2), encoded(parameters.tag, &parameters.content)]));
        let mut key = [0; 16];
        openssl::pkcs5::pbkdf2_hmac(b"password", b"eightbyt", 2, MessageDigest::from_name(hash).unwrap(), &mut key).unwrap();
        let ciphertext = openssl::symm::encrypt(Cipher::aes_128_cbc(), &key, Some(&[0x43; 16]), b"PBES2").unwrap();
        for encoded in [&encoded_algorithm, &legacy] { assert_eq!(parse(encoded).unwrap().decrypt(&ciphertext, "password").unwrap().as_slice(), b"PBES2"); }
    }
}

#[test]
fn sun_md5_des_uses_utf8_and_requires_exactly_eight_salt_bytes() {
    let _default = Provider::load(None, "default").unwrap(); let _legacy = Provider::load(None, "legacy").unwrap();
    let digest = openssl::hash::hash(MessageDigest::md5(), &["é".as_bytes(), b"eightbyt"].concat()).unwrap();
    let ciphertext = openssl::symm::encrypt(Cipher::des_cbc(), &digest[..8], Some(&digest[8..]), b"MD5 DES plaintext").unwrap();
    let parameters = seq(&[encoded(4, b"eightbyt"), encoded(2, &[1])]);
    assert_eq!(parse(&algorithm(MD5_DES, &parameters)).unwrap().decrypt(&ciphertext, "é").unwrap().as_slice(), b"MD5 DES plaintext");
    assert!(parse(&algorithm(MD5_DES, &seq(&[encoded(4, b"ninebytes"), encoded(2, &[1])]))).is_err());
}

#[test]
fn sun_legacy_password_conversion_differs_from_mac_unicode_and_preserves_zero_retry() {
    assert_eq!(password_bytes("é", true).unwrap().as_slice(), &[0, 0x43, 0, 0x29, 0, 0]);
    assert_eq!(password_bytes("é", false).unwrap().as_slice(), &[0, 0xe9, 0, 0]);
    assert_eq!(password_bytes("", true).unwrap().as_slice(), &[0, 0]);
    assert!(password_bytes("\0", true).unwrap().is_empty()); assert!(password_bytes("\0", false).unwrap().is_empty());
    let _default = Provider::load(None, "default").unwrap(); let _legacy = Provider::load(None, "legacy").unwrap();
    let key = derive(MessageDigest::sha1(), &[0, 0x43, 0, 0x29, 0, 0], b"eightbyt", 2, 1, 16).unwrap();
    let ciphertext = openssl::symm::encrypt(Cipher::rc4(), &key, None, b"legacy unicode").unwrap();
    assert_eq!(parse(&legacy(1, b"eightbyt", 2)).unwrap().decrypt(&ciphertext, "é").unwrap().as_slice(), b"legacy unicode");
}

#[test]
fn sun_empty_retry_repeats_secret_parsing_after_successful_stream_decryption() {
    let _default = Provider::load(None, "default").unwrap(); let _legacy = Provider::load(None, "legacy").unwrap();
    let plaintext = seq(&[encoded(2, &[0]), seq(&[encoded(6, &[0x2a, 3])]), encoded(4, &[7])]);
    let key = derive(MessageDigest::sha1(), &[], b"eightbyt", 2, 1, 16).unwrap();
    let ciphertext = openssl::symm::encrypt(Cipher::rc4(), &key, None, &plaintext).unwrap();
    let algorithm = legacy(1, b"eightbyt", 2); let protection = parse(&algorithm).unwrap();
    let first = protection.decrypt(&ciphertext, "").unwrap(); assert!(secret_key(&first).is_err());
    assert!(secret_key(&protection.decrypt(&ciphertext, "\0").unwrap()).is_ok());
    let protected = seq(&[algorithm, encoded(4, &ciphertext)]);
    let bytes = crate::sun_pkcs12::tests::pfx(&[crate::sun_pkcs12::tests::secret_bag(&protected, "secret")]);
    let memory = MemoryBudget::new(32 << 20);
    assert!(crate::sun_pkcs12::tests::load(&bytes, Some(""), &memory).unwrap().identities().is_empty()); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_mac_uses_published_key_material_and_constructed_mac_octets() {
    let key = PKey::hmac(&hex("8D967D88F6CAA9D714800AB3D48051D63F73A312")).unwrap();
    let mut signer = Signer::new(MessageDigest::sha1(), &key).unwrap(); signer.update(b"authenticated safe").unwrap();
    let digest = signer.sign_to_vec().unwrap();
    let mac = seq(&[seq(&[seq(&[encoded(6, &[0x2b, 0x0e, 3, 2, 0x1a]), encoded(5, &[])]), encoded(0x24, &encoded(4, &digest))]), encoded(0x24, &encoded(4, &hex("3D83C0E4546AC140")))]);
    assert!(verify_mac(next(&mut Der(&mac)).unwrap(), b"authenticated safe", "smeg").is_ok());
    assert!(verify_mac(next(&mut Der(&mac)).unwrap(), b"changed safe", "smeg").is_err());
    assert!(verify_mac(next(&mut Der(&mac)).unwrap(), b"authenticated safe", "wrong").is_err());
}

#[test]
fn sun_secret_key_ignores_inner_parameters_and_tail_but_rejects_outer_tail() {
    let content = [encoded(2, &[0xff]), seq(&[encoded(6, &[0x2a, 3]), encoded(5, &[7])]), encoded(4, &[7]), vec![0xff]].concat();
    let encoded = encoded(0x31, &content); assert!(secret_key(&encoded).is_ok());
    assert!(secret_key(&[encoded, vec![0]].concat()).is_err());
    for oid in [MD5_DES.to_vec(), [PKCS12_PBE, &[1]].concat(), [PKCS12_PBE, &[4]].concat()] {
        let encoded = seq(&[crate::java_der::encoded(2, &[0]), seq(&[crate::java_der::encoded(6, &oid)]), crate::java_der::encoded(4, &[0xff])]);
        assert!(secret_key(&encoded).is_ok());
    }
}

#[test]
fn sun_empty_retry_does_not_repeat_memory_admission_failure() {
    let memory = MemoryBudget::new(0); let mut calls = 0;
    let result = retry(Some(""), |_| { calls += 1; memory.reserve(1).map_err(TlsError::Other) });
    assert_eq!(result.err().unwrap().code(), "rtdb/memory-budget-exceeded"); assert_eq!(calls, 1);
}
