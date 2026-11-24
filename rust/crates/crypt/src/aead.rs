use ring::{
    aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey},
    hkdf,
    rand::{SecureRandom, SystemRandom},
};
use rtdb_types::{Anomaly, Result};
use zeroize::Zeroizing;

use super::{DataKey, invalid_envelope};

const SALT_LEN: usize = 32;

pub(super) fn random_key() -> Result<DataKey> {
    let mut key = DataKey::new([0; 32]);
    random(&mut *key)?;
    Ok(key)
}

fn random(bytes: &mut [u8]) -> Result<()> {
    SystemRandom::new()
        .fill(bytes)
        .map_err(|_| Anomaly::fault("rtdb.crypt/randomness", "System randomness unavailable"))
}

pub(super) fn seal(key: &DataKey, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
    let mut salt = [0; SALT_LEN];
    random(&mut salt)?;
    let key = one_time_key(key, &salt)?;
    let mut ciphertext = plaintext.to_vec();
    key.seal_in_place_append_tag(nonce(), Aad::from(aad), &mut ciphertext)
        .map_err(|_| invalid_envelope())?;
    let mut bytes = Vec::with_capacity(SALT_LEN + ciphertext.len());
    bytes.extend_from_slice(&salt);
    bytes.extend_from_slice(&ciphertext);
    Ok(bytes)
}

pub(super) fn open(key: &DataKey, aad: &[u8], envelope: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    let (salt, ciphertext) = envelope
        .split_first_chunk::<SALT_LEN>()
        .ok_or_else(invalid_envelope)?;
    let key = one_time_key(key, salt)?;
    let mut bytes = Zeroizing::new(ciphertext.to_vec());
    let length = key
        .open_in_place(nonce(), Aad::from(aad), &mut bytes)
        .map_err(|_| invalid_envelope())?
        .len();
    bytes.truncate(length);
    Ok(bytes)
}

fn one_time_key(key: &DataKey, salt: &[u8; SALT_LEN]) -> Result<LessSafeKey> {
    let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, salt);
    let prk = salt.extract(&**key);
    let info = [b"rtdb/v1/aes-256-gcm".as_slice()];
    let okm = prk
        .expand(&info, &AES_256_GCM)
        .map_err(|_| invalid_envelope())?;
    let mut derived = DataKey::new([0; 32]);
    okm.fill(&mut *derived).map_err(|_| invalid_envelope())?;
    let key = UnboundKey::new(&AES_256_GCM, &*derived).map_err(|_| invalid_envelope())?;
    Ok(LessSafeKey::new(key))
}

fn nonce() -> Nonce {
    Nonce::assume_unique_for_key([0; NONCE_LEN])
}
