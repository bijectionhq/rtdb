//! AES-256-GCM under a deployment's key, shared by its objects and its log.
//! Each sealed value is a random nonce, the ciphertext and the tag, bound to an associated name so a value cannot be passed off as another's.

use bytes::Bytes;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use rtdb_storage::Error;
use rtdb_types::{Anomaly, Result};

pub const SEALED_OVERHEAD: usize = NONCE_LEN + 16;

pub struct Cipher { key: LessSafeKey }

impl Cipher {
    pub fn new(key: &[u8]) -> Result<Self> {
        let key = UnboundKey::new(&AES_256_GCM, key).map_err(|_| Anomaly::fault("rtdb.multi/key", "A data key has the wrong length"))?;
        Ok(Self { key: LessSafeKey::new(key) })
    }

    pub fn seal(&self, associated: &str, plaintext: &[u8]) -> rtdb_storage::Result<Bytes> {
        let mut nonce = [0u8; NONCE_LEN];
        SystemRandom::new().fill(&mut nonce).map_err(|_| Error::Task("no randomness for a nonce".into()))?;
        let mut sealed = Vec::with_capacity(plaintext.len() + SEALED_OVERHEAD);
        sealed.extend_from_slice(&nonce);
        sealed.extend_from_slice(plaintext);
        let tag = self.key.seal_in_place_separate_tag(Nonce::assume_unique_for_key(nonce), Aad::from(associated.as_bytes()), &mut sealed[NONCE_LEN..])
            .map_err(|_| Error::Task("encryption failed".into()))?;
        sealed.extend_from_slice(tag.as_ref());
        Ok(Bytes::from(sealed))
    }

    pub fn open(&self, associated: &str, sealed: &[u8]) -> rtdb_storage::Result<Bytes> {
        if sealed.len() < SEALED_OVERHEAD { return Err(Error::Task(format!("{associated}: too short to be sealed"))); }
        let nonce = Nonce::try_assume_unique_for_key(&sealed[..NONCE_LEN]).map_err(|_| Error::Task("malformed nonce".into()))?;
        let mut body = sealed[NONCE_LEN..].to_vec();
        let length = self.key.open_in_place(nonce, Aad::from(associated.as_bytes()), &mut body)
            .map_err(|_| Error::Task(format!("{associated} cannot be decrypted under this deployment's key")))?.len();
        body.truncate(length);
        Ok(Bytes::from(body))
    }
}
