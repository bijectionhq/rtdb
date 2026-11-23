//! AES-256-GCM under a deployment's key, shared by its objects and its log.
//! Each sealed value is a nonce, the ciphertext and the tag, bound to an associated name so a value cannot be passed off as another's.
//! A log payload's nonce is random; an object's is derived from its name and contents, so the same object sealed twice is the same bytes, and put-if-absent takes a second upload of it, by a deposed holder and its successor say, as the first.

#[path = "object.rs"]
pub mod object;

use crate::Error;
use bytes::Bytes;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use rtdb_types::{Anomaly, Result};

pub const SEALED_OVERHEAD: usize = NONCE_LEN + 16;

pub struct Cipher {
    key: LessSafeKey,
    nonces: ring::hmac::Key,
}

impl Cipher {
    pub fn new(key: &[u8]) -> Result<Self> {
        let nonces = ring::hmac::Key::new(
            ring::hmac::HMAC_SHA256,
            ring::hmac::sign(
                &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key),
                b"rtdb.multi/object-nonces",
            )
            .as_ref(),
        );
        let key = UnboundKey::new(&AES_256_GCM, key)
            .map_err(|_| Anomaly::fault("rtdb.multi/key", "A data key has the wrong length"))?;
        Ok(Self {
            key: LessSafeKey::new(key),
            nonces,
        })
    }

    pub fn seal(&self, associated: &str, plaintext: &[u8]) -> crate::Result<Bytes> {
        let mut nonce = [0u8; NONCE_LEN];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| Error::Task("no randomness for a nonce".into()))?;
        self.seal_with(nonce, associated, plaintext)
    }

    /// Seals with a nonce derived from `associated` and `plaintext`: the same pair always seals to the same bytes, and two different ones never share a nonce.
    pub fn seal_deterministic(&self, associated: &str, plaintext: &[u8]) -> crate::Result<Bytes> {
        let mut sealed = Vec::with_capacity(plaintext.len() + SEALED_OVERHEAD);
        self.append_deterministic(associated, plaintext, &mut sealed)?;
        Ok(Bytes::from(sealed))
    }

    pub(super) fn append_deterministic(&self, associated: &str, plaintext: &[u8], sealed: &mut Vec<u8>) -> crate::Result<()> {
        let mut context = ring::hmac::Context::with_key(&self.nonces);
        context.update(&(associated.len() as u64).to_be_bytes());
        context.update(associated.as_bytes());
        context.update(plaintext);
        let mut nonce = [0u8; NONCE_LEN];
        nonce.copy_from_slice(&context.sign().as_ref()[..NONCE_LEN]);
        self.append_sealed(nonce, associated, plaintext, sealed)
    }

    fn seal_with(&self, nonce: [u8; NONCE_LEN], associated: &str, plaintext: &[u8]) -> crate::Result<Bytes> {
        let mut sealed = Vec::with_capacity(plaintext.len() + SEALED_OVERHEAD);
        self.append_sealed(nonce, associated, plaintext, &mut sealed)?;
        Ok(Bytes::from(sealed))
    }

    fn append_sealed(&self, nonce: [u8; NONCE_LEN], associated: &str, plaintext: &[u8], sealed: &mut Vec<u8>) -> crate::Result<()> {
        sealed.extend_from_slice(&nonce);
        let start = sealed.len();
        sealed.extend_from_slice(plaintext);
        let tag = self.key.seal_in_place_separate_tag(Nonce::assume_unique_for_key(nonce), Aad::from(associated.as_bytes()), &mut sealed[start..])
            .map_err(|_| Error::Task("encryption failed".into()))?;
        sealed.extend_from_slice(tag.as_ref());
        Ok(())
    }

    pub fn open(&self, associated: &str, sealed: &[u8]) -> crate::Result<Bytes> {
        if sealed.len() < SEALED_OVERHEAD {
            return Err(Error::Task(format!("{associated}: too short to be sealed")));
        }
        let nonce = Nonce::try_assume_unique_for_key(&sealed[..NONCE_LEN])
            .map_err(|_| Error::Task("malformed nonce".into()))?;
        let mut body = sealed[NONCE_LEN..].to_vec();
        let length = self
            .key
            .open_in_place(nonce, Aad::from(associated.as_bytes()), &mut body)
            .map_err(|_| {
                Error::Task(format!(
                    "{associated} cannot be decrypted under this deployment's key"
                ))
            })?
            .len();
        body.truncate(length);
        Ok(Bytes::from(body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_object_seals_to_the_same_bytes_every_time_and_a_log_payload_never_does() {
        let cipher = Cipher::new(&[3; 32]).unwrap();
        let object = cipher
            .seal_deterministic("blocks/b00.binpb", b"catalog")
            .unwrap();
        assert_eq!(
            object,
            cipher
                .seal_deterministic("blocks/b00.binpb", b"catalog")
                .unwrap()
        );
        assert_ne!(
            object,
            cipher
                .seal_deterministic("blocks/b01.binpb", b"catalog")
                .unwrap(),
            "another name is another nonce"
        );
        assert_ne!(
            object,
            cipher
                .seal_deterministic("blocks/b00.binpb", b"other")
                .unwrap(),
            "and so are other contents"
        );
        assert_eq!(
            cipher.open("blocks/b00.binpb", &object).unwrap().as_ref(),
            b"catalog"
        );
        assert_ne!(
            cipher.seal("stream", b"payload").unwrap(),
            cipher.seal("stream", b"payload").unwrap()
        );
    }
}
