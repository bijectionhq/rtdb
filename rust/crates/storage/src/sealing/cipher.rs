//! AES-256-GCM under a deployment's key, shared by its objects and its log.
//! Each sealed value is a nonce, the ciphertext and the tag, bound to an associated name so a value cannot be passed off as another's.
//! A log payload's nonce is random; an object's is derived from its name and contents, so the same object sealed twice is the same bytes, and put-if-absent takes a second upload of it, by a deposed holder and its successor say, as the first.

#[path = "object.rs"]
pub mod object;

use crate::Error;
use bytes::Bytes;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, Tag, UnboundKey};
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
        self.seal_prefixed(associated, &[], plaintext)
    }

    /// Prepends bytes to the returned envelope. Bind any authenticated prefix fields through `associated`.
    pub fn seal_prefixed(
        &self,
        associated: &str,
        prefix: &[u8],
        plaintext: &[u8],
    ) -> crate::Result<Bytes> {
        let mut nonce = [0u8; NONCE_LEN];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| Error::Task("no randomness for a nonce".into()))?;
        self.seal_with(nonce, associated, prefix, plaintext)
    }

    /// Seals with a nonce derived from `associated` and `plaintext`: the same pair always seals to the same bytes, and two different ones never share a nonce.
    pub fn seal_deterministic(&self, associated: &str, plaintext: &[u8]) -> crate::Result<Bytes> {
        self.seal_deterministic_prefixed(associated, &[], plaintext)
    }

    /// Uses the deterministic nonce of the envelope and prepends bytes as in [Self::seal_prefixed].
    pub fn seal_deterministic_prefixed(
        &self,
        associated: &str,
        prefix: &[u8],
        plaintext: &[u8],
    ) -> crate::Result<Bytes> {
        self.seal_with(
            self.deterministic_nonce(associated, plaintext),
            associated,
            prefix,
            plaintext,
        )
    }

    fn deterministic_nonce(&self, associated: &str, plaintext: &[u8]) -> [u8; NONCE_LEN] {
        let mut context = ring::hmac::Context::with_key(&self.nonces);
        context.update(&(associated.len() as u64).to_be_bytes());
        context.update(associated.as_bytes());
        context.update(plaintext);
        let mut nonce = [0u8; NONCE_LEN];
        nonce.copy_from_slice(&context.sign().as_ref()[..NONCE_LEN]);
        nonce
    }

    fn seal_with(
        &self,
        nonce: [u8; NONCE_LEN],
        associated: &str,
        prefix: &[u8],
        plaintext: &[u8],
    ) -> crate::Result<Bytes> {
        let mut sealed = Vec::with_capacity(prefix.len() + plaintext.len() + SEALED_OVERHEAD);
        sealed.extend_from_slice(prefix);
        self.append_sealed(nonce, associated, plaintext, &mut sealed)?;
        Ok(sealed.into())
    }

    pub(super) fn append_deterministic(
        &self,
        associated: &str,
        plaintext: &[u8],
        sealed: &mut Vec<u8>,
    ) -> crate::Result<()> {
        self.append_sealed(
            self.deterministic_nonce(associated, plaintext),
            associated,
            plaintext,
            sealed,
        )
    }

    fn append_sealed(
        &self,
        nonce: [u8; NONCE_LEN],
        associated: &str,
        plaintext: &[u8],
        sealed: &mut Vec<u8>,
    ) -> crate::Result<()> {
        let start = sealed.len();
        sealed.extend_from_slice(&nonce);
        sealed.extend_from_slice(plaintext);
        let Ok(tag) = self.key.seal_in_place_separate_tag(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(associated.as_bytes()),
            &mut sealed[start + NONCE_LEN..],
        ) else {
            sealed.truncate(start);
            return Err(Error::Task("encryption failed".into()));
        };
        sealed.extend_from_slice(tag.as_ref());
        Ok(())
    }

    pub fn open(&self, associated: &str, sealed: &[u8]) -> crate::Result<Bytes> {
        let mut out = Vec::with_capacity(sealed.len().saturating_sub(SEALED_OVERHEAD));
        self.append_opened(associated, sealed, &mut out)?;
        Ok(out.into())
    }

    fn append_opened(
        &self,
        associated: &str,
        sealed: &[u8],
        out: &mut Vec<u8>,
    ) -> crate::Result<()> {
        if sealed.len() < SEALED_OVERHEAD {
            return Err(Error::Task(format!("{associated}: too short to be sealed")));
        }
        let nonce = Nonce::try_assume_unique_for_key(&sealed[..NONCE_LEN])
            .map_err(|_| Error::Task("malformed nonce".into()))?;
        let (ciphertext, tag) = sealed[NONCE_LEN..].split_at(sealed.len() - SEALED_OVERHEAD);
        let tag = Tag::try_from(tag).expect("fixed AES-GCM tag size");
        let start = out.len();
        out.extend_from_slice(ciphertext);
        if self
            .key
            .open_in_place_separate_tag(
                nonce,
                Aad::from(associated.as_bytes()),
                tag,
                &mut out[start..],
                0..,
            )
            .is_err()
        {
            out.truncate(start);
            return Err(Error::Task(format!(
                "{associated} cannot be decrypted under this deployment's key"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sealing_byte_path_preserves_the_original_nonce_ciphertext_and_tag() {
        let cipher = Cipher::new(&[5; 32]).unwrap();
        let prefix = [6; 64];
        let associated = "journal slot";
        for (body, nonce) in [
            (
                b"".as_slice(),
                [246, 163, 190, 5, 124, 65, 235, 94, 86, 251, 100, 118],
            ),
            (
                b"one journal record".as_slice(),
                [195, 69, 99, 161, 237, 44, 205, 29, 137, 69, 242, 12],
            ),
        ] {
            let mut expected = prefix.to_vec();
            expected.extend_from_slice(&nonce);
            let start = expected.len();
            expected.extend_from_slice(body);
            let tag = cipher
                .key
                .seal_in_place_separate_tag(
                    Nonce::assume_unique_for_key(nonce),
                    Aad::from(associated.as_bytes()),
                    &mut expected[start..],
                )
                .unwrap();
            expected.extend_from_slice(tag.as_ref());

            let framed = cipher.seal_with(nonce, associated, &prefix, body).unwrap();
            assert_eq!(framed.as_ref(), expected);
            assert_eq!(
                cipher
                    .seal_deterministic_prefixed(associated, &prefix, body)
                    .unwrap(),
                framed,
            );
            assert_eq!(
                cipher
                    .open(associated, &framed[prefix.len()..])
                    .unwrap()
                    .as_ref(),
                body
            );
        }
    }

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
