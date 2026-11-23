//! Ed25519 signing for durable storage authorities.

use ring::rand::SystemRandom;
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use std::io;

pub struct SigningKey(Ed25519KeyPair);

fn invalid_key() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid Ed25519 signing key")
}

impl SigningKey {
    pub fn generate() -> io::Result<Vec<u8>> {
        Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
            .map(|key| key.as_ref().to_vec())
            .map_err(|_| io::Error::other("could not generate an Ed25519 signing key"))
    }

    pub fn open(encoded: &[u8]) -> io::Result<Self> {
        Ed25519KeyPair::from_pkcs8(encoded).map(Self).map_err(|_| invalid_key())
    }

    pub fn public_key(&self) -> [u8; 32] {
        self.0.public_key().as_ref().try_into().expect("Ed25519 public keys have 32 bytes")
    }

    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        self.0.sign(message).as_ref().try_into().expect("Ed25519 signatures have 64 bytes")
    }
}

pub fn verifies(public_key: &[u8; 32], message: &[u8], signature: &[u8; 64]) -> bool {
    UnparsedPublicKey::new(&ED25519, public_key).verify(message, signature).is_ok()
}
