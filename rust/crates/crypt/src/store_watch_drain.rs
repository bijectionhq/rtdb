//! Terminal evidence from one Flight receiver. A new receiver has a
//! different signing key even if it reopens the same Store hold.

use ring::signature::{Ed25519KeyPair, KeyPair};
use rtdb_types::{Anomaly, Result};

const DOMAIN: &[u8] = b"rtdb.store.watch-drained.v1\0";

pub struct Signer {
    key: Ed25519KeyPair,
    receiver: [u8; 16],
}

impl Signer {
    pub fn from_seed(seed: &[u8; 32], receiver: [u8; 16]) -> Result<Self> {
        let key = Ed25519KeyPair::from_seed_unchecked(seed)
            .map_err(|_| Anomaly::fault("rtdb.store/watch-receiver-key", "Invalid Store receiver key"))?;
        Ok(Self { key, receiver })
    }

    pub fn public_key(&self) -> [u8; 32] {
        self.key.public_key().as_ref().try_into().expect("Ed25519 public key width")
    }

    /// Both the registered and the verifiably forgotten close path sign
    /// after closing admission and observing every provider buffer gone.
    pub fn drained(&self) -> Vec<u8> {
        let message = [DOMAIN, &self.receiver].concat();
        let signed = self.key.sign(&message);
        [self.receiver.as_slice(), signed.as_ref()].concat()
    }
}
