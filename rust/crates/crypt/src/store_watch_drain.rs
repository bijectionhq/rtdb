//! Terminal evidence from one Flight receiver. A new receiver has a
//! different signing key even if it reopens the same Store hold.

use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use rtdb_types::{Anomaly, Result};

const DOMAIN: &[u8] = b"rtdb.store.watch-drained.v1\0";

#[derive(Clone, Copy, Debug)]
pub struct Proof {
    pub receiver: [u8; 16],
    pub signature: [u8; 64],
}

impl Proof {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (receiver, signature) = bytes.split_first_chunk::<16>().ok_or_else(invalid)?;
        let signature = signature.try_into().map_err(|_| invalid())?;
        if *receiver == [0; 16] {
            return Err(invalid());
        }
        Ok(Self { receiver: *receiver, signature })
    }

    pub fn verify(&self, key: &[u8; 32]) -> Result<()> {
        let message = [DOMAIN, &self.receiver].concat();
        UnparsedPublicKey::new(&ED25519, key).verify(&message, &self.signature)
            .map_err(|_| invalid())
    }
}

fn invalid() -> Anomaly {
    Anomaly::incorrect("rtdb.store/watch-drain", "The Store watch drain must come from its original receiver")
}

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
