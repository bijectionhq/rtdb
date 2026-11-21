use sha2::{Digest, Sha256, digest::Output};

use crate::protocol::Entry;

pub(crate) struct RetainedChain(Output<Sha256>);

impl RetainedChain {
    pub fn new(stream: &str, first: i64) -> Self {
        let mut root = Sha256::new();
        root.update(b"rtdb.logkeeper.retained-chain.v1\0");
        root.update((stream.len() as u64).to_be_bytes());
        root.update(stream.as_bytes());
        root.update(first.to_be_bytes());
        Self(root.finalize())
    }

    pub fn continue_from(encoded: &str) -> Option<Self> {
        let mut digest = [0; 32];
        if encoded.len() != 2 * digest.len() || !encoded.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) {
            return None;
        }
        for (index, byte) in digest.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&encoded[2 * index..2 * index + 2], 16).ok()?;
        }
        Some(Self::from_bytes(digest))
    }

    pub fn from_bytes(digest: [u8; 32]) -> Self { Self(digest.into()) }

    pub fn bytes(&self) -> [u8; 32] { self.0.into() }

    pub fn append(&mut self, entry: &Entry) {
        let mut link = Sha256::new();
        link.update(self.0);
        link.update(entry.lsn.to_be_bytes());
        link.update(entry.epoch.to_be_bytes());
        link.update(entry.timestamp.to_be_bytes());
        link.update((entry.payload.len() as u64).to_be_bytes());
        link.update(&entry.payload);
        self.0 = link.finalize();
    }

    pub fn digest(&self) -> String { format!("{:x}", self.0) }
}
