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
