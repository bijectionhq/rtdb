use std::sync::Arc;

use rtdb_types::Result;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{Context, Generation, Kms, Plaintext, open, seal};

/// Each database incarnation has independent custody of every group key.
#[derive(Clone)]
pub struct CustodyVault {
    incarnation: Uuid,
    keys: Arc<dyn Kms>,
}

impl CustodyVault {
    /// `keys` is the deployment's dedicated custody vault.
    pub fn new(incarnation: Uuid, keys: Arc<dyn Kms>) -> Self { Self { incarnation, keys } }

    pub async fn create(&self, group: [u8; 16]) -> Result<()> {
        self.keys.create(self.generation(group)).await
    }

    pub async fn seal(&self, group: [u8; 16], binding: [u8; 32], payload: &[u8]) -> Result<Vec<u8>> {
        seal(self.keys.as_ref(), self.generation(group), Context::CustodyValue { binding }, payload).await
    }

    pub async fn open(&self, group: [u8; 16], binding: [u8; 32], envelope: &[u8]) -> Result<Plaintext> {
        let generation = self.generation(group);
        let plaintext = open(self.keys.as_ref(), generation.database, Context::CustodyValue { binding }, envelope).await?;
        if plaintext.generation != generation {
            return Err(crate::invalid_envelope());
        }
        Ok(plaintext)
    }

    pub async fn check_readable(&self, group: [u8; 16]) -> Result<()> {
        self.keys.check_readable(self.generation(group)).await
    }

    pub async fn retire(&self, group: [u8; 16], unreadable_by: i64) -> Result<()> {
        self.keys.retire(self.generation(group), unreadable_by).await
    }

    pub async fn destroy(&self, group: [u8; 16]) -> Result<()> {
        self.keys.destroy(self.generation(group)).await
    }

    fn generation(&self, group: [u8; 16]) -> Generation {
        let mut digest = Sha256::new();
        digest.update(b"rtdb/v1/custody-group\0");
        digest.update(self.incarnation.as_bytes());
        digest.update(group);
        let digest: [u8; 32] = digest.finalize().into();
        let (key_domain, _) = digest.split_at(16);
        Generation { database: Uuid::from_bytes(key_domain.try_into().expect("16-byte custody key domain")), number: 0 }
    }
}
