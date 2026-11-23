//! Operation-only custody for a checkpoint archive and its dependent suffix.

use std::sync::Arc;

use bytes::Bytes;
use ring::hmac;
use rtdb_storage::Error;
use rtdb_types::{Anomaly, Result};

use crate::cipher::Cipher;

use super::{KEY_BYTES, LocalKeyService};

#[async_trait::async_trait]
pub trait ArchiveKey: Send + Sync {
    async fn seal(&self, associated: &str, plaintext: Bytes) -> rtdb_storage::Result<Bytes>;
    async fn open(&self, associated: &str, ciphertext: Bytes) -> rtdb_storage::Result<Bytes>;
    async fn matches(&self, candidate: [u8; KEY_BYTES]) -> Result<bool>;
}

const KEY_COMPARISON: &[u8] = b"rtdb.archive.key-comparison.v1";

struct LegacyArchiveKey {
    cipher: Cipher,
    comparison: hmac::Tag,
}

pub(crate) fn legacy_archive_key(key: &[u8; KEY_BYTES]) -> Result<Arc<dyn ArchiveKey>> {
    let comparison = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), KEY_COMPARISON);
    Ok(Arc::new(LegacyArchiveKey { cipher: Cipher::new(key)?, comparison }))
}

#[async_trait::async_trait]
impl ArchiveKey for LegacyArchiveKey {
    async fn seal(&self, associated: &str, plaintext: Bytes) -> rtdb_storage::Result<Bytes> {
        self.cipher.seal_deterministic(associated, &plaintext)
    }

    async fn open(&self, associated: &str, ciphertext: Bytes) -> rtdb_storage::Result<Bytes> {
        self.cipher.open(associated, &ciphertext)
    }

    async fn matches(&self, candidate: [u8; KEY_BYTES]) -> Result<bool> {
        let candidate = hmac::Key::new(hmac::HMAC_SHA256, &candidate);
        Ok(hmac::verify(&candidate, KEY_COMPARISON, self.comparison.as_ref()).is_ok())
    }
}

pub(super) struct LocalArchiveKey {
    pub vault: LocalKeyService,
    pub id: String,
}

impl LocalArchiveKey {
    async fn operate<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&[u8; KEY_BYTES]) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let vault = self.vault.clone();
        let id = self.id.clone();
        tokio::task::spawn_blocking(move || {
            let path = vault.path(&id)?;
            let _lock = vault.lock_key(&path)?;
            let key = super::read_key(&path, std::fs::read(&path).map_err(super::io)?)?;
            operation(&key)
        })
        .await
        .map_err(|error| Anomaly::fault("rtdb.multi/archive-vault-task", error.to_string()))?
    }
}

fn archive_io(error: Anomaly) -> Error {
    Error::PermissionDenied { key: "checkpoint archive".into(), message: error.code.into() }
}

#[async_trait::async_trait]
impl ArchiveKey for LocalArchiveKey {
    async fn seal(&self, associated: &str, plaintext: Bytes) -> rtdb_storage::Result<Bytes> {
        let associated = associated.to_owned();
        self.operate(move |key| Ok(Cipher::new(key)?.seal_deterministic(&associated, &plaintext)?))
            .await.map_err(archive_io)
    }

    async fn open(&self, associated: &str, ciphertext: Bytes) -> rtdb_storage::Result<Bytes> {
        let associated = associated.to_owned();
        self.operate(move |key| Ok(Cipher::new(key)?.open(&associated, &ciphertext)?))
            .await.map_err(archive_io)
    }

    async fn matches(&self, candidate: [u8; KEY_BYTES]) -> Result<bool> {
        self.operate(move |key| {
            let candidate = hmac::Key::new(hmac::HMAC_SHA256, &candidate);
            let comparison = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), KEY_COMPARISON);
            Ok(hmac::verify(&candidate, KEY_COMPARISON, comparison.as_ref()).is_ok())
        }).await
    }
}
