use rtdb_types::{Anomaly, Result};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{DataKey, aead};

const DOMAIN: &[u8] = b"rtdb.v1/source-admission/1";

pub struct SourceAdmissionKey(DataKey);

impl SourceAdmissionKey {
    pub fn generate() -> Result<Self> { aead::random_key().map(Self) }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let key = bytes.try_into().map_err(|_| Anomaly::fault("rtdb/source-admission-key", "Invalid source admission key"))?;
        Ok(Self(Zeroizing::new(key)))
    }

    pub fn as_bytes(&self) -> &[u8; 32] { &self.0 }

    pub fn seal(&self, incarnation: Uuid, request: &[u8]) -> Result<Vec<u8>> {
        aead::seal(&self.0, &context(incarnation), request)
    }

    pub fn open(&self, incarnation: Uuid, record: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
        aead::open(&self.0, &context(incarnation), record).map_err(|_| {
            Anomaly::incorrect("rtdb/source-admission", "The source record has no valid admission authority")
        })
    }
}

fn context(incarnation: Uuid) -> Vec<u8> {
    let mut context = Vec::with_capacity(DOMAIN.len() + 16);
    context.extend_from_slice(DOMAIN);
    context.extend_from_slice(incarnation.as_bytes());
    context
}
