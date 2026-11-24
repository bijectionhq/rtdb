//! Database key generations and authenticated storage and journal envelopes.

mod aead;
mod custody;
pub mod custody_provider;
mod file;
mod local;
mod source;
pub use source::SourceAdmissionKey;
pub mod tokens;

use async_trait::async_trait;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use uuid::Uuid;
use zeroize::Zeroizing;

pub use file::FileKms;
pub use local::LocalKms;
pub use custody::CustodyVault;

pub type DataKey = Zeroizing<[u8; 32]>;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Generation {
    pub database: Uuid,
    pub number: u64,
}

/// Custody for generation keys.
/// Creation, retirement and destruction are idempotent for the same generation and deadline.
/// Implementations enforce deadlines on each unwrap.
/// Implementations serving durable databases preserve retirement and destruction across restart.
#[async_trait]
pub trait Kms: Send + Sync {
    fn supports_durable_generation_retirement(&self) -> bool { false }
    async fn generation_destroyed(&self, _generation: Generation) -> Result<bool> { Ok(false) }

    async fn create(&self, generation: Generation) -> Result<()>;
    async fn wrap(&self, generation: Generation, key: &DataKey, context: &[u8]) -> Result<Vec<u8>>;
    async fn unwrap(
        &self,
        generation: Generation,
        wrapped: &[u8],
        context: &[u8],
    ) -> Result<DataKey>;
    /// Stops new wraps and authorizes unwrap until the exclusive UTC microsecond deadline.
    async fn retire(&self, generation: Generation, unreadable_by: i64) -> Result<()>;
    async fn check_readable(&self, generation: Generation) -> Result<()>;
    async fn destroy(&self, generation: Generation) -> Result<()>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Context {
    File { epoch: u64, seq: u64 },
    Manifest { epoch: u64, seq: u64 },
    Journal { lsn: u64 },
    CustodyValue { binding: [u8; 32] },
}

const HEADER: &[u8; 6] = b"RTDBE\x01";

impl Context {
    fn aad(self, generation: Generation) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(47);
        bytes.extend_from_slice(HEADER);
        bytes.extend_from_slice(generation.database.as_bytes());
        bytes.extend_from_slice(&generation.number.to_be_bytes());
        match self {
            Self::File { epoch, seq } | Self::Manifest { epoch, seq } => {
                bytes.push(if matches!(self, Self::File { .. }) {
                    1
                } else {
                    2
                });
                bytes.extend_from_slice(&epoch.to_be_bytes());
                bytes.extend_from_slice(&seq.to_be_bytes());
            }
            Self::Journal { lsn } => {
                bytes.push(3);
                bytes.extend_from_slice(&lsn.to_be_bytes());
            }
            Self::CustodyValue { binding } => {
                bytes.push(4);
                bytes.extend_from_slice(&binding);
            }
        }
        bytes
    }
}

/// Seal once and keep the returned bytes for retries of the same journal append or object put.
pub async fn seal(
    kms: &dyn Kms,
    generation: Generation,
    context: Context,
    plaintext: &[u8],
) -> Result<Vec<u8>> {
    let data_key = aead::random_key()?;
    let aad = context.aad(generation);
    let wrapped = kms.wrap(generation, &data_key, &aad).await?;
    let wrapped_len = u32::try_from(wrapped.len()).map_err(|_| invalid_envelope())?;
    let ciphertext = aead::seal(&data_key, &aad, plaintext)?;

    let mut bytes = Vec::with_capacity(HEADER.len() + 12 + wrapped.len() + ciphertext.len());
    bytes.extend_from_slice(HEADER);
    bytes.extend_from_slice(&generation.number.to_be_bytes());
    bytes.extend_from_slice(&wrapped_len.to_be_bytes());
    bytes.extend_from_slice(&wrapped);
    bytes.extend_from_slice(&ciphertext);
    Ok(bytes)
}

pub struct Plaintext {
    pub generation: Generation,
    pub bytes: Zeroizing<Vec<u8>>,
}

/// Opening checks custody again after authentication.
/// A caller that defers delivery must check readability at delivery time.
pub async fn open(
    kms: &dyn Kms,
    database: Uuid,
    context: Context,
    envelope: &[u8],
) -> Result<Plaintext> {
    let input = envelope.strip_prefix(HEADER).ok_or_else(invalid_envelope)?;
    let (number, input) = input
        .split_first_chunk::<8>()
        .ok_or_else(invalid_envelope)?;
    let (wrapped_len, input) = input
        .split_first_chunk::<4>()
        .ok_or_else(invalid_envelope)?;
    let (wrapped, ciphertext) = input
        .split_at_checked(u32::from_be_bytes(*wrapped_len) as usize)
        .ok_or_else(invalid_envelope)?;
    let generation = Generation {
        database,
        number: u64::from_be_bytes(*number),
    };
    let aad = context.aad(generation);
    let key = kms.unwrap(generation, wrapped, &aad).await?;
    let bytes = aead::open(&key, &aad, ciphertext)?;
    kms.check_readable(generation).await?;
    Ok(Plaintext { generation, bytes })
}

fn invalid_envelope() -> Anomaly {
    Anomaly::fault("rtdb.crypt/invalid-envelope", "Invalid encrypted envelope")
}

pub fn erased_under_view() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::NotFound,
        "rtdb/erased-under-view",
        "The view requires an erased key generation",
    )
}

#[cfg(test)]
mod tests;
