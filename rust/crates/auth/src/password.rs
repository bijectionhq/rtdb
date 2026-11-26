use std::sync::Arc;

use argon2::{Algorithm, Argon2, Params, PasswordHasher, PasswordVerifier, Version, password_hash::{PasswordHash as PhcHash, SaltString}};
use rand_core::OsRng;
use rtdb_columnar::MemoryBudget;
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use zeroize::Zeroizing;

#[derive(Clone, Serialize, Deserialize)]
pub enum PasswordHash { Argon2id(String), BCrypt(String) }

impl std::fmt::Debug for PasswordHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self { Self::Argon2id(_) => "Argon2id([REDACTED])", Self::BCrypt(_) => "BCrypt([REDACTED])" })
    }
}

impl PasswordHash {
    pub fn encoded(&self) -> &str { match self { Self::Argon2id(value) | Self::BCrypt(value) => value } }

    /// Malformed hashes and mismatched algorithms fail authentication; allocation-budget exhaustion is an error.
    pub fn verify(&self, password: &str, memory: &Arc<MemoryBudget>) -> Result<bool> {
        match self {
            Self::Argon2id(encoded) => {
                let Some(parsed) = argon2id_hash(encoded) else { return Ok(false); };
                let _reservation = memory.reserve(self.verification_bytes()?)?;
                Ok(Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok())
            }
            Self::BCrypt(encoded) => Ok(bcrypt::verify(password.as_bytes(), encoded).unwrap_or(false)),
        }
    }

    /// The memory `verify` reserves: an Argon2id hash's memory cost, and nothing for bcrypt or a hash that fails before hashing.
    pub fn verification_bytes(&self) -> Result<usize> {
        let Self::Argon2id(encoded) = self else { return Ok(0); };
        let Some(parsed) = argon2id_hash(encoded) else { return Ok(0); };
        let Ok(params) = Params::try_from(&parsed) else { return Ok(0); };
        (params.m_cost() as usize).checked_mul(1024)
            .ok_or_else(|| Anomaly::incorrect("rtdb.auth/password-memory", "Password memory cost exceeds addressable memory"))
    }

    /// Derives an Argon2id v19 PHC hash with 64 MiB, three iterations, one lane and a random 16-byte salt.
    pub fn argon2id(password: &str, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let _reservation = memory.reserve(65536 * 1024)?;
        let params = Params::new(65536, 3, 1, Some(32)).map_err(hash_error)?;
        let salt = SaltString::generate(&mut OsRng);
        let hash = Argon2::new(Algorithm::Argon2id, Version::V0x13, params).hash_password(password.as_bytes(), &salt).map_err(hash_error)?;
        Ok(Self::Argon2id(hash.to_string()))
    }

    /// Derives a bcrypt hash at cost 12 with a random salt; bcrypt truncates input after 72 UTF-8 bytes.
    pub fn bcrypt(password: &str) -> Result<Self> {
        bcrypt::hash(password.as_bytes(), 12).map(Self::BCrypt).map_err(hash_error)
    }
}

fn argon2id_hash(encoded: &str) -> Option<PhcHash<'_>> {
    PhcHash::new(encoded).ok().filter(|parsed| parsed.algorithm.as_str() == "argon2id")
}

fn hash_error(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.auth/password-hash", error.to_string()) }

/// Admits password logins to verification on memory of their own, so a login never draws on the query budget.
/// At most one verification per available core runs at a time, each on the blocking pool; a later login waits for a turn.
/// The budget holds that many verifications of the costliest configured hash, so an admitted verification always has room.
pub(crate) struct PasswordAdmission { turns: Semaphore, memory: Arc<MemoryBudget> }

impl PasswordAdmission {
    pub(crate) fn new<'a>(hashes: impl IntoIterator<Item = &'a PasswordHash>) -> Result<Self> {
        let turns = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        let costliest = hashes.into_iter().try_fold(0, |costliest: usize, hash| Ok::<_, Anomaly>(costliest.max(hash.verification_bytes()?)))?;
        let limit = costliest.checked_mul(turns)
            .ok_or_else(|| Anomaly::incorrect("rtdb.auth/password-memory", "Concurrent password verification memory exceeds addressable memory"))?;
        Ok(Self { turns: Semaphore::new(turns), memory: MemoryBudget::new(limit) })
    }

    pub(crate) async fn verify(&self, hash: &PasswordHash, password: &str) -> Result<bool> {
        let _turn = self.turns.acquire().await.map_err(admission_error)?;
        let (hash, password, memory) = (hash.clone(), Zeroizing::new(password.to_owned()), Arc::clone(&self.memory));
        tokio::task::spawn_blocking(move || hash.verify(&password, &memory)).await.map_err(admission_error)?
    }
}

fn admission_error(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.auth/password-admission", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn verification_draws_on_its_own_budget_sized_for_the_costliest_configured_hash() {
        let argon2id = PasswordHash::argon2id("hunter2", &MemoryBudget::new(64 << 20)).unwrap();
        let bcrypt = PasswordHash::bcrypt("hunter2").unwrap();
        let verifier = PasswordAdmission::new([&argon2id, &bcrypt, &PasswordHash::Argon2id("not-a-real-hash".into())]).unwrap();
        let turns = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        assert_eq!(verifier.memory.limit_bytes(), turns * (64 << 20));
        assert!(verifier.verify(&argon2id, "hunter2").await.unwrap());
        assert!(!verifier.verify(&argon2id, "wrong").await.unwrap());
        assert!(verifier.verify(&bcrypt, "hunter2").await.unwrap());
        assert_eq!(verifier.memory.peak_bytes(), 64 << 20);
        assert_eq!(verifier.memory.used_bytes(), 0);
    }

    #[test]
    fn a_verifier_without_argon2_hashes_holds_no_memory() {
        let verifier = PasswordAdmission::new([&PasswordHash::BCrypt("$2b$12$unused".into())]).unwrap();
        assert_eq!(verifier.memory.limit_bytes(), 0);
    }
}
