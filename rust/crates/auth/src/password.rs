use std::sync::Arc;

use argon2::{Algorithm, Argon2, Params, PasswordHasher, PasswordVerifier, Version, password_hash::{PasswordHash as PhcHash, SaltString}};
use rand_core::OsRng;
use rtdb_columnar::MemoryBudget;
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};

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
                let Ok(parsed) = PhcHash::new(encoded) else { return Ok(false); };
                if parsed.algorithm.as_str() != "argon2id" { return Ok(false); }
                let Ok(params) = Params::try_from(&parsed) else { return Ok(false); };
                let bytes = (params.m_cost() as usize).checked_mul(1024)
                    .ok_or_else(|| Anomaly::incorrect("rtdb.auth/password-memory", "Password memory cost exceeds addressable memory"))?;
                let _reservation = memory.reserve(bytes)?;
                Ok(Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok())
            }
            Self::BCrypt(encoded) => Ok(bcrypt::verify(password.as_bytes(), encoded).unwrap_or(false)),
        }
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

fn hash_error(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.auth/password-hash", error.to_string()) }
