use rtdb_storage::attestation::{SigningKey, verifies};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Directory member identifiers are commitments to the retained journal key.
pub(crate) fn journal_id(public_key: &[u8; 32]) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(b"rtdb.logkeeper/physical-journal/v1");
    digest.update(public_key);
    let mut identity: [u8; 16] = digest.finalize()[..16].try_into().expect("SHA-256 supplies 16 bytes");
    identity[6] = identity[6] & 0x0f | 0x80;
    identity[8] = identity[8] & 0x3f | 0x80;
    Uuid::from_bytes(identity)
}

fn challenge_bytes(nonce: &[u8; 16]) -> Vec<u8> {
    let mut bytes = b"rtdb.logkeeper/identity-challenge/v1".to_vec();
    bytes.extend_from_slice(nonce);
    bytes
}

pub(crate) fn sign_challenge(key: &SigningKey, nonce: &[u8; 16]) -> [u8; 64] {
    key.sign(&challenge_bytes(nonce))
}

pub(crate) fn proves_identity(identity: &str, public_key: &[u8], signature: &[u8], nonce: &[u8; 16]) -> bool {
    let Ok(key) = <&[u8; 32]>::try_from(public_key) else { return false; };
    let Ok(signature) = <&[u8; 64]>::try_from(signature) else { return false; };
    let Ok(parsed) = Uuid::parse_str(identity) else { return false; };
    rtdb_types::valid_journal_identity(identity) && parsed == journal_id(key)
        && verifies(key, &challenge_bytes(nonce), signature)
}
