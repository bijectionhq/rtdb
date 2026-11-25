use ring::{digest, signature};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

const DOMAIN: &[u8] = b"rtdb/custody/provider/v1\0";
const FAMILY_PLACEMENT_DOMAIN: &[u8] = b"rtdb/v1/original-family-placement\0";

pub fn verify_family_placement(
    key: &[u8; 32],
    namespace: &[u8; 32],
    request_sha256: &[u8; 32],
    signature_bytes: &[u8; 64],
) -> Result<()> {
    let mut message = [0; FAMILY_PLACEMENT_DOMAIN.len() + 64];
    message[..FAMILY_PLACEMENT_DOMAIN.len()].copy_from_slice(FAMILY_PLACEMENT_DOMAIN);
    message[FAMILY_PLACEMENT_DOMAIN.len()..][..32].copy_from_slice(namespace);
    message[FAMILY_PLACEMENT_DOMAIN.len() + 32..].copy_from_slice(request_sha256);
    signature::UnparsedPublicKey::new(&signature::ED25519, key)
        .verify(&message, signature_bytes)
        .map_err(|_| Anomaly::new(AnomalyCategory::Forbidden, "rtdb/original-family-placement",
            "The exact Original family placement has no commissioned signature"))
}

pub fn verify_private_replay(
    key: &[u8; 32], purpose: &str, body_sha256: &[u8; 32], signature_bytes: &[u8; 64],
) -> Result<()> {
    let message = [b"rtdb/v1/original-private-replay\0".as_slice(), purpose.as_bytes(), b"\0", body_sha256].concat();
    signature::UnparsedPublicKey::new(&signature::ED25519, key)
        .verify(&message, signature_bytes)
        .map_err(|_| Anomaly::new(AnomalyCategory::Forbidden, "rtdb/original-private-replay",
            "The exact private replay evidence has no commissioned signature"))
}

pub fn verify_response(
    key: &[u8; 32],
    request: &[u8],
    payload: &[u8],
    signature_bytes: &[u8; 64],
) -> Result<()> {
    let mut message = [0; DOMAIN.len() + 64];
    message[..DOMAIN.len()].copy_from_slice(DOMAIN);
    message[DOMAIN.len()..DOMAIN.len() + 32]
        .copy_from_slice(digest::digest(&digest::SHA256, request).as_ref());
    message[DOMAIN.len() + 32..].copy_from_slice(digest::digest(&digest::SHA256, payload).as_ref());
    signature::UnparsedPublicKey::new(&signature::ED25519, key)
        .verify(&message, signature_bytes)
        .map_err(|_| {
            Anomaly::new(
                AnomalyCategory::Forbidden,
                "rtdb/custody-provider-signature",
                "The original custody provider response is unauthenticated",
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{Ed25519KeyPair, KeyPair};

    #[test]
    fn family_placement_signature_binds_the_namespace_and_exact_request() {
        let signing = Ed25519KeyPair::from_seed_unchecked(&[6; 32]).unwrap();
        let key = signing.public_key().as_ref().try_into().unwrap();
        let namespace = [7; 32];
        let request = [8; 32];
        let message = [FAMILY_PLACEMENT_DOMAIN, namespace.as_slice(), request.as_slice()].concat();
        let signature = signing.sign(&message);
        let signature = signature.as_ref().try_into().unwrap();
        assert!(verify_family_placement(key, &namespace, &request, signature).is_ok());
        assert!(verify_family_placement(key, &[9; 32], &request, signature).is_err());
        assert!(verify_family_placement(key, &namespace, &[9; 32], signature).is_err());
    }

    #[test]
    fn provider_attestation_is_bound_to_request_challenge_and_payload() {
        let signing = Ed25519KeyPair::from_seed_unchecked(&[6; 32]).unwrap();
        let key: [u8; 32] = signing.public_key().as_ref().try_into().unwrap();
        let request = br#"{"operation":"writer","challenge":"first"}"#;
        let payload = br#"{"receipt":{}}"#;
        let message = [
            b"rtdb/custody/provider/v1\0".as_slice(),
            digest::digest(&digest::SHA256, request).as_ref(),
            digest::digest(&digest::SHA256, payload).as_ref(),
        ]
        .concat();
        let signature = signing.sign(&message);
        let sig = signature.as_ref().try_into().unwrap();
        verify_response(&key, request, payload, sig).unwrap();
        assert!(
            verify_response(
                &key,
                br#"{"operation":"writer","challenge":"second"}"#,
                payload,
                sig
            )
            .is_err()
        );
        assert!(verify_response(&key, request, b"{}", sig).is_err());
        assert!(verify_response(&[7; 32], request, payload, sig).is_err());
    }
}
