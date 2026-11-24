use ring::{digest, signature};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

const DOMAIN: &[u8] = b"rtdb/custody/provider/v1\0";

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
