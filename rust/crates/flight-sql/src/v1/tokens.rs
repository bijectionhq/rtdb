use hmac::{Hmac, Mac};
use rtdb_types::{
    Timestamp,
    rtdb_v1::{Basis, ClaimScope, ClaimSealer, Layer, MAIN_BRANCH},
};
use sha2::Sha256;
use tonic::Status;
use uuid::Uuid;
use zeroize::Zeroizing;

use super::{error, proto};

pub(super) struct Tokens(Zeroizing<[u8; 32]>);

impl Default for Tokens {
    fn default() -> Self {
        let mut key = Zeroizing::new([0; 32]);
        key[..16].copy_from_slice(Uuid::new_v4().as_bytes());
        key[16..].copy_from_slice(Uuid::new_v4().as_bytes());
        Self(key)
    }
}

pub(super) struct Continuation {
    pub cut: Timestamp,
    pub clock: Timestamp,
    pub offset: u64,
    pub request: [u8; 32],
}

impl Tokens {
    fn mac(&self, scope: Uuid, purpose: &[u8], body: &[u8]) -> Hmac<Sha256> {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.0.as_ref())
            .expect("HMAC accepts fixed length keys");
        mac.update(scope.as_bytes());
        mac.update(purpose);
        mac.update(body);
        mac
    }

    fn seal(&self, scope: Uuid, purpose: &[u8], mut body: Vec<u8>) -> Vec<u8> {
        body.extend_from_slice(&self.mac(scope, purpose, &body).finalize().into_bytes());
        body
    }

    fn open<const N: usize>(&self, scope: Uuid, purpose: &[u8], token: &[u8]) -> Option<[u8; N]> {
        let (body, tag) = token.split_at_checked(N)?;
        self.mac(scope, purpose, body).verify_slice(tag).ok()?;
        body.try_into().ok()
    }

    fn claim_sealer(&self, incarnation: [u8; 16]) -> ClaimSealer {
        let key = self.mac(Uuid::from_bytes(incarnation), b"claim-key", &[])
            .finalize()
            .into_bytes();
        ClaimSealer::new(incarnation, key.into())
    }

    pub fn claim(&self, basis: Basis) -> Result<proto::Claim, Status> {
        let value = self.claim_sealer(basis.incarnation())
            .seal(&ClaimScope {
                branch: MAIN_BRANCH.into(),
                basis,
                layer: Layer::Merged,
            })
            .map_err(error::anomaly)?
            .into_bytes();
        Ok(proto::Claim { value })
    }

    pub fn continuation(&self, scope: Uuid, page: &Continuation) -> Vec<u8> {
        let mut body = Vec::with_capacity(88);
        body.extend_from_slice(&page.cut.0.to_be_bytes());
        body.extend_from_slice(&page.clock.0.to_be_bytes());
        body.extend_from_slice(&page.offset.to_be_bytes());
        body.extend_from_slice(&page.request);
        self.seal(scope, b"page", body)
    }

    pub fn read_continuation(&self, scope: Uuid, token: &[u8]) -> Result<Continuation, Status> {
        let bytes = self
            .open::<56>(scope, b"page", token)
            .ok_or_else(cursor_expired)?;
        let [cut, clock, offset, ..] = bytes.as_chunks::<8>().0 else {
            unreachable!()
        };
        let request = bytes[24..].try_into().expect("request fingerprint width");
        Ok(Continuation {
            cut: Timestamp(i64::from_be_bytes(*cut)),
            clock: Timestamp(i64::from_be_bytes(*clock)),
            offset: u64::from_be_bytes(*offset),
            request,
        })
    }
}

// The temporal read driver names a system-time boundary, including between transactions.
const TIMESTAMP_SIGN: u64 = 1 << 63;

pub(super) fn basis(scope: Uuid, cut: Timestamp) -> Basis {
    Basis::new(*scope.as_bytes(), (cut.0 as u64) ^ TIMESTAMP_SIGN)
}

impl From<Basis> for proto::Basis {
    fn from(basis: Basis) -> Self {
        Self { value: basis.to_bytes().to_vec() }
    }
}

pub(super) fn read_basis(scope: Uuid, basis: &proto::Basis) -> Result<Timestamp, Status> {
    let basis = Basis::from_bytes(&basis.value).map_err(error::anomaly)?;
    if basis.incarnation() != *scope.as_bytes() {
        return Err(error::not_found(
            "rtdb/unknown-basis",
            "Basis is unknown in this database incarnation",
        ));
    }
    Ok(Timestamp((basis.position() ^ TIMESTAMP_SIGN) as i64))
}

pub(super) fn cursor_expired() -> Status {
    error::not_found("rtdb/cursor-expired", "Resume from a fresh query")
}

#[cfg(test)]
mod tests {
    use rtdb_types::rtdb_v1::Claim;

    use super::*;

    #[test]
    fn wire_basis_and_claim_use_the_shared_codec() {
        let scope = Uuid::from_u128(1);
        let early = basis(scope, Timestamp(-1));
        let cut = basis(scope, Timestamp(1));
        let wire: proto::Basis = cut.into();
        assert_eq!(Basis::from_bytes(&wire.value).unwrap(), cut);
        assert!(early.to_bytes() < cut.to_bytes());
        assert_eq!(read_basis(scope, &wire).unwrap(), Timestamp(1));
        assert_eq!(
            read_basis(Uuid::from_u128(2), &wire).unwrap_err().code(),
            tonic::Code::NotFound,
        );

        let tokens = Tokens(Zeroizing::new([7; 32]));
        let claim = Claim::from_bytes(tokens.claim(cut).unwrap().value);
        assert_eq!(
            tokens.claim_sealer(*scope.as_bytes()).open(&claim).unwrap(),
            ClaimScope {
                branch: MAIN_BRANCH.into(),
                basis: cut,
                layer: Layer::Merged,
            },
        );
    }
}
