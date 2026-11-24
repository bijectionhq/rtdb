use hmac::{Hmac, Mac};
use rtdb_types::Timestamp;
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

    pub fn basis(&self, scope: Uuid, cut: Timestamp) -> proto::Basis {
        let body = ((cut.0 as u64) ^ (1 << 63)).to_be_bytes();
        proto::Basis {
            value: self.seal(scope, b"basis", body.to_vec()),
        }
    }

    pub fn read_basis(&self, scope: Uuid, basis: &proto::Basis) -> Result<Timestamp, Status> {
        let body = self.open(scope, b"basis", &basis.value).ok_or_else(|| {
            error::not_found(
                "rtdb/unknown-basis",
                "Basis is unknown in this database incarnation",
            )
        })?;
        Ok(Timestamp((u64::from_be_bytes(body) ^ (1 << 63)) as i64))
    }

    pub fn claim(&self, scope: Uuid, cut: Timestamp) -> proto::Claim {
        let mut body = cut.0.to_be_bytes().to_vec();
        body.extend_from_slice(b"main\0database");
        proto::Claim {
            value: self.seal(scope, b"claim", body),
        }
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

pub(super) fn cursor_expired() -> Status {
    error::not_found("rtdb/cursor-expired", "Resume from a fresh query")
}
