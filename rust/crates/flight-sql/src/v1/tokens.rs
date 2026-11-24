use rtdb_types::{Timestamp, rtdb_v1::Basis};
use tonic::Status;
use uuid::Uuid;

use super::{error, proto};

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
    use super::*;

    #[test]
    fn wire_basis_matches_the_shared_encoding() {
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
    }
}
