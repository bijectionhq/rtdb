use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use prost::Message;
use rtdb_node::serving::custody_release::{CustodyCarrierAuthority, CustodyReleaseRegistration, NativeLandingWitness};
use rtdb_tx::transact::CustodyOccurrence;
use rtdb_types::{EntityId, TableRef, Timestamp, rtdb_v1::Basis};
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::v1::{error, proto};

pub(super) const MAX_ACTION_BYTES: usize = 6 * 1024 * 1024;
pub(super) const MAX_CARRIER_BYTES: usize = 2 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Request {
    Scope {},
    SubmitPlacement {
        family: String,
        transaction_b64: String,
        signature: String,
    },
    PlacementReceipt {
        family: String,
        key: String,
    },
    EnrollAuthoring {
        family: String,
        assertion: String,
        stage: String,
        group: String,
        native_target_b64: String,
        branch_created_basis: String,
        write_key: String,
        disclosure_sha256: String,
        line_binding: Option<LineBinding>,
        rewrite: Option<Rewrite>,
    },
    SealAuthoring {
        family: String,
        assertion: String,
        write_key: String,
        carrier_b64: String,
    },
    ConfirmAuthoring {
        family: String,
        assertion: String,
        write_key: String,
    },
    Reserve {
        family: String,
        member: String,
        winner: String,
        reservation: String,
        target: String,
        reviewed_source_b64: String,
        original_carrier_b64: String,
        selected_carrier_b64: String,
        native_target_b64: String,
        reserve_key: String,
        accept_key: String,
        request_sha256: String,
        receipt: Witness,
        proof: Witness,
    },
    ReserveRetainedAuthoring {
        family: String,
        member: String,
        winner: String,
        reservation: String,
        target: String,
        reviewed_source_b64: String,
        original_author: AuthorReceipt,
        selected_author: AuthorReceipt,
        private_replay: Option<super::private_replay::ReplayReference>,
        custody_read_proof_b64: String,
        claim_b64: String,
        native_target_b64: String,
        reserve_key: String,
        accept_key: String,
        request_sha256: String,
        receipt: Witness,
        proof: Witness,
    },
    AcquirePrivateReplay {
        family: String,
        hold: String,
        authorization_sha256: String,
        reviewed_source_b64: String,
        original_author: AuthorReceipt,
        selected_author: AuthorReceipt,
        custody_read_proof_b64: String,
        claim_b64: String,
    },
    SealPayload {
        family: String,
        destination: String,
        carrier_b64: String,
    },
    SealLink {
        family: String,
        destination: String,
        payload_sha256: String,
    },
    BindAcceptanceBody {
        family: String,
        destination: String,
        accept_key: String,
        body_sha256: String,
    },
    Activate {
        family: String,
        destination: String,
        accept_key: String,
    },
    Open {
        family: String,
        destination: String,
        binding: String,
        registration: CustodyReleaseRegistration,
        envelope_b64: String,
    },
    OpenAuthoring {
        family: String,
        assertion: String,
        registration: CustodyReleaseRegistration,
        envelope_b64: String,
    },
    Dispose {
        family: String,
        registration: CustodyReleaseRegistration,
    },
    PlanClose {
        family: String,
    },
    Close {
        family: String,
        revoke_key: String,
        revoked_at: String,
        inventory_sha256: String,
        family_inventory_sha256: String,
    },
    RecordDrain {
        family: String,
        revoked_at: String,
        inventory_sha256: String,
    },
}

impl Request {
    pub(super) fn family(&self) -> Result<Option<[u8; 16]>, Status> {
        let family = match self {
            Self::Scope {} => return Ok(None),
            Self::SubmitPlacement { family, .. }
            | Self::PlacementReceipt { family, .. }
            | Self::EnrollAuthoring { family, .. }
            | Self::SealAuthoring { family, .. }
            | Self::ConfirmAuthoring { family, .. }
            | Self::Reserve { family, .. }
            | Self::ReserveRetainedAuthoring { family, .. }
            | Self::AcquirePrivateReplay { family, .. }
            | Self::SealPayload { family, .. }
            | Self::SealLink { family, .. }
            | Self::BindAcceptanceBody { family, .. }
            | Self::Activate { family, .. }
            | Self::Open { family, .. }
            | Self::OpenAuthoring { family, .. }
            | Self::Dispose { family, .. }
            | Self::PlanClose { family, .. }
            | Self::Close { family, .. }
            | Self::RecordDrain { family, .. } => family,
        };
        Ok(Some(nonzero(family)?))
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct AuthorReceipt {
    pub(super) key: String,
    pub(super) basis: String,
    pub(super) system_time_micros: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LineBinding {
    pub(super) line_generation: String,
    pub(super) creation_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Rewrite {
    pub(super) source_occurrence_sha256: String,
    pub(super) link_stage: String,
    pub(super) reserve_key: String,
    pub(super) review_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Witness {
    table: String,
    row_id: String,
    valid_at_micros: String,
    column: String,
}

impl Witness {
    pub(super) fn decode(self) -> Result<NativeLandingWitness, Status> {
        if self.column.is_empty() || !self.table.contains('.') {
            return Err(invalid());
        }
        Ok(NativeLandingWitness {
            table: TableRef::parse(&self.table).map_err(error::anomaly)?,
            id: EntityId::from_handle_bytes(&hex(&self.row_id)?).map_err(error::anomaly)?,
            valid_at: Timestamp(self.valid_at_micros.parse().map_err(|_| invalid())?),
            column: self.column,
        })
    }
}

pub(super) fn invalid() -> Status {
    error::incorrect("Invalid Original family landing request")
}

pub(super) fn fixed<const N: usize>(value: &str) -> Result<[u8; N], Status> {
    if value.len() != N * 2 {
        return Err(invalid());
    }
    hex(value)?.try_into().map_err(|_| invalid())
}

pub(in crate::v1) fn nonzero<const N: usize>(value: &str) -> Result<[u8; N], Status> {
    let bytes = fixed(value)?;
    if bytes == [0; N] {
        return Err(invalid());
    }
    Ok(bytes)
}

fn hex(value: &str) -> Result<Vec<u8>, Status> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }
    if value.len() % 2 != 0 {
        return Err(invalid());
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok((nibble(pair[0]).ok_or_else(invalid)? << 4) | nibble(pair[1]).ok_or_else(invalid)?))
        .collect()
}

pub(super) fn encode_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        write!(encoded, "{byte:02x}").expect("write to string");
    }
    encoded
}

pub(super) fn key(value: &str) -> Result<Vec<u8>, Status> {
    if !(32..=128).contains(&value.len()) {
        return Err(invalid());
    }
    hex(value)
}

pub(in crate::v1) fn basis(value: &str) -> Result<Basis, Status> {
    Basis::from_bytes(&fixed::<24>(value)?).map_err(error::anomaly)
}

pub(super) fn decode_proto<M: Message + Default>(value: &str) -> Result<M, Status> {
    M::decode(blob(value, 8192)?.as_slice()).map_err(|_| invalid())
}

pub(super) fn blob(value: &str, limit: usize) -> Result<Vec<u8>, Status> {
    if value.is_empty() || value.len() > limit.div_ceil(3) * 4 {
        return Err(invalid());
    }
    let bytes = URL_SAFE_NO_PAD.decode(value).map_err(|_| invalid())?;
    if bytes.is_empty() || bytes.len() > limit {
        return Err(invalid());
    }
    Ok(bytes)
}

pub(super) fn stage_scope(authority: CustodyCarrierAuthority) -> [u8; 24] {
    let mut scope = [0; 24];
    scope[..16].copy_from_slice(&authority.incarnation);
    let digest = super::vault::hash(&authority.installation);
    let mut generation: [u8; 8] = digest[..8].try_into().expect("generation width");
    generation[0] |= 1;
    scope[16..].copy_from_slice(&generation);
    scope
}

pub(super) fn stage(bytes: &[u8], authority: CustodyCarrierAuthority) -> Result<[u8; 16], Status> {
    let (scope, stage) = bytes.split_first_chunk::<24>().ok_or_else(invalid)?;
    let stage: [u8; 16] = stage.try_into().map_err(|_| invalid())?;
    if *scope != stage_scope(authority) || stage == [0; 16] {
        return Err(invalid());
    }
    Ok(stage)
}

pub(super) fn contributor(
    group: [u8; 16],
    original: [u8; 32],
    occurrence: &CustodyOccurrence,
    clock: Timestamp,
) -> proto::CustodyReadContributor {
    let target = &occurrence.target;
    proto::CustodyReadContributor {
        group: group.to_vec(),
        original: original.to_vec(),
        occurrence: occurrence.commitment(group).to_vec(),
        receipt_key: occurrence.receipt_key.clone(),
        written_basis: Some(occurrence.written.into()),
        system_time_micros: clock.0,
        table: format!("{}.{}", target.table.schema, target.table.table),
        row_id: target.id.handle_bytes(),
        valid_from_micros: target.valid.lower().0,
        valid_to_micros: target.valid.upper().0,
        carrier_sha256: occurrence.carrier_sha256.to_vec(),
        branch: target.branch.clone(),
        ordinal: occurrence.ordinal,
        placement_id: occurrence.placement_id.to_vec(),
        reservation_sha256: occurrence.reservation.map(|digest| digest.to_vec()),
        link_sha256: occurrence.link_sha256.map(|digest| digest.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_stage_handles_reject_another_installation() {
        let authority = CustodyCarrierAuthority {
            incarnation: [1; 16],
            source_history: [2; 32],
            installation: [3; 16],
            registry: [4; 32],
            original_writer: [5; 32],
        };
        let expected = [6; 16];
        let bytes = [stage_scope(authority).as_slice(), &expected].concat();
        assert_eq!(stage(&bytes, authority).unwrap(), expected);
        let other = CustodyCarrierAuthority { installation: [7; 16], ..authority };
        assert!(stage(&bytes, other).is_err());
        assert!(stage(&[stage_scope(authority).as_slice(), &[0; 16]].concat(), authority).is_err());
        assert!(serde_json::from_str::<Request>(r#"{"operation":"scope","family":"foreign"}"#).is_err());
    }
}
