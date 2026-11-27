use rtdb_types::{Anomaly, AnomalyCategory, Result};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const LEGACY_ENVELOPE: u8 = 1;
const COMMISSIONED_ENVELOPE: u8 = 2;

/// The authority and database name bound to an incarnation's encrypted journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActivationScope([u8; 32]);

impl ActivationScope {
    pub fn new(authority_id: Uuid, database_name: &str) -> Result<Self> {
        Self::for_cell(authority_id, database_name, None)
    }

    pub fn for_cell(authority_id: Uuid, database_name: &str, cell: Option<Uuid>) -> Result<Self> {
        if authority_id.is_nil() || database_name.is_empty() || cell.is_some_and(|id| id.is_nil()) {
            return Err(Anomaly::incorrect(
                "rtdb/activation-scope",
                "Activation requires an authority identity and database name",
            ));
        }
        let mut digest = Sha256::new();
        if let Some(cell) = cell {
            digest.update(b"rtdb.database-cell-activation.v1");
            digest.update(cell.as_bytes());
        } else {
            digest.update(b"rtdb.database-activation.v1");
        }
        digest.update(authority_id.as_bytes());
        digest.update(database_name.as_bytes());
        Ok(Self(digest.finalize().into()))
    }
}

pub(crate) fn encode_entry(scope: Option<ActivationScope>, epoch: u64, record: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(9 + scope.map_or(0, |scope| scope.0.len()) + record.len());
    bytes.push(if scope.is_some() {
        COMMISSIONED_ENVELOPE
    } else {
        LEGACY_ENVELOPE
    });
    bytes.extend_from_slice(&epoch.to_be_bytes());
    if let Some(scope) = scope {
        bytes.extend_from_slice(&scope.0);
    }
    bytes.extend_from_slice(record);
    bytes
}

pub(crate) fn decode_entry(scope: Option<ActivationScope>, bytes: &[u8]) -> Result<(u64, &[u8])> {
    let (header, record) = bytes.split_first_chunk::<9>().ok_or_else(invalid_entry)?;
    let epoch = u64::from_be_bytes(header[1..].try_into().unwrap());
    if epoch == 0 {
        return Err(invalid_entry());
    }
    match (header[0], scope) {
        (LEGACY_ENVELOPE, None) => Ok((epoch, record)),
        (COMMISSIONED_ENVELOPE, Some(scope)) => {
            let (record_scope, record) =
                record.split_first_chunk::<32>().ok_or_else(invalid_entry)?;
            if *record_scope != scope.0 {
                return Err(scope_mismatch());
            }
            Ok((epoch, record))
        }
        (LEGACY_ENVELOPE, Some(_)) | (COMMISSIONED_ENVELOPE, None) => Err(scope_mismatch()),
        _ => Err(invalid_entry()),
    }
}

pub(crate) fn scope_mismatch() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/activation-scope",
        "The journal requires its commissioned database name and authority",
    )
}

pub(crate) fn invalid_entry() -> Anomaly {
    Anomaly::fault(
        "rtdb/multi-journal-envelope",
        "Invalid Multi journal record envelope",
    )
}
