use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use prost::Message;
use rtdb_types::{Anomaly, Result, TxBasis, TxId};

use crate::generated::xtdb::proto::basis as proto;

pub fn encode_tx_basis(basis: &TxBasis) -> String {
    let wire = proto::TxBasis {
        databases: basis.databases.iter().map(|(name, ids)| (
            name.clone(), proto::MessageIds { message_ids: ids.iter().map(|id| id.0).collect() }
        )).collect(),
    };
    STANDARD.encode(wire.encode_to_vec())
}

pub fn decode_tx_basis(token: &str) -> Result<TxBasis> {
    let bytes = STANDARD.decode(token).map_err(invalid)?;
    let wire = proto::TxBasis::decode(bytes.as_slice()).map_err(invalid)?;
    Ok(TxBasis { databases: wire.databases.into_iter().map(|(name, ids)| (name, ids.message_ids.into_iter().map(TxId).collect())).collect() })
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("xtdb.basis/invalid-basis", format!("Invalid basis: {error}"))
}
