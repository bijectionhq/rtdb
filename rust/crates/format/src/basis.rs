use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use prost::Message;
use rtdb_types::{Anomaly, Instant, Result, TimeBasis, TxBasis, TxId};

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

/// A missing partition time and the Unix epoch share the JVM token's default Timestamp encoding.
pub fn encode_time_basis(basis: &TimeBasis) -> String {
    let wire = proto::TimeBasis {
        databases: basis.databases.iter().map(|(name, times)| (
            name.clone(), proto::SystemTimes {
                system_times: times.iter().map(|time| match time {
                    None => prost_types::Timestamp::default(),
                    Some(time) => prost_types::Timestamp {
                        seconds: time.seconds(),
                        nanos: time.nanos() as i32,
                    },
                }).collect(),
            }
        )).collect(),
    };
    STANDARD.encode(wire.encode_to_vec())
}

pub fn decode_time_basis(token: &str) -> Result<TimeBasis> {
    let bytes = STANDARD.decode(token).map_err(invalid)?;
    let wire = proto::TimeBasis::decode(bytes.as_slice()).map_err(invalid)?;
    let mut databases = std::collections::BTreeMap::new();
    for (name, times) in wire.databases {
        let mut decoded = Vec::with_capacity(times.system_times.len());
        for time in times.system_times {
            if time == prost_types::Timestamp::default() {
                decoded.push(None);
            } else {
                if !(0..1_000_000_000).contains(&time.nanos) {
                    return Err(invalid("Time basis has an invalid nanosecond component"));
                }
                decoded.push(Some(Instant::new(time.seconds, time.nanos as u32)?));
            }
        }
        databases.insert(name, decoded);
    }
    Ok(TimeBasis { databases })
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("xtdb.basis/invalid-basis", format!("Invalid basis: {error}"))
}
