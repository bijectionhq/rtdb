use base64::Engine;
use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use base64::engine::general_purpose::STANDARD;
use prost::Message;
use rtdb_types::{Anomaly, Instant, Result, TimeBasis, TxBasis, TxId};

use crate::generated::xtdb::proto::basis as proto;

/// `java.util.Base64.getDecoder()`: padding is optional and trailing bits are ignored.
const JAVA_DECODER: GeneralPurpose = GeneralPurpose::new(&alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent).with_decode_allow_trailing_bits(true));

const JAVA_INSTANT_SECONDS: std::ops::RangeInclusive<i64> = -31_557_014_167_219_200..=31_556_889_864_403_199;

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

/// XTDB's snapshot token (`xtdb.basis/->time-basis-str`): base64 protobuf `TimeBasis`, one system time per partition.
/// An absent time is the default `Timestamp`.
pub fn encode_time_basis(basis: &TimeBasis) -> String {
    let wire = proto::TimeBasis {
        databases: basis.databases.iter().map(|(name, times)| (name.clone(), proto::SystemTimes {
            system_times: times.iter().map(|time| time.map_or_else(prost_types::Timestamp::default, |time| prost_types::Timestamp {
                seconds: time.seconds(), nanos: time.nanos() as i32,
            })).collect(),
        })).collect(),
    };
    STANDARD.encode(wire.encode_to_vec())
}

/// Decodes an XTDB snapshot token (`xtdb.basis/<-time-basis-str`); the default `Timestamp` is an absent time.
pub fn decode_time_basis(token: &str) -> Result<TimeBasis> {
    let bytes = JAVA_DECODER.decode(token).map_err(invalid)?;
    let wire = proto::TimeBasis::decode(bytes.as_slice()).map_err(invalid)?;
    let databases = wire.databases.into_iter().map(|(name, times)| {
        let times = times.system_times.into_iter().map(|time| {
            if time == prost_types::Timestamp::default() { return Ok(None); }
            // As `Instant/ofEpochSecond`, nanoseconds outside one second carry into the seconds.
            let seconds = time.seconds.checked_add(i64::from(time.nanos).div_euclid(1_000_000_000))
                .ok_or_else(|| invalid("long overflow"))?;
            if !JAVA_INSTANT_SECONDS.contains(&seconds) {
                return Err(invalid("Instant exceeds minimum or maximum instant"));
            }
            Instant::new(seconds, time.nanos.rem_euclid(1_000_000_000) as u32).map(Some).map_err(invalid)
        }).collect::<Result<Vec<_>>>()?;
        Ok((name, times))
    }).collect::<Result<_>>()?;
    Ok(TimeBasis { databases })
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("xtdb.basis/invalid-basis", format!("Invalid basis: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn a_time_basis_reads_the_token_xtdb_documents() {
        // `xtdb.api/q` documents this token: database "xtdb", one partition at 2020-01-01T00:00:00Z.
        let basis = decode_time_basis("ChAKBHh0ZGISCAoGCIDCr/AF").unwrap();
        assert_eq!(basis.databases, BTreeMap::from([("xtdb".to_owned(), vec![Some(Instant::new(1_577_836_800, 0).unwrap())])]));
        assert_eq!(encode_time_basis(&basis), "ChAKBHh0ZGISCAoGCIDCr/AF");
    }

    #[test]
    fn a_time_basis_round_trips_absent_times_and_several_databases() {
        let basis = TimeBasis { databases: BTreeMap::from([
            ("a".to_owned(), vec![None, Some(Instant::new(7, 123_000).unwrap())]),
            ("b".to_owned(), vec![Some(Instant::new(-3, 5).unwrap())]),
        ]) };
        let token = encode_time_basis(&basis);
        assert_eq!(decode_time_basis(&token).unwrap(), basis);
        assert_eq!(decode_time_basis(token.trim_end_matches('=')).unwrap(), basis, "Java's decoder does not require padding");
    }

    #[test]
    fn an_undecodable_time_basis_is_an_invalid_basis() {
        for token in ["not base64!", "rtdb-snapshot-v3:e30", "AAAA", "ChAKBHh0ZGISCAoGCIDCr"] {
            let error = decode_time_basis(token).unwrap_err();
            assert_eq!((error.category, error.code.as_str()), (rtdb_types::AnomalyCategory::Incorrect, "xtdb.basis/invalid-basis"), "{token}");
        }
    }

    fn wire_token(system_times: Vec<prost_types::Timestamp>) -> String {
        let wire = proto::TimeBasis { databases: BTreeMap::from([
            ("xtdb".to_owned(), proto::SystemTimes { system_times }),
        ]) };
        STANDARD.encode(wire.encode_to_vec())
    }

    #[test]
    fn a_time_basis_checks_the_java_instant_range_after_carrying_nanos() {
        let minimum = Instant::parse_iso("-1000000000-01-01T00:00:00Z").unwrap();
        let maximum = Instant::parse_iso("+1000000000-12-31T23:59:59.999999999Z").unwrap();
        let token = wire_token(vec![
            prost_types::Timestamp::default(),
            prost_types::Timestamp { seconds: 1, nanos: -1_000_000_000 },
            prost_types::Timestamp { seconds: minimum.seconds() - 1, nanos: 1_000_000_000 },
            prost_types::Timestamp { seconds: maximum.seconds() + 1, nanos: -1 },
        ]);
        assert_eq!(
            decode_time_basis(&token).unwrap().databases["xtdb"],
            [None, Some(Instant::new(0, 0).unwrap()), Some(minimum), Some(maximum)],
        );
    }

    #[test]
    fn a_time_basis_outside_the_java_instant_range_is_invalid() {
        for (seconds, nanos, message) in [
            (-31_557_014_167_219_200, -1, "Instant exceeds minimum or maximum instant"),
            (31_556_889_864_403_199, 1_000_000_000, "Instant exceeds minimum or maximum instant"),
            (i64::MIN, -1, "long overflow"),
            (i64::MAX, 1_000_000_000, "long overflow"),
        ] {
            let token = wire_token(vec![prost_types::Timestamp { seconds, nanos }]);
            let error = decode_time_basis(&token).unwrap_err();
            assert_eq!(error.category, rtdb_types::AnomalyCategory::Incorrect);
            assert_eq!(error.code, "xtdb.basis/invalid-basis");
            assert_eq!(error.message, Some(format!("Invalid basis: {message}")));
        }
    }
}
