use rtdb_format::transit::{TransitValue, decode_msgpack, encode_anomaly};
use rtdb_types::Anomaly;

/// The reference's `xt$txs` error for an inverted valid time writes its data in this order, a `PersistentHashMap`'s.
#[test]
fn an_anomalys_data_is_written_in_clojure_hash_map_order() {
    let error = ["valid-to", "tx-op-idx", "valid-from", "tx-key", "table"].into_iter()
        .fold(Anomaly::incorrect("xtdb.indexer/invalid-valid-times", "Invalid valid times"), |error, key| error.with_context(key, 0.into()));
    let TransitValue::Tagged { representation, .. } = decode_msgpack(&encode_anomaly(&error).unwrap()).unwrap() else { panic!("tagged") };
    let TransitValue::Array(parts) = *representation else { panic!("array") };
    let TransitValue::Map(data) = &parts[3] else { panic!("map") };
    let keys: Vec<_> = data.iter().map(|(key, _)| match key { TransitValue::Keyword(key) => key.as_str(), _ => panic!("keyword") }).collect();
    assert_eq!(keys, ["table", "valid-from", "tx-key", "valid-to", "tx-op-idx"]);
}
