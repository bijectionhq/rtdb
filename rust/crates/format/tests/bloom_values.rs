use rtdb_columnar::{build_array, to_field};
use rtdb_format::bloom::{BloomFilter, from_column};
use rtdb_types::{LogicalType, ScalarType, Value};

#[test]
fn semantic_build_keys_intersect_persisted_jvm_column_blooms() {
    let ty = LogicalType::Scalar(ScalarType::Utf8).maybe();
    let values = [Value::Utf8("foo".into()), Value::Null, Value::Utf8("bar".into())];
    let array = build_array(&ty, &values).unwrap(); let field = to_field("key", &ty).unwrap();
    let persisted = from_column(&field, array.as_ref()).unwrap();
    let mut probe = BloomFilter::default(); probe.insert_value(&values[0]).unwrap();
    assert!(persisted.intersects(&probe));
    assert_eq!(probe.cardinality(), 5);
    assert_eq!(BloomFilter::from_bytes(&persisted.to_bytes().unwrap()).unwrap(), persisted);
    let mut null = BloomFilter::default(); null.insert_value(&Value::Null).unwrap();
    assert_eq!(null.cardinality(), 1); assert!(null.contains_hashes(0, 0));
}
