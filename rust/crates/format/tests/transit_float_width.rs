use rtdb_format::transit::{TransitValue as T,decode_json,decode_msgpack,encode_msgpack,encode_json,from_json_value,to_json_value};

#[test]
fn single_precision_writers_preserve_width_and_readers_widen_both_wire_forms() {
    let single = vec![0x92,0xa3,b'~',b'#',b'\'',0xca,0x3f,0xc0,0,0];
    assert_eq!(encode_msgpack(&T::Float32(1.5)).unwrap(),single);
    assert_eq!(decode_msgpack(&single).unwrap(),T::Float(1.5));
    assert_eq!(decode_msgpack(&encode_msgpack(&T::Float(1.5)).unwrap()).unwrap(),T::Float(1.5));
    assert_eq!(encode_msgpack(&T::Array(vec![T::Float32(1.5)])).unwrap(),vec![0x91,0xca,0x3f,0xc0,0,0]);
    assert_eq!(encode_msgpack(&T::Map(vec![(T::Float32(1.5),T::Integer(2))])).unwrap(),vec![0x81,0xca,0x3f,0xc0,0,0,2]);
}

#[test]
fn single_precision_json_spelling_and_internal_roundtrip_retain_the_writer_value() {
    assert_eq!(encode_json(&T::Float32(1.1)).unwrap(),br#"["~#'",1.1]"#);
    let value = T::Array(vec![T::Float32(1.1),T::Float32(-0.0),T::Float32(f32::INFINITY)]);
    assert_eq!(from_json_value(&to_json_value(&value).unwrap()).unwrap(),value);
}

#[test]
fn double_precision_json_restores_the_exact_bits() {
    for value in [0.19736842105263158, 0.09868421052631579, f64::from_bits(1), f64::MAX, -0.0] {
        let bytes = encode_json(&T::Float(value)).unwrap();
        let T::Float(decoded) = decode_json(&bytes).unwrap() else { panic!("Expected a double") };
        assert_eq!(decoded.to_bits(), value.to_bits());
    }
}
