use rtdb_format::{envelope::{SourceBody, SourceMessage}, log::proto};

#[test]
fn borrowed_transaction_classification_preserves_source_oneof_and_legacy_headers() {
    for (message, transaction) in [
        (SourceMessage::LegacyTransaction(vec![0xff, 1, 2, 3].into()), true),
        (SourceMessage::Protobuf(SourceBody::Tx(proto::Tx { tx_ops: Some(vec![42; 4096].into()), ..Default::default() })), true),
        (SourceMessage::Protobuf(SourceBody::FlushBlock(proto::FlushBlock { expected_block_idx: Some(0) })), false),
        (SourceMessage::Protobuf(SourceBody::TriesAdded(proto::TriesAdded::default())), false),
        (SourceMessage::Protobuf(SourceBody::AttachDatabase(proto::AttachDatabase::default())), false),
        (SourceMessage::Protobuf(SourceBody::DetachDatabase(proto::DetachDatabase::default())), false),
        (SourceMessage::Protobuf(SourceBody::ArtefactsAdded(proto::ArtefactsAdded::default())), false),
        (SourceMessage::Protobuf(SourceBody::ArtefactsRemoved(proto::ArtefactsRemoved::default())), false),
    ] {
        assert_eq!(SourceMessage::is_transaction_encoded(&message.encode().unwrap()).unwrap(), transaction);
    }
    assert!(!SourceMessage::is_transaction_encoded(&[2]).unwrap());
    assert!(!SourceMessage::is_transaction_encoded(&[3]).unwrap());
    assert!(!SourceMessage::is_transaction_encoded(&[3, 0x32, 0, 0x0a, 0]).unwrap());
    assert!(SourceMessage::is_transaction_encoded(&[3, 0x0a, 0, 0x32, 0]).unwrap());
    assert!(SourceMessage::is_transaction_encoded(&[3, 0x32, 0, 0x43, 0x08, 1, 0x44]).unwrap());
}

#[test]
fn malformed_outer_envelopes_cannot_select_a_log_clock() {
    for bytes in [&[][..], &[0], &[3, 0], &[3, 0x32, 10, 0], &[3, 0x30, 0], &[3, 0x3b, 0x44], &[3, 0x3b], &[3, 0x3f], &[3, 0x80], &[3, 0x32, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2]] {
        assert!(SourceMessage::is_transaction_encoded(bytes).is_err(), "{bytes:?}");
    }
}

#[test]
fn generated_source_decoding_rejects_an_incomplete_field_after_a_known_body() {
    let original = SourceMessage::Protobuf(SourceBody::FlushBlock(proto::FlushBlock { expected_block_idx: Some(0) }));
    let mut stored = original.encode().unwrap();
    stored.extend([0xa0, 0x06, 1]);
    assert_eq!(SourceMessage::decode_wire(&stored).unwrap(), Some(original));
    stored.extend([0xa2, 0x06, 2, 1]);
    assert!(SourceMessage::decode_wire(&stored).is_err());
}

#[test]
fn arrow_byte_path_source_decode_keeps_owned_protobuf_and_legacy_bytes() {
    use prost::{Message, bytes::Bytes};

    let legacy = Bytes::from(vec![0xff, 1, 2, 3]);
    let Some(SourceMessage::LegacyTransaction(decoded)) = SourceMessage::decode_bytes(legacy.clone()).unwrap() else { panic!("expected legacy transaction") };
    assert_eq!(decoded.as_ptr(), legacy.as_ptr());

    let wire = proto::Tx { tx_ops: Some(Bytes::from_static(b"\0\xffipc")), user_metadata: Some(Bytes::from_static(b"user")), ..Default::default() };
    let mut expected = vec![3];
    proto::SourceLogMessage { message: Some(SourceBody::Tx(wire.clone())) }.encode(&mut expected).unwrap();
    let frame = Bytes::from(SourceMessage::Protobuf(SourceBody::Tx(wire)).encode().unwrap());
    assert_eq!(frame, expected);
    let Some(SourceMessage::Protobuf(SourceBody::Tx(decoded))) = SourceMessage::decode_bytes(frame.clone()).unwrap() else { panic!("expected protobuf transaction") };
    let offset = frame.windows(5).position(|window| window == b"\0\xffipc").unwrap();
    assert_eq!(decoded.tx_ops.unwrap().as_ptr(), frame[offset..].as_ptr());
}
