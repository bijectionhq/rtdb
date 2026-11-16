use rtdb_format::{envelope::{SourceBody, SourceMessage}, log::proto};

#[test]
fn borrowed_transaction_classification_preserves_source_oneof_and_legacy_headers() {
    for (message, transaction) in [
        (SourceMessage::LegacyTransaction(vec![0xff, 1, 2, 3]), true),
        (SourceMessage::Protobuf(SourceBody::Tx(proto::Tx { tx_ops: Some(vec![42; 4096]), ..Default::default() })), true),
        (SourceMessage::Protobuf(SourceBody::FlushBlock(proto::FlushBlock { expected_block_idx: Some(0) })), false),
        (SourceMessage::Protobuf(SourceBody::TriesAdded(proto::TriesAdded::default())), false),
        (SourceMessage::Protobuf(SourceBody::AttachDatabase(proto::AttachDatabase::default())), false),
        (SourceMessage::Protobuf(SourceBody::DetachDatabase(proto::DetachDatabase::default())), false),
    ] {
        assert_eq!(SourceMessage::is_transaction_encoded(&message.encode().unwrap()).unwrap(), transaction);
    }
    assert!(!SourceMessage::is_transaction_encoded(&[2]).unwrap());
    assert!(!SourceMessage::is_transaction_encoded(&[3]).unwrap());
    assert!(!SourceMessage::is_transaction_encoded(&[3, 0x32, 0, 0x0a, 0]).unwrap());
    assert!(SourceMessage::is_transaction_encoded(&[3, 0x0a, 0, 0x32, 0]).unwrap());
    assert!(SourceMessage::is_transaction_encoded(&[3, 0x32, 0, 0x3b, 0x08, 1, 0x3c]).unwrap());
}

#[test]
fn malformed_outer_envelopes_cannot_select_a_log_clock() {
    for bytes in [&[][..], &[0], &[3, 0], &[3, 0x32, 10, 0], &[3, 0x30, 0], &[3, 0x3b, 0x44], &[3, 0x3b], &[3, 0x3f], &[3, 0x80], &[3, 0x32, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2]] {
        assert!(SourceMessage::is_transaction_encoded(bytes).is_err(), "{bytes:?}");
    }
}
