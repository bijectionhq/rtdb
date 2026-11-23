use rtdb_types::StructFields;
use std::collections::BTreeSet;
use rtdb_columnar::MemoryBudget;
use rtdb_pg_source::{ConfirmationState, Decoder, Lsn, ReconnectPolicy, ResumeToken, RowKind, read_text};
use rtdb_types::{TimeUnit, Value};

#[test]
fn confirmation_waits_for_adopted_blocks_then_crosses_unpublished_wal_gaps() {
    let mut state = ConfirmationState::new(Lsn(100), Some(Lsn(100)));
    state.received_through(Lsn(150));
    assert_eq!(state.confirmable(), Lsn(150));
    state.confirmed_through(Lsn(150));
    state.submitted(Lsn(200));
    state.received_through(Lsn(250));
    assert_eq!(state.confirmable(), Lsn(100));
    state.adopted(Some(Lsn(200)));
    assert_eq!(state.confirmable(), Lsn(250));
}
#[test]
fn no_durable_extent_permits_slot_progress_before_the_first_block() {
    let mut state = ConfirmationState::new(Lsn(100), None);
    state.submitted(Lsn(200)); state.received_through(Lsn(250));
    assert_eq!(state.confirmable(), Lsn(250));
}
#[test]
fn opening_from_applied_resume_does_not_acknowledge_it_past_the_durable_extent() {
    let state = ConfirmationState::new(Lsn(200), Some(Lsn(100)));
    assert_eq!(state.confirmed(), Lsn(0)); assert_eq!(state.confirmable(), Lsn(100));
}
#[test]
fn source_tokens_retain_signed_lsn_bits_and_reject_incomplete_snapshots() {
    let token = ResumeToken { lsn: Lsn(u64::MAX), snapshot_completed: true, legacy_tx_id: Some(42) };
    assert_eq!(ResumeToken::decode(&token.encode()).unwrap(), token);
    assert_eq!("FFFFFFFF/FFFFFFFF".parse::<Lsn>().unwrap(), token.lsn);
    assert_eq!(ResumeToken::new(Lsn(1), false).require_complete().unwrap_err().code, "xtdb.postgres/incomplete-snapshot");
}
#[test]
fn reconnect_delay_doubles_caps_and_jitters_downward() {
    let policy = ReconnectPolicy { initial_delay: std::time::Duration::from_millis(100), maximum_delay: std::time::Duration::from_secs(1), ..Default::default() };
    assert_eq!((1..7).map(|attempt| policy.delay_after(attempt, 0.0).as_millis()).collect::<Vec<_>>(), vec![100, 200, 400, 800, 1000, 1000]);
    assert_eq!(policy.delay_after(u32::MAX, 1.0).as_millis(), 500);
    assert!(policy.delay_after(0, 0.0).is_zero());
}
#[test]
fn postgres_text_types_preserve_numeric_array_and_temporal_values() {
    assert_eq!(read_text(21, "-32768").unwrap(), Value::I16(i16::MIN));
    assert_eq!(read_text(23, "2147483647").unwrap(), Value::I32(i32::MAX));
    assert_eq!(read_text(20, "9223372036854775807").unwrap(), Value::I64(i64::MAX));
    assert_eq!(read_text(1009, r#"{"NULL",NULL,"a,b","a\"b","a\\b"}"#).unwrap(), Value::list(vec![Value::Utf8("NULL".into()), Value::Null, Value::Utf8("a,b".into()), Value::Utf8("a\"b".into()), Value::Utf8("a\\b".into())]));
    assert_eq!(read_text(1184, "2000-01-01 00:00:00+00").unwrap(), Value::Timestamp { value: 946_684_800_000_000, unit: TimeUnit::Microsecond, timezone: Some("Z".into()) });
    assert_eq!(read_text(1186, "P1Y-2M3DT4H5M6.000001S").unwrap(), Value::IntervalMonthDayMicro { months: 10, days: 3, nanoseconds: 14_706_000_001_000 });
    let Value::Decimal(decimal) = read_text(1700, "0.000000001").unwrap() else { panic!("expected decimal") };
    assert_eq!((decimal.precision, decimal.scale), (32, 9));
    assert_eq!(read_text(90000, "custom enum").unwrap(), Value::Utf8("custom enum".into()));
    assert_eq!(read_text(11111, "keyword/name").unwrap(), Value::Utf8("keyword/name".into()));
    assert!(read_text(26, "12").is_err());
    assert_eq!(read_text(3910, "empty").unwrap(), Value::Null);
}
fn relation() -> Vec<u8> {
    let mut bytes = vec![b'R']; bytes.extend_from_slice(&1u32.to_be_bytes()); bytes.extend_from_slice(b"public\0docs\0f"); bytes.extend_from_slice(&2u16.to_be_bytes());
    for (name, oid) in [("_id", 23u32), ("payload", 25u32)] { bytes.push(0); bytes.extend_from_slice(name.as_bytes()); bytes.push(0); bytes.extend_from_slice(&oid.to_be_bytes()); bytes.extend_from_slice(&(-1i32).to_be_bytes()); }
    bytes
}
fn begin() -> Vec<u8> { let mut bytes = vec![b'B']; bytes.extend_from_slice(&100u64.to_be_bytes()); bytes.extend_from_slice(&0i64.to_be_bytes()); bytes.extend_from_slice(&1u32.to_be_bytes()); bytes }
fn tuple(values: &[Option<&str>]) -> Vec<u8> { let mut bytes = (values.len() as u16).to_be_bytes().to_vec(); for value in values { match value { None => bytes.push(b'u'), Some(text) => { bytes.push(b't'); bytes.extend_from_slice(&(text.len() as i32).to_be_bytes()); bytes.extend_from_slice(text.as_bytes()); } } } bytes }
fn commit(lsn: u64) -> Vec<u8> { let mut bytes = vec![b'C', 0]; bytes.extend_from_slice(&(lsn - 1).to_be_bytes()); bytes.extend_from_slice(&lsn.to_be_bytes()); bytes.extend_from_slice(&0i64.to_be_bytes()); bytes }
#[test]
fn unchanged_toast_uses_the_old_tuple_and_commit_uses_end_lsn() {
    let memory = MemoryBudget::new(1 << 20); let mut decoder = Decoder::new(BTreeSet::new(), memory.clone());
    decoder.receive(&relation()).unwrap(); decoder.receive(&begin()).unwrap();
    let mut update = vec![b'U']; update.extend_from_slice(&1u32.to_be_bytes()); update.push(b'O'); update.extend(tuple(&[Some("1"), Some("old toast")])); update.push(b'N'); update.extend(tuple(&[Some("1"), None])); decoder.receive(&update).unwrap();
    let transaction = decoder.receive(&commit(123)).unwrap().unwrap();
    assert_eq!(transaction.lsn, Lsn(123)); assert_eq!(transaction.commit_time.unwrap().0, 946_684_800_000_000);
    assert_eq!(transaction.operations[0].kind, RowKind::Put); assert_eq!(transaction.operations[0].row["payload"], Value::Utf8("old toast".into()));
    drop(decoder); assert!(memory.used_bytes() > 0); drop(transaction); assert_eq!(memory.used_bytes(), 0);
}
#[test]
fn unchanged_toast_without_full_old_value_is_fatal_and_ignored_tables_skip_conversion() {
    let memory = MemoryBudget::new(1 << 20); let mut decoder = Decoder::new(BTreeSet::new(), memory.clone());
    decoder.receive(&relation()).unwrap(); decoder.receive(&begin()).unwrap();
    let mut update = vec![b'U']; update.extend_from_slice(&1u32.to_be_bytes()); update.push(b'N'); update.extend(tuple(&[Some("1"), None]));
    assert_eq!(decoder.receive(&update).unwrap_err().code, "xtdb.postgres/unchanged-toast");
    let mut ignored = Decoder::new(BTreeSet::from([rtdb_types::TableRef::new("public", "docs")]), memory);
    ignored.receive(&relation()).unwrap(); ignored.receive(&begin()).unwrap(); ignored.receive(&update).unwrap(); assert!(ignored.receive(&commit(123)).unwrap().is_none());
}

#[test]
fn json_retains_large_integers_and_decodes_json_ld_values() {
    let Value::Decimal(decimal) = read_text(3802, "18446744073709551623").unwrap() else { panic!("expected decimal") };
    assert_eq!(decimal.precision, 32);
    assert_eq!(read_text(114, r#"{"@type":"xt:uuid","@value":"550e8400-e29b-41d4-a716-446655440000"}"#).unwrap(), Value::Uuid(*uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap().as_bytes()));
    assert_eq!(read_text(114, r#"{"@type":"xt:set","@value":[1,1,2]}"#).unwrap(), Value::Set(vec![Value::I64(1), Value::I64(2)]));
    assert_eq!(read_text(16384, r#"["^ ","~:xt/id",1,"~:Some/Field-Name",2]"#).unwrap(), Value::Struct(StructFields::from([("_id".into(), Value::I64(1)), ("some$field_name".into(), Value::I64(2))])));
}

#[test]
fn default_remote_tls_uses_an_explicit_provider() {
    let rtdb_pg_source::SslMode::Prefer(config) = rtdb_pg_source::SslMode::default() else { panic!("default remote must prefer TLS"); };
    assert!(!config.crypto_provider().cipher_suites.is_empty());
}
