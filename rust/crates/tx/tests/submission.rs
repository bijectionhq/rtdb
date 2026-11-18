use rtdb_columnar::MemoryBudget;
use rtdb_format::envelope::SourceMessage;
use rtdb_tx::*;
use rtdb_types::{LogicalType, ScalarType, Value};

#[test]
fn submitted_sql_admits_temporary_syntax_before_lexing() {
    let submit = |sql: &str, memory: &std::sync::Arc<MemoryBudget>| {
        prepare_submission(vec![TxOp::Sql { sql: sql.into(), args: None }], SubmitOptions::default(), "xtdb", memory)
    };
    let calibration = MemoryBudget::new(1 << 20);
    let scope = rtdb_expr::EvaluationScope::new(&calibration).unwrap();
    let scope_bytes = calibration.used_bytes();
    drop(scope);

    let malformed = "INSERT INTO docs (_id) VALUES (@)";
    let syntax_bytes = rtdb_sql::statement_syntax_estimate(malformed.len()).unwrap();
    let short = MemoryBudget::new(scope_bytes + syntax_bytes - 1);
    let error = submit(malformed, &short).err().unwrap();
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(short.peak_bytes(), scope_bytes);
    assert_eq!(short.used_bytes(), 0);

    let exact = MemoryBudget::new(scope_bytes + syntax_bytes);
    let expected = rtdb_sql::parse_in(malformed, &calibration).unwrap_err();
    assert_eq!(expected.code, "xtdb/sql-error");
    assert_eq!(submit(malformed, &exact).err().unwrap(), expected);
    assert_eq!(exact.peak_bytes(), scope_bytes + syntax_bytes);
    assert_eq!(exact.used_bytes(), 0);

    let dependent = "UPDATE docs SET v = v + 1";
    let syntax_bytes = rtdb_sql::statement_syntax_estimate(dependent.len()).unwrap();
    let memory = MemoryBudget::new(scope_bytes + syntax_bytes);
    let request = submit(dependent, &memory).unwrap();
    assert_eq!(memory.peak_bytes(), scope_bytes + syntax_bytes);
    assert!(memory.used_bytes() > 0 && memory.used_bytes() < scope_bytes);
    drop(request);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn volatile_and_query_clock_sql_remain_in_source_log_for_transaction_time_evaluation() {
    for sql in ["INSERT INTO foo (_id, v) VALUES (1, PG_SLEEP(60.0))", "INSERT INTO foo (_id, v) VALUES (1, CAST(TIME '12:00:00' AS TIMESTAMP))"] {
        let memory = MemoryBudget::new(1 << 25);
        let request = prepare_submission(vec![TxOp::Sql { sql: sql.into(), args: None }], SubmitOptions::default(), "xtdb", &memory).unwrap();
        let encoded = request.encode().unwrap();
        let SourceMessage::Protobuf(rtdb_format::envelope::SourceBody::Tx(tx)) = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap() else { panic!() };
        let mut stream = rtdb_format::ipc::ArrowStream::open(std::sync::Arc::from(tx.tx_ops.as_deref().unwrap()), None).unwrap();
        let page = stream.next().unwrap().unwrap();
        let arrow_schema::DataType::Union(fields, _) = page.column(0).data_type() else { panic!() };
        assert_eq!(fields.iter().map(|(_, field)| field.name().as_str()).collect::<Vec<_>>(), ["sql"], "{sql}");
    }
}

#[test]
fn static_sql_submission_rejects_functions_absent_from_the_sql_registry() {
    let memory = MemoryBudget::new(1 << 25);
    for sql in ["INSERT INTO foo (_id, v) VALUES (1, RANDOM())", "INSERT INTO foo (_id, v) VALUES (1, \"double\"(7))"] {
        assert!(prepare_submission(vec![TxOp::Sql { sql: sql.into(), args: None }], SubmitOptions::default(), "xtdb", &memory).is_err(), "{sql}");
    }
}

#[test]
fn sql_to_static_ops_3484_retains_decimal_values_and_groups_valid_bounds() {
    let memory = MemoryBudget::new(1 << 27);
    let operation = TxOp::Sql { sql: "INSERT INTO foo (_id, v, _valid_from) VALUES (1, 12.34, TIMESTAMP '2024-01-01T00:00:00Z'), (2, 56.78, TIMESTAMP '2024-01-01T00:00:00Z')".into(), args: None };
    let request = prepare_submission(vec![operation], SubmitOptions::default(), "xtdb", &memory).unwrap();
    let encoded = request.encode().unwrap();
    let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
    let SourceMessage::Protobuf(rtdb_format::envelope::SourceBody::Tx(tx)) = &message else { panic!("Expected transaction") };
    let mut stream = rtdb_format::ipc::ArrowStream::open(std::sync::Arc::from(tx.tx_ops.as_deref().unwrap()), None).unwrap();
    let page = stream.next().unwrap().unwrap();
    let arrow_schema::DataType::Union(fields, _) = page.column(0).data_type() else { panic!("Expected operations union") };
    assert_eq!(fields.iter().map(|(_, field)| field.name().as_str()).collect::<Vec<_>>(), ["put-docs"]);
    let decoded = SourceTransaction::decode(&message, &memory).unwrap();
    assert_eq!(decoded.encode().unwrap().as_slice(), encoded.as_slice());
}

#[test]
fn static_expansion_empty_parameters_preserves_the_present_empty_batch() {
    let memory = MemoryBudget::new(1 << 27);
    let args = SqlArguments::new(vec![LogicalType::Scalar(ScalarType::I64)], vec![], &memory).unwrap();
    let operations = vec![TxOp::Sql { sql: "INSERT INTO foo (_id) VALUES (?)".into(), args: Some(args) }];
    let request = prepare_submission(operations, SubmitOptions::default(), "xtdb", &memory).unwrap();
    let encoded = request.encode().unwrap();
    let decoded = SourceTransaction::decode(&SourceMessage::decode(encoded.as_slice()).unwrap().unwrap(), &memory).unwrap();
    assert_eq!(decoded.encode().unwrap().as_slice(), encoded.as_slice());
}

#[test]
fn retained_source_bytes_and_abort_results_hold_their_charge_through_transport_clones() {
    let memory = MemoryBudget::new(1 << 20);
    let owned = RetainedBytes::new(vec![1, 2, 3], &memory).unwrap();
    let bytes = owned.into_bytes();
    let clone = bytes.clone();
    drop(bytes);
    assert!(memory.used_bytes() >= 3);
    drop(clone);
    assert_eq!(memory.used_bytes(), 0);
    let error = RetainedAnomaly::new(rtdb_types::Anomaly::incorrect("test/error", "test").with_context("payload", "x".repeat(8192).into()), &memory).unwrap();
    let clone = error.clone();
    drop(error);
    assert!(memory.used_bytes() >= 8192);
    drop(clone);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn historical_exception_info_retains_original_wire_through_error_clones() {
    use rtdb_format::transit::{TransitValue as T, encode_msgpack};
    let memory = MemoryBudget::new(1 << 20);
    let wire = encode_msgpack(&T::Tagged { tag: "xtdb/exception-info".into(), representation: Box::new(T::Array(vec![T::String("old failure".into()), T::Map(vec![(T::Keyword("detail".into()), T::String("payload".into()))])])) }).unwrap();
    let error = RetainedAnomaly::from_transit(&wire, &memory).unwrap();
    assert_eq!(error.code, "xtdb.error/unknown");
    let clone = error.clone();
    drop(error);
    let encoded = clone.encode().unwrap();
    assert_eq!(encoded.as_slice(), wire);
    drop(clone);
    assert!(memory.used_bytes() >= wire.len());
    drop(encoded);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn nullable_abort_messages_preserve_error_presence_and_release_all_retained_charges() {
    use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, Timestamp, TransactionKey, TxId};
    for message in [None, Some(String::new()), Some("failed".into())] {
        let memory = MemoryBudget::new(1 << 20);
        let error = Anomaly::new_nullable(AnomalyCategory::Fault, "test/error", message.clone());
        let retained = RetainedAnomaly::new(error, &memory).unwrap();
        let outcome = TransactionResult::Aborted { tx_key: TransactionKey { tx_id: TxId(1), system_time: Timestamp(2) }, error: Some(retained) };
        assert!(!outcome.committed());
        assert_eq!(outcome.error().unwrap().message, message);
        let wire = outcome.encoded_error().unwrap().unwrap();
        let encoded = rtdb_format::log::proto::ResolvedTx { tx_id: Some(1), system_time_micros: Some(2), committed: Some(false), error: Some(wire.as_slice().to_vec()), ..Default::default() };
        let (staged, decoded) = decode_replica(&encoded, None, BlockIndex(0), &memory).unwrap();
        assert_eq!(decoded, outcome);
        let clone = decoded.clone();
        drop(decoded); drop(outcome); drop(staged); drop(wire);
        assert!(memory.used_bytes() > 0);
        assert_eq!(clone.error().unwrap().message, message);
        let bytes = clone.encoded_error().unwrap().unwrap();
        assert_eq!(bytes.as_slice(), encoded.error.as_deref().unwrap());
        drop(clone); assert!(memory.used_bytes() > 0);
        drop(bytes); assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn historical_absent_and_empty_messages_keep_exact_wire_bytes() {
    use rtdb_format::transit::{TransitValue as T, encode_msgpack};
    for message in [T::Null, T::String(String::new())] {
        let memory = MemoryBudget::new(1 << 20);
        let wire = encode_msgpack(&T::Tagged { tag: "xtdb/exception-info".into(), representation: Box::new(T::Array(vec![message.clone(), T::Map(vec![])])) }).unwrap();
        let error = RetainedAnomaly::from_transit(&wire, &memory).unwrap();
        assert_eq!(error.message.as_deref(), if message == T::Null { None } else { Some("") });
        let encoded = error.encode().unwrap();
        drop(error);
        assert_eq!(encoded.as_slice(), wire);
        assert!(memory.used_bytes() >= wire.len());
        drop(encoded); assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn coalescing_argument_batches_preserves_old_clones_and_joined_types() {
    let memory = MemoryBudget::new(1 << 24);
    let mut args = SqlArguments::from_rows(vec![vec![Value::I64(1)]], &memory).unwrap();
    let original = args.clone();
    let used = memory.used_bytes();
    let clone = args.clone();
    assert_eq!(memory.used_bytes(), used);
    args.append(SqlArguments::from_rows(vec![vec![Value::Utf8("two".into())]], &memory).unwrap()).unwrap();
    assert_eq!(original.rows().collect::<Vec<_>>(), [vec![Value::I64(1)].as_slice()]);
    assert_eq!(args.row_count(), 2);
    assert_eq!(args.types()[0], LogicalType::Scalar(ScalarType::I64).join(&LogicalType::Scalar(ScalarType::Utf8)));
    drop(args); drop(original); drop(clone);
    assert_eq!(memory.used_bytes(), 0);
}
