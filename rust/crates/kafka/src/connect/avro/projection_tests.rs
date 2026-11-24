use super::*;
use rtdb_columnar::MemoryBudget;

fn compare(
    writer: &str,
    reader: Option<&str>,
    records: &[&[u8]],
    options: &BTreeMap<String, String>,
) {
    let writer = Arc::new(AvroDatumPlan::parse(writer, &[], options).unwrap());
    let reader = reader.map(|text| Arc::new(AvroDatumPlan::parse(text, &[], options).unwrap()));
    let memory = MemoryBudget::new(1 << 26);
    let mut cache_context = ConversionContext::new(Arc::clone(&memory));
    let resolution = reader
        .as_ref()
        .map(|reader| AvroResolution::compile(&writer, reader, &mut cache_context).unwrap());
    let selected = reader.as_ref().unwrap_or(&writer);
    for bytes in records {
        let mut legacy_union = 0;
        let mut wire_union = 0;
        let mut legacy_context = ConversionContext::new(Arc::clone(&memory));
        let mut wire_context = ConversionContext::new(Arc::clone(&memory));
        let legacy = (|| {
            let datum = match &resolution {
                Some(resolution) => resolution.decode_datum(bytes, &mut legacy_context)?,
                None => writer.decode_datum(bytes, &mut legacy_context)?,
            };
            let datum = writer.binary_result(datum, &mut legacy_context)?;
            let (output, datum) = writer.prepare_output(selected, datum, &mut legacy_context)?;
            let (schema, branch) = match output {
                AvroOutput::Writer => (&writer, None),
                AvroOutput::Reader { branch } => (selected, branch),
            };
            let output =
                schema.connect_plan(branch, Some(7), &mut legacy_union, &mut legacy_context)?;
            output.legacy_convert(datum, &mut legacy_union, &mut legacy_context)
        })();
        let direct = (|| {
            let datum = match &resolution {
                Some(resolution) => resolution.decode_wire(bytes, &mut wire_context)?,
                None => writer.decode_wire(bytes, &mut wire_context)?,
            };
            let datum = writer.wire_result(datum, &mut wire_context)?;
            let (schema, branch) =
                match writer.prepare_wire_output(selected, &datum, &mut wire_context)? {
                    AvroOutput::Writer => (&writer, None),
                    AvroOutput::Reader { branch } => (selected, branch),
                };
            let output =
                schema.connect_plan(branch, Some(7), &mut wire_union, &mut wire_context)?;
            output.convert_wire(datum, &mut wire_union, &mut wire_context)
        })();
        assert_eq!(
            wire_context.allocation_remaining(),
            legacy_context.allocation_remaining(),
            "{bytes:?}"
        );
        assert_eq!(wire_union, legacy_union, "{bytes:?}");
        match (direct, legacy) {
            (Ok(direct), Ok(legacy)) => {
                assert_eq!(direct.schema, legacy.schema, "{bytes:?}");
                assert_eq!(
                    serde_json::to_vec(&direct.value).unwrap(),
                    serde_json::to_vec(&legacy.value).unwrap(),
                    "{bytes:?}"
                );
                if matches!(direct.value, Value::Struct(_)) {
                    assert_eq!(document_page(direct), document_page(legacy), "{bytes:?}");
                }
            }
            (Err(direct), Err(legacy)) => assert_eq!(
                (direct.code, direct.message),
                (legacy.code, legacy.message),
                "{bytes:?}"
            ),
            (direct, legacy) => panic!("{bytes:?}: {direct:?} differs from {legacy:?}"),
        }
    }
}

fn document_page(data: ConnectData) -> Vec<u8> {
    use rtdb_format::data::{StoredEvent, StoredOperation, build_event_batch};
    use rtdb_types::{Interval, Timestamp};
    let document =
        super::super::docs::convert_with_schema(data.value, data.schema.as_deref()).unwrap();
    let Value::Struct(mut fields) = document else {
        panic!("expected document");
    };
    let id = Value::I64(42);
    let iid = id.entity_id().unwrap().iid();
    fields.insert("_id".into(), id);
    let put_schema = fields
        .iter()
        .map(|(name, value)| (name.clone(), value.logical_type()))
        .collect();
    let event = StoredEvent {
        position: None,
        origin: None,
        commit_position: 0,
        entity_key: None,
        iid,
        system_from: Timestamp(0),
        valid_time: Interval::new(Timestamp(0), Timestamp::MAX).unwrap(),
        operation: StoredOperation::Put(fields),
        recency: None,
    };
    let batch = build_event_batch(&[event], Some(put_schema), None).unwrap();
    rtdb_format::ipc::write_file(batch.schema().as_ref(), &[batch]).unwrap()
}

#[test]
fn f5_avro_connect_projection_matches_binary_record_corpus() {
    compare(
        r#"{"type":"record","name":"W","fields":[{"name":"v","type":{"type":"int","connect.type":"int8"}},{"name":"t","type":{"type":"int","logicalType":"time-millis"}},{"name":"a","type":{"type":"array","items":["null","double"]}},{"name":"next","type":["null","W"]}]}"#,
        None,
        &[
            &[130, 2, 0, 0, 0],
            &[130, 2, 1, 0, 0],
            &[130, 2, 1, 2, 0],
            &[2, 0, 4, 2, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0],
            &[2, 0, 4, 2, 42, 0, 0, 0, 0, 0, 248, 127, 0, 0, 0],
            &[2, 2, 0, 2, 4, 0, 0, 0],
        ],
        &BTreeMap::new(),
    );
    compare(
        r#"{"type":"record","name":"W","fields":[{"name":"old","type":"int"},{"name":"skip","type":{"type":"array","items":"int"}}]}"#,
        Some(
            r#"["null",{"type":"record","name":"R","fields":[{"name":"added","type":{"type":"array","items":"string"},"default":["a","b"]},{"name":"new","aliases":["old"],"type":"long"}]}]"#,
        ),
        &[&[14, 0], &[14, 1, 2, 14, 0], &[14, 1, 8], &[]],
        &BTreeMap::new(),
    );
    compare(
        r#"{"type":"map","values":["null","int"]}"#,
        Some(r#"{"type":"map","values":["int","null"]}"#),
        &[
            &[4, 2, 128, 2, 2, 2, 129, 2, 4, 0],
            &[4, 2, b'b', 2, 2, 2, b'b', 0, 0],
            &[0],
        ],
        &BTreeMap::new(),
    );
    compare(
        r#"[{"type":"fixed","name":"F","size":2},"bytes"]"#,
        None,
        &[&[2, 4, b'a', b'b'], &[2, 2, b'z'], &[0, b'a', b'b']],
        &BTreeMap::new(),
    );
    compare(
        r#"{"type":"array","items":{"type":"record","name":"MapEntry","namespace":"io.confluent.connect.avro","fields":[{"name":"value","type":"string"},{"name":"key","type":{"type":"int","connect.type":"int16"}}]}}"#,
        None,
        &[&[2, 2, b'x', 14, 0], &[0], &[2, 6, b'x']],
        &BTreeMap::new(),
    );
    compare(
        r#""string""#,
        Some(r#""bytes""#),
        &[&[2, b'z'], &[6, 0xed, 0xa0, 0x80], &[0]],
        &BTreeMap::new(),
    );
    compare(
        r#"["int","string","null"]"#,
        None,
        &[&[0, 84], &[2, 2, b'x'], &[4], &[6]],
        &BTreeMap::from([("generalized.sum.type.support".into(), "true".into())]),
    );
    compare(
        r#"["null","int","string"]"#,
        None,
        &[&[0], &[2, 84], &[4, 2, b'x']],
        &BTreeMap::from([("generalized.sum.type.support".into(), "true".into())]),
    );
}
