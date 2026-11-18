use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn request_head(socket: &mut tokio::net::TcpStream) -> String {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        request.push(socket.read_u8().await.unwrap());
        assert!(request.len() < 8192);
    }
    String::from_utf8(request).unwrap()
}

#[tokio::test]
async fn registry_requests_accept_only_ok_or_no_content_as_success() {
    for (status, body, expected) in [(200, "{}", Some(serde_json::json!({}))), (204, "", Some(Json::Null)), (201, "{}", None)] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            request_head(&mut socket).await;
            socket.write_all(format!("HTTP/1.1 {status} fixture\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        });
        let spec = PluginSpec { class_name: "io.confluent.connect.avro.AvroConverter".into(), options: BTreeMap::from([("schema.registry.url".into(), format!("http://{address}"))]) };
        let mut converter = RegistryConverter::open(&spec, RegistryFormat::Avro, false).unwrap();
        let memory = rtdb_columnar::MemoryBudget::new(1 << 27);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let result = converter.request(&["status".into()], None, &mut context).await;
        match expected { Some(expected) => assert_eq!(result.unwrap(), expected), None => assert!(result.is_err()) }
        server.await.unwrap();
        drop(converter); drop(context);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn stock_avro_converter_parses_fetched_schema_strings_with_avros_comment_policy() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let schema = r#"/* registry schema */ {"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}"#;
        for (request, body) in [
            ("GET /schemas/ids/17 HTTP/1.1", serde_json::json!({"schema": schema})),
            ("POST /subjects/events-value?normalize=false&deleted=true HTTP/1.1", serde_json::json!({"id": 17, "version": 3})),
        ] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let head = request_head(&mut socket).await;
            assert_eq!(head.lines().next(), Some(request));
            if let Some(length) = head.lines().filter_map(|line| line.split_once(':'))
                .find(|(key, _)| key.eq_ignore_ascii_case("content-length")).map(|(_, value)| value.trim().parse::<usize>().unwrap()) {
                assert!(length < 8192);
                let mut bytes = vec![0; length];
                socket.read_exact(&mut bytes).await.unwrap();
            }
            let body = body.to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        }
    });
    let spec = PluginSpec { class_name: "io.confluent.connect.avro.AvroConverter".into(), options: BTreeMap::from([
        ("schema.registry.url".into(), format!("http://{address}")),
    ]) };
    let plugins = crate::connect::PluginRegistry::default();
    let mut converter = plugins.converter(&spec, false).unwrap();
    let memory = rtdb_columnar::MemoryBudget::new(1 << 27);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    let data = tokio::time::timeout(std::time::Duration::from_secs(5), converter.decode("events", Some(&[0, 0, 0, 0, 17, 14]), &mut context))
        .await.unwrap().unwrap();
    assert_eq!(data.value, rtdb_types::Value::Struct(BTreeMap::from([("id".into(), rtdb_types::Value::I64(7))])));
    assert_eq!(data.schema.as_ref().unwrap().version, Some(3));
    converter.close().await.unwrap();
    server.await.unwrap();
    drop((data, converter, plugins, context));
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn writer_schema_id_projects_eligible_envelopes_and_uses_legacy_for_other_fields() {
    for (body, expected_type) in [
        (r#"{"schema":"\"string\"","references":[ ]}"#, None),
        (r#"{"references":null,"schemaType":"AVRO","schema":"\"string\""}"#, Some("AVRO")),
        (r#"{"schema":"ignored","schema":"\"string\"","schemaType":null,"unknown":1}"#, None),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let head = request_head(&mut socket).await;
            assert_eq!(head.lines().next(), Some("GET /schemas/ids/17 HTTP/1.1"));
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            drop(socket);

            let (mut socket, _) = listener.accept().await.unwrap();
            let head = request_head(&mut socket).await;
            assert_eq!(head.lines().next(), Some("POST /subjects/events-value?normalize=false&deleted=true HTTP/1.1"));
            let length = head.lines().filter_map(|line| line.split_once(':'))
                .find(|(key, _)| key.eq_ignore_ascii_case("content-length")).unwrap().1.trim().parse::<usize>().unwrap();
            assert!(length < 8192);
            let mut request = vec![0; length];
            socket.read_exact(&mut request).await.unwrap();
            let mut expected = serde_json::json!({"schema": "\"string\""});
            if let Some(schema_type) = expected_type { expected["schemaType"] = schema_type.into(); }
            assert_eq!(serde_json::from_slice::<Json>(&request).unwrap(), expected);
            let version = br#"{"id":17,"version":3}"#;
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", version.len()).as_bytes()).await.unwrap();
            socket.write_all(version).await.unwrap();
        });
        let spec = PluginSpec {
            class_name: "io.confluent.connect.avro.AvroConverter".into(),
            options: BTreeMap::from([("schema.registry.url".into(), format!("http://{address}"))]),
        };
        let mut converter = RegistryConverter::open(&spec, RegistryFormat::Avro, false).unwrap();
        let memory = rtdb_columnar::MemoryBudget::new(1 << 27);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let document = tokio::time::timeout(std::time::Duration::from_secs(5), converter.schema("events", 17, &mut context)).await.unwrap().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), server).await.unwrap().unwrap();
        assert_eq!(document.subject, "events-value");
        assert_eq!(document.version, 3);
        assert_eq!(document.root.schema_type.as_deref(), expected_type);
        assert_eq!(document.root.schema, "\"string\"");
        assert!(Arc::ptr_eq(&document, &converter.schema("events", 17, &mut context).await.unwrap()));
        drop(document);
        drop(converter);
        drop(context);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn eligible_schema_id_response_uses_typed_admission_on_the_live_get() {
    let body = br#"{"schema":"\"string\"","references":[]}"#;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let head = request_head(&mut socket).await;
            assert_eq!(head.lines().next(), Some("GET /schemas/ids/17 HTTP/1.1"));
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
            socket.write_all(body).await.unwrap();
        }
    });
    let spec = PluginSpec {
        class_name: "io.confluent.connect.avro.AvroConverter".into(),
        options: BTreeMap::from([("schema.registry.url".into(), format!("http://{address}"))]),
    };
    let mut converter = RegistryConverter::open(&spec, RegistryFormat::Avro, false).unwrap();
    let memory = rtdb_columnar::MemoryBudget::new(1 << 27);
    let transport_charge = {
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let path = ["schemas".into(), "ids".into(), "17".into()];
        converter.request_raw(&path, &[], None, &mut context).await.unwrap();
        context.allocation_limit() - context.allocation_remaining()
    };
    drop(converter);
    let mut projection_context = ConversionContext::new(Arc::clone(&memory));
    schema_id::decode(body, &mut projection_context).unwrap().unwrap();
    let projection = projection_context.allocation_limit() - projection_context.allocation_remaining();
    drop(projection_context);
    let mut converter = RegistryConverter::open(&spec, RegistryFormat::Avro, false).unwrap();
    let mut combined = ConversionContext::with_limit(Arc::clone(&memory), transport_charge + projection);
    let root = tokio::time::timeout(std::time::Duration::from_secs(5), converter.fetch_schema_id(17, &mut combined)).await.unwrap().unwrap();
    assert_eq!(root.schema, "\"string\"");
    assert_eq!(combined.allocation_remaining(), 0);
    let mut legacy = ConversionContext::with_limit(Arc::clone(&memory), projection);
    let failure = json_admission::parse(body, &mut legacy).unwrap_err();
    assert!(failure.code.ends_with("/batch-too-large"));
    tokio::time::timeout(std::time::Duration::from_secs(5), server).await.unwrap().unwrap();
    drop(root);
    drop(combined);
    drop(legacy);
    drop(converter);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn raw_registry_response_owns_exact_bytes_after_the_request_context_drops() {
    let cases = [(200, Some(br#"{"id":1,"id":2}"#.as_slice())), (204, None)];
    for (status, body) in cases {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(socket.read_u8().await.unwrap());
                assert!(request.len() < 8192);
            }
            let body = body.unwrap_or_default();
            let head = format!("HTTP/1.1 {status} fixture\r\nContent-Length: {}\r\n\r\n", body.len());
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(body).await.unwrap();
        });
        let spec = PluginSpec {
            class_name: "io.confluent.connect.avro.AvroConverter".into(),
            options: BTreeMap::from([("schema.registry.url".into(), format!("http://{address}"))]),
        };
        let mut converter = RegistryConverter::open(&spec, RegistryFormat::Avro, false).unwrap();
        let response_allowance = converter.maximum_schema_bytes * 4;
        let memory = rtdb_columnar::MemoryBudget::new(1 << 27);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let response = converter.request_raw(&["raw".into()], &[], None, &mut context).await.unwrap();
        server.await.unwrap();
        drop(converter);
        drop(context);

        assert_eq!(response.bytes(), body);
        assert_eq!(memory.used_bytes(), if body.is_some() { response_allowance } else { 0 });
        drop(response);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn chunked_raw_registry_input_preserves_a_multi_window_body_and_duplicate_fields() {
    let mut expected = br#"{"id":1,"padding":""#.to_vec();
    expected.extend(std::iter::repeat_n(b'x', 20_000));
    expected.extend_from_slice(br#"","id":2}"#);
    let body = expected.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") { request.push(socket.read_u8().await.unwrap()); assert!(request.len() < 8192); }
        socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        for chunk in body.chunks(4093) {
            socket.write_all(format!("{:x}\r\n", chunk.len()).as_bytes()).await.unwrap();
            socket.write_all(chunk).await.unwrap();
            socket.write_all(b"\r\n").await.unwrap();
        }
        socket.write_all(b"0\r\n\r\n").await.unwrap();
    });
    let spec = PluginSpec { class_name: "io.confluent.connect.avro.AvroConverter".into(), options: BTreeMap::from([("schema.registry.url".into(), format!("http://{address}"))]) };
    let mut converter = RegistryConverter::open(&spec, RegistryFormat::Avro, false).unwrap();
    let allowance = converter.maximum_schema_bytes * 4;
    let memory = rtdb_columnar::MemoryBudget::new(1 << 27);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    let response = converter.request_raw(&["windows".into()], &[], None, &mut context).await.unwrap();
    server.await.unwrap();
    drop(converter); drop(context);
    assert_eq!(response.bytes(), Some(expected.as_slice()));
    assert_eq!(memory.used_bytes(), allowance);
    drop(response);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn incomplete_http_bodies_fail_before_interpreting_their_json_prefix() {
    for (wire, schema_id) in [
        (b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n{[".as_slice(), false),
        (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{[\r\n".as_slice(), false),
        (b"HTTP/1.1 200 OK\r\nContent-Length: 200\r\n\r\n{\"schema\":\"\\\"string\\\"\"}".as_slice(), true),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = request_head(&mut socket).await;
            if schema_id { assert_eq!(request.lines().next(), Some("GET /schemas/ids/17 HTTP/1.1")); }
            socket.write_all(wire).await.unwrap();
        });
        let spec = PluginSpec { class_name: "io.confluent.connect.avro.AvroConverter".into(), options: BTreeMap::from([("schema.registry.url".into(), format!("http://{address}"))]) };
        let mut converter = RegistryConverter::open(&spec, RegistryFormat::Avro, false).unwrap();
        let memory = rtdb_columnar::MemoryBudget::new(1 << 27);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let failure = if schema_id {
            converter.fetch_schema_id(17, &mut context).await.err().unwrap()
        } else {
            converter.request(&["incomplete".into()], None, &mut context).await.unwrap_err()
        };
        assert!(failure.code.ends_with("/schema-registry"));
        assert!(matches!(failure.message.as_deref(), Some("HTTP response failed" | "HTTP connection failed")));
        server.await.unwrap();
        drop(converter); drop(context);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn registry_http_input_keeps_content_length_stream_limits_and_present_empty_bodies() {
    for (maximum, wire, expected) in [
        (4, b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n".as_slice(), None),
        (4, b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n12345\r\n0\r\n\r\n".as_slice(), None),
        (0, b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".as_slice(), Some(b"".as_slice())),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") { request.push(socket.read_u8().await.unwrap()); assert!(request.len() < 8192); }
            socket.write_all(wire).await.unwrap();
        });
        let spec = PluginSpec { class_name: "io.confluent.connect.avro.AvroConverter".into(), options: BTreeMap::from([("schema.registry.url".into(), format!("http://{address}"))]) };
        let mut converter = RegistryConverter::open(&spec, RegistryFormat::Avro, false).unwrap();
        converter.maximum_schema_bytes = maximum;
        let memory = rtdb_columnar::MemoryBudget::new(1 << 27);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let result = converter.request_raw(&["limit".into()], &[], None, &mut context).await;
        match expected {
            Some(bytes) => assert_eq!(result.unwrap().bytes(), Some(bytes)),
            None => assert_eq!(result.err().unwrap().message.as_deref(), Some("Schema response exceeds its byte limit")),
        }
        server.await.unwrap();
        drop(converter); drop(context);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn cancelling_raw_registry_collection_closes_the_pending_body_and_releases_its_owners() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (received, ready) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") { request.push(socket.read_u8().await.unwrap()); assert!(request.len() < 8192); }
        socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n7\r\npartial\r\n").await.unwrap();
        received.send(()).unwrap();
        match tokio::time::timeout(std::time::Duration::from_secs(5), socket.read(&mut [0])).await.unwrap() {
            Ok(0) => (),
            Err(cause) if cause.kind() == std::io::ErrorKind::ConnectionReset => (),
            result => panic!("expected closed cancelled connection: {result:?}"),
        }
    });
    let spec = PluginSpec { class_name: "io.confluent.connect.avro.AvroConverter".into(), options: BTreeMap::from([("schema.registry.url".into(), format!("http://{address}"))]) };
    let mut converter = RegistryConverter::open(&spec, RegistryFormat::Avro, false).unwrap();
    let memory = rtdb_columnar::MemoryBudget::new(1 << 27);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    let path = ["cancel".into()];
    let mut request = Box::pin(converter.request_raw(&path, &[], None, &mut context));
    tokio::select! { result = &mut request => panic!("unexpected completion: {}", result.is_ok()), _ = ready => () }
    futures::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(request.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    }).await;
    assert!(memory.used_bytes() > 0);
    drop(request);
    server.await.unwrap();
    drop(converter); drop(context);
    assert_eq!(memory.used_bytes(), 0);
}
