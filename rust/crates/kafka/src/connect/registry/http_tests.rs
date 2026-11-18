use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn registry_requests_accept_only_ok_or_no_content_as_success() {
    for (status, body, expected) in [(200, "{}", Some(serde_json::json!({}))), (204, "", Some(Json::Null)), (201, "{}", None)] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") { request.push(socket.read_u8().await.unwrap()); assert!(request.len() < 8192); }
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
async fn incomplete_http_bodies_fail_before_interpreting_their_invalid_json_prefix() {
    for wire in [
        b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n{[".as_slice(),
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{[\r\n".as_slice(),
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
        let memory = rtdb_columnar::MemoryBudget::new(1 << 27);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let failure = converter.request(&["incomplete".into()], None, &mut context).await.unwrap_err();
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
