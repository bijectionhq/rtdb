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
