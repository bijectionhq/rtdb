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
