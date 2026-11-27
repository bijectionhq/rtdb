use std::{collections::BTreeMap, sync::Arc, time::Duration};
use openssl::ssl::SslVersion;
use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec};
use serde_json::json;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, task::JoinHandle};
#[path = "support/kafka_tls.rs"]
mod tls;

async fn converter(mut options: BTreeMap<String, String>) -> (Box<dyn Converter>, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    options.insert("schema.registry.url".into(), format!("http://{}", listener.local_addr().unwrap()));
    options.insert("dlq.topic".into(), "failures".into());
    options.insert("buffer.memory".into(), "1048576".into());
    options.insert("delivery.timeout.ms".into(), "500".into());
    options.insert("request.timeout.ms".into(), "100".into());
    options.insert("linger.ms".into(), "0".into());
    let server = tokio::spawn(async move {
        for body in [json!({"schema":"{\"type\":\"integer\"}","schemaType":"JSON","ruleSet":{"domainRules":[{"name":"guard","type":"missing","onFailure":"DLQ"}]}}), json!({"id":7,"version":1})] {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept()).await.unwrap().unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") { let mut byte = [0]; stream.read_exact(&mut byte).await.unwrap(); header.push(byte[0]); assert!(header.len() < 8192); }
            let header = String::from_utf8(header).unwrap();
            let length = header.lines().find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length:").map(|value| value.trim().parse::<usize>().unwrap())).unwrap_or(0);
            let mut payload = vec![0; length]; stream.read_exact(&mut payload).await.unwrap();
            let body = body.to_string(); let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let plugin = PluginSpec { class_name: "io.confluent.connect.json.JsonSchemaConverter".into(), options };
    (PluginRegistry::default().converter(&plugin, false).unwrap(), server)
}

async fn observe(options: BTreeMap<String, String>, broker: std::thread::JoinHandle<Result<tls::Handshake, String>>) -> Result<tls::Handshake, String> {
    let (mut converter, registry) = converter(options).await;
    let memory = MemoryBudget::new(1 << 28);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    let error = converter.decode("events", Some(&[0, 0, 0, 0, 7, b'9']), &mut context).await.unwrap_err();
    assert!(error.to_string().contains("Rule failed: guard"));
    registry.await.unwrap();
    drop(context); assert!(memory.used_bytes() > 0);
    let observation = tokio::task::spawn_blocking(move || broker.join().unwrap()).await.unwrap();
    if let Err(failure) = tokio::time::timeout(Duration::from_secs(5), converter.close()).await.unwrap() {
        assert_eq!(failure.code, "xtdb.kafka-connect-source/dlq");
    }
    drop(converter); assert_eq!(memory.used_bytes(), 0);
    observation
}

fn options(address: String, root: &tls::Credential, keystore: &std::path::Path) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("bootstrap.servers".into(), address), ("security.protocol".into(), "SSL".into()),
        ("ssl.truststore.type".into(), "PEM".into()), ("ssl.truststore.certificates".into(), String::from_utf8(root.certificate.to_pem().unwrap()).unwrap()),
        ("ssl.keystore.type".into(), "JKS".into()), ("ssl.keystore.location".into(), keystore.to_str().unwrap().into()),
        ("ssl.keystore.password".into(), "store-password".into()), ("ssl.key.password".into(), "key-password".into()),
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_dlq_selects_a_nonfirst_jks_identity_by_issuer_and_enforces_java_cipher_names() {
    let first_root = tls::Credential::new("first issuer", false, None); let second_root = tls::Credential::new("second issuer", false, None);
    let server = tls::Credential::new("localhost", false, Some(&first_root));
    let first = tls::Credential::new("first client", false, Some(&first_root)); let second = tls::Credential::new("second client", false, Some(&second_root));
    let directory = tempfile::tempdir().unwrap(); let file = directory.path().join("clients.jks");
    std::fs::write(&file, tls::jks(&[("first", &first), ("second", &second)], "store-password", "key-password")).unwrap();
    let (address, broker) = tls::broker(&server, &second_root, SslVersion::TLS1_2, None);
    let mut options = options(address, &first_root, &file);
    options.insert("ssl.enabled.protocols".into(), "TLSv1.2".into());
    options.insert("ssl.cipher.suites".into(), "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256".into());
    let observed = observe(options, broker).await.unwrap();
    assert_eq!(observed.certificate, second.certificate.to_der().unwrap());
    assert_eq!(observed.protocol, "TLSv1.2"); assert_eq!(observed.cipher, "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_dlq_selects_an_ec_identity_when_the_peer_requires_ecdsa() {
    let root = tls::Credential::new("issuer", true, None); let server = tls::Credential::new("localhost", true, Some(&root));
    let first = tls::Credential::new("rsa client", false, Some(&root)); let second = tls::Credential::new("ec client", true, Some(&root));
    let directory = tempfile::tempdir().unwrap(); let file = directory.path().join("clients.jks");
    std::fs::write(&file, tls::jks(&[("first", &first), ("second", &second)], "store-password", "key-password")).unwrap();
    let (address, broker) = tls::broker(&server, &root, SslVersion::TLS1_3, Some("ecdsa_secp256r1_sha256"));
    let mut options = options(address, &root, &file);
    options.insert("ssl.protocol".into(), "TLSv1.2".into());
    options.insert("ssl.enabled.protocols".into(), "TLSv1.3".into());
    options.insert("ssl.cipher.suites".into(), "TLS_AES_128_GCM_SHA256".into());
    let observed = observe(options, broker).await.unwrap();
    assert_eq!(observed.certificate, second.certificate.to_der().unwrap());
    assert_eq!(observed.protocol, "TLSv1.3"); assert_eq!(observed.cipher, "TLS_AES_128_GCM_SHA256");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_dlq_rejects_peer_protocols_and_issuers_outside_its_configuration() {
    let root = tls::Credential::new("issuer", false, None); let other = tls::Credential::new("other issuer", false, None);
    let server = tls::Credential::new("localhost", false, Some(&root)); let client = tls::Credential::new("client", false, Some(&root));
    let directory = tempfile::tempdir().unwrap(); let file = directory.path().join("client.jks");
    std::fs::write(&file, tls::jks(&[("client", &client)], "store-password", "key-password")).unwrap();
    for (accepted, protocols, ciphers) in [
        (&root, "TLSv1.3", "TLS_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"),
        (&root, "TLSv1.2,TLSv1.3", "TLS_AES_128_GCM_SHA256"),
        (&other, "TLSv1.2", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"),
    ] {
        let (address, broker) = tls::broker(&server, accepted, SslVersion::TLS1_2, None);
        let mut options = options(address, &root, &file);
        options.insert("ssl.enabled.protocols".into(), protocols.into()); options.insert("ssl.cipher.suites".into(), ciphers.into());
        let error = observe(options, broker).await.err().expect("TLS handshake must fail");
        assert!(error.starts_with("handshake:"), "{error}");
    }
}

#[tokio::test]
async fn public_dlq_invalid_cipher_failure_releases_configuration_and_material() {
    let mut options = BTreeMap::from([("bootstrap.servers".into(), "127.0.0.1:1".into()), ("security.protocol".into(), "SSL".into()),
        ("ssl.cipher.suites".into(), "TLS_AES_128_GCM_SHA256,TLS_NOT_A_CIPHER".into())]);
    options.insert("ssl.keystore.location".into(), "/missing/keystore.jks".into());
    let (mut converter, registry) = converter(options).await;
    let memory = MemoryBudget::new(1 << 28); let mut context = ConversionContext::new(Arc::clone(&memory));
    let error = converter.decode("events", Some(&[0, 0, 0, 0, 7, b'9']), &mut context).await.unwrap_err();
    assert!(error.to_string().contains("Rule failed: guard"));
    registry.await.unwrap(); converter.close().await.unwrap(); drop(converter); drop(context);
    assert_eq!(memory.used_bytes(), 0);
}
