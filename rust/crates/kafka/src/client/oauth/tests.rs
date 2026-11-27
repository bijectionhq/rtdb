use super::*;
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    sync::mpsc,
    thread,
};

use rdkafka::{
    admin::AdminClient,
    client::DefaultClientContext,
    consumer::BaseConsumer,
    error::{KafkaError, RDKafkaErrorCode},
    producer::BaseProducer,
};
use serde_json::json;

fn legacy_options(url: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("bootstrap.servers".into(), "127.0.0.1:1".into()),
        ("group.id".into(), "oauth-source".into()),
        ("security.protocol".into(), "SASL_PLAINTEXT".into()),
        ("sasl.mechanism".into(), "OAUTHBEARER".into()),
        (
            CALLBACK.into(),
            format!("{PREFIX}OAuthBearerLoginCallbackHandler"),
        ),
        (ENDPOINT.into(), url.into()),
        (
            "sasl.jaas.config".into(),
            format!(
                "{LOGIN_MODULE} required clientId=\"id with:space\" clientSecret=\"fixture/secret\" scope=\"read write\" extension_logicalCluster=\"fixture\";"
            ),
        ),
        ("sasl.oauthbearer.header.urlencode".into(), "true".into()),
        ("sasl.oauthbearer.sub.claim.name".into(), "principal".into()),
        ("sasl.oauthbearer.scope.claim.name".into(), "roles".into()),
        (RETRY_BACKOFF.into(), "20".into()),
        (RETRY_MAX.into(), "200".into()),
        (CONNECT_TIMEOUT.into(), "1000".into()),
        (READ_TIMEOUT.into(), "1000".into()),
    ])
}

fn token(exp_in_seconds: i64, issued_ago_seconds: i64, distinct: i64) -> String {
    let now = current_time_millis() / 1000;
    let claims = json!({
        "principal": "fixture-service",
        "iat": now - issued_ago_seconds,
        "exp": now + exp_in_seconds,
        "roles": ["read", "write"],
        "jti": distinct,
    });
    let payload = URL_SAFE_NO_PAD.encode(claims.to_string());
    format!("e30.{payload}.c2ln")
}

fn serve(script: Vec<(u16, String)>) -> (String, mpsc::Receiver<()>, thread::JoinHandle<()>) {
    let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let endpoint = format!("http://{}/token", socket.local_addr().unwrap());
    socket.set_nonblocking(true).unwrap();
    let (seen, requests) = mpsc::channel();
    let server = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(18);
        for (status, body) in script {
            let (mut stream, _) = loop {
                match socket.accept() {
                    Ok(connection) => break connection,
                    Err(err)
                        if err.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(err) => panic!("Token endpoint stopped accepting requests: {err}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut reader = BufReader::new(&mut stream);
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            assert_eq!(line.trim_end(), "POST /token HTTP/1.1");
            let mut headers = BTreeMap::new();
            loop {
                line.clear();
                assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
                let (name, value) = line.split_once(':').unwrap();
                headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
            }
            assert_eq!(
                headers["authorization"],
                format!(
                    "Basic {}",
                    STANDARD.encode("id+with%3Aspace:fixture%2Fsecret")
                )
            );
            assert_eq!(headers["content-type"], "application/x-www-form-urlencoded");
            let mut form = vec![0; headers["content-length"].parse::<usize>().unwrap()];
            reader.read_exact(&mut form).unwrap();
            assert_eq!(&form, b"grant_type=client_credentials&scope=read+write");
            write!(
                stream,
                "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
            stream.flush().unwrap();
            seen.send(()).unwrap();
        }
    });
    (endpoint, requests, server)
}

fn prepare(options: &BTreeMap<String, String>, memory: &Arc<MemoryBudget>) -> ClientConfig {
    let (properties, _, oauth) = crate::config::native_client_properties(options).unwrap();
    let mut config = crate::client_config(&properties);
    oauth
        .unwrap()
        .attach_with_allowlist(&mut config, memory, &options[ENDPOINT])
        .unwrap();
    config
}

#[test]
fn native_producer_consumer_and_admin_each_complete_initial_oidc_login() {
    let response = json!({"access_token": token(120, 0, 0)}).to_string();
    let (url, requests, server) = serve(vec![
        (503, String::new()),
        (200, response.clone()),
        (201, response.clone()),
        (200, response),
    ]);
    let memory = MemoryBudget::new(128 << 20);
    let config = prepare(&legacy_options(&url), &memory);
    assert!(!format!("{config:?}").contains("fixture/secret"));
    assert!(config.get(ENDPOINT).is_none());
    let producer = config.create::<BaseProducer>().unwrap();
    let consumer = config.create::<BaseConsumer>().unwrap();
    let admin = config
        .create::<AdminClient<DefaultClientContext>>()
        .unwrap();
    server.join().unwrap();
    assert_eq!(requests.try_iter().count(), 4);
    drop((producer, consumer, admin, config));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn oauth_refresh_runs_without_polling_and_retries_after_endpoint_rejection() {
    let initial = json!({"access_token": token(60, 3600, 0)}).to_string();
    let next = json!({"access_token": token(120, 0, 1)}).to_string();
    let (url, requests, server) = serve(vec![
        (200, initial),
        (401, "response-must-not-leak".into()),
        (200, next),
    ]);
    let mut options = legacy_options(&url);
    options.extend(BTreeMap::from([
        ("sasl.login.refresh.window.jitter".into(), "0".into()),
        ("sasl.login.refresh.min.period.seconds".into(), "0".into()),
        ("sasl.login.refresh.buffer.seconds".into(), "0".into()),
    ]));
    let config = prepare(&options, &MemoryBudget::new(128 << 20));
    let admin = config
        .create::<AdminClient<DefaultClientContext>>()
        .unwrap();
    for _ in 0..3 {
        requests.recv_timeout(Duration::from_secs(16)).unwrap();
    }
    server.join().unwrap();
    drop(admin);
}

#[test]
fn a_failed_refresh_does_not_report_consumer_authentication_failure_while_the_token_is_valid() {
    let initial = json!({"access_token": token(60, 3600, 0)}).to_string();
    let (url, requests, server) = serve(vec![(200, initial), (401, String::new())]);
    let mut options = legacy_options(&url);
    options.extend(BTreeMap::from([
        ("sasl.login.refresh.window.jitter".into(), "0".into()),
        ("sasl.login.refresh.min.period.seconds".into(), "0".into()),
        ("sasl.login.refresh.buffer.seconds".into(), "0".into()),
    ]));
    let config = prepare(&options, &MemoryBudget::new(128 << 20));
    let consumer = config.create::<BaseConsumer>().unwrap();
    for _ in 0..2 {
        requests.recv_timeout(Duration::from_secs(4)).unwrap();
    }
    server.join().unwrap();
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        let polled = consumer.poll(Duration::from_millis(50));
        assert!(!matches!(
            polled,
            Some(Err(KafkaError::MessageConsumption(
                RDKafkaErrorCode::Authentication
            ) | KafkaError::MessageConsumptionFatal(
                RDKafkaErrorCode::Authentication
            ))),
        ));
    }
}

#[test]
fn initial_endpoint_rejection_surfaces_as_client_creation_error_without_secrets() {
    let (url, requests, server) = serve(vec![(401, "response-must-not-leak".into())]);
    let config = prepare(&legacy_options(&url), &MemoryBudget::new(128 << 20));
    let error = config
        .create::<BaseProducer>()
        .err()
        .expect("expected OAuth login error")
        .to_string();
    assert!(error.contains("HTTP 401"));
    assert!(!error.contains("response-must-not-leak"));
    assert!(!error.contains("fixture/secret"));
    server.join().unwrap();
    assert_eq!(requests.try_iter().count(), 1);
}

#[test]
fn kafka_client_credentials_properties_precede_jaas_and_are_consumed() {
    let mut options = legacy_options("http://localhost/token");
    options.insert(CLIENT_ID.into(), "modern".into());
    options.insert(CLIENT_SECRET.into(), "modern-secret".into());
    options.insert(SCOPE.into(), "modern-scope".into());
    let (properties, _, config) = crate::config::native_client_properties(&options).unwrap();
    let config = config.unwrap();
    assert_eq!(
        config.authorization,
        format!("Basic {}", STANDARD.encode("modern:modern-secret"))
    );
    assert_eq!(
        config.form,
        "grant_type=client_credentials&scope=modern-scope"
    );
    assert_eq!(config.extensions[0].to_str().unwrap(), "logicalCluster");
    assert_eq!(config.extensions[1].to_str().unwrap(), "fixture");
    for name in [
        CALLBACK,
        ENDPOINT,
        CLIENT_ID,
        CLIENT_SECRET,
        SCOPE,
        "sasl.jaas.config",
    ] {
        assert!(!properties.contains_key(name));
    }
}

#[test]
fn conflicting_unsecured_options_and_unknown_jvm_classes_fail_at_configuration() {
    let options = legacy_options("http://localhost/token");
    for (key, value, expected) in [
        (
            "enable.sasl.oauthbearer.unsecure.jwt",
            "true",
            "xtdb.kafka/conflicting-config",
        ),
        (
            "sasl.oauthbearer.jwt.retriever.class",
            "custom.Handler",
            "xtdb.kafka/unsupported-config",
        ),
        (
            "sasl.oauthbearer.assertion.file",
            "/unused",
            "xtdb.kafka/unsupported-config",
        ),
    ] {
        let mut options = options.clone();
        options.insert(key.into(), value.into());
        assert_eq!(
            crate::native_properties(&options).unwrap_err().code,
            expected
        );
    }
}

#[test]
fn jwt_claims_and_refresh_windows_use_the_configured_names_and_bounds() {
    let jwt = token(120, 30, 0);
    let (principal, exp, iat) = jwt_claims(&jwt, "principal", "roles").unwrap();
    assert_eq!(principal, "fixture-service");
    assert_eq!(exp - iat.unwrap(), 150_000);
    assert!(jwt_claims(&jwt, "sub", "roles").is_err());
    assert!(jwt_claims(&jwt, "principal", "roles").is_ok());
    assert!(jwt_claims("e30.e30.", "principal", "roles").is_err());

    let refresh = Refresh {
        window: 0.8,
        jitter: 0.05,
        minimum_ms: 60,
        buffer_ms: 300,
    };
    assert_eq!(refresh.next(100, 200, Some(0), 0.0), 180);
    assert_eq!(refresh.next(100, 1000, Some(90), 0.0), 700);
    assert_eq!(refresh.next(100, 500, Some(0), 0.0), 200);
    assert_eq!(refresh.next(100, 1000, Some(90), 0.5), 700);
    assert_eq!(refresh.next(100, 200, Some(0), 0.5), 182);
    assert_eq!(refresh.next(150, 511, Some(0), 0.0), 211);
}

#[test]
fn oauth_endpoint_restriction_rejects_unlisted_urls_and_wildcards() {
    let options = legacy_options("http://localhost/token");
    for allowed in ["", "*", "http://localhost/other,http://localhost:80/token"] {
        let (_, _, oauth) = crate::config::native_client_properties(&options).unwrap();
        let error = oauth
            .unwrap()
            .attach_with_allowlist(
                &mut ClientConfig::new(),
                &MemoryBudget::new(128 << 20),
                allowed,
            )
            .unwrap_err();
        assert!(error.to_string().contains(ALLOWED_URLS_ENV));
    }
}
