use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn cache() -> Arc<Mutex<MetadataCache>> { Arc::new(Mutex::new(MetadataCache::default())) }

fn authority(host: &str, tenant: &str, discovery_endpoint: &Url, metadata: Arc<Mutex<MetadataCache>>) -> Authority {
    let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let mut authority = Authority::with_metadata(host, tenant, client, metadata).unwrap();
    if let Mode::Configured { discovery: Some(discovery), .. } = &mut authority.mode { *discovery = discovery_endpoint.clone(); }
    authority
}

async fn server(responses: Vec<(u16, String)>) -> (Url, tokio::task::JoinHandle<Vec<String>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Url::parse(&format!("http://{}/discovery", listener.local_addr().unwrap())).unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (status, body) in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let mut bytes = [0; 1024];
                let count = socket.read(&mut bytes).await.unwrap();
                assert!(count > 0);
                request.extend_from_slice(&bytes[..count]);
                assert!(request.len() <= 16_384);
            }
            requests.push(String::from_utf8(request).unwrap());
            let _ = socket.write_all(format!("HTTP/1.1 {status} Result\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await;
        }
        requests
    });
    (endpoint, task)
}

#[test]
fn discovery_targets_trusted_hosts_and_omits_authority_port_from_authorize_parameter() {
    for (host, discovery_host, port) in [
        ("https://login.windows.net:8443", "login.windows.net", Some(8443)),
        ("https://custom.example:8443", "login.microsoftonline.com", None),
        ("https://login.chinacloudapi.cn", "login.chinacloudapi.cn", None),
    ] {
        let authority = Authority::with_metadata(host, "TENANT", reqwest::Client::new(), cache()).unwrap();
        let Mode::Configured { discovery: Some(discovery), .. } = authority.mode else { panic!("AAD discovery required") };
        assert_eq!(discovery.host_str(), Some(discovery_host));
        assert_eq!(discovery.port(), port);
        assert_eq!(discovery.path(), "/common/discovery/instance");
        let query: BTreeMap<_, _> = discovery.query_pairs().into_owned().collect();
        assert_eq!(query["api-version"], "1.1");
        let authority_host = Url::parse(host).unwrap().host_str().unwrap().to_owned();
        assert_eq!(query["authorization_endpoint"], format!("https://{authority_host}/tenant/oauth2/v2.0/authorize"));
    }
}

#[tokio::test]
async fn network_failure_caches_known_cloud_aliases_across_authorities_and_tenants() {
    let (endpoint, server) = server(vec![]).await;
    server.await.unwrap();
    let metadata = cache();
    let first = authority("https://login.windows.net", "first", &endpoint, metadata.clone());
    assert_eq!(first.token_endpoint().await.unwrap().as_str(), "https://login.microsoftonline.com/first/oauth2/v2.0/token");
    let next = authority("https://sts.windows.net", "second", &endpoint, metadata.clone());
    assert_eq!(next.token_endpoint().await.unwrap().as_str(), "https://login.microsoftonline.com/second/oauth2/v2.0/token");
    assert_eq!(metadata.lock().await.aliases.len(), 4);
}

#[tokio::test]
async fn malformed_json_uses_cached_fallback_without_retrying_discovery() {
    let (endpoint, server) = server(vec![(200, "invalid private response".into())]).await;
    let authority = authority("https://custom.example", "common", &endpoint, cache());
    let expected = "https://custom.example/common/oauth2/v2.0/token";
    assert_eq!(authority.token_endpoint().await.unwrap().as_str(), expected);
    assert_eq!(authority.token_endpoint().await.unwrap().as_str(), expected);
    assert_eq!(server.await.unwrap().len(), 1);
}

#[tokio::test]
async fn discovery_metadata_selects_preferred_host_preserving_port_and_populates_aliases() {
    let response = r#"{"tenant_discovery_endpoint":"https://unused.example/openid","metadata":[{"preferred_network":"network.example","preferred_cache":"cache.example","aliases":["original.example","alias.example"]}]}"#;
    let (endpoint, server) = server(vec![(200, response.into())]).await;
    let metadata = cache();
    let first = authority("https://original.example:8443", "tenant", &endpoint, metadata.clone());
    assert_eq!(first.token_endpoint().await.unwrap().as_str(), "https://network.example:8443/tenant/oauth2/v2.0/token");
    let next = authority("https://alias.example", "other", &endpoint, metadata);
    assert_eq!(next.token_endpoint().await.unwrap().as_str(), "https://network.example/other/oauth2/v2.0/token");
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("GET /discovery HTTP/1.1"));
    assert!(!requests[0].to_lowercase().contains("authorization:"));
}

#[tokio::test]
async fn invalid_instance_rejects_without_caching_or_exposing_service_payload() {
    let (endpoint, server) = server(vec![(400, r#"{"error":"invalid_instance","error_description":"private service detail"}"#.into())]).await;
    let metadata = cache();
    let authority = authority("https://custom.example", "common", &endpoint, metadata.clone());
    let error = authority.token_endpoint().await.unwrap_err();
    assert!(!error.to_string().contains("private service detail"));
    assert!(metadata.lock().await.aliases.is_empty());
    server.await.unwrap();
}

#[tokio::test]
async fn success_without_tenant_discovery_endpoint_rejects_without_caching() {
    for body in ["{}", "null", r#"{"tenant_discovery_endpoint":" \t\r\n"}"#] {
        let (endpoint, server) = server(vec![(200, body.into())]).await;
        let metadata = cache();
        let authority = authority("https://custom.example", "common", &endpoint, metadata.clone());
        assert!(authority.token_endpoint().await.is_err());
        assert!(metadata.lock().await.aliases.is_empty());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn http_failure_caches_fallback_before_tenant_endpoint_validation() {
    let (endpoint, server) = server(vec![(503, r#"{"error":"unavailable"}"#.into())]).await;
    let authority = authority("https://login.windows.net", "common", &endpoint, cache());
    assert!(authority.token_endpoint().await.is_err());
    assert_eq!(authority.token_endpoint().await.unwrap().as_str(), "https://login.microsoftonline.com/common/oauth2/v2.0/token");
    assert_eq!(server.await.unwrap().len(), 1);
}

#[tokio::test]
async fn bad_request_without_error_code_uses_fallback_without_validation() {
    let (endpoint, server) = server(vec![(400, "{}".into())]).await;
    let authority = authority("https://custom.example", "common", &endpoint, cache());
    assert_eq!(authority.token_endpoint().await.unwrap().host_str(), Some("custom.example"));
    server.await.unwrap();
}

#[tokio::test]
async fn metadata_overwrites_previously_cached_known_aliases() {
    let response = r#"{"tenant_discovery_endpoint":"present","metadata":[{"preferred_network":"selected.example","aliases":["login.windows.net"]}]}"#;
    let (endpoint, server) = server(vec![(200, response.into())]).await;
    let metadata = cache();
    metadata.lock().await.fallback("login.windows.net").unwrap();
    let first = authority("https://custom.example", "common", &endpoint, metadata.clone());
    assert_eq!(first.token_endpoint().await.unwrap().host_str(), Some("custom.example"));
    let next = authority("https://login.windows.net", "common", &endpoint, metadata);
    assert_eq!(next.token_endpoint().await.unwrap().host_str(), Some("selected.example"));
    server.await.unwrap();
}

#[tokio::test]
async fn adfs_and_ciam_skip_network_discovery_and_user_realm() {
    let (endpoint, server) = server(vec![]).await;
    server.await.unwrap();
    for (host, expected) in [
        ("https://example.org/ADFS", "https://example.org/adfs/oauth2/token"),
        ("https://tenant.ciamlogin.com", "https://tenant.ciamlogin.com/common/oauth2/v2.0/token"),
        ("https://login.windows.net/adfs", "https://login.microsoftonline.com/adfs/oauth2/token"),
    ] {
        let authority = authority(host, "common", &endpoint, cache());
        assert_eq!(authority.token_endpoint().await.unwrap().as_str(), expected);
        assert!(authority.user_realm_endpoint("developer@example.com").unwrap().is_none());
    }
}

#[test]
fn authority_rejects_b2c_and_malformed_configuration() {
    for host in ["https://tenant.b2clogin.com", "https://example.org/tfp", "http://example.org", "https://user:password@example.org", "https://example.org?query=x", "https://example.org#fragment", "https://example.org//tenant"] {
        assert!(Authority::with_metadata(host, "common", reqwest::Client::new(), cache()).is_err());
    }
    for tenant in ["", "invalid/path", "invalid?query", "invalid#fragment"] {
        assert!(Authority::with_metadata("https://example.org", tenant, reqwest::Client::new(), cache()).is_err());
    }
}

#[tokio::test]
async fn user_realm_uses_original_host_before_preferred_host_selection() {
    let (endpoint, server) = server(vec![]).await;
    server.await.unwrap();
    let authority = authority("https://login.windows.net:8443", "common", &endpoint, cache());
    assert_eq!(authority.user_realm_endpoint("developer@example.com").unwrap().unwrap().as_str(), "https://login.windows.net:8443/common/userrealm/developer@example.com?api-version=1.0");
    assert_eq!(authority.token_endpoint().await.unwrap().host_str(), Some("login.microsoftonline.com"));
    assert_eq!(authority.user_realm_endpoint("developer@example.com").unwrap().unwrap().host_str(), Some("login.windows.net"));
}

#[test]
fn discovery_json_retains_last_duplicate_scalar_values_and_ignores_unvisited_suffix() {
    let response = parse_response(br#"{"tenant_discovery_endpoint":"","tenant_discovery_endpoint":true,"metadata":[{"preferred_network":"first","preferred_network":123,"aliases":["one","one",true]}]}BROKEN"#).unwrap().unwrap();
    assert_eq!(response.tenant_discovery_endpoint.as_deref(), Some("true"));
    let entry = response.metadata.unwrap().pop().unwrap().unwrap();
    assert_eq!(entry.preferred_network.as_deref(), Some("123"));
    assert_eq!(entry.aliases.unwrap(), BTreeSet::from([Some("one".into()), Some("true".into())]));
}

#[test]
fn missing_metadata_aliases_fail_during_cache_publication() {
    let response = parse_response(br#"{"tenant_discovery_endpoint":"present","metadata":[{"preferred_network":"example.org"}]}"#).unwrap().unwrap();
    let mut metadata = MetadataCache::default();
    assert!(metadata.publish("example.org", response).is_err());
    assert!(metadata.aliases.is_empty());
    assert!(parse_response(br#"{"metadata":[{"aliases":null}]}"#).is_err());
}

#[test]
fn discovery_error_codes_require_json_numbers_in_java_long_range() {
    for value in ["0", "-9223372036854775808", "1.75", "1e2"] {
        assert!(parse_response(format!("{{\"error_codes\":[{value}]}}").as_bytes()).is_ok());
    }
    for value in ["\"1\"", "true", "null", "9223372036854775808", "1e500"] {
        assert!(parse_response(format!("{{\"error_codes\":[{value}]}}").as_bytes()).is_err());
    }
}

#[tokio::test]
async fn oversized_response_rejects_without_caching_or_exposing_its_payload() {
    let (endpoint, server) = server(vec![(200, "private ".repeat(MAX_RESPONSE_BYTES / 8 + 1))]).await;
    let metadata = cache();
    let authority = authority("https://custom.example", "common", &endpoint, metadata.clone());
    let error = authority.token_endpoint().await.unwrap_err();
    assert!(error.to_string().contains("byte limit"));
    assert!(!error.to_string().contains("private"));
    assert!(metadata.lock().await.aliases.is_empty());
    server.await.unwrap();
}

#[test]
fn metadata_cache_bounds_count_and_bytes_without_mutating_on_rejection() {
    let mut metadata = MetadataCache::default();
    for index in 0..MAX_CACHE_ALIASES { metadata.insert(format!("alias{index}.example"), Some("network.example".into()), true).unwrap(); }
    let bytes = metadata.bytes;
    assert!(metadata.insert("overflow.example".into(), Some("network.example".into()), true).is_err());
    assert_eq!(metadata.aliases.len(), MAX_CACHE_ALIASES);
    assert_eq!(metadata.bytes, bytes);
    assert!(metadata.insert("alias0.example".into(), Some("x".repeat(MAX_CACHE_BYTES)), true).is_err());
    assert_eq!(metadata.aliases["alias0.example"].as_deref(), Some("network.example"));
    assert_eq!(metadata.bytes, bytes);
}
