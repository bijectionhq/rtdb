use super::*;
use futures::StreamExt;
use rtdb_columnar::MemoryBudget;
use tokio::net::TcpListener;
use crate::connect::ConversionContext;

#[path = "../../../../tests/support/kafka_tls.rs"]
mod tls_support;

fn client(options: BTreeMap<String, String>, budget: &Arc<MemoryBudget>) -> Client {
    let mut context = ConversionContext::new(Arc::clone(budget));
    let (client, _) = super::super::security::client(&options, &mut context).unwrap();
    client
}

async fn head(socket: &mut (impl AsyncRead + Unpin)) -> Vec<u8> {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") { request.push(socket.read_u8().await.unwrap()); assert!(request.len() <= HEADER_LIMIT); }
    request
}

async fn collect(mut response: Response) -> Vec<u8> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.next().await { bytes.extend_from_slice(&chunk.unwrap()); }
    bytes
}

fn normalize_headers(bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let mut normalizer = HeaderNormalizer::new();
    for byte in bytes { if normalizer.push(*byte)? { return Ok(normalizer.output); } }
    Err(Error("Incomplete HTTP headers"))
}

#[tokio::test]
async fn chunked_response_runs_on_the_caller_and_retains_configuration() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = String::from_utf8(head(&mut socket).await).unwrap();
        assert!(request.starts_with("GET /schema?id=7 HTTP/1.1\r\n"));
        assert!(!request.contains("fragment"));
        socket.write_all(b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nX-Folded: first\r\n second\r\nTransfer-Encoding: chunked\r\n\r\n3\r\none\r\n3\r\ntwo\r\n0\r\n\r\n").await.unwrap();
    });
    let memory = MemoryBudget::new(1 << 27);
    let client = client(BTreeMap::new(), &memory);
    let retained = Arc::downgrade(&client.0);
    let response = client.request(Method::GET, Url::parse(&format!("http://{address}/schema?id=7#fragment")).unwrap(), HeaderMap::new(), Vec::new()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.content_length(), None);
    drop(client);
    assert!(retained.upgrade().is_some());
    assert_eq!(collect(response).await, b"onetwo");
    assert!(retained.upgrade().is_none());
    server.await.unwrap();
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn cancelling_a_pending_response_closes_its_socket_and_releases_request_memory() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (received, ready) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        head(&mut socket).await;
        received.send(()).unwrap();
        assert_eq!(timeout(Duration::from_secs(5), socket.read(&mut [0])).await.unwrap().unwrap(), 0);
    });
    let memory = MemoryBudget::new(1 << 27);
    let client = client(BTreeMap::new(), &memory);
    let baseline = memory.used_bytes();
    let mut request = Box::pin(client.request(Method::GET, Url::parse(&format!("http://{address}/schema")).unwrap(), HeaderMap::new(), Vec::new()));
    tokio::select! { result = &mut request => panic!("unexpected response: {}", result.is_ok()), _ = ready => () }
    assert!(memory.used_bytes() > baseline);
    drop(request);
    assert_eq!(memory.used_bytes(), baseline);
    server.await.unwrap();
    drop(client);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn explicit_http_proxy_receives_absolute_target_and_308_remains_visible() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = String::from_utf8(head(&mut socket).await).unwrap();
        assert!(request.starts_with("POST http://registry.invalid/schema?v=1 HTTP/1.1\r\n"));
        assert!(request.to_ascii_lowercase().contains("host: registry.invalid\r\n"));
        let mut body = [0; 2]; socket.read_exact(&mut body).await.unwrap(); assert_eq!(&body, b"{}");
        socket.write_all(b"HTTP/1.1 308 Permanent Redirect\r\nLocation: http://other.invalid/\r\nContent-Length: 0\r\n\r\n").await.unwrap();
    });
    let memory = MemoryBudget::new(1 << 27);
    let client = client(BTreeMap::from([("proxy.host".into(), address.ip().to_string()), ("proxy.port".into(), address.port().to_string())]), &memory);
    let response = client.request(Method::POST, Url::parse("http://registry.invalid/schema?v=1").unwrap(), HeaderMap::new(), b"{}".to_vec()).await.unwrap();
    assert_eq!(response.status(), StatusCode::PERMANENT_REDIRECT);
    assert!(collect(response).await.is_empty());
    server.await.unwrap();
}

#[tokio::test]
async fn read_timeout_bounds_idle_reads_without_limiting_total_response_duration() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        head(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n").await.unwrap();
        for _ in 0..4 { tokio::time::sleep(Duration::from_millis(150)).await; socket.write_all(b"a").await.unwrap(); }
    });
    let memory = MemoryBudget::new(1 << 27);
    let client = client(BTreeMap::from([("http.read.timeout.ms".into(), "400".into())]), &memory);
    let response = client.request(Method::GET, Url::parse(&format!("http://{address}/")).unwrap(), HeaderMap::new(), Vec::new()).await.unwrap();
    assert_eq!(collect(response).await, b"aaaa");
    server.await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        head(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n").await.unwrap();
        let _ = socket.read(&mut [0]).await;
    });
    let mut response = client.request(Method::GET, Url::parse(&format!("http://{address}/")).unwrap(), HeaderMap::new(), Vec::new()).await.unwrap();
    assert!(timeout(Duration::from_secs(5), response.next()).await.unwrap().unwrap().is_err());
    drop(response);
    server.await.unwrap();
}

#[tokio::test]
async fn https_proxy_tunnels_to_native_cbc_and_drops_tls_before_its_owner() {
    use openssl::ssl::{SslContextBuilder, SslMethod, SslVersion};
    let root = tls_support::Credential::new("registry root", false, None);
    let identity = tls_support::Credential::new("localhost", false, Some(&root));
    let mut context = SslContextBuilder::new(SslMethod::tls_server()).unwrap();
    context.set_min_proto_version(Some(SslVersion::TLS1_2)).unwrap();
    context.set_max_proto_version(Some(SslVersion::TLS1_2)).unwrap();
    context.set_cipher_list("ECDHE-RSA-AES128-SHA256").unwrap();
    context.set_certificate(&identity.certificate).unwrap();
    context.set_private_key(&identity.key).unwrap();
    let context = context.build();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let connect = String::from_utf8(head(&mut socket).await).unwrap();
        assert!(connect.starts_with("CONNECT localhost:443 HTTP/1.1\r\n"));
        tokio::time::sleep(Duration::from_millis(300)).await;
        socket.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n").await.unwrap();
        let mut socket = SslStream::new(Ssl::new(&context).unwrap(), socket).unwrap();
        Pin::new(&mut socket).accept().await.unwrap();
        let request = String::from_utf8(head(&mut socket).await).unwrap();
        assert!(request.starts_with("GET /schema HTTP/1.1\r\n"));
        assert_eq!(socket.ssl().current_cipher().unwrap().name(), "ECDHE-RSA-AES128-SHA256");
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}").await.unwrap();
        socket.shutdown().await.unwrap();
    });
    let memory = MemoryBudget::new(1 << 27);
    let client = client(BTreeMap::from([
        ("proxy.host".into(), address.ip().to_string()), ("proxy.port".into(), address.port().to_string()),
        ("http.connect.timeout.ms".into(), "100".into()), ("http.read.timeout.ms".into(), "3000".into()),
        ("schema.registry.ssl.truststore.type".into(), "PEM".into()),
        ("schema.registry.ssl.truststore.certificates".into(), String::from_utf8(root.certificate.to_pem().unwrap()).unwrap()),
    ]), &memory);
    let weak = Arc::downgrade(&client.0);
    let response = client.request(Method::GET, Url::parse("https://localhost/schema").unwrap(), HeaderMap::new(), Vec::new()).await.unwrap();
    drop(client);
    assert!(weak.upgrade().is_some());
    assert_eq!(collect(response).await, b"{}");
    assert!(weak.upgrade().is_none());
    server.await.unwrap();
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn request_memory_is_admitted_before_opening_a_connection() {
    let memory = MemoryBudget::new(1 << 27);
    let client = client(BTreeMap::new(), &memory);
    let remaining = memory.limit_bytes() - memory.used_bytes();
    let hold = memory.reserve(remaining).unwrap();
    let error = client.request(Method::GET, Url::parse("http://registry.invalid/").unwrap(), HeaderMap::new(), Vec::new()).await.err().unwrap();
    assert_eq!(error.to_string(), "HTTP memory limit exceeded");
    drop(hold);
}

#[test]
fn invalid_http_options_destroy_the_native_context_before_its_last_owner() {
    for options in [
        BTreeMap::from([("proxy.host".into(), "host/path".into()), ("proxy.port".into(), "80".into())]),
        BTreeMap::from([("http.read.timeout.ms".into(), "2147483648".into())]),
        BTreeMap::from([("http.connect.timeout.ms".into(), "-1".into())]),
        BTreeMap::from([("proxy.port".into(), "invalid".into())]),
    ] {
        let memory = MemoryBudget::new(1 << 27);
        let client = client(BTreeMap::new(), &memory);
        let Configuration { tls, _owner: owner, charge, .. } = Arc::try_unwrap(client.0).ok().unwrap();
        let weak = Arc::downgrade(&owner);
        assert!(Client::new(tls, owner, true, &options, charge).is_err());
        assert!(weak.upgrade().is_none());
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn proxy_admission_preserves_java_integer_gating_and_raw_ipv6_hosts() {
    let memory = MemoryBudget::new(1 << 27);
    for (host, port, expected) in [("::1", "80", Some(("::1", 80))), ("[::1]", "80", Some(("::1", 80))), ("localhost", "-1", None), ("", "100000", None)] {
        let client = client(BTreeMap::from([("proxy.host".into(), host.into()), ("proxy.port".into(), port.into())]), &memory);
        assert_eq!(client.0.proxy.as_ref().map(|(host, port)| (host.as_str(), *port)), expected);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn redirect_post_rewrites_to_get_and_clears_request_headers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = String::from_utf8(head(&mut socket).await).unwrap();
        assert!(request.starts_with("POST /first HTTP/1.1\r\n"));
        let mut body = [0; 2]; socket.read_exact(&mut body).await.unwrap();
        socket.write_all(b"HTTP/1.1 302 Found\r\nLocation: /second\r\nContent-Length: 0\r\n\r\n").await.unwrap();
        drop(socket);
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = String::from_utf8(head(&mut socket).await).unwrap().to_ascii_lowercase();
        assert!(request.starts_with("get /second http/1.1\r\n"));
        for name in ["authorization:", "content-type:", "x-custom:"] { assert!(!request.contains(name)); }
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}").await.unwrap();
    });
    let memory = MemoryBudget::new(1 << 27);
    let client = client(BTreeMap::new(), &memory);
    let headers = HeaderMap::from_iter([(AUTHORIZATION, HeaderValue::from_static("Bearer private")), (hyper::header::CONTENT_TYPE, HeaderValue::from_static("application/json")), (hyper::header::HeaderName::from_static("x-custom"), HeaderValue::from_static("value"))]);
    let response = client.request(Method::POST, Url::parse(&format!("http://{address}/first")).unwrap(), headers, b"{}".to_vec()).await.unwrap();
    assert_eq!(collect(response).await, b"{}");
    server.await.unwrap();
}

#[tokio::test]
async fn redirect_307_retains_post_body_but_strips_cross_destination_credentials() {
    let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_address = first.local_addr().unwrap();
    let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_address = second.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = first.accept().await.unwrap(); head(&mut socket).await;
        let mut body = [0; 2]; socket.read_exact(&mut body).await.unwrap();
        socket.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{second_address}/second\r\nContent-Length: 0\r\n\r\n").as_bytes()).await.unwrap();
        drop(socket);
        let (mut socket, _) = second.accept().await.unwrap();
        let request = String::from_utf8(head(&mut socket).await).unwrap().to_ascii_lowercase();
        assert!(request.starts_with("post /second http/1.1\r\n"));
        for name in ["authorization:", "cookie:", "cookie2:"] { assert!(!request.contains(name)); }
        assert!(request.contains("x-custom: value\r\n"));
        socket.read_exact(&mut body).await.unwrap(); assert_eq!(&body, b"{}");
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").await.unwrap();
    });
    let memory = MemoryBudget::new(1 << 27);
    let client = client(BTreeMap::new(), &memory);
    let mut headers = HeaderMap::new();
    for name in ["authorization", "cookie", "cookie2", "x-custom"] { headers.insert(hyper::header::HeaderName::from_static(name), HeaderValue::from_static("value")); }
    let response = client.request(Method::POST, Url::parse(&format!("http://{first_address}/first")).unwrap(), headers, b"{}".to_vec()).await.unwrap();
    assert!(collect(response).await.is_empty());
    server.await.unwrap();
}

#[tokio::test]
async fn large_header_counts_are_admitted_and_truncated_bodies_fail() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap(); head(&mut socket).await;
        let mut response = String::from("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n");
        for index in 0..120 { response.push_str(&format!("X-Header-{index}: value\r\n")); }
        response.push_str("\r\n{}");
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    let memory = MemoryBudget::new(1 << 27);
    let client = client(BTreeMap::new(), &memory);
    let mut response = client.request(Method::GET, Url::parse(&format!("http://{address}/")).unwrap(), HeaderMap::new(), Vec::new()).await.unwrap();
    assert_eq!(response.content_length(), Some(5));
    let mut failed = false;
    while let Some(chunk) = response.next().await { if chunk.is_err() { failed = true; break; } }
    assert!(failed);
    server.await.unwrap();
}

#[tokio::test]
async fn redirects_stop_at_scheme_changes_and_twenty_responses() {
    let memory = MemoryBudget::new(1 << 27);
    let client = client(BTreeMap::new(), &memory);
    for (cross_scheme, count) in [(true, 1), (false, 20)] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..count {
                let (mut socket, _) = listener.accept().await.unwrap(); head(&mut socket).await;
                let location = if cross_scheme { format!("https://{address}/") } else { "/again".into() };
                socket.write_all(format!("HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\n\r\n").as_bytes()).await.unwrap();
            }
        });
        let result = client.request(Method::GET, Url::parse(&format!("http://{address}/")).unwrap(), HeaderMap::new(), Vec::new()).await;
        if cross_scheme { assert_eq!(result.unwrap().status(), StatusCode::FOUND); }
        else { assert_eq!(result.err().unwrap().to_string(), "HTTP redirects exceeded their limit"); }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn redirect_305_changes_proxy_without_changing_the_origin_target() {
    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = origin.local_addr().unwrap();
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = origin.accept().await.unwrap(); head(&mut socket).await;
        socket.write_all(format!("HTTP/1.1 305 Use Proxy\r\nLocation: http://{proxy_address}/\r\nContent-Length: 0\r\n\r\n").as_bytes()).await.unwrap();
        drop(socket);
        let (mut socket, _) = proxy.accept().await.unwrap();
        let request = String::from_utf8(head(&mut socket).await).unwrap();
        assert!(request.starts_with(&format!("GET http://{address}/schema HTTP/1.1\r\n")));
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").await.unwrap();
    });
    let memory = MemoryBudget::new(1 << 27);
    let client = client(BTreeMap::new(), &memory);
    let response = client.request(Method::GET, Url::parse(&format!("http://{address}/schema")).unwrap(), HeaderMap::new(), Vec::new()).await.unwrap();
    assert!(collect(response).await.is_empty());
    server.await.unwrap();
}

#[test]
fn response_header_limit_includes_status_reason_and_untrimmed_field_length() {
    let value_length = HEADER_LIMIT - (32 + 13 + 4096) - (32 + 3);
    for extra in [0, 1] {
        let mut headers = b"HTTP/1.1 200 ".to_vec();
        headers.extend(std::iter::repeat_n(b'r', 4096));
        headers.extend_from_slice(b"\r\nX: ");
        headers.extend(std::iter::repeat_n(b'v', value_length + extra));
        headers.extend_from_slice(b"\r\n\r\n");
        assert_eq!(normalize_headers(&headers).is_ok(), extra == 0);
    }
}

#[test]
fn proxy_header_limit_counts_fields_and_unfolds_continuations() {
    let mut many = b"HTTP/1.1 200 OK\r\n".to_vec();
    for _ in 0..12_000 { many.extend_from_slice(b"X: a\r\n"); }
    many.extend_from_slice(b"\r\n");
    assert!(many.len() < HEADER_LIMIT);
    assert!(normalize_headers(&many).is_err());
    for extra in [0, 1] {
        let mut headers = b"HTTP/1.1 200 OK\r\nX: ".to_vec();
        headers.extend(std::iter::repeat_n(b'v', HEADER_LIMIT - 82 + extra));
        headers.extend_from_slice(b"\r\n\r\n");
        assert_eq!(normalize_headers(&headers).is_ok(), extra == 0);
    }
    let mut folded = b"HTTP/1.1 200 OK\r\nX: ".to_vec();
    folded.extend(std::iter::repeat_n(b'v', HEADER_LIMIT - 84));
    folded.extend_from_slice(b"\r\n a\r\n\r\n");
    assert!(normalize_headers(&folded).is_ok());
}

#[test]
fn aggregate_wire_whitespace_does_not_consume_the_normalized_header_allowance() {
    let mut raw = b"HTTP/1.1 200 OK\r\n".to_vec();
    for _ in 0..4 {
        raw.extend_from_slice(b"X-Pad: ");
        raw.extend(std::iter::repeat_n(b' ', HEADER_LIMIT / 2));
        raw.extend_from_slice(b"value \t\r\n");
    }
    raw.extend_from_slice(b"\r\n");
    assert!(raw.len() > 2 * HEADER_LIMIT);
    assert_eq!(normalize_headers(&raw).unwrap(), b"HTTP/1.1 200 OK\r\nX-Pad:value\r\nX-Pad:value\r\nX-Pad:value\r\nX-Pad:value\r\n\r\n");
}

#[test]
fn unfinished_raw_records_are_rejected_before_discarding_whitespace() {
    let mut raw = b"HTTP/1.1 200 OK\r\nX: ".to_vec();
    raw.extend(std::iter::repeat_n(b' ', HEADER_LIMIT - 81));
    assert_eq!(normalize_headers(&raw).unwrap_err().0, "HTTP headers exceed their byte limit");
}

#[test]
fn folded_records_preserve_java_tab_and_line_ending_normalization() {
    for ending in [b"\r\n".as_slice(), b"\n".as_slice(), b"\r".as_slice()] {
        let lines: [&[u8]; 5] = [b"HTTP/1.1 200 Fine\tReason", b"X:\t first \t", b"\t second\tpart \t", b"Y: value", b""];
        let mut raw = lines.join(ending); raw.extend_from_slice(ending);
        assert_eq!(normalize_headers(&raw).unwrap(), b"HTTP/1.1 200 Fine Reason\r\nX:first    second part\r\nY:value\r\n\r\n");
    }
    assert!(normalize_headers(b"HTTP/1.1 101 Switching Protocols\r\n\r\n").is_err());
    for empty in [b"\r\n".as_slice(), b"\n".as_slice()] { assert_eq!(normalize_headers(empty).unwrap_err().0, "HTTP response status is absent"); }
}

#[tokio::test]
async fn oversized_wire_heads_normalize_before_hyper_and_preserve_interim_chunked_bodies() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap(); head(&mut socket).await;
        let mut response = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nX-Interim: ".to_vec();
        response.extend(std::iter::repeat_n(b'v', HEADER_LIMIT / 2));
        response.extend_from_slice(b"\r\n\r\nHTTP/1.1 200 OK\r\n");
        for _ in 0..4 {
            response.extend_from_slice(b"X-Pad: "); response.extend(std::iter::repeat_n(b' ', HEADER_LIMIT / 2)); response.extend_from_slice(b"v\r\n");
        }
        response.extend_from_slice(b"X-Final: "); response.extend(std::iter::repeat_n(b'v', HEADER_LIMIT / 2)); response.extend_from_slice(b"\r\n");
        response.extend_from_slice(b"X-Folded: one\r\n\t two\r\nTransfer-Encoding: chunked\r\n\r\n3\r\none\r\n3\r\ntwo\r\n0\r\n\r\n");
        for bytes in response.chunks(4093) { socket.write_all(bytes).await.unwrap(); }
    });
    let memory = MemoryBudget::new(1 << 27); let client = client(BTreeMap::new(), &memory);
    let response = client.request(Method::GET, Url::parse(&format!("http://{address}/")).unwrap(), HeaderMap::new(), Vec::new()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(collect(response).await, b"onetwo");
    server.await.unwrap(); drop(client);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn connect_normalizes_large_wire_heads_without_consuming_tls_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap(); head(&mut socket).await;
        let mut response = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 Connection established\r\n".to_vec();
        for _ in 0..4 {
            response.extend_from_slice(b"X: "); response.extend(std::iter::repeat_n(b' ', HEADER_LIMIT / 2)); response.extend_from_slice(b"v\r\n");
        }
        response.extend_from_slice(b"\r\n\x16\x03\x03\x00\x01");
        socket.write_all(&response).await.unwrap();
    });
    let socket = TcpStream::connect(address).await.unwrap();
    let mut socket = ReadTimeout { socket, duration: Some(Duration::from_secs(5)), deadline: None };
    tunnel(&mut socket, "registry.invalid", 443).await.unwrap();
    let mut following = [0; 5]; socket.read_exact(&mut following).await.unwrap();
    assert_eq!(following, [0x16, 3, 3, 0, 1]);
    server.await.unwrap();
}

#[tokio::test]
async fn cancelling_partial_header_normalization_closes_the_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap(); let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut response = b"HTTP/1.1 200 OK\r\nX: ".to_vec(); response.extend(std::iter::repeat_n(b' ', 65_536));
        socket.write_all(&response).await.unwrap();
        assert_eq!(timeout(Duration::from_secs(5), socket.read(&mut [0])).await.unwrap().unwrap(), 0);
    });
    let socket = TcpStream::connect(address).await.unwrap();
    let socket = ReadTimeout { socket, duration: Some(Duration::from_secs(5)), deadline: None };
    let mut transport = HttpTransport::new(Transport::Plain(socket));
    poll_fn(|cx| {
        let mut bytes = [0; 16]; let mut output = ReadBuf::new(&mut bytes);
        assert!(Pin::new(&mut transport).poll_read(cx, &mut output).is_pending());
        assert!(output.filled().is_empty());
        if matches!(&transport.state, HttpInput::Headers(headers) if headers.record.len() == 65_536 + 3) { Poll::Ready(()) } else { Poll::Pending }
    }).await;
    drop(transport);
    server.await.unwrap();
}

#[tokio::test]
async fn raw_http_header_admission_precedes_hypers_whitespace_normalization() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap(); head(&mut socket).await;
        let mut response = b"HTTP/1.1 200 OK\r\nX: ".to_vec();
        response.extend(std::iter::repeat_n(b'v', HEADER_LIMIT - 81));
        response.extend_from_slice(b"\r\n\r\n");
        let _ = socket.write_all(&response).await;
    });
    let memory = MemoryBudget::new(1 << 27);
    let client = client(BTreeMap::new(), &memory);
    assert!(client.request(Method::GET, Url::parse(&format!("http://{address}/")).unwrap(), HeaderMap::new(), Vec::new()).await.is_err());
    server.await.unwrap();
}
