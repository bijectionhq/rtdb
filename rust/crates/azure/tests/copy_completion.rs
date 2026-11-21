use std::{collections::BTreeMap, io::Write, time::Duration};
use rtdb_azure::{AzureConfig, AzureCredentials};
use rtdb_storage::{IoRegion, ObjectKey};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::oneshot,
    time::timeout,
};

#[tokio::test]
async fn a_pending_copy_finishes_before_publication_and_staging_cleanup() {
    check_copy(true).await;
}

#[tokio::test]
async fn an_unconfirmed_copy_preserves_its_staging_source() {
    check_copy(false).await;
}

async fn check_copy(storage_reports_completion: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (done, mut stop) = oneshot::channel::<()>();
    let provider = tokio::spawn(async move {
        let mut copy_pending = false;
        let mut polls = 0;
        let mut removed = false;
        loop {
            let (connection, _) = tokio::select! {
                connection = listener.accept() => connection.unwrap(),
                _ = &mut stop => return (polls, removed),
            };
            let mut stream = BufReader::new(connection);
            let mut first = String::new();
            stream.read_line(&mut first).await.unwrap();
            let mut pieces = first.split_whitespace();
            let (method, url) = (pieces.next().unwrap(), pieces.next().unwrap());
            let mut headers = BTreeMap::new();
            loop {
                let mut line = String::new();
                stream.read_line(&mut line).await.unwrap();
                if line == "\r\n" { break; }
                let (name, value) = line.trim_end().split_once(':').unwrap();
                headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
            }
            if headers.get("expect").is_some_and(|value| value.eq_ignore_ascii_case("100-continue")) {
                stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await.unwrap();
            }
            let mut body = vec![0; headers.get("content-length").map_or(0, |length| length.parse().unwrap())];
            stream.read_exact(&mut body).await.unwrap();
            let (status, extra, response_body) = if method == "PUT" && url.ends_with("?restype=container") {
                ("201 Created", "", "")
            } else if method == "HEAD" && !copy_pending {
                ("404 Not Found", "x-ms-error-code: BlobNotFound\r\n", "")
            } else if method == "PUT" && (url.contains("?comp=block&") || url.ends_with("?comp=blocklist")) {
                ("201 Created", "", "")
            } else if method == "PUT" && headers.contains_key("x-ms-copy-source") {
                assert!(headers["x-ms-copy-source"].contains("/.tmp/"));
                assert!(!copy_pending);
                copy_pending = true;
                ("202 Accepted", "x-ms-copy-status: pending\r\nx-ms-copy-id: copy-fixture\r\n", "")
            } else if method == "HEAD" && copy_pending {
                assert!(headers.contains_key("authorization"), "the probe must be independently signed");
                polls += 1;
                if !storage_reports_completion && polls == 1 {
                    ("503 Service Unavailable", "", "")
                } else if !storage_reports_completion || polls == 1 {
                    ("200 OK", "x-ms-copy-status: pending\r\nx-ms-copy-id: copy-fixture\r\n", "")
                } else {
                    ("200 OK", "x-ms-copy-status: success\r\nx-ms-copy-id: copy-fixture\r\n", "")
                }
            } else if method == "POST" && url.contains("comp=batch") {
                assert!(String::from_utf8(body).unwrap().contains("/.tmp/upload-"));
                removed = true;
                ("202 Accepted", "Content-Type: multipart/mixed; boundary=phase\r\n",
                 "--phase\r\nContent-Type: application/http\r\nContent-ID: 0\r\n\r\nHTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n\r\n--phase--\r\n")
            } else {
                panic!("unexpected Azure operation {method} {url}");
            };
            let response = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\nETag: \"test\"\r\nLast-Modified: Sat, 21 Nov 2026 00:00:00 GMT\r\n{extra}\r\n{response_body}", response_body.len());
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    let mut config = AzureConfig::new("account", "container");
    config.storage_account_endpoint = Some(endpoint);
    config.storage_account_key = Some("a2V5".into());
    let io = IoRegion::new();
    let store = config.open(&ObjectKey::new("v06").unwrap(), &BTreeMap::<String, AzureCredentials>::new(), io.clone()).await.unwrap();
    let mut input = tempfile::NamedTempFile::new().unwrap();
    input.write_all(b"not-yet-copied").unwrap();
    let destination = ObjectKey::new("data.arrow").unwrap();
    let result = timeout(Duration::from_secs(10), store.put_file(&destination, input.path())).await.unwrap();
    if storage_reports_completion {
        result.unwrap();
    } else {
        assert!(result.unwrap_err().to_string().contains("copy outcome is unknown"));
        let retry = timeout(Duration::from_secs(10), store.put_file(&destination, input.path())).await.unwrap();
        assert!(retry.unwrap_err().to_string().contains("the destination copy is still pending"));
    }
    io.close_and_wait().await;
    done.send(()).unwrap();
    let (polls, removed) = timeout(Duration::from_secs(10), provider).await.unwrap().unwrap();
    if storage_reports_completion {
        assert_eq!((polls, removed), (2, true), "publication or cleanup preceded copy completion");
    } else {
        assert_eq!((polls, removed), (2, false), "an uncertain copy did not keep its staging source");
    }
}
