use super::*;
use std::{io::{Read, Write}, net::TcpListener};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use openssl::{asn1::Asn1Time, bn::BigNum, hash::MessageDigest, pkey::PKey, rsa::Rsa, ssl::{SslAcceptor, SslMethod}, x509::{X509, X509NameBuilder, extension::{BasicConstraints, ExtendedKeyUsage, SubjectAlternativeName}}};
use url::Url;

type ServerTask = tokio::task::JoinHandle<Vec<String>>;

fn token(name: &str, lifetime: i64) -> String {
    let mut token = serde_json::json!({"access_token":name,"expires_in":lifetime,"refresh_token":"rotated-refresh"});
    token["id_token"] = format!("header.{}.signature", URL_SAFE_NO_PAD.encode(br#"{"sub":"user"}"#)).into();
    token["client_info"] = URL_SAFE_NO_PAD.encode(br#"{"uid":"user","utid":"tenant"}"#).into();
    token.to_string()
}

fn server(responses: impl FnOnce(&Url) -> Vec<(u16, String)>) -> (Url, reqwest::Client, ServerTask) {
    let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
    let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", "127.0.0.1").unwrap(); let name = name.build();
    let mut certificate = X509::builder().unwrap();
    certificate.set_version(2).unwrap();
    certificate.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
    certificate.set_subject_name(&name).unwrap(); certificate.set_issuer_name(&name).unwrap(); certificate.set_pubkey(&key).unwrap();
    certificate.set_not_before(&Asn1Time::days_from_now(0).unwrap()).unwrap(); certificate.set_not_after(&Asn1Time::days_from_now(1).unwrap()).unwrap();
    certificate.append_extension(BasicConstraints::new().critical().build().unwrap()).unwrap();
    certificate.append_extension(ExtendedKeyUsage::new().server_auth().build().unwrap()).unwrap();
    let san = SubjectAlternativeName::new().ip("127.0.0.1").build(&certificate.x509v3_context(None, None)).unwrap();
    certificate.append_extension(san).unwrap(); certificate.sign(&key, MessageDigest::sha256()).unwrap(); let certificate = certificate.build();
    let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    acceptor.set_private_key(&key).unwrap(); acceptor.set_certificate(&certificate).unwrap(); let acceptor = acceptor.build();
    let client = reqwest::Client::builder().add_root_certificate(reqwest::Certificate::from_der(&certificate.to_der().unwrap()).unwrap())
        .redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(10)).build().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap(); listener.set_nonblocking(true).unwrap();
    let endpoint = Url::parse(&format!("https://{}/", listener.local_addr().unwrap())).unwrap();
    let responses = responses(&endpoint);
    let task = tokio::task::spawn_blocking(move || {
        let mut requests = Vec::new();
        for (status, body) in responses {
            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            let socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock && std::time::Instant::now() < deadline => std::thread::sleep(Duration::from_millis(5)),
                    Err(error) => panic!("test server failed: {error}"),
                }
            };
            socket.set_nonblocking(false).unwrap(); socket.set_read_timeout(Some(Duration::from_secs(10))).unwrap(); socket.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
            let mut socket = acceptor.accept(socket).unwrap();
            let mut request = Vec::new();
            loop {
                let mut bytes = [0; 1024]; let count = socket.read(&mut bytes).unwrap(); assert!(count > 0); request.extend_from_slice(&bytes[..count]); assert!(request.len() <= 2 * 1024 * 1024);
                if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&request[..end]).unwrap();
                    let length: usize = headers.lines().find_map(|line| line.split_once(':').filter(|(name, _)| name.eq_ignore_ascii_case("content-length")).map(|(_, value)| value.trim().parse().unwrap())).unwrap_or(0);
                    if request.len() >= end + 4 + length { break; }
                }
            }
            requests.push(String::from_utf8(request).unwrap());
            let _ = socket.write_all(format!("HTTP/1.1 {status} Result\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes());
        }
        requests
    });
    (endpoint, client, task)
}

fn credential(endpoint: &Url, client: reqwest::Client, realm: bool) -> PasswordCredential {
    PasswordCredential {
        authority: Authority::static_endpoints(endpoint.join("common/oauth2/v2.0/token").unwrap(), realm.then(|| endpoint.join("realm").unwrap())),
        client, client_id: "application-id".into(), username: Secret::new("user+name@example.org"), password: Secret::new("private&password"), sessions: Mutex::new(Sessions::for_client_id("application-id".into())),
    }
}
fn form(request: &str) -> BTreeMap<String, String> { url::form_urlencoded::parse(request.split_once("\r\n\r\n").unwrap().1.as_bytes()).into_owned().collect() }

#[tokio::test]
async fn password_endpoint_fixture_validates_the_server_certificate() {
    let (endpoint, client, server) = server(|_| vec![(200, "ready".into())]);
    assert_eq!(client.get(endpoint).send().await.unwrap().text().await.unwrap(), "ready");
    assert_eq!(server.await.unwrap().len(), 1);
}

#[tokio::test]
async fn managed_password_flow_discovers_realm_exchanges_form_and_reuses_cached_token() {
    let (endpoint, client, server) = server(|_| vec![(200, r#"{"account_type":"Managed"}"#.into()), (200, token("managed", 3600))]);
    let credential = credential(&endpoint, client, true);
    assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "managed");
    assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "managed");
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with("GET /realm HTTP/1.1"));
    assert!(requests[0].to_ascii_lowercase().contains("accept: application/json, text/javascript, */*"));
    let form = form(&requests[1]);
    assert_eq!(form["client_id"], "application-id"); assert_eq!(form["grant_type"], "password");
    assert_eq!(form["username"], "user+name@example.org"); assert_eq!(form["password"], "private&password"); assert_eq!(form["client_info"], "1");
    assert!(form["scope"].split(' ').any(|scope| scope == STORAGE_SCOPE));
}

#[tokio::test]
async fn account_refresh_avoids_realm_discovery_and_rotates_token() {
    let (endpoint, client, server) = server(|_| vec![(200, token("initial", 1)), (200, token("refreshed", 3600))]);
    let credential = credential(&endpoint, client, false);
    assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "initial");
    assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "refreshed");
    let requests = server.await.unwrap();
    assert_eq!(form(&requests[1])["grant_type"], "refresh_token"); assert_eq!(form(&requests[1])["refresh_token"], "rotated-refresh");
    assert!(!form(&requests[1]).contains_key("password"));
}

#[tokio::test]
async fn failed_refresh_repeats_password_authentication() {
    let (endpoint, client, server) = server(|_| vec![(200, token("initial", 1)), (400, "private service detail".into()), (200, token("retried", 3600))]);
    let credential = credential(&endpoint, client, false);
    assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "initial");
    assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "retried");
    let requests = server.await.unwrap();
    assert_eq!(form(&requests[2])["grant_type"], "password");
}

#[tokio::test]
async fn missing_account_does_not_refresh_returned_refresh_token() {
    let (endpoint, client, server) = server(|_| vec![(200, r#"{"access_token":"first","expires_in":1,"refresh_token":"unused"}"#.into()), (200, token("second", 3600))]);
    let credential = credential(&endpoint, client, false);
    assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "first");
    assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "second");
    let requests = server.await.unwrap(); assert_eq!(form(&requests[1])["grant_type"], "password");
}

#[tokio::test]
async fn federated_password_flow_exchanges_saml_and_preserves_original_credentials() {
    for old in [false, true] {
        let (endpoint, client, server) = server(|base| {
            let namespace = if old { "http://schemas.xmlsoap.org/ws/2005/02/trust" } else { "http://docs.oasis-open.org/ws-sx/ws-trust/200512" };
            let action = format!("{namespace}/RST/Issue");
            let mex = format!("<w:definitions xmlns:w='http://schemas.xmlsoap.org/wsdl/' xmlns:p='http://schemas.xmlsoap.org/ws/2004/09/policy' xmlns:s='http://schemas.xmlsoap.org/ws/2005/07/securitypolicy' xmlns:wsu='urn:ignored-prefix-uri' xmlns:soap='http://schemas.xmlsoap.org/wsdl/soap12/' xmlns:a='http://www.w3.org/2005/08/addressing'><p:Policy wsu:Id='policy'><p:ExactlyOne><p:All><s:TransportBinding/><s:SignedSupportingTokens><p:Policy><s:UsernameToken><p:Policy><s:WssUsernameToken10/></p:Policy></s:UsernameToken></p:Policy></s:SignedSupportingTokens></p:All></p:ExactlyOne></p:Policy><w:binding name='binding'><p:PolicyReference URI='#policy'/><soap:binding transport='http://schemas.xmlsoap.org/soap/http'/><w:operation><soap:operation soapAction='{action}'/></w:operation></w:binding><w:service><w:port binding='w:binding'><a:EndpointReference><a:Address>{}trust</a:Address></a:EndpointReference></w:port></w:service></w:definitions>", base);
            let token = "<t:RequestSecurityTokenResponse><t:TokenType>urn:oasis:names:tc:SAML:1.0:assertion</t:TokenType><t:RequestedSecurityToken><Assertion>signed</Assertion></t:RequestedSecurityToken></t:RequestSecurityTokenResponse>";
            let token = if old { token.to_owned() } else { format!("<t:RequestSecurityTokenResponseCollection>{token}</t:RequestSecurityTokenResponseCollection>") };
            let trust = format!("<e:Envelope xmlns:e='http://www.w3.org/2003/05/soap-envelope' xmlns:t='{namespace}'><e:Body>{token}</e:Body></e:Envelope>");
            vec![(200, serde_json::json!({"account_type":"Federated","federation_protocol":"unknown","federation_metadata_url":format!("{base}mex"),"cloud_audience_urn":"urn:custom:audience"}).to_string()), (200, mex), (500, trust), (200, token_response())]
        });
        let credential = credential(&endpoint, client, true);
        assert_eq!(credential.get_token(&[STORAGE_SCOPE], None).await.unwrap().token.secret(), "federated");
        let requests = server.await.unwrap(); assert_eq!(requests.len(), 4);
        assert!(requests[1].starts_with("GET /mex HTTP/1.1")); assert!(requests[2].starts_with("POST /trust HTTP/1.1"));
        assert!(requests[2].contains("private&amp;password")); assert!(requests[2].contains("urn:custom:audience"));
        let body = form(&requests[3]);
        assert_eq!(body["grant_type"], "urn:ietf:params:oauth:grant-type:saml1_1-bearer");
        assert_eq!(body["password"], "private&password"); assert_eq!(body["username"], "user+name@example.org");
        assert_eq!(base64::engine::general_purpose::URL_SAFE.decode(&body["assertion"]).unwrap(), b"<Assertion>signed</Assertion>");
    }
    fn token_response() -> String { token("federated", 3600) }
}

#[tokio::test]
async fn endpoint_failure_redacts_service_payload_and_rejects_before_token_exchange() {
    let (endpoint, client, server) = server(|_| vec![(400, "private-user private-password".into())]);
    let credential = credential(&endpoint, client, true);
    let error = credential.get_token(&[STORAGE_SCOPE], None).await.unwrap_err();
    assert!(!error.to_string().contains("private-user")); assert!(!error.to_string().contains("private-password"));
    assert_eq!(server.await.unwrap().len(), 1);
}

#[test]
fn username_password_environment_defaults_tenant_and_debug_redacts_principal() {
    let environment = BTreeMap::from([("AZURE_CLIENT_ID", "application".into()), ("AZURE_USERNAME", "private-user".into()), ("AZURE_PASSWORD", "private-password".into())]);
    let credential = PasswordCredential::new(&environment).unwrap();
    assert_eq!(credential.authority.configured_token_endpoint().path(), "/organizations/oauth2/v2.0/token");
    assert!(!format!("{credential:?}").contains("private-user")); assert!(!format!("{credential:?}").contains("private-password"));
    for field in ["AZURE_CLIENT_ID", "AZURE_USERNAME", "AZURE_PASSWORD"] { let mut incomplete = environment.clone(); incomplete.remove(field); assert!(PasswordCredential::new(&incomplete).is_err()); }
}
