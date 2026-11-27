use std::{
    fs, io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_rustls::{TlsAcceptor, rustls};

use super::{Binding, Closure, Enrollment, Resolution, Uuid};
use crate::multi::activation::funded::issuer::{Providers, Settings};

pub(super) struct OriginalIssuer {
    _directory: tempfile::TempDir,
    task: JoinHandle<()>,
    requests: Arc<AtomicUsize>,
    pub(super) providers: Providers,
    pub(super) profile: Value,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ReplyBehavior {
    Exact,
    LoseAndSubstitute,
}

impl OriginalIssuer {
    pub(super) async fn start(
        enrollment: Enrollment,
        authority_id: Uuid,
        database: &'static str,
        predecessor: Binding,
        closure: Closure,
        resolutions: Vec<Resolution>,
        expected_fence: Value,
        replies: ReplyBehavior,
    ) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let mut ca_profile = CertificateParams::default();
        ca_profile.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_key = KeyPair::generate().unwrap();
        let ca = ca_profile.self_signed(&ca_key).unwrap();
        let issuer = rcgen::Issuer::new(ca_profile, ca_key);
        let leaf = |purpose| {
            let mut profile = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
            profile.extended_key_usages = vec![purpose];
            let key = KeyPair::generate().unwrap();
            (profile.signed_by(&key, &issuer).unwrap(), key)
        };
        let (server, server_key) = leaf(ExtendedKeyUsagePurpose::ServerAuth);
        let (client, client_key) = leaf(ExtendedKeyUsagePurpose::ClientAuth);
        let ca_file = directory.path().join("ca.pem");
        let certificate_file = directory.path().join("client.pem");
        let key_file = directory.path().join("client-key.pem");
        fs::write(&ca_file, ca.pem()).unwrap();
        fs::write(&certificate_file, client.pem()).unwrap();
        fs::write(&key_file, client_key.serialize_pem()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&key_file, fs::Permissions::from_mode(0o600)).unwrap();
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pin: [u8; 32] = Sha256::digest(server.der()).into();
        let profile = json!({
            "enrollment": enrollment,
            "connection": {
                "endpoint": format!("https://{}", listener.local_addr().unwrap()),
                "ca_file": ca_file,
                "client_certificate_file": certificate_file,
                "client_private_key_file": key_file,
                "server_certificate_sha256": pin,
            },
        });
        let settings: Settings = serde_json::from_value(json!([profile])).unwrap();
        let providers = settings
            .connect(|path| Ok(fs::File::open(path).unwrap()))
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca.der().clone()).unwrap();
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            provider.clone(),
        )
        .build()
        .unwrap();
        let mut config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                vec![server.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
            )
            .unwrap();
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let requests = Arc::new(AtomicUsize::new(0));
        let requests_seen = requests.clone();
        let task = tokio::spawn(async move {
            let outcomes = Arc::new(resolutions);
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let incoming = acceptor.accept(stream).await.unwrap();
                assert_eq!(
                    &incoming.get_ref().1.peer_certificates().unwrap()[0],
                    client.der()
                );
                let service = service_fn(|request: Request<Incoming>| {
                    let outcomes = outcomes.clone();
                    let expected_fence = expected_fence.clone();
                    let closure = closure.clone();
                    let requests_seen = requests_seen.clone();
                    async move {
                        assert_eq!(request.uri().path(), "/rtdb/funded-issuer/v1/recovery");
                        let body = request.into_body().collect().await.unwrap().to_bytes();
                        let decoded: Value = serde_json::from_slice(&body).unwrap();
                        assert_eq!(decoded["version"], 1);
                        assert_eq!(decoded["database"], database);
                        assert_eq!(decoded["authority_id"], json!(authority_id));
                        assert_eq!(decoded["predecessor"], json!(predecessor));
                        assert_eq!(decoded["scope_sha256"], json!(closure.scope_sha256));
                        let call = requests_seen.fetch_add(1, Ordering::Relaxed);
                        let action = &decoded["action"];
                        let result = match action["kind"].as_str() {
                            Some("close")
                                if call == 0 && replies == ReplyBehavior::LoseAndSubstitute =>
                            {
                                return Err(io::Error::other("lost closure reply"));
                            }
                            Some("close") => json!(closure),
                            Some("outcome") => {
                                let index = action["index"].as_u64().unwrap();
                                assert_eq!(
                                    action["inventory_sha256"],
                                    json!(closure.inventory_sha256)
                                );
                                assert_eq!(action["source_fence"], expected_fence);
                                match (replies, call) {
                                    (ReplyBehavior::LoseAndSubstitute, 2) => {
                                        return Err(io::Error::other("lost resolution reply"));
                                    }
                                    (ReplyBehavior::LoseAndSubstitute, 3) => {
                                        json!(outcomes[index as usize])
                                    }
                                    _ => json!(outcomes[index as usize - 1]),
                                }
                            }
                            _ => panic!("invalid funded issuer recovery request"),
                        };
                        let request_sha256: [u8; 32] = Sha256::digest(&body).into();
                        let reply = json!({ "version": 1, "request_sha256": request_sha256, "result": result });
                        Ok(Response::builder()
                            .header("content-type", "application/json")
                            .header("connection", "close")
                            .body(Full::new(Bytes::from(serde_json::to_vec(&reply).unwrap())))
                            .unwrap())
                    }
                });
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(incoming), service)
                    .await;
            }
        });
        Self {
            _directory: directory,
            task,
            requests,
            providers,
            profile,
        }
    }

    pub(super) fn requests(&self) -> usize {
        self.requests.load(Ordering::Relaxed)
    }
}

impl Drop for OriginalIssuer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
