use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tokio_rustls::rustls::{
    self, ClientConfig, RootCertStore,
    client::Resumption,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use url::Url;
use zeroize::Zeroizing;

use super::classic_opening::operator::LeafVerifier;

const MAX_PEM_BYTES: usize = 64 << 10;

#[derive(Debug)]
pub(super) enum Error {
    Configuration,
    Unavailable,
    Refused,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Profile {
    endpoint: String,
    pub(super) ca_file: PathBuf,
    pub(super) client_certificate_file: PathBuf,
    pub(super) client_private_key_file: PathBuf,
    server_certificate_sha256: [u8; 32],
}

pub(super) struct Connection {
    endpoint: Url,
    client: reqwest::Client,
    max_bytes: usize,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Reply<T> {
    pub(super) version: u32,
    pub(super) request_sha256: [u8; 32],
    pub(super) result: T,
}

impl Connection {
    pub(super) fn from_profile(
        profile: Profile,
        max_bytes: usize,
        deadline: Duration,
    ) -> Result<Self, Error> {
        let key = read_key(&profile.client_private_key_file)?;
        Self::from_profile_with_key(profile, key, max_bytes, deadline)
    }

    pub(super) fn from_profile_with_key(
        profile: Profile,
        key: PrivateKeyDer<'static>,
        max_bytes: usize,
        deadline: Duration,
    ) -> Result<Self, Error> {
        let endpoint = Url::parse(&profile.endpoint).map_err(|_| Error::Configuration)?;
        if endpoint.scheme() != "https"
            || !endpoint.has_host()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || profile.server_certificate_sha256 == [0; 32]
        {
            return Err(Error::Configuration);
        }
        let roots = read_roots(&profile.ca_file)?;
        let chain = read_chain(&profile.client_certificate_file)?;
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let verifier = LeafVerifier::new(
            profile.server_certificate_sha256,
            roots,
            provider.signature_verification_algorithms,
        );
        let mut tls = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|_| Error::Configuration)?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_client_auth_cert(chain, key)
            .map_err(|_| Error::Configuration)?;
        tls.resumption = Resumption::disabled();
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        let client = reqwest::Client::builder()
            .tls_backend_preconfigured(tls)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .http1_only()
            .connect_timeout(deadline)
            .timeout(deadline)
            .build()
            .map_err(|_| Error::Configuration)?;
        Ok(Self {
            endpoint,
            client,
            max_bytes,
        })
    }

    pub(super) async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        request: &impl Serialize,
        version: u32,
    ) -> Result<T, Error> {
        let request = encode(request, self.max_bytes)?;
        let request_sha256 = digest(&request);
        let mut endpoint = self.endpoint.clone();
        endpoint.set_path(path);
        let mut response = self
            .client
            .post(endpoint)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(request)
            .send()
            .await
            .map_err(|_| Error::Unavailable)?;
        match response.status() {
            http::StatusCode::OK => (),
            http::StatusCode::TOO_MANY_REQUESTS | http::StatusCode::SERVICE_UNAVAILABLE => {
                return Err(Error::Unavailable);
            }
            _ => return Err(Error::Refused),
        }
        if !json_content_type(response.headers())
            || response
                .content_length()
                .is_some_and(|len| len > self.max_bytes as u64)
        {
            return Err(Error::Refused);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| Error::Unavailable)? {
            if chunk.len() > self.max_bytes - bytes.len() {
                return Err(Error::Refused);
            }
            bytes.extend_from_slice(&chunk);
        }
        let reply: Reply<T> = serde_json::from_slice(&bytes).map_err(|_| Error::Refused)?;
        if reply.version != version || reply.request_sha256 != request_sha256 {
            return Err(Error::Refused);
        }
        Ok(reply.result)
    }
}

pub(super) fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(super) fn encode(value: &impl Serialize, max_bytes: usize) -> Result<Vec<u8>, Error> {
    let bytes = serde_json::to_vec(value).map_err(|_| Error::Refused)?;
    if bytes.len() > max_bytes {
        return Err(Error::Refused);
    }
    Ok(bytes)
}

pub(super) fn json_content_type(headers: &http::HeaderMap) -> bool {
    headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

pub(super) fn read_chain(path: &Path) -> Result<Vec<CertificateDer<'static>>, Error> {
    CertificateDer::pem_slice_iter(&read_file(path, MAX_PEM_BYTES)?)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| Error::Configuration)
}

pub(super) fn read_roots(path: &Path) -> Result<RootCertStore, Error> {
    let mut roots = RootCertStore::empty();
    for cert in read_chain(path)? {
        roots.add(cert).map_err(|_| Error::Configuration)?;
    }
    if roots.is_empty() {
        return Err(Error::Configuration);
    }
    Ok(roots)
}

pub(super) fn read_key(path: &Path) -> Result<PrivateKeyDer<'static>, Error> {
    read_private_key(File::open(path).map_err(|_| Error::Configuration)?)
}

pub(super) fn read_private_key(file: File) -> Result<PrivateKeyDer<'static>, Error> {
    PrivateKeyDer::from_pem_slice(&read_bounded(file, MAX_PEM_BYTES)?)
        .map_err(|_| Error::Configuration)
}

pub(super) fn read_file(path: &Path, max_bytes: usize) -> Result<Zeroizing<Vec<u8>>, Error> {
    read_bounded(
        File::open(path).map_err(|_| Error::Configuration)?,
        max_bytes,
    )
}

fn read_bounded(file: File, max_bytes: usize) -> Result<Zeroizing<Vec<u8>>, Error> {
    let mut bytes = Zeroizing::new(Vec::new());
    file.take((max_bytes as u64).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Configuration)?;
    if bytes.is_empty() || bytes.len() > max_bytes {
        return Err(Error::Configuration);
    }
    Ok(bytes)
}
