use std::{collections::BTreeMap, path::PathBuf};

use rtdb_types::rtdb_v1::{
    NativeCheckpointMember, NativeDependentCustodyDisposition, NativeDependentCustodyEnrollment,
};
use tokio_rustls::rustls::{
    self, ClientConfig, RootCertStore,
    client::Resumption,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use super::*;
use crate::multi::{NativeDependentCustodyAuthority, classic_opening::operator::LeafVerifier};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConnectionProfile {
    endpoint: String,
    ca_file: PathBuf,
    client_certificate_file: PathBuf,
    client_private_key_file: PathBuf,
    server_certificate_sha256: [u8; 32],
}

pub(super) struct Connection {
    endpoint: Url,
    client: reqwest::Client,
}

impl Connection {
    pub(super) fn from_profile(profile: ConnectionProfile) -> Result<Self> {
        let endpoint = Url::parse(&profile.endpoint).map_err(|_| invalid())?;
        if endpoint.scheme() != "https"
            || !endpoint.has_host()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || profile.server_certificate_sha256 == [0; 32]
        {
            return Err(invalid());
        }
        let roots = read_roots(&profile.ca_file)?;
        let chain = read_chain(&profile.client_certificate_file)?;
        let key = read_key(&profile.client_private_key_file)?;
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let verifier = LeafVerifier::new(
            profile.server_certificate_sha256,
            roots,
            provider.signature_verification_algorithms,
        );
        let mut tls = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid())?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_client_auth_cert(chain, key)
            .map_err(|_| invalid())?;
        tls.resumption = Resumption::disabled();
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        let client = reqwest::Client::builder()
            .tls_backend_preconfigured(tls)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .http1_only()
            .connect_timeout(DEADLINE)
            .timeout(DEADLINE)
            .build()
            .map_err(|_| invalid())?;
        Ok(Self { endpoint, client })
    }

    pub(super) async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        request: &impl Serialize,
    ) -> Result<T> {
        let request = encode(request)?;
        let expected = digest(&request);
        let mut endpoint = self.endpoint.clone();
        endpoint.set_path(path);
        let mut response = self
            .client
            .post(endpoint)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(request)
            .send()
            .await
            .map_err(|_| pending())?;
        match response.status() {
            http::StatusCode::OK => (),
            http::StatusCode::TOO_MANY_REQUESTS | http::StatusCode::SERVICE_UNAVAILABLE => {
                return Err(pending());
            }
            _ => return Err(refused()),
        }
        if !json_content_type(response.headers())
            || response
                .content_length()
                .is_some_and(|len| len > MAX_BYTES as u64)
        {
            return Err(refused());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| pending())? {
            if chunk.len() > MAX_BYTES - bytes.len() {
                return Err(refused());
            }
            bytes.extend_from_slice(&chunk);
        }
        let reply: Reply<T> = serde_json::from_slice(&bytes).map_err(|_| refused())?;
        if reply.version != VERSION || reply.request_sha256 != expected {
            return Err(refused());
        }
        Ok(reply.result)
    }
}

pub(super) fn json_content_type(headers: &http::HeaderMap) -> bool {
    headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

pub(super) fn read_chain(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_slice_iter(&read_file(path)?)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| invalid())
}

pub(super) fn read_roots(path: &Path) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in read_chain(path)? {
        roots.add(cert).map_err(|_| invalid())?;
    }
    if roots.is_empty() {
        return Err(invalid());
    }
    Ok(roots)
}

pub(super) fn read_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_slice(&read_file(path)?).map_err(|_| invalid())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ObserverProfile {
    source_incarnation: Uuid,
    destination_incarnation: Uuid,
    owner: [u8; 32],
    application_scope: [u8; 32],
    connection: ConnectionProfile,
}

pub(super) struct Observer(BTreeMap<[u8; 16], Commissioned>);

struct Commissioned {
    source: [u8; 16],
    owner: [u8; 32],
    application_scope: [u8; 32],
    connection: Connection,
}

impl Observer {
    pub(super) fn from_profiles(profiles: Vec<ObserverProfile>) -> Result<Self> {
        let mut commissioned = BTreeMap::new();
        for profile in profiles {
            if profile.source_incarnation.is_nil()
                || profile.destination_incarnation.is_nil()
                || profile.source_incarnation == profile.destination_incarnation
                || profile.owner == [0; 32]
                || profile.application_scope == [0; 32]
            {
                return Err(invalid());
            }
            let record = Commissioned {
                source: *profile.source_incarnation.as_bytes(),
                owner: profile.owner,
                application_scope: profile.application_scope,
                connection: Connection::from_profile(profile.connection)?,
            };
            if commissioned
                .insert(*profile.destination_incarnation.as_bytes(), record)
                .is_some()
            {
                return Err(invalid());
            }
        }
        Ok(Self(commissioned))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn select(&self, scope: NativeDependentCustodyScope) -> Result<&Commissioned> {
        self.0
            .get(&scope.reservation.target.incarnation)
            .filter(|profile| profile.source == scope.reservation.source.basis.incarnation())
            .ok_or_else(pending)
    }
}

#[derive(Serialize)]
struct ObservationRequest<'a> {
    version: u32,
    observation: Observation<'a>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Observation<'a> {
    Enrollment {
        scope: NativeDependentCustodyScope,
        native: &'a [NativeCheckpointMember],
    },
    Disposition {
        enrollment: &'a NativeDependentCustodyEnrollment,
        fence: &'a NativeCustodyFence,
    },
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ObservationResult {
    Pending,
    Enrollment {
        enrollment: NativeDependentCustodyEnrollment,
    },
    Disposition {
        disposition: NativeDependentCustodyDisposition,
    },
}

#[async_trait::async_trait]
impl NativeDependentCustodyAuthority for Observer {
    async fn observe_enrollment(
        &self,
        scope: NativeDependentCustodyScope,
        native: &[NativeCheckpointMember],
        cancellation: &CancellationToken,
    ) -> Result<NativeDependentCustodyEnrollment> {
        let profile = self.select(scope)?;
        let request = ObservationRequest {
            version: VERSION,
            observation: Observation::Enrollment { scope, native },
        };
        let result = cancellation
            .run_until_cancelled(profile.connection.post(OBSERVE, &request))
            .await
            .ok_or_else(pending)??;
        match result {
            ObservationResult::Enrollment { enrollment }
                if enrollment.scope == scope
                    && enrollment.owner == profile.owner
                    && enrollment.application_scope == profile.application_scope =>
            {
                Ok(enrollment)
            }
            ObservationResult::Pending => Err(pending()),
            _ => Err(refused()),
        }
    }

    async fn observe_disposition(
        &self,
        enrollment: &NativeDependentCustodyEnrollment,
        fence: &NativeCustodyFence,
        cancellation: &CancellationToken,
    ) -> Result<NativeDependentCustodyDisposition> {
        let profile = self.select(enrollment.scope)?;
        if profile.owner != enrollment.owner
            || profile.application_scope != enrollment.application_scope
        {
            return Err(refused());
        }
        let request = ObservationRequest {
            version: VERSION,
            observation: Observation::Disposition { enrollment, fence },
        };
        let result = cancellation
            .run_until_cancelled(profile.connection.post(OBSERVE, &request))
            .await
            .ok_or_else(pending)??;
        match result {
            ObservationResult::Disposition { disposition }
                if disposition.scope == enrollment.scope
                    && disposition.enrollment == enrollment.receipt
                    && &disposition.fence == fence =>
            {
                Ok(disposition)
            }
            ObservationResult::Pending => Err(pending()),
            _ => Err(refused()),
        }
    }
}
