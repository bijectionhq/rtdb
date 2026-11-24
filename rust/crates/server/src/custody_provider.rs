use std::{collections::BTreeSet, sync::Arc, time::Duration};

use rtdb_auth::Secret;
use rtdb_columnar::MemoryBudget;
use rtdb_format::config::{Yaml, YamlFields};
use rtdb_node::serving::custody_release::{
    CarrierObservation, CustodyCarrierAuthority, CustodyProviderInstallation,
    CustodyReleaseRegistration, CustodyResponsePart, OriginalCarrierObservations,
    OriginalCarrierReceipt, OriginalReceipt, OriginalWriterPoint, RegisteredCustodyResponse,
};
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, Result, TableRef, Timestamp, rtdb_v1::Basis};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

const MAX_RESPONSE: usize = 512 * 1024;

#[derive(Clone, Debug)]
pub struct Settings {
    pub database: String,
    endpoint: Url,
    bearer: Secret,
    public_key: [u8; 32],
    authority: CustodyCarrierAuthority,
    writer: OriginalWriterPoint,
}

pub(crate) fn read_settings(value: Yaml) -> Result<Settings> {
    let mut fields = YamlFields::new(value, "flightSql.custodyProviders")?;
    let database = fields.required_string("database")?;
    if database.is_empty() {
        return Err(invalid());
    }
    let endpoint = Url::parse(&fields.required_string("endpoint")?).map_err(|_| invalid())?;
    if endpoint.scheme() != "https"
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(invalid());
    }
    let bearer = fields.required_string("bearerToken")?;
    if !(16..=2048).contains(&bearer.len()) || !bearer.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(invalid());
    }
    let writer_table = fields.required_string("writerTable")?;
    let (schema, table) = writer_table
        .split_once('.')
        .filter(|(schema, table)| !schema.is_empty() && !table.is_empty() && !table.contains('.'))
        .ok_or_else(invalid)?;
    let writer_id = fields.required_string("writerId")?;
    if writer_id.is_empty() {
        return Err(invalid());
    }
    let writer = OriginalWriterPoint {
        table: TableRef::new(schema, table),
        id: EntityId::String(writer_id),
        valid_at: Timestamp(
            fields
                .required_string("writerValidAtMicros")?
                .parse()
                .map_err(|_| invalid())?,
        ),
    };
    let authority = CustodyCarrierAuthority {
        incarnation: fixed_hex(&fields.required_string("incarnation")?)?,
        source_history: fixed_hex(&fields.required_string("sourceHistory")?)?,
        installation: fixed_hex(&fields.required_string("installation")?)?,
        registry: fixed_hex(&fields.required_string("registry")?)?,
        original_writer: fixed_hex(&fields.required_string("originalWriter")?)?,
    };
    let public_key = fixed_hex(&fields.required_string("providerPublicKey")?)?;
    fields.finish()?;
    if authority.incarnation == [0; 16]
        || authority.installation == [0; 16]
        || authority.source_history == [0; 32]
        || authority.registry == [0; 32]
        || authority.original_writer == [0; 32]
        || public_key == [0; 32]
    {
        return Err(invalid());
    }
    Ok(Settings {
        database,
        endpoint,
        bearer: Secret::new(bearer),
        public_key,
        authority,
        writer,
    })
}

fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/custody-provider-configuration",
        "Invalid original custody provider configuration",
    )
}
fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/custody-provider",
        "The original custody provider is unavailable",
    )
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    fn digit(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }
    if text.len() % 2 != 0 {
        return None;
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|chunk| Some((digit(chunk[0])? << 4) | digit(chunk[1])?))
        .collect()
}
fn fixed_hex<const N: usize>(text: &str) -> Result<[u8; N]> {
    if text.len() != 2 * N {
        return Err(invalid());
    }
    decode_hex(text)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(invalid)
}

#[derive(Serialize)]
struct Request {
    challenge: String,
    #[serde(flatten)]
    observation: Observation,
}
#[derive(Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum Observation {
    Writer {
        authority: CustodyCarrierAuthority,
    },
    Carrier {
        registration: CustodyReleaseRegistration,
    },
    Response {
        authority: CustodyCarrierAuthority,
        part: CustodyResponsePart,
    },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SignedResponse {
    payload_hex: String,
    signature_hex: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    key: Vec<u8>,
    basis: Vec<u8>,
    system_time_micros: i64,
}
impl Receipt {
    fn open(self) -> Result<OriginalReceipt> {
        if !(16..=64).contains(&self.key.len()) {
            return Err(unavailable());
        }
        Ok(OriginalReceipt {
            key: self.key,
            basis: Basis::from_bytes(&self.basis).map_err(|_| unavailable())?,
            system_time: Timestamp(self.system_time_micros),
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Writer {
    authority: CustodyCarrierAuthority,
    receipt: Receipt,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CarrierState {
    Live,
    Disposed,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Carrier {
    registration: CustodyReleaseRegistration,
    receipt: Receipt,
    state: CarrierState,
    evidence_sha256: Option<[u8; 32]>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Response {
    authority: CustodyCarrierAuthority,
    part: CustodyResponsePart,
    expires_at: Timestamp,
    frame_sha256: [u8; 32],
    read_proofs_sha256: Option<[u8; 32]>,
    contributors: Vec<([u8; 16], [u8; 32])>,
}

struct Provider {
    client: reqwest::Client,
    memory: Arc<MemoryBudget>,
    endpoint: Url,
    bearer: Secret,
    public_key: [u8; 32],
    authority: CustodyCarrierAuthority,
}

impl Settings {
    pub(crate) fn matches_installation(&self, name: &str, id: Uuid) -> bool {
        self.database == name && self.authority.incarnation == *id.as_bytes()
    }

    pub(crate) fn installation(
        &self,
        memory: Arc<MemoryBudget>,
    ) -> Result<CustodyProviderInstallation> {
        let client = reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| invalid())?;
        let observations = Arc::new(Provider {
            client,
            memory,
            endpoint: self.endpoint.clone(),
            bearer: self.bearer.clone(),
            public_key: self.public_key,
            authority: self.authority,
        });
        Ok(CustodyProviderInstallation {
            authority: self.authority,
            writer: self.writer.clone(),
            observations,
        })
    }
}

impl Provider {
    async fn observe<T: DeserializeOwned>(
        &self,
        observation: Observation,
        cancellation: &CancellationToken,
    ) -> Result<T> {
        let request = serde_json::to_vec(&Request {
            challenge: Uuid::new_v4().simple().to_string(),
            observation,
        })
        .map_err(|_| unavailable())?;
        let _reservation = self.memory.reserve(MAX_RESPONSE * 5)?;
        cancellation
            .run_until_cancelled(async {
                let mut response = self
                    .client
                    .post(self.endpoint.clone())
                    .bearer_auth(self.bearer.expose())
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(request.clone())
                    .send()
                    .await
                    .map_err(|_| unavailable())?;
                if response.status().as_u16() != 200
                    || response
                        .content_length()
                        .is_some_and(|size| size > MAX_RESPONSE as u64)
                {
                    return Err(unavailable());
                }
                let mut body = Vec::new();
                while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
                    if chunk.len() > MAX_RESPONSE - body.len() {
                        return Err(unavailable());
                    }
                    body.extend_from_slice(&chunk);
                }
                open_response(&self.public_key, &request, &body)
            })
            .await
            .ok_or_else(unavailable)?
    }
}

fn open_response<T: DeserializeOwned>(key: &[u8; 32], request: &[u8], body: &[u8]) -> Result<T> {
    let response: SignedResponse = serde_json::from_slice(body).map_err(|_| unavailable())?;
    let payload = decode_hex(&response.payload_hex).ok_or_else(unavailable)?;
    let signature = fixed_hex(&response.signature_hex).map_err(|_| unavailable())?;
    rtdb_crypt::custody_provider::verify_response(key, request, &payload, &signature)?;
    serde_json::from_slice(&payload).map_err(|_| unavailable())
}

#[async_trait::async_trait]
impl OriginalCarrierObservations for Provider {
    async fn confirmed_writer(
        &self,
        authority: CustodyCarrierAuthority,
        cancellation: &CancellationToken,
    ) -> Result<OriginalReceipt> {
        if authority != self.authority {
            return Err(unavailable());
        }
        let observed: Writer = self
            .observe(Observation::Writer { authority }, cancellation)
            .await?;
        if observed.authority != authority {
            return Err(unavailable());
        }
        observed.receipt.open()
    }
    async fn registered_carrier(
        &self,
        registration: CustodyReleaseRegistration,
        cancellation: &CancellationToken,
    ) -> Result<OriginalCarrierReceipt> {
        if registration.authority != self.authority {
            return Err(unavailable());
        }
        let observed: Carrier = self
            .observe(Observation::Carrier { registration }, cancellation)
            .await?;
        if observed.registration != registration {
            return Err(unavailable());
        }
        let observation = match (observed.state, observed.evidence_sha256) {
            (CarrierState::Live, None) => CarrierObservation::Live,
            (CarrierState::Disposed, Some(evidence_sha256)) if evidence_sha256 != [0; 32] => {
                CarrierObservation::Disposed { evidence_sha256 }
            }
            _ => return Err(unavailable()),
        };
        Ok(OriginalCarrierReceipt {
            registration,
            receipt: observed.receipt.open()?,
            observation,
        })
    }
    async fn registered_response(
        &self,
        authority: CustodyCarrierAuthority,
        part: CustodyResponsePart,
        cancellation: &CancellationToken,
    ) -> Result<RegisteredCustodyResponse> {
        if authority != self.authority {
            return Err(unavailable());
        }
        let observed: Response = self
            .observe(Observation::Response { authority, part }, cancellation)
            .await?;
        let count = observed.contributors.len();
        if observed.authority != authority
            || observed.part != part
            || !(1..=512).contains(&count)
            || observed
                .contributors
                .iter()
                .any(|(group, occurrence)| *group == [0; 16] || *occurrence == [0; 32])
        {
            return Err(unavailable());
        }
        let contributors = observed.contributors.into_iter().collect::<BTreeSet<_>>();
        if contributors.len() != count || observed.frame_sha256 == [0; 32] {
            return Err(unavailable());
        }
        Ok(RegisteredCustodyResponse {
            authority,
            part,
            expires_at: observed.expires_at,
            frame_sha256: observed.frame_sha256,
            read_proofs_sha256: observed.read_proofs_sha256,
            contributors,
        })
    }
}
