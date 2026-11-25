use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use ring::{
    rand::{SecureRandom, SystemRandom},
    signature::{ED25519, UnparsedPublicKey},
};
use rtdb_crypt::receiver_clock::VerifiedReceiverCertificate;
use rtdb_format::config::{Yaml, YamlFields};
use rtdb_node::{DecisionClock, DecisionObservation};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp};
use serde::Deserialize;
use tokio::sync::Mutex;

use super::{SiteBinding, invalid, nonzero_hex};

const CHALLENGE_DOMAIN: &[u8] = b"rtdb/native/receiver-clock/v1\0";
const PURPOSE: &str = "bijection.rtdb.durable-acceptance-clock.v4";
const NONCE_BYTES: usize = 32;
const READING_BYTES: usize = 32;
const REPLY_BYTES: usize = READING_BYTES + 64;
const NANOS_PER_MICRO: u64 = 1000;

#[derive(Clone)]
pub(super) struct Transport {
    socket: PathBuf,
    verification_key: [u8; 32],
    evidence: [u8; 32],
    timeout: Duration,
}

#[derive(Deserialize)]
struct Envelope {
    statement: String,
    signature: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Evidence {
    evidence_sha256: String,
    max_journal_clock_error_ns: u64,
    expires_at_ns: u64,
}

impl Transport {
    pub(super) fn decode(value: Yaml) -> Result<Self> {
        let mut fields = YamlFields::new(value, "fundedReceiverClock.transport")?;
        let socket = PathBuf::from(fields.required_string("socket")?);
        let verification_key =
            nonzero_hex(&fields.required_string("verificationKey")?).ok_or_else(invalid)?;
        let evidence =
            nonzero_hex(&fields.required_string("evidenceSha256")?).ok_or_else(invalid)?;
        let millis: u64 = fields
            .required_string("timeoutMillis")?
            .parse()
            .map_err(|_| invalid())?;
        fields.finish()?;
        if !socket.is_absolute() || !(1..=1000).contains(&millis) {
            return Err(invalid());
        }
        Ok(Self {
            socket,
            verification_key,
            evidence,
            timeout: Duration::from_millis(millis),
        })
    }

    pub(super) fn bind(
        &self,
        purpose: &str,
        domain: [u8; 16],
        site: &SiteBinding,
        issuer: &[u8; 32],
        certificate: &VerifiedReceiverCertificate,
    ) -> Result<Receiver> {
        if purpose != PURPOSE || self.verification_key == *issuer {
            return Err(invalid());
        }
        let envelope: Envelope = serde_json::from_str(&site.certificate).map_err(|_| invalid())?;
        let evidence: Evidence =
            serde_json::from_str(&envelope.statement).map_err(|_| invalid())?;
        if nonzero_hex(&evidence.evidence_sha256) != Some(self.evidence)
            || [
                &envelope.signature,
                &evidence.evidence_sha256,
                &site.service_scope,
            ]
            .into_iter()
            .any(|hex| hex.bytes().any(|byte| byte.is_ascii_uppercase()))
        {
            return Err(invalid());
        }
        let mut binding = CHALLENGE_DOMAIN.to_vec();
        binding.extend_from_slice(PURPOSE.as_bytes());
        binding.push(0);
        binding.extend_from_slice(&certificate.source());
        binding.extend_from_slice(&domain);
        binding.extend_from_slice(&certificate.digest());
        binding.extend_from_slice(&self.evidence);
        Ok(Receiver {
            transport: self.clone(),
            binding,
            domain,
            maximum_error_ns: evidence.max_journal_clock_error_ns,
            expires_ns: evidence.expires_at_ns,
            previous: Mutex::new(None),
        })
    }
}

pub(super) struct Receiver {
    transport: Transport,
    binding: Vec<u8>,
    domain: [u8; 16],
    maximum_error_ns: u64,
    expires_ns: u64,
    previous: Mutex<Option<DecisionObservation>>,
}

impl Receiver {
    async fn sample(&self, started: Instant) -> Option<DecisionObservation> {
        let mut previous = self.previous.lock().await;
        let mut nonce = [0; NONCE_BYTES];
        SystemRandom::new().fill(&mut nonce).ok()?;
        let mut request = self.binding.clone();
        request.extend_from_slice(&nonce);
        let reply = exchange(&self.transport.socket, &request).await?;
        let (reading, signature) = reply.split_at(READING_BYTES);
        request.extend_from_slice(reading);
        UnparsedPublicKey::new(&ED25519, self.transport.verification_key)
            .verify(&request, signature)
            .ok()?;

        let [epoch, sequence, sampled_ns, error_ns] = std::array::from_fn(|index| {
            let offset = index * 8;
            u64::from_be_bytes(reading[offset..offset + 8].try_into().unwrap())
        });
        if epoch == 0
            || sequence < epoch
            || sampled_ns == 0
            || !(1..=self.maximum_error_ns).contains(&error_ns)
        {
            return None;
        }
        let upper_micros = sampled_ns.checked_add(error_ns)?.div_ceil(NANOS_PER_MICRO);
        let elapsed = started.elapsed();
        let expiry_upper = upper_micros
            .checked_mul(NANOS_PER_MICRO)?
            .checked_add(u64::try_from(elapsed.as_nanos()).ok()?)?;
        if elapsed >= self.transport.timeout || expiry_upper >= self.expires_ns {
            return None;
        }
        let instant = Timestamp(i64::try_from(upper_micros).ok()?);
        if previous.is_some_and(|prior| {
            epoch < prior.epoch || sequence <= prior.sequence || instant < prior.instant
        }) {
            return None;
        }
        let observation = DecisionObservation {
            domain: self.domain,
            epoch,
            sequence,
            instant,
        };
        *previous = Some(observation);
        Some(observation)
    }
}

#[async_trait::async_trait]
impl DecisionClock for Receiver {
    async fn observe(&self) -> Result<DecisionObservation> {
        tokio::time::timeout(self.transport.timeout, self.sample(Instant::now()))
            .await
            .ok()
            .flatten()
            .ok_or_else(unavailable)
    }
}

#[cfg(unix)]
async fn exchange(path: &std::path::Path, request: &[u8]) -> Option<[u8; REPLY_BYTES]> {
    use std::os::unix::fs::{FileTypeExt, PermissionsExt};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UnixStream,
    };

    let metadata = tokio::fs::symlink_metadata(path).await.ok()?;
    if !metadata.file_type().is_socket() || metadata.permissions().mode() & 0o007 != 0 {
        return None;
    }
    let mut stream = UnixStream::connect(path).await.ok()?;
    stream.write_all(request).await.ok()?;
    let mut reply = [0; REPLY_BYTES];
    stream.read_exact(&mut reply).await.ok()?;
    (stream.read(&mut [0]).await.ok()? == 0).then_some(reply)
}

#[cfg(not(unix))]
async fn exchange(_: &std::path::Path, _: &[u8]) -> Option<[u8; REPLY_BYTES]> {
    None
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.clock/unavailable",
        "No fresh authenticated receiver clock sample is available",
    )
}

#[cfg(all(test, unix))]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt};

    use ring::{
        digest::{SHA256, digest},
        signature::{Ed25519KeyPair, KeyPair},
    };
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UnixListener,
    };
    use uuid::Uuid;

    use super::super::NodeConfiguration;
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[tokio::test]
    async fn stock_receiver_requires_a_fresh_signed_sample_for_the_installed_binding() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("clock.sock");
        let file = directory.path().join("receiver.json");
        let issuer = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
        let receiver = Ed25519KeyPair::from_seed_unchecked(&[8; 32]).unwrap();
        let key = hex(receiver.public_key().as_ref());
        let source = Uuid::from_u128(1);
        let deployment = Uuid::from_u128(2);
        let domain = Uuid::from_u128(3);
        let evidence = [0xcd; 32];
        let scope = "ab".repeat(32);
        let statement = json!({
            "purpose": PURPOSE, "deployment": deployment, "source": source, "cell": "cell",
            "rtdbImage": "image", "serviceScope": scope, "maxStageToDurableLagNs": 1_500,
            "maxJournalClockErrorNs": 999, "expiresAtNs": 10_000_000_000u64,
            "evidenceSha256": hex(&evidence),
        })
        .to_string();
        let certificate = json!({
            "signature": hex(issuer.sign(statement.as_bytes()).as_ref()), "statement": statement,
        })
        .to_string();
        let digest = digest(&SHA256, certificate.as_bytes());
        let site = json!({
            "deployment": deployment, "source": source, "placement_epoch": 7,
            "issuer": hex(issuer.public_key().as_ref()), "cell": "cell", "image": "image",
            "service_scope": scope, "certificate": certificate,
        });
        let yaml = format!(
            "fundedReceiverClock:\n  installationFile: {}\n  domain: {domain}\n  placementEpoch: 7\n  purpose: {PURPOSE}\n  transport:\n    socket: {}\n    verificationKey: \"{key}\"\n    evidenceSha256: \"{}\"\n    timeoutMillis: 100\n",
            file.display(),
            socket.display(),
            hex(&evidence),
        );
        let decode = |yaml: &str| {
            NodeConfiguration::from_yaml_with_environment(yaml, &|_| None, false).unwrap()
        };
        let mut config = decode(&yaml);
        assert!(
            config
                .clone()
                .take_native_receiver_clock(true)
                .unwrap()
                .is_none()
        );
        fs::write(&file, site.to_string()).unwrap();
        assert!(
            config
                .clone()
                .take_native_receiver_clock(false)
                .unwrap()
                .is_none()
        );
        assert!(
            decode(&yaml.replace(&hex(&evidence), &"ef".repeat(32)))
                .take_native_receiver_clock(true)
                .is_err()
        );
        assert!(
            decode(&yaml.replace(&key, &hex(issuer.public_key().as_ref())))
                .take_native_receiver_clock(true)
                .is_err()
        );
        let clock = config.take_native_receiver_clock(true).unwrap().unwrap();
        assert!(!clock.funded_offers_for(*source.as_bytes()).await);

        let listener = UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o666)).unwrap();
        assert!(!clock.funded_offers_for(*source.as_bytes()).await);
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o660)).unwrap();
        let mut expected_request =
            b"rtdb/native/receiver-clock/v1\0bijection.rtdb.durable-acceptance-clock.v4\0".to_vec();
        expected_request.extend_from_slice(source.as_bytes());
        expected_request.extend_from_slice(domain.as_bytes());
        expected_request.extend_from_slice(digest.as_ref());
        expected_request.extend_from_slice(&evidence);
        let replies = [
            ([41u64, 50, 1_100_000_000, 999], false, true),
            ([41, 51, 1_100_001_000, 999], true, false),
            ([41, 50, 1_100_001_000, 999], false, false),
            ([40, 51, 1_100_001_000, 999], false, false),
            ([41, 51, 1_099_999_000, 999], false, false),
            ([41, 51, 1_100_001_000, 1_000], false, false),
            ([41, 51, 10_000_000_000, 999], false, false),
            ([42, 52, 1_100_002_000, 999], false, true),
        ];
        let server = tokio::spawn(async move {
            let mut prior_request = Vec::new();
            for (reading, replay_challenge, _) in replies {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = vec![0; expected_request.len() + 32];
                stream.read_exact(&mut request).await.unwrap();
                assert_eq!(
                    &request[..expected_request.len()],
                    expected_request.as_slice()
                );
                assert_ne!(request, prior_request);
                let mut signed = if replay_challenge {
                    prior_request
                } else {
                    request.clone()
                };
                prior_request = request;
                let reading: Vec<_> = reading.into_iter().flat_map(u64::to_be_bytes).collect();
                signed.extend_from_slice(&reading);
                stream.write_all(&reading).await.unwrap();
                stream
                    .write_all(receiver.sign(&signed).as_ref())
                    .await
                    .unwrap();
                stream.shutdown().await.unwrap();
            }
            let (_stalled, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        for ([epoch, sequence, sample, error], _, accepted) in replies {
            let observation = clock.observe().await;
            if accepted {
                assert_eq!(
                    observation.unwrap(),
                    DecisionObservation {
                        domain: *domain.as_bytes(),
                        epoch,
                        sequence,
                        instant: Timestamp((sample + error).div_ceil(1_000) as i64),
                    }
                );
            } else {
                assert_eq!(observation.unwrap_err().code, "rtdb.clock/unavailable");
            }
        }
        assert_eq!(
            clock.observe().await.unwrap_err().code,
            "rtdb.clock/unavailable"
        );
        server.abort();
    }
}
