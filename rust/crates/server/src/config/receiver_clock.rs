use std::{
    fs,
    io::{self, Read},
    num::NonZeroU64,
    path::PathBuf,
    sync::Arc,
};

use rtdb_crypt::receiver_clock::{ReceiverCertificateBinding, VerifiedReceiverCertificate};
use rtdb_format::config::{Yaml, YamlFields};
use rtdb_node::{CertifiedDecisionClock, DecisionClock};
use rtdb_types::{Anomaly, Result};
use serde::Deserialize;
use uuid::Uuid;

use super::NodeConfiguration;

mod transport;
use transport::Transport;

const MAX_INSTALLATION_BYTES: u64 = 16 * 1024;

#[derive(Clone)]
pub(crate) struct Installation {
    file: PathBuf,
    domain: Uuid,
    placement_epoch: NonZeroU64,
    purpose: String,
    transport: Option<Transport>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SiteBinding {
    deployment: Uuid,
    source: Uuid,
    placement_epoch: NonZeroU64,
    certificate: String,
    issuer: String,
    image: String,
    cell: String,
    service_scope: String,
}

impl Installation {
    pub(super) fn decode(value: Yaml) -> Result<Self> {
        let mut fields = YamlFields::new(value, "fundedReceiverClock")?;
        let file = PathBuf::from(fields.required_string("installationFile")?);
        let domain: Uuid = fields
            .required_string("domain")?
            .parse()
            .map_err(|_| invalid())?;
        let placement_epoch = fields
            .required_string("placementEpoch")?
            .parse()
            .map_err(|_| invalid())?;
        let purpose = fields.required_string("purpose")?;
        let transport = fields
            .take("transport")
            .map(Transport::decode)
            .transpose()?;
        fields.finish()?;
        if !file.is_absolute() || domain.is_nil() || purpose.is_empty() {
            return Err(invalid());
        }
        Ok(Self {
            file,
            domain,
            placement_epoch,
            purpose,
            transport,
        })
    }

    fn read_site(&self) -> Result<Option<SiteBinding>> {
        match fs::metadata(&self.file) {
            Ok(metadata) if metadata.is_file() => (),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            _ => return Err(invalid()),
        }
        let mut bytes = Vec::new();
        match fs::File::open(&self.file) {
            Ok(file) => file
                .take(MAX_INSTALLATION_BYTES + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| invalid())?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(invalid()),
        };
        if bytes.len() as u64 > MAX_INSTALLATION_BYTES {
            return Err(invalid());
        }
        let site: SiteBinding = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if site.placement_epoch != self.placement_epoch {
            return Err(invalid());
        }
        Ok(Some(site))
    }

    fn verify(&self, site: &SiteBinding, issuer: &[u8; 32]) -> Result<VerifiedReceiverCertificate> {
        VerifiedReceiverCertificate::verify(
            site.certificate.as_bytes(),
            ReceiverCertificateBinding {
                issuer,
                purpose: &self.purpose,
                deployment: site.deployment,
                source: site.source,
                cell: &site.cell,
                image: &site.image,
                service_scope: &site.service_scope,
            },
        )
    }
}

impl NodeConfiguration {
    /// Pairs the commissioned receiver with the site's installed certificate for a certified server opener.
    pub fn certify_receiver_clock(
        mut self,
        receiver: Arc<dyn DecisionClock>,
    ) -> Result<(Self, Arc<CertifiedDecisionClock>)> {
        let installation = self.funded_receiver_clock.take().ok_or_else(invalid)?;
        let site = installation.read_site()?.ok_or_else(invalid)?;
        let issuer = nonzero_hex(&site.issuer).ok_or_else(invalid)?;
        let clock = CertifiedDecisionClock::new(
            receiver,
            installation.verify(&site, &issuer)?,
            *installation.domain.as_bytes(),
        )?;
        Ok((self, Arc::new(clock)))
    }

    pub(crate) fn take_native_receiver_clock(
        &mut self,
        enabled: bool,
    ) -> Result<Option<Arc<CertifiedDecisionClock>>> {
        let Some(installation) = self.funded_receiver_clock.take() else {
            return Ok(None);
        };
        let Some(transport) = installation.transport.as_ref() else {
            return Ok(None);
        };
        if !enabled || self.read_only_databases || !cfg!(unix) {
            return Ok(None);
        }
        let Some(site) = installation.read_site()? else {
            return Ok(None);
        };
        let issuer = nonzero_hex(&site.issuer).ok_or_else(invalid)?;
        let certificate = installation.verify(&site, &issuer)?;
        let domain = *installation.domain.as_bytes();
        let receiver =
            transport.bind(&installation.purpose, domain, &site, &issuer, &certificate)?;
        Ok(Some(Arc::new(CertifiedDecisionClock::new(
            Arc::new(receiver),
            certificate,
            domain,
        )?)))
    }
}

fn nonzero_hex(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 || !hex.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    let mut key = [0; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..][..2], 16).ok()?;
    }
    (key != [0; 32]).then_some(key)
}

fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.clock/installation",
        "Invalid funded receiver clock installation",
    )
}

#[cfg(all(test, feature = "multi"))]
mod tests {
    use std::sync::Mutex;

    use ring::signature::{Ed25519KeyPair, KeyPair};
    use rtdb_node::DecisionObservation;
    use rtdb_types::{Timestamp, rtdb_v1::Basis};
    use serde_json::json;
    use sha2::{Digest, Sha256};

    use super::*;

    struct LiveReceiver(Mutex<Option<DecisionObservation>>);

    #[async_trait::async_trait]
    impl DecisionClock for LiveReceiver {
        async fn observe(&self) -> Result<DecisionObservation> {
            self.0.lock().unwrap().ok_or_else(invalid)
        }
    }

    #[tokio::test]
    async fn mounted_receiver_uses_the_exact_site_certificate_and_live_domain() {
        let signer = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
        let hex = |bytes: &[u8]| {
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        let source = Uuid::from_u128(1);
        let deployment = Uuid::from_u128(2);
        let domain = Uuid::from_u128(3);
        let purpose = "bijection.rtdb.durable-acceptance-clock.v4";
        let scope = "ab".repeat(32);
        let statement = json!({
            "purpose": purpose, "deployment": deployment, "source": source, "cell": "test-cell",
            "rtdbImage": "test-image", "serviceScope": scope,
            "maxStageToDurableLagNs": 1_501, "maxJournalClockErrorNs": 999,
            "expiresAtNs": 2_000_000_500u64, "evidenceSha256": "cd".repeat(32),
        })
        .to_string();
        let certificate = json!({
            "signature": hex(signer.sign(statement.as_bytes()).as_ref()), "statement": statement,
        })
        .to_string();
        let digest: [u8; 32] = Sha256::digest(certificate.as_bytes()).into();
        let mut site = json!({
            "deployment": deployment, "source": source, "placement_epoch": 7,
            "issuer": hex(signer.public_key().as_ref()), "cell": "test-cell", "image": "test-image",
            "service_scope": scope, "certificate": certificate,
        });
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("receiver.json");
        let write = |site: &serde_json::Value| std::fs::write(&file, site.to_string()).unwrap();
        write(&site);
        let config = NodeConfiguration::from_yaml_with_environment(&format!(
            "fundedReceiverClock:\n  installationFile: {}\n  domain: {domain}\n  placementEpoch: 7\n  purpose: {purpose}\n",
            file.display(),
        ), &|_| None, false).unwrap();
        let sample = DecisionObservation {
            domain: *domain.as_bytes(),
            epoch: 7,
            sequence: 11,
            instant: Timestamp(1_100_000),
        };
        let receiver = Arc::new(LiveReceiver(Mutex::new(Some(sample))));
        let (_, clock) = config
            .clone()
            .certify_receiver_clock(receiver.clone())
            .unwrap();
        assert!(clock.funded_offers_for(*source.as_bytes()).await);
        assert!(!clock.funded_offers_for(*deployment.as_bytes()).await);
        let predecessor = Basis::new(*source.as_bytes(), 3);
        let cutoff = clock
            .authorize_offer(predecessor, sample, Timestamp(2_500_000))
            .await
            .unwrap();
        assert_eq!(cutoff.decision_before, Timestamp(1_999_998));
        assert_eq!(cutoff.journal.before(), Timestamp(1_999_999));
        assert_eq!(cutoff.journal.certificate_sha256(), digest);
        assert!(
            clock
                .authorize_offer(
                    Basis::new(*deployment.as_bytes(), 3),
                    sample,
                    Timestamp(2_500_000)
                )
                .await
                .is_err()
        );

        *receiver.0.lock().unwrap() = Some(DecisionObservation {
            domain: *deployment.as_bytes(),
            ..sample
        });
        assert!(!clock.funded_offers_for(*source.as_bytes()).await);
        *receiver.0.lock().unwrap() = Some(DecisionObservation {
            instant: Timestamp(2_000_001),
            ..sample
        });
        assert!(!clock.funded_offers_for(*source.as_bytes()).await);
        *receiver.0.lock().unwrap() = None;
        assert!(!clock.funded_offers_for(*source.as_bytes()).await);

        site["placement_epoch"] = json!(8);
        write(&site);
        assert!(
            config
                .clone()
                .certify_receiver_clock(receiver.clone())
                .is_err()
        );
        site["placement_epoch"] = json!(7);
        site["service_scope"] = json!("ef".repeat(32));
        write(&site);
        assert!(config.certify_receiver_clock(receiver).is_err());
    }
}
