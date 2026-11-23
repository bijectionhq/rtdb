use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use object_store::path::Path;
use ring::signature::{ED25519, UnparsedPublicKey};
use rtdb_storage::{Error, IoRegion, ObjectKey, ObjectStore, remote::RemoteStore};
use rtdb_types::{Anomaly, AnomalyCategory, Result, S3ObjectNamespace};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{CELL_STORAGE_NAMESPACE_MARKER, S3Config, S3CredentialResolver, TerminalS3Scope};

const QUALIFICATION: &str = "~terminal-enrollment";
const DOMAIN: &[u8] = b"rtdb.s3/terminal-enrollment/v1\0";
const MAXIMUM_CERTIFICATE_BYTES: u64 = 8192;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SignedEnrollment {
    payload: String,
    signature: String,
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Enrollment {
    policy: TerminalPolicy,
    namespace: S3ObjectNamespace,
    root: String,
    allocation: Uuid,
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum TerminalPolicy { CreateOnceV1 }

fn invalid(message: &'static str) -> Anomaly {
    Anomaly::incorrect("rtdb.s3/terminal-enrollment", message)
}

fn unavailable(message: &'static str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.s3/terminal-enrollment", message)
}

fn authenticate(bytes: &[u8], namespace: &S3ObjectNamespace, root: &ObjectKey, scope: &TerminalS3Scope) -> Result<()> {
    if bytes.len() as u64 > MAXIMUM_CERTIFICATE_BYTES {
        return Err(invalid("Terminal enrollment evidence is too large"));
    }
    let signed: SignedEnrollment = serde_json::from_slice(bytes).map_err(|_| invalid("Invalid terminal enrollment envelope"))?;
    let payload = URL_SAFE_NO_PAD.decode(signed.payload).map_err(|_| invalid("Invalid terminal enrollment payload"))?;
    let signature = URL_SAFE_NO_PAD.decode(signed.signature).map_err(|_| invalid("Invalid terminal enrollment signature"))?;
    let message = [DOMAIN, payload.as_slice()].concat();
    UnparsedPublicKey::new(&ED25519, scope.authority).verify(&message, &signature)
        .map_err(|_| invalid("Terminal enrollment evidence does not match the configured authority"))?;
    let evidence: Enrollment = serde_json::from_slice(&payload).map_err(|_| invalid("Invalid terminal enrollment claims"))?;
    let root = Path::from(root.as_str());
    if evidence.namespace != *namespace || evidence.root != root.as_ref() || evidence.allocation != scope.allocation {
        return Err(invalid("Terminal enrollment belongs to a different S3 allocation"));
    }
    Ok(())
}

async fn read_enrollment(store: &RemoteStore) -> Result<Option<Bytes>> {
    let key = ObjectKey::new(QUALIFICATION)?;
    let length = match store.head_fresh(&key).await {
        Ok(found) if found.size <= MAXIMUM_CERTIFICATE_BYTES => found.size as usize,
        Ok(_) => return Err(invalid("Stored terminal enrollment evidence is too large")),
        Err(Error::MissingObject(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    Ok(Some(store.get_fresh_exact(&key, length).await?))
}

pub(crate) async fn require_enrollment(store: &RemoteStore, root: &ObjectKey, scope: &TerminalS3Scope) -> Result<()> {
    let namespace = store.s3_namespace().ok_or_else(|| invalid("The cell has no resolved S3 namespace"))?;
    let evidence = read_enrollment(store).await?.ok_or_else(|| unavailable("The terminal allocation has no enrollment evidence"))?;
    authenticate(&evidence, namespace, root, scope)?;
    let marker = ObjectKey::new(CELL_STORAGE_NAMESPACE_MARKER)?;
    if store.get_fresh_exact(&marker, 16).await?.as_ref() != scope.allocation.as_bytes() {
        return Err(invalid("The cell namespace marker does not match its terminal allocation"));
    }
    Ok(())
}

impl S3Config {
    /// Installs provisioning evidence in a fresh, qualified terminal allocation.
    pub async fn enroll_terminal_cell(&self, remotes: &dyn S3CredentialResolver, evidence: Bytes) -> Result<Uuid> {
        let scope = self.terminal_scope.as_ref().ok_or_else(|| invalid("S3 terminal enrollment requires a terminal allocation"))?;
        let root = self.cell_root()?;
        let physical_root = self.object_prefix(&root)?;
        let inventory = self.open_inventory(&root, remotes).await?;
        authenticate(&evidence, inventory.namespace(), &physical_root, scope)?;
        let store = self.open_store(&root, remotes, IoRegion::new(), true, false).await?;
        if store.s3_namespace() != Some(inventory.namespace()) {
            return Err(invalid("The store and inventory resolve to different S3 namespaces"));
        }
        inventory.never_versioned_retirement().await?;
        let qualification = ObjectKey::new(QUALIFICATION)?;
        let marker = ObjectKey::new(CELL_STORAGE_NAMESPACE_MARKER)?;
        let previous = read_enrollment(store.as_ref()).await?;
        if let Some(bytes) = &previous { authenticate(bytes, inventory.namespace(), &physical_root, scope)?; }
        match store.head_fresh(&marker).await {
            Ok(_) => {
                require_enrollment(store.as_ref(), &physical_root, scope).await?;
                return Ok(scope.allocation);
            }
            Err(Error::MissingObject(_)) => {},
            Err(error) => return Err(error.into()),
        }
        let residue = previous.as_ref().map(|_| &qualification);
        if store.list(None).await?.iter().any(|object| residue != Some(&object.key))
            || !inventory.enrollment_inventory_is_clean(residue).await?
        {
            return Err(invalid("A terminal allocation must be empty when first enrolled"));
        }
        if previous.is_none() { store.put(&qualification, evidence).await?; }
        store.put(&marker, Bytes::copy_from_slice(scope.allocation.as_bytes())).await?;
        require_enrollment(store.as_ref(), &physical_root, scope).await?;
        Ok(scope.allocation)
    }
}

#[cfg(test)]
mod tests {
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use serde_json::json;

    use super::*;
    use crate::versions::VersionedInventory;
    use rtdb_storage::remote::PublicationUploads;

    fn terminal_config() -> (S3Config, Ed25519KeyPair) {
        let key = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
        let mut config = S3Config::new("terminal-fixture");
        config.region = Some("us-east-1".to_owned());
        config.terminal_scope = Some(TerminalS3Scope {
            allocation: Uuid::new_v4(),
            authority: key.public_key().as_ref().try_into().unwrap(),
        });
        (config, key)
    }

    fn sign(config: &S3Config, namespace: &S3ObjectNamespace, key: &Ed25519KeyPair) -> Bytes {
        let root = config.object_prefix(&config.cell_root().unwrap()).unwrap();
        let payload = serde_json::to_vec(&Enrollment {
            policy: TerminalPolicy::CreateOnceV1, namespace: namespace.clone(),
            root: Path::from(root.as_str()).to_string(), allocation: config.terminal_scope.as_ref().unwrap().allocation,
        }).unwrap();
        let signature = key.sign(&[DOMAIN, payload.as_slice()].concat());
        serde_json::to_vec(&json!({
            "payload": URL_SAFE_NO_PAD.encode(payload), "signature": URL_SAFE_NO_PAD.encode(signature.as_ref()),
        })).unwrap().into()
    }

    #[test]
    fn qualification_is_bound_to_its_issuer_provider_and_physical_root() {
        let (config, key) = terminal_config();
        let provider = S3ObjectNamespace { bucket: config.bucket.clone(), endpoint: None, region: config.region.clone() };
        let evidence = sign(&config, &provider, &key);
        let root = config.object_prefix(&config.cell_root().unwrap()).unwrap();
        let scope = config.terminal_scope.as_ref().unwrap();
        authenticate(&evidence, &provider, &root, scope).unwrap();
        let another_region = S3ObjectNamespace { region: Some("eu-west-1".into()), ..provider.clone() };
        assert!(authenticate(&evidence, &another_region, &root, scope).is_err());
        let another_endpoint = S3ObjectNamespace { endpoint: Some("http://localhost:8080".into()), ..provider.clone() };
        assert!(authenticate(&evidence, &another_endpoint, &root, scope).is_err());
        assert!(authenticate(&evidence, &provider, &ObjectKey::new("cell").unwrap(), scope).is_err());
        assert!(authenticate(&evidence, &provider, &root, &TerminalS3Scope { authority: [0; 32], ..scope.clone() }).is_err());
        assert!(authenticate(&evidence, &provider, &root, &TerminalS3Scope { allocation: Uuid::new_v4(), ..scope.clone() }).is_err());
    }

    #[tokio::test]
    async fn legacy_open_cannot_enter_a_terminal_root() {
        let (terminal, _) = terminal_config();
        let root = terminal.cell_root().unwrap();
        let remotes = std::collections::BTreeMap::new();
        let mut legacy = S3Config::new(&terminal.bucket);
        assert!(legacy.open(&root, &remotes, IoRegion::new()).await.is_err());
        assert!(legacy.open_cell(&root, &remotes, IoRegion::new()).await.is_err());
        assert!(legacy.versioned_inventory(&root, &remotes).await.is_err());
        legacy.prefix = Some(format!("{root}/inside"));
        assert!(legacy.cell_root().is_err());
        legacy.prefix = Some(format!("other/{root}/inside"));
        assert!(legacy.cell_root().is_ok());
        assert!(terminal.open_cell(&ObjectKey::new("cell").unwrap(), &remotes, IoRegion::new()).await.is_err());
    }

    #[test]
    fn terminal_allocation_cannot_be_nested_in_a_legacy_residence() {
        let (mut config, _) = terminal_config();
        config.prefix = Some("cell/legacy-residence".into());
        assert!(config.cell_root().is_err());
    }

    async fn reserve(inventory: &VersionedInventory, config: &S3Config, root: &ObjectKey) {
        let path = Path::from(format!("{}/pending", config.object_prefix(root).unwrap()));
        inventory.reserve(&path, Uuid::new_v4()).await.unwrap();
    }

    #[tokio::test]
    async fn terminal_enrollment_requires_a_pristine_provider_and_resumes_only_its_own_bootstrap() {
        let Ok(endpoint) = std::env::var("RTDB_S3_ENDPOINT") else { return; };
        if !endpoint.starts_with("http://127.0.0.1:") { return; }
        let bucket = std::env::var("RTDB_S3_BUCKET").expect("a never-versioned fixture bucket");
        let remotes = std::collections::BTreeMap::new();
        let marker = ObjectKey::new(CELL_STORAGE_NAMESPACE_MARKER).unwrap();
        let qualification = ObjectKey::new(QUALIFICATION).unwrap();
        let object = ObjectKey::new("old-payload").unwrap();
        for case in ["fresh", "resume", "occupied", "outstanding-upload", "dirty-resume", "versioned"] {
            let (mut config, key) = terminal_config();
            config.bucket = if case == "versioned" {
                let Ok(bucket) = std::env::var("RTDB_S3_VERSIONED_BUCKET") else { continue; };
                bucket
            } else {
                bucket.clone()
            };
            config.endpoint = Some(endpoint.clone());
            config.path_style_access_enabled = true;
            let root = config.cell_root().unwrap();
            let inventory = config.open_inventory(&root, &remotes).await.unwrap();
            let evidence = sign(&config, inventory.namespace(), &key);
            let raw = config.open_store(&root, &remotes, IoRegion::new(), true, false).await.unwrap();
            if matches!(case, "resume" | "dirty-resume") {
                raw.put(&qualification, evidence.clone()).await.unwrap();
            }
            if matches!(case, "occupied" | "dirty-resume") {
                raw.put(&object, Bytes::from_static(b"existing bytes")).await.unwrap();
            }
            if case == "outstanding-upload" { reserve(&inventory, &config, &root).await; }
            assert!(config.open_cell(&root, &remotes, IoRegion::new()).await.is_err());
            assert!(config.versioned_inventory(&root, &remotes).await.is_err());

            let enrolled = config.enroll_terminal_cell(&remotes, evidence.clone()).await;
            if matches!(case, "fresh" | "resume") {
                let allocation = config.terminal_scope.as_ref().unwrap().allocation;
                assert_eq!(enrolled.unwrap(), allocation, "{case}");
                let store = config.open_cell(&root, &remotes, IoRegion::new()).await.unwrap();
                assert_eq!(store.get(&marker).await.unwrap().as_ref(), allocation.as_bytes());
                let Some(rtdb_types::PhysicalObjectPublication::Terminal(publication)) =
                    store.put_with_publication(&object, Bytes::from_static(b"after enrollment")).await.unwrap()
                else { panic!("the terminal provider returned no publication identity"); };
                assert_eq!(config.enroll_terminal_cell(&remotes, evidence).await.unwrap(), allocation);
                let release = ObjectKey::new("release").unwrap();
                store.put(&release, Bytes::from_static(b"installed release")).await.unwrap();
                let inventory = config.versioned_inventory(&root, &remotes).await.unwrap();
                let matched = inventory.terminal_release_targets(&object, &release, &publication).await.unwrap().unwrap();
                assert_eq!(matched[0].condition, rtdb_types::S3TombstoneCondition::Matches(publication.etag.clone()));
                let foreign = rtdb_types::TerminalObjectPublication { allocation: Uuid::new_v4(), ..publication.clone() };
                assert!(inventory.terminal_release_targets(&object, &release, &foreign).await.unwrap().is_none());
                let stale = rtdb_types::TerminalObjectPublication { etag: "\"old\"".into(), ..publication };
                assert!(inventory.terminal_release_targets(&object, &release, &stale).await.unwrap().is_none());
            } else {
                assert!(enrolled.is_err(), "{case}");
                assert!(matches!(raw.get(&marker).await, Err(Error::MissingObject(_))));
            }
        }
    }
}
