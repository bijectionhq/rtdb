use std::str::FromStr;

use bytes::Bytes;
use rtdb_format::journal::{ObjectId, ObjectKind};
use rtdb_s3::VersionedInventory;
use rtdb_storage::{Error, ObjectKey, remote::decode_provider_key};
use rtdb_types::PhysicalObjectVersion;

use crate::multi::activation::operator::{attestation::Attestation, authorize_object_release};

use super::{held_objects::{Readback, read_receipt}, *};

#[derive(Clone, Debug)]
pub(super) struct Release {
    readback: PathBuf,
    proof: PathBuf,
    authority_socket: PathBuf,
    objects: rtdb_s3::S3Config,
}

impl Release {
    pub(super) fn parse(mut fields: BTreeMap<String, String>) -> Result<Self> {
        let readback = backup_field(&mut fields, "--readback")?.into();
        let proof = backup_field(&mut fields, "--installation-proof")?.into();
        let authority_socket = PathBuf::from(backup_field(&mut fields, "--authority-socket")?);
        let objects = hosted_s3(&mut fields)?;
        if !authority_socket.is_absolute() || !fields.is_empty() {
            return Err(arguments_error());
        }
        Ok(Self { readback, proof, authority_socket, objects })
    }

    pub(super) async fn run(self) -> Result<Value> {
        let readback: Readback = read_receipt(&self.readback, 32 << 20)?;
        let proof = Attestation::read(&self.proof)?;
        let objects_sha256 = readback.digest()?;
        let (intent, hold, manifest_sha256) = proof.hosted_hold(objects_sha256)?;
        if intent.database != DEVELOPMENT_DATABASE || readback.incarnation != intent.candidate()?
            || readback.key_hold != Some(hold) || readback.archive_sha256 != digest_hex(&manifest_sha256)
            || !readback.object_holds_verified
        {
            return Err(invalid_release());
        }
        let prefix = format!("recovery-custody/{}/{}", hold.authority, readback.incarnation);
        let authorization = ObjectKey::new(format!("{prefix}/authorized"))?;
        let completion = ObjectKey::new(format!("{prefix}/released"))?;
        let proof_bytes = serde_json::to_vec(&proof).map_err(|_| invalid_release())?;
        let proof_sha256 = digest_hex(&Sha256::digest(proof_bytes).into());
        let record = Bytes::from(format!("rtdb.recovery-object-release/v1 {proof_sha256}"));

        let root = ObjectKey::new("multi")?;
        let remotes = BTreeMap::new();
        let inventory = self.objects.versioned_inventory(&root, &remotes).await?;
        inventory.require_versioned().await?;
        let selected = selected_versions(&inventory, &readback, hold.generation.number)?;
        let io = IoRegion::new();
        let settled = async {
            let store = self.objects.open(&root, &remotes, Arc::clone(&io)).await?;
            if !receipt_exists(store.as_ref(), &authorization, &record).await? {
                authorize_object_release(&self.authority_socket, DEVELOPMENT_DATABASE, proof, objects_sha256).await?;
                store.put(&authorization, record.clone()).await?;
                if !receipt_exists(store.as_ref(), &authorization, &record).await? {
                    return Err(invalid_release());
                }
            }
            if !receipt_exists(store.as_ref(), &completion, &record).await? {
                for (key, version) in selected {
                    inventory.release_legal_hold(&key, version).await?;
                }
                store.put(&completion, record.clone()).await?;
                if !receipt_exists(store.as_ref(), &completion, &record).await? {
                    return Err(invalid_release());
                }
            }
            Ok::<(), Anomaly>(())
        }.await;
        io.close_and_wait().await;
        settled?;
        Ok(json!({
            "incarnation": readback.incarnation,
            "operation_id": hold.operation,
            "released_versions": readback.held_versions.len(),
            "installation_proof_sha256": proof_sha256,
        }))
    }
}

fn selected_versions<'a>(
    inventory: &VersionedInventory, readback: &'a Readback, generation: u64,
) -> Result<Vec<(ObjectKey, &'a PhysicalObjectVersion)>> {
    let checkpoint_id = ObjectId::from_str(&readback.checkpoint)?;
    if checkpoint_id.database != readback.incarnation || checkpoint_id.kind != ObjectKind::Manifest {
        return Err(invalid_release());
    }
    let checkpoint = ObjectKey::new(readback.checkpoint.as_str())?;
    let token = ObjectKey::new(format!("~rtdb-v1/{}/keys/{generation:016x}", readback.incarnation.simple()))?;
    let mut seen_keys = BTreeSet::new();
    let mut selected = Vec::with_capacity(readback.held_versions.len());
    for version in &readback.held_versions {
        let relative = version.key.strip_prefix("multi/").ok_or_else(invalid_release)?;
        let key = decode_provider_key(relative)?;
        if key != token && key != checkpoint {
            let object = ObjectId::from_str(key.as_str())?;
            if object.kind != ObjectKind::File || object.database != readback.incarnation {
                return Err(invalid_release());
            }
        }
        if !seen_keys.insert(key.clone()) {
            return Err(invalid_release());
        }
        inventory.check_legal_hold_target(&key, version)?;
        selected.push((key, version));
    }
    if !seen_keys.contains(&token) || !seen_keys.contains(&checkpoint) {
        return Err(invalid_release());
    }
    Ok(selected)
}

async fn receipt_exists(store: &dyn ObjectStore, key: &ObjectKey, expected: &[u8]) -> Result<bool> {
    match store.get_fresh_exact(key, expected.len()).await {
        Ok(bytes) if bytes.as_ref() == expected => Ok(true),
        Ok(_) => Err(invalid_release()),
        Err(Error::MissingObject(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn invalid_release() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/backup-object-release",
        "Object release requires the same signed inventory and an exclusive staged allocation",
    )
}
