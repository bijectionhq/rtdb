use bytes::Bytes;
use rtdb_s3::VersionedInventory;
use rtdb_storage::{ObjectKey, remote::decode_provider_key};

use super::*;
use crate::multi::activation::{ActivationAuthority, operator::attestation};

#[cfg(unix)]
mod abandon;
#[cfg(unix)]
pub(in super::super) use abandon::Abandon;

const MAX_PLAN_BYTES: usize = 32 << 20;

#[derive(Clone, Debug)]
pub(in super::super) struct Acquirer(Verifier);

#[derive(Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct HoldPlan {
    key_hold: RecoveryKeyHold,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    intent: Option<attestation::Intent>,
    journal_head: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    journal_anchor: Option<journal_contract::CheckpointAnchor>,
    checkpoint: String,
    versions: Vec<PhysicalObjectVersion>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum Acquisition {
    Planned(HoldPlan),
    Closed(ClosedAcquisition),
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct ClosedAcquisition {
    abandoned_before_acquisition: RecoveryKeyHold,
}

impl Acquirer {
    pub(in super::super) fn parse(fields: BTreeMap<String, String>) -> Result<Self> {
        if !fields.contains_key("--cell-id") {
            return Err(arguments_error());
        }
        let verifier = Verifier::parse(fields)?;
        if verifier.stage_receipt.is_none() || verifier.readback.is_some() {
            return Err(arguments_error());
        }
        Ok(Self(verifier))
    }

    pub(in super::super) async fn run(self) -> Result<Value> {
        let authority = activation::installed(&self.0.destination.journal_url, true).await?
            .ok_or_else(|| Anomaly::incorrect("rtdb/backup-object-acquisition-authority", "Configure the independent activation journal reader"))?;
        self.run_with_authority(&authority).await
    }

    pub(super) async fn run_with_authority(self, authority: &ActivationAuthority) -> Result<Value> {
        let staged = self.0.staged_backup()?;
        let root = ObjectKey::new("multi")?;
        let remotes = BTreeMap::new();
        let objects = &self.0.destination.objects;
        let inventory = objects.versioned_inventory(&root, &remotes).await?;
        inventory.require_versioned().await?;
        let io = IoRegion::new();
        let result = async {
            let store = objects.open(&root, &remotes, Arc::clone(&io)).await?;
            self.acquire(authority, &staged, store, &inventory).await
        }.await;
        io.close_and_wait().await;
        serde_json::to_value(result?)
            .map_err(|error| Anomaly::fault("rtdb/backup-object-readback", error.to_string()))
    }

    async fn acquire(
        &self, authority: &ActivationAuthority, staged: &StagedBackup,
        store: Arc<dyn ObjectStore>, inventory: &VersionedInventory,
    ) -> Result<Readback> {
        let receipt = staged.receipt.as_ref().ok_or_else(invalid_plan)?;
        if receipt.object_namespace.as_ref() != Some(inventory.namespace()) {
            return Err(invalid_plan());
        }
        let key_hold = receipt.key_hold;
        if let Some(order) = staged::read_staging_order(store.as_ref(), key_hold.authority, key_hold.generation.database).await? {
            if order.key_hold != key_hold || order.intent != receipt.intent {
                return Err(invalid_plan());
            }
            if order.tracked_uploads {
                inventory.staging_uploads(key_hold.authority, key_hold.generation.database)?.require_closed().await?;
            }
        }
        let plan_key = acquisition_key(key_hold.authority, key_hold.generation.database)?;
        let saved = match read_acquisition(store.as_ref(), &plan_key).await? {
            Some(Acquisition::Planned(plan)) => Some(plan),
            Some(Acquisition::Closed(_)) => return Err(invalid_plan()),
            None => None,
        };
        if saved.as_ref().is_some_and(|plan| plan.key_hold != key_hold) {
            return Err(invalid_plan());
        }
        let selected = if let Some(plan) = &saved {
            inventory.pinned_read_store(&plan.versions)?
        } else {
            Arc::clone(&store)
        };
        let (observed, observed_key) = self.0.inspect(staged, selected, None).await?;
        if observed_key != Some(key_hold) {
            return Err(invalid_plan());
        }
        let journal_anchor = observed.journal_anchor.ok_or_else(invalid_plan)?;
        let plan = HoldPlan {
            key_hold,
            intent: saved.as_ref().is_none_or(|plan| plan.intent.is_some()).then(|| receipt.intent.clone()),
            journal_head: observed.journal_head.0,
            journal_anchor: saved.as_ref().is_none_or(|plan| plan.journal_anchor.is_some()).then_some(journal_anchor),
            checkpoint: observed.checkpoint.to_string(),
            versions: observed.versions,
        };
        let targets = plan.targets(inventory)?;
        receipt.require_pending(authority, observed.installation.source_lsn()).await?;
        if let Some(saved) = saved {
            if saved != plan {
                return Err(invalid_plan());
            }
        } else {
            let bytes = serde_json::to_vec(&plan).map_err(|_| invalid_plan())?;
            if bytes.len() > MAX_PLAN_BYTES {
                return Err(invalid_plan());
            }
            store.put(&plan_key, Bytes::from(bytes)).await?;
            if !matches!(read_acquisition(store.as_ref(), &plan_key).await?,
                Some(Acquisition::Planned(observed)) if observed == plan)
            {
                return Err(invalid_plan());
            }
            receipt.require_pending(authority, observed.installation.source_lsn()).await?;
        }
        for (key, version) in targets {
            receipt.require_pending(authority, observed.installation.source_lsn()).await?;
            inventory.acquire_legal_hold(&key, version).await?;
        }
        let pinned = inventory.pinned_read_store(&plan.versions)?;
        let (checked, checked_key) = self.0.inspect(staged, pinned, Some(inventory)).await?;
        if checked_key != Some(plan.key_hold) || checked.journal_head.0 != plan.journal_head
            || plan.journal_anchor.is_some_and(|anchor| checked.journal_anchor != Some(anchor))
            || checked.checkpoint.to_string() != plan.checkpoint || checked.versions != plan.versions
        {
            return Err(invalid_plan());
        }
        receipt.require_pending(authority, checked.installation.source_lsn()).await?;
        Ok(Readback::confirmed(self.0.incarnation, checked, checked_key))
    }
}

impl HoldPlan {
    fn targets(&self, inventory: &VersionedInventory) -> Result<Vec<(ObjectKey, &PhysicalObjectVersion)>> {
        use rtdb_format::journal::{ObjectId, ObjectKind};

        let generation = self.key_hold.generation;
        let checkpoint: ObjectId = self.checkpoint.parse()?;
        if checkpoint.database != generation.database || checkpoint.kind != ObjectKind::Manifest {
            return Err(invalid_plan());
        }
        let token = format!("~rtdb-v1/{}/keys/{:016x}", generation.database.simple(), generation.number);
        let mut seen = BTreeSet::new();
        let targets = self.versions.iter().map(|version| {
            let relative = version.key.strip_prefix("multi/").ok_or_else(invalid_plan)?;
            let key = decode_provider_key(relative)?;
            if key.as_str() != token && key.as_str() != self.checkpoint {
                let file: ObjectId = key.as_str().parse()?;
                if file.database != generation.database || file.kind != ObjectKind::File {
                    return Err(invalid_plan());
                }
            }
            if !seen.insert(key.clone()) {
                return Err(invalid_plan());
            }
            inventory.check_legal_hold_target(&key, version)?;
            Ok((key, version))
        }).collect::<Result<Vec<_>>>()?;
        if !seen.contains(&ObjectKey::new(token)?) || !seen.contains(&ObjectKey::new(self.checkpoint.as_str())?) {
            return Err(invalid_plan());
        }
        Ok(targets)
    }
}

fn acquisition_key(authority: Uuid, candidate: Uuid) -> Result<ObjectKey> {
    Ok(ObjectKey::new(format!("recovery-custody/{authority}/{candidate}/acquisition"))?)
}

async fn read_acquisition(store: &dyn ObjectStore, key: &ObjectKey) -> Result<Option<Acquisition>> {
    read_provider_record(store, key, MAX_PLAN_BYTES).await
}

pub(in crate::multi::portable_backup) async fn require_staging_open(
    store: &dyn ObjectStore, hold: RecoveryKeyHold,
) -> Result<()> {
    let key = acquisition_key(hold.authority, hold.generation.database)?;
    if matches!(read_acquisition(store, &key).await?, Some(Acquisition::Closed(_))) {
        return Err(invalid_plan());
    }
    Ok(())
}

#[cfg(unix)]
pub(super) async fn require_attestation_plan(
    store: &dyn ObjectStore, receipt: &staged::StageReceipt, readback: &Readback,
) -> Result<()> {
    let hold = receipt.key_hold;
    if staged::read_staging_order(store, hold.authority, hold.generation.database).await?.is_none() {
        return Ok(());
    }
    let key = acquisition_key(hold.authority, hold.generation.database)?;
    match read_acquisition(store, &key).await? {
        Some(Acquisition::Planned(plan)) if plan.key_hold == hold
            && plan.intent.as_ref().is_none_or(|intent| *intent == receipt.intent)
            && plan.checkpoint == readback.checkpoint
            && plan.versions == readback.held_versions
            && plan.journal_head == readback.journal_head => Ok(()),
        _ => Err(invalid_plan()),
    }
}

fn invalid_plan() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb/backup-object-acquisition",
        "The acquisition must use the exact staged object inventory and recovery key hold")
}
