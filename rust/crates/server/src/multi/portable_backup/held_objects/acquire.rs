use bytes::Bytes;
use rtdb_s3::VersionedInventory;
use rtdb_storage::{Error, ObjectKey, remote::decode_provider_key};

use super::*;
use crate::multi::activation::ActivationAuthority;

const MAX_PLAN_BYTES: usize = 32 << 20;

#[derive(Clone, Debug)]
pub(in super::super) struct Acquirer(Verifier);

#[derive(Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct HoldPlan {
    key_hold: RecoveryKeyHold,
    journal_head: u64,
    checkpoint: String,
    versions: Vec<PhysicalObjectVersion>,
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
        let plan_key = ObjectKey::new(format!("recovery-custody/{}/{}/acquisition", key_hold.authority, self.0.incarnation))?;
        let saved = read_plan(store.as_ref(), &plan_key).await?;
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
        let plan = HoldPlan {
            key_hold, journal_head: observed.journal_head.0, checkpoint: observed.checkpoint.to_string(),
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
            if read_plan(store.as_ref(), &plan_key).await?.as_ref() != Some(&plan) {
                return Err(invalid_plan());
            }
            receipt.require_pending(authority, observed.installation.source_lsn()).await?;
        }
        for (key, version) in targets {
            inventory.acquire_legal_hold(&key, version).await?;
        }
        let pinned = inventory.pinned_read_store(&plan.versions)?;
        let (checked, checked_key) = self.0.inspect(staged, pinned, Some(inventory)).await?;
        if checked_key != Some(plan.key_hold) || checked.journal_head.0 != plan.journal_head
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
        let mut seen = BTreeSet::new();
        self.versions.iter().map(|version| {
            let relative = version.key.strip_prefix("multi/").ok_or_else(invalid_plan)?;
            let key = decode_provider_key(relative)?;
            if !seen.insert(key.clone()) {
                return Err(invalid_plan());
            }
            inventory.check_legal_hold_target(&key, version)?;
            Ok((key, version))
        }).collect()
    }
}

async fn read_plan(store: &dyn ObjectStore, key: &ObjectKey) -> Result<Option<HoldPlan>> {
    let size = match store.head_fresh(key).await {
        Ok(metadata) => metadata.size,
        Err(Error::MissingObject(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let size = usize::try_from(size).ok().filter(|size| *size <= MAX_PLAN_BYTES).ok_or_else(invalid_plan)?;
    let bytes = store.get_fresh_exact(key, size).await?;
    serde_json::from_slice(&bytes).map(Some).map_err(|_| invalid_plan())
}

fn invalid_plan() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb/backup-object-acquisition",
        "The acquisition must use the exact staged object inventory and recovery key hold")
}
