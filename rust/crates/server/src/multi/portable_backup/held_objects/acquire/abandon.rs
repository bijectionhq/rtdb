use crate::multi::activation::{
    journal_custody::KeeperJournal,
    operator::{authorize_abandoned_acquisition, authorize_abandoned_staging},
};

use super::*;

#[derive(Clone, Debug)]
pub(in crate::multi::portable_backup) struct Abandon {
    selection: Selection,
    authority_socket: PathBuf,
    objects: rtdb_s3::S3Config,
    journal_url: String,
    journal_tls: PathBuf,
    key_vault: PathBuf,
}

#[derive(Clone, Debug)]
enum Selection {
    StageReceipt(PathBuf),
    ProviderRecord { authority: Uuid, candidate: Uuid },
}

enum StagingSettlement {
    Acquired(HoldPlan),
    Retired { objects_purged: bool },
}

impl Abandon {
    pub(in crate::multi::portable_backup) fn parse(mut fields: BTreeMap<String, String>) -> Result<Self> {
        let selection = match fields.remove("--stage-receipt") {
            Some(path) => Selection::StageReceipt(path.into()),
            None => Selection::ProviderRecord {
                authority: backup_uuid(&backup_field(&mut fields, "--authority-id")?)?,
                candidate: backup_uuid(&backup_field(&mut fields, "--incarnation")?)?,
            },
        };
        let authority_socket = PathBuf::from(backup_field(&mut fields, "--authority-socket")?);
        let journal_url = backup_field(&mut fields, "--journal-url")?;
        let journal_tls = backup_field(&mut fields, "--journal-custodian-tls-dir")?.into();
        let key_vault = backup_field(&mut fields, "--key-vault")?.into();
        let objects = hosted_s3(&mut fields)?;
        if !authority_socket.is_absolute() || !fields.is_empty() {
            return Err(arguments_error());
        }
        Ok(Self { selection, authority_socket, objects, journal_url, journal_tls, key_vault })
    }

    pub(in crate::multi::portable_backup) async fn run(self) -> Result<Value> {
        let (authority, candidate, receipt) = match &self.selection {
            Selection::StageReceipt(path) => {
                let receipt: staged::StageReceipt = read_receipt(path, 4096)?;
                (receipt.intent.authority_id, receipt.intent.candidate()?, Some(receipt))
            }
            Selection::ProviderRecord { authority, candidate } => (*authority, *candidate, None),
        };
        let root = ObjectKey::new("multi")?;
        let remotes = BTreeMap::new();
        let inventory = self.objects.versioned_inventory(&root, &remotes).await?;
        inventory.require_versioned().await?;
        if receipt.as_ref().is_some_and(|receipt| receipt.object_namespace.as_ref() != Some(inventory.namespace())) {
            return Err(invalid_plan());
        }
        let custodian = KeeperJournal::connect(&self.journal_url, &crate::cli::journal_credentials(&self.journal_tls)?)?;
        let keys = FileKms::open(&self.key_vault, || SystemClock.now().0)?;
        let io = IoRegion::new();
        let result = async {
            let store = self.objects.open(&root, &remotes, Arc::clone(&io)).await?;
            let key = acquisition_key(authority, candidate)?;
            let plan = match read_acquisition(store.as_ref(), &key).await? {
                Some(Acquisition::Planned(plan)) => plan,
                _ => {
                    let order = staged::read_staging_order(store.as_ref(), authority, candidate).await?
                        .ok_or_else(invalid_plan)?;
                    if order.intent.authority_id != authority || order.intent.candidate()? != candidate
                        || receipt.as_ref().is_some_and(|receipt| receipt.intent != order.intent || receipt.key_hold != order.key_hold)
                    {
                        return Err(invalid_plan());
                    }
                    let operation = order.key_hold.operation;
                    let closed = self.close_staging(store.as_ref(), &key, &inventory, &custodian, &keys, order).await?;
                    match closed {
                        StagingSettlement::Acquired(plan) => plan,
                        StagingSettlement::Retired { objects_purged } => return Ok(json!({
                            "incarnation": candidate, "operation_id": operation, "retired_versions": 0,
                            "journal_retired": true, "generation_destroyed": true, "objects_purged": objects_purged,
                        })),
                    }
                }
            };
            let hold = plan.key_hold;
            let (intent, namespace) = match receipt {
                Some(receipt) if receipt.key_hold == hold && plan.intent.as_ref().is_none_or(|intent| *intent == receipt.intent) =>
                    (receipt.intent, None),
                Some(_) => return Err(invalid_plan()),
                None => (plan.intent.clone().ok_or_else(invalid_plan)?, Some(inventory.namespace().clone())),
            };
            if hold.generation.database != candidate || hold.authority != authority || intent.authority_id != authority
                || intent.candidate()? != candidate || intent.operation_id != hold.operation || intent.cell.is_none()
            {
                return Err(invalid_plan());
            }
            let targets = plan.targets(&inventory)?;
            let journal = authorize_abandoned_acquisition(
                &self.authority_socket, intent, hold, journal_contract::Lsn(plan.journal_head), plan.journal_anchor, namespace,
            ).await?;
            custodian.abandon(candidate, journal).await?;
            for (key, version) in targets {
                if inventory.exact_retirement_absent(&key, version).await? {
                    continue;
                }
                inventory.release_legal_hold(&key, version).await?;
                if inventory.exact_retirement_available(&key, version).await? {
                    inventory.retire_observed_version(&key, version).await?;
                } else {
                    let target = inventory.versioned_retirement_target(&key, version).await?.ok_or_else(invalid_plan)?;
                    inventory.retire_version_behind_tombstone(&key, version, &target, hold.operation).await?;
                }
            }
            keys.release_recovery_key(hold).await?;
            Ok(json!({
                "incarnation": candidate, "operation_id": hold.operation,
                "retired_versions": plan.versions.len(), "journal_hold_released": true, "key_hold_released": true,
            }))
        }.await;
        io.close_and_wait().await;
        result
    }

    async fn close_staging(
        &self, store: &dyn ObjectStore, key: &ObjectKey, inventory: &VersionedInventory,
        custodian: &KeeperJournal, keys: &FileKms, order: staged::StagingOrder,
    ) -> Result<StagingSettlement> {
        let hold = order.key_hold;
        let journal = authorize_abandoned_staging(
            &self.authority_socket, order.intent, hold, inventory.namespace().clone(),
        ).await?;
        if journal != order.destination_journal_sha256 || journal != custodian.connection_sha256 {
            return Err(invalid_plan());
        }
        keys.require_recovery_vault(order.vault_identity).await?;
        let closed = ClosedAcquisition { abandoned_before_acquisition: hold };
        let bytes = serde_json::to_vec(&closed).map_err(|_| invalid_plan())?;
        let written = store.put(key, Bytes::from(bytes)).await;
        match read_acquisition(store, key).await? {
            Some(Acquisition::Planned(plan)) => return Ok(StagingSettlement::Acquired(plan)),
            Some(Acquisition::Closed(observed)) if observed.abandoned_before_acquisition == hold => (),
            _ => {
                written?;
                return Err(invalid_plan());
            }
        }
        let uploads = order.tracked_uploads.then(|| inventory.staging_uploads(hold.authority, hold.generation.database)).transpose()?;
        if let Some(uploads) = &uploads {
            uploads.close().await?;
        }
        custodian.retire_unacquired(hold.generation.database).await?;
        keys.abandon_recovery_key(order.vault_identity, hold).await?;
        if let Some(uploads) = &uploads {
            uploads.purge_abandoned().await?;
        }
        Ok(StagingSettlement::Retired { objects_purged: uploads.is_some() })
    }
}
