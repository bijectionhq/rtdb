use crate::multi::activation::{journal_custody::KeeperJournal, operator::authorize_abandoned_acquisition};

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
    ProviderPlan { authority: Uuid, candidate: Uuid },
}

impl Abandon {
    pub(in crate::multi::portable_backup) fn parse(mut fields: BTreeMap<String, String>) -> Result<Self> {
        let selection = match fields.remove("--stage-receipt") {
            Some(path) => Selection::StageReceipt(path.into()),
            None => Selection::ProviderPlan {
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
        let (authority, candidate, receipt) = match self.selection {
            Selection::StageReceipt(path) => {
                let receipt: staged::StageReceipt = read_receipt(&path, 4096)?;
                (receipt.intent.authority_id, receipt.intent.candidate()?, Some(receipt))
            }
            Selection::ProviderPlan { authority, candidate } => (authority, candidate, None),
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
            let plan = read_plan(store.as_ref(), &acquisition_key(authority, candidate)?).await?
                .ok_or_else(invalid_plan)?;
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
}
