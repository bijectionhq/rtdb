use std::{path::PathBuf, sync::Arc, time::Duration};

use rtdb_columnar::MemoryBudget;
use rtdb_directory::Checkpoint;
use rtdb_types::{Anomaly, Result};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{deployment::CellStorage, keys::KeyService, replay_broker::Client};

use super::{Placement, Routes, WriterAdmission, invalid};
use crate::journal::retention_decision::{RetentionAction, retention_actions};

pub struct Settings {
    pub placements: PathBuf,
    pub serving: CellStorage,
    pub archive: CellStorage,
    pub keys: Arc<dyn KeyService>,
    pub broker: Option<Client>,
    pub memory: Arc<MemoryBudget>,
}

pub async fn run(settings: Settings, cancellation: CancellationToken) -> Result<()> {
    let routes = Routes::read(
        &settings.placements,
        &settings.serving,
        settings.broker.as_ref(),
    )?;
    for placement in routes.configured.values() {
        if placement.custodian.is_none() || placement.archive != settings.archive {
            return Err(invalid(
                "Each placement needs a custodian and the installed cell archive",
            ));
        }
    }

    let releases = TaskTracker::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            _ = ticker.tick() => (),
        }
        let pass = async {
            for (_, placement) in routes.custody_placements() {
                maintain(
                    placement,
                    None,
                    settings.broker.as_ref(),
                    &settings.keys,
                    &settings.serving,
                    &settings.memory,
                    &releases,
                    Duration::from_secs(30),
                )
                .await;
            }
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            _ = pass => (),
        }
    }
    releases.close();
    tokio::time::timeout(Duration::from_secs(10), releases.wait())
        .await
        .map_err(|_| {
            Anomaly::fault(
                "rtdb.multi/custodian-shutdown",
                "Installed custody read releases are still pending",
            )
        })
}

pub(crate) async fn maintain(
    placement: &Placement,
    offered: Option<(WriterAdmission, Checkpoint)>,
    broker: Option<&Client>,
    keys: &Arc<dyn KeyService>,
    serving: &CellStorage,
    memory: &Arc<MemoryBudget>,
    releases: &TaskTracker,
    limit: Duration,
) {
    if let Some(broker) = broker {
        let erasure = placement.advance_installed_erasure(broker, keys);
        if let Ok(Err(error)) = tokio::time::timeout(limit, erasure).await {
            tracing::warn!(code = %error.code, "Installed erasure awaits custody evidence");
        }
    }
    for action in retention_actions() {
        match action {
            RetentionAction::DisposeExpired => {
                let _ = placement.dispose_withdrawn_workers().await;
            }
            RetentionAction::RetainSelected => {
                let _ = placement.retain_selected().await;
            }
            RetentionAction::Publish => {
                let publish =
                    placement.publish_checkpoint(keys, offered, serving, memory, releases);
                if matches!(tokio::time::timeout(limit, publish).await, Ok(Ok(Some(_)))) {
                    let _ = placement.retain_selected().await;
                }
            }
        }
    }
    let _ = tokio::time::timeout(limit, placement.retire_unheld_checkpoint(keys)).await;
    let _ = tokio::time::timeout(limit, placement.settle_serving_receipts(serving, keys)).await;
    let _ = tokio::time::timeout(limit, placement.retire_serving_prefixes(serving, keys)).await;
}
