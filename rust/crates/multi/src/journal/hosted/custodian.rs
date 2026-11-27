use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use rtdb_columnar::MemoryBudget;
use rtdb_directory::{Checkpoint, Directory};
use rtdb_types::{Anomaly, Result};
use tokio::time::Instant;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

use crate::{deployment::CellStorage, keys::KeyService, replay_broker::Client};

use super::{InstalledJournal, Placement, Routes, WriterAdmission, invalid, retention::Retention};
use crate::journal::cell::{CellAccess, InstalledCell, Role};
use crate::journal::retention_decision::{RetentionAction, retention_actions};

pub struct Settings {
    pub placements: Option<PathBuf>,
    pub discovery: Option<Discovery>,
    pub serving: CellStorage,
    pub archive: CellStorage,
    pub keys: Arc<dyn KeyService>,
    pub broker: Option<Client>,
    pub memory: Arc<MemoryBudget>,
}

pub struct Discovery {
    directory: Arc<dyn Directory>,
    routes: CellAccess,
}

impl Discovery {
    pub fn new(directory: Arc<dyn Directory>, cell: &Arc<InstalledCell>) -> Result<Self> {
        Ok(Self {
            directory,
            routes: cell.access(Role::Custodian)?,
        })
    }
}

struct DiscoveredJournal {
    journal: Arc<InstalledJournal>,
    retention: Retention,
}

pub async fn run(settings: Settings, cancellation: CancellationToken) -> Result<()> {
    run_with_sync(settings, rtdb_storage::AppleSync::Full, cancellation).await
}

pub async fn run_with_sync(
    settings: Settings,
    apple_sync: rtdb_storage::AppleSync,
    cancellation: CancellationToken,
) -> Result<()> {
    let routes = settings
        .placements
        .as_deref()
        .map(|path| Routes::read(path, &settings.serving, settings.broker.as_ref()))
        .transpose()?
        .unwrap_or_default();
    if settings.placements.is_none() && settings.discovery.is_none() {
        return Err(invalid(
            "The custodian needs a placement manifest or a cell directory",
        ));
    }
    for placement in routes.configured.values() {
        if placement.custodian.is_none() || placement.archive != settings.archive {
            return Err(invalid(
                "Each placement needs a custodian and the installed cell archive",
            ));
        }
    }

    let releases = TaskTracker::new();
    let release_stop = CancellationToken::new();
    let configured: BTreeSet<_> = routes
        .configured
        .values()
        .map(|placement| placement.worker.deployment())
        .collect();
    let mut discovered = BTreeMap::new();
    let mut next_discovery = Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            _ = ticker.tick() => (),
        }
        let pass = async {
            if let Some(discovery) = &settings.discovery
                && next_discovery <= Instant::now()
            {
                next_discovery = Instant::now() + Duration::from_secs(30);
                if let Err(error) = refresh(discovery, &configured, &mut discovered).await {
                    tracing::warn!("{}: {}", error.code, error.message.as_deref().unwrap_or(""));
                }
            }
            for (_, placement) in routes.custody_placements() {
                maintain_with_sync(
                    placement,
                    None,
                    settings.broker.as_ref(),
                    &settings.keys,
                    &settings.serving,
                    &settings.memory,
                    &releases,
                    &release_stop,
                    Duration::from_secs(30),
                    apple_sync,
                )
                .await;
            }
            for placement in discovered.values() {
                let _ = placement
                    .retention
                    .dispose_withdrawn_workers(&placement.journal)
                    .await;
                let _ = placement
                    .retention
                    .retain_selected(&placement.journal, None)
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
    if tokio::time::timeout(Duration::from_secs(10), releases.wait())
        .await
        .is_err()
    {
        release_stop.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), releases.wait()).await;
        return Err(Anomaly::fault(
            "rtdb.multi/custodian-shutdown",
            "Installed custody read releases are still pending",
        ));
    }
    release_stop.cancel();
    Ok(())
}

async fn refresh(
    discovery: &Discovery,
    configured: &BTreeSet<Uuid>,
    discovered: &mut BTreeMap<Uuid, DiscoveredJournal>,
) -> Result<()> {
    use futures::StreamExt;

    let candidates = discovery.directory.installed_journal_reservations().await?;
    let mut lookups =
        futures::stream::iter(candidates.into_iter().filter(|id| !configured.contains(id)))
            .map(|id| async move { (id, discovery.routes.custody_journal(id).await) })
            .buffer_unordered(8);
    let mut failure = None;
    while let Some((id, result)) = lookups.next().await {
        match result {
            Ok(Some(journal)) => {
                let current = discovered.entry(id).or_insert_with(|| DiscoveredJournal {
                    journal: Arc::clone(&journal),
                    retention: Retention::default(),
                });
                if current.journal.placement() != journal.placement() {
                    current.retention = Retention::default();
                }
                current.journal = journal;
            }
            Ok(None) => {
                discovered.remove(&id);
            }
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
    }
    failure.map_or(Ok(()), Err)
}

pub(crate) async fn maintain(
    placement: &Placement,
    offered: Option<(WriterAdmission, Checkpoint)>,
    broker: Option<&Client>,
    keys: &Arc<dyn KeyService>,
    serving: &CellStorage,
    memory: &Arc<MemoryBudget>,
    releases: &TaskTracker,
    release_stop: &CancellationToken,
    limit: Duration,
) {
    maintain_with_sync(placement, offered, broker, keys, serving, memory, releases, release_stop, limit, rtdb_storage::AppleSync::Full).await;
}

pub(crate) async fn maintain_with_sync(
    placement: &Placement,
    offered: Option<(WriterAdmission, Checkpoint)>,
    broker: Option<&Client>,
    keys: &Arc<dyn KeyService>,
    serving: &CellStorage,
    memory: &Arc<MemoryBudget>,
    releases: &TaskTracker,
    release_stop: &CancellationToken,
    limit: Duration,
    apple_sync: rtdb_storage::AppleSync,
) {
    placement.worker.reconcile_pending_releases().await;
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
                    placement.publish_checkpoint(keys, offered, serving, memory, releases, release_stop);
                if matches!(tokio::time::timeout(limit, publish).await, Ok(Ok(Some(_)))) {
                    let _ = placement.retain_selected().await;
                }
            }
        }
    }
    let _ = tokio::time::timeout(limit, placement.retire_unheld_checkpoint(keys)).await;
    let _ = tokio::time::timeout(limit, placement.settle_serving_receipts(serving, keys)).await;
    let _ = tokio::time::timeout(limit, placement.retire_serving_prefixes_with_sync(serving, keys, apple_sync)).await;
}
