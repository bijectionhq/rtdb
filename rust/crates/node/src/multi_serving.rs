use std::{collections::BTreeSet, net::IpAddr, sync::{Arc, Weak}, time::Duration};

use async_trait::async_trait;
use rtdb_crypt::tokens::TokenAuthority;
use rtdb_db::{ChangeHistory, MultiDatabase, NativeCheckpointInstallation, PinnedExport, Role, WritePipeline};
use rtdb_index::branches::{BranchCut, BranchEffect, BranchIndex, BranchWrite};
use rtdb_tx::transact::{
    COUNT_RANGE_CONSTRAINT, CustodyGroupId, CustodyWriterFence, GROUPED_COUNT_CONSTRAINT, LINEARIZABLE, PHYSICAL_ERASURE, RELATIVE_VALID_TIME,
    ROW_CUSTODY, Receipt, TIMED_TRANSACT, TRANSACT_REQUIRES, Transact, TrustedCustodyCommand, workload,
};
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, MemoryBudget, Result, TableRef, Timestamp,
    rtdb_v1::{At, Basis, ClaimEvidence, ClaimScope, Layer, ManagedCheckpointReservation, ManagedCheckpointTarget, NativeCheckpointCustody, PinnedCheckpointIdentity},
};
use tokio::{sync::Mutex, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::serving::{
    ChangeGroup, ChangeOrder, ChangePage, Changes, QuerySource, QueryView, ROW_VISIBILITY,
    ServingDriver, ServingScope, append_change_rows,
};
use crate::{Database, DatabaseCatalog};

pub(crate) struct MultiSqlDelivery {
    pub(crate) catalog: Arc<DatabaseCatalog>,
    pub(crate) attachment: Weak<Database>,
    pub(crate) generation: Option<u64>,
    pub(crate) groups: BTreeSet<CustodyGroupId>,
}

#[async_trait]
impl rtdb_query::QueryDeliveryGuard for MultiSqlDelivery {
    async fn check(&self, cancellation: &CancellationToken) -> Result<()> {
        let attachment = self.attachment.upgrade().ok_or_else(interrupted)?;
        self.catalog.with_current_attachment(&attachment, || Ok(()))?;
        let driver = attachment.serving_driver().ok_or_else(interrupted)?;
        driver.check_read_delivery(self.generation, &self.groups, cancellation).await?;
        self.catalog.with_current_attachment(&attachment, || Ok(()))
    }
}

#[async_trait]
impl rtdb_storage::sealing::PlaintextReleaseCheck for MultiSqlDelivery {
    async fn require_current(&self) -> Result<()> {
        rtdb_query::QueryDeliveryGuard::check(self, &CancellationToken::new()).await
    }
}

pub struct MultiServing {
    database: Arc<Mutex<MultiDatabase>>,
    writes: WritePipeline,
    incarnation: [u8; 16],
    tokens: TokenAuthority,
    memory: Arc<MemoryBudget>,
    route: Option<Arc<dyn MultiWriteRoute>>,
}

pub struct MultiFollower {
    serving: Arc<MultiServing>,
    owner: Arc<()>,
}

pub struct MultiFollowerProgress {
    pub applied_position: u64,
    pub writer_head: Basis,
    pub writer_head_position: u64,
}

impl MultiFollower {
    pub fn matches_driver(&self, driver: &dyn ServingDriver) -> bool {
        std::ptr::addr_eq(Arc::as_ptr(&self.serving) as *const dyn ServingDriver, driver)
    }

    pub async fn query(
        &self, at: At, now: Timestamp, observe_progress: bool, cancellation: &CancellationToken,
    ) -> Result<(QueryView, Option<MultiFollowerProgress>)> {
        cancellation.run_until_cancelled(async {
            let (index, generation) = self.serving.database.lock().await.follower_view(&self.owner)?;
            require_plain_follower(&index)?;
            match at {
                At::Exactly(basis) => { index.cut(At::AtLeast(basis))?; }
                At::Latest | At::AtLeast(_) => (),
                _ => return Err(Anomaly::unsupported("rtdb/follower-source", "The follower selects Latest, AtLeast or Exactly")),
            }
            let view = index.view(rtdb_types::rtdb_v1::MAIN_BRANCH, at, Layer::Merged, now).map_err(|error| {
                if error.code == "rtdb/unknown-basis" {
                    Anomaly::new(AnomalyCategory::NotFound, "rtdb/cursor-expired", "The replica has no retained exact cut at this basis")
                } else { error }
            })?;
            let cut = view.cut();
            let progress = if observe_progress {
                let applied_position = index.applied_position(cut)?;
                let (writer_head, writer_head_position) = self.serving.database.lock().await.follower_writer_head(&self.owner).await?;
                if writer_head.incarnation() != cut.basis.incarnation() || writer_head_position < applied_position {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb/follower-progress-unavailable", "The writer head does not cover the selected follower cut"));
                }
                Some(MultiFollowerProgress { applied_position, writer_head, writer_head_position })
            } else { None };
            let snapshot = rtdb_query::branch_snapshot(&view, &self.serving.memory)?;
            Ok((QueryView {
                snapshot, cut, claim: None, custody_groups: BTreeSet::new(),
                custody_contributors: Some(BTreeSet::new()), key_generation: Some(generation),
            }, progress))
        }).await.ok_or_else(interrupted)?
    }

    pub async fn check_delivery(&self, basis: Basis, generation: u64, now: Timestamp, cancellation: &CancellationToken) -> Result<()> {
        cancellation.run_until_cancelled(async {
            let database = self.serving.database.lock().await;
            let (index, _) = database.follower_view(&self.owner)?;
            require_plain_follower(&index)?;
            index.view(rtdb_types::rtdb_v1::MAIN_BRANCH, At::Exactly(basis), Layer::Merged, now)?;
            database.check_generation(generation).await
        }).await.ok_or_else(interrupted)?
    }
}

fn require_plain_follower(index: &BranchIndex) -> Result<()> {
    index.require_local_custody()?;
    if index.has_custody_groups() || index.has_visibility_history() {
        return Err(Anomaly::unsupported("rtdb/follower-plain", "The follower requires a plain Multi database"));
    }
    Ok(())
}

pub struct ManagedCheckpointRead<'a> {
    pub generation: u64,
    pub branch: &'a str,
    pub table: &'a TableRef,
    pub id: &'a EntityId,
    pub carrier: &'a [u8],
}

/// A trusted host binds each provider to one private catalog reservation.
#[async_trait::async_trait]
pub trait ManagedCheckpointProvider: Send + Sync {
    async fn install(&self, export: &PinnedExport, destination: &MultiServing) -> Result<Basis>;
    async fn recover_installation(&self, destination: &MultiServing) -> Result<Basis>;
    async fn require_read(&self, destination: &Arc<MultiServing>, read: ManagedCheckpointRead<'_>) -> Result<()>;
    async fn release_source(&self, source: &MultiServing) -> Result<()>;
}

/// The runtime mounts the original source authority for one source incarnation.
pub trait ManagedCheckpointSource: Send + Sync {
    fn bind<'a>(
        &'a self,
        reservation: ManagedCheckpointReservation,
        cancellation: &'a CancellationToken,
    ) -> Result<Box<dyn ManagedCheckpointProvider + 'a>>;
}

/// The host can route an already authorized write to the node appointed for this incarnation.
#[async_trait]
pub trait MultiWriteRoute: Send + Sync {
    async fn forward(
        &self, incarnation: [u8; 16], request: &Transact, admission: MultiWriteAdmission<'_>,
    ) -> Result<Option<Receipt>>;
}

pub enum MultiWriteAdmission<'a> {
    Ordinary(Option<&'a ServingScope>),
    CustodyResponse,
}

impl MultiServing {
    pub async fn assign_follower(self: &Arc<Self>) -> Result<MultiFollower> {
        let owner = Arc::new(());
        let mut database = self.database.lock().await;
        database.assign_follower(&owner).await?;
        let (index, _) = database.follower_view(&owner)?;
        require_plain_follower(&index)?;
        Ok(MultiFollower { serving: Arc::clone(self), owner })
    }

    pub async fn restored_checkpoint_basis(&self, manifest_digest: [u8; 32]) -> Result<Option<Basis>> {
        self.database.lock().await.restored_checkpoint_basis(manifest_digest).await
    }

    pub async fn pin_managed_checkpoint(
        &self, pin: uuid::Uuid, basis: Basis, target: ManagedCheckpointTarget,
        require_unbound: impl Future<Output = Result<()>> + Send,
    ) -> Result<PinnedCheckpointIdentity> {
        let mut database = self.database.lock().await;
        // The reservation check and pin share the writer lock with release, so a stale retry cannot repin.
        require_unbound.await?;
        database.pin_managed_checkpoint(pin, basis, target).await
    }

    pub async fn require_managed_checkpoint_writer(&self) -> Result<()> {
        self.database.lock().await.require_managed_checkpoint_writer().await
    }

    pub async fn has_native_checkpoint_custody(&self) -> Result<bool> {
        let mut database = self.database.lock().await;
        let index = database.fresh_index().await?;
        index.require_local_custody()?;
        Ok(index.has_custody_groups())
    }

    pub async fn claim_native_checkpoint_copy(&self, reservation: ManagedCheckpointReservation) -> Result<NativeCheckpointCustody> {
        self.database.lock().await.claim_native_checkpoint_copy(reservation).await
    }

    pub async fn restore_native_checkpoint(&self, export: &PinnedExport, custody: &NativeCheckpointCustody) -> Result<Basis> {
        Ok(self.database.lock().await.restore_native_checkpoint(export, custody).await?.basis)
    }

    pub async fn record_native_checkpoint_drain(&self, installed: NativeCheckpointInstallation) -> Result<()> {
        self.database.lock().await.record_native_checkpoint_drain(installed).await
    }

    pub async fn native_checkpoint_installation(&self, reservation: ManagedCheckpointReservation) -> Result<NativeCheckpointInstallation> {
        self.database.lock().await.native_checkpoint_installation(reservation).await
    }

    pub async fn native_checkpoint_forward(&self, reservation: ManagedCheckpointReservation) -> Result<Option<(NativeCheckpointCustody, Option<Basis>)>> {
        let (index, generation) = self.database.lock().await.fresh_view().await?;
        self.database.lock().await.check_generation(generation).await?;
        Ok(index.native_checkpoint_forward(reservation)?.map(|(custody, terminal)| (custody.clone(), terminal)))
    }

    pub async fn check_native_export_read(&self, custody: &NativeCheckpointCustody, destination: Basis, group: CustodyGroupId, occurrence: [u8; 32]) -> Result<()> {
        let (index, generation) = self.database.lock().await.fresh_view().await?;
        index.check_native_export_read(custody, destination, group, occurrence)?;
        let vault = self.tokens.custody_vault();
        for &(group, _) in &custody.groups { vault.check_readable(group).await?; }
        let mut database = self.database.lock().await;
        database.fresh_index().await?.check_native_export_read(custody, destination, group, occurrence)?;
        database.check_generation(generation).await
    }

    pub async fn open_managed_checkpoint(&self, reservation: ManagedCheckpointReservation) -> Result<PinnedExport> {
        self.database.lock().await.open_managed_checkpoint(reservation).await
    }

    pub async fn prepare_managed_checkpoint(&self, reservation: ManagedCheckpointReservation) -> Result<()> {
        self.database.lock().await.require_managed_checkpoint_destination(reservation).await
    }

    pub async fn restore_managed_checkpoint(&self, export: &PinnedExport, reservation: ManagedCheckpointReservation) -> Result<BranchCut> {
        self.database.lock().await.restore_managed_checkpoint(export, reservation).await
    }

    pub async fn managed_checkpoint_view(&self, reservation: ManagedCheckpointReservation) -> Result<(Basis, BranchIndex, u64)> {
        self.database.lock().await.require_managed_checkpoint_restore(reservation).await
    }

    pub async fn release_managed_checkpoint(&self, reservation: ManagedCheckpointReservation) -> Result<()> {
        self.database.lock().await.release_managed_checkpoint(reservation).await
    }

    pub fn new(database: MultiDatabase, memory: Arc<MemoryBudget>) -> Self {
        let incarnation = database.incarnation();
        let tokens = database.token_authority();
        let database = Arc::new(Mutex::new(database));
        let writes = WritePipeline::start(Arc::clone(&database));
        Self {
            incarnation,
            tokens,
            database,
            writes,
            memory,
            route: None,
        }
    }

    pub fn with_write_route(mut self, route: Arc<dyn MultiWriteRoute>) -> Self {
        self.route = Some(route);
        self
    }

    /// Applies the authorized request only under this node's active writer lease.
    /// Peer ingress calls this entry point and cannot submit a second forwarding attempt.
    pub async fn transact_local(
        &self, mut request: Transact, scope: Option<ServingScope>,
    ) -> Result<Receipt> {
        require_ordinary_request(&request)?;
        if let Some(scope) = scope {
            return self.writes.transact_scoped(request, scope.labels, scope.receipt_namespace).await;
        }
        if request.uses_custody() && self.features().contains(&ROW_CUSTODY) {
            crate::serving::custody::prepare(&mut request, &self.tokens.custody_vault()).await?;
        }
        self.writes.transact(request).await
    }

    async fn transact_routed(
        &self, request: Transact, scope: Option<ServingScope>, cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        require_ordinary_request(&request)?;
        cancellation.run_until_cancelled(async {
            if let Some(route) = &self.route
                && let Some(receipt) = route.forward(
                    self.incarnation, &request, MultiWriteAdmission::Ordinary(scope.as_ref()),
                ).await?
            {
                return Ok(receipt);
            }
            self.transact_local(request, scope).await
        }).await.ok_or_else(interrupted)?
    }

    /// Accepts only the narrow ordered response-release command from an admitted peer.
    pub async fn submit_response_local(&self, request: Transact) -> Result<Receipt> {
        if !request.is_response_release() {
            return Err(crate::serving::custody_release::release_unavailable());
        }
        self.writes.transact(request).await
    }

    pub async fn replay_follower(&self) -> Result<()> {
        self.database.lock().await.fresh_index().await?;
        Ok(())
    }

    pub async fn tick(&self, live_nodes: &BTreeSet<IpAddr>) -> Result<Role> {
        self.writes.tick(live_nodes.clone()).await
    }

    pub async fn retire_deployment(&self) -> Result<rtdb_db::DeploymentRetirement> {
        self.writes.retire_deployment().await
    }

    pub async fn deployment_retirement(&self) -> Result<Option<rtdb_db::DeploymentRetirement>> {
        self.database.lock().await.deployment_retirement().await
    }
}

#[async_trait]
impl ServingDriver for MultiServing {
    fn incarnation(&self) -> [u8; 16] {
        self.incarnation
    }

    fn token_authority(&self) -> Option<TokenAuthority> { Some(self.tokens.clone()) }

    async fn apply_token_key_rotation(&self, generation: u64, unreadable_by: i64) -> Result<()> {
        self.database.lock().await.finish_token_rotation(generation, unreadable_by).await
    }

    async fn destroy_retired_token_key(&self, generation: u64) -> Result<()> {
        self.database.lock().await.destroy_retired_token_key(generation).await
    }

    async fn export_checkpoint(&self, pin: uuid::Uuid, basis: Basis, until: Option<Timestamp>, cancellation: &CancellationToken) -> Result<Vec<u8>> {
        cancellation.run_until_cancelled(self.writes.export(pin, basis, until)).await.ok_or_else(interrupted)?
    }

    async fn open_checkpoint_export(&self, ticket: Vec<u8>, cancellation: &CancellationToken) -> Result<rtdb_db::PinnedExport> {
        cancellation.run_until_cancelled(self.writes.open_export(ticket)).await.ok_or_else(interrupted)?
    }

    async fn release_checkpoint_export(&self, ticket: Vec<u8>, cancellation: &CancellationToken) -> Result<()> {
        cancellation.run_until_cancelled(self.writes.release_export(ticket)).await.ok_or_else(interrupted)?
    }

    async fn restore_checkpoint(&self, export: rtdb_db::PinnedExport, cancellation: &CancellationToken) -> Result<BranchCut> {
        cancellation.run_until_cancelled(self.writes.restore(export)).await.ok_or_else(interrupted)?
    }

    async fn checkpoint_source(&self, cancellation: &CancellationToken) -> Result<Option<rtdb_db::CheckpointSource>> {
        cancellation.run_until_cancelled(async { self.database.lock().await.checkpoint_source().await })
            .await.ok_or_else(interrupted)?
    }

    fn features(&self) -> &[&str] {
        &[LINEARIZABLE, TIMED_TRANSACT, TRANSACT_REQUIRES, PHYSICAL_ERASURE, RELATIVE_VALID_TIME, COUNT_RANGE_CONSTRAINT, GROUPED_COUNT_CONSTRAINT, ROW_VISIBILITY, "query", "query.track", "query.page", rtdb_types::rtdb_v1::QUERY_WRITTEN_BASIS,
          "query.linearizable", "branches", "branch.local-layer", "changes", "changes.progress-only", "transact", "tx-status", "sql.history",
          "sql.search", "sql.search-table", "sql.nearest-table", "sql.cosine-distance",
          rtdb_types::rtdb_v1::CLEAR_RANK_INPUT, rtdb_types::rtdb_v1::RANKED_INDEX_DECLARATIONS,
          rtdb_types::rtdb_v1::RANKED_ARTEFACT_READINESS]
    }

    fn supports_row_visibility(&self) -> bool { true }

    async fn query_scoped(&self, source: QuerySource, track: bool, now: Timestamp,
                          scope: &ServingScope, cancellation: &CancellationToken) -> Result<QueryView> {
        cancellation.run_until_cancelled(async {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let (index, generation) = self.database.lock().await.fresh_view().await?;
                index.require_local_custody()?;
                let vault = index.has_custody_groups().then(|| self.tokens.custody_vault());
                match scope.query(&index, source.clone(), now, &self.memory, vault.as_ref()).await {
                    Ok((mut view, raw)) => {
                        if track {
                            view.claim = Some(self.tokens.issuer().await?.seal_scoped_claim(raw, &scope.labels).await?);
                        }
                        self.database.lock().await.check_generation(generation).await?;
                        view.key_generation = Some(generation);
                        return Ok(view);
                    }
                    Err(error) if error.code == "rtdb/basis-ahead" && Instant::now() < deadline => {
                        tokio::time::sleep_until((Instant::now() + Duration::from_millis(50)).min(deadline)).await;
                    }
                    Err(error) => return Err(error),
                }
            }
        }).await.ok_or_else(interrupted)?
    }

    async fn managed_workload(&self, request: workload::Request,
                              cancellation: &CancellationToken) -> Result<workload::Reply> {
        cancellation.run_until_cancelled(self.writes.workload(request)).await.ok_or_else(interrupted)?
    }

    async fn transact_scoped(&self, request: Transact, scope: ServingScope,
                             cancellation: &CancellationToken) -> Result<Receipt> {
        self.transact_routed(request, Some(scope), cancellation).await
    }

    async fn tx_status_scoped(&self, key: &[u8], scope: &ServingScope,
                              cancellation: &CancellationToken) -> Result<Receipt> {
        cancellation.run_until_cancelled(async {
            self.database.lock().await.tx_status_scoped(key, scope.transaction_scope()).await
        }).await.ok_or_else(interrupted)?
    }

    async fn changes_scoped(&self, request: Changes, scope: &ServingScope,
                            cancellation: &CancellationToken) -> Result<ChangePage> {
        cancellation.run_until_cancelled(async {
            let start = Instant::now();
            loop {
                let (index, generation) = self.database.lock().await.fresh_view().await?;
                index.require_local_custody()?;
                let mut page = scope.changes(&index, &request)?;
                let remaining = request.wait.saturating_sub(start.elapsed());
                if !page.groups.is_empty() || request.until.is_some() && page.complete || remaining.is_zero() {
                    self.database.lock().await.check_generation(generation).await?;
                    page.key_generation = Some(generation);
                    if index.has_custody_groups() {
                        return crate::serving::custody::open_changes(&index, &self.tokens.custody_vault(), page).await;
                    }
                    return Ok(page);
                }
                tokio::time::sleep(remaining.min(Duration::from_millis(50))).await;
            }
        }).await.ok_or_else(interrupted)?
    }

    async fn query(
        &self,
        source: QuerySource,
        track: bool,
        now: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<QueryView> {
        cancellation
            .run_until_cancelled(async {
                let (index, generation) = self.database.lock().await.fresh_view().await?;
                index.require_local_custody()?;
                let at = if source.at == At::Linearizable {
                    At::Latest
                } else {
                    source.at
                };
                let view = index.view(&source.branch, at, source.layer, now)?;
                let cut = view.cut();
                let (snapshot, custody_groups, custody_contributors) = if index.has_custody_groups() {
                    crate::serving::custody::snapshot(&view, &self.memory, &self.tokens.custody_vault()).await?
                } else { (rtdb_query::branch_snapshot(&view, &self.memory)?, BTreeSet::new(), Some(BTreeSet::new())) };
                let claim = if track {
                    Some(self.tokens.seal_claim(&ClaimEvidence {
                        scope: ClaimScope { branch: source.branch, basis: cut.basis, layer: source.layer },
                        footprint: Default::default(),
                    }).await?)
                } else {
                    None
                };
                self.database
                    .lock()
                    .await
                    .check_generation(generation)
                    .await?;
                Ok(QueryView {
                    snapshot,
                    cut,
                    claim,
                    custody_groups,
                    custody_contributors,
                    key_generation: Some(generation),
                })
            })
            .await
            .ok_or_else(interrupted)?
    }

    async fn transact(
        &self,
        request: Transact,
        cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        self.transact_routed(request, None, cancellation).await
    }

    async fn check_custody_delivery(&self, groups: &BTreeSet<CustodyGroupId>, cancellation: &CancellationToken) -> Result<()> {
        self.check_read_delivery(None, groups, cancellation).await
    }

    async fn check_read_delivery(&self, generation: Option<u64>, groups: &BTreeSet<CustodyGroupId>, cancellation: &CancellationToken) -> Result<()> {
        if generation.is_none() && groups.is_empty() { return Ok(()); }
        cancellation.run_until_cancelled(async {
            let mut database = self.database.lock().await;
            if !groups.is_empty() {
                let index = database.fresh_index().await?;
                index.require_local_custody()?;
                crate::serving::custody::check_delivery(index, &self.tokens.custody_vault(), groups).await?;
            }
            if let Some(generation) = generation { database.check_generation(generation).await?; }
            Ok(())
        }).await.ok_or_else(interrupted)?
    }

    async fn current_custody(&self, cancellation: &CancellationToken) -> Result<BranchIndex> {
        cancellation.run_until_cancelled(async {
            let (index, generation) = self.database.lock().await.fresh_view().await?;
            index.require_local_custody()?;
            self.database.lock().await.check_generation(generation).await?;
            Ok(index)
        }).await.ok_or_else(interrupted)?
    }

    async fn submit_custody_release(&self, command: TrustedCustodyCommand, cancellation: &CancellationToken) -> Result<Receipt> {
        let request = command.into_request()?;
        cancellation.run_until_cancelled(async {
            if let Some(route) = &self.route
                && let Some(receipt) = route.forward(
                    self.incarnation, &request, MultiWriteAdmission::CustodyResponse,
                ).await?
            {
                return Ok(receipt);
            }
            self.submit_response_local(request).await
        }).await.ok_or_else(interrupted)?
    }

    async fn check_custody_release_delivery(&self, groups: &BTreeSet<CustodyGroupId>, writer: &CustodyWriterFence,
                                            generation: Option<u64>, cancellation: &CancellationToken) -> Result<()> {
        cancellation.run_until_cancelled(async {
            let vault = self.tokens.custody_vault();
            for &group in groups { vault.check_readable(group).await?; }
            let mut database = self.database.lock().await;
            let (index, current_generation) = database.fresh_view().await?;
            index.require_local_custody()?;
            crate::serving::custody_release::check_release_cut(&index, writer, groups, crate::Clock::now(&crate::SystemClock))?;
            database.check_generation(current_generation).await?;
            if let Some(selected) = generation.filter(|selected| *selected != current_generation) {
                database.check_generation(selected).await?;
            }
            Ok(())
        }).await.ok_or_else(interrupted)?
    }

    async fn reconcile_custody(&self, cancellation: &CancellationToken) -> Result<()> {
        cancellation.run_until_cancelled(async {
            let (index, _) = self.database.lock().await.fresh_view().await?;
            index.require_local_custody()?;
            crate::serving::custody::reconcile_revocations(&index, &self.tokens.custody_vault(), crate::Clock::now(&crate::SystemClock)).await
        }).await.ok_or_else(interrupted)?
    }

    async fn tx_status(&self, key: &[u8], cancellation: &CancellationToken) -> Result<Receipt> {
        cancellation
            .run_until_cancelled(async { self.database.lock().await.tx_status(key).await })
            .await
            .ok_or_else(interrupted)?
    }

    async fn original_custody_receipt(&self, key: &[u8], basis: Basis, cancellation: &CancellationToken) -> Result<Receipt> {
        let receipt = self.tx_status(key, cancellation).await?;
        if receipt.basis != basis { return Err(crate::serving::custody_release::release_unavailable()); }
        Ok(receipt)
    }

    async fn changes(
        &self,
        request: Changes,
        cancellation: &CancellationToken,
    ) -> Result<ChangePage> {
        if request.limit == 0 {
            return Err(Anomaly::incorrect(
                "rtdb/change-limit",
                "Changes requires a positive group limit",
            ));
        }
        cancellation
            .run_until_cancelled(async {
                let start = Instant::now();
                loop {
                    let mut database = self.database.lock().await;
                    let history = database.fresh_history().await?;
                    history.index.require_local_custody()?;
                    let page = changes_page(&history, &request, self.incarnation)?;
                    let remaining = request.wait.saturating_sub(start.elapsed());
                    if !page.groups.is_empty() || request.until.is_some() && page.complete || remaining.is_zero() {
                        let index = history.index.has_custody_groups().then(|| history.index.clone());
                        drop(database);
                        return match index {
                            Some(index) => crate::serving::custody::open_changes(&index, &self.tokens.custody_vault(), page).await,
                            None => Ok(page),
                        };
                    }
                    drop(database);
                    tokio::time::sleep(remaining.min(Duration::from_millis(50))).await;
                }
            })
            .await
            .ok_or_else(interrupted)?
    }
}

fn require_ordinary_request(request: &Transact) -> Result<()> {
    if request.contains_response_release() {
        return Err(crate::serving::custody_release::release_unavailable());
    }
    if request.contains_native_custody_transfer() {
        return Err(Anomaly::unsupported("rtdb/native-checkpoint-command", "Native copy enrollment requires the installed custody provider"));
    }
    Ok(())
}

fn changes_page(
    history: &ChangeHistory<'_>,
    request: &Changes,
    incarnation: [u8; 16],
) -> Result<ChangePage> {
    let position = |basis: Basis| {
        if basis.incarnation() != incarnation {
            return Err(Anomaly::incorrect(
                "rtdb/foreign-basis",
                "The basis belongs to another database incarnation",
            ));
        }
        Ok(basis.position())
    };
    let floor = history.after_floor;
    let after = request.after.map(position).transpose()?.unwrap_or(0);
    if after < floor {
        return Err(Anomaly::new(
            AnomalyCategory::NotFound,
            "rtdb/cursor-expired",
            "The changes cursor is below the retained floor",
        ));
    }
    let until = request.until.map(position).transpose()?;
    if until.is_some_and(|until| until < after) {
        return Err(Anomaly::incorrect(
            "rtdb/change-range",
            "The change interval is inverted",
        ));
    }
    if after > history.through {
        return Err(Anomaly::new(
            AnomalyCategory::Unavailable,
            "rtdb/change-basis-ahead",
            "The requested basis has not applied",
        ));
    }
    let upper = until.unwrap_or(u64::MAX).min(history.through);
    let index = history.index;
    index.view(&request.branch, At::Latest, Layer::Merged, history.now)?;
    let descending = request.order == ChangeOrder::Descending;
    let groups = history.groups.iter();
    let order: Box<dyn Iterator<Item = _>> = if descending {
        Box::new(groups.rev())
    } else {
        Box::new(groups)
    };
    let mut page = Vec::new();
    let mut next = None;
    for group in order
        .filter(|group| group.cut.basis.position() > after && group.cut.basis.position() <= upper)
    {
        let before = index
            .view(
                &request.branch,
                At::Exactly(group.previous.basis),
                Layer::Merged,
                group.cut.system_time,
            )
            .ok();
        let after = index.view(&request.branch, At::Exactly(group.cut.basis), Layer::Merged, group.cut.system_time).ok();
        let mut rows = Vec::new();
        for effect in &group.effects {
            match effect {
                BranchEffect::Write {
                    branch,
                    table,
                    event,
                } if branch == &request.branch && matches_table(request, table) => {
                    let write = BranchWrite {
                        branch: branch.clone(), table: table.clone(), id: event.id.clone(),
                        valid: Some(event.valid), erased_written_basis: None,
                    };
                    append_change_rows(&mut rows, &write, before.as_ref(), after.as_ref(), request.images)?;
                }
                BranchEffect::Erase { branch, table, id }
                    if branch
                        .as_ref()
                        .is_none_or(|branch| branch == &request.branch)
                        && matches_table(request, table) =>
                {
                    let write = BranchWrite {
                        branch: request.branch.clone(),
                        table: table.clone(),
                        id: id.clone(),
                        valid: None,
                        erased_written_basis: None,
                    };
                    append_change_rows(&mut rows, &write, None, None, request.images)?;
                }
                BranchEffect::RevokeCustodyGroup { group: id } => {
                    for write in index.custody_revocation_writes(*id, group.cut)?.into_iter().filter(|write| {
                        write.branch == request.branch && matches_table(request, &write.table)
                    }) {
                        append_change_rows(&mut rows, &write, None, None, request.images)?;
                    }
                }
                _ => {}
            }
        }
        if rows.is_empty() {
            continue;
        }
        if page.len() == request.limit {
            next = Some(group.cut.basis.position());
            break;
        }
        page.push(ChangeGroup {
            cut: group.cut,
            rows,
        });
    }
    let through = match (descending, next, page.last()) {
        (true, Some(next), _) => next,
        (false, Some(_), Some(group)) => group.cut.basis.position(),
        (true, None, _) => after,
        _ => upper.max(after),
    };
    Ok(ChangePage {
            read_custody: None,
        groups: page,
        through: Basis::new(incarnation, through),
        complete: next.is_none() && until.is_none_or(|until| until <= history.through),
        custody_groups: BTreeSet::new(),
        key_generation: Some(history.generation),
    })
}

fn matches_table(request: &Changes, table: &TableRef) -> bool {
    request.tables.is_empty() || request.tables.contains(table)
}

fn interrupted() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Interrupted,
        "rtdb/request-cancelled",
        "The request was cancelled",
    )
}

#[cfg(test)]
mod tests {
    use journal_contract::{Identity, reference::MemoryJournal};
    use rtdb_crypt::LocalKms;
    use rtdb_db::MultiOptions;
    use rtdb_query::{
        CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions,
    };
    use rtdb_storage::cache::BufferPool;
    use rtdb_store::MemoryStore;
    use rtdb_tx::transact::{Cell, Op, Outcome, Row, Window};
    use rtdb_types::{
        EntityId, Interval, StructFields, Value,
        rtdb_v1::MAIN_BRANCH,
    };
    use uuid::Uuid;

    use super::*;
    use crate::serving::{ChangeImages, ChangeKind};

    fn write(key: u8, title: &str, before: Option<Timestamp>) -> Transact {
        Transact {
            custody_request_digest: None,
            request_memory: None,
            key: vec![key; 16],
            require: vec![],
            window: Window {
                not_before: None,
                before,
            },
            guards: vec![],
            requires: vec![],
            ops: vec![Op::Put {
                branch: MAIN_BRANCH.into(),
                table: TableRef::new("public", "notes"),
                rows: vec![Row {
                    id: EntityId::Integer(1),
                    valid: None,
                    custody_group: None,
                    protected_columns: Vec::new(),
                    custody_payload: None,
                    cells: StructFields::from([(
                        "title".into(),
                        Cell::Value(Value::Utf8(title.into())),
                    )]),
                }],
                if_absent: false,
            }],
            otherwise: vec![],
        }
    }

    #[tokio::test]
    async fn assigned_query_uses_the_local_cut_and_reports_a_separate_writer_head() {
        let journal = MemoryJournal::default();
        let id = Uuid::from_u128(1);
        let store = Arc::new(MemoryStore::default());
        let kms = Arc::new(LocalKms::new(|| 10));
        let open = |node, identity| MultiDatabase::new(
            MultiOptions { database: id, node, lease_micros: 100, drift_micros: 1 },
            Arc::new(journal.handle(identity)), store.clone(), kms.clone(), Arc::new(|| Timestamp(10)),
        ).unwrap();
        let writer_node = IpAddr::from([127, 0, 0, 1]);
        let mut writer = open(writer_node, Identity::from_static("urn:rtdb:test:1"));
        writer.create().await.unwrap();
        writer.tick(&BTreeSet::from([writer_node])).await.unwrap();
        let first = writer.transact(&write(1, "one", None)).await.unwrap();
        let follower_node = IpAddr::from([127, 0, 0, 2]);
        let follower = Arc::new(MultiServing::new(open(follower_node, Identity::from_static("urn:rtdb:test:2")), MemoryBudget::new(64 << 20)));
        let reader = follower.assign_follower().await.unwrap();
        let second = writer.transact(&write(2, "two", Some(Timestamp(20)))).await.unwrap();
        let cancellation = CancellationToken::new();
        let select = |at, progress| reader.query(at, Timestamp(10), progress, &cancellation);

        for selector in [At::AtLeast(second.basis), At::Exactly(second.basis)] {
            assert_eq!(select(selector, false).await.err().unwrap().code, "rtdb/basis-ahead");
        }
        let (local, evidence) = select(At::Latest, true).await.unwrap();
        assert_eq!(local.cut.basis, first.basis);
        let evidence = evidence.unwrap();
        assert_eq!(evidence.writer_head, second.basis);
        assert_eq!(evidence.writer_head_position, evidence.applied_position + 2);
        assert_eq!(select(At::AtLeast(first.basis), false).await.unwrap().0.cut.basis, second.basis);
        assert_eq!(select(At::Exactly(first.basis), false).await.unwrap().0.cut.basis, first.basis);
        let unretained = Basis::new(*id.as_bytes(), evidence.applied_position + 1);
        assert_eq!(select(At::Exactly(unretained), false).await.err().unwrap().code, "rtdb/cursor-expired");
        assert_eq!(select(At::Exactly(Basis::new([5; 16], 0)), false).await.err().unwrap().code, "rtdb/foreign-basis");

        drop(reader);
        let live = BTreeSet::from([follower_node]);
        assert_eq!(writer.tick(&live).await.unwrap(), Role::Follower);
        follower.replay_follower().await.unwrap();
        assert_eq!(follower.database.lock().await.role(), Role::Follower);
        assert!(matches!(follower.tick(&live).await.unwrap(), Role::Writer(_)));
    }

    #[tokio::test]
    async fn multi_serving_uses_the_settled_cut_for_queries_and_change_pages() {
        let journal = MemoryJournal::default();
        let id = Uuid::from_u128(1);
        let node = IpAddr::from([127, 0, 0, 1]);
        let database = MultiDatabase::new(
            MultiOptions {
                database: id,
                node,
                lease_micros: 100,
                drift_micros: 1,
            },
            Arc::new(journal.handle(Identity::from_static("urn:rtdb:test:1"))),
            Arc::new(MemoryStore::default()),
            Arc::new(LocalKms::new(|| 10)),
            Arc::new(|| Timestamp(10)),
        )
        .unwrap();
        database.create().await.unwrap();
        let driver = MultiServing::new(database, MemoryBudget::new(64 * 1024 * 1024));
        driver.tick(&BTreeSet::from([node])).await.unwrap();
        let cancel = CancellationToken::new();
        let first = driver
            .transact(write(1, "one", None), &cancel)
            .await
            .unwrap();
        let second = driver
            .transact(write(2, "two", Some(Timestamp(20))), &cancel)
            .await
            .unwrap();
        assert_eq!(second.outcome, Outcome::Then);
        assert_eq!(second.basis.position(), first.basis.position() + 2);
        assert!(driver.tx_status(&[2; 16], &cancel).await.unwrap().replayed);

        let view = driver
            .query(
                QuerySource {
                    branch: MAIN_BRANCH.into(),
                    at: At::Linearizable,
                    layer: Layer::Merged,
                },
                true,
                Timestamp(11),
                &cancel,
            )
            .await
            .unwrap();
        assert_eq!(view.cut.basis, second.basis);
        assert!(view.claim.is_some());
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let catalog = CatalogSnapshot::new(
            "xtdb",
            std::collections::BTreeMap::from([(
                "xtdb".into(),
                DatabaseSnapshot {
                    partitions: vec![view.snapshot],
                    storage,
                },
            )]),
            false,
        )
        .unwrap();
        let prepared = rtdb_sql::prepare_in(
            "SELECT title FROM notes",
            &catalog,
            &Default::default(),
            &driver.memory,
        )
        .unwrap();
        let context = QueryContext::new(
            catalog,
            Arc::clone(&driver.memory),
            Timestamp(11),
            "UTC",
            QueryOptions::default(),
            &cancel,
        )
        .unwrap();
        let mut cursor = PreparedQuery::from_prepared(prepared, &driver.memory)
            .unwrap()
            .execute(context, Vec::new())
            .await
            .unwrap();
        let result = cursor.next_batch().await.unwrap().unwrap();
        let batch = result.batch();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            rtdb_columnar::read_value(batch.schema().field(0), batch.column(0).as_ref(), 0)
                .unwrap(),
            Value::Utf8("two".into())
        );

        let changes = || Changes {
            branch: MAIN_BRANCH.into(),
            tables: BTreeSet::new(),
            after: Some(first.basis),
            until: Some(second.basis),
            order: ChangeOrder::Ascending,
            images: ChangeImages::Both,
            limit: 1,
            wait: Duration::ZERO,
        };
        let page = driver.changes(changes(), &cancel).await.unwrap();
        assert!(page.complete);
        assert_eq!(page.through, second.basis);
        assert_eq!(page.groups.len(), 1);
        let [closed, opened] = page.groups[0].rows.as_slice() else { panic!("expected a closure and an opening"); };
        assert!(matches!(closed.kind, ChangeKind::Closed));
        assert!(matches!(opened.kind, ChangeKind::Opened));
        assert_eq!(
            closed.before.as_ref().unwrap().cells["title"],
            Value::Utf8("one".into())
        );
        assert_eq!(
            opened.after.as_ref().unwrap().cells["title"],
            Value::Utf8("two".into())
        );

        driver
            .database
            .lock()
            .await
            .checkpoint(vec![])
            .await
            .unwrap();
        let error = driver.changes(changes(), &cancel).await.err().unwrap();
        assert_eq!(error.code, "rtdb/cursor-expired");
        let from_beginning = Changes { after: None, ..changes() };
        let error = driver.changes(from_beginning, &cancel).await.err().unwrap();
        assert_eq!(error.code, "rtdb/cursor-expired");
    }

    #[tokio::test]
    async fn multi_serving_scoped_requires_preserves_projected_coordinates() {
        use rtdb_tx::transact::{Declaration, Guard, REQUIREMENT_FAILED, Version, Visibility};
        use rtdb_types::rtdb_v1::LabelScope;

        let journal = MemoryJournal::default();
        let id = Uuid::from_u128(1);
        let node = IpAddr::from([127, 0, 0, 1]);
        let database = MultiDatabase::new(
            MultiOptions { database: id, node, lease_micros: 100, drift_micros: 1 },
            Arc::new(journal.handle(Identity::from_static("urn:rtdb:test:1"))),
            Arc::new(MemoryStore::default()),
            Arc::new(LocalKms::new(|| 10)), Arc::new(|| Timestamp(10)),
        ).unwrap();
        database.create().await.unwrap();
        let driver = MultiServing::new(database, MemoryBudget::new(64 * 1024 * 1024));
        driver.tick(&BTreeSet::from([node])).await.unwrap();
        assert!(driver.features().contains(&TRANSACT_REQUIRES));
        let cancel = CancellationToken::new();
        let mut declared = write(1, "declaration", None);
        declared.ops = vec![Op::Declare {
            table: TableRef::new("public", "notes"), change: Declaration::Visibility(Visibility { column: "label".into() }),
        }];
        assert_eq!(driver.transact(declared, &cancel).await.unwrap().outcome, Outcome::Then);
        let labelled = |key, label, id| {
            let mut submission = write(key, "row", None);
            let Op::Put { rows, .. } = &mut submission.ops[0] else { unreachable!() };
            rows[0].id = EntityId::Integer(id);
            rows[0].valid = Some(Interval::ALL.into());
            rows[0].cells.insert("label".into(), Cell::Value(Value::Binary(vec![label])));
            submission
        };
        let scope = ServingScope {
            labels: LabelScope { read: BTreeSet::from([vec![1]]), write: BTreeSet::from([vec![1]]) },
            receipt_namespace: [7; 32],
        };
        let receipt = driver.transact_scoped(labelled(2, 1, 1), scope.clone(), &cancel).await.unwrap();
        assert_eq!(receipt.outcome, Outcome::Then);
        let raw = driver.transact(labelled(3, 2, 2), &cancel).await.unwrap();
        let source = QuerySource { branch: MAIN_BRANCH.into(), at: At::Linearizable, layer: Layer::Merged };
        let view = driver.query_scoped(source, true, Timestamp(11), &scope, &cancel).await.unwrap();
        assert_eq!(view.cut.basis, receipt.basis);
        assert_ne!(view.cut.basis.incarnation(), raw.basis.incarnation());
        let claim = view.claim.unwrap();
        let changes = || Changes {
            branch: MAIN_BRANCH.into(), tables: BTreeSet::new(), after: Some(receipt.basis), until: None,
            order: ChangeOrder::Ascending, images: ChangeImages::Both, limit: 1, wait: Duration::ZERO,
        };
        let page = driver.changes_scoped(changes(), &scope, &cancel).await.unwrap();
        assert_eq!(page.through, receipt.basis);
        assert!(page.groups.is_empty());
        let status = driver.tx_status_scoped(&[2; 16], &scope, &cancel).await.unwrap();
        assert_eq!(status.basis, receipt.basis);
        assert!(status.replayed);
        assert!(driver.tx_status(&[2; 16], &cancel).await.is_err());

        let version = |id, expect| Guard::Version {
            branch: MAIN_BRANCH.into(), table: TableRef::new("public", "notes"),
            id: EntityId::Integer(id), valid_at: None, expect,
        };
        let mut transaction = labelled(4, 1, 3);
        transaction.require.push(TRANSACT_REQUIRES.into());
        transaction.requires = vec![
            Guard::Unchanged(claim),
            version(1, Version::Written(receipt.basis)),
            version(2, Version::Absent),
        ];
        transaction.guards = vec![version(3, Version::Present)];
        transaction.otherwise = labelled(5, 1, 4).ops;

        let refused = driver.transact_scoped(transaction.clone(), scope.clone(), &cancel).await.unwrap();
        assert_eq!(refused.outcome, Outcome::Refused {
            error: Anomaly::new(AnomalyCategory::Conflict, REQUIREMENT_FAILED, "Requirement 2 failed"),
        });
        assert_eq!(refused.basis, receipt.basis);
        assert_eq!(refused.system_time, receipt.system_time);
        assert!(driver.changes_scoped(changes(), &scope, &cancel).await.unwrap().groups.is_empty());
        let replayed = Receipt { replayed: true, ..refused };
        let retry = driver.transact_scoped(transaction.clone(), scope.clone(), &cancel).await.unwrap();
        assert_eq!(retry, replayed);
        assert_eq!(driver.tx_status_scoped(&transaction.key, &scope, &cancel).await.unwrap(), replayed);
        assert_eq!(driver.tx_status(&transaction.key, &cancel).await.unwrap_err().code, "rtdb/unknown-transaction");

        transaction.key = vec![5; 16];
        transaction.requires.truncate(2);
        let selected = driver.transact_scoped(transaction, scope.clone(), &cancel).await.unwrap();
        assert_eq!(selected.outcome, Outcome::Otherwise { failed: vec![0] });
        let page = driver.changes_scoped(changes(), &scope, &cancel).await.unwrap();
        assert_eq!(page.through, selected.basis);
        let [group] = page.groups.as_slice() else { panic!("expected one change group"); };
        let [row] = group.rows.as_slice() else { panic!("expected one changed row"); };
        assert_eq!(row.id, EntityId::Integer(4));
    }
}
