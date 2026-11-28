use std::future::Future;
use std::{collections::BTreeSet, net::IpAddr, sync::{Arc, Weak}, time::Duration};

use async_trait::async_trait;
use rtdb_crypt::tokens::TokenAuthority;
use rtdb_db::{ChangeHistory, MultiDatabase, NativeCheckpointInstallation, NativeDependentCarrier, NativeDependentOriginal, PinnedExport, Role, ServingAuthority, WritePipeline};
use rtdb_index::branches::{BranchCut, BranchEffect, BranchIndex, BranchWrite};
use rtdb_index::branches::store::{KeyRange, StorePage, StoreRead, StoreWriterClaim, sizes::StorageSizes};
use rtdb_tx::transact::{
    CustodyGroupId, CustodyWriterFence, PHYSICAL_ERASURE, ROW_CUSTODY, Receipt, Transact, TrustedCustodyCommand, workload,
};
use rtdb_types::{
    Anomaly, AnomalyCategory, AnomalyKind, EntityId, MemoryBudget, MemoryReservation, Result, TableRef, Timestamp,
    rtdb_v1::{
        At, Basis, ClaimEvidence, ClaimScope, Layer, ManagedCheckpointReservation, ManagedCheckpointTarget,
        NativeCheckpointCustody, NativeCheckpointMember, NativeCustodyFence, NativeDependentCustodyDisposition,
        NativeDependentCustodyEnrollment, NativeDependentCustodyOrder, NativeDependentCustodyScope, NativeDependentRead, NativeDependentSourceGeneration, PinnedCheckpointIdentity,
    },
};
use tokio::{sync::Mutex, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::serving::{
    ChangeGroup, ChangeOrder, ChangePage, Changes, FamilyReviewSelection, QuerySource, QueryView, ROW_VISIBILITY,
    ServingDriver, ServingScope, StoreReadTurn, StoreVersionHoldReply, StoreVersionHoldRequest, TimedStoreAppend, TimedStoreReceipt,
    WorkloadReadCut, append_change_rows,
};
use crate::{Database, DatabaseCatalog};

mod ranked;
pub use ranked::MaintenanceSchedule;

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

enum ServingAccess {
    Unscoped,
    Activated(Arc<dyn ServingAuthority>),
    Offline,
}

pub struct MultiServing {
    database: Arc<Mutex<MultiDatabase>>,
    writes: WritePipeline,
    incarnation: [u8; 16],
    access: ServingAccess,
    tokens: TokenAuthority,
    features: Vec<&'static str>,
    memory: Arc<MemoryBudget>,
    route: Option<Arc<dyn MultiWriteRoute>>,
    classic_workload_fence: Option<workload::Fence>,
    ranked_schedule: Mutex<Option<Instant>>,
}

pub struct MultiFollower {
    serving: Arc<MultiServing>,
    owner: Arc<()>,
}

#[derive(Debug)]
pub struct FollowerStoreDelivery {
    basis: Basis,
    generation: u64,
    owner: Weak<()>,
}

impl FollowerStoreDelivery {
    pub fn include(&mut self, other: Self) -> Result<()> {
        if !self.owner.ptr_eq(&other.owner) || self.generation != other.generation
            || self.basis.incarnation() != other.basis.incarnation()
        {
            return Err(interrupted());
        }
        self.basis = self.basis.min(other.basis);
        Ok(())
    }
}

pub struct MultiFollowerProgress {
    pub applied_position: u64,
    pub retained_from: Basis,
    pub writer_head: Basis,
    pub writer_head_position: u64,
}

impl MultiFollower {
    pub fn matches_driver(&self, driver: &dyn ServingDriver) -> bool {
        std::ptr::addr_eq(Arc::as_ptr(&self.serving) as *const dyn ServingDriver, driver)
    }

    pub fn supports_ranked_ivf(&self) -> bool {
        self.serving.supports_ranked_ivf()
    }

    pub async fn read_store(
        &self, read: StoreRead, at: At, now: Timestamp, cancellation: &CancellationToken,
    ) -> Result<(StorePage, FollowerStoreDelivery)> {
        self.serving.serve(async {
            cancellation.run_until_cancelled(async {
                let database = self.serving.database.lock().await;
                let (index, generation) = database.follower_view(&self.owner)?;
                require_plain_follower(&index)?;
                if let At::Exactly(basis) = at {
                    index.cut(At::AtLeast(basis))?;
                }
                let page = index.view(rtdb_types::rtdb_v1::MAIN_BRANCH, at, Layer::Merged, now)?.store_page(&read)?;
                database.check_follower_store_basis(&self.owner, page.cut.basis).await?;
                database.check_generation(generation).await?;
                let delivery = FollowerStoreDelivery { basis: page.cut.basis, generation, owner: Arc::downgrade(&self.owner) };
                Ok((page, delivery))
            }).await.ok_or_else(interrupted)?
        }).await
    }

    pub async fn check_store_delivery(&self, delivery: &FollowerStoreDelivery, cancellation: &CancellationToken) -> Result<()> {
        self.serving.serve(async {
            cancellation.run_until_cancelled(async {
                if !delivery.owner.ptr_eq(&Arc::downgrade(&self.owner)) {
                    return Err(interrupted());
                }
                let database = self.serving.database.lock().await;
                let (index, generation) = database.follower_view(&self.owner)?;
                require_plain_follower(&index)?;
                if generation != delivery.generation
                    || index.head().basis.incarnation() != delivery.basis.incarnation()
                {
                    return Err(interrupted());
                }
                database.check_follower_store_basis(&self.owner, delivery.basis).await?;
                database.check_generation(generation).await
            }).await.ok_or_else(interrupted)?
        }).await
    }

    pub async fn store_changes(&self, request: Changes, cancellation: &CancellationToken) -> Result<(ChangePage, FollowerStoreDelivery)> {
        let after = request.after.ok_or_else(|| Anomaly::incorrect("rtdb/follower-source", "A byte-store watch requires its resume basis"))?;
        if request.branch != rtdb_types::rtdb_v1::MAIN_BRANCH
            || request.tables.len() != 1
            || !request.tables.contains(&rtdb_index::branches::store::store_table())
            || !matches!(request.images, crate::serving::ChangeImages::Keys)
        {
            return Err(Anomaly::unsupported("rtdb/follower-source", "A byte-store watch selects keys from the store table"));
        }
        {
            let (index, _) = self.serving.database.lock().await.follower_view(&self.owner)?;
            require_plain_follower(&index)?;
        }
        let page = self.serving.changes(request, cancellation).await?;
        let generation = page.key_generation.ok_or_else(interrupted)?;
        let delivery = FollowerStoreDelivery { basis: after, generation, owner: Arc::downgrade(&self.owner) };
        self.check_store_delivery(&delivery, cancellation).await?;
        Ok((page, delivery))
    }

    pub async fn query_ranked_ivf(&self, source: QuerySource, target: &crate::serving::ranked_ivf::Target,
        now: Timestamp, observe_progress: bool, cancellation: &CancellationToken,
    ) -> Result<(QueryView, crate::serving::ranked_ivf::Catalog, Option<MultiFollowerProgress>)> {
        if !self.supports_ranked_ivf() { return Err(crate::serving::ranked_ivf::unsupported()); }
        let (view, catalog) = self.serving.ranked_query(source, target, now, Some(&self.owner), cancellation).await?;
        let progress = self.progress(view.cut, observe_progress, cancellation).await?;
        Ok((view, catalog, progress))
    }

    pub async fn query_ranked_artefacts(&self, database: &str, source: QuerySource, now: Timestamp,
        observe_progress: bool, cancellation: &CancellationToken,
    ) -> Result<(QueryView, Option<MultiFollowerProgress>)> {
        let view = self.serving.ranked_artefacts(database, source, now, Some(&self.owner), cancellation).await?;
        let progress = self.progress(view.cut, observe_progress, cancellation).await?;
        Ok((view, progress))
    }

    async fn progress(&self, cut: BranchCut, observe: bool, cancellation: &CancellationToken)
        -> Result<Option<MultiFollowerProgress>> {
        self.serving.serve(async {
            if !observe { return Ok(None); }
            cancellation.run_until_cancelled(async {
                let mut database = self.serving.database.lock().await;
                let (index, _) = database.follower_view(&self.owner)?;
                let applied_position = index.applied_position(cut)?;
                let retained_from = index.oldest_retained_cut().basis;
                let (writer_head, writer_head_position) = database.follower_writer_head(&self.owner).await?;
                if writer_head.incarnation() != cut.basis.incarnation() || writer_head_position < applied_position {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb/follower-progress-unavailable", "The writer head does not cover the selected follower cut"));
                }
                Ok(Some(MultiFollowerProgress { applied_position, retained_from, writer_head, writer_head_position }))
            }).await.ok_or_else(interrupted)?
        }).await
    }

    pub async fn query(
        &self, at: At, now: Timestamp, observe_progress: bool, cancellation: &CancellationToken,
    ) -> Result<(QueryView, Option<MultiFollowerProgress>)> {
        let view = self.query_view(at, now, cancellation).await?;
        let progress = self.progress(view.cut, observe_progress, cancellation).await?;
        Ok((view, progress))
    }

    /// Select a locally retained snapshot for an assigned exact scope.
    pub async fn retained_view(
        &self, basis: Basis, now: Timestamp, cancellation: &CancellationToken,
    ) -> Result<QueryView> {
        self.query_view(At::Exactly(basis), now, cancellation).await
    }

    async fn query_view(
        &self, at: At, now: Timestamp, cancellation: &CancellationToken,
    ) -> Result<QueryView> {
        self.serving.serve(async {
            cancellation.run_until_cancelled(async {
                let (index, generation) = self.serving.database.lock().await.follower_view(&self.owner)?;
                require_plain_follower(&index)?;
                match at {
                    At::Exactly(basis) => { index.cut(At::AtLeast(basis))?; }
                    At::Latest | At::AtLeast(_) => (),
                    _ => return Err(Anomaly::unsupported("rtdb/follower-source", "The follower selects Latest, AtLeast or Exactly")),
                }
                let view = index.view(rtdb_types::rtdb_v1::MAIN_BRANCH, at, Layer::Merged, now).map_err(|error| {
                    if error.is(AnomalyKind::UnknownBasis) {
                        Anomaly::new(AnomalyCategory::NotFound, "rtdb/cursor-expired", "The replica has no retained exact cut at this basis")
                    } else { error }
                })?;
                let cut = view.cut();
                let snapshot = rtdb_query::branch_snapshot(&view, &self.serving.memory)?;
                Ok(QueryView {
                    snapshot, cut, claim: None, custody_groups: BTreeSet::new(),
                    custody_contributors: Some(BTreeSet::new()), scoped_source_basis: None, key_generation: Some(generation),
                })
            }).await.ok_or_else(interrupted)?
        }).await
    }

    pub async fn check_delivery(&self, basis: Basis, generation: u64, now: Timestamp, cancellation: &CancellationToken) -> Result<()> {
        self.serving.serve(async {
            cancellation.run_until_cancelled(async {
                let database = self.serving.database.lock().await;
                let (index, _) = database.follower_view(&self.owner)?;
                require_plain_follower(&index)?;
                index.view(rtdb_types::rtdb_v1::MAIN_BRANCH, At::Exactly(basis), Layer::Merged, now)?;
                database.check_generation(generation).await
            }).await.ok_or_else(interrupted)?
        }).await
    }

    pub async fn check_retained_delivery(&self, generation: u64, cancellation: &CancellationToken) -> Result<()> {
        self.serving.serve(async {
            cancellation.run_until_cancelled(async {
                let database = self.serving.database.lock().await;
                let (index, _) = database.follower_view(&self.owner)?;
                require_plain_follower(&index)?;
                database.check_generation(generation).await
            }).await.ok_or_else(interrupted)?
        }).await
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

    async fn forward_workload(&self, _incarnation: [u8; 16], _request: workload::Request)
        -> Result<workload::Reply> {
        Err(Anomaly::unsupported("rtdb/managed-workload-route", "The host has no managed workload peer route"))
    }
}

pub enum MultiWriteAdmission<'a> {
    Ordinary(Option<&'a ServingScope>),
    CustodyResponse,
}

impl MultiServing {
    async fn serve<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        let permit = match &self.access {
            ServingAccess::Activated(authority) => Some(authority.acquire().await?),
            ServingAccess::Unscoped => None,
            ServingAccess::Offline => return Err(Anomaly::new(
                AnomalyCategory::Unavailable,
                "rtdb/activation-fenced",
                "The commissioned database is open for offline operations",
            )),
        };
        let result = operation.await?;
        if let Some(permit) = permit {
            permit.check()?;
        }
        Ok(result)
    }

    fn require_unmanaged_effects(&self) -> Result<()> {
        if !matches!(self.access, ServingAccess::Unscoped) {
            return Err(Anomaly::unsupported(
                "rtdb/activation-effects",
                "This operation requires an effect provider commissioned for database activation",
            ));
        }
        Ok(())
    }

    fn check_appointment_origin(&self, appointment: &workload::ManagedAppointment) -> Result<()> {
        if self.classic_workload_fence.as_ref().is_some_and(|imported| !appointment.fence().at_or_after_import(imported)) {
            return Err(workload::denied());
        }
        Ok(())
    }

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

    pub async fn portable_backup(&self) -> Result<rtdb_db::PortableBackup> {
        self.database.lock().await.portable_backup().await
    }

    pub async fn install_portable_backup(&self, backup: rtdb_db::VerifiedBackup) -> Result<rtdb_db::InstalledBackup> {
        self.database.lock().await.install_portable_backup(backup).await
    }

    pub async fn installed_backup(&self) -> Result<Option<rtdb_db::InstalledBackup>> {
        self.database.lock().await.installed_backup().await
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

    pub async fn native_dependent_custody(
        &self, scope: NativeDependentCustodyScope,
    ) -> Result<Option<(NativeDependentCustodyEnrollment, Option<NativeDependentCustodyDisposition>)>> {
        Ok(self.database.lock().await.fresh_index().await?.native_dependent_custody(scope)?
            .map(|(enrollment, disposition)| (enrollment.clone(), disposition.cloned())))
    }

    pub async fn native_dependent_custody_roster(&self, scope: NativeDependentCustodyScope) -> Result<Vec<NativeCheckpointMember>> {
        self.database.lock().await.fresh_index().await?.native_dependent_custody_roster(scope)
    }

    pub async fn require_native_dependent_publication(&self, reservation: ManagedCheckpointReservation, basis: Basis) -> Result<()> {
        self.database.lock().await.fresh_index().await?.require_native_dependent_publication(reservation, basis)
    }

    pub async fn require_native_dependent_fence(&self, scope: NativeDependentCustodyScope, fence: &NativeCustodyFence) -> Result<()> {
        self.database.lock().await.require_native_dependent_fence(scope, fence).await
    }

    pub async fn native_dependent_custody_status(&self, scope: NativeDependentCustodyScope, fence: &NativeCustodyFence) -> Result<Option<Receipt>> {
        self.database.lock().await.native_dependent_custody_status(scope, fence).await
    }

    pub async fn record_native_dependent_custody(&self, order: NativeDependentCustodyOrder) -> Result<Receipt> {
        self.database.lock().await.record_native_dependent_custody(order).await
    }

    pub async fn record_native_dependent_in_generation(
        &self, order: NativeDependentCustodyOrder, expected: Option<NativeDependentSourceGeneration>,
    ) -> Result<Receipt> {
        self.database.lock().await.record_native_dependent_in_generation(order, expected).await
    }

    pub async fn native_dependent_source_generation(&self, scope: NativeDependentCustodyScope) -> Result<NativeDependentSourceGeneration> {
        self.database.lock().await.native_dependent_source_generation(scope).await
    }

    pub async fn native_checkpoint_installation(&self, reservation: ManagedCheckpointReservation) -> Result<NativeCheckpointInstallation> {
        self.database.lock().await.native_checkpoint_installation(reservation).await
    }

    pub async fn native_dependent_original(
        &self,
        custody: &NativeCheckpointCustody,
        read: &NativeDependentRead,
        writer: &CustodyWriterFence,
    ) -> Result<NativeDependentOriginal> {
        self.require_native_copy_readable(custody).await?;
        self.database.lock().await.native_dependent_original(custody, read, writer).await
    }

    pub async fn native_dependent_carrier(
        &self, custody: &NativeCheckpointCustody, read: &NativeDependentRead,
        writer: &CustodyWriterFence, binding: [u8; 32],
    ) -> Result<NativeDependentCarrier> {
        self.require_native_copy_readable(custody).await?;
        self.database.lock().await.native_dependent_carrier(custody, read, writer, binding).await
    }

    async fn require_native_copy_readable(&self, custody: &NativeCheckpointCustody) -> Result<()> {
        let vault = self.tokens.custody_vault();
        for &(group, _) in &custody.groups {
            vault.check_readable(group).await?;
        }
        Ok(())
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
        let database = database.with_manifest_budget(Arc::clone(&memory));
        let incarnation = database.incarnation();
        let access = match database.serving_authority() {
            Some(authority) => ServingAccess::Activated(authority),
            None if database.activation_required() => ServingAccess::Offline,
            None => ServingAccess::Unscoped,
        };
        let tokens = database.token_authority();
        let mut features = database.write_features().to_vec();
        if !matches!(access, ServingAccess::Unscoped) {
            features.retain(|&feature| feature != PHYSICAL_ERASURE && feature != ROW_CUSTODY);
        }
        features.extend([
            "query", "query.track", "query.page", rtdb_types::rtdb_v1::QUERY_WRITTEN_BASIS,
            "query.linearizable", "branch.local-layer", "changes", "changes.progress-only", "transact", "tx-status",
            "sql.history", "sql.search", "sql.search-table", "sql.nearest-table", "sql.cosine-distance",
            rtdb_types::rtdb_v1::CLEAR_RANK_INPUT, rtdb_types::rtdb_v1::RANKED_ARTEFACT_READINESS,
        ]);
        let database = Arc::new(Mutex::new(database));
        let writes = WritePipeline::start(Arc::clone(&database));
        Self {
            incarnation,
            access,
            tokens,
            features,
            database,
            writes,
            memory,
            route: None,
            classic_workload_fence: None,
            ranked_schedule: Mutex::new(None),
        }
    }

    pub fn with_write_route(mut self, route: Arc<dyn MultiWriteRoute>) -> Self {
        self.route = Some(route);
        self
    }

    pub fn with_classic_workload_fence(mut self, fence: workload::Fence) -> Self {
        self.classic_workload_fence = Some(fence);
        self
    }

    /// Applies the authorized request only under this node's active writer lease.
    /// Peer ingress calls this entry point and cannot submit a second forwarding attempt.
    pub async fn transact_local(
        &self, mut request: Transact, scope: Option<ServingScope>,
    ) -> Result<Receipt> {
        self.serve(async {
            if self.classic_workload_fence.is_some() { return Err(workload::denied()); }
            require_ordinary_request(&request)?;
            if request.uses_custody() || request.uses_erasure() {
                self.require_unmanaged_effects()?;
            }
            if let Some(scope) = scope {
                return self.writes.transact_scoped(request, scope.labels, scope.receipt_namespace).await;
            }
            if request.uses_custody() && self.features().contains(&ROW_CUSTODY) {
                crate::serving::custody::prepare(&mut request, &self.tokens.custody_vault()).await?;
            }
            self.writes.transact(request).await
        }).await
    }

    async fn transact_routed(
        &self, request: Transact, scope: Option<ServingScope>, cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        self.serve(async {
            require_ordinary_request(&request)?;
            if request.uses_custody() || request.uses_erasure() {
                self.require_unmanaged_effects()?;
            }
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
        }).await
    }

    /// Accepts only the narrow ordered response-release command from an admitted peer.
    pub async fn managed_workload_local(&self, mut request: workload::Request) -> Result<workload::Reply> {
        self.require_unmanaged_effects()?;
        self.serve(async {
            if matches!(&request.operation, workload::Operation::Submit {
                authority: workload::Authority::OriginalFamilyWrite { .. } | workload::Authority::ProvisionBranch { .. }, ..
            } | workload::Operation::Status { family: workload::Family::OriginalFamilyWrite, .. }
                | workload::Operation::ImportedStatus { family: workload::Family::OriginalFamilyWrite, .. }
                | workload::Operation::Settle { family: workload::Family::OriginalFamilyWrite, .. }
                | workload::Operation::ImportedSettle { family: workload::Family::OriginalFamilyWrite, .. }) {
                return Err(workload::denied());
            }
            if let Some(expected) = &self.classic_workload_fence {
                if let workload::Operation::Status { key, family } = &request.operation {
                    request.operation = workload::Operation::ImportedStatus {
                        key: key.clone(), family: *family, after: Basis::from_bytes(&expected.applied.basis)?,
                    };
                    return self.writes.workload(request).await;
                }
                if let workload::Operation::Settle { key, family, digest } = &request.operation {
                    request.operation = workload::Operation::ImportedSettle {
                        key: key.clone(), family: *family, digest: *digest,
                        after: Basis::from_bytes(&expected.applied.basis)?,
                    };
                    return self.writes.workload(request).await;
                }
                let covered = match &request.operation {
                    workload::Operation::Submit { authority, .. } => authority.permits_imported_workload(expected),
                    workload::Operation::Observe(fence) | workload::Operation::ReadRoot { fence, .. }
                    | workload::Operation::ReadCustody { fence, .. } => fence.at_or_after_import(expected),
                    workload::Operation::Predecessor(predecessor) => predecessor.fence.at_or_after_import(expected),
                    _ => false,
                };
                if !covered { return Err(workload::denied()); }
            }
            self.writes.workload(request).await
        }).await
    }

    pub async fn submit_response_local(&self, request: Transact) -> Result<Receipt> {
        self.require_unmanaged_effects()?;
        self.serve(async {
            if self.classic_workload_fence.is_some() || !request.is_response_release() {
                return Err(crate::serving::custody_release::release_unavailable());
            }
            self.writes.transact(request).await
        }).await
    }

    pub async fn replay_follower(&self) -> Result<()> {
        self.database.lock().await.fresh_index().await?;
        Ok(())
    }

    pub async fn tick(&self, live_nodes: &BTreeSet<IpAddr>) -> Result<Role> {
        self.writes.tick(live_nodes.clone()).await
    }

    pub async fn shutdown_writer(&self) -> Result<()> {
        self.writes.shutdown().await
    }

    pub async fn local_writer_epoch(&self) -> Option<u64> {
        match self.database.lock().await.role() {
            Role::Follower => None,
            Role::Writer(epoch) => Some(epoch.0),
        }
    }

    pub async fn watch_applied(&self) -> rtdb_db::AppliedWatch {
        self.database.lock().await.watch_applied()
    }

    pub async fn next_deadline(&self, live_nodes: &BTreeSet<IpAddr>) -> Option<Duration> {
        self.database.lock().await.next_deadline(live_nodes)
    }

    pub async fn is_writer(&self) -> bool {
        matches!(self.database.lock().await.role(), Role::Writer(_))
    }

    pub async fn retire_deployment(&self) -> Result<rtdb_db::DeploymentRetirement> {
        self.require_unmanaged_effects()?;
        self.writes.retire_deployment().await
    }

    pub async fn deployment_retirement(&self) -> Result<Option<rtdb_db::DeploymentRetirement>> {
        self.database.lock().await.deployment_retirement().await
    }
}

#[async_trait]
impl ServingDriver for MultiServing {
    fn classic_workload_fence(&self) -> Option<&workload::Fence> { self.classic_workload_fence.as_ref() }
    fn incarnation(&self) -> [u8; 16] {
        self.incarnation
    }

    fn token_authority(&self) -> Option<TokenAuthority> { Some(self.tokens.clone()) }

    async fn apply_token_key_rotation(&self, generation: u64, unreadable_by: i64) -> Result<()> {
        self.require_unmanaged_effects()?;
        self.serve(async {
            self.database.lock().await.finish_token_rotation(generation, unreadable_by).await
        }).await
    }

    async fn destroy_retired_token_key(&self, generation: u64) -> Result<()> {
        self.require_unmanaged_effects()?;
        self.serve(async {
            self.database.lock().await.destroy_retired_token_key(generation).await
        }).await
    }

    async fn export_checkpoint(&self, pin: uuid::Uuid, basis: Basis, until: Option<Timestamp>, cancellation: &CancellationToken) -> Result<Vec<u8>> {
        self.require_unmanaged_effects()?;
        self.serve(async {
            cancellation.run_until_cancelled(self.writes.export(pin, basis, until)).await.ok_or_else(interrupted)?
        }).await
    }

    async fn open_checkpoint_export(&self, ticket: Vec<u8>, cancellation: &CancellationToken) -> Result<rtdb_db::PinnedExport> {
        self.require_unmanaged_effects()?;
        self.serve(async {
            cancellation.run_until_cancelled(self.writes.open_export(ticket)).await.ok_or_else(interrupted)?
        }).await
    }

    async fn release_checkpoint_export(&self, ticket: Vec<u8>, cancellation: &CancellationToken) -> Result<()> {
        self.require_unmanaged_effects()?;
        self.serve(async {
            cancellation.run_until_cancelled(self.writes.release_export(ticket)).await.ok_or_else(interrupted)?
        }).await
    }

    async fn restore_checkpoint(&self, export: rtdb_db::PinnedExport, cancellation: &CancellationToken) -> Result<BranchCut> {
        self.require_unmanaged_effects()?;
        self.serve(async {
            cancellation.run_until_cancelled(self.writes.restore(export)).await.ok_or_else(interrupted)?
        }).await
    }

    fn features(&self) -> &[&str] {
        &self.features
    }

    fn supports_row_visibility(&self) -> bool { true }

    fn supports_ranked_ivf(&self) -> bool { cfg!(feature = "search") }

    async fn query_ranked_ivf(&self, source: QuerySource, target: &crate::serving::ranked_ivf::Target,
        now: Timestamp, cancellation: &CancellationToken) -> Result<(QueryView, crate::serving::ranked_ivf::Catalog)> {
        self.serve(async {
            if !self.supports_ranked_ivf() { return Err(crate::serving::ranked_ivf::unsupported()); }
            self.ranked_query(source, target, now, None, cancellation).await
        }).await
    }

    async fn query_ranked_artefacts(&self, database: &str, source: QuerySource, now: Timestamp,
        cancellation: &CancellationToken) -> Result<QueryView> {
        self.serve(async {
            self.ranked_artefacts(database, source, now, None, cancellation).await
        }).await
    }

    async fn query_scoped(&self, source: QuerySource, track: bool, now: Timestamp,
                          scope: &ServingScope, cancellation: &CancellationToken) -> Result<QueryView> {
        self.serve(async {
            cancellation.run_until_cancelled(async {
                let deadline = Instant::now() + Duration::from_secs(30);
                let mut applied = self.database.lock().await.watch_applied();
                loop {
                    applied.mark_seen();
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
                        Err(error) if error.is(AnomalyKind::BasisAhead) && Instant::now() < deadline => {
                            applied.changed(deadline).await?;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn managed_workload(&self, request: workload::Request,
                              cancellation: &CancellationToken) -> Result<workload::Reply> {
        self.require_unmanaged_effects()?;
        self.serve(async {
            cancellation.run_until_cancelled(async {
                let local = self.managed_workload_local(request.clone()).await;
                if local.as_ref().err().is_some_and(|error| error.is(AnomalyKind::NoWriter))
                    && let Some(route) = &self.route
                {
                    return route.forward_workload(self.incarnation, request).await;
                }
                local
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn managed_original_family(&self, mut request: workload::Request,
                                     cancellation: &CancellationToken) -> Result<workload::Reply> {
        self.require_unmanaged_effects()?;
        self.serve(async {
            let imported = self.classic_workload_fence.as_ref();
            let after = imported.map(|fence| Basis::from_bytes(&fence.applied.basis)).transpose()?;
            match &request.operation {
                workload::Operation::ReadRoot { fence, .. }
                    if imported.is_none_or(|origin| fence.at_or_after_import(origin)) => {}
                workload::Operation::Submit { authority: workload::Authority::OriginalFamilyWrite { fence, writer, created, .. }, .. }
                    if imported.is_none_or(|origin| fence.covers_imported_family_line(origin, writer, *created)) => {}
                workload::Operation::Submit { authority: workload::Authority::ProvisionBranch { fence, .. }, .. }
                    if imported.is_none_or(|origin| fence.at_or_after_import(origin)) => {}
                workload::Operation::Status { key, family: family @ workload::Family::OriginalFamilyWrite } => {
                    if let Some(after) = after {
                        request.operation = workload::Operation::ImportedStatus { key: key.clone(), family: *family, after };
                    }
                }
                _ => return Err(workload::denied()),
            }
            let reply = cancellation.run_until_cancelled(self.writes.workload(request)).await.ok_or_else(interrupted)??;
            if let (Some(after), workload::Reply::Receipt(receipt)) = (after, &reply)
                && (receipt.basis.incarnation() != after.incarnation() || receipt.basis <= after)
            {
                return Err(workload::denied());
            }
            Ok(reply)
        }).await
    }

    async fn query_original_family_review(&self, source: QuerySource, review: FamilyReviewSelection,
        now: Timestamp, cancellation: &CancellationToken) -> Result<QueryView> {
        self.serve(async {
            let At::Exactly(basis) = source.at else { return Err(workload::denied()); };
            let imported = self.classic_workload_fence.as_ref().map(|fence| Basis::from_bytes(&fence.applied.basis)).transpose()?;
            if source.layer != Layer::Merged || basis < review.created
                || imported.is_some_and(|after| review.created.incarnation() != after.incarnation() || review.created <= after)
            {
                return Err(workload::denied());
            }
            cancellation.run_until_cancelled(async {
                let (index, generation) = self.database.lock().await.fresh_view().await?;
                index.require_local_custody()?;
                if index.has_visibility_history() { return Err(workload::denied()); }
                workload::require_family_branch(&index, &source.branch, review.created, now)?;
                let view = index.view(&source.branch, At::Exactly(basis), Layer::Merged, now)?;
                let (_, _, selected) = index.custody_review_at(&source.branch, basis, review.valid_at, review.group, review.selected)?;
                let target = &selected.target;
                let event = view.row(&target.table, &target.id, review.valid_at).ok_or_else(workload::denied)?;
                if view.custody_event_contributor(&target.table, event) != Some((review.group, review.selected)) {
                    return Err(workload::denied());
                }
                let snapshot = rtdb_query::branch_family_review_snapshot(&view, &self.memory, &target.table, event)?;
                self.database.lock().await.check_generation(generation).await?;
                Ok(QueryView {
                    snapshot, cut: view.cut(), claim: None, scoped_source_basis: None,
                    custody_groups: BTreeSet::from([review.group]), key_generation: Some(generation),
                    custody_contributors: Some(BTreeSet::from([(review.group, review.selected)])),
                })
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn transact_scoped(&self, request: Transact, scope: ServingScope,
                             cancellation: &CancellationToken) -> Result<Receipt> {
        self.transact_routed(request, Some(scope), cancellation).await
    }

    async fn managed_appointment_ready(&self, appointment: &workload::ManagedAppointment) -> Result<bool> {
        self.require_unmanaged_effects()?;
        self.check_appointment_origin(appointment)?;
        self.database.lock().await.managed_appointment_ready(appointment).await
    }

    async fn managed_appointment_status(
        &self, appointment: &workload::ManagedAppointment, key: &[u8], cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        self.serve(async {
            self.check_appointment_origin(appointment)?;
            cancellation.run_until_cancelled(async {
                self.database.lock().await.managed_appointment_status(appointment, key).await
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn tx_status_scoped(&self, key: &[u8], scope: &ServingScope,
                              cancellation: &CancellationToken) -> Result<Receipt> {
        self.serve(async {
            cancellation.run_until_cancelled(async {
                self.database.lock().await.tx_status_scoped(key, scope.transaction_scope()).await
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn changes_scoped(&self, request: Changes, scope: &ServingScope,
                            cancellation: &CancellationToken) -> Result<ChangePage> {
        self.serve(async {
            cancellation.run_until_cancelled(async {
                let start = Instant::now();
                let mut applied = self.database.lock().await.watch_applied();
                loop {
                    applied.mark_seen();
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
                    applied.changed(Instant::now() + remaining).await?;
                }
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn query(
        &self,
        source: QuerySource,
        track: bool,
        now: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<QueryView> {
        self.serve(async {
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
                        scoped_source_basis: None,
                        key_generation: Some(generation),
                    })
                })
                .await
                .ok_or_else(interrupted)?
        }).await
    }

    async fn read_store(
        &self, read: rtdb_index::branches::store::StoreRead, at: At, now: Timestamp, cancellation: &CancellationToken,
    ) -> Result<rtdb_index::branches::store::StorePage> {
        self.serve(async {
            cancellation.run_until_cancelled(async {
                let (index, generation) = self.database.lock().await.fresh_view().await?;
                if index.has_custody_groups() || index.has_visibility_history() {
                    return Err(Anomaly::unsupported("rtdb.store/unsupported", "The ordered-bytes store serves only an unlabelled database"));
                }
                let at = if at == At::Linearizable { At::Latest } else { at };
                let page = index.view(rtdb_types::rtdb_v1::MAIN_BRANCH, at, Layer::Merged, now)?.store_page(&read)?;
                self.database.lock().await.check_generation(generation).await?;
                Ok(page)
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn check_local_store_basis(&self, basis: Basis, cancellation: &CancellationToken, turn: Option<&StoreReadTurn>) -> Result<()> {
        self.serve(async {
            cancellation.run_until_cancelled(async {
                self.database.lock().await.check_local_store_basis(basis, turn).await
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn enter_local_store_read(&self, at: At, cancellation: &CancellationToken) -> Result<Option<StoreReadTurn>> {
        self.serve(async {
            cancellation.run_until_cancelled(async {
                self.database.lock().await.enter_local_store_read(at).await
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn store_sizes(&self, epoch: Option<u64>, now: Timestamp, cancellation: &CancellationToken) -> Result<StorageSizes> {
        self.serve(async {
            cancellation.run_until_cancelled(async {
                let (index, generation) = self.database.lock().await.fresh_view().await?;
                let sizes = crate::serving::store_size_snapshot(index, epoch, now, cancellation).await?;
                self.database.lock().await.check_generation(generation).await?;
                Ok(sizes)
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn supports_store_purge_floor(&self) -> bool {
        self.classic_workload_fence.is_none()
            && self.serve(async { Ok(self.is_writer().await) }).await.unwrap_or(false)
    }

    async fn store_version_hold(
        &self, reader: [u8; 32], request: StoreVersionHoldRequest, cancellation: &CancellationToken,
    ) -> Result<StoreVersionHoldReply> {
        if self.classic_workload_fence.is_some() {
            return Err(workload::denied());
        }
        cancellation.run_until_cancelled(self.serve(self.writes.store_version_hold(reader, request)))
            .await.ok_or_else(interrupted)?
    }

    async fn publish_store_purge_floor(
        &self, since: Basis, keep_from: u64, retention_micros: i64, reads: Vec<KeyRange>, writer: StoreWriterClaim,
        memory: Arc<MemoryReservation>, cancellation: &CancellationToken,
    ) -> Result<()> {
        if self.classic_workload_fence.is_some() {
            return Err(workload::denied());
        }
        cancellation.run_until_cancelled(self.serve(
            self.writes.publish_store_purge_floor(since, keep_from, retention_micros, reads, Some(writer), memory),
        )).await.ok_or_else(interrupted)?
    }

    async fn query_workload_root(
        &self, source: QuerySource, cut: WorkloadReadCut, now: Timestamp, cancellation: &CancellationToken,
    ) -> Result<QueryView> {
        self.serve(async {
            cancellation.run_until_cancelled(async {
                let (index, generation) = self.database.lock().await.fresh_view().await?;
                index.require_local_custody()?;
                if source.branch != rtdb_types::rtdb_v1::MAIN_BRANCH || source.layer != Layer::Merged
                    || index.has_visibility_history() || cut.basis.incarnation() != self.incarnation
                {
                    return Err(workload::denied());
                }
                let basis = match source.at {
                    At::Exactly(basis) if basis.incarnation() == self.incarnation && basis <= cut.basis => basis,
                    At::AtLeast(basis) if basis.incarnation() == self.incarnation && basis <= cut.basis => cut.basis,
                    At::Linearizable => cut.basis,
                    _ => return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb/basis-ahead",
                        "The requested basis is outside the admitted writer cut").with_kind(AnomalyKind::BasisAhead)),
                };
                let view = index.view(&source.branch, At::Exactly(basis), source.layer, now)?;
                let snapshot = if let Some(groups) = &cut.custody_groups {
                    if !matches!(source.at, At::Exactly(_)) { return Err(workload::denied()); }
                    for group in groups {
                        if !index.custody_group_live(*group)? { return Err(workload::denied()); }
                    }
                    rtdb_query::branch_workload_custody_snapshot(&view, &self.memory, groups)?
                } else {
                    let tables = view.tables().filter(|(table, _)| !index.has_custody_relation(table))
                        .map(|(table, _)| table.clone()).collect();
                    rtdb_query::branch_workload_clear_snapshot(&view, &self.memory, &tables)?
                };
                self.database.lock().await.check_generation(generation).await?;
                Ok(QueryView {
                    snapshot,
                    cut: view.cut(),
                    claim: None,
                    custody_groups: BTreeSet::new(),
                    custody_contributors: Some(BTreeSet::new()),
                    scoped_source_basis: None,
                    key_generation: Some(generation),
                })
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn watch_applied(&self) -> Option<rtdb_db::AppliedWatch> {
        Some(self.database.lock().await.watch_applied())
    }

    async fn changes_workload_root(
        &self, mut request: Changes, cut: Basis, cancellation: &CancellationToken,
    ) -> Result<ChangePage> {
        self.serve(async {
            cancellation.run_until_cancelled(async {
                let mut database = self.database.lock().await;
                let history = database.fresh_history().await?;
                history.index.require_local_custody()?;
                if request.branch != rtdb_types::rtdb_v1::MAIN_BRANCH || history.index.has_visibility_history()
                    || cut.incarnation() != self.incarnation
                    || history.index.has_custody_groups() && (request.tables.is_empty()
                        || request.tables.iter().any(|table| history.index.has_custody_relation(table)))
                {
                    return Err(workload::denied());
                }
                if request.after.is_some_and(|after| after.position() > cut.position()) {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb/change-basis-ahead",
                        "The changes cursor is beyond the admitted writer cut"));
                }
                let until = request.until;
                request.until = Some(until.map_or(cut, |until| until.min(cut)));
                let mut page = changes_page(&history, &request, self.incarnation)?;
                if until.is_some_and(|until| until > cut) { page.complete = false; }
                page.key_generation = Some(history.generation);
                Ok(page)
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn transact(
        &self,
        request: Transact,
        cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        self.transact_routed(request, None, cancellation).await
    }

    async fn supports_timed_store_append(&self) -> bool {
        matches!(self.access, ServingAccess::Activated(_))
            && self.classic_workload_fence.is_none()
            && self.database.lock().await.supports_timed_store_append()
    }

    async fn append_timed_store(
        &self, request: TimedStoreAppend, cancellation: &CancellationToken,
    ) -> Result<TimedStoreReceipt> {
        if !matches!(self.access, ServingAccess::Activated(_)) || self.classic_workload_fence.is_some() {
            return Err(rtdb_tx::transact::funding_kernel_unavailable());
        }
        self.serve(async {
            cancellation.run_until_cancelled(self.writes.append_timed_store(request))
                .await.ok_or_else(interrupted)?
        }).await
    }

    async fn check_custody_delivery(&self, groups: &BTreeSet<CustodyGroupId>, cancellation: &CancellationToken) -> Result<()> {
        self.check_read_delivery(None, groups, cancellation).await
    }

    async fn check_read_delivery(&self, generation: Option<u64>, groups: &BTreeSet<CustodyGroupId>, cancellation: &CancellationToken) -> Result<()> {
        if !groups.is_empty() {
            self.require_unmanaged_effects()?;
        }
        self.serve(async {
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
        }).await
    }

    async fn current_custody(&self, cancellation: &CancellationToken) -> Result<BranchIndex> {
        self.require_unmanaged_effects()?;
        self.serve(async {
            cancellation.run_until_cancelled(async {
                let (index, generation) = self.database.lock().await.fresh_view().await?;
                index.require_local_custody()?;
                self.database.lock().await.check_generation(generation).await?;
                Ok(index)
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn submit_custody_release(&self, command: TrustedCustodyCommand, cancellation: &CancellationToken) -> Result<Receipt> {
        self.require_unmanaged_effects()?;
        self.serve(async {
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
        }).await
    }

    async fn check_custody_release_delivery(&self, groups: &BTreeSet<CustodyGroupId>, writer: &CustodyWriterFence,
                                            generation: Option<u64>, cancellation: &CancellationToken) -> Result<()> {
        self.require_unmanaged_effects()?;
        self.serve(async {
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
        }).await
    }

    async fn reconcile_custody(&self, cancellation: &CancellationToken) -> Result<()> {
        self.require_unmanaged_effects()?;
        cancellation.run_until_cancelled(async {
            let (index, _) = self.database.lock().await.fresh_view().await?;
            index.require_local_custody()?;
            crate::serving::custody::reconcile_revocations(&index, &self.tokens.custody_vault(), crate::Clock::now(&crate::SystemClock)).await
        }).await.ok_or_else(interrupted)?
    }

    async fn tx_status(&self, key: &[u8], cancellation: &CancellationToken) -> Result<Receipt> {
        self.serve(async {
            cancellation
                .run_until_cancelled(async { self.database.lock().await.tx_status(key).await })
                .await
                .ok_or_else(interrupted)?
        }).await
    }

    async fn original_custody_receipt(&self, key: &[u8], basis: Basis, cancellation: &CancellationToken) -> Result<Receipt> {
        self.serve(async {
            cancellation.run_until_cancelled(async {
                self.database.lock().await.original_custody_receipt(key, basis).await
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn original_writer_receipt(&self, key: &[u8], basis: Basis, cancellation: &CancellationToken) -> Result<Receipt> {
        self.serve(async {
            cancellation.run_until_cancelled(async {
                self.database.lock().await.original_writer_receipt(key, basis).await
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn original_line_creation_receipt(&self, key: &[u8], basis: Basis, cancellation: &CancellationToken) -> Result<Receipt> {
        self.serve(async {
            if let Some(imported) = &self.classic_workload_fence {
                let after = Basis::from_bytes(&imported.applied.basis)?;
                if basis.incarnation() != after.incarnation() || basis <= after { return Err(workload::denied()); }
            }
            cancellation.run_until_cancelled(async {
                self.database.lock().await.original_line_creation_receipt(key, basis).await
            }).await.ok_or_else(interrupted)?
        }).await
    }

    async fn changes(
        &self,
        request: Changes,
        cancellation: &CancellationToken,
    ) -> Result<ChangePage> {
        self.serve(async {
            if request.limit == 0 {
                return Err(Anomaly::incorrect(
                    "rtdb/change-limit",
                    "Changes requires a positive group limit",
                ));
            }
            cancellation
                .run_until_cancelled(async {
                    let start = Instant::now();
                    let mut applied = self.database.lock().await.watch_applied();
                    loop {
                        applied.mark_seen();
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
                        applied.changed(Instant::now() + remaining).await?;
                    }
                })
                .await
                .ok_or_else(interrupted)?
        }).await
    }
}

fn require_ordinary_request(request: &Transact) -> Result<()> {
    if request.contains_response_release() {
        return Err(crate::serving::custody_release::release_unavailable());
    }
    if request.contains_native_custody_transfer() {
        return Err(Anomaly::unsupported("rtdb/native-checkpoint-command", "Native checkpoint custody commands require the installed custody provider"));
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
        scoped_custody: None,
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
    use std::sync::atomic::{AtomicI64, Ordering};

    use journal_contract::{Identity, reference::MemoryJournal};
    use rtdb_tx::transact::TRANSACT_REQUIRES;
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
    async fn the_multi_writer_fences_purge_and_older_follower_reads() {
        use rtdb_index::branches::store::{Projection, StoreWriterFence};
        use rtdb_tx::transact::{Guard, StoreAppend, StoreWrite};

        let journal = MemoryJournal::default();
        let id = Uuid::from_u128(1);
        let store = Arc::new(MemoryStore::default());
        let kms = Arc::new(LocalKms::new(|| 10));
        let memory = MemoryBudget::new(64 << 20);
        let open = |node, identity| MultiDatabase::new(
            MultiOptions { database: id, node, lease_micros: 100, drift_micros: 1 },
            Arc::new(journal.handle(identity)), store.clone(), kms.clone(), Arc::new(|| Timestamp(10)),
        ).unwrap();
        let writer_node = IpAddr::from([127, 0, 0, 1]);
        let mut database = open(writer_node, Identity::from_static("urn:rtdb:test:1"));
        database.create().await.unwrap();
        database.tick(&BTreeSet::from([writer_node])).await.unwrap();
        let writer = MultiServing::new(database, Arc::clone(&memory));
        let follower = Arc::new(MultiServing::new(open(
            IpAddr::from([127, 0, 0, 2]), Identity::from_static("urn:rtdb:test:2"),
        ), Arc::clone(&memory)));
        let cancellation = CancellationToken::new();
        let store_write = |request_key, key: &[u8], value: &[u8]| Transact {
            custody_request_digest: None,
            request_memory: None,
            key: vec![request_key; 16],
            require: Vec::new(),
            window: Window::default(),
            guards: Vec::new(),
            requires: Vec::new(),
            ops: vec![Op::Store(StoreAppend { alternative: None,
                writes: vec![StoreWrite { key: key.to_vec(), value: Some(value.to_vec()), key_stamp: None, value_stamp: None }],
                bounds: Vec::new(),
                derives: Vec::new(),
                window: Window::default(),
            })],
            otherwise: Vec::new(),
        };
        let first = writer.transact(store_write(1, b"a", b"original"), &cancellation).await.unwrap().basis;
        let assigned = follower.assign_follower().await.unwrap();
        let floor = writer.transact(store_write(2, b"a", b"replacement"), &cancellation).await.unwrap().basis;
        let incarnation = floor.incarnation();
        let fence = StoreWriterFence { epoch: 1, writer: Some([2; 16]) };
        let first_attempt = [3; 16];
        let first_claim = writer.transact(Transact::claim_store_writer(incarnation, fence, first_attempt), &cancellation)
            .await.unwrap().basis;
        let first_writer = StoreWriterClaim { fence, generation: first_claim.position(), claim: first_attempt };
        let guards = || vec![KeyRange { start: b"b".to_vec(), end: Some(b"c".to_vec()) }];
        let reserve = || memory.try_reserve(4096).unwrap();
        let read = || StoreRead {
            range: KeyRange { start: b"a".to_vec(), end: Some(b"b".to_vec()) },
            after: None, limit: 1, projection: Projection::Values, filter: None, reverse: false,
        };
        let original = writer.read_store(read(), At::Exactly(first), Timestamp(100), &cancellation).await.unwrap();
        assert_eq!(original.entries[0].value.as_deref(), Some(b"original".as_slice()));
        writer.check_local_store_basis(original.cut.basis, &cancellation, None).await.unwrap();
        let (original, delivery) = assigned.read_store(read(), At::Exactly(first), Timestamp(100), &cancellation).await.unwrap();
        assert_eq!(original.entries[0].value.as_deref(), Some(b"original".as_slice()));
        assigned.check_store_delivery(&delivery, &cancellation).await.unwrap();
        let (changes, watch_delivery) = assigned.store_changes(Changes {
            branch: MAIN_BRANCH.into(),
            tables: [rtdb_index::branches::store::store_table()].into(),
            after: Some(first), until: None, order: ChangeOrder::Ascending,
            images: ChangeImages::Keys, limit: 16, wait: Duration::ZERO,
        }, &cancellation).await.unwrap();
        assert!(changes.through >= floor);

        assert!(!follower.supports_store_purge_floor().await);
        let refusal = follower.publish_store_purge_floor(floor, floor.position(), 0, guards(), first_writer, reserve(), &cancellation)
            .await.unwrap_err();
        assert_eq!(refusal.code, "rtdb/no-writer");
        assert!(writer.supports_store_purge_floor().await);
        let active = writer.enter_local_store_read(At::Exactly(first), &cancellation).await.unwrap().unwrap();
        assert_eq!(active.basis(), first);
        writer.check_local_store_basis(first, &cancellation, Some(&active)).await.unwrap();
        assert_eq!(writer.check_local_store_basis(floor, &cancellation, Some(&active)).await.unwrap_err().code,
            "rtdb.store/stale-read-turn");
        let retained = writer.publish_store_purge_floor(floor, floor.position(), 0, guards(), first_writer, reserve(), &cancellation)
            .await.unwrap_err();
        assert_eq!(retained.code, "rtdb.store/purge-retention");
        drop(active);
        writer.publish_store_purge_floor(floor, floor.position(), 0, guards(), first_writer, reserve(), &cancellation)
            .await.unwrap();

        assert_eq!(writer.check_local_store_basis(first, &cancellation, None).await.unwrap_err().code, "rtdb/cursor-expired");
        writer.check_local_store_basis(floor, &cancellation, None).await.unwrap();

        let expired = assigned.check_store_delivery(&delivery, &cancellation).await.unwrap_err();
        assert_eq!(expired.code, "rtdb/cursor-expired");
        let expired = assigned.check_store_delivery(&watch_delivery, &cancellation).await.unwrap_err();
        assert_eq!(expired.code, "rtdb/cursor-expired");

        let second_attempt = [4; 16];
        let second_claim = writer.transact(Transact::claim_store_writer(incarnation, fence, second_attempt), &cancellation)
            .await.unwrap().basis;
        let current_writer = StoreWriterClaim { fence, generation: second_claim.position(), claim: second_attempt };
        for stale in [first_writer, StoreWriterClaim { claim: first_attempt, ..current_writer }] {
            let refusal = writer.publish_store_purge_floor(floor, floor.position(), 0, guards(), stale, reserve(), &cancellation)
                .await.unwrap_err();
            assert_eq!(refusal.code, "rtdb.store/stale-writer");
        }
        writer.publish_store_purge_floor(floor, floor.position(), 0, guards(), current_writer, reserve(), &cancellation)
            .await.unwrap();

        let expired = writer.read_store(read(), At::Exactly(first), Timestamp(100), &cancellation).await.unwrap_err();
        assert_eq!(expired.code, "rtdb/cursor-expired");
        let expired = assigned.read_store(read(), At::Exactly(first), Timestamp(100), &cancellation).await.unwrap_err();
        assert_eq!(expired.code, "rtdb/cursor-expired");
        let retained = writer.read_store(read(), At::Exactly(floor), Timestamp(100), &cancellation).await.unwrap();
        assert_eq!(retained.entries[0].value.as_deref(), Some(b"replacement".as_slice()));

        let mut write = store_write(3, b"b", b"new");
        write.guards.push(Guard::StoreWriterGeneration {
            incarnation, fence, generation: current_writer.generation, claim: current_writer.claim,
        });
        writer.transact(write, &cancellation).await.unwrap();
        let refusal = writer.publish_store_purge_floor(floor, floor.position(), 0, guards(), current_writer, reserve(), &cancellation)
            .await.unwrap_err();
        assert_eq!(refusal.code, "rtdb.store/purge-guard");
    }

    #[tokio::test]
    async fn assigned_query_uses_the_local_cut_and_reports_a_separate_writer_head() {
        async fn titles(context: &Arc<QueryContext>, sql: &str) -> Vec<Value> {
            let prepared = rtdb_sql::prepare_in(sql, &context.catalog, &Default::default(), &context.memory).unwrap();
            let mut rows = PreparedQuery::from_prepared(prepared, &context.memory).unwrap()
                .execute(Arc::clone(context), Vec::new()).await.unwrap();
            let mut values = Vec::new();
            while let Some(result) = rows.next_batch().await.unwrap() {
                let batch = result.batch();
                for index in 0..batch.num_rows() {
                    values.push(rtdb_columnar::read_value(batch.schema().field(0), batch.column(0).as_ref(), index).unwrap());
                }
            }
            values
        }

        let journal = MemoryJournal::default();
        let id = Uuid::from_u128(1);
        let store = Arc::new(MemoryStore::default());
        let kms = Arc::new(LocalKms::new(|| 10));
        let clock = Arc::new(AtomicI64::new(10));
        let node_clock = {
            let clock = Arc::clone(&clock);
            Arc::new(move || Timestamp(clock.load(Ordering::Relaxed)))
        };
        let memory = MemoryBudget::new(64 << 20);
        let open = |node, identity| MultiDatabase::new(
            MultiOptions { database: id, node, lease_micros: 100, drift_micros: 1 },
            Arc::new(journal.handle(identity)), store.clone(), kms.clone(), node_clock.clone(),
        ).unwrap();
        let writer_node = IpAddr::from([127, 0, 0, 1]);
        let mut writer = open(writer_node, Identity::from_static("urn:rtdb:test:1"));
        writer.create().await.unwrap();
        writer.tick(&BTreeSet::from([writer_node])).await.unwrap();
        let mut initial = write(1, "one", None);
        let Op::Put { rows, .. } = &mut initial.ops[0] else { panic!() };
        rows.push(Row {
            id: EntityId::Integer(2),
            cells: StructFields::from([("title".into(), Cell::Value(Value::Utf8("other".into())))]),
            ..rows[0].clone()
        });
        let first = writer.transact(&initial).await.unwrap();
        let original_index = writer.fresh_index().await.unwrap();
        let original = original_index
            .view(MAIN_BRANCH, At::Exactly(first.basis), Layer::Merged, first.system_time).unwrap();
        let original = rtdb_query::branch_snapshot(&original, &memory).unwrap();
        let follower_node = IpAddr::from([127, 0, 0, 2]);
        let follower = Arc::new(MultiServing::new(open(follower_node, Identity::from_static("urn:rtdb:test:2")), Arc::clone(&memory)));
        let reader = follower.assign_follower().await.unwrap();
        let second = writer.transact(&write(2, "two", Some(Timestamp(20)))).await.unwrap();
        let cancellation = CancellationToken::new();
        let select = |at, progress| reader.query(at, Timestamp(10), progress, &cancellation);
        let read = || StoreRead {
            range: rtdb_index::branches::store::KeyRange { start: Vec::new(), end: None },
            after: None, limit: 1, projection: rtdb_index::branches::store::Projection::Keys,
            filter: None, reverse: false,
        };
        assert_eq!(reader.read_store(read(), At::Latest, Timestamp(10), &cancellation).await.unwrap().cut.basis, first.basis);
        assert_eq!(reader.read_store(read(), At::Exactly(second.basis), Timestamp(10), &cancellation).await
            .err().unwrap().code, "rtdb/basis-ahead");

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

        clock.store(200, Ordering::Relaxed);
        assert_eq!(
            select(At::Latest, true).await.err().unwrap().code,
            "rtdb/follower-progress-unavailable",
        );
        let view = reader.retained_view(first.basis, Timestamp(200), &cancellation).await.unwrap();
        assert_eq!(view.cut.basis, first.basis);
        reader.check_retained_delivery(view.key_generation.unwrap(), &cancellation).await.unwrap();
        let storage = Arc::new(BufferPool::new(store.clone(), 1 << 20));
        let context = |snapshot| {
            let catalog = CatalogSnapshot::new("xtdb", std::collections::BTreeMap::from([("xtdb".into(), DatabaseSnapshot {
                partitions: vec![snapshot], storage: Arc::clone(&storage),
            })]), false).unwrap();
            QueryContext::new(catalog, Arc::clone(&memory), first.system_time, "UTC", QueryOptions::default(), &cancellation)
                .unwrap().with_wire_cut(first.system_time, first.system_time.into()).unwrap()
        };
        let original = context(original);
        let snapshot = view.snapshot;
        let retained = context(snapshot);
        retained.retain_wire_scope((Instant::now() + Duration::from_secs(2)).into_std()).await.unwrap();
        for (sql, expected) in [
            ("SELECT title FROM notes WHERE _id = 1", vec![Value::Utf8("one".into())]),
            ("SELECT title FROM notes WHERE _id = 3", vec![]),
            ("SELECT title FROM notes ORDER BY _id LIMIT 1", vec![Value::Utf8("one".into())]),
            ("SELECT title FROM notes ORDER BY _id LIMIT 1 OFFSET 1", vec![Value::Utf8("other".into())]),
        ] {
            let page = retained.for_wire_scope_page(&cancellation);
            assert_eq!(titles(&original, sql).await, expected);
            assert_eq!(titles(&page, sql).await, expected);
        }

        drop(reader);
        let live = BTreeSet::from([follower_node]);
        assert_eq!(writer.tick(&live).await.unwrap(), Role::Follower);
        follower.replay_follower().await.unwrap();
        assert_eq!(follower.database.lock().await.role(), Role::Follower);
        assert!(matches!(follower.tick(&live).await.unwrap(), Role::Writer(_)));
    }

    #[cfg(feature = "search")]
    #[tokio::test]
    async fn multi_ivf_mounts_the_writer_catalogue_and_rebuilds_at_the_restored_basis() {
        use crate::serving::ranked_ivf::{Request, Target};
        use rtdb_index::branches::{RankedIndexDefinition, RankedIndexKind};
        use rtdb_tx::transact::Declaration;

        let journal = MemoryJournal::default();
        let node = IpAddr::from([127, 0, 0, 1]);
        let store = Arc::new(MemoryStore::default());
        let kms = Arc::new(LocalKms::new(|| 10));
        let memory = MemoryBudget::new(64 << 20);
        let cancel = CancellationToken::new();
        let open = |id| MultiDatabase::new(
            MultiOptions { database: Uuid::from_u128(id), node, lease_micros: 100, drift_micros: 1 },
            Arc::new(journal.handle(Identity::from_static("urn:rtdb:test:1"))),
            store.clone(), kms.clone(), Arc::new(|| Timestamp(10)),
        ).unwrap();
        let proposal = open(1);
        proposal.create().await.unwrap();
        let writer = MultiServing::new(proposal, Arc::clone(&memory));
        writer.tick(&BTreeSet::from([node])).await.unwrap();

        let table = TableRef::new("public", "notes");
        let mut declaration = write(1, "", None);
        declaration.ops = vec![Op::Declare {
            table: table.clone(),
            change: Declaration::RankedIndex {
                name: "notes_ivf".into(),
                definition: RankedIndexDefinition {
                    columns: vec!["embedding".into()],
                    kind: RankedIndexKind::Ivf { dimensions: 2, recall_bits: 0.95f64.to_bits() },
                },
            },
        }];
        let declared = writer.transact(declaration, &cancel).await.unwrap();
        assert_eq!(declared.outcome, Outcome::Then);
        let target = Target { table, index: "notes_ivf".into(), generation: declared.basis.position() };
        let mut rows = write(2, "one", None);
        let Op::Put { rows: stored, .. } = &mut rows.ops[0] else { panic!() };
        stored[0].valid = Some(Interval::ALL.into());
        stored[0].cells.insert("embedding".into(), Cell::Value(Value::list(vec![Value::F32(1.0), Value::F32(0.0)])));
        let written = writer.transact(rows, &cancel).await.unwrap();
        assert_eq!(written.outcome, Outcome::Then);
        let source = |basis| QuerySource { branch: MAIN_BRANCH.into(), at: At::Exactly(basis), layer: Layer::Merged };
        assert_eq!(
            writer.query_ranked_ivf(source(written.basis), &target, Timestamp(10), &cancel).await.err().unwrap().code,
            "rtdb.vector/native-uncovered",
        );
        writer.maintain_ranked_catalog(&cancel).await.unwrap();

        let follower = Arc::new(MultiServing::new(open(1), Arc::clone(&memory)));
        let assigned = follower.assign_follower().await.unwrap();
        let (view, physical, progress) = assigned.query_ranked_ivf(
            source(written.basis), &target, Timestamp(10), true, &cancel,
        ).await.unwrap();
        assert_eq!(progress.unwrap().applied_position, written.basis.position());
        let catalog = CatalogSnapshot::new("xtdb", std::collections::BTreeMap::from([("xtdb".into(), DatabaseSnapshot {
            partitions: vec![view.snapshot], storage: physical.mounted_storage().unwrap().clone(),
        })]), false).unwrap();
        let context = QueryContext::new(catalog, Arc::clone(&memory), view.cut.system_time, "UTC", QueryOptions::default(), &cancel)
            .unwrap().with_wire_cut(view.cut.system_time, view.cut.system_time.into()).unwrap();
        let (mut cursor, count, approximate) = physical.execute(context, Request {
            target: target.clone(), valid: Timestamp(10), k: 1, probe: vec![1.0, 0.0], filters: Vec::new(), staged: Vec::new(),
        }).await.unwrap();
        assert_eq!(count, 1);
        assert!(!approximate);
        assert_eq!(cursor.next_batch().await.unwrap().unwrap().batch().num_rows(), 1);
        let (ready, _) = assigned.query_ranked_artefacts("xtdb", source(written.basis), Timestamp(10), false, &cancel).await.unwrap();
        let catalog = CatalogSnapshot::new("xtdb", std::collections::BTreeMap::from([("xtdb".into(), DatabaseSnapshot {
            partitions: vec![ready.snapshot], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
        })]), false).unwrap();
        let ready_sql = rtdb_sql::prepare_in("SELECT artefacts_ready FROM xt.ranked_indexes", &catalog, &Default::default(), &memory).unwrap();
        let context = QueryContext::new(catalog, Arc::clone(&memory), ready.cut.system_time, "UTC", QueryOptions::default(), &cancel).unwrap();
        let mut readiness = PreparedQuery::from_prepared(ready_sql, &memory).unwrap().execute(context, Vec::new()).await.unwrap();
        let batch = readiness.next_batch().await.unwrap().unwrap();
        assert_eq!(rtdb_columnar::read_value(batch.batch().schema().field(0), batch.batch().column(0).as_ref(), 0).unwrap(), Value::Bool(true));

        let ticket = writer.export_checkpoint(Uuid::from_u128(9), written.basis, None, &cancel).await.unwrap();
        let export = writer.open_checkpoint_export(ticket, &cancel).await.unwrap();
        let destination = open(2);
        destination.create().await.unwrap();
        let destination = MultiServing::new(destination, Arc::clone(&memory));
        destination.tick(&BTreeSet::from([node])).await.unwrap();
        let restored = destination.restore_checkpoint(export, &cancel).await.unwrap();
        assert_eq!(restored.basis.incarnation(), *Uuid::from_u128(2).as_bytes());
        assert!(destination.query_ranked_ivf(source(restored.basis), &target, Timestamp(10), &cancel).await.is_err());
        destination.maintain_ranked_catalog(&cancel).await.unwrap();
        let (view, _) = destination.query_ranked_ivf(source(restored.basis), &target, Timestamp(10), &cancel).await.unwrap();
        assert_eq!(view.cut, restored);
        assert!(destination.query_ranked_ivf(source(written.basis), &target, Timestamp(10), &cancel).await.is_err());
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
    async fn a_waiting_change_page_answers_when_the_next_write_applies() {
        let journal = MemoryJournal::default();
        let node = IpAddr::from([127, 0, 0, 1]);
        let database = MultiDatabase::new(
            MultiOptions { database: Uuid::from_u128(1), node, lease_micros: 100, drift_micros: 1 },
            Arc::new(journal.handle(Identity::from_static("urn:rtdb:test:1"))),
            Arc::new(MemoryStore::default()),
            Arc::new(LocalKms::new(|| 10)),
            Arc::new(|| Timestamp(10)),
        )
        .unwrap();
        database.create().await.unwrap();
        let driver = Arc::new(MultiServing::new(database, MemoryBudget::new(64 * 1024 * 1024)));
        driver.tick(&BTreeSet::from([node])).await.unwrap();
        let cancel = CancellationToken::new();
        let first = driver.transact(write(1, "one", None), &cancel).await.unwrap();
        let wait = Duration::from_secs(30);
        let request = Changes {
            branch: MAIN_BRANCH.into(), tables: BTreeSet::new(), after: Some(first.basis), until: None,
            order: ChangeOrder::Ascending, images: ChangeImages::Both, limit: 1, wait,
        };
        let waiting = tokio::spawn({
            let (driver, cancel) = (Arc::clone(&driver), cancel.clone());
            async move { driver.changes(request, &cancel).await }
        });
        tokio::task::yield_now().await;
        let second = driver.transact(write(2, "two", None), &cancel).await.unwrap();
        let page = tokio::time::timeout(Duration::from_secs(10), waiting).await
            .expect("the page answers when the write applies, not when the wait runs out").unwrap().unwrap();
        assert_eq!(page.groups.len(), 1);
        assert_eq!(page.through, second.basis);
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
            receipt_namespace: [7; 32], custody_evidence: false, hold: None,
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
