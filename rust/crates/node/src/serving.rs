use std::{collections::BTreeSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use rtdb_crypt::tokens::TokenAuthority;
use rtdb_index::{
    Snapshot,
    branches::{BranchCut, BranchIndex, BranchView, BranchWrite, RowImage, store::{KeyRange, StorePage, StoreRead, StoreWriterClaim, sizes::StorageSizes}},
};
use rtdb_tx::transact::{CustodyGroupId, CustodyWriterFence, Receipt, Transact, TrustedCustodyCommand, workload};
#[cfg(feature = "multi")]
pub use rtdb_db::{CatalogRoot, TimedStoreAppend, TimedStoreAuthorizer, TimedStoreCommitment, TimedStoreReceipt};
pub use rtdb_replication::{NativeCheckpointAuthority, NativeServingAuthority};
use rtdb_types::{
    EntityId, Interval, MemoryReservation, Result, TableRef, Timestamp,
    rtdb_v1::{At, Basis, Branch, Claim, Layer},
};
use tokio_util::sync::CancellationToken;

pub(crate) mod custody;
pub mod custody_release;
pub(crate) mod default;
pub mod ranked_ivf;
mod visibility;
pub use visibility::{ServingScope, ROW_VISIBILITY};
pub use rtdb_index::branches::ScopedHold;

#[derive(Clone)]
pub struct QuerySource {
    pub branch: Branch,
    pub at: At,
    pub layer: Layer,
}

pub struct FamilyReviewSelection {
    pub created: Basis,
    pub group: CustodyGroupId,
    pub selected: [u8; 32],
    pub valid_at: Timestamp,
}

pub struct WorkloadReadCut {
    pub basis: Basis,
    pub custody_groups: Option<BTreeSet<CustodyGroupId>>,
}

pub struct QueryView {
    pub snapshot: Snapshot,
    pub cut: BranchCut,
    pub claim: Option<Claim>,
    pub custody_groups: BTreeSet<CustodyGroupId>,
    pub custody_contributors: Option<custody_release::CustodyContributors>,
    pub scoped_source_basis: Option<Basis>,
    pub key_generation: Option<u64>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ChangeOrder {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ChangeImages {
    None,
    Keys,
    Before,
    After,
    Both,
}

#[derive(Clone)]
pub struct Changes {
    pub branch: Branch,
    pub tables: BTreeSet<TableRef>,
    pub after: Option<Basis>,
    pub until: Option<Basis>,
    pub order: ChangeOrder,
    pub images: ChangeImages,
    pub limit: usize,
    pub wait: Duration,
}

pub struct ChangePage {
    pub read_custody: Option<Arc<rtdb_replication::LocalErasureRead>>,
    pub scoped_custody: Option<(Basis, custody_release::CustodyContributors)>,
    pub groups: Vec<ChangeGroup>,
    pub through: Basis,
    pub complete: bool,
    pub custody_groups: BTreeSet<CustodyGroupId>,
    pub key_generation: Option<u64>,
}

pub struct ChangeGroup {
    pub cut: BranchCut,
    pub rows: Vec<ChangeRow>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    Opened,
    Closed,
    Erased,
}

pub struct ChangeRow {
    pub table: TableRef,
    pub id: EntityId,
    pub valid: Option<Interval>,
    pub kind: ChangeKind,
    pub erased_written_basis: Option<Basis>,
    pub before: Option<Arc<RowImage>>,
    pub after: Option<Arc<RowImage>>,
}

pub(crate) fn append_change_rows(
    output: &mut Vec<ChangeRow>,
    write: &BranchWrite,
    before: Option<&BranchView<'_>>,
    after: Option<&BranchView<'_>>,
    images: ChangeImages,
) -> Result<()> {
    let row = |valid, kind, before, after| ChangeRow {
        table: write.table.clone(),
        id: write.id.clone(),
        valid,
        kind,
        erased_written_basis: write.erased_written_basis,
        before,
        after,
    };
    let Some(valid) = write.valid.filter(|_| write.erased_written_basis.is_none()) else {
        output.push(row(write.valid, ChangeKind::Erased, None, None));
        return Ok(());
    };
    let mut boundaries = BTreeSet::from([valid.lower(), valid.upper()]);
    for view in before.into_iter().chain(after) {
        for event in view.history(&write.table, &write.id) {
            let expiry = event.image.as_ref().and_then(|image| image.expires_at);
            boundaries.extend([Some(event.valid.lower()), Some(event.valid.upper()), expiry]
                .into_iter().flatten().filter(|boundary| valid.contains(*boundary)));
        }
    }
    fn image_at<'a>(view: Option<&BranchView<'a>>, write: &BranchWrite, from: Timestamp) -> Option<&'a Arc<RowImage>> {
        view?.row(&write.table, &write.id, from)?.image.as_ref()
    }
    let starts = boundaries.iter().copied();
    for (from, to) in starts.clone().zip(starts.skip(1)) {
        let valid = Some(Interval::new(from, to)?);
        let previous = image_at(before, write, from);
        let current = image_at(after, write, from);
        if previous.is_some() || current.is_none() {
            let image = previous.filter(|_| matches!(images, ChangeImages::Before | ChangeImages::Both)).cloned();
            output.push(row(valid, ChangeKind::Closed, image, None));
        }
        if current.is_some() {
            let image = current.filter(|_| matches!(images, ChangeImages::After | ChangeImages::Both)).cloned();
            output.push(row(valid, ChangeKind::Opened, None, image));
        }
    }
    Ok(())
}

#[test]
fn change_rows_share_the_selected_image_only_inside_its_expiry_slice() {
    use rtdb_index::branches::{BranchEffect, BranchEvent, WrittenVersion};
    use rtdb_types::{StructFields, Value, rtdb_v1::MAIN_BRANCH};

    let table = TableRef::new("public", "facts");
    let id = EntityId::Integer(2);
    let valid = Interval::new(Timestamp(0), Timestamp(10)).unwrap();
    let image = Arc::new(RowImage {
        cells: StructFields::from([("_id".into(), Value::I64(2))]),
        expires_at: Some(Timestamp(4)), custody_group: None, custody_payload: None,
    });
    let mut index = BranchIndex::new([1; 16]);
    let cut = BranchCut { basis: Basis::new([1; 16], 1), system_time: Timestamp(1) };
    index.advance(cut).unwrap();
    index.apply(BranchEffect::Write {
        branch: MAIN_BRANCH.into(),
        table: table.clone(),
        event: BranchEvent { id: id.clone(), valid, cut, image: Some(Arc::clone(&image)), written: WrittenVersion::Branch },
    }).unwrap();
    let view = index.view(MAIN_BRANCH, At::Latest, Layer::Merged, cut.system_time).unwrap();
    let write = BranchWrite { branch: MAIN_BRANCH.into(), table, id, valid: Some(valid), erased_written_basis: None };
    let mut rows = Vec::new();
    append_change_rows(&mut rows, &write, None, Some(&view), ChangeImages::Both).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].valid, Some(Interval::new(Timestamp(0), Timestamp(4)).unwrap()));
    assert!(rows[0].kind == ChangeKind::Opened);
    assert!(Arc::ptr_eq(rows[0].after.as_ref().unwrap(), &image));
    assert_eq!(rows[1].valid, Some(Interval::new(Timestamp(4), Timestamp(10)).unwrap()));
    assert!(rows[1].kind == ChangeKind::Closed);
    assert!(rows[1].after.is_none());
}

pub struct FundingSubmission {
    pub command: rtdb_tx::transact::FundingCommand,
    pub owner: [u8; 32],
    pub clock: Arc<dyn rtdb_tx::DecisionClock>,
}

#[cfg(feature = "multi")]
pub use rtdb_db::AppliedWatch;
#[cfg(feature = "multi")]
pub use rtdb_db::{StoreReadTurn, StoreVersionHoldKey, StoreVersionHoldReply, StoreVersionHoldRequest};

/// Without `multi` no attachment applies a journal, so there is no position to watch.
#[cfg(not(feature = "multi"))]
pub enum AppliedWatch {}

#[cfg(not(feature = "multi"))]
impl AppliedWatch {
    pub fn mark_seen(&mut self) { match *self {} }

    pub async fn changed(&mut self, _until: tokio::time::Instant) -> Result<()> { match *self {} }
}

/// An admitted database attachment. Each mode establishes its own barriers and publishes only applied state.
#[async_trait]
pub trait ServingDriver: Send + Sync {
    fn incarnation(&self) -> [u8; 16];
    fn features(&self) -> &[&str];
    fn classic_workload_fence(&self) -> Option<&rtdb_tx::transact::workload::Fence> { None }
    fn token_authority(&self) -> Option<TokenAuthority> { None }
    fn uses_database_tokens(&self) -> bool { false }

    #[cfg(feature = "multi")]
    async fn supports_timed_store_append(&self) -> bool { false }

    #[cfg(feature = "multi")]
    async fn append_timed_store(
        &self, _request: TimedStoreAppend, _cancellation: &CancellationToken,
    ) -> Result<TimedStoreReceipt> {
        Err(rtdb_tx::transact::funding_kernel_unavailable())
    }

    async fn supports_funded_transact(&self) -> bool { false }

    async fn supports_funded_appointment(&self) -> bool { false }

    async fn managed_appointment_ready(&self, _appointment: &workload::ManagedAppointment) -> Result<bool> {
        Err(workload::denied())
    }

    async fn transact_managed_appointment(
        &self, _request: Transact, _appointment: workload::ManagedAppointment,
        _command: rtdb_tx::transact::AppointmentCommand, _clock: Arc<dyn rtdb_tx::DecisionClock>,
        _cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        Err(rtdb_tx::transact::funding_kernel_unavailable())
    }

    async fn managed_appointment_status(
        &self, _appointment: &workload::ManagedAppointment, _key: &[u8], _cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        Err(workload::denied())
    }

    async fn transact_funded(
        &self, _request: Transact, _scope: Option<ServingScope>, _funding: FundingSubmission,
        _cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        Err(rtdb_tx::transact::funding_kernel_unavailable())
    }

    async fn apply_token_key_rotation(&self, _generation: u64, _unreadable_by: i64) -> Result<()> {
        Err(rtdb_types::Anomaly::unsupported("rtdb/token-rotation", "This driver does not own token key rotation"))
    }

    async fn destroy_retired_token_key(&self, _generation: u64) -> Result<()> {
        Err(rtdb_types::Anomaly::unsupported("rtdb/token-rotation", "This driver does not own token key rotation"))
    }

    #[cfg(feature = "multi")]
    async fn export_checkpoint(&self, _pin: uuid::Uuid, _basis: Basis, _until: Option<Timestamp>, _cancellation: &CancellationToken) -> Result<Vec<u8>> {
        Err(rtdb_types::Anomaly::unsupported("rtdb/export-checkpoint", "The serving attachment has no ordered checkpoint exports"))
    }

    #[cfg(feature = "multi")]
    async fn open_checkpoint_export(&self, _ticket: Vec<u8>, _cancellation: &CancellationToken) -> Result<rtdb_db::PinnedExport> {
        Err(rtdb_types::Anomaly::unsupported("rtdb/export-checkpoint", "The serving attachment has no ordered checkpoint exports"))
    }

    #[cfg(feature = "multi")]
    async fn release_checkpoint_export(&self, _ticket: Vec<u8>, _cancellation: &CancellationToken) -> Result<()> {
        Err(rtdb_types::Anomaly::unsupported("rtdb/export-checkpoint", "The serving attachment has no ordered checkpoint exports"))
    }

    #[cfg(feature = "multi")]
    async fn restore_checkpoint(&self, _export: rtdb_db::PinnedExport, _cancellation: &CancellationToken) -> Result<BranchCut> {
        Err(rtdb_types::Anomaly::unsupported("rtdb/restore-checkpoint", "The serving attachment has no independent checkpoint restore"))
    }

    fn supports_query_tokens(&self) -> bool { true }

    fn supports_row_visibility(&self) -> bool { false }

    async fn check_custody_delivery(&self, groups: &BTreeSet<CustodyGroupId>, _cancellation: &CancellationToken) -> Result<()> {
        if groups.is_empty() { return Ok(()); }
        Err(rtdb_types::Anomaly::unsupported("rtdb/row-custody", "The query route has no protected delivery authority"))
    }

    async fn check_read_delivery(&self, generation: Option<u64>, groups: &BTreeSet<CustodyGroupId>, cancellation: &CancellationToken) -> Result<()> {
        if generation.is_some() {
            return Err(rtdb_types::Anomaly::fault("rtdb/delivery-generation", "The serving route cannot verify its view generation"));
        }
        self.check_custody_delivery(groups, cancellation).await
    }

    async fn current_custody(&self, _cancellation: &CancellationToken) -> Result<BranchIndex> {
        Err(rtdb_types::Anomaly::unsupported("rtdb/custody-release", "The attachment has no ordered custody catalog"))
    }

    async fn original_custody_receipt(&self, _key: &[u8], _basis: Basis, _cancellation: &CancellationToken) -> Result<Receipt> {
        Err(custody_release::release_unavailable())
    }

    async fn original_writer_receipt(&self, key: &[u8], basis: Basis, cancellation: &CancellationToken) -> Result<Receipt> {
        self.original_custody_receipt(key, basis, cancellation).await
    }

    async fn original_line_creation_receipt(&self, key: &[u8], basis: Basis, cancellation: &CancellationToken) -> Result<Receipt> {
        self.original_custody_receipt(key, basis, cancellation).await
    }

    async fn submit_custody_release(&self, _command: TrustedCustodyCommand, _cancellation: &CancellationToken) -> Result<Receipt> {
        Err(rtdb_types::Anomaly::unsupported("rtdb/custody-release", "The attachment has no trusted release submission"))
    }

    async fn check_custody_release_delivery(&self, _groups: &BTreeSet<CustodyGroupId>, _writer: &CustodyWriterFence,
                                            _generation: Option<u64>, _cancellation: &CancellationToken) -> Result<()> {
        Err(rtdb_types::Anomaly::unsupported("rtdb/custody-release", "The attachment has no original release delivery authority"))
    }

    async fn reconcile_custody(&self, _cancellation: &CancellationToken) -> Result<()> { Ok(()) }

    async fn query_scoped(&self, _source: QuerySource, _track: bool, _now: Timestamp,
                          _scope: &ServingScope, _cancellation: &CancellationToken) -> Result<QueryView> {
        Err(visibility::unsupported())
    }

    async fn managed_workload(&self, _request: workload::Request,
                              _cancellation: &CancellationToken) -> Result<workload::Reply> {
        Err(rtdb_types::Anomaly::unsupported("rtdb/managed-workload", "The attachment has no managed workload writer"))
    }

    async fn managed_original_family(&self, _request: workload::Request,
                                     _cancellation: &CancellationToken) -> Result<workload::Reply> {
        Err(workload::denied())
    }

    async fn query_original_family_review(&self, _source: QuerySource, _review: FamilyReviewSelection,
        _now: Timestamp, _cancellation: &CancellationToken) -> Result<QueryView> {
        Err(workload::denied())
    }

    async fn query_workload_root(
        &self, _source: QuerySource, _cut: WorkloadReadCut, _now: Timestamp, _cancellation: &CancellationToken,
    ) -> Result<QueryView> {
        Err(workload::denied())
    }

    async fn changes_workload_root(
        &self, _request: Changes, _cut: Basis, _cancellation: &CancellationToken,
    ) -> Result<ChangePage> {
        Err(workload::denied())
    }

    /// Watches the journal position this attachment has applied, and the journal past it; a long
    /// poll over `changes_workload_root` waits on it between pages.
    async fn watch_applied(&self) -> Option<AppliedWatch> {
        None
    }

    async fn transact_scoped(&self, _request: Transact, _scope: ServingScope,
                             _cancellation: &CancellationToken) -> Result<Receipt> {
        Err(visibility::unsupported())
    }

    async fn tx_status_scoped(&self, _key: &[u8], _scope: &ServingScope,
                              _cancellation: &CancellationToken) -> Result<Receipt> {
        Err(visibility::unsupported())
    }

    async fn changes_scoped(&self, _request: Changes, _scope: &ServingScope,
                            _cancellation: &CancellationToken) -> Result<ChangePage> {
        Err(visibility::unsupported())
    }

    async fn query(
        &self,
        source: QuerySource,
        track: bool,
        now: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<QueryView>;

    async fn query_ranked_artefacts(
        &self, _database: &str, source: QuerySource, now: Timestamp, cancellation: &CancellationToken,
    ) -> Result<QueryView> {
        self.query(source, false, now, cancellation).await
    }

    fn supports_ranked_ivf(&self) -> bool { false }

    async fn query_ranked_ivf(
        &self, _source: QuerySource, _target: &ranked_ivf::Target, _now: Timestamp, _cancellation: &CancellationToken,
    ) -> Result<(QueryView, ranked_ivf::Catalog)> {
        Err(ranked_ivf::unsupported())
    }

    async fn transact(
        &self,
        request: Transact,
        cancellation: &CancellationToken,
    ) -> Result<Receipt>;

    async fn tx_status(&self, key: &[u8], cancellation: &CancellationToken) -> Result<Receipt>;

    /// One page of the ordered-bytes store at `at` (`rtdb.store.v1.Read`).
    async fn read_store(
        &self, _read: StoreRead, _at: At, _now: Timestamp, _cancellation: &CancellationToken,
    ) -> Result<StorePage> {
        Err(rtdb_types::Anomaly::unsupported("rtdb.store/unsupported", "This route does not serve the ordered-bytes store"))
    }

    async fn read_store_batch(
        &self, reads: &mut (dyn ExactSizeIterator<Item = StoreRead> + Send),
        mut at: At, now: Timestamp, cancellation: &CancellationToken,
    ) -> Result<Vec<StorePage>> {
        let mut pages = Vec::with_capacity(reads.len());
        for read in reads {
            let page = self.read_store(read, at, now, cancellation).await?;
            at = At::Exactly(page.cut.basis);
            pages.push(page);
        }
        Ok(pages)
    }

    /// Revalidate a materialized Store reply before release. Routes without
    /// an ordered Store floor have no further floor check.
    async fn check_local_store_basis(
        &self, _basis: Basis, _cancellation: &CancellationToken,
        #[cfg(feature = "multi")] _turn: Option<&StoreReadTurn>,
    ) -> Result<()> {
        Ok(())
    }

    #[cfg(feature = "multi")]
    async fn enter_local_store_read(&self, _at: At, _cancellation: &CancellationToken) -> Result<Option<StoreReadTurn>> {
        Ok(None)
    }

    #[cfg(feature = "multi")]
    async fn enter_local_store_watch(&self, _after: Basis, _cancellation: &CancellationToken) -> Result<Option<StoreReadTurn>> {
        Ok(None)
    }

    #[cfg(feature = "multi")]
    async fn enter_held_store_read(
        &self, _basis: Basis, _reader: [u8; 32], _hold: StoreVersionHoldKey, _receiver: rtdb_format::journal::StoreWatchReceiver, _cancellation: &CancellationToken,
    ) -> Result<StoreReadTurn> {
        Err(rtdb_types::Anomaly::unsupported("rtdb.store/hold-unsupported", "This route cannot admit an exact retained Store read"))
    }

    #[cfg(feature = "multi")]
    async fn enter_held_store_watch(
        &self, _after: Basis, _reader: [u8; 32], _hold: StoreVersionHoldKey, _receiver: rtdb_format::journal::StoreWatchReceiver, _cancellation: &CancellationToken,
    ) -> Result<StoreReadTurn> {
        Err(rtdb_types::Anomaly::unsupported("rtdb.store/hold-unsupported", "This route cannot admit an exact retained Store watch"))
    }

    async fn store_sizes(&self, _epoch: Option<u64>, _now: Timestamp, _cancellation: &CancellationToken) -> Result<StorageSizes> {
        Err(rtdb_types::Anomaly::unsupported("rtdb.store/unsupported", "This route does not serve the ordered-bytes store"))
    }

    async fn supports_store_purge_floor(&self) -> bool { false }

    #[cfg(feature = "multi")]
    async fn supports_store_version_holds(&self) -> bool { false }

    #[cfg(feature = "multi")]
    async fn store_version_hold(
        &self, _reader: [u8; 32], _request: StoreVersionHoldRequest, _cancellation: &CancellationToken,
    ) -> Result<StoreVersionHoldReply> {
        Err(rtdb_types::Anomaly::unsupported("rtdb.store/hold-unsupported", "This route cannot order a Store Version hold"))
    }

    /// Success confirms the durable floor after checking the retained basis, census time, guards and current writer claim.
    async fn publish_store_purge_floor(
        &self, _since: Basis, _keep_from: u64, _retention_micros: i64, _reads: Vec<KeyRange>, _writer: StoreWriterClaim,
        _memory: Arc<MemoryReservation>, _cancellation: &CancellationToken,
    ) -> Result<()> {
        Err(rtdb_types::Anomaly::unsupported("rtdb.store/purge-unsupported", "This route cannot order a store purge floor"))
    }

    /// A cursor below the retained effect floor reports `rtdb/cursor-expired`.
    async fn changes(
        &self,
        request: Changes,
        cancellation: &CancellationToken,
    ) -> Result<ChangePage>;
}

pub(crate) async fn store_size_snapshot(
    index: BranchIndex, epoch: Option<u64>, now: Timestamp, cancellation: &CancellationToken,
) -> Result<StorageSizes> {
    use rtdb_types::{Anomaly, AnomalyCategory};

    if index.has_custody_groups() || index.has_visibility_history() {
        return Err(Anomaly::unsupported("rtdb.store/unsupported", "The ordered-bytes store serves only an unlabelled database"));
    }
    let cancellation = cancellation.clone();
    tokio::task::spawn_blocking(move || index.storage_sizes(now, epoch, || {
        if cancellation.is_cancelled() {
            return Err(Anomaly::new(AnomalyCategory::Interrupted, "rtdb/request-cancelled", "The request was cancelled"));
        }
        Ok(())
    })).await.map_err(|error| Anomaly::fault("rtdb.store/size-snapshot", error.to_string()))?
}
