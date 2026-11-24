use std::{collections::BTreeSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use rtdb_crypt::tokens::TokenAuthority;
use rtdb_index::{
    Snapshot,
    branches::{BranchCut, BranchIndex, BranchView, BranchWrite, RowImage},
};
use rtdb_tx::transact::{CustodyGroupId, CustodyWriterFence, Receipt, Transact, TrustedCustodyCommand};
pub use rtdb_replication::{NativeCheckpointAuthority, NativeServingAuthority};
use rtdb_types::{
    EntityId, Interval, Result, TableRef, Timestamp,
    rtdb_v1::{At, Basis, Branch, Claim, Layer},
};
use tokio_util::sync::CancellationToken;

pub(crate) mod custody;
pub mod custody_release;
pub(crate) mod default;
pub mod ranked_ivf;
mod visibility;
pub use visibility::{ServingScope, ROW_VISIBILITY};

#[derive(Clone)]
pub struct QuerySource {
    pub branch: Branch,
    pub at: At,
    pub layer: Layer,
}

pub struct QueryView {
    pub snapshot: Snapshot,
    pub cut: BranchCut,
    pub claim: Option<Claim>,
    pub custody_groups: BTreeSet<CustodyGroupId>,
    pub custody_contributors: Option<custody_release::CustodyContributors>,
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
    pub before: Option<RowImage>,
    pub after: Option<RowImage>,
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
    fn image_at<'a>(view: Option<&BranchView<'a>>, write: &BranchWrite, from: Timestamp) -> Option<&'a RowImage> {
        view.and_then(|view| view.row(&write.table, &write.id, from))
            .and_then(|event| event.image_at(from))
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

/// An admitted database attachment. Each mode establishes its own barriers and publishes only applied state.
#[async_trait]
pub trait ServingDriver: Send + Sync {
    fn incarnation(&self) -> [u8; 16];
    fn features(&self) -> &[&str];
    fn token_authority(&self) -> Option<TokenAuthority> { None }
    fn uses_database_tokens(&self) -> bool { false }

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

    #[cfg(feature = "multi")]
    async fn checkpoint_source(&self, _cancellation: &CancellationToken) -> Result<Option<rtdb_db::CheckpointSource>> {
        Err(rtdb_types::Anomaly::unsupported("rtdb/restore-checkpoint", "The serving attachment has no retained checkpoint source"))
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

    /// A cursor below the retained effect floor reports `rtdb/cursor-expired`.
    async fn changes(
        &self,
        request: Changes,
        cancellation: &CancellationToken,
    ) -> Result<ChangePage>;
}
