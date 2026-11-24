use std::{collections::BTreeSet, time::Duration};

use async_trait::async_trait;
use rtdb_crypt::tokens::TokenAuthority;
use rtdb_index::{
    Snapshot,
    branches::{BranchCut, RowImage},
};
use rtdb_tx::transact::{CustodyGroupId, Receipt, Transact};
pub use rtdb_replication::{NativeCheckpointAuthority, NativeServingAuthority};
use rtdb_types::{
    EntityId, Interval, Result, TableRef, Timestamp,
    rtdb_v1::{At, Basis, Branch, Claim, Layer},
};
use tokio_util::sync::CancellationToken;

pub(crate) mod custody;
pub(crate) mod default;
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
    pub groups: Vec<ChangeGroup>,
    pub through: Basis,
    pub complete: bool,
    pub custody_groups: BTreeSet<CustodyGroupId>,
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

    fn supports_row_visibility(&self) -> bool { false }

    async fn check_custody_delivery(&self, groups: &BTreeSet<CustodyGroupId>, _cancellation: &CancellationToken) -> Result<()> {
        if groups.is_empty() { return Ok(()); }
        Err(rtdb_types::Anomaly::unsupported("rtdb/row-custody", "The query route has no protected delivery authority"))
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
