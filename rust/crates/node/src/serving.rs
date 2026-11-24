use std::{collections::BTreeSet, time::Duration};

use async_trait::async_trait;
use rtdb_index::{Snapshot, branches::BranchCut};
use rtdb_tx::transact::Receipt;
pub use rtdb_replication::{NativeCheckpointAuthority, NativeServingAuthority, TransactSubmission};
use rtdb_types::{
    EntityId, Interval, Result, StructFields, TableRef, Timestamp, Value,
    rtdb_v1::{At, Basis, Branch, Claim, Layer},
};
use tokio_util::sync::CancellationToken;

pub(crate) mod default;

pub struct QuerySource {
    pub branch: Branch,
    pub at: At,
    pub layer: Layer,
}

pub struct QueryView {
    pub snapshot: Snapshot,
    pub cut: BranchCut,
    pub claim: Option<Claim>,
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
    pub before: Option<StructFields<Value>>,
    pub after: Option<StructFields<Value>>,
}

/// An admitted database attachment. Each mode establishes its own barriers and publishes only applied state.
#[async_trait]
pub trait ServingDriver: Send + Sync {
    fn incarnation(&self) -> [u8; 16];
    fn features(&self) -> &[&str];

    async fn query(
        &self,
        source: QuerySource,
        track: bool,
        now: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<QueryView>;

    async fn transact(
        &self,
        request: TransactSubmission,
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
