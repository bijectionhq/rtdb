use std::collections::BTreeMap;

use async_trait::async_trait;
use rtdb_types::{Result, Value};

use crate::CatalogRows;

pub type InspectionRow = BTreeMap<String, Value>;

#[derive(Clone, Debug)]
pub enum InspectionRequest {
    SourceLog { from: i64, to: i64, tx_ops: bool },
    ReplicaLog { from: i64, to: i64 },
    BlockFiles { from: i64, to: i64 },
    TableBlock { table: String, block: i64, tries: bool },
}

#[async_trait]
pub trait CatalogInspection: Send {
    /// Each page retains its complete allocation charge; `None` is the end of the finite requested range.
    /// Cancellation may drop this future and cursor; all child resources remain owned until their I/O finishes.
    async fn next(&mut self) -> Result<Option<CatalogRows<InspectionRow>>>;
}
