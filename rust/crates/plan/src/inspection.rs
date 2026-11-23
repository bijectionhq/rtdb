use rtdb_types::StructFields;

use async_trait::async_trait;
use rtdb_types::{Result, Value};

use crate::CatalogRows;

pub type InspectionRow = StructFields<Value>;

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

    fn supports_arrow(&self) -> bool { false }

    /// Returns at most `limit` records with the provider's physical fields.
    async fn next_arrow(&mut self, _limit: usize) -> Result<Option<rtdb_columnar::OwnedBatch>> {
        Err(rtdb_types::Anomaly::unsupported("rtdb.plan/inspection-arrow", "The inspection provider does not return Arrow batches"))
    }
}
