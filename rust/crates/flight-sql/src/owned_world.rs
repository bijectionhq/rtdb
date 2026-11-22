use std::sync::Arc;

use futures::future::BoxFuture;
use rtdb_columnar::MemoryReservation;
use rtdb_node::{Database, NativeSession, Node};
use rtdb_types::{EntityId, OwnedRelation, Result, StructFields, Timestamp, Value};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// The placed host's closed checkpoint-world authority. Flight supplies the
/// authenticated native session and its already admitted database attachment.
pub trait OwnedWorldHost: std::fmt::Debug + Send + Sync {
    fn read_point<'a>(&'a self, call: WorldCall<'a>, point: WorldPoint<'a>) -> BoxFuture<'a, Result<WorldPointResult>>;
    fn drop_world<'a>(&'a self, call: WorldCall<'a>, world: Uuid) -> BoxFuture<'a, Result<()>>;
}

pub struct WorldCall<'a> {
    pub node: &'a Node,
    pub attachment: &'a Database,
    pub session: &'a mut NativeSession,
    pub cancellation: &'a CancellationToken,
}

pub struct WorldPoint<'a> {
    pub world: Uuid,
    pub relation: &'a OwnedRelation,
    pub id: &'a EntityId,
    pub valid_at: Timestamp,
}

pub struct WorldPointResult {
    pub present: Timestamp,
    pub row: Option<WorldPointRow>,
}

pub struct WorldPointRow {
    pub fields: StructFields<Value>,
    pub revision: Timestamp,
    pub reservation: Arc<MemoryReservation>,
}
