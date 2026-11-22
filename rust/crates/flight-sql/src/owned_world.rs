use std::sync::Arc;

use futures::future::BoxFuture;
use rtdb_columnar::MemoryReservation;
use rtdb_node::{Database, NativeSession, Node, OwnedWorldPointWitness, OwnedWorldWrite};
use rtdb_types::{EntityId, OwnedRelation, Result, StructFields, Timestamp, TransactionKey, Value};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// The placed host's closed checkpoint-world authority. Flight supplies the
/// authenticated native session and its already admitted database attachment.
pub trait OwnedWorldHost: std::fmt::Debug + Send + Sync {
    fn read_point<'a>(&'a self, call: WorldCall<'a>, point: WorldPoint<'a>) -> BoxFuture<'a, Result<WorldPointResult>>;
    fn open_world_read<'a>(&'a self, call: WorldCall<'a>, world: Uuid) -> BoxFuture<'a, Result<Box<dyn OwnedWorldRead>>>;
    fn fork_world<'a>(&'a self, call: WorldCall<'a>, capture: Uuid, suffix: Option<Uuid>, until: Timestamp) -> BoxFuture<'a, Result<Uuid>>;
    fn commit_world<'a>(&'a self, call: WorldCall<'a>, world: Uuid, writes: Vec<OwnedWorldWrite>, checks: Vec<OwnedWorldPointWitness>) -> BoxFuture<'a, Result<TransactionKey>>;
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
    pub witness: Option<OwnedWorldPointWitness>,
}

pub struct WorldPointRow {
    pub fields: StructFields<Value>,
    pub revision: Timestamp,
    pub reservation: Arc<MemoryReservation>,
}


/// A verified live cut and its already admitted archived parent.
pub trait OwnedWorldRead: Send + Sync {
    fn present(&self) -> Timestamp;
    fn check(&self) -> Result<()>;
    fn close(&self);
    fn read_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid_at: Timestamp,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
}
