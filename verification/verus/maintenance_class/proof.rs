#![feature(proc_macro_hygiene)]

mod laws;
#[path = "../../../rust/crates/replication/src/maintenance_class.rs"]
mod production;

pub struct NoOp {
    pub src_msg_id: Option<i64>,
    pub placed_slot_reservation: Option<u8>,
}

pub enum ReplicaBody {
    ResolvedTx(()),
    TriesAdded(()),
    BlockBoundary(()),
    BlockUploaded(()),
    NoOp(NoOp),
    TriesDeleted(()),
    OversizedMessage(()),
    ArtefactsAdded(()),
    ArtefactsRemoved(()),
    TablesRetired(()),
}

fn main() {}
