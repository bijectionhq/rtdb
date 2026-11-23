#![allow(unexpected_cfgs)]

#[cfg(not(verus_keep_ghost))]
use rtdb_format::envelope::ReplicaBody;
#[cfg(verus_keep_ghost)]
use crate::ReplicaBody;

pub(crate) fn is_maintenance(message: &ReplicaBody, sequence: Option<i64>, fenced: bool, fenced_terminal: bool) -> bool {
    match message {
        ReplicaBody::BlockBoundary(_) | ReplicaBody::BlockUploaded(_) => true,
        ReplicaBody::NoOp(message) => !fenced && sequence == Some(0) && message.src_msg_id.is_none()
            && message.placed_slot_reservation.is_none(),
        ReplicaBody::ResolvedTx(_) => fenced_terminal,
        ReplicaBody::TriesAdded(_) | ReplicaBody::TriesDeleted(_)
        | ReplicaBody::OversizedMessage(_) | ReplicaBody::ArtefactsAdded(_) | ReplicaBody::ArtefactsRemoved(_)
        | ReplicaBody::TablesRetired(_) => false,
    }
}
