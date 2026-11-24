use crate::{ReplicaBody, production::is_maintenance};
use vstd::prelude::*;

verus! {

pub enum Phase { Idle, Boundary, Uploaded }

pub enum ReceiverSlot { Vacant, Current, Inherited }

pub open spec fn shape(phase: Phase, used: nat) -> bool {
    match phase {
        Phase::Idle => used == 1,
        Phase::Boundary => used == 2,
        Phase::Uploaded => used == 3,
    }
}

pub open spec fn outstanding(slot: ReceiverSlot) -> nat {
    if slot is Vacant { 0 } else { 1 }
}

pub open spec fn funded_records(receiver_budget: nat) -> nat {
    3 + receiver_budget
}

pub open spec fn maintenance_append_allowed(
    phase: Phase, slot: ReceiverSlot, body: &ReplicaBody, fenced_terminal: bool,
) -> bool {
    match body {
        ReplicaBody::BlockBoundary(_) => phase is Idle && slot is Vacant,
        ReplicaBody::BlockUploaded(_) => phase is Boundary,
        ReplicaBody::ResolvedTx(_) if fenced_terminal => phase is Idle && slot is Inherited,
        _ => true,
    }
}

pub open spec fn record_charge(body: &ReplicaBody, fenced_terminal: bool) -> nat {
    match body {
        ReplicaBody::BlockBoundary(_) | ReplicaBody::BlockUploaded(_) => 1,
        ReplicaBody::ResolvedTx(_) if fenced_terminal => 1,
        _ => 0,
    }
}

}

#[verus_spec(maintenance =>
    ensures (if maintenance { 1nat } else { 0nat }) == record_charge(body, fenced_terminal),
)]
fn classifier_refines_record_charge(body: &ReplicaBody, sequence: Option<i64>, fenced_terminal: bool) -> bool {
    let fenced = matches!(body, ReplicaBody::NoOp(_));
    is_maintenance(body, sequence, fenced, fenced_terminal)
}

#[verus_spec(maintenance =>
    requires
        shape(phase, checkpoint_used as nat),
        settled as nat + outstanding(slot) <= receiver_budget as nat,
        maintenance_append_allowed(phase, slot, body, fenced_terminal),
    ensures checkpoint_used as nat + settled as nat + (if maintenance { 1nat } else { 0nat })
        <= funded_records(receiver_budget as nat),
)]
fn classifier_refines_maintenance_bound(
    phase: Phase, checkpoint_used: u64, slot: ReceiverSlot, settled: u64, receiver_budget: u64,
    body: &ReplicaBody, sequence: Option<i64>, fenced_terminal: bool,
) -> bool {
    classifier_refines_record_charge(body, sequence, fenced_terminal)
}
