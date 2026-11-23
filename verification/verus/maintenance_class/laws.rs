use crate::{ReplicaBody, production::is_maintenance};
use vstd::prelude::*;

verus! {

pub enum Phase { Idle, Boundary, Uploaded }

pub open spec fn shape(phase: Phase, used: nat) -> bool {
    match phase {
        Phase::Idle => used == 1,
        Phase::Boundary => used == 2,
        Phase::Uploaded => used == 3,
    }
}

pub open spec fn maintenance_append_allowed(phase: Phase, body: &ReplicaBody) -> bool {
    (body is BlockBoundary ==> phase is Idle) && (body is BlockUploaded ==> phase is Boundary)
}

pub open spec fn record_charge(body: &ReplicaBody) -> nat {
    if body is BlockBoundary || body is BlockUploaded { 1 } else { 0 }
}

}

#[verus_spec(maintenance =>
    ensures
        (if maintenance { 1nat } else { 0nat }) == record_charge(body)
        && ((shape(phase, used as nat) && maintenance_append_allowed(phase, body))
            ==> used as nat + (if maintenance { 1nat } else { 0nat }) <= 3),
)]
fn classifier_refines_three_record_bound(phase: Phase, used: u64, body: &ReplicaBody, sequence: Option<i64>) -> bool {
    let fenced = matches!(body, ReplicaBody::NoOp(_));
    is_maintenance(body, sequence, fenced, false)
}
