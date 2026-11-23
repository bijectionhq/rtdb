use crate::{AppendClass, Bytes, Command, Enrollment, Grant, ObjectName, Placement, Principal, RequestId};
use crate::production::{
    append_command, barrier_command, barrier_read_matches, disposal_is_due, dispose_expired_command,
    lease_command, placement_matches, release_read_command, same_writer, scoped_request,
};
use vstd::prelude::*;

verus! {

pub open spec fn exact_scope(installed: Placement, observed: Placement) -> bool {
    observed == installed
}

pub open spec fn current_grant(principal: Principal, current: Option<Grant>, used: Grant) -> bool {
    principal == Principal::Worker(used.enrollment) && current == Some(used)
}

pub open spec fn barrier_reader(principal: Principal, roster: Option<Enrollment>) -> bool {
    match principal {
        Principal::Worker(reader) => roster == Some(reader),
        _ => false,
    }
}

pub open spec fn exact_read_identity(invocation: RequestId, returned: RequestId) -> bool {
    returned == invocation
}

pub open spec fn append_identity(principal: Principal, current: Option<Grant>, used: Grant, head: u64, next: u64) -> bool {
    current_grant(principal, current, used) && next == head
}

pub open spec fn reference_presence(explicit: bool, fenced: bool) -> bool {
    explicit || !fenced
}

pub open spec fn expiry_disposal_admits(
    principal: Principal, serial: u64, issued_serial: u64, still_enrolled: bool,
    eligible_at: Option<u64>, decision_time: u64,
) -> bool {
    (principal == Principal::Administrator || principal == Principal::Custodian)
        && 0 < serial && serial <= issued_serial && !still_enrolled
        && (exists |eligible: u64| eligible_at == Some(eligible) && eligible <= decision_time)
}

fn request_names_the_exact_scope(placement: Placement, principal: Principal, id: RequestId, command: Command) {
    let request = scoped_request(placement, principal, id, command);
    assert(exact_scope(placement, request.placement) && request.principal == principal
        && request.id == id && request.command == command);
}

fn observed_scope_must_match(placement: Placement, route: Placement) {
    let matched = placement_matches(placement, route);
    assert(matched == exact_scope(placement, route));
}

fn barrier_names_the_enrolled_reader(
    placement: Placement, reader: Enrollment, id: RequestId,
) {
    let command = barrier_command();
    let request = scoped_request(placement, Principal::Worker(reader), id, command);
    assert(request.command is Barrier);
    assert(barrier_reader(request.principal, Some(reader)));
}

fn barrier_returns_the_exact_read_identity(invocation: RequestId, read: RequestId) {
    let matched = barrier_read_matches(invocation, read);
    assert(matched == exact_read_identity(invocation, read));
}

fn release_names_the_barrier_invocation(invocation: RequestId) {
    let command = release_read_command(invocation);
    assert(command matches Command::ReleaseRead { read } && exact_read_identity(invocation, read));
}

fn append_request_names_observed_head(principal: Principal, current: Option<Grant>, local: Grant,
    head: u64, class: AppendClass, timestamp: i64, payload: Bytes)
    requires principal == Principal::Worker(local.enrollment),
{
    if same_writer(current, local) {
        let command = append_command(class, local, head, timestamp, payload, None);
        assert(command matches Command::Append { grant, expected_next, .. }
            && append_identity(principal, current, grant, head, expected_next));
    }
}

fn append_preserves_reference_list(
    class: AppendClass, grant: Grant, head: u64, timestamp: i64, payload: Bytes,
    references: Option<Vec<ObjectName>>,
) {
    let ghost supplied = references;
    let command = append_command(class, grant, head, timestamp, payload, references);
    assert(command matches Command::Append { references: sent, .. } && sent == supplied);
}

fn opaque_append_is_refused_after_a_fence(
    class: AppendClass, grant: Grant, head: u64, timestamp: i64, payload: Bytes,
) {
    let command = append_command(class, grant, head, timestamp, payload, None);
    assert(command matches Command::Append { references, .. }
        && !reference_presence(references is Some, true));
}

fn lease_request_names_the_supplied_grant(principal: Principal, current: Option<Grant>, grant: Grant)
    requires current_grant(principal, current, grant),
{
    let command = lease_command(grant);
    assert(command matches Command::Lease { grant: used } && current_grant(principal, current, used));
}

fn disposal_attempt_copies_the_queried_enrollment(
    owner: Enrollment, issued_serial: u64, still_enrolled: bool, eligible: u64,
    local_time: u64, route_time: u64, decision_time: u64,
)
    requires 0 < owner.serial <= issued_serial, !still_enrolled, eligible <= decision_time,
{
    if disposal_is_due(Some(eligible), local_time, route_time) {
        let command = dispose_expired_command(owner);
        assert(command matches Command::DisposeExpired { enrollment } && enrollment == owner
            && expiry_disposal_admits(Principal::Custodian, enrollment.serial, issued_serial,
                still_enrolled, Some(eligible), decision_time));
    }
}

fn disposal_due_uses_eligibility(eligible_at: Option<u64>, local_time: u64, route_time: u64) {
    let due = disposal_is_due(eligible_at, local_time, route_time);
    assert(due == (exists |eligible: u64| eligible_at == Some(eligible) && (eligible <= local_time || eligible <= route_time)));
}

}
