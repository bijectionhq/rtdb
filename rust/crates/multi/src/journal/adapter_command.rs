#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
use crate::{
    AppendClass, Bytes, Command, Enrollment, Grant, ObjectName, Placement, Principal, Request, RequestId,
};
#[cfg(not(verus_keep_ghost))]
use bytes::Bytes;
#[cfg(not(verus_keep_ghost))]
use ordered_logkeeper::ordered::{
    AppendClass, Command, Enrollment, Grant, ObjectName, Placement, Principal, Request, RequestId,
};

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (expected == observed),
))]
pub(super) fn placement_matches(expected: Placement, observed: Placement) -> bool {
    let Placement {
        deployment,
        incarnation,
        group,
        generation,
    } = expected;
    deployment.as_u128() == observed.deployment.as_u128()
        && incarnation.as_u128() == observed.incarnation.as_u128()
        && group.as_u128() == observed.group.as_u128()
        && generation == observed.generation
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (invocation == read),
))]
pub(super) fn barrier_read_matches(invocation: RequestId, read: RequestId) -> bool {
    let RequestId { nonce, expires_at } = invocation;
    nonce.as_u128() == read.nonce.as_u128() && expires_at == read.expires_at
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (match eligible_at {
        Some(eligible) => eligible <= local_time || eligible <= route_time,
        None => false,
    }),
))]
pub(super) fn disposal_is_due(eligible_at: Option<u64>, local_time: u64, route_time: u64) -> bool {
    match eligible_at {
        Some(eligible) => eligible <= local_time || eligible <= route_time,
        None => false,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out.placement == placement, out.principal == principal, out.id == id, out.command == command,
))]
pub(super) fn scoped_request(
    placement: Placement,
    principal: Principal,
    id: RequestId,
    command: Command,
) -> Request {
    Request {
        placement,
        principal,
        id,
        command,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (observed == Some(local)),
))]
pub(super) fn same_writer(observed: Option<Grant>, local: Grant) -> bool {
    let Some(grant) = observed else {
        return false;
    };
    let Grant {
        enrollment: Enrollment { worker, serial },
        epoch,
        key_generation,
    } = local;
    grant.enrollment.worker.as_u128() == worker.as_u128()
        && grant.enrollment.serial == serial
        && grant.epoch == epoch
        && grant.key_generation == key_generation
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (successor_epoch as int == predecessor_epoch as int + 1
        && (predecessor_key.is_some() ==> predecessor_key.unwrap() == successor_key)),
))]
pub(super) fn ordered_writer_successor(
    predecessor_epoch: u64,
    predecessor_key: Option<u64>,
    successor_epoch: u64,
    successor_key: u64,
) -> bool {
    let Some(next_epoch) = predecessor_epoch.checked_add(1) else {
        return false;
    };
    let same_key = match predecessor_key {
        Some(key) => successor_key == key,
        None => true,
    };
    next_epoch == successor_epoch && same_key
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == Command::Barrier,
))]
pub(super) fn barrier_command() -> Command {
    Command::Barrier
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (Command::ReleaseRead { read }),
))]
pub(super) fn release_read_command(read: RequestId) -> Command {
    Command::ReleaseRead { read }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out matches Command::Append {
        class: returned_class, grant: returned_grant, expected_next: returned_next,
        timestamp: returned_timestamp, payload: returned_payload, references: sent,
    } && returned_class == class && returned_grant == grant && returned_next == expected_next
        && returned_timestamp == timestamp && returned_payload == payload && sent == references,
))]
pub(super) fn append_command(
    class: AppendClass,
    grant: Grant,
    expected_next: u64,
    timestamp: i64,
    payload: Bytes,
    references: Option<Vec<ObjectName>>,
) -> Command {
    Command::Append {
        class,
        grant,
        expected_next,
        timestamp,
        payload,
        references,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (Command::Lease { grant }),
))]
pub(super) fn lease_command(grant: Grant) -> Command {
    Command::Lease { grant }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (Command::DisposeExpired { enrollment }),
))]
pub(super) fn dispose_expired_command(enrollment: Enrollment) -> Command {
    Command::DisposeExpired { enrollment }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_writer_nomination_must_replace_its_exact_predecessor() {
        assert!(ordered_writer_successor(37, Some(2), 38, 2));
        assert!(ordered_writer_successor(0, None, 1, 2));
        assert!(!ordered_writer_successor(37, Some(2), 37, 2));
        assert!(!ordered_writer_successor(37, Some(2), 39, 2));
        assert!(!ordered_writer_successor(37, Some(2), 38, 3));
        assert!(!ordered_writer_successor(u64::MAX, None, 0, 2));
    }

    #[test]
    fn release_names_the_original_read_and_its_horizon() {
        let read = RequestId {
            nonce: uuid::Uuid::new_v4(),
            expires_at: 49,
        };
        assert!(
            matches!(release_read_command(read), Command::ReleaseRead { read: released } if released == read)
        );
    }

    #[test]
    fn disposal_can_be_due_on_either_clock() {
        assert!(disposal_is_due(Some(9), 9, 8));
        assert!(disposal_is_due(Some(9), 8, 9));
        assert!(!disposal_is_due(Some(9), 8, 8));
        assert!(!disposal_is_due(None, 9, 9));
    }

    #[test]
    fn observed_writer_must_match_their_entire_grant() {
        let enrollment = Enrollment {
            worker: uuid::Uuid::new_v4(),
            serial: 1,
        };
        let grant = Grant {
            enrollment,
            epoch: 37,
            key_generation: 1,
        };
        assert!(same_writer(Some(grant), grant));
        assert!(!same_writer(
            Some(Grant {
                key_generation: 2,
                ..grant
            }),
            grant
        ));
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (expected_lo == observed_lo && expected_hi == observed_hi
        && prepared > 0 && prepared < fence && fence < opening),
))]
pub(crate) fn exact_writer_settlement(
    expected_lo: u128, expected_hi: u128, observed_lo: u128, observed_hi: u128,
    prepared: u64, fence: u64, opening: u64,
) -> bool {
    expected_lo == observed_lo && expected_hi == observed_hi
        && prepared > 0 && prepared < fence && fence < opening
}
