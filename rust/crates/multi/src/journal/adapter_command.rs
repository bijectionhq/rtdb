use bytes::Bytes;
use ordered_logkeeper::ordered::{
    AppendClass, Command, Enrollment, Grant, ObjectName, Placement, Principal, Request, RequestId,
};

pub(super) fn placement_matches(expected: Placement, observed: Placement) -> bool {
    expected == observed
}

pub(super) fn barrier_read_matches(invocation: RequestId, read: RequestId) -> bool {
    invocation == read
}

pub(super) fn disposal_is_due(eligible_at: Option<u64>, local_time: u64, route_time: u64) -> bool {
    match eligible_at {
        Some(eligible) => eligible <= local_time || eligible <= route_time,
        None => false,
    }
}

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

pub(super) fn same_writer(observed: Option<Grant>, local: Grant) -> bool {
    observed == Some(local)
}

pub(super) fn barrier_command() -> Command {
    Command::Barrier
}

pub(super) fn release_read_command(read: RequestId) -> Command {
    Command::ReleaseRead { read }
}

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

pub(super) fn lease_command(grant: Grant) -> Command {
    Command::Lease { grant }
}

pub(super) fn dispose_expired_command(enrollment: Enrollment) -> Command {
    Command::DisposeExpired { enrollment }
}

#[cfg(test)]
mod tests {
    use super::*;

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
