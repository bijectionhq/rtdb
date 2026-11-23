#![allow(unexpected_cfgs)]

pub(crate) fn replica_read_is_empty(record_count: usize) -> bool {
    record_count == 0
}

pub(crate) fn can_promote_fenced(may_lead: bool, already_leading: bool, holds_grant: bool) -> bool {
    may_lead && !already_leading && holds_grant
}

pub(crate) fn should_start_claim(fenced: bool, may_lead: bool, already_leading: bool, claim_pending: bool) -> bool {
    !fenced && may_lead && !already_leading && !claim_pending
}

pub(crate) fn initial_term_sequence(fenced: bool) -> i64 {
    if fenced { 0 } else { 1 }
}
