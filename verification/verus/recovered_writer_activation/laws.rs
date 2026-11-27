use crate::recovery_promotion::{may_promote, publish_installed_prefix, writer_activation};
use vstd::prelude::*;

verus! {

pub fn unknown_prefix_cannot_promote(
    prefix_applied: bool, may_lead: bool, already_leading: bool, holds_grant: bool,
) {
    let accepted = may_promote(false, prefix_applied, may_lead, already_leading, holds_grant);
    assert(!accepted);
}

pub fn writer_waits_for_the_recovered_prefix(received: i64, previous: i64)
    requires previous < received,
{
    let initial = writer_activation(true);
    assert(initial is Recovering);
    let appended = initial.installed(true, true, true, received, Some(previous));
    assert(appended is Recovering);
    let unconfirmed = appended.installed(false, true, true, received, Some(received));
    assert(unconfirmed is Recovering);
    let unknown = unconfirmed.installed(true, false, true, received, Some(received));
    assert(unknown is Recovering);
    let unapplied = unknown.installed(true, true, false, received, Some(received));
    assert(unapplied is Recovering);
    let recovered = unapplied.installed(true, true, true, received, Some(received));
    assert(recovered is Active);
}

pub fn unapplied_prefix_blocks_promotion_and_activation(next_pos: i64) {
    let can_promote = may_promote(true, false, true, false, true);
    assert(!can_promote);
    let published = publish_installed_prefix(true, Some(next_pos), false);
    assert(published is None);
    let activation = writer_activation(true).installed(true, true, false, next_pos, Some(next_pos));
    assert(activation is Recovering);
}

pub fn applied_prefix_enables_promotion_and_serving(next_pos: i64) {
    let can_promote = may_promote(true, true, true, false, true);
    assert(can_promote);
    let published = publish_installed_prefix(true, Some(next_pos), true);
    assert(published == Some(next_pos));
    let activation = writer_activation(true).installed(true, true, true, next_pos, Some(next_pos));
    assert(activation is Active);
}

}
