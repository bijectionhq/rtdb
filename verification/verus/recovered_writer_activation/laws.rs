use crate::recovery_promotion::{may_promote, writer_activation};
use vstd::prelude::*;

verus! {

pub fn unknown_prefix_cannot_promote(may_lead: bool, already_leading: bool, holds_grant: bool) {
    let accepted = may_promote(false, may_lead, already_leading, holds_grant);
    assert(!accepted);
}

pub fn writer_waits_for_the_recovered_prefix(received: i64, previous: i64)
    requires previous < received,
{
    let initial = writer_activation(true);
    assert(initial is Recovering);
    let appended = initial.installed(true, true, received, Some(previous));
    assert(appended is Recovering);
    let unconfirmed = appended.installed(false, true, received, Some(received));
    assert(unconfirmed is Recovering);
    let unknown = unconfirmed.installed(true, false, received, Some(received));
    assert(unknown is Recovering);
    let recovered = unknown.installed(true, true, received, Some(received));
    assert(recovered is Active);
}

}
