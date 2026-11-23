use crate::production::receipt_follows_selection;
use vstd::prelude::*;

verus! {

// LAW RETIREMENT-INTENT-JOURNAL.
pub fn a_later_receipt_from_another_journal_refuses(selected: i64, installed: i64, journal: i32, other: i32)
    requires installed > selected, journal != other,
{
    let accepted = receipt_follows_selection(selected, journal, installed, other);
    assert(!accepted);
}

// LAW RETIREMENT-INTENT-ADVANCE.
pub fn an_equal_or_older_receipt_refuses(selected: i64, installed: i64, journal: i32)
    requires installed <= selected,
{
    let accepted = receipt_follows_selection(selected, journal, installed, journal);
    assert(!accepted);
}

// LAW RETIREMENT-INTENT-PROGRESS.
pub fn a_later_receipt_in_this_journal_confirms(selected: i64, installed: i64, journal: i32)
    requires installed > selected,
{
    let accepted = receipt_follows_selection(selected, journal, installed, journal);
    assert(accepted);
}

}
