#![allow(unexpected_cfgs)]

#[cfg(not(verus_keep_ghost))]
use crate::shared_plans::SharedPlanKey;
#[cfg(verus_keep_ghost)]
use crate::SharedPlanKey;
#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {

pub open spec fn distinct_keys<V>(entries: Seq<(SharedPlanKey, V)>) -> bool {
    forall|i: int, j: int| 0 <= i < entries.len() && 0 <= j < entries.len() && i != j ==> entries[i].0 != entries[j].0
}

}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out matches Some(index) ==> index < entries@.len() && entries@[index as int].0 == *key,
        out is None ==> forall|i: int| 0 <= i < entries@.len() ==> entries@[i].0 != *key,
))]
pub(crate) fn find<V>(entries: &Vec<(SharedPlanKey, V)>, key: &SharedPlanKey) -> Option<usize> {
    let mut index = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        index <= entries@.len(),
        forall|i: int| 0 <= i < index ==> entries@[i].0 != *key,
        decreases entries@.len() - index,
    ))]
    while index < entries.len() {
        if entries[index].0 == *key { return Some(index); }
        index += 1;
    }
    None
}

#[cfg_attr(verus_keep_ghost, verus_spec(
    requires index < old(entries)@.len(),
    ensures final(entries)@ == old(entries)@.remove(index as int).push(old(entries)@[index as int]),
))]
pub(crate) fn touch<V>(entries: &mut Vec<(SharedPlanKey, V)>, index: usize) {
    let entry = entries.remove(index);
    entries.push(entry);
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        old(entries)@.len() == 0 ==> out is None && final(entries)@ == old(entries)@,
        old(entries)@.len() > 0 ==> out == Some(old(entries)@[0]) && final(entries)@ == old(entries)@.remove(0),
))]
pub(crate) fn evict_oldest<V>(entries: &mut Vec<(SharedPlanKey, V)>) -> Option<(SharedPlanKey, V)> {
    if entries.len() == 0 { None } else { Some(entries.remove(0)) }
}

#[cfg_attr(verus_keep_ghost, verus_spec(
    requires
        0 < capacity,
        old(entries)@.len() <= capacity,
        distinct_keys(old(entries)@),
    ensures
        final(entries)@.len() <= capacity,
        final(entries)@.len() > 0,
        final(entries)@.last() == (key, value),
        distinct_keys(final(entries)@),
        forall|i: int| 0 <= i < final(entries)@.len() - 1 ==> old(entries)@.contains(#[trigger] final(entries)@[i]),
))]
pub(crate) fn admit<V>(entries: &mut Vec<(SharedPlanKey, V)>, key: SharedPlanKey, value: V, capacity: usize) {
    if let Some(index) = find(entries, &key) { entries.remove(index); }
    if entries.len() == capacity { entries.remove(0); }
    entries.push((key, value));
}
