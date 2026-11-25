use crate::SharedPlanKey;
use crate::production::{admit, distinct_keys, evict_oldest, find, touch};
use crate::replay::replays_every;
use vstd::prelude::*;

verus! {

/// A lookup returns the plan stored under an equal key, and only that.
fn a_hit_is_the_entry_of_an_equal_key<V>(entries: &Vec<(SharedPlanKey, V)>, key: &SharedPlanKey) -> (out: Option<usize>)
    requires distinct_keys(entries@),
    ensures
        out matches Some(index) ==> entries@[index as int].0 == *key
            && forall|j: int| 0 <= j < entries@.len() && j != index ==> entries@[j].0 != *key,
        out is None ==> forall|j: int| 0 <= j < entries@.len() ==> entries@[j].0 != *key,
{
    find(entries, key)
}

/// A plan is reused only where every lookup its planning recorded answers as recorded.
fn a_replayed_plan_saw_every_recorded_answer(recorded: &[(usize, u64)], catalog: &Vec<u64>) -> (out: bool)
    requires forall|i: int| 0 <= i < recorded@.len() ==> recorded@[i].0 < catalog@.len(),
    ensures out ==> forall|i: int| 0 <= i < recorded@.len() ==> catalog@[recorded@[i].0 as int] == recorded@[i].1,
{
    let same = |lookup: &(usize, u64)| -> (answered: Result<bool, ()>)
        requires lookup.0 < catalog@.len(),
        ensures answered == Ok::<bool, ()>(catalog@[lookup.0 as int] == lookup.1),
    {
        Ok(catalog[lookup.0] == lookup.1)
    };
    match replays_every(recorded, same) {
        Ok(same) => same,
        Err(()) => false,
    }
}

/// Admitting a plan bounds the entries by the capacity and keeps one entry per key.
fn admitting_keeps_one_bounded_entry_per_key<V>(entries: &mut Vec<(SharedPlanKey, V)>, key: SharedPlanKey, value: V)
    requires old(entries)@.len() <= 128, distinct_keys(old(entries)@),
    ensures
        final(entries)@.len() <= 128,
        final(entries)@.last() == (key, value),
        distinct_keys(final(entries)@),
{
    admit(entries, key, value, 128);
}

/// Evicting a plan removes exactly the least recently used one.
fn eviction_releases_the_oldest_entry<V>(entries: &mut Vec<(SharedPlanKey, V)>) -> (out: Option<(SharedPlanKey, V)>)
    requires old(entries)@.len() > 0,
    ensures out == Some(old(entries)@[0]), final(entries)@.len() == old(entries)@.len() - 1,
{
    evict_oldest(entries)
}

/// Using a plan moves it to the back without losing or adding an entry.
fn using_a_plan_keeps_every_entry<V>(entries: &mut Vec<(SharedPlanKey, V)>, index: usize)
    requires index < old(entries)@.len(),
    ensures
        final(entries)@.len() == old(entries)@.len(),
        final(entries)@.last() == old(entries)@[index as int],
{
    touch(entries, index);
}

}
