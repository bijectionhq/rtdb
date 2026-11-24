#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, PartialEq, Eq))]
pub enum TrieState<At> { Nascent, Live, Garbage { as_of: At } }

#[cfg(verus_keep_ghost)]
verus! {
    pub(crate) open spec fn live_entries<T, At>(inventory: Seq<(T, TrieState<At>)>) -> Seq<T> {
        inventory.filter(|entry: (T, TrieState<At>)| entry.1 is Live)
            .map_values(|entry: (T, TrieState<At>)| entry.0)
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(selected => ensures selected@ == live_entries(inventory@)))]
pub(super) fn select_live<T: Copy, At: Copy>(inventory: &[(T, TrieState<At>)]) -> Vec<T> {
    let mut selected: Vec<T> = Vec::new();
    let mut index = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant index <= inventory@.len(),
            selected@ == live_entries(inventory@.take(index as int)),
        decreases inventory@.len() - index,
    ))]
    while index < inventory.len() {
        let (entry, state) = inventory[index];
        if matches!(state, TrieState::Live) {
            selected.push(entry);
        }
        #[cfg(verus_keep_ghost)]
        proof! {
            let prior = inventory@.take(index as int);
            let current = inventory@[index as int];
            let next = inventory@.take(index + 1);
            let is_live = |entry: (T, TrieState<At>)| entry.1 is Live;
            assert(next =~= prior.push(current));
            prior.lemma_filter_push(current, is_live);
            prior.filter(is_live).lemma_push_map_commute(|entry: (T, TrieState<At>)| entry.0, current);
            assert(selected@ =~= live_entries(next));
        }
        index += 1;
    }
    #[cfg(verus_keep_ghost)]
    proof! { assert(inventory@.take(index as int) =~= inventory@); }
    selected
}
