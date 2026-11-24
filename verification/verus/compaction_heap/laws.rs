use crate::production::{Head, Key, from_pages, initialized, minimum, ordered_from, ordered_keys,
    ordered_pages, ordered_rows, precedes, rank, readable, readable_lengths, remaining_heads,
    take_minimum, valid_heads};
use vstd::{multiset::{Multiset, axiom_multiset_sub, lemma_insert_other_elements_unchanged}, prelude::*};

verus! {

// LAW COMP-1-HEAP-SIGNED-ORDER.
pub fn an_equal_iid_prefers_the_later_system_time(iid: u128, earlier: i64, later: i64)
    requires earlier < later,
{
    let chosen = precedes((iid, later, 0, 0, 0, 0), (iid, earlier, i128::MAX, 0, 0, 0));
    let tie = precedes((iid, later, 0, 0, 0, 0), (iid, later, 0, 0, 0, 0));
    assert(chosen && !tie);
}

pub fn equal_commit_times_prefer_the_later_assertion(iid: u128, time: i64, earlier: i128, later: i128)
    requires earlier < later,
{
    let chosen = precedes((iid, time, later, 1, 1, 1), (iid, time, earlier, 0, 0, 0));
    let reverse = precedes((iid, time, earlier, 0, 0, 0), (iid, time, later, 1, 1, 1));
    assert(chosen && !reverse);
}

pub fn equal_assertions_prefer_the_earlier_scan_source(iid: u128, time: i64, position: i128, first: usize, last: usize)
    requires first < last,
{
    let chosen = precedes((iid, time, position, first, usize::MAX, usize::MAX), (iid, time, position, last, 0, 0));
    let reverse = precedes((iid, time, position, last, 0, 0), (iid, time, position, first, usize::MAX, usize::MAX));
    assert(chosen && !reverse);
}

// LAW COMP-1-HEAP-INITIAL-HEADS.
pub fn every_nonempty_page_contributes_exactly_one_head<L: Fn(usize) -> usize, K: Fn(Head) -> Key + Copy>(
    count: usize, length_of: L, key_of: K, pages: Ghost<Seq<Seq<Key>>>,
)
    requires
        count == pages@.len(), readable_lengths(length_of, pages@), readable(key_of, pages@),
        ordered_pages(pages@, pages@.len() as int),
{
    let heads = from_pages(count, length_of, key_of, pages).unwrap();
    assert(initialized(heads@, pages@, count as int) && minimum(heads@, pages@));
}

// LAW COMP-1-HEAP-EMITTED-COORDINATE.
pub fn extraction_reports_the_selected_occurrence<K: Fn(Head) -> Key + Copy>(heads: &mut Vec<Head>,
    page_length: usize, key_of: K, pages: Ghost<Seq<Seq<Key>>>,
)
    requires
        old(heads)@.len() > 0, valid_heads(old(heads)@, pages@), readable(key_of, pages@),
        ordered_from(old(heads)@, pages@, 0), page_length == pages@[old(heads)@[0].0 as int].len(),
{
    let ghost original = heads@;
    let emitted = take_minimum(heads, page_length, key_of, pages);
    assert(emitted == original[0]);
}

proof fn remaining_coordinate_came_from_a_page(original: Seq<Head>, page_length: nat, remaining: Head)
    requires
        original.len() > 0, original[0].1 < page_length,
        remaining_heads(original, page_length).count(remaining) > 0,
    ensures original.contains(remaining) || (
        original[0].1 as int + 1 < page_length && remaining == (original[0].0, (original[0].1 as int + 1) as usize)
    ),
{
    let selected = original[0];
    let next = (selected.0, (selected.1 as int + 1) as usize);
    if selected.1 as int + 1 >= page_length || remaining != next {
        let bag = original.to_multiset();
        let other = bag.remove(selected);
        if selected.1 as int + 1 < page_length {
            lemma_insert_other_elements_unchanged(other, next, remaining);
        }
        assert(other.count(remaining) > 0);
        axiom_multiset_sub(bag, Multiset::singleton(selected), remaining);
        original.to_multiset_ensures();
        assert(bag.count(remaining) > 0);
    }
}

proof fn old_head_is_no_earlier(original: Seq<Head>, pages: Seq<Seq<Key>>, remaining: Head)
    requires minimum(original, pages), original.contains(remaining),
    ensures ordered_keys(rank(pages, original[0]), rank(pages, remaining)),
{
    let at = choose|at: int| 0 <= at < original.len() && original[at] == remaining;
    assert(ordered_keys(rank(pages, original[0]), rank(pages, original[at])));
}

proof fn adjacent_rows_are_ordered(page: Seq<Key>, row: int)
    requires 0 <= row && row + 1 < page.len(), ordered_rows(page, page.len() as int),
    ensures ordered_keys(page[row], page[row + 1]),
{
}

// LAW COMP-1-HEAP-EMISSIONS-ORDERED.
pub fn every_remaining_head_is_at_or_after_the_minimum<K: Fn(Head) -> Key + Copy>(heads: &mut Vec<Head>,
    page_length: usize, key_of: K, pages: Ghost<Seq<Seq<Key>>>,
)
    requires
        old(heads)@.len() > 0, valid_heads(old(heads)@, pages@), readable(key_of, pages@),
        ordered_from(old(heads)@, pages@, 0), page_length == pages@[old(heads)@[0].0 as int].len(),
        ordered_pages(pages@, pages@.len() as int),
{
    let ghost original = heads@;
    let emitted = take_minimum(heads, page_length, key_of, pages);
    proof {
        assert(emitted == original[0]);
        heads@.to_multiset_ensures();
        assert forall|i: int| 0 <= i < heads@.len()
            implies ordered_keys(rank(pages@, emitted), rank(pages@, #[trigger] heads@[i])) by {
            let remaining = heads@[i];
            assert(heads@.contains(remaining));
            remaining_coordinate_came_from_a_page(original, page_length as nat, remaining);
            if original.contains(remaining) {
                old_head_is_no_earlier(original, pages@, remaining);
            } else {
                let page = pages@[emitted.0 as int];
                assert(ordered_rows(page, page.len() as int));
                adjacent_rows_are_ordered(page, emitted.1 as int);
            }
        }
    }
}

}
