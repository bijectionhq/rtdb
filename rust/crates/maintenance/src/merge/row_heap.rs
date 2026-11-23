#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::multiset::Multiset;
#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

pub(super) type Head = (usize, usize);
pub(super) type Key = (u128, i64, u32);

#[cfg(verus_keep_ghost)]
verus! {
    pub open spec fn ordered_keys(left: Key, right: Key) -> bool {
        left.0 < right.0
            || (left.0 == right.0 && (left.1 > right.1 || (left.1 == right.1 && left.2 >= right.2)))
    }

    pub open spec fn valid_head(pages: Seq<Seq<Key>>, head: Head) -> bool {
        head.0 < pages.len() && head.1 < pages[head.0 as int].len()
    }

    pub open spec fn rank(pages: Seq<Seq<Key>>, head: Head) -> Key {
        pages[head.0 as int][head.1 as int]
    }

    pub open spec fn readable<K: Fn(Head) -> Key>(key_of: K, pages: Seq<Seq<Key>>) -> bool {
        &&& forall|head: Head| valid_head(pages, head) ==> #[trigger] call_requires(key_of, (head,))
        &&& forall|head: Head, result: Key| valid_head(pages, head) && #[trigger] call_ensures(key_of, (head,), result)
            ==> result == rank(pages, head)
    }

    pub open spec fn readable_lengths<L: Fn(usize) -> usize>(length_of: L, pages: Seq<Seq<Key>>) -> bool {
        &&& forall|stream: usize| stream < pages.len() ==> #[trigger] call_requires(length_of, (stream,))
        &&& forall|stream: usize, length: usize| stream < pages.len() && #[trigger] call_ensures(length_of, (stream,), length)
            ==> length == pages[stream as int].len()
    }

    pub open spec fn ordered_rows(page: Seq<Key>, end: int) -> bool {
        forall|i: int| 0 < i < end && i < page.len() ==> ordered_keys(page[i - 1], #[trigger] page[i])
    }

    pub open spec fn ordered_pages(pages: Seq<Seq<Key>>, end: int) -> bool {
        forall|stream: int| 0 <= stream < end ==> ordered_rows(#[trigger] pages[stream], pages[stream].len() as int)
    }

    pub open spec fn initialized(heads: Seq<Head>, pages: Seq<Seq<Key>>, end: int) -> bool {
        forall|head: Head| #[trigger] heads.to_multiset().count(head) ==
            if head.0 < end && pages[head.0 as int].len() > 0 && head.1 == 0 { 1nat } else { 0nat }
    }

    pub open spec fn valid_heads(heads: Seq<Head>, pages: Seq<Seq<Key>>) -> bool {
        forall|i: int| 0 <= i < heads.len() ==> valid_head(pages, #[trigger] heads[i])
    }

    pub open spec fn parent(child: int) -> int { (child - 1) / 2 }

    pub open spec fn ordered_from(heads: Seq<Head>, pages: Seq<Seq<Key>>, start: int) -> bool {
        forall|child: int| 0 < child < heads.len() && start <= parent(child)
            ==> ordered_keys(rank(pages, heads[parent(child)]), rank(pages, #[trigger] heads[child]))
    }

    pub open spec fn minimum(heads: Seq<Head>, pages: Seq<Seq<Key>>) -> bool {
        forall|i: int| 0 <= i < heads.len()
            ==> ordered_keys(rank(pages, heads[0]), rank(pages, #[trigger] heads[i]))
    }

    spec fn ordered_except(heads: Seq<Head>, pages: Seq<Seq<Key>>, start: int, hole: int) -> bool {
        forall|child: int| 0 < child < heads.len() && start <= parent(child) && parent(child) != hole
            ==> ordered_keys(rank(pages, heads[parent(child)]), rank(pages, #[trigger] heads[child]))
    }

    spec fn upper_bound_at_hole(heads: Seq<Head>, pages: Seq<Seq<Key>>, start: int, hole: int) -> bool {
        hole == start || forall|child: int| 0 < child < heads.len() && parent(child) == hole
            ==> ordered_keys(rank(pages, heads[parent(hole)]), rank(pages, #[trigger] heads[child]))
    }

    spec fn swapped(heads: Seq<Head>, left: int, right: int) -> Seq<Head> {
        heads.update(left, heads[right]).update(right, heads[left])
    }

    proof fn swap_keeps_occurrences(heads: Seq<Head>, left: int, right: int)
        requires 0 <= left < right < heads.len(),
        ensures swapped(heads, left, right).to_multiset() =~= heads.to_multiset(),
    {
        broadcast use vstd::multiset::group_multiset_properties;
        let moved = swapped(heads, left, right);
        let left_head = heads[left];
        let right_head = heads[right];
        let original_rest = heads.remove(right);
        let moved_rest = moved.remove(right);
        assert(moved_rest.remove(left) =~= original_rest.remove(left));
        heads.to_multiset_ensures();
        moved.to_multiset_ensures();
        original_rest.to_multiset_ensures();
        moved_rest.to_multiset_ensures();
        assert(original_rest.to_multiset() =~= heads.to_multiset().remove(right_head));
        assert(moved_rest.to_multiset() =~= moved.to_multiset().remove(left_head));
        assert(original_rest[left] == left_head && moved_rest[left] == right_head);
        assert(original_rest.remove(left).to_multiset() =~= original_rest.to_multiset().remove(left_head));
        assert(moved_rest.remove(left).to_multiset() =~= moved_rest.to_multiset().remove(right_head));
        assert(heads.contains(right_head));
        assert(moved.contains(left_head));
        assert(original_rest.contains(left_head));
        assert(moved_rest.contains(right_head));
        assert forall|head: Head| #[trigger] moved.to_multiset().count(head) == heads.to_multiset().count(head) by {
            assert(original_rest.to_multiset().count(left_head) > 0);
            assert(moved_rest.to_multiset().count(right_head) > 0);
            assert(heads.to_multiset().count(right_head) > 0);
            assert(moved.to_multiset().count(left_head) > 0);
            assert(original_rest.remove(left).to_multiset().count(head) == moved_rest.remove(left).to_multiset().count(head));
        }
    }

    proof fn move_hole(heads: Seq<Head>, pages: Seq<Seq<Key>>, start: int, hole: int, child: int)
        requires
            0 <= start <= hole < child < heads.len(),
            parent(child) == hole,
            ordered_except(heads, pages, start, hole),
            upper_bound_at_hole(heads, pages, start, hole),
            ordered_keys(rank(pages, heads[child]), rank(pages, heads[hole])),
            forall|sibling: int| 0 < sibling < heads.len() && parent(sibling) == hole
                ==> ordered_keys(rank(pages, heads[child]), rank(pages, #[trigger] heads[sibling])),
        ensures
            ordered_except(swapped(heads, hole, child), pages, start, child),
            upper_bound_at_hole(swapped(heads, hole, child), pages, start, child),
    {
        let moved = swapped(heads, hole, child);
        assert forall|i: int| 0 < i < moved.len() && start <= parent(i) && parent(i) != child
            implies ordered_keys(rank(pages, moved[parent(i)]), rank(pages, #[trigger] moved[i])) by {
            if i == hole {
                assert(ordered_keys(rank(pages, heads[parent(hole)]), rank(pages, heads[child])));
            } else if parent(i) == hole && i != child {
                assert(ordered_keys(rank(pages, heads[child]), rank(pages, heads[i])));
            } else if parent(i) != hole {
                assert(ordered_keys(rank(pages, heads[parent(i)]), rank(pages, heads[i])));
            }
        }
        assert forall|i: int| 0 < i < moved.len() && parent(i) == child
            implies ordered_keys(rank(pages, moved[hole]), rank(pages, #[trigger] moved[i])) by {
            assert(ordered_keys(rank(pages, heads[child]), rank(pages, heads[i])));
        }
    }

    pub proof fn root_is_no_later(heads: Seq<Head>, pages: Seq<Seq<Key>>, index: int)
        requires ordered_from(heads, pages, 0), 0 <= index < heads.len(),
        ensures ordered_keys(rank(pages, heads[0]), rank(pages, heads[index])),
        decreases index,
    {
        if index > 0 {
            root_is_no_later(heads, pages, parent(index));
            assert(ordered_keys(rank(pages, heads[parent(index)]), rank(pages, heads[index])));
        }
    }

    proof fn heap_has_minimum(heads: Seq<Head>, pages: Seq<Seq<Key>>)
        requires ordered_from(heads, pages, 0),
        ensures minimum(heads, pages),
    {
        assert forall|i: int| 0 <= i < heads.len()
            implies ordered_keys(rank(pages, heads[0]), rank(pages, #[trigger] heads[i])) by {
            root_is_no_later(heads, pages, i);
        }
    }

    pub open spec fn remaining_heads(heads: Seq<Head>, page_length: nat) -> Multiset<Head> {
        let root = heads[0];
        let others = heads.to_multiset().remove(root);
        if root.1 as int + 1 == page_length { others } else { others.insert((root.0, (root.1 + 1) as usize)) }
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == !ordered_keys(right, left),
))]
pub(super) fn precedes(left: Key, right: Key) -> bool {
    left.0 < right.0
        || (left.0 == right.0 && (left.1 > right.1 || (left.1 == right.1 && left.2 > right.2)))
}

#[cfg_attr(verus_keep_ghost, verus_spec(
    requires
        start < old(heads)@.len(),
        readable(key_of, pages@),
        valid_heads(old(heads)@, pages@),
        ordered_from(old(heads)@, pages@, start as int + 1),
    ensures
        final(heads)@.len() == old(heads)@.len(),
        final(heads)@.to_multiset() =~= old(heads)@.to_multiset(),
        valid_heads(final(heads)@, pages@),
        ordered_from(final(heads)@, pages@, start as int),
))]
fn sift<K: Fn(Head) -> Key + Copy>(heads: &mut Vec<Head>, start: usize, key_of: K,
    #[cfg(verus_keep_ghost)] pages: Ghost<Seq<Seq<Key>>>) {
    let node = heads[start];
    let node_key = key_of(node);
    let mut index = start;
    let len = heads.len();
    // Treat the saved node as occupying the hole while smaller children move upward.
    #[cfg(verus_keep_ghost)]
    proof! { assert(heads@.update(index as int, node) =~= heads@); }
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            len == heads@.len() == old(heads)@.len(),
            start <= index < len,
            readable(key_of, pages@),
            node_key == rank(pages@, node),
            valid_heads(heads@.update(index as int, node), pages@),
            ordered_except(heads@.update(index as int, node), pages@, start as int, index as int),
            upper_bound_at_hole(heads@.update(index as int, node), pages@, start as int, index as int),
            heads@.update(index as int, node).to_multiset() =~= old(heads)@.to_multiset(),
        decreases len - index,
    ))]
    while index < len / 2 {
        #[cfg(verus_keep_ghost)]
        proof_decl! { let ghost before = heads@.update(index as int, node); }
        let left = 2 * index + 1;
        let mut child = left;
        let mut child_head = heads[left];
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(child_head == before[left as int]);
            assert(valid_head(pages@, child_head));
        }
        let mut child_key = key_of(child_head);
        if left + 1 < len {
            let right_head = heads[left + 1];
            #[cfg(verus_keep_ghost)]
            proof! {
                assert(right_head == before[left as int + 1]);
                assert(valid_head(pages@, right_head));
            }
            let right_key = key_of(right_head);
            if precedes(right_key, child_key) {
                child = left + 1;
                child_head = right_head;
                child_key = right_key;
            }
        }
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(parent(child as int) == index);
            assert(child_key == rank(pages@, before[child as int]));
            assert forall|sibling: int| 0 < sibling < before.len() && parent(sibling) == index
                implies ordered_keys(child_key, rank(pages@, #[trigger] before[sibling])) by {
                assert(sibling == left || sibling == left + 1);
            }
        }
        if !precedes(child_key, node_key) {
            heads[index] = node;
            #[cfg(verus_keep_ghost)]
            proof! {
                assert(heads@ =~= before);
                assert forall|i: int| 0 < i < heads@.len() && start <= parent(i)
                    implies ordered_keys(rank(pages@, heads@[parent(i)]), rank(pages@, #[trigger] heads@[i])) by {
                    if parent(i) == index {
                        assert(ordered_keys(child_key, rank(pages@, before[i])));
                    } else {
                        assert(ordered_keys(rank(pages@, before[parent(i)]), rank(pages@, before[i])));
                    }
                }
            }
            return;
        }
        heads[index] = child_head;
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(heads@.update(child as int, node) =~= swapped(before, index as int, child as int));
            swap_keeps_occurrences(before, index as int, child as int);
            move_hole(before, pages@, start as int, index as int, child as int);
            assert forall|i: int| 0 <= i < heads@.len()
                implies valid_head(pages@, #[trigger] heads@.update(child as int, node)[i]) by {
                let origin = if i == index { child as int } else if i == child { index as int } else { i };
                assert(valid_head(pages@, before[origin]));
            }
        }
        index = child;
    }
    heads[index] = node;
    #[cfg(verus_keep_ghost)]
    proof! {
        assert forall|i: int| 0 < i < len && start <= parent(i)
            implies ordered_keys(rank(pages@, heads@[parent(i)]), rank(pages@, #[trigger] heads@[i])) by {
            assert(parent(i) < len / 2);
        }
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(
    requires readable(key_of, pages@), valid_heads(old(heads)@, pages@),
    ensures
        final(heads)@.len() == old(heads)@.len(),
        final(heads)@.to_multiset() =~= old(heads)@.to_multiset(),
        valid_heads(final(heads)@, pages@),
        ordered_from(final(heads)@, pages@, 0),
        minimum(final(heads)@, pages@),
))]
fn heapify<K: Fn(Head) -> Key + Copy>(heads: &mut Vec<Head>, key_of: K,
    #[cfg(verus_keep_ghost)] pages: Ghost<Seq<Seq<Key>>>) {
    let mut start = heads.len() / 2;
    #[cfg(verus_keep_ghost)]
    proof! {
        assert forall|i: int| 0 < i < heads@.len() implies parent(i) < start by {};
    }
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            start <= heads@.len(),
            heads@.len() == old(heads)@.len(),
            heads@.to_multiset() =~= old(heads)@.to_multiset(),
            readable(key_of, pages@),
            valid_heads(heads@, pages@),
            ordered_from(heads@, pages@, start as int),
        decreases start,
    ))]
    while start > 0 {
        start -= 1;
        sift(heads, start, key_of, #[cfg(verus_keep_ghost)] pages);
    }
    #[cfg(verus_keep_ghost)]
    proof! { heap_has_minimum(heads@, pages@); }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires page_count == pages@.len(), readable_lengths(length_of, pages@), readable(key_of, pages@),
    ensures
        out.is_some() == ordered_pages(pages@, pages@.len() as int),
        out.is_some() ==> initialized(out.unwrap()@, pages@, pages@.len() as int)
            && valid_heads(out.unwrap()@, pages@)
            && ordered_from(out.unwrap()@, pages@, 0)
            && minimum(out.unwrap()@, pages@),
))]
pub(super) fn from_pages<L: Fn(usize) -> usize, K: Fn(Head) -> Key + Copy>(page_count: usize,
    length_of: L, key_of: K, #[cfg(verus_keep_ghost)] pages: Ghost<Seq<Seq<Key>>>) -> Option<Vec<Head>> {
    let mut heads = Vec::new();
    let mut stream = 0;
    #[cfg(verus_keep_ghost)]
    proof! { heads@.to_multiset_ensures(); }
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            page_count == pages@.len(), stream <= page_count,
            readable_lengths(length_of, pages@), readable(key_of, pages@),
            initialized(heads@, pages@, stream as int), valid_heads(heads@, pages@),
            ordered_pages(pages@, stream as int),
        decreases page_count - stream,
    ))]
    while stream < page_count {
        let length = length_of(stream);
        let mut row = 1;
        #[cfg_attr(verus_keep_ghost, verus_spec(
            invariant
                stream < page_count == pages@.len(),
                length == pages@[stream as int].len(),
                1 <= row && (row <= length || length == 0),
                readable(key_of, pages@), ordered_rows(pages@[stream as int], row as int),
            decreases length as int - row,
        ))]
        while row < length {
            #[cfg(verus_keep_ghost)]
            proof! {
                assert(valid_head(pages@, (stream, row)));
                assert(valid_head(pages@, (stream, (row - 1) as usize)));
            }
            let previous = key_of((stream, row - 1));
            let current = key_of((stream, row));
            if precedes(current, previous) {
                #[cfg(verus_keep_ghost)]
                proof! {
                    assert(!ordered_keys(pages@[stream as int][row as int - 1], pages@[stream as int][row as int]));
                    assert(!ordered_rows(pages@[stream as int], length as int));
                }
                return None;
            }
            #[cfg(verus_keep_ghost)]
            proof! {
                assert forall|i: int| 0 < i < row as int + 1 && i < length
                    implies ordered_keys(pages@[stream as int][i - 1], #[trigger] pages@[stream as int][i]) by {
                    if i < row { assert(ordered_keys(pages@[stream as int][i - 1], pages@[stream as int][i])); }
                }
            }
            row += 1;
        }
        #[cfg(verus_keep_ghost)]
        proof_decl! { let ghost previous_heads = heads@; }
        if length != 0 { heads.push((stream, 0)); }
        #[cfg(verus_keep_ghost)]
        proof! {
            broadcast use vstd::multiset::group_multiset_properties;
            previous_heads.to_multiset_ensures();
            assert forall|h: Head| #[trigger] heads@.to_multiset().count(h) ==
                if h.0 <= stream && pages@[h.0 as int].len() > 0 && h.1 == 0 { 1nat } else { 0nat } by {
                assert(previous_heads.to_multiset().count(h) ==
                    if h.0 < stream && pages@[h.0 as int].len() > 0 && h.1 == 0 { 1nat } else { 0nat });
            }
            assert forall|i: int| 0 <= i < heads@.len() implies valid_head(pages@, #[trigger] heads@[i]) by {
                if i < previous_heads.len() { assert(valid_head(pages@, previous_heads[i])); }
            }
            assert forall|s: int| 0 <= s <= stream
                implies ordered_rows(#[trigger] pages@[s], pages@[s].len() as int) by {
                if s == stream { assert(ordered_rows(pages@[s], length as int)); }
            }
        }
        stream += 1;
    }
    heapify(&mut heads, key_of, #[cfg(verus_keep_ghost)] pages);
    Some(heads)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires
        old(heads)@.len() > 0,
        readable(key_of, pages@),
        valid_heads(old(heads)@, pages@),
        ordered_from(old(heads)@, pages@, 0),
        page_length == pages@[old(heads)@[0].0 as int].len(),
    ensures
        out == old(heads)@[0],
        minimum(old(heads)@, pages@),
        final(heads)@.to_multiset() =~= remaining_heads(old(heads)@, page_length as nat),
        valid_heads(final(heads)@, pages@),
        ordered_from(final(heads)@, pages@, 0),
        minimum(final(heads)@, pages@),
))]
pub(super) fn take_minimum<K: Fn(Head) -> Key + Copy>(heads: &mut Vec<Head>, page_length: usize, key_of: K,
    #[cfg(verus_keep_ghost)] pages: Ghost<Seq<Seq<Key>>>) -> Head {
    let result = heads[0];
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(valid_head(pages@, result));
        heap_has_minimum(heads@, pages@);
    }
    let row = result.1 + 1;
    if row == page_length {
        let last = heads.pop().unwrap();
        if !heads.is_empty() {
            heads[0] = last;
            #[cfg(verus_keep_ghost)]
            proof! {
                let original = old(heads)@;
                let last_index = original.len() - 1;
                let moved = swapped(original, 0, last_index);
                assert(heads@ =~= moved.remove(last_index));
                swap_keeps_occurrences(original, 0, last_index);
                moved.to_multiset_ensures();
                assert(heads@.to_multiset() =~= original.to_multiset().remove(result));
            }
        } else {
            #[cfg(verus_keep_ghost)]
            proof! {
                let original = old(heads)@;
                assert(heads@ =~= original.remove(0));
                original.to_multiset_ensures();
            }
        }
    } else {
        let next = (result.0, row);
        heads[0] = next;
        #[cfg(verus_keep_ghost)]
        proof! {
            broadcast use vstd::multiset::group_multiset_properties;
            let original = old(heads)@;
            assert(heads@.remove(0) =~= original.remove(0));
            original.to_multiset_ensures();
            heads@.to_multiset_ensures();
            assert(heads@.contains(next));
            assert(heads@.to_multiset().count(next) > 0);
            assert forall|h: Head| #[trigger] heads@.to_multiset().count(h)
                == original.to_multiset().remove(result).insert(next).count(h) by {
                assert(heads@.to_multiset().remove(next).count(h) == original.remove(0).to_multiset().count(h));
            }
        }
    }
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(heads@.to_multiset() =~= remaining_heads(old(heads)@, page_length as nat));
        assert forall|i: int| 0 <= i < heads@.len()
            implies valid_head(pages@, #[trigger] heads@[i]) by {
            if i != 0 {
                assert(heads@[i] == old(heads)@[i]);
            } else if row == page_length {
                assert(heads@[0] == old(heads)@[old(heads)@.len() - 1]);
            }
        }
        assert forall|i: int| 0 < i < heads@.len() && 0 < parent(i)
            implies ordered_keys(rank(pages@, heads@[parent(i)]), rank(pages@, #[trigger] heads@[i])) by {
            assert(heads@[i] == old(heads)@[i]);
            assert(heads@[parent(i)] == old(heads)@[parent(i)]);
        }
    }
    if !heads.is_empty() {
        sift(heads, 0, key_of, #[cfg(verus_keep_ghost)] pages);
    }
    #[cfg(verus_keep_ghost)]
    proof! { heap_has_minimum(heads@, pages@); }
    result
}
