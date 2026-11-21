#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {
    broadcast use vstd::seq_lib::group_seq_properties;

    pub(crate) type ChunkLength<T> = spec_fn(T) -> nat;

    pub(crate) open spec fn ordered_starts<T>(chunks: Seq<(u32, T)>) -> bool {
        forall |i: int, j: int| 0 <= i < j < chunks.len() ==>
            (#[trigger] chunks[i]).0 <= (#[trigger] chunks[j]).0
    }

    pub(crate) open spec fn chunk_end<T>(chunk: (u32, T), length: ChunkLength<T>) -> int {
        chunk.0 as int + length(chunk.1)
    }

    pub(crate) open spec fn contiguous<T>(
        chunks: Seq<(u32, T)>, rows: u32, length: ChunkLength<T>,
    ) -> bool {
        if chunks.len() == 0 {
            rows == 0
        } else {
            &&& chunks[0].0 == 0
            &&& (forall |index: int| 0 <= index < chunks.len() - 1 ==>
                chunk_end(#[trigger] chunks[index], length) == chunks[index + 1].0 as int)
            &&& chunk_end(chunks.last(), length) == rows as int
        }
    }

    proof fn starts_ordered_between<T>(
        chunks: Seq<(u32, T)>, rows: u32, length: ChunkLength<T>, left: int, right: int,
    )
        requires contiguous(chunks, rows, length), 0 <= left <= right < chunks.len(),
        ensures chunks[left].0 <= chunks[right].0,
        decreases right - left,
    {
        if left < right {
            starts_ordered_between(chunks, rows, length, left, right - 1);
            assert(chunk_end(chunks[right - 1], length) == chunks[right].0 as int);
        }
    }

    pub(crate) proof fn contiguous_starts_are_ordered<T>(
        chunks: Seq<(u32, T)>, rows: u32, length: ChunkLength<T>,
    )
        requires contiguous(chunks, rows, length),
        ensures ordered_starts(chunks),
    {
        assert forall |left: int, right: int| 0 <= left < right < chunks.len() implies
            (#[trigger] chunks[left]).0 <= (#[trigger] chunks[right]).0 by {
            starts_ordered_between(chunks, rows, length, left, right);
        }
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(address =>
    requires ordered_starts(chunks@),
    ensures match address {
        None => forall |i: int| 0 <= i < chunks@.len() ==> row < (#[trigger] chunks@[i]).0,
        Some((index, offset)) => {
            &&& index < chunks@.len()
            &&& chunks@[index as int].0 as int + offset as int == row as int
            &&& forall |i: int| 0 <= i <= index ==> (#[trigger] chunks@[i]).0 <= row
            &&& forall |i: int| index < i < chunks@.len() ==> row < (#[trigger] chunks@[i]).0
        },
    },
))]
pub(crate) fn last_started_chunk<T>(chunks: &[(u32, T)], row: u32) -> Option<(usize, u32)> {
    let mut low = 0;
    let mut high = chunks.len();
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            low <= high <= chunks@.len(),
            ordered_starts(chunks@),
            forall |i: int| 0 <= i < low ==> (#[trigger] chunks@[i]).0 <= row,
            forall |i: int| high <= i < chunks@.len() ==> row < (#[trigger] chunks@[i]).0,
        decreases high - low,
    ))]
    while low < high {
        let middle = low + (high - low) / 2;
        if chunks[middle].0 <= row {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    if low == 0 {
        return None;
    }
    let index = low - 1;
    Some((index, row - chunks[index].0))
}

#[cfg_attr(verus_keep_ghost, verus_spec(total =>
    with Ghost(length): Ghost<ChunkLength<T>>
    requires
        contiguous(old(chunks)@, rows, length),
        length(batch) == count,
    ensures
        total is Some <==> rows as int + count as int <= u32::MAX as int,
        total is Some ==> {
            &&& total->Some_0 as int == rows as int + count as int
            &&& final(chunks)@ == old(chunks)@.push((rows, batch))
            &&& contiguous(final(chunks)@, total->Some_0, length)
        },
        total is None ==> final(chunks)@ == old(chunks)@,
))]
pub(crate) fn append_chunk<T>(chunks: &mut Vec<(u32, T)>, rows: u32, count: u32, batch: T) -> Option<u32> {
    let Some(total) = rows.checked_add(count) else {
        return None;
    };
    #[cfg(verus_keep_ghost)]
    proof_decl! { let ghost old_chunks = chunks@; }
    chunks.push((rows, batch));
    #[cfg(verus_keep_ghost)]
    proof! {
        assert forall |index: int| 0 <= index < chunks@.len() - 1 implies
            chunk_end(#[trigger] chunks@[index], length) == chunks@[index + 1].0 as int by {
            if index + 1 < old_chunks.len() {
                assert(chunk_end(old_chunks[index], length) == old_chunks[index + 1].0 as int);
            } else {
                assert(index + 1 == old_chunks.len());
                assert(old_chunks[index] == old_chunks.last());
            }
        }
    }
    Some(total)
}

#[cfg(verus_keep_ghost)]
verus! {
    pub(crate) type TailLayout<T> = (u32, ChunkLength<T>);
}

#[cfg_attr(verus_keep_ghost, verus_spec(
    with Ghost(layout): Ghost<TailLayout<T>>
    requires
        old(chunks)@.len() >= 2,
        contiguous(old(chunks)@, layout.0, layout.1),
        (layout.1)(replacement) ==
            (layout.1)(old(chunks)@[old(chunks)@.len() - 2].1) + (layout.1)(old(chunks)@.last().1),
    ensures
        final(chunks)@ == old(chunks)@.take(old(chunks)@.len() - 2).push((
            old(chunks)@[old(chunks)@.len() - 2].0, replacement,
        )),
        contiguous(final(chunks)@, layout.0, layout.1),
))]
pub(crate) fn replace_last_two<T>(chunks: &mut Vec<(u32, T)>, replacement: T) {
    let index = chunks.len() - 2;
    let start = chunks[index].0;
    #[cfg(verus_keep_ghost)]
    proof_decl! { let ghost old_chunks = chunks@; }
    chunks.truncate(index);
    chunks.push((start, replacement));
    #[cfg(verus_keep_ghost)]
    proof! {
        let length = layout.1;
        assert(chunk_end(old_chunks[index as int], length) == old_chunks[index as int + 1].0 as int);
        assert(chunk_end(old_chunks[index as int + 1], length) == layout.0 as int);
        assert forall |prior: int| 0 <= prior < chunks@.len() - 1 implies
            chunk_end(#[trigger] chunks@[prior], length) == chunks@[prior + 1].0 as int by {
            assert(chunks@[prior] == old_chunks[prior]);
            if prior + 1 < index {
                assert(chunks@[prior + 1] == old_chunks[prior + 1]);
            } else {
                assert(prior + 1 == index);
                assert(chunks@[prior + 1].0 == old_chunks[prior + 1].0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{append_chunk, last_started_chunk, replace_last_two};

    #[test]
    fn search_selects_the_last_start_at_or_before_the_requested_row() {
        let upper = u32::MAX;
        let starts = [(1, ()), (1, ()), (3, ()), (3, ()), (upper - 1, ()), (upper, ())];
        for row in [0, 1, 2, 3, 4, upper - 2, upper - 1, upper] {
            for count in 0..=starts.len() {
                let chunks = &starts[..count];
                let expected = chunks.iter().enumerate().rev().find(|(_, (start, _))| *start <= row)
                    .map(|(index, (start, _))| (index, row - start));
                assert_eq!(last_started_chunk(chunks, row), expected);
            }
        }
    }

    #[test]
    fn appending_and_joining_chunks_keep_their_expected_starts() {
        let mut chunks = vec![(0, 2), (2, 0)];
        assert_eq!(append_chunk(&mut chunks, 2, 3, 3), Some(5));
        assert_eq!(chunks, [(0, 2), (2, 0), (2, 3)]);
        replace_last_two(&mut chunks, 3);
        assert_eq!(chunks, [(0, 2), (2, 3)]);
        assert_eq!(last_started_chunk(&chunks, 4), Some((1, 2)));
    }

    #[test]
    fn overflowing_an_append_keeps_the_original_chunks() {
        let mut chunks = vec![(0, u32::MAX)];
        assert_eq!(append_chunk(&mut chunks, u32::MAX, 1, 1), None);
        assert_eq!(chunks, [(0, u32::MAX)]);
    }
}
