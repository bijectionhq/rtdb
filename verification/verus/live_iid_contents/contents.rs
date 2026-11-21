use crate::production::{chunk_end, contiguous, contiguous_starts_are_ordered, last_started_chunk, ChunkLength};
use vstd::prelude::*;

verus! {
type Iid = [u8; 16];
type IidChunk = (u32, Vec<Iid>);

pub(crate) open spec fn chunk_size() -> ChunkLength<Vec<Iid>> {
    |rows: Vec<Iid>| rows@.len()
}

pub(crate) open spec fn owns(chunks: Seq<IidChunk>, row: u32, index: int, offset: int) -> bool {
    0 <= index < chunks.len()
        && 0 <= offset < chunks[index].1@.len()
        && chunks[index].0 as int + offset == row as int
}

pub(crate) open spec fn retained_iid(chunks: Seq<IidChunk>, row: u32) -> Iid {
    let (index, offset) = choose |index: int, offset: int| #[trigger] owns(chunks, row, index, offset);
    chunks[index].1@[offset]
}

pub(crate) fn locate_iid(chunks: &[IidChunk], rows: u32, row: u32)
    -> (at: (usize, u32, Iid))
    requires contiguous(chunks@, rows, chunk_size()), row < rows,
    ensures
        owns(chunks@, row, at.0 as int, at.1 as int),
        at.2 == chunks@[at.0 as int].1@[at.1 as int],
        at.2 == retained_iid(chunks@, row),
        forall |other: int, offset: int| #[trigger] owns(chunks@, row, other, offset) ==>
            other == at.0 as int && offset == at.1 as int,
{
    proof { contiguous_starts_are_ordered(chunks@, rows, chunk_size()); }
    let owner = last_started_chunk(chunks, row);
    if owner.is_none() {
        assert(chunks@.len() > 0);
        assert(chunks@[0].0 == 0);
        assert(false);
    }
    let (index, offset) = owner.unwrap();
    proof {
        let here = chunks@[index as int];
        if index + 1 < chunks@.len() {
            assert(chunk_end(here, chunk_size()) == chunks@[index as int + 1].0 as int);
        } else {
            assert(here == chunks@.last());
        }
        assert(row < chunk_end(here, chunk_size()));
        assert(owns(chunks@, row, index as int, offset as int));
        assert forall |other: int, cell: int| #[trigger] owns(chunks@, row, other, cell)
            implies other == index as int && cell == offset as int by {
            if other < index {
                assert(chunk_end(chunks@[other], chunk_size()) == chunks@[other + 1].0 as int);
                assert(chunks@[other + 1].0 <= row);
                assert(false);
            } else if other > index {
                assert(row < chunks@[other].0);
                assert(false);
            }
        }
        let (selected, cell) = choose |selected: int, cell: int|
            #[trigger] owns(chunks@, row, selected, cell);
        assert(selected == index as int && cell == offset as int);
    }
    (index, offset, chunks[index].1[offset as usize])
}

pub(crate) fn append_keeps_a_retained_iid(
    chunks: &mut Vec<IidChunk>, rows: u32, row: u32, batch: Vec<Iid>,
) -> (observed: (Iid, Option<Iid>))
    requires
        contiguous(old(chunks)@, rows, chunk_size()), row < rows,
        batch@.len() <= u32::MAX,
    ensures
        observed.0 == retained_iid(old(chunks)@, row),
        (observed.1 is Some) == (rows as int + batch@.len() <= u32::MAX),
        observed.1 is Some ==> observed.1->Some_0 == retained_iid(final(chunks)@, row),
        retained_iid(final(chunks)@, row) == retained_iid(old(chunks)@, row),
{
    let (index, offset, before) = locate_iid(chunks.as_slice(), rows, row);
    let ghost previous = chunks@;
    let count = batch.len() as u32;
    let total = #[verus_spec(with Ghost(chunk_size()))]
        crate::production::append_chunk(chunks, rows, count, batch);
    if let Some(total) = total {
        let (later_index, later_offset, after) = locate_iid(chunks.as_slice(), total, row);
        proof {
            assert(chunks@[index as int] == previous[index as int]);
            assert(owns(chunks@, row, index as int, offset as int));
            assert(later_index == index && later_offset == offset);
        }
        (before, Some(after))
    } else {
        (before, None)
    }
}

pub(crate) fn joining_tail_keeps_a_retained_iid(
    chunks: &mut Vec<IidChunk>, rows: u32, row: u32, replacement: Vec<Iid>,
) -> (observed: (Iid, Iid))
    requires
        old(chunks)@.len() >= 2,
        contiguous(old(chunks)@, rows, chunk_size()), row < rows,
        replacement@ == old(chunks)@[old(chunks)@.len() - 2].1@ + old(chunks)@.last().1@,
    ensures
        observed.0 == retained_iid(old(chunks)@, row),
        observed.1 == retained_iid(final(chunks)@, row),
        retained_iid(final(chunks)@, row) == retained_iid(old(chunks)@, row),
{
    let (index, offset, before) = locate_iid(chunks.as_slice(), rows, row);
    let ghost previous = chunks@;
    let ghost replacement_cells = replacement@;
    let joined_index = chunks.len() - 2;
    { #[verus_spec(with Ghost((rows, chunk_size())))]
        crate::production::replace_last_two(chunks, replacement);
    }
    let (later_index, later_offset, after) = locate_iid(chunks.as_slice(), rows, row);
    proof {
        if index < joined_index {
            assert(chunks@[index as int] == previous[index as int]);
            assert(owns(chunks@, row, index as int, offset as int));
            assert(later_index == index && later_offset == offset);
        } else {
            let left = previous[joined_index as int];
            let right = previous.last();
            assert(chunk_end(left, chunk_size()) == right.0);
            assert(chunks@[joined_index as int].1@ == replacement_cells);
            if index == joined_index {
                assert(owns(chunks@, row, joined_index as int, offset as int));
                assert(later_index == joined_index && later_offset == offset);
                assert(replacement_cells[offset as int] == left.1@[offset as int]);
            } else {
                assert(index == joined_index + 1);
                let joined_offset = left.1@.len() + offset as int;
                assert(owns(chunks@, row, joined_index as int, joined_offset));
                assert(later_index == joined_index && later_offset as int == joined_offset);
                assert(replacement_cells[joined_offset] == right.1@[offset as int]);
            }
        }
    }
    (before, after)
}
}
