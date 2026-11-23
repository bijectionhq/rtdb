use crate::production::{chunk_end, contiguous, contiguous_starts_are_ordered, last_started_chunk, ChunkLength};
use vstd::prelude::*;

verus! {

pub(crate) open spec fn payload_size() -> ChunkLength<u32> {
    |count: u32| count as nat
}

pub(crate) fn in_range_row_has_one_owner(chunks: &[(u32, u32)], rows: u32, row: u32)
    -> (owner: Option<(usize, u32)>)
    requires contiguous(chunks@, rows, payload_size()), row < rows,
    ensures
        owner is Some,
        owner is Some ==> {
            let (index, offset) = owner->Some_0;
            &&& index < chunks@.len()
            &&& offset < chunks@[index as int].1
            &&& chunks@[index as int].0 as int + offset as int == row as int
            &&& forall |i: int| 0 <= i < chunks@.len() && i != index ==>
                row < (#[trigger] chunks@[i]).0 || chunk_end(chunks@[i], payload_size()) <= row
        },
{
    proof {
        contiguous_starts_are_ordered(chunks@, rows, payload_size());
    }
    let owner = last_started_chunk(chunks, row);
    proof {
        if let Some((index, offset)) = owner {
            let here = chunks@[index as int];
            if index + 1 < chunks@.len() {
                assert(chunk_end(here, payload_size()) == chunks@[index as int + 1].0 as int);
            } else {
                assert(here == chunks@.last());
            }
            assert(row < chunk_end(here, payload_size()));
            assert(offset < here.1);
            assert forall |i: int| 0 <= i < chunks@.len() && i != index implies
                row < (#[trigger] chunks@[i]).0 || chunk_end(chunks@[i], payload_size()) <= row by {
                if i < index {
                    assert(chunk_end(chunks@[i], payload_size()) == chunks@[i + 1].0 as int);
                    assert(chunks@[i + 1].0 <= row);
                }
            }
        } else {
            assert(row < chunks@[0].0);
        }
    }
    owner
}

pub(crate) fn append_preserves_existing_addresses(
    chunks: &mut Vec<(u32, u32)>, rows: u32, count: u32, row: u32,
)
    requires contiguous(old(chunks)@, rows, payload_size()), row < rows,
{
    let before = in_range_row_has_one_owner(chunks.as_slice(), rows, row);
    let (index, offset) = before.unwrap();
    let ghost old_chunks = chunks@;
    let total = #[verus_spec(with Ghost(payload_size()))]
        crate::production::append_chunk(chunks, rows, count, count);
    if let Some(total) = total {
        let after = in_range_row_has_one_owner(chunks.as_slice(), total, row);
        let (next_index, next_offset) = after.unwrap();
        assert(chunks@[index as int] == old_chunks[index as int]);
        assert(!(row < chunks@[index as int].0 || chunk_end(chunks@[index as int], payload_size()) <= row));
        assert(next_index == index && next_offset == offset);
    }
}

pub(crate) fn joining_tail_preserves_the_existing_row_ordinal(
    chunks: &mut Vec<(u32, u32)>, rows: u32, row: u32,
)
    requires old(chunks)@.len() >= 2, contiguous(old(chunks)@, rows, payload_size()), row < rows,
{
    let before = in_range_row_has_one_owner(chunks.as_slice(), rows, row);
    let (index, offset) = before.unwrap();
    let joined_index = chunks.len() - 2;
    let first = chunks[joined_index];
    let second = chunks[joined_index + 1];
    let ghost old_chunks = chunks@;
    let count = first.1 + second.1;
    { #[verus_spec(with Ghost((rows, payload_size())))]
        crate::production::replace_last_two(chunks, count);
    }
    let after = in_range_row_has_one_owner(chunks.as_slice(), rows, row);
    let (next_index, next_offset) = after.unwrap();
    proof {
        if index < joined_index {
            assert(chunks@[index as int] == old_chunks[index as int]);
            assert(next_index == index && next_offset == offset);
        } else {
            assert(chunks@[joined_index as int] == (first.0, count));
            assert(row >= first.0 && row < chunk_end((first.0, count), payload_size()));
            assert(next_index == joined_index);
            assert(next_offset as int == if index == joined_index {
                offset as int
            } else {
                first.1 as int + offset as int
            });
        }
    }
}

}
