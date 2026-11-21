use crate::production::{last_started_chunk, ordered_starts};
use vstd::prelude::*;

verus! {

pub(crate) open spec fn end(chunk: (u32, u32)) -> int {
    chunk.0 as int + chunk.1 as int
}

pub(crate) open spec fn contiguous(chunks: Seq<(u32, u32)>, rows: u32) -> bool {
    &&& chunks.len() > 0
    &&& chunks[0].0 == 0
    &&& (forall |i: int| 0 <= i < chunks.len() - 1 ==>
        end(#[trigger] chunks[i]) == chunks[i + 1].0 as int)
    &&& end(chunks.last()) == rows as int
}

pub(crate) fn in_range_row_has_one_owner(chunks: &[(u32, u32)], rows: u32, row: u32)
    -> (owner: Option<(usize, u32)>)
    requires contiguous(chunks@, rows), ordered_starts(chunks@), row < rows,
    ensures
        owner is Some,
        owner is Some ==> {
            let (index, offset) = owner->Some_0;
            &&& index < chunks@.len()
            &&& offset < chunks@[index as int].1
            &&& chunks@[index as int].0 as int + offset as int == row as int
            &&& forall |i: int| 0 <= i < chunks@.len() && i != index ==>
                row < (#[trigger] chunks@[i]).0 || end(chunks@[i]) <= row
        },
{
    let owner = last_started_chunk(chunks, row);
    proof {
        if let Some((index, offset)) = owner {
            let here = chunks@[index as int];
            if index + 1 < chunks@.len() {
                assert(end(here) == chunks@[index as int + 1].0 as int);
            } else {
                assert(here == chunks@.last());
            }
            assert(row < end(here));
            assert(offset < here.1);
            assert forall |i: int| 0 <= i < chunks@.len() && i != index implies
                row < (#[trigger] chunks@[i]).0 || end(chunks@[i]) <= row by {
                if i < index {
                    assert(end(chunks@[i]) == chunks@[i + 1].0 as int);
                    assert(chunks@[i + 1].0 <= row);
                }
            }
        } else {
            assert(row < chunks@[0].0);
        }
    }
    owner
}

}
