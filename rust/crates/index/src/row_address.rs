#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {
    pub(crate) open spec fn ordered_starts<T>(chunks: Seq<(u32, T)>) -> bool {
        forall |i: int, j: int| 0 <= i < j < chunks.len() ==>
            (#[trigger] chunks[i]).0 <= (#[trigger] chunks[j]).0
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

#[cfg(test)]
mod tests {
    use super::last_started_chunk;

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
}
