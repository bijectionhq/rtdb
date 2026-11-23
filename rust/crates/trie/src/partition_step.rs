#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::iter::{ExactSizeIteratorSpec, IteratorSpec};

use std::cmp::Ordering;

#[cfg(verus_keep_ghost)]
verus! {
    pub open spec fn digit(byte: int, lane: int) -> int {
        let place = if lane == 0 { 64 } else if lane == 1 { 16 } else if lane == 2 { 4 } else { 1 };
        (byte / place) % 4
    }
}

/// Selects the base-four digit at the given depth in the big-endian IID.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        (out is Some) == (depth < 64),
        out is Some ==> (out->Some_0) < 4 &&
            (out->Some_0) as int == digit(iid@[(depth as int) / 4] as int, (depth as int) % 4),
))]
#[inline]
pub fn base4_bucket(iid: &[u8; 16], depth: usize) -> Option<u8> {
    if depth >= 64 {
        return None;
    }
    let lane = depth % 4;
    let byte = iid[depth / 4];
    let shift = 6 - 2 * lane;
    let bucket = (byte >> shift) & 3;
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(lane < 4);
        if lane == 0 {
            assert(shift == 6);
            assert((byte >> 6usize) & 3u8 == (byte / 64u8) % 4u8) by (bit_vector);
            assert(bucket == (byte / 64u8) % 4u8);
        } else if lane == 1 {
            assert(shift == 4);
            assert((byte >> 4usize) & 3u8 == (byte / 16u8) % 4u8) by (bit_vector);
            assert(bucket == (byte / 16u8) % 4u8);
        } else if lane == 2 {
            assert(shift == 2);
            assert((byte >> 2usize) & 3u8 == (byte / 4u8) % 4u8) by (bit_vector);
            assert(bucket == (byte / 4u8) % 4u8);
        } else {
            assert(shift == 0);
            assert((byte >> 0usize) & 3u8 == byte % 4u8) by (bit_vector);
            assert(bucket == byte % 4u8);
        }
        assert(bucket as int == digit(byte as int, lane as int));
        assert(bucket < 4);
    }
    Some(bucket)
}

#[cfg(verus_keep_ghost)]
verus! {
    pub open spec fn iid_digit(iid: Seq<u8>, level: int) -> int {
        digit(iid[level / 4] as int, level % 4)
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires path@.len() <= 64,
    ensures
        out == Ordering::Equal ==> forall |i: int| 0 <= i < path@.len() ==>
            #[trigger] iid_digit(iid@, i) == path@[i] as int,
        out != Ordering::Equal ==> exists |first: int| 0 <= first < path@.len() &&
            (forall |i: int| 0 <= i < first ==>
                #[trigger] iid_digit(iid@, i) == path@[i] as int) &&
            (match out {
                Ordering::Less => #[trigger] iid_digit(iid@, first) < path@[first] as int,
                Ordering::Greater => iid_digit(iid@, first) > path@[first] as int,
                Ordering::Equal => false,
            }),
))]
pub(crate) fn compare_base4_path(iid: &[u8; 16], path: &[u8]) -> Ordering {
    let mut level = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            level <= path@.len() <= 64,
            forall |i: int| 0 <= i < level ==>
                #[trigger] iid_digit(iid@, i) == path@[i] as int,
        decreases path@.len() - level,
    ))]
    while level < path.len() {
        let bucket = base4_bucket(iid, level).unwrap();
        let expected = path[level];
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(bucket as int == iid_digit(iid@, level as int));
        }
        if bucket < expected {
            return Ordering::Less;
        }
        if bucket > expected {
            return Ordering::Greater;
        }
        level += 1;
    }
    Ordering::Equal
}

#[cfg(verus_keep_ghost)]
verus! {
    pub(crate) struct GroupingInput<E> {
        pub rows: Seq<u32>,
        pub read: spec_fn(u32) -> Result<[u8; 16], E>,
    }

    pub(crate) open spec fn bucket_rows<E>(
        input: GroupingInput<E>, depth: int, count: nat, bucket: int,
    ) -> Seq<u32> {
        input.rows.take(count as int).filter(|row: u32|
            match (input.read)(row) {
                Ok(iid) => iid_digit(iid@, depth) == bucket,
                Err(_) => false,
            },
        )
    }

    proof fn bucket_rows_advance<E>(
        input: GroupingInput<E>, depth: int, count: nat, bucket: int, digit: int,
    )
        requires
            count < input.rows.len(),
            (input.read)(input.rows[count as int]) is Ok,
            digit == iid_digit((input.read)(input.rows[count as int])->Ok_0@, depth),
        ensures
            bucket_rows(input, depth, count + 1, bucket) == if digit == bucket {
                bucket_rows(input, depth, count, bucket).push(input.rows[count as int])
            } else {
                bucket_rows(input, depth, count, bucket)
            },
    {
        let prefix = input.rows.take(count as int + 1);
        input.rows.lemma_take_succ_push(count as int);
        assert(prefix.drop_last() =~= input.rows.take(count as int));
        assert(prefix.last() == input.rows[count as int]);
        reveal(Seq::filter);
    }

    pub(crate) open spec fn read_prefix<E>(input: GroupingInput<E>, count: int) -> bool {
        forall |i: int| 0 <= i < count ==> (#[trigger] (input.read)(input.rows[i])) is Ok
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    with Ghost(input): Ghost<GroupingInput<E>>
    requires
        depth < 64,
        rows.obeys_prophetic_iter_laws(),
        rows.remaining() == input.rows,
        forall |row: u32| #[trigger] iid_of.requires((row,)),
        forall |row: u32, result: Result<[u8; 16], E>| #[trigger] iid_of.ensures((row,), result) ==>
            result == (input.read)(row),
    ensures match out {
        Ok(buckets) => read_prefix(input, input.rows.len() as int) &&
            (forall |bucket: int| 0 <= bucket < 4 ==> #[trigger] buckets@[bucket]@ ==
                bucket_rows(input, depth as int, input.rows.len(), bucket)),
        Err(error) => exists |i: int| 0 <= i < input.rows.len() &&
            read_prefix(input, i) && (#[trigger] (input.read)(input.rows[i])) == Err(error),
    },
))]
pub(crate) fn group_base4_rows<I, F, D, E>(
    mut rows: I, depth: usize, iid_of: F, depth_error: D,
) -> Result<[Vec<u32>; 4], E>
where
    I: ExactSizeIterator<Item = u32>,
    F: Fn(u32) -> Result<[u8; 16], E>,
    D: FnOnce() -> E,
{
    #[cfg(verus_keep_ghost)]
    let _ = rows.len();
    let mut buckets = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    #[cfg(verus_keep_ghost)]
    proof! {
        reveal(Seq::filter);
        assert(input.rows.take(0) =~= Seq::<u32>::empty());
    }
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            depth < 64,
            rows.obeys_prophetic_iter_laws(),
            rows.remaining().is_suffix_of(input.rows),
            rows.exact_len() == rows.remaining().len(),
            forall |row: u32| #[trigger] iid_of.requires((row,)),
            forall |row: u32, result: Result<[u8; 16], E>| #[trigger] iid_of.ensures((row,), result) ==>
                result == (input.read)(row),
            read_prefix(input, input.rows.len() - rows.remaining().len()),
            forall |bucket: int| 0 <= bucket < 4 ==> #[trigger] buckets@[bucket]@ ==
                bucket_rows(input, depth as int, (input.rows.len() - rows.remaining().len()) as nat, bucket),
        decreases rows.exact_len(),
    ))]
    loop {
        #[cfg(verus_keep_ghost)]
        proof_decl! {
            let ghost remaining = rows.remaining();
            let ghost index = (input.rows.len() - remaining.len()) as nat;
            let ghost previous_buckets = buckets@;
        }
        let Some(row) = rows.next() else {
            #[cfg(verus_keep_ghost)]
            proof! {
                assert(remaining.len() == 0);
                assert forall |bucket: int| 0 <= bucket < 4 implies #[trigger] buckets@[bucket]@ ==
                    bucket_rows(input, depth as int, input.rows.len(), bucket) by {
                    assert(previous_buckets[bucket]@ == bucket_rows(input, depth as int, index, bucket));
                }
            }
            return Ok(buckets);
        };
        #[cfg(verus_keep_ghost)]
        let _ = rows.len();
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(remaining == input.rows.skip(index as int));
            assert(row == input.rows[index as int]);
            assert(rows.remaining() =~= input.rows.skip(index as int + 1));
        }
        let iid = match iid_of(row) {
            Ok(iid) => iid,
            Err(error) => return Err(error),
        };
        let Some(bucket) = base4_bucket(&iid, depth) else {
            return Err(depth_error());
        };
        buckets[usize::from(bucket)].push(row);
        #[cfg(verus_keep_ghost)]
        proof! {
            assert forall |i: int| 0 <= i < index + 1 implies
                (#[trigger] (input.read)(input.rows[i])) is Ok by {
                if i < index {
                    assert(read_prefix(input, index as int));
                }
            }
            assert forall |target: int| 0 <= target < 4 implies
                #[trigger] buckets@[target]@ ==
                    bucket_rows(input, depth as int, (input.rows.len() - rows.remaining().len()) as nat, target) by {
                assert((input.read)(row) == Ok(iid));
                assert(iid_digit(iid@, depth as int) == bucket as int);
                bucket_rows_advance(input, depth as int, index, target, bucket as int);
                assert(previous_buckets[target]@ == bucket_rows(input, depth as int, index, target));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::convert::Infallible;

    use super::group_base4_rows;

    #[test]
    fn grouping_retains_repeats_and_order_at_the_last_iid_digit() {
        let remaining = [2, 1, 0, 2, 3, 1].into_iter();
        let iids = [[0; 16], [1; 16], [4; 16], [0xff; 16]];
        let actual = group_base4_rows(
            remaining,
            63,
            |row| Ok::<_, Infallible>(iids[row as usize]),
            || unreachable!(),
        );
        assert_eq!(actual.unwrap(), [vec![2, 0, 2], vec![1, 1], vec![], vec![3]]);
    }

    #[test]
    fn grouping_returns_the_first_read_error_without_reading_the_suffix() {
        let visited = RefCell::new(Vec::new());
        let actual = group_base4_rows(
            0..4,
            0,
            |row| {
                visited.borrow_mut().push(row);
                if row >= 2 { Err(row) } else { Ok([0; 16]) }
            },
            || unreachable!(),
        );
        assert_eq!(actual, Err(2));
        assert_eq!(visited.into_inner(), [0, 1, 2]);
    }
}
