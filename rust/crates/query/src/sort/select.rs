use std::{cmp::Ordering, mem::size_of};
use rtdb_types::Result;

struct Candidate<K> {
    index: usize,
    key: K,
}

pub(super) fn scratch_bytes<K>(limit: usize) -> Result<usize> {
    let sort = super::timsort::scratch_bytes(limit)?;
    limit.checked_mul(size_of::<Candidate<K>>())
        .and_then(|bytes| bytes.checked_add(sort))
        .ok_or_else(super::size_error)
}

/// The `limit` elements of `0..len` that a stable sort under `compare` places first, in that order.
/// `compare` must be a total order; equal elements keep their input order, as a stable sort keeps them.
pub(super) fn first<K>(
    len: usize,
    limit: usize,
    mut read: impl FnMut(usize) -> K,
    mut compare: impl FnMut(usize, &K, usize, &K) -> Result<Ordering>,
) -> Result<Vec<usize>> {
    let limit = limit.min(len);
    if limit == 0 { return Ok(Vec::new()); }
    let mut kept = Vec::with_capacity(limit);
    let mut order = |left: &Candidate<K>, right: &Candidate<K>| {
        compare(left.index, &left.key, right.index, &right.key)
    };
    // A max-heap under (key, input position): its root is the element a stable sort would place last among those kept.
    for index in 0..len {
        let element = Candidate { index, key: read(index) };
        if kept.len() < limit {
            kept.push(element);
            let mut child = kept.len() - 1;
            while child > 0 {
                let parent = (child - 1) / 2;
                if !after(&mut order, &kept[child], &kept[parent])? { break; }
                kept.swap(child, parent);
                child = parent;
            }
        } else if after(&mut order, &kept[0], &element)? {
            kept[0] = element;
            let mut parent = 0;
            loop {
                let (left, right) = (2 * parent + 1, 2 * parent + 2);
                let mut latest = parent;
                if left < limit && after(&mut order, &kept[left], &kept[latest])? { latest = left; }
                if right < limit && after(&mut order, &kept[right], &kept[latest])? { latest = right; }
                if latest == parent { break; }
                kept.swap(parent, latest);
                parent = latest;
            }
        }
    }
    kept.sort_unstable_by_key(|candidate| candidate.index);
    let mut indices: Vec<_> = (0..kept.len()).collect();
    super::timsort::sort(&mut indices, |left, right| order(&kept[left], &kept[right]))?;
    for index in &mut indices {
        *index = kept[*index].index;
    }
    Ok(indices)
}

fn after<K>(
    compare: &mut impl FnMut(&Candidate<K>, &Candidate<K>) -> Result<Ordering>,
    left: &Candidate<K>,
    right: &Candidate<K>,
) -> Result<bool> {
    Ok(compare(left, right)?.then(left.index.cmp(&right.index)).is_gt())
}

#[cfg(test)]
mod tests {
    use super::first;

    fn stable_first(keys: &[u8], limit: usize) -> Vec<usize> {
        let mut order: Vec<usize> = (0..keys.len()).collect();
        order.sort_by_key(|&index| keys[index]);
        order.truncate(limit);
        order
    }

    #[test]
    fn selects_what_a_stable_sort_places_first_including_ties() {
        let mut state = 0x9E37_79B9_u32;
        for len in [0, 1, 2, 7, 64, 1000] {
            let keys: Vec<u8> = (0..len).map(|_| { state ^= state << 13; state ^= state >> 17; state ^= state << 5; (state % 11) as u8 }).collect();
            for limit in [0, 1, 3, 10, 100, 2000] {
                let selected = first(keys.len(), limit, |index| keys[index], |_, a, _, b| Ok(a.cmp(b))).unwrap();
                assert_eq!(selected, stable_first(&keys, limit), "len {len}, limit {limit}");
            }
        }
    }

    #[test]
    fn a_failed_comparison_stops_the_selection() {
        let mut calls = 0;
        let error = first(100, 10, |_| (), |_, _, _, _| {
            calls += 1;
            if calls == 20 { Err(rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "xtdb/operation-cancelled", "cancelled")) } else { Ok(std::cmp::Ordering::Equal) }
        }).unwrap_err();
        assert_eq!(error.code, "xtdb/operation-cancelled");
    }

    #[test]
    fn heap_and_final_order_read_each_candidate_key_once() {
        let mut reads = [0; 96];
        let indices = first(96, 9, |index| {
            reads[index] += 1;
            index % 3
        }, |_, a, _, b| Ok(a.cmp(b))).unwrap();
        assert_eq!(reads, [1; 96]);
        assert_eq!(indices, (0..27).step_by(3).collect::<Vec<_>>());
    }
}
