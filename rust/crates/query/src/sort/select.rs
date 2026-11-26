use std::cmp::Ordering;
use rtdb_types::Result;

/// The `limit` elements of `0..len` that a stable sort under `compare` places first, in that order.
/// `compare` must be a total order; equal elements keep their input order, as a stable sort keeps them.
pub(super) fn first(len: usize, limit: usize, mut compare: impl FnMut(usize, usize) -> Result<Ordering>) -> Result<Vec<usize>> {
    let limit = limit.min(len);
    let mut kept: Vec<usize> = Vec::with_capacity(limit);
    if limit == 0 { return Ok(kept); }
    // A max-heap under (key, input position): its root is the element a stable sort would place last among those kept.
    for element in 0..len {
        if kept.len() < limit {
            kept.push(element);
            let mut child = kept.len() - 1;
            while child > 0 {
                let parent = (child - 1) / 2;
                if !after(&mut compare, kept[child], kept[parent])? { break; }
                kept.swap(child, parent);
                child = parent;
            }
        } else if after(&mut compare, kept[0], element)? {
            kept[0] = element;
            let mut parent = 0;
            loop {
                let (left, right) = (2 * parent + 1, 2 * parent + 2);
                let mut latest = parent;
                if left < limit && after(&mut compare, kept[left], kept[latest])? { latest = left; }
                if right < limit && after(&mut compare, kept[right], kept[latest])? { latest = right; }
                if latest == parent { break; }
                kept.swap(parent, latest);
                parent = latest;
            }
        }
    }
    kept.sort_unstable();
    super::timsort::sort(&mut kept, compare)?;
    Ok(kept)
}

fn after(compare: &mut impl FnMut(usize, usize) -> Result<Ordering>, left: usize, right: usize) -> Result<bool> {
    Ok(compare(left, right)?.then(left.cmp(&right)) == Ordering::Greater)
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
                let selected = first(keys.len(), limit, |left, right| Ok(keys[left].cmp(&keys[right]))).unwrap();
                assert_eq!(selected, stable_first(&keys, limit), "len {len}, limit {limit}");
            }
        }
    }

    #[test]
    fn a_failed_comparison_stops_the_selection() {
        let mut calls = 0;
        let error = first(100, 10, |_, _| {
            calls += 1;
            if calls == 20 { Err(rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "xtdb/operation-cancelled", "cancelled")) } else { Ok(std::cmp::Ordering::Equal) }
        }).unwrap_err();
        assert_eq!(error.code, "xtdb/operation-cancelled");
    }
}
