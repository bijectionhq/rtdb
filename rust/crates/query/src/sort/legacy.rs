// OpenJDK 25 Arrays.legacyMergeSort comparison schedule (jdk-25.0.1+8):
// https://github.com/openjdk/jdk25u/blob/jdk-25.0.1%2B8/src/java.base/share/classes/java/util/Arrays.java
use std::cmp::Ordering;
use rtdb_types::{Anomaly,Result};

pub(super) fn scratch_bytes(len: usize) -> Result<usize> {
    if len > i32::MAX as usize - 4 { return Err(size_error()); }
    len.checked_mul(std::mem::size_of::<usize>()).ok_or_else(size_error)
}

/// The caller retains `scratch_bytes(indices.len())` until return and abandons the partial order on error.
pub(super) fn sort(indices: &mut [usize],mut compare: impl FnMut(usize,usize) -> Result<Ordering>) -> Result<()> {
    scratch_bytes(indices.len())?;
    let mut source = indices.to_vec();
    let length = indices.len();
    merge(&mut source,indices,0,length,&mut compare)
}

fn merge(source: &mut [usize],destination: &mut [usize],low: usize,high: usize,compare: &mut impl FnMut(usize,usize) -> Result<Ordering>) -> Result<()> {
    if high - low < 7 {
        for at in low..high {
            let mut current = at;
            while current > low && compare(destination[current - 1],destination[current])? == Ordering::Greater {
                destination.swap(current,current - 1);
                current -= 1;
            }
        }
        return Ok(());
    }
    let middle = low + (high - low) / 2;
    merge(destination,source,low,middle,compare)?;
    merge(destination,source,middle,high,compare)?;
    if compare(source[middle - 1],source[middle])? != Ordering::Greater {
        destination[low..high].copy_from_slice(&source[low..high]);
        return Ok(());
    }
    let (mut left,mut right) = (low,middle);
    for output in &mut destination[low..high] {
        if right >= high || left < middle && compare(source[left],source[right])? != Ordering::Greater {
            *output = source[left];left += 1;
        } else { *output = source[right];right += 1; }
    }
    Ok(())
}
fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.query/sort-size","Legacy merge sort row count exceeds the JVM array domain").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded) }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn insertion_compares_predecessor_first_and_preserves_ties() {
        let keys = [3,1,2,1];let mut indices = [0,1,2,3];let mut trace = Vec::new();
        sort(&mut indices,|left,right| { trace.push((left,right));Ok(keys[left].cmp(&keys[right])) }).unwrap();
        assert_eq!(indices,[1,3,2,0]);
        assert_eq!(trace,[(0,1),(0,2),(1,2),(0,3),(2,3),(1,3)]);
    }
    #[test]
    fn seven_elements_sort_both_halves_before_the_ordered_boundary_copy() {
        let mut indices = [0,1,2,3,4,5,6];let mut trace = Vec::new();
        sort(&mut indices,|left,right| { trace.push((left,right));Ok(left.cmp(&right)) }).unwrap();
        assert_eq!(trace,[(0,1),(1,2),(3,4),(4,5),(5,6),(2,3)]);
        assert_eq!(indices,[0,1,2,3,4,5,6]);
    }
    #[test]
    fn recursion_and_complete_scratch_preserve_stable_duplicate_order() {
        for length in [0,1,6,7,8,31,32,127,1025] {
            let keys: Vec<_> = (0..length).map(|index|(index * 73 % 101) / 3).collect();
            let mut indices: Vec<_> = (0..length).collect();
            sort(&mut indices,|left,right|Ok(keys[left].cmp(&keys[right]))).unwrap();
            assert_eq!(scratch_bytes(length).unwrap(),length * std::mem::size_of::<usize>());
            for pair in indices.windows(2) { assert!(keys[pair[0]] < keys[pair[1]] || keys[pair[0]] == keys[pair[1]] && pair[0] < pair[1]); }
        }
        assert!(scratch_bytes(i32::MAX as usize - 3).is_err());
    }
    #[test]
    fn first_comparator_error_preserves_the_exact_successful_prefix() {
        let keys: Vec<_> = (0..129).map(|index|index * 53 % 127).collect();
        let mut full = Vec::new();let mut indices: Vec<_> = (0..keys.len()).collect();
        sort(&mut indices,|left,right| { full.push((left,right));Ok(keys[left].cmp(&keys[right])) }).unwrap();
        for fail_at in [1,full.len() / 2,full.len()] {
            let mut trace = Vec::new();let mut indices: Vec<_> = (0..keys.len()).collect();
            let error = sort(&mut indices,|left,right| {
                trace.push((left,right));
                if trace.len() == fail_at { Err(Anomaly::incorrect("test/comparator","original")) } else { Ok(keys[left].cmp(&keys[right])) }
            }).unwrap_err();
            assert_eq!(error.code,"test/comparator");assert_eq!(error.message.as_deref(),Some("original"));assert_eq!(trace.as_slice(),&full[..fail_at]);
        }
    }
    #[test]
    fn inconsistent_comparator_does_not_invent_timsort_contract_checks() {
        let mut indices: Vec<_> = (0..64).collect();
        sort(&mut indices,|_,_|Ok(Ordering::Greater)).unwrap();
    }
}
