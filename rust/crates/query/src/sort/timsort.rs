// Comparison order follows OpenJDK 25 TimSort (jdk-25.0.1+8), including its gallop thresholds:
// https://github.com/openjdk/jdk25u/blob/jdk-25.0.1%2B8/src/java.base/share/classes/java/util/TimSort.java
use std::cmp::Ordering;
use rtdb_types::{Anomaly,Result};

const MIN_MERGE: usize = 32;
const MIN_GALLOP: usize = 7;
const MAX_LENGTH: usize = i32::MAX as usize - 4;

#[derive(Clone,Copy,Default)]
struct Run { base: usize,len: usize }

pub(super) fn scratch_bytes(len: usize) -> Result<usize> {
    if len > MAX_LENGTH { return Err(size_error()); }
    if len < MIN_MERGE { return Ok(0); }
    (len / 2).checked_mul(std::mem::size_of::<usize>()).ok_or_else(size_error)
}

/// The caller must retain a reservation of `scratch_bytes(indices.len())` throughout this call.
/// The first comparator error stops the sort; the index slice may then be partially reordered.
pub(super) fn sort(indices: &mut [usize],mut compare: impl FnMut(usize,usize) -> Result<Ordering>) -> Result<()> {
    scratch_bytes(indices.len())?;
    let len = indices.len();
    if len < 2 { return Ok(()); }
    if len < MIN_MERGE {
        let run = count_run(indices,0,&mut compare)?;
        return binary_sort(indices,0,len,run,&mut compare);
    }
    let mut state = TimSort { indices,compare: &mut compare,temporary: vec![0;len / 2],runs: [Run::default();49],run_count: 0,min_gallop: MIN_GALLOP };
    let min_run = min_run_length(len);
    let mut base = 0;
    while base < len {
        let mut run = count_run(state.indices,base,state.compare)?;
        if run < min_run {
            let extended = min_run.min(len - base);
            binary_sort(state.indices,base,base + extended,base + run,state.compare)?;
            run = extended;
        }
        state.runs[state.run_count] = Run { base,len: run };
        state.run_count += 1;
        state.collapse(false)?;
        base += run;
    }
    state.collapse(true)
}

struct TimSort<'a,C> {
    indices: &'a mut [usize],compare: &'a mut C,temporary: Vec<usize>,
    runs: [Run;49],run_count: usize,min_gallop: usize,
}

fn min_run_length(mut len: usize) -> usize {
    let mut remainder = 0;
    while len >= MIN_MERGE { remainder |= len & 1;len >>= 1; }
    len + remainder
}

fn count_run(indices: &mut [usize],base: usize,compare: &mut impl FnMut(usize,usize) -> Result<Ordering>) -> Result<usize> {
    let mut end = base + 1;
    if end == indices.len() { return Ok(1); }
    let descending = compare(indices[end],indices[base])? == Ordering::Less;
    end += 1;
    while end < indices.len() {
        let order = compare(indices[end],indices[end - 1])?;
        if (order == Ordering::Less) != descending { break; }
        end += 1;
    }
    if descending { indices[base..end].reverse(); }
    Ok(end - base)
}

fn binary_sort(indices: &mut [usize],base: usize,end: usize,start: usize,compare: &mut impl FnMut(usize,usize) -> Result<Ordering>) -> Result<()> {
    for at in start.max(base + 1)..end {
        let pivot = indices[at];
        let (mut left,mut right) = (base,at);
        while left < right {
            let middle = left + (right - left) / 2;
            if compare(pivot,indices[middle])? == Ordering::Less { right = middle; }
            else { left = middle + 1; }
        }
        indices.copy_within(left..at,left + 1);
        indices[left] = pivot;
    }
    Ok(())
}

fn gallop(key: usize,values: &[usize],hint: usize,upper: bool,compare: &mut impl FnMut(usize,usize) -> Result<Ordering>) -> Result<usize> {
    let after = |order| if upper { order != Ordering::Less } else { order == Ordering::Greater };
    let (mut previous,mut offset) = (0,1);
    let (mut left,mut right) = if after(compare(key,values[hint])?) {
        let max = values.len() - hint;
        while offset < max && after(compare(key,values[hint + offset])?) {
            previous = offset;offset = (2 * offset + 1).min(max);
        }
        (hint + previous + 1,hint + offset.min(max))
    } else {
        let max = hint + 1;
        while offset < max && !after(compare(key,values[hint - offset])?) {
            previous = offset;offset = (2 * offset + 1).min(max);
        }
        (hint + 1 - offset.min(max),hint - previous)
    };
    while left < right {
        let middle = left + (right - left) / 2;
        if after(compare(key,values[middle])?) { left = middle + 1; }
        else { right = middle; }
    }
    Ok(right)
}

impl<C: FnMut(usize,usize) -> Result<Ordering>> TimSort<'_,C> {
    fn collapse(&mut self,force: bool) -> Result<()> {
        while self.run_count > 1 {
            let mut at = self.run_count - 2;
            if force || at > 0 && self.runs[at - 1].len <= self.runs[at].len + self.runs[at + 1].len
                || at > 1 && self.runs[at - 2].len <= self.runs[at].len + self.runs[at - 1].len {
                if at > 0 && self.runs[at - 1].len < self.runs[at + 1].len { at -= 1; }
            } else if self.runs[at].len > self.runs[at + 1].len { break; }
            self.merge_at(at)?;
        }
        Ok(())
    }

    fn merge_at(&mut self,at: usize) -> Result<()> {
        let (mut left,mut right) = (self.runs[at],self.runs[at + 1]);
        self.runs[at].len = left.len + right.len;
        if at + 3 == self.run_count { self.runs[at + 1] = self.runs[at + 2]; }
        self.run_count -= 1;
        let skipped = gallop(self.indices[right.base],&self.indices[left.base..left.base + left.len],0,true,self.compare)?;
        left.base += skipped;left.len -= skipped;
        if left.len == 0 { return Ok(()); }
        right.len = gallop(self.indices[left.base + left.len - 1],&self.indices[right.base..right.base + right.len],right.len - 1,false,self.compare)?;
        if right.len == 0 { return Ok(()); }
        if left.len <= right.len { self.merge_low(left,right) } else { self.merge_high(left,right) }
    }

    fn merge_low(&mut self,left: Run,right: Run) -> Result<()> {
        self.temporary[..left.len].copy_from_slice(&self.indices[left.base..left.base + left.len]);
        let (mut first,mut second,mut destination) = (0,right.base,left.base);
        let (mut left_len,mut right_len) = (left.len,right.len);
        self.indices[destination] = self.indices[second];destination += 1;second += 1;right_len -= 1;
        if right_len == 0 { self.indices[destination..destination + left_len].copy_from_slice(&self.temporary[first..first + left_len]);return Ok(()); }
        if left_len == 1 {
            self.indices.copy_within(second..second + right_len,destination);
            self.indices[destination + right_len] = self.temporary[first];return Ok(());
        }
        let mut threshold = self.min_gallop as isize;
        'merge: loop {
            let (mut left_wins,mut right_wins) = (0,0);
            loop {
                if (self.compare)(self.indices[second],self.temporary[first])? == Ordering::Less {
                    self.indices[destination] = self.indices[second];destination += 1;second += 1;
                    right_wins += 1;left_wins = 0;right_len -= 1;
                    if right_len == 0 { break 'merge; }
                } else {
                    self.indices[destination] = self.temporary[first];destination += 1;first += 1;
                    left_wins += 1;right_wins = 0;left_len -= 1;
                    if left_len == 1 { break 'merge; }
                }
                if (left_wins | right_wins) as isize >= threshold { break; }
            }
            loop {
                left_wins = gallop(self.indices[second],&self.temporary[first..first + left_len],0,true,self.compare)?;
                if left_wins > 0 {
                    self.indices[destination..destination + left_wins].copy_from_slice(&self.temporary[first..first + left_wins]);
                    destination += left_wins;first += left_wins;left_len -= left_wins;
                    if left_len <= 1 { break 'merge; }
                }
                self.indices[destination] = self.indices[second];destination += 1;second += 1;right_len -= 1;
                if right_len == 0 { break 'merge; }
                right_wins = gallop(self.temporary[first],&self.indices[second..second + right_len],0,false,self.compare)?;
                if right_wins > 0 {
                    self.indices.copy_within(second..second + right_wins,destination);
                    destination += right_wins;second += right_wins;right_len -= right_wins;
                    if right_len == 0 { break 'merge; }
                }
                self.indices[destination] = self.temporary[first];destination += 1;first += 1;left_len -= 1;
                if left_len == 1 { break 'merge; }
                threshold -= 1;
                if left_wins < MIN_GALLOP && right_wins < MIN_GALLOP { break; }
            }
            threshold = threshold.max(0) + 2;
        }
        self.min_gallop = threshold.max(1) as usize;
        if left_len == 1 {
            self.indices.copy_within(second..second + right_len,destination);
            self.indices[destination + right_len] = self.temporary[first];
        } else if left_len == 0 { return Err(contract_error()); }
        else { self.indices[destination..destination + left_len].copy_from_slice(&self.temporary[first..first + left_len]); }
        Ok(())
    }

    fn merge_high(&mut self,left: Run,right: Run) -> Result<()> {
        self.temporary[..right.len].copy_from_slice(&self.indices[right.base..right.base + right.len]);
        let (mut first,mut second,mut destination) = (left.base + left.len,right.len,right.base + right.len);
        let (mut left_len,mut right_len) = (left.len,right.len);
        destination -= 1;first -= 1;self.indices[destination] = self.indices[first];left_len -= 1;
        if left_len == 0 { self.indices[destination - right_len..destination].copy_from_slice(&self.temporary[..right_len]);return Ok(()); }
        if right_len == 1 {
            self.indices.copy_within(first - left_len..first,destination - left_len);
            self.indices[destination - left_len - 1] = self.temporary[second - 1];return Ok(());
        }
        let mut threshold = self.min_gallop as isize;
        'merge: loop {
            let (mut left_wins,mut right_wins) = (0,0);
            loop {
                if (self.compare)(self.temporary[second - 1],self.indices[first - 1])? == Ordering::Less {
                    destination -= 1;first -= 1;self.indices[destination] = self.indices[first];
                    left_wins += 1;right_wins = 0;left_len -= 1;
                    if left_len == 0 { break 'merge; }
                } else {
                    destination -= 1;second -= 1;self.indices[destination] = self.temporary[second];
                    right_wins += 1;left_wins = 0;right_len -= 1;
                    if right_len == 1 { break 'merge; }
                }
                if (left_wins | right_wins) as isize >= threshold { break; }
            }
            loop {
                left_wins = left_len - gallop(self.temporary[second - 1],&self.indices[left.base..left.base + left_len],left_len - 1,true,self.compare)?;
                if left_wins > 0 {
                    destination -= left_wins;first -= left_wins;left_len -= left_wins;
                    self.indices.copy_within(first..first + left_wins,destination);
                    if left_len == 0 { break 'merge; }
                }
                destination -= 1;second -= 1;self.indices[destination] = self.temporary[second];right_len -= 1;
                if right_len == 1 { break 'merge; }
                right_wins = right_len - gallop(self.indices[first - 1],&self.temporary[..right_len],right_len - 1,false,self.compare)?;
                if right_wins > 0 {
                    destination -= right_wins;second -= right_wins;right_len -= right_wins;
                    self.indices[destination..destination + right_wins].copy_from_slice(&self.temporary[second..second + right_wins]);
                    if right_len <= 1 { break 'merge; }
                }
                destination -= 1;first -= 1;self.indices[destination] = self.indices[first];left_len -= 1;
                if left_len == 0 { break 'merge; }
                threshold -= 1;
                if left_wins < MIN_GALLOP && right_wins < MIN_GALLOP { break; }
            }
            threshold = threshold.max(0) + 2;
        }
        self.min_gallop = threshold.max(1) as usize;
        if right_len == 1 {
            self.indices.copy_within(first - left_len..first,destination - left_len);
            self.indices[destination - left_len - 1] = self.temporary[second - 1];
        } else if right_len == 0 { return Err(contract_error()); }
        else { self.indices[destination - right_len..destination].copy_from_slice(&self.temporary[..right_len]); }
        Ok(())
    }
}

fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.query/sort-size","TimSort row count exceeds the JVM array domain").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded) }
fn contract_error() -> Anomaly { Anomaly::fault("xtdb.error/unknown","Comparison method violates its general contract!") }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_binary_insertion_keeps_the_jdk_comparison_trace_and_equal_order() {
        let keys = [3,1,2,1];
        let mut indices = [0,1,2,3];
        let mut trace = Vec::new();
        sort(&mut indices,|left,right| { trace.push((left,right));Ok(keys[left].cmp(&keys[right])) }).unwrap();
        assert_eq!(indices,[1,3,2,0]);
        assert_eq!(trace,[(1,0),(2,1),(2,0),(2,1),(3,2),(3,1)]);
    }

    #[test]
    fn gallop_bounds_use_the_pinned_hint_and_binary_comparison_order() {
        let values = [0,2,2,4,4,6,8];
        let mut trace = Vec::new();
        let mut compare = |left: usize,right: usize| { trace.push((left,right));Ok(left.cmp(&right)) };
        assert_eq!(gallop(4,&values,0,false,&mut compare).unwrap(),3);
        assert_eq!(trace,[(4,0),(4,2),(4,4),(4,2)]);
        trace.clear();
        let mut compare = |left: usize,right: usize| { trace.push((left,right));Ok(left.cmp(&right)) };
        assert_eq!(gallop(4,&values,6,true,&mut compare).unwrap(),5);
        assert_eq!(trace,[(4,8),(4,6),(4,4),(4,4)]);
    }

    #[test]
    fn two_reversed_runs_enter_low_merge_galloping_after_seven_wins() {
        let keys: Vec<_> = (16..32).chain(0..16).collect();
        let mut indices: Vec<_> = (0..32).collect();
        let mut trace = Vec::new();
        sort(&mut indices,|left,right| { trace.push((left,right));Ok(keys[left].cmp(&keys[right])) }).unwrap();
        let mut expected: Vec<_> = (1..=16).map(|index|(index,index - 1)).chain((17..32).map(|index|(index,index - 1))).collect();
        expected.extend([(16,0),(15,31)]);
        expected.extend((17..24).map(|index|(index,0)));
        expected.extend([(24,0),(0,25),(0,26),(0,28),(0,30),(0,31)]);
        assert_eq!(trace,expected);
        assert_eq!(indices,(16..32).chain(0..16).collect::<Vec<_>>());
    }

    #[test]
    fn reverse_merge_gallops_from_the_high_hint_without_negative_cursors() {
        let mut indices: Vec<_> = (16..64).chain(0..16).collect();
        let mut trace = Vec::new();
        let mut compare = |left: usize,right: usize| { trace.push((left,right));Ok(left.cmp(&right)) };
        let mut state = TimSort { indices: &mut indices,compare: &mut compare,temporary: vec![0;32],runs: [Run::default();49],run_count: 0,min_gallop: 7 };
        state.merge_high(Run { base: 0,len: 48 },Run { base: 48,len: 16 }).unwrap();
        let mut expected: Vec<_> = (56..63).rev().map(|value|(15,value)).collect();
        expected.extend([(15,55),(15,54),(15,52),(15,48),(15,40),(15,24),(15,20),(15,18),(15,17),(15,16)]);
        assert_eq!(trace,expected);
        assert_eq!(indices,(0..64).collect::<Vec<_>>());
    }

    #[test]
    fn run_boundaries_and_duplicate_keys_sort_stably() {
        for len in [0usize,1,2,31,32,33,63,64,119,120,1541,1542,4096] {
            let keys: Vec<_> = (0..len).map(|index|((index * 73) % len) / 3).collect();
            let mut indices: Vec<_> = (0..len).collect();
            sort(&mut indices,|left,right|Ok(keys[left].cmp(&keys[right]))).unwrap();
            assert_eq!(indices.len(),len);
            let mut seen = vec![false;len];
            for index in &indices { assert!(!seen[*index]);seen[*index] = true; }
            for pair in indices.windows(2) {
                assert!(keys[pair[0]] < keys[pair[1]] || keys[pair[0]] == keys[pair[1]] && pair[0] < pair[1]);
            }
        }
        for (len,expected) in [(31,31),(32,16),(33,17),(63,32),(64,16),(65,17),(127,32),(128,16),(1542,25)] { assert_eq!(min_run_length(len),expected); }
        assert_eq!(scratch_bytes(31).unwrap(),0);
        assert_eq!(scratch_bytes(32).unwrap(),16 * std::mem::size_of::<usize>());
        assert!(scratch_bytes(MAX_LENGTH + 1).is_err());
    }

    #[test]
    fn collapse_checks_the_fourth_run_and_force_collapse_selects_the_shorter_neighbor() {
        let mut indices: Vec<_> = (0..23).collect();
        let mut compare = |left: usize,right: usize|Ok(left.cmp(&right));
        let mut runs = [Run::default();49];
        runs[..4].copy_from_slice(&[Run { base: 0,len: 10 },Run { base: 10,len: 7 },Run { base: 17,len: 4 },Run { base: 21,len: 2 }]);
        let mut state = TimSort { indices: &mut indices,compare: &mut compare,temporary: vec![0;11],runs,run_count: 4,min_gallop: 7 };
        state.collapse(false).unwrap();
        assert_eq!(state.run_count,1);assert_eq!(state.runs[0].len,23);

        let mut indices: Vec<_> = (0..9).collect();
        let mut trace = Vec::new();
        let mut compare = |left: usize,right: usize| { trace.push((left,right));Ok(left.cmp(&right)) };
        let mut runs = [Run::default();49];
        runs[..3].copy_from_slice(&[Run { base: 0,len: 2 },Run { base: 2,len: 3 },Run { base: 5,len: 4 }]);
        let mut state = TimSort { indices: &mut indices,compare: &mut compare,temporary: vec![0;4],runs,run_count: 3,min_gallop: 7 };
        state.collapse(true).unwrap();
        assert_eq!(state.run_count,1);
        assert_eq!(trace,[(2,0),(2,1),(5,0),(5,1),(5,3),(5,4)]);
    }

    #[test]
    fn the_first_comparator_error_stops_every_stage_without_replacing_it() {
        let keys: Vec<_> = (0..512).map(|index|(index * 73) % 127).collect();
        let mut indices: Vec<_> = (0..keys.len()).collect();
        let mut full = Vec::new();
        sort(&mut indices,|left,right| { full.push((left,right));Ok(keys[left].cmp(&keys[right])) }).unwrap();
        for fail_at in [1,full.len() / 3,full.len() / 2,full.len()] {
            let mut indices: Vec<_> = (0..keys.len()).collect();
            let mut trace = Vec::new();
            let error = sort(&mut indices,|left,right| {
                trace.push((left,right));
                if trace.len() == fail_at { Err(Anomaly::incorrect("test/comparator","sentinel")) }
                else { Ok(keys[left].cmp(&keys[right])) }
            }).unwrap_err();
            assert_eq!(error.code,"test/comparator");assert_eq!(error.message.as_deref(),Some("sentinel"));
            assert_eq!(trace.as_slice(),&full[..fail_at]);
        }
    }

    #[test]
    fn inconsistent_comparators_retain_both_jdk_merge_contract_failures() {
        for high in [false,true] {
            let mut indices: Vec<_> = (0..32).collect();
            let mut calls = 0;
            let mut compare = |_,_| {
                calls += 1;
                Ok(if if high { calls <= 8 } else { calls > 7 } { Ordering::Greater } else { Ordering::Less })
            };
            let mut state = TimSort { indices: &mut indices,compare: &mut compare,temporary: vec![0;16],runs: [Run::default();49],run_count: 0,min_gallop: 7 };
            let (left,right) = (Run { base: 0,len: 16 },Run { base: 16,len: 16 });
            let error = if high { state.merge_high(left,right) } else { state.merge_low(left,right) }.unwrap_err();
            assert_eq!(error.code,"xtdb.error/unknown");
            assert_eq!(error.message.as_deref(),Some("Comparison method violates its general contract!"));
            assert_eq!(calls,12);
        }
    }
}
