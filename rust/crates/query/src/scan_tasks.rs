use std::sync::Arc;
use rtdb_columnar::{MemoryBudget,MemoryReservation};
use rtdb_types::{Iid,Result};
use crate::invalid;

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(super) struct PageRange { pub lower: Iid, pub upper: Iid }
impl PageRange {
    fn intersects(self,other: Self) -> bool { self.lower <= other.upper && other.lower <= self.upper }
    fn contains(self,other: Self) -> bool { self.lower <= other.lower && self.upper >= other.upper }
    fn child(self,depth: u8,bucket: u8) -> Self {
        let shift = 126 - u32::from(depth) * 2;
        let lower = u128::from_be_bytes(self.lower.0) | (u128::from(bucket) << shift);
        Self { lower: Iid(lower.to_be_bytes()),upper: Iid((lower | ((1u128 << shift) - 1)).to_be_bytes()) }
    }
}

pub(super) struct ScanTask { pub range: PageRange, pub pages: Vec<usize>, _reservation: Arc<MemoryReservation> }
struct Work { task: ScanTask, depth: u8 }
/// Tasks leave the plan in ascending order of their IID ranges, or in descending order where `descending` is set.
pub(super) struct ScanTaskPlan { ranges: Vec<PageRange>, stack: Vec<Work>, descending: bool, memory: Arc<MemoryBudget>, _reservation: Arc<MemoryReservation> }
impl ScanTaskPlan {
    pub fn new(ranges: impl ExactSizeIterator<Item=PageRange>, descending: bool, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bound = ranges.len().checked_mul(std::mem::size_of::<PageRange>()).and_then(|bytes|bytes.checked_add(193 * std::mem::size_of::<Work>() + 512)).ok_or_else(size)?;
        let reservation = memory.reserve(bound)?;
        let ranges: Vec<_> = ranges.collect();
        if ranges.iter().any(|range|range.lower > range.upper) { return Err(invalid("scan-page-range","Source page has inverted IID bounds")); }
        let mut plan = Self { ranges,stack: Vec::with_capacity(193),descending,memory: Arc::clone(memory),_reservation: reservation };
        if !plan.ranges.is_empty() {
            let reservation = plan.memory.reserve(index_bytes(plan.ranges.len())?)?;
            plan.stack.push(Work { task: ScanTask { range: PageRange { lower: Iid([0;16]),upper: Iid([255;16]) },pages: (0..plan.ranges.len()).collect(),_reservation: reservation },depth: 0 });
        }
        Ok(plan)
    }
    pub fn next(&mut self) -> Result<Option<ScanTask>> {
        while let Some(work) = self.stack.pop() {
            if work.task.pages.iter().all(|index|self.ranges[*index].contains(work.task.range)) { return Ok(Some(work.task)); }
            if work.depth == 64 { return Err(invalid("scan-page-range","Page bounds cannot be partitioned by IID")); }
            // The stack hands back the bucket pushed last, which is the one to read first.
            for bucket in if self.descending { [0, 1, 2, 3] } else { [3, 2, 1, 0] } {
                let range = work.task.range.child(work.depth,bucket);
                let count = work.task.pages.iter().filter(|index|self.ranges[**index].intersects(range)).count();
                if count == 0 { continue; }
                let reservation = self.memory.reserve(index_bytes(count)?)?;
                let mut pages = Vec::with_capacity(count);
                pages.extend(work.task.pages.iter().copied().filter(|index|self.ranges[*index].intersects(range)));
                self.stack.push(Work { task: ScanTask { range,pages,_reservation: reservation },depth: work.depth + 1 });
            }
        }
        Ok(None)
    }
}
fn index_bytes(count: usize) -> Result<usize> { count.checked_mul(std::mem::size_of::<usize>()).and_then(|bytes|bytes.checked_add(256)).ok_or_else(size) }
fn size() -> rtdb_types::Anomaly { invalid("scan-task-size","Scan task allocation exceeds addressable memory") }

#[cfg(test)]
mod tests {
    use super::*;
    fn root() -> PageRange { PageRange { lower: Iid([0;16]),upper: Iid([255;16]) } }

    #[test]
    fn ancestor_pages_repeat_across_all_children_in_prefix_order() {
        let memory = MemoryBudget::new(1 << 20);
        let child = root().child(0,1);
        let grandchild = child.child(1,2);
        let mut plan = ScanTaskPlan::new([root(),child,grandchild].into_iter(),false,&memory).unwrap();
        let mut tasks = Vec::new();
        while let Some(task) = plan.next().unwrap() { tasks.push((task.range,task.pages.clone())); }
        assert_eq!(tasks,vec![(root().child(0,0),vec![0]),(child.child(1,0),vec![0,1]),(child.child(1,1),vec![0,1]),(grandchild,vec![0,1,2]),(child.child(1,3),vec![0,1]),(root().child(0,2),vec![0]),(root().child(0,3),vec![0])]);
        drop(plan);assert_eq!(memory.used_bytes(),0);
    }

    #[test]
    fn a_descending_plan_hands_out_the_same_tasks_in_reverse() {
        let memory = MemoryBudget::new(1 << 20);
        let child = root().child(0,1);
        let ranges = [root(),child,child.child(1,2),root().child(0,3).child(1,0)];
        let tasks = |descending| {
            let mut plan = ScanTaskPlan::new(ranges.into_iter(),descending,&memory).unwrap();
            let mut tasks = Vec::new();
            while let Some(task) = plan.next().unwrap() { tasks.push((task.range,task.pages.clone())); }
            tasks
        };
        let ascending = tasks(false);
        assert!(ascending.windows(2).all(|pair| pair[0].0.upper < pair[1].0.lower));
        let mut descending = tasks(true);
        descending.reverse();
        assert_eq!(descending,ascending);
        assert_eq!(memory.used_bytes(),0);
    }

    #[test]
    fn task_retains_its_index_container_after_plan_drop() {
        let memory = MemoryBudget::new(1 << 20);
        let mut plan = ScanTaskPlan::new([root()].into_iter(),false,&memory).unwrap();
        let task = plan.next().unwrap().unwrap();
        drop(plan);assert!(memory.used_bytes() > 0);
        drop(task);assert_eq!(memory.used_bytes(),0);
    }

    #[test]
    fn deepest_iid_partition_and_allocation_failure_release_plan_state() {
        let memory = MemoryBudget::new(1 << 20);
        let iid = Iid([255;16]);
        let mut plan = ScanTaskPlan::new([root(),PageRange { lower: iid,upper: iid }].into_iter(),false,&memory).unwrap();
        let mut count = 0;
        while let Some(task) = plan.next().unwrap() { count += 1;if task.range.lower == iid { assert_eq!(task.pages,[0,1]); } }
        assert_eq!(count,193);drop(plan);assert_eq!(memory.used_bytes(),0);
        let small = MemoryBudget::new(32);
        assert!(ScanTaskPlan::new([root()].into_iter(),false,&small).is_err());assert_eq!(small.used_bytes(),0);
    }
}
