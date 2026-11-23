use std::sync::Arc;
use rtdb_columnar::{MemoryBudget,MemoryReservation};
use rtdb_types::{Result,Value};
use crate::invalid;

pub(crate) fn hash_values<'a>(values: impl IntoIterator<Item=&'a Value>) -> Result<u32> {
    values.into_iter().try_fold(0u32,|hash,value| Ok(combine_cell_hash(hash,rtdb_columnar::hash_value(value,0)?)))
}
/// Folds one cell's `hash_value` into a row hash, as `hash_values` does for each of a row's cells in turn.
pub(crate) fn combine_cell_hash(hash: u32,cell: i32) -> u32 {
    let cell = (cell as u32).wrapping_mul(0xcc9e_2d51).rotate_left(15).wrapping_mul(0x1b87_3593);
    (hash ^ cell).rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64)
}

pub(crate) fn partition_count(blocks: usize,minimum: usize) -> Result<usize> {
    if minimum == 0 || minimum > i32::MAX as usize || !minimum.is_power_of_two() { return Err(invalid("join-partitions","Minimum join partitions must be a positive signed power of two")); }
    let blocks = i32::try_from(blocks).map_err(|_|size_error())?;
    let count = if blocks == 0 { 0 } else { (1u32 << (31 - blocks.leading_zeros())).wrapping_shl(1) as i32 };
    Ok(count.max(minimum as i32) as usize)
}

#[derive(Clone,Copy)]
enum Slot { Empty,Occupied { hash: u32,row: usize } }
pub(crate) struct HashIndex { slots: Vec<Slot>,mask: usize,_reservation: Arc<MemoryReservation> }
impl HashIndex {
    pub(crate) fn new(hashes: &[u32],memory: &Arc<MemoryBudget>) -> Result<Self> {
        if hashes.len() > i32::MAX as usize { return Err(size_error()); }
        let expected = (hashes.len() as f64 / 0.6) as u64;
        let capacity = 1usize.checked_shl(u64::BITS - expected.leading_zeros()).ok_or_else(size_error)?;
        let reservation = memory.reserve(capacity.checked_mul(std::mem::size_of::<Slot>()).and_then(|bytes|bytes.checked_add(256)).ok_or_else(size_error)?)?;
        let _temporary = memory.reserve(capacity.checked_mul(std::mem::size_of::<usize>()).ok_or_else(size_error)?)?;
        let mut slots = vec![Slot::Empty;capacity];let mask = capacity - 1;
        let mut skip = (0..capacity).collect::<Vec<_>>();
        for (row,hash) in hashes.iter().enumerate() {
            let masked = *hash as usize & mask;
            let mut index = skip[masked];
            while matches!(slots[index],Slot::Occupied { .. }) {
                let next = (index + 1) & mask;
                skip[index] = skip[next];
                index = skip[next];
            }
            skip[masked] = skip[(index + 1) & mask];
            slots[index] = Slot::Occupied { hash: *hash,row };
        }
        Ok(Self { slots,mask,_reservation: reservation })
    }
    pub fn candidates(&self,hash: u32) -> Candidates<'_> { Candidates { index: self,next: hash as usize & self.mask,hash,done: false } }
}
pub(crate) struct Candidates<'a> { index: &'a HashIndex,next: usize,hash: u32,done: bool }
impl Iterator for Candidates<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        if self.done { return None; }
        loop {
            match self.index.slots[self.next] {
                Slot::Empty => { self.done = true;return None; }
                Slot::Occupied { hash,row } => {
                    self.next = (self.next + 1) & self.index.mask;
                    if hash == self.hash { return Some(row); }
                }
            }
        }
    }
}

fn size_error() -> rtdb_types::Anomaly { invalid("join-size","Join rows, partitions or index exceed supported allocation bounds") }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partition_count_uses_reference_signed_shift_and_probe_minimum() {
        for (blocks,minimum,expected) in [(0,1,1),(1,1,2),(2,1,4),(3,1,4),(4,1,8),(1,8,8),(0,8,8),(1usize << 30,1,1)] {
            assert_eq!(partition_count(blocks,minimum).unwrap(),expected);
        }
        assert!(partition_count(1,3).is_err());assert!(partition_count(1,0).is_err());
    }

    #[test]
    fn hash_index_candidates_keep_insertion_order_through_wrapped_collisions() {
        let memory = MemoryBudget::new(1 << 20);
        let index = HashIndex::new(&[u32::MAX,7,u32::MAX,15,u32::MAX],&memory).unwrap();
        assert_eq!(index.candidates(u32::MAX).collect::<Vec<_>>(),[0,2,4]);
        assert_eq!(index.candidates(7).collect::<Vec<_>>(),[1]);assert_eq!(index.candidates(15).collect::<Vec<_>>(),[3]);
        assert_eq!(index.candidates(23).count(),0);drop(index);assert_eq!(memory.used_bytes(),0);
        let memory = MemoryBudget::new(1);
        assert!(HashIndex::new(&[1],&memory).is_err());assert_eq!(memory.used_bytes(),0);
    }

}
