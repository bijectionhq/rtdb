use super::*;
use crate::MemoryBudget;
use std::sync::{Arc,atomic::{AtomicUsize,Ordering}};

struct Key { ordinal: usize,hash: i32,equality: usize,null: bool,drops: Arc<AtomicUsize> }
impl Drop for Key { fn drop(&mut self) { self.drops.fetch_add(1,Ordering::Relaxed); } }
#[derive(Clone,Debug,PartialEq,Eq)]
enum Event { Hash(usize),Equal(usize,usize) }
#[derive(Default)]
struct Semantics { events: Vec<Event>,fail_at: Option<usize> }
impl Semantics {
    fn event(&mut self,event: Event) -> Result<()> {
        let index = self.events.len();self.events.push(event);
        if self.fail_at == Some(index) { Err(callback_error()) } else { Ok(()) }
    }
}
impl KeySemantics<Key> for Semantics {
    fn is_null(&self,key: &Key) -> bool { key.null }
    fn hash(&mut self,key: &Key) -> Result<i32> { self.event(Event::Hash(key.ordinal))?;Ok(key.hash) }
    fn equivalent(&mut self,left: &Key,right: &Key) -> Result<bool> {
        self.event(Event::Equal(left.ordinal,right.ordinal))?;Ok(left.equality == right.equality)
    }
}
fn callback_error() -> Anomaly { Anomaly::fault("test/key-callback","Key callback failed") }
fn input(hashes: &[i32],drops: &Arc<AtomicUsize>,scope: Option<&AllocationScope>) -> Vec<Key> {
    if let Some(scope) = scope { scope.reserve(hashes.len() * std::mem::size_of::<Key>()).unwrap(); }
    hashes.iter().enumerate().map(|(ordinal,hash)|Key { ordinal,hash: *hash,equality: ordinal,null: false,drops: Arc::clone(drops) }).collect()
}
fn ordinals(index: &ClojureKeyIndex<'_,Key>) -> Vec<usize> { index.entries().map(|(ordinal,key)| { assert_eq!(ordinal,key.ordinal);ordinal }).collect() }

#[test]
fn checked_maps_switch_factory_after_eight_entries() {
    for count in [0,8,9] {
        let drops = Arc::new(AtomicUsize::new(0));let mut semantics = Semantics::default();
        let hashes: Vec<_> = (0..count as i32).collect();
        let CheckedIndex::Unique(index) = ClojureKeyIndex::checked_map(input(&hashes,&drops,None),&mut semantics,None).unwrap() else { panic!("unique") };
        assert_eq!(index.len(),count);assert_eq!(index.is_empty(),count == 0);assert_eq!(ordinals(&index),(0..count).collect::<Vec<_>>());
        let expected: Vec<_> = if count <= 8 { (0..count).flat_map(|left|(left + 1..count).map(move |right|Event::Equal(left,right))).collect() }
            else { (0..count).map(Event::Hash).collect() };
        assert_eq!(semantics.events,expected);assert_eq!(drops.load(Ordering::Relaxed),0);drop(index);assert_eq!(drops.load(Ordering::Relaxed),count);
    }
}

#[test]
fn array_duplicates_select_outer_loop_key_and_hash_duplicates_select_incoming_key() {
    for count in [4,9] {
        let drops = Arc::new(AtomicUsize::new(0));let mut hashes: Vec<_> = (0..count as i32).collect();hashes[2] = 1;hashes[3] = 0;
        let mut entries = input(&hashes,&drops,None);entries[2].equality = 1;entries[3].equality = 0;
        let mut semantics = Semantics::default();
        let CheckedIndex::Duplicate(duplicate) = ClojureKeyIndex::checked_map(entries,&mut semantics,None).unwrap() else { panic!("duplicate") };
        if count == 4 {
            assert_eq!((duplicate.earlier_ordinal(),duplicate.incoming_ordinal(),duplicate.diagnostic_key().ordinal),(0,3,0));
            assert_eq!(semantics.events,[Event::Equal(0,1),Event::Equal(0,2),Event::Equal(0,3)]);
        } else {
            assert_eq!((duplicate.earlier_ordinal(),duplicate.incoming_ordinal(),duplicate.diagnostic_key().ordinal),(1,2,2));
            assert_eq!(semantics.events,[Event::Hash(0),Event::Hash(1),Event::Hash(2),Event::Equal(2,1)]);
        }
        assert_eq!(drops.load(Ordering::Relaxed),0);drop(duplicate);assert_eq!(drops.load(Ordering::Relaxed),count);
    }
}

#[test]
fn hashed_null_keys_bypass_callbacks_and_iterate_first() {
    let drops = Arc::new(AtomicUsize::new(0));let mut entries = input(&[1,99,0],&drops,None);entries[1].null = true;
    let mut semantics = Semantics::default();
    let CheckedIndex::Unique(index) = ClojureKeyIndex::checked_set(entries,&mut semantics,None).unwrap() else { panic!("unique") };
    assert_eq!(semantics.events,[Event::Hash(0),Event::Hash(2)]);assert_eq!(ordinals(&index),[1,2,0]);
    let mut entries = input(&[99,99,0],&drops,None);entries[0].null = true;entries[1].null = true;
    let mut semantics = Semantics { fail_at: Some(0),..Default::default() };
    let CheckedIndex::Duplicate(duplicate) = ClojureKeyIndex::checked_set(entries,&mut semantics,None).unwrap() else { panic!("duplicate") };
    assert!(semantics.events.is_empty());assert_eq!((duplicate.earlier_ordinal(),duplicate.incoming_ordinal(),duplicate.diagnostic_key().ordinal),(0,1,1));
}

#[test]
fn prefix_collisions_compare_before_resident_rehash_even_when_full_hashes_differ() {
    let drops = Arc::new(AtomicUsize::new(0));let mut semantics = Semantics::default();
    let CheckedIndex::Unique(index) = ClojureKeyIndex::checked_set(input(&[0,32],&drops,None),&mut semantics,None).unwrap() else { panic!("unique") };
    assert_eq!(semantics.events,[Event::Hash(0),Event::Hash(1),Event::Equal(1,0),Event::Hash(0)]);assert_eq!(ordinals(&index),[0,1]);
    let mut entries = input(&[0,32],&drops,None);entries[1].equality = 0;let mut semantics = Semantics::default();
    let CheckedIndex::Duplicate(duplicate) = ClojureKeyIndex::checked_set(entries,&mut semantics,None).unwrap() else { panic!("duplicate") };
    assert_eq!(semantics.events,[Event::Hash(0),Event::Hash(1),Event::Equal(1,0)]);assert_eq!(duplicate.diagnostic_key().ordinal,1);
}

#[test]
fn full_hash_collisions_keep_insertion_order_and_wrap_without_comparing_other_hashes() {
    let drops = Arc::new(AtomicUsize::new(0));let mut semantics = Semantics::default();
    let CheckedIndex::Unique(index) = ClojureKeyIndex::checked_set(input(&[0,0,0,32],&drops,None),&mut semantics,None).unwrap() else { panic!("unique") };
    assert_eq!(semantics.events,[Event::Hash(0),Event::Hash(1),Event::Equal(1,0),Event::Hash(0),Event::Hash(2),Event::Equal(2,0),Event::Equal(2,1),Event::Hash(3)]);
    assert_eq!(ordinals(&index),[0,1,2,3]);
    let mut entries = input(&[0,0,0],&drops,None);entries[2].equality = 1;let mut semantics = Semantics::default();
    let CheckedIndex::Duplicate(duplicate) = ClojureKeyIndex::checked_set(entries,&mut semantics,None).unwrap() else { panic!("duplicate") };
    assert_eq!((duplicate.earlier_ordinal(),duplicate.incoming_ordinal()),(1,2));
    assert_eq!(&semantics.events[4..],[Event::Hash(2),Event::Equal(2,0),Event::Equal(2,1)]);
}

#[test]
fn fresh_bitmap_promotes_on_seventeenth_slot_and_rehashes_old_leaves_in_slot_order() {
    let drops = Arc::new(AtomicUsize::new(0));let mut semantics = Semantics::default();let hashes: Vec<_> = (0..17).collect();
    let CheckedIndex::Unique(index) = ClojureKeyIndex::checked_set(input(&hashes,&drops,None),&mut semantics,None).unwrap() else { panic!("unique") };
    assert_eq!(semantics.events,(0..17).chain(0..16).map(Event::Hash).collect::<Vec<_>>());assert_eq!(ordinals(&index),(0..17).collect::<Vec<_>>());
}

#[test]
fn array_nodes_keep_existing_children_and_admit_new_children_in_call_order() {
    let hashes: Vec<_> = (0..18).chain([32,0]).collect();let drops = Arc::new(AtomicUsize::new(0));let mut semantics = Semantics::default();
    let CheckedIndex::Unique(index) = ClojureKeyIndex::checked_set(input(&hashes,&drops,None),&mut semantics,None).unwrap() else { panic!("unique") };
    let mut expected: Vec<_> = (0..17).chain(0..16).map(Event::Hash).collect();
    expected.extend([Event::Hash(17),Event::Hash(18),Event::Hash(19),Event::Equal(19,0),Event::Hash(0)]);
    assert_eq!(semantics.events,expected);assert_eq!(ordinals(&index),[0,19,18].into_iter().chain(1..18).collect::<Vec<_>>());
}

#[test]
fn collision_wrapper_spare_slots_delay_promotion_until_nineteenth_slot() {
    for occupied in [17,18,19] {
        let hashes: Vec<_> = [0,0].into_iter().chain((1..occupied).map(|value|value * 32)).collect();
        let drops = Arc::new(AtomicUsize::new(0));let mut semantics = Semantics::default();
        let CheckedIndex::Unique(index) = ClojureKeyIndex::checked_set(input(&hashes,&drops,None),&mut semantics,None).unwrap() else { panic!("unique") };
        let mut expected = vec![Event::Hash(0),Event::Hash(1),Event::Equal(1,0),Event::Hash(0)];
        expected.extend((2..hashes.len()).map(Event::Hash));
        if occupied == 19 { expected.extend((2..hashes.len() - 1).map(Event::Hash)); }
        assert_eq!(semantics.events,expected);assert_eq!(ordinals(&index),(0..hashes.len()).collect::<Vec<_>>());
    }
}

#[test]
fn high_hash_bits_and_deep_collisions_fit_the_fixed_traversal_stack() {
    let drops = Arc::new(AtomicUsize::new(0));let mut semantics = Semantics::default();
    let CheckedIndex::Unique(index) = ClojureKeyIndex::checked_set(input(&[0,i32::MIN,i32::MIN],&drops,None),&mut semantics,None).unwrap() else { panic!("unique") };
    let mut expected = vec![Event::Hash(0),Event::Hash(1)];
    for _ in 0..6 { expected.extend([Event::Equal(1,0),Event::Hash(0)]); }
    expected.extend([Event::Hash(2),Event::Equal(2,1),Event::Hash(1)]);
    assert_eq!(semantics.events,expected);assert_eq!(ordinals(&index),[0,1,2]);
    let CheckedIndex::Unique(index) = ClojureKeyIndex::checked_set(input(&[-1,0,i32::MIN,1 << 30],&drops,None),&mut Semantics::default(),None).unwrap() else { panic!("unique") };
    assert_eq!(ordinals(&index),[1,3,2,0]);
}

#[test]
fn every_hash_and_equality_failure_stops_at_its_exact_callback_and_drops_inputs() {
    for hashes in [vec![0,32],vec![0,0,0],(0..17).collect(),[0,0].into_iter().chain((1..19).map(|value|value * 32)).collect(),vec![0,i32::MIN,i32::MIN]] {
        let drops = Arc::new(AtomicUsize::new(0));let mut complete = Semantics::default();
        drop(ClojureKeyIndex::checked_set(input(&hashes,&drops,None),&mut complete,None).unwrap());
        for failure in 0..complete.events.len() {
            let drops = Arc::new(AtomicUsize::new(0));let mut semantics = Semantics { fail_at: Some(failure),..Default::default() };
            let error = ClojureKeyIndex::checked_set(input(&hashes,&drops,None),&mut semantics,None).err().expect("callback error");
            assert_eq!(error,callback_error());assert_eq!(semantics.events,complete.events[..=failure]);assert_eq!(drops.load(Ordering::Relaxed),hashes.len());
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));let mut semantics = Semantics { fail_at: Some(1),..Default::default() };
    assert_eq!(ClojureKeyIndex::checked_map(input(&[0,1,2],&drops,None),&mut semantics,None).err().expect("callback error"),callback_error());
    assert_eq!(semantics.events,[Event::Equal(0,1),Event::Equal(0,2)]);assert_eq!(drops.load(Ordering::Relaxed),3);
}

fn index_charge(hashes: &[i32]) -> usize {
    let memory = MemoryBudget::new(1 << 24);let charge;
    {
        let scope = AllocationScope::new(&memory).unwrap();let drops = Arc::new(AtomicUsize::new(0));let entries = input(hashes,&drops,Some(&scope));
        let initial = memory.used_bytes();let result = ClojureKeyIndex::checked_set(entries,&mut Semantics::default(),Some(&scope)).unwrap();
        charge = memory.used_bytes() - initial;drop(result);assert_eq!(drops.load(Ordering::Relaxed),hashes.len());assert!(memory.used_bytes() > 0);
    }
    assert_eq!(memory.used_bytes(),0);charge
}

#[test]
fn growth_reserves_new_capacity_while_old_charges_remain_live() {
    for (prefix,hashes,headroom) in [
        (vec![],vec![0],0),
        (vec![0,1 << 15],vec![0,1 << 15,i32::MIN],0),
        (vec![0,0,0,0],vec![0,0,0,0,0],256 + 4 * std::mem::size_of::<usize>()),
        ((0..16).collect(),(0..17).collect(),0),
    ] {
        let limit = 4096 + 256 + hashes.len() * std::mem::size_of::<Key>() + index_charge(&prefix) + headroom;
        let memory = MemoryBudget::new(limit);let drops = Arc::new(AtomicUsize::new(0));
        {
            let scope = AllocationScope::new(&memory).unwrap();let entries = input(&hashes,&drops,Some(&scope));let mut semantics = Semantics::default();
            let error = ClojureKeyIndex::checked_set(entries,&mut semantics,Some(&scope)).err().expect("growth exceeds budget");
            assert_eq!(error.code,"rtdb/memory-budget-exceeded");assert!(memory.used_bytes() <= limit);assert_eq!(drops.load(Ordering::Relaxed),hashes.len());
        }
        assert_eq!(memory.used_bytes(),0);
    }
}

#[test]
fn cumulative_storage_bound_covers_trie_collisions_and_scope_bookkeeping() {
    for hashes in [vec![],vec![0],vec![0;129],(0..257).collect(),[0,0].into_iter().chain((1..65).map(|value|value * 32)).collect(),vec![0,i32::MIN,i32::MIN]] {
        let bound = ClojureKeyIndex::<Key>::allocation_bound(hashes.len()).unwrap();assert!(index_charge(&hashes) <= bound);
    }
    assert!(ClojureKeyIndex::<Key>::allocation_bound(usize::MAX).is_err());
}

#[test]
fn unaddressable_native_capacity_is_rejected_before_allocation() {
    let mut values = Vec::<usize>::new();
    let required = isize::MAX as usize / std::mem::size_of::<usize>() + 1;
    assert_eq!(grow(&mut values,required,None).unwrap_err(),size_error());
    assert_eq!(values.capacity(),0);
}

#[test]
fn completed_and_duplicate_owners_keep_scoped_inputs_until_their_drop() {
    for duplicate in [false,true] {
        let memory = MemoryBudget::new(1 << 20);let drops = Arc::new(AtomicUsize::new(0));
        {
            let scope = AllocationScope::new(&memory).unwrap();let mut entries = input(&[0,1,2],&drops,Some(&scope));
            if duplicate { entries[1].equality = 0; }
            let result = ClojureKeyIndex::checked_map(entries,&mut Semantics::default(),Some(&scope)).unwrap();
            assert_eq!(drops.load(Ordering::Relaxed),0);assert!(memory.used_bytes() > 4096);drop(result);
            assert_eq!(drops.load(Ordering::Relaxed),3);assert!(memory.used_bytes() > 4096);
        }
        assert_eq!(memory.used_bytes(),0);
    }
}
