use super::*;
use std::sync::Arc;
use rtdb_types::{MemoryBudget,MemoryReservation};

#[derive(Clone,Copy,Debug)]
struct Key { id:i32, hash:i32, stamp:i32 }
fn key(id:i32,hash:i32)->Key {Key{id,hash,stamp:id}}
struct Budget {memory:Arc<MemoryBudget>,calls:usize,deny:bool}
impl Budget {fn new()->Self {Self{memory:MemoryBudget::new(usize::MAX),calls:0,deny:false}}}
impl Admission for Budget {
    type Lease=Arc<MemoryReservation>;
    fn reserve(&mut self,bytes:usize)->Result<Self::Lease> {self.calls+=1;if self.deny {return Err(invalid("Rejected identity admission"));}self.memory.reserve(bytes)}
}
#[derive(Default)]
struct Ops {calls:Vec<i32>,reject:Option<i32>}
impl IdentityOps<Key,Budget> for Ops {
    fn same(&self,key:&Key,stored:&Key)->bool {key.id==stored.id}
    fn hash(&mut self,key:&Key,_:&mut Budget)->Result<i32> {self.calls.push(key.id);if self.reject==Some(key.id) {Err(invalid("Rejected identity hash"))} else {Ok(key.hash)}}
}
type Table=IdentityTable<Key,i32,Arc<MemoryReservation>>;

#[test]
fn default_identity_tables_allocate_before_use_and_empty_lookup_always_hashes() {
    let mut budget=Budget::new();let mut ops=Ops::default();let table=Table::new(&mut budget).unwrap();assert_eq!(table.table.len(),32);assert_eq!(budget.calls,1);assert!(budget.memory.used_bytes()>0);assert!(!table.contains_key(&key(7,1),&mut ops,&mut budget).unwrap());assert!(table.get(&key(7,1),&mut ops,&mut budget).unwrap().is_none());assert_eq!(ops.calls,vec![7,7]);
    let mut denied=Budget::new();denied.deny=true;assert!(Table::new(&mut denied).is_err());assert_eq!(denied.calls,1);assert_eq!(denied.memory.used_bytes(),0);
}

#[test]
fn identity_replacement_preserves_the_first_key_and_equal_payloads_do_not_define_identity() {
    let mut budget=Budget::new();let mut ops=Ops::default();let mut table=Table::new(&mut budget).unwrap();table.put(key(1,0),7,&mut ops,&mut budget).unwrap();table.put(key(2,0),7,&mut ops,&mut budget).unwrap();assert_eq!(table.len,2);let replacement=Key{stamp:99,..key(1,0)};assert_eq!(table.put(replacement,9,&mut ops,&mut budget).unwrap(),Some(7));assert_eq!(table.len,2);assert_eq!(table.table.iter().flatten().find(|entry|entry.key.id==1).unwrap().key.stamp,1);assert_eq!(table.get(&key(1,0),&mut ops,&mut budget).unwrap(),Some(&9));assert_eq!(table.get(&key(2,0),&mut ops,&mut budget).unwrap(),Some(&7));
}

#[test]
fn resize_rehashes_old_slots_before_retrying_the_incoming_key() {
    let mut budget=Budget::new();let mut ops=Ops::default();let mut table=Table::new(&mut budget).unwrap();for id in 0..21 {table.put(key(id,0),id,&mut ops,&mut budget).unwrap();}assert_eq!(table.table.len(),32);ops.calls.clear();table.put(key(21,0),21,&mut ops,&mut budget).unwrap();let mut expected=vec![21];expected.extend(0..21);expected.push(21);assert_eq!(ops.calls,expected);assert_eq!(table.table.len(),64);assert_eq!(table.len,22);for id in 0..22 {assert_eq!(table.get(&key(id,0),&mut ops,&mut budget).unwrap(),Some(&id));}
}

#[test]
fn close_deletion_rehashes_and_shifts_a_wrapped_collision_run_in_source_order() {
    let mut budget=Budget::new();let mut ops=Ops::default();let mut table=Table::new(&mut budget).unwrap();for id in 10..14 {table.put(key(id,31),id,&mut ops,&mut budget).unwrap();}assert_eq!(table.table[31].as_ref().unwrap().key.id,10);ops.calls.clear();assert_eq!(table.remove(&key(10,31),&mut ops,&mut budget).unwrap(),Some(10));assert_eq!(ops.calls,vec![10,11,12,13]);assert_eq!(table.table[31].as_ref().unwrap().key.id,11);assert_eq!(table.table[0].as_ref().unwrap().key.id,12);assert_eq!(table.table[1].as_ref().unwrap().key.id,13);assert!(table.table[2].is_none());ops.calls.clear();assert_eq!(table.remove(&key(12,31),&mut ops,&mut budget).unwrap(),Some(12));assert_eq!(ops.calls,vec![12,13]);assert_eq!(table.get(&key(13,31),&mut ops,&mut budget).unwrap(),Some(&13));
}

#[test]
fn clear_preserves_warm_capacity_and_leases_without_hashing_or_admission() {
    let mut budget=Budget::new();let memory=Arc::clone(&budget.memory);let mut ops=Ops::default();let mut table=Table::new(&mut budget).unwrap();for id in 0..22 {table.put(key(id,0),id,&mut ops,&mut budget).unwrap();}let requests=budget.calls;let retained=memory.used_bytes();ops.calls.clear();budget.deny=true;table.clear().unwrap();assert!(table.is_empty());assert_eq!(table.table.len(),64);assert!(ops.calls.is_empty());assert_eq!(budget.calls,requests);assert_eq!(memory.used_bytes(),retained);assert!(!table.contains_key(&key(99,0),&mut ops,&mut budget).unwrap());assert_eq!(ops.calls,vec![99]);drop(table);assert_eq!(memory.used_bytes(),0);
}

#[test]
fn read_failures_do_not_poison_but_resize_hash_failure_prevents_transient_reuse() {
    let mut budget=Budget::new();let mut ops=Ops::default();let mut table=Table::new(&mut budget).unwrap();for id in 0..21 {table.put(key(id,0),id,&mut ops,&mut budget).unwrap();}ops.reject=Some(99);assert!(table.get(&key(99,0),&mut ops,&mut budget).is_err());assert!(!table.failed);ops.reject=None;assert_eq!(table.get(&key(7,0),&mut ops,&mut budget).unwrap(),Some(&7));ops.calls.clear();ops.reject=Some(7);assert_eq!(table.put(key(21,0),21,&mut ops,&mut budget).unwrap_err().message.as_deref(),Some("Rejected identity hash"));assert_eq!(ops.calls,vec![21,0,1,2,3,4,5,6,7]);assert!(table.failed);assert!(table.clear().is_err());assert!(table.get(&key(8,0),&mut ops,&mut budget).is_err());drop(table);assert_eq!(budget.memory.used_bytes(),0);
}

#[test]
fn resize_admission_failure_follows_incoming_hash_and_precedes_old_key_rehashes() {
    let mut budget=Budget::new();let mut ops=Ops::default();let mut table=Table::new(&mut budget).unwrap();for id in 0..21 {table.put(key(id,0),id,&mut ops,&mut budget).unwrap();}ops.calls.clear();budget.deny=true;assert_eq!(table.put(key(21,0),21,&mut ops,&mut budget).unwrap_err().message.as_deref(),Some("Rejected identity admission"));assert_eq!(ops.calls,vec![21]);assert!(table.failed);assert!(table.clear().is_err());
}
