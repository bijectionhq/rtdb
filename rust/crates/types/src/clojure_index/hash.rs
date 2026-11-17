use super::{AllocationScope,Anomaly,KeySemantics,Result,grow,size_error};

// Clojure 1.12.0 PersistentHashMap transient assoc/createNode and NodeSeq traversal.
// https://github.com/clojure/clojure/blob/clojure-1.12.0/src/jvm/clojure/lang/PersistentHashMap.java
pub(super) struct Hash { null: Option<usize>,root: Option<usize>,nodes: Vec<Node> }
enum Node {
    Bitmap { bitmap: u32,slots: Vec<Slot>,pair_capacity: usize },
    Array([Option<usize>;32]),
    Collision { hash: i32,entries: Vec<usize> },
}
#[derive(Clone,Copy)]
enum Slot { Leaf(usize),Child(usize) }
pub(super) enum Built { Unique(Hash),Duplicate { earlier: usize,incoming: usize } }

struct Builder<'entries,'keys,'scope,T,K> {
    entries: &'entries [T],keys: &'keys mut K,allocation: Option<&'scope AllocationScope>,hash: Hash,
}

pub(super) fn build<T>(entries: &[T],keys: &mut impl KeySemantics<T>,allocation: Option<&AllocationScope>) -> Result<Built> {
    let mut builder = Builder { entries,keys,allocation,hash: Hash { null: None,root: None,nodes: Vec::new() } };
    for incoming in 0..entries.len() {
        if builder.keys.is_null(&entries[incoming]) {
            if let Some(earlier) = builder.hash.null { return Ok(Built::Duplicate { earlier,incoming }); }
            builder.hash.null = Some(incoming);
        } else {
            let hash = builder.keys.hash(&entries[incoming])?;
            let root = match builder.hash.root { Some(root) => root,None => builder.empty()? };
            let (root,duplicate) = builder.assoc(root,0,hash,incoming)?;
            builder.hash.root = Some(root);
            if let Some(earlier) = duplicate { return Ok(Built::Duplicate { earlier,incoming }); }
        }
    }
    Ok(Built::Unique(builder.hash))
}

impl<T,K: KeySemantics<T>> Builder<'_,'_,'_,T,K> {
    fn push(&mut self,node: Node) -> Result<usize> {
        let ordinal = self.hash.nodes.len();grow(&mut self.hash.nodes,ordinal.checked_add(1).ok_or_else(size_error)?,self.allocation)?;
        self.hash.nodes.push(node);Ok(ordinal)
    }
    fn empty(&mut self) -> Result<usize> { self.push(Node::Bitmap { bitmap: 0,slots: Vec::new(),pair_capacity: 0 }) }

    fn assoc(&mut self,node: usize,shift: u32,hash: i32,key: usize) -> Result<(usize,Option<usize>)> {
        if shift > 35 { return Err(unstable_keys()); }
        match &self.hash.nodes[node] {
            Node::Bitmap { .. } if shift > 30 => Err(unstable_keys()),
            Node::Bitmap { .. } => self.bitmap(node,shift,hash,key),
            Node::Array(children) => {
                let index = mask(hash,shift);let child = children[index];
                let child = match child { Some(child) => child,None => self.empty()? };
                let (child,duplicate) = self.assoc(child,shift + 5,hash,key)?;
                let Node::Array(children) = &mut self.hash.nodes[node] else { unreachable!() };children[index] = Some(child);
                Ok((node,duplicate))
            }
            Node::Collision { hash: existing,entries } => {
                if *existing == hash {
                    for index in 0..entries.len() {
                        let earlier = entries[index];
                        if self.keys.equivalent(&self.entries[key],&self.entries[earlier])? { return Ok((node,Some(earlier))); }
                    }
                    let Node::Collision { entries,.. } = &mut self.hash.nodes[node] else { unreachable!() };
                    let required = entries.len().checked_add(1).ok_or_else(size_error)?;
                    grow(entries,required,self.allocation)?;entries.push(key);
                    Ok((node,None))
                } else {
                    if shift > 30 { return Err(unstable_keys()); }
                    let bitmap = bit(*existing,shift);let mut slots = Vec::new();grow(&mut slots,2,self.allocation)?;slots.push(Slot::Child(node));
                    let wrapper = self.push(Node::Bitmap { bitmap,slots,pair_capacity: 2 })?;
                    self.assoc(wrapper,shift,hash,key)
                }
            }
        }
    }

    fn bitmap(&mut self,node: usize,shift: u32,hash: i32,key: usize) -> Result<(usize,Option<usize>)> {
        let Node::Bitmap { bitmap,slots,pair_capacity } = &self.hash.nodes[node] else { unreachable!() };
        let bitmap = *bitmap;let capacity = *pair_capacity;let count = slots.len();let bit = bit(hash,shift);
        let index = (bitmap & bit.wrapping_sub(1)).count_ones() as usize;
        if bitmap & bit != 0 {
            match slots[index] {
                Slot::Child(child) => {
                    let (child,duplicate) = self.assoc(child,shift + 5,hash,key)?;
                    let Node::Bitmap { slots,.. } = &mut self.hash.nodes[node] else { unreachable!() };slots[index] = Slot::Child(child);
                    Ok((node,duplicate))
                }
                Slot::Leaf(earlier) => {
                    if self.keys.equivalent(&self.entries[key],&self.entries[earlier])? { return Ok((node,Some(earlier))); }
                    let child = self.create_node(shift + 5,earlier,hash,key)?;
                    let Node::Bitmap { slots,.. } = &mut self.hash.nodes[node] else { unreachable!() };slots[index] = Slot::Child(child);
                    Ok((node,None))
                }
            }
        } else if count < capacity || count < 16 {
            let required = if count < capacity { capacity } else { count.checked_add(4).ok_or_else(size_error)? };
            let Node::Bitmap { bitmap,slots,pair_capacity } = &mut self.hash.nodes[node] else { unreachable!() };
            grow(slots,required,self.allocation)?;slots.insert(index,Slot::Leaf(key));*bitmap |= bit;*pair_capacity = required;
            Ok((node,None))
        } else {
            let mut children = [None;32];let child = self.empty()?;
            children[mask(hash,shift)] = Some(self.assoc(child,shift + 5,hash,key)?.0);
            let mut old_index = 0;
            for (index,child) in children.iter_mut().enumerate() {
                if bitmap & (1u32 << index) == 0 { continue; }
                let Node::Bitmap { slots,.. } = &self.hash.nodes[node] else { unreachable!() };
                let slot = slots[old_index];old_index += 1;
                *child = Some(match slot {
                    Slot::Child(child) => child,
                    Slot::Leaf(earlier) => {
                        let hash = self.keys.hash(&self.entries[earlier])?;let child = self.empty()?;
                        self.assoc(child,shift + 5,hash,earlier)?.0
                    }
                });
            }
            self.hash.nodes[node] = Node::Array(children);Ok((node,None))
        }
    }

    fn create_node(&mut self,shift: u32,earlier: usize,hash: i32,key: usize) -> Result<usize> {
        let earlier_hash = self.keys.hash(&self.entries[earlier])?;
        if earlier_hash == hash {
            let mut entries = Vec::new();grow(&mut entries,2,self.allocation)?;entries.push(earlier);entries.push(key);
            self.push(Node::Collision { hash,entries })
        } else {
            let node = self.empty()?;let node = self.assoc(node,shift,earlier_hash,earlier)?.0;
            let (node,duplicate) = self.assoc(node,shift,hash,key)?;
            if duplicate.is_some() { return Err(unstable_keys()); }
            Ok(node)
        }
    }
}

fn mask(hash: i32,shift: u32) -> usize { ((hash as u32).wrapping_shr(shift) & 31) as usize }
fn bit(hash: i32,shift: u32) -> u32 { 1u32 << mask(hash,shift) }
fn unstable_keys() -> Anomaly { Anomaly::incorrect("rtdb.types/clojure-index-keys","Collection key semantics changed during construction") }

#[derive(Clone,Copy)]
struct Frame { node: usize,next: usize }
pub(super) struct Order<'a> { hash: &'a Hash,null: Option<usize>,stack: [Frame;8],depth: usize }
impl Hash {
    pub(super) fn order(&self) -> Order<'_> {
        let mut order = Order { hash: self,null: self.null,stack: [Frame { node: 0,next: 0 };8],depth: 0 };
        if let Some(root) = self.root { order.stack[0].node = root;order.depth = 1; }order
    }
}
impl Iterator for Order<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        if let Some(null) = self.null.take() { return Some(null); }
        while self.depth != 0 {
            let frame = &mut self.stack[self.depth - 1];let index = frame.next;frame.next += 1;
            let slot = match &self.hash.nodes[frame.node] {
                Node::Bitmap { slots,.. } => slots.get(index).copied(),
                Node::Collision { entries,.. } => entries.get(index).copied().map(Slot::Leaf),
                Node::Array(children) => match children.get(index) { Some(Some(child)) => Some(Slot::Child(*child)),Some(None) => continue,None => None },
            };
            match slot {
                Some(Slot::Leaf(ordinal)) => return Some(ordinal),
                Some(Slot::Child(node)) => { self.stack[self.depth] = Frame { node,next: 0 };self.depth += 1; }
                None => self.depth -= 1,
            }
        }
        None
    }
}

pub(super) fn allocation_bound(entries: usize) -> Result<usize> {
    if entries == 0 { return Ok(0); }
    let per_entry = 32usize.checked_mul(std::mem::size_of::<Node>())
        .and_then(|bytes|512usize.checked_mul(std::mem::size_of::<Slot>()).and_then(|slots|bytes.checked_add(slots)))
        .and_then(|bytes|bytes.checked_add(4 * std::mem::size_of::<usize>()))
        .and_then(|bytes|bytes.checked_add(48 * 256)).ok_or_else(size_error)?;
    entries.checked_mul(per_entry).and_then(|bytes|bytes.checked_add(256)).ok_or_else(size_error)
}
