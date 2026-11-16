use std::collections::BTreeMap;
use rtdb_types::Result;
use super::{Object,invalid};

type Entry = (Object,Object);
#[derive(Clone,Debug)]
pub(crate) enum Map { Array(Vec<Entry>),Hash(Hash) }
#[derive(Clone,Debug,Default)]
pub(crate) struct Hash { null: Option<Box<Entry>>,buckets: BTreeMap<u64,Vec<Entry>> }
#[derive(Clone,Debug)]
pub(crate) struct Record { fields: Box<[Entry;2]>,extension: Map }
pub(super) enum Array { Vector(Vec<Object>),List(Vec<Object>),Set(Map),Composite { map: Map,key: Option<Object> } }

impl Array {
    pub(super) fn new(tag: Option<&str>,size: usize) -> Result<Self> {
        Ok(match tag { None => Self::Vector(Vec::with_capacity(size)),Some("list") => Self::List(Vec::with_capacity(size)),Some("set") => Self::Set(Map::set()),Some("cmap") => Self::Composite { map: Map::new(),key: None },_ => return Err(invalid()) })
    }
    pub(super) fn push(&mut self,value: Object) -> Result<()> {
        match self {
            Self::Vector(values) | Self::List(values) => values.push(value),
            Self::Set(map) => map.assoc(value,Object::Null)?,
            Self::Composite { map,key } => if let Some(key) = key.take() { map.assoc(key,value)?; } else { *key = Some(value); },
        }Ok(())
    }
    pub(super) fn finish(self) -> Object { match self { Self::Vector(values) => Object::Vector(values),Self::List(values) => Object::List(values),Self::Set(map) => Object::Set(map),Self::Composite { map,.. } => Object::Map(map) } }
}

pub(super) struct Entries<'a> { fields: std::slice::Iter<'a,Entry>,map: MapEntries<'a> }
enum MapEntries<'a> {
    Array(std::slice::Iter<'a,Entry>),
    Hash { null: Option<&'a Entry>,buckets: std::collections::btree_map::Values<'a,u64,Vec<Entry>>,entries: std::slice::Iter<'a,Entry> },
}
impl<'a> Iterator for Entries<'a> {
    type Item = (&'a Object,&'a Object);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key,value)) = self.fields.next() { return Some((key,value)); }
        match &mut self.map {
            MapEntries::Array(entries) => entries.next().map(|(key,value)|(key,value)),
            MapEntries::Hash { null,buckets,entries } => {
                if let Some((key,value)) = null.take() { return Some((key,value)); }
                loop {
                    if let Some((key,value)) = entries.next() { return Some((key,value)); }
                    *entries = buckets.next()?.iter();
                }
            }
        }
    }
}
fn order(hash: i32) -> u64 { (0..7).fold(0u64,|order,index|(order << 5) | u64::from((hash as u32 >> (index * 5)) & 31)) }
fn persistent_key_equal(key: &Object,other: &Object) -> Result<bool> {
    if let (Object::Number(key),Object::Number(other)) = (key,other) { Ok(key.equivalent(other)) }
    else { key.equivalent(other) }
}

impl Map {
    pub(super) fn new() -> Self { Self::Array(Vec::new()) }
    pub(super) fn set() -> Self { Self::Hash(Hash::default()) }
    pub(super) fn from_entries(entries: impl IntoIterator<Item=Entry>) -> Result<Self> {
        let mut map = Self::new();for (key,value) in entries { map.assoc(key,value)?; }Ok(map)
    }
    pub(super) fn len(&self) -> usize { match self { Self::Array(entries) => entries.len(),Self::Hash(hash) => usize::from(hash.null.is_some()) + hash.buckets.values().map(Vec::len).sum::<usize>() } }
    pub(super) fn is_empty(&self) -> bool { self.len() == 0 }
    pub(super) fn entries(&self) -> Entries<'_> {
        Entries { fields: [].iter(),map: match self {
            Self::Array(entries) => MapEntries::Array(entries.iter()),
            Self::Hash(hash) => MapEntries::Hash { null: hash.null.as_deref(),buckets: hash.buckets.values(),entries: [].iter() },
        } }
    }
    pub(super) fn into_entries(self) -> Vec<Entry> {
        match self {
            Self::Array(entries) => entries,
            Self::Hash(hash) => hash.null.into_iter().map(|entry|*entry).chain(hash.buckets.into_values().flatten()).collect(),
        }
    }
    pub(super) fn get(&self,key: &Object) -> Result<Option<&Object>> {
        match self {
            Self::Array(entries) => { for (candidate,value) in entries { if persistent_key_equal(key,candidate)? { return Ok(Some(value)); } }Ok(None) }
            Self::Hash(hash) => {
                if matches!(key,Object::Null) { return Ok(hash.null.as_ref().map(|entry|&entry.1)); }
                if hash.buckets.is_empty() { return Ok(None); }
                if let Some(entries) = hash.buckets.get(&order(key.hasheq()?)) { for (candidate,value) in entries { if key.equivalent(candidate)? { return Ok(Some(value)); } } }
                Ok(None)
            }
        }
    }
    pub(super) fn assoc(&mut self,key: Object,value: Object) -> Result<()> {
        if let Self::Array(entries) = self {
            for (candidate,current) in entries.iter_mut() { if candidate.equivalent(&key)? { *current = value;return Ok(()); } }
            if entries.len() < 8 { entries.push((key,value));return Ok(()); }
            let old = std::mem::take(entries);let mut hashed = Self::set();
            for (key,value) in old { hashed.assoc(key,value)?; }*self = hashed;
        }
        let Self::Hash(hash) = self else { unreachable!() };
        if matches!(key,Object::Null) { hash.null = Some(Box::new((key,value)));return Ok(()); }
        let entries = hash.buckets.entry(order(key.hasheq()?)).or_default();
        for (candidate,current) in entries.iter_mut() { if key.equivalent(candidate)? { *current = value;return Ok(()); } }
        entries.push((key,value));Ok(())
    }
    pub(super) fn remove_keyword(&mut self,name: &str) -> Option<Object> {
        let take = |entries: &mut Vec<Entry>| entries.iter().position(|(key,_)|matches!(key,Object::Keyword(key) if key == name)).map(|index|entries.remove(index).1);
        match self {
            Self::Array(entries) => take(entries),
            Self::Hash(hash) => { let value = hash.buckets.values_mut().find_map(take);hash.buckets.retain(|_,entries|!entries.is_empty());value }
        }
    }
    pub(super) fn conj(&mut self,value: Object) -> Result<()> {
        match value {
            Object::Vector(mut pair) => { if pair.len() != 2 { return Err(invalid()); }let value = pair.pop().unwrap();self.assoc(pair.pop().unwrap(),value)?; }
            Object::Map(map) => for (key,value) in map.into_entries() { self.assoc(key,value)?; },
            Object::TxKey(record) => for (key,value) in record.into_entries() { self.assoc(key,value)?; },
            value if empty_sequence(&value) => {},_ => return Err(invalid()),
        }Ok(())
    }
}
impl Record {
    pub(super) fn read(value: Object) -> Result<Self> {
        let mut map = match value {
            Object::Map(map) => map,
            Object::TxKey(record) => Map::from_entries(record.into_entries())?,
            Object::Vector(values) | Object::List(values) => { let mut map = Map::new();for value in values { map.conj(value)?; }map }
            Object::Set(values) => { let mut map = Map::new();for (value,_) in values.into_entries() { map.conj(value)?; }map }
            value if empty_sequence(&value) => Map::new(),_ => return Err(invalid()),
        };
        let tx_id = map.remove_keyword("tx-id").unwrap_or(Object::Null);
        let system_time = map.remove_keyword("system-time").unwrap_or(Object::Null);
        Ok(Self { fields: Box::new([(Object::Keyword("tx-id".into()),tx_id),(Object::Keyword("system-time".into()),system_time)]),extension: map })
    }
    pub(super) fn entries(&self) -> Entries<'_> { let mut entries = self.extension.entries();entries.fields = self.fields.iter();entries }
    pub(super) fn equivalent(&self,other: &Self) -> Result<bool> {
        for ((_,left),(_,right)) in self.fields.iter().zip(other.fields.iter()) { if !left.equivalent(right)? { return Ok(false); } }
        if self.extension.len() != other.extension.len() { return Ok(false); }
        for (key,value) in self.extension.entries() {
            let Some(candidate) = other.extension.get(key)? else { return Ok(false); };
            if !value.equivalent(candidate)? { return Ok(false); }
        }Ok(true)
    }
    pub(super) fn into_entries(self) -> Vec<Entry> { (*self.fields).into_iter().chain(self.extension.into_entries()).collect() }
    pub(super) fn get(&self,key: &Object) -> Result<Option<&Object>> {
        for (candidate,value) in self.fields.iter() { if key.equivalent(candidate)? { return Ok(Some(value)); } }
        self.extension.get(key)
    }
    pub(super) fn assoc_keyword(&mut self,name: &str,value: Object) -> Result<()> {
        for (key,current) in self.fields.iter_mut() { if matches!(key,Object::Keyword(key) if key == name) { *current = value;return Ok(()); } }
        self.extension.assoc(Object::Keyword(name.into()),value)
    }
    pub(super) fn strip_error_metadata(&mut self) { for name in ["xtdb.error/cause","xtdb.error/code","cognitect.anomalies/category"] { self.extension.remove_keyword(name); } }
    pub(super) fn take_error_cause(&mut self) -> Option<Object> { self.extension.remove_keyword("xtdb.error/cause") }
}
pub(super) fn empty_sequence(value: &Object) -> bool {
    match value { Object::Null => true,Object::String(value) => value.is_empty(),Object::Bytes(value) => value.is_empty(),Object::List(value) => value.is_empty(),Object::Set(value) => value.is_empty(),_ => false }
}
