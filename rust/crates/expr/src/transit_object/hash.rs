use std::sync::Arc;
use rtdb_types::Result;
use super::{Object,invalid};

pub(super) fn string(text: &str) -> i32 { text.encode_utf16().fold(0i32,|hash,ch|hash.wrapping_mul(31).wrapping_add(i32::from(ch))) }
pub(super) fn int(value: i32) -> i32 { if value == 0 { 0 } else { rtdb_columnar::murmur3_32(&value.to_le_bytes(),0) as i32 } }
fn combine(seed: i32,hash: i32) -> i32 { seed ^ hash.wrapping_add(0x9e37_79b9u32 as i32).wrapping_add(seed << 6).wrapping_add(seed >> 2) }
fn symbol(text: &str,equivalent: bool) -> i32 {
    let (namespace,name) = if text == "/" { (None,text) } else { text.split_once('/').map_or((None,text),|(namespace,name)|(Some(namespace),name)) };
    let name = if equivalent { rtdb_columnar::murmur3_32(&name.encode_utf16().flat_map(u16::to_le_bytes).collect::<Vec<_>>(),0) as i32 } else { string(name) };
    combine(name,namespace.map_or(0,string))
}
pub(super) fn collection(hash: i32,count: usize) -> i32 {
    let hash = (hash as u32).wrapping_mul(0xcc9e_2d51).rotate_left(15).wrapping_mul(0x1b87_3593);
    let mut hash = hash.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64) ^ count as u32;
    hash ^= hash >> 16;hash = hash.wrapping_mul(0x85eb_ca6b);
    hash ^= hash >> 13;hash = hash.wrapping_mul(0xc2b2_ae35);(hash ^ (hash >> 16)) as i32
}
pub(super) fn identity<T>(value: &Arc<T>) -> i32 { let pointer = Arc::as_ptr(value) as usize as u64;(pointer ^ (pointer >> 32)) as i32 }

impl Object {
    pub(super) fn java_hash_code(&self) -> Result<i32> { self.object_hash(false) }
    pub(super) fn hasheq(&self) -> Result<i32> { self.object_hash(true) }
    fn object_hash(&self,equivalent: bool) -> Result<i32> {
        Ok(match self {
            Self::Null => 0,Self::Bool(value) => if *value { 1231 } else { 1237 },
            Self::Number(value) => if equivalent { value.hasheq().map_err(super::number::error)? } else { value.java_hash_code() },
            Self::String(value) => if equivalent { int(string(value)) } else { string(value) },
            Self::Keyword(value) => symbol(value,equivalent).wrapping_add(0x9e37_79b9u32 as i32),
            Self::Symbol(value) => symbol(value,equivalent),Self::Character(value) => i32::from(*value),
            Self::Bytes(value) => identity(value),Self::Throwable(value) => super::Throwable::hash(value)?,Self::Link(value) => identity(value),
            Self::Uri(value) => rtdb_types::UriParts::parse(value)?.java_hash_code(),
            Self::Uuid([left,right]) => { let hash = *left ^ *right;(hash ^ ((hash as u64 >> 32) as i64)) as i32 },
            Self::ByteBuffer(value) => value.iter().rev().fold(1i32,|hash,value|hash.wrapping_mul(31).wrapping_add(i32::from(*value as i8))),
            Self::KeyFn(value) => string(value),
            Self::ClojureForm(value) => value.java_hash_code()?,
            Self::Time(value) => value.java_hash()?,
            Self::Path(value) => value.java_hash(),
            Self::TableRef(value) => string(&value.schema).wrapping_mul(31).wrapping_add(string(&value.table)),
            Self::VectorType(value) => value.java_hash(),
            Self::Reg(value) => value.oid,
            Self::Vector(values) | Self::List(values) => {
                let hash = values.iter().try_fold(1i32,|hash,value|Ok::<_,rtdb_types::Anomaly>(hash.wrapping_mul(31).wrapping_add(value.object_hash(equivalent)?)))?;
                if equivalent { collection(hash,values.len()) } else { hash }
            }
            Self::Set(values) => {
                let hash = values.entries().try_fold(0i32,|hash,(value,_)|Ok::<_,rtdb_types::Anomaly>(hash.wrapping_add(value.object_hash(equivalent)?)))?;
                if equivalent { collection(hash,values.len()) } else { hash }
            }
            Self::Map(_) | Self::TxKey(_) => {
                let mut count = 0;
                let hash = self.entries().ok_or_else(invalid)?.try_fold(0i32,|hash,(key,value)| {
                    count += 1;
                    let (key,value) = (key.object_hash(equivalent)?,value.object_hash(equivalent)?);
                    let entry = if equivalent { collection(31i32.wrapping_add(key).wrapping_mul(31).wrapping_add(value),2) } else { key ^ value };
                    Ok::<_,rtdb_types::Anomaly>(hash.wrapping_add(entry))
                })?;
                if equivalent { collection(hash,count) ^ if matches!(self,Self::TxKey(_)) { symbol("xtdb.serde.TxKey",true) } else { 0 } } else { hash }
            }
            Self::Tagged(value) => value.hash_code()?,
        })
    }
}
