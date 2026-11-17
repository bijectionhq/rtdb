use std::sync::Arc;
use rtdb_types::Result;
use super::{Object,invalid};

impl Object {
    pub(crate) fn equivalent(&self,other: &Self) -> Result<bool> { self.object_equal(other,false) }
    pub(super) fn java_equal(&self,other: &Self) -> Result<bool> { self.object_equal(other,true) }
    fn object_equal(&self,other: &Self,java: bool) -> Result<bool> {
        Ok(match (self,other) {
            (Self::Null,Self::Null) => true,(Self::Bool(left),Self::Bool(right)) => left == right,
            (Self::Number(left),Self::Number(right)) => if java { left.java_equal(right) } else { Arc::ptr_eq(left,right) || left.equivalent(right) },
            (Self::String(left),Self::String(right)) | (Self::Keyword(left),Self::Keyword(right)) | (Self::Symbol(left),Self::Symbol(right)) | (Self::KeyFn(left),Self::KeyFn(right)) => left == right,
            (Self::Uri(left),Self::Uri(right)) => rtdb_types::UriParts::parse(left)?.equivalent(&rtdb_types::UriParts::parse(right)?),
            (Self::Character(left),Self::Character(right)) => left == right,(Self::Uuid(left),Self::Uuid(right)) => left == right,
            (Self::Time(left),Self::Time(right)) => left == right,
            (Self::Path(left),Self::Path(right)) => left == right,
            (Self::TableRef(left),Self::TableRef(right)) => left == right,
            (Self::VectorType(left),Self::VectorType(right)) => left.equivalent(right),
            (Self::ByteBuffer(left),Self::ByteBuffer(right)) => left == right,
            (Self::Bytes(left),Self::Bytes(right)) => Arc::ptr_eq(left,right),(Self::Throwable(left),Self::Throwable(right)) => super::Throwable::equal(left,right)?,(Self::Link(left),Self::Link(right)) => Arc::ptr_eq(left,right),
            (Self::Vector(left) | Self::List(left),Self::Vector(right) | Self::List(right)) => {
                if matches!((self,other),(Self::Vector(_),Self::Vector(_))) || java && matches!(self,Self::Vector(_)) {
                    if left.len() != right.len() { return Ok(false); }
                }
                if java && matches!((self,other),(Self::Vector(_),Self::List(_))) && other.java_hash_code()? != self.java_hash_code()? { return Ok(false); }
                for (index,left) in left.iter().enumerate() {
                    let Some(right) = right.get(index) else { return Ok(false); };
                    if !left.object_equal(right,java)? { return Ok(false); }
                }
                left.len() == right.len()
            }
            (Self::Set(left),Self::Set(right)) => {
                if left.len() != right.len() { return Ok(false); }
                for (key,_) in right.entries() { if left.get(key)?.is_none() { return Ok(false); } }true
            }
            (Self::Map(_),Self::Map(_)) => self.map_equal(other,java)?,
            (Self::TxKey(left),Self::TxKey(right)) => if java { self.map_equal(other,true)? } else { left.equivalent(right)? },
            (Self::Map(_),Self::TxKey(_)) | (Self::TxKey(_),Self::Map(_)) if java => self.map_equal(other,true)?,
            (Self::Tagged(left),Self::Tagged(right)) => super::Tagged::equal(left,right)?,
            (Self::ClojureForm(left),Self::ClojureForm(right)) => left.java_equal(right)?,
            _ => false,
        })
    }
    fn map_equal(&self,other: &Self,java: bool) -> Result<bool> {
        if self.entries().ok_or_else(invalid)?.count() != other.entries().ok_or_else(invalid)?.count() { return Ok(false); }
        for (key,value) in self.entries().ok_or_else(invalid)? {
            let candidate = match other { Self::Map(map) => map.get(key)?,Self::TxKey(record) => record.get(key)?,_ => unreachable!() };
            let Some(candidate) = candidate else { return Ok(false); };
            if !value.object_equal(candidate,java)? { return Ok(false); }
        }
        Ok(true)
    }
}
