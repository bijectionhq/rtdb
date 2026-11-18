use std::sync::Arc;
use rtdb_types::Result;
use super::{Object, T, hash, invalid, tagged};

#[derive(Debug)]
pub(crate) struct Tagged { tag: String, representation: Object }

impl Tagged {
    pub(super) fn tag_bytes(&self) -> usize { self.tag.len() }
    pub(super) fn new(tag: String, representation: Object) -> Self { Self { tag, representation } }
    pub(super) fn equal(left: &Arc<Self>, right: &Arc<Self>) -> Result<bool> {
        if Arc::ptr_eq(left, right) { return Ok(true); }
        if left.tag != right.tag { return Ok(false); }
        if matches!(&left.representation, Object::Null) { return Err(invalid()); }
        left.representation.java_equal(&right.representation)
    }
    pub(super) fn hash_code(&self) -> Result<i32> {
        if matches!(&self.representation, Object::Null) { return Err(invalid()); }
        Ok(17i32.wrapping_mul(31).wrapping_mul(hash::string(&self.tag)).wrapping_mul(31).wrapping_mul(self.representation.java_hash_code()?))
    }
    pub(super) fn wire(&self,allocation: super::Admission<'_>) -> Result<T> { Ok(tagged(&self.tag, self.representation.wire_with(allocation)?)) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::transit::encode_msgpack;

    fn decoded(tag: &str, representation: T,allocation: &crate::EvaluationScope) -> Object {
        super::super::Decoded::read(&encode_msgpack(&tagged(tag, representation)).unwrap(),allocation).unwrap().value
    }

    #[test]
    fn aliases_compare_before_nullable_representations_are_dereferenced() {
        let allocation = crate::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(1 << 20)).unwrap();
        let value = decoded("unknown", T::Null,&allocation);
        let alias = value.clone();
        let distinct = decoded("unknown", T::Null,&allocation);
        assert!(value.equivalent(&alias).unwrap());
        assert!(value.java_equal(&alias).unwrap());
        assert!(value.equivalent(&distinct).is_err());
        assert!(value.java_equal(&distinct).is_err());
        assert!(!value.java_equal(&decoded("different", T::Null,&allocation)).unwrap());
        assert!(value.java_hash_code().is_err());
        assert!(alias.hasheq().is_err());
        assert_eq!(alias.wire(&allocation).unwrap(), tagged("unknown", T::Null));
    }

    #[test]
    fn distinct_tagged_values_keep_java_numeric_classes_and_content_hashes() {
        let allocation = crate::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(1 << 20)).unwrap();
        let byte = decoded("x", tagged("i8", T::Integer(1)),&allocation);
        let long = decoded("x", T::Integer(1),&allocation);
        let same = decoded("x", T::Integer(1),&allocation);
        assert!(!byte.equivalent(&long).unwrap());
        assert!(long.java_equal(&same).unwrap());
        assert_eq!(long.java_hash_code().unwrap(), 1_960_440);
        assert_eq!(long.hasheq().unwrap(), same.hasheq().unwrap());
        assert_eq!(byte.wire(&allocation).unwrap(), tagged("x", T::Integer(1)));
    }
}
