use rtdb_types::Result;
use crate::{admission::Admission,numeric::overflow};
#[cfg(test)]
use crate::EvaluationScope;
use super::{Object,Path,number};

pub(super) struct Text<'a> { value: String,allocation: Admission<'a> }

impl crate::semantic_text::Sink for Text<'_> {
    fn push(&mut self,text: &str) -> Result<()> { Text::push(self,text) }
}

impl<'a> Text<'a> {
    pub(super) fn new(allocation: impl Into<Admission<'a>>) -> Self { Self { value: String::new(),allocation: allocation.into() } }
    pub(super) fn push(&mut self,text: &str) -> Result<()> {
        let required = self.value.len().checked_add(text.len()).ok_or_else(overflow)?;
        if required > self.value.capacity() {
            let capacity = required.max(self.value.capacity().checked_mul(2).ok_or_else(overflow)?).max(64);
            self.allocation.reserve(capacity)?;
            self.value.reserve_exact(capacity - self.value.len());
        }
        self.value.push_str(text);Ok(())
    }
    pub(super) fn character(&mut self,value: char) -> Result<()> { self.push(value.encode_utf8(&mut [0;4])) }
    pub(super) fn temporary(&mut self,bytes: usize,render: impl FnOnce() -> Result<String>) -> Result<()> {
        self.allocation.reserve(bytes)?;
        self.push(&render()?)
    }
    pub(super) fn admission(&self) -> Admission<'a> { self.allocation }
    pub(super) fn finish(self) -> String { self.value }
}

pub(super) fn path_capacity(value: &Path) -> Result<usize> {
    value.text_allocation_bound().ok_or_else(overflow)
}

pub(super) fn wire_charge(value: &Object,allocation: Admission<'_>) -> Result<()> {
    let bytes = match value {
        Object::String(value) | Object::Keyword(value) | Object::Symbol(value) | Object::Uri(value) | Object::KeyFn(value) => value.len(),
        Object::Bytes(value) => value.len(),Object::ByteBuffer(value) => value.len().checked_mul(2).ok_or_else(overflow)?,
        Object::Number(value) => value.text_capacity().map_err(number::error)?.checked_mul(8).ok_or_else(overflow)?,
        Object::Vector(values) | Object::List(values) => values.len().checked_mul(std::mem::size_of::<super::T>()).ok_or_else(overflow)?,
        Object::Set(values) | Object::Map(values) => values.len().checked_mul(std::mem::size_of::<(super::T,super::T)>()).ok_or_else(overflow)?,
        Object::TxKey(value) => value.entries().count().checked_mul(std::mem::size_of::<(super::T,super::T)>() + std::mem::size_of::<(Object,Object)>()).and_then(|bytes|bytes.checked_mul(4)).ok_or_else(overflow)?,
        Object::Link(values) => values.len().checked_mul(std::mem::size_of::<(super::T,super::T)>()).ok_or_else(overflow)?,
        Object::Path(value) => path_capacity(value)?,
        Object::TableRef(value) => value.schema.len().checked_add(value.table.len()).ok_or_else(overflow)?,
        Object::Time(_) => 4096,
        Object::Tagged(value) => value.tag_bytes(),
        _ => 0,
    };
    allocation.reserve(bytes.checked_mul(2).and_then(|bytes|bytes.checked_add(1024)).ok_or_else(overflow)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoted_text_rejects_growth_before_mutation_and_releases_its_scope() {
        let memory = rtdb_columnar::MemoryBudget::new(4096 + 256 + 64);
        {
            let scope = EvaluationScope::new(&memory).unwrap();let mut text = Text::new(&scope);
            text.push("x").unwrap();let capacity = text.value.capacity();
            let value = "a".repeat(capacity);
            assert!(crate::semantic_text::quoted(&mut text,&value).is_err());
            assert_eq!(text.value.len(),capacity);assert_eq!(text.value.capacity(),capacity);
            assert!(text.value.starts_with("x\""));assert!(text.value[2..].bytes().all(|byte|byte == b'a'));
            assert_eq!(memory.used_bytes(),4096 + 256 + 64);
        }
        assert_eq!(memory.used_bytes(),0);
    }
}
