use std::{collections::BTreeMap, mem::size_of};
use apache_avro::Schema;
use rtdb_types::Result;
use super::error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CycleReference<T> { pub schema: T, pub optional: bool }

#[derive(Clone, Copy)]
pub(super) struct RecordIdentity<'a> { source: &'a Schema }
struct RecordEntry<'a, T> { source: RecordIdentity<'a>, reference: CycleReference<T> }

pub(super) struct ToConnectContext<'a, T> {
    records: Vec<RecordEntry<'a, T>>,
    detected: Vec<RecordIdentity<'a>>,
}

impl<'a> RecordIdentity<'a> {
    pub(super) fn canonical(source: &Schema, definitions: &'a BTreeMap<String, Schema>) -> Result<Option<Self>> {
        let Schema::Record(record) = super::resolve(source, definitions)? else { return Ok(None); };
        let name = super::names::NameParts::parse(&record.name.name, record.name.namespace.as_deref()).map_err(|cause| error("avro-schema", cause))?;
        let source = definitions.iter().find_map(|(fullname, source)| (super::names::NameParts::parse(fullname, None).ok() == Some(name)).then_some(source)).ok_or_else(|| error("avro-schema", "Canonical record identity is absent from the named-definition owner"))?;
        if !matches!(source, Schema::Record(_)) { return Err(error("avro-schema", "Canonical record identity does not identify an Avro record")); }
        Ok(Some(Self { source }))
    }
}

impl<'a, T: Copy> ToConnectContext<'a, T> {
    pub(super) fn new() -> Self { Self { records: Vec::new(), detected: Vec::new() } }

    /// The caller retains each admission lease until this context is dropped.
    pub(super) fn register_record(&mut self, source: RecordIdentity<'a>, reference: CycleReference<T>, admit: &mut impl FnMut(usize) -> Result<()>) -> Result<()> {
        if let Some(entry) = self.records.iter_mut().find(|entry| std::ptr::eq(entry.source.source, source.source)) {
            entry.reference = reference;
            return Ok(());
        }
        grow(&mut self.records, admit)?;
        self.records.push(RecordEntry { source, reference });
        Ok(())
    }

    pub(super) fn seed_cached(&mut self, source: Option<RecordIdentity<'a>>, reference: CycleReference<T>, admit: &mut impl FnMut(usize) -> Result<()>) -> Result<()> {
        if let Some(source) = source { self.register_record(source, reference, admit)?; }
        Ok(())
    }

    /// The caller retains each admission lease until this context is dropped.
    /// `java_schema_equal` must implement Avro Java Schema equality, including properties and field defaults.
    pub(super) fn reference(&mut self, source: RecordIdentity<'a>, optional: bool, mut java_schema_equal: impl FnMut(&Schema, &Schema) -> Result<bool>, admit: &mut impl FnMut(usize) -> Result<()>) -> Result<Option<CycleReference<T>>> {
        let Some(reference) = self.records.iter().find(|entry| std::ptr::eq(entry.source.source, source.source)).map(|entry| entry.reference) else { return Ok(None); };
        let mut detected = false;
        for existing in &self.detected {
            if java_schema_equal(source.source, existing.source)? { detected = true; break; }
        }
        if !detected {
            grow(&mut self.detected, admit)?;
            self.detected.push(source);
        }
        Ok(Some(CycleReference { schema: reference.schema, optional }))
    }

    /// `java_schema_equal` must implement Avro Java Schema equality, including properties and field defaults.
    pub(super) fn finish(&mut self, source: RecordIdentity<'a>, mut java_schema_equal: impl FnMut(&Schema, &Schema) -> Result<bool>) -> Result<()> {
        for detected in &self.detected {
            if java_schema_equal(source.source, detected.source)? { return Ok(()); }
        }
        if let Some(index) = self.records.iter().position(|entry| std::ptr::eq(entry.source.source, source.source)) { self.records.remove(index); }
        Ok(())
    }
}

fn grow<T>(values: &mut Vec<T>, admit: &mut impl FnMut(usize) -> Result<()>) -> Result<()> {
    if values.len() < values.capacity() { return Ok(()); }
    let capacity = if values.capacity() == 0 { 4 } else { values.capacity().checked_mul(2).ok_or_else(super::resolution::overflow)? };
    let bytes = capacity.checked_mul(size_of::<T>()).and_then(|bytes| bytes.checked_mul(16)).and_then(|bytes| bytes.checked_add(256)).ok_or_else(super::resolution::overflow)?;
    admit(bytes)?;
    values.try_reserve_exact(capacity - values.len()).map_err(|cause| error("allocation-failed", cause))
}

#[cfg(test)]
mod tests;
