use super::*;
use super::evaluation::SchemaIdentityOps;
use super::identity::IdentityTable;
use rtdb_types::java_hash_map::{KeyOps, Map};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect::avro) struct CycleReference<T> { pub schema: T, pub optional: bool }
pub(in crate::connect::avro) struct CycleContext<T> {
    records: IdentityTable<SchemaHandle, CycleReference<T>, Arc<MemoryReservation>>,
    detected: Map<SchemaHandle, (), Arc<MemoryReservation>>,
    failed: bool,
}
struct SchemaOps;
impl<W: Witness> KeyOps<SchemaHandle, Evaluation<'_, W>> for SchemaOps {
    type Comparable = ();
    fn is_null(&self, _: &SchemaHandle) -> bool { false }
    fn same_identity(&self, key: &SchemaHandle, stored: &SchemaHandle) -> bool { key.same_object(stored) }
    fn hash_code(&mut self, key: &SchemaHandle, evaluation: &mut Evaluation<'_, W>) -> Result<i32> { evaluation.hash_code(key) }
    fn equals(&mut self, key: &SchemaHandle, stored: &SchemaHandle, evaluation: &mut Evaluation<'_, W>) -> Result<bool> { evaluation.equal(key, stored) }
    fn comparable_class(&mut self, _: &SchemaHandle, _: &mut Evaluation<'_, W>) -> Result<Option<()>> { Ok(None) }
    fn compare(&mut self, _: (), _: &SchemaHandle, _: &SchemaHandle, _: &mut Evaluation<'_, W>) -> Result<i32> { Ok(0) }
    fn class_name<'a>(&'a self, key: &'a SchemaHandle) -> &'a str {
        match source_type(&key.view().node().kind) {
            Type::Record => "org.apache.avro.Schema$RecordSchema",
            Type::Enum => "org.apache.avro.Schema$EnumSchema",
            Type::Array => "org.apache.avro.Schema$ArraySchema",
            Type::Map => "org.apache.avro.Schema$MapSchema",
            Type::Union => "org.apache.avro.Schema$UnionSchema",
            Type::Fixed => "org.apache.avro.Schema$FixedSchema",
            Type::String => "org.apache.avro.Schema$StringSchema",
            Type::Bytes => "org.apache.avro.Schema$BytesSchema",
            Type::Int => "org.apache.avro.Schema$IntSchema",
            Type::Long => "org.apache.avro.Schema$LongSchema",
            Type::Float => "org.apache.avro.Schema$FloatSchema",
            Type::Double => "org.apache.avro.Schema$DoubleSchema",
            Type::Boolean => "org.apache.avro.Schema$BooleanSchema",
            Type::Null => "org.apache.avro.Schema$NullSchema",
        }
    }
    fn identity_hash_code(&mut self, key: &SchemaHandle, evaluation: &mut Evaluation<'_, W>) -> Result<i32> {
        evaluation.authority.witness.schema_identity(key, evaluation.authority.context)
    }
}
impl<T: Copy> CycleContext<T> {
    pub(in crate::connect::avro) fn new<W: Witness>(evaluation: &mut Evaluation<'_, W>) -> Result<Self> {
        Ok(Self { records: IdentityTable::new(&mut evaluation.authority)?, detected: Map::new(None)?, failed: false })
    }
    fn usable(&self) -> Result<()> {
        if self.failed { Err(invalid("Failed Avro cycle construction cannot be reused")) } else { Ok(()) }
    }
    fn complete<R>(&mut self, result: Result<R>) -> Result<R> {
        if result.is_err() { self.failed = true; }
        result
    }
    pub(in crate::connect::avro) fn register_record<W: Witness>(&mut self, source: &SchemaHandle, reference: CycleReference<T>, evaluation: &mut Evaluation<'_, W>) -> Result<()> {
        self.usable()?;
        if !source.is_record() { return Err(invalid("Only records may enter Avro cycleReferences")); }
        let result = self.records.put(source.clone(), reference, &mut SchemaIdentityOps, &mut evaluation.authority).map(|_| ());
        self.complete(result)
    }
    pub(in crate::connect::avro) fn seed_cached<W: Witness>(&mut self, source: &SchemaHandle, reference: CycleReference<T>, evaluation: &mut Evaluation<'_, W>) -> Result<()> {
        self.usable()?;
        if source.is_record() { self.register_record(source, reference, evaluation) } else { Ok(()) }
    }
    pub(in crate::connect::avro) fn reference<W: Witness>(&mut self, source: &SchemaHandle, optional: bool, evaluation: &mut Evaluation<'_, W>) -> Result<Option<CycleReference<T>>> {
        self.usable()?;
        let result = (|| {
            if !self.records.contains_key(source, &mut SchemaIdentityOps, &mut evaluation.authority)? { return Ok(None); }
            self.detected.put(source.clone(), (), &mut SchemaOps, evaluation)?;
            let reference = self.records.get(source, &mut SchemaIdentityOps, &mut evaluation.authority)?.copied()
                .ok_or_else(|| invalid("Avro cycle reference disappeared during its synchronous lookup"))?;
            Ok(Some(CycleReference { schema: reference.schema, optional }))
        })();
        self.complete(result)
    }
    pub(in crate::connect::avro) fn finish<W: Witness>(&mut self, source: &SchemaHandle, evaluation: &mut Evaluation<'_, W>) -> Result<()> {
        self.usable()?;
        let result = (|| {
            if !self.detected.contains_key(source, &mut SchemaOps, evaluation)?
                && self.records.contains_key(source, &mut SchemaIdentityOps, &mut evaluation.authority)? {
                self.records.remove(source, &mut SchemaIdentityOps, &mut evaluation.authority)?;
            }
            Ok(())
        })();
        self.complete(result)
    }
    pub(in crate::connect::avro) fn retain(&self, context: &mut ConversionContext) -> Result<()> {
        self.usable()?;
        let mut seen = RetainSeen::default();
        for lease in self.records.leases() { context.retain(Arc::clone(lease))?; }
        for (source, _) in self.records.entries()? { source.retain_with(context, &mut seen)?; }
        for lease in self.detected.leases() { context.retain(Arc::clone(lease))?; }
        for (source, _) in self.detected.try_iter()? { source.retain_with(context, &mut seen)?; }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
