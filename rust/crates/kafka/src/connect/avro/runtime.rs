use std::{collections::BTreeMap, mem::size_of, sync::Arc};
use rtdb_columnar::MemoryReservation;
use rtdb_types::{Result, java_text::JavaText};
use super::{AvroDatumPlan, ConversionContext, InputBuffers, SchemaStringFactory, SourcePolicy, error};
use crate::connect::registry::reader_symbols::InternAuthority;
mod pool;
pub(in crate::connect) use pool::ThreadTokenPool;

pub(in crate::connect) struct AvroSchemaRuntime {
    factory: SchemaStringFactory,
    buffers: Box<dyn InputBuffers + Send>,
    intern: Box<dyn InternAuthority + Send>,
    charge: Arc<MemoryReservation>,
}

impl AvroSchemaRuntime {
    /// Supply the retained Avro factory, reusable recycler authority and effective intern authority.
    /// Authorities own admitted nested payloads and retain demanded cached storage in each call's context.
    pub(in crate::connect) fn new<B, I>(
        factory: SchemaStringFactory,
        buffers: B,
        intern: I,
        context: &mut ConversionContext,
    ) -> Result<Self>
    where
        B: InputBuffers + Send + 'static,
        I: InternAuthority + Send + 'static,
    {
        let bytes = size_of::<Self>().checked_add(size_of::<B>())
            .and_then(|bytes| bytes.checked_add(size_of::<I>()))
            .and_then(|bytes| bytes.checked_mul(16))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(overflow)?;
        let charge = context.reserve(bytes)?;
        Ok(Self { factory, buffers: Box::new(buffers), intern: Box::new(intern), charge })
    }

    /// Input documents stay charged through this synchronous parse.
    pub(in crate::connect) fn parse(
        &mut self,
        root: Option<JavaText<'_>>,
        references: &[JavaText<'_>],
        options: &BTreeMap<String, String>,
        policy: SourcePolicy,
        context: &mut ConversionContext,
    ) -> Result<Option<AvroDatumPlan>> {
        context.retain(Arc::clone(&self.charge))?;
        let Some(root) = root else {
            self.factory.parse_without_root(references, &mut *self.buffers, &mut *self.intern, policy, context)?;
            return Ok(None);
        };
        AvroDatumPlan::parse_admitted_java_text_with_reader(
            root, references, options, policy, &self.factory,
            &mut *self.buffers, &mut *self.intern, context,
        ).map(Some)
    }
}

fn overflow() -> rtdb_types::Anomaly {
    error("allocation-overflow", "Avro runtime owner allocation exceeds addressable memory")
}

#[cfg(test)]
mod tests;
