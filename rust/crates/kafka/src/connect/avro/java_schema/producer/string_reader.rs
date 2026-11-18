use super::*;
use crate::connect::registry::reader_symbols::{ChildSymbols, FactoryProfile, Failure, InternAuthority, Profile, ReaderSymbols};

mod buffers;
mod syntax;
mod identifier;
mod projection;
pub(in crate::connect) use buffers::{InputBuffers, TokenBuffer, TokenRecycler};
use buffers::StringInput;

const MAX_DOCUMENT_UNITS: usize = 256 * 1024;
const DIRECT_STRING_LIMIT: usize = 0x8000;

#[derive(Clone)]
pub(in crate::connect) struct SchemaStringFactory { symbols: Arc<ReaderSymbols> }
impl SchemaStringFactory {
    /// Retain this root across parses belonging to the same actual Avro factory/classloader profile.
    /// Uses the pinned default mapper/constraints and Schema.FACTORY's ALLOW_COMMENTS setting.
    pub(in crate::connect) fn new_default(profile: FactoryProfile, context: &mut ConversionContext) -> Result<Self> {
        Ok(Self { symbols: ReaderSymbols::new(profile, context).map_err(symbol_error)? })
    }
    /// Input owners remain charged and stable through the synchronous callback and close.
    /// The supplied intern/recycler authorities describe the effective source environment.
    pub(super) fn with_tree<T>(
        &self, text: JavaText<'_>, buffers: &mut dyn InputBuffers, intern: &mut dyn InternAuthority,
        context: &mut ConversionContext, assemble: impl FnOnce(Option<&JsonNode>, &mut ConversionContext) -> Result<T>,
    ) -> Result<T> {
        if text.len_units() > MAX_DOCUMENT_UNITS {
            return Err(Anomaly::unsupported("rtdb/avro-reader-native-profile", "Schema String exceeds the native Reader document profile"));
        }
        if self.symbols.factory().profile != buffers.profile() {
            return Err(Anomaly::unsupported("rtdb/avro-reader-buffer-profile", "Factory and captured recycler source profiles differ"));
        }
        let mut session = Session::open(&self.symbols, text, buffers, context)?;
        let result = (|| {
            let tree = syntax::tree(&mut session, intern, context)?;
            if tree.is_none() { session.close(buffers, context)?; }
            let result = assemble(tree.as_ref(), context)?;
            session.dangling()?;
            Ok(result)
        })();
        match session.close(buffers, context) { Err(cause) => Err(cause), Ok(()) => result }
    }
    pub(super) fn parse_canonical(
        &self, text: JavaText<'_>, references: &[JavaText<'_>], buffers: &mut dyn InputBuffers,
        intern: &mut dyn InternAuthority, policy: SourcePolicy, logical: &mut dyn LogicalEnvironment,
        context: &mut ConversionContext,
    ) -> Result<SchemaHandle> {
        let mut producer = Producer { builder: SchemasBuilder::new(context)?, policy, native: NativeFootprint::new(), logical };
        for reference in references {
            self.read_schema(&mut producer, *reference, buffers, intern, context)?;
        }
        let (root, _) = self.read_schema(&mut producer, text, buffers, intern, context)?;
        producer.builder.finish(root, context)
    }
    pub(in crate::connect::avro) fn parse_without_root(
        &self, references: &[JavaText<'_>], buffers: &mut dyn InputBuffers,
        intern: &mut dyn InternAuthority, policy: SourcePolicy, context: &mut ConversionContext,
    ) -> Result<()> {
        let mut logical = logical::Unregistered;
        let mut producer = Producer { builder: SchemasBuilder::new(context)?, policy, native: NativeFootprint::new(), logical: &mut logical };
        for reference in references {
            self.read_schema(&mut producer, *reference, buffers, intern, context)?;
        }
        Ok(())
    }
    fn read_schema(
        &self, producer: &mut Producer<'_>, text: JavaText<'_>, buffers: &mut dyn InputBuffers,
        intern: &mut dyn InternAuthority, context: &mut ConversionContext,
    ) -> Result<(SchemaId, JsonNode)> {
        self.with_tree(text, buffers, intern, context, |tree, context| {
            let tree = tree.ok_or_else(|| error("avro-schema", "Cannot parse <null> schema"))?;
            let schema = producer.schema(tree, None, context, 0)?;
            Ok((schema, tree.clone()))
        })
    }
    pub(super) fn parse_input(
        &self, text: JavaText<'_>, references: &[JavaText<'_>], buffers: &mut dyn InputBuffers,
        intern: &mut dyn InternAuthority, policy: SourcePolicy, logical: &mut dyn LogicalEnvironment,
        context: &mut ConversionContext,
    ) -> Result<java_text::ParsedInput> {
        let document_count = add(references.len(), 1)?;
        let document_slots = mul(document_count, size_of::<JsonNode>())?;
        let _documents_charge = context.reserve(add(mul(document_slots, 16)?, 4096)?)?;
        let mut documents = Vec::new();
        documents.try_reserve_exact(document_count).map_err(allocation)?;
        let mut producer = Producer { builder: SchemasBuilder::new(context)?, policy, native: NativeFootprint::new(), logical };
        for reference in references {
            let (_, tree) = self.read_schema(&mut producer, *reference, buffers, intern, context)?;
            documents.push(tree);
        }
        let (root, tree) = self.read_schema(&mut producer, text, buffers, intern, context)?;
        documents.push(tree);
        let root = producer.builder.finish(root, context)?;
        let mut native = producer.native;
        let mut inputs = java_text::ProjectedInputs::new(references.len(), context)?;
        for (index, tree) in documents.iter().enumerate() {
            let projected = projection::render(tree, context)?;
            native.document(projected.text.len(), tree)?;
            if index < references.len() { inputs.push_reference(projected)?; }
            else { inputs.set_root(projected)?; }
        }
        let canonical = CanonicalSource { root, native };
        Ok(java_text::ParsedInput { canonical, inputs })
    }
}

struct Session<'a> { input: Option<StringInput<'a>>, child: Option<ChildSymbols>, closed: bool }
impl<'a> Session<'a> {
    fn open(symbols: &Arc<ReaderSymbols>, text: JavaText<'a>, buffers: &mut dyn InputBuffers, context: &mut ConversionContext) -> Result<Self> {
        if text.len_units() <= DIRECT_STRING_LIMIT {
            let buffer = buffers.take(text.len_units(), context)?;
            let input = StringInput::direct(text, buffer)?;
            let child = symbols.child(context).map_err(symbol_error)?;
            Ok(Self { input: Some(input), child: Some(child), closed: false })
        } else {
            let child = symbols.child(context).map_err(symbol_error)?;
            let buffer = buffers.take(0, context)?;
            let input = StringInput::reader(text, buffer)?;
            Ok(Self { input: Some(input), child: Some(child), closed: false })
        }
    }
    fn dangling(&self) -> Result<()> {
        if self.input.as_ref().is_some_and(|input| input.buffered_tail().iter().any(|unit| *unit > 0x20)) {
            Err(error("avro-schema-dangling", "Dangling buffered content after the schema"))
        } else { Ok(()) }
    }
    fn close(&mut self, buffers: &mut dyn InputBuffers, context: &mut ConversionContext) -> Result<()> {
        if self.closed { return Ok(()); }
        self.closed = true;
        if let Some(child) = self.child.take() { child.release(context).map_err(symbol_error)?; }
        if let Some(input) = self.input.take() { buffers.give(input.into_buffer(), context)?; }
        Ok(())
    }
}
fn symbol_error(cause: Failure) -> Anomaly {
    match cause {
        Failure::Native(cause) => cause,
        Failure::NameLength(_) => error("avro-reader-name-length", "Source field-name length constraint exceeded"),
        Failure::CollisionOverflow { .. } | Failure::RehashCount { .. } => error("avro-reader-unchecked", "Source name canonicalizer failed"),
        Failure::Poisoned => error("avro-reader-native-state", "Native Reader symbol child is poisoned"),
    }
}

#[cfg(test)]
mod tests;
