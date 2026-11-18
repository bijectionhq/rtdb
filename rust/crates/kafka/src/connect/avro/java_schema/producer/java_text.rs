use super::*;

pub(super) struct ParsedInput {
    pub(super) canonical: CanonicalSource,
    pub(super) inputs: ProjectedInputs,
}
pub(super) struct ProjectedInputs {
    root: String,
    references: Vec<String>,
    payloads: Vec<Arc<MemoryReservation>>,
    _control: Arc<MemoryReservation>,
}
pub(super) struct Projection { pub(super) text: String, pub(super) charge: Arc<MemoryReservation> }

impl ProjectedInputs {
    pub(super) fn new(reference_count: usize, context: &mut ConversionContext) -> Result<Self> {
        let payload_count = add(reference_count, 1)?;
        let slots = add(mul(reference_count, size_of::<String>())?, mul(payload_count, size_of::<Arc<MemoryReservation>>())?)?;
        let control = context.reserve(add(mul(slots, 16)?, 4096)?)?;
        let mut references = Vec::new();
        references.try_reserve_exact(reference_count).map_err(allocation)?;
        let mut payloads = Vec::new();
        payloads.try_reserve_exact(payload_count).map_err(allocation)?;
        Ok(Self { root: String::new(), references, payloads, _control: control })
    }
    pub(super) fn root(&self) -> &str { &self.root }
    pub(super) fn references(&self) -> &[String] { &self.references }
    pub(super) fn push_reference(&mut self, projected: Projection) -> Result<()> {
        if self.references.len() == self.references.capacity() || self.payloads.len() == self.payloads.capacity() {
            return Err(invalid("Backend input exceeds its admitted reference slots"));
        }
        self.references.push(projected.text);
        self.payloads.push(projected.charge);
        Ok(())
    }
    pub(super) fn set_root(&mut self, projected: Projection) -> Result<()> {
        if self.payloads.len() == self.payloads.capacity() { return Err(invalid("Backend input exceeds its admitted root slot")); }
        self.root = projected.text;
        self.payloads.push(projected.charge);
        Ok(())
    }
}

fn scalar(point: u32) -> Result<char> {
    char::from_u32(point).ok_or_else(|| Anomaly::unsupported(
        "rtdb/avro-java-text-reader-required",
        "Unmatched schema input units require the source JavaText Reader path",
    ))
}
fn length(source: JavaText<'_>) -> Result<usize> {
    let mut bytes = 0usize;
    for point in source.code_points() {
        bytes = add(bytes, scalar(point)?.len_utf8())?;
        if bytes > input::MAX_DOCUMENT_BYTES { return Err(input::profile_limit()); }
    }
    Ok(bytes)
}
fn project(source: JavaText<'_>, context: &mut ConversionContext) -> Result<Projection> {
    let bytes = length(source)?;
    let charge = context.reserve(add(mul(bytes, 16)?, 256)?)?;
    let mut text = String::new();
    text.try_reserve_exact(bytes).map_err(allocation)?;
    for point in source.code_points() { text.push(scalar(point)?); }
    Ok(Projection { text, charge })
}

pub(super) fn parse(
    text: JavaText<'_>, references: &[JavaText<'_>], policy: SourcePolicy,
    logical: &mut dyn LogicalEnvironment, context: &mut ConversionContext,
) -> Result<ParsedInput> {
    let mut producer = Producer { builder: SchemasBuilder::new(context)?, policy, native: NativeFootprint::new(), logical };
    let mut inputs = ProjectedInputs::new(references.len(), context)?;
    for reference in references {
        let projected = project(*reference, context)?;
        let document = input::parse(&projected.text, context)?;
        producer.native.document(projected.text.len(), &document)?;
        producer.schema(&document, None, context, 0)?;
        inputs.references.push(projected.text);
        inputs.payloads.push(projected.charge);
    }
    let projected = project(text, context)?;
    let document = input::parse(&projected.text, context)?;
    producer.native.document(projected.text.len(), &document)?;
    let root = producer.schema(&document, None, context, 0)?;
    inputs.root = projected.text;
    inputs.payloads.push(projected.charge);
    let canonical = CanonicalSource { root: producer.builder.finish(root, context)?, native: producer.native };
    Ok(ParsedInput { canonical, inputs })
}

#[cfg(test)]
mod tests;
