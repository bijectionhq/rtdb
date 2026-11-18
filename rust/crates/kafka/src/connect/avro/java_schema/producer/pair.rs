use super::*;
use apache_avro::{Schema, schema::{Name as NativeName, Namespace, SchemaNamePolicy}};
use crate::connect::avro::{AvroDatumPlan, names::{JavaNames, NameParts}};

/// Uses the declared standard-JSON UTF8 and native-depth profile; unsupported native representations return an error.
pub(in crate::connect::avro) struct ParsedPair {
    datum: AvroDatumPlan,
    source: SchemaHandle,
    charge: Arc<MemoryReservation>,
}
pub(in crate::connect::avro) struct AdmittedSource {
    root: SchemaHandle,
    completion: Arc<MemoryReservation>,
}
#[derive(Clone, Copy)]
struct PairOwner<'a> {
    datum: &'a AvroDatumPlan,
    source: &'a SchemaHandle,
}
#[derive(Clone, Copy)]
pub(in crate::connect::avro) struct PairSelection<'a> {
    pair: PairOwner<'a>,
    native: &'a Schema,
    id: SchemaId,
}
struct NativePolicy;
impl SchemaNamePolicy for NativePolicy {
    fn name(&self, name: &str, namespace: &Namespace) -> apache_avro::AvroResult<NativeName> { JavaNames.name(name, namespace) }
    fn record_field(&self, name: &str) -> apache_avro::AvroResult<()> { JavaNames.record_field(name) }
    fn enum_symbol(&self, name: &str) -> apache_avro::AvroResult<()> { JavaNames.enum_symbol(name) }
    fn reference_fallback(&self, name: &str, namespace: &Namespace) -> apache_avro::AvroResult<Option<NativeName>> { JavaNames.reference_fallback(name, namespace) }
    fn record_field_lookup_uses_aliases(&self) -> bool { false }
    fn record_field_type_is_separate(&self) -> bool { true }
    fn named_aliases_are_references(&self) -> bool { false }
    fn logical_fixed_references_are_named(&self) -> bool { true }
    fn parse_references_in_input_order(&self) -> bool { true }
}
fn native_name(name: &NativeName, expected: &Text) -> Result<bool> {
    let name = NameParts::parse(&name.name, name.namespace.as_deref()).map_err(|cause| error("avro-native-pairing", cause))?;
    Ok(expected.view().units().eq((!name.namespace.is_empty()).then_some(name.namespace).into_iter().flat_map(str::encode_utf16)
        .chain((!name.namespace.is_empty()).then_some(46)).chain(name.simple.encode_utf16())))
}
fn admit_native<T>(bytes: usize, context: &mut ConversionContext, parse: impl FnOnce() -> Result<T>) -> Result<(T, Arc<MemoryReservation>)> {
    let charge = context.reserve(bytes)?;
    Ok((parse()?, charge))
}
impl ParsedPair {
    pub(in crate::connect::avro) fn parse(text: &str, references: &[String], options: &BTreeMap<String, String>, policy: SourcePolicy, context: &mut ConversionContext) -> Result<Self> {
        let canonical = super::parse(text, references, policy, context)?;
        let (datum, charge) = admit_native(canonical.native.bound()?, context, || {
            AvroDatumPlan::parse_with_name_policy(text, references, options, Some(&NativePolicy))
                .map_err(|cause| Anomaly::unsupported("xtdb.kafka-connect-source/unsupported-avro-native-profile", cause.to_string()))
        })?;
        let pair = Self { datum, source: canonical.root, charge };
        pair.owner().validate(&pair.datum.root, pair.source.id, 0)?;
        for (name, native) in &pair.datum.names {
            let id = pair.source.owner.bindings.iter().find_map(|(full, id)| full.view().units().eq(name.encode_utf16()).then_some(*id))
                .ok_or_else(|| error("avro-native-pairing", "Native named definition has no source declaration"))?;
            pair.owner().validate(native, id, 0)?;
        }
        Ok(pair)
    }
    fn owner(&self) -> PairOwner<'_> { PairOwner { datum: &self.datum, source: &self.source } }
    pub(in crate::connect::avro) fn root(&self) -> PairSelection<'_> { self.owner().root() }
    pub(in crate::connect::avro) fn definition(&self, name: &str) -> Result<PairSelection<'_>> { self.owner().definition(name) }
    pub(in crate::connect::avro) fn retain(&self, context: &mut ConversionContext) -> Result<()> {
        retain_canonical_then_completion(&self.source, &self.charge, context)
    }
    pub(in crate::connect::avro) fn into_datum(self) -> AvroDatumPlan {
        let Self { mut datum, source, charge } = self;
        datum.source = Some(AdmittedSource { root: source, completion: charge });
        datum
    }
    #[cfg(test)]
    fn resolved<'a>(&'a self, native: &'a Schema) -> Result<&'a Schema> { self.owner().resolved(native) }
}
fn retain_canonical_then_completion(source: &SchemaHandle, completion: &Arc<MemoryReservation>, context: &mut ConversionContext) -> Result<()> {
    if context.retains(completion) { return Ok(()); }
    source.retain(context)?;
    context.retain(Arc::clone(completion))
}
impl AdmittedSource {
    pub(in crate::connect::avro) fn retain(&self, context: &mut ConversionContext) -> Result<()> {
        retain_canonical_then_completion(&self.root, &self.completion, context)
    }
}
impl AvroDatumPlan {
    pub(in crate::connect::avro) fn paired_root(&self) -> Option<PairSelection<'_>> {
        self.source.as_ref().map(|source| PairOwner { datum: self, source: &source.root }.root())
    }
    pub(in crate::connect::avro) fn paired_definition(&self, name: &str) -> Result<Option<PairSelection<'_>>> {
        self.source.as_ref().map(|source| PairOwner { datum: self, source: &source.root }.definition(name)).transpose()
    }
}
impl<'a> PairOwner<'a> {
    fn root(self) -> PairSelection<'a> { PairSelection { pair: self, native: &self.datum.root, id: self.source.id } }
    fn definition(self, name: &str) -> Result<PairSelection<'a>> {
        let native = self.datum.names.get(name).ok_or_else(|| error("avro-native-pairing", "Native definition is absent"))?;
        let id = self.source.owner.bindings.iter().find_map(|(full, id)| full.view().units().eq(name.encode_utf16()).then_some(*id))
            .ok_or_else(|| error("avro-native-pairing", "Canonical definition is absent"))?;
        Ok(PairSelection { pair: self, native, id })
    }
    fn resolved(&self, native: &'a Schema) -> Result<&'a Schema> { crate::connect::avro::resolve(native, &self.datum.names) }
    fn ref_id(&self, name: &NativeName) -> Result<SchemaId> {
        for (full, id) in &self.source.owner.bindings { if native_name(name, full)? { return Ok(*id); } }
        Err(error("avro-native-pairing", "Native reference does not select a canonical declaration"))
    }
    fn validate(&self, native: &'a Schema, id: SchemaId, depth: usize) -> Result<()> {
        guard_depth(depth, "Native pairing exceeds the native traversal depth limit of 128")?;
        self.source.owner.require(id)?;
        if let Schema::Ref { name } = native {
            if self.ref_id(name)? != id { return Err(error("avro-native-pairing", "Native reference selects a different source object")); }
            self.resolved(native)?;
            return Ok(());
        }
        if let Schema::Decimal(decimal) = native { return self.validate(decimal.inner.as_ref(), id, depth); }
        let source = &self.source.owner.nodes[id.index].kind;
        let primitive = match native {
            Schema::Null => Some(Type::Null), Schema::Boolean => Some(Type::Boolean),
            Schema::Int | Schema::Date | Schema::TimeMillis => Some(Type::Int),
            Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros | Schema::LocalTimestampNanos => Some(Type::Long),
            Schema::Float => Some(Type::Float), Schema::Double => Some(Type::Double), Schema::String | Schema::Uuid => Some(Type::String),
            Schema::Bytes | Schema::BigDecimal => Some(Type::Bytes), _ => None,
        };
        if let Some(kind) = primitive {
            return if matches!(source, Kind::Primitive(expected) if *expected == kind) { Ok(()) }
                else { Err(error("avro-native-pairing", "Native primitive differs from its source node")) };
        }
        let same_name = |native, source: &Name| -> Result<bool> { match source.full() { Some(full) => native_name(native, full), None => Ok(false) } };
        match (native, source) {
            (Schema::Record(native), Kind::Record { name, fields: Some(fields), .. }) => {
                if !same_name(&native.name, name)? || native.fields.len() != fields.len() { return Err(error("avro-native-pairing", "Native record differs from its source declaration")); }
                for (native, source) in native.fields.iter().zip(fields) {
                    if !source.name.view().units().eq(native.name.encode_utf16()) { return Err(error("avro-native-pairing", "Native field order differs from source order")); }
                    self.validate(&native.schema, source.schema, depth + 1)?;
                }
            }
            (Schema::Enum(native), Kind::Enum { name, symbols, .. }) => {
                if !same_name(&native.name, name)? || native.symbols.len() != symbols.len()
                    || !native.symbols.iter().zip(symbols).all(|(native, source)| source.view().units().eq(native.encode_utf16())) {
                    return Err(error("avro-native-pairing", "Native enum differs from its source declaration"));
                }
            }
            (Schema::Fixed(native), Kind::Fixed { name, size }) => {
                if !same_name(&native.name, name)? || usize::try_from(*size).ok() != Some(native.size) { return Err(error("avro-native-pairing", "Native fixed differs from its source declaration")); }
            }
            (Schema::Array(native), Kind::Array(child)) => self.validate(&native.items, *child, depth + 1)?,
            (Schema::Map(native), Kind::Map(child)) => self.validate(&native.types, *child, depth + 1)?,
            (Schema::Union(native), Kind::Union(children)) => {
                if native.variants().len() != children.len() { return Err(error("avro-native-pairing", "Native union order differs from source order")); }
                for (native, child) in native.variants().iter().zip(children) { self.validate(native, *child, depth + 1)?; }
            }
            _ => return Err(error("avro-native-pairing", "Native schema has an unsupported source pairing")),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
impl<'a> PairSelection<'a> {
    pub(in crate::connect::avro) fn canonical(self) -> Result<SchemaHandle> { self.pair.source.at(self.id) }
    /// Borrows the native physical representation; source metadata and default node classes belong to canonical().
    pub(in crate::connect::avro) fn native(self) -> &'a Schema { self.native }
    pub(in crate::connect::avro) fn field(self, index: usize) -> Result<Self> {
        let native = self.pair.resolved(self.native)?;
        let (Schema::Record(native), Kind::Record { fields: Some(fields), .. }) = (native, &self.pair.source.owner.nodes[self.id.index].kind)
            else { return Err(error("avro-native-pairing", "Field selection requires a paired record")); };
        let native = native.fields.get(index).ok_or_else(|| error("avro-native-pairing", "Native field is absent"))?;
        let source = fields.get(index).ok_or_else(|| error("avro-native-pairing", "Canonical field is absent"))?;
        Ok(Self { pair: self.pair, native: &native.schema, id: source.schema })
    }
    pub(in crate::connect::avro) fn variant(self, index: usize) -> Result<Self> {
        let native = self.pair.resolved(self.native)?;
        let (Schema::Union(native), Kind::Union(children)) = (native, &self.pair.source.owner.nodes[self.id.index].kind)
            else { return Err(error("avro-native-pairing", "Variant selection requires a paired union")); };
        Ok(Self { pair: self.pair, native: native.variants().get(index).ok_or_else(|| error("avro-native-pairing", "Native union member is absent"))?,
            id: *children.get(index).ok_or_else(|| error("avro-native-pairing", "Canonical union member is absent"))? })
    }
    pub(in crate::connect::avro) fn element(self) -> Result<Self> {
        match (self.pair.resolved(self.native)?, &self.pair.source.owner.nodes[self.id.index].kind) {
            (Schema::Array(native), Kind::Array(child)) => Ok(Self { pair: self.pair, native: &native.items, id: *child }),
            _ => Err(error("avro-native-pairing", "Element selection requires a paired array")),
        }
    }
    pub(in crate::connect::avro) fn map_value(self) -> Result<Self> {
        match (self.pair.resolved(self.native)?, &self.pair.source.owner.nodes[self.id.index].kind) {
            (Schema::Map(native), Kind::Map(child)) => Ok(Self { pair: self.pair, native: &native.types, id: *child }),
            _ => Err(error("avro-native-pairing", "Value selection requires a paired map")),
        }
    }
}
