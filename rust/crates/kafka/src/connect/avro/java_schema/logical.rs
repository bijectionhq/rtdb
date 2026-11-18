use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect::avro) enum Builtin {
    Uuid, BigDecimal, Date, TimeMillis, TimeMicros, TimestampMillis,
    TimestampMicros, LocalTimestampMillis, LocalTimestampMicros,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect::avro) enum LogicalType {
    Builtin(Builtin),
    Decimal { precision: i32, scale: i32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect::avro) enum LogicalIdentity {
    Singleton(Builtin),
    Decimal(SchemaId),
}

pub(in crate::connect::avro) enum FactoryOutcome {
    NoType,
    RuntimeException,
    Singleton(Builtin),
    UnmodelledType,
}

/// Singleton outcomes identify the actual pinned LogicalTypes instances in the effective source registry.
/// Native operator scratch is admitted through the supplied context, and its errors remain outer errors.
/// RuntimeException models only a source factory failure caught by fromSchemaIgnoreInvalid.
pub(in crate::connect::avro) trait LogicalEnvironment {
    fn factory(
        &mut self, name: JavaText<'_>, schema: SchemaView<'_>, context: &mut ConversionContext,
    ) -> Result<FactoryOutcome>;
    fn fixed_decimal_precision(&mut self, size: i32, context: &mut ConversionContext) -> Result<i64>;
}

pub(in crate::connect::avro) struct Unregistered;
impl LogicalEnvironment for Unregistered {
    fn factory(&mut self, _: JavaText<'_>, _: SchemaView<'_>, _: &mut ConversionContext) -> Result<FactoryOutcome> {
        Ok(FactoryOutcome::NoType)
    }
    fn fixed_decimal_precision(&mut self, _: i32, _: &mut ConversionContext) -> Result<i64> {
        Err(Anomaly::unsupported(
            "rtdb/avro-logical-math-profile-required",
            "Fixed Decimal precision requires an effective source Math observation",
        ))
    }
}

fn property<'a>(schema: SchemaView<'a>, key: &str) -> Option<&'a JsonNode> {
    schema.node().properties.get(key)
}
fn textual<'a>(schema: SchemaView<'a>, key: &str) -> Option<JavaText<'a>> {
    property(schema, key)?.as_text().map(Text::view)
}
fn equal(text: JavaText<'_>, expected: &str) -> bool { text.units().eq(expected.encode_utf16()) }

impl Builtin {
    pub(in crate::connect::avro) fn name(self) -> &'static str {
        match self {
            Self::Uuid => "uuid", Self::BigDecimal => "big-decimal", Self::Date => "date",
            Self::TimeMillis => "time-millis", Self::TimeMicros => "time-micros",
            Self::TimestampMillis => "timestamp-millis", Self::TimestampMicros => "timestamp-micros",
            Self::LocalTimestampMillis => "local-timestamp-millis", Self::LocalTimestampMicros => "local-timestamp-micros",
        }
    }
    fn named(name: JavaText<'_>) -> Option<Self> {
        [Self::Uuid, Self::BigDecimal, Self::Date, Self::TimeMillis, Self::TimeMicros,
            Self::TimestampMillis, Self::TimestampMicros, Self::LocalTimestampMillis, Self::LocalTimestampMicros]
            .into_iter().find(|kind| equal(name, kind.name()))
    }
    fn accepts(self, kind: Type) -> bool {
        match self {
            Self::Uuid => true,
            Self::BigDecimal => kind == Type::Bytes,
            Self::Date | Self::TimeMillis => kind == Type::Int,
            _ => kind == Type::Long,
        }
    }
}
impl LogicalType {
    pub(in crate::connect::avro) fn name(self) -> &'static str {
        match self { Self::Builtin(kind) => kind.name(), Self::Decimal { .. } => "decimal" }
    }
    fn identity(self, schema: SchemaId) -> LogicalIdentity {
        match self { Self::Builtin(kind) => LogicalIdentity::Singleton(kind), Self::Decimal { .. } => LogicalIdentity::Decimal(schema) }
    }
    pub(in crate::connect::avro) fn decimal_hash(self) -> Option<i32> {
        match self { Self::Decimal { precision, scale } => Some(precision.wrapping_mul(31).wrapping_add(scale)), _ => None }
    }
}

fn integer(schema: SchemaView<'_>, key: &str) -> Option<i32> {
    match property(schema, key)?.value() { JsonValue::I32(value) => Some(*value), _ => None }
}
fn has_object_property(schema: SchemaView<'_>, key: &str) -> bool {
    property(schema, key).is_some_and(|node| !matches!(node.value(), JsonValue::Integer(_) | JsonValue::Decimal(_)))
}
fn decimal(schema: SchemaView<'_>) -> Option<LogicalType> {
    let precision = integer(schema, "precision")?;
    let scale = if has_object_property(schema, "scale") { integer(schema, "scale")? } else { 0 };
    Some(LogicalType::Decimal { precision, scale })
}

fn fixed_precision_bounds(size: i32) -> (i64, i64) {
    let bits = size.wrapping_mul(8).wrapping_sub(1);
    let low = f64::from_bits(0x3fd3_4413_509f_79fe);
    let high = f64::from_bits(0x3fd3_4413_509f_79ff);
    let (low, high) = if bits < 0 { (high, low) } else { (low, high) };
    ((low * f64::from(bits)).floor() as i64, (high * f64::from(bits)).floor() as i64)
}
fn decimal_fits(
    size: i32, precision: i32, environment: &mut dyn LogicalEnvironment, context: &mut ConversionContext,
) -> Result<bool> {
    let (low, high) = fixed_precision_bounds(size);
    if i64::from(precision) <= low { return Ok(true); }
    if i64::from(precision) > high { return Ok(false); }
    let observed = environment.fixed_decimal_precision(size, context)?;
    if !(low..=high).contains(&observed) {
        return Err(error("avro-logical-math-observation", "Source precision observation violates the pinned Math interval"));
    }
    Ok(i64::from(precision) <= observed)
}
fn valid(
    value: LogicalType, schema: SchemaView<'_>, environment: &mut dyn LogicalEnvironment, context: &mut ConversionContext,
) -> Result<bool> {
    for key in ["avro.java.string", "java-class", "java-key-class", "java-element-class"] {
        if textual(schema, key).is_some() { return Ok(false); }
    }
    Ok(match value {
        LogicalType::Builtin(kind) => kind.accepts(source_type(&schema.node().kind)),
        LogicalType::Decimal { precision, scale } => {
            let kind = &schema.node().kind;
            if !matches!(kind, Kind::Primitive(Type::Bytes) | Kind::Fixed { .. }) || precision <= 0 {
                return Ok(false);
            }
            if let Kind::Fixed { size, .. } = kind {
                if !decimal_fits(*size, precision, environment, context)? { return Ok(false); }
            }
            scale >= 0 && scale <= precision
        }
    })
}

pub(super) fn construct(
    schema: SchemaView<'_>, environment: &mut dyn LogicalEnvironment, context: &mut ConversionContext,
) -> Result<Option<LogicalType>> {
    let Some(name) = textual(schema, "logicalType") else { return Ok(None); };
    let value = if equal(name, "decimal") {
        let Some(value) = decimal(schema) else { return Ok(None); };
        value
    } else if let Some(kind) = Builtin::named(name) {
        LogicalType::Builtin(kind)
    } else {
        match environment.factory(name, schema, context)? {
            FactoryOutcome::NoType | FactoryOutcome::RuntimeException => return Ok(None),
            FactoryOutcome::Singleton(value) => LogicalType::Builtin(value),
            FactoryOutcome::UnmodelledType => return Err(Anomaly::unsupported(
                "rtdb/avro-custom-logical-type",
                "Custom source logical type requires its actual retained class/value model",
            )),
        }
    };
    Ok(valid(value, schema, environment, context)?.then_some(value))
}

impl SchemasBuilder {
    pub(super) fn construct_logical(
        &mut self, id: SchemaId, environment: &mut dyn LogicalEnvironment, context: &mut ConversionContext,
    ) -> Result<()> {
        self.require(id)?;
        let value = construct(SchemaView { owner: &self.owner, id }, environment, context)?;
        self.owner.nodes[id.index].logical = value;
        Ok(())
    }
}
impl<'a> SchemaView<'a> {
    pub(in crate::connect::avro) fn logical_type(self) -> Option<LogicalType> { self.node().logical }
    pub(in crate::connect::avro) fn logical_identity(self) -> Option<LogicalIdentity> { self.logical_type().map(|value| value.identity(self.id)) }
    pub(in crate::connect::avro) fn raw_logical_name(self) -> Option<JavaText<'a>> { textual(self, "logicalType") }
}

#[cfg(test)]
mod tests;
