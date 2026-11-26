use std::sync::Arc;
use arrow_schema::{Schema, SchemaRef};
use rtdb_columnar::{DecimalParameters, INTERNAL_DECIMAL_PRECISION_KEY, INTERNAL_DECIMAL_SCALE_KEY, decimal_field_parameters, decimal_parameters, decimal_storage_precision, decimal_storage_scale, map_schema_fields, with_decimal_parameters};
use rtdb_types::{Anomaly, Result};

pub(crate) struct DecimalSchema { native: SchemaRef, pub wire: SchemaRef, decimals: Vec<DecimalParameters> }
impl DecimalSchema {
    pub fn new(schema: &Schema) -> Result<Self> {
        if schema.metadata().keys().any(|key| reserved(key)) { return Err(invalid("Decimal annotation belongs to a field")); }
        let mut decimals = Vec::new();
        let wire = map_schema_fields(schema, |mut field| {
            if let Some(parameters) = decimal_field_parameters(&field)? { decimals.push(parameters); }
            let mut metadata = field.metadata().clone();
            metadata.remove(INTERNAL_DECIMAL_SCALE_KEY);
            metadata.remove(INTERNAL_DECIMAL_PRECISION_KEY);
            field.set_metadata(metadata);
            Ok(field)
        })?;
        Ok(Self { native: Arc::new(schema.clone()), wire: Arc::new(wire), decimals })
    }
    pub fn native(&self) -> &Schema { &self.native }
    pub fn validate_batch(&self, batch: &arrow_array::RecordBatch) -> Result<()> {
        if batch.schema().as_ref() != self.native.as_ref() {
            return Err(Anomaly::incorrect("rtdb.format/arrow-schema", "Arrow page schema differs from its declared stream schema"));
        }
        Ok(())
    }
    pub fn patch_message(&self, bytes: &mut [u8]) -> Result<()> {
        let schema = arrow_ipc::root_as_message(bytes).map_err(invalid)?.header_as_schema().ok_or_else(|| invalid("Arrow message has no schema"))?;
        let patches = patches(schema, &self.decimals)?;
        apply(bytes, &patches)
    }
    pub fn patch_footer(&self, bytes: &mut [u8]) -> Result<()> {
        let schema = arrow_ipc::root_as_footer(bytes).map_err(invalid)?.schema().ok_or_else(|| invalid("Arrow footer has no schema"))?;
        let patches = patches(schema, &self.decimals)?;
        apply(bytes, &patches)
    }
}

fn visit<'a>(schema: arrow_ipc::Schema<'a>, mut field: impl FnMut(arrow_ipc::Field<'a>) -> Result<()>) -> Result<()> {
    fn descend<'a>(value: arrow_ipc::Field<'a>, field: &mut impl FnMut(arrow_ipc::Field<'a>) -> Result<()>, depth: usize) -> Result<()> {
        if depth > 64 { return Err(invalid("Arrow field nesting exceeds the decoder limit")); }
        if let Some(children) = value.children() { for child in children { descend(child, field, depth + 1)?; } }
        field(value)
    }
    if let Some(fields) = schema.fields() { for value in fields { descend(value, &mut field, 0)?; } }
    Ok(())
}

pub fn validate(schema: arrow_ipc::Schema<'_>) -> Result<()> {
    if schema.custom_metadata().is_some_and(|entries| entries.iter().any(|entry| entry.key().is_some_and(reserved))) {
        return Err(invalid("External schema contains reserved decimal metadata"));
    }
    visit(schema, |field| {
        if field.custom_metadata().is_some_and(|entries| entries.iter().any(|entry| entry.key().is_some_and(reserved))) {
            return Err(invalid("External field contains reserved decimal metadata"));
        }
        if field.type_type() == arrow_ipc::Type::Decimal {
            let decimal = field.type_as_decimal().ok_or_else(|| invalid("Arrow decimal field has no metadata"))?;
            if !matches!(decimal.bitWidth(), 32 | 64 | 128 | 256) {
                return Err(invalid("Arrow decimal bit width is unsupported"));
            }
            if schema.endianness() == arrow_ipc::Endianness::Big { return Err(invalid("Big-endian decimal IPC is unsupported")); }
        }
        Ok(())
    })
}

pub fn decode(schema: arrow_ipc::Schema<'_>) -> Result<Schema> {
    validate(schema)?;
    let mut decimals = Vec::new();
    let mut replacements = Vec::new();
    visit(schema, |field| {
        if let Some(decimal) = field.type_as_decimal() {
            decimals.push(DecimalParameters { precision: decimal.precision(), scale: decimal.scale(), bit_width: decimal.bitWidth() as u16 });
            if u8::try_from(decimal.precision()).is_err() {
                replacements.push((parameter_position(decimal, arrow_ipc::Decimal::VT_PRECISION)?, i32::from(decimal_storage_precision(decimal.precision()))));
            }
            if i8::try_from(decimal.scale()).is_err() {
                replacements.push((parameter_position(decimal, arrow_ipc::Decimal::VT_SCALE)?, i32::from(decimal_storage_scale(decimal.scale()))));
            }
        }
        Ok(())
    })?;
    let native = if replacements.is_empty() { arrow_ipc::convert::fb_to_schema(schema) } else {
        let mut bytes = schema._tab.buf().to_vec();
        apply(&mut bytes, &replacements)?;
        let root = u32::try_from(schema._tab.loc()).map_err(invalid)?.to_le_bytes();
        bytes.get_mut(..4).ok_or_else(|| invalid("Arrow schema has no root offset"))?.copy_from_slice(&root);
        arrow_ipc::convert::fb_to_schema(arrow_ipc::root_as_schema(&bytes).map_err(invalid)?)
    };
    let mut decimals = decimals.into_iter();
    let restored = map_schema_fields(&native, |field| {
        if decimal_parameters(field.data_type()).is_some() { with_decimal_parameters(field, decimals.next().ok_or_else(|| invalid("Decimal schema field count differs"))?) }
        else { Ok(field) }
    })?;
    if decimals.next().is_some() { return Err(invalid("Decimal schema field count differs")); }
    Ok(restored)
}

fn patches(schema: arrow_ipc::Schema<'_>, decimals: &[DecimalParameters]) -> Result<Vec<(usize, i32)>> {
    let mut decimals = decimals.iter();
    let mut patches = Vec::new();
    visit(schema, |field| {
        if let Some(decimal) = field.type_as_decimal() {
            let parameters = decimals.next().ok_or_else(|| invalid("Decimal wire field count differs"))?;
            if decimal.precision() != i32::from(decimal_storage_precision(parameters.precision)) || decimal.scale() != i32::from(decimal_storage_scale(parameters.scale)) || decimal.bitWidth() != i32::from(parameters.bit_width) {
                return Err(invalid("Decimal wire carrier differs"));
            }
            if u8::try_from(parameters.precision).is_err() {
                patches.push((parameter_position(decimal, arrow_ipc::Decimal::VT_PRECISION)?, parameters.precision));
            }
            if i8::try_from(parameters.scale).is_err() {
                patches.push((parameter_position(decimal, arrow_ipc::Decimal::VT_SCALE)?, parameters.scale));
            }
        }
        Ok(())
    })?;
    if decimals.next().is_some() { return Err(invalid("Decimal wire field count differs")); }
    Ok(patches)
}
fn parameter_position(decimal: arrow_ipc::Decimal<'_>, parameter: u16) -> Result<usize> {
    let offset = decimal._tab.vtable().get(parameter);
    if offset == 0 { return Err(invalid("Decimal carrier omitted its parameter slot")); }
    decimal._tab.loc().checked_add(usize::from(offset)).ok_or_else(|| invalid("Decimal parameter offset overflow"))
}
fn apply(bytes: &mut [u8], patches: &[(usize, i32)]) -> Result<()> {
    for (offset, value) in patches {
        let end = offset.checked_add(4).ok_or_else(|| invalid("Decimal parameter offset overflow"))?;
        bytes.get_mut(*offset..end).ok_or_else(|| invalid("Decimal parameter exceeds metadata"))?.copy_from_slice(&value.to_le_bytes());
    }
    Ok(())
}
fn reserved(key: &str) -> bool { matches!(key, INTERNAL_DECIMAL_PRECISION_KEY | INTERNAL_DECIMAL_SCALE_KEY) }
fn invalid(message: impl std::fmt::Display) -> Anomaly { Anomaly::unsupported("rtdb.format/decimal-metadata", message.to_string()) }
