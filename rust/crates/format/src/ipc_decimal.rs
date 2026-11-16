use std::sync::Arc;
use arrow_schema::{Schema, SchemaRef};
use rtdb_columnar::{INTERNAL_DECIMAL_SCALE_KEY, decimal_parameters, decimal_scale, decimal_storage_scale, map_schema_fields, with_decimal_scale};
use rtdb_types::{Anomaly, Result};

pub(crate) struct DecimalSchema { native: SchemaRef, pub wire: SchemaRef, scales: Vec<Option<i32>> }
impl DecimalSchema {
    pub fn new(schema: &Schema) -> Result<Self> {
        if schema.metadata().contains_key(INTERNAL_DECIMAL_SCALE_KEY) { return Err(invalid("Decimal scale annotation belongs to a field")); }
        let mut scales = Vec::new();
        let wire = map_schema_fields(schema, |mut field| {
            if let Some(scale) = decimal_scale(&field)? { scales.push(i8::try_from(scale).is_err().then_some(scale)); }
            let mut metadata = field.metadata().clone();
            metadata.remove(INTERNAL_DECIMAL_SCALE_KEY);
            field.set_metadata(metadata);
            Ok(field)
        })?;
        Ok(Self { native: Arc::new(schema.clone()), wire: Arc::new(wire), scales })
    }
    pub fn validate_batch(&self, batch: &arrow_array::RecordBatch) -> Result<()> {
        if batch.schema().as_ref() != self.native.as_ref() {
            return Err(Anomaly::incorrect("rtdb.format/arrow-schema", "Arrow page schema differs from its declared stream schema"));
        }
        Ok(())
    }
    pub fn patch_message(&self, bytes: &mut [u8]) -> Result<()> {
        let schema = arrow_ipc::root_as_message(bytes).map_err(invalid)?.header_as_schema().ok_or_else(|| invalid("Arrow message has no schema"))?;
        let patches = patches(schema, &self.scales)?;
        apply(bytes, &patches)
    }
    pub fn patch_footer(&self, bytes: &mut [u8]) -> Result<()> {
        let schema = arrow_ipc::root_as_footer(bytes).map_err(invalid)?.schema().ok_or_else(|| invalid("Arrow footer has no schema"))?;
        let patches = patches(schema, &self.scales)?;
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
    if schema.custom_metadata().is_some_and(|entries| entries.iter().any(|entry| entry.key() == Some(INTERNAL_DECIMAL_SCALE_KEY))) {
        return Err(invalid("External schema contains reserved decimal metadata"));
    }
    visit(schema, |field| {
        if field.custom_metadata().is_some_and(|entries| entries.iter().any(|entry| entry.key() == Some(INTERNAL_DECIMAL_SCALE_KEY))) {
            return Err(invalid("External field contains reserved decimal metadata"));
        }
        if field.type_type() == arrow_ipc::Type::Decimal {
            let decimal = field.type_as_decimal().ok_or_else(|| invalid("Arrow decimal field has no metadata"))?;
            if u8::try_from(decimal.precision()).is_err() || !matches!(decimal.bitWidth(), 32 | 64 | 128 | 256) {
                return Err(invalid("Arrow decimal precision or bit width is unsupported"));
            }
            if schema.endianness() == arrow_ipc::Endianness::Big { return Err(invalid("Big-endian decimal IPC is unsupported")); }
        }
        Ok(())
    })
}

pub fn decode(schema: arrow_ipc::Schema<'_>) -> Result<Schema> {
    validate(schema)?;
    let mut scales = Vec::new();
    let mut replacements = Vec::new();
    visit(schema, |field| {
        if let Some(decimal) = field.type_as_decimal() {
            scales.push(decimal.scale());
            if i8::try_from(decimal.scale()).is_err() {
                replacements.push((scale_position(decimal)?, i32::from(decimal_storage_scale(decimal.scale()))));
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
    let mut scales = scales.into_iter();
    let restored = map_schema_fields(&native, |field| {
        if decimal_parameters(field.data_type()).is_some() { with_decimal_scale(field, scales.next().ok_or_else(|| invalid("Decimal schema field count differs"))?) }
        else { Ok(field) }
    })?;
    if scales.next().is_some() { return Err(invalid("Decimal schema field count differs")); }
    Ok(restored)
}

fn patches(schema: arrow_ipc::Schema<'_>, scales: &[Option<i32>]) -> Result<Vec<(usize, i32)>> {
    let mut scales = scales.iter();
    let mut patches = Vec::new();
    visit(schema, |field| {
        if let Some(decimal) = field.type_as_decimal() {
            if let Some(scale) = scales.next().ok_or_else(|| invalid("Decimal wire field count differs"))? {
                if decimal.scale() != i32::from(decimal_storage_scale(*scale)) { return Err(invalid("Decimal wire carrier scale differs")); }
                patches.push((scale_position(decimal)?, *scale));
            }
        }
        Ok(())
    })?;
    if scales.next().is_some() { return Err(invalid("Decimal wire field count differs")); }
    Ok(patches)
}
fn scale_position(decimal: arrow_ipc::Decimal<'_>) -> Result<usize> {
    let offset = decimal._tab.vtable().get(arrow_ipc::Decimal::VT_SCALE);
    if offset == 0 { return Err(invalid("Decimal carrier omitted its scale slot")); }
    decimal._tab.loc().checked_add(usize::from(offset)).ok_or_else(|| invalid("Decimal scale offset overflow"))
}
fn apply(bytes: &mut [u8], patches: &[(usize, i32)]) -> Result<()> {
    for (offset, scale) in patches {
        let end = offset.checked_add(4).ok_or_else(|| invalid("Decimal scale offset overflow"))?;
        bytes.get_mut(*offset..end).ok_or_else(|| invalid("Decimal scale exceeds metadata"))?.copy_from_slice(&scale.to_le_bytes());
    }
    Ok(())
}
fn invalid(message: impl std::fmt::Display) -> Anomaly { Anomaly::unsupported("rtdb.format/decimal-metadata", message.to_string()) }
