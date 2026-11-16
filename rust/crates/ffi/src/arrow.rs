use std::{ffi::{CStr, c_void}, sync::Arc};
use arrow_array::{Array, RecordBatch, StructArray, ffi::{FFI_ArrowArray, from_ffi}};
use arrow_schema::{DataType, Schema, ffi::FFI_ArrowSchema};
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, estimate_read_value_bytes, from_field, read_value, to_field};
use rtdb_tx::SqlArguments;
use rtdb_types::{Anomaly, Result};

pub(crate) fn logical_schema(schema: &rtdb_plan::Schema) -> Result<Schema> {
    Ok(Schema::new(schema.fields().iter().map(|field| to_field(&field.name, &field.data_type)).collect::<Result<Vec<_>>>()?))
}
pub(crate) struct OwnedSchema { pub schema: Schema, _reservation: Arc<MemoryReservation> }
impl OwnedSchema {
    pub fn logical(schema: &rtdb_plan::Schema, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bound = schema.fields().iter().try_fold(4096, |bytes, field| super::add(bytes, super::add(super::mul(field.name.len(), 4)?, super::mul(field.data_type.retained_bytes()?, 16)?)?))?;
        let reservation = memory.reserve(bound)?;
        Ok(Self { schema: logical_schema(schema)?, _reservation: reservation })
    }
    pub fn empty(memory: &Arc<MemoryBudget>) -> Result<Self> { Self::logical(&Default::default(), memory) }
}

pub(crate) fn schema_bound(schema: &Schema) -> Result<usize> {
    fn field(field: &arrow_schema::Field) -> Result<usize> {
        let mut bytes = data_type(field.data_type())?;
        bytes = super::add(bytes, super::mul(field.name().len(), 4)?)?;
        for (key, value) in field.metadata() { bytes = super::add(bytes, super::mul(super::add(key.len(), value.len())?, 4)?)?; }
        Ok(bytes)
    }
    fn data_type(ty: &DataType) -> Result<usize> {
        let mut bytes = 4096;
        match ty {
            DataType::Struct(fields) => for child in fields { bytes = super::add(bytes, field(child)?)?; },
            DataType::Union(fields, _) => for (_, child) in fields.iter() { bytes = super::add(bytes, field(child)?)?; },
            DataType::List(child) | DataType::LargeList(child) | DataType::ListView(child) | DataType::LargeListView(child) | DataType::FixedSizeList(child, _) | DataType::Map(child, _) => bytes = super::add(bytes, field(child)?)?,
            DataType::RunEndEncoded(ends, values) => bytes = super::add(bytes, super::add(field(ends)?, field(values)?)?)?,
            DataType::Dictionary(key, value) => bytes = super::add(bytes, super::add(data_type(key)?, data_type(value)?)?)?,
            DataType::Timestamp(_, Some(zone)) => bytes = super::add(bytes, super::mul(zone.len(), 4)?)?,
            _ => {}
        }
        Ok(bytes)
    }
    let mut bytes = schema.fields().iter().try_fold(4096, |bytes, next| super::add(bytes, field(next)?))?;
    for (key, value) in schema.metadata() { bytes = super::add(bytes, super::mul(super::add(key.len(), value.len())?, 4)?)?; }
    Ok(bytes)
}

pub(crate) fn export_schema(schema: &Schema, memory: &Arc<MemoryBudget>) -> Result<FFI_ArrowSchema> {
    let reservation = memory.reserve(schema_bound(schema)?)?;
    let mut exported = FFI_ArrowSchema::try_from(schema).map_err(arrow_error)?;
    retain_schema(&mut exported, reservation);
    Ok(exported)
}

pub(crate) fn export_batch(batch: OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<(FFI_ArrowArray, FFI_ArrowSchema)> {
    let reservation = memory.reserve(super::add(schema_bound(batch.batch().schema().as_ref())?, batch.batch().get_array_memory_size())?)?;
    let schema = export_schema(batch.batch().schema().as_ref(), memory)?;
    let array = StructArray::from(batch.batch().clone());
    let mut exported = FFI_ArrowArray::new(&array.to_data());
    retain_array(&mut exported, reservation);
    Ok((exported, schema))
}

struct ArrayLease { private: *mut c_void, release: Option<unsafe extern "C" fn(*mut FFI_ArrowArray)>, _reservation: Arc<MemoryReservation> }
struct SchemaLease { private: *mut c_void, release: Option<unsafe extern "C" fn(*mut FFI_ArrowSchema)>, _reservation: Arc<MemoryReservation> }

fn retain_array(array: &mut FFI_ArrowArray, reservation: Arc<MemoryReservation>) {
    for index in 0..array.n_children as usize { unsafe { retain_array(&mut **array.children.add(index), Arc::clone(&reservation)); } }
    if !array.dictionary.is_null() { unsafe { retain_array(&mut *array.dictionary, Arc::clone(&reservation)); } }
    let lease = Box::new(ArrayLease { private: array.private_data, release: array.release, _reservation: reservation });
    array.private_data = Box::into_raw(lease).cast(); array.release = Some(release_array);
}
fn retain_schema(schema: &mut FFI_ArrowSchema, reservation: Arc<MemoryReservation>) {
    for index in 0..schema.n_children as usize { unsafe { retain_schema(&mut **schema.children.add(index), Arc::clone(&reservation)); } }
    if !schema.dictionary.is_null() { unsafe { retain_schema(&mut *schema.dictionary, Arc::clone(&reservation)); } }
    let lease = Box::new(SchemaLease { private: schema.private_data, release: schema.release, _reservation: reservation });
    schema.private_data = Box::into_raw(lease).cast(); schema.release = Some(release_schema);
}
unsafe extern "C" fn release_array(pointer: *mut FFI_ArrowArray) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        let Some(array) = pointer.as_mut() else { return; };
        if array.release.is_none() { return; }
        let lease = Box::from_raw(array.private_data.cast::<ArrayLease>());
        array.private_data = lease.private; array.release = lease.release;
        if let Some(release) = array.release { release(array); }
        array.release = None;
    }));
}
unsafe extern "C" fn release_schema(pointer: *mut FFI_ArrowSchema) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        let Some(schema) = pointer.as_mut() else { return; };
        if schema.release.is_none() { return; }
        let lease = Box::from_raw(schema.private_data.cast::<SchemaLease>());
        schema.private_data = lease.private; schema.release = lease.release;
        if let Some(release) = schema.release { release(schema); }
        schema.release = None;
    }));
}

/// Borrows a valid Arrow C Data schema tree, including its terminated strings and metadata buffers.
/// All pointers must remain readable throughout this call; the Arrow producer owns their validity.
pub(crate) unsafe fn foreign_schema_bound(schema: &FFI_ArrowSchema, depth: usize) -> Result<usize> {
    if depth > 64 || schema.n_children < 0 { return Err(super::invalid("Arrow schema nesting or child count is invalid")); }
    let mut bytes = 4096;
    for pointer in [schema.format, schema.name] {
        if !pointer.is_null() { bytes = super::add(bytes, super::mul(unsafe { CStr::from_ptr(pointer) }.to_bytes().len(), 8)?)?; }
    }
    if !schema.metadata.is_null() {
        let mut pointer = schema.metadata.cast::<u8>();
        let count = unsafe { pointer.cast::<i32>().read_unaligned() };
        if count < 0 { return Err(super::invalid("Arrow metadata has a negative entry count")); }
        pointer = unsafe { pointer.add(4) };
        for _ in 0..count {
            for _ in 0..2 {
                let length = unsafe { pointer.cast::<i32>().read_unaligned() };
                if length < 0 { return Err(super::invalid("Arrow metadata has a negative string length")); }
                bytes = super::add(bytes, super::add(super::mul(length as usize, 8)?, 256)?)?;
                pointer = unsafe { pointer.add(super::add(length as usize, 4)?) };
            }
        }
    }
    if schema.n_children > 0 && schema.children.is_null() { return Err(super::invalid("Arrow schema children pointer is null")); }
    for index in 0..schema.n_children as usize {
        let child = unsafe { *schema.children.add(index) };
        let child = unsafe { child.as_ref() }.ok_or_else(|| super::invalid("Arrow schema child is null"))?;
        bytes = super::add(bytes, unsafe { foreign_schema_bound(child, depth + 1) }?)?;
    }
    if !schema.dictionary.is_null() { bytes = super::add(bytes, unsafe { foreign_schema_bound(&*schema.dictionary, depth + 1) }?)?; }
    Ok(bytes)
}

pub(crate) struct ImportedRows {
    pub arguments: SqlArguments,
    pub names: Vec<String>,
    pub _reservation: Arc<MemoryReservation>,
}

/// Takes ownership of a valid Arrow C Data array and schema by replacing both with released structs.
/// Foreign buffers are copied into retained native values before returning, including on zero-row binds.
pub(crate) unsafe fn import_rows(array: &mut FFI_ArrowArray, schema: &mut FFI_ArrowSchema, memory: &Arc<MemoryBudget>) -> Result<ImportedRows> {
    if array.release.is_none() || schema.release.is_none() { return Err(super::invalid("Arrow bind inputs have already been released")); }
    let schema_charge = memory.reserve(unsafe { foreign_schema_bound(schema, 0) }?)?;
    let array = std::mem::replace(array, FFI_ArrowArray::empty());
    let schema = std::mem::replace(schema, FFI_ArrowSchema::empty());
    let fields = Schema::try_from(&schema).map_err(arrow_error)?;
    let imported = unsafe { from_ffi(array, &schema) }.map_err(arrow_error)?;
    imported.validate_full().map_err(arrow_error)?;
    if !matches!(imported.data_type(), DataType::Struct(_)) { return Err(super::invalid("Arrow bind requires a record-batch struct")); }
    let array = StructArray::from(imported);
    if array.null_count() > 0 { return Err(super::invalid("The bound record batch may not contain null rows")); }
    let batch = RecordBatch::from(array);
    let mut bound = super::add(4096, super::mul(batch.num_rows(), 64)?)?;
    for row in 0..batch.num_rows() {
        for (field, column) in fields.fields().iter().zip(batch.columns()) {
            bound = super::add(bound, estimate_read_value_bytes(field, column.as_ref(), row)?)?;
        }
    }
    let _scratch = memory.reserve(super::mul(bound, 2)?)?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        rows.push(fields.fields().iter().zip(batch.columns()).map(|(field, column)| read_value(field, column.as_ref(), row)).collect::<Result<Vec<_>>>()?);
    }
    let types = fields.fields().iter().map(|field| from_field(field)).collect::<Result<Vec<_>>>()?;
    let names = fields.fields().iter().map(|field| field.name().clone()).collect();
    Ok(ImportedRows { arguments: SqlArguments::new(types, rows, memory)?, names, _reservation: schema_charge })
}
pub(crate) fn arrow_error(error: arrow_schema::ArrowError) -> Anomaly { Anomaly::incorrect("rtdb.ffi/arrow", error.to_string()) }
