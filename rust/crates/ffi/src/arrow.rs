use std::{ffi::{CStr, CString, c_char, c_void}, mem::size_of, sync::Arc};
use arrow_array::{
    Array, RecordBatch, RecordBatchOptions, StructArray, cast::AsArray,
    ffi::{FFI_ArrowArray, from_ffi_and_data_type}, make_array,
    types::{BinaryViewType, Int16Type, Int32Type, Int64Type, StringViewType},
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Schema, ffi::FFI_ArrowSchema};
use rtdb_columnar::{DecimalParameters, DeclaredBatch, MemoryBudget, MemoryReservation, OwnedBatch, from_field, to_field};
use rtdb_tx::SqlArguments;
use rtdb_types::{Anomaly, LogicalType, Result};

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
    pub fn parameters(count: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        u32::try_from(count.saturating_sub(1))
            .map_err(|_| Anomaly::incorrect("rtdb.node/preparation-size", "Prepared statement retained size exceeds usize"))?;
        let unknown = LogicalType::Null;
        let field_bytes = super::mul(unknown.retained_bytes()?, 16)?;
        let reservation = memory.reserve(super::add(4096, super::mul(count, field_bytes)?)?)?;
        let fields = (0..count).map(|_| to_field("", &unknown)).collect::<Result<Vec<_>>>()?;
        Ok(Self { schema: Schema::new(fields), _reservation: reservation })
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
    if schema.metadata().keys().any(|key| reserved_decimal_key(key)) { return Err(super::invalid("Decimal annotation cannot appear on a schema")); }
    let mut decimals = Vec::new();
    let clean = rtdb_columnar::map_schema_fields(schema, |mut field| {
        if let Some(parameters) = rtdb_columnar::decimal_field_parameters(&field)? { decimals.push(parameters); }
        let mut metadata = field.metadata().clone(); metadata.retain(|key, _| !reserved_decimal_key(key)); field.set_metadata(metadata); Ok(field)
    })?;
    let mut exported = FFI_ArrowSchema::try_from(&clean).map_err(arrow_error)?;
    let mut decimals = decimals.into_iter();
    retain_schema(&mut exported, reservation, &mut decimals)?;
    if decimals.next().is_some() { return Err(super::invalid("Unused decimal C export parameters")); }
    Ok(exported)
}

#[cfg(test)]
pub(crate) fn export_batch(batch: OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<(FFI_ArrowArray, FFI_ArrowSchema)> {
    let schema = batch.batch().schema();
    export_declared_batch(rtdb_columnar::conform_batch(batch, &schema, memory)?, memory)
}

pub(crate) fn export_declared_batch(batch: DeclaredBatch, memory: &Arc<MemoryBudget>) -> Result<(FFI_ArrowArray, FFI_ArrowSchema)> {
    let schema = export_schema(batch.declared_schema(), memory)?;
    Ok((export_declared_array(batch, memory)?, schema))
}

pub(crate) fn export_declared_array(batch: DeclaredBatch, memory: &Arc<MemoryBudget>) -> Result<FFI_ArrowArray> {
    let scratch = batch.batch().columns().iter().try_fold(512, |bytes, column| {
        super::add(bytes, array_data_scratch_bound(column.as_ref())?)
    })?;
    let _scratch = memory.reserve(scratch)?;
    let data = StructArray::from(batch.batch().clone()).into_data();
    let reservation = memory.reserve(super::add(size_of::<ArrayExport>() + 64, c_array_bound(&data)?)?)?;
    let mut exported = FFI_ArrowArray::new(&data);
    retain_array(&mut exported, Arc::new(ArrayExport { _batch: batch, _reservation: reservation }));
    Ok(exported)
}

fn array_data_scratch_bound(array: &dyn Array) -> Result<usize> {
    fn columns_bound(columns: &[arrow_array::ArrayRef]) -> Result<usize> {
        columns.iter().try_fold(0, |bytes, column| super::add(bytes, array_data_scratch_bound(column.as_ref())?))
    }

    // Arrow's built-in arrays reuse backing on to_data(). A byte view clones a
    // variable number of Buffer handles, and a dictionary clones boxed types.
    let nested = match array.data_type() {
        DataType::Struct(_) => columns_bound(array.as_struct().columns())?,
        DataType::Union(fields, _) => fields.iter().try_fold(0, |bytes, (id, _)| {
            super::add(bytes, array_data_scratch_bound(array.as_union().child(id).as_ref())?)
        })?,
        DataType::List(_) => array_data_scratch_bound(array.as_list::<i32>().values().as_ref())?,
        DataType::LargeList(_) => array_data_scratch_bound(array.as_list::<i64>().values().as_ref())?,
        DataType::ListView(_) => array_data_scratch_bound(array.as_list_view::<i32>().values().as_ref())?,
        DataType::LargeListView(_) => array_data_scratch_bound(array.as_list_view::<i64>().values().as_ref())?,
        DataType::FixedSizeList(_, _) => array_data_scratch_bound(array.as_fixed_size_list().values().as_ref())?,
        DataType::Map(_, _) => array_data_scratch_bound(array.as_map().entries())?,
        DataType::RunEndEncoded(ends, _) => {
            let values = match ends.data_type() {
                DataType::Int16 => array.as_run::<Int16Type>().values(),
                DataType::Int32 => array.as_run::<Int32Type>().values(),
                DataType::Int64 => array.as_run::<Int64Type>().values(),
                _ => return Err(super::invalid("Arrow run-end type is invalid")),
            };
            super::add(512, array_data_scratch_bound(values.as_ref())?)?
        }
        DataType::Dictionary(_, _) => {
            let mut bytes = array_data_scratch_bound(array.as_any_dictionary().values().as_ref())?;
            let mut data_type = array.data_type();
            while let DataType::Dictionary(_, values) = data_type {
                bytes = super::add(bytes, 2 * size_of::<DataType>())?;
                data_type = values;
            }
            bytes
        }
        DataType::Utf8View => super::mul(array.as_byte_view::<StringViewType>().data_buffers().len(), 2 * size_of::<Buffer>())?,
        DataType::BinaryView => super::mul(array.as_byte_view::<BinaryViewType>().data_buffers().len(), 2 * size_of::<Buffer>())?,
        _ => 0,
    };
    super::add(512, nested)
}

fn c_array_bound(data: &ArrayData) -> Result<usize> {
    // FFI_ArrowArray::new allocates per-node private data, buffer slots and child
    // pointers. Its only new payloads are validity alignment and view lengths.
    let mut bytes = 256 + size_of::<FFI_ArrowArray>() + size_of::<ArrayLease>();
    let slots = super::add(data.buffers().len(), 2)?;
    bytes = super::add(bytes, super::mul(slots, 2 * (size_of::<Option<Buffer>>() + size_of::<*const c_void>()))?)?;
    bytes = super::add(bytes, super::mul(data.child_data().len(), size_of::<*mut FFI_ArrowArray>())?)?;
    if matches!(data.data_type(), DataType::Utf8View | DataType::BinaryView) {
        let entries = data.buffers().len().saturating_sub(1);
        bytes = super::add(bytes, super::mul(entries, 2 * size_of::<i64>())?.max(4 * size_of::<i64>()))?;
    }
    if let Some(nulls) = data.nulls() {
        let bitmap = if data.offset() == nulls.offset() {
            0
        } else if data.offset() != 0 {
            super::add(data.offset(), nulls.len())?.div_ceil(8)
        } else if nulls.offset() % 8 != 0 {
            super::mul(nulls.len().div_ceil(64).max(4), size_of::<u64>())?
        } else {
            0
        };
        bytes = super::add(bytes, bitmap)?;
    }
    data.child_data().iter().try_fold(bytes, |bytes, child| super::add(bytes, c_array_bound(child)?))
}

struct ArrayExport { _batch: DeclaredBatch, _reservation: Arc<MemoryReservation> }
struct ArrayLease { private: *mut c_void, release: Option<unsafe extern "C" fn(*mut FFI_ArrowArray)>, _owner: Arc<ArrayExport> }
struct SchemaLease { private: *mut c_void, release: Option<unsafe extern "C" fn(*mut FFI_ArrowSchema)>, format: *const c_char, _full_format: Option<CString>, _reservation: Arc<MemoryReservation> }

fn retain_array(array: &mut FFI_ArrowArray, owner: Arc<ArrayExport>) {
    for index in 0..array.n_children as usize { unsafe { retain_array(&mut **array.children.add(index), Arc::clone(&owner)); } }
    if !array.dictionary.is_null() { unsafe { retain_array(&mut *array.dictionary, Arc::clone(&owner)); } }
    let lease = Box::new(ArrayLease { private: array.private_data, release: array.release, _owner: owner });
    array.private_data = Box::into_raw(lease).cast(); array.release = Some(release_array);
}
fn retain_schema(schema: &mut FFI_ArrowSchema, reservation: Arc<MemoryReservation>, decimals: &mut impl Iterator<Item = DecimalParameters>) -> Result<()> {
    for index in 0..schema.n_children as usize { unsafe { retain_schema(&mut **schema.children.add(index), Arc::clone(&reservation), decimals)?; } }
    if !schema.dictionary.is_null() { unsafe { retain_schema(&mut *schema.dictionary, Arc::clone(&reservation), decimals)?; } }
    let format = schema.format;
    let full_format = if decimal_format(schema.format())?.is_some() {
        let DecimalParameters { precision, scale, bit_width } = decimals.next().ok_or_else(|| super::invalid("Decimal C schema traversal disagrees with native schema"))?;
        if i8::try_from(scale).is_err() || u8::try_from(precision).is_err() {
            let text = if bit_width == 128 { format!("d:{precision},{scale}") } else { format!("d:{precision},{scale},{bit_width}") };
            Some(CString::new(text).map_err(|_| super::invalid("Invalid decimal C format"))?)
        } else { None }
    } else { None };
    if let Some(full_format) = &full_format { schema.format = full_format.as_ptr(); }
    let lease = Box::new(SchemaLease { private: schema.private_data, release: schema.release, format, _full_format: full_format, _reservation: reservation });
    schema.private_data = Box::into_raw(lease).cast(); schema.release = Some(release_schema);
    Ok(())
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
        schema.format = lease.format;
        schema.private_data = lease.private; schema.release = lease.release;
        if let Some(release) = schema.release { release(schema); }
        schema.release = None;
    }));
}

// The producer must keep the C strings, metadata and child pointers readable.
unsafe fn foreign_schema_bound(schema: &FFI_ArrowSchema, depth: usize) -> Result<usize> {
    if depth > 64 || schema.n_children < 0 { return Err(super::invalid("Arrow schema nesting or child count is invalid")); }
    let mut bytes = 4096;
    for pointer in [schema.format, schema.name] {
        if !pointer.is_null() { bytes = super::add(bytes, super::mul(unsafe { CStr::from_ptr(pointer) }.to_bytes().len(), 8)?)?; }
    }
    unsafe {
        visit_foreign_metadata(schema, |text, _| {
            bytes = super::add(bytes, super::add(super::mul(text.len(), 8)?, 256)?)?;
            Ok(())
        })?;
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

unsafe fn visit_foreign_metadata(schema: &FFI_ArrowSchema, mut visit: impl FnMut(&[u8], bool) -> Result<()>) -> Result<()> {
    if schema.metadata.is_null() { return Ok(()); }
    let mut pointer = schema.metadata.cast::<u8>();
    let count = unsafe { pointer.cast::<i32>().read_unaligned() };
    if count < 0 { return Err(super::invalid("Arrow metadata has a negative entry count")); }
    pointer = unsafe { pointer.add(4) };
    for _ in 0..count {
        for is_key in [true, false] {
            let length = unsafe { pointer.cast::<i32>().read_unaligned() };
            if length < 0 { return Err(super::invalid("Arrow metadata has a negative string length")); }
            let length = length as usize;
            let next = super::add(length, 4)?;
            let text = unsafe { std::slice::from_raw_parts(pointer.add(4), length) };
            visit(text, is_key)?;
            pointer = unsafe { pointer.add(next) };
        }
    }
    Ok(())
}

// Arrow's schema parser can skip metadata on dictionary value nodes.
unsafe fn foreign_schema_needs_carrier(schema: &FFI_ArrowSchema) -> Result<bool> {
    unsafe {
        visit_foreign_metadata(schema, |text, is_key| {
            let text = std::str::from_utf8(text).map_err(|error| arrow_error(error.into()))?;
            if is_key && reserved_decimal_key(text) {
                return Err(super::invalid("External Arrow metadata uses a reserved decimal key"));
            }
            Ok(())
        })?;
    }
    let _ = schema.name();
    let format = schema.format();
    let mut needs_carrier = decimal_format(format)?.is_some_and(|decimal| {
        u8::try_from(decimal.precision).is_err()
            || i8::try_from(decimal.scale).is_err()
            || format.split(',').nth(2).is_some_and(|width| !matches!(width, "32" | "64" | "128" | "256"))
    });
    for child in schema.children().chain(schema.dictionary()) {
        needs_carrier |= unsafe { foreign_schema_needs_carrier(child) }?;
    }
    Ok(needs_carrier)
}

pub(crate) struct BoundArguments {
    pub arguments: SqlArguments,
    pub names: Vec<String>,
    pub _reservation: Arc<MemoryReservation>,
}

/// Takes ownership of a valid Arrow C Data array and schema by replacing both with released structs.
/// Foreign buffers are copied into native Arrow allocations before returning, including on zero-row binds.
/// Decimal precision and scale come from C format strings; reserved native field annotations are rejected.
pub(crate) unsafe fn import_arguments(array: &mut FFI_ArrowArray, schema: &mut FFI_ArrowSchema, memory: &Arc<MemoryBudget>) -> Result<BoundArguments> {
    unsafe { import_arguments_with_lifetime(array, schema, memory, false) }
}

pub(crate) unsafe fn import_retained_arguments(array: &mut FFI_ArrowArray, schema: &mut FFI_ArrowSchema, memory: &Arc<MemoryBudget>) -> Result<BoundArguments> {
    unsafe { import_arguments_with_lifetime(array, schema, memory, true) }
}

unsafe fn import_arguments_with_lifetime(
    array: &mut FFI_ArrowArray, schema: &mut FFI_ArrowSchema, memory: &Arc<MemoryBudget>, retain: bool,
) -> Result<BoundArguments> {
    if array.release.is_none() || schema.release.is_none() { return Err(super::invalid("Arrow bind inputs have already been released")); }
    let schema_charge = memory.reserve(unsafe { foreign_schema_bound(schema, 0) }?)?;
    let array = std::mem::replace(array, FFI_ArrowArray::empty());
    let schema = std::mem::replace(schema, FFI_ArrowSchema::empty());
    let fields = if unsafe { foreign_schema_needs_carrier(&schema) }? {
        import_decimal_schema(&schema)?
    } else {
        Schema::try_from(&schema).map_err(arrow_error)?
    };
    let imported = unsafe { from_ffi_and_data_type(array, DataType::Struct(fields.fields().clone())) }.map_err(arrow_error)?;
    imported.validate_full().map_err(arrow_error)?;
    let array = StructArray::from(imported);
    if array.null_count() > 0 { return Err(super::invalid("The bound record batch may not contain null rows")); }
    for (field, column) in fields.fields().iter().zip(array.columns()) {
        let logical = from_field(field)?;
        rtdb_columnar::validate_readable_column(field, column.as_ref(), &logical, memory)?;
    }
    let names = fields.fields().iter().map(|field| field.name().clone()).collect();
    let options = RecordBatchOptions::new().with_row_count(Some(array.len()));
    let fields = Arc::new(fields);
    let batch = if retain {
        let batch = RecordBatch::try_new_with_options(fields, array.columns().to_vec(), &options).map_err(arrow_error)?;
        OwnedBatch::new(batch, memory)?
    } else {
        let bound = super::add(4096, super::mul(array.get_array_memory_size(), 4)?)?;
        let mut reservation = memory.reserve(bound)?;
        let columns = array.columns().iter().map(|column| copy_foreign_data(column.to_data()).map(make_array))
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(fields, columns, &options).map_err(arrow_error)?;
        Arc::get_mut(&mut reservation).expect("Arrow import owns its reservation").shrink_to(batch.get_array_memory_size())?;
        OwnedBatch::with_reservation(batch, reservation)?
    };
    drop(array);
    let arguments = SqlArguments::from_arrow_with_canonical_source(batch, memory)?;
    Ok(BoundArguments { arguments, names, _reservation: schema_charge })
}

fn copy_foreign_data(data: ArrayData) -> Result<ArrayData> {
    let buffers = data.buffers().iter().map(|buffer| Buffer::from_slice_ref(buffer.as_slice())).collect();
    let children = data.child_data().iter().map(|child| copy_foreign_data(child.clone())).collect::<Result<Vec<_>>>()?;
    let nulls = data.nulls().map(|nulls| {
        let buffer = Buffer::from_slice_ref(nulls.buffer().as_slice());
        NullBuffer::new(BooleanBuffer::new(buffer, nulls.offset(), nulls.len()))
    });
    data.into_builder().buffers(buffers).child_data(children).nulls(nulls).build().map_err(arrow_error)
}

fn decimal_format(format: &str) -> Result<Option<DecimalParameters>> {
    let Some(decimal) = format.strip_prefix("d:") else { return Ok(None); };
    let mut parts = decimal.split(',');
    let precision = parts.next().and_then(|part| part.parse::<i32>().ok()).ok_or_else(|| super::invalid("Invalid decimal C precision"))?;
    let scale = parts.next().and_then(|part| part.parse::<i32>().ok()).ok_or_else(|| super::invalid("Invalid decimal C scale"))?;
    let width = parts.next().map(|part| part.parse::<u16>().map_err(|_| super::invalid("Invalid decimal C width"))).transpose()?.unwrap_or(128);
    if parts.next().is_some() || !matches!(width, 32 | 64 | 128 | 256) { return Err(super::invalid("Invalid decimal C format")); }
    Ok(Some(DecimalParameters { precision, scale, bit_width: width }))
}

fn import_decimal_schema(schema: &FFI_ArrowSchema) -> Result<Schema> {
    let mut decimals = Vec::new();
    let carrier = foreign_carrier(schema, &mut decimals)?;
    let fields = Schema::try_from(&carrier).map_err(arrow_error)?;
    let mut decimals = decimals.into_iter();
    let fields = rtdb_columnar::map_schema_fields(&fields, |field| {
        if rtdb_columnar::decimal_parameters(field.data_type()).is_some() {
            rtdb_columnar::with_decimal_parameters(field, decimals.next().ok_or_else(|| super::invalid("Decimal C schema traversal disagrees with native schema"))?)
        } else { Ok(field) }
    })?;
    if decimals.next().is_some() { return Err(super::invalid("Unused decimal C schema parameters")); }
    Ok(fields)
}

fn foreign_carrier(schema: &FFI_ArrowSchema, decimals: &mut Vec<DecimalParameters>) -> Result<FFI_ArrowSchema> {
    let metadata = schema.metadata().map_err(arrow_error)?;
    let children = schema.children().map(|child| foreign_carrier(child, decimals)).collect::<Result<Vec<_>>>()?;
    let dictionary = schema.dictionary().map(|dictionary| foreign_carrier(dictionary, decimals)).transpose()?;
    let format = match decimal_format(schema.format())? {
        Some(parameters) => {
            decimals.push(parameters);
            let precision = rtdb_columnar::decimal_storage_precision(parameters.precision);
            let scale = rtdb_columnar::decimal_storage_scale(parameters.scale);
            if parameters.bit_width == 128 { format!("d:{precision},{scale}") }
            else { format!("d:{precision},{scale},{}", parameters.bit_width) }
        }
        None => schema.format().to_owned(),
    };
    let mut carrier = FFI_ArrowSchema::try_new(&format, children, dictionary).map_err(arrow_error)?.with_metadata(metadata).map_err(arrow_error)?;
    if let Some(name) = schema.name() { carrier = carrier.with_name(name).map_err(arrow_error)?; }
    if let Some(flags) = schema.flags() { carrier = carrier.with_flags(flags).map_err(arrow_error)?; }
    Ok(carrier)
}
fn reserved_decimal_key(key: &str) -> bool { matches!(key, rtdb_columnar::INTERNAL_DECIMAL_PRECISION_KEY | rtdb_columnar::INTERNAL_DECIMAL_SCALE_KEY) }
pub(crate) fn arrow_error(error: arrow_schema::ArrowError) -> Anomaly { Anomaly::incorrect("rtdb.ffi/arrow", error.to_string()) }
