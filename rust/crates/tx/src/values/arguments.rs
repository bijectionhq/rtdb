use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, cast::AsArray};
use arrow_schema::{DataType, Field, Schema, UnionMode};
use rtdb_columnar::{ArrowCell, BatchBuilder, MemoryBudget, MemoryReservation, OwnedBatch, arrow_relayout_bound, copy_evaluated_cells, copy_selected_cells, estimate_copy_cell_bytes, estimate_read_cell_bytes, read_value_as, relayout_arrow_column, to_field, validate_readable_column};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType};

use crate::source::canonical;
use super::{ArgumentChunk, SourceArgumentLayout, SqlArguments, normalized_document_array, normalized_field, overflow, plain_scalar_field};

impl SqlArguments {
    pub(crate) fn source_columnar_batch(&self, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
        if let [(_, chunk)] = self.chunks.as_slice()
            && let ArgumentChunk::Arrow { batch, source_layout: SourceArgumentLayout::BuiltFromRows, .. } = chunk.as_ref() {
            return Ok(batch.clone());
        }
        let mut output = BatchBuilder::new(super::schema_bytes(&self.types)?, memory)?;
        let mut fields = Vec::with_capacity(self.types.len());
        let mut columns = Vec::with_capacity(self.types.len());
        for (position, ty) in self.types.iter().enumerate() {
            let field = to_field(&format!("?_{position}"), ty)?;
            let vectors = self.chunks.iter().map(|(_, chunk)| argument_vector(chunk, position, &field, ty, memory)).collect::<Result<Vec<_>>>()?;
            let inputs = vectors.iter().map(|(array, _)| array.as_ref()).collect::<Vec<_>>();
            let bound = inputs.iter().try_fold(4096usize, |bytes, array| {
                (0..array.len()).try_fold(bytes, |bytes, row| {
                    estimate_copy_cell_bytes(&field, *array, row)?.checked_mul(4).and_then(|size| bytes.checked_add(size)).ok_or_else(overflow)
                })
            })?;
            let column = if let [(_, chunk)] = self.chunks.as_slice()
                && matches!(chunk.as_ref(), ArgumentChunk::Rows { .. }
                    | ArgumentChunk::Arrow { source_layout: SourceArgumentLayout::CanonicalRow, .. })
                && let [(array, _)] = vectors.as_slice() {
                output.column(array.get_array_memory_size(), || Ok(Arc::clone(array)))?
            } else { output.column(bound, || {
                let mut picks = Vec::with_capacity(self.row_count());
                for (page, array) in inputs.iter().enumerate() {
                    picks.extend((0..array.len()).map(|row| (page, row)));
                }
                copy_selected_cells(&inputs, &picks)
            })? };
            fields.push(field);
            columns.push(column);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(self.row_count()));
        let batch = RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), columns, &options)
            .map_err(|error| Anomaly::incorrect("rtdb.tx/invalid-sql-arguments", error.to_string()))?;
        output.finish(batch)
    }
}

type ArgumentVector = (ArrayRef, Option<Arc<MemoryReservation>>);

fn argument_vector(chunk: &ArgumentChunk, position: usize, target: &Field, ty: &LogicalType, memory: &Arc<MemoryBudget>)
    -> Result<ArgumentVector> {
    match chunk {
        ArgumentChunk::Arrow { batch, types, source_layout, .. } => {
            let batch = batch.batch();
            let field = batch.schema_ref().field(position);
            let array = batch.column(position);
            let source = &types[position];
            // A decoded row already has the writer's masked bytes, which are included in the source digest.
            if matches!(source_layout, SourceArgumentLayout::CanonicalRow | SourceArgumentLayout::BuiltFromRows)
                && source == ty && field.data_type() == target.data_type() {
                return Ok((Arc::clone(array), None));
            }
            if *source_layout == SourceArgumentLayout::Canonical
                && matches!(field.data_type(), DataType::List(_) | DataType::FixedSizeList(..) | DataType::Struct(_))
                && *ty == ty.unencoded() && ty.join(source) == *ty
                && canonical::selectable(field) && canonical::selectable(target)
                && validate_readable_column(field, array.as_ref(), source, memory).is_ok()
                && (0..array.len()).all(|row| canonical::selectable_cell(ArrowCell { field, array: array.as_ref(), row }, false))
                && let Some(aligned) = canonical::align(array, field, target, memory) {
                return Ok(aligned);
            }
            if (*source_layout == SourceArgumentLayout::Uploaded || canonical_argument_field(field))
                && can_copy_argument(source) && source.join(ty) == *ty {
                validate_argument_uris(field, array.as_ref())?;
                if has_valid_union_nulls(field, array.as_ref()) {
                    if let Some(output) = aligned_argument(array, field, target, memory) { return Ok(output); }
                } else if canonical_float_field(field)
                    && let Ok(Some(normalized)) = normalize_float_argument(array, memory) {
                    let physical = normalized_field(field, &normalized);
                    if let Some((output, scratch)) = aligned_argument(&normalized, &physical, target, memory) {
                        return Ok((output, scratch));
                    }
                }
            }
            if !unambiguous_field(field) && float_field(field) && canonical_float_field(target)
                && has_valid_union_nulls(field, array.as_ref())
                && (0..array.len()).all(|row| compatible_float_cell(ArrowCell { field, array: array.as_ref(), row }, Some(source))) {
                let cells = (0..array.len()).map(|row| Some(ArrowCell { field, array: array.as_ref(), row }));
                if let Ok(output) = crate::source::copy_float_cells(target, cells, memory) {
                    return Ok((output.0, Some(output.1)));
                }
            }
            let (output, scratch) = copy_evaluated_cells(ty, batch.num_rows(), memory,
                |row| estimate_read_cell_bytes(field, array.as_ref(), row),
                |row| {
                    let mut cell = read_value_as(source, field, array.as_ref(), row)?;
                    cell.admit_object_dates()?;
                    Ok(cell)
                })?;
            Ok((output, Some(scratch)))
        }
        ArgumentChunk::Rows { rows, .. } => {
            let (output, scratch) = copy_evaluated_cells(ty, rows.len(), memory,
                |row| rows[row][position].retained_bytes(), |row| Ok(rows[row][position].clone()))?;
            Ok((output, Some(scratch)))
        }
    }
}

fn normalize_float_argument(array: &ArrayRef, memory: &Arc<MemoryBudget>) -> Result<Option<ArrayRef>> {
    let DataType::List(field) = array.data_type() else {
        return normalized_document_array(array, memory);
    };
    let source = array.as_list::<i32>();
    let Some(values) = normalize_float_argument(source.values(), memory)? else { return Ok(None); };
    let field = normalized_field(field, &values);
    // A union supplies logical nulls through its legs, even when the list item has no validity bitmap.
    let layout = DataType::List(Arc::new(field));
    let data = source.to_data().into_builder().data_type(layout).child_data(vec![values.to_data()]).build();
    Ok(data.ok().map(arrow_array::make_array))
}

fn aligned_argument(array: &ArrayRef, source: &Field, target: &Field, memory: &Arc<MemoryBudget>) -> Option<ArgumentVector> {
    if source.data_type() == target.data_type() && source.metadata() == target.metadata() {
        return Some((Arc::clone(array), None));
    }
    let scratch = memory.reserve(arrow_relayout_bound(array.as_ref(), source, target).ok()?).ok()?;
    let (field, converted) = relayout_arrow_column(Arc::clone(array), source, target).ok()?;
    (field.data_type() == target.data_type()).then_some((converted, Some(scratch)))
}

pub(crate) fn canonical_float_field(field: &Field) -> bool {
    float_field(field) && unambiguous_field(field)
}

pub(crate) fn float_field(field: &Field) -> bool {
    field.metadata().is_empty() && match field.data_type() {
        DataType::List(element) | DataType::FixedSizeList(element, _) => float_field(element),
        DataType::Union(fields, UnionMode::Dense) => fields.iter().all(|(_, field)| float_field(field)),
        DataType::Float32 | DataType::Float64 => true,
        DataType::Null => field.is_nullable(),
        _ => false,
    }
}

fn unambiguous_field(field: &Field) -> bool {
    match field.data_type() {
        DataType::Union(fields, UnionMode::Dense) =>
            unambiguous_union(field, fields) && fields.iter().all(|(_, child)| unambiguous_field(child)),
        DataType::List(child) | DataType::FixedSizeList(child, _) | DataType::Map(child, _)
        | DataType::RunEndEncoded(_, child) => unambiguous_field(child),
        DataType::Struct(fields) => fields.iter().all(|child| unambiguous_field(child)),
        _ => true,
    }
}

pub(crate) fn compatible_float_cell(cell: ArrowCell<'_>, declared: Option<&LogicalType>) -> bool {
    fn compatible(cell: ArrowCell<'_>, logical: &LogicalType) -> Option<()> {
        let ArrowCell { field, array, row } = cell;
        if let DataType::Union(fields, UnionMode::Dense) = field.data_type() {
            let union = array.as_union();
            let tag = union.type_id(row);
            let (_, child) = fields.iter().find(|(id, _)| *id == tag)?;
            let physical = rtdb_columnar::from_field(child).ok()?;
            let named = match logical {
                LogicalType::Union(legs) => legs.iter().find(|leg| rtdb_columnar::leg_name(leg).is_ok_and(|name| name == *child.name())),
                _ => None,
            };
            let logical = named.unwrap_or(&physical);
            if *logical != physical && logical.clone().maybe() != physical { return None; }
            return compatible(ArrowCell { field: child, array: union.child(tag).as_ref(), row: union.value_offset(row) }, logical);
        }
        let physical = rtdb_columnar::from_field(field).ok()?;
        if *logical != physical && logical.clone().maybe() != physical { return None; }
        if array.is_null(row) { return Some(()); }
        let logical = match logical { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
        if let LogicalType::List { element, .. } = logical {
            let (field, array, range) = cell.sequence_range()?;
            for row in range { compatible(ArrowCell { field, array, row }, element)?; }
        }
        Some(())
    }

    if !float_field(cell.field) { return false; }
    if declared.is_none() && unambiguous_field(cell.field) { return true; }
    let Ok(physical) = rtdb_columnar::from_field(cell.field) else { return false; };
    if declared.is_none_or(|declared| *declared == physical) && unambiguous_field(cell.field) { return true; }
    compatible(cell, declared.unwrap_or(&physical)).is_some()
}

fn canonical_argument_field(field: &Field) -> bool {
    if !super::compatible_document_metadata(field) { return false; }
    match field.data_type() {
        DataType::List(item) | DataType::FixedSizeList(item, _) => canonical_float_field(item),
        DataType::Struct(fields) => fields.iter().all(|field| canonical_argument_field(field)),
        DataType::Union(fields, UnionMode::Dense) => fields.iter().all(|(_, field)| canonical_argument_field(field)) && unambiguous_union(field, fields),
        DataType::Utf8 => true,
        _ => plain_scalar_field(field),
    }
}

fn unambiguous_union(field: &Field, children: &arrow_schema::UnionFields) -> bool {
    let Ok(logical) = rtdb_columnar::from_field(field) else { return false; };
    let LogicalType::Union(legs) = logical else { return true; };
    children.iter().all(|(_, child)| {
        let named = legs.iter().find(|leg| rtdb_columnar::leg_name(leg).is_ok_and(|name| name == *child.name()));
        named.is_none_or(|ty| rtdb_columnar::from_field(child).is_ok_and(|actual| actual == *ty || actual == ty.clone().maybe()))
    })
}

fn validate_argument_uris(field: &Field, array: &dyn Array) -> Result<()> {
    fn has_uri(field: &Field) -> bool {
        match field.data_type() {
            DataType::Struct(fields) => fields.iter().any(|field| has_uri(field)),
            DataType::Union(fields, UnionMode::Dense) => fields.iter().any(|(_, field)| has_uri(field)),
            DataType::List(field) | DataType::FixedSizeList(field, _) | DataType::Map(field, _)
            | DataType::RunEndEncoded(_, field) => has_uri(field),
            DataType::Utf8 => matches!(field.extension_type_name(), Some("uri")),
            _ => false,
        }
    }
    fn validate(cell: ArrowCell<'_>) -> Result<()> {
        if !has_uri(cell.field) { return Ok(()); }
        let cell = cell.physical()?;
        if cell.is_null()? { return Ok(()); }
        if let Some(fields) = cell.structure() {
            for child in fields { validate(child)?; }
        } else if let Some(items) = cell.sequence() {
            for child in items { validate(child)?; }
        } else if cell.field.data_type() == &DataType::Utf8 && has_uri(cell.field) {
            rtdb_types::UriParts::parse(cell.array.as_string::<i32>().value(cell.row))?;
        }
        Ok(())
    }
    if has_uri(field) {
        for row in 0..array.len() { validate(ArrowCell { field, array, row })?; }
    }
    Ok(())
}

fn has_valid_union_nulls(field: &Field, array: &dyn Array) -> bool {
    !contains_union(field.data_type()) || (0..array.len()).all(|row| valid_argument_union_cell(field, array, row))
}

fn contains_union(layout: &DataType) -> bool {
    match layout {
        DataType::Union(_, UnionMode::Dense) => true,
        DataType::Struct(fields) => fields.iter().any(|field| contains_union(field.data_type())),
        DataType::List(field) | DataType::FixedSizeList(field, _) => contains_union(field.data_type()),
        _ => false,
    }
}

pub(crate) fn valid_argument_union_cell(field: &Field, array: &dyn Array, row: usize) -> bool {
    match field.data_type() {
        DataType::Union(fields, UnionMode::Dense) => {
            let union = array.as_union();
            let tag = union.type_id(row);
            let (_, field) = fields.iter().find(|(id, _)| *id == tag).expect("Arrow union tags identify a leg");
            if field.data_type() == &DataType::Null { return field.is_nullable(); }
            let child = union.child(tag);
            let offset = union.value_offset(row);
            child.is_valid(offset) && valid_argument_union_cell(field, child.as_ref(), offset)
        }
        _ if array.is_null(row) => true,
        DataType::Struct(fields) => fields.iter().zip(array.as_struct().columns())
            .all(|(field, child)| valid_argument_union_cell(field, child.as_ref(), row)),
        DataType::List(field) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            (offsets[row]..offsets[row + 1]).all(|index| valid_argument_union_cell(field, list.values().as_ref(), index as usize))
        }
        DataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            let start = row * *size as usize;
            (start..start + *size as usize).all(|index| valid_argument_union_cell(field, list.values().as_ref(), index))
        }
        _ => true,
    }
}

fn can_copy_argument(ty: &LogicalType) -> bool {
    match ty {
        LogicalType::Nothing => false,
        LogicalType::RunEndEncoded { value, .. } => can_copy_argument(value),
        LogicalType::Maybe(value) => can_copy_argument(value),
        LogicalType::Struct(fields) => fields.values().all(|ty| *ty == LogicalType::Nothing || can_copy_argument(ty)),
        LogicalType::List { element, .. } => can_copy_argument(element),
        LogicalType::Union(legs) => legs.iter().all(can_copy_argument),
        LogicalType::Null | LogicalType::Scalar(_) => true,
    }
}

#[test]
fn decoding_one_argument_chunk_preserves_compatible_arrow_inputs() {
    use rtdb_types::{ScalarType, Value};
    let memory = MemoryBudget::new(1 << 20);
    let binary = LogicalType::Scalar(ScalarType::Binary);
    let uri = LogicalType::Scalar(ScalarType::Uri);
    let first = Value::Binary(vec![7; 4096]);
    let second = Value::Uri("https://example.com".into());
    let page = |label, ty: &LogicalType, value: &Value| {
        let array = rtdb_columnar::build_array(ty, std::slice::from_ref(value)).unwrap();
        let schema = Arc::new(Schema::new(vec![to_field(label, ty).unwrap()]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        SqlArguments::from_arrow(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap()
    };
    let mut args = page("binary", &binary, &first);
    args.append(page("uri", &uri, &second)).unwrap();
    let ty = &args.types()[0];
    let field = to_field("?_0", ty).unwrap();
    let ArgumentChunk::Arrow { batch, .. } = args.chunks[0].1.as_ref() else { panic!("Expected Arrow argument"); };
    let original = batch.batch().column(0).to_data();

    let (kept, _scratch) = argument_vector(&args.chunks[0].1, 0, &field, ty, &memory).unwrap();
    let kept = kept.as_union().child(0).to_data();
    assert_eq!(kept.buffers()[1].as_ptr(), original.buffers()[1].as_ptr());
    let aligned = args.source_columnar_batch(&memory).unwrap();
    let replay = SqlArguments::from_arrow(aligned, &memory).unwrap();
    let values = replay.rows().map(|row| row.unwrap()[0].clone()).collect::<Vec<_>>();
    assert_eq!(values, [first, second]);
}

#[test]
fn canonical_dense_float_vectors_keep_their_buffers_and_encode_as_their_rows_would() {
    use rtdb_types::{ListValue, Value};
    let memory = MemoryBudget::new(1 << 20);
    let vector = |values: Vec<f32>| Value::FixedSizeList(ListValue::from_f32(arrow_array::Float32Array::from(values)));
    let rows = vec![vec![vector(vec![1.0, -0.0, f32::MAX])], vec![vector(vec![0.5, f32::MIN_POSITIVE, 3.0])]];
    let ty = rows[0][0].logical_type();
    let array = rtdb_columnar::build_array(&ty, &rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>()).unwrap();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![to_field("?_0", &ty).unwrap()])), vec![array]).unwrap();
    let canonical = SqlArguments::from_arrow_with_canonical_source(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
    let ArgumentChunk::Arrow { batch, .. } = canonical.chunks[0].1.as_ref() else { panic!("Expected Arrow argument"); };
    let original = batch.batch().column(0).as_fixed_size_list().values().to_data();
    let field = to_field("?_0", &canonical.types()[0]).unwrap();

    let (kept, scratch) = argument_vector(&canonical.chunks[0].1, 0, &field, &canonical.types()[0], &memory).unwrap();
    assert!(scratch.is_none());
    assert_eq!(kept.as_fixed_size_list().values().to_data().buffers()[0].as_ptr(), original.buffers()[0].as_ptr());

    let bits = |batch: &OwnedBatch| {
        let column = batch.batch().column(0);
        let values = column.as_fixed_size_list().values().as_primitive::<arrow_array::types::Float32Type>();
        (batch.batch().schema(), column.null_count(), values.values().iter().map(|value| value.to_bits()).collect::<Vec<_>>())
    };
    let from_rows = SqlArguments::from_rows(rows, &memory).unwrap().source_columnar_batch(&memory).unwrap();
    assert_eq!(bits(&canonical.source_columnar_batch(&memory).unwrap()), bits(&from_rows));
}

#[test]
fn canonical_argument_ipc_matches_rows_with_nullable_vectors_and_extension_fields() {
    use rtdb_types::Value;
    let memory = MemoryBudget::new(1 << 24);
    let null = Value::Null;
    let f32_list = Value::FixedSizeList(vec![Value::F32(f32::from_bits(0x7fc00003)), null.clone(), Value::F32(-0.0)].into());
    let f64_list = Value::list(vec![Value::F64(f64::from_bits(0x7ff8000000000003)), null.clone(), Value::F64(-0.0)]);
    let keyword = Value::Keyword(rtdb_types::Keyword::from_name("ns/name"));
    let uri = Value::Uri("https://example.com/é".into());
    let rows = vec![
        vec![null.clone(), f64_list.clone(), uri.clone(), keyword.clone()],
        vec![f32_list.clone(), null.clone(), null.clone(), keyword.clone()],
        vec![f32_list, f64_list, uri, null],
    ];
    let from_rows = SqlArguments::from_rows(rows.clone(), &memory).unwrap();
    let page = |from: usize, to: usize| {
        let mut fields = Vec::new();
        let mut arrays = Vec::new();
        for (column, ty) in from_rows.types().iter().enumerate() {
            fields.push(to_field(&format!("?_{column}"), ty).unwrap());
            let cells = rows[from..to].iter().map(|row| row[column].clone()).collect::<Vec<_>>();
            arrays.push(rtdb_columnar::copy_array(ty, &cells).unwrap());
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
        SqlArguments::from_arrow_with_canonical_source(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap()
    };
    let mut arguments = page(0, 2);
    for (position, ty) in arguments.types().iter().enumerate() {
        let ArgumentChunk::Arrow { batch, .. } = arguments.chunks[0].1.as_ref() else { panic!("Expected Arrow arguments"); };
        let field = to_field(&format!("?_{position}"), ty).unwrap();
        let (retained, scratch) = argument_vector(&arguments.chunks[0].1, position, &field, ty, &memory).unwrap();
        assert!(scratch.is_none());
        assert!(Arc::ptr_eq(&retained, batch.batch().column(position)));
    }
    arguments.append(SqlArguments::from_rows(rows[2..].to_vec(), &memory).unwrap()).unwrap();
    let encoded = crate::source::encode_arguments(&arguments, &memory).unwrap();
    let expected = crate::source::encode_arguments(&from_rows, &memory).unwrap();
    assert_eq!(encoded.as_slice(), expected.as_slice());
}

#[test]
fn argument_ipc_gathers_float_union_positions_with_an_unused_conflicting_alias() {
    use arrow_array::{Float32Array, Float64Array, UnionArray};
    use arrow_buffer::ScalarBuffer;
    use arrow_schema::UnionFields;
    use rtdb_types::Value;

    let memory = MemoryBudget::new(1 << 24);
    let fields = UnionFields::try_new([5, 3, 8], [
        Field::new("f64", DataType::Float32, false),
        Field::new("alternate", DataType::Float32, false),
        Field::new("double", DataType::Float64, false),
    ]).unwrap();
    let array = UnionArray::try_new(fields, ScalarBuffer::from(vec![3, 8, 3]), Some(ScalarBuffer::from(vec![0, 0, 1])), vec![
        Arc::new(Float32Array::from(vec![123.0])),
        Arc::new(Float32Array::from(vec![-0.0, f32::from_bits(0x7fc00003)])),
        Arc::new(Float64Array::from(vec![2.0])),
    ]).unwrap();
    let field = Field::new("?_0", array.data_type().clone(), false);
    assert!(!canonical_float_field(&field));
    assert!((0..array.len()).all(|row| compatible_float_cell(ArrowCell { field: &field, array: &array, row }, None)));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(array)]).unwrap();
    let arrow = SqlArguments::from_arrow_with_canonical_source(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
    let rows = SqlArguments::from_rows(vec![vec![Value::F32(-0.0)], vec![Value::F64(2.0)],
        vec![Value::F32(f32::from_bits(0x7fc00003))]], &memory).unwrap();
    assert_eq!(crate::source::encode_arguments(&arrow, &memory).unwrap().as_slice(),
        crate::source::encode_arguments(&rows, &memory).unwrap().as_slice());
}

#[test]
fn canonical_nullable_float_vectors_keep_their_buffers_and_ipc_bytes() {
    use rtdb_types::{ListValue, Value};

    let memory = MemoryBudget::new(1 << 20);
    let vector = Value::FixedSizeList(ListValue::from_f32(arrow_array::Float32Array::from(vec![
        Some(f32::from_bits(0x7fc0_4321)), None, Some(-0.0),
    ])));
    let ty = vector.logical_type().maybe();
    let rows = vec![vec![Value::Null], vec![vector]];
    let values = rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>();
    let array = rtdb_columnar::build_array(&ty, &values).unwrap();
    let field = to_field("?_0", &ty).unwrap();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field.clone()])), vec![Arc::clone(&array)]).unwrap();
    let canonical = SqlArguments::from_arrow_with_canonical_source(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
    let (kept, scratch) = argument_vector(&canonical.chunks[0].1, 0, &field, &ty, &memory).unwrap();
    assert!(scratch.is_none());
    assert_eq!(kept.as_fixed_size_list().values().to_data().buffers()[0].as_ptr(),
        array.as_fixed_size_list().values().to_data().buffers()[0].as_ptr());
    let from_rows = SqlArguments::from_rows(rows, &memory).unwrap().source_columnar_batch(&memory).unwrap();
    let from_arrow = canonical.source_columnar_batch(&memory).unwrap();
    let encode = |batch: &OwnedBatch| crate::ipc::encode(batch.batch().schema_ref(), [batch.batch()], &memory).unwrap();
    assert_eq!(encode(&from_arrow).as_slice(), encode(&from_rows).as_slice());
}

#[test]
fn canonical_mixed_float_vectors_stay_columnar_in_source_arguments() {
    use rtdb_types::{ListValue, Value};

    let memory = MemoryBudget::new(1 << 20);
    let values = vec![
        Value::list(vec![Value::F32(f32::from_bits(0x7fc0_1234)), Value::Null, Value::F64(-0.0)]),
        Value::FixedSizeList(ListValue::from_f64(arrow_array::Float64Array::from(vec![Some(-0.0), None]))),
        Value::Null,
    ];
    let types = values.iter().map(Value::logical_type).collect::<Vec<_>>();
    let ty = LogicalType::join_all(&types);
    let field = to_field("?_0", &ty).unwrap();
    let array = rtdb_columnar::build_array(&ty, &values).unwrap();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field.clone()])), vec![Arc::clone(&array)]).unwrap();
    let canonical = SqlArguments::from_arrow_with_canonical_source(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
    let (kept, scratch) = argument_vector(&canonical.chunks[0].1, 0, &field, &ty, &memory).unwrap();
    assert!(scratch.is_none());
    assert_eq!(kept.as_union().type_ids().as_ptr(), array.as_union().type_ids().as_ptr());
    let rows = SqlArguments::from_rows(values.into_iter().map(|value| vec![value]).collect(), &memory).unwrap();
    assert_eq!(crate::source::encode_arguments(&canonical, &memory).unwrap().as_slice(),
        crate::source::encode_arguments(&rows, &memory).unwrap().as_slice());
}

#[test]
fn canonical_argument_ipc_remaps_a_null_in_a_dense_float_union() {
    use arrow_array::{Float32Array, Float64Array, UnionArray};
    use arrow_buffer::ScalarBuffer;
    use arrow_schema::UnionFields;
    use rtdb_types::{ListKind, ScalarType, Value};
    let memory = MemoryBudget::new(1 << 24);
    let f32 = LogicalType::Scalar(ScalarType::F32);
    let f64 = LogicalType::Scalar(ScalarType::F64);
    let fields = UnionFields::try_new([5, 3, 8], [
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, false),
        Field::new("null", DataType::Null, true),
    ]).unwrap();
    let children = UnionArray::try_new(fields, ScalarBuffer::from(vec![5, 5, 3]), Some(ScalarBuffer::from(vec![0, 1, 0])), vec![
        Arc::new(Float32Array::from(vec![Some(-0.0), None])), Arc::new(Float64Array::from(vec![1.0])),
        Arc::new(arrow_array::NullArray::new(0)),
    ]).unwrap();
    let layout = DataType::List(Arc::new(Field::new("item", children.data_type().clone(), false)));
    let array = arrow_array::make_array(arrow_array::new_empty_array(&layout).to_data().into_builder()
        .len(1).buffers(vec![ScalarBuffer::from(vec![0i32, 3]).into_inner()]).child_data(vec![children.to_data()]).build().unwrap());
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("vector", layout, false)])), vec![array]).unwrap();
    let arguments = SqlArguments::from_arrow_with_canonical_source(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
    let ty = LogicalType::List { kind: ListKind::List, element: Box::new(f32.join(&LogicalType::Null).join(&f64)) };
    let expected = SqlArguments::new(vec![ty.clone()], vec![vec![Value::list(vec![Value::F32(-0.0), Value::Null, Value::F64(1.0)])]], &memory).unwrap();
    let (kept, _) = argument_vector(&arguments.chunks[0].1, 0, &to_field("?_0", &ty).unwrap(), &ty, &memory).unwrap();
    let kept = kept.as_list::<i32>().values().as_union();
    assert_eq!(kept.logical_null_count(), 1);
    let ArgumentChunk::Arrow { batch, .. } = arguments.chunks[0].1.as_ref() else { panic!("Expected Arrow arguments"); };
    let original = batch.batch().column(0).as_list::<i32>().values().as_union();
    assert_eq!(kept.child(kept.type_id(2)).to_data().buffers()[0].as_ptr(), original.child(3).to_data().buffers()[0].as_ptr());
    let encoded = crate::source::encode_arguments(&arguments, &memory).unwrap();
    let expected = crate::source::encode_arguments(&expected, &memory).unwrap();
    assert_eq!(encoded.as_slice(), expected.as_slice());
}

#[test]
fn canonical_argument_ipc_widens_float_vector_children_when_pages_join() {
    use rtdb_types::Value;
    let memory = MemoryBudget::new(1 << 24);
    let left = Value::list(vec![Value::F32(-0.0), Value::F32(1.0)]);
    let right = Value::list(vec![Value::F64(f64::from_bits(0x7ff8000000000009)), Value::Null]);
    let page = |value: &Value| {
        let ty = value.logical_type();
        let column = rtdb_columnar::copy_array(&ty, std::slice::from_ref(value)).unwrap();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![to_field("vector", &ty).unwrap()])), vec![column]).unwrap();
        SqlArguments::from_arrow_with_canonical_source(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap()
    };
    let mut arguments = page(&left);
    arguments.append(page(&right)).unwrap();
    let ty = &arguments.types()[0];
    let (kept, _) = argument_vector(&arguments.chunks[0].1, 0, &to_field("?_0", ty).unwrap(), ty, &memory).unwrap();
    let child = kept.as_list::<i32>().values().as_union();
    let ArgumentChunk::Arrow { batch, .. } = arguments.chunks[0].1.as_ref() else { panic!("Expected Arrow arguments"); };
    let original = batch.batch().column(0).as_list::<i32>().values();
    assert_eq!(child.child(child.type_id(0)).to_data().buffers()[0].as_ptr(), original.to_data().buffers()[0].as_ptr());

    let from_rows = SqlArguments::from_rows(vec![vec![left], vec![right]], &memory).unwrap();
    let expected = crate::source::encode_arguments(&from_rows, &memory).unwrap();
    let actual = crate::source::encode_arguments(&arguments, &memory).unwrap();
    assert_eq!(actual.as_slice(), expected.as_slice());
}

#[test]
fn canonical_source_arguments_keep_nested_buffers_and_match_row_padding() {
    use arrow_array::Int64Array;
    use arrow_buffer::NullBuffer;
    use rtdb_types::{ListKind, StructFields};

    let memory = MemoryBudget::new(1 << 24);
    let item = LogicalType::Scalar(ScalarType::I64);
    let list = LogicalType::List { kind: ListKind::FixedSizeList(2), element: Box::new(item) };
    let ty = LogicalType::Struct(StructFields::from([("items".into(), list)])).maybe();
    let field = to_field("?_0", &ty).unwrap();
    let DataType::Struct(fields) = field.data_type() else { unreachable!() };
    let DataType::FixedSizeList(child, width) = fields[0].data_type() else { unreachable!() };

    for rows in [3, 130] {
        let values = Arc::new(Int64Array::from_iter_values(1..=(2 * (rows + 1)) as i64));
        let items = arrow_array::FixedSizeListArray::new(Arc::clone(child), *width, values, None);
        let validity = NullBuffer::from((0..rows + 1).map(|row| row % 3 != 2).collect::<Vec<_>>());
        let structure = rtdb_columnar::build_struct_array(fields.clone(), &[Arc::new(items)], rows + 1, Some(validity)).unwrap();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field.clone()])), vec![structure]).unwrap().slice(1, rows);
        let arguments = SqlArguments::from_arrow_with_canonical_source(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
        let chunk = &arguments.chunks[0].1;
        let ArgumentChunk::Arrow { batch, .. } = chunk.as_ref() else { unreachable!() };
        let (kept, _) = argument_vector(chunk, 0, &field, &ty, &memory).unwrap();
        assert!(Arc::ptr_eq(&kept, batch.batch().column(0)));

        let rows = arguments.rows().map(|row| row.unwrap().to_vec()).collect();
        let reference = SqlArguments::new(vec![ty.clone()], rows, &memory).unwrap();
        let actual = crate::source::encode_arguments(&arguments, &memory).unwrap();
        let expected = crate::source::encode_arguments(&reference, &memory).unwrap();
        assert_eq!(actual.as_slice(), expected.as_slice());
    }
}

#[test]
fn canonical_source_arguments_align_nested_list_elements_and_missing_fields() {
    use rtdb_types::{StructFields, Value};

    let memory = MemoryBudget::new(1 << 24);
    let page = |items, name: &str| {
        let value = Value::Struct(StructFields::from([("items".into(), Value::list(items)), (name.into(), Value::Bool(true))]));
        let ty = value.logical_type();
        let array = rtdb_columnar::build_array(&ty, std::slice::from_ref(&value)).unwrap();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![to_field("binding", &ty).unwrap()])), vec![array]).unwrap();
        SqlArguments::from_arrow_with_canonical_source(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap()
    };
    let mut arguments = page(vec![Value::I64(i64::MIN)], "left");
    arguments.append(page(vec![Value::Utf8("second".into())], "right")).unwrap();
    let ty = &arguments.types()[0];
    let field = to_field("?_0", ty).unwrap();
    let first = &arguments.chunks[0].1;
    let ArgumentChunk::Arrow { batch, .. } = first.as_ref() else { unreachable!() };
    let values = batch.batch().column(0).as_struct().column(0).as_list::<i32>().values();
    let (aligned, _scratch) = argument_vector(first, 0, &field, ty, &memory).unwrap();
    let aligned = aligned.as_struct().column(0).as_list::<i32>().values().as_union().child(0);
    assert_eq!(values.to_data().buffers()[0].as_ptr(), aligned.to_data().buffers()[0].as_ptr());

    let rows = arguments.rows().map(|row| row.unwrap().to_vec()).collect();
    let reference = SqlArguments::new(vec![ty.clone()], rows, &memory).unwrap();
    let actual = crate::source::encode_arguments(&arguments, &memory).unwrap();
    let expected = crate::source::encode_arguments(&reference, &memory).unwrap();
    assert_eq!(actual.as_slice(), expected.as_slice());
}

#[test]
fn canonical_fixed_float_union_arguments_preserve_padding_under_null_vectors() {
    use rtdb_types::Value;

    let memory = MemoryBudget::new(1 << 20);
    let vector = Value::FixedSizeList(vec![Value::F32(-0.0), Value::Null, Value::F64(2.0)].into());
    let ty = vector.logical_type().maybe();
    let field = to_field("?_0", &ty).unwrap();
    let array = rtdb_columnar::build_array(&ty, &[Value::Null, vector]).unwrap();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field.clone()])), vec![Arc::clone(&array)]).unwrap();
    let canonical = SqlArguments::from_arrow_with_canonical_source(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
    let (kept, scratch) = argument_vector(&canonical.chunks[0].1, 0, &field, &ty, &memory).unwrap();
    assert!(scratch.is_none());
    assert_eq!(kept.as_fixed_size_list().values().as_union().type_ids().as_ptr(),
        array.as_fixed_size_list().values().as_union().type_ids().as_ptr());
    let (rebuilt, _scratch) = copy_evaluated_cells(&ty, 2, &memory,
        |row| estimate_read_cell_bytes(&field, array.as_ref(), row),
        |row| read_value_as(&ty, &field, array.as_ref(), row)).unwrap();
    let rebuilt = copy_selected_cells(&[rebuilt.as_ref()], &[(0, 0), (0, 1)]).unwrap();
    let old = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![rebuilt]).unwrap();
    let old = crate::ipc::encode(old.schema_ref(), [&old], &memory).unwrap();
    assert_eq!(crate::source::encode_arguments(&canonical, &memory).unwrap().as_slice(), old.as_slice());
}

#[test]
fn canonical_source_arguments_preserve_the_null_map_key_error() {
    use arrow_array::{Int64Array, ListArray, MapArray, NullArray, StringArray, UnionArray};
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::UnionFields;
    use rtdb_types::Value;

    let memory = MemoryBudget::new(1 << 20);
    let legs = UnionFields::try_new([0, 1, 2], [
        Field::new("i64", DataType::Int64, false),
        Field::new("utf8", DataType::Utf8, false),
        Field::new("null", DataType::Null, true),
    ]).unwrap();
    let keys = UnionArray::try_new(legs, vec![2i8].into(), Some(vec![0i32].into()), vec![
        Arc::new(Int64Array::from(Vec::<i64>::new())),
        Arc::new(StringArray::from(Vec::<&str>::new())),
        Arc::new(NullArray::new(1)),
    ]).unwrap();
    let fields = vec![Field::new("key", keys.data_type().clone(), false), Field::new("value", DataType::Int64, false)].into();
    let entries = rtdb_columnar::build_struct_array(fields, &[Arc::new(keys), Arc::new(Int64Array::from(vec![42]))], 1, None).unwrap();
    let map = MapArray::new(Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(vec![0, 1].into()), entries.as_struct().clone(), None, false);
    let list = ListArray::new(Arc::new(Field::new("$data$", map.data_type().clone(), false)),
        OffsetBuffer::new(vec![0, 1].into()), Arc::new(map), None);
    let field = Field::new("binding", list.data_type().clone(), false);
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(list)]).unwrap();
    let arguments = SqlArguments::from_arrow_with_canonical_source(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
    let rows = vec![vec![Value::list(vec![Value::Map { sorted: false, entries: vec![(Value::Null, Value::I64(42))] }])]];
    let reference = SqlArguments::new(arguments.types().to_vec(), rows, &memory).unwrap();
    let expected = crate::source::encode_arguments(&reference, &memory).unwrap_err();
    assert_eq!(expected.code, "rtdb/null-map-key");
    assert_eq!(crate::source::encode_arguments(&arguments, &memory).unwrap_err().code, expected.code);
}
