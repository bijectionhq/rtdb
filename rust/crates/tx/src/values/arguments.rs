use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, cast::AsArray};
use arrow_schema::{DataType, Field, Schema, UnionMode};
use rtdb_columnar::{BatchBuilder, MemoryBudget, MemoryReservation, OwnedBatch, arrow_relayout_bound, copy_evaluated_cells, copy_selected_cells, estimate_copy_cell_bytes, estimate_read_cell_bytes, read_value_as, relayout_arrow_column, to_field};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType};

use super::{ArgumentChunk, SourceArgumentLayout, SqlArguments, overflow, plain_scalar_field};

impl SqlArguments {
    pub(crate) fn source_columnar_batch(&self, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
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
                && matches!(chunk.as_ref(), ArgumentChunk::Rows { .. })
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
            if (*source_layout == SourceArgumentLayout::Uploaded || plain_scalar_field(field) || dense_float_vectors(field, array.as_ref()))
                && can_copy_argument(source) && source.join(ty) == *ty && has_valid_union_nulls(field, array.as_ref()) {
                if field.data_type() == target.data_type() { return Ok((Arc::clone(array), None)); }
                if let Ok(bound) = arrow_relayout_bound(array.as_ref(), field, target)
                    && let Ok(scratch) = memory.reserve(bound)
                    && let Ok((field, converted)) = relayout_arrow_column(Arc::clone(array), field, target)
                    && field.data_type() == target.data_type() {
                    return Ok((converted, Some(scratch)));
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

/// A float vector column with no absent vector or component, which the canonical cell rebuild would reproduce exactly.
fn dense_float_vectors(field: &Field, array: &dyn Array) -> bool {
    let (element, values) = match field.data_type() {
        DataType::List(element) => (element, array.as_list::<i32>().values()),
        DataType::FixedSizeList(element, _) => (element, array.as_fixed_size_list().values()),
        _ => return false,
    };
    field.metadata().is_empty() && element.metadata().is_empty()
        && matches!(element.data_type(), DataType::Float32 | DataType::Float64)
        && array.null_count() == 0 && values.null_count() == 0
}

fn has_valid_union_nulls(field: &Field, array: &dyn Array) -> bool {
    !contains_union(field.data_type()) || (0..array.len()).all(|row| has_valid_union_cell(field, array, row))
}

fn contains_union(layout: &DataType) -> bool {
    match layout {
        DataType::Union(_, UnionMode::Dense) => true,
        DataType::Struct(fields) => fields.iter().any(|field| contains_union(field.data_type())),
        DataType::List(field) | DataType::FixedSizeList(field, _) => contains_union(field.data_type()),
        _ => false,
    }
}

fn has_valid_union_cell(field: &Field, array: &dyn Array, row: usize) -> bool {
    match field.data_type() {
        DataType::Union(fields, UnionMode::Dense) => {
            let union = array.as_union();
            let tag = union.type_id(row);
            let (_, field) = fields.iter().find(|(id, _)| *id == tag).expect("Arrow union tags identify a leg");
            if field.data_type() == &DataType::Null { return field.is_nullable(); }
            let child = union.child(tag);
            let offset = union.value_offset(row);
            child.is_valid(offset) && has_valid_union_cell(field, child.as_ref(), offset)
        }
        _ if array.is_null(row) => true,
        DataType::Struct(fields) => fields.iter().zip(array.as_struct().columns())
            .all(|(field, child)| has_valid_union_cell(field, child.as_ref(), row)),
        DataType::List(field) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            (offsets[row]..offsets[row + 1]).all(|index| has_valid_union_cell(field, list.values().as_ref(), index as usize))
        }
        DataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            let start = row * *size as usize;
            (start..start + *size as usize).all(|index| has_valid_union_cell(field, list.values().as_ref(), index))
        }
        _ => true,
    }
}

fn can_copy_argument(ty: &LogicalType) -> bool {
    match ty {
        LogicalType::Nothing | LogicalType::Scalar(ScalarType::Keyword | ScalarType::Uri) => false,
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
    use rtdb_types::Value;
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
