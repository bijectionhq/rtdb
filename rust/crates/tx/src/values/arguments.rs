use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, cast::AsArray};
use arrow_schema::{DataType, Field, Schema, UnionMode};
use rtdb_columnar::{BatchBuilder, MemoryBudget, MemoryReservation, OwnedBatch, arrow_relayout_bound, copy_array, copy_selected_cells, estimate_builder_bytes_refs, estimate_copy_cell_bytes, estimate_read_cell_bytes, read_value_as, relayout_arrow_column, to_field};
use rtdb_types::{Anomaly, ListKind, LogicalType, Result, ScalarType, Value};

use super::{ArgumentChunk, SqlArguments, overflow};

impl SqlArguments {
    pub(crate) fn source_columnar_batch(&self, memory: &Arc<MemoryBudget>) -> Result<Option<OwnedBatch>> {
        if !self.chunks.iter().any(|(_, chunk)| matches!(chunk.as_ref(), ArgumentChunk::Arrow { .. })) {
            return Ok(None);
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
            let column = output.column(bound, || {
                let mut picks = Vec::with_capacity(self.row_count());
                for (page, array) in inputs.iter().enumerate() {
                    picks.extend((0..array.len()).map(|row| (page, row)));
                }
                copy_selected_cells(&inputs, &picks)
            })?;
            fields.push(field);
            columns.push(column);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(self.row_count()));
        let batch = RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), columns, &options)
            .map_err(|error| Anomaly::incorrect("rtdb.tx/invalid-sql-arguments", error.to_string()))?;
        output.finish(batch).map(Some)
    }
}

type ArgumentVector = (ArrayRef, Option<Arc<MemoryReservation>>);

fn argument_vector(chunk: &ArgumentChunk, position: usize, target: &Field, ty: &LogicalType, memory: &Arc<MemoryBudget>)
    -> Result<ArgumentVector> {
    match chunk {
        ArgumentChunk::Arrow { batch, types, .. } => {
            let batch = batch.batch();
            let field = batch.schema_ref().field(position);
            let array = batch.column(position);
            let source = &types[position];
            if can_copy_argument(source) && source.join(ty) == *ty && has_valid_union_nulls(field, array.as_ref()) {
                if field.data_type() == target.data_type() { return Ok((Arc::clone(array), None)); }
                if let Ok(bound) = arrow_relayout_bound(array.as_ref(), field, target)
                    && let Ok(scratch) = memory.reserve(bound)
                    && let Ok((field, converted)) = relayout_arrow_column(Arc::clone(array), field, target)
                    && field.data_type() == target.data_type() {
                    return Ok((converted, Some(scratch)));
                }
            }
            let base = batch.num_rows().checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?;
            let bound = (0..batch.num_rows()).try_fold(base, |bytes, row| {
                bytes.checked_add(estimate_read_cell_bytes(field, array.as_ref(), row)?).ok_or_else(overflow)
            })?;
            let _decoded = memory.reserve(bound)?;
            let cells = (0..batch.num_rows()).map(|row| {
                let mut cell = read_value_as(source, field, array.as_ref(), row)?;
                cell.admit_object_dates();
                Ok(cell)
            }).collect::<Result<Vec<_>>>()?;
            let bound = estimate_builder_bytes_refs(ty, &cells)?;
            build_argument_vector(bound, memory, || copy_array(ty, &cells))
        }
        ArgumentChunk::Rows { rows, .. } => {
            let bound = estimate_builder_bytes_refs(ty, rows.iter().map(|row| &row[position]))?;
            build_argument_vector(bound, memory, || {
                let cells = rows.iter().map(|row| row[position].clone()).collect::<Vec<_>>();
                copy_array(ty, &cells)
            })
        }
    }
}

fn build_argument_vector(bound: usize, memory: &Arc<MemoryBudget>, build: impl FnOnce() -> Result<ArrayRef>) -> Result<ArgumentVector> {
    let mut scratch = memory.reserve(bound)?;
    let array = build()?;
    Arc::get_mut(&mut scratch).expect("An argument vector owns its scratch").shrink_to(array.get_array_memory_size())?;
    Ok((array, Some(scratch)))
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
        LogicalType::Nothing | LogicalType::RunEndEncoded { .. } | LogicalType::Scalar(ScalarType::Keyword | ScalarType::Uri) => false,
        LogicalType::Maybe(value) => can_copy_argument(value),
        LogicalType::Struct(fields) => fields.values().all(|ty| *ty == LogicalType::Nothing || can_copy_argument(ty)),
        LogicalType::List { kind: ListKind::List | ListKind::Set | ListKind::FixedSizeList(_), element } => can_copy_argument(element),
        LogicalType::List { .. } => false,
        LogicalType::Union(legs) => legs.iter().all(can_copy_argument),
        LogicalType::Null | LogicalType::Scalar(_) => true,
    }
}

#[test]
fn decoding_one_argument_chunk_preserves_compatible_arrow_inputs() {
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
    let aligned = args.source_columnar_batch(&memory).unwrap().unwrap();
    let replay = SqlArguments::from_arrow(aligned, &memory).unwrap();
    let values = replay.rows().map(|row| row.unwrap()[0].clone()).collect::<Vec<_>>();
    assert_eq!(values, [first, second]);
}
