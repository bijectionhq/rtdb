use std::sync::Arc;
use arrow_array::{Array, cast::AsArray, types::{Int16Type, Int32Type, Int64Type}};
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_types::{Anomaly, LogicalType, Result, UriParts, Value};

use crate::{EXTENSION_NAME_KEY, MemoryBudget, estimate_read_value_bytes, from_field, leg_name, read_value_as};

pub struct ColumnPresence<'a> {
    field: &'a Field,
    logical: LogicalType,
    ambiguous: bool,
    check: bool,
}

impl<'a> ColumnPresence<'a> {
    pub fn new(field: &'a Field) -> Result<Self> {
        let logical = from_field(field)?;
        let ambiguous = ambiguous_union(field, &logical)?;
        Ok(Self { field, logical, ambiguous, check: needs_check(field, false) })
    }

    pub fn is_bottom(&self) -> bool { self.logical == LogicalType::Nothing }

    pub fn is_present(&self, array: &dyn Array, row: usize) -> Result<bool> {
        if row >= array.len() || array.data_type() != self.field.data_type() { return Err(layout()); }
        if self.ambiguous {
            return Ok(!matches!(read_value_as(&self.logical, self.field, array, row)?, Value::Null | Value::Undefined));
        }
        if self.check { check_cell(self.field, array, row, false)?; }
        present(self.field, array, row)
    }
}

fn present(field: &Field, array: &dyn Array, row: usize) -> Result<bool> {
    match field.data_type() {
        DataType::Null => Ok(false),
        DataType::Union(fields, UnionMode::Dense) => {
            let union = array.as_union();
            let id = union.type_id(row);
            let (_, child) = fields.iter().find(|(tag, _)| *tag == id).ok_or_else(layout)?;
            present(child, union.child(id).as_ref(), union.value_offset(row))
        }
        DataType::RunEndEncoded(ends, child) => {
            macro_rules! in_run {
                ($ty:ty) => {{
                    let runs = array.as_run::<$ty>();
                    present(child, runs.values().as_ref(), runs.get_physical_index(row))
                }}
            }
            match ends.data_type() {
                DataType::Int16 => in_run!(Int16Type),
                DataType::Int32 => in_run!(Int32Type),
                DataType::Int64 => in_run!(Int64Type),
                _ => Err(layout()),
            }
        }
        _ => Ok(array.is_valid(row)),
    }
}

/// Checks value-dependent reads from a column whose Arrow layout has already been validated.
pub fn validate_readable_column(field: &Field, array: &dyn Array, logical: &LogicalType, memory: &Arc<MemoryBudget>) -> Result<()> {
    if array.is_empty() { return Ok(()); }
    if ambiguous_union(field, logical)? {
        for row in 0..array.len() {
            let _cell = memory.reserve(estimate_read_value_bytes(field, array, row)?)?;
            read_value_as(logical, field, array, row)?;
        }
        return Ok(());
    }
    if needs_check(field, false) {
        for row in 0..array.len() { check_cell(field, array, row, false)?; }
    }
    Ok(())
}

/// Checks the selected cell without reading payloads whose physical layouts already establish validity.
pub fn validate_readable_cell(field: &Field, array: &dyn Array, logical: &LogicalType, row: usize, memory: &Arc<MemoryBudget>) -> Result<()> {
    if row >= array.len() || array.data_type() != field.data_type() { return Err(layout()); }
    if ambiguous_union(field, logical)? {
        let _cell = memory.reserve(estimate_read_value_bytes(field, array, row)?)?;
        read_value_as(logical, field, array, row)?;
    } else if needs_check(field, false) {
        check_cell(field, array, row, false)?;
    }
    Ok(())
}

fn ambiguous_union(field: &Field, logical: &LogicalType) -> Result<bool> {
    if let DataType::Union(children, UnionMode::Dense) = field.data_type() {
        for (_, child) in children.iter() {
            let physical = from_field(child)?;
            if let LogicalType::Union(legs) = logical
                && let Some(named) = legs.iter().find(|leg| leg_name(leg).is_ok_and(|name| name == *child.name()))
                && *named != physical
            {
                return Ok(true);
            }
            if ambiguous_union(child, &physical)? { return Ok(true); }
        }
        return Ok(false);
    }
    let logical = match logical { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
    match (field.data_type(), logical) {
        (DataType::Struct(fields), LogicalType::Struct(types)) => {
            for field in fields {
                if ambiguous_union(field, types.get(field.name()).ok_or_else(layout)?)? { return Ok(true); }
            }
            Ok(false)
        }
        (DataType::List(child) | DataType::FixedSizeList(child, _) | DataType::Map(child, _), LogicalType::List { element, .. }) =>
            ambiguous_union(child, element),
        (DataType::RunEndEncoded(_, child), LogicalType::RunEndEncoded { value, .. }) => ambiguous_union(child, value),
        _ => Ok(false),
    }
}

fn extension(field: &Field) -> Option<&str> {
    field.metadata().get(EXTENSION_NAME_KEY).map(String::as_str)
}

fn needs_check(field: &Field, under_struct: bool) -> bool {
    if matches!(extension(field), Some("uri" | "xt/tstz-range")) { return true; }
    match field.data_type() {
        DataType::Null => !field.is_nullable() && !under_struct,
        DataType::Struct(fields) => fields.iter().any(|child| needs_check(child, true)),
        DataType::Union(fields, UnionMode::Dense) => fields.iter().any(|(_, child)| needs_check(child, false)),
        DataType::List(child) | DataType::FixedSizeList(child, _) | DataType::Map(child, _) | DataType::RunEndEncoded(_, child) => {
            needs_check(child, false)
        }
        _ => false,
    }
}

// Returns whether the selected cell reads as a timestamp, for range endpoints.
fn check_cell(field: &Field, array: &dyn Array, row: usize, under_struct: bool) -> Result<bool> {
    match field.data_type() {
        DataType::Null => {
            if !field.is_nullable() && !under_struct {
                return Err(Anomaly::fault("xtdb/zero-leg-read", "The bottom type has no values to read"));
            }
        }
        _ if array.is_null(row) => {}
        DataType::Timestamp(..) => return Ok(true),
        DataType::Utf8 if extension(field) == Some("uri") => { UriParts::parse(array.as_string::<i32>().value(row))?; }
        DataType::Union(fields, UnionMode::Dense) => {
            let union = array.as_union();
            let id = union.type_id(row);
            let (_, child) = fields.iter().find(|(tag, _)| *tag == id).ok_or_else(layout)?;
            return check_cell(child, union.child(id).as_ref(), union.value_offset(row), false);
        }
        DataType::RunEndEncoded(ends, child) => {
            macro_rules! check_run {
                ($ty:ty) => {{
                    let runs = array.as_run::<$ty>();
                    check_cell(child, runs.values().as_ref(), runs.get_physical_index(row), false)
                }};
            }
            return match ends.data_type() {
                DataType::Int16 => check_run!(Int16Type),
                DataType::Int32 => check_run!(Int32Type),
                DataType::Int64 => check_run!(Int64Type),
                _ => Err(layout()),
            };
        }
        DataType::Struct(fields) => {
            for (child, column) in fields.iter().zip(array.as_struct().columns()) {
                check_cell(child, column.as_ref(), row, true)?;
            }
        }
        DataType::List(child) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            for index in offsets[row]..offsets[row + 1] {
                check_cell(child, list.values().as_ref(), index as usize, false)?;
            }
        }
        DataType::FixedSizeList(child, width) => {
            let list = array.as_fixed_size_list();
            let width = *width as usize;
            let start = row * width;
            let mut endpoints = true;
            for index in start..start + width {
                endpoints &= check_cell(child, list.values().as_ref(), index, false)?;
            }
            if extension(field) == Some("xt/tstz-range") && (width != 2 || !endpoints) {
                return Err(Anomaly::incorrect("rtdb/timestamp-range", "Timestamp range requires two non-null timestamps"));
            }
        }
        DataType::Map(child, _) => {
            let map = array.as_map();
            let offsets = map.value_offsets();
            for index in offsets[row]..offsets[row + 1] {
                check_cell(child, map.entries(), index as usize, false)?;
            }
        }
        _ => {}
    }
    Ok(false)
}

fn layout() -> Anomaly { Anomaly::incorrect("rtdb/array-layout", "Arrow array and logical field layout disagree") }

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, FixedSizeListArray, StringArray, TimestampMicrosecondArray, UnionArray};
    use arrow_buffer::{NullBuffer, ScalarBuffer};
    use arrow_schema::UnionFields;
    use rtdb_types::{ListKind, LogicalType, ScalarType, TimeUnit};

    fn validate(field: &Field, array: &dyn Array) -> Result<()> {
        let memory = MemoryBudget::new(1 << 20);
        validate_readable_column(field, array, &from_field(field)?, &memory)
    }

    #[test]
    fn invalid_uri_cells_beneath_a_null_struct_are_unread() {
        let uri = Arc::new(crate::to_field("uri", &LogicalType::Scalar(ScalarType::Uri)).unwrap());
        let child: ArrayRef = Arc::new(StringArray::from(vec!["bad uri"]));
        let fields = vec![uri].into();
        let null = crate::build_struct_array(fields, &[Arc::clone(&child)], 1, Some(NullBuffer::from(vec![false]))).unwrap();
        let field = Field::new("arg", null.data_type().clone(), true);
        validate(&field, null.as_ref()).unwrap();

        let DataType::Struct(fields) = field.data_type() else { unreachable!() };
        let visible = crate::build_struct_array(fields.clone(), &[child], 1, None).unwrap();
        assert_eq!(validate(&field, visible.as_ref()).unwrap_err().code,
            crate::read_value(&field, visible.as_ref(), 0).unwrap_err().code);
    }

    #[test]
    fn timestamp_range_validation_matches_scalar_reads_for_null_endpoints() {
        let timestamp = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
        let range = LogicalType::List { kind: ListKind::TimestampTzRange, element: Box::new(timestamp.maybe()) };
        let field = crate::to_field("arg", &range).unwrap();
        let DataType::FixedSizeList(child, width) = field.data_type() else { unreachable!() };
        let endpoints = Arc::new(TimestampMicrosecondArray::from(vec![Some(5), None]).with_timezone("UTC"));
        let array = FixedSizeListArray::try_new(Arc::clone(child), *width, endpoints, None).unwrap();

        assert_eq!(validate(&field, &array).unwrap_err().code, "rtdb/timestamp-range");
        assert_eq!(crate::read_value(&field, &array, 0).unwrap_err().code, "rtdb/timestamp-range");
    }

    #[test]
    fn dense_union_read_checks_respect_ambiguous_leg_names() {
        let uri_type = LogicalType::Scalar(ScalarType::Uri);
        let plain = Arc::new(Field::new("uri", DataType::Utf8, false));
        let uri = Arc::new(crate::to_field("other", &uri_type).unwrap());
        let fields = UnionFields::try_new([0, 1], [plain, uri]).unwrap();
        let array = UnionArray::try_new(fields, ScalarBuffer::from(vec![0_i8]), Some(ScalarBuffer::from(vec![0_i32])), vec![
            Arc::new(StringArray::from(vec!["bad uri"])),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ]).unwrap();
        let field = Field::new("arg", array.data_type().clone(), false);

        assert_eq!(validate(&field, &array).unwrap_err().code, crate::read_value(&field, &array, 0).unwrap_err().code);
    }
}
