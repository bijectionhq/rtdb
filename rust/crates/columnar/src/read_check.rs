use std::sync::Arc;
use arrow_array::{Array, cast::AsArray, types::{Int16Type, Int32Type, Int64Type}};
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_types::{Anomaly, ListKind, ListValue, LogicalType, Result, ScalarType, UriParts, Value};

use crate::{EXTENSION_NAME_KEY, MemoryBudget, from_field, leg_name, read_value_as, schema::scalar_reader_accepts, to_field};
use crate::values::union_read_type;

pub struct ColumnPresence<'a> {
    field: &'a Field,
    logical: LogicalType,
    plan: Option<ReadPlan>,
    check: bool,
}

impl<'a> ColumnPresence<'a> {
    pub fn new(field: &'a Field) -> Result<Self> {
        let logical = from_field(field)?;
        let plan = ambiguous_union(field, &logical)?.then(|| ReadPlan::new(field, &logical));
        Ok(Self { field, logical, plan, check: needs_check(field, false) })
    }

    pub fn is_bottom(&self) -> bool { self.logical == LogicalType::Nothing }

    pub fn uses_parent_validity(&self) -> bool {
        self.plan.is_none() && !self.check
            && !matches!(self.field.data_type(), DataType::Null | DataType::Union(..) | DataType::RunEndEncoded(..))
    }

    /// Checks a stored event slot and reports whether its logical read follows its physical layout.
    pub fn validate_stored(&self, array: &dyn Array, row: usize) -> Result<bool> {
        if self.is_bottom() { return Ok(true); }
        if row >= array.len() { return Err(Anomaly::incorrect("rtdb/row-index", "Cell index exceeds array length")); }
        if array.data_type() != self.field.data_type() { return Err(layout()); }
        if self.plan.is_some() {
            return present_as(&self.logical, self.field, array, row).map(|(_, follows_layout)| follows_layout);
        }
        self.is_present(array, row)?;
        Ok(true)
    }

    /// Returns the logical field for a validated alias that can use the selected leaf's buffers.
    pub fn stored_alias(&self, array: &dyn Array, row: usize) -> Result<Option<Field>> {
        stored_alias(&self.logical, self.field, array, row)
    }

    pub fn is_present(&self, array: &dyn Array, row: usize) -> Result<bool> {
        if row >= array.len() || array.data_type() != self.field.data_type() { return Err(layout()); }
        if let Some(plan) = &self.plan {
            return Ok(plan.check(self.field, array, row, None)? != ReadKind::Absent);
        }
        if self.check { check_cell(self.field, array, row, false)?; }
        present(self.field, array, row)
    }
}

fn stored_alias(logical: &LogicalType, field: &Field, array: &dyn Array, row: usize) -> Result<Option<Field>> {
    match field.data_type() {
        DataType::Union(fields, UnionMode::Dense) => {
            let union = array.as_union();
            let tag = union.type_id(row);
            let (_, child) = fields.iter().find(|(id, _)| *id == tag).ok_or_else(layout)?;
            let logical = union_read_type(logical, child)?;
            stored_alias(&logical, child, union.child(tag).as_ref(), union.value_offset(row))
        }
        DataType::RunEndEncoded(ends, child) => {
            let physical;
            let value = if let LogicalType::RunEndEncoded { value, .. } = logical { value.as_ref() }
                else { physical = from_field(child)?; &physical };
            macro_rules! in_run {
                ($ty:ty) => {{
                    let run = array.as_run::<$ty>();
                    stored_alias(value, child, run.values().as_ref(), run.get_physical_index(row))
                }}
            }
            match ends.data_type() {
                DataType::Int16 => in_run!(Int16Type),
                DataType::Int32 => in_run!(Int32Type),
                DataType::Int64 => in_run!(Int64Type),
                _ => Err(layout()),
            }
        }
        _ => {
            let expected = to_field(field.name(), logical)?;
            Ok((expected.data_type() == field.data_type() && expected.metadata() != field.metadata()).then_some(expected))
        }
    }
}

// A named union leg can override its child's type. Track whether the read
// uses the child's stored representation as well as whether it is present.
fn present_as(logical: &LogicalType, field: &Field, array: &dyn Array, row: usize) -> Result<(bool, bool)> {
    if logical == &LogicalType::Nothing {
        return Err(Anomaly::fault("xtdb/zero-leg-read", "The bottom type has no values to read"));
    }
    match field.data_type() {
        DataType::Union(fields, UnionMode::Dense) => {
            let union = array.as_union();
            let id = union.type_id(row);
            let (_, child) = fields.iter().find(|(tag, _)| *tag == id).ok_or_else(layout)?;
            let selected = union_read_type(logical, child)?;
            return present_as(&selected, child, union.child(id).as_ref(), union.value_offset(row));
        }
        DataType::RunEndEncoded(ends, child) => {
            let derived;
            let logical = match logical {
                LogicalType::RunEndEncoded { value, .. } => value.as_ref(),
                _ => { derived = from_field(child)?; &derived }
            };
            macro_rules! in_run {
                ($ty:ty) => {{
                    let runs = array.as_run::<$ty>();
                    present_as(logical, child, runs.values().as_ref(), runs.get_physical_index(row))
                }}
            }
            return match ends.data_type() {
                DataType::Int16 => in_run!(Int16Type),
                DataType::Int32 => in_run!(Int32Type),
                DataType::Int64 => in_run!(Int64Type),
                _ => Err(layout()),
            };
        }
        DataType::Null if array.is_null(row) || logical == &LogicalType::Null => return Ok((false, field.is_nullable())),
        _ if array.is_null(row) => return Ok((false, true)),
        _ if logical == &LogicalType::Null => return Ok((false, false)),
        _ => {}
    }
    let physical = from_field(field)?;
    let physical = required(&physical);
    let logical = required(logical);
    if physical == logical && !ambiguous_union(field, logical)? {
        if needs_check(field, false) { check_cell(field, array, row, false)?; }
        return Ok((true, true));
    }
    match (field.data_type(), logical) {
        (DataType::Struct(fields), LogicalType::Struct(types)) => {
            let mut follows_layout = true;
            for (field, child) in fields.iter().zip(array.as_struct().columns()) {
                let ty = types.get(field.name()).ok_or_else(layout)?;
                follows_layout &= if ty == &LogicalType::Nothing {
                    field.data_type() == &DataType::Null && !field.is_nullable()
                } else {
                    present_as(ty, field, child.as_ref(), row)?.1
                };
            }
            Ok((true, follows_layout))
        }
        (DataType::List(_) | DataType::FixedSizeList(_, _), LogicalType::List { kind, element })
            if matches!(kind, ListKind::List | ListKind::Set | ListKind::FixedSizeList(_)) => {
            let (field, values) = match field.data_type() {
                DataType::List(child) => (child, array.as_list::<i32>().value(row)),
                DataType::FixedSizeList(child, _) => (child, array.as_fixed_size_list().value(row)),
                _ => unreachable!(),
            };
            let mut follows_layout = matches!(physical, LogicalType::List { kind: stored, .. } if stored == kind);
            for row in 0..values.len() { follows_layout &= present_as(element, field, values.as_ref(), row)?.1; }
            Ok((true, follows_layout))
        }
        (_, LogicalType::Scalar(_)) => {
            let expected = to_field(field.name(), logical)?;
            if expected.data_type() == array.data_type() {
                if needs_check(&expected, false) { check_cell(&expected, array, row, false)?; }
                Ok((true, false))
            } else {
                Ok((!matches!(read_value_as(logical, field, array, row)?, Value::Null | Value::Undefined), false))
            }
        }
        _ => Ok((!matches!(read_value_as(logical, field, array, row)?, Value::Null | Value::Undefined), false)),
    }
}

fn required(ty: &LogicalType) -> &LogicalType {
    match ty { LogicalType::Maybe(value) => value, other => other }
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
    let check = CellReadCheck::for_layout(field, logical, memory)?;
    if !matches!(check.mode, CellReadMode::Plain) {
        for row in 0..array.len() { check.check(array, row)?; }
    }
    Ok(())
}

/// Checks the selected cell without reading payloads whose physical layouts already establish validity.
pub fn validate_readable_cell(field: &Field, array: &dyn Array, logical: &LogicalType, row: usize, memory: &Arc<MemoryBudget>) -> Result<()> {
    CellReadCheck::admit(field, array, logical, row, memory).map(drop)
}

pub struct CellReadCheck<'a> {
    field: &'a Field,
    mode: CellReadMode,
}

enum CellReadMode {
    Plain,
    Physical,
    Logical { plan: ReadPlan, reservation: Arc<crate::MemoryReservation> },
}

impl<'a> CellReadCheck<'a> {
    /// Admits the first selected cell and prepares a check for later cells of the same layout.
    pub fn admit(field: &'a Field, array: &dyn Array, logical: &LogicalType, row: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if row >= array.len() || array.data_type() != field.data_type() { return Err(layout()); }
        let check = Self::for_layout(field, logical, memory)?;
        check.check(array, row)?;
        Ok(check)
    }

    pub fn validate(&self, array: &dyn Array, row: usize) -> Result<()> {
        if row >= array.len() || array.data_type() != self.field.data_type() { return Err(layout()); }
        self.check(array, row)
    }

    fn for_layout(field: &'a Field, logical: &LogicalType, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let mode = if ambiguous_union(field, logical)? {
            let reservation = reserve_plan(field, memory)?;
            CellReadMode::Logical { plan: ReadPlan::new(field, logical), reservation }
        } else if needs_check(field, false) {
            CellReadMode::Physical
        } else {
            CellReadMode::Plain
        };
        Ok(Self { field, mode })
    }

    fn check(&self, array: &dyn Array, row: usize) -> Result<()> {
        match &self.mode {
            CellReadMode::Plain => {},
            CellReadMode::Physical => { check_cell(self.field, array, row, false)?; }
            CellReadMode::Logical { plan, reservation } => {
                plan.check(self.field, array, row, Some(reservation.budget()))?;
            }
        }
        Ok(())
    }
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

fn reserve_plan(field: &Field, memory: &Arc<MemoryBudget>) -> Result<Arc<crate::MemoryReservation>> {
    let bytes = field.size().checked_mul(8).ok_or_else(layout)?;
    memory.reserve(bytes)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadKind { Absent, Present, Timestamp, MapEntry }

enum ReadPlan {
    Bottom,
    Null { bottom: bool },
    Plain(ReadKind),
    Uri,
    Union(Vec<(i8, ReadPlan)>),
    Run(Box<ReadPlan>),
    Struct { children: Vec<Option<ReadPlan>>, kind: ReadKind },
    Sequence { child: Box<ReadPlan>, kind: ListKind },
    Invalid(Anomaly),
}

impl ReadPlan {
    fn new(field: &Field, logical: &LogicalType) -> Self {
        if *logical == LogicalType::Nothing { return Self::Bottom; }
        match field.data_type() {
            DataType::Union(fields, UnionMode::Dense) => {
                let children = fields.iter().map(|(id, child)| {
                    let named = match logical {
                        LogicalType::Union(legs) => legs.iter().find(|leg| leg_name(leg).is_ok_and(|name| name == *child.name())),
                        _ => None,
                    };
                    (id, named.map_or_else(|| Self::derive(child), |leg| Self::new(child, leg)))
                }).collect();
                return Self::Union(children);
            }
            DataType::RunEndEncoded(_, child) => {
                let child = match logical {
                    LogicalType::RunEndEncoded { value, .. } => Self::new(child, value),
                    _ => Self::derive(child),
                };
                return Self::Run(Box::new(child));
            }
            _ => {}
        }
        let logical = match logical { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
        match (logical, field.data_type()) {
            (LogicalType::Null, _) => Self::Null { bottom: field.data_type() == &DataType::Null && !field.is_nullable() },
            (LogicalType::Struct(types), DataType::Struct(fields)) => {
                let children = fields.iter().map(|field| match types.get(field.name()) {
                    Some(LogicalType::Nothing) => None,
                    Some(ty) => Some(Self::new(field, ty)),
                    None => Some(Self::Invalid(layout())),
                }).collect();
                let entry = fields.iter().any(|field| field.name() == "key") && fields.iter().any(|field| field.name() == "value");
                Self::Struct { children, kind: if entry { ReadKind::MapEntry } else { ReadKind::Present } }
            }
            (LogicalType::List { kind, element }, DataType::List(child) | DataType::FixedSizeList(child, _) | DataType::Map(child, _)) => {
                if matches!(kind, ListKind::List | ListKind::FixedSizeList(_))
                    && ListValue::supports_numeric_arrow(child)
                    && from_field(child).is_ok_and(|physical| physical == **element) {
                    return Self::Plain(ReadKind::Present);
                }
                let child = Self::new(child, element);
                if matches!(kind, ListKind::List | ListKind::FixedSizeList(_) | ListKind::Set) && child.infallible() {
                    Self::Plain(ReadKind::Present)
                } else {
                    Self::Sequence { child: Box::new(child), kind: kind.clone() }
                }
            }
            (LogicalType::Scalar(ScalarType::Uri), DataType::Utf8) => Self::Uri,
            (LogicalType::Scalar(scalar), data_type) if scalar_reader_accepts(scalar, data_type) => match scalar {
                ScalarType::Timestamp { .. } => Self::Plain(ReadKind::Timestamp),
                _ => Self::Plain(ReadKind::Present),
            },
            (LogicalType::Scalar(ScalarType::Decimal { bit_width, .. }), _)
                if !matches!(bit_width, 32 | 64 | 128 | 256) =>
                Self::Invalid(Anomaly::incorrect("rtdb/decimal-width", "Decimal coefficient must fill its declared signed width").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded)),
            _ => Self::Invalid(layout()),
        }
    }

    fn infallible(&self) -> bool {
        match self {
            Self::Null { bottom: false } | Self::Plain(_) => true,
            Self::Union(children) => children.iter().all(|(_, child)| child.infallible()),
            _ => false,
        }
    }

    fn derive(field: &Field) -> Self {
        match from_field(field) {
            Ok(logical) => Self::new(field, &logical),
            Err(error) => Self::Invalid(error),
        }
    }

    fn check(&self, field: &Field, array: &dyn Array, row: usize, memory: Option<&Arc<MemoryBudget>>) -> Result<ReadKind> {
        use ReadKind::*;
        match self {
            Self::Bottom => return Err(Anomaly::fault("xtdb/zero-leg-read", "The bottom type has no values to read")),
            Self::Union(children) => {
                let union = array.as_union();
                let id = union.type_id(row);
                let DataType::Union(fields, _) = field.data_type() else { return Err(layout()); };
                let (_, child) = fields.iter().find(|(tag, _)| *tag == id).ok_or_else(layout)?;
                let (_, plan) = children.iter().find(|(tag, _)| *tag == id).ok_or_else(layout)?;
                return plan.check(child, union.child(id).as_ref(), union.value_offset(row), memory);
            }
            Self::Run(plan) => {
                let DataType::RunEndEncoded(ends, child) = field.data_type() else { return Err(layout()); };
                macro_rules! check_run {
                    ($ty:ty) => {{
                        let run = array.as_run::<$ty>();
                        plan.check(child, run.values().as_ref(), run.get_physical_index(row), memory)
                    }}
                }
                return match ends.data_type() {
                    DataType::Int16 => check_run!(Int16Type),
                    DataType::Int32 => check_run!(Int32Type),
                    DataType::Int64 => check_run!(Int64Type),
                    _ => Err(layout()),
                };
            }
            _ => {}
        }
        if array.is_null(row) { return Ok(Absent); }
        Ok(match self {
            Self::Null { bottom: true } if memory.is_some() => {
                return Err(Anomaly::fault("xtdb/zero-leg-read", "The bottom type has no values to read"));
            }
            Self::Null { .. } => Absent,
            Self::Plain(kind) => *kind,
            Self::Uri => {
                UriParts::parse(array.as_string::<i32>().value(row))?;
                Present
            }
            Self::Struct { children, kind } => {
                let DataType::Struct(fields) = field.data_type() else { return Err(layout()); };
                for ((field, array), plan) in fields.iter().zip(array.as_struct().columns()).zip(children) {
                    if let Some(plan) = plan { plan.check(field, array.as_ref(), row, memory)?; }
                }
                *kind
            }
            Self::Sequence { child, kind } => {
                let cell = crate::ArrowCell { field, array, row };
                let (field, values, range) = cell.sequence_range().ok_or_else(layout)?;
                let mut range_ok = range.len() == 2;
                let mut entries_ok = true;
                for row in range {
                    let result = child.check(field, values, row, memory)?;
                    range_ok &= result == Timestamp;
                    entries_ok &= result == MapEntry;
                }
                match kind {
                    ListKind::TimestampTzRange if !range_ok =>
                        return Err(Anomaly::incorrect("rtdb/timestamp-range", "Timestamp range requires two non-null timestamps")),
                    ListKind::Map { .. } if !entries_ok => return Err(layout()),
                    _ => {}
                }
                Present
            }
            Self::Invalid(error) => return Err(error.clone()),
            Self::Bottom | Self::Union(_) | Self::Run(_) => unreachable!(),
        })
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

    #[test]
    fn keyword_alias_admission_does_not_reserve_a_copy_of_the_spelling() {
        let fields = UnionFields::try_new([0, 1], [
            Field::new("keyword", DataType::Utf8, false),
            crate::to_field("other", &LogicalType::Scalar(ScalarType::Keyword)).unwrap(),
        ]).unwrap();
        let spelling = "k".repeat(64 * 1024);
        let array = UnionArray::try_new(fields, vec![0_i8].into(), Some(vec![0_i32].into()), vec![
            Arc::new(StringArray::from(vec![spelling.as_str()])),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ]).unwrap();
        let field = Field::new("arg", array.data_type().clone(), false);
        let logical = from_field(&field).unwrap();
        let memory = MemoryBudget::new(16 * 1024);
        assert!(ambiguous_union(&field, &logical).unwrap());
        assert!(crate::estimate_read_value_bytes(&field, &array, 0).unwrap() > memory.limit_bytes());
        validate_readable_column(&field, &array, &logical, &memory).unwrap();
        validate_readable_cell(&field, &array, &logical, 0, &memory).unwrap();
        assert!(ColumnPresence::new(&field).unwrap().is_present(&array, 0).unwrap());
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn ambiguous_union_checks_selected_offsets_without_vector_decode_scratch() {
        use arrow_array::{Float32Array, ListArray, NullArray, types::Float32Type};

        let vector = ListArray::from_iter_primitive::<Float32Type, _, _>([
            Some((0..8192).map(|i| (i % 2 == 0).then_some(i as f32)).collect::<Vec<_>>()),
            None,
        ]);
        assert!(vector.values().as_any().is::<Float32Array>());
        let fields = UnionFields::try_new([0, 2, 4, 6], [
            Field::new("list", vector.data_type().clone(), true),
            Field::new("uri", DataType::Utf8, false),
            crate::to_field("other", &LogicalType::Scalar(ScalarType::Uri)).unwrap(),
            Field::new("null", DataType::Null, true),
        ]).unwrap();
        let array = UnionArray::try_new(fields, vec![0_i8, 2, 0, 6].into(), Some(vec![0_i32, 1, 1, 0].into()), vec![
            Arc::new(vector),
            Arc::new(StringArray::from(vec!["bad uri", "https://example.com/"])),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(NullArray::new(1)),
        ]).unwrap();
        let field = Field::new("arg", array.data_type().clone(), false);
        let logical = from_field(&field).unwrap();
        assert!(ambiguous_union(&field, &logical).unwrap());
        let memory = MemoryBudget::new(32 * 1024);
        assert!(crate::estimate_read_value_bytes(&field, &array, 0).unwrap() > memory.limit_bytes());

        validate_readable_column(&field, &array, &logical, &memory).unwrap();
        let presence = ColumnPresence::new(&field).unwrap();
        let actual = (0..array.len()).map(|row| {
            validate_readable_cell(&field, &array, &logical, row, &memory).unwrap();
            presence.is_present(&array, row).unwrap()
        }).collect::<Vec<_>>();
        assert_eq!(actual, [true, true, false, false]);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn presence_and_admission_keep_their_selected_bottom_leg_rules() {
        use arrow_array::{Int64Array, NullArray};

        let fields = UnionFields::try_new([0, 1, 2, 3], [
            Field::new("null", DataType::Null, false),
            Field::new("nullable", DataType::Null, true),
            Field::new("utf8", DataType::Utf8, false),
            Field::new("i64", DataType::Int64, false),
        ]).unwrap();
        let array = UnionArray::try_new(fields, vec![0_i8, 2].into(), Some(vec![0_i32, 0].into()), vec![
            Arc::new(NullArray::new(1)),
            Arc::new(NullArray::new(0)),
            Arc::new(StringArray::from(vec!["valid"])),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ]).unwrap();
        let field = Field::new("v", array.data_type().clone(), false);
        let logical = from_field(&field).unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let reader = ColumnPresence::new(&field).unwrap();

        assert!(!reader.is_present(&array, 0).unwrap());
        assert!(reader.is_present(&array, 1).unwrap());
        assert_eq!(validate_readable_cell(&field, &array, &logical, 0, &memory).unwrap_err().code, "xtdb/zero-leg-read");
        validate_readable_cell(&field, &array, &logical, 1, &memory).unwrap();
    }

}
