use std::{ops::Range, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, NullArray, StringArray,
    builder::{ArrayBuilder, BinaryBuilder, StringBuilder},
};
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_columnar::{ArrowCell, MemoryReservation};
use rtdb_plan::{BinaryOp, Expr, Schema};
use rtdb_types::{Anomaly, AnomalyKind, ListBuilder, ListKind, LogicalType, Result, ScalarType, Value};

use super::binary::{Input, SignedInput};
use super::{BatchRow, Column, ColumnBatch, SelectedColumn, StoredVector, owned_projection_bytes, plain_scalar, size_error, stable_read_layout};
use crate::context::Execution;

fn bind_input<'a, T: Copy>(
    expr: &'a Expr, schema: &Schema, batch: &'a ColumnBatch, execution: &'a Execution,
    accepts: impl Fn(&LogicalType) -> bool, literal: impl Fn(&'a Value) -> Option<T>,
) -> Option<Input<'a, T>> {
    if let Some(input) = Input::bind(expr, schema, batch, execution, &accepts, |expr| {
        let Expr::Literal(value) = expr else { return None; };
        literal(value)
    }) {
        return Some(input);
    }
    let Expr::Parameter { index, .. } = expr else { return None; };
    if !accepts(execution.parameters.types().get(*index)?) { return None; }
    let value = execution.parameters.evaluator().known(*index)?;
    Some(Input::Literal(match value {
        Value::Null => None,
        value => Some(literal(value)?),
    }))
}

fn bind_signed_input<'a>(
    expr: &'a Expr, schema: &Schema, batch: &'a ColumnBatch, execution: &'a Execution,
) -> Option<SignedInput<'a>> {
    bind_input(expr, schema, batch, execution, |ty|
        *ty == LogicalType::Null || matches!(plain_scalar(ty), Some(ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64)),
        |value| match value {
            Value::I8(value) => Some(i64::from(*value)),
            Value::I16(value) => Some(i64::from(*value)),
            Value::I32(value) => Some(i64::from(*value)),
            Value::I64(value) => Some(*value),
            _ => None,
        })
}

pub(super) struct RuntimeIndex<'a> {
    input: SignedInput<'a>,
    one_based: bool,
}

impl<'a> RuntimeIndex<'a> {
    pub(super) fn bind(expr: &'a Expr, schema: &Schema, batch: &'a ColumnBatch, execution: &'a Execution) -> Option<Self> {
        let (input, one_based) = match expr {
            Expr::Binary { op: BinaryOp::Subtract, left, right } if matches!(right.as_ref(), Expr::Literal(Value::I64(1))) =>
                (left.as_ref(), true),
            _ => (expr, false),
        };
        if !matches!(input, Expr::Column { .. } | Expr::Parameter { .. }) { return None; }
        let input = bind_signed_input(input, schema, batch, execution)?;
        Some(Self { input, one_based })
    }

    pub(super) fn zero_based(&self, row: usize) -> Option<Option<usize>> {
        let Some(index) = self.input.signed(row)? else { return Some(None); };
        let index = if self.one_based { index.checked_sub(1)? } else { index };
        Some(usize::try_from(index).ok())
    }
}

fn text_cell(cell: ArrowCell<'_>) -> Option<Option<&str>> {
    let text = cell.array.as_any().downcast_ref::<StringArray>()?;
    Some(text.is_valid(cell.row).then(|| text.value(cell.row)))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ListShape { Null, Empty, NullItems, Text(bool), Binary(bool) }

enum ListRow<'a> {
    Null,
    Empty,
    Split(&'a str, &'a str),
    AsciiUnits(&'a str),
    Text(&'a StringArray, Range<usize>),
    Binary(&'a BinaryArray, Range<usize>),
}

struct ListRun { shape: ListShape, end: usize, items: usize, payload: usize }

pub(super) fn list_call(
    expr: &Expr, schema: &Schema, batch: &ColumnBatch, execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let Expr::Call { name, args, .. } = expr else { return Ok(None); };
    let [source, argument] = args.as_slice() else { return Ok(None); };
    if batch.len == 0 { return Ok(None); }
    let result = match name.as_str() {
        "string_to_array" => {
            let bind = |expr| bind_input(expr, schema, batch, execution,
                |ty| *ty == LogicalType::Null || plain_scalar(ty) == Some(&ScalarType::Utf8),
                |value| match value { Value::Utf8(text) => Some(text.as_str()), _ => None });
            let (Some(source), Some(delimiter)) = (bind(source), bind(argument)) else { return Ok(None); };
            let row = |row| {
                let text = source.read(row, text_cell)?;
                let delimiter = delimiter.read(row, text_cell)?;
                match (text, delimiter) {
                    (None, _) => Some(ListRow::Null),
                    (Some(""), _) => Some(ListRow::Empty),
                    (Some(text), Some(delimiter)) => Some(ListRow::Split(text, delimiter)),
                    (Some(text), None) if text.is_ascii() => Some(ListRow::AsciiUnits(text)),
                    _ => None,
                }
            };
            project_runs(row, batch.len, execution)
        }
        "trim_array" => {
            let Some(input) = bind_input(source, schema, batch, execution, |ty| {
                let ty = match ty { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
                matches!(ty, LogicalType::List { kind: ListKind::List, element }
                    if matches!(plain_scalar(element), Some(ScalarType::Utf8 | ScalarType::Binary)))
            }, |_| None) else { return Ok(None); };
            let Some(count) = bind_signed_input(argument, schema, batch, execution) else { return Ok(None); };
            project_runs(|row| {
                let Some(cell) = input.read(row, |cell| {
                    let DataType::List(child) = cell.field.data_type() else { return None; };
                    if !child.metadata().is_empty() || !matches!(child.data_type(), DataType::Utf8 | DataType::Binary) { return None; }
                    Some(cell.array.is_valid(cell.row).then_some(cell))
                })? else { return Some(ListRow::Null); };
                let Some(count) = count.signed(row)? else { return Some(ListRow::Null); };
                let (field, array, range) = cell.sequence_range()?;
                let count = usize::try_from(count).ok()?;
                let end = range.end.checked_sub(count).filter(|end| *end >= range.start)?;
                let range = range.start..end;
                if range.is_empty() { return Some(ListRow::Empty); }
                match field.data_type() {
                    DataType::Utf8 => Some(ListRow::Text(array.as_any().downcast_ref()?, range)),
                    DataType::Binary => Some(ListRow::Binary(array.as_any().downcast_ref()?, range)),
                    _ => None,
                }
            }, batch.len, execution)
        }
        _ => return Ok(None),
    };
    match result {
        Err(error) if error.is(AnomalyKind::ResourceExhausted) => Ok(None),
        result => result,
    }
}

fn project_runs<'a>(
    input: impl Fn(usize) -> Option<ListRow<'a>>, len: usize, execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let context = &execution.context;
    let memory = &context.memory;
    let mut run_slots = memory.reserve(0)?;
    let mut runs: Vec<ListRun> = Vec::new();
    for row in 0..len {
        context.check_cancelled_at(row)?;
        let Some(input) = input(row) else { return Ok(None); };
        let (shape, items, payload) = match input {
            ListRow::Null => (ListShape::Null, 0, 0),
            ListRow::Empty => (ListShape::Empty, 0, 0),
            ListRow::Split(text, "") => (ListShape::Text(false), 1, text.len()),
            ListRow::Split(text, delimiter) => {
                let separators = text.matches(delimiter).count();
                (ListShape::Text(false), separators + 1, text.len() - separators * delimiter.len())
            }
            ListRow::AsciiUnits(text) => (ListShape::Text(false), text.len(), text.len()),
            ListRow::Text(array, rows) => measure_prefix(array, rows, |row| array.value(row).len(), ListShape::Text, execution)?,
            ListRow::Binary(array, rows) => measure_prefix(array, rows, |row| array.value(row).len(), ListShape::Binary, execution)?,
        };
        if runs.last().is_none_or(|run| run.shape != shape) {
            if runs.len() == runs.capacity() {
                let capacity = runs.capacity().saturating_mul(2).max(4).min(len);
                let bytes = capacity.checked_mul(size_of::<ListRun>()).ok_or_else(size_error)?;
                Arc::get_mut(&mut run_slots).expect("The list projection owns its run inventory").grow_to(bytes)?;
                runs.reserve_exact(capacity - runs.len());
            }
            runs.push(ListRun { shape, end: row, items: 0, payload: 0 });
        }
        let run = runs.last_mut().expect("The current list run exists");
        run.end = row + 1;
        run.items = run.items.checked_add(items).ok_or_else(size_error)?;
        run.payload = run.payload.checked_add(payload).ok_or_else(size_error)?;
        if i32::try_from(run.items).is_err() || i32::try_from(run.payload).is_err() { return Ok(None); }
    }
    if let [run] = runs.as_slice() {
        return write_run(input, 0..len, run, execution).map(Some);
    }
    let inventory = len.checked_mul(size_of::<(usize, usize)>())
        .and_then(|bytes| bytes.checked_add(runs.len().checked_mul(size_of::<Option<StoredVector>>())?))
        .and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
    let mut owner = memory.reserve(inventory)?;
    let mut sources = Vec::with_capacity(runs.len());
    let mut picks = Vec::with_capacity(len);
    let mut start = 0;
    for run in runs {
        let (column, custody) = write_run(&input, start..run.end, &run, execution)?;
        let Column::Arrow { array, field, logical } = column else { unreachable!("A list run is Arrow"); };
        let index = sources.len();
        picks.extend((0..run.end - start).map(|row| (index, row)));
        sources.push(Some((array, field, logical, Some(custody))));
        start = run.end;
    }
    let column = Column::Selected(SelectedColumn::new(sources, Arc::new(picks)));
    settle_list_charge(&column, &mut owner)?;
    Ok(Some((column, owner)))
}

fn measure_prefix(
    array: &dyn Array, rows: Range<usize>, value_len: impl Fn(usize) -> usize, shape: impl FnOnce(bool) -> ListShape,
    execution: &Execution,
) -> Result<(ListShape, usize, usize)> {
    let items = rows.len();
    let mut present = 0;
    let mut payload = 0;
    for row in rows {
        execution.context.check_cancelled_at(row)?;
        if array.is_valid(row) {
            present += 1;
            payload += value_len(row);
        }
    }
    Ok((if present == 0 { ListShape::NullItems } else { shape(present != items) }, items, payload))
}

fn write_run<'a>(
    input: impl Fn(usize) -> Option<ListRow<'a>>, rows: Range<usize>, run: &ListRun, execution: &Execution,
) -> Result<(Column, Arc<MemoryReservation>)> {
    let validity_bytes = if matches!(run.shape, ListShape::Text(true) | ListShape::Binary(true)) {
        run.items.div_ceil(8)
    } else { 0 };
    let bound = rows.len().checked_add(run.items).and_then(|slots| slots.checked_add(2))
        .and_then(|slots| slots.checked_mul(size_of::<i32>())).and_then(|bytes| bytes.checked_add(run.payload))
        .and_then(|bytes| bytes.checked_add(validity_bytes))
        .and_then(|bytes| bytes.checked_add(1024)).ok_or_else(size_error)?;
    let mut owner = execution.context.memory.reserve(bound)?;
    let scalar = |scalar, nullable| {
        let ty = LogicalType::Scalar(scalar);
        if nullable { ty.maybe() } else { ty }
    };
    let logical = match run.shape {
        ListShape::Null => LogicalType::Null,
        ListShape::Empty => LogicalType::list(LogicalType::Nothing),
        ListShape::NullItems => LogicalType::list(LogicalType::Null),
        ListShape::Text(nullable) => LogicalType::list(scalar(ScalarType::Utf8, nullable)),
        ListShape::Binary(nullable) => LogicalType::list(scalar(ScalarType::Binary, nullable)),
    };
    let field = Arc::new(rtdb_columnar::to_field("value", &logical)?);
    let array: ArrayRef = match field.data_type() {
        DataType::Null => Arc::new(NullArray::new(rows.len())),
        DataType::List(child) => {
            let mut offsets = Vec::with_capacity(rows.len() + 1);
            offsets.push(0);
            let values: ArrayRef = match run.shape {
                ListShape::Empty => {
                    offsets.resize(rows.len() + 1, 0);
                    Arc::new(NullArray::new(0))
                }
                ListShape::NullItems => {
                    let mut count = 0;
                    for row in rows {
                        execution.context.check_cancelled_at(row)?;
                        let count_in_row = match input(row) {
                            Some(ListRow::Text(_, rows) | ListRow::Binary(_, rows)) => rows.len(),
                            _ => unreachable!("An all-null list run has child ranges"),
                        };
                        count += count_in_row;
                        offsets.push(count as i32);
                    }
                    Arc::new(NullArray::new(run.items))
                }
                ListShape::Text(_) => {
                    let mut builder = StringBuilder::with_capacity(run.items, run.payload);
                    for row in rows {
                        execution.context.check_cancelled_at(row)?;
                        match input(row) {
                            Some(ListRow::Split(text, "")) => builder.append_value(text),
                            Some(ListRow::Split(text, delimiter)) => {
                                for part in text.split(delimiter) { builder.append_value(part); }
                            }
                            Some(ListRow::AsciiUnits(text)) => {
                                for start in 0..text.len() {
                                    execution.context.check_cancelled_at(start + 1)?;
                                    builder.append_value(&text[start..start + 1]);
                                }
                            }
                            Some(ListRow::Text(array, rows)) => {
                                for row in rows {
                                    execution.context.check_cancelled_at(row)?;
                                    builder.append_option(array.is_valid(row).then(|| array.value(row)));
                                }
                            }
                            _ => unreachable!("A text list run has splits or text child ranges"),
                        }
                        offsets.push(builder.len() as i32);
                    }
                    Arc::new(builder.finish())
                }
                ListShape::Binary(_) => {
                    let mut builder = BinaryBuilder::with_capacity(run.items, run.payload);
                    for row in rows {
                        execution.context.check_cancelled_at(row)?;
                        let Some(ListRow::Binary(array, rows)) = input(row) else { unreachable!("A binary list run has binary child ranges"); };
                        for row in rows {
                            execution.context.check_cancelled_at(row)?;
                            builder.append_option(array.is_valid(row).then(|| array.value(row)));
                        }
                        offsets.push(builder.len() as i32);
                    }
                    Arc::new(builder.finish())
                }
                ListShape::Null => unreachable!("A null run has Arrow Null layout"),
            };
            rtdb_columnar::build_list_array(Arc::clone(child), offsets.into(), &values, None)?
        }
        _ => unreachable!("A list projection result is a list or null"),
    };
    let column = Column::Arrow { array, field, logical: Some(logical) };
    settle_list_charge(&column, &mut owner)?;
    Ok((column, owner))
}

fn settle_list_charge(column: &Column, owner: &mut Arc<MemoryReservation>) -> Result<()> {
    let bytes = owned_projection_bytes(column)?;
    let lease = Arc::get_mut(owner).expect("The list projection owns its reservation");
    if bytes <= lease.bytes() { lease.shrink_to(bytes) } else { lease.grow_to(bytes) }
}

pub(crate) fn column_constructor(
    expr: &Expr, schema: &Schema, batch: &ColumnBatch, execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let Expr::List(items) = expr else { return Ok(None); };
    if !items.iter().all(|item| {
        let Expr::Column { id, .. } = item else { return false; };
        let Some(position) = schema.position(*id) else { return false; };
        matches!(&batch.columns[position], Column::Arrow { field, .. } if rtdb_columnar::lendable_list_child(field))
    }) {
        return Ok(None);
    }
    let Some(bound) = super::projection::bound(expr, schema, batch)? else { return Ok(None); };
    let Some(mut owner) = execution.context.memory.try_reserve(bound) else { return Ok(None); };
    execution.context.check_cancelled()?;
    let Some(column) = super::projection::column(expr, schema, batch, execution.parameters.evaluator())? else { return Ok(None); };
    execution.context.check_cancelled()?;
    let bytes = owned_projection_bytes(&column)?;
    let lease = Arc::get_mut(&mut owner).expect("The list constructor owns its reservation");
    if bytes <= lease.bytes() { lease.shrink_to(bytes)?; } else { lease.grow_to(bytes)?; }
    Ok(Some((column, owner)))
}

fn primitive_numeric(ty: &LogicalType) -> bool {
    match ty {
        LogicalType::Null => true,
        LogicalType::Scalar(ty) => matches!(ty,
            ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64 |
            ScalarType::F16 | ScalarType::F32 | ScalarType::F64),
        LogicalType::Maybe(ty) => primitive_numeric(ty),
        LogicalType::Union(legs) => legs.iter().all(primitive_numeric),
        _ => false,
    }
}

pub(super) fn readable_numeric_union(field: &Field) -> bool {
    let DataType::Union(legs, UnionMode::Dense) = field.data_type() else { return false; };
    rtdb_types::ListValue::supports_numeric_arrow(field) && legs.iter().all(|(_, leg)| {
        rtdb_columnar::from_field(leg).is_ok_and(|ty| {
            let ty = match &ty { LogicalType::Maybe(inner) => inner.as_ref(), _ => &ty };
            primitive_numeric(ty) && rtdb_columnar::leg_name(ty).is_ok_and(|name| name == *leg.name())
        })
    })
}

pub(crate) fn captured_list_child(field: &Field) -> bool {
    rtdb_columnar::lendable_list_child(field) || readable_numeric_union(field)
        || (field.metadata().is_empty()
            && matches!(field.data_type(), DataType::List(child) if rtdb_columnar::lendable_list_child(child)))
}

fn numeric_item(expr: &Expr, schema: &Schema, batch: &ColumnBatch) -> bool {
    stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || match expr {
        Expr::Literal(value) => matches!(value,
            Value::Null | Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_) |
            Value::F16(_) | Value::F32(_) | Value::F64(_)),
        Expr::Column { id, .. } => schema.position(*id).is_some_and(|position| {
            let admitted = |field, logical: Option<&LogicalType>| logical.is_some_and(|ty|
                primitive_numeric(ty) && stable_read_layout(ty, field))
                && rtdb_types::ListValue::supports_numeric_arrow(field);
            primitive_numeric(&schema.fields()[position].data_type) && match &batch.columns[position] {
                Column::Arrow { field, logical, .. } => admitted(field, logical.as_ref()),
                Column::Selected(selected) => selected.sources.iter().flatten()
                    .all(|(_, field, logical, _)| admitted(field, logical.as_ref())),
                Column::Values(_) => false,
            }
        }),
        Expr::Binary { op: BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply |
            BinaryOp::Divide | BinaryOp::Modulo, left, right } => {
            numeric_item(left, schema, batch) && numeric_item(right, schema, batch)
        }
        _ => false,
    })
}

pub(crate) async fn numeric_constructor(
    expr: &Expr, schema: &Schema, batch: &ColumnBatch, execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let Expr::List(items) = expr else { return Ok(None); };
    let Some(count) = items.len().checked_mul(batch.len).filter(|&count| i32::try_from(count).is_ok()) else {
        return Ok(None);
    };
    if count == 0 || items.iter().all(|item| matches!(item, Expr::Literal(_)))
        || !items.iter().all(|item| numeric_item(item, schema, batch))
    {
        return Ok(None);
    }
    let bytes = count.checked_mul(64).and_then(|bytes| bytes.checked_add(4096))
        .and_then(|bytes| bytes.checked_add(batch.len.checked_mul(size_of::<i32>())?)).ok_or_else(size_error)?;
    let mut owner = match execution.context.memory.reserve(bytes) {
        Ok(owner) => owner,
        Err(error) if error.is(AnomalyKind::ResourceExhausted) => return Ok(None),
        Err(error) => return Err(error),
    };
    let metadata = execution.scope()?;
    let invocation = execution.prepare_expression(expr, schema, &metadata)?;
    let scope = execution.scope()?;
    let mut builder = ListBuilder::with_capacity(count);
    for row in 0..batch.len {
        let input = BatchRow { batch, row, admit_dates: false };
        execution.append_numeric_list_items(&scope, &invocation, rtdb_expr::ExpressionRow::Borrowed(&input), &mut builder).await?;
        scope.reset()?;
    }
    let (child, values) = builder.finish().packed_column()
        .ok_or_else(|| Anomaly::fault("rtdb.query/list-result", "A numeric list constructor returned a non-numeric item"))?;
    let child = Arc::new(Arc::unwrap_or_clone(child).with_name("$data$"));
    let logical = LogicalType::list(rtdb_columnar::from_field(&child)?);
    let offsets = (0..=batch.len).map(|row| (row * items.len()) as i32).collect::<Vec<_>>();
    let array = rtdb_columnar::build_list_array(child, offsets.into(), &values, None)?;
    let field = Arc::new(Field::new("value", array.data_type().clone(), false));
    let column = Column::Arrow { array, field, logical: Some(logical) };
    let bytes = owned_projection_bytes(&column)?;
    let reservation = Arc::get_mut(&mut owner).expect("The list constructor owns its reservation");
    if bytes <= reservation.bytes() { reservation.shrink_to(bytes)?; } else { reservation.grow_to(bytes)?; }
    Ok(Some((column, owner)))
}
