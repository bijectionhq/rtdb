use std::{ops::Range, sync::Arc};

use arrow_array::{Array, ArrayRef, NullArray, StringArray, builder::{ArrayBuilder, StringBuilder}};
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_columnar::{ArrowCell, MemoryReservation};
use rtdb_plan::{BinaryOp, Expr, Schema};
use rtdb_types::{Anomaly, AnomalyKind, ListBuilder, LogicalType, Result, ScalarType, Value};

use super::{BatchRow, Column, ColumnBatch, SelectedColumn, StoredVector, owned_projection_bytes, plain_scalar, size_error, stable_read_layout};
use crate::context::Execution;

enum SplitInput<'a> {
    Column(&'a Column),
    Parameter(ArrowCell<'a>),
    Literal(Option<&'a str>),
}

impl<'a> SplitInput<'a> {
    fn bind(expr: &'a Expr, schema: &Schema, batch: &'a ColumnBatch, execution: &'a Execution) -> Option<Self> {
        let ty = expr.data_type();
        if ty != LogicalType::Null && plain_scalar(&ty) != Some(&ScalarType::Utf8) { return None; }
        Some(match expr {
            Expr::Column { id, .. } => Self::Column(&batch.columns[schema.position(*id)?]),
            Expr::Parameter { index, .. } => Self::Parameter(execution.parameters.evaluator().arrow_cell(*index)?),
            Expr::Literal(Value::Utf8(text)) => Self::Literal(Some(text)),
            Expr::Literal(Value::Null) => Self::Literal(None),
            _ => return None,
        })
    }

    fn text(&self, row: usize) -> Option<Option<&'a str>> {
        let cell = match *self {
            Self::Column(column) => {
                let Some((array, field, logical, row)) = column.arrow_cell(row)? else { return Some(None); };
                if logical.is_some_and(|logical| !stable_read_layout(logical, field)) { return None; }
                ArrowCell { array, field, row }
            }
            Self::Parameter(cell) => cell,
            Self::Literal(text) => return Some(text),
        };
        let ArrowCell { array, field, row } = cell;
        if !field.metadata().is_empty() || array.data_type() != field.data_type() { return None; }
        if field.data_type() == &DataType::Null && field.is_nullable() { return Some(None); }
        let text = array.as_any().downcast_ref::<StringArray>()?;
        Some(text.is_valid(row).then(|| text.value(row)))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SplitShape { Null, EmptyList, TextList }

struct SplitRun { shape: SplitShape, end: usize, tokens: usize, payload: usize }

pub(super) fn split_text(
    expr: &Expr, schema: &Schema, batch: &ColumnBatch, execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let Expr::Call { name, args, .. } = expr else { return Ok(None); };
    let [source, Expr::Literal(Value::Utf8(delimiter))] = args.as_slice() else { return Ok(None); };
    if name != "string_to_array" || delimiter.is_empty() || batch.len == 0 { return Ok(None); }
    let Some(source) = SplitInput::bind(source, schema, batch, execution) else { return Ok(None); };
    match split_text_runs(&source, delimiter, batch.len, execution) {
        Err(error) if error.is(AnomalyKind::ResourceExhausted) => Ok(None),
        result => result,
    }
}

fn split_text_runs(
    source: &SplitInput<'_>, delimiter: &str, len: usize, execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let context = &execution.context;
    let memory = &context.memory;
    let mut run_slots = memory.reserve(0)?;
    let mut runs: Vec<SplitRun> = Vec::new();
    for row in 0..len {
        context.check_cancelled_at(row)?;
        let Some(text) = source.text(row) else { return Ok(None); };
        let shape = match text { None => SplitShape::Null, Some("") => SplitShape::EmptyList, Some(_) => SplitShape::TextList };
        if runs.last().is_none_or(|run| run.shape != shape) {
            if runs.len() == runs.capacity() {
                let capacity = runs.capacity().saturating_mul(2).max(4).min(len);
                let bytes = capacity.checked_mul(size_of::<SplitRun>()).ok_or_else(size_error)?;
                Arc::get_mut(&mut run_slots).expect("The split projection owns its run inventory").grow_to(bytes)?;
                runs.reserve_exact(capacity - runs.len());
            }
            runs.push(SplitRun { shape, end: row, tokens: 0, payload: 0 });
        }
        let run = runs.last_mut().expect("The current split run exists");
        run.end = row + 1;
        if let Some(text) = text.filter(|text| !text.is_empty()) {
            let separators = text.matches(delimiter).count();
            run.tokens = run.tokens.checked_add(separators + 1).ok_or_else(size_error)?;
            run.payload = run.payload.checked_add(text.len() - separators * delimiter.len()).ok_or_else(size_error)?;
            if i32::try_from(run.tokens).is_err() || i32::try_from(run.payload).is_err() { return Ok(None); }
        }
    }
    if let [run] = runs.as_slice() {
        return write_split_run(source, delimiter, 0..len, run, execution).map(Some);
    }
    let inventory = len.checked_mul(size_of::<(usize, usize)>())
        .and_then(|bytes| bytes.checked_add(runs.len().checked_mul(size_of::<Option<StoredVector>>())?))
        .and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
    let mut owner = memory.reserve(inventory)?;
    let mut sources = Vec::with_capacity(runs.len());
    let mut picks = Vec::with_capacity(len);
    let mut start = 0;
    for run in runs {
        let (column, custody) = write_split_run(source, delimiter, start..run.end, &run, execution)?;
        let Column::Arrow { array, field, logical } = column else { unreachable!("A split run is Arrow"); };
        let index = sources.len();
        picks.extend((0..run.end - start).map(|row| (index, row)));
        sources.push(Some((array, field, logical, Some(custody))));
        start = run.end;
    }
    let column = Column::Selected(SelectedColumn::new(sources, Arc::new(picks)));
    settle_split_charge(&column, &mut owner)?;
    Ok(Some((column, owner)))
}

fn write_split_run(
    source: &SplitInput<'_>, delimiter: &str, rows: Range<usize>, run: &SplitRun, execution: &Execution,
) -> Result<(Column, Arc<MemoryReservation>)> {
    let bound = rows.len().checked_add(run.tokens).and_then(|slots| slots.checked_add(2))
        .and_then(|slots| slots.checked_mul(size_of::<i32>())).and_then(|bytes| bytes.checked_add(run.payload))
        .and_then(|bytes| bytes.checked_add(1024)).ok_or_else(size_error)?;
    let mut owner = execution.context.memory.reserve(bound)?;
    let logical = match run.shape {
        SplitShape::Null => LogicalType::Null,
        SplitShape::EmptyList => LogicalType::list(LogicalType::Nothing),
        SplitShape::TextList => LogicalType::list(LogicalType::Scalar(ScalarType::Utf8)),
    };
    let field = Arc::new(rtdb_columnar::to_field("value", &logical)?);
    let array: ArrayRef = match field.data_type() {
        DataType::Null => Arc::new(NullArray::new(rows.len())),
        DataType::List(child) => {
            let (offsets, values): (_, ArrayRef) = if run.shape == SplitShape::EmptyList {
                (vec![0; rows.len() + 1], Arc::new(NullArray::new(0)))
            } else {
                let mut offsets = Vec::with_capacity(rows.len() + 1);
                let mut builder = StringBuilder::with_capacity(run.tokens, run.payload);
                offsets.push(0);
                for row in rows {
                    execution.context.check_cancelled_at(row)?;
                    let text = source.text(row).flatten().expect("The split run contains text");
                    for part in text.split(delimiter) { builder.append_value(part); }
                    offsets.push(builder.len() as i32);
                }
                (offsets, Arc::new(builder.finish()))
            };
            rtdb_columnar::build_list_array(Arc::clone(child), offsets.into(), &values, None)?
        }
        _ => unreachable!("A split result is a list or null"),
    };
    let column = Column::Arrow { array, field, logical: Some(logical) };
    settle_split_charge(&column, &mut owner)?;
    Ok((column, owner))
}

fn settle_split_charge(column: &Column, owner: &mut Arc<MemoryReservation>) -> Result<()> {
    let bytes = owned_projection_bytes(column)?;
    let lease = Arc::get_mut(owner).expect("The split projection owns its reservation");
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
