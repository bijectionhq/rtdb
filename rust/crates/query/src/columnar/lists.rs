use std::sync::Arc;

use arrow_schema::{DataType, Field, UnionMode};
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{BinaryOp, Expr, Schema};
use rtdb_types::{Anomaly, AnomalyKind, ListBuilder, LogicalType, Result, ScalarType, Value};

use super::{BatchRow, Column, ColumnBatch, owned_projection_bytes, size_error, stable_read_layout};
use crate::context::Execution;

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
