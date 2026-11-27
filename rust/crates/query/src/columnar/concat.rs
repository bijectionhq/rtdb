use std::{fmt::Write, sync::Arc};

use arrow_array::{Array, ArrayRef, NullArray, StringArray, builder::StringBuilder};
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation};
use rtdb_plan::{BinaryOp, Expr, Schema};
use rtdb_types::{LogicalType, Result, ScalarType, Value};

use super::{Column, ColumnBatch, SelectedRows, owned_projection_bytes, plain_scalar, size_error};
use crate::{QueryContext, scalar_key::ScalarColumn};

#[derive(Clone, Copy)]
enum TextSource<'a> {
    Text(&'a StringArray),
    Null(usize),
    Encoded(&'a dyn Array, &'a Field),
}

fn text_layout(field: &Field) -> bool {
    field.metadata().is_empty() && match field.data_type() {
        DataType::Utf8 => true,
        DataType::Null => field.is_nullable(),
        DataType::RunEndEncoded(_, values) => text_layout(values),
        DataType::Union(fields, UnionMode::Dense) => fields.iter().all(|(_, field)| text_layout(field)),
        _ => false,
    }
}

impl<'a> TextSource<'a> {
    fn bind(array: &'a dyn Array, field: &'a Field, logical: Option<&LogicalType>) -> Option<Self> {
        if matches!(field.data_type(), DataType::RunEndEncoded(..) | DataType::Union(..)) {
            if field.data_type() != array.data_type() || !text_layout(field) { return None; }
            let actual = rtdb_columnar::from_field(field).ok()?;
            return logical.is_none_or(|logical| *logical == actual).then_some(Self::Encoded(array, field));
        }
        let inferred;
        let logical = match logical {
            Some(logical) => logical,
            None => {
                inferred = match field.data_type() {
                    DataType::Utf8 => LogicalType::Scalar(ScalarType::Utf8),
                    DataType::Null => LogicalType::Null,
                    _ => return None,
                };
                &inferred
            }
        };
        match ScalarColumn::bind(array, field, Some(logical))? {
            ScalarColumn::Text(array) => Some(Self::Text(array)),
            ScalarColumn::Null => Some(Self::Null(array.len())),
            _ => None,
        }
    }

    fn get(self, row: usize) -> Option<Option<&'a str>> {
        match self {
            Self::Text(array) if row < array.len() => Some(array.is_valid(row).then(|| array.value(row))),
            Self::Null(len) if row < len => Some(None),
            Self::Encoded(array, field) => {
                let cell = ArrowCell { field, array, row }.physical().ok()?;
                if cell.field.data_type() == &DataType::Null || cell.array.is_null(cell.row) { return Some(None); }
                cell.utf8().map(Some)
            }
            _ => None,
        }
    }
}

enum TextInput<'a> {
    Literal(&'a str),
    Direct(TextSource<'a>),
    Selected {
        rows: &'a SelectedRows,
        sources: Vec<Option<TextSource<'a>>>,
        _reservation: Arc<MemoryReservation>,
    },
}

impl<'a> TextInput<'a> {
    fn bind(expr: &'a Expr, schema: &Schema, batch: &'a ColumnBatch, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        let column = match expr {
            Expr::Column { id, .. } => {
                let Some(position) = schema.position(*id) else { return Ok(None); };
                let logical = match &schema.fields()[position].data_type {
                    LogicalType::RunEndEncoded { value, .. } => value.as_ref(),
                    logical => logical,
                };
                if plain_scalar(logical) != Some(&ScalarType::Utf8) { return Ok(None); }
                &batch.columns[position]
            }
            Expr::Literal(Value::Utf8(text)) => return Ok(Some(Self::Literal(text))),
            _ => return Ok(None),
        };
        Ok(match column {
            Column::Arrow { array, field, logical } => TextSource::bind(array.as_ref(), field, logical.as_ref()).map(Self::Direct),
            Column::Selected(column) => {
                let bytes = column.sources.len().checked_mul(size_of::<Option<TextSource<'_>>>()).ok_or_else(size_error)?;
                let reservation = match memory.reserve(bytes) {
                    Ok(reservation) => reservation,
                    Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => return Ok(None),
                    Err(error) => return Err(error),
                };
                let mut sources = Vec::with_capacity(column.sources.len());
                for source in column.sources.iter() {
                    let bound = match source {
                        Some((array, field, logical, _)) => {
                            let Some(text) = TextSource::bind(array.as_ref(), field, logical.as_ref()) else { return Ok(None); };
                            Some(text)
                        }
                        None => None,
                    };
                    sources.push(bound);
                }
                Some(Self::Selected { rows: &column.cells, sources, _reservation: reservation })
            }
            Column::Values(_) => None,
        })
    }

    fn get(&self, row: usize) -> Option<Option<&'a str>> {
        match self {
            Self::Literal(text) => Some(Some(text)),
            Self::Direct(source) => source.get(row),
            Self::Selected { rows, sources, .. } => {
                let (source, offset) = rows.get(row)?;
                match sources.get(source)? {
                    Some(source) => source.get(offset),
                    None => Some(None),
                }
            }
        }
    }
}

pub(super) fn utf8_column(
    expression: &Expr, schema: &Schema, batch: &ColumnBatch, context: &QueryContext,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let Expr::Binary { op: BinaryOp::Concat, left, right } = expression else { return Ok(None); };
    if batch.len == 0 { return Ok(None); }
    let (Some(left), Some(right)) = (TextInput::bind(left, schema, batch, &context.memory)?, TextInput::bind(right, schema, batch, &context.memory)?)
    else { return Ok(None); };
    if matches!((&left, &right), (TextInput::Literal(_), TextInput::Literal(_))) { return Ok(None); }
    let pair = |row| {
        let Some(left) = left.get(row)? else { return Some(None); };
        Some(right.get(row)?.map(|right| (left, right)))
    };

    let mut remaining_bytes = i32::MAX as usize;
    let mut nulls = 0;
    for row in 0..batch.len {
        context.check_cancelled_at(row)?;
        match pair(row) {
            Some(Some((left, right))) => {
                let Some(remaining) = remaining_bytes.checked_sub(left.len()).and_then(|bytes| bytes.checked_sub(right.len()))
                else { return Ok(None); };
                remaining_bytes = remaining;
            }
            Some(None) => nulls += 1,
            None => return Ok(None),
        }
    }

    let (array, logical): (ArrayRef, LogicalType);
    let mut owner;
    if nulls == batch.len {
        owner = context.memory.reserve(512)?;
        array = Arc::new(NullArray::new(batch.len));
        logical = LogicalType::Null;
    } else {
        let value_bytes = i32::MAX as usize - remaining_bytes;
        let validity_bytes = if nulls == 0 { 0 } else { batch.len.div_ceil(8) };
        let bytes = batch.len.checked_add(1).and_then(|offsets| offsets.checked_mul(size_of::<i32>()))
            .and_then(|offsets| offsets.checked_add(value_bytes))
            .and_then(|bytes| bytes.checked_add(validity_bytes))
            .and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
        owner = context.memory.reserve(bytes)?;
        let mut builder = StringBuilder::with_capacity(batch.len, value_bytes);
        for row in 0..batch.len {
            context.check_cancelled_at(row)?;
            if let Some((left, right)) = pair(row).expect("The page's text cells were sized") {
                builder.write_str(left).expect("Writing into a StringBuilder only appends bytes");
                builder.append_value(right);
            } else {
                builder.append_null();
            }
        }
        array = Arc::new(builder.finish());
        let text = LogicalType::Scalar(ScalarType::Utf8);
        logical = if nulls == 0 { text } else { text.maybe() };
    }

    let field = Arc::new(rtdb_columnar::to_field("value", &logical)?);
    let column = Column::Arrow { array, field, logical: Some(logical) };
    Arc::get_mut(&mut owner).expect("Concatenation owns its reservation").shrink_to(owned_projection_bytes(&column)?)?;
    Ok(Some((column, owner)))
}
