use std::{cmp::Ordering, io::Write, mem::size_of, sync::Arc};

use arrow_array::{
    Array, BinaryArray,
    builder::{BinaryBuilder, Int32Builder},
};
use arrow_schema::DataType;
use rtdb_columnar::{ArrowCell, MemoryReservation};
use rtdb_plan::{BinaryOp, Expr, Schema, UnaryOp};
use rtdb_types::{AnomalyKind, LogicalType, Result, ScalarType, Value};

use super::{
    Column, ColumnBatch, observed_scalar_column, owned_projection_bytes, plain_scalar, size_error,
};
use crate::context::Execution;

enum Input<'a, T> {
    Column(&'a Column),
    Parameter(ArrowCell<'a>),
    Literal(Option<T>),
}

impl<'a, T: Copy> Input<'a, T> {
    fn bind(
        expr: &'a Expr,
        schema: &Schema,
        batch: &'a ColumnBatch,
        execution: &'a Execution,
        accepts: impl FnOnce(&ScalarType) -> bool,
        literal: impl FnOnce(&'a Expr) -> Option<T>,
    ) -> Option<Self> {
        Some(match expr {
            Expr::Column { id, .. } => {
                let position = schema.position(*id)?;
                if !accepts(plain_scalar(&schema.fields()[position].data_type)?) {
                    return None;
                }
                Self::Column(&batch.columns[position])
            }
            Expr::Parameter { index, .. } => {
                if !accepts(plain_scalar(execution.parameters.types().get(*index)?)?) {
                    return None;
                }
                Self::Parameter(execution.parameters.evaluator().arrow_cell(*index)?)
            }
            Expr::Literal(Value::Null) => Self::Literal(None),
            expr => Self::Literal(Some(literal(expr)?)),
        })
    }

    fn read(
        &self,
        row: usize,
        read: impl FnOnce(ArrowCell<'a>) -> Option<Option<T>>,
    ) -> Option<Option<T>> {
        let cell = match *self {
            Self::Column(column) => {
                let Some((array, field, logical, row)) = column.arrow_cell(row)? else {
                    return Some(None);
                };
                if logical.is_some_and(|logical| !super::stable_read_layout(logical, field)) {
                    return None;
                }
                ArrowCell { array, field, row }
            }
            Self::Parameter(cell) => cell,
            Self::Literal(value) => return Some(value),
        };
        let ArrowCell { array, field, row } = cell;
        if !field.metadata().is_empty()
            || field.data_type() != array.data_type()
            || row >= array.len()
        {
            return None;
        }
        if field.data_type() == &DataType::Null && field.is_nullable() {
            return Some(None);
        }
        read(cell)
    }
}

type BinaryInput<'a> = Input<'a, &'a [u8]>;

impl<'a> Input<'a, &'a [u8]> {
    fn bind_binary(
        expr: &'a Expr,
        schema: &Schema,
        batch: &'a ColumnBatch,
        execution: &'a Execution,
    ) -> Option<Self> {
        Self::bind(
            expr,
            schema,
            batch,
            execution,
            |scalar| *scalar == ScalarType::Binary,
            |expr| match expr {
                Expr::Literal(Value::Binary(bytes)) => Some(bytes.as_slice()),
                _ => None,
            },
        )
    }

    fn binary(&self, row: usize) -> Option<Option<&'a [u8]>> {
        self.read(row, |cell| {
            let binary = cell.array.as_any().downcast_ref::<BinaryArray>()?;
            Some(binary.is_valid(cell.row).then(|| binary.value(cell.row)))
        })
    }
}

type SignedInput<'a> = Input<'a, i64>;

impl<'a> Input<'a, i64> {
    fn bind_signed(
        expr: &'a Expr,
        schema: &Schema,
        batch: &'a ColumnBatch,
        execution: &'a Execution,
    ) -> Option<Self> {
        Self::bind(
            expr,
            schema,
            batch,
            execution,
            |scalar| {
                matches!(
                    scalar,
                    ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64
                )
            },
            signed_literal,
        )
    }

    fn signed(&self, row: usize) -> Option<Option<i64>> {
        self.read(row, |cell| {
            if !matches!(
                cell.field.data_type(),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            ) {
                return None;
            }
            if cell.array.is_null(cell.row) {
                Some(None)
            } else {
                Some(Some(cell.signed_integer()?))
            }
        })
    }
}

pub(super) fn column(
    expr: &Expr,
    output: &LogicalType,
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let result = match (plain_scalar(output), expr) {
        (
            Some(ScalarType::Binary),
            Expr::Binary {
                op: BinaryOp::Concat,
                left,
                right,
            },
        ) => concat(left, right, schema, batch, execution),
        (Some(ScalarType::Binary), Expr::Call { name, args, .. })
            if name.eq_ignore_ascii_case("substring") =>
        {
            substring(args, schema, batch, execution)
        }
        (Some(ScalarType::Binary), Expr::Call { name, args, .. })
            if name.eq_ignore_ascii_case("overlay") =>
        {
            overlay(args, schema, batch, execution)
        }
        (Some(ScalarType::Binary), Expr::Call { name, args, .. })
            if name.eq_ignore_ascii_case("nullif") =>
        {
            nullif(args, schema, batch, execution)
        }
        (Some(ScalarType::Binary), Expr::Call { name, args, .. })
            if name.eq_ignore_ascii_case("least") =>
        {
            extremum(args, Ordering::Less, schema, batch, execution)
        }
        (Some(ScalarType::Binary), Expr::Call { name, args, .. })
            if name.eq_ignore_ascii_case("greatest") =>
        {
            extremum(args, Ordering::Greater, schema, batch, execution)
        }
        (Some(ScalarType::Binary), Expr::Call { name, args, .. })
            if name.eq_ignore_ascii_case("entity_sort_key") =>
        {
            entity_sort_key(args, schema, batch, execution)
        }
        (Some(ScalarType::Binary), Expr::Call { name, args, .. }) => {
            trim(name, args, schema, batch, execution)
        }
        (Some(ScalarType::I32), Expr::Call { name, args, .. }) => {
            octets(name, args, schema, batch, execution)
        }
        _ => Ok(None),
    };
    match result {
        Err(error) if error.is(AnomalyKind::ResourceExhausted) => Ok(None),
        result => result,
    }
}

fn entity_sort_key(
    args: &[Expr],
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let [Expr::Column { id, .. }] = args else {
        return Ok(None);
    };
    let Some(position) = schema.position(*id) else {
        return Ok(None);
    };
    let input = &batch.columns[position];
    let read = |row| {
        if input.arrow_cell(row)?.is_none() {
            return Some(None);
        }
        let cell = input.expression_cell(row)?.physical().ok()?;
        if cell.is_null().ok()? {
            return Some(None);
        }
        let storage = cell.field.data_type();
        if !storage.is_integer() && !matches!(storage, DataType::Utf8 | DataType::FixedSizeBinary(16)) {
            return None;
        }
        let logical = rtdb_columnar::from_field(cell.field).ok()?;
        use ScalarType as S;
        if !matches!(
            plain_scalar(&logical),
            Some(
                S::Utf8
                    | S::Keyword
                    | S::Uuid
                    | S::I8
                    | S::I16
                    | S::I32
                    | S::I64
                    | S::U8
                    | S::U16
                    | S::U32
                    | S::U64
            )
        ) {
            return None;
        }
        rtdb_columnar::read_execution_entity_id_as(Some(&logical), cell.field, cell.array, cell.row)
            .ok()
    };
    let len = batch.len;
    let context = &execution.context;
    let mut payload = 0usize;
    for row in 0..len {
        context.check_cancelled_at(row)?;
        let Some(id) = read(row) else {
            return Ok(None);
        };
        if let Some(id) = id {
            let mut integer = [0; 8];
            let (_, bytes) = id.handle_parts(&mut integer);
            let Some(total) = payload
                .checked_add(1)
                .and_then(|total| total.checked_add(bytes.len()))
                .filter(|total| i32::try_from(*total).is_ok())
            else {
                return Ok(None);
            };
            payload = total;
        }
    }
    let bound = len
        .checked_add(1)
        .and_then(|offsets| offsets.checked_mul(size_of::<i32>()))
        .and_then(|bytes| bytes.checked_add(payload))
        .and_then(|bytes| bytes.checked_add(len.div_ceil(8) + 1024))
        .ok_or_else(size_error)?;
    let charge = context.memory.reserve(bound)?;
    let mut builder = BinaryBuilder::with_capacity(len, payload);
    for row in 0..len {
        context.check_cancelled_at(row)?;
        let Some(id) = read(row) else {
            return Ok(None);
        };
        if let Some(id) = id {
            let mut integer = [0; 8];
            let (tag, bytes) = id.handle_parts(&mut integer);
            builder
                .write_all(&[tag])
                .expect("Writing into a BinaryBuilder only appends bytes");
            builder.append_value(bytes);
        } else {
            builder.append_null();
        }
    }
    finish(builder.finish(), charge).map(Some)
}

fn concat(
    left: &Expr,
    right: &Expr,
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let bind = |expr| BinaryInput::bind_binary(expr, schema, batch, execution);
    let (Some(left), Some(right)) = (bind(left), bind(right)) else {
        return Ok(None);
    };
    project(batch, execution, |row| {
        let Some(left) = left.binary(row)? else {
            return Some(Ok(None));
        };
        Some(Ok(right.binary(row)?.map(|right| [left, right])))
    })
}

fn substring(
    args: &[Expr],
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let (input, position, length) = match args {
        [input, position] => (input, position, None),
        [input, position, length] => (input, position, Some(length)),
        _ => return Ok(None),
    };
    let bind = |expr| SignedInput::bind_signed(expr, schema, batch, execution);
    let Some(position) = bind(position) else {
        return Ok(None);
    };
    let length = match length {
        Some(length) => match bind(length) {
            Some(length) => Some(length),
            None => return Ok(None),
        },
        None => None,
    };
    let Some(input) = BinaryInput::bind_binary(input, schema, batch, execution) else {
        return Ok(None);
    };
    project(batch, execution, |row| {
        let Some(source) = input.binary(row)? else {
            return Some(Ok(None));
        };
        let Some(position) = position.signed(row)? else {
            return Some(Ok(None));
        };
        let length = if let Some(length) = &length {
            let Some(length) = length.signed(row)? else {
                return Some(Ok(None));
            };
            Some(length)
        } else {
            None
        };
        Some(
            rtdb_expr::kernels::binary_substring(source, position, length)
                .map(|slice| Some([slice])),
        )
    })
}

fn overlay(
    args: &[Expr],
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let [source, replacement, start, count] = args else {
        return Ok(None);
    };
    let bind = |expr| BinaryInput::bind_binary(expr, schema, batch, execution);
    let (Some(source), Some(replacement_input), Some(start)) = (
        bind(source),
        bind(replacement),
        SignedInput::bind_signed(start, schema, batch, execution),
    ) else {
        return Ok(None);
    };
    let length = SignedInput::bind_signed(count, schema, batch, execution);
    if length.is_none()
        && !matches!(count, Expr::Call { name, args, .. }
        if (name.eq_ignore_ascii_case("default_overlay_length") || name.eq_ignore_ascii_case("default-overlay-length"))
            && matches!(args.as_slice(), [operand] if operand == replacement))
    {
        return Ok(None);
    }
    project(batch, execution, |row| {
        let Some(source) = source.binary(row)? else {
            return Some(Ok(None));
        };
        let Some(replacement) = replacement_input.binary(row)? else {
            return Some(Ok(None));
        };
        let Some(start) = start.signed(row)? else {
            return Some(Ok(None));
        };
        let length = if let Some(length) = &length {
            let Some(length) = length.signed(row)? else {
                return Some(Ok(None));
            };
            Ok(length)
        } else {
            rtdb_expr::kernels::binary_octet_length(replacement).map(i64::from)
        };
        Some(
            length
                .and_then(|length| rtdb_expr::kernels::binary_overlay_edges(source, start, length))
                .map(|[prefix, suffix]| Some([prefix, replacement, suffix])),
        )
    })
}

fn trim(
    name: &str,
    args: &[Expr],
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let (leading, trailing) = if name.eq_ignore_ascii_case("trim") {
        (true, true)
    } else if name.eq_ignore_ascii_case("trim_leading") || name.eq_ignore_ascii_case("trim-leading")
    {
        (true, false)
    } else if name.eq_ignore_ascii_case("trim_trailing")
        || name.eq_ignore_ascii_case("trim-trailing")
    {
        (false, true)
    } else {
        return Ok(None);
    };
    let bind = |expr| BinaryInput::bind_binary(expr, schema, batch, execution);
    let (source, token) = match args {
        [source] => (source, Some(BinaryInput::Literal(Some(&b" "[..])))),
        [source, token] => (source, bind(token)),
        _ => return Ok(None),
    };
    let (Some(source), Some(token)) = (bind(source), token) else {
        return Ok(None);
    };
    project(batch, execution, |row| {
        let Some(source) = source.binary(row)? else {
            return Some(Ok(None));
        };
        let Some(token) = token.binary(row)? else {
            return Some(Ok(None));
        };
        Some(
            rtdb_expr::kernels::binary_trim(source, token, leading, trailing)
                .map(|slice| Some([slice])),
        )
    })
}

fn nullif(
    args: &[Expr],
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let [left, right] = args else {
        return Ok(None);
    };
    let bind = |expr| BinaryInput::bind_binary(expr, schema, batch, execution);
    let (Some(left), Some(right)) = (bind(left), bind(right)) else {
        return Ok(None);
    };
    project(batch, execution, |row| {
        let Some(left) = left.binary(row)? else {
            return Some(Ok(None));
        };
        Some(Ok((right.binary(row)? != Some(left)).then_some([left])))
    })
}

fn extremum(
    args: &[Expr],
    order: Ordering,
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let Some((first, rest)) = args.split_first() else {
        return Ok(None);
    };
    let bind = |expr| BinaryInput::bind_binary(expr, schema, batch, execution);
    let Some(first) = bind(first) else {
        return Ok(None);
    };
    let bytes = rest
        .len()
        .checked_mul(size_of::<BinaryInput<'_>>())
        .ok_or_else(size_error)?;
    let _inputs = execution.context.memory.reserve(bytes)?;
    let mut inputs = Vec::with_capacity(rest.len());
    for arg in rest {
        let Some(input) = bind(arg) else {
            return Ok(None);
        };
        inputs.push(input);
    }
    project(batch, execution, |row| {
        Some(Ok(
            extreme_at(&first, &inputs, row, order)?.map(|slice| [slice])
        ))
    })
}

fn extreme_at<'a>(
    first: &BinaryInput<'a>,
    rest: &[BinaryInput<'a>],
    row: usize,
    order: Ordering,
) -> Option<Option<&'a [u8]>> {
    let Some(mut best) = first.binary(row)? else {
        return Some(None);
    };
    for input in rest {
        let Some(next) = input.binary(row)? else {
            return Some(None);
        };
        if best.cmp(next) != order {
            best = next;
        }
    }
    Some(Some(best))
}

fn project<'a, const N: usize>(
    batch: &ColumnBatch,
    execution: &Execution,
    mut read: impl FnMut(usize) -> Option<Result<Option<[&'a [u8]; N]>>>,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let len = batch.len;
    let context = &execution.context;
    let slots = len
        .checked_mul(size_of::<Option<[&[u8]; N]>>())
        .and_then(|bytes| bytes.checked_add(512))
        .ok_or_else(size_error)?;
    let _slots = context.memory.reserve(slots)?;
    let mut rows = Vec::with_capacity(len);
    let mut payload = 0usize;
    for row in 0..len {
        context.check_cancelled_at(row)?;
        let Some(value) = read(row).transpose()? else {
            return Ok(None);
        };
        if let Some(spans) = &value {
            for span in spans {
                payload = payload.checked_add(span.len()).ok_or_else(size_error)?;
            }
            if i32::try_from(payload).is_err() {
                return Ok(None);
            }
        }
        rows.push(value);
    }

    let bound = len
        .checked_add(1)
        .and_then(|offsets| offsets.checked_mul(size_of::<i32>()))
        .and_then(|bytes| bytes.checked_add(payload))
        .and_then(|bytes| bytes.checked_add(len.div_ceil(8) + 1024))
        .ok_or_else(size_error)?;
    let charge = context.memory.reserve(bound)?;
    let mut builder = BinaryBuilder::with_capacity(len, payload);
    for (row, value) in rows.into_iter().enumerate() {
        context.check_cancelled_at(row)?;
        if let Some(spans) = value {
            for span in spans {
                builder
                    .write_all(span)
                    .expect("Writing into a BinaryBuilder only appends bytes");
            }
            // The empty append records the offset after all spans of this cell.
            builder.append_value([]);
        } else {
            builder.append_null();
        }
    }
    finish(builder.finish(), charge).map(Some)
}

fn octets(
    name: &str,
    args: &[Expr],
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let (input, haystack) = match args {
        [input]
            if name.eq_ignore_ascii_case("octet_length")
                || name.eq_ignore_ascii_case("octet-length") =>
        {
            (input, None)
        }
        [needle, haystack]
            if name.eq_ignore_ascii_case("octet_position")
                || name.eq_ignore_ascii_case("octet-position") =>
        {
            let Some(haystack) = BinaryInput::bind_binary(haystack, schema, batch, execution)
            else {
                return Ok(None);
            };
            (needle, Some(haystack))
        }
        _ => return Ok(None),
    };
    let Some(input) = BinaryInput::bind_binary(input, schema, batch, execution) else {
        return Ok(None);
    };
    let len = batch.len;
    let context = &execution.context;
    let bound = len
        .checked_mul(size_of::<i32>())
        .and_then(|bytes| bytes.checked_add(len.div_ceil(8) + 1024))
        .ok_or_else(size_error)?;
    let charge = context.memory.reserve(bound)?;
    let mut builder = Int32Builder::with_capacity(len);
    for row in 0..len {
        context.check_cancelled_at(row)?;
        let Some(cell) = input.binary(row) else {
            return Ok(None);
        };
        let Some(cell) = cell else {
            builder.append_null();
            continue;
        };
        let result = if let Some(haystack) = &haystack {
            let Some(haystack) = haystack.binary(row) else {
                return Ok(None);
            };
            haystack
                .map(|haystack| rtdb_expr::kernels::binary_octet_position(cell, haystack))
                .transpose()?
        } else {
            Some(rtdb_expr::kernels::binary_octet_length(cell)?)
        };
        builder.append_option(result);
    }
    finish(builder.finish(), charge).map(Some)
}

fn finish(
    array: impl Array + 'static,
    mut charge: Arc<MemoryReservation>,
) -> Result<(Column, Arc<MemoryReservation>)> {
    let column = observed_scalar_column(array)?;
    let bytes = owned_projection_bytes(&column)?;
    let reservation = Arc::get_mut(&mut charge).expect("The Binary writer owns its reservation");
    if bytes <= reservation.bytes() {
        reservation.shrink_to(bytes)?;
    } else {
        reservation.grow_to(bytes)?;
    }
    Ok((column, charge))
}

fn signed_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(Value::I8(value)) => Some(i64::from(*value)),
        Expr::Literal(Value::I16(value)) => Some(i64::from(*value)),
        Expr::Literal(Value::I32(value)) => Some(i64::from(*value)),
        Expr::Literal(Value::I64(value)) => Some(*value),
        Expr::Unary {
            op: UnaryOp::Negate,
            expr,
        } => match expr.as_ref() {
            Expr::Literal(Value::I64(value)) => value.checked_neg(),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{BinaryArray, Int64Array};
    use arrow_schema::{DataType, Field};
    use rtdb_columnar::{ArrowCell, EXTENSION_NAME_KEY};

    use super::{BinaryInput, Ordering, SignedInput, extreme_at};

    #[test]
    fn binary_projection_declines_tagged_payload_and_control_cells() {
        let metadata = [(EXTENSION_NAME_KEY.into(), "tagged".into())];
        let field =
            Field::new("payload", DataType::Binary, false).with_metadata(metadata.clone().into());
        let array = BinaryArray::from_vec(vec![b"a".as_slice()]);
        let input = BinaryInput::Parameter(ArrowCell {
            field: &field,
            array: &array,
            row: 0,
        });
        assert!(input.binary(0).is_none());
        let field = Field::new("position", DataType::Int64, false).with_metadata(metadata.into());
        let array = Int64Array::from(vec![1]);
        let input = SignedInput::Parameter(ArrowCell {
            field: &field,
            array: &array,
            row: 0,
        });
        assert!(input.signed(0).is_none());
    }

    #[test]
    fn binary_extrema_borrow_the_later_tied_operand() {
        let left = vec![0x80];
        let right = left.clone();
        let first = BinaryInput::Literal(Some(left.as_slice()));
        let rest = [BinaryInput::Literal(Some(right.as_slice()))];
        for order in [Ordering::Less, Ordering::Greater] {
            let selected = extreme_at(&first, &rest, 0, order).unwrap().unwrap();
            assert!(std::ptr::eq(selected, right.as_slice()));
        }
    }
}
