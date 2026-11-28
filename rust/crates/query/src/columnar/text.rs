use std::{fmt::Write, mem::size_of, sync::Arc};

use arrow_array::{
    Array, NullArray, StringArray,
    builder::{ListBuilder, StringBuilder},
};
use arrow_schema::DataType;
use rtdb_columnar::MemoryReservation;
use rtdb_expr::kernels::{
    identifier_needs_quotes, literal_replace_length, text_overlay_edges, text_substring, text_trim,
};
use rtdb_plan::{Expr, Schema};
use rtdb_types::{AnomalyKind, ListKind, LogicalType, Result, ScalarType, Value};

use super::{
    Column, ColumnBatch,
    binary::{Input, SignedInput},
    observed_scalar_column, owned_projection_bytes, plain_scalar, size_error,
};
use crate::{QueryContext, context::Execution};

type TextInput<'a> = Input<'a, &'a str>;

impl<'a> Input<'a, &'a str> {
    fn bind_text(
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
            |logical| {
                *logical == LogicalType::Null || plain_scalar(logical) == Some(&ScalarType::Utf8)
            },
            |expr| match expr {
                Expr::Literal(Value::Utf8(text)) => Some(text.as_str()),
                _ => None,
            },
        )
    }

    fn text(&self, row: usize) -> Option<Option<&'a str>> {
        self.read(row, |cell| {
            let array = cell.array.as_any().downcast_ref::<StringArray>()?;
            Some(array.is_valid(cell.row).then(|| array.value(cell.row)))
        })
    }
}

enum Spans<'a> {
    Slice(&'a str),
    QuotedIdentifier(&'a str),
    Overlay([&'a str; 3]),
    Replace {
        source: &'a str,
        target: &'a str,
        replacement: &'a str,
    },
    Reverse(&'a str),
    Join {
        inputs: &'a [TextInput<'a>],
        row: usize,
    },
}

impl Spans<'_> {
    fn len(&self) -> Option<usize> {
        match *self {
            Self::Slice(text) | Self::Reverse(text) => Some(text.len()),
            Self::QuotedIdentifier(text) => text
                .len()
                .checked_add(text.bytes().filter(|byte| *byte == b'"').count())?
                .checked_add(2),
            Self::Overlay([prefix, replacement, suffix]) => prefix
                .len()
                .checked_add(replacement.len())?
                .checked_add(suffix.len()),
            Self::Replace {
                source,
                target,
                replacement,
            } => literal_replace_length(source, target, replacement).ok(),
            Self::Join { inputs, row } => inputs.iter().try_fold(0usize, |len, input| {
                len.checked_add(input.text(row)?.unwrap_or_default().len())
            }),
        }
    }

    fn append(self, builder: &mut StringBuilder) -> Option<()> {
        let write = |builder: &mut StringBuilder, text: &str| {
            builder
                .write_str(text)
                .expect("Writing into a StringBuilder only appends bytes");
        };
        match self {
            Self::Slice(text) => write(builder, text),
            Self::QuotedIdentifier(text) => {
                write(builder, "\"");
                for part in text.split_inclusive('"') {
                    write(builder, part);
                    if part.ends_with('"') {
                        write(builder, "\"");
                    }
                }
                write(builder, "\"");
            }
            Self::Overlay(parts) => {
                for part in parts {
                    write(builder, part);
                }
            }
            Self::Replace {
                source,
                target,
                replacement,
            } => {
                let mut start = 0;
                for (position, found) in source.match_indices(target) {
                    write(builder, &source[start..position]);
                    write(builder, replacement);
                    start = position + found.len();
                }
                write(builder, &source[start..]);
            }
            Self::Reverse(source) => {
                let mut end = source.len();
                for (start, _) in source.char_indices().rev() {
                    write(builder, &source[start..end]);
                    end = start;
                }
            }
            Self::Join { inputs, row } => {
                for input in inputs {
                    write(builder, input.text(row)?.unwrap_or_default());
                }
            }
        }
        builder.append_value("");
        Some(())
    }
}

/// Projects a prepared expression, with declined layouts, sizes and reservations retried by the row evaluator.
pub(super) fn column(
    expr: &Expr,
    output: &LogicalType,
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    if batch.len == 0 {
        return Ok(None);
    }
    let result = match expr {
        Expr::Call { name, args, .. }
            if name.eq_ignore_ascii_case("parse_ident")
                || name.eq_ignore_ascii_case("parse-ident") =>
        {
            parse_ident(args, output, schema, batch, execution)
        }
        _ if plain_scalar(output) != Some(&ScalarType::Utf8) => return Ok(None),
        Expr::Call { name, args, .. } if name.eq_ignore_ascii_case("str") => {
            concatenate(args, Some, schema, batch, execution)
        }
        Expr::Call { name, args, .. } if name.eq_ignore_ascii_case("concat") => {
            concatenate(args, sql_concat_input, schema, batch, execution)
        }
        Expr::Coalesce(_) => match sql_concat_input(expr) {
            Some(input) => concatenate(std::slice::from_ref(input), Some, schema, batch, execution),
            None => Ok(None),
        },
        Expr::Call { name, args, .. } => transform(name, args, schema, batch, execution),
        _ => Ok(None),
    };
    match result {
        Err(error) if error.is(AnomalyKind::ResourceExhausted) => Ok(None),
        result => result,
    }
}

fn parse_ident(
    args: &[Expr],
    output: &LogicalType,
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let [source] = args else {
        return Ok(None);
    };
    let output = match output {
        LogicalType::Maybe(inner) => inner.as_ref(),
        output => output,
    };
    if !matches!(output, LogicalType::List { kind: ListKind::List, element }
        if **element == LogicalType::Scalar(ScalarType::Utf8))
    {
        return Ok(None);
    }
    let Some(source) = TextInput::bind_text(source, schema, batch, execution) else {
        return Ok(None);
    };

    let context = &execution.context;
    let mut present = 0;
    let mut items = 0usize;
    let mut payload = 0usize;
    for row in 0..batch.len {
        context.check_cancelled_at(row)?;
        let Some(cell) = source.text(row) else {
            return Ok(None);
        };
        let Some(cell) = cell else { continue };
        if !cell.is_ascii() {
            return Ok(None);
        }
        for (index, part) in cell.split('.').enumerate() {
            context.check_cancelled_at(index)?;
            if part.is_empty() || part.starts_with('"') {
                return Ok(None);
            }
            items = items.checked_add(1).ok_or_else(size_error)?;
            payload = payload.checked_add(part.len()).ok_or_else(size_error)?;
        }
        if i32::try_from(items).is_err() || i32::try_from(payload).is_err() {
            return Ok(None);
        }
        present += 1;
    }

    if present == 0 {
        let charge = context.memory.reserve(1024)?;
        return finish(observed_scalar_column(NullArray::new(batch.len))?, charge).map(Some);
    }
    let bound = batch
        .len
        .checked_add(items)
        .and_then(|offsets| offsets.checked_add(2))
        .and_then(|offsets| offsets.checked_mul(size_of::<i32>()))
        .and_then(|bytes| bytes.checked_add(payload))
        .and_then(|bytes| bytes.checked_add(batch.len.div_ceil(8) + 1024))
        .ok_or_else(size_error)?;
    let charge = context.memory.reserve(bound)?;
    let logical = if present == batch.len {
        output.clone()
    } else {
        output.clone().maybe()
    };
    let field = Arc::new(rtdb_columnar::to_field("value", &logical)?);
    let DataType::List(item) = field.data_type() else {
        unreachable!("PARSE_IDENT produces a list of text");
    };
    let strings = StringBuilder::with_capacity(items, payload);
    let mut builder = ListBuilder::with_capacity(strings, batch.len).with_field(Arc::clone(item));
    for row in 0..batch.len {
        context.check_cancelled_at(row)?;
        match source.text(row) {
            Some(Some(cell)) => {
                for (index, part) in cell.split('.').enumerate() {
                    context.check_cancelled_at(index)?;
                    append_ascii_lowercase(builder.values(), part);
                }
                builder.append(true);
            }
            Some(None) => builder.append(false),
            None => return Ok(None),
        }
    }
    let column = Column::Arrow {
        array: Arc::new(builder.finish()),
        field,
        logical: Some(logical),
    };
    finish(column, charge).map(Some)
}

fn append_ascii_lowercase(builder: &mut StringBuilder, part: &str) {
    if part.bytes().any(|byte| byte.is_ascii_uppercase()) {
        for byte in part.bytes() {
            builder
                .write_char(char::from(byte.to_ascii_lowercase()))
                .expect("Writing into a StringBuilder only appends bytes");
        }
        builder.append_value("");
    } else {
        builder.append_value(part);
    }
}

fn sql_concat_input(expr: &Expr) -> Option<&Expr> {
    let Expr::Coalesce(parts) = expr else {
        return None;
    };
    let [
        Expr::Call { name, args, .. },
        Expr::Literal(Value::Utf8(default)),
    ] = parts.as_slice()
    else {
        return None;
    };
    if !name.eq_ignore_ascii_case("str") || !default.is_empty() {
        return None;
    }
    let [input] = args.as_slice() else {
        return None;
    };
    Some(input)
}

fn concatenate<'a>(
    args: &'a [Expr],
    input: impl Fn(&'a Expr) -> Option<&'a Expr>,
    schema: &Schema,
    batch: &'a ColumnBatch,
    execution: &'a Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let bytes = args
        .len()
        .checked_mul(size_of::<TextInput<'_>>())
        .ok_or_else(size_error)?;
    let _inputs = execution.context.memory.reserve(bytes)?;
    let mut inputs = Vec::with_capacity(args.len());
    for arg in args {
        let Some(arg) =
            input(arg).and_then(|arg| TextInput::bind_text(arg, schema, batch, execution))
        else {
            return Ok(None);
        };
        inputs.push(arg);
    }
    project(batch.len, execution, |row| {
        Some(Some(Spans::Join {
            inputs: &inputs,
            row,
        }))
    })
}

fn transform(
    name: &str,
    args: &[Expr],
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let bind = |expr| TextInput::bind_text(expr, schema, batch, execution);
    match args {
        [source, position] | [source, position, _] if name.eq_ignore_ascii_case("substring") => {
            let signed = |expr| SignedInput::bind_signed(expr, schema, batch, execution);
            let (Some(source), Some(position)) = (bind(source), signed(position)) else {
                return Ok(None);
            };
            let length = if let Some(length) = args.get(2) {
                let Some(length) = signed(length) else {
                    return Ok(None);
                };
                Some(length)
            } else {
                None
            };
            project(batch.len, execution, |row| {
                let source = source.text(row)?;
                let position = position.signed(row)?;
                let length = match &length {
                    Some(length) => length.signed(row)?.map(Some),
                    None => Some(None),
                };
                let (Some(source), Some(position), Some(length)) = (source, position, length)
                else {
                    return Some(None);
                };
                Some(Some(Spans::Slice(
                    text_substring(source, position, length).ok()?,
                )))
            })
        }
        [source, replacement, start, length] if name.eq_ignore_ascii_case("overlay") => {
            let (Some(source), Some(placing), Some(start)) = (
                bind(source),
                bind(replacement),
                SignedInput::bind_signed(start, schema, batch, execution),
            ) else {
                return Ok(None);
            };
            let length_input = SignedInput::bind_signed(length, schema, batch, execution);
            if length_input.is_none()
                && !matches!(length, Expr::Call { name, args, .. }
                    if (name.eq_ignore_ascii_case("default_overlay_length") || name.eq_ignore_ascii_case("default-overlay-length"))
                        && matches!(args.as_slice(), [input] if input == replacement))
            {
                return Ok(None);
            }
            project(batch.len, execution, |row| {
                let source = source.text(row)?;
                let placing = placing.text(row)?;
                let start = start.signed(row)?;
                let length = match &length_input {
                    Some(length) => length.signed(row)?,
                    None => placing
                        .map(|placing| i32::try_from(placing.chars().count()))
                        .transpose()
                        .ok()?
                        .map(i64::from),
                };
                let (Some(source), Some(placing), Some(start), Some(length)) =
                    (source, placing, start, length)
                else {
                    return Some(None);
                };
                let [prefix, suffix] = text_overlay_edges(source, start, length).ok()?;
                Some(Some(Spans::Overlay([prefix, placing, suffix])))
            })
        }
        [source, target, replacement] if name.eq_ignore_ascii_case("replace") => {
            let (Some(source), Some(target), Some(replacement)) =
                (bind(source), bind(target), bind(replacement))
            else {
                return Ok(None);
            };
            project(batch.len, execution, |row| {
                let (Some(source), Some(target), Some(replacement)) =
                    (source.text(row)?, target.text(row)?, replacement.text(row)?)
                else {
                    return Some(None);
                };
                if target.is_empty() {
                    return None;
                }
                Some(Some(Spans::Replace {
                    source,
                    target,
                    replacement,
                }))
            })
        }
        [source] if name.eq_ignore_ascii_case("reverse") => {
            let Some(source) = bind(source) else {
                return Ok(None);
            };
            reverse(source, batch.len, &execution.context)
        }
        [source]
            if name.eq_ignore_ascii_case("quote_ident")
                || name.eq_ignore_ascii_case("quote-ident") =>
        {
            let Some(source) = bind(source) else {
                return Ok(None);
            };
            project(batch.len, execution, |row| {
                Some(source.text(row)?.map(|text| {
                    if identifier_needs_quotes(text) {
                        Spans::QuotedIdentifier(text)
                    } else {
                        Spans::Slice(text)
                    }
                }))
            })
        }
        [source] | [source, _] => {
            let (leading, trailing) = if name.eq_ignore_ascii_case("trim") {
                (true, true)
            } else if name.eq_ignore_ascii_case("trim_leading")
                || name.eq_ignore_ascii_case("trim-leading")
            {
                (true, false)
            } else if name.eq_ignore_ascii_case("trim_trailing")
                || name.eq_ignore_ascii_case("trim-trailing")
            {
                (false, true)
            } else {
                return Ok(None);
            };
            let token = match args.get(1) {
                Some(token) => bind(token),
                None => Some(TextInput::Literal(Some(" "))),
            };
            let (Some(source), Some(token)) = (bind(source), token) else {
                return Ok(None);
            };
            project(batch.len, execution, |row| {
                let (Some(source), Some(token)) = (source.text(row)?, token.text(row)?) else {
                    return Some(None);
                };
                Some(Some(Spans::Slice(
                    text_trim(source, token, leading, trailing).ok()?,
                )))
            })
        }
        _ => Ok(None),
    }
}

pub(super) fn record_reverse(
    source: &Expr,
    schema: &Schema,
    batch: &ColumnBatch,
    parameters: rtdb_expr::Parameters<'_>,
    context: &QueryContext,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let accepts = |logical: &LogicalType| {
        *logical == LogicalType::Null || plain_scalar(logical) == Some(&ScalarType::Utf8)
    };
    let source = match source {
        Expr::Column { id, .. } => {
            let Some(position) = schema.position(*id) else {
                return Ok(None);
            };
            if !accepts(&schema.fields()[position].data_type) {
                return Ok(None);
            }
            TextInput::Column(&batch.columns[position])
        }
        Expr::Parameter { index, data_type } if accepts(data_type) => {
            let Some(cell) = parameters.arrow_cell(*index) else {
                return Ok(None);
            };
            TextInput::Parameter(cell)
        }
        Expr::Literal(Value::Utf8(text)) => TextInput::Literal(Some(text.as_str())),
        Expr::Literal(Value::Null) => TextInput::Literal(None),
        _ => return Ok(None),
    };
    reverse(source, batch.len, context)
}

fn reverse(
    source: TextInput<'_>,
    rows: usize,
    context: &QueryContext,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    project_with_context(rows, context, |row| {
        Some(source.text(row)?.map(Spans::Reverse))
    })
}

fn project<'a>(
    rows: usize,
    execution: &Execution,
    read: impl Fn(usize) -> Option<Option<Spans<'a>>>,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    project_with_context(rows, &execution.context, read)
}

pub(super) fn project_slices<'a>(
    rows: usize,
    context: &QueryContext,
    read: impl Fn(usize) -> Option<Option<&'a str>>,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    match project_with_context(rows, context, |row| Some(read(row)?.map(Spans::Slice))) {
        Err(error) if error.is(AnomalyKind::ResourceExhausted) => Ok(None),
        result => result,
    }
}

fn project_with_context<'a>(
    rows: usize,
    context: &QueryContext,
    read: impl Fn(usize) -> Option<Option<Spans<'a>>>,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let mut payload = 0usize;
    for row in 0..rows {
        context.check_cancelled_at(row)?;
        let Some(value) = read(row) else {
            return Ok(None);
        };
        if let Some(spans) = value {
            let Some(next) = spans
                .len()
                .and_then(|len| payload.checked_add(len))
                .filter(|len| i32::try_from(*len).is_ok())
            else {
                return Ok(None);
            };
            payload = next;
        }
    }

    let bound = rows
        .checked_add(1)
        .and_then(|offsets| offsets.checked_mul(size_of::<i32>()))
        .and_then(|bytes| bytes.checked_add(payload))
        .and_then(|bytes| bytes.checked_add(rows.div_ceil(8) + 1024))
        .ok_or_else(size_error)?;
    let charge = context.memory.reserve(bound)?;
    let mut builder = StringBuilder::with_capacity(rows, payload);
    for row in 0..rows {
        context.check_cancelled_at(row)?;
        match read(row) {
            Some(Some(spans)) => {
                if spans.append(&mut builder).is_none() {
                    return Ok(None);
                }
            }
            Some(None) => builder.append_null(),
            None => return Ok(None),
        }
    }
    finish(observed_scalar_column(builder.finish())?, charge).map(Some)
}

fn finish(
    column: Column,
    mut charge: Arc<MemoryReservation>,
) -> Result<(Column, Arc<MemoryReservation>)> {
    let bytes = owned_projection_bytes(&column)?;
    let reservation = Arc::get_mut(&mut charge).expect("The text writer owns its reservation");
    if bytes <= reservation.bytes() {
        reservation.shrink_to(bytes)?;
    } else {
        reservation.grow_to(bytes)?;
    }
    Ok((column, charge))
}
