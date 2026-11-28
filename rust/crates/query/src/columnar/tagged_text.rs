use std::sync::Arc;

use arrow_array::{Array, StringArray};
use arrow_schema::DataType;
use rtdb_columnar::{EXTENSION_NAME_KEY, MemoryReservation};
use rtdb_plan::{Expr, Schema};
use rtdb_types::{LogicalType, Result, ScalarType, UriParts, Value};

use super::{Column, ColumnBatch, binary::Input, plain_scalar, text};
use crate::context::Execution;

pub(super) fn column(
    expr: &Expr,
    output: &LogicalType,
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    if batch.len == 0 || plain_scalar(output) != Some(&ScalarType::Utf8) {
        return Ok(None);
    }
    let Expr::Call { name, args, .. } = expr else {
        return Ok(None);
    };
    let [source] = args.as_slice() else {
        return Ok(None);
    };
    let named = |candidate| name.eq_ignore_ascii_case(candidate);
    let (scalar, select): (ScalarType, fn(&str) -> Option<Option<&str>>) = if named("namespace") {
        (ScalarType::Keyword, |value| {
            Some(value.split_once('/').map(|(namespace, _)| namespace))
        })
    } else if named("local_name") || named("local-name") {
        (ScalarType::Keyword, |value| {
            Some(Some(
                value.split_once('/').map_or(value, |(_, local)| local),
            ))
        })
    } else if named("uri_scheme") || named("uri-scheme") {
        (ScalarType::Uri, |value| {
            Some(UriParts::parse(value).ok()?.scheme())
        })
    } else if named("uri_host") || named("uri-host") {
        (ScalarType::Uri, |value| {
            Some(UriParts::parse(value).ok()?.host())
        })
    } else {
        return Ok(None);
    };
    let Some(input) = Input::bind(
        source,
        schema,
        batch,
        execution,
        |ty| *ty == LogicalType::Null || plain_scalar(ty) == Some(&scalar),
        |expr| match (expr, &scalar) {
            (Expr::Literal(Value::Keyword(keyword)), ScalarType::Keyword) => Some(keyword.as_str()),
            (Expr::Literal(Value::Uri(uri)), ScalarType::Uri) => Some(uri.as_str()),
            _ => None,
        },
    ) else {
        return Ok(None);
    };
    text::project_slices(batch.len, &execution.context, |row| {
        let Some(value) = tagged_text(&input, row, &scalar)? else {
            return Some(None);
        };
        select(value)
    })
}

fn tagged_text<'a>(
    input: &Input<'a, &'a str>,
    row: usize,
    scalar: &ScalarType,
) -> Option<Option<&'a str>> {
    let (array, field, logical, row) = match input {
        Input::Column(column) => {
            let Some(cell) = column.arrow_cell(row)? else {
                return Some(None);
            };
            cell
        }
        Input::Parameter(cell) => (cell.array, cell.field, None, cell.row),
        Input::Literal(value) => return Some(*value),
    };
    if field.data_type() != array.data_type() || row >= array.len() {
        return None;
    }
    if field.data_type() == &DataType::Null && field.is_nullable() && field.metadata().is_empty() {
        return logical
            .is_none_or(|ty| *ty == LogicalType::Null)
            .then_some(None);
    }
    if field.data_type() != &DataType::Utf8 || field.metadata().len() != 1 {
        return None;
    }
    let extension = match scalar {
        ScalarType::Keyword => "xt/clj-keyword",
        ScalarType::Uri => "uri",
        _ => return None,
    };
    if field.metadata().get(EXTENSION_NAME_KEY).map(String::as_str) != Some(extension)
        || logical.is_some_and(|ty| plain_scalar(ty) != Some(scalar))
    {
        return None;
    }
    let array = array.as_any().downcast_ref::<StringArray>()?;
    Some(array.is_valid(row).then(|| array.value(row)))
}
