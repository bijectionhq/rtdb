use std::{collections::BTreeMap, sync::Arc};
use arrow_array::{ArrayRef, ListArray, RecordBatch, StringArray, StructArray, UInt32Array, UnionArray, new_empty_array};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
use rtdb_columnar::{OwnedBatch, build_array, from_field, to_field};
use rtdb_expr::{EvalContext, evaluate};
use rtdb_plan::Expr;
use rtdb_types::{Result, Value};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use crate::{arrow::arrow_error, statement::{CursorHandle, SessionHandle}};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub kind: u32,
    #[serde(default)] pub depth: u32,
    pub catalog: Option<String>, pub schema: Option<String>, pub table: Option<String>, pub column: Option<String>,
    pub table_types: Option<Vec<String>>, pub info_codes: Option<Vec<u32>>,
}
pub(crate) fn open(session: &Arc<SessionHandle>, token: CancellationToken, request: Request) -> Result<Arc<CursorHandle>> {
    if token.is_cancelled() { return Err(crate::cancelled()); }
    let region = &session.region;
    let batch = region.run(async {
        let mut session_state = session.state.lock();
        session_state.state().revalidate().await?;
        let _fixed = region.memory().reserve(131_072)?;
        match request.kind {
            0 => info(request.info_codes.as_deref(), region.memory()),
            1 => rows(Schema::new(vec![Field::new("table_type", DataType::Utf8, false)]), vec![vec![Value::Utf8("TABLE".into())]], region.memory()),
            2 | 3 => {
                let context = session_state.query_context_with(&token, false).await?;
                if request.kind == 3 {
                    let database = request.catalog.as_deref().unwrap_or_else(|| session_state.state().database());
                    if context.catalog.database(database).is_err() { return rows(Schema::empty(), Vec::new(), region.memory()); }
                    let table = request.table.as_deref().ok_or_else(|| crate::invalid("Table schema requires a table name"))?;
                    let tables = context.catalog.tables_reserved(database, region.memory())?;
                    let binding = tables.rows().iter().find(|binding| binding.name.table.schema == request.schema.as_deref().unwrap_or("public") && binding.name.table.table == table);
                    let bound = binding.map(|binding| binding.columns.iter().try_fold(8192, |bytes, (name, ty)| crate::add(bytes, crate::mul(crate::add(name.len(), ty.retained_bytes()?)?, 16)?))).transpose()?.unwrap_or(8192);
                    let _schema_charge = region.memory().reserve(bound)?;
                    let fields = binding.map(|binding| binding.columns.iter().map(|(name, ty)| to_field(name, ty)).collect::<Result<Vec<_>>>()).transpose()?.unwrap_or_default();
                    return rows(Schema::new(fields), Vec::new(), region.memory());
                }
                if request.depth > 3 { return Err(crate::invalid("Metadata depth must be ALL, CATALOGS, DB_SCHEMAS or TABLES")); }
                let mut output = Vec::new();
                let mut charges = Vec::new();
                for database in context.catalog.database_names() {
                    context.check_cancelled()?;
                    if !matches_pattern(database, request.catalog.as_deref(), &context)? { continue; }
                    let tables = context.catalog.tables_reserved(database, region.memory())?;
                    let mut bound = crate::add(8192, crate::mul(database.len(), 8)?)?;
                    for table in tables.rows() {
                        bound = crate::add(bound, crate::add(8192, crate::mul(crate::add(table.name.table.schema.len(), table.name.table.table.len())?, 8)?)?)?;
                        for (name, ty) in &table.columns { bound = crate::add(bound, crate::add(8192, crate::mul(crate::add(name.len(), ty.retained_bytes()?)?, 8)?)?)?; }
                    }
                    charges.push(region.memory().reserve(bound)?);
                    let schemas = if request.depth == 1 { Value::Null } else {
                        let mut schemas = BTreeMap::<String, Vec<Value>>::new();
                        if request.table_types.as_ref().is_none_or(|types| types.iter().any(|ty| ty == "TABLE")) {
                            for table in tables.rows() {
                                if !matches_pattern(&table.name.table.schema, request.schema.as_deref(), &context)? || !matches_pattern(&table.name.table.table, request.table.as_deref(), &context)? { continue; }
                                let mut columns = Vec::new();
                                if request.depth == 0 {
                                    for name in table.columns.keys() {
                                        if matches_pattern(name, request.column.as_deref(), &context)? {
                                            columns.push(object([("column_name", Value::Utf8(name.clone())), ("ordinal_position", Value::I32(i32::try_from(columns.len() + 1).map_err(|_| crate::invalid("Column count exceeds i32"))?))]));
                                        }
                                    }
                                    if columns.is_empty() { continue; }
                                }
                                schemas.entry(table.name.table.schema.clone()).or_default().push(object([
                                    ("table_name", Value::Utf8(table.name.table.table.clone())), ("table_type", Value::Utf8("TABLE".into())),
                                    ("table_columns", if request.depth == 0 { Value::list(columns) } else { Value::Null }),
                                ]));
                            }
                        }
                        Value::list(schemas.into_iter().map(|(name, tables)| object([("db_schema_name", Value::Utf8(name)), ("db_schema_tables", if request.depth == 2 { Value::Null } else { Value::list(tables) })])).collect())
                    };
                    output.push(vec![Value::Utf8(database.into()), schemas]);
                }
                rows(objects_schema(), output, region.memory())
            }
            _ => Err(crate::invalid("Unknown metadata request kind")),
        }
    })?;
    CursorHandle::from_batch(Arc::clone(region), batch, token)
}

fn matches_pattern(value: &str, pattern: Option<&str>, context: &rtdb_query::QueryContext) -> Result<bool> {
    let Some(pattern) = pattern else { return Ok(true); };
    let _scratch = context.memory.reserve(crate::add(65_536, crate::mul(crate::add(value.len(), pattern.len())?, 256)?)?)?;
    let scope = rtdb_expr::EvaluationScope::new(&context.memory)?;
    let expr = Expr::Like { expr: Box::new(Expr::Literal(Value::Utf8(value.into()))), pattern: Box::new(Expr::Literal(Value::Utf8(pattern.into()))), escape: Some('\\'), case_insensitive: false, negated: false };
    Ok(evaluate(&expr, &Default::default(), &[], &EvalContext { parameters: rtdb_expr::Parameters::Values(&[]), parameter_types: None, current_time: context.current_time, timezone: &context.timezone, outer_rows: &[], snapshot_token: None, allocation: Some(&scope), catalog: Some(&context.catalog) })? == Value::Bool(true))
}
fn object<const N: usize>(entries: [(&str, Value); N]) -> Value { Value::Struct(entries.into_iter().map(|(key, value)| (key.into(), value)).collect()) }
fn list(name: &str, children: Vec<Field>) -> Field { Field::new(name, DataType::List(Arc::new(Field::new("item", DataType::Struct(children.into()), true))), true) }
pub(crate) fn objects_schema() -> Schema {
    let usage = vec![Field::new("fk_catalog", DataType::Utf8, true), Field::new("fk_db_schema", DataType::Utf8, true), Field::new("fk_table", DataType::Utf8, false), Field::new("fk_column_name", DataType::Utf8, false)];
    let constraints = vec![Field::new("constraint_name", DataType::Utf8, false), Field::new("constraint_type", DataType::Utf8, false), Field::new("constraint_column_names", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true), list("constraint_column_usage", usage)];
    let columns = vec![
        Field::new("column_name", DataType::Utf8, false), Field::new("ordinal_position", DataType::Int32, true), Field::new("remarks", DataType::Utf8, true),
        Field::new("xdbc_data_type", DataType::Int16, true), Field::new("xdbc_type_name", DataType::Utf8, true), Field::new("xdbc_column_size", DataType::Int32, true),
        Field::new("xdbc_decimal_digits", DataType::Int16, true), Field::new("xdbc_num_prec_radix", DataType::Int16, true), Field::new("xdbc_nullable", DataType::Int16, true),
        Field::new("xdbc_column_def", DataType::Utf8, true), Field::new("xdbc_sql_data_type", DataType::Int16, true), Field::new("xdbc_datetime_sub", DataType::Int16, true),
        Field::new("xdbc_char_octet_length", DataType::Int32, true), Field::new("xdbc_is_nullable", DataType::Utf8, true), Field::new("xdbc_scope_catalog", DataType::Utf8, true),
        Field::new("xdbc_scope_schema", DataType::Utf8, true), Field::new("xdbc_scope_table", DataType::Utf8, true), Field::new("xdbc_is_autoincrement", DataType::Boolean, true), Field::new("xdbc_is_generatedcolumn", DataType::Boolean, true),
    ];
    let tables = vec![Field::new("table_name", DataType::Utf8, false), Field::new("table_type", DataType::Utf8, false), list("table_columns", columns), list("table_constraints", constraints)];
    let schemas = vec![Field::new("db_schema_name", DataType::Utf8, false), list("db_schema_tables", tables)];
    Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false), list("catalog_db_schemas", schemas)])
}
fn info(codes: Option<&[u32]>, memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<OwnedBatch> {
    let reservation = memory.reserve(131_072)?;
    let entries = [(0, "XTDB"), (1, env!("CARGO_PKG_VERSION")), (100, "XTDB ADBC Driver"), (101, env!("CARGO_PKG_VERSION"))];
    let entries: Vec<_> = entries.into_iter().filter(|(code, _)| codes.is_none_or(|codes| codes.contains(code))).collect();
    let map = DataType::Map(Arc::new(Field::new("entries", DataType::Struct(vec![Field::new("key", DataType::Int32, false), Field::new("value", DataType::Int32, true)].into()), false)), false);
    let fields: Vec<_> = [("string_value", DataType::Utf8), ("bool_value", DataType::Boolean), ("int64_value", DataType::Int64), ("int32_bitmask", DataType::Int32), ("string_list", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))), ("int32_to_int32_list_map", map)].into_iter().map(|(name, ty)| Arc::new(Field::new(name, ty, true))).collect();
    let union_fields = UnionFields::try_new(0i8..6, fields.clone()).map_err(arrow_error)?;
    let mut children = vec![Arc::new(StringArray::from(entries.iter().map(|(_, value)| *value).collect::<Vec<_>>())) as ArrayRef];
    children.extend(fields.iter().skip(1).map(|field| new_empty_array(field.data_type())));
    let union = UnionArray::try_new(union_fields.clone(), ScalarBuffer::from(vec![0i8; entries.len()]), Some(ScalarBuffer::from((0..entries.len()).map(|index| index as i32).collect::<Vec<_>>())), children).map_err(arrow_error)?;
    let schema = Schema::new(vec![Field::new("info_name", DataType::UInt32, false), Field::new("info_value", DataType::Union(union_fields, UnionMode::Dense), true)]);
    OwnedBatch::with_reservation(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(UInt32Array::from(entries.iter().map(|(code, _)| *code).collect::<Vec<_>>())), Arc::new(union)]).map_err(arrow_error)?, reservation)
}
fn rows(schema: Schema, rows: Vec<Vec<Value>>, memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<OwnedBatch> {
    let mut bound = crate::arrow::schema_bound(&schema)?;
    for (index, field) in schema.fields().iter().enumerate() {
        bound = crate::add(bound, rtdb_columnar::estimate_builder_bytes_refs(&from_field(field)?, rows.iter().map(|row| &row[index]))?)?;
    }
    let reservation = memory.reserve(crate::mul(bound, 4)?)?;
    let arrays = schema.fields().iter().enumerate().map(|(index, field)| exact_array(field, &rows.iter().map(|row| row[index].clone()).collect::<Vec<_>>())).collect::<Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new_with_options(Arc::new(schema), arrays, &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows.len()))).map_err(arrow_error)?;
    OwnedBatch::with_reservation(batch, reservation)
}
fn exact_array(field: &Field, values: &[Value]) -> Result<ArrayRef> {
    match field.data_type() {
        DataType::Struct(fields) => {
            let arrays = fields.iter().map(|field| exact_array(field, &values.iter().map(|value| match value { Value::Struct(value) => value.get(field.name()).cloned().unwrap_or(Value::Null), _ => Value::Null }).collect::<Vec<_>>())).collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new_with_length(fields.clone(), arrays, validity(values), values.len()).map_err(arrow_error)?))
        }
        DataType::List(child) => {
            let mut offsets = vec![0i32]; let mut flattened = Vec::new();
            for value in values { if let Value::List(children) = value { flattened.extend(children.iter().map(std::borrow::Cow::into_owned)); } offsets.push(i32::try_from(flattened.len()).map_err(|_| crate::invalid("Metadata list exceeds i32"))?); }
            Ok(Arc::new(ListArray::try_new(Arc::clone(child), OffsetBuffer::new(ScalarBuffer::from(offsets)), exact_array(child, &flattened)?, validity(values)).map_err(arrow_error)?))
        }
        _ => build_array(&from_field(&field.clone().with_nullable(true))?, values),
    }
}
fn validity(values: &[Value]) -> Option<NullBuffer> { values.iter().any(|value| matches!(value, Value::Null)).then(|| NullBuffer::from(values.iter().map(|value| !matches!(value, Value::Null)).collect::<Vec<_>>())) }
