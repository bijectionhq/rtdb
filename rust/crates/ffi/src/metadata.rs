use std::{collections::BTreeMap, sync::Arc};
use arrow_array::{ArrayRef, ListArray, RecordBatch, StringArray, StructArray, UInt32Array, UnionArray, builder::{ArrayBuilder, Int32Builder, StringBuilder}, new_empty_array, new_null_array};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_flight::{error::FlightError, sql::CommandGetTableTypes};
use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
use rtdb_columnar::{OwnedBatch, from_field, to_field};
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
            1 => {
                let mut builder = CommandGetTableTypes {}.into_builder();
                let reservation = metadata_reservation(builder.schema().as_ref(), region.memory())?;
                builder.append("TABLE");
                let batch = builder.build().map_err(|error| match error {
                    FlightError::Arrow(error) => arrow_error(error),
                    error => rtdb_types::Anomaly::incorrect("rtdb.ffi/arrow", error.to_string()),
                })?;
                OwnedBatch::with_reservation(batch, reservation)
            }
            2 | 3 => {
                let context = session_state.query_context_with(&token, false).await?;
                if request.kind == 3 {
                    let database = request.catalog.as_deref().unwrap_or_else(|| session_state.state().database());
                    if context.catalog.database(database).is_err() { return empty(Schema::empty(), region.memory()); }
                    let table = request.table.as_deref().ok_or_else(|| crate::invalid("Table schema requires a table name"))?;
                    let tables = context.catalog.tables_reserved(database, region.memory())?;
                    let binding = tables.rows().iter().find(|binding| binding.name.table.schema == request.schema.as_deref().unwrap_or("public") && binding.name.table.table == table);
                    let bound = binding.map(|binding| binding.columns.iter().try_fold(8192, |bytes, (name, ty)| crate::add(bytes, crate::mul(crate::add(name.len(), ty.retained_bytes()?)?, 16)?))).transpose()?.unwrap_or(8192);
                    let _schema_charge = region.memory().reserve(bound)?;
                    let fields = binding.map(|binding| binding.columns.iter().map(|(name, ty)| to_field(name, ty)).collect::<Result<Vec<_>>>()).transpose()?.unwrap_or_default();
                    return empty(Schema::new(fields), region.memory());
                }
                if request.depth > 3 { return Err(crate::invalid("Metadata depth must be ALL, CATALOGS, DB_SCHEMAS or TABLES")); }
                let schema = Arc::new(objects_schema());
                let mut reservation = metadata_reservation(&schema, region.memory())?;
                let mut output = Objects::default();
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
                    let schemas_before = output.schema_names.len();
                    if request.depth != 1 {
                        let mut schemas = BTreeMap::<&str, Vec<_>>::new();
                        if request.table_types.as_ref().is_none_or(|types| types.iter().any(|ty| ty == "TABLE")) {
                            for table in tables.rows() {
                                if !matches_pattern(&table.name.table.schema, request.schema.as_deref(), &context)? || !matches_pattern(&table.name.table.table, request.table.as_deref(), &context)? { continue; }
                                schemas.entry(&table.name.table.schema).or_default().push(table);
                            }
                        }
                        for (name, tables) in schemas {
                            let tables_before = output.table_names.len();
                            if request.depth != 2 {
                                for table in tables {
                                    let mut columns = 0;
                                    if request.depth == 0 {
                                        for name in table.columns.keys() {
                                            if matches_pattern(name, request.column.as_deref(), &context)? {
                                                columns += 1;
                                                output.column_names.append_value(name);
                                                output.ordinals.append_value(i32::try_from(columns).map_err(|_| crate::invalid("Column count exceeds i32"))?);
                                            }
                                        }
                                        if columns == 0 { continue; }
                                    }
                                    output.table_names.append_value(&table.name.table.table);
                                    output.table_types.append_value("TABLE");
                                    output.table_columns.append((request.depth == 0).then_some(columns))?;
                                }
                            }
                            let count = output.table_names.len() - tables_before;
                            if request.depth == 0 && count == 0 { continue; }
                            output.schema_names.append_value(name);
                            output.schema_tables.append((request.depth != 2).then_some(count))?;
                        }
                    }
                    output.catalog_names.append_value(database);
                    output.catalog_schemas.append((request.depth != 1).then_some(output.schema_names.len() - schemas_before))?;
                }
                let batch = output.finish(schema)?;
                let bytes = reservation.bytes().max(batch.get_array_memory_size());
                Arc::get_mut(&mut reservation).expect("metadata owns its reservation").grow_to(bytes)?;
                OwnedBatch::with_reservation(batch, reservation)
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
    let entries = [(0, "XTDB"), (1, rtdb_types::runtime_version()), (100, "XTDB ADBC Driver"), (101, rtdb_types::runtime_version())];
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
fn metadata_reservation(schema: &Schema, memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<Arc<rtdb_columnar::MemoryReservation>> {
    let mut bound = crate::arrow::schema_bound(schema)?;
    for field in schema.fields() {
        bound = crate::add(bound, rtdb_columnar::estimate_builder_bytes_refs(&from_field(field)?, std::iter::empty())?)?;
    }
    memory.reserve(crate::mul(bound, 4)?)
}

fn empty(schema: Schema, memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<OwnedBatch> {
    let reservation = metadata_reservation(&schema, memory)?;
    OwnedBatch::with_reservation(RecordBatch::new_empty(Arc::new(schema)), reservation)
}

#[derive(Default)]
struct Objects {
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    table_types: StringBuilder,
    column_names: StringBuilder,
    ordinals: Int32Builder,
    catalog_schemas: ListOffsets,
    schema_tables: ListOffsets,
    table_columns: ListOffsets,
}

impl Objects {
    fn finish(mut self, schema: Arc<Schema>) -> Result<RecordBatch> {
        let schemas = struct_fields(schema.field(1));
        let tables = struct_fields(&schemas[1]);
        let columns = struct_fields(&tables[2]);
        let column_count = self.column_names.len();
        let mut column_arrays = vec![Arc::new(self.column_names.finish()) as ArrayRef, Arc::new(self.ordinals.finish())];
        column_arrays.extend(columns[2..].iter().map(|field| new_null_array(field.data_type(), column_count)));
        let columns = self.table_columns.finish(&tables[2], column_arrays)?;
        let constraints = new_null_array(tables[3].data_type(), self.table_names.len());
        let tables = self.schema_tables.finish(&schemas[1], vec![Arc::new(self.table_names.finish()), Arc::new(self.table_types.finish()), columns, constraints])?;
        let schemas = self.catalog_schemas.finish(schema.field(1), vec![Arc::new(self.schema_names.finish()), tables])?;
        RecordBatch::try_new(schema, vec![Arc::new(self.catalog_names.finish()), schemas]).map_err(arrow_error)
    }
}

struct ListOffsets { offsets: Vec<i32>, validity: NullBufferBuilder }

impl Default for ListOffsets {
    fn default() -> Self { Self { offsets: vec![0], validity: NullBufferBuilder::new(0) } }
}

impl ListOffsets {
    fn append(&mut self, count: Option<usize>) -> Result<()> {
        let count_i32 = i32::try_from(count.unwrap_or(0)).ok();
        let offset = count_i32.and_then(|count| self.offsets.last().unwrap().checked_add(count))
            .ok_or_else(|| crate::invalid("Metadata list exceeds i32"))?;
        self.offsets.push(offset);
        self.validity.append(count.is_some());
        Ok(())
    }

    fn finish(mut self, field: &Field, columns: Vec<ArrayRef>) -> Result<ArrayRef> {
        let DataType::List(child) = field.data_type() else { unreachable!() };
        let entries = StructArray::try_new(struct_fields(field).clone(), columns, None).map_err(arrow_error)?;
        Ok(Arc::new(ListArray::try_new(Arc::clone(child), OffsetBuffer::new(ScalarBuffer::from(self.offsets)), Arc::new(entries), self.validity.finish()).map_err(arrow_error)?))
    }
}

fn struct_fields(list: &Field) -> &arrow_schema::Fields {
    let DataType::List(child) = list.data_type() else { unreachable!() };
    let DataType::Struct(fields) = child.data_type() else { unreachable!() };
    fields
}

#[cfg(test)]
mod tests {
    use arrow_array::{UnionArray, cast::AsArray};

    #[test]
    fn adbc_vendor_and_driver_versions_are_the_runtime_version() {
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let batch = super::info(Some(&[1, 101]), &memory).unwrap();
        let union = batch.batch().column(1).as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(union.child(0).as_string::<i32>().iter().collect::<Vec<_>>(), [Some(rtdb_types::runtime_version()); 2]);
    }
}
