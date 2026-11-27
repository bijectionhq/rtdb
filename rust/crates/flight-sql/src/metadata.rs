use std::sync::Arc;

use arrow_array::{RecordBatch, builder::BinaryBuilder, cast::AsArray};
use arrow_flight::{
    FlightData,
    sql::{Command, CommandGetTables, SqlInfo, SqlSupportedTransaction, metadata::SqlInfoDataBuilder},
};
use arrow_schema::{Schema, SchemaRef};
use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_plan::QualifiedTable;
use rtdb_query::CatalogSnapshot;
use rtdb_types::TableRef;
use tonic::{Response, Status};

use crate::{error::{arrow, cancelled, status, unsupported}, service::{Call, Output, Service}, wire};

impl Service {
    pub fn metadata_schema(&self, command: &Command) -> Result<wire::OwnedSchema, Status> {
        wire::OwnedSchema::build(65_536, self.memory(), || match command {
            Command::CommandGetCatalogs(command) => Ok(command.clone().into_builder().schema().as_ref().clone()),
            Command::CommandGetDbSchemas(command) => Ok(command.clone().into_builder().schema().as_ref().clone()),
            Command::CommandGetTables(command) => Ok(command.clone().into_builder().schema().as_ref().clone()),
            Command::CommandGetTableTypes(command) => Ok(command.clone().into_builder().schema().as_ref().clone()),
            Command::CommandGetSqlInfo(_) => Ok(SqlInfoDataBuilder::schema().clone()),
            _ => Err(unsupported()),
        })
    }
    fn sql_info(&self, requested: &[u32]) -> Result<RecordBatch, Status> {
        let includes = |info: SqlInfo| requested.is_empty() || requested.contains(&(info as u32));
        let millis = |timeout: std::time::Duration| timeout.as_millis().min(i32::MAX as u128) as i32;
        let mut builder = SqlInfoDataBuilder::new();
        for (info, value) in [
            (SqlInfo::FlightSqlServerName, "XTDB"),
            (SqlInfo::FlightSqlServerVersion, rtdb_types::runtime_version()),
        ] {
            if includes(info) { builder.append(info, value); }
        }
        for (info, value) in [
            (SqlInfo::FlightSqlServerTransaction, SqlSupportedTransaction::Transaction as i32),
            (SqlInfo::FlightSqlServerStatementTimeout, millis(self.0.config.prepared_statement_idle_timeout)),
            (SqlInfo::FlightSqlServerTransactionTimeout, millis(self.0.config.transaction_idle_timeout)),
        ] {
            if includes(info) { builder.append(info, value); }
        }
        builder.build().and_then(|info| info.record_batch([])).map_err(arrow)
    }
    pub async fn metadata_stream(&self, call: Call, command: Command) -> Result<Response<Output<FlightData>>, Status> {
        self.metadata_schema(&command).map_err(|error| call.error(error))?;
        let work = async {
            let mut charges = vec![self.memory().reserve(65_536).map_err(status)?];
            let batch = match command {
                Command::CommandGetSqlInfo(command) => self.sql_info(&command.info)?,
                Command::CommandGetTableTypes(command) => { self.connection(&call, &call.database).await?; let mut builder = command.into_builder(); builder.append("TABLE"); builder.build().map_err(arrow)? }
                Command::CommandGetCatalogs(command) => {
                    self.connection(&call, &call.database).await?;
                    let mut builder = command.into_builder();
                    if let Some(database) = call.session.placed_database() {
                        charges.push(self.memory().reserve(wire::add(8192, wire::mul(database.len(), 8)?)?).map_err(status)?);
                        builder.append(database);
                    } else {
                        let databases = self.0.node.catalog().open_database_snapshot(self.memory()).map_err(status)?;
                        let bytes = databases.rows().iter().try_fold(8192, |bytes, database| wire::add(bytes, wire::add(wire::mul(database.name().len(), 8)?, 512)?))?;
                        charges.push(self.memory().reserve(bytes).map_err(status)?);
                        for database in databases.rows() { builder.append(database.name()); }
                    }
                    builder.build().map_err(arrow)?
                }
                command => {
                    let connection = self.connection(&call, &call.database).await?;
                    let mut native = connection.borrow().await?;
                    let token = native.state().cancellation().child_token(); let _cancel = token.clone().drop_guard();
                    let context = native.query_context_with(&token, false).await.map_err(status)?;
                    let catalog = &context.catalog;
                    let memory = self.memory();
                    match command {
                        Command::CommandGetDbSchemas(mut command) => {
                            let filter = command.catalog.take();
                            let mut builder = command.into_builder();
                            for database in catalog.database_names().filter(|database| filter.as_deref().is_none_or(|name| *database == name)) {
                                let names = catalog.table_names_reserved(database, memory).map_err(status)?;
                                let mut previous = None;
                                for table in names.rows() {
                                    let schema = table.schema.as_str();
                                    if previous == Some(schema) {
                                        continue;
                                    }
                                    let bytes = wire::add(512, wire::mul(wire::add(database.len(), schema.len())?, 16)?)?;
                                    charges.push(memory.reserve(bytes).map_err(status)?);
                                    builder.append(database, schema);
                                    previous = Some(schema);
                                }
                            }
                            builder.build().map_err(arrow)?
                        }
                        Command::CommandGetTables(mut command) => {
                            let filter = command.catalog.take();
                            let includes_table = command.table_types.is_empty() || command.table_types.iter().any(|kind| kind == "TABLE");
                            let output_schema = command.include_schema.then(|| {
                                CommandGetTables { include_schema: true, ..Default::default() }.into_builder().schema()
                            });
                            let mut names = CommandGetTables { include_schema: false, ..command }.into_builder();
                            let empty = Schema::empty();
                            for database in catalog.database_names().filter(|database| includes_table && filter.as_deref().is_none_or(|name| *database == name)) {
                                let tables = catalog.table_names_reserved(database, memory).map_err(status)?;
                                let bytes = tables.rows().iter().try_fold(8192, |bytes, table| {
                                    let name = wire::add(database.len(), wire::add(table.schema.len(), table.table.len())?)?;
                                    wire::add(bytes, wire::add(512, wire::mul(name, 16)?)?)
                                })?;
                                charges.push(memory.reserve(bytes).map_err(status)?);
                                for table in tables.rows() {
                                    names.append(database, &table.schema, &table.table, "TABLE", &empty).map_err(arrow)?;
                                }
                            }
                            let selected = names.build().map_err(arrow)?;
                            if let Some(output_schema) = output_schema {
                                with_table_schemas(selected, output_schema, catalog, memory, &mut charges)?
                            } else {
                                selected
                            }
                        }
                        _ => return Err(unsupported()),
                    }
                }
            };
            let retained = self.memory().reserve(batch.get_array_memory_size()).map_err(status)?;
            let batch = OwnedBatch::with_reservation(batch, retained).map_err(status)?;
            drop(charges); Ok::<_, Status>(batch)
        };
        let batch = call.cancellation.run_until_cancelled(work).await.ok_or_else(cancelled).and_then(|result| result).map_err(|error| call.error(error))?;
        let headers = call.response(())?.metadata().clone(); let memory = Arc::clone(self.memory());
        let stream: Output<FlightData> = Box::pin(async_stream::try_stream! {
            let _call = call;
            if _call.cancellation.is_cancelled() { Err(cancelled())?; }
            yield wire::schema_data(batch.batch().schema().as_ref(), &memory)?;
            for data in wire::Encoder::new(batch.batch().schema().as_ref(), &memory)?.batch(batch.batch(), &memory)? { yield data; }
        });
        let mut response = Response::new(stream); *response.metadata_mut() = headers; Ok(response)
    }
}

fn encode_table_schema<'a, S: AsRef<str> + 'a>(
    columns: impl IntoIterator<Item = (S, &'a rtdb_types::LogicalType)>,
    memory: &Arc<MemoryBudget>,
) -> Result<Bytes, Status> {
    let fields = columns.into_iter()
        .map(|(name, ty)| rtdb_columnar::to_field(name.as_ref(), ty).map_err(status))
        .collect::<Result<Vec<_>, _>>()?;
    let table_schema = Schema::new(fields);
    wire::schema_message(&table_schema, memory)
}

fn with_table_schemas(
    selected: RecordBatch,
    schema: SchemaRef,
    catalog: &CatalogSnapshot,
    memory: &Arc<MemoryBudget>,
    charges: &mut Vec<Arc<MemoryReservation>>,
) -> Result<RecordBatch, Status> {
    let databases = selected.column(0).as_string::<i32>();
    let schemas = selected.column(1).as_string::<i32>();
    let tables = selected.column(2).as_string::<i32>();
    let mut ipc = BinaryBuilder::new();
    for row in 0..selected.num_rows() {
        let name = QualifiedTable {
            database: databases.value(row).into(),
            table: TableRef::new(schemas.value(row), tables.value(row)),
        };
        let (table, _binding) = catalog.table_binding_reserved(&name, memory).map_err(status)?
            .ok_or_else(|| Status::internal("Selected table is missing from its captured catalog"))?;
        let bytes = table.columns.iter().try_fold(512, |bytes, (name, ty)| {
            let field = wire::add(name.len(), wire::add(ty.retained_bytes().map_err(status)?, 512)?)?;
            wire::add(bytes, wire::mul(field, 32)?)
        })?;
        charges.push(memory.reserve(bytes).map_err(status)?);
        let message = encode_table_schema(table.columns.iter().map(|(name, ty)| (name.as_str(), ty)), memory)?;
        ipc.append_value(message);
    }
    let mut columns = selected.columns().to_vec();
    columns.push(Arc::new(ipc.finish()));
    RecordBatch::try_new(schema, columns).map_err(arrow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::{LogicalType, ScalarType};

    #[test]
    fn table_schema_ipc_patches_extended_decimals_without_internal_metadata() {
        let memory = MemoryBudget::new(1 << 20);
        let decimal = LogicalType::Scalar(ScalarType::Decimal { precision: 38, scale: 200, bit_width: 128 });
        let encoded = encode_table_schema([("amount", &decimal)], &memory).unwrap();
        let decoded = rtdb_format::ipc::read_schema_message(&encoded).unwrap();
        assert_eq!(rtdb_columnar::from_field(decoded.field(0)).unwrap(), decimal);
        let length = i32::from_le_bytes(encoded[4..8].try_into().unwrap()) as usize;
        let message = arrow_ipc::root_as_message(&encoded[8..8 + length]).unwrap();
        let fb_schema = message.header_as_schema().unwrap();
        let fb_field = fb_schema.fields().unwrap().get(0);
        assert!(fb_field.custom_metadata().is_none_or(|entries| entries.is_empty()));
        let fb_decimal = fb_field.type_as_decimal().unwrap();
        assert_eq!((fb_decimal.precision(), fb_decimal.scale(), fb_decimal.bitWidth()), (38, 200, 128));
    }
}
