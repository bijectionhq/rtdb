use std::{collections::BTreeSet, sync::Arc};
use arrow_flight::{FlightData, sql::{Command, SqlInfo, metadata::sql_info::{SqlInfoData, SqlInfoDataBuilder}}};
use arrow_schema::Schema;
use rtdb_columnar::OwnedBatch;
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
    fn sql_info(&self) -> Result<SqlInfoData, Status> {
        let mut builder = SqlInfoDataBuilder::new();
        builder.append(SqlInfo::FlightSqlServerName, "XTDB");
        builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        builder.append(SqlInfo::FlightSqlServerTransaction, 1i32);
        builder.append(SqlInfo::FlightSqlServerStatementTimeout, self.0.config.prepared_statement_idle_timeout.as_millis().min(i32::MAX as u128) as i32);
        builder.append(SqlInfo::FlightSqlServerTransactionTimeout, self.0.config.transaction_idle_timeout.as_millis().min(i32::MAX as u128) as i32);
        builder.build().map_err(arrow)
    }
    pub async fn metadata_stream(&self, call: Call, command: Command) -> Result<Response<Output<FlightData>>, Status> {
        self.metadata_schema(&command).map_err(|error| call.error(error))?;
        let work = async {
            let mut charges = vec![self.memory().reserve(65_536).map_err(status)?];
            let batch = match command {
                Command::CommandGetSqlInfo(command) => self.sql_info()?.record_batch(command.info).map_err(arrow)?,
                Command::CommandGetTableTypes(command) => { self.connection(&call, &call.database).await?; let mut builder = command.into_builder(); builder.append("TABLE"); builder.build().map_err(arrow)? }
                Command::CommandGetCatalogs(command) => {
                    self.connection(&call, &call.database).await?;
                    let databases = self.0.node.catalog().open_database_snapshot(self.memory()).map_err(status)?;
                    let bytes = databases.rows().iter().try_fold(8192, |bytes, database| wire::add(bytes, wire::add(wire::mul(database.name().len(), 8)?, 512)?))?;
                    charges.push(self.memory().reserve(bytes).map_err(status)?);
                    let mut builder = command.into_builder(); for database in databases.rows() { builder.append(database.name()); } builder.build().map_err(arrow)?
                }
                command => {
                    let connection = self.connection(&call, &call.database).await?;
                    let mut native = connection.borrow().await?;
                    let token = native.state().cancellation().child_token(); let _cancel = token.clone().drop_guard();
                    let context = native.query_context_with(&token, false).await.map_err(status)?;
                    let mut tables = Vec::new();
                    for database in context.catalog.database_names() {
                        let next = context.catalog.tables_reserved(database, self.memory()).map_err(status)?;
                        let mut bound = 8192;
                        for table in next.rows() { bound = wire::add(bound, wire::mul(wire::add(database.len(), wire::add(table.name.table.schema.len(), table.name.table.table.len())?)?, 16)?)?; for (name, ty) in &table.columns { bound = wire::add(bound, wire::mul(wire::add(name.len(), wire::add(ty.retained_bytes().map_err(status)?, 512)?)?, 32)?)?; } }
                        charges.push(self.memory().reserve(bound).map_err(status)?); tables.push(next);
                    }
                    match command {
                        Command::CommandGetDbSchemas(command) => {
                            let mut builder = command.into_builder();
                            let mut schemas = BTreeSet::new();
                            for table in tables.iter().flat_map(|tables| tables.rows()) { schemas.insert((table.name.database.as_str(), table.name.table.schema.as_str())); }
                            for (database, schema) in schemas { builder.append(database, schema); } builder.build().map_err(arrow)?
                        }
                        Command::CommandGetTables(command) => {
                            let mut builder = command.into_builder();
                            for table in tables.iter().flat_map(|tables| tables.rows()) {
                                let schema = Schema::new(table.columns.iter().map(|(name, ty)| rtdb_columnar::to_field(name, ty).map_err(status)).collect::<Result<Vec<_>, _>>()?);
                                builder.append(&table.name.database, &table.name.table.schema, &table.name.table.table, "TABLE", &schema).map_err(arrow)?;
                            }
                            builder.build().map_err(arrow)?
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
            for data in wire::Encoder::new().batch(batch.batch(), &memory)? { yield data; }
        });
        let mut response = Response::new(stream); *response.metadata_mut() = headers; Ok(response)
    }
}
