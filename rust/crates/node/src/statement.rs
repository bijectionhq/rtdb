use std::sync::Arc;

use rtdb_columnar::MemoryReservation;
use rtdb_plan::{ColumnId, Field, Schema, Statement};
use rtdb_query::{PreparedQuery, QueryContext, QueryCursor};
use rtdb_sql::{PrepareOptions, StatementKind};
use rtdb_tx::{SqlArguments, TxOp};
use rtdb_types::{Anomaly, AnomalyCategory, LogicalType, PgType, Result, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultSignaturePolicy { Dynamic, Exact, PostgreSql }

struct PreparedData {
    source: String,
    options: PrepareOptions,
    parameters: Vec<LogicalType>,
    result: PreparedResult,
    policy: ResultSignaturePolicy,
    _reservation: Arc<MemoryReservation>,
}
enum PreparedResult { Query { schema: Schema, pg_types: Vec<PgType> }, Dml }

/// An immutable SQL preparation; each query execution binds against its supplied snapshot.
/// Session access checks and read-basis capture precede this API and remain the caller's obligation.
#[derive(Clone)]
pub struct PreparedSql(Arc<PreparedData>);

impl PreparedSql {
    pub fn prepare(source: &str, context: &Arc<QueryContext>, options: PrepareOptions, policy: ResultSignaturePolicy) -> Result<Self> {
        Self::prepare_inner(source, context, options, policy, false)
    }

    pub fn prepare_postgres(source: &str, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32]) -> Result<Self> {
        let parsed = one_statement(source)?;
        let count = parsed.parameter_count();
        if parameter_oids.len() > count { return Err(parameter_error("More parameter OIDs than SQL parameters")); }
        let _parameters = context.memory.reserve(count.checked_mul(1024).and_then(|n| n.checked_add(1024)).ok_or_else(size_error)?)?;
        let mut options = PrepareOptions { search_path, ..Default::default() };
        let mut dynamic = false;
        for index in 0..count {
            match PgType::from_oid(parameter_oids.get(index).copied().unwrap_or(0)).and_then(PgType::logical_type) {
                Some(ty) => { options.parameter_types.insert(index, ty.maybe()); }
                None => { dynamic = true; }
            }
        }
        Self::prepare_inner(source, context, options, ResultSignaturePolicy::PostgreSql, dynamic)
    }

    fn prepare_inner(source: &str, context: &Arc<QueryContext>, options: PrepareOptions, policy: ResultSignaturePolicy, dynamic_pg: bool) -> Result<Self> {
        context.check_cancelled()?;
        let source_reservation = context.memory.reserve(source.len().checked_add(256).ok_or_else(size_error)?)?;
        let parsed = one_statement(source)?;
        let count = parsed.parameter_count();
        if options.parameter_types.keys().any(|index| *index >= count) { return Err(parameter_error("Declared parameter lies outside the statement")); }
        let parameter_bytes = options.parameter_types.values().try_fold(count.checked_mul(1024).and_then(|n| n.checked_add(1024)).ok_or_else(size_error)?, |bytes, ty| bytes.checked_add(ty.retained_bytes()?).ok_or_else(size_error))?;
        let _parameters = context.memory.reserve(parameter_bytes)?;
        let mut parameters = (0..count).map(|index| options.parameter_types.get(&index).cloned().unwrap_or(LogicalType::Null)).collect::<Vec<_>>();
        let result = match parsed.kind() {
            StatementKind::Dml => PreparedResult::Dml,
            StatementKind::Query => {
                let prepared = rtdb_sql::prepare(source, &context.catalog, &options)?;
                for (index, ty) in &prepared.parameter_types {
                    if !options.parameter_types.contains_key(index) { parameters[*index] = ty.clone(); }
                }
                let Statement::Query(relation) = prepared.statement else { return Err(kind_error()); };
                let schema = relation.schema().clone();
                let pg_types = schema.fields().iter().map(|field| if dynamic_pg || policy != ResultSignaturePolicy::PostgreSql { Ok(PgType::Default) } else { PgType::from_logical(&field.data_type) }).collect::<Result<_>>()?;
                PreparedResult::Query { schema, pg_types }
            }
            StatementKind::Session => return Err(Anomaly::unsupported("rtdb.node/prepared-session-statement", "Session-control statements execute through the session API")),
        };
        let source = source.to_owned();
        let bytes = retained_bytes(&source, &options, &parameters, &result)?;
        let reservation = context.memory.reserve(bytes)?;
        drop(source_reservation);
        Ok(Self(Arc::new(PreparedData { source, options, parameters, result, policy, _reservation: reservation })))
    }

    pub fn source(&self) -> &str { &self.0.source }
    pub fn parameter_count(&self) -> usize { self.0.parameters.len() }
    pub fn parameter_types(&self) -> &[LogicalType] { &self.0.parameters }
    pub fn is_dml(&self) -> bool { matches!(&self.0.result, PreparedResult::Dml) }
    pub fn prepared_schema(&self) -> Option<&Schema> { match &self.0.result { PreparedResult::Query { schema, .. } => Some(schema), PreparedResult::Dml => None } }
    pub fn postgres_types(&self) -> Option<&[PgType]> { match &self.0.result { PreparedResult::Query { pg_types, .. } => Some(pg_types), PreparedResult::Dml => None } }

    /// ADBC parameters are positional, have empty field names, and publish unknown nullable types.
    pub fn parameter_schema(&self) -> Result<Schema> {
        Schema::new((0..self.parameter_count()).map(|index| Ok(Field { id: ColumnId(u32::try_from(index).map_err(|_| size_error())?), name: String::new(), qualifier: None, qualified_only: false, data_type: LogicalType::Null })).collect::<Result<_>>()?)
    }

    /// Returns the current Arrow signature without opening an execution cursor.
    pub fn result_schema(&self, context: &Arc<QueryContext>, argument_types: &[LogicalType]) -> Result<Schema> {
        Ok(self.bind(context, argument_types)?.schema().clone())
    }

    pub fn open_query(&self, context: Arc<QueryContext>, mut arguments: Vec<Value>) -> Result<QueryCursor> {
        if arguments.len() != self.parameter_count() { return Err(parameter_error("Argument count differs from the prepared statement")); }
        for (index, ty) in &self.0.options.parameter_types { arguments[*index] = rtdb_expr::cast(&arguments[*index], ty, &context.timezone)?; }
        let types = arguments.iter().map(Value::logical_type).collect::<Vec<_>>();
        let query = self.bind(&context, &types)?;
        self.check_signature(query.schema())?;
        query.execute_with_text(context, arguments, Some(self.source()))
    }

    /// Preserves absent arguments and an explicitly empty argument batch as distinct source operations.
    /// DML planning and table lookup occur during transaction resolution.
    pub fn dml_operation(&self, arguments: Option<SqlArguments>) -> Result<TxOp> {
        if !self.is_dml() { return Err(kind_error()); }
        match &arguments {
            Some(arguments) if arguments.types().len() != self.parameter_count() => return Err(parameter_error("Argument width differs from the prepared statement")),
            None if self.parameter_count() != 0 => return Err(parameter_error("Prepared DML requires arguments")),
            _ => {}
        }
        Ok(TxOp::Sql { sql: self.0.source.clone(), args: arguments })
    }

    fn bind(&self, context: &Arc<QueryContext>, argument_types: &[LogicalType]) -> Result<PreparedQuery> {
        context.check_cancelled()?;
        if self.is_dml() { return Err(kind_error()); }
        if argument_types.len() != self.parameter_count() { return Err(parameter_error("Parameter field count differs from the prepared statement")); }
        let mut options = self.0.options.clone();
        for (index, ty) in argument_types.iter().enumerate() { options.parameter_types.entry(index).or_insert_with(|| ty.clone()); }
        let prepared = rtdb_sql::prepare(&self.0.source, &context.catalog, &options)?;
        let Statement::Query(relation) = prepared.statement else { return Err(kind_error()); };
        PreparedQuery::new(relation)
    }

    fn check_signature(&self, actual: &Schema) -> Result<()> {
        let PreparedResult::Query { schema, pg_types } = &self.0.result else { return Err(kind_error()); };
        let compatible = match self.0.policy {
            ResultSignaturePolicy::Dynamic => true,
            ResultSignaturePolicy::Exact => schema.fields().iter().map(|field| (&field.name, &field.data_type)).eq(actual.fields().iter().map(|field| (&field.name, &field.data_type))),
            ResultSignaturePolicy::PostgreSql => {
                schema.len() == actual.len() && pg_types.iter().zip(actual.fields()).map(|(prepared, field)| prepared.accepts_result(&field.data_type)).collect::<Result<Vec<_>>>()?.into_iter().all(|compatible| compatible)
            }
        };
        if compatible { Ok(()) } else { Err(Anomaly::new(AnomalyCategory::Conflict, "xtdb/prepared-query-out-of-date", "cached plan must not change result type")) }
    }
}

fn one_statement(source: &str) -> Result<rtdb_sql::ParsedStatement> {
    let mut parsed = rtdb_sql::parse(source)?;
    if parsed.len() != 1 { return Err(Anomaly::incorrect("xtdb/sql-error", "A prepared statement must contain exactly one SQL statement")); }
    Ok(parsed.remove(0))
}
fn parameter_error(message: &str) -> Anomaly { Anomaly::incorrect("xtdb/parameter-count-mismatch", message) }
fn kind_error() -> Anomaly { Anomaly::incorrect("rtdb.node/statement-kind", "Prepared statement has the wrong execution kind") }
fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.node/preparation-size", "Prepared statement retained size exceeds usize") }
fn retained_bytes(source: &String, options: &PrepareOptions, parameters: &Vec<LogicalType>, result: &PreparedResult) -> Result<usize> {
    let mut bytes = 1024usize.checked_add(source.capacity()).ok_or_else(size_error)?;
    for path in &options.search_path { bytes = bytes.checked_add(path.capacity()).and_then(|n| n.checked_add(64)).ok_or_else(size_error)?; }
    for ty in options.parameter_types.values().chain(parameters) { bytes = bytes.checked_add(ty.retained_bytes()?).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?; }
    bytes = bytes.checked_add(options.search_path.capacity().checked_mul(std::mem::size_of::<String>()).ok_or_else(size_error)?).and_then(|n| n.checked_add(parameters.capacity().checked_mul(std::mem::size_of::<LogicalType>())?)).ok_or_else(size_error)?;
    if let PreparedResult::Query { schema, pg_types } = result {
        bytes = bytes.checked_add(pg_types.capacity().checked_mul(std::mem::size_of::<PgType>()).ok_or_else(size_error)?).ok_or_else(size_error)?;
        for field in schema.fields() { bytes = bytes.checked_add(field.data_type.retained_bytes()?).and_then(|n| n.checked_add(field.name.capacity())).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?; }
    }
    Ok(bytes)
}
