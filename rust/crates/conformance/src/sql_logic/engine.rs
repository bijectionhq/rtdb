use async_trait::async_trait;
use bytes::Bytes;
use regex::Regex;
use rtdb_columnar::{MemoryReservation, estimate_read_value_bytes, read_value};
use rtdb_format::database::proto::DatabaseConfig;
use rtdb_maintenance::CollectionObserver;
use rtdb_node::{AutonomousTxOptions, DatabaseAccess, DatabaseResources, LogAccess, NativeResources, NativeSession, Node, NodeOptions, NodeServices, ResourceFactory, SessionIdentity};
use rtdb_plan::{Catalog, CatalogMetric, CatalogMetrics, CatalogRows, CatalogUser, CatalogUsers, Identifier, QualifiedTable, Relation, Statement, TableBinding};
use rtdb_query::{PreparedQuery, QueryContext};
use rtdb_storage::log::{AppendReceipt, Log, LogOffset, LogRange, LogRecord, LogTail};
use rtdb_tx::{Documents, TransactionAuthorization, TxOp};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, TableRef, TimeUnit, Timestamp, Value};
use serde_json::{Value as Json, json};
use sqlparser::{ast, dialect::GenericDialect, parser::Parser};
use std::{collections::{BTreeMap, BTreeSet}, sync::{Arc, atomic::{AtomicI64, Ordering}}, time::Duration};
use tokio_util::sync::CancellationToken;

use super::{Error, Options, Record, format};

struct Identity;
impl TransactionAuthorization for Identity { fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { Ok(()) } }
#[async_trait]
impl SessionIdentity for Identity {
    fn user(&self) -> Option<&str> { None }
    async fn revalidate(&self, _: Timestamp) -> Result<()> { Ok(()) }
    fn authorize_database(&self, _: &str, _: DatabaseAccess) -> Result<()> { Ok(()) }
}
struct RuntimeCatalog;
impl CatalogUsers for RuntimeCatalog {
    fn users(&self) -> Result<CatalogRows<CatalogUser>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) }
    fn is_superuser(&self, _: &str) -> bool { true }
}
impl CatalogMetrics for RuntimeCatalog {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) }
}
#[derive(Default)]
struct Observer(parking_lot::Mutex<Option<Anomaly>>);
impl CollectionObserver for Observer { fn table_failed(&self, _: Option<&TableRef>, error: &Anomaly) { self.0.lock().get_or_insert_with(|| error.clone()); } }

struct ClockedLog { delegate: Arc<dyn Log>, next: AtomicI64 }
#[async_trait]
impl Log for ClockedLog {
    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> rtdb_storage::Result<AppendReceipt> {
        let timestamp = if rtdb_format::envelope::SourceMessage::is_transaction_encoded(&payload).map_err(|error| rtdb_storage::Error::Task(error.to_string()))? {
            Timestamp(self.next.fetch_add(86_400_000_000, Ordering::SeqCst))
        } else { timestamp };
        self.delegate.enqueue(payload, timestamp).await
    }
    fn latest(&self) -> rtdb_storage::Result<LogOffset> { self.delegate.latest() }
    async fn tail(&self, after: LogOffset) -> rtdb_storage::Result<Box<dyn LogTail>> { self.delegate.tail(after).await }
    async fn range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Box<dyn LogRange>> { self.delegate.range(from, to).await }
    async fn read_range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Vec<LogRecord>> { self.delegate.read_range(from, to).await }
}
struct Resources(NativeResources);
#[async_trait]
impl ResourceFactory for Resources {
    fn validate(&self, name: &str, configuration: &DatabaseConfig) -> Result<()> { self.0.validate(name, configuration) }
    async fn open(&self, name: &str, configuration: &DatabaseConfig, access: LogAccess, cancellation: CancellationToken) -> Result<DatabaseResources> {
        let mut resources = self.0.open(name, configuration, access, cancellation).await?;
        resources.logs.source = Arc::new(ClockedLog { delegate: resources.logs.source, next: AtomicI64::new(1_577_836_800_000_000) });
        Ok(resources)
    }
    async fn close(&self) -> Result<()> { self.0.close().await }
}

pub(super) struct Running { pub node: Node, owner: tokio::task::JoinHandle<Result<()>>, observer: Arc<Observer> }
impl Running {
    pub async fn open() -> std::result::Result<Self, Error> {
        let mut options = NodeOptions::default();
        options.default_timezone = "Europe/London".into();
        options.automatic_gc = false;
        options.automatic_compaction = false;
        options.replication.assertion_interval = Duration::from_millis(10);
        let observer = Arc::new(Observer::default());
        let services = NodeServices { resources: Arc::new(Resources(NativeResources::default())),
            ..NodeServices::local(Arc::new(Identity), observer.clone(), Arc::new(RuntimeCatalog), Arc::new(RuntimeCatalog)) };
        let (node, driver) = Node::open(options, services).await?;
        Ok(Self { node, owner: tokio::spawn(driver.run()), observer })
    }
    pub async fn close(self) -> std::result::Result<(), Error> {
        self.node.shutdown();
        let joined = match self.owner.await { Ok(result) => result.map_err(Error::from), Err(error) => Err(Error::from(error)) };
        let observed = self.observer.0.lock().take().map_or(Ok(()), |error| Err(Error::from(error)));
        super::combine(joined, observed)
    }
}

#[derive(Clone)]
pub(super) enum Bookkeeping { Ready { tables: BTreeMap<String, Vec<String>>, views: BTreeMap<String, String> }, Nil }
impl Default for Bookkeeping {
    fn default() -> Self { Self::Ready { tables: BTreeMap::new(), views: BTreeMap::new() } }
}
impl Bookkeeping {
    fn validate_size(&self) -> Result<()> {
        let Self::Ready { tables, views } = self else { return Ok(()); };
        let sizes = tables.iter().flat_map(|(name, columns)| std::iter::once(name.len() + 256).chain(columns.iter().map(|column| column.len() + 64)))
            .chain(views.iter().map(|(name, query)| name.len().saturating_add(query.len()).saturating_add(256)));
        let bytes = sizes.fold(0usize, usize::saturating_add);
        if bytes > 16 * 1024 * 1024 { Err(gap("Synthetic bookkeeping exceeds retained size limit")) } else { Ok(()) }
    }
}
pub(super) struct Engine { node: Node, session: NativeSession, pub state: Bookkeeping }
impl Engine {
    pub fn new(node: Node) -> Result<Self> { Ok(Self { session: node.session("xtdb", Arc::new(Identity))?, node, state: Bookkeeping::default() }) }
    pub fn valid(&self) -> bool { matches!(self.state, Bookkeeping::Ready { .. }) }
    async fn submit(&mut self, operation: TxOp) -> Result<()> {
        let result = self.session.state_mut().execute_tx(vec![operation], AutonomousTxOptions::default()).await?;
        if result.committed() { Ok(()) } else { Err(result.error().cloned().unwrap_or_else(|| Anomaly::fault("xtdb/tx-aborted", "Transaction aborted"))) }
    }
    pub async fn statement(&mut self, sql: &str, options: &Options) -> Result<(Bookkeeping, &'static str)> {
        let mut next = self.state.clone();
        if Regex::new(r"(?is)^\s*CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*\((.+)\)\s*$").expect("constant regex").is_match(sql) {
            return Ok((next, "ignored_index"));
        }
        if options.direct_sql {
            self.submit(TxOp::Sql { sql: sql.into(), args: None }).await?;
            return Ok((next, "direct_sql"));
        }
        let Bookkeeping::Ready { tables, views } = &mut next else { return Err(gap("Synthetic statement requires an available engine")); };
        if let Some(captures) = Regex::new(r"(?is)^\s*CREATE\s+TABLE\s+(\w+)\s*\((.+)\)\s*$").expect("constant regex").captures(sql) {
            if !sql.starts_with("CREATE TABLE") && !captures[2].split(',').any(|column| column.split_whitespace().count() > 1) {
                return Ok((Bookkeeping::Nil, "unhandled_visitor_returns_nil"));
            }
            let name = captures[1].to_owned();
            if tables.contains_key(&name) { return Err(Anomaly::incorrect("rtdb.conformance/slt-assertion", "Synthetic table already exists")); }
            tables.insert(name, captures[2].split(',').filter_map(|column| column.split_whitespace().next().map(str::to_owned)).collect());
            next.validate_size()?;
            return Ok((next, "synthetic_create_table"));
        }
        if let Some(captures) = Regex::new(r"(?is)^\s*CREATE\s+VIEW\s+(\w+)\s+AS\s+(.+)\s*$").expect("constant regex").captures(sql) {
            let name = captures[1].to_owned();
            if views.contains_key(&name) { return Err(Anomaly::incorrect("rtdb.conformance/slt-assertion", "Synthetic view already exists")); }
            views.insert(name, captures[2].to_owned());
            next.validate_size()?;
            return Ok((next, "synthetic_create_view"));
        }
        if sql.starts_with("CREATE TABLE") || sql.starts_with("CREATE VIEW") {
            return Err(Anomaly::incorrect("rtdb.conformance/slt-create-record", "Synthetic CREATE regex did not produce a record"));
        }
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| Anomaly::incorrect("xtdb.sql/parse-error", error.to_string()))?;
        if statements.len() != 1 { return Err(gap("Non-direct parser produced other than one statement")); }
        let ast::Statement::Insert(insert) = statements.remove(0) else {
            return Ok((Bookkeeping::Nil, "unhandled_visitor_returns_nil"));
        };
        let Some(query) = &insert.source else { return Err(gap("Non-direct INSERT has no source")); };
        let ast::SetExpr::Values(values) = query.body.as_ref() else {
            self.submit(TxOp::Sql { sql: sql.into(), args: None }).await?;
            return Ok((next, "insert_subquery_sql"));
        };
        let ast::TableObject::TableName(name) = &insert.table else { return Err(gap("Non-direct INSERT target is not a named table")); };
        let name = object_name(name)?;
        let columns = if insert.columns.is_empty() { tables.get(&name).cloned().unwrap_or_default() }
            else { insert.columns.iter().map(object_name).collect::<Result<Vec<_>>>()? };
        let mut documents = Vec::new();
        for row in &values.rows {
            if row.len() == 1 && columns.len() != 1 { return Err(Anomaly::incorrect("rtdb.conformance/slt-assertion", "Single-expression insert requires exactly one synthetic column")); }
            let mut document = BTreeMap::from([("_id".into(), Value::Uuid(*uuid::Uuid::new_v4().as_bytes()))]);
            for (column, expression) in columns.iter().zip(row) { document.insert(Identifier::unquoted(column).normalized(), literal(expression)?); }
            documents.push(document);
        }
        self.submit(TxOp::PutDocs { table: TableRef::new("public", name), documents: Documents::new(documents, self.node.memory())?, valid_from: None, valid_to: None }).await?;
        Ok((next, "synthetic_insert_documents"))
    }
    pub async fn query(&mut self, record: &Record, variables: &BTreeMap<String, Option<String>>, options: &Options) -> Result<(Json, Vec<Arc<MemoryReservation>>)> {
        let mut basis = self.session.state_mut().read_basis().await?;
        basis.timezone = "UTC".into();
        if let Some(Some(value)) = variables.get("CURRENT_TIMESTAMP") {
            basis.current_time = Timestamp(chrono::DateTime::parse_from_rfc3339(value).map_err(|error| Anomaly::incorrect("rtdb.conformance/slt-clock", error.to_string()))?.timestamp_micros());
        }
        let token = self.session.state().cancellation().child_token();
        let context = self.session.query_context_at(&basis, &token).await?;
        let sql = record.sql.as_deref().unwrap_or("");
        let catalog = HiddenIdCatalog { delegate: &context.catalog, hide: !options.direct_sql };
        let statement = rtdb_sql::prepare(sql, &catalog, &rtdb_sql::PrepareOptions::default())?.statement;
        let Statement::Query(relation) = statement else { return Err(Anomaly::incorrect("xtdb/not-a-query", "Statement is not a query")); };
        let relation = if options.direct_sql { relation } else { restore_dependencies(&relation, &context)? };
        let mut cursor = PreparedQuery::new(relation)?.execute(context.clone(), vec![])?;
        let schema = cursor.schema().fields().iter().map(|field| json!({"name":field.name,"type":field.data_type})).collect::<Vec<_>>();
        let mut rows = Vec::new();
        let mut charges: Vec<Arc<MemoryReservation>> = Vec::new();
        let mut bytes = 0usize;
        let mut actual_values = 0usize;
        let types = record.type_string.as_deref().unwrap_or("").as_bytes();
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            for row in 0..batch.num_rows() {
                if rows.len() >= options.max_rows { return Err(gap("SLT result exceeded configured row retention limit")); }
                let mut bound = 1024usize;
                for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
                    bound = bound.checked_add(estimate_read_value_bytes(field, array.as_ref(), row)?.checked_mul(8).ok_or_else(|| gap("SLT row size overflow"))?).ok_or_else(|| gap("SLT row size overflow"))?;
                }
                bytes = bytes.checked_add(bound).ok_or_else(|| gap("SLT result size overflow"))?;
                if bytes > options.max_result_bytes { return Err(gap("SLT result exceeded configured byte retention limit")); }
                charges.push(context.memory.reserve(bound)?);
                let mut output = Vec::new();
                actual_values += batch.num_columns();
                for ((kind, field), array) in types.iter().zip(batch.schema().fields()).zip(batch.columns()) {
                    let value = read_value(field, array.as_ref(), row)?;
                    output.push(format::cell(*kind, &value)?);
                }
                rows.push(output);
            }
        }
        let row_count = rows.len();
        let result = format::finish(rows, record.sort.as_deref().unwrap_or("nosort"))?;
        Ok((json!({"text":result,"rows":row_count,"values":actual_values,"schema":schema,
                  "basis":{"current_time_micros":basis.current_time.0,"timezone":"UTC","snapshot_token":basis.snapshot_token()}}), charges))
    }
}

struct HiddenIdCatalog<'a> { delegate: &'a dyn Catalog, hide: bool }
impl HiddenIdCatalog<'_> { fn filter(&self, mut binding: TableBinding) -> TableBinding { if self.hide { binding.columns.remove("_id"); } binding } }
impl Catalog for HiddenIdCatalog<'_> {
    fn default_database(&self) -> &str { self.delegate.default_database() }
    fn databases(&self) -> BTreeSet<String> { self.delegate.databases() }
    fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(self.delegate.table(name)?.map(|binding| self.filter(binding))) }
    fn tables(&self, database: &str) -> Result<Vec<TableBinding>> { Ok(self.delegate.tables(database)?.into_iter().map(|binding| self.filter(binding)).collect()) }
}

fn restore_dependencies(relation: &Relation, context: &QueryContext) -> Result<Arc<Relation>> {
    fn visit(value: &mut Json, catalog: &dyn Catalog) -> Result<()> {
        if let Some(table) = value.get_mut("node").and_then(|node| node.get_mut("Scan")).and_then(|scan| scan.get_mut("table")) {
            let binding: TableBinding = serde_json::from_value(table.clone()).map_err(|error| gap(&error.to_string()))?;
            *table = serde_json::to_value(catalog.table(&binding.name)?.ok_or_else(|| gap("Prepared table disappeared from its own snapshot"))?).map_err(|error| gap(&error.to_string()))?;
        }
        match value { Json::Object(fields) => { for child in fields.values_mut() { visit(child, catalog)?; } }, Json::Array(values) => { for child in values { visit(child, catalog)?; } }, _ => {} }
        Ok(())
    }
    let mut value = serde_json::to_value(relation).map_err(|error| gap(&error.to_string()))?;
    visit(&mut value, &context.catalog)?;
    Ok(Arc::new(serde_json::from_value(value).map_err(|error| gap(&error.to_string()))?))
}
fn object_name(name: &ast::ObjectName) -> Result<String> {
    let parts = name.0.iter().map(|part| part.as_ident().map(|identifier| Identifier { text: identifier.value.clone(), quoted: identifier.quote_style.is_some() }.normalized()).ok_or_else(|| gap("Non-identifier object name"))).collect::<Result<Vec<_>>>()?;
    Ok(parts.join("."))
}
fn literal(expression: &ast::Expr) -> Result<Value> {
    match expression {
        ast::Expr::Value(value) => match &value.value {
            ast::Value::Null => Ok(Value::Null), ast::Value::Boolean(value) => Ok(Value::Bool(*value)),
            ast::Value::Number(value, _) => if value.contains(['.', 'e', 'E']) { value.parse().map(Value::F64).map_err(|_| gap("Invalid floating literal")) } else { value.parse().map(Value::I64).map_err(|_| gap("Integer literal exceeds pinned long range")) },
            ast::Value::SingleQuotedString(value) => Ok(Value::Utf8(value.clone())),
            ast::Value::DollarQuotedString(value) => Ok(Value::Utf8(value.value.clone())),
            ast::Value::HexStringLiteral(value) => {
                if value.len() % 2 != 0 { return Err(Anomaly::incorrect("xtdb.sql/parse-error", "Odd-length binary literal")); }
                (0..value.len()).step_by(2).map(|index| u8::from_str_radix(&value[index..index + 2], 16).map_err(|_| Anomaly::incorrect("xtdb.sql/parse-error", "Invalid binary literal"))).collect::<Result<Vec<_>>>().map(Value::Binary)
            }
            _ => Err(gap("Unqualified synthetic insert literal")),
        },
        ast::Expr::Nested(expression) => literal(expression),
        ast::Expr::UnaryOp { op: ast::UnaryOperator::Plus, expr } => literal(expr),
        ast::Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } => {
            if let ast::Expr::Value(value) = expr.as_ref() {
                if matches!(&value.value, ast::Value::Number(value, _) if value == "9223372036854775808") { return Ok(Value::I64(i64::MIN)); }
            }
            match literal(expr)? { Value::I64(value) => value.checked_neg().map(Value::I64).ok_or_else(|| gap("Synthetic negation overflow")), Value::F64(value) => Ok(Value::F64(-value)), _ => Err(expression_data_error()) }
        }
        ast::Expr::Array(array) => array.elem.iter().map(literal).collect::<Result<Vec<_>>>().map(Value::List),
        ast::Expr::Dictionary(fields) => fields.iter().map(|field| Ok((Identifier { text: field.key.value.clone(), quoted: field.key.quote_style.is_some() }.normalized(), literal(&field.value)?))).collect::<Result<BTreeMap<_, _>>>().map(Value::Struct),
        ast::Expr::TypedString(value) => {
            let data_type = match value.data_type.to_string().to_uppercase().as_str() {
                "DATE" => ScalarType::DateDay,
                "TIME" => ScalarType::Time { unit: TimeUnit::Nanosecond, bit_width: 64 },
                "TIMESTAMP" => ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: None },
                "UUID" => ScalarType::Uuid,
                _ => return Err(gap("Unqualified synthetic typed string")),
            };
            let ast::Value::SingleQuotedString(value) = &value.value.value else { return Err(gap("Unqualified synthetic typed string form")); };
            rtdb_expr::cast(&Value::Utf8(value.clone()), &LogicalType::Scalar(data_type), "Europe/London")
        }
        ast::Expr::BinaryOp { .. } | ast::Expr::Function(_) | ast::Expr::Cast { .. } | ast::Expr::UnaryOp { .. } => Err(expression_data_error()),
        _ => Err(gap("Unqualified synthetic INSERT expression-data form")),
    }
}
fn expression_data_error() -> Anomaly { Anomaly::unsupported("rtdb.conformance/slt-symbol-value", "Pinned synthetic insert stores an unevaluated expression list whose Symbol has no Arrow value type") }
pub(super) fn gap(message: &str) -> Anomaly { Anomaly::unsupported("rtdb.conformance/slt-harness-gap", message) }

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn synthetic_literal_translation_does_not_evaluate_arithmetic() {
        let statements = Parser::parse_sql(&GenericDialect {}, "INSERT INTO t VALUES(NULL,-102,TRUE,'101',104.5)").unwrap();
        let ast::Statement::Insert(insert) = &statements[0] else { panic!() };
        let ast::SetExpr::Values(values) = insert.source.as_ref().unwrap().body.as_ref() else { panic!() };
        assert_eq!(values.rows[0].iter().map(literal).collect::<Result<Vec<_>>>().unwrap(), vec![Value::Null, Value::I64(-102), Value::Bool(true), Value::Utf8("101".into()), Value::F64(104.5)]);
        let statements = Parser::parse_sql(&GenericDialect {}, "INSERT INTO t VALUES(100+4)").unwrap();
        let ast::Statement::Insert(insert) = &statements[0] else { panic!() };
        let ast::SetExpr::Values(values) = insert.source.as_ref().unwrap().body.as_ref() else { panic!() };
        assert_eq!(literal(&values.rows[0][0]).unwrap_err().code, "rtdb.conformance/slt-symbol-value");
    }
    #[test] fn synthetic_bookkeeping_has_a_retained_size_limit() {
        assert!(Bookkeeping::default().validate_size().is_ok());
        let state = Bookkeeping::Ready { tables: BTreeMap::new(), views: BTreeMap::from([("v".into(), "x".repeat(16 * 1024 * 1024))]) };
        assert_eq!(state.validate_size().unwrap_err().code, "rtdb.conformance/slt-harness-gap");
        assert!(Bookkeeping::Nil.validate_size().is_ok());
    }
}
