use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rtdb_plan::*;
use rtdb_types::{Anomaly, LogicalType, MemoryBudget, Result, ScalarType, TableRef, Value};
use sqlparser::ast as ast;

use crate::parse::{ParsedStatement, Syntax, TemporalClause, TemporalExtent};
use crate::PrepareOptions;
use crate::planning_diagnostic::PlanningDiagnostic;
use crate::expression::{ReplacementKey, Replacements};
use crate::subquery::{DeferredExpressionInput, ExpressionAttachment, ExpressionInput};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HiddenColumn { ExplicitOnly, Internal }

#[derive(Clone, Copy, PartialEq, Eq)]
enum TablePurpose { Query, Mutation, Erase }

#[derive(Clone, Copy, PartialEq, Eq)]
enum ListTableKind { Series, Unnest }

#[derive(Clone, Copy)]
enum RankedTable { Search, Nearest, Hybrid }

/// The reciprocal rank fusion constant of RTDB_HYBRID: a row ranked r in a ranking contributes 1 / (k + r).
const HYBRID_RANK_CONSTANT: f64 = 60.0;

impl RankedTable {
    fn from_name(name: &ast::ObjectName) -> Option<Self> {
        let [part] = name.0.as_slice() else { return None; };
        let ident = part.as_ident().filter(|ident| ident.quote_style.is_none())?;
        match ident.value.to_ascii_lowercase().as_str() {
            "rtdb_search" => Some(Self::Search),
            "rtdb_nearest" => Some(Self::Nearest),
            "rtdb_hybrid" => Some(Self::Hybrid),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum FormAllocation<'a> { Tracked(&'a Arc<MemoryBudget>), Untracked }
impl<'a> FormAllocation<'a> {
    pub(crate) fn memory(self) -> Option<&'a Arc<MemoryBudget>> {
        match self { Self::Tracked(memory) => Some(memory), Self::Untracked => None }
    }
    pub(crate) fn literal(self, value: &Value) -> Result<RegexForm> {
        let mut builder = match self { Self::Tracked(memory) => RegexFormBuilder::new_in(memory), Self::Untracked => RegexFormBuilder::new_untracked() };
        builder.literal(value)?;
        builder.finish()
    }
    pub(crate) fn query(self, form: &rtdb_xtql::Edn) -> Result<rtdb_xtql::Query> {
        match self { Self::Tracked(memory) => rtdb_xtql::parse_query_in(form, memory), Self::Untracked => rtdb_xtql::parse_query(form) }
    }
    fn compile_query(self, query: &rtdb_xtql::Query, catalog: &dyn Catalog, binder: &mut Binder, next: &mut usize, arguments: &[(usize, LogicalType)], timezone: &str) -> Result<Arc<Relation>> {
        match self { Self::Tracked(memory) => rtdb_xtql::compile_query_in(query, catalog, binder, next, arguments, timezone, memory), Self::Untracked => rtdb_xtql::compile_query(query, catalog, binder, next, arguments, timezone) }
    }
}

pub(crate) struct Context<'a> {
    pub(crate) forms: FormAllocation<'a>,
    pub(crate) literals: crate::literal_admission::LiteralAdmission,
    pub source: &'a str,
    pub catalog: &'a dyn Catalog,
    pub options: &'a PrepareOptions,
    pub cast_timezone: Option<&'a str>,
    pub binder: Binder,
    temporal: Vec<TemporalClause>,
    inline_xtql: Vec<crate::inline_xtql::InlineXtql>,
    pub(crate) wildcard_renames: Vec<crate::wildcard::WildcardRename>,
    ctes: BTreeMap<String, Cte>,
    next_symbol: u64,
    next_rewrite_symbol: u64,
    pub(crate) hidden_columns: BTreeMap<ColumnId, HiddenColumn>,
    pub(crate) outer_scopes: Vec<crate::subquery::CorrelationScope>,
    inferred_group_scopes: Vec<InferredGroupScope>,
    pub(crate) next_parameter: usize,
    period_columns: BTreeSet<String>,
    notices: Vec<SqlNotice>,
    diagnostics: Vec<PlanningDiagnostic>,
    default_valid: Option<TemporalExtent>,
    default_system: Option<TemporalExtent>,
}

#[derive(Clone)]
enum Cte { Inline(Arc<Relation>), Materialized(Arc<MaterializedBinding>) }

struct SelectProjection<'a> {
    items: &'a [ast::SelectItem],
    group_by: Option<&'a ast::GroupByExpr>,
    having: Option<&'a ast::Expr>,
    distinct: bool,
}

struct InferredGroupScope {
    source: BTreeSet<ColumnId>,
    required: BTreeSet<ColumnId>,
}

pub(crate) fn control_expression(source: &str, expression: &ast::Expr, options: &PrepareOptions, forms: FormAllocation<'_>) -> Result<Expr> {
    struct ScalarCatalog;
    impl Catalog for ScalarCatalog {
        fn default_database(&self) -> &str { "xtdb" }
        fn databases(&self) -> BTreeSet<String> { BTreeSet::new() }
        fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Err(error("Control expression cannot read a table")) }
        fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Err(error("Control expression cannot read a table")) }
    }
    let mut context = Context { forms, literals: Default::default(), source, catalog: &ScalarCatalog, options, cast_timezone: None, binder: Binder::default(), temporal: Vec::new(), inline_xtql: Vec::new(), wildcard_renames: Vec::new(), ctes: BTreeMap::new(), next_symbol: 0, next_rewrite_symbol: 0, hidden_columns: BTreeMap::new(), outer_scopes: Vec::new(), inferred_group_scopes: Vec::new(), next_parameter: 0, period_columns: BTreeSet::new(), notices: Vec::new(), diagnostics: Vec::new(), default_valid: None, default_system: None };
    context.admit_expression_literals(expression)?;
    let expression = context.expr(expression, &Schema::default(), &BTreeMap::new())?;
    context.literals.check()?;
    Ok(expression)
}

pub(crate) fn bind(parsed: ParsedStatement<'_>, catalog: &dyn Catalog, options: &PrepareOptions) -> Result<PreparedStatement> {
    let source = parsed.source();
    bind_using(parsed, catalog, options, |context, statement, settings| {
        let statement = context.statement(statement);
        Ok(PreparedStatement {
            statement: context.check_bound(statement)?,
            parameter_types: context.binder.parameter_types().clone(),
            source: source.to_owned(),
            notices: std::mem::take(&mut context.notices),
            settings,
        })
    })
}

/// Whether `parsed` is a statement `bind_static_document_source` binds at all: records, or an INSERT of plain VALUES with named columns.
/// Any other statement has no static source, whatever the catalog and options.
pub(crate) fn has_static_document_shape(parsed: &ParsedStatement<'_>) -> bool {
    match &parsed.tree.statement {
        Syntax::Records { .. } => true,
        Syntax::Standard(ast::Statement::Insert(insert)) => !insert.columns.is_empty() && insert.source.as_ref().is_some_and(|query|
            matches!(query.body.as_ref(), ast::SetExpr::Values(_)) && query.with.is_none() && query.order_by.is_none() && query.limit_clause.is_none()),
        _ => false,
    }
}

pub(crate) fn bind_static_document_source(
    parsed: ParsedStatement<'_>,
    catalog: &dyn Catalog,
    options: &PrepareOptions,
) -> Result<Option<crate::StaticMutation>> {
    use crate::{StaticMutation, StaticMutationKind};
    if !has_static_document_shape(&parsed) { return Ok(None); }
    bind_using(parsed, catalog, options, |context, statement, _settings| {
        if context.literals.has_errors() { return Ok(None); }
        let result = match statement {
            Syntax::Records { patch, table, records } => context.record_rows(*patch, table, records).map(Some),
            Syntax::Standard(ast::Statement::Insert(insert)) => {
                match context.insert_rows(insert) {
                    Ok(InsertRows { table, input }) => Ok(Some(StaticMutation { table, input, kind: StaticMutationKind::Put })),
                    Err(InsertRowsError::Projection { .. }) => Ok(None),
                    Err(InsertRowsError::Other(error)) => Err(error),
                }
            }
            _ => return Ok(None),
        };
        context.check_bound(result)
    })
}

fn bind_using<T>(
    parsed: ParsedStatement<'_>,
    catalog: &dyn Catalog,
    options: &PrepareOptions,
    bind: impl FnOnce(&mut Context<'_>, &Syntax, QuerySettings) -> Result<T>,
) -> Result<T> {
    let (input, span, forms) = (parsed.tree.source.input, parsed.span, parsed.forms);
    bind_parsed(parsed, catalog, options, bind).map_err(|error| grammar_report(error, input, span, forms))
}

/// Replaces a binder-detected grammar rejection with the reference listener's report when the ported grammar produces one.
/// The statement keeps its line and column: text before it is blanked rather than removed.
fn grammar_report(error: Anomaly, input: &str, span: SourceSpan, forms: FormAllocation<'_>) -> Anomaly {
    if !is_grammar_rejection(&error) { return error; }
    let mut source: String = input[..span.start].chars().map(|ch| if matches!(ch, '\n' | '\r') { ch } else { ' ' }).collect();
    source.push_str(&input[span.start..span.end]);
    let untracked;
    let memory = match forms.memory() {
        Some(memory) => memory,
        None => { untracked = MemoryBudget::new(usize::MAX); &untracked }
    };
    match crate::parser::statement_syntax_diagnostic(&source, &error, memory) {
        Ok(Some(report)) => report.anomaly().clone(),
        Ok(None) => error,
        Err(admission) => admission,
    }
}

fn bind_parsed<T>(
    parsed: ParsedStatement<'_>,
    catalog: &dyn Catalog,
    options: &PrepareOptions,
    bind: impl FnOnce(&mut Context<'_>, &Syntax, QuerySettings) -> Result<T>,
) -> Result<T> {
    let next_parameter = parsed.parameter_count();
    let forms = parsed.forms;
    let crate::parse::StatementTree { source, statement, temporal, period_columns, settings, wildcard_renames } = parsed.tree;
    let scoped_catalog = TransactionCatalog(catalog);
    let catalog: &dyn Catalog = if matches!(&statement, Syntax::Records { .. } | Syntax::CreateTable { .. } | Syntax::CreateSearchIndex { .. } | Syntax::DropSearchIndex { .. } | Syntax::CreateOrderedIndex { .. } | Syntax::DropOrderedIndex { .. } | Syntax::CreateVectorIndex { .. } | Syntax::DropVectorIndex { .. } | Syntax::GrantRole { .. } | Syntax::Erase(_) | Syntax::Assert(_, _) | Syntax::Standard(ast::Statement::Insert(_) | ast::Statement::Update(_) | ast::Statement::Delete(_))) { &scoped_catalog } else { catalog };
    let mut context = Context { forms, literals: Default::default(), source: source.input, catalog, options, cast_timezone: Some(&options.default_timezone), binder: Binder::default(), temporal, inline_xtql: parsed.inline_xtql, wildcard_renames, ctes: BTreeMap::new(), next_symbol: 0, next_rewrite_symbol: 0, hidden_columns: BTreeMap::new(), outer_scopes: Vec::new(), inferred_group_scopes: Vec::new(), next_parameter, period_columns, notices: Vec::new(), diagnostics: Vec::new(), default_valid: None, default_system: None };
    let (settings, valid, system) = settings.bind(&mut context)?;
    context.default_valid = valid;
    context.default_system = system;
    context.admit_statement_literals(&statement)?;
    bind(&mut context, &statement, settings)
}

pub(crate) struct InsertRows {
    pub(crate) table: QualifiedTable,
    pub(crate) input: Arc<Relation>,
}

enum InsertRowsError {
    Projection { expected: usize, given: Vec<usize>, declares_id: bool },
    Other(Anomaly),
}

impl From<Anomaly> for InsertRowsError {
    fn from(error: Anomaly) -> Self { Self::Other(error) }
}

fn insert_missing_id() -> PlanningDiagnostic {
    PlanningDiagnostic::new("INSERT does not contain mandatory _id column")
}

struct TransactionCatalog<'a>(&'a dyn Catalog);

impl Catalog for TransactionCatalog<'_> {
    fn default_database(&self) -> &str { self.0.default_database() }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from([self.default_database().to_owned()]) }
    fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> {
        if name.database != self.default_database() { return Ok(None); }
        self.0.table(name)
    }
    fn tables(&self, database: &str) -> Result<Vec<TableBinding>> {
        if database != self.default_database() { return Ok(Vec::new()); }
        self.0.tables(database)
    }
    fn transaction_scoped(&self) -> bool { true }
}

impl Context<'_> {
    fn check_bound<T>(&mut self, result: Result<T>) -> Result<T> {
        self.check_diagnostics(result.as_ref().err())?;
        let value = result?;
        self.literals.check()?;
        Ok(value)
    }

    fn check_diagnostics(&mut self, terminal: Option<&Anomaly>) -> Result<()> {
        if terminal.is_some_and(|error| !error.is_caller_error() || error.code.starts_with("rtdb/") || is_grammar_rejection(error)) {
            return Ok(());
        }
        let mut errors = std::mem::take(&mut self.diagnostics).into_iter();
        if let Some(first) = errors.next() {
            let terminal = terminal.map(|error| {
                let mut diagnostic = PlanningDiagnostic::new(error.message.as_deref().unwrap_or(&error.code));
                for (key, value) in &error.context {
                    diagnostic = diagnostic.with_detail(key.clone(), value.clone());
                }
                diagnostic
            });
            return Err(first.into_anomaly(errors.chain(terminal)));
        }
        Ok(())
    }

    fn planning_failure(&mut self, cause: PlanningDiagnostic) -> Anomaly {
        let mut prior = std::mem::take(&mut self.diagnostics).into_iter();
        match prior.next() {
            Some(first) => first.into_anomaly(prior.chain([cause])),
            None => cause.into_anomaly([]),
        }
    }

    /// A name no scope has fails the statement, unless earlier diagnostics already doom it; then it joins them and binds as NULL.
    pub(crate) fn missing_column(&mut self, names: &[Identifier]) -> Result<Expr> {
        let missing = Unresolved::Missing.into_anomaly(names);
        if self.diagnostics.is_empty() { return Err(missing); }
        let chain = names.iter().rev().map(Identifier::normalized).collect::<Vec<_>>();
        self.diagnostics.push(PlanningDiagnostic::new(missing.message.unwrap_or(missing.code))
            .with_detail("chain", serde_json::json!(chain)));
        Ok(Expr::Literal(Value::Null))
    }

    pub(crate) fn record_column_demand(&mut self, id: ColumnId) {
        for scope in &mut self.inferred_group_scopes {
            if scope.source.contains(&id) {
                scope.required.insert(id);
            }
        }
    }

    fn with_inferred_group_scope<T>(
        &mut self, source: &Schema, bind: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<(T, BTreeSet<ColumnId>)> {
        self.inferred_group_scopes.push(InferredGroupScope {
            source: source.ids(),
            required: BTreeSet::new(),
        });
        let result = bind(self);
        let scope = self.inferred_group_scopes.pop().expect("Inference scope installed by caller");
        Ok((result?, scope.required))
    }

    fn row_count(&mut self,expression: &ast::Expr) -> Result<Expr> {
        let valid = matches!(expression,ast::Expr::Value(value) if matches!(&value.value,ast::Value::Placeholder(_)) || matches!(&value.value,ast::Value::Number(text,_) if !text.is_empty() && text.bytes().all(|byte|byte.is_ascii_digit())));
        if !valid { return Err(error("LIMIT and OFFSET require an unsigned integer or parameter")); }
        self.expr(expression,&Schema::default(),&BTreeMap::new())
    }
    pub(crate) fn cast_target(&self, target: &ast::DataType) -> Result<CastTarget> { crate::types::cast_target(target, self.cast_timezone) }
    fn statement(&mut self, statement: &Syntax) -> Result<Statement> {
        match statement {
            Syntax::Standard(statement) => self.standard(statement),
            Syntax::Erase(delete) => self.delete(delete, true),
            Syntax::CreateTable { table, columns } => {
                let mut seen = BTreeSet::new();
                let columns = columns.iter().map(|(name, ty)| {
                    let name = name.normalized();
                    if !seen.insert(name.clone()) { return Err(error("CREATE TABLE repeats a column")); }
                    if name.starts_with('_') && name != "_id" { return Err(error("CREATE TABLE cannot declare internal columns")); }
                    let ty = ty.as_ref().map(crate::types::data_type).transpose()?.map_or(LogicalType::Nothing, LogicalType::maybe);
                    Ok((name, ty))
                }).collect::<Result<_>>()?;
                Ok(Statement::Dml(Dml::CreateTable { table: self.target(table)?, columns }))
            }
            Syntax::GrantRole { user, role, revoke } => Ok(Statement::Dml(Dml::GrantRole { user: user.normalized(), role: role.normalized(), revoke: *revoke })),
            Syntax::CreateSearchIndex { name, table, columns, analyzer, history, if_not_exists } => {
                let columns: Vec<_> = columns.iter().map(Identifier::normalized).collect();
                if columns.iter().collect::<BTreeSet<_>>().len() != columns.len() { return Err(error("CREATE SEARCH INDEX repeats a column")); }
                if columns.iter().any(|name| name.starts_with('_')) { return Err(error("CREATE SEARCH INDEX cannot index internal columns")); }
                Ok(Statement::Dml(Dml::CreateSearchIndex { name: name.normalized(), table: self.target(table)?, columns, analyzer: *analyzer, history: *history, if_not_exists: *if_not_exists }))
            }
            Syntax::DropSearchIndex { name } => Ok(Statement::Dml(Dml::DropSearchIndex { name: name.normalized() })),
            Syntax::CreateOrderedIndex { name, table, columns, if_not_exists } => {
                let columns: Vec<_> = columns.iter().map(Identifier::normalized).collect();
                if columns.iter().collect::<BTreeSet<_>>().len() != columns.len() { return Err(error("CREATE INDEX repeats a column")); }
                if columns.iter().any(|name| name.starts_with('_') && name != "_id") { return Err(error("CREATE INDEX cannot index internal columns")); }
                Ok(Statement::Dml(Dml::CreateOrderedIndex { name: name.normalized(), table: self.target(table)?, columns, if_not_exists: *if_not_exists }))
            }
            Syntax::DropOrderedIndex { name } => Ok(Statement::Dml(Dml::DropOrderedIndex { name: name.normalized() })),
            Syntax::CreateVectorIndex { name, table, column, partition, dimensions, method, recall, if_not_exists } => {
                let column = column.normalized();
                let partition: Vec<_> = partition.iter().map(Identifier::normalized).collect();
                if column.starts_with('_') || partition.iter().any(|name| name.starts_with('_')) { return Err(error("CREATE VECTOR INDEX cannot index internal columns")); }
                if partition.iter().collect::<BTreeSet<_>>().len() != partition.len() || partition.contains(&column) {
                    return Err(error("CREATE VECTOR INDEX requires distinct partition columns other than the vector column"));
                }
                Ok(Statement::Dml(Dml::CreateVectorIndex { name: name.normalized(), table: self.target(table)?, column, partition,
                    dimensions: *dimensions, method: method.clone(), recall: *recall, if_not_exists: *if_not_exists }))
            }
            Syntax::DropVectorIndex { name } => Ok(Statement::Dml(Dml::DropVectorIndex { name: name.normalized() })),
            Syntax::Records { patch, table, records } => {
                use crate::{StaticMutation, StaticMutationKind};
                let StaticMutation { table, input, kind } = self.record_rows(*patch, table, records)?;
                Ok(Statement::Dml(match kind {
                    StaticMutationKind::Put => {
                        self.validate_insert_columns(&input)?;
                        Dml::Insert { table, input }
                    }
                    StaticMutationKind::Patch(valid_time) => {
                        if !input.schema().fields().iter().any(|field| field.name == "_id") {
                            return Err(error("RECORDS requires an _id column"));
                        }
                        let forbidden: Vec<_> = input.schema().fields().iter().map(|field| field.name.as_str())
                            .filter(|name| name.starts_with('_') && *name != "_id").collect();
                        if !forbidden.is_empty() {
                            return Err(self.planning_failure(PlanningDiagnostic::new(format!("Cannot PATCH ({}) column", forbidden.join(" ")))));
                        }
                        Dml::Patch { table, input, valid_time }
                    }
                }))
            }
            Syntax::Set { name, value, local } => Ok(Statement::Set { name: name.clone(), value: self.expr(value, &Schema::default(), &BTreeMap::new())?, local: *local }),
            Syntax::Show(name) => Ok(Statement::Show { name: name.clone() }),
            Syntax::Assert(expression, message) => {
                let mut input = Arc::new(Relation::unit());
                let predicate = self.relational_expr(expression, &mut input, &BTreeMap::new())?;
                Ok(Statement::Dml(Dml::Assert { input: Arc::new(input.filter(predicate)?), message: message.clone() }))
            }
        }
    }

    fn record_rows(&mut self, patch: bool, table: &ast::ObjectName, records: &[ast::Expr]) -> Result<crate::StaticMutation> {
        let input = self.bind_records(records).and_then(|records| self.records(records))?;
        let target = self.target(table)?;
        let (valid_time, system_time, _) = self.temporal_filters(table)?;
        if system_time != TemporalFilter::Default || (!patch && valid_time != TemporalFilter::Default) {
            return Err(error("Invalid temporal clause for RECORDS"));
        }
        let kind = if patch { crate::StaticMutationKind::Patch(valid_time) } else { crate::StaticMutationKind::Put };
        Ok(crate::StaticMutation { table: target, input, kind })
    }

    fn bind_records(&mut self, records: &[ast::Expr]) -> Result<Vec<Expr>> {
        records.iter().map(|expr| self.expr(expr, &Schema::default(), &BTreeMap::new())).collect()
    }

    fn records_query(&mut self, records: &[ast::Expr]) -> Result<Arc<Relation>> {
        let records = self.bind_records(records)?;
        if records.iter().any(|record| matches!(record, Expr::Parameter { data_type, .. } if data_type.legs().is_empty())) {
            return Err(Anomaly::incorrect("xtdb.sql/records-param-outside-DML", "RECORDS ? not supported outside of DML"));
        }
        self.records(records)
    }

    fn records(&mut self, expressions: Vec<Expr>) -> Result<Arc<Relation>> {
        let mut types = BTreeMap::<String, LogicalType>::new();
        let mut order = Vec::new();
        let mut register = |name: String, ty: LogicalType| match types.entry(name) {
            std::collections::btree_map::Entry::Occupied(mut entry) => { let joined = entry.get().join(&ty); entry.insert(joined); }
            std::collections::btree_map::Entry::Vacant(entry) => { order.push(entry.key().clone()); entry.insert(ty); }
        };
        for expression in &expressions {
            if let Expr::Struct(fields) = expression {
                for (name, value) in fields { register(name.clone(), value.data_type()); }
                continue;
            }
            for leg in expression.data_type().legs() {
                let LogicalType::Struct(fields) = leg else { return Err(error("RECORDS requires document values")); };
                for (name, ty) in fields { register(name, ty); }
            }
        }
        let fields = order.into_iter().map(|name| {
            let ty = types.remove(&name).expect("ordered column has a type");
            self.binder.field(name, None, ty.maybe())
        }).collect::<Result<Vec<_>>>()?;
        let rows = expressions.into_iter().map(|expr| fields.iter().map(|field| Expr::Field {
            expr: Box::new(expr.clone()), name: field.name.clone(), data_type: field.data_type.clone(),
        }).collect()).collect();
        Ok(Arc::new(Relation::new(Schema::new(fields)?, RelationNode::Values { rows })?))
    }

    fn insert_rows(&mut self, insert: &ast::Insert) -> std::result::Result<InsertRows, InsertRowsError> {
        if insert.returning.is_some() { return Err(unsupported("INSERT RETURNING").into()); }
        if insert.on.is_some() || insert.overwrite || insert.ignore || insert.replace_into || insert.or.is_some() || !insert.assignments.is_empty() || insert.output.is_some() {
            return Err(grammar_rejected("An INSERT modifier").into());
        }
        let ast::TableObject::TableName(name) = &insert.table else { return Err(grammar_rejected("A dynamic INSERT target").into()); };
        let table = self.target(name)?;
        let source = insert.source.as_ref().ok_or_else(|| grammar_rejected("INSERT DEFAULT VALUES"))?;
        let expected = insert.columns.len();
        if let ast::SetExpr::Values(values) = source.body.as_ref() {
            if expected == 0 {
                return Err(PlanningDiagnostic::new("INSERT with VALUES needs column name list").into_anomaly([insert_missing_id()]).into());
            }
            let given = values.rows.iter().map(|row| row.expressions().len())
                .filter(|width| *width != expected).collect::<Vec<_>>();
            if !given.is_empty() {
                let declares_id = insert.columns.iter().any(|name| {
                    matches!(name.0.as_slice(), [part] if part.as_ident().is_some_and(|name| identifier(name).normalized() == "_id"))
                });
                return Err(InsertRowsError::Projection { expected, given, declares_id });
            }
        }
        let mut input = self.query(source)?;
        if expected != 0 {
            if expected != input.schema().len() {
                return Err(InsertRowsError::Projection { expected, given: vec![input.schema().len()], declares_id: false });
            }
            let names = insert.columns.iter().map(|name| {
                let names = identifiers(name)?;
                if names.len() != 1 { return Err(error("INSERT columns must be unqualified")); }
                Ok(names[0].normalized())
            }).collect::<Result<Vec<_>>>()?;
            if names.iter().collect::<BTreeSet<_>>().len() != names.len() { return Err(error("INSERT repeats a column").into()); }
            input = self.rename(input, None, Some(names))?;
        }
        Ok(InsertRows { table, input })
    }

    fn validate_insert_columns(&mut self, input: &Relation) -> Result<()> {
        use rtdb_types::transit_value::{TransitValue, to_json_value};
        if !input.schema().fields().iter().any(|field| field.name == "_id") {
            self.diagnostics.push(insert_missing_id());
        }
        for name in input.schema().fields().iter().map(|field| field.name.as_str()) {
            if name.starts_with('_') && !["_id", "_valid_from", "_valid_to"].contains(&name) {
                self.diagnostics.push(PlanningDiagnostic::new(format!("Cannot INSERT {name} column"))
                    .with_detail("col", to_json_value(&TransitValue::Symbol(name.into()))?));
            }
        }
        Ok(())
    }

    fn standard(&mut self, statement: &ast::Statement) -> Result<Statement> {
        Ok(match statement {
            ast::Statement::Query(query) => Statement::Query(self.query(query)?),
            ast::Statement::Insert(insert) => {
                let InsertRows { table, input } = match self.insert_rows(insert) {
                    Ok(rows) => rows,
                    Err(InsertRowsError::Other(error)) => return Err(error),
                    Err(InsertRowsError::Projection { expected, given, declares_id }) => {
                        let mut mismatches = given.into_iter().map(|given| PlanningDiagnostic::column_count(expected, given));
                        let last = if declares_id {
                            mismatches.next_back().expect("projection rejected with a row width mismatch")
                        } else {
                            insert_missing_id()
                        };
                        self.diagnostics.extend(mismatches);
                        return Err(self.planning_failure(last));
                    }
                };
                self.validate_insert_columns(&input)?;
                Statement::Dml(Dml::Insert { table, input })
            }
            ast::Statement::Update(update) => {
                if update.returning.is_some() { return Err(unsupported("UPDATE RETURNING")); }
                if update.from.is_some() || update.output.is_some() || !update.order_by.is_empty() || update.limit.is_some() || !update.table.joins.is_empty() { return Err(grammar_rejected("UPDATE FROM, a joined target or ordering")); }
                let mut input = self.mutation_table(&update.table.relation, false)?;
                let (target, valid_time) = scan_target(&input)?;
                let target_columns = input.schema().fields().iter().filter(|field| !matches!(field.name.as_str(), "_valid_time" | "_system_time")).map(|field| field.id).collect();
                if let Some(predicate) = &update.selection { let predicate = self.relational_expr(predicate, &mut input, &BTreeMap::new())?; input = Arc::new(input.filter(predicate)?); }
                let assignments = update.assignments.iter().map(|assignment| {
                    let ast::AssignmentTarget::ColumnName(name) = &assignment.target else { return Err(grammar_rejected("A tuple assignment")); };
                    let names = identifiers(name)?;
                    if names.len() != 1 { return Err(error("UPDATE assignment target must be a column")); }
                    let name = names[0].normalized();
                    if name.starts_with('_') { return Err(error(format!("Cannot UPDATE {name} column"))); }
                    Ok((name, self.relational_expr(&assignment.value, &mut input, &BTreeMap::new())?))
                }).collect::<Result<Vec<_>>>()?;
                if assignments.iter().map(|(name, _)| name).collect::<BTreeSet<_>>().len() != assignments.len() { return Err(error("UPDATE repeats an assignment")); }
                Statement::Dml(Dml::Update { table: target, input, target_columns, assignments, valid_time })
            }
            ast::Statement::Delete(delete) => return self.delete(delete, false),
            ast::Statement::StartTransaction { modes, statements, .. } => {
                if !statements.is_empty() { return Err(grammar_rejected("A procedural transaction block")); }
                let mut access_mode = None;
                for mode in modes {
                    match mode {
                        ast::TransactionMode::AccessMode(mode) => access_mode = Some(match mode { ast::TransactionAccessMode::ReadOnly => AccessMode::ReadOnly, ast::TransactionAccessMode::ReadWrite => AccessMode::ReadWrite }),
                        ast::TransactionMode::IsolationLevel(ast::TransactionIsolationLevel::ReadUncommitted | ast::TransactionIsolationLevel::ReadCommitted
                            | ast::TransactionIsolationLevel::RepeatableRead | ast::TransactionIsolationLevel::Serializable) => (),
                        _ => return Err(grammar_rejected("A transaction isolation level other than the four standard levels")),
                    }
                }
                Statement::Begin { access_mode }
            }
            ast::Statement::Commit { chain, .. } if !chain => Statement::Commit,
            ast::Statement::Rollback { chain, savepoint, .. } if !chain && savepoint.is_none() => Statement::Rollback,
            ast::Statement::Explain { statement, analyze, describe_alias, verbose, query_plan, estimate, format, options } => {
                if *describe_alias != ast::DescribeAlias::Explain || *verbose || *query_plan || *estimate || format.is_some() || options.is_some() { return Err(grammar_rejected("An EXPLAIN option other than ANALYZE")); }
                let Statement::Query(relation) = self.standard(statement)? else { return Err(error("EXPLAIN requires a query expression")); };
                Statement::Explain { relation, mode: if *analyze { ExplainMode::Analyze } else { ExplainMode::Plan } }
            }
            ast::Statement::Copy { .. } | ast::Statement::Prepare { .. } | ast::Statement::Execute { .. } | ast::Statement::Discard { .. }
            | ast::Statement::AttachDatabase { .. } | ast::Statement::AttachDuckDBDatabase { .. } | ast::Statement::DetachDuckDBDatabase { .. }
            | ast::Statement::Set(_) | ast::Statement::ShowVariable { .. } | ast::Statement::ShowVariables { .. } => return Err(unsupported(format!("SQL statement: {statement}"))),
            _ => return Err(grammar_rejected(format!("SQL statement {statement}"))),
        })
    }

    fn delete(&mut self, delete: &ast::Delete, erase: bool) -> Result<Statement> {
        if delete.returning.is_some() { return Err(unsupported("DELETE RETURNING")); }
        if delete.using.is_some() || delete.output.is_some() || !delete.order_by.is_empty() || delete.limit.is_some() || !delete.tables.is_empty() { return Err(grammar_rejected("DELETE USING, ordering or a multi-table target")); }
        let tables = match &delete.from { ast::FromTable::WithFromKeyword(tables) | ast::FromTable::WithoutKeyword(tables) => tables };
        if tables.len() != 1 || !tables[0].joins.is_empty() { return Err(error("DELETE and ERASE require one target table")); }
        let mut input = self.mutation_table(&tables[0].relation, erase)?;
        let RelationNode::Scan(scan) = input.node() else { return Err(error("Mutation target must be a base table")); };
        let (target, valid_time) = (scan.table.name.clone(), scan.valid_time.clone());
        if let Some(predicate) = &delete.selection { let predicate = self.relational_expr(predicate, &mut input, &BTreeMap::new())?; input = Arc::new(input.filter(predicate)?); }
        Ok(Statement::Dml(if erase { Dml::Erase { table: target, input } } else { Dml::Delete { table: target, input, valid_time } }))
    }

    pub(crate) fn query(&mut self, query: &ast::Query) -> Result<Arc<Relation>> {
        if !query.locks.is_empty() || query.for_clause.is_some() || !query.pipe_operators.is_empty() || query.settings.is_some() { return Err(grammar_rejected("Query locking, an output clause, a pipe or SETTINGS")); }
        let old_ctes = self.ctes.clone();
        let mut bindings = Vec::new();
        if let Some(with) = &query.with {
            if with.recursive { return Err(Anomaly::unsupported("xtdb.sql/recursive-ctes", "Recursive CTEs are not supported yet")); }
            for cte in &with.cte_tables {
                if !cte.alias.columns.is_empty() { return Err(unsupported("Column aliases are not supported yet")); }
                if cte.materialized == Some(ast::CteAsMaterialized::NotMaterialized) { return Err(grammar_rejected("AS NOT MATERIALIZED")); }
                let input = self.query(&cte.query)?;
                let name = identifier(&cte.alias.name).normalized();
                let symbol = self.symbol(&name)?;
                let cte = if cte.materialized == Some(ast::CteAsMaterialized::Materialized) {
                    let binding = Arc::new(MaterializedBinding { name: symbol, relation: input });
                    bindings.push(Arc::clone(&binding));
                    Cte::Materialized(binding)
                } else { Cte::Inline(input) };
                self.ctes.insert(name, cte);
            }
        }
        let integrated_order = matches!(query.body.as_ref(), ast::SetExpr::Select(_) | ast::SetExpr::FromPipeline(_));
        let mut input = match query.body.as_ref() {
            ast::SetExpr::Select(select) => self.select(select, query.order_by.as_ref())?,
            ast::SetExpr::FromPipeline(pipeline) => self.from_pipeline(pipeline, query.order_by.as_ref())?,
            body => self.set_expr(body)?,
        };
        if let Some(order) = query.order_by.as_ref().filter(|_| !integrated_order) {
            let ast::OrderByKind::Expressions(expressions) = &order.kind else { return Err(grammar_rejected("ORDER BY ALL")); };
            let fields = input.schema().fields().to_vec();
            let order = expressions.iter().map(|order| {
                if let Some(index) = ordinal(&order.expr)? {
                    let expr = fields.get(index).ok_or_else(|| error("ORDER BY position is out of range"))?.expression();
                    let descending = sort_descending(&order.options)?;
                    Ok(SortExpr { expr, descending, nulls_first: order.options.nulls_first.unwrap_or(descending) })
                } else { self.relational_sort_expr(order, &mut input, &BTreeMap::new()) }
            }).collect::<Result<_>>()?;
            input = Arc::new(input.sort(order)?);
            if input.schema().len() != fields.len() { input = Arc::new(input.project(fields.into_iter().map(|field| NamedExpr { expr: field.expression(), field }).collect())?); }
        }
        let mut offset = None;
        let mut fetch = None;
        if let Some(limit) = &query.limit_clause {
            match limit {
                ast::LimitClause::LimitOffset { limit, offset: value, limit_by } => {
                    if !limit_by.is_empty() { return Err(grammar_rejected("LIMIT BY")); }
                    fetch = limit.as_ref().map(|expr| self.row_count(expr)).transpose()?;
                    offset = value.as_ref().map(|offset| self.row_count(&offset.value)).transpose()?;
                }
                ast::LimitClause::OffsetCommaLimit { .. } => return Err(grammar_rejected("Comma LIMIT syntax")),
            }
        }
        if let Some(value) = &query.fetch {
            if fetch.is_some() { return Err(error("LIMIT and FETCH cannot both specify the row count")); }
            if value.percent || value.with_ties { return Err(grammar_rejected("FETCH PERCENT or WITH TIES")); }
            fetch = Some(value.quantity.as_ref().map(|expr| self.row_count(expr)).transpose()?.unwrap_or(Expr::Literal(Value::I64(1))));
        }
        if offset.is_some() || fetch.is_some() { input = Arc::new(input.limit(offset, fetch)?); }
        for binding in bindings.into_iter().rev() {
            input = Arc::new(Relation::new(input.schema().clone(), RelationNode::Let { binding, body: input })?);
        }
        self.ctes = old_ctes;
        Ok(input)
    }

    fn values(&mut self, rows: &[ast::ValuesRow]) -> Result<Arc<Relation>> {
        let prefix = self.symbol("xt.values")?;
        let mut bound = Vec::new();
        for row in rows {
            let mut input = Arc::new(Relation::unit());
            let expressions = row.expressions().iter().map(|expr| self.relational_expr(expr, &mut input, &BTreeMap::new())).collect::<Result<Vec<_>>>()?;
            bound.push((input, expressions));
        }
        let width = bound.first().map_or(0, |(_, row)| row.len());
        if let Some((_, row)) = bound.iter().find(|(_, row)| row.len() != width) {
            return Err(error(format!("Column count mismatch: expected {width}, given {}", row.len())));
        }
        let fields = (0..width).map(|index| {
            let types: Vec<_> = bound.iter().map(|(_, row)| row[index].data_type()).collect();
            self.binder.field(format!("_column_{}", index + 1), None, LogicalType::join_all(&types))
        }).collect::<Result<Vec<_>>>()?;
        let schema = Schema::new(fields.clone())?;
        if bound.iter().all(|(input, _)| input.schema().is_empty() && matches!(input.node(), RelationNode::Values { rows } if rows == &vec![vec![]])) {
            let rows = bound.into_iter().map(|(_, row)| row).collect();
            return rename_source(Arc::new(Relation::new(schema, RelationNode::Values { rows })?), prefix);
        }
        let mut result = None;
        for (input, row) in bound {
            let projected = Arc::new(input.project(fields.iter().cloned().zip(row).map(|(field, expr)| NamedExpr { field, expr }).collect())?);
            result = Some(match result {
                None => projected,
                Some(left) => Arc::new(Relation::new(schema.clone(), RelationNode::Set { left, right: projected, operator: SetOperator::Union, all: true })?),
            });
        }
        rename_source(result.unwrap_or_else(|| Arc::new(Relation::empty(schema))), prefix)
    }

    fn set_expr(&mut self, set: &ast::SetExpr) -> Result<Arc<Relation>> {
        match set {
            ast::SetExpr::Select(select) => self.select(select, None),
            ast::SetExpr::FromPipeline(pipeline) => self.from_pipeline(pipeline, None),
            ast::SetExpr::Query(query) => self.query(query),
            ast::SetExpr::Values(values) => self.values(&values.rows),
            ast::SetExpr::Records(records) => self.records_query(records),
            ast::SetExpr::SetOperation { left, op, set_quantifier, right } => {
                let left = self.set_expr(left)?;
                let right = self.set_expr(right)?;
                if left.schema().len() != right.schema().len() { return Err(error("Set inputs have different column counts")); }
                let fields = left.schema().fields().iter().zip(right.schema().fields()).map(|(left, right)| self.binder.field(left.name.clone(), None, left.data_type.join(&right.data_type))).collect::<Result<_>>()?;
                let operator = match op { ast::SetOperator::Union => SetOperator::Union, ast::SetOperator::Intersect => SetOperator::Intersect, ast::SetOperator::Except => SetOperator::Except, _ => return Err(grammar_rejected(format!("Set operator {op}"))) };
                let all = match set_quantifier { ast::SetQuantifier::All => true, ast::SetQuantifier::None | ast::SetQuantifier::Distinct => false, _ => return Err(grammar_rejected("A set operation correspondence modifier")) };
                Ok(Arc::new(Relation::new(Schema::new(fields)?, RelationNode::Set { left, right, operator, all })?))
            }
            _ => Err(grammar_rejected(format!("Query body {set}"))),
        }
    }

    fn select(&mut self, select: &ast::Select, order_by: Option<&ast::OrderBy>) -> Result<Arc<Relation>> {
        if select.top.is_some() || select.into.is_some() || select.qualify.is_some() || !select.lateral_views.is_empty() || select.prewhere.is_some() || !select.cluster_by.is_empty() || !select.distribute_by.is_empty() || !select.sort_by.is_empty() || !select.connect_by.is_empty() { return Err(grammar_rejected("A dialect-specific SELECT clause")); }
        let mut input = self.table_input(&select.from)?;
        if let Some(predicate) = &select.selection {
            input = self.bind_filter(input, predicate)?;
        }
        let distinct = match &select.distinct {
            None | Some(ast::Distinct::All) => false,
            Some(ast::Distinct::Distinct) => true,
            Some(ast::Distinct::On(_)) => return Err(grammar_rejected("DISTINCT ON")),
        };
        self.select_projection(input, SelectProjection {
            items: &select.projection,
            group_by: Some(&select.group_by),
            having: select.having.as_ref(),
            distinct,
        }, order_by)
    }

    fn from_pipeline(&mut self, pipeline: &ast::FromPipeline, order_by: Option<&ast::OrderBy>) -> Result<Arc<Relation>> {
        let input = self.table_input(&pipeline.from)?;
        let first_projection = pipeline.tails.iter().position(|tail| matches!(tail, ast::FromPipelineTail::Select(_)));
        if let Some(boundary) = first_projection.map(|index| index + 1).filter(|index| *index < pipeline.tails.len()) {
            let (initial, remaining) = pipeline.tails.split_at(boundary);
            let projected = self.bind_from_tails(input, initial, None)?;
            return self.with_projection_scope(|context| context.bind_from_tails(projected, remaining, order_by));
        }
        self.bind_from_tails(input, &pipeline.tails, order_by)
    }

    fn bind_from_tails(&mut self, mut input: Arc<Relation>, tails: &[ast::FromPipelineTail], order_by: Option<&ast::OrderBy>) -> Result<Arc<Relation>> {
        for (index, tail) in tails.iter().enumerate() {
            match tail {
                ast::FromPipelineTail::Where(predicate) => input = self.bind_filter(input, predicate)?,
                ast::FromPipelineTail::Select(select) => {
                    let last = index + 1 == tails.len();
                    input = self.select_projection(input, SelectProjection {
                        items: &select.projection,
                        group_by: select.group_by.as_ref(),
                        having: select.having.as_ref(),
                        distinct: select.distinct,
                    }, if last { order_by } else { None })?;
                    if last {
                        return Ok(input);
                    }
                }
            }
        }
        let wildcard = [ast::SelectItem::Wildcard(Default::default())];
        self.select_projection(input, SelectProjection {
            items: &wildcard, group_by: None, having: None, distinct: false,
        }, order_by)
    }

    fn table_input(&mut self, tables: &[ast::TableWithJoins]) -> Result<Arc<Relation>> {
        let mut input = Arc::new(Relation::unit());
        for (index, table) in tables.iter().enumerate() {
            let (right, arguments) = if index == 0 { (self.joined_table(table, self.outer_scopes.len())?, Vec::new()) }
                else { self.from_scope(input.schema().clone(), |context, start| context.joined_table(table, start))? };
            input = if arguments.is_empty() && input.schema().is_empty() && matches!(input.node(), RelationNode::Values { rows } if rows == &vec![vec![]]) {
                right
            } else {
                self.cross_join_from(input, right, arguments)?
            };
        }
        Ok(input)
    }

    fn bind_filter(&mut self, mut input: Arc<Relation>, predicate: &ast::Expr) -> Result<Arc<Relation>> {
        let predicate = self.relational_expr(predicate, &mut input, &BTreeMap::new())?;
        Ok(Arc::new(input.filter(predicate)?))
    }

    fn select_projection(&mut self, mut input: Arc<Relation>, select: SelectProjection<'_>, order_by: Option<&ast::OrderBy>) -> Result<Arc<Relation>> {
        let projection = select.items;
        let groups = match select.group_by {
            None => &[][..],
            Some(ast::GroupByExpr::Expressions(expressions, modifiers)) if modifiers.is_empty() => expressions,
            _ => return Err(grammar_rejected("A GROUP BY modifier")),
        };
        let empty_grouping_set = groups.iter().any(|group| matches!(group, ast::Expr::Tuple(elements) if elements.is_empty()));
        let groups = groups.iter().filter(|group| !matches!(group, ast::Expr::Tuple(elements) if elements.is_empty())).collect::<Vec<_>>();
        let mut aggregate_calls = Vec::new();
        for item in projection {
            if let ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } = item { crate::expression::aggregate_calls(expr, &mut aggregate_calls)?; crate::expression::aggregate_notices(expr, &mut self.notices); }
        }
        if let Some(having) = select.having { crate::expression::aggregate_calls(having, &mut aggregate_calls)?; crate::expression::aggregate_notices(having, &mut self.notices); }
        let inferred_scope = (groups.is_empty() && !empty_grouping_set && !aggregate_calls.is_empty())
            .then(|| input.schema().clone());
        let mut replacements = BTreeMap::new();
        let mut named_groups = Vec::new();
        let groups = groups.into_iter()
            .map(|group| alias_projection(group, projection, input.schema()).unwrap_or(group))
            .collect::<Vec<_>>();
        for (index, &group) in groups.iter().enumerate() {
            let expr = self.relational_expr(group, &mut input, &BTreeMap::new())?;
            if let Some(existing) = named_groups.iter().find(|named: &&NamedExpr| named.expr == expr) {
                replacements.insert(ReplacementKey::Expression(group.to_string()), existing.field.clone());
                continue;
            }
            let source = match &expr {
                Expr::Column { id, .. } => input.schema().get(*id),
                _ => None,
            };
            let label = implicit_expression_label(group, &expr, input.schema());
            let unnamed = label.is_none();
            let name = label.unwrap_or_else(|| format!("_column_{}", index + 1));
            let mut field = self.binder.field(name, None, expr.data_type())?;
            field.qualifier = source.and_then(|field| field.qualifier.clone());
            field.unnamed = unnamed;
            replacements.insert(ReplacementKey::Expression(group.to_string()), field.clone());
            named_groups.push(NamedExpr { field, expr });
        }
        let position = |named: &NamedExpr| match &named.expr { Expr::Column { id, .. } => input.schema().fields().iter().position(|field| field.id == *id).unwrap_or(usize::MAX), _ => usize::MAX };
        named_groups.sort_by_key(|named| position(named));
        let mut aggregates = Vec::new();
        for (key, function) in aggregate_calls {
            let aggregate = self.aggregate(&function, &mut input, aggregates.len())?;
            replacements.insert(ReplacementKey::Aggregate(key), aggregate.field.clone());
            aggregates.push(aggregate);
        }

        let (mut input, projected, names) = if let Some(source) = inferred_scope {
            let aggregate_fields = aggregates.iter().map(|aggregate| aggregate.field.clone()).collect();
            for aggregate in &aggregates {
                self.hidden_columns.insert(aggregate.field.id, HiddenColumn::Internal);
            }
            let mut deferred = DeferredExpressionInput::new(source.concat(&Schema::new(aggregate_fields)?)?);
            let ((projected, names), demand) = self.with_inferred_group_scope(&source, |context| {
                context.select_outputs(&select, &mut ExpressionInput::Deferred(&mut deferred), &mut replacements)
            })?;
            let keys = source.fields().iter().filter(|field| demand.contains(&field.id))
                .map(|field| NamedExpr { field: field.clone(), expr: field.expression() }).collect();
            let grouped = aggregate_relation(input, keys, aggregates)?;
            (deferred.bind(grouped)?, projected, names)
        } else {
            if !named_groups.is_empty() || !aggregates.is_empty() || empty_grouping_set {
                input = aggregate_relation(input, named_groups, aggregates)?;
            }
            let (projected, names) = self.select_outputs(
                &select, &mut ExpressionInput::Relational(&mut input), &mut replacements,
            )?;
            (input, projected, names)
        };
        let mut distinct = select.distinct;
        if let Some(order_by) = order_by {
            let ast::OrderByKind::Expressions(expressions) = &order_by.kind else { return Err(grammar_rejected("ORDER BY ALL")); };
            let fields = projected.iter().map(|projection| projection.field.clone()).collect::<Vec<_>>();
            let mut retained = input.schema().fields().iter().map(|field| {
                let mut output = field.clone();
                if names.contains(&field.name) { output.qualified_only = true; }
                NamedExpr { field: output, expr: field.expression() }
            }).collect::<Vec<_>>();
            retained.extend(projected);
            input = Arc::new(input.project(retained)?);
            let mut order_replacements = replacements.clone();
            for &group in &groups {
                if crate::expression::references_unqualified_names(group, &names) { order_replacements.remove(&ReplacementKey::Expression(group.to_string())); }
            }
            let mut order = Vec::new();
            for expression in expressions {
                let mut aggregates = Vec::new();
                crate::expression::aggregate_calls(&expression.expr, &mut aggregates)?;
                if !aggregates.is_empty() { return Err(error("Aggregate functions are not allowed directly in ORDER BY")); }
                let expr = if let Some(index) = ordinal(&expression.expr)? {
                    fields.get(index).ok_or_else(|| error("ORDER BY position is out of range"))?.expression()
                } else { self.relational_expr(&expression.expr, &mut input, &order_replacements)? };
                let descending = sort_descending(&expression.options)?;
                order.push(SortExpr { expr, descending, nulls_first: expression.options.nulls_first.unwrap_or(descending) });
            }
            let order_uses_projection = order.iter().all(|sort| {
                matches!(&sort.expr, Expr::Column { id, .. } if fields.iter().any(|field| field.id == *id))
            });
            let output = fields.into_iter().map(|field| NamedExpr { expr: field.expression(), field }).collect();
            if distinct && order_uses_projection {
                // Sorting observes only the retained output, so equal rows share a sort key.
                input = Arc::new(input.project(output)?);
                input = Arc::new(Relation::new(input.schema().clone(), RelationNode::Distinct { input })?);
                input = Arc::new(input.sort(order)?);
                distinct = false;
            } else {
                input = Arc::new(input.sort(order)?);
                input = Arc::new(input.project(output)?);
            }
        } else { input = Arc::new(input.project(projected)?); }
        if distinct {
            input = Arc::new(Relation::new(input.schema().clone(), RelationNode::Distinct { input })?);
        }
        // The reference parses `GROUP BY ()` but its plan never conforms, so the rest of the query is bound first for its errors to take precedence.
        if empty_grouping_set { return Err(Anomaly::incorrect("xtdb.query/invalid-query-plan", "internal error conforming query plan")); }
        Ok(input)
    }

    fn select_outputs(
        &mut self, select: &SelectProjection<'_>, input: &mut ExpressionInput<'_>,
        replacements: &mut Replacements,
    ) -> Result<(Vec<NamedExpr>, BTreeSet<String>)> {
        let projection = select.items;
        if let Some(having) = select.having {
            let predicate = self.expr_bound(having, input, replacements)?;
            input.attach(ExpressionAttachment::Filter(predicate))?;
        }
        let mut windows = Vec::new();
        for item in projection {
            if let ast::SelectItem::UnnamedExpr(expression) | ast::SelectItem::ExprWithAlias { expr: expression, .. } = item { crate::expression::window_calls(expression, &mut windows); }
        }
        if windows.len() > 1 { return Err(unsupported("TODO: only one window function supported at the moment!")); }
        if let Some((key, function)) = windows.pop() {
            let window = self.bind_window(input.relational_schema()?, &function, replacements)?;
            let field = window.output().clone();
            input.attach(ExpressionAttachment::Window(window))?;
            replacements.insert(ReplacementKey::Expression(key), field);
        }
        let mut projected = Vec::new();
        for (index, item) in projection.iter().filter(|item| !matches!(item, ast::SelectItem::Wildcard(_))).enumerate() {
            match item {
                ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } => {
                    let expression = self.expr_bound(expr, input, replacements)?;
                    let name = match item {
                        ast::SelectItem::ExprWithAlias { alias, .. } => identifier(alias).normalized(),
                        _ => implicit_expression_label(expr, &expression, input.relational_schema()?)
                            .unwrap_or_else(|| format!("_column_{}", index + 1)),
                    };
                    let field = self.binder.field(name, None, expression.data_type())?;
                    projected.push(NamedExpr { field, expr: expression });
                }
                ast::SelectItem::QualifiedWildcard(ast::SelectItemQualifiedWildcardKind::ObjectName(name), options) => {
                    projected.extend(self.wildcard(input.relational_schema()?, Some(name), options, &BTreeSet::new())?);
                }
                _ => return Err(grammar_rejected("An expression wildcard or multi-alias projection")),
            }
        }
        let wildcards = projection.iter().enumerate().filter_map(|(index, item)| match item { ast::SelectItem::Wildcard(options) => Some((index, options)), _ => None }).collect::<Vec<_>>();
        if let Some((index, options)) = wildcards.first() {
            if wildcards.len() != 1 || (*index != 0 && *index + 1 != projection.len()) { return Err(error("Unqualified wildcard must occur once, at the beginning or end of the projection")); }
            let explicit_names = projected.iter().map(|projection| projection.field.name.clone()).collect::<BTreeSet<_>>();
            let mut expanded = self.wildcard(input.relational_schema()?, None, options, &explicit_names)?;
            expanded.extend(projected);
            projected = expanded;
        }
        let mut names = BTreeSet::new();
        for projection in &projected {
            if !names.insert(projection.field.name.clone()) { return Err(error(format!("Duplicate column projection: {}", projection.field.name))); }
        }
        Ok((projected, names))
    }

    fn joined_table(&mut self, table: &ast::TableWithJoins, lateral_start: usize) -> Result<Arc<Relation>> {
        let mut left = self.table_for(&table.relation, TablePurpose::Query, lateral_start)?;
        for joined in &table.joins {
            let (kind, constraint) = match &joined.join_operator {
                ast::JoinOperator::Inner(constraint) | ast::JoinOperator::Join(constraint) => (JoinKind::Inner, Some(constraint)),
                ast::JoinOperator::LeftOuter(constraint) | ast::JoinOperator::Left(constraint) => (JoinKind::Left, Some(constraint)),
                ast::JoinOperator::RightOuter(constraint) | ast::JoinOperator::Right(constraint) => (JoinKind::Right, Some(constraint)),
                ast::JoinOperator::FullOuter(constraint) => (JoinKind::Full, Some(constraint)),
                ast::JoinOperator::CrossJoin(_) => (JoinKind::Cross, None),
                _ => return Err(grammar_rejected("A dialect-specific join operator")),
            };
            let (right, mut arguments) = self.from_scope(left.schema().clone(), |context, start| {
                let start = if kind == JoinKind::Cross || matches!(constraint, Some(ast::JoinConstraint::Natural)) { start } else { lateral_start };
                context.table_for(&joined.relation, TablePurpose::Query, start)
            })?;
            if !arguments.is_empty() && matches!(kind, JoinKind::Right | JoinKind::Full) { return Err(unsupported("Correlated LATERAL in RIGHT or FULL OUTER JOIN")); }
            let schema = left.schema().concat(right.schema())?;
            let common = match constraint {
                Some(ast::JoinConstraint::Using(names)) => Some(names.iter().map(|name| {
                    let names = identifiers(name)?;
                    if names.len() != 1 { return Err(error("USING columns must be unqualified")); }
                    Ok(names[0].normalized())
                }).collect::<Result<BTreeSet<_>>>()?),
                Some(ast::JoinConstraint::Natural) => {
                    let names = |input: &Relation| input.schema().fields().iter().filter(|field| !self.hidden_columns.contains_key(&field.id)).map(|field| field.name.clone()).collect::<BTreeSet<_>>();
                    Some(names(&left).intersection(&names(&right)).cloned().collect())
                }
                _ => None,
            };
            if let Some(common) = common {
                let predicates = common.iter().map(|name| {
                    let names = [Identifier::quoted(name)];
                    let left_field = left.schema().resolve(&names)?;
                    let left = if arguments.is_empty() { left_field.expression() } else {
                        let (parameter, capture) = self.parameter_capture(left_field)?;
                        arguments.push(capture);
                        parameter
                    };
                    Ok(Expr::binary(BinaryOp::Eq, left, right.schema().resolve(&names)?.expression()))
                }).collect::<Result<Vec<_>>>()?;
                let predicate = predicates.into_iter().reduce(|left, right| Expr::binary(BinaryOp::And, left, right)).unwrap_or(Expr::Literal(Value::Bool(true)));
                let right_ids = right.schema().fields().iter().filter(|field| common.contains(&field.name)).map(|field| field.id).collect::<BTreeSet<_>>();
                let joined = if arguments.is_empty() { join(left, right, kind, Some(predicate))? }
                    else { apply_join(left, Arc::new(right.filter(predicate)?), if kind == JoinKind::Left { ApplyKind::Left } else { ApplyKind::Inner }, arguments)? };
                let projections = joined.schema().fields().iter().map(|field| {
                    let mut output = field.clone();
                    if right_ids.contains(&field.id) { output.qualified_only = true; }
                    NamedExpr { field: output, expr: field.expression() }
                }).collect();
                left = Arc::new(joined.project(projections)?);
                continue;
            }
            if let Some(ast::JoinConstraint::On(predicate)) = constraint {
                if !arguments.is_empty() || crate::expression::contains_subquery(predicate) {
                    if !matches!(kind, JoinKind::Inner | JoinKind::Left | JoinKind::Right) { return Err(unsupported("Subqueries in FULL OUTER JOIN conditions")); }
                    crate::expression::reject_ambiguous_columns(predicate, &schema)?;
                    let original_order = schema.fields().iter().map(|field| field.id).collect::<Vec<_>>();
                    let (outer, mut dependent) = if kind == JoinKind::Right { (right, left) } else { (left, right) };
                    let fields = dependent.schema().fields().to_vec();
                    self.outer_scopes.push(crate::subquery::CorrelationScope::new(outer.schema().clone()));
                    let predicate = self.relational_expr(predicate, &mut dependent, &BTreeMap::new());
                    arguments.extend(self.outer_scopes.pop().ok_or_else(|| error("Join correlation scope is unavailable"))?.arguments());
                    dependent = Arc::new(dependent.filter(predicate?)?);
                    dependent = Arc::new(dependent.project(fields.into_iter().map(|field| NamedExpr { expr: field.expression(), field }).collect())?);
                    let nullable = kind != JoinKind::Inner;
                    let schema = outer.schema().concat(&if nullable { dependent.schema().nullable()? } else { dependent.schema().clone() })?;
                    let joined = Arc::new(Relation::new(schema, RelationNode::Apply { input: outer, dependent, arguments, kind: if nullable { ApplyKind::Left } else { ApplyKind::Inner }, output: None })?);
                    left = if kind == JoinKind::Right {
                        let expressions = original_order.into_iter().map(|id| {
                            let field = joined.schema().get(id).cloned().ok_or_else(|| error("Joined column is unavailable"))?;
                            Ok(NamedExpr { expr: field.expression(), field })
                        }).collect::<Result<Vec<_>>>()?;
                        Arc::new(joined.project(expressions)?)
                    } else { joined };
                    continue;
                }
            }
            let condition = match constraint {
                Some(ast::JoinConstraint::On(expr)) => Some(self.expr(expr, &schema, &BTreeMap::new())?),
                Some(ast::JoinConstraint::None) | None => None,
                _ => return Err(error("Named join constraint was not lowered")),
            };
            left = if kind == JoinKind::Cross {
                self.cross_join_from(left, right, arguments)?
            } else if arguments.is_empty() {
                join(left, right, kind, condition)?
            } else {
                apply_join(left, right, if kind == JoinKind::Left { ApplyKind::Left } else { ApplyKind::Inner }, arguments)?
            };
        }
        Ok(left)
    }

    fn cross_join_from(&mut self, input: Arc<Relation>, dependent: Arc<Relation>, arguments: Vec<(usize, Expr)>) -> Result<Arc<Relation>> {
        if arguments.is_empty() {
            return join(input, dependent, JoinKind::Cross, None);
        }
        if let Some(lowered) = self.lower_column_table_apply(&input, &dependent, &arguments)? {
            return Ok(lowered);
        }
        apply_join(input, dependent, ApplyKind::Inner, arguments)
    }

    fn lower_column_table_apply(&mut self, input: &Arc<Relation>, dependent: &Arc<Relation>, arguments: &[(usize, Expr)]) -> Result<Option<Arc<Relation>>> {
        let table = match dependent.node() {
            RelationNode::Ordinality { input, scope: rtdb_plan::OrdinalityScope::Page, .. } => input.as_ref(),
            _ => dependent.as_ref(),
        };
        let RelationNode::ColumnTable { expr, .. } = table.node() else { return Ok(None); };
        let mut collection = expr.clone();
        let bindings = arguments.iter().map(|(index, expr)| (*index, expr)).collect::<BTreeMap<_, _>>();
        let mut captured = false;
        collection.rewrite(&mut |expr| {
            if let Expr::Parameter { index, .. } = expr {
                if let Some(&replacement) = bindings.get(index) {
                    *expr = replacement.clone();
                    captured = true;
                }
            }
        });
        if !captured {
            return Ok(Some(join(Arc::clone(input), Arc::clone(dependent), JoinKind::Cross, None)?));
        }

        let name = format!("_unnest_{}", self.next_rewrite_symbol);
        self.next_rewrite_symbol = self.next_rewrite_symbol.checked_add(1).ok_or_else(|| error("SQL rewrite symbol counter overflow"))?;
        let staged_field = self.binder.field(name, None, collection.data_type())?;
        self.hidden_columns.insert(staged_field.id, HiddenColumn::Internal);
        let staged = Arc::new(Arc::clone(input).extend(vec![NamedExpr { field: staged_field.clone(), expr: collection }])?);
        let element = dependent.schema().fields()[0].clone();
        let ordinal = dependent.schema().fields().get(1).cloned().map(|mut field| {
            field.data_type = LogicalType::Scalar(ScalarType::I32);
            field
        });
        let schema = Schema::new(staged.schema().fields().iter().cloned().chain([element.clone()]).chain(ordinal.iter().cloned()).collect())?;
        let expanded = Arc::new(Relation::new(schema, RelationNode::Unnest {
            input: staged, expr: staged_field.expression(), element, ordinality: ordinal, outer: false,
        })?);
        Ok(Some(expanded))
    }

    fn table_for(&mut self, table: &ast::TableFactor, purpose: TablePurpose, lateral_start: usize) -> Result<Arc<Relation>> {
        match table {
            ast::TableFactor::Table { name, alias, args, version, with_hints, partitions, sample, with_ordinality, json_path, index_hints } => {
                if version.is_some() || !with_hints.is_empty() || !partitions.is_empty() || sample.is_some() || json_path.is_some() || !index_hints.is_empty() { return Err(grammar_rejected("A table sample or dialect-specific table modifier")); }
                grammar_table_name(name)?;
                if let Some(args) = args {
                    if let Some(ranked) = RankedTable::from_name(name) && args.settings.is_none() {
                        return self.ranked_table(ranked, &args.args, alias.as_ref(), *with_ordinality, purpose);
                    }
                    return Err(grammar_rejected("Table arguments"));
                }
                if *with_ordinality { return Err(unsupported("WITH ORDINALITY requires a list table")); }
                if let Some(inline) = self.inline_xtql.iter().find(|inline| inline.matches(name)).cloned() {
                    let arguments = inline.parameters.iter().map(|index| (*index,self.options.parameter_types.get(index).cloned().unwrap_or(LogicalType::Nothing))).collect::<Vec<_>>();
                    let relation = self.forms.compile_query(&inline.query,self.catalog,&mut self.binder,&mut self.next_parameter,&arguments,&self.options.default_timezone).map_err(|error| inline.error(error,self.source))?;
                    return match alias { Some(alias) => self.alias(relation,alias),None => Ok(relation) };
                }
                let names = identifiers(name)?;
                if names.len() == 1 {
                    let cte_name = names[0].normalized();
                    if let Some(cte) = self.ctes.get(&cte_name).cloned() {
                        let end = name.0.last().and_then(ast::ObjectNamePart::as_ident).map(|name| name.span.end);
                        if self.temporal.iter().any(|clause| Some(clause.owner_end) == end) {
                            use rtdb_types::transit_value::{TransitValue, to_json_value};
                            let diagnostic = PlanningDiagnostic::new(format!("Period specifications not allowed on CTE reference: {cte_name}"))
                                .with_detail("table-name", to_json_value(&TransitValue::Symbol(cte_name.clone()))?);
                            self.diagnostics.push(diagnostic);
                        }
                        let input = match cte {
                            Cte::Inline(input) => input,
                            Cte::Materialized(binding) => Arc::new(Relation::new(binding.relation.schema().clone(), RelationNode::Reference { binding })?),
                        };
                        return match alias { Some(alias) => self.alias(input, alias), None => self.rename(input, Some(cte_name), None) };
                    }
                }
                let (valid_time, system_time, clamp_valid_time) = self.temporal_filters(name)?;
                let binding = resolve_table(self.catalog, &names, &self.options.search_path)?;
                let qualifier = alias.as_ref().map(|alias| identifier(&alias.name).normalized()).unwrap_or_else(|| binding.name.table.table.clone());
                if purpose != TablePurpose::Query && alias.as_ref().is_some_and(|alias| !alias.columns.is_empty()) { return Err(error("Mutation targets do not accept column lists")); }
                let mut visible = alias.as_ref().filter(|alias| !alias.columns.is_empty()).map(|alias| alias.columns.iter().map(|column| identifier(&column.name).normalized()).collect::<Vec<_>>())
                    .unwrap_or_else(|| binding.columns.keys().cloned().collect());
                let archive_column = |name: &str| matches!(name, rtdb_types::ARCHIVE_TX_FROM | rtdb_types::ARCHIVE_TX_TO);
                let written_basis = rtdb_types::rtdb_v1::WRITTEN_BASIS_COLUMN;
                let mut seen = BTreeSet::new();
                visible.retain(|name| !archive_column(name) && name.as_str() != written_basis && seen.insert(name.clone()));
                let mut requested = visible.clone();
                for name in ["_iid", "_valid_from", "_valid_to", "_system_from", "_system_to"] {
                    if !requested.iter().any(|column| column == name) { requested.push(name.into()); }
                }
                if purpose == TablePurpose::Query && binding.columns.contains_key(written_basis) {
                    requested.push(written_basis.into());
                }
                if purpose != TablePurpose::Erase {
                    for name in &self.period_columns {
                        if archive_column(name) && (!matches!(&system_time, TemporalFilter::All)
                            || purpose != TablePurpose::Query || !binding.columns.contains_key(name)) {
                            continue;
                        }
                        if !requested.contains(name) { requested.push(name.clone()); }
                    }
                }
                let mut columns = Vec::new();
                let mut periods = Vec::new();
                let mut output = Vec::new();
                for name in requested {
                    let period = matches!(name.as_str(), "_valid_time" | "_system_time");
                    let ty = match name.as_str() {
                        "_valid_time" | "_system_time" if purpose == TablePurpose::Query => rtdb_plan::function_type("period", &[crate::mutation::instant_type(), crate::mutation::instant_type().maybe()])?,
                        _ => binding.scan_column_type(&name),
                    };
                    let mut field = self.binder.field(name.clone(), Some(qualifier.clone()), ty)?;
                    if purpose == TablePurpose::Query {
                        field.qualifier = Some(ColumnQualifier::BaseTable { database: binding.name.database.clone(), schema: binding.name.table.schema.clone(), alias: qualifier.clone() });
                    }
                    if !visible.contains(&name) { self.hidden_columns.insert(field.id, HiddenColumn::ExplicitOnly); }
                    if period && purpose == TablePurpose::Query { periods.push(field.clone()); }
                    else { columns.push(field.clone()); }
                    output.push(field);
                }
                let relation = Arc::new(Relation::scan(Scan { table: binding, columns, valid_time, system_time, clamp_valid_time, iids: None, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new() })?);
                let relation = if purpose == TablePurpose::Query {
                    let prefix = self.symbol(&qualifier)?;
                    rename_source(relation, prefix)?
                } else { relation };
                if periods.is_empty() { return Ok(relation); }
                let expressions = output.into_iter().map(|field| {
                    let expr = if periods.iter().any(|period| period.id == field.id) {
                        let (from, to) = if field.name == "_valid_time" { ("_valid_from", "_valid_to") } else { ("_system_from", "_system_to") };
                        let endpoint = |name| relation.schema().fields().iter().find(|field| field.name == name).map(Field::expression).ok_or_else(|| error("Period endpoint is unavailable"));
                        crate::period::call("period", vec![endpoint(from)?, endpoint(to)?])?
                    } else { field.expression() };
                    Ok(NamedExpr { field, expr })
                }).collect::<Result<_>>()?;
                Ok(Arc::new(relation.project(expressions)?))
            }
            ast::TableFactor::Derived { subquery, alias, lateral, .. } => {
                if *lateral && alias.is_none() { return Err(error("LATERAL derived tables require an alias")); }
                let relation = if *lateral { self.lateral_scope(lateral_start, |context| context.query(subquery))? } else { self.query(subquery)? };
                match alias { Some(alias) => self.alias(relation, alias), None => Ok(relation) }
            }
            ast::TableFactor::ExpressionDerived { expr, alias, with_ordinality } => {
                if purpose != TablePurpose::Query { return Err(error("Mutation target must be a base table")); }
                if let ast::Expr::Function(function) = expr
                    && let Some(ranked) = RankedTable::from_name(&function.name)
                {
                    if let ast::FunctionArguments::List(args) = &function.args
                        && args.duplicate_treatment.is_none() && args.clauses.is_empty()
                        && matches!(function.parameters, ast::FunctionArguments::None)
                        && function.filter.is_none() && function.over.is_none()
                        && function.null_treatment.is_none() && function.within_group.is_empty()
                    {
                        return self.ranked_table(ranked, &args.args, alias.as_ref(), *with_ordinality, purpose);
                    }
                    return Err(grammar_rejected("Ranked table function modifiers"));
                }
                let expression = self.lateral_scope(lateral_start, |context| context.expr(expr, &Schema::default(), &BTreeMap::new()))?;
                let series = alias.is_some() && matches!(expr, ast::Expr::Function(function)
                    if function.name.0.last().and_then(ast::ObjectNamePart::as_ident)
                        .is_some_and(|name| name.quote_style.is_none()
                            && matches!(name.value.to_ascii_lowercase().as_str(), "generate_series" | "range")));
                if series {
                    self.list_table(expression, alias.as_ref(), *with_ordinality, ListTableKind::Series)
                } else {
                    self.scalar_table(expression, alias.as_ref(), *with_ordinality)
                }
            }
            ast::TableFactor::NestedJoin { table_with_joins, alias } => {
                let relation = self.joined_table(table_with_joins, lateral_start)?;
                match alias { Some(alias) => self.alias(relation, alias), None => Ok(relation) }
            }
            ast::TableFactor::UNNEST { array_exprs, alias, with_ordinality, with_offset, .. } => {
                if array_exprs.len() != 1 || *with_offset { return Err(grammar_rejected("Multi-array UNNEST or WITH OFFSET")); }
                let expr = self.lateral_scope(lateral_start, |context| context.expr(&array_exprs[0], &Schema::default(), &BTreeMap::new()))?;
                self.list_table(expr, alias.as_ref(), *with_ordinality, ListTableKind::Unnest)
            }
            _ => Err(grammar_rejected(format!("Table source {table}"))),
        }
    }

    fn ranked_table(&mut self, ranked: RankedTable, args: &[ast::FunctionArg], alias: Option<&ast::TableAlias>, with_ordinality: bool, purpose: TablePurpose) -> Result<Arc<Relation>> {
        if purpose != TablePurpose::Query || with_ordinality {
            return Err(grammar_rejected("Ranked table function modifiers"));
        }
        let expressions = args.iter().map(|arg| match arg {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => Ok(expr),
            _ => Err(grammar_rejected("Ranked table function argument")),
        }).collect::<Result<Vec<_>>>()?;
        if matches!(ranked, RankedTable::Hybrid) { return self.hybrid_table(&expressions, alias); }
        let (table, column, probe, analyzer, fetch, rank_name, function, descending) = match (ranked, expressions.as_slice()) {
            (RankedTable::Search, [table, column, probe]) => (table, column, probe, None, None, "_score", "search_score", true),
            (RankedTable::Search, [table, column, probe, analyzer]) => (table, column, probe, Some(*analyzer), None, "_score", "search_score", true),
            (RankedTable::Nearest, [table, column, probe, limit]) => (table, column, probe, None, Some(*limit), "_distance", "cosine_distance", false),
            _ => return Err(error("RTDB_SEARCH requires a table, column, query and an optional analyzer. RTDB_NEAREST requires a table, column, probe and limit")),
        };
        let parts = match table {
            ast::Expr::Identifier(part) => vec![part.clone()],
            ast::Expr::CompoundIdentifier(parts) => parts.clone(),
            _ => return Err(error("A ranked table function requires a source table name")),
        };
        let ast::Expr::Identifier(column) = column else { return Err(error("A ranked table function requires a column name")); };
        let table = ast::TableFactor::Table {
            name: ast::ObjectName(parts.into_iter().map(ast::ObjectNamePart::Identifier).collect()),
            alias: None, args: None, with_hints: vec![], version: None, with_ordinality: false,
            partitions: vec![], json_path: None, sample: None, index_hints: vec![],
        };
        let input = self.table_for(&table, TablePurpose::Query, 0)?;
        let field = |name: &str| input.schema().fields().iter().find(|field| field.name == name);
        let source = field(&identifier(column).normalized()).ok_or_else(|| error("Ranked column does not exist"))?;
        let tie = field("_iid").ok_or_else(|| error("A ranked table function requires a base table"))?.expression();
        if field(rank_name).is_some() { return Err(error(format!("Ranked table output already has a column named {rank_name}"))); }
        let probe = self.expr(probe, &Schema::default(), &BTreeMap::new())?;
        let fetch = fetch.map(|value| self.expr(value, &Schema::default(), &BTreeMap::new())).transpose()?;
        let mut arguments = vec![source.expression(), probe];
        if let Some(analyzer) = analyzer { arguments.push(self.expr(analyzer, &Schema::default(), &BTreeMap::new())?); }
        let rank = crate::expression::call(function.into(), arguments.clone())?;
        let mut rank_field = self.binder.field(rank_name, None, rank.data_type())?;
        rank_field.qualifier = source.qualifier.clone();
        let rank_column = rank_field.expression();
        let input = if matches!(ranked, RankedTable::Search) {
            Arc::new(input.filter(crate::expression::call("match".into(), arguments)?)?)
        } else { input };
        let input = Arc::new(input.extend(vec![NamedExpr { field: rank_field, expr: rank }])?);
        let input = if matches!(ranked, RankedTable::Nearest) {
            Arc::new(input.filter(Expr::Unary { op: UnaryOp::IsNotNull, expr: Box::new(rank_column.clone()) })?)
        } else { input };
        let sorted = Arc::new(input.sort(vec![
            SortExpr { expr: rank_column, descending, nulls_first: false },
            SortExpr { expr: tie, descending: false, nulls_first: false },
        ])?);
        let output = if let Some(fetch) = fetch { Arc::new(sorted.limit(None, Some(fetch))?) } else { sorted };
        alias.map_or(Ok(output.clone()), |alias| self.alias(output, alias))
    }

    /// RTDB_HYBRID(table, text_column, query, vector_column, vector, window, limit[, analyzer]) fuses, at one snapshot, the first `window`
    /// rows of the exact text ranking of RTDB_SEARCH and of the exact nearest ranking of RTDB_NEAREST by reciprocal rank fusion.
    /// Each ranking orders its rows with ties by `_iid` from rank 1; `_score` sums 1 / (k + rank) over the windows that hold the row,
    /// text first, and the output keeps the first `limit` rows by `_score` descending, then `_iid`.
    fn hybrid_table(&mut self, expressions: &[&ast::Expr], alias: Option<&ast::TableAlias>) -> Result<Arc<Relation>> {
        let [table, text, query, vector, probe, window, limit, analyzer @ ..] = expressions else {
            return Err(error("RTDB_HYBRID requires a table, text column, query, vector column, vector, window, limit and an optional analyzer"));
        };
        if analyzer.len() > 1 { return Err(error("RTDB_HYBRID takes at most one analyzer")); }
        let parts = match table {
            ast::Expr::Identifier(part) => vec![part.clone()],
            ast::Expr::CompoundIdentifier(parts) => parts.clone(),
            _ => return Err(error("A ranked table function requires a source table name")),
        };
        let (ast::Expr::Identifier(text), ast::Expr::Identifier(vector)) = (text, vector) else { return Err(error("A ranked table function requires a column name")); };
        let table = ast::TableFactor::Table {
            name: ast::ObjectName(parts.into_iter().map(ast::ObjectNamePart::Identifier).collect()),
            alias: None, args: None, with_hints: vec![], version: None, with_ordinality: false,
            partitions: vec![], json_path: None, sample: None, index_hints: vec![],
        };
        let input = self.table_for(&table, TablePurpose::Query, 0)?;
        let visible: Vec<Field> = input.schema().fields().to_vec();
        let field = |name: &str| visible.iter().find(|field| field.name == name);
        let text = field(&identifier(text).normalized()).ok_or_else(|| error("Ranked column does not exist"))?.clone();
        let vector = field(&identifier(vector).normalized()).ok_or_else(|| error("Ranked column does not exist"))?.clone();
        let tie = field("_iid").ok_or_else(|| error("A ranked table function requires a base table"))?.expression();
        if field("_score").is_some() { return Err(error("Ranked table output already has a column named _score")); }
        let constant = |context: &mut Self, expression: &ast::Expr| context.expr(expression, &Schema::default(), &BTreeMap::new());
        let mut text_arguments = vec![text.expression(), constant(self, query)?];
        if let [analyzer] = analyzer { text_arguments.push(constant(self, analyzer)?); }
        let probe = constant(self, probe)?;
        let window = constant(self, window)?;
        let fetch = constant(self, limit)?;

        let matched = crate::expression::call("match".into(), text_arguments.clone())?;
        let score = crate::expression::call("search_score".into(), text_arguments)?;
        let distance = crate::expression::call("cosine_distance".into(), vec![vector.expression(), probe])?;
        let matched_field = self.binder.field("_hybrid_matched", None, matched.data_type())?;
        let score_field = self.binder.field("_hybrid_text_score", None, score.data_type())?;
        let distance_field = self.binder.field("_hybrid_distance", None, distance.data_type())?;
        let near = Expr::unary(UnaryOp::IsNotNull, distance_field.expression());
        let near_field = self.binder.field("_hybrid_near", None, LogicalType::Scalar(ScalarType::Bool))?;
        let input = Arc::new(input.extend(vec![
            NamedExpr { field: matched_field.clone(), expr: matched },
            NamedExpr { field: score_field.clone(), expr: score },
            NamedExpr { field: distance_field.clone(), expr: distance },
        ])?);
        let input = Arc::new(input.extend(vec![NamedExpr { field: near_field.clone(), expr: near }])?);
        let input = Arc::new(input.filter(Expr::binary(BinaryOp::Or, matched_field.expression(), near_field.expression()))?);

        let rank = |context: &mut Self, input: Arc<Relation>, name: &str, partition: &Field, order: Expr, descending: bool| -> Result<(Arc<Relation>, Field)> {
            let field = context.binder.field(name, None, LogicalType::Scalar(ScalarType::I64))?;
            let schema = input.schema().concat(&Schema::new(vec![field.clone()])?)?;
            let window = Relation::new(schema, RelationNode::Window {
                input, partition_by: vec![partition.id],
                order_by: vec![
                    SortExpr { expr: order, descending, nulls_first: false },
                    SortExpr { expr: tie.clone(), descending: false, nulls_first: false },
                ],
                expressions: vec![WindowExpr { field: field.clone(), function: WindowFunction::RowNumber }],
            })?;
            Ok((Arc::new(window), field))
        };
        let (input, text_rank) = rank(self, input, "_hybrid_text_rank", &matched_field, score_field.expression(), true)?;
        let (input, near_rank) = rank(self, input, "_hybrid_near_rank", &near_field, distance_field.expression(), false)?;
        let windowed = |member: &Field, rank: &Field| Expr::binary(BinaryOp::And, member.expression(),
            Expr::binary(BinaryOp::LtEq, rank.expression(), window.clone()));
        let reciprocal = |member: &Field, rank: &Field| Expr::Case {
            branches: vec![(windowed(member, rank), Expr::binary(BinaryOp::Divide, Expr::Literal(Value::F64(1.0)),
                Expr::binary(BinaryOp::Add, Expr::Literal(Value::F64(HYBRID_RANK_CONSTANT)), rank.expression())))],
            otherwise: Box::new(Expr::Literal(Value::F64(0.0))),
        };
        let input = Arc::new(input.filter(Expr::binary(BinaryOp::Or, windowed(&matched_field, &text_rank), windowed(&near_field, &near_rank)))?);
        let fused = Expr::binary(BinaryOp::Add, reciprocal(&matched_field, &text_rank), reciprocal(&near_field, &near_rank));
        let mut score_field = self.binder.field("_score", None, LogicalType::Scalar(ScalarType::F64))?;
        score_field.qualifier = text.qualifier.clone();
        let input = Arc::new(input.extend(vec![NamedExpr { field: score_field.clone(), expr: fused }])?);
        let sorted = Arc::new(input.sort(vec![
            SortExpr { expr: score_field.expression(), descending: true, nulls_first: false },
            SortExpr { expr: tie, descending: false, nulls_first: false },
        ])?);
        let limited = Arc::new(sorted.limit(None, Some(fetch))?);
        let output = visible.iter().chain([&score_field]).map(|field| NamedExpr { field: field.clone(), expr: field.expression() }).collect();
        let output = Arc::new(limited.project(output)?);
        alias.map_or(Ok(output.clone()), |alias| self.alias(output, alias))
    }

    pub(crate) fn admit_table_temporal_literals(&mut self, name: &ast::ObjectName) -> Result<()> {
        let end = name.0.last().and_then(ast::ObjectNamePart::as_ident).map(|name| name.span.end);
        let clauses = self.temporal.iter().filter(|clause| Some(clause.owner_end) == end);
        let extents = clauses.clone().filter(|clause| !clause.system).chain(clauses.filter(|clause| clause.system)).map(|clause| clause.extent.clone()).collect::<Vec<_>>();
        for extent in &extents { self.admit_extent_literals(extent)?; }
        Ok(())
    }

    fn scalar_table(&mut self, expression: Expr, alias: Option<&ast::TableAlias>, with_ordinality: bool) -> Result<Arc<Relation>> {
        let name = match alias {
            Some(alias) => identifier(&alias.name).normalized(),
            None => self.symbol("_expr")?,
        };
        let prefix = self.symbol(&name)?;
        let columns = alias.map_or(&[][..], |alias| alias.columns.as_slice());
        let width = usize::from(with_ordinality) + 1;
        if !columns.is_empty() && columns.len() != width {
            let names = columns.iter().map(|column| identifier(&column.name).normalized()).collect::<Vec<_>>();
            self.diagnostics.push(PlanningDiagnostic::table_projection(name, names));
            return Ok(Arc::new(Relation::empty(Schema::default())));
        }
        if columns.iter().any(|column| column.data_type.is_some()) {
            return Err(error("Table projections specify column names"));
        }
        let value_name = match columns.first() {
            Some(column) => identifier(&column.name).normalized(),
            None => format!("_column_{}", self.allocate_symbol()?),
        };
        let mut fields = vec![self.binder.field(value_name, Some(name.clone()), expression.data_type())?];
        let mut row = vec![expression];
        if with_ordinality {
            let ordinal_name = match columns.get(1) {
                Some(column) => identifier(&column.name).normalized(),
                None => self.symbol("_ordinal")?,
            };
            fields.push(self.binder.field(ordinal_name, Some(name), LogicalType::Scalar(ScalarType::I64))?);
            row.push(Expr::Literal(Value::I64(1)));
        }
        let input = Arc::new(Relation::new(Schema::new(fields)?, RelationNode::Values { rows: vec![row] })?);
        rename_source(input, prefix)
    }

    fn list_table(&mut self, expr: Expr, alias: Option<&ast::TableAlias>, with_ordinality: bool, kind: ListTableKind) -> Result<Arc<Relation>> {
        let alias = alias.ok_or_else(|| error("List tables require an alias"))?;
        if !alias.columns.is_empty() && alias.columns.len() != 1 + usize::from(with_ordinality) {
            let name = identifier(&alias.name).normalized();
            let columns = alias.columns.iter().map(|column| identifier(&column.name).normalized()).collect();
            self.diagnostics.push(PlanningDiagnostic::table_projection(name, columns));
            return Ok(Arc::new(Relation::empty(Schema::default())));
        }
        let elements = expr.data_type().legs().into_iter().map(|ty| match ty.unencoded() {
            LogicalType::Nothing | LogicalType::Null => Ok(LogicalType::Null),
            LogicalType::List { kind: rtdb_types::ListKind::List | rtdb_types::ListKind::Set | rtdb_types::ListKind::FixedSizeList(_), element } => Ok(*element),
            _ => Ok(LogicalType::Null),
        }).collect::<Result<Vec<_>>>()?;
        let alias_name = identifier(&alias.name).normalized();
        let symbol = self.symbol(&alias_name)?;
        let qualifier = ColumnQualifier::ScopedAlias { alias: alias_name, symbol };
        let element_name = match alias.columns.first() {
            Some(column) => identifier(&column.name).normalized(),
            None => self.symbol(if kind == ListTableKind::Series { "_genseries" } else { "_unnest" })?,
        };
        let mut element = self.binder.field(element_name, None, LogicalType::join_all(&elements))?;
        element.qualifier = Some(qualifier.clone());
        element.unnamed = alias.columns.is_empty();
        let ordinality = if with_ordinality {
            let name = match alias.columns.get(1) {
                Some(column) => identifier(&column.name).normalized(),
                None => self.symbol("_ordinal")?,
            };
            let mut field = self.binder.field(name, None, LogicalType::Scalar(ScalarType::I64))?;
            field.qualifier = Some(qualifier);
            field.unnamed = alias.columns.is_empty();
            Some(field)
        } else { None };
        let element_schema = Schema::new(vec![element.clone()])?;
        let node = match kind {
            ListTableKind::Series => RelationNode::List { expr, element, page_rows: None },
            ListTableKind::Unnest => RelationNode::ColumnTable { expr, element },
        };
        let input = Arc::new(Relation::new(element_schema, node)?);
        let relation = match ordinality {
            Some(field) => Arc::new(input.with_ordinality(field, rtdb_plan::OrdinalityScope::Page)?),
            None => input,
        };
        Ok(relation)
    }

    fn mutation_table(&mut self, table: &ast::TableFactor, erase: bool) -> Result<Arc<Relation>> {
        let input = self.table_for(table, if erase { TablePurpose::Erase } else { TablePurpose::Mutation }, self.outer_scopes.len())?;
        let RelationNode::Scan(source) = input.node() else { return Err(error("Mutation target must be a base table")); };
        let mut scan = source.clone();
        if scan.system_time != TemporalFilter::Default { return Err(error("Mutation targets do not accept SYSTEM_TIME clauses")); }
        if erase {
            if scan.valid_time != TemporalFilter::Default { return Err(error("ERASE does not accept a valid-time extent")); }
            scan.valid_time = TemporalFilter::All;
            scan.system_time = TemporalFilter::All;
        } else {
            if scan.valid_time == TemporalFilter::Default {
                scan.valid_time = TemporalFilter::FromTo { from: Expr::Call { name: "current_timestamp".into(), args: vec![], return_type: crate::mutation::instant_type() }, to: crate::mutation::timestamp_literal(i64::MAX) };
            }
            if matches!(scan.valid_time, TemporalFilter::At(_) | TemporalFilter::Between { .. }) { return Err(error("UPDATE and DELETE require ALL or FROM/TO valid time")); }
            scan.clamp_valid_time = true;
        }
        let qualifier = scan.columns.first().and_then(|field| field.qualifier.clone());
        for (name, ty) in [("_iid", LogicalType::Scalar(ScalarType::FixedSizeBinary(16))), ("_valid_from", crate::mutation::instant_type()), ("_valid_to", crate::mutation::instant_type().maybe())] {
            if !scan.columns.iter().any(|field| field.name == name) {
                let mut field = self.binder.field(name, None, ty)?;
                field.qualifier = qualifier.clone();
                scan.columns.push(field);
            }
        }
        Ok(Arc::new(Relation::scan(scan)?))
    }

    fn temporal_filters(&mut self, table: &ast::ObjectName) -> Result<(TemporalFilter, TemporalFilter, bool)> {
        let end = table.0.last().and_then(ast::ObjectNamePart::as_ident).map(|name| name.span.end);
        let clauses: Vec<_> = self.temporal.iter().filter(|clause| Some(clause.owner_end) == end).cloned().collect();
        let mut valid = self.default_valid.clone();
        let mut system = self.default_system.clone();
        let mut clamp = false;
        for clause in clauses {
            if clause.system { system = Some(clause.extent); } else { valid = Some(clause.extent); clamp = clause.clamp; }
        }
        let valid = valid.map(|extent| self.bind_extent(extent)).transpose()?.unwrap_or(TemporalFilter::Default);
        let system = system.map(|extent| self.bind_extent(extent)).transpose()?.unwrap_or(TemporalFilter::Default);
        Ok((valid, system, clamp))
    }

    fn target(&self, name: &ast::ObjectName) -> Result<QualifiedTable> {
        grammar_table_name(name)?;
        let parts = identifiers(name)?.iter().map(Identifier::normalized).collect::<Vec<_>>();
        let (database, table) = match parts.as_slice() {
            [table] => (self.catalog.default_database().to_owned(), TableRef::new(self.options.search_path.first().cloned().unwrap_or_else(|| "public".into()), table)),
            [schema, table] => (self.catalog.default_database().to_owned(), TableRef::new(schema, table)),
            _ => return Err(error("DML target names have one or two components")),
        };
        Ok(QualifiedTable { database, table })
    }

    fn alias(&mut self, input: Arc<Relation>, alias: &ast::TableAlias) -> Result<Arc<Relation>> {
        let columns = if alias.columns.is_empty() { None } else { Some(alias.columns.iter().map(|column| identifier(&column.name).normalized()).collect()) };
        self.rename(input, Some(identifier(&alias.name).normalized()), columns)
    }

    fn rename(&mut self, input: Arc<Relation>, qualifier: Option<String>, columns: Option<Vec<String>>) -> Result<Arc<Relation>> {
        let prefix = self.symbol(qualifier.as_deref().unwrap_or("_table"))?;
        let input = match input.node() {
            RelationNode::Rename { input: previous, expressions, .. }
                if input.schema() == previous.schema() && expressions.iter().all(|named|
                    matches!(&named.expr, Expr::Column { id, .. } if *id == named.field.id)) =>
            {
                Arc::clone(previous)
            }
            _ => input,
        };
        let visible = input.schema().fields().iter().filter(|field| !self.hidden_columns.contains_key(&field.id)).count();
        if let Some(columns) = &columns
            && columns.len() != visible {
            self.diagnostics.push(PlanningDiagnostic::column_count(columns.len(), visible));
            return Ok(Arc::new(Relation::empty(Schema::default())));
        }
        let mut index = 0;
        let mut expressions = Vec::new();
        for field in input.schema().fields() {
            let hidden = self.hidden_columns.get(&field.id).copied();
            let name = if hidden.is_some() { field.name.clone() } else {
                let name = columns.as_ref().map(|columns| columns[index].clone()).unwrap_or_else(|| field.name.clone());
                index += 1;
                name
            };
            let mut renamed = self.binder.field(name, qualifier.clone(), field.data_type.clone())?;
            renamed.unnamed = field.unnamed && (hidden.is_some() || columns.is_none());
            if let Some(hidden) = hidden { self.hidden_columns.insert(renamed.id,hidden); }
            expressions.push(NamedExpr { field: renamed, expr: field.expression() });
        }
        Ok(Arc::new(Relation::new(Schema::new(expressions.iter().map(|expression| expression.field.clone()).collect())?, RelationNode::Rename { input, prefix, expressions })?))
    }

    fn symbol(&mut self, name: &str) -> Result<String> {
        Ok(format!("{name}.{}", self.allocate_symbol()?))
    }

    pub(crate) fn allocate_symbol(&mut self) -> Result<u64> {
        self.next_symbol = self.next_symbol.checked_add(1).ok_or_else(|| error("SQL symbol counter overflow"))?;
        Ok(self.next_symbol)
    }

    pub(crate) fn relational_sort_expr(&mut self, order: &ast::OrderByExpr, input: &mut Arc<Relation>, replacements: &Replacements) -> Result<SortExpr> {
        let expr = if let Some(index) = ordinal(&order.expr)? { input.schema().fields().get(index).ok_or_else(|| error("ORDER BY position is out of range"))?.expression() } else { self.relational_expr(&order.expr, input, replacements)? };
        let descending = sort_descending(&order.options)?;
        Ok(SortExpr { expr, descending, nulls_first: order.options.nulls_first.unwrap_or(descending) })
    }

}

pub(crate) fn sort_descending(options: &ast::OrderByOptions) -> Result<bool> {
    match &options.sort { Some(ast::OrderBySort::Desc) => Ok(true), None | Some(ast::OrderBySort::Asc) => Ok(false), Some(ast::OrderBySort::Using(_)) => Err(grammar_rejected("ORDER BY USING")) }
}

pub(crate) fn identifier(identifier: &ast::Ident) -> Identifier { Identifier { text: identifier.value.clone(), quoted: identifier.quote_style.is_some() } }
pub(crate) fn identifiers(name: &ast::ObjectName) -> Result<Vec<Identifier>> { name.0.iter().map(|part| part.as_ident().map(identifier).ok_or_else(|| grammar_rejected("A computed identifier"))).collect() }
pub(crate) fn error(message: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("xtdb/sql-error", message.to_string()) }
pub(crate) fn unsupported(message: impl std::fmt::Display) -> Anomaly { Anomaly::unsupported("xtdb/sql-unsupported", message.to_string()) }

const GRAMMAR_REJECTION: &str = "Errors parsing SQL statement:\n  - ";

/// A construct the vendor parser admits but the reference `Sql.g4` grammar has no production for.
pub(crate) fn grammar_rejected(construct: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("xtdb/sql-error", format!("{GRAMMAR_REJECTION}{construct} is not part of the XTDB SQL grammar"))
}

/// `Sql.g4`'s `identifier` admits an unquoted keyword only when it is non-reserved, whereas the vendor parser reads any word as a table name.
fn grammar_table_name(name: &ast::ObjectName) -> Result<()> {
    match name.0.iter().filter_map(ast::ObjectNamePart::as_ident).find(|part| part.quote_style.is_none() && !crate::parser::is_unquoted_identifier(&part.value)) {
        Some(reserved) => Err(grammar_rejected(format!("The reserved word {} as a table name", reserved.value))),
        None => Ok(()),
    }
}

fn is_grammar_rejection(error: &Anomaly) -> bool {
    error.code == "xtdb/sql-error" && error.message.as_deref().is_some_and(|message| message.starts_with(GRAMMAR_REJECTION))
}

fn scan_target(input: &Relation) -> Result<(QualifiedTable, TemporalFilter)> {
    match input.node() {
        RelationNode::Scan(scan) if scan.system_time == TemporalFilter::Default => Ok((scan.table.name.clone(), scan.valid_time.clone())),
        _ => Err(error("DML requires an unmodified table scan without SYSTEM_TIME")),
    }
}

fn apply_join(input: Arc<Relation>, dependent: Arc<Relation>, kind: ApplyKind, arguments: Vec<(usize, Expr)>) -> Result<Arc<Relation>> {
    let schema = input.schema().concat(&if kind == ApplyKind::Left { dependent.schema().nullable()? } else { dependent.schema().clone() })?;
    Ok(Arc::new(Relation::new(schema, RelationNode::Apply { input, dependent, arguments, kind, output: None })?))
}

fn join(left: Arc<Relation>, right: Arc<Relation>, kind: JoinKind, condition: Option<Expr>) -> Result<Arc<Relation>> {
    Ok(Arc::new(Relation::join(left, right, kind, condition)?))
}


fn rename_source(input: Arc<Relation>, prefix: String) -> Result<Arc<Relation>> {
    let schema = input.schema().clone();
    let expressions = schema.fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() }).collect();
    Ok(Arc::new(Relation::new(schema, RelationNode::Rename { input, prefix, expressions })?))
}

pub(crate) fn aggregate_relation(input: Arc<Relation>, groups: Vec<NamedExpr>, aggregates: Vec<AggregateExpr>) -> Result<Arc<Relation>> {
    let schema = Schema::new(groups.iter().map(|group| group.field.clone())
        .chain(aggregates.iter().map(|aggregate| aggregate.field.clone())).collect())?;
    Ok(Arc::new(Relation::new(schema, RelationNode::Aggregate { input, groups, aggregates })?))
}

fn implicit_expression_label(mut syntax: &ast::Expr, expression: &Expr, schema: &Schema) -> Option<String> {
    while let ast::Expr::Nested(inner) | ast::Expr::UnaryOp { op: ast::UnaryOperator::Plus, expr: inner } = syntax {
        syntax = inner;
    }
    let column = match syntax {
        ast::Expr::Identifier(name) => Some((name, 1)),
        ast::Expr::CompoundIdentifier(names) => names.last().map(|name| (name, names.len())),
        _ => None,
    };
    if let Some((column, parts)) = column {
        let column = identifier(column);
        if crate::expression::bare_scalar_name(&column, parts).is_some() {
            None
        } else {
            match expression {
                Expr::Column { id, .. } => schema.get(*id).filter(|field| !field.unnamed).map(|field| field.name.clone()),
                Expr::Parameter { .. } => Some(column.normalized()),
                _ => None,
            }
        }
    } else {
        match syntax {
            ast::Expr::CompoundFieldAccess { access_chain, .. } => match access_chain.last() {
                Some(ast::AccessExpr::Dot(ast::Expr::Identifier(name))) => Some(identifier(name).normalized()),
                _ => None,
            },
            ast::Expr::Function(function) if function.name.0.last()
                .and_then(ast::ObjectNamePart::as_ident)
                .is_some_and(|name| name.quote_style.is_none() && name.value.eq_ignore_ascii_case("version")) =>
            {
                Some("version".into())
            }
            _ => None,
        }
    }
}

fn ordinal(expr: &ast::Expr) -> Result<Option<usize>> {
    if let ast::Expr::Value(value) = expr {
        if let ast::Value::Number(number, _) = &value.value {
            if !number.contains(['.', 'e', 'E']) { return number.parse::<usize>().ok().and_then(|number| number.checked_sub(1)).map(Some).ok_or_else(|| error("SQL column positions start at one")); }
        }
    }
    Ok(None)
}

/// A bare GROUP BY name that no input column takes groups by the select item it aliases; an input column wins a clash.
fn alias_projection<'a>(expr: &ast::Expr, projection: &'a [ast::SelectItem], input: &Schema) -> Option<&'a ast::Expr> {
    let ast::Expr::Identifier(name) = expr else { return None; };
    let name = identifier(name);
    if input.resolve(std::slice::from_ref(&name)).is_ok() { return None; }
    projection.iter().find_map(|item| match item {
        ast::SelectItem::ExprWithAlias { expr, alias } if identifier(alias).normalized() == name.normalized() => Some(expr),
        _ => None,
    })
}
