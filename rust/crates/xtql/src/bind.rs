use rtdb_types::StructFields;
use crate::{Binding, Clause, Expr, FormAllocation, From, Query, RelationInput, SubqueryKind, Tail, TemporalFilter, error, read};
use rtdb_plan::{AggregateExpr, AggregateFunction, ApplyKind, BinaryOp, Binder, Catalog, Expr as PExpr, Field, Identifier, JoinKind, NamedExpr, PreparedStatement, QualifiedTable, Relation, RelationNode, Scan, Schema, SetOperator, SortExpr, Statement, TableBinding, TemporalFilter as PTemporal, UnaryOp};
use rtdb_types::{Anomaly, ListKind, LogicalType, MemoryBudget, Result, ScalarType, TimeUnit, Value};
use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

mod ra;
pub use ra::compile_ra_in;

#[derive(Clone, Debug)]
pub struct CompileOptions {
    /// Indexed by external argument position; absent positions remain unknown until argument rebinding.
    pub parameter_types: Vec<LogicalType>,
    /// Maps a top-level function's parameters to external argument positions, as required by SQL-inline XTQL.
    pub parameter_indices: Option<Vec<usize>>,
    pub named_parameters: BTreeMap<String, (usize, LogicalType)>,
    /// Execution-zone metadata for `cast-tstz`; defaults to UTC.
    pub default_timezone: String,
}

impl Default for CompileOptions {
    fn default() -> Self { Self { parameter_types: Vec::new(), parameter_indices: None, named_parameters: BTreeMap::new(), default_timezone: "UTC".into() } }
}

/// Binds all sources against one retained catalog and emits a native query plan.
/// Parameter types describe the supplied arguments; nested function arguments receive separate apply-local positions.
pub fn compile(source: &str, catalog: &dyn Catalog, options: &CompileOptions) -> Result<PreparedStatement> {
    compile_with(source, catalog, options, FormAllocation::Untracked)
}

/// Binds source at its existing parse/bind demand points, retaining raw metadata in the supplied budget.
/// Size, native allocation and admission errors propagate without an untracked fallback.
pub fn compile_in(source: &str, catalog: &dyn Catalog, options: &CompileOptions, memory: &Arc<MemoryBudget>) -> Result<PreparedStatement> {
    compile_with(source, catalog, options, FormAllocation::Tracked(memory))
}

fn compile_with(source: &str, catalog: &dyn Catalog, options: &CompileOptions, forms: FormAllocation<'_>) -> Result<PreparedStatement> {
    let query = crate::ast::parse_query_with(&read(source)?, forms)?;
    let mut next_parameter = options.parameter_types.len();
    for index in options.named_parameters.values().map(|(index, _)| index).chain(options.parameter_indices.iter().flatten()) {
        next_parameter = next_parameter.max(index.checked_add(1).ok_or_else(|| error("xtdb/query-too-large", "Parameter identifier space is exhausted"))?);
    }
    let mut binder = Binder::default();
    let mut compiler = Compiler { catalog, binder: &mut binder, next_parameter: &mut next_parameter, timezone: &options.default_timezone, forms, symbols: Symbols::Xtql };
    let mut parameters = Parameters::default();
    for (name, (index, data_type)) in &options.named_parameters {
        parameters.named.insert(normal(name), compiler.binder.parameter(*index, data_type.clone()));
    }
    let body = if let Query::Function { parameters: names, body } = &query {
        if options.parameter_indices.as_ref().is_some_and(|indices| indices.len() != names.len())
            || (!options.parameter_types.is_empty() && options.parameter_indices.is_none() && options.parameter_types.len() != names.len())
        { return Err(error("xtql/fn-arity-error", "XTQL function argument count differs from its parameter vector")); }
        for (position, name) in names.iter().enumerate() {
            let index = options.parameter_indices.as_ref().map_or(position, |indices| indices[position]);
            let data_type = options.parameter_types.get(index).cloned().unwrap_or(LogicalType::Null);
            parameters.lexical.insert(name.clone(), compiler.binder.parameter(index, data_type));
            *compiler.next_parameter = (*compiler.next_parameter).max(index.checked_add(1).ok_or_else(|| error("xtdb/query-too-large", "Parameter identifier space is exhausted"))?);
        }
        body.as_ref()
    } else { &query };
    let plan = compiler.query(body, &parameters)?;
    Ok(PreparedStatement { statement: Statement::Query(plan.relation), parameter_types: compiler.binder.parameter_types().clone(), source: source.into(), notices: Vec::new(), settings: Default::default() })
}

/// Binds an embedded query using the caller's column identities and apply-parameter allocator.
/// Arguments pair external parameter positions with their types and must match a top-level function's arity.
/// Queries without a top-level function ignore the arguments.
/// The allocator starts above every parameter owned by the containing statement and advances for nested applies.
/// `timezone` binds session-dependent cast result metadata to the containing statement's execution zone.
pub fn compile_query(query: &Query, catalog: &dyn Catalog, binder: &mut Binder, next_parameter: &mut usize, arguments: &[(usize, LogicalType)], timezone: &str) -> Result<Arc<Relation>> {
    compile_query_with(query, catalog, binder, next_parameter, arguments, timezone, FormAllocation::Untracked)
}

/// Binds a possibly foreign AST while copying every reached raw recipe into the caller's budget.
/// Catalog/argument/source order is unchanged; native copy/admission errors leave the input AST unchanged.
pub fn compile_query_in(query: &Query, catalog: &dyn Catalog, binder: &mut Binder, next_parameter: &mut usize, arguments: &[(usize, LogicalType)], timezone: &str, memory: &Arc<MemoryBudget>) -> Result<Arc<Relation>> {
    compile_query_with(query, catalog, binder, next_parameter, arguments, timezone, FormAllocation::Tracked(memory))
}

fn compile_query_with(query: &Query, catalog: &dyn Catalog, binder: &mut Binder, next_parameter: &mut usize, arguments: &[(usize, LogicalType)], timezone: &str, forms: FormAllocation<'_>) -> Result<Arc<Relation>> {
    let mut parameters = Parameters::default();
    let body = if let Query::Function { parameters: names, body } = query {
        if names.len() != arguments.len() { return Err(error("xtql/fn-arity-error", format!("XTQL fn expects {} argument(s), got {}", names.len(), arguments.len()))); }
        for (name, (index, data_type)) in names.iter().zip(arguments) {
            *next_parameter = (*next_parameter).max(index.checked_add(1).ok_or_else(|| error("xtdb/query-too-large", "Parameter identifier space is exhausted"))?);
            parameters.lexical.insert(name.clone(), binder.parameter(*index, data_type.clone()));
        }
        body.as_ref()
    } else { query };
    let mut compiler = Compiler { catalog, binder, next_parameter, timezone, forms, symbols: Symbols::Xtql };
    Ok(compiler.query(body, &parameters)?.relation)
}

#[derive(Clone, Default)]
struct Parameters {
    lexical: BTreeMap<String, PExpr>,
    named: BTreeMap<String, PExpr>,
}

impl Parameters {
    fn xtql(&self, name: &str) -> Option<&PExpr> {
        self.lexical.get(name).or_else(|| self.named.get(&normal(name)))
    }
}

#[derive(Clone, Copy)]
enum Symbols { Xtql, Relational }

impl Symbols {
    fn out_of_scope(self, name: &str) -> Anomaly {
        match self {
            Self::Xtql => error("xtql/invalid-expression", format!("Not all variables in expression are in scope: {name}")),
            Self::Relational => error("xtdb.expression/unknown-symbol", format!("Unknown symbol: '{name}'")).with_context("symbol", name.into()),
        }
    }

    fn column(self, name: &str) -> std::borrow::Cow<'_, str> {
        match self {
            Self::Xtql => std::borrow::Cow::Owned(normal(name)),
            Self::Relational => std::borrow::Cow::Borrowed(name),
        }
    }
}

struct Compiler<'a, 'b> {
    catalog: &'a dyn Catalog,
    binder: &'b mut Binder,
    next_parameter: &'b mut usize,
    timezone: &'a str,
    forms: FormAllocation<'a>,
    symbols: Symbols,
}

struct Bound { relation: Arc<Relation>, variables: BTreeMap<String, Field> }

impl Bound {
    fn unit() -> Self { Self { relation: Arc::new(Relation::unit()), variables: BTreeMap::new() } }
    fn from_relation(relation: Relation) -> Self {
        let variables = relation.schema().fields().iter().map(|field| (field.name.clone(), field.clone())).collect();
        Self { relation: Arc::new(relation), variables }
    }
}

impl Compiler<'_, '_> {
    fn parameter_index(&mut self) -> Result<usize> {
        let index = *self.next_parameter;
        *self.next_parameter = index.checked_add(1).ok_or_else(|| error("xtdb/query-too-large", "Parameter identifier space is exhausted"))?;
        Ok(index)
    }

    fn query(&mut self, query: &Query, parameters: &Parameters) -> Result<Bound> {
        match query {
            Query::Function { parameters: names, body } => {
                if names.iter().any(|name| parameters.xtql(name).is_none()) { return Err(error("xtql/fn-arity-error", "Query function has unbound parameters")); }
                self.query(body, parameters)
            }
            Query::Pipeline { head, tails } => {
                let mut bound = self.query(head, parameters)?;
                for tail in tails { bound = self.tail(bound, tail, parameters)?; }
                Ok(bound)
            }
            Query::From(from) => self.from(from, parameters),
            Query::Rel { input, bindings } => self.rel(input, bindings, parameters),
            Query::Unify(clauses) => self.unify(clauses, parameters),
            Query::UnionAll(queries) => {
                let mut plans = queries.iter().map(|query| self.query(query, parameters)).collect::<Result<Vec<_>>>()?;
                let names: BTreeSet<_> = plans.iter().flat_map(|plan| plan.variables.keys().cloned()).collect();
                for plan in &mut plans {
                    let expressions = names.iter().map(|name| {
                        let expr = plan.variables.get(name).map_or(PExpr::Literal(Value::Null), Field::expression);
                        Ok(NamedExpr { field: self.binder.field(name, None, expr.data_type())?, expr })
                    }).collect::<Result<Vec<_>>>()?;
                    *plan = Bound::from_relation(plan.relation.clone().project(expressions)?);
                }
                let mut plans = plans.into_iter();
                let mut result = plans.next().ok_or_else(|| error("xtql/malformed-union", "Union requires an input"))?;
                for right in plans {
                    let fields = result.relation.schema().fields().iter().zip(right.relation.schema().fields()).map(|(left, right)| {
                        self.binder.field(&left.name, None, left.data_type.join(&right.data_type))
                    }).collect::<Result<_>>()?;
                    result = Bound::from_relation(Relation::new(Schema::new(fields)?, RelationNode::Set {
                        left: result.relation, right: right.relation, operator: SetOperator::Union, all: true,
                    })?);
                }
                Ok(result)
            }
        }
    }

    fn from(&mut self, from: &From, parameters: &Parameters) -> Result<Bound> {
        let database = from.database.clone().unwrap_or_else(|| self.catalog.default_database().into());
        if !self.catalog.databases().contains(&database) { return Err(error("xtdb/unknown-db", format!("Unknown database: {database}"))); }
        let name = QualifiedTable { database, table: from.table.clone() };
        let table = self.catalog.table(&name)?.unwrap_or(TableBinding { name, oid: None, columns: StructFields::new() });
        let star_names: BTreeSet<_> = table.columns.keys().cloned().collect();
        let mut names: BTreeSet<String> = from.bindings.iter().map(|binding| normal(&binding.name)).collect();
        if from.star { names.extend(table.columns.keys().cloned()); }
        for (period, from, to) in [("_valid_time", "_valid_from", "_valid_to"), ("_system_time", "_system_from", "_system_to")] {
            if names.remove(period) { names.insert(from.into()); names.insert(to.into()); }
        }
        let columns = names.iter().map(|name| self.binder.field(name, None, table.scan_column_type(name))).collect::<Result<Vec<_>>>()?;
        let valid_time = self.temporal(from.valid_time.as_ref(), parameters)?;
        let system_time = self.temporal(from.system_time.as_ref(), parameters)?;
        let scan = Relation::scan(Scan { table, columns, valid_time, system_time, clamp_valid_time: false, iids: None, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new() })?;
        let mut bound = Bound::from_relation(scan);
        let mut period_projections = Vec::new();
        for (period, lower_name, upper_name) in [("_valid_time", "_valid_from", "_valid_to"), ("_system_time", "_system_from", "_system_to")] {
            if from.bindings.iter().any(|binding| normal(&binding.name) == period) {
                let lower = bound.variables.get(lower_name).ok_or_else(|| error("xtql/invalid-expression", "Temporal lower endpoint is missing"))?.expression();
                let upper = bound.variables.get(upper_name).ok_or_else(|| error("xtql/invalid-expression", "Temporal upper endpoint is missing"))?.expression();
                let data_type = LogicalType::List { kind: ListKind::TimestampTzRange, element: Box::new(timestamp_type()) };
                period_projections.push(NamedExpr { field: self.binder.field(period, None, data_type.clone())?, expr: PExpr::Call { name: "period".into(), args: vec![lower, upper], return_type: data_type } });
            }
        }
        if !period_projections.is_empty() {
            let mut expressions = pass_through(&bound);
            expressions.extend(period_projections);
            bound = Bound::from_relation(bound.relation.project(expressions)?);
        }
        self.outputs(bound, &from.bindings, from.star.then_some(&star_names), parameters)
    }

    fn temporal(&mut self, filter: Option<&TemporalFilter>, parameters: &Parameters) -> Result<PTemporal> {
        let mut endpoint = |expr: &Expr| {
            if contains_subquery(expr) {
                return Err(error("xtql/invalid-expression", "Temporal scan filters cannot contain relational subqueries; pass the bound as a query parameter"));
            }
            self.expr(expr, &mut Bound::unit(), parameters)
        };
        Ok(match filter {
            None => PTemporal::Default,
            Some(TemporalFilter::All) => PTemporal::All,
            Some(TemporalFilter::At(at)) => PTemporal::At(endpoint(at)?),
            Some(TemporalFilter::In { from, to }) => PTemporal::FromTo {
                from: from.as_ref().map(&mut endpoint).transpose()?.unwrap_or(PExpr::Literal(Value::Null)),
                to: to.as_ref().map(endpoint).transpose()?.unwrap_or(PExpr::Literal(Value::Null)),
            },
        })
    }

    fn outputs(&mut self, mut bound: Bound, bindings: &[Binding], star: Option<&BTreeSet<String>>, parameters: &Parameters) -> Result<Bound> {
        let sources = bound.variables.clone();
        let mut output: BTreeMap<String, NamedExpr> = BTreeMap::new();
        let mut order = Vec::new();
        if let Some(star) = star {
            for (name, source) in sources.iter().filter(|(name, _)| star.contains(*name)) {
                output.insert(name.clone(), NamedExpr { field: self.binder.field(name, None, source.data_type.clone())?, expr: source.expression() });
            }
        }
        let mut predicates = Vec::new();
        let mut expression_scope = BTreeMap::new();
        for binding in bindings {
            let source = sources.get(&normal(&binding.name)).ok_or_else(|| error("xtql/invalid-expression", format!("Missing source column {}", binding.name)))?;
            if let Some(variable) = binding_variable(&binding.expression, parameters) {
                let variable = normal(variable);
                if !order.contains(&variable) { order.push(variable.clone()); }
                if let Some(existing) = output.get(&variable) {
                    if existing.expr != source.expression() { predicates.push(PExpr::binary(BinaryOp::Eq, existing.expr.clone(), source.expression())); }
                }
                else { output.insert(variable.clone(), NamedExpr { field: self.binder.field(&variable, None, source.data_type.clone())?, expr: source.expression() }); }
                expression_scope.insert(variable, source.clone());
            }
        }
        if let Some(star) = star { expression_scope.extend(sources.iter().filter(|(name, _)| star.contains(*name)).map(|(name, field)| (name.clone(), field.clone()))); }
        bound.variables = expression_scope;
        for binding in bindings {
            if binding_variable(&binding.expression, parameters).is_some() { continue; }
            let source = sources.get(&normal(&binding.name)).ok_or_else(|| error("xtql/invalid-expression", "Missing binding source"))?.expression();
            let value = self.expr(&binding.expression, &mut bound, parameters)?;
            predicates.push(PExpr::binary(BinaryOp::Eq, source, value));
        }
        for predicate in predicates { bound.relation = Arc::new(bound.relation.filter(predicate)?); }
        let mut projections: Vec<_> = order.iter().filter_map(|name| output.remove(name)).collect();
        projections.extend(output.into_values());
        Ok(Bound::from_relation(bound.relation.project(projections)?))
    }

    fn rel(&mut self, input: &RelationInput, bindings: &[Binding], parameters: &Parameters) -> Result<Bound> {
        let requested: BTreeSet<_> = bindings.iter().map(|binding| normal(&binding.name)).collect();
        let bound = match input {
            RelationInput::Documents(documents) => {
                let names: BTreeSet<_> = documents.iter().flat_map(|doc| doc.iter().map(|(name, _)| normal(name))).chain(requested).collect();
                let mut types: StructFields<LogicalType> = names.iter().map(|name| (name.clone(), LogicalType::Nothing)).collect();
                let mut rows = Vec::new();
                for document in documents {
                    let document: BTreeMap<_, _> = document.iter().map(|(name, value)| (normal(name), value)).collect();
                    let mut row = Bound::unit();
                    let mut values = Vec::new();
                    for name in &names {
                        let expr = document.get(name).map(|expr| self.expr(expr, &mut row, parameters)).transpose()?.unwrap_or(PExpr::Literal(Value::Null));
                        types.entry(name.clone()).and_modify(|data_type| *data_type = data_type.join(&expr.data_type()));
                        values.push(expr);
                    }
                    rows.push((row, values));
                }
                let fields = names.iter().map(|name| {
                    let data_type = match types.get(name) { Some(LogicalType::Nothing) | None => LogicalType::Null, Some(data_type) => data_type.clone() };
                    self.binder.field(name, None, data_type)
                }).collect::<Result<Vec<_>>>()?;
                let schema = Schema::new(fields.clone())?;
                let mut rows = rows.into_iter();
                if let Some((row, expressions)) = rows.next() {
                    let first = row.relation.project(fields.iter().cloned().zip(expressions).map(|(field, expr)| NamedExpr { field, expr }).collect())?;
                    let mut relation = Arc::new(first);
                    for (row, expressions) in rows {
                        let right = Arc::new(row.relation.project(fields.iter().cloned().zip(expressions).map(|(field, expr)| NamedExpr { field, expr }).collect())?);
                        relation = Arc::new(Relation::new(schema.clone(), RelationNode::Set { left: relation, right, operator: SetOperator::Union, all: true })?);
                    }
                    Bound { variables: fields.into_iter().map(|field| (field.name.clone(), field)).collect(), relation }
                } else { Bound::from_relation(Relation::empty(schema)) }
            }
            RelationInput::Parameter(name) => {
                let expression = parameters.xtql(name).cloned().ok_or_else(|| error("xtql/rel", "Illegal second argument to `rel`"))?;
                let element_type = list_element(&expression.data_type());
                let element = self.binder.field("xt$document", None, element_type.clone())?;
                let relation = Relation::new(Schema::new(vec![element.clone()])?, RelationNode::Unnest {
                    input: Arc::new(Relation::unit()), expr: expression, element: element.clone(), ordinality: None, outer: false,
                })?;
                let expressions = requested.iter().map(|name| {
                    let data_type = field_type(&element_type, name);
                    Ok(NamedExpr { field: self.binder.field(name, None, data_type.clone())?, expr: PExpr::Field { expr: Box::new(element.expression()), name: name.clone(), data_type } })
                }).collect::<Result<Vec<_>>>()?;
                Bound::from_relation(Arc::new(relation).project(expressions)?)
            }
        };
        self.outputs(bound, bindings, None, parameters)
    }

    fn tail(&mut self, mut bound: Bound, tail: &Tail, parameters: &Parameters) -> Result<Bound> {
        match tail {
            Tail::Where(predicates) => {
                for predicate in predicates {
                    let predicate = self.expr(predicate, &mut bound, parameters)?;
                    bound.relation = Arc::new(bound.relation.filter(predicate)?);
                }
                self.visible(bound)
            }
            Tail::With(bindings) => self.projection(bound, bindings, true, false, parameters),
            Tail::Return(bindings) => self.projection(bound, bindings, false, false, parameters),
            Tail::Without(names) => {
                for name in names { bound.variables.remove(&normal(name)); }
                self.visible(bound)
            }
            Tail::Unnest(binding) => self.unnest(bound, binding, false, parameters),
            Tail::Aggregate(bindings) => self.aggregate(bound, bindings, parameters),
            Tail::OrderBy(order) => {
                let mut order_by = Vec::new();
                for order in order {
                    order_by.push(SortExpr { expr: self.expr(&order.expression, &mut bound, parameters)?, descending: order.descending, nulls_first: order.nulls_first.unwrap_or(order.descending) });
                }
                bound.relation = Arc::new(bound.relation.sort(order_by)?);
                self.visible(bound)
            }
            Tail::Limit(expr) | Tail::Offset(expr) => {
                let expression = self.expr(expr, &mut bound, parameters)?;
                if !matches!(&expression, PExpr::Literal(Value::I64(value)) if *value >= 0) && !matches!(&expression, PExpr::Parameter { .. }) {
                    return Err(if matches!(tail, Tail::Limit(_)) { error("xtql/invalid-limit", "Limit must be a non-negative integer literal or parameter") }
                        else { error("xtql/invalid-offset", "Offset must be a non-negative integer literal or parameter") });
                }
                bound.relation = Arc::new(if matches!(tail, Tail::Limit(_)) { bound.relation.limit(None, Some(expression))? } else { bound.relation.limit(Some(expression), None)? });
                Ok(bound)
            }
        }
    }

    fn visible(&self, bound: Bound) -> Result<Bound> {
        Ok(Bound::from_relation(bound.relation.clone().project(pass_through(&bound))?))
    }

    fn projection(&mut self, mut bound: Bound, bindings: &[Binding], keep: bool, unify: bool, parameters: &Parameters) -> Result<Bound> {
        let old = bound.variables.clone();
        // In the reference's projection order: passed-through variables, then each new binding as written.
        let mut projections: Vec<(String, NamedExpr)> = if keep { pass_through(&bound).into_iter().map(|projection| (projection.field.name.clone(), projection)).collect() } else { Vec::new() };
        let mut predicates = Vec::new();
        for binding in bindings {
            let name = normal(&binding.name);
            let expr = self.expr(&binding.expression, &mut bound, parameters)?;
            if unify {
                if let Some(existing) = old.get(&name) { predicates.push(PExpr::binary(BinaryOp::Eq, existing.expression(), expr)); continue; }
            }
            let field = self.binder.field(&name, None, expr.data_type())?;
            match projections.iter_mut().find(|(existing, _)| *existing == name) {
                Some((_, projection)) => *projection = NamedExpr { field, expr },
                None => projections.push((name, NamedExpr { field, expr })),
            }
        }
        for predicate in predicates { bound.relation = Arc::new(bound.relation.filter(predicate)?); }
        Ok(Bound::from_relation(bound.relation.project(projections.into_iter().map(|(_, projection)| projection).collect())?))
    }

    fn unnest(&mut self, mut bound: Bound, binding: &Binding, unify: bool, parameters: &Parameters) -> Result<Bound> {
        let expression = self.expr(&binding.expression, &mut bound, parameters)?;
        let name = normal(&binding.name);
        let element = self.binder.field(&name, None, list_element(&expression.data_type()))?;
        let schema = bound.relation.schema().concat(&Schema::new(vec![element.clone()])?)?;
        bound.relation = Arc::new(Relation::new(schema, RelationNode::Unnest { input: bound.relation, expr: expression, element: element.clone(), ordinality: None, outer: false })?);
        if let Some(existing) = bound.variables.get(&name).filter(|_| unify) {
            bound.relation = Arc::new(bound.relation.filter(PExpr::binary(BinaryOp::Eq, existing.expression(), element.expression()))?);
        } else { bound.variables.insert(name, element); }
        self.visible(bound)
    }

    fn unify(&mut self, clauses: &[Clause], parameters: &Parameters) -> Result<Bound> {
        let mut bound = Bound::unit();
        let mut pending = Vec::new();
        for clause in clauses {
            match clause {
                Clause::Source(source) => {
                    if matches!(source, Query::From(From { star: true, .. })) { return Err(error("xtql/invalid-from", "* is not a valid in from when inside a unify context")); }
                    let source = self.query(source, parameters)?;
                    bound = self.join(bound, source, false, None)?;
                }
                Clause::With(bindings) => pending.extend(bindings.iter().map(|binding| Clause::With(vec![binding.clone()]))),
                Clause::Where(predicates) => pending.extend(predicates.iter().map(|predicate| Clause::Where(vec![predicate.clone()]))),
                clause => pending.push(clause.clone()),
            }
        }
        pending.sort_by_key(|clause| match clause { Clause::Where(_) => 0, Clause::With(_) => 1, Clause::Join { .. } => 2, Clause::Unnest(_) => 3, Clause::Source(_) => 4 });
        while !pending.is_empty() {
            let Some(index) = pending.iter().position(|clause| required_clause(clause, parameters).iter().all(|name| bound.variables.contains_key(name))) else {
                return Err(error("no-available-clauses", "No available clauses: unresolved or cyclic variable dependencies"));
            };
            match pending.remove(index) {
                Clause::Where(predicates) => bound = self.tail(bound, &Tail::Where(predicates), parameters)?,
                Clause::With(bindings) => bound = self.projection(bound, &bindings, true, true, parameters)?,
                Clause::Unnest(binding) => bound = self.unnest(bound, &binding, true, parameters)?,
                Clause::Join { left, query, arguments, bindings } => {
                    let (dependent, arguments) = self.subquery(&query, arguments.as_deref(), &mut bound, parameters)?;
                    let dependent = self.outputs(dependent, &bindings, None, parameters)?;
                    bound = self.join(bound, dependent, left, Some(arguments))?;
                }
                Clause::Source(_) => return Err(error("xtql/invalid-expression", "Source clause escaped source planning")),
            }
        }
        Ok(bound)
    }

    fn join(&mut self, left: Bound, mut right: Bound, outer: bool, apply: Option<Vec<(usize, PExpr)>>) -> Result<Bound> {
        // A join with arguments is the reference's left-outer apply, whose dependent-side variable replaces the outer one it unifies with;
        // without arguments it is a left-outer join, which keeps the outer one.
        let dependent_wins = outer && apply.as_ref().is_some_and(|arguments| !arguments.is_empty());
        let shared: Vec<_> = left.variables.iter().filter_map(|(name, left)| right.variables.get(name).map(|right| (left.clone(), right.clone()))).collect();
        let relation = if let Some(mut arguments) = apply {
            for (left, right_field) in &shared {
                let index = self.parameter_index()?;
                arguments.push((index, left.expression()));
                let parameter = PExpr::Parameter { index, data_type: left.data_type.clone() };
                right.relation = Arc::new(right.relation.filter(PExpr::binary(BinaryOp::Eq, right_field.expression(), parameter))?);
            }
            let schema = left.relation.schema().concat(&if outer { right.relation.schema().nullable()? } else { right.relation.schema().clone() })?;
            Relation::new(schema, RelationNode::Apply { input: left.relation, dependent: right.relation, arguments, kind: if outer { ApplyKind::Left } else { ApplyKind::Inner }, output: None })?
        } else {
            let condition = and(shared.iter().map(|(left, right)| PExpr::binary(BinaryOp::Eq, left.expression(), right.expression())).collect());
            Relation::join(left.relation, right.relation, if outer { JoinKind::Left } else { JoinKind::Inner }, condition)?
        };
        let mut variables = left.variables;
        for (name, mut field) in right.variables {
            if outer { field.data_type = field.data_type.maybe(); }
            if dependent_wins { variables.insert(name, field); } else { variables.entry(name).or_insert(field); }
        }
        self.visible(Bound { relation: Arc::new(relation), variables })
    }

    fn subquery(&mut self, query: &Query, args: Option<&[Expr]>, outer: &mut Bound, parameters: &Parameters) -> Result<(Bound, Vec<(usize, PExpr)>)> {
        let mut local = parameters.clone();
        let mut arguments = Vec::new();
        let body = if let Query::Function { parameters: names, body } = query {
            if args.is_some_and(|args| args.len() != names.len()) { return Err(error("xtql/subquery-arity-error", "Subquery arity error")); }
            for (position, name) in names.iter().enumerate() {
                let implicit = Expr::Variable(name.clone());
                let argument = self.expr(args.map_or(&implicit, |args| &args[position]), outer, parameters)?;
                let index = self.parameter_index()?;
                local.lexical.insert(name.clone(), PExpr::Parameter { index, data_type: argument.data_type() });
                arguments.push((index, argument));
            }
            body.as_ref()
        } else {
            if args.is_some_and(|args| !args.is_empty()) { return Err(error("xtql/subquery-arity-error", "A query without a function takes no arguments")); }
            query
        };
        Ok((self.query(body, &local)?, arguments))
    }

    fn expr(&mut self, expr: &Expr, bound: &mut Bound, parameters: &Parameters) -> Result<PExpr> {
        Ok(match expr {
            Expr::Literal(value) => PExpr::Literal(value.clone()),
            Expr::RegexReplace { source,recipe } => PExpr::regex_replace(self.expr(source,bound,parameters)?,self.forms.copy(recipe)?),
            Expr::Cast { expr, target } => {
                let expr = self.expr(expr, bound, parameters)?;
                target.validate_source(&expr.data_type())?;
                PExpr::Cast { expr: Box::new(expr), target: target.bind_timezone(self.timezone) }
            }
            Expr::Variable(name) => {
                let parameter = match self.symbols {
                    Symbols::Xtql => parameters.xtql(name),
                    Symbols::Relational if name.starts_with('?') => parameters.named.get(&relational_parameter(name)),
                    Symbols::Relational => None,
                };
                parameter.cloned()
                    .or_else(|| bound.variables.get(self.symbols.column(name).as_ref()).map(Field::expression))
                    .ok_or_else(|| self.symbols.out_of_scope(name))?
            }
            Expr::LogicVariable(name) => bound.variables.get(self.symbols.column(name).as_ref()).map(Field::expression)
                .ok_or_else(|| self.symbols.out_of_scope(name))?,
            Expr::Get { object, field } => {
                let object = self.expr(object, bound, parameters)?;
                let name = field.clone();
                let data_type = field_type(&object.data_type(), &name);
                PExpr::Field { expr: Box::new(object), name, data_type }
            }
            Expr::List(values) => PExpr::List(values.iter().map(|value| self.expr(value, bound, parameters)).collect::<Result<_>>()?),
            Expr::Set(values) => {
                let args = values.iter().map(|value| self.expr(value, bound, parameters)).collect::<Result<Vec<_>>>()?;
                let types: Vec<_> = args.iter().map(PExpr::data_type).collect();
                PExpr::Call { name: "set".into(), args, return_type: LogicalType::List { kind: ListKind::Set, element: Box::new(LogicalType::join_all(&types)) } }
            }
            Expr::Map(values) => PExpr::Struct(values.iter().map(|(name, value)| Ok((normal(name), self.expr(value, bound, parameters)?))).collect::<Result<_>>()?),
            Expr::Call { function, arguments } => {
                if matches!(self.symbols, Symbols::Xtql)
                    && ["percentile-cont", "percentile_cont", "percentile-disc", "percentile_disc"]
                        .iter().any(|name| function.eq_ignore_ascii_case(name))
                {
                    return Err(error("xtql/invalid-expression", "Ordered-set percentiles require SQL PERCENTILE_CONT or PERCENTILE_DISC with WITHIN GROUP"));
                }
                if aggregate_function(function).is_some() { return Err(error("xtql/invalid-expression", "Aggregate functions require an aggregate clause")); }
                let args = arguments.iter().map(|value| self.expr(value, bound, parameters)).collect::<Result<Vec<_>>>()?;
                rtdb_plan::bind_clock_timezone(scalar_call(function, args)?,Some(self.timezone))?
            }
            Expr::Subquery { kind, query, arguments } => {
                let (dependent, arguments) = self.subquery(query, arguments.as_deref(), bound, parameters)?;
                let document = LogicalType::Struct(dependent.variables.iter().map(|(name, field)| (name.clone(), field.data_type.clone())).collect());
                let (kind, data_type) = match kind {
                    SubqueryKind::Scalar => {
                        if dependent.relation.schema().len() != 1 { return Err(error("xtql/invalid-scalar-subquery", "Scalar subquery must only return a single column")); }
                        (ApplyKind::Scalar, dependent.relation.schema().fields()[0].data_type.clone().maybe())
                    }
                    SubqueryKind::Exists => (ApplyKind::Exists, LogicalType::Scalar(ScalarType::Bool)),
                    SubqueryKind::Pull => (ApplyKind::Pull, document.maybe()),
                    SubqueryKind::PullMany => (ApplyKind::PullMany, LogicalType::list(document).maybe()),
                };
                let output = self.binder.field("xt$subquery", None, data_type)?;
                let schema = bound.relation.schema().concat(&Schema::new(vec![output.clone()])?)?;
                bound.relation = Arc::new(Relation::new(schema, RelationNode::Apply { input: bound.relation.clone(), dependent: dependent.relation, arguments, kind, output: Some(output.clone()) })?);
                output.expression()
            }
        })
    }

    fn aggregate(&mut self, mut bound: Bound, bindings: &[Binding], parameters: &Parameters) -> Result<Bound> {
        let mut groups = BTreeMap::new();
        for binding in bindings {
            if let Some(name) = binding_variable(&binding.expression, parameters) {
                let field = bound.variables.get(&normal(name)).cloned().ok_or_else(|| error("xtql/invalid-expression", "Not all variables in expression are in scope"))?;
                groups.insert(normal(name), NamedExpr { expr: field.expression(), field });
            }
        }
        let mut aggregates = Vec::new();
        let mut outputs = Vec::new();
        for binding in bindings {
            let expression = self.rewrite_aggregates(&binding.expression, &mut bound, parameters, &mut aggregates)?;
            let mut required = BTreeSet::new();
            required_expr(&expression, parameters, &mut required);
            let ungrouped = || required.iter().filter(|name| !name.starts_with("xt$aggregate$") && !groups.contains_key(*name));
            if ungrouped().any(|name| !bound.variables.contains_key(name)) {
                return Err(error("xtql/invalid-expression", "Not all variables in expression are in scope"));
            }
            if ungrouped().next().is_some() {
                return Err(error("xtql/invalid-expression", "Variables outside of aggregate expressions must be grouping columns"));
            }
            outputs.push(Binding { name: binding.name.clone(), expression });
        }
        if !groups.is_empty() {
            for aggregate in &mut aggregates {
                if aggregate.function.canonical() == AggregateFunction::ArrayAgg {
                    let input_type = aggregate.argument.as_ref().map(PExpr::data_type).unwrap_or(LogicalType::Null);
                    aggregate.field.data_type = aggregate_type(&aggregate.function, input_type, true)?;
                }
            }
        }
        let mut variables: BTreeMap<_, _> = groups.iter().map(|(name, group)| (name.clone(), group.field.clone())).collect();
        variables.extend(aggregates.iter().map(|aggregate: &AggregateExpr| (aggregate.field.name.clone(), aggregate.field.clone())));
        let fields = groups.values().map(|group| group.field.clone()).chain(aggregates.iter().map(|aggregate| aggregate.field.clone())).collect();
        let relation = Relation::new(Schema::new(fields)?, RelationNode::Aggregate { input: bound.relation, groups: groups.into_values().collect(), aggregates })?;
        self.projection(Bound { relation: Arc::new(relation), variables }, &outputs, false, false, parameters)
    }

    fn rewrite_aggregates(&mut self, expr: &Expr, bound: &mut Bound, parameters: &Parameters, aggregates: &mut Vec<AggregateExpr>) -> Result<Expr> {
        Ok(match expr {
            Expr::Call { function, arguments } if aggregate_function(function).is_some() => {
                let (function, distinct) = aggregate_function(function).ok_or_else(|| error("xtql/invalid-expression", "Unknown aggregate"))?;
                if arguments.iter().any(contains_aggregate) { return Err(error("xtql/invalid-expression", "Aggregate functions cannot be nested")); }
                let argument = if function.canonical() == AggregateFunction::CountStar {
                    if !arguments.is_empty() { return Err(error("xtql/invalid-expression", "row-count takes no arguments")); }
                    None
                } else {
                    if arguments.len() != 1 { return Err(error("xtql/invalid-expression", "Aggregate requires one argument")); }
                    Some(self.expr(&arguments[0], bound, parameters)?)
                };
                let argument = match argument {
                    Some(expr) if !matches!(expr, PExpr::Column { .. }) => {
                        let field = self.binder.field(format!("xt$aggregate$input${}", aggregates.len()), None, expr.data_type())?;
                        let mut projections = bound.relation.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() }).collect::<Vec<_>>();
                        projections.push(NamedExpr { field: field.clone(), expr });
                        bound.relation = Arc::new(Arc::clone(&bound.relation).project(projections)?);
                        Some(field.expression())
                    }
                    argument => argument,
                };
                let input_type = argument.as_ref().map(PExpr::data_type).unwrap_or(LogicalType::Nothing);
                let data_type = aggregate_type(&function, input_type, false)?;
                let name = format!("xt$aggregate${}", aggregates.len());
                let field = self.binder.field(&name, None, data_type)?;
                aggregates.push(AggregateExpr { field, function, argument, distinct, filter: None, order_by: Vec::new() });
                Expr::Variable(name)
            }
            Expr::Call { function, arguments } => Expr::Call { function: function.clone(), arguments: arguments.iter().map(|expr| self.rewrite_aggregates(expr, bound, parameters, aggregates)).collect::<Result<_>>()? },
            Expr::RegexReplace { source,recipe } => Expr::RegexReplace { source: Box::new(self.rewrite_aggregates(source,bound,parameters,aggregates)?),recipe: recipe.clone() },
            Expr::Cast { expr, target } => Expr::Cast { expr: Box::new(self.rewrite_aggregates(expr, bound, parameters, aggregates)?), target: target.clone() },
            Expr::Get { object, field } => Expr::Get { object: Box::new(self.rewrite_aggregates(object, bound, parameters, aggregates)?), field: field.clone() },
            Expr::List(values) => Expr::List(values.iter().map(|expr| self.rewrite_aggregates(expr, bound, parameters, aggregates)).collect::<Result<_>>()?),
            Expr::Set(values) => Expr::Set(values.iter().map(|expr| self.rewrite_aggregates(expr, bound, parameters, aggregates)).collect::<Result<_>>()?),
            Expr::Map(values) => Expr::Map(values.iter().map(|(name, expr)| Ok((name.clone(), self.rewrite_aggregates(expr, bound, parameters, aggregates)?))).collect::<Result<_>>()?),
            expr => expr.clone(),
        })
    }
}

fn normal(name: &str) -> String { Identifier::unquoted(name).normalized() }
fn relational_parameter(name: &str) -> String { normal(name.strip_prefix('?').unwrap_or(name)) }
fn binding_variable<'a>(expr: &'a Expr, parameters: &Parameters) -> Option<&'a str> {
    match expr {
        Expr::LogicVariable(name) => Some(name),
        Expr::Variable(name) if parameters.xtql(name).is_none() => Some(name),
        _ => None,
    }
}
fn pass_through(bound: &Bound) -> Vec<NamedExpr> { bound.variables.values().map(|field| NamedExpr { field: field.clone(), expr: field.expression() }).collect() }
fn timestamp_type() -> LogicalType { LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }) }
fn field_type(logical: &LogicalType, name: &str) -> LogicalType {
    rtdb_plan::field_type(logical, name)
}
fn list_element(logical: &LogicalType) -> LogicalType {
    let types: Vec<_> = logical.legs().iter().filter_map(|leg| match leg { LogicalType::List { element, .. } => Some(element.as_ref().clone()), _ => None }).collect();
    if types.is_empty() { LogicalType::Null } else { LogicalType::join_all(&types) }
}
fn and(mut predicates: Vec<PExpr>) -> Option<PExpr> {
    let first = predicates.pop()?;
    Some(predicates.into_iter().fold(first, |left, right| PExpr::binary(BinaryOp::And, left, right)))
}
fn scalar_call(name: &str, args: Vec<PExpr>) -> Result<PExpr> {
    let binary = match name { "+" => Some(BinaryOp::Add), "-" => Some(BinaryOp::Subtract), "*" => Some(BinaryOp::Multiply), "/" => Some(BinaryOp::Divide),
        "=" | "==" => Some(BinaryOp::Eq), "<>" | "not=" => Some(BinaryOp::NotEq), "<" => Some(BinaryOp::Lt), "<=" => Some(BinaryOp::LtEq), ">" => Some(BinaryOp::Gt), ">=" => Some(BinaryOp::GtEq), "and" => Some(BinaryOp::And), "or" => Some(BinaryOp::Or), _ => None };
    if let Some(op) = binary {
        if args.is_empty() {
            return match name { "+" => Ok(PExpr::Literal(Value::I64(0))), "*" => Ok(PExpr::Literal(Value::I64(1))), "and" | "=" | "==" => Ok(PExpr::Literal(Value::Bool(true))), "or" => Ok(PExpr::Literal(Value::Bool(false))), _ => Err(error("xtql/malformed-call", "Function requires arguments")) };
        }
        if args.len() == 1 {
            return Ok(match name { "-" => PExpr::unary(UnaryOp::Negate, args[0].clone()), "/" => PExpr::binary(BinaryOp::Divide, PExpr::Literal(Value::I64(1)), args[0].clone()), "=" | "==" | "<" | "<=" | ">" | ">=" => PExpr::Literal(Value::Bool(true)), _ => args[0].clone() });
        }
        if matches!(op, BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq) {
            return and(args.windows(2).map(|pair| PExpr::binary(op, pair[0].clone(), pair[1].clone())).collect()).ok_or_else(|| error("xtql/malformed-call", "Comparison requires operands"));
        }
        let mut args = args.into_iter();
        let first = args.next().ok_or_else(|| error("xtql/malformed-call", "Function requires an operand"))?;
        return Ok(args.fold(first, |left, right| PExpr::binary(op, left, right)));
    }
    if matches!(name, "not" | "nil?" | "null?" | "true?" | "false?") {
        if args.len() != 1 { return Err(error("xtql/malformed-call", "Unary predicate requires one argument")); }
        let op = match name { "not" => UnaryOp::Not, "nil?" | "null?" => UnaryOp::IsNull, "true?" => UnaryOp::IsTrue, _ => UnaryOp::IsFalse };
        return Ok(PExpr::unary(op, args[0].clone()));
    }
    if matches!(name,"between" | "between-symmetric" | "between_symmetric") {
        let [operand,low,high]: [PExpr;3] = args.try_into().map_err(|_|error("xtql/malformed-call","BETWEEN requires three arguments"))?;
        return Ok(PExpr::between(operand,low,high,name != "between"));
    }
    if name == "coalesce" { return Ok(PExpr::Coalesce(args)); }
    if name == "if" {
        if args.len() != 3 { return Err(error("xtql/malformed-call", "if requires condition, consequent, and alternative")); }
        return Ok(PExpr::Case { branches: vec![(args[0].clone(), args[1].clone())], otherwise: Box::new(args[2].clone()) });
    }
    let name = normal(name);
    let return_type = rtdb_plan::function_type_with_args(&name, &args)?;
    Ok(PExpr::Call { name, args, return_type })
}
fn required_expr(expr: &Expr, parameters: &Parameters, result: &mut BTreeSet<String>) {
    match expr {
        Expr::Variable(name) if parameters.xtql(name).is_none() => { result.insert(normal(name)); }
        Expr::LogicVariable(name) => { result.insert(normal(name)); }
        Expr::Call { arguments, .. } | Expr::List(arguments) | Expr::Set(arguments) => for argument in arguments { required_expr(argument, parameters, result); },
        Expr::Get { object, .. } => required_expr(object, parameters, result),
        Expr::RegexReplace { source,.. } => required_expr(source,parameters,result),
        Expr::Cast { expr, .. } => required_expr(expr, parameters, result),
        Expr::Map(values) => for (_, value) in values { required_expr(value, parameters, result); },
        Expr::Subquery { query, arguments, .. } => required_arguments(query, arguments.as_deref(), parameters, result),
        _ => {}
    }
}
fn required_arguments(query: &Query, arguments: Option<&[Expr]>, parameters: &Parameters, result: &mut BTreeSet<String>) {
    if let Some(arguments) = arguments { for argument in arguments { required_expr(argument, parameters, result); } }
    else if let Query::Function { parameters: names, .. } = query {
        for name in names { if parameters.xtql(name).is_none() { result.insert(normal(name)); } }
    }
}
fn required_clause(clause: &Clause, parameters: &Parameters) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    match clause {
        Clause::Where(predicates) => for predicate in predicates { required_expr(predicate, parameters, &mut result); },
        Clause::With(bindings) => for binding in bindings { required_expr(&binding.expression, parameters, &mut result); },
        Clause::Unnest(binding) => required_expr(&binding.expression, parameters, &mut result),
        Clause::Join { query, arguments, .. } => required_arguments(query, arguments.as_deref(), parameters, &mut result),
        Clause::Source(_) => {}
    }
    result
}
fn aggregate_function(name: &str) -> Option<(AggregateFunction, bool)> {
    let name = rtdb_plan::AggregateName::new(name).ok()?;
    let distinct = name.distinct();
    Some((AggregateFunction::Named(name), distinct))
}

fn aggregate_type(function: &AggregateFunction, input: LogicalType, grouped: bool) -> Result<LogicalType> {
    let promoted = || LogicalType::least_upper_bound(std::slice::from_ref(&input));
    Ok(match function.canonical() {
        AggregateFunction::Alias(_) | AggregateFunction::Named(_) => unreachable!(),
        AggregateFunction::Count | AggregateFunction::CountStar => LogicalType::Scalar(ScalarType::I64),
        AggregateFunction::Average => match promoted() {
            Some(data_type @ LogicalType::Scalar(ScalarType::Duration(_))) => data_type.maybe(),
            _ => LogicalType::Scalar(ScalarType::F64).maybe(),
        },
        AggregateFunction::VariancePopulation | AggregateFunction::VarianceSample | AggregateFunction::StddevPopulation | AggregateFunction::StddevSample => LogicalType::Scalar(ScalarType::F64).maybe(),
        AggregateFunction::Every | AggregateFunction::Any => LogicalType::Scalar(ScalarType::Bool).maybe(),
        AggregateFunction::ArrayAgg if !grouped => LogicalType::list(input).maybe(),
        AggregateFunction::ArrayAgg | AggregateFunction::VecAgg => LogicalType::list(input),
        AggregateFunction::Percentile { kind: rtdb_plan::PercentileKind::Continuous, .. } => LogicalType::Scalar(ScalarType::F64).maybe(),
        AggregateFunction::Percentile { kind: rtdb_plan::PercentileKind::Discrete, .. } => input.maybe(),
        AggregateFunction::Sum => promoted().ok_or_else(|| error("xtql/invalid-expression", "Cannot compute SUM over the input type"))?.maybe(),
        AggregateFunction::Min | AggregateFunction::Max => input.maybe(),
    })
}

fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Call { function, arguments } => aggregate_function(function).is_some() || arguments.iter().any(contains_aggregate),
        Expr::Get { object, .. } => contains_aggregate(object),
        Expr::RegexReplace { source,.. } => contains_aggregate(source),
        Expr::Cast { expr, .. } => contains_aggregate(expr),
        Expr::List(values) | Expr::Set(values) => values.iter().any(contains_aggregate),
        Expr::Map(values) => values.iter().any(|(_, value)| contains_aggregate(value)),
        _ => false,
    }
}

fn contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery { .. } => true,
        Expr::Call { arguments, .. } | Expr::List(arguments) | Expr::Set(arguments) => arguments.iter().any(contains_subquery),
        Expr::Get { object, .. } | Expr::RegexReplace { source: object, .. } | Expr::Cast { expr: object, .. } => contains_subquery(object),
        Expr::Map(fields) => fields.iter().any(|(_, value)| contains_subquery(value)),
        Expr::Literal(_) | Expr::Variable(_) | Expr::LogicVariable(_) => false,
    }
}

#[cfg(test)]
mod owner_tests {
    use super::*;
    struct EmptyCatalog;
    impl Catalog for EmptyCatalog {
        fn default_database(&self) -> &str { "xtdb" }
        fn databases(&self) -> BTreeSet<String> { BTreeSet::new() }
        fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
        fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
    }
    #[test]
    fn native_compile_retains_raw_metadata_after_source_and_creator_locals_drop() {
        let memory = MemoryBudget::new(1 << 20);
        let source = r#"(-> (rel [{}] []) (return {:v (regexp-replace "abc" "a" "x")}))"#;
        let prepared = compile_in(source, &EmptyCatalog, &CompileOptions::default(), &memory).unwrap();
        let legacy = compile(source, &EmptyCatalog, &CompileOptions::default()).unwrap();
        assert_eq!(serde_json::to_string(&prepared).unwrap(), serde_json::to_string(&legacy).unwrap());
        let retained = memory.used_bytes(); assert!(retained > 0);
        let clone = prepared.clone(); drop(prepared);
        assert_eq!(memory.used_bytes(), retained);
        drop(clone);
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn foreign_ast_import_uses_the_destination_budget_and_does_not_reuse_foreign_raw_leases() {
        let form = read(r#"(-> (rel [{}] []) (return {:v (regexp-replace "abc" "a" "x")}))"#).unwrap();
        let query = crate::parse_query(&form).unwrap();
        let denied = MemoryBudget::new(0);
        let error = compile_query_in(&query, &EmptyCatalog, &mut Binder::default(), &mut 0, &[], "UTC", &denied).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(denied.used_bytes(), 0);
        let memory = MemoryBudget::new(1 << 20);
        let relation = compile_query_in(&query, &EmptyCatalog, &mut Binder::default(), &mut 0, &[], "UTC", &memory).unwrap();
        drop(query); drop(form);
        let retained = memory.used_bytes(); assert!(retained > 0);
        let clone = Arc::clone(&relation); drop(relation);
        assert_eq!(memory.used_bytes(), retained);
        drop(clone);
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn native_expression_parser_admits_current_domain_without_interpreting_raw_host_forms() {
        let memory = MemoryBudget::new(1 << 20);
        let form = read(r#"(regexp-replace nil #opaque[unresolved \😀] nil)"#).unwrap();
        let expression = crate::ast::parse_expr_in(&form, &memory).unwrap();
        let Expr::RegexReplace { recipe, .. } = &expression else { panic!() };
        assert_eq!(serde_json::to_string(&recipe.pattern).unwrap(), r#"{"Tagged":{"tag":"opaque","value":{"Vector":[{"Symbol":"unresolved"},{"Character":"😀"}]}}}"#);
        assert!(memory.used_bytes() > 0);
        drop(expression);
        assert_eq!(memory.used_bytes(), 0);
    }
}
