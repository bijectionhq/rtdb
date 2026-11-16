use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rtdb_plan::*;
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, TableRef, TimeUnit, Value};
use sqlparser::ast as ast;

use crate::parse::{ParsedStatement, Syntax, TemporalClause, TemporalExtent};
use crate::PrepareOptions;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HiddenColumn { ExplicitOnly, Internal }

pub(crate) struct Context<'a> {
    pub source: &'a str,
    pub catalog: &'a dyn Catalog,
    pub options: &'a PrepareOptions,
    pub binder: Binder,
    temporal: Vec<TemporalClause>,
    inline_xtql: Vec<crate::inline_xtql::InlineXtql>,
    ctes: BTreeMap<String, Arc<Relation>>,
    pub(crate) hidden_columns: BTreeMap<ColumnId, HiddenColumn>,
    pub(crate) outer_scopes: Vec<crate::subquery::CorrelationScope>,
    pub(crate) next_parameter: usize,
}

pub(crate) fn control_expression(source: &str, expression: &ast::Expr, options: &PrepareOptions) -> Result<Expr> {
    struct ScalarCatalog;
    impl Catalog for ScalarCatalog {
        fn default_database(&self) -> &str { "xtdb" }
        fn databases(&self) -> BTreeSet<String> { BTreeSet::new() }
        fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Err(error("Control expression cannot read a table")) }
        fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Err(error("Control expression cannot read a table")) }
    }
    let mut context = Context { source, catalog: &ScalarCatalog, options, binder: Binder::default(), temporal: Vec::new(), inline_xtql: Vec::new(), ctes: BTreeMap::new(), hidden_columns: BTreeMap::new(), outer_scopes: Vec::new(), next_parameter: 0 };
    context.expr(expression, &Schema::default(), &BTreeMap::new())
}

pub(crate) fn bind(parsed: ParsedStatement, source: &str, catalog: &dyn Catalog, options: &PrepareOptions) -> Result<PreparedStatement> {
    let scoped_catalog = TransactionCatalog(catalog);
    let catalog: &dyn Catalog = if matches!(&parsed.statement, Syntax::Records { .. } | Syntax::CreateTable { .. } | Syntax::GrantRole { .. } | Syntax::Erase(_) | Syntax::Assert(_, _) | Syntax::Standard(ast::Statement::Insert(_) | ast::Statement::Update(_) | ast::Statement::Delete(_))) { &scoped_catalog } else { catalog };
    let next_parameter = parsed.parameter_count();
    let mut context = Context { source, catalog, options, binder: Binder::default(), temporal: parsed.temporal, inline_xtql: parsed.inline_xtql, ctes: BTreeMap::new(), hidden_columns: BTreeMap::new(), outer_scopes: Vec::new(), next_parameter };
    let statement = context.statement(&parsed.statement)?;
    Ok(PreparedStatement { statement, parameter_types: context.binder.parameter_types().clone(), source: source.to_owned() })
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
    fn statement(&mut self, statement: &Syntax) -> Result<Statement> {
        match statement {
            Syntax::Standard(statement) => self.standard(statement),
            Syntax::Erase(delete) => self.delete(delete, true),
            Syntax::CreateTable { table, columns } => {
                let columns: Vec<_> = columns.iter().map(Identifier::normalized).collect();
                if columns.iter().collect::<BTreeSet<_>>().len() != columns.len() { return Err(error("CREATE TABLE repeats a column")); }
                if columns.iter().any(|name| name.starts_with('_') && name != "_id") { return Err(error("CREATE TABLE cannot declare internal columns")); }
                Ok(Statement::Dml(Dml::CreateTable { table: self.target(table)?, columns }))
            }
            Syntax::GrantRole { user, role, revoke } => Ok(Statement::Dml(Dml::GrantRole { user: user.normalized(), role: role.normalized(), revoke: *revoke })),
            Syntax::Records { patch, table, records } => {
                let expressions = records.iter().map(|expr| self.expr(expr, &Schema::default(), &BTreeMap::new())).collect::<Result<Vec<_>>>()?;
                let mut columns = BTreeMap::<String, LogicalType>::new();
                for expression in &expressions {
                    for leg in expression.data_type().legs() {
                        let LogicalType::Struct(fields) = leg else { return Err(error("RECORDS requires document values")); };
                        for (name, ty) in fields { columns.entry(name).and_modify(|old| *old = old.join(&ty)).or_insert(ty); }
                    }
                }
                if !columns.contains_key("_id") { return Err(error("RECORDS requires an _id column")); }
                if columns.keys().any(|name| name.starts_with('_') && !(if *patch { ["_id", "_fn"].contains(&name.as_str()) } else { ["_id", "_fn", "_valid_from", "_valid_to"].contains(&name.as_str()) })) { return Err(error("RECORDS cannot write internal columns")); }
                let fields = columns.iter().map(|(name, ty)| self.binder.field(name.clone(), None, ty.clone().maybe())).collect::<Result<Vec<_>>>()?;
                let rows = expressions.into_iter().map(|expr| fields.iter().map(|field| Expr::Field { expr: Box::new(expr.clone()), name: field.name.clone(), data_type: field.data_type.clone() }).collect()).collect();
                let input = Arc::new(Relation::new(Schema::new(fields)?, RelationNode::Values { rows })?);
                let target = self.target(table)?;
                let (valid_time, system_time, _) = self.temporal_filters(table)?;
                if system_time != TemporalFilter::Default || (!*patch && valid_time != TemporalFilter::Default) { return Err(error("Invalid temporal clause for RECORDS")); }
                Ok(Statement::Dml(if *patch { Dml::Patch { table: target, input, valid_time } } else { Dml::Insert { table: target, input } }))
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

    fn standard(&mut self, statement: &ast::Statement) -> Result<Statement> {
        Ok(match statement {
            ast::Statement::Query(query) => Statement::Query(self.query(query)?),
            ast::Statement::Insert(insert) => {
                if insert.returning.is_some() || insert.on.is_some() || insert.overwrite || insert.ignore || insert.replace_into || insert.or.is_some() || !insert.assignments.is_empty() || insert.output.is_some() { return Err(unsupported("INSERT modifiers and RETURNING")); }
                let ast::TableObject::TableName(name) = &insert.table else { return Err(unsupported("Dynamic INSERT target")); };
                let target = self.target(name)?;
                let source = insert.source.as_ref().ok_or_else(|| unsupported("INSERT DEFAULT VALUES"))?;
                let mut input = self.query(source)?;
                if !insert.columns.is_empty() {
                    if insert.columns.len() != input.schema().len() { return Err(error("INSERT column count differs from source")); }
                    let names = insert.columns.iter().map(|name| { let names = identifiers(name)?; if names.len() != 1 { return Err(error("INSERT columns must be unqualified")); } Ok(names[0].normalized()) }).collect::<Result<Vec<_>>>()?;
                    if names.iter().collect::<BTreeSet<_>>().len() != names.len() { return Err(error("INSERT repeats a column")); }
                    input = self.rename(input, None, Some(names))?;
                }
                if !input.schema().fields().iter().any(|field| field.name == "_id") { return Err(error("INSERT requires an _id column")); }
                if input.schema().fields().iter().any(|field| field.name.starts_with('_') && !["_id", "_fn", "_valid_from", "_valid_to"].contains(&field.name.as_str())) { return Err(error("INSERT cannot write internal identity or system-time columns")); }
                Statement::Dml(Dml::Insert { table: target, input })
            }
            ast::Statement::Update(update) => {
                if update.from.is_some() || update.returning.is_some() || update.output.is_some() || !update.order_by.is_empty() || update.limit.is_some() || !update.table.joins.is_empty() { return Err(unsupported("UPDATE FROM, joined targets, ordering, or RETURNING")); }
                let mut input = self.mutation_table(&update.table.relation, false)?;
                let (target, valid_time) = scan_target(&input)?;
                let target_columns = input.schema().ids();
                if let Some(predicate) = &update.selection { let predicate = self.relational_expr(predicate, &mut input, &BTreeMap::new())?; input = Arc::new(input.filter(predicate)?); }
                let assignments = update.assignments.iter().map(|assignment| {
                    let ast::AssignmentTarget::ColumnName(name) = &assignment.target else { return Err(unsupported("Tuple assignments")); };
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
                if !statements.is_empty() { return Err(unsupported("SQL procedural transaction blocks")); }
                let mut access_mode = None;
                for mode in modes {
                    match mode {
                        ast::TransactionMode::AccessMode(mode) => access_mode = Some(match mode { ast::TransactionAccessMode::ReadOnly => AccessMode::ReadOnly, ast::TransactionAccessMode::ReadWrite => AccessMode::ReadWrite }),
                        _ => return Err(unsupported("Transaction isolation modifiers")),
                    }
                }
                Statement::Begin { access_mode, system_time: None }
            }
            ast::Statement::Commit { chain, .. } if !chain => Statement::Commit,
            ast::Statement::Rollback { chain, savepoint, .. } if !chain && savepoint.is_none() => Statement::Rollback,
            ast::Statement::Explain { statement, analyze, .. } if !analyze => Statement::Explain { statement: Box::new(self.standard(statement)?) },
            _ => return Err(unsupported(format!("SQL statement: {statement}"))),
        })
    }

    fn delete(&mut self, delete: &ast::Delete, erase: bool) -> Result<Statement> {
        if delete.using.is_some() || delete.returning.is_some() || delete.output.is_some() || !delete.order_by.is_empty() || delete.limit.is_some() || !delete.tables.is_empty() { return Err(unsupported("DELETE USING, ordering, or RETURNING")); }
        let tables = match &delete.from { ast::FromTable::WithFromKeyword(tables) | ast::FromTable::WithoutKeyword(tables) => tables };
        if tables.len() != 1 || !tables[0].joins.is_empty() { return Err(error("DELETE and ERASE require one target table")); }
        let mut input = self.mutation_table(&tables[0].relation, erase)?;
        let RelationNode::Scan(scan) = input.node() else { return Err(error("Mutation target must be a base table")); };
        let (target, valid_time) = (scan.table.name.clone(), scan.valid_time.clone());
        if let Some(predicate) = &delete.selection { let predicate = self.relational_expr(predicate, &mut input, &BTreeMap::new())?; input = Arc::new(input.filter(predicate)?); }
        Ok(Statement::Dml(if erase { Dml::Erase { table: target, input } } else { Dml::Delete { table: target, input, valid_time } }))
    }

    pub(crate) fn query(&mut self, query: &ast::Query) -> Result<Arc<Relation>> {
        if !query.locks.is_empty() || query.for_clause.is_some() || !query.pipe_operators.is_empty() || query.settings.is_some() { return Err(unsupported("Query locking, output clauses, pipes, or settings")); }
        let old_ctes = self.ctes.clone();
        if let Some(with) = &query.with {
            if with.recursive { return Err(unsupported("Recursive CTEs")); }
            for cte in &with.cte_tables {
                let input = self.query(&cte.query)?;
                let input = self.alias(input, &cte.alias)?;
                self.ctes.insert(identifier(&cte.alias.name).normalized(), input);
            }
        }
        let integrated_order = matches!(query.body.as_ref(), ast::SetExpr::Select(_));
        let mut input = match query.body.as_ref() {
            ast::SetExpr::Select(select) => self.select(select, query.order_by.as_ref())?,
            body => self.set_expr(body)?,
        };
        if let Some(order) = query.order_by.as_ref().filter(|_| !integrated_order) {
            let ast::OrderByKind::Expressions(expressions) = &order.kind else { return Err(unsupported("ORDER BY ALL")); };
            let fields = input.schema().fields().to_vec();
            let order = expressions.iter().map(|order| {
                if let Some(index) = ordinal(&order.expr)? {
                    let expr = fields.get(index).ok_or_else(|| error("ORDER BY position is out of range"))?.expression();
                    let descending = order.options.asc == Some(false);
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
                    if !limit_by.is_empty() { return Err(unsupported("LIMIT BY")); }
                    fetch = limit.as_ref().map(|expr| self.expr(expr, &Schema::default(), &BTreeMap::new())).transpose()?;
                    offset = value.as_ref().map(|offset| self.expr(&offset.value, &Schema::default(), &BTreeMap::new())).transpose()?;
                }
                ast::LimitClause::OffsetCommaLimit { .. } => return Err(unsupported("Comma LIMIT syntax")),
            }
        }
        if let Some(value) = &query.fetch {
            if value.percent || value.with_ties { return Err(unsupported("FETCH PERCENT or WITH TIES")); }
            fetch = Some(value.quantity.as_ref().map(|expr| self.expr(expr, &Schema::default(), &BTreeMap::new())).transpose()?.unwrap_or(Expr::Literal(Value::I64(1))));
        }
        if offset.is_some() || fetch.is_some() { input = Arc::new(input.limit(offset, fetch)?); }
        self.ctes = old_ctes;
        Ok(input)
    }

    fn values(&mut self, rows: &[ast::Parens<Vec<ast::Expr>>]) -> Result<Arc<Relation>> {
        let mut bound = Vec::new();
        for row in rows {
            let mut input = Arc::new(Relation::unit());
            let expressions = row.iter().map(|expr| self.relational_expr(expr, &mut input, &BTreeMap::new())).collect::<Result<Vec<_>>>()?;
            bound.push((input, expressions));
        }
        let width = bound.first().map_or(0, |(_, row)| row.len());
        if bound.iter().any(|(_, row)| row.len() != width) { return Err(error("VALUES rows have different column counts")); }
        let fields = (0..width).map(|index| {
            let types: Vec<_> = bound.iter().map(|(_, row)| row[index].data_type()).collect();
            self.binder.field(format!("column{}", index + 1), None, LogicalType::join_all(&types))
        }).collect::<Result<Vec<_>>>()?;
        let schema = Schema::new(fields.clone())?;
        if bound.iter().all(|(input, _)| input.schema().is_empty() && matches!(input.node(), RelationNode::Values { rows } if rows == &vec![vec![]])) {
            return Ok(Arc::new(Relation::new(schema, RelationNode::Values { rows: bound.into_iter().map(|(_, row)| row).collect() })?));
        }
        let mut result = None;
        for (input, row) in bound {
            let projected = Arc::new(input.project(fields.iter().cloned().zip(row).map(|(field, expr)| NamedExpr { field, expr }).collect())?);
            result = Some(match result {
                None => projected,
                Some(left) => Arc::new(Relation::new(schema.clone(), RelationNode::Set { left, right: projected, operator: SetOperator::Union, all: true })?),
            });
        }
        Ok(result.unwrap_or_else(|| Arc::new(Relation::empty(schema))))
    }

    fn set_expr(&mut self, set: &ast::SetExpr) -> Result<Arc<Relation>> {
        match set {
            ast::SetExpr::Select(select) => self.select(select, None),
            ast::SetExpr::Query(query) => self.query(query),
            ast::SetExpr::Values(values) => self.values(&values.rows),
            ast::SetExpr::SetOperation { left, op, set_quantifier, right } => {
                let left = self.set_expr(left)?;
                let right = self.set_expr(right)?;
                if left.schema().len() != right.schema().len() { return Err(error("Set inputs have different column counts")); }
                let fields = left.schema().fields().iter().zip(right.schema().fields()).map(|(left, right)| self.binder.field(left.name.clone(), None, left.data_type.join(&right.data_type))).collect::<Result<_>>()?;
                let operator = match op { ast::SetOperator::Union => SetOperator::Union, ast::SetOperator::Intersect => SetOperator::Intersect, ast::SetOperator::Except => SetOperator::Except, _ => return Err(unsupported("Set operator")) };
                let all = match set_quantifier { ast::SetQuantifier::All => true, ast::SetQuantifier::None | ast::SetQuantifier::Distinct => false, _ => return Err(unsupported("Set correspondence modifiers")) };
                Ok(Arc::new(Relation::new(Schema::new(fields)?, RelationNode::Set { left, right, operator, all })?))
            }
            _ => Err(unsupported("Query body")),
        }
    }

    fn select(&mut self, select: &ast::Select, order_by: Option<&ast::OrderBy>) -> Result<Arc<Relation>> {
        if select.top.is_some() || select.into.is_some() || select.qualify.is_some() || !select.lateral_views.is_empty() || select.prewhere.is_some() || !select.cluster_by.is_empty() || !select.distribute_by.is_empty() || !select.sort_by.is_empty() || !select.connect_by.is_empty() { return Err(unsupported("SELECT extension")); }
        let mut input = Arc::new(Relation::unit());
        for table in &select.from {
            let right = self.joined_table(table)?;
            input = if input.schema().is_empty() && matches!(input.node(), RelationNode::Values { rows } if rows == &vec![vec![]]) { right } else { join(input, right, JoinKind::Cross, None)? };
        }
        if let Some(predicate) = &select.selection { let predicate = self.relational_expr(predicate, &mut input, &BTreeMap::new())?; input = Arc::new(input.filter(predicate)?); }
        let groups = match &select.group_by { ast::GroupByExpr::Expressions(expressions, modifiers) if modifiers.is_empty() => expressions, _ => return Err(unsupported("GROUP BY ALL or grouping sets")) };
        let mut aggregate_calls = BTreeMap::new();
        for item in &select.projection {
            if let ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } = item { crate::expression::aggregate_calls(expr, &mut aggregate_calls)?; }
        }
        if let Some(having) = &select.having { crate::expression::aggregate_calls(having, &mut aggregate_calls)?; }
        let mut replacements = BTreeMap::new();
        if !groups.is_empty() || !aggregate_calls.is_empty() {
            let mut named_groups = Vec::new();
            for (index, group) in groups.iter().enumerate() {
                let group = ordinal_projection(group, &select.projection)?.unwrap_or(group);
                let expr = self.relational_expr(group, &mut input, &BTreeMap::new())?;
                let qualifier = match &expr { Expr::Column { id, .. } => input.schema().get(*id).and_then(|field| field.qualifier.clone()), _ => None };
                let field = self.binder.field(expression_name(group, index), qualifier, expr.data_type())?;
                replacements.insert(group.to_string(), field.clone());
                named_groups.push(NamedExpr { field, expr });
            }
            let mut aggregates = Vec::new();
            for (key, function) in aggregate_calls {
                let aggregate = self.aggregate(&function, &mut input, aggregates.len())?;
                replacements.insert(key, aggregate.field.clone());
                aggregates.push(aggregate);
            }
            let schema = Schema::new(named_groups.iter().map(|group| group.field.clone()).chain(aggregates.iter().map(|agg| agg.field.clone())).collect())?;
            input = Arc::new(Relation::new(schema, RelationNode::Aggregate { input, groups: named_groups, aggregates })?);
        }
        if let Some(having) = &select.having { let predicate = self.relational_expr(having, &mut input, &replacements)?; input = Arc::new(input.filter(predicate)?); }
        let mut windows = Vec::new();
        for item in &select.projection {
            if let ast::SelectItem::UnnamedExpr(expression) | ast::SelectItem::ExprWithAlias { expr: expression, .. } = item { crate::expression::window_calls(expression, &mut windows); }
        }
        if windows.len() > 1 { return Err(unsupported("Only one window function per SELECT is supported")); }
        if let Some((key, function)) = windows.pop() {
            let (window, field) = self.window(input, &function, &replacements)?;
            input = window;
            replacements.insert(key, field);
        }
        let mut projected = Vec::new();
        for (index, item) in select.projection.iter().filter(|item| !matches!(item, ast::SelectItem::Wildcard(_))).enumerate() {
            match item {
                ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } => {
                    let expression = self.relational_expr(expr, &mut input, &replacements)?;
                    let name = if let ast::SelectItem::ExprWithAlias { alias, .. } = item { identifier(alias).normalized() } else { expression_name(expr, index) };
                    let field = self.binder.field(name, None, expression.data_type())?;
                    projected.push(NamedExpr { field, expr: expression });
                }
                ast::SelectItem::QualifiedWildcard(ast::SelectItemQualifiedWildcardKind::ObjectName(name), options) => {
                    if !wildcard_plain(options) { return Err(unsupported("Wildcard modifiers")); }
                    let qualifier = identifiers(name)?.last().map(Identifier::normalized).ok_or_else(|| error("Empty wildcard qualifier"))?;
                    let matching: Vec<_> = input.schema().fields().iter().filter(|field| field.qualifier.as_ref() == Some(&qualifier)).collect();
                    if matching.is_empty() { return Err(error(format!("Unknown wildcard qualifier: {qualifier}"))); }
                    for field in matching.into_iter().filter(|field| !self.hidden_columns.contains_key(&field.id)) { projected.push(NamedExpr { field: self.binder.field(field.name.clone(), None, field.data_type.clone())?, expr: field.expression() }); }
                }
                _ => return Err(unsupported("Projection shape")),
            }
        }
        let wildcards = select.projection.iter().enumerate().filter_map(|(index, item)| match item { ast::SelectItem::Wildcard(options) => Some((index, options)), _ => None }).collect::<Vec<_>>();
        if let Some((index, options)) = wildcards.first() {
            if wildcards.len() != 1 || (*index != 0 && *index + 1 != select.projection.len()) { return Err(error("Unqualified wildcard must occur once, at the beginning or end of the projection")); }
            if !wildcard_plain(options) { return Err(unsupported("Wildcard modifiers")); }
            let explicit_names = projected.iter().map(|projection| projection.field.name.clone()).collect::<BTreeSet<_>>();
            let mut expanded = Vec::new();
            for field in input.schema().fields().iter().filter(|field| !self.hidden_columns.contains_key(&field.id) && !explicit_names.contains(&field.name)) {
                expanded.push(NamedExpr { field: self.binder.field(field.name.clone(), None, field.data_type.clone())?, expr: field.expression() });
            }
            expanded.extend(projected);
            projected = expanded;
        }
        let mut names = BTreeSet::new();
        for projection in &projected {
            if !names.insert(projection.field.name.clone()) { return Err(error(format!("Duplicate column projection: {}", projection.field.name))); }
        }
        if let Some(order_by) = order_by {
            let ast::OrderByKind::Expressions(expressions) = &order_by.kind else { return Err(unsupported("ORDER BY ALL")); };
            let fields = projected.iter().map(|projection| projection.field.clone()).collect::<Vec<_>>();
            let mut retained = input.schema().fields().iter().map(|field| {
                let mut output = field.clone();
                if names.contains(&field.name) { output.qualified_only = true; }
                NamedExpr { field: output, expr: field.expression() }
            }).collect::<Vec<_>>();
            retained.extend(projected);
            input = Arc::new(input.project(retained)?);
            let mut order_replacements = replacements.clone();
            for group in groups {
                let group = ordinal_projection(group, &select.projection)?.unwrap_or(group);
                if crate::expression::references_unqualified_names(group, &names) { order_replacements.remove(&group.to_string()); }
            }
            let mut order = Vec::new();
            for expression in expressions {
                let mut aggregates = BTreeMap::new();
                crate::expression::aggregate_calls(&expression.expr, &mut aggregates)?;
                if !aggregates.is_empty() { return Err(error("Aggregate functions are not allowed directly in ORDER BY")); }
                let expr = if let Some(index) = ordinal(&expression.expr)? {
                    fields.get(index).ok_or_else(|| error("ORDER BY position is out of range"))?.expression()
                } else { self.relational_expr(&expression.expr, &mut input, &order_replacements)? };
                let descending = expression.options.asc == Some(false);
                order.push(SortExpr { expr, descending, nulls_first: expression.options.nulls_first.unwrap_or(descending) });
            }
            input = Arc::new(input.sort(order)?);
            input = Arc::new(input.project(fields.into_iter().map(|field| NamedExpr { expr: field.expression(), field }).collect())?);
        } else { input = Arc::new(input.project(projected)?); }
        if let Some(distinct) = &select.distinct {
            if !matches!(distinct, ast::Distinct::Distinct) { return Err(unsupported("DISTINCT ON")); }
            input = Arc::new(Relation::new(input.schema().clone(), RelationNode::Distinct { input })?);
        }
        Ok(input)
    }

    fn joined_table(&mut self, table: &ast::TableWithJoins) -> Result<Arc<Relation>> {
        let mut left = self.table(&table.relation)?;
        for joined in &table.joins {
            let right = self.table(&joined.relation)?;
            let (kind, constraint) = match &joined.join_operator {
                ast::JoinOperator::Inner(constraint) | ast::JoinOperator::Join(constraint) => (JoinKind::Inner, Some(constraint)),
                ast::JoinOperator::LeftOuter(constraint) | ast::JoinOperator::Left(constraint) => (JoinKind::Left, Some(constraint)),
                ast::JoinOperator::RightOuter(constraint) | ast::JoinOperator::Right(constraint) => (JoinKind::Right, Some(constraint)),
                ast::JoinOperator::FullOuter(constraint) => (JoinKind::Full, Some(constraint)),
                ast::JoinOperator::CrossJoin(_) => (JoinKind::Cross, None),
                _ => return Err(unsupported("Join operator")),
            };
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
                    Ok(Expr::binary(BinaryOp::Eq, left.schema().resolve(&names)?.expression(), right.schema().resolve(&names)?.expression()))
                }).collect::<Result<Vec<_>>>()?;
                let predicate = predicates.into_iter().reduce(|left, right| Expr::binary(BinaryOp::And, left, right)).unwrap_or(Expr::Literal(Value::Bool(true)));
                let right_ids = right.schema().fields().iter().filter(|field| common.contains(&field.name)).map(|field| field.id).collect::<BTreeSet<_>>();
                let joined = join(left, right, kind, Some(predicate))?;
                let projections = joined.schema().fields().iter().map(|field| {
                    let mut output = field.clone();
                    if right_ids.contains(&field.id) { output.qualified_only = true; }
                    NamedExpr { field: output, expr: field.expression() }
                }).collect();
                left = Arc::new(joined.project(projections)?);
                continue;
            }
            if let Some(ast::JoinConstraint::On(predicate)) = constraint {
                if crate::expression::contains_subquery(predicate) {
                    if !matches!(kind, JoinKind::Inner | JoinKind::Left | JoinKind::Right) { return Err(unsupported("Subqueries in FULL OUTER JOIN conditions")); }
                    crate::expression::reject_ambiguous_columns(predicate, &schema)?;
                    let original_order = schema.fields().iter().map(|field| field.id).collect::<Vec<_>>();
                    let (outer, mut dependent) = if kind == JoinKind::Right { (right, left) } else { (left, right) };
                    let fields = dependent.schema().fields().to_vec();
                    self.outer_scopes.push(crate::subquery::CorrelationScope::new(outer.schema().clone()));
                    let predicate = self.relational_expr(predicate, &mut dependent, &BTreeMap::new());
                    let arguments = self.outer_scopes.pop().ok_or_else(|| error("Join correlation scope is unavailable"))?.arguments();
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
            left = join(left, right, kind, condition)?;
        }
        Ok(left)
    }

    fn table(&mut self, table: &ast::TableFactor) -> Result<Arc<Relation>> {
        match table {
            ast::TableFactor::Table { name, alias, args, version, with_hints, partitions, sample, .. } => {
                if args.is_some() || version.is_some() || !with_hints.is_empty() || !partitions.is_empty() || sample.is_some() { return Err(unsupported("Table function, sample, or dialect-specific version")); }
                if let Some(inline) = self.inline_xtql.iter().find(|inline| inline.matches(name)).cloned() {
                    let arguments = inline.parameters.iter().map(|index| (*index,self.options.parameter_types.get(index).cloned().unwrap_or(LogicalType::Nothing))).collect::<Vec<_>>();
                    let relation = rtdb_xtql::compile_query(&inline.query,self.catalog,&mut self.binder,&mut self.next_parameter,&arguments).map_err(|error| inline.error(error,self.source))?;
                    return match alias { Some(alias) => self.alias(relation,alias),None => Ok(relation) };
                }
                let names = identifiers(name)?;
                if names.len() == 1 {
                    if let Some(cte) = self.ctes.get(&names[0].normalized()).cloned() {
                        let end = name.0.last().and_then(ast::ObjectNamePart::as_ident).map(|name| name.span.end);
                        if self.temporal.iter().any(|clause| Some(clause.owner_end) == end) { return Err(error("Temporal clauses are not allowed on CTE references")); }
                        return match alias { Some(alias) => self.alias(cte, alias), None => self.rename(cte, Some(names[0].normalized()), None) };
                    }
                }
                let binding = resolve_table(self.catalog, &names, &self.options.search_path)?;
                let qualifier = alias.as_ref().map(|alias| identifier(&alias.name).normalized()).unwrap_or_else(|| binding.name.table.table.clone());
                let mut columns = binding.columns.iter().map(|(name, ty)| self.binder.field(name.clone(), Some(qualifier.clone()), ty.clone())).collect::<Result<Vec<_>>>()?;
                if !matches!(binding.name.table.schema.as_str(), "information_schema" | "pg_catalog" | "xt") || binding.name.table == TableRef::new("xt", "txs") {
                    for (name, ty) in [("_iid", LogicalType::Scalar(ScalarType::FixedSizeBinary(16))), ("_valid_from", crate::mutation::instant_type()), ("_system_from", crate::mutation::instant_type()), ("_valid_to", crate::mutation::instant_type().maybe()), ("_system_to", crate::mutation::instant_type().maybe())] {
                        if columns.iter().any(|field| field.name == name) { continue; }
                        let field = self.binder.field(name, Some(qualifier.clone()), ty)?;
                        self.hidden_columns.insert(field.id,HiddenColumn::ExplicitOnly);
                        columns.push(field);
                    }
                }
                let (valid_time, system_time, clamp_valid_time) = self.temporal_filters(name)?;
                let relation = Arc::new(Relation::scan(Scan { table: binding, columns, valid_time, system_time, clamp_valid_time, iids: None })?);
                if alias.as_ref().is_some_and(|alias| !alias.columns.is_empty()) { self.alias(relation, alias.as_ref().ok_or_else(|| error("Missing alias"))?) } else { Ok(relation) }
            }
            ast::TableFactor::Derived { subquery, alias, lateral, .. } => {
                if *lateral { return Err(unsupported("LATERAL subqueries")); }
                let relation = self.query(subquery)?;
                match alias { Some(alias) => self.alias(relation, alias), None => Ok(relation) }
            }
            ast::TableFactor::NestedJoin { table_with_joins, alias } => {
                let relation = self.joined_table(table_with_joins)?;
                match alias { Some(alias) => self.alias(relation, alias), None => Ok(relation) }
            }
            ast::TableFactor::UNNEST { array_exprs, alias, with_ordinality, with_offset, .. } => {
                if array_exprs.len() != 1 || *with_offset { return Err(unsupported("Multi-array UNNEST or OFFSET")); }
                let expr = self.expr(&array_exprs[0], &Schema::default(), &BTreeMap::new())?;
                let ty = match expr.data_type().unencoded() { LogicalType::Nothing | LogicalType::Null => LogicalType::Null, LogicalType::List { element, .. } => *element, LogicalType::Maybe(value) => match *value { LogicalType::List { element, .. } => *element, _ => return Err(error("UNNEST requires an array")) }, _ => return Err(error("UNNEST requires an array")) };
                let element = self.binder.field("unnest", None, ty)?;
                let ordinality = if *with_ordinality { Some(self.binder.field("ordinality", None, LogicalType::Scalar(ScalarType::I64))?) } else { None };
                let schema = Schema::new(std::iter::once(element.clone()).chain(ordinality.clone()).collect())?;
                let relation = Arc::new(Relation::new(schema, RelationNode::Unnest { input: Arc::new(Relation::unit()), expr, element, ordinality, outer: false })?);
                match alias { Some(alias) => self.alias(relation, alias), None => Ok(relation) }
            }
            _ => Err(unsupported("Table source")),
        }
    }

    fn mutation_table(&mut self, table: &ast::TableFactor, erase: bool) -> Result<Arc<Relation>> {
        let input = self.table(table)?;
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
            if !scan.columns.iter().any(|field| field.name == name) { scan.columns.push(self.binder.field(name, qualifier.clone(), ty)?); }
        }
        Ok(Arc::new(Relation::scan(scan)?))
    }

    fn temporal_filters(&mut self, table: &ast::ObjectName) -> Result<(TemporalFilter, TemporalFilter, bool)> {
        let end = table.0.last().and_then(ast::ObjectNamePart::as_ident).map(|name| name.span.end);
        let clauses: Vec<_> = self.temporal.iter().filter(|clause| Some(clause.owner_end) == end).cloned().collect();
        let mut valid = TemporalFilter::Default;
        let mut system = TemporalFilter::Default;
        let mut clamp = false;
        for clause in clauses {
            let extent = match clause.extent {
                TemporalExtent::All => TemporalFilter::All,
                TemporalExtent::At(at) => TemporalFilter::At(self.expr(&at, &Schema::default(), &BTreeMap::new())?),
                TemporalExtent::FromTo(from, to) => TemporalFilter::FromTo {
                    from: self.expr(&from, &Schema::default(), &BTreeMap::new())?,
                    to: to.as_ref().map(|to| self.expr(to, &Schema::default(), &BTreeMap::new())).transpose()?.unwrap_or(Expr::Literal(Value::Timestamp { value: i64::MAX, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) })),
                },
                TemporalExtent::Between(from, to) => TemporalFilter::Between { from: self.expr(&from, &Schema::default(), &BTreeMap::new())?, to: self.expr(&to, &Schema::default(), &BTreeMap::new())? },
            };
            if clause.system { system = extent; } else { valid = extent; clamp = clause.clamp; }
        }
        Ok((valid, system, clamp))
    }

    fn target(&self, name: &ast::ObjectName) -> Result<QualifiedTable> {
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
        let visible = input.schema().fields().iter().filter(|field| !self.hidden_columns.contains_key(&field.id)).count();
        if columns.as_ref().is_some_and(|columns| columns.len() != visible) { return Err(error("Alias column count differs from relation")); }
        let mut index = 0;
        let mut expressions = Vec::new();
        for field in input.schema().fields() {
            let hidden = self.hidden_columns.get(&field.id).copied();
            let name = if hidden.is_some() { field.name.clone() } else {
                let name = columns.as_ref().map(|columns| columns[index].clone()).unwrap_or_else(|| field.name.clone());
                index += 1;
                name
            };
            let renamed = self.binder.field(name, qualifier.clone(), field.data_type.clone())?;
            if let Some(hidden) = hidden { self.hidden_columns.insert(renamed.id,hidden); }
            expressions.push(NamedExpr { field: renamed, expr: field.expression() });
        }
        Ok(Arc::new(input.project(expressions)?))
    }

    pub(crate) fn relational_sort_expr(&mut self, order: &ast::OrderByExpr, input: &mut Arc<Relation>, replacements: &BTreeMap<String, Field>) -> Result<SortExpr> {
        let expr = if let Some(index) = ordinal(&order.expr)? { input.schema().fields().get(index).ok_or_else(|| error("ORDER BY position is out of range"))?.expression() } else { self.relational_expr(&order.expr, input, replacements)? };
        let descending = order.options.asc == Some(false);
        Ok(SortExpr { expr, descending, nulls_first: order.options.nulls_first.unwrap_or(descending) })
    }

}

pub(crate) fn identifier(identifier: &ast::Ident) -> Identifier { Identifier { text: identifier.value.clone(), quoted: identifier.quote_style.is_some() } }
pub(crate) fn identifiers(name: &ast::ObjectName) -> Result<Vec<Identifier>> { name.0.iter().map(|part| part.as_ident().map(identifier).ok_or_else(|| unsupported("Computed identifiers"))).collect() }
pub(crate) fn error(message: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("xtdb/sql-error", message.to_string()) }
pub(crate) fn unsupported(message: impl std::fmt::Display) -> Anomaly { Anomaly::unsupported("xtdb/sql-unsupported", message.to_string()) }

fn scan_target(input: &Relation) -> Result<(QualifiedTable, TemporalFilter)> {
    match input.node() {
        RelationNode::Scan(scan) if scan.system_time == TemporalFilter::Default => Ok((scan.table.name.clone(), scan.valid_time.clone())),
        _ => Err(error("DML requires an unmodified table scan without SYSTEM_TIME")),
    }
}

fn join(left: Arc<Relation>, right: Arc<Relation>, kind: JoinKind, condition: Option<Expr>) -> Result<Arc<Relation>> {
    let schema = match kind {
        JoinKind::Left => left.schema().concat(&right.schema().nullable()?)?,
        JoinKind::Right => left.schema().nullable()?.concat(right.schema())?,
        JoinKind::Full => left.schema().nullable()?.concat(&right.schema().nullable()?)?,
        _ => left.schema().concat(right.schema())?,
    };
    Ok(Arc::new(Relation::new(schema, RelationNode::Join { left, right, kind, condition })?))
}

fn wildcard_plain(options: &ast::WildcardAdditionalOptions) -> bool { options.to_string().is_empty() }

pub(crate) fn expression_name(expr: &ast::Expr, index: usize) -> String {
    match expr { ast::Expr::Identifier(name) => identifier(name).normalized(), ast::Expr::CompoundIdentifier(names) => names.last().map(|name| identifier(name).normalized()).unwrap_or_default(), ast::Expr::Function(function) if function.over.is_none() => function.name.to_string().to_lowercase(), _ => format!("_column_{}", index + 1) }
}

fn ordinal(expr: &ast::Expr) -> Result<Option<usize>> {
    if let ast::Expr::Value(value) = expr {
        if let ast::Value::Number(number, _) = &value.value {
            if !number.contains(['.', 'e', 'E']) { return number.parse::<usize>().ok().and_then(|number| number.checked_sub(1)).map(Some).ok_or_else(|| error("SQL column positions start at one")); }
        }
    }
    Ok(None)
}

fn ordinal_projection<'a>(expr: &ast::Expr, projection: &'a [ast::SelectItem]) -> Result<Option<&'a ast::Expr>> {
    match ordinal(expr)? {
        Some(index) => match projection.get(index) { Some(ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. }) => Ok(Some(expr)), _ => Err(error("GROUP BY position is out of range or names a wildcard")) },
        None => Ok(None),
    }
}
