use rtdb_types::Result;
use sqlparser::ast;
use crate::bind::Context;

impl Context<'_> {
    pub(crate) fn admit_statement_literals(&mut self, statement: &crate::parse::Syntax) -> Result<()> {
        use crate::parse::Syntax as S;
        match statement {
            S::Standard(statement) => self.admit_standard_literals(statement)?,
            S::Erase(delete) => self.admit_delete_literals(delete)?,
            S::Records { table, records, .. } => {
                self.admit_table_temporal_literals(table)?;
                for expression in records { self.admit_expression_literals(expression)?; }
            }
            S::Assert(value, _) => self.admit_expression_literals(value)?,
            S::Control(_) | S::CreateTable { .. } | S::CreateSearchIndex { .. } | S::DropSearchIndex { .. } | S::CreateOrderedIndex { .. } | S::DropOrderedIndex { .. } | S::CreateVectorIndex { .. } | S::DropVectorIndex { .. } | S::GrantRole { .. } => (),
        }
        Ok(())
    }

    fn admit_standard_literals(&mut self, statement: &ast::Statement) -> Result<()> {
        match statement {
            ast::Statement::Query(query) => self.admit_query_literals(query)?,
            ast::Statement::Explain { statement, .. } => self.admit_standard_literals(statement)?,
            ast::Statement::Insert(insert) => if let Some(query) = &insert.source { self.admit_query_literals(query)?; },
            ast::Statement::Update(update) => {
                self.admit_joined_table_literals(&update.table)?;
                for assignment in &update.assignments { self.admit_expression_literals(&assignment.value)?; }
                if let Some(predicate) = &update.selection { self.admit_expression_literals(predicate)?; }
            }
            ast::Statement::Delete(delete) => self.admit_delete_literals(delete)?,
            _ => (),
        }
        Ok(())
    }

    fn admit_delete_literals(&mut self, delete: &ast::Delete) -> Result<()> {
        let tables = match &delete.from { ast::FromTable::WithFromKeyword(tables) | ast::FromTable::WithoutKeyword(tables) => tables };
        for table in tables { self.admit_joined_table_literals(table)?; }
        if let Some(predicate) = &delete.selection { self.admit_expression_literals(predicate)?; }
        Ok(())
    }

    pub(crate) fn admit_query_literals(&mut self, query: &ast::Query) -> Result<()> {
        if let Some(with) = &query.with { for cte in &with.cte_tables { self.admit_query_literals(&cte.query)?; } }
        self.admit_set_literals(&query.body)?;
        if let Some(order) = &query.order_by {
            if let ast::OrderByKind::Expressions(expressions) = &order.kind {
                for expression in expressions { self.admit_expression_literals(&expression.expr)?; }
            }
        }
        Ok(())
    }

    fn admit_set_literals(&mut self, set: &ast::SetExpr) -> Result<()> {
        match set {
            ast::SetExpr::Select(select) => {
                for table in &select.from { self.admit_joined_table_literals(table)?; }
                if let Some(predicate) = &select.selection { self.admit_expression_literals(predicate)?; }
                if let Some(having) = &select.having { self.admit_expression_literals(having)?; }
                for item in &select.projection {
                    self.admit_projection_literal(item)?;
                }
                if let ast::GroupByExpr::Expressions(expressions, _) = &select.group_by {
                    for expression in expressions { self.admit_expression_literals(expression)?; }
                }
            }
            ast::SetExpr::FromPipeline(pipeline) => {
                for table in &pipeline.from {
                    self.admit_joined_table_literals(table)?;
                }
                for tail in &pipeline.tails {
                    match tail {
                        ast::FromPipelineTail::Where(predicate) => self.admit_expression_literals(predicate)?,
                        ast::FromPipelineTail::Select(select) => {
                            if let Some(having) = &select.having {
                                self.admit_expression_literals(having)?;
                            }
                            for item in &select.projection {
                                self.admit_projection_literal(item)?;
                            }
                            if let Some(ast::GroupByExpr::Expressions(expressions, _)) = &select.group_by {
                                for expression in expressions {
                                    self.admit_expression_literals(expression)?;
                                }
                            }
                        }
                    }
                }
            }
            ast::SetExpr::Query(query) => self.admit_query_literals(query)?,
            ast::SetExpr::Values(values) => for row in &values.rows { for expression in row.expressions() { self.admit_expression_literals(expression)?; } },
            ast::SetExpr::Records(records) => for expression in records { self.admit_expression_literals(expression)?; },
            ast::SetExpr::SetOperation { left, right, .. } => { self.admit_set_literals(left)?; self.admit_set_literals(right)?; }
            _ => (),
        }
        Ok(())
    }

    fn admit_projection_literal(&mut self, item: &ast::SelectItem) -> Result<()> {
        match item {
            ast::SelectItem::UnnamedExpr(expression) | ast::SelectItem::ExprWithAlias { expr: expression, .. } =>
                self.admit_expression_literals(expression),
            _ => Ok(()),
        }
    }

    fn admit_joined_table_literals(&mut self, table: &ast::TableWithJoins) -> Result<()> {
        self.admit_table_literals(&table.relation)?;
        for join in &table.joins {
            self.admit_table_literals(&join.relation)?;
            use ast::JoinOperator as J;
            let constraint = match &join.join_operator {
                J::Join(constraint) | J::Inner(constraint) | J::Left(constraint) | J::LeftOuter(constraint)
                | J::Right(constraint) | J::RightOuter(constraint) | J::FullOuter(constraint) | J::CrossJoin(constraint) => Some(constraint),
                _ => None,
            };
            if let Some(ast::JoinConstraint::On(expression)) = constraint { self.admit_expression_literals(expression)?; }
        }
        Ok(())
    }

    fn admit_table_literals(&mut self, table: &ast::TableFactor) -> Result<()> {
        match table {
            ast::TableFactor::Table { name, args, .. } => {
                if let Some(args) = args {
                    for argument in &args.args {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expression)) = argument { self.admit_expression_literals(expression)?; }
                    }
                }
                self.admit_table_temporal_literals(name)?;
            }
            ast::TableFactor::Derived { subquery, .. } => self.admit_query_literals(subquery)?,
            ast::TableFactor::ExpressionDerived { expr, .. } => self.admit_expression_literals(expr)?,
            ast::TableFactor::NestedJoin { table_with_joins, .. } => self.admit_joined_table_literals(table_with_joins)?,
            ast::TableFactor::UNNEST { array_exprs, .. } => for expression in array_exprs { self.admit_expression_literals(expression)?; },
            _ => (),
        }
        Ok(())
    }

    pub(crate) fn admit_expression_literals(&mut self, expression: &ast::Expr) -> Result<()> {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || self.admit_expression_literals_at(expression))
    }

    fn admit_expression_literals_at(&mut self, expression: &ast::Expr) -> Result<()> {
        match expression {
            ast::Expr::Interval(interval) => { self.interval_literal(interval)?; }
            ast::Expr::TypedString(value) => {
                use crate::literal_admission::TemporalLiteralKind as Temporal;
                let kind = if value.uses_odbc_syntax { None } else {
                    match &value.data_type {
                        ast::DataType::Date => Some(Temporal::Date),
                        ast::DataType::Time(None, ast::TimezoneInfo::None) => Some(Temporal::Time),
                        ast::DataType::Timestamp(None, ast::TimezoneInfo::None | ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::WithoutTimeZone) => Some(Temporal::Timestamp),
                        _ => None,
                    }
                };
                if let Some(kind) = kind
                    && let rtdb_types::Value::Utf8(text) = crate::types::literal(&value.value.value)?
                {
                    self.literals.temporal(value.value.span, kind, &text);
                }
            }
            ast::Expr::Subquery(query) | ast::Expr::Exists { subquery: query, .. } => self.admit_query_literals(query)?,
            ast::Expr::InSubquery { expr, subquery, .. } => { self.admit_expression_literals(expr)?; self.admit_query_literals(subquery)?; }
            ast::Expr::Tuple(expressions) => for expression in expressions { self.admit_expression_literals(expression)?; },
            ast::Expr::Trim { expr, trim_what, .. } => {
                if let Some(character) = trim_what { self.admit_expression_literals(character)?; }
                self.admit_expression_literals(expr)?;
            }
            ast::Expr::Function(function) => {
                if let ast::FunctionArguments::Subquery(query) = &function.args { self.admit_query_literals(query)?; }
                for child in crate::expression::children(expression) { self.admit_expression_literals(child)?; }
                if function.within_group.is_empty() {
                    if let Some(filter) = &function.filter { self.admit_expression_literals(filter)?; }
                }
                if let ast::FunctionArguments::List(arguments) = &function.args {
                    for clause in &arguments.clauses {
                        if let ast::FunctionArgumentClause::OrderBy(expressions) = clause {
                            for expression in expressions { self.admit_expression_literals(&expression.expr)?; }
                        }
                    }
                }
                for order in &function.within_group { self.admit_expression_literals(&order.expr)?; }
                if !function.within_group.is_empty() {
                    if let Some(filter) = &function.filter { self.admit_expression_literals(filter)?; }
                }
                if let Some(ast::WindowType::WindowSpec(window)) = &function.over {
                    for expression in &window.partition_by { self.admit_expression_literals(expression)?; }
                    for order in &window.order_by { self.admit_expression_literals(&order.expr)?; }
                }
            }
            _ => for child in crate::expression::children(expression) { self.admit_expression_literals(child)?; },
        }
        Ok(())
    }

    pub(crate) fn admit_extent_literals(&mut self, extent: &crate::parse::TemporalExtent) -> Result<()> {
        use crate::parse::TemporalExtent as E;
        match extent {
            E::All => (),
            E::At(expression) => self.admit_expression_literals(expression)?,
            E::FromTo(from, to) => { self.admit_expression_literals(from)?; if let Some(to) = to { self.admit_expression_literals(to)?; } }
            E::Between(from, to) => { self.admit_expression_literals(from)?; self.admit_expression_literals(to)?; }
        }
        Ok(())
    }
}
