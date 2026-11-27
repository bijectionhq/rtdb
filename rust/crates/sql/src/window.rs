use std::{collections::BTreeMap, sync::Arc};
use rtdb_plan::{ColumnId, Expr, Field, NamedExpr, Relation, RelationNode, Schema, SortExpr, WindowExpr, WindowFunction};
use rtdb_types::{Anomaly, Result};
use sqlparser::ast;
use crate::bind::{Context, HiddenColumn, error, grammar_rejected, identifiers, unsupported};
use crate::expression::Replacements;

pub(crate) struct BoundWindow {
    keys: Vec<NamedExpr>,
    partition_by: Vec<ColumnId>,
    order_by: Vec<SortExpr>,
    expression: WindowExpr,
}

impl BoundWindow {
    pub(crate) fn output(&self) -> &Field { &self.expression.field }

    pub(crate) fn output_schema(&self) -> Result<Schema> {
        Schema::new(self.keys.iter().map(|key| key.field.clone())
            .chain([self.expression.field.clone()]).collect())
    }

    pub(crate) fn bind(self, input: Arc<Relation>) -> Result<Arc<Relation>> {
        let input = if self.keys.is_empty() {
            input
        } else {
            Arc::new(input.extend(self.keys)?)
        };
        let schema = input.schema().concat(&Schema::new(vec![self.expression.field.clone()])?)?;
        Ok(Arc::new(Relation::new(schema, RelationNode::Window {
            input, partition_by: self.partition_by, order_by: self.order_by,
            expressions: vec![self.expression],
        })?))
    }
}

impl Context<'_> {
    pub(crate) fn bind_window(&mut self, schema: &Schema, function: &ast::Function, replacements: &Replacements) -> Result<BoundWindow> {
        let name = grammar_window_name(function)?;
        let aggregate = AGGREGATE_WINDOW_FUNCTIONS.contains(&name.as_str());
        let modifier = |construct: &str| if aggregate { unsupported(construct) } else { grammar_rejected(format!("{construct} on {name}")) };
        let Some(ast::WindowType::WindowSpec(specification)) = &function.over else { return Err(unsupported("TODO: Window names currently not supported!")); };
        if specification.window_frame.is_some() { return Err(unsupported("TODO: Window frames currently not supported!")); }
        if function.filter.is_some() || !function.within_group.is_empty() || !matches!(function.parameters, ast::FunctionArguments::None) || function.uses_odbc_syntax { return Err(modifier("Window function modifiers")); }
        let ast::FunctionArguments::List(arguments) = &function.args else { return Err(error("Window function requires an argument list")); };
        if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() { return Err(modifier("Window argument modifiers")); }
        let arguments = arguments.args.iter().map(|argument| match argument {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => Ok(expr),
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) if name == "count" => Err(unsupported("Window function count")),
            _ => Err(grammar_rejected("A named or wildcard window function argument")),
        }).collect::<Result<Vec<_>>>()?;
        let function = match name.as_str() {
            "row_number" if arguments.is_empty() && function.null_treatment.is_none() => WindowFunction::RowNumber,
            "lead" | "lag" => {
                if !(1..=3).contains(&arguments.len()) { return Err(grammar_rejected("LEAD/LAG without one to three arguments")); }
                let offset = arguments.get(1).map(|argument| offset(argument)).transpose()?.unwrap_or(1);
                if arguments.len() == 3 { return Err(Anomaly::unsupported("xtdb.sql/lead-lag-default-expr", "LEAD/LAG default expressions are not currently supported")); }
                if function.null_treatment.is_some() { return Err(Anomaly::unsupported("xtdb.sql/lead-lag-ignore-nulls", "LEAD/LAG IGNORE NULLS is not currently supported")); }
                let Expr::Column { id: column, .. } = self.expr(arguments[0], schema, replacements)? else { return Err(Anomaly::unsupported("xtdb.sql/lead-lag-expr", "LEAD/LAG currently only supports column references, not arbitrary expressions")); };
                if name == "lead" { WindowFunction::Lead { column, offset } } else { WindowFunction::Lag { column, offset } }
            }
            _ if REFERENCE_WINDOW_FUNCTIONS.contains(&name.as_str()) || aggregate => return Err(unsupported(format!("Window function {name}"))),
            _ => return Err(grammar_rejected(format!("Window function {name}"))),
        };
        let result_name = format!("xt${name}_{}", self.allocate_symbol()?);
        // Inline specifications occupy a symbol between the window result and the projected sort keys.
        self.allocate_symbol()?;
        let partition_by = specification.partition_by.iter().map(|column| {
            if !matches!(column, ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_)) { return Err(grammar_rejected("A non-column-reference window partition key")); }
            match self.expr(column, schema, replacements)? {
                Expr::Column { id, .. } => Ok(id),
                _ => Err(error("Window partitions require column references")),
            }
        }).collect::<Result<Vec<ColumnId>>>()?;
        let mut keys = Vec::new();
        let order_by = specification.order_by.iter().map(|order| {
            if matches!(&order.expr, ast::Expr::Value(value) if matches!(&value.value, ast::Value::Number(value, _) if !value.contains(['.', 'e', 'E']))) { return Err(error("Window ORDER BY has no output ordinal positions")); }
            let expr = self.expr(&order.expr, schema, &BTreeMap::new())?;
            let descending = crate::bind::sort_descending(&order.options)?;
            let name = format!("_ob{}", self.allocate_symbol()?);
            let field = self.binder.field(name, None, expr.data_type())?;
            self.hidden_columns.insert(field.id,HiddenColumn::Internal);
            let key = field.expression();
            keys.push(NamedExpr { field,expr });
            Ok(SortExpr { expr: key, descending, nulls_first: order.options.nulls_first.unwrap_or(descending) })
        }).collect::<Result<Vec<_>>>()?;
        let field = self.binder.field(result_name, None, function.result_type(schema)?)?;
        self.hidden_columns.insert(field.id,HiddenColumn::Internal);
        Ok(BoundWindow { keys, partition_by, order_by, expression: WindowExpr { field, function } })
    }
}

const REFERENCE_WINDOW_FUNCTIONS: &[&str] = &["rank", "dense_rank", "percent_rank", "cume_dist", "ntile", "first_value", "last_value", "nth_value"];
const AGGREGATE_WINDOW_FUNCTIONS: &[&str] = &[
    "count", "array_agg", "avg", "max", "min", "sum", "every", "bool_and", "bool_or",
    "stddev_pop", "stddev_samp", "var_samp", "var_pop", "percentile_cont", "percentile_disc",
];

pub(crate) fn grammar_window_name(function: &ast::Function) -> Result<String> {
    let names = identifiers(&function.name)?;
    let [name] = names.as_slice() else { return Err(grammar_rejected("A qualified window function name")); };
    let normalized = name.normalized();
    if !name.quoted && (matches!(normalized.as_str(), "row_number" | "lead" | "lag")
        || REFERENCE_WINDOW_FUNCTIONS.contains(&normalized.as_str())
        || AGGREGATE_WINDOW_FUNCTIONS.contains(&normalized.as_str()))
    {
        Ok(normalized)
    } else {
        Err(grammar_rejected(format!("Window function {}", function.name)))
    }
}

fn offset(expression: &ast::Expr) -> Result<u64> {
    offset_literal(expression)?.parse::<i64>().map(|offset| offset as u64).map_err(|_| error("Window offset exceeds signed 64 bits"))
}

pub(crate) fn offset_literal(expression: &ast::Expr) -> Result<&str> {
    if let ast::Expr::Value(value) = expression
        && let ast::Value::Number(value, false) = &value.value
        && value.bytes().all(|byte| byte.is_ascii_digit())
    {
        Ok(value)
    } else {
        Err(grammar_rejected("A window offset other than an unsigned integer literal"))
    }
}
