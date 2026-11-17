use std::{collections::BTreeMap, sync::Arc};
use rtdb_plan::{ColumnId, Expr, Field, NamedExpr, Relation, RelationNode, Schema, SortExpr, WindowExpr, WindowFunction};
use rtdb_types::{Anomaly, Result};
use sqlparser::ast;
use crate::bind::{Context, HiddenColumn, error, identifiers, unsupported};
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
            let projections = input.schema().fields().iter()
                .map(|field| NamedExpr { field: field.clone(), expr: field.expression() })
                .chain(self.keys).collect();
            Arc::new(input.project(projections)?)
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
        let Some(ast::WindowType::WindowSpec(specification)) = &function.over else { return Err(unsupported("Named windows")); };
        if specification.window_frame.is_some() { return Err(unsupported("Window frames")); }
        if function.filter.is_some() || !function.within_group.is_empty() || !matches!(function.parameters, ast::FunctionArguments::None) || function.uses_odbc_syntax { return Err(unsupported("Window function modifiers")); }
        let ast::FunctionArguments::List(arguments) = &function.args else { return Err(error("Window function requires an argument list")); };
        if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() { return Err(unsupported("Window argument modifiers")); }
        let arguments = arguments.args.iter().map(|argument| match argument {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => Ok(expr),
            _ => Err(unsupported("Named window arguments and wildcard arguments")),
        }).collect::<Result<Vec<_>>>()?;
        let names = identifiers(&function.name)?;
        if names.len() != 1 { return Err(unsupported("Qualified window function names")); }
        let name = names[0].normalized();
        let function = match name.as_str() {
            "row_number" if arguments.is_empty() && function.null_treatment.is_none() => WindowFunction::RowNumber,
            "lead" | "lag" => {
                if arguments.len() > 2 { return Err(Anomaly::unsupported("xtdb.sql/lead-lag-default-expr", "LEAD/LAG default expressions are not currently supported")); }
                if function.null_treatment.is_some() { return Err(Anomaly::unsupported("xtdb.sql/lead-lag-ignore-nulls", "LEAD/LAG IGNORE NULLS is not currently supported")); }
                let argument = arguments.first().ok_or_else(|| error("LEAD/LAG requires a column"))?;
                let Expr::Column { id: column, .. } = self.expr(argument, schema, replacements)? else { return Err(Anomaly::unsupported("xtdb.sql/lead-lag-expr", "LEAD/LAG currently only supports column references, not arbitrary expressions")); };
                let offset = arguments.get(1).map(|argument| offset(argument)).transpose()?.unwrap_or(1);
                if name == "lead" { WindowFunction::Lead { column, offset } } else { WindowFunction::Lag { column, offset } }
            }
            _ => return Err(unsupported(format!("Window function {name}"))),
        };
        let partition_by = specification.partition_by.iter().map(|column| {
            if !matches!(column, ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_)) { return Err(error("Window partitions require column references")); }
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
            let mut field = self.binder.field("",None,expr.data_type())?;
            field.name = format!("_window_order_{}",field.id.0);
            self.hidden_columns.insert(field.id,HiddenColumn::Internal);
            let key = field.expression();
            keys.push(NamedExpr { field,expr });
            Ok(SortExpr { expr: key, descending, nulls_first: order.options.nulls_first.unwrap_or(descending) })
        }).collect::<Result<Vec<_>>>()?;
        let mut field = self.binder.field("", None, function.result_type(schema)?)?;
        field.name = format!("_window_{}", field.id.0);
        self.hidden_columns.insert(field.id,HiddenColumn::Internal);
        Ok(BoundWindow { keys, partition_by, order_by, expression: WindowExpr { field, function } })
    }
}

fn offset(expression: &ast::Expr) -> Result<u64> {
    match expression {
        ast::Expr::Value(value) => match &value.value {
            ast::Value::Number(value, _) if value.bytes().all(|byte| byte.is_ascii_digit()) => value.parse::<i64>().ok().filter(|offset| *offset >= 0).map(|offset| offset as u64).ok_or_else(|| error("Window offset exceeds signed 64 bits")),
            _ => Err(error("Window offset requires an unsigned integer literal")),
        },
        _ => Err(error("Window offset requires an unsigned integer literal")),
    }
}
