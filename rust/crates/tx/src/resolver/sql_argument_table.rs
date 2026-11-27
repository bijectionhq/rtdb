use std::sync::Arc;

use arrow_array::{Array, ListArray, RecordBatch, StructArray, cast::AsArray};
use arrow_buffer::OffsetBuffer;
use rtdb_columnar::{MemoryReservation, OwnedBatch};
use rtdb_plan::{ColumnId, Expr, Field, NamedExpr, Relation, RelationNode, Schema};
use rtdb_query::QueryContext;
use rtdb_types::{Anomaly, AnomalyCategory, AnomalyKind, Result, TableRef};

use crate::{SqlArguments, values::BoundArgumentRow};

use super::{
    HOSTED_CAPACITY_DECLARATIONS, OpenTransaction, check_table, execute, mutation_put_batches, overflow,
    plain_scalar_field, scalar_mutation_put_field,
};

/// How the batched argument-table path ended: it wrote every row, or the statement's output has a shape only the
/// row-by-row path writes the way XTDB does, and nothing was written. Its errors are the statement's, except one an
/// argument row's values raise: the batch cannot tell which row, so the row-by-row path raises it again with that
/// row's `arg-idx`, as XTDB reports it.
#[derive(Debug, PartialEq, Eq)]
enum ArgumentTable {
    Applied,
    Unsupported,
}

impl OpenTransaction<'_> {
    pub(super) async fn execute_sql_arguments(&mut self, sql: &str, arguments: &SqlArguments) -> Result<()> {
        let mut rows = arguments.indexed_bound_rows();
        let mut planned = None;
        if let Some(page) = arguments.single_arrow_page()
            && page.batch().num_rows() > 1
            && page
                .batch()
                .schema_ref()
                .fields()
                .iter()
                .all(|field| plain_scalar_field(field))
            && arguments
                .types()
                .iter()
                .zip(arguments.execution_types())
                .all(|(source, target)| *source == target)
            && let Some((index, first)) = rows.next().transpose()?
        {
            let at_argument = |error: Anomaly| error.with_context("arg-idx", index.into());
            let (mutation, query, context, syntax) = self.plan_sql_mutation(sql, Some(first), &mut planned).map_err(at_argument)?;
            if let rtdb_sql::Mutation::Put { table, input } = &mutation
                && table.database == self.context.database
                && !(table.table.schema == "xt" && table.table.table == HOSTED_CAPACITY_DECLARATIONS)
                && input.parameter_count() == page.batch().num_columns()
                && let Some(maximum) = row_local_max_column(input)
                && let Some(argument_column) = maximum.checked_add(1).map(ColumnId)
            {
                match self
                    .try_argument_table(
                        &table.table,
                        input,
                        argument_column,
                        Arc::clone(&context),
                        Arc::clone(&syntax),
                        page,
                    )
                    .await
                {
                    Ok(ArgumentTable::Applied) => return Ok(()),
                    Ok(ArgumentTable::Unsupported) => {}
                    // The batch cannot tell which argument row failed; the row-by-row path raises the same error
                    // with that row's `arg-idx`, as XTDB reports it.
                    Err(error) if raised_by_a_row(&error) => {}
                    Err(error) => return Err(error.with_context("sql", sql.into())),
                }
            }
            self.execute_mutation(mutation, query, context, Some(first), Some(syntax))
                .await
                .map_err(|error| at_argument(error.with_context("sql", sql.into())))?;
        }
        for row in rows {
            let (index, row) = row?;
            self.execute_sql_typed(sql, Some(row), &mut planned)
                .await
                .map_err(|error| error.with_context("arg-idx", index.into()))?;
        }
        Ok(())
    }

    async fn try_argument_table(
        &mut self,
        table: &TableRef,
        input: &Relation,
        argument_column: ColumnId,
        context: Arc<QueryContext>,
        syntax: Arc<MemoryReservation>,
        owned_page: &OwnedBatch,
    ) -> Result<ArgumentTable> {
        check_table(table)?;

        let memory = &context.memory;
        let page = owned_page.batch();
        // The parameter wraps the page's own buffers, which the page's charge already covers: only the list's two
        // offsets and the plan's schema translation are new.
        let new_bytes = rtdb_format::ipc::schema_translation_bound(page.schema_ref())?
            .checked_add(2 * std::mem::size_of::<i32>())
            .ok_or_else(overflow)?;
        let _new = memory.reserve(new_bytes)?;
        let fields = page
            .schema_ref()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| field.as_ref().clone().with_name(format!("arg_{index}")))
            .collect::<arrow_schema::Fields>();
        let documents = rtdb_columnar::build_struct_array(fields, page.columns(), page.num_rows(), None)?;
        let item = Arc::new(arrow_schema::Field::new("item", documents.data_type().clone(), false));
        let offsets = OffsetBuffer::new(vec![0, i32::try_from(page.num_rows()).map_err(|_| overflow())?].into());
        let list = ListArray::new(item.clone(), offsets, documents, None);
        let field = arrow_schema::Field::new("?_0", list.data_type().clone(), false);
        let types = [rtdb_columnar::from_field(&field)?];
        let parameter = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(vec![field])), vec![Arc::new(list)])
            .map_err(|error| Anomaly::fault("rtdb.tx/mutation-schema", error.to_string()))?;
        let parameter = owned_page.with_retained_buffers(parameter);
        let element = Field {
            id: argument_column,
            name: "sql_arguments".into(),
            qualifier: None,
            qualified_only: false,
            unnamed: false,
            data_type: rtdb_columnar::from_field(&item)?,
        };
        let expr = Expr::Parameter {
            index: 0,
            data_type: types[0].clone(),
        };
        let source = Arc::new(Relation::new(
            Schema::new(vec![element.clone()])?,
            RelationNode::ColumnTable {
                expr,
                element: element.clone(),
            },
        )?);
        let plan = bind_argument_table(input, &source, &element)?;
        let plan = Arc::new(
            plan.project(
                input
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| NamedExpr {
                        field: field.clone(),
                        expr: field.expression(),
                    })
                    .collect(),
            )?,
        );

        let binding = BoundArgumentRow::Arrow {
            batch: &parameter,
            row: 0,
            types: &types,
        };
        let mut cursor = execute(plan, Arc::clone(&context), Some(binding), Some(syntax)).await?;
        let first = loop {
            let Some(output) = cursor.next_batch().await? else { return Ok(ArgumentTable::Applied); };
            if output.batch().num_rows() != 0 {
                break output;
            }
        };
        let Some(first_doc) = argument_put_document(first.batch()) else {
            return Ok(ArgumentTable::Unsupported);
        };
        // The serial writer's declarations differ between a null-only row and a populated row.
        if !first_doc.fields().iter().all(|field| scalar_mutation_put_field(field))
            || first_doc
                .columns()
                .iter()
                .any(|column| column.null_count() != 0 && column.null_count() != column.len())
        {
            return Ok(ArgumentTable::Unsupported);
        }

        let mut inventory = None;
        let mut rest = Vec::new();
        while let Some(output) = cursor.next_batch().await? {
            let batch = output.batch();
            if batch.num_rows() == 0 {
                continue;
            }
            let Some(doc) = argument_put_document(batch) else { return Ok(ArgumentTable::Unsupported); };
            if batch.schema_ref() != first.batch().schema_ref()
                || !doc.columns().iter().zip(first_doc.columns()).all(|(column, initial)| {
                    let expected_nulls = if initial.null_count() == 0 { 0 } else { column.len() };
                    column.null_count() == expected_nulls
                })
            {
                return Ok(ArgumentTable::Unsupported);
            }
            if inventory.is_none() {
                let bytes = page.num_rows().checked_mul(std::mem::size_of::<OwnedBatch>() * 2)
                    .ok_or_else(overflow)?;
                inventory = Some(memory.reserve(bytes)?);
            }
            rest.push(output);
        }
        drop(cursor);
        drop(parameter);
        let Some(put) = mutation_put_batches(first.batch(), &rest, self.tx_key().system_time, memory)? else {
            return Ok(ArgumentTable::Unsupported);
        };
        drop(rest);
        drop(inventory);
        drop(first);
        self.buffer.append_batch(table, put)?;
        Ok(ArgumentTable::Applied)
    }
}

/// Whether `error` is one an argument row's own values raise, rather than an unsupported feature, cancellation,
/// exhaustion or a fault.
fn raised_by_a_row(error: &Anomaly) -> bool {
    error.category == AnomalyCategory::Incorrect && error.kind != Some(AnomalyKind::ResourceExhausted)
}

fn argument_put_document(batch: &RecordBatch) -> Option<&StructArray> {
    let doc = batch.column_by_name("doc")?.as_struct_opt()?;
    (doc.null_count() == 0).then_some(doc)
}

fn row_local_max_column(input: &Relation) -> Option<u32> {
    let supported = match input.node() {
        RelationNode::Values { rows } => rows.len() == 1,
        RelationNode::Project { .. }
        | RelationNode::Rename { .. }
        | RelationNode::Extend { .. }
        | RelationNode::Filter { .. } => true,
        _ => false,
    };
    if !supported
        || input
            .expressions()
            .iter()
            .any(|expr| expr.is_volatile() || rtdb_expr::requires_pg_catalog(expr))
    {
        return None;
    }
    let maximum = input.schema().fields().iter().map(|field| field.id.0).max().unwrap_or(0);
    input.children().into_iter().try_fold(maximum, |maximum, child| {
        row_local_max_column(child).map(|id| maximum.max(id))
    })
}

fn bind_argument_table(input: &Relation, source: &Arc<Relation>, element: &Field) -> Result<Arc<Relation>> {
    let bind = |mut expr: Expr| {
        expr.rewrite(&mut |expr| {
            if let Expr::Parameter { index, .. } = expr {
                let name = format!("arg_{index}");
                let data_type = rtdb_plan::field_type(&element.data_type, &name);
                *expr = Expr::Field {
                    expr: Box::new(element.expression()),
                    name,
                    data_type,
                };
            }
        });
        expr
    };
    let named = |expr: &NamedExpr| NamedExpr {
        field: expr.field.clone(),
        expr: bind(expr.expr.clone()),
    };
    Ok(Arc::new(match input.node() {
        RelationNode::Values { rows } => Arc::clone(source).extend(
            input
                .schema()
                .fields()
                .iter()
                .zip(&rows[0])
                .map(|(field, expr)| NamedExpr {
                    field: field.clone(),
                    expr: bind(expr.clone()),
                })
                .collect(),
        )?,
        RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. } => {
            let mut expressions = expressions.iter().map(named).collect::<Vec<_>>();
            expressions.push(NamedExpr {
                field: element.clone(),
                expr: element.expression(),
            });
            bind_argument_table(input, source, element)?.project(expressions)?
        }
        RelationNode::Extend { input, expressions } => {
            bind_argument_table(input, source, element)?.extend(expressions.iter().map(named).collect())?
        }
        RelationNode::Filter { input, predicate } => {
            bind_argument_table(input, source, element)?.filter(bind(predicate.clone()))?
        }
        _ => unreachable!("The argument table requires a row-local plan"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_row_error_runs_the_statement_again_row_by_row() {
        assert!(raised_by_a_row(&Anomaly::incorrect("xtdb.expression/division-by-zero", "division by zero")));
        assert!(!raised_by_a_row(&Anomaly::new(AnomalyCategory::Unsupported, "rtdb.test/unsupported", "unsupported")));
        let mut exhausted = Anomaly::incorrect("rtdb/memory-budget-exceeded", "no room");
        exhausted.kind = Some(AnomalyKind::ResourceExhausted);
        assert!(!raised_by_a_row(&exhausted));
        assert!(!raised_by_a_row(&Anomaly::new(AnomalyCategory::Interrupted, "rtdb/cancelled", "cancelled")));
        assert!(!raised_by_a_row(&Anomaly::fault("rtdb.tx/mutation-schema", "bad schema")));
    }
}
