use std::sync::Arc;

use arrow_array::{Array, ListArray, RecordBatch, cast::AsArray};
use arrow_buffer::OffsetBuffer;
use rtdb_columnar::{MemoryReservation, OwnedBatch};
use rtdb_plan::{ColumnId, Expr, Field, NamedExpr, Relation, RelationNode, Schema};
use rtdb_query::QueryContext;
use rtdb_types::{Anomaly, Result, TableRef};

use crate::{SqlArguments, values::BoundArgumentRow};

use super::{
    HOSTED_CAPACITY_DECLARATIONS, OpenTransaction, check_table, execute, mutation_put_batch, overflow, plain_scalar_field,
};

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
            let (mutation, context, syntax) = self.plan_sql_mutation(sql, Some(first), &mut planned).map_err(at_argument)?;
            if let rtdb_sql::Mutation::Put { table, input } = &mutation
                && table.database == self.context.database
                && !(table.table.schema == "xt" && table.table.table == HOSTED_CAPACITY_DECLARATIONS)
                && input.parameter_count() == page.batch().num_columns()
                && let Some(maximum) = row_local_max_column(input)
                && let Some(argument_column) = maximum.checked_add(1).map(ColumnId)
                && self
                    .try_argument_table(
                        &table.table,
                        input,
                        argument_column,
                        Arc::clone(&context),
                        Arc::clone(&syntax),
                        page.batch(),
                    )
                    .await
                    .unwrap_or(false)
            {
                return Ok(());
            }
            self.execute_mutation(mutation, context, Some(first), Some(syntax))
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
        page: &RecordBatch,
    ) -> Result<bool> {
        check_table(table)?;

        let memory = &context.memory;
        let plan_bytes = rtdb_format::ipc::schema_translation_bound(page.schema_ref())?;
        let bound = page
            .get_array_memory_size()
            .checked_add(plan_bytes)
            .and_then(|size| size.checked_add(4096))
            .ok_or_else(overflow)?;
        let reservation = memory.reserve(bound)?;
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
        let parameter = OwnedBatch::with_reservation(parameter, reservation)?;
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
        let _inventory = memory.reserve(
            page.num_rows()
                .checked_mul(std::mem::size_of::<OwnedBatch>() * 2)
                .ok_or_else(overflow)?,
        )?;
        let mut pages = Vec::new();
        while let Some(page) = cursor.next_batch().await? {
            if page.batch().num_rows() > 0 {
                pages.push(page);
            }
        }
        drop(cursor);
        let Some(first) = pages.first() else {
            return Ok(true);
        };
        let scratch = pages.iter().try_fold(4096usize, |bytes, page| {
            page.batch()
                .get_array_memory_size()
                .checked_mul(4)
                .and_then(|size| bytes.checked_add(size))
                .ok_or_else(overflow)
        })?;
        let _scratch = memory.reserve(scratch)?;
        let output = if pages.len() == 1 {
            first.batch().clone()
        } else {
            arrow_select::concat::concat_batches(first.batch().schema_ref(), pages.iter().map(OwnedBatch::batch))
                .map_err(|error| Anomaly::fault("rtdb.tx/mutation-schema", error.to_string()))?
        };
        let Some(doc) = output.column_by_name("doc").and_then(|column| column.as_struct_opt()) else {
            return Ok(false);
        };
        // The serial writer's declarations differ between a null-only row and a populated row.
        if !doc.fields().iter().all(|field| plain_scalar_field(field))
            || doc
                .columns()
                .iter()
                .any(|column| column.null_count() != 0 && column.null_count() != column.len())
        {
            return Ok(false);
        }
        let Some(put) = mutation_put_batch(&output, self.tx_key().system_time, memory)? else {
            return Ok(false);
        };
        self.buffer.append_batch(table, put)?;
        Ok(true)
    }
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
