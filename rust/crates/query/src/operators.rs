use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_expr::{integer, is_true, null_eq_in_timezone};
use rtdb_plan::{ApplyKind, Expr, NamedExpr, Relation, RelationNode, Schema, SetOperator};
use rtdb_types::{LogicalType, Result, Value};

use crate::context::Execution;
use crate::limit_bounds::{checked_row_count, sort_prefix_len};
use crate::runtime_schema::{EmittedRelation, RuntimeSchema};
use crate::invalid;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum BatchOutput { Arrow, ArrowPages }

impl BatchOutput {
    fn semantic_pages(self) -> bool { self == Self::ArrowPages }
}

pub(crate) fn build_batch_input(
    runtime: Arc<EmittedRelation>, execution: Execution, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>, output: BatchOutput,
) -> futures::future::BoxFuture<'static, Result<Option<(crate::columnar::BoxBatchCursor, bool)>>> {
    build_batch_input_with_state(runtime, execution, pushdowns, output, false)
}

fn build_batch_input_with_state(
    runtime: Arc<EmittedRelation>, execution: Execution, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>, output: BatchOutput, reserve_state: bool,
) -> futures::future::BoxFuture<'static, Result<Option<(crate::columnar::BoxBatchCursor, bool)>>> {
    let mut build = Box::pin(async move {
        let relation = Arc::clone(runtime.relation());
        if matches!(relation.node(), RelationNode::SortLimit { fetch: Some(_), .. })
            && !execution.context.accelerators.is_empty()
            && let Some(input) = build_ordered_columns(Arc::clone(&runtime), execution.clone(), (0..runtime.schema().len()).collect()).await?
        {
            return Ok(Some((input, false)));
        }
        let already_in_operator = execution.context.operator_root.is_some();
        let execution = execution.in_operator();
        let context = Arc::clone(&execution.context);
        let analysis = execution.analysis.clone();
        let reservation = if reserve_state && !matches!(relation.node(), RelationNode::Let { .. }) {
            Some(state_reservation(&runtime, &execution)?)
        } else { None };
        let metadata_scan = matches!(relation.node(), RelationNode::Scan(scan) if
            crate::inspection::is_inspection(&scan.table.name.table) || crate::system_schema::definition(&scan.table.name.table).is_some());
        let Some((mut input, projected)) = build_batches_at(runtime, execution, pushdowns, output).await? else { return Ok(None); };
        if let Some(reservation) = reservation { input = crate::columnar::retain_batch_state(input, reservation); }
        let input = if !already_in_operator && !metadata_scan {
            let input = crate::analysis::wrap_batches(input, analysis, &context.memory);
            crate::trace::wrap_batches(input, &relation, &context)
        } else { input };
        Ok(Some((input, projected)))
    });
    Box::pin(std::future::poll_fn(move |context| {
        stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || build.as_mut().poll(context))
    }))
}

async fn build_batches_at(
    runtime: Arc<EmittedRelation>, execution: Execution, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>, output: BatchOutput,
) -> Result<Option<(crate::columnar::BoxBatchCursor, bool)>> {
    use crate::{columnar::{BoxBatchCursor, ColumnBatch, single_batch}, sort::batches::BatchSort};

    let relation = Arc::clone(runtime.relation());
    match relation.node() {
        RelationNode::Values { rows } => {
            let batch = if ColumnBatch::supports_literal_rows(rows) {
                match ColumnBatch::from_literal_rows(rows, &execution.context) {
                    Ok(batch) => batch,
                    Err(error) => return Err(error),
                }
            } else {
                materialize_columns(rows, runtime.schema().len(), &execution).await?
            };
            Ok(Some((single_batch(batch, execution.context), false)))
        }
        RelationNode::List { expr, page_rows, .. } => {
            let cursor = execution.open_list(expr.clone(), *page_rows).await?;
            Ok(Some((Box::new(cursor), false)))
        }
        RelationNode::ArrowLiteralPages { .. } => {
            Ok(Some((Box::new(ArrowLiteralCursor { relation, next: 0, execution }), false)))
        }
        RelationNode::LiteralPages { .. } => {
            Ok(Some((Box::new(LiteralBatchCursor { relation, width: runtime.schema().len(), next: 0, execution }), false)))
        }
        RelationNode::Scan(scan) if !crate::inspection::is_inspection(&scan.table.name.table) && crate::system_schema::definition(&scan.table.name.table).is_none() => {
            let batches: crate::columnar::BoxBatchCursor = Box::new(crate::scan::ScanCursor::new(scan.clone(),execution.clone(),pushdowns).await?);
            let batches = match scan.predicates.iter().cloned().reduce(|left,right|Expr::binary(rtdb_plan::BinaryOp::And,left,right)) {
                Some(predicate) => Box::new(crate::columnar::BatchFilter::new(batches,runtime.schema().clone(),predicate,execution)) as crate::columnar::BoxBatchCursor,
                None => batches,
            };
            Ok(Some((batches,false)))
        }
        RelationNode::Scan(scan) => {
            let batches = open_catalog_scan(scan, &runtime, execution).await?;
            Ok(Some((batches, false)))
        }
        RelationNode::ColumnTable { expr, .. } => {
            Ok(Some((crate::column_table::open_columns(&execution, expr.clone()).await?, false)))
        }
        RelationNode::Filter { predicate,.. } => {
            let mut input = runtime.child(0)?;
            let mut input_execution = execution.child(0);
            let schema = input.schema().clone();
            let mut stages = vec![predicate.clone()];
            let mut state = execution.context.memory.reserve(0)?;
            while execution.analysis.is_none() && execution.context.trace_parent.is_none()
                && let RelationNode::Filter { predicate, .. } = input.relation().node() {
                let bound = crate::runtime_schema::local_allocation_bound(input.relation())?;
                let lease = Arc::get_mut(&mut state).expect("Stacked filters own their reservation");
                lease.grow_to(lease.bytes().checked_add(bound).ok_or_else(|| invalid("operator-size", "Filter stages exceed addressable memory"))?)?;
                stages.push(predicate.clone());
                input = input.child(0)?;
                input_execution = input_execution.child(0);
            }
            let Some((batches, projected)) = build_batch_input(input, input_execution, pushdowns, output).await? else { return Ok(None); };
            stages.reverse();
            let context = Arc::clone(&execution.context);
            let filter = Box::new(crate::columnar::BatchFilter::with_stages(batches, schema, stages, execution));
            let filter = crate::columnar::retain_batch_state(filter, state);
            let filter = if output.semantic_pages() { crate::columnar::coalesce_pages(filter, context) } else { filter };
            Ok(Some((filter, projected)))
        }
        RelationNode::Project { expressions,.. } | RelationNode::Rename { expressions,.. } => {
            let child = runtime.child(0)?;
            let positions = expressions.iter().map(|named| match &named.expr { Expr::Column { id,.. } => child.schema().position(*id), _ => None }).collect::<Option<Vec<_>>>();
            let scalar_fallback = !expressions.iter().all(|named| crate::columnar::projection::shaped(&named.expr));
            let schema = child.schema().clone();
            let pushdowns = pushdowns.map(|filters|filters.project(expressions,&execution.context.memory)).transpose()?;
            let Some((batches,_)) = build_batch_input(child,execution.child(0),pushdowns,output).await? else { return Ok(None) };
            let batches: crate::columnar::BoxBatchCursor = match positions {
                Some(positions) => Box::new(crate::columnar::BatchProject::new(batches,positions,Arc::clone(&execution.context.memory))),
                None => Box::new(crate::columnar::BatchExpressions::new(batches,schema,expressions.iter().map(|named|named.expr.clone()).collect(),execution)
                    .validating_input(scalar_fallback)),
            };
            Ok(Some((batches,true)))
        }
        RelationNode::Extend { expressions, .. } => {
            let child = runtime.child(0)?;
            let scalar_fallback = !expressions.iter().all(|named| crate::columnar::projection::shaped(&named.expr));
            let schema = child.schema().clone();
            let projections = schema.fields().iter().map(rtdb_plan::Field::expression)
                .chain(expressions.iter().map(|named| named.expr.clone())).collect();
            let Some((batches, _)) = build_batch_input(child, execution.child(0), pushdowns, output).await? else { return Ok(None); };
            let expressions = crate::columnar::BatchExpressions::new(batches, schema, projections, execution).validating_input(scalar_fallback);
            Ok(Some((Box::new(expressions), true)))
        }
        RelationNode::Set { operator, all, .. } => {
            let left = runtime.child(0)?;
            let right = runtime.child(1)?;
            let Some((left, left_projected)) = build_batch_input(left, execution.child(0), pushdowns.clone(), output).await? else { return Ok(None); };
            let Some((right, right_projected)) = build_batch_input(right, execution.child(1), pushdowns, output).await? else { return Ok(None); };
            if *operator == SetOperator::Union {
                let projected = left_projected || right_projected;
                let input: BoxBatchCursor = Box::new(BatchUnion { left: Some(left), right });
                if *all { return Ok(Some((input, projected))); }
                let input = BatchDistinct::new(input, runtime.output(), execution, projected)?;
                return Ok(Some((Box::new(input), projected)));
            }
            let seen = crate::set_keys::BatchKeyIndex::new(&execution.context)?;
            let input = BatchSet { left, right: Some((right, right_projected)), left_projected, operator: *operator,
                all: *all, seen, built: None, runtime, execution };
            Ok(Some((Box::new(input), left_projected)))
        }
        RelationNode::Limit { offset, fetch, .. } => {
            let Some((input, projected)) = build_batch_input(runtime.child(0)?, execution.child(0), None, output).await? else { return Ok(None); };
            let offset = row_limit(offset.as_ref(), &execution).await?.unwrap_or(0);
            let remaining = row_limit(fetch.as_ref(), &execution).await?;
            let limit = BatchLimit { input: Some(input), offset, remaining, context: execution.context };
            Ok(Some((Box::new(limit), projected)))
        }
        RelationNode::Unnest { expr, ordinality, outer, .. } => {
            let child = runtime.child(0)?;
            let schema = child.schema().clone();
            let position = match expr { Expr::Column { id, .. } => schema.position(*id), _ => None };
            let Some((input, projected)) = build_batch_input(child, execution.child(0), pushdowns, output).await? else { return Ok(None); };
            let unnest = BatchUnnest { input, position, expr: expr.clone(), schema, projected,
                ordinality: ordinality.as_ref().map(|field| field.name.clone()), outer: *outer, execution };
            Ok(Some((Box::new(unnest), projected)))
        }
        RelationNode::Ordinality { scope, field, .. } => {
            let child = runtime.child(0)?;
            let input_mode = if *scope == rtdb_plan::OrdinalityScope::Page { BatchOutput::ArrowPages } else { output };
            let Some((input, projected)) = build_batch_input(child, execution.child(0), None, input_mode).await? else { return Ok(None); };
            let ordinal = crate::list::BatchOrdinality::new(input, field.name.clone(), *scope, execution.context);
            Ok(Some((Box::new(ordinal), projected)))
        }
        RelationNode::Window { partition_by, order_by, expressions, .. } => {
            let child = runtime.child(0)?;
            let schema = child.schema().clone();
            let computed = !order_by.is_empty() && !BatchSort::supports(&schema, order_by);
            if !crate::window::BatchWindow::supports(&schema, partition_by, &[], expressions) { return Ok(None); }
            let Some((input, projected)) = build_batch_input(child, execution.child(0), None, output).await? else { return Ok(None); };
            if computed {
                let width = schema.len();
                let Some((input, schema, order)) = project_sort_keys(input, schema, order_by, &execution)? else { return Ok(None); };
                let positions = (0..width).chain(schema.len()..schema.len() + expressions.len()).collect();
                let memory = Arc::clone(&execution.context.memory);
                let window = crate::window::BatchWindow::new(input, schema, partition_by, order, expressions, true, execution)?;
                let window = crate::columnar::BatchProject::new(Box::new(window), positions, memory);
                return Ok(Some((Box::new(window), true)));
            }
            let window = crate::window::BatchWindow::new(input, schema, partition_by, order_by.clone(), expressions, projected, execution)?;
            Ok(Some((Box::new(window), projected)))
        }
        RelationNode::Distinct { .. } => {
            let Some((input, projected)) = build_batch_input(runtime.child(0)?, execution.child(0), pushdowns, output).await? else { return Ok(None); };
            let input = BatchDistinct::new(input, runtime.output(), execution, projected)?;
            Ok(Some((Box::new(input), projected)))
        }
        RelationNode::Aggregate { groups, aggregates, .. } => {
            let child = runtime.child(0)?;
            let schema = child.schema().clone();
            let Some((input, projected)) = build_batch_input(child, execution.child(0), pushdowns, output).await? else { return Ok(None); };
            let (input, schema, groups) = crate::aggregate::project_group_keys(input, schema, groups, &execution)?;
            let aggregate = crate::aggregate::AggregateCursor::from_batches(input, projected, schema,
                groups, aggregates.clone(), runtime, execution)?;
            Ok(Some((Box::new(aggregate), false)))
        }
        RelationNode::Let { binding, .. } => {
            build_materialized_columns(&runtime, binding, execution, output).await
        }
        RelationNode::Reference { binding } => {
            let scope = execution.bindings.as_ref().ok_or_else(|| invalid("missing-relation", "Reference has no lexical binding"))?;
            Ok(Some((scope.open_columns(binding).await?, false)))
        }
        RelationNode::Sort { order_by, .. } | RelationNode::SortLimit { order_by, .. } => {
            let child = runtime.child(0)?;
            let schema = child.schema().clone();
            let computed = !order_by.is_empty() && !BatchSort::supports(&schema, order_by);
            let pushdowns = match relation.node() {
                RelationNode::SortLimit { offset, fetch, .. } if offset.is_some() || fetch.is_some() => None,
                _ => pushdowns,
            };
            let Some((batches, projected)) = build_batch_input(child, execution.child(0), pushdowns, output).await? else { return Ok(None); };
            let (offset, limit) = match relation.node() {
                RelationNode::SortLimit { offset, fetch, .. } => {
                    let offset = row_limit(offset.as_ref(), &execution).await?.unwrap_or(0);
                    let count = row_limit(fetch.as_ref(), &execution).await?.unwrap_or(i64::MAX as u64);
                    if count == 0 && offset == 0 {
                        let empty = BatchLimit { input: Some(batches), offset, remaining: Some(0), context: execution.context };
                        return Ok(Some((Box::new(empty), projected || computed)));
                    }
                    (offset, offset.saturating_add(count).min(i64::MAX as u64))
                }
                _ => (0, i64::MAX as u64),
            };
            if order_by.is_empty() {
                let limit = BatchLimit { input: Some(batches), offset, remaining: Some(limit - offset), context: execution.context };
                return Ok(Some((Box::new(limit), projected)));
            }
            if computed {
                let positions = (0..schema.len()).collect();
                let memory = Arc::clone(&execution.context.memory);
                let Some((input, schema, order)) = project_sort_keys(batches, schema, order_by, &execution)? else { return Ok(None); };
                let sort = BatchSort::new(input, schema, order, execution, true, offset, limit)?;
                let sort = Box::new(if output.semantic_pages() { sort.preserving_pages() } else { sort });
                let output = Box::new(crate::columnar::BatchProject::new(sort, positions, memory));
                return Ok(Some((output, true)));
            }
            let sort = BatchSort::new(batches, schema, order_by.clone(), execution, projected, offset, limit)?;
            let sort = if output.semantic_pages() { sort.preserving_pages() } else { sort };
            Ok(Some((Box::new(sort), projected)))
        }
        RelationNode::Mark { dependent, arguments, condition, comparison, .. }
            if arguments.is_empty() => {
            let build = runtime.child(1)?;
            let probe = runtime.child(0)?;
            let Some(probe_projected) = batch_shape(&probe, output)? else { return Ok(None); };
            if batch_shape(&build, output)?.is_none() { return Ok(None); }
            let keys = condition.is_none().then(|| crate::mark::batch_membership_keys(probe.schema(), build.schema(), comparison)).flatten();
            let schema = probe.schema().concat(build.schema())?;
            let build = crate::batch_join::ProbeInput { runtime: build, execution: execution.child(1), pushdowns: None, output };
            let probe = crate::batch_join::ProbeInput { runtime: probe, execution: execution.child(0), pushdowns: None, output };
            let mark: BoxBatchCursor = if let Some(keys) = keys {
                let bounded = crate::mark::bounded_membership_input(dependent)
                    && crate::join::rows_at_most(dependent, &execution)? <= execution.context.options.join_build_rows;
                Box::new(crate::mark::BatchMembership::new(build, probe, keys, !bounded, execution))
            } else {
                let comparison = match condition {
                    Some(condition) => Expr::Case { branches: vec![(condition.clone(), comparison.clone())],
                        otherwise: Box::new(Expr::Literal(Value::Bool(false))) },
                    None => comparison.clone(),
                };
                Box::new(crate::mark::SharedBatchMark::new(build, probe, schema, comparison, execution))
            };
            Ok(Some((mark, probe_projected)))
        }
        RelationNode::Mark { arguments, condition, comparison, .. } => {
            let child = runtime.child(0)?;
            let schema = child.schema().clone();
            let dependent = runtime.child(1)?;
            let comparison = match condition {
                Some(condition) => Expr::Case { branches: vec![(condition.clone(), comparison.clone())],
                    otherwise: Box::new(Expr::Literal(Value::Bool(false))) },
                None => comparison.clone(),
            };
            let comparison_schema = schema.concat(dependent.schema())?;
            let Some((input, projected)) = build_batch_input(child, execution.child(0), None, output).await? else { return Ok(None); };
            let plan = BatchApplyPlan { schema, dependent, arguments: arguments.clone(), kind: ApplyKind::Mark,
                document_field: None, comparison: Some((comparison, comparison_schema)), execution };
            let apply = BatchApply { input, plan };
            Ok(Some((Box::new(apply), projected)))
        }
        RelationNode::Apply { arguments, kind, .. } => {
            let child = runtime.child(0)?;
            let schema = child.schema().clone();
            let dependent = runtime.child(1)?;
            let Some((input, projected)) = build_batch_input(child, execution.child(0), None, output).await? else { return Ok(None); };
            let document_field = matches!(kind, ApplyKind::Pull | ApplyKind::PullMany)
                .then(|| runtime.schema().fields().last().cloned()).flatten();
            let plan = BatchApplyPlan { schema, dependent, arguments: arguments.clone(), kind: *kind, document_field, comparison: None, execution };
            let apply = BatchApply { input, plan };
            Ok(Some((Box::new(apply), projected)))
        }
        RelationNode::PatchGaps { input, iid, valid_from, valid_to, document, from, to } => {
            let sorted = runtime.child(0)?;
            let RelationNode::Sort { order_by, .. } = input.node() else {
                let Some((batches, projected)) = build_batch_input(Arc::clone(&sorted), execution.child(0), None, BatchOutput::ArrowPages).await? else { return Ok(None); };
                let gaps = crate::patch::BatchPatchGaps::new(batches, sorted.schema(),
                    [*iid, *valid_from, *valid_to, *document], from, to, execution)?;
                return Ok(Some((Box::new(gaps), projected)));
            };
            let child = sorted.child(0)?;
            let schema = child.schema().clone();
            let sort_execution = execution.child(0).in_operator();
            let Some((batches, projected)) = build_batch_input(child, sort_execution.child(0), None, output).await? else { return Ok(None); };
            let batches: BoxBatchCursor = if order_by.is_empty() {
                batches
            } else if BatchSort::supports(&schema, order_by) {
                Box::new(BatchSort::new(batches, schema, order_by.clone(), sort_execution.clone(), projected, 0, i64::MAX as u64)?.preserving_pages())
            } else {
                let positions = (0..schema.len()).collect();
                let memory = Arc::clone(&sort_execution.context.memory);
                let Some((input, schema, order)) = project_sort_keys(batches, schema, order_by, &sort_execution)? else { return Ok(None); };
                let sort = BatchSort::new(input, schema, order, sort_execution.clone(), true, 0, i64::MAX as u64)?.preserving_pages();
                Box::new(crate::columnar::BatchProject::new(Box::new(sort), positions, memory))
            };
            let batches = crate::analysis::wrap_batches(batches, sort_execution.analysis, &sort_execution.context.memory);
            let batches = crate::trace::wrap_batches(batches, sorted.relation(), &sort_execution.context);
            let gaps = crate::patch::BatchPatchGaps::new(batches, sorted.schema(),
                [*iid, *valid_from, *valid_to, *document], from, to, execution)?;
            Ok(Some((Box::new(gaps), projected)))
        }
        RelationNode::Join { kind: rtdb_plan::JoinKind::Cross, .. } => {
            let Some((left, left_projected)) = build_batch_input(runtime.child(0)?, execution.child(0), pushdowns.clone(), output).await? else { return Ok(None); };
            let Some((right, right_projected)) = build_batch_input(runtime.child(1)?, execution.child(1), pushdowns, output).await? else { return Ok(None); };
            let join = crate::cross::CrossBatchJoin::new(left, right, execution.context);
            Ok(Some((Box::new(join), left_projected || right_projected)))
        }
        RelationNode::Join { left, right, kind, condition } => {
            let (Some(build_projected),Some(probe_projected)) = (batch_shape(&runtime.child(0)?, output)?,batch_shape(&runtime.child(1)?, output)?) else { return Ok(None) };
            let swapped = !crate::join::build_left(left, right, *kind, &execution)?;
            let (build_side,probe_side,build_relation,probe_relation) = if swapped { (1,0,right,left) } else { (0,1,left,right) };
            let mut layout = crate::join_key::JoinLayout::new(build_relation,probe_relation,condition.as_ref(),&execution.context.memory)?;
            layout.resolve_types(runtime.child(build_side)?.schema(),runtime.child(probe_side)?.schema(),&execution)?;
            let Some(order) = crate::batch_join::key_order(&layout) else { return Ok(None) };
            let (theta, residual) = match crate::batch_join::theta_tests(&layout) {
                Some(theta) => (theta, None),
                None => (Vec::new(), Some(crate::batch_join::Residual::new(&layout, &execution)?)),
            };
            let Some((build,projected)) = build_batch_input(runtime.child(build_side)?,execution.child(build_side),pushdowns.clone(),output).await? else { return Ok(None) };
            let probe = crate::batch_join::ProbeInput { runtime: runtime.child(probe_side)?,execution: execution.child(probe_side),pushdowns, output };
            let original_layout_charge = if swapped {
                let mut layout = crate::join_key::JoinLayout::new(left,right,condition.as_ref(),&execution.context.memory)?;
                layout.resolve_types(runtime.child(0)?.schema(),runtime.child(1)?.schema(),&execution)?;
                Some(layout.into_reservation())
            } else { None };
            let probe_projected = if swapped { build_projected } else { probe_projected };
            let join = crate::batch_join::BatchJoin::new(build, probe, layout, order, [projected, probe_projected], execution)
                .swapped(original_layout_charge).with_kind(*kind)
                .with_theta(theta).with_residual(residual);
            Ok(Some((Box::new(join) as crate::columnar::BoxBatchCursor, projected || probe_projected)))
        }
    }
}

/// Opens a plan as Arrow batches, including the charge for the root operator's state.
pub(crate) async fn build_columns(runtime: Arc<EmittedRelation>, execution: Execution) -> Result<Option<crate::columnar::BoxBatchCursor>> {
    build_column_output(runtime, execution, BatchOutput::Arrow).await
}

pub(crate) async fn build_column_pages(runtime: Arc<EmittedRelation>, execution: Execution) -> Result<Option<crate::columnar::BoxBatchCursor>> {
    build_column_output(runtime, execution, BatchOutput::ArrowPages).await
}

async fn build_column_output(runtime: Arc<EmittedRelation>, execution: Execution, output: BatchOutput) -> Result<Option<crate::columnar::BoxBatchCursor>> {
    execution.context.check_cancelled()?;
    if batch_shape(&runtime, output)?.is_none() { return Ok(None); }
    Ok(build_batch_input_with_state(runtime, execution, None, output, true).await?.map(|(cursor, _)| cursor))
}

async fn build_materialized_columns(runtime: &Arc<EmittedRelation>, binding: &Arc<rtdb_plan::MaterializedBinding>,
    execution: Execution, output: BatchOutput) -> Result<Option<(crate::columnar::BoxBatchCursor, bool)>> {
    let body = runtime.child(1)?;
    let bound = runtime.child(0)?;
    if batch_shape(&body, output)?.is_none() || batch_shape(&bound, output)?.is_none() {
        return Ok(None);
    }
    let reservation = state_reservation(runtime, &execution)?;
    let schema = bound.schema().clone();
    let Some((input, _)) = build_batch_input(bound, execution.child(0), None, output).await? else { return Ok(None); };
    let context = &execution.context;
    let scope = crate::materialized::BindingScope::columns(Arc::clone(binding), input, schema, execution.bindings.clone(), Arc::clone(context))?;
    let mut body_execution = execution.child(1);
    body_execution.bindings = Some(Arc::clone(&scope));
    let (body, projected) = build_batch_input(body, body_execution, None, output).await?
        .ok_or_else(|| invalid("materialized-binding", "Materialized body cannot open as Arrow"))?;
    let cursor = Box::new(crate::materialized::ColumnLetCursor { body, _binding: scope });
    let cursor = crate::columnar::retain_batch_state(cursor, reservation);
    Ok(Some((cursor, projected)))
}

fn passthrough(expressions: &[NamedExpr], schema: &Schema, positions: &[usize]) -> Option<Vec<usize>> {
    positions.iter().map(|&position| match &expressions.get(position)?.expr {
        Expr::Column { id, .. } => schema.position(*id),
        _ => None,
    }).collect()
}

/// Opens an ordered result as batches when the outer projections only select columns.
pub(crate) fn build_ordered_columns(
    runtime: Arc<EmittedRelation>,
    execution: Execution,
    projection: Vec<usize>,
) -> futures::future::BoxFuture<'static, Result<Option<crate::columnar::BoxBatchCursor>>> {
    let mut build = Box::pin(async move {
        let already_in_operator = execution.context.operator_root.is_some();
        let execution = execution.in_operator();
        let input = match runtime.relation().node() {
            RelationNode::SortLimit { order_by, offset, fetch: Some(fetch), .. } if !order_by.is_empty() => {
                let Some((offset, remaining)) = ordered_window(offset.as_ref(), fetch, &execution) else {
                    return Ok(None);
                };
                let limit = sort_prefix_len(offset, Some(remaining));
                let (Ok(limit), Ok(offset)) = (usize::try_from(limit), usize::try_from(offset)) else { return Ok(None); };
                let Some(input) = ordered_input(runtime.child(0)?, execution.child(0), limit, offset, projection).await? else {
                    return Ok(None);
                };
                input
            }
            RelationNode::Project { expressions, .. } | RelationNode::Rename { expressions, .. } => {
                let child = runtime.child(0)?;
                let Some(positions) = passthrough(expressions, child.schema(), &projection) else { return Ok(None); };
                let Some(input) = build_ordered_columns(child, execution.child(0), positions).await? else { return Ok(None); };
                input
            }
            _ => return Ok(None),
        };
        let input = if already_in_operator { input } else { observe_ordered(input, &runtime, &execution)? };
        Ok(Some(input))
    });
    Box::pin(std::future::poll_fn(move |context| {
        stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || build.as_mut().poll(context))
    }))
}

fn state_reservation(runtime: &Arc<EmittedRelation>, execution: &Execution) -> Result<Arc<rtdb_columnar::MemoryReservation>> {
    let relation = runtime.relation();
    let mut state_bound = crate::runtime_schema::local_allocation_bound(relation)?;
    state_bound = state_bound.checked_add(crate::runtime_schema::schema_bound(runtime.schema())?).ok_or_else(||invalid("operator-size","Operator schema copies exceed addressable memory"))?;
    for (index,_) in relation.children().iter().enumerate() { state_bound = state_bound.checked_add(crate::runtime_schema::schema_bound(runtime.child(index)?.schema())?).ok_or_else(|| invalid("operator-size", "Operator schema copies exceed addressable memory"))?; }
    execution.context.memory.reserve(state_bound)
}

/// Whether the subtree opens as batches, and if so whether they pass through a projection.
fn batch_shape(runtime: &Arc<EmittedRelation>, output: BatchOutput) -> Result<Option<bool>> {
    stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || batch_shape_at(runtime, output))
}

fn project_sort_keys(
    input: crate::columnar::BoxBatchCursor, schema: Schema, order_by: &[rtdb_plan::SortExpr], execution: &Execution,
) -> Result<Option<(crate::columnar::BoxBatchCursor, Schema, Vec<rtdb_plan::SortExpr>)>> {
    let scope = execution.scope()?;
    let mut columns = schema.fields().iter().map(rtdb_plan::Field::expression).collect::<Vec<_>>();
    let mut fields = schema.fields().to_vec();
    let mut order = Vec::with_capacity(order_by.len());
    let mut max_id = fields.iter().map(|field| field.id.0).max().unwrap_or(0);
    for key in order_by {
        if matches!(key.expr, Expr::Column { .. }) {
            order.push(key.clone());
            continue;
        }
        let ty = execution.expression_type(&scope, &key.expr, &schema)?;
        let id = max_id.checked_add(1).or_else(|| (0..u32::MAX).find(|id| !fields.iter().any(|field| field.id.0 == *id)))
            .ok_or_else(|| invalid("sort-key", "Sort key column ID exceeds its range"))?;
        max_id = max_id.max(id);
        let field = rtdb_plan::Field { id: rtdb_plan::ColumnId(id), name: format!("sort_key_{id}"),
            qualifier: None, qualified_only: false, unnamed: true, data_type: ty };
        order.push(rtdb_plan::SortExpr { expr: field.expression(), descending: key.descending, nulls_first: key.nulls_first });
        fields.push(field);
        columns.push(key.expr.clone());
    }
    let output = Schema::new(fields)?;
    let input = Box::new(crate::columnar::BatchExpressions::new(input, schema, columns, execution.clone()).validating_input(true));
    Ok(Some((input, output, order)))
}

fn batch_shape_at(runtime: &Arc<EmittedRelation>, output: BatchOutput) -> Result<Option<bool>> {
    let relation = Arc::clone(runtime.relation());
    Ok(match relation.node() {
        RelationNode::Scan(_) => Some(false),
        RelationNode::Let { .. } => {
            if batch_shape(&runtime.child(0)?, output)?.is_none() { return Ok(None); }
            batch_shape(&runtime.child(1)?, output)?
        }
        RelationNode::Reference { .. } => Some(false),
        RelationNode::Values { .. } | RelationNode::LiteralPages { .. } | RelationNode::ArrowLiteralPages { .. } | RelationNode::List { .. } => Some(false),
        RelationNode::ColumnTable { .. } => Some(false),
        RelationNode::Filter { .. } => {
            let mut child = runtime.child(0)?;
            while matches!(child.relation().node(), RelationNode::Filter { .. }) {
                child = child.child(0)?;
            }
            batch_shape(&child, output)?
        }
        RelationNode::Project { .. } | RelationNode::Rename { .. } => {
            let child = runtime.child(0)?;
            batch_shape(&child, output)?.map(|_| true)
        }
        RelationNode::Extend { .. } => {
            let child = runtime.child(0)?;
            batch_shape(&child, output)?.map(|_| true)
        }
        RelationNode::Set { operator, .. } => {
            batch_shape(&runtime.child(0)?, output)?.zip(batch_shape(&runtime.child(1)?, output)?)
                .map(|(left, right)| left || (*operator == SetOperator::Union && right))
        }
        RelationNode::Distinct { .. } => batch_shape(&runtime.child(0)?, output)?,
        RelationNode::Aggregate { .. } => batch_shape(&runtime.child(0)?, output)?.map(|_| false),
        RelationNode::Mark { arguments, .. } if !arguments.is_empty() =>
            batch_shape(&runtime.child(0)?, output)?,
        RelationNode::Mark { .. } => {
            if batch_shape(&runtime.child(1)?, output)?.is_none() { return Ok(None); }
            batch_shape(&runtime.child(0)?, output)?
        }
        RelationNode::Apply { .. } =>
            batch_shape(&runtime.child(0)?, output)?,
        RelationNode::PatchGaps { .. } => batch_shape(&runtime.child(0)?, output)?,
        RelationNode::Join { kind: rtdb_plan::JoinKind::Cross, .. } => {
            batch_shape(&runtime.child(0)?, output)?.zip(batch_shape(&runtime.child(1)?, output)?).map(|(left, right)| left || right)
        }
        RelationNode::Join { .. } => {
            batch_shape(&runtime.child(0)?, output)?.zip(batch_shape(&runtime.child(1)?, output)?).map(|(left, right)| left || right)
        }
        RelationNode::Limit { .. } => batch_shape(&runtime.child(0)?, output)?,
        RelationNode::Unnest { .. } => batch_shape(&runtime.child(0)?, output)?,
        RelationNode::Ordinality { .. } => batch_shape(&runtime.child(0)?, output)?,
        RelationNode::Window { partition_by, order_by, expressions, .. } => {
            let child = runtime.child(0)?;
            if !crate::window::BatchWindow::supports(child.schema(), partition_by, &[], expressions) { return Ok(None); }
            let computed = !order_by.is_empty() && !crate::sort::batches::BatchSort::supports(child.schema(), order_by);
            batch_shape(&child, output)?.map(|projected| projected || computed)
        }
        RelationNode::Sort { order_by, .. } | RelationNode::SortLimit { order_by, .. } => {
            let child = runtime.child(0)?;
            let computed = !order_by.is_empty() && !crate::sort::batches::BatchSort::supports(child.schema(), order_by);
            batch_shape(&child, output)?.map(|projected| projected || computed)
        }
    })
}

async fn open_catalog_scan(scan: &rtdb_plan::Scan, runtime: &Arc<EmittedRelation>, execution: Execution) -> Result<crate::columnar::BoxBatchCursor> {
    let execution = execution.in_operator();
    execution.context.check_cancelled()?;
    let reservation = state_reservation(runtime, &execution)?;
    let analysis = execution.analysis.clone();
    let context = Arc::clone(&execution.context);
    let mut constraints = crate::inspection::Constraints::default();
    for predicate in &scan.predicates { constraints.add(predicate); }
    let input: crate::columnar::BoxBatchCursor = if crate::inspection::is_inspection(&scan.table.name.table) {
        Box::new(crate::inspection::InspectionCursor::new(scan.clone(), execution.clone(), &constraints).await?)
    } else {
        Box::new(crate::system::SystemCursor::new(scan.clone(), execution.clone()))
    };
    let batches: crate::columnar::BoxBatchCursor = match scan.predicates.iter().cloned().reduce(|left, right| Expr::binary(rtdb_plan::BinaryOp::And, left, right)) {
        Some(predicate) => Box::new(crate::columnar::BatchFilter::new(input, runtime.schema().clone(), predicate, execution)),
        None => input,
    };
    let batches = crate::columnar::retain_batch_state(batches, reservation);
    Ok(crate::trace::wrap_batches(crate::analysis::wrap_batches(batches, analysis, &context.memory), runtime.relation(), &context))
}

/// A valid literal or bound parameter can size a seek before the normal input opens.
fn ordered_window(offset: Option<&Expr>, fetch: &Expr, execution: &Execution) -> Option<(u64, u64)> {
    let bound = |expr: &Expr| {
        let value = match expr {
            Expr::Literal(value) => integer(value),
            Expr::Parameter { index, .. } => {
                let parameters = execution.parameters.evaluator();
                parameters.known(*index).and_then(integer)
                    .or_else(|| parameters.arrow_cell(*index)?.signed_integer().map(i128::from))
            }
            _ => None,
        };
        value.and_then(checked_row_count)
    };
    Some((match offset { Some(expr) => bound(expr)?, None => 0 }, bound(fetch)?))
}

async fn row_limit(expr: Option<&Expr>, execution: &Execution) -> Result<Option<u64>> {
    let Some(expr) = expr else { return Ok(None); };
    let scope = execution.scope()?;
    let value = execution.evaluate(&scope, expr, &Schema::default(), &[]).await?;
    if let Some(count) = integer(&value).and_then(checked_row_count) { return Ok(Some(count)); }
    let description = match value {
        Value::Null | Value::Undefined => "null".to_owned(),
        Value::Utf8(value) => value,
        Value::Bool(value) => value.to_string(),
        other => integer(&other).map(|value|value.to_string())
            .or_else(||rtdb_expr::number(&other).map(|value|value.to_string()))
            .unwrap_or_else(||format!("{:?}",other.logical_type())),
    };
    Err(rtdb_types::Anomaly::incorrect("xtdb/expected-number", format!("Expected: number, got: {description}")))
}

async fn materialize_columns(
    rows: &[Vec<Expr>], width: usize, execution: &Execution,
) -> Result<Option<crate::columnar::ColumnBatch>> {
    use crate::columnar::{ColumnBatch, pack_evaluated_column};

    if rows.is_empty() { return Ok(None); }
    let memory = &execution.context.memory;
    let size = || invalid("table-size", "Table column allocation overflow");
    let _inventory = memory.reserve(width.checked_mul(256).and_then(|size| size.checked_add(4096)).ok_or_else(size)?)?;
    let bytes = rows.len().checked_mul(std::mem::size_of::<Value>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size)?;
    let mut columns = (0..width).map(|_| {
        let reservation = memory.reserve(bytes)?;
        Ok((Vec::with_capacity(rows.len()), reservation))
    }).collect::<Result<Vec<_>>>()?;
    let input = Schema::default();
    for (index, expressions) in rows.iter().enumerate() {
        execution.context.check_cancelled_at(index)?;
        if expressions.len() != width { return Err(invalid("table-width", "Table row does not match its declared width")); }
        let scope = execution.scope()?;
        for (expression, (column, reservation)) in expressions.iter().zip(&mut columns) {
            let value = execution.evaluate(&scope, expression, &input, &[]).await?;
            let lease = Arc::get_mut(reservation).expect("table evaluation owns its column");
            lease.grow_to(lease.bytes().checked_add(value.retained_bytes()?).ok_or_else(size)?)?;
            column.push(value);
        }
    }
    let mut output = ColumnBatch::settled(Vec::new(), rows.len(), memory.reserve(256)?)?;
    for (values, scratch) in columns {
        let (column, reservation) = pack_evaluated_column(&values, memory)?;
        drop((values, scratch));
        output.append_column(column, reservation, memory)?;
    }
    Ok(Some(output))
}

struct ArrowLiteralCursor { relation: Arc<Relation>, next: usize, execution: Execution }

#[async_trait]
impl crate::columnar::BatchCursor for ArrowLiteralCursor {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        use crate::columnar::{Column, ColumnBatch};
        self.execution.context.check_cancelled()?;
        let RelationNode::ArrowLiteralPages { pages } = self.relation.node() else { return Err(invalid("page-source", "Expected Arrow literal pages")); };
        let Some(page) = pages.pages().get(self.next) else { return Ok(None); };
        self.next += 1;
        let batch = page.batch();
        let bytes = batch.num_columns().checked_mul(512).and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(|| invalid("table-size", "Table column allocation overflow"))?;
        let reservation = self.execution.context.memory.reserve(bytes)?;
        let columns = batch.columns().iter().zip(batch.schema_ref().fields()).map(|(array, field)| {
            Column::Arrow { array: Arc::clone(array), field: Arc::clone(field), logical: None }
        }).collect();
        Ok(Some(ColumnBatch::sharing(columns, batch.num_rows(), reservation)))
    }
}

struct LiteralBatchCursor { relation: Arc<Relation>, width: usize, next: usize, execution: Execution }

#[async_trait]
impl crate::columnar::BatchCursor for LiteralBatchCursor {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        let RelationNode::LiteralPages { pages } = self.relation.node() else { return Err(invalid("page-source", "Expected literal pages")); };
        let Some(rows) = pages.get(self.next) else { return Ok(None); };
        self.next += 1;
        let batch = materialize_columns(rows, self.width, &self.execution).await?;
        if batch.is_some() { return Ok(batch); }
        let columns = (0..self.width).map(|_| crate::columnar::Column::Values(Vec::new())).collect();
        crate::columnar::ColumnBatch::settled(columns, 0, self.execution.context.memory.reserve(self.width.saturating_mul(256).saturating_add(256))?).map(Some)
    }
}

fn observe_ordered(input: crate::columnar::BoxBatchCursor, runtime: &Arc<EmittedRelation>, execution: &Execution) -> Result<crate::columnar::BoxBatchCursor> {
    let reservation = state_reservation(runtime, execution)?;
    let context = &execution.context;
    let input = crate::columnar::retain_batch_state(input, reservation);
    let input = crate::analysis::wrap_batches(input, execution.analysis.clone(), &context.memory);
    Ok(crate::trace::wrap_batches(input, runtime.relation(), context))
}

fn ordered_input(
    runtime: Arc<EmittedRelation>,
    execution: Execution,
    limit: usize,
    offset: usize,
    projection: Vec<usize>,
) -> futures::future::BoxFuture<'static, Result<Option<crate::columnar::BoxBatchCursor>>> {
    let mut build = Box::pin(async move {
        let execution = execution.in_operator();
        let input = match runtime.relation().node() {
            RelationNode::Scan(scan) => {
                let Some(input) = crate::scan::ordered_input(scan, &execution, limit, offset, &projection).await? else { return Ok(None); };
                input
            }
            RelationNode::Project { expressions, .. } | RelationNode::Rename { expressions, .. } => {
                let child = runtime.child(0)?;
                let Some(positions) = passthrough(expressions, child.schema(), &projection) else { return Ok(None); };
                let Some(input) = ordered_input(child, execution.child(0), limit, offset, positions).await? else { return Ok(None); };
                input
            }
            RelationNode::Filter { .. } | RelationNode::Distinct { .. } => {
                let Some(input) = ordered_input(runtime.child(0)?, execution.child(0), limit, offset, projection).await? else { return Ok(None); };
                input
            }
            _ => return Ok(None),
        };
        observe_ordered(input, &runtime, &execution).map(Some)
    });
    Box::pin(std::future::poll_fn(move |context| {
        stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || build.as_mut().poll(context))
    }))
}

struct BatchUnion { left: Option<crate::columnar::BoxBatchCursor>, right: crate::columnar::BoxBatchCursor }

struct BatchLimit {
    input: Option<crate::columnar::BoxBatchCursor>,
    offset: u64,
    remaining: Option<u64>,
    context: Arc<crate::QueryContext>,
}

#[async_trait]
impl crate::columnar::BatchCursor for BatchLimit {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        loop {
            self.context.check_cancelled()?;
            if self.remaining == Some(0) && self.offset == 0 { self.input = None; }
            let Some(input) = &mut self.input else { return Ok(None); };
            if self.offset > 0 {
                self.offset -= input.skip_complete_pages(self.offset)?;
                if self.offset == 0 && self.remaining == Some(0) { self.input = None; return Ok(None); }
            }
            let Some(batch) = crate::columnar::poll_nested_batch(input.as_mut()).await? else { self.input = None; return Ok(None); };
            let skipped = self.offset.min(batch.len as u64) as usize;
            self.offset -= skipped as u64;
            let count = self.remaining.unwrap_or(u64::MAX).min((batch.len - skipped) as u64) as usize;
            if count == 0 { continue; }
            if let Some(remaining) = &mut self.remaining {
                *remaining -= count as u64;
                if *remaining == 0 { self.input = None; }
            }
            return batch.slice(skipped, count, &self.context.memory).map(Some);
        }
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        if let Some(input) = &self.input { input.trace_attributes(span); }
    }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> {
        self.input.as_ref().and_then(|input| input.scan_counts())
    }
}

#[async_trait]
impl crate::columnar::BatchCursor for BatchUnion {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        if let Some(left) = &mut self.left {
            while let Some(batch) = left.next_batch().await? {
                if batch.len != 0 { return Ok(Some(batch)); }
            }
        }
        self.left = None;
        while let Some(batch) = self.right.next_batch().await? {
            if batch.len != 0 { return Ok(Some(batch)); }
        }
        Ok(None)
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        span.attribute("union_all_columnar", rtdb_plan::TraceValue::Boolean(true));
    }
}

struct BatchDistinct {
    input: crate::columnar::BoxBatchCursor,
    seen: crate::set_keys::BatchKeyIndex,
    schema: Arc<RuntimeSchema>,
    execution: Execution,
    projected: bool,
}

impl BatchDistinct {
    fn new(input: crate::columnar::BoxBatchCursor, schema: Arc<RuntimeSchema>, execution: Execution, projected: bool) -> Result<Self> {
        let seen = crate::set_keys::BatchKeyIndex::new(&execution.context)?;
        Ok(Self { input, seen, schema, execution, projected })
    }
}

#[async_trait]
impl crate::columnar::BatchCursor for BatchDistinct {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        while let Some(batch) = crate::columnar::poll_nested_batch(self.input.as_mut()).await? {
            if batch.len == 0 { continue; }
            let context = &self.execution.context;
            let memory = &context.memory;
            let _mask = memory.reserve(batch.len.saturating_add(256))?;
            let mut mask = Vec::with_capacity(batch.len);
            self.seen.distinct(&batch, self.projected, &mut mask, context, self.schema.schema(), |_| Ok(true))?;
            if mask.iter().any(|keep| *keep) { return batch.select(&mask, memory).map(Some); }
        }
        Ok(None)
    }
}

pub(crate) fn has_uri(value: &Value) -> bool {
    match value {
        Value::Uri(_) => true,
        Value::Set(values) => values.iter().any(has_uri),
        Value::List(values) | Value::FixedSizeList(values) => matches!(values.view(), rtdb_types::ListView::Values(values) if values.iter().any(has_uri)),
        Value::Struct(fields) => fields.values().any(has_uri),
        Value::Map { entries, .. } => entries.iter().any(|(key,value)| has_uri(key) || has_uri(value)),
        _ => false,
    }
}

pub(crate) fn uri_hashes_match(left: &[Value], right: &[Value], memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<bool> {
    if !left.iter().chain(right).any(has_uri) { return Ok(true); }
    let _scratch = memory.reserve(4096)?;
    Ok(crate::join_storage::hash_values(left)? == crate::join_storage::hash_values(right)?)
}

pub(crate) fn rows_equal(left: &[Value], right: &[Value], timezone: &str, memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<bool> {
    if left.len() != right.len() || !uri_hashes_match(left,right,memory)? { return Ok(false); }
    for (left, right) in left.iter().zip(right) {
        let allocation = rtdb_expr::EvaluationScope::new(memory)?;
        if !null_eq_in_timezone(left, right, timezone, &allocation)? { return Ok(false); }
    }
    Ok(true)
}
/// A hash that rows equal under `rows_equal` share.
/// A date or timestamp equals one of another unit or zone once the session zone localizes it, so it hashes as the
/// wall-clock time it reads as there; date-times nested in a container hash alike, and `rows_equal` tells them apart.
pub(crate) fn equality_hash(values: &[Value], timezone: &str) -> u32 {
    values.iter().fold(0, |hash, value| combine_equality_hash(hash, equality_cell_hash(value, timezone)))
}

pub(crate) fn equality_cell_hash(value: &Value, timezone: &str) -> u32 {
    fn temporal(value: &Value) -> bool {
        match value {
            Value::Timestamp { .. } | Value::DateDay(_) | Value::DateMilli(_) | Value::Date(_) | Value::TimestampTzRange { .. } => true,
            Value::Set(values) => values.iter().any(temporal),
            Value::List(values) | Value::FixedSizeList(values) => match values.view() {
                rtdb_types::ListView::Values(values) => values.iter().any(temporal),
                rtdb_types::ListView::Scalars(_) => values.element_type().legs().iter().any(|leg|
                    matches!(leg, LogicalType::Scalar(rtdb_types::ScalarType::Timestamp { .. }
                        | rtdb_types::ScalarType::DateDay | rtdb_types::ScalarType::DateMilli))),
                _ => false,
            },
            Value::Struct(fields) => fields.values().any(temporal),
            Value::Map { entries, .. } => entries.iter().any(|(key, value)| temporal(key) || temporal(value)),
            _ => false,
        }
    }
    match value {
            Value::Timestamp { .. } | Value::DateDay(_) | Value::DateMilli(_) => rtdb_expr::local_micros(value, timezone)
                .and_then(|micros| rtdb_columnar::hash_value(&Value::I64(micros), 0).ok()).map_or(0x7e3a_11c5, |cell| (cell as u32) ^ 0x7e3a_11c5),
            value if temporal(value) => 0x7e3a_11c5,
            value => rtdb_columnar::hash_value(value, 0).map_or(0x5bd1_e995, |cell| cell as u32),
    }
}

pub(crate) fn combine_equality_hash(hash: u32, cell: u32) -> u32 {
    (hash ^ cell.wrapping_mul(0xcc9e_2d51).rotate_left(15)).rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64)
}

pub(crate) fn validate_distinct_types(schema: &Schema) -> Result<()> {
    for field in schema.fields() { rtdb_plan::validate_equality_types(&field.data_type, &field.data_type)?; }
    Ok(())
}
fn validate_set_types(runtime: &EmittedRelation) -> Result<()> {
    let left = runtime.child(0)?;
    let right = runtime.child(1)?;
    for (left, right) in left.schema().fields().iter().zip(right.schema().fields()) {
        rtdb_plan::validate_equality_types(&left.data_type, &right.data_type)?;
    }
    Ok(())
}

struct BatchSet {
    left: crate::columnar::BoxBatchCursor,
    right: Option<(crate::columnar::BoxBatchCursor, bool)>,
    left_projected: bool,
    operator: SetOperator,
    all: bool,
    seen: crate::set_keys::BatchKeyIndex,
    built: Option<crate::set_keys::BatchKeyIndex>,
    runtime: Arc<EmittedRelation>,
    execution: Execution,
}

#[async_trait]
impl crate::columnar::BatchCursor for BatchSet {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        let context = &self.execution.context;
        let memory = &context.memory;
        if let Some((mut right, projected)) = self.right.take() {
            let mut built = crate::set_keys::BatchKeyIndex::new(context)?;
            while let Some(batch) = crate::columnar::poll_nested_batch(right.as_mut()).await? {
                built.build(batch, projected, context)?;
            }
            self.built = Some(built);
        }
        while let Some(batch) = crate::columnar::poll_nested_batch(self.left.as_mut()).await? {
            if batch.len == 0 { continue; }
            validate_set_types(&self.runtime)?;
            let _mask = memory.reserve(batch.len.saturating_add(256))?;
            let mut mask = Vec::with_capacity(batch.len);
            let built = self.built.as_mut().ok_or_else(|| invalid("set-state", "Set input was not built"))?;
            let left_projected = self.left_projected;
            let mut retain = |row| {
                let matched = built.matches(&batch, row, left_projected, self.all, context)?;
                Ok(matched == (self.operator == SetOperator::Intersect))
            };
            if self.all {
                for row in 0..batch.len {
                    context.check_cancelled_at(row)?;
                    mask.push(retain(row)?);
                }
            } else {
                self.seen.distinct(&batch, left_projected, &mut mask, context, self.runtime.child(0)?.schema(), retain)?;
            }
            if mask.iter().any(|keep| *keep) { return batch.select(&mask, memory).map(Some); }
        }
        Ok(None)
    }
}

struct BatchUnnest {
    input: crate::columnar::BoxBatchCursor,
    position: Option<usize>,
    expr: Expr,
    schema: Schema,
    projected: bool,
    ordinality: Option<String>,
    outer: bool,
    execution: Execution,
}

#[async_trait]
impl crate::columnar::BatchCursor for BatchUnnest {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        let context = Arc::clone(&self.execution.context);
        while let Some(batch) = crate::columnar::poll_nested_batch(self.input.as_mut()).await? {
            if let Some(position) = self.position
                && let Some(output) = batch.expand_list(position, self.ordinality.as_deref(), self.outer, &context)?
            {
                if output.len != 0 { return Ok(Some(output)); }
                continue;
            }
            if let Some(output) = self.expand_expression(&batch).await? {
                return Ok(Some(output));
            }
        }
        Ok(None)
    }
}


impl BatchUnnest {
    async fn expand_expression(&mut self, batch: &crate::columnar::ColumnBatch) -> Result<Option<crate::columnar::ColumnBatch>> {
        use arrow_array::{Array, Int32Array};
        use arrow_schema::{DataType, Field};
        use crate::columnar::{BatchRow, Column, ColumnBatch, gather_column_rows, gather_nullable_column};

        let context = &self.execution.context;
        let memory = &context.memory;
        let too_large = || invalid("unnest-size", "Unnest output exceeds addressable memory");
        let scope = self.execution.scope()?;
        let invocation = self.execution.prepare_expression(&self.expr, &self.schema, &scope)?;
        let mut selection = memory.reserve(256)?;
        let mut parents = Vec::new();
        let mut children = Vec::new();
        let mut picks = Vec::new();
        let mut ordinals = Vec::new();
        for row in 0..batch.len {
            context.check_cancelled_at(row)?;
            let evaluation = self.execution.scope()?;
            let input = BatchRow { batch, row, admit_dates: self.projected };
            let elements = self.execution.evaluate_list_invocation(
                &evaluation, &invocation, rtdb_expr::ExpressionRow::Borrowed(&input),
            ).await?;
            let count = elements.len();
            let appended = if self.outer { count.max(1) } else { count };
            if appended == 0 { continue; }
            let row_bytes = 2 * (std::mem::size_of::<(usize, usize)>() + std::mem::size_of::<Option<(usize, usize)>>()
                + std::mem::size_of::<Option<i32>>());
            let bytes = appended.checked_mul(row_bytes).and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<ColumnBatch>()))
                .ok_or_else(too_large)?;
            let lease = Arc::get_mut(&mut selection).expect("Unnest owns its selection");
            lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(too_large)?)?;
            if count == 0 {
                parents.push((0, row));
                picks.push(None);
                if self.ordinality.is_some() { ordinals.push(None); }
                continue;
            }
            let (column, custody) = if let Some((field, array)) = elements.arrow_column(&evaluation)? {
                let bytes = array.get_array_memory_size().checked_add(field.size()).ok_or_else(too_large)?;
                (Column::Arrow { array, field, logical: None }, memory.reserve(bytes)?)
            } else {
                let values = elements.scalar_values().expect("non-Arrow list has evaluated cells");
                crate::columnar::pack_evaluated_column(values, memory)?
            };
            let source = children.len();
            let mut child = ColumnBatch::settled(Vec::new(), count, memory.reserve(256)?)?;
            child.append_column(column, custody, memory)?;
            children.push(child);
            for index in 0..count {
                context.check_cancelled_at(index)?;
                parents.push((0, row));
                picks.push(Some((source, index)));
                if self.ordinality.is_some() {
                    let ordinal = i32::try_from(index).ok().and_then(|index| index.checked_add(1))
                        .ok_or_else(|| invalid("ordinality-overflow", "List ordinal exceeds signed32 bits"))?;
                    ordinals.push(Some(ordinal));
                }
            }
        }
        if parents.is_empty() { return Ok(None); }
        let mut output = ColumnBatch::settled(Vec::new(), parents.len(), memory.reserve(256)?)?;
        for column in 0..batch.columns.len() {
            let (column, reservation) = gather_column_rows(|_| batch, &parents, column, memory)?;
            output.append_column(column, reservation, memory)?;
        }
        let (elements, custody) = gather_nullable_column(|source| &children[source], &picks, 0, memory)?;
        output.append_column(elements, custody, memory)?;
        if let Some(name) = self.ordinality.as_deref() {
            let bytes = output.len.checked_mul(8).and_then(|bytes| bytes.checked_add(512)).ok_or_else(too_large)?;
            let custody = memory.reserve(bytes)?;
            let array = Arc::new(Int32Array::from(ordinals));
            let field = Arc::new(Field::new(name, DataType::Int32, self.outer));
            output.append_column(Column::Arrow { array, field, logical: None }, custody, memory)?;
        }
        Ok(Some(output))
    }
}

struct BatchApply {
    input: crate::columnar::BoxBatchCursor,
    plan: BatchApplyPlan,
}

struct BatchApplyPlan {
    schema: Schema,
    dependent: Arc<EmittedRelation>,
    arguments: Vec<(usize, Expr)>,
    kind: ApplyKind,
    document_field: Option<rtdb_plan::Field>,
    comparison: Option<(Expr, Schema)>,
    execution: Execution,
}

impl BatchApplyPlan {
    async fn bind(&self, batch: &Arc<crate::columnar::ColumnBatch>, row: usize) -> Result<Execution> {
        self.execution.bind_arrow_arguments(&self.arguments, &self.schema, Arc::clone(batch), row).await
    }

    async fn dependent_batches(&self, batch: &Arc<crate::columnar::ColumnBatch>, row: usize) -> Result<crate::columnar::BoxBatchCursor> {
        self.open_dependent(self.bind(batch, row).await?).await
    }

    async fn open_dependent(&self, execution: Execution) -> Result<crate::columnar::BoxBatchCursor> {
        let execution = execution.child(1);
        let runtime = Arc::clone(&self.dependent);
        let input = if matches!(self.kind, ApplyKind::Inner | ApplyKind::Left | ApplyKind::PullMany) {
            build_columns(runtime, execution).await?
        } else {
            build_column_pages(runtime, execution).await?
        };
        input.ok_or_else(|| invalid("apply-input", "Dependent plan cannot open as Arrow"))
    }

    async fn documents(&self, batch: Arc<crate::columnar::ColumnBatch>, output: &rtdb_plan::Field) -> Result<crate::columnar::ColumnBatch> {
        use arrow_array::{ArrayRef, StructArray};
        use crate::columnar::{Column, ColumnBatch, gather_nullable_column, poll_nested_batch};

        let context = &self.execution.context;
        let memory = &context.memory;
        let many = self.kind == ApplyKind::PullMany;
        let fields = self.dependent.schema().fields();
        let width = fields.len().checked_mul(256).ok_or_else(|| invalid("row-size", "Apply document fields overflow"))?;
        let _fields = memory.reserve(width)?;
        let order: BTreeMap<_, _> = fields.iter().enumerate().map(|(index, field)| (field.name.as_str(), index)).collect();
        let positions = order.values().copied().collect::<Vec<_>>();
        let schema = Schema::new(positions.iter().map(|&position| fields[position].clone()).collect())?;
        let mut sources = Vec::new();
        let mut picks = Vec::new();
        let mut offsets = Vec::with_capacity(batch.len + 1);
        let mut valid = Vec::with_capacity(batch.len);
        let mut reservation = memory.reserve(apply_join_bytes(batch.len, 0)?)?;
        offsets.push(0i32);
        for row in 0..batch.len {
            context.check_cancelled_at(row)?;
            let mut input = self.dependent_batches(&batch, row).await?;
            let before = picks.len();
            while let Some(page) = poll_nested_batch(input.as_mut()).await? {
                if page.len == 0 { continue; }
                page.validate_readable(context)?;
                if !many && (picks.len() != before || page.len > 1) { return Err(crate::cardinality_violation()); }
                let rows = picks.len().checked_add(page.len).filter(|rows| *rows <= i32::MAX as usize)
                    .ok_or_else(|| invalid("row-size", "Apply document count exceeds the Arrow limit"))?;
                let lease = Arc::get_mut(&mut reservation).expect("The apply documents own their result inventory");
                lease.grow_to(apply_join_bytes(rows.max(batch.len), sources.len() + 1)?)?;
                let projection = page.key_projection(&positions, memory)?;
                let count = projection.len;
                let (page, charge) = if let Some(physical) = projection.physical_record(&schema, memory)? {
                    physical
                } else {
                    let output = crate::cursor::column_batch(&schema, memory, projection)?;
                    (output.batch().clone(), Arc::clone(output.reservation()))
                };
                let record: ArrayRef = Arc::new(StructArray::try_new_with_length(page.schema_ref().fields().clone(),
                    page.columns().to_vec(), None, count).map_err(|error| invalid("apply-document", error))?);
                let field = Arc::new(arrow_schema::Field::new("doc", record.data_type().clone(), false));
                let logical = rtdb_columnar::from_field(&field)?;
                let doc = ColumnBatch::settled(vec![Column::Arrow { array: record, field, logical: Some(logical) }], count, charge)?;
                picks.extend((0..count).map(|index| Some((sources.len(), index))));
                sources.push(doc);
            }
            let found = before < picks.len();
            if many {
                offsets.push(picks.len() as i32);
                valid.push(found);
            } else if !found {
                picks.push(None);
            }
        }
        let gathered = gather_nullable_column(|index| &sources[index], &picks, 0, memory)?;
        let (column, charge) = if many {
            let page = ColumnBatch::settled(vec![gathered.0], picks.len(), gathered.1)?;
            page.collect_lists(offsets, valid, output, memory)?
        } else {
            gathered
        };
        let mut batch = Arc::try_unwrap(batch).map_err(|_| invalid("apply-input", "Apply document input remains borrowed"))?;
        batch.append_column(column, charge, memory)?;
        Ok(batch)
    }

    async fn joined(&self, batch: &Arc<crate::columnar::ColumnBatch>) -> Result<Option<crate::columnar::ColumnBatch>> {
        use crate::columnar::{ColumnBatch, gather_nullable_column, poll_nested_batch};
        let context = &self.execution.context;
        let memory = &context.memory;
        let width = self.dependent.schema().len();
        let mut left = Vec::new();
        let mut right = Vec::new();
        let mut sources = Vec::new();
        let mut reservation = memory.reserve(4096)?;
        let single = matches!(self.kind, ApplyKind::Single | ApplyKind::Scalar);
        for source_row in 0..batch.len {
            context.check_cancelled_at(source_row)?;
            let mut cursor = self.dependent_batches(batch, source_row).await?;
            let before = left.len();
            while let Some(output) = poll_nested_batch(cursor.as_mut()).await? {
                if output.len == 0 { continue; }
                output.validate_readable(context)?;
                if single && (output.len > 1 || left.len() != before) { return Err(crate::cardinality_violation()); }
                let rows = left.len().checked_add(output.len).filter(|rows| *rows <= i32::MAX as usize)
                    .ok_or_else(|| invalid("row-size", "Apply output has too many rows"))?;
                let bytes = apply_join_bytes(rows, sources.len() + 1)?;
                Arc::get_mut(&mut reservation).expect("The apply join owns its result inventory").grow_to(bytes)?;
                left.extend(std::iter::repeat_n((0, source_row), output.len));
                right.extend((0..output.len).map(|row| Some((sources.len(), row))));
                sources.push(output);
            }
            if left.len() == before && (self.kind == ApplyKind::Left || single) {
                let bytes = apply_join_bytes(left.len() + 1, sources.len())?;
                Arc::get_mut(&mut reservation).expect("The apply join owns its result inventory").grow_to(bytes)?;
                left.push((0, source_row));
                right.push(None);
            }
        }
        if left.is_empty() { return Ok(None); }
        let _left_columns = memory.reserve(batch.columns.len().checked_mul(16).ok_or_else(|| invalid("row-size", "Apply width overflow"))?)?;
        let columns = (0..batch.columns.len()).collect::<Vec<_>>();
        let mut output = ColumnBatch::take_rows(&[batch.as_ref()], &left, &columns, memory)?;
        for position in 0..width {
            let (column, charge) = gather_nullable_column(|source| &sources[source], &right, position, memory)?;
            output.append_column(column, charge, memory)?;
        }
        Ok(Some(output))
    }
}

fn apply_join_bytes(rows: usize, sources: usize) -> Result<usize> {
    rows.checked_mul(128).and_then(|bytes| bytes.checked_add(sources.checked_mul(512)?))
        .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("row-size", "Apply output allocation overflow"))
}

#[async_trait]
impl crate::columnar::BatchCursor for BatchApply {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        use crate::columnar::Column;
        let plan = &self.plan;
        let context = &plan.execution.context;
        let memory = &context.memory;
        loop {
            context.check_cancelled()?;
            let Some(batch) = crate::columnar::poll_nested_batch(self.input.as_mut()).await? else { return Ok(None); };
            batch.validate_readable(context)?;
            let batch = Arc::new(batch);
            if let Some(field) = &plan.document_field { return plan.documents(batch, field).await.map(Some); }
            if matches!(plan.kind, ApplyKind::Inner | ApplyKind::Left | ApplyKind::Single | ApplyKind::Scalar) {
                if let Some(output) = plan.joined(&batch).await? { return Ok(Some(output)); }
                continue;
            }
            let bytes = batch.len.checked_mul(128).and_then(|bytes| bytes.checked_add(256))
                .ok_or_else(|| invalid("row-size", "Apply output allocation overflow"))?;
            let reservation = memory.reserve(bytes)?;
            let mut selected = Vec::with_capacity(batch.len);
            let mut marks = Vec::with_capacity(batch.len);
            for position in 0..batch.len {
                context.check_cancelled_at(position)?;
                if let Some((comparison, schema)) = &plan.comparison {
                    let execution = plan.bind(&batch, position).await?;
                    let dependent = plan.open_dependent(execution.clone()).await?;
                    let mark = crate::mark::correlated_batch_mark(&batch, position, dependent, comparison, schema, &execution).await?;
                    marks.push(mark);
                    continue;
                }
                let mut cursor = plan.dependent_batches(&batch, position).await?;
                let mut first = None;
                while let Some(page) = crate::columnar::poll_nested_batch(cursor.as_mut()).await? {
                    if page.len > 0 { page.validate_readable(context)?; first = Some(page); break; }
                }
                match plan.kind {
                    ApplyKind::Semi | ApplyKind::Anti => selected.push(first.is_some() == (plan.kind == ApplyKind::Semi)),
                    ApplyKind::Exists => marks.push(Some(first.is_some())),
                    ApplyKind::Mark => marks.push(apply_batch_mark(&mut cursor, first, &plan.execution).await?),
                    _ => unreachable!("The apply mode was checked"),
                }
            }
            let mut batch = Arc::try_unwrap(batch).map_err(|_| invalid("apply-input", "Apply input remains borrowed"))?;
            if matches!(plan.kind, ApplyKind::Semi | ApplyKind::Anti) {
                if !selected.iter().any(|keep| *keep) { continue; }
                return batch.select(&selected, memory).map(Some);
            }
            let array = Arc::new(arrow_array::BooleanArray::from(marks));
            let ty = LogicalType::Scalar(rtdb_types::ScalarType::Bool);
            let ty = if plan.kind == ApplyKind::Mark { ty.maybe() } else { ty };
            let field = Arc::new(rtdb_columnar::to_field("mark", &ty)?);
            batch.append_column(Column::Arrow { array, field, logical: Some(ty) }, reservation, memory)?;
            return Ok(Some(batch));
        }
    }
}

async fn apply_batch_mark(cursor: &mut crate::columnar::BoxBatchCursor, mut next: Option<crate::columnar::ColumnBatch>, execution: &Execution) -> Result<Option<bool>> {
    use crate::scalar_key::ScalarKey;
    let context = &execution.context;
    let mut mark = Some(false);
    while let Some(page) = next {
        page.validate_readable(context)?;
        for row in 0..page.len {
            context.check_cancelled_at(row)?;
            let column = page.columns.first().ok_or_else(|| invalid("mark-type", "Mark comparison must return Boolean or null"))?;
            match ScalarKey::read(column, row) {
                Some(ScalarKey::Bool(true)) => return Ok(Some(true)),
                Some(ScalarKey::Bool(false)) => continue,
                Some(ScalarKey::Null) => mark = None,
                _ => {
                    let _cell = context.memory.reserve(column.bound(row)?)?;
                    match column.value(row)? {
                        Value::Bool(true) => return Ok(Some(true)),
                        Value::Bool(false) => (),
                        Value::Null | Value::Undefined => mark = None,
                        _ => return Err(invalid("mark-type", "Mark comparison must return Boolean or null")),
                    }
                }
            }
        }
        next = crate::columnar::poll_nested_batch(cursor.as_mut()).await?;
    }
    Ok(mark)
}

#[cfg(test)]
#[path = "page_contract_tests.rs"]
mod page_contract_tests;

#[cfg(test)]
#[path = "project_page_tests.rs"]
mod project_page_tests;

#[cfg(test)]
#[path = "mark_page_tests.rs"]
mod mark_page_tests;

#[cfg(test)]
#[path = "join_page_tests.rs"]
mod join_page_tests;

#[cfg(test)]
mod uri_equality_tests {
    use super::*;

    fn equality_scope() -> rtdb_expr::EvaluationScope {
        rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap()
    }

    #[test]
    fn uri_candidate_hash_scratch_is_reserved_and_released_before_object_equality() {
        let left = [Value::Uri("HTTP://EXAMPLE.com/%ab".into())];
        let right = [Value::Uri("http://example.com/%AB".into())];
        let memory = rtdb_columnar::MemoryBudget::new(4096);
        assert!(rtdb_expr::null_eq(&left[0],&right[0], &equality_scope()).unwrap());
        assert!(!rows_equal(&left,&right,"UTC",&memory).unwrap());
        assert!(rows_equal(&left,&left,"UTC",&memory).unwrap());
        assert_eq!(memory.used_bytes(),0);
        let too_small = rtdb_columnar::MemoryBudget::new(4095);
        assert!(rows_equal(&left,&right,"UTC",&too_small).is_err());
        assert_eq!(too_small.used_bytes(),0);
    }
}

#[cfg(test)]
mod equality_allocation_tests {
    use super::*;
    use rtdb_format::transit::{TransitValue,encode_msgpack};

    #[test]
    fn packed_temporal_lists_share_the_hash_of_equivalent_scalar_lists() {
        let target = rtdb_plan::CastTarget::List(Box::new(rtdb_plan::CastTarget::Fixed(
            LogicalType::Scalar(rtdb_types::ScalarType::DateDay),
        )));
        let source = Value::list(vec![Value::Utf8("1970-01-02".into())]);
        let packed = [rtdb_expr::cast_target(&source, &target, "UTC").unwrap()];
        let scalar = [Value::list(vec![Value::DateDay(1)])];
        assert!(matches!(packed[0].list_view(), Some(rtdb_types::ListView::Scalars(_))));
        assert_eq!(equality_hash(&packed, "UTC"), equality_hash(&scalar, "UTC"));
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        assert!(rows_equal(&packed, &scalar, "UTC", &memory).unwrap());
    }

    #[test]
    fn transit_comparison_scopes_release_between_cells_rows_and_failures() {
        let value = Value::Transit(encode_msgpack(&TransitValue::Integer(7)).unwrap());
        let row = [value.clone(),value];
        let memory = rtdb_columnar::MemoryBudget::new(32 * 1024);
        for _ in 0..64 {
            assert!(rows_equal(&row,&row,"UTC",&memory).unwrap());
            assert_eq!(memory.used_bytes(),0);
        }
        assert!(rows_equal(&[Value::Transit(vec![0xc1])],&row[..1],"UTC",&memory).is_err());
        assert_eq!(memory.used_bytes(),0);
    }
}
