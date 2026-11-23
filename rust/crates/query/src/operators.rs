use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_expr::{integer, is_true, null_eq_in_timezone};
use rtdb_plan::{ApplyKind, Expr, NamedExpr, Relation, RelationNode, Schema, SetOperator};
use rtdb_types::{LogicalType, Result, Value};

use crate::context::Execution;
use crate::cursor::{BoxCursor, OwnedRow};
use crate::limit_bounds::{checked_row_count, sort_prefix_len};
use crate::runtime_schema::{EmittedRelation, RuntimeSchema};
use crate::{invalid, QueryContext};
use crate::pages::{BoxPageCursor,OwnedPage,PageBuilder,PageCursor};

pub(crate) fn build(runtime: Arc<EmittedRelation>, execution: Execution) -> futures::future::BoxFuture<'static, Result<BoxCursor>> {
    build_constrained(runtime, execution, crate::inspection::Constraints::default(), None)
}
pub(crate) fn build_pages(runtime: Arc<EmittedRelation>, execution: Execution) -> futures::future::BoxFuture<'static, Result<BoxPageCursor>> {
    build_pages_constrained(runtime,execution,crate::inspection::Constraints::default(),None)
}

pub(crate) struct DeferredInput {
    runtime: Arc<EmittedRelation>, execution: Execution,
    constraints: crate::inspection::Constraints, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>,
}
impl DeferredInput {
    pub fn new(runtime: Arc<EmittedRelation>, execution: Execution, constraints: crate::inspection::Constraints, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>) -> Self { Self { runtime, execution, constraints, pushdowns } }
    pub async fn open_pages(self, own: Option<Arc<crate::pushdown::Pushdowns>>) -> Result<BoxPageCursor> {
        let pushdowns = crate::pushdown::Pushdowns::merge(self.pushdowns.as_ref(), own, &self.execution.context.memory)?;
        build_pages_constrained(self.runtime, self.execution, self.constraints, pushdowns).await
    }
}

fn build_constrained(runtime: Arc<EmittedRelation>, execution: Execution, constraints: crate::inspection::Constraints, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>) -> futures::future::BoxFuture<'static, Result<BoxCursor>> { Box::pin(async move {
    let context = Arc::clone(&execution.context);
    let pages = build_pages_constrained(runtime, execution, constraints, pushdowns).await?;
    Ok(crate::pages::rows(pages, context))
}) }

/// Builds a scan, its filters and column-only projections as a batch source, or `None` where the subtree has another shape.
/// The boolean reports whether the batches passed through a projection.
pub(crate) fn build_batches(runtime: Arc<EmittedRelation>, execution: Execution, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>) -> futures::future::BoxFuture<'static, Result<Option<(crate::columnar::BoxBatchCursor, bool)>>> {
    build_batch_input(runtime, execution, pushdowns, BatchOutput::Pages)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum BatchOutput { Pages, Arrow }

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
        let already_in_operator = execution.context.operator_root.is_some();
        let execution = execution.in_operator();
        let context = Arc::clone(&execution.context);
        let analysis = execution.analysis.clone();
        let reservation = if reserve_state && !matches!(relation.node(), RelationNode::Let { .. }) {
            Some(state_reservation(&runtime, &execution)?)
        } else { None };
        let Some((mut input, projected)) = build_batches_at(runtime, execution, pushdowns, output).await? else { return Ok(None); };
        if let Some(reservation) = reservation { input = crate::columnar::retain_batch_state(input, reservation); }
        let input = if !already_in_operator && (output == BatchOutput::Arrow || context.operator_root.is_some()) {
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
        RelationNode::Values { rows } if output == BatchOutput::Arrow && ColumnBatch::supports_literal_rows(rows) => {
            match ColumnBatch::from_literal_rows(rows, &execution.context) {
                Ok(batch) => Ok(Some((single_batch(batch, execution.context), false))),
                Err(error) if error.code == "rtdb/memory-budget-exceeded" => Ok(None),
                Err(error) => Err(error),
            }
        }
        RelationNode::Scan(scan) if !crate::inspection::is_inspection(&scan.table.name.table) && crate::system_schema::definition(&scan.table.name.table).is_none() => {
            let batches: crate::columnar::BoxBatchCursor = Box::new(crate::scan::ScanCursor::new(scan.clone(),execution.clone(),pushdowns).await?);
            let batches = match scan.predicates.iter().cloned().reduce(|left,right|Expr::binary(rtdb_plan::BinaryOp::And,left,right)) {
                Some(predicate) => Box::new(crate::columnar::BatchFilter::new(batches,runtime.schema().clone(),predicate,execution)) as crate::columnar::BoxBatchCursor,
                None => batches,
            };
            Ok(Some((batches,false)))
        }
        RelationNode::ColumnTable { expr: Expr::Parameter { index, .. }, .. } if output == BatchOutput::Arrow => {
            Ok(crate::column_table::open_arrow(&execution, *index)?.map(|input| (input, false)))
        }
        RelationNode::Filter { predicate,.. } => {
            let mut input = runtime.child(0)?;
            let mut input_execution = execution.child(0);
            let schema = input.schema().clone();
            let mut stages = vec![predicate.clone()];
            let mut state = execution.context.memory.reserve(0)?;
            if output == BatchOutput::Pages && matches!(input.relation().node(), RelationNode::Filter { .. }) { return Ok(None); }
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
            if projected && output == BatchOutput::Pages { return Ok(None); }
            stages.reverse();
            let filter = Box::new(crate::columnar::BatchFilter::with_stages(batches, schema, stages, execution));
            Ok(Some((crate::columnar::retain_batch_state(filter, state), projected)))
        }
        RelationNode::Project { expressions,.. } | RelationNode::Rename { expressions,.. } => {
            let child = runtime.child(0)?;
            let positions = expressions.iter().map(|named| match &named.expr { Expr::Column { id,.. } => child.schema().position(*id), _ => None }).collect::<Option<Vec<_>>>();
            let scalar_fallback = !expressions.iter().all(|named| crate::columnar::projection::shaped(&named.expr));
            if positions.is_none() && scalar_fallback && output != BatchOutput::Arrow { return Ok(None); }
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
        RelationNode::Extend { expressions, .. } if output == BatchOutput::Arrow => {
            let child = runtime.child(0)?;
            let scalar_fallback = !expressions.iter().all(|named| crate::columnar::projection::shaped(&named.expr));
            let schema = child.schema().clone();
            let projections = schema.fields().iter().map(rtdb_plan::Field::expression)
                .chain(expressions.iter().map(|named| named.expr.clone())).collect();
            let Some((batches, _)) = build_batch_input(child, execution.child(0), pushdowns, output).await? else { return Ok(None); };
            let expressions = crate::columnar::BatchExpressions::new(batches, schema, projections, execution).validating_input(scalar_fallback);
            Ok(Some((Box::new(expressions), true)))
        }
        RelationNode::Set { operator, all, .. } if output == BatchOutput::Arrow => {
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
        RelationNode::Limit { offset, fetch, .. } if output == BatchOutput::Arrow => {
            let Some((input, projected)) = build_batch_input(runtime.child(0)?, execution.child(0), None, output).await? else { return Ok(None); };
            let offset = row_limit(offset.as_ref(), &execution).await?.unwrap_or(0);
            let remaining = row_limit(fetch.as_ref(), &execution).await?;
            let limit = BatchLimit { input: Some(input), offset, remaining, context: execution.context };
            Ok(Some((Box::new(limit), projected)))
        }
        RelationNode::Unnest { expr: expr @ Expr::Column { id, .. }, ordinality, outer, .. } if output == BatchOutput::Arrow => {
            let child = runtime.child(0)?;
            let schema = child.schema().clone();
            let Some(position) = schema.position(*id) else { return Ok(None); };
            let Some((input, projected)) = build_batch_input(child, execution.child(0), pushdowns, output).await? else { return Ok(None); };
            let unnest = BatchUnnest { input, position, expr: expr.clone(), schema, projected,
                ordinality: ordinality.as_ref().map(|field| field.name.clone()), outer: *outer, execution };
            Ok(Some((Box::new(unnest), projected)))
        }
        RelationNode::Ordinality { scope, field, .. } if output == BatchOutput::Arrow => {
            let child = runtime.child(0)?;
            if *scope == rtdb_plan::OrdinalityScope::Page && !matches!(child.relation().node(), RelationNode::ColumnTable { .. }) {
                return Ok(None);
            }
            let Some((input, projected)) = build_batch_input(child, execution.child(0), None, output).await? else { return Ok(None); };
            let ordinal = crate::list::BatchOrdinality::new(input, field.name.clone(), *scope, execution.context);
            Ok(Some((Box::new(ordinal), projected)))
        }
        RelationNode::Window { partition_by, order_by, expressions, .. } if output == BatchOutput::Arrow => {
            let child = runtime.child(0)?;
            let schema = child.schema().clone();
            let computed = !order_by.is_empty() && !BatchSort::supports(&schema, order_by);
            if computed && !computed_batch_sort(&schema, order_by) { return Ok(None); }
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
        RelationNode::Distinct { .. } if output == BatchOutput::Arrow => {
            let Some((input, projected)) = build_batch_input(runtime.child(0)?, execution.child(0), pushdowns, output).await? else { return Ok(None); };
            let input = BatchDistinct::new(input, runtime.output(), execution, projected)?;
            Ok(Some((Box::new(input), projected)))
        }
        RelationNode::Aggregate { groups, aggregates, .. } if output == BatchOutput::Arrow => {
            if !crate::aggregate::AggregateCursor::supports_columnar(&runtime, groups, aggregates)? { return Ok(None); }
            let child = runtime.child(0)?;
            let schema = child.schema().clone();
            let Some((input, projected)) = build_batch_input(child, execution.child(0), pushdowns, output).await? else { return Ok(None); };
            let aggregate = crate::aggregate::AggregateCursor::from_batches(input, projected, schema,
                groups.clone(), aggregates.clone(), runtime, execution)?;
            Ok(Some((Box::new(aggregate), false)))
        }
        RelationNode::Let { binding, .. } if output == BatchOutput::Arrow => {
            build_materialized_columns(&runtime, binding, execution).await
        }
        RelationNode::Reference { binding } if output == BatchOutput::Arrow => {
            let scope = execution.bindings.as_ref().ok_or_else(|| invalid("missing-relation", "Reference has no lexical binding"))?;
            Ok(Some((scope.open_columns(binding).await?, false)))
        }
        RelationNode::Sort { order_by, .. } | RelationNode::SortLimit { order_by, .. } if output == BatchOutput::Arrow => {
            if matches!(relation.node(), RelationNode::SortLimit { fetch: Some(_), .. }) && !execution.context.accelerators.is_empty()
                && let Some(batches) = build_ordered_columns(Arc::clone(&runtime), execution.clone(), (0..runtime.schema().len()).collect()).await?
            {
                return Ok(Some((batches, false)));
            }
            let child = runtime.child(0)?;
            let schema = child.schema().clone();
            let computed = !BatchSort::supports(&schema, order_by);
            if computed && !computed_batch_sort(&schema, order_by) { return Ok(None); }
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
            if computed {
                let positions = (0..schema.len()).collect();
                let memory = Arc::clone(&execution.context.memory);
                let Some((input, schema, order)) = project_sort_keys(batches, schema, order_by, &execution)? else { return Ok(None); };
                let sort = Box::new(BatchSort::new(input, schema, order, execution, true, offset, limit)?);
                let output = Box::new(crate::columnar::BatchProject::new(sort, positions, memory));
                return Ok(Some((output, true)));
            }
            let sort: BoxBatchCursor = Box::new(BatchSort::new(batches, schema, order_by.clone(), execution, projected, offset, limit)?);
            Ok(Some((sort, projected)))
        }
        RelationNode::Mark { input, dependent, arguments, condition: None, comparison, .. }
            if output == BatchOutput::Arrow && arguments.is_empty() => {
            let build = runtime.child(1)?;
            let probe = runtime.child(0)?;
            let Some(keys) = crate::mark::batch_membership_keys(probe.schema(), build.schema(), comparison) else { return Ok(None); };
            let bounded = crate::mark::bounded_membership_input(dependent)
                && crate::join::rows_at_most(dependent, &execution)? <= execution.context.options.join_build_rows;
            let spill_comparison = if bounded { None }
                else if replayable_spill_input(input) && replayable_spill_input(dependent) { Some(comparison.clone()) }
                else { return Ok(None); };
            let Some(probe_projected) = batch_shape(&probe, output)? else { return Ok(None); };
            if batch_shape(&build, output)?.is_none() { return Ok(None); }
            let build = crate::batch_join::ProbeInput { runtime: build, execution: execution.child(1), pushdowns: None, output };
            let probe = crate::batch_join::ProbeInput { runtime: probe, execution: execution.child(0), pushdowns: None, output };
            let mark = crate::mark::BatchMembership::new(build, probe, keys, spill_comparison, execution);
            Ok(Some((Box::new(mark), probe_projected)))
        }
        RelationNode::Mark { arguments, condition, comparison, .. } if output == BatchOutput::Arrow && !arguments.is_empty() => {
            let child = runtime.child(0)?;
            let schema = child.schema().clone();
            let dependent = runtime.child(1)?;
            let mut positions = apply_input_positions(&schema, arguments, dependent.relation())?;
            let comparison = match condition {
                Some(condition) => Expr::Case { branches: vec![(condition.clone(), comparison.clone())],
                    otherwise: Box::new(Expr::Literal(Value::Bool(false))) },
                None => comparison.clone(),
            };
            positions.extend(comparison.referenced_columns().into_iter().filter_map(|id| schema.position(id)));
            positions.sort_unstable();
            positions.dedup();
            let comparison_schema = schema.concat(dependent.schema())?;
            let Some((input, projected)) = build_batch_input(child, execution.child(0), None, output).await? else { return Ok(None); };
            let plan = BatchApplyPlan { schema, dependent, arguments: arguments.clone(), positions, kind: ApplyKind::Mark,
                document_field: None, comparison: Some((comparison, comparison_schema)), execution };
            let apply = BatchApply { input, plan };
            Ok(Some((Box::new(apply), projected)))
        }
        RelationNode::Apply { arguments, kind, .. } if output == BatchOutput::Arrow => {
            let child = runtime.child(0)?;
            let schema = child.schema().clone();
            let dependent = runtime.child(1)?;
            let positions = apply_input_positions(&schema, arguments, dependent.relation())?;
            let Some((input, projected)) = build_batch_input(child, execution.child(0), None, output).await? else { return Ok(None); };
            let document_field = matches!(kind, ApplyKind::Pull | ApplyKind::PullMany)
                .then(|| runtime.schema().fields().last().cloned()).flatten();
            let plan = BatchApplyPlan { schema, dependent, arguments: arguments.clone(), positions, kind: *kind, document_field, comparison: None, execution };
            let apply = BatchApply { input, plan };
            Ok(Some((Box::new(apply), projected)))
        }
        RelationNode::PatchGaps { input, iid, valid_from, valid_to, document, from, to } if output == BatchOutput::Arrow => {
            let RelationNode::Sort { order_by, .. } = input.node() else { return Ok(None); };
            let sorted = runtime.child(0)?;
            let child = sorted.child(0)?;
            if !BatchSort::supports(child.schema(), order_by) { return Ok(None); }
            let schema = child.schema().clone();
            let sort_execution = execution.child(0).in_operator();
            let Some((batches, projected)) = build_batch_input(child, sort_execution.child(0), None, output).await? else { return Ok(None); };
            let sort = BatchSort::new(batches, schema, order_by.clone(), sort_execution.clone(), projected, 0, i64::MAX as u64)?.preserving_pages();
            let sort = crate::analysis::wrap_batches(Box::new(sort), sort_execution.analysis, &sort_execution.context.memory);
            let sort = crate::trace::wrap_batches(sort, sorted.relation(), &sort_execution.context);
            let gaps = crate::patch::BatchPatchGaps::new(sort, sorted.schema(),
                [*iid, *valid_from, *valid_to, *document], from, to, execution)?;
            Ok(Some((Box::new(gaps), projected)))
        }
        RelationNode::Join { kind: rtdb_plan::JoinKind::Cross, .. } if output == BatchOutput::Arrow => {
            let Some((left, left_projected)) = build_batch_input(runtime.child(0)?, execution.child(0), pushdowns.clone(), output).await? else { return Ok(None); };
            let Some((right, right_projected)) = build_batch_input(runtime.child(1)?, execution.child(1), pushdowns, output).await? else { return Ok(None); };
            let join = crate::cross::CrossBatchJoin::new(left, right, execution.context);
            Ok(Some((Box::new(join), left_projected || right_projected)))
        }
        RelationNode::Join { left,right,kind,condition: Some(condition) } if *kind != rtdb_plan::JoinKind::Cross => {
            use rtdb_plan::JoinKind;
            if *kind != JoinKind::Inner && output != BatchOutput::Arrow { return Ok(None); }
            let (Some(build_projected),Some(probe_projected)) = (batch_shape(&runtime.child(0)?, output)?,batch_shape(&runtime.child(1)?, output)?) else { return Ok(None) };
            if build_projected != probe_projected { return Ok(None); }
            let build_left = crate::join::build_left(left,right,*kind,&execution)?;
            // Preserve the page join's left-first order for a standalone inner join.
            let threshold = execution.context.options.join_build_rows;
            let swapped = !build_left || (output == BatchOutput::Pages
                && crate::join::rows_at_most(left,&execution)? > threshold && crate::join::rows_at_most(right,&execution)? <= threshold);
            let (build_side,probe_side,build_relation,probe_relation) = if swapped { (1,0,right,left) } else { (0,1,left,right) };
            let mut layout = crate::join_key::JoinLayout::new(build_relation,probe_relation,Some(condition),&execution.context.memory)?;
            layout.resolve_types(runtime.child(build_side)?.schema(),runtime.child(probe_side)?.schema(),&execution)?;
            let Some(order) = crate::batch_join::key_order(&layout) else { return Ok(None) };
            let (theta, residual) = match crate::batch_join::theta_tests(&layout) {
                Some(theta) if output == BatchOutput::Arrow || theta.is_empty() => (theta, None),
                None if output == BatchOutput::Arrow => (Vec::new(), Some(crate::batch_join::Residual::new(&layout, &execution)?)),
                _ => return Ok(None),
            };
            let Some((build,projected)) = build_batch_input(runtime.child(build_side)?,execution.child(build_side),pushdowns.clone(),output).await? else { return Ok(None) };
            if projected != build_projected { return Ok(None); }
            let probe = crate::batch_join::ProbeInput { runtime: runtime.child(probe_side)?,execution: execution.child(probe_side),pushdowns, output };
            let unswapped = if swapped {
                let mut layout = crate::join_key::JoinLayout::new(left,right,Some(condition),&execution.context.memory)?;
                layout.resolve_types(runtime.child(0)?.schema(),runtime.child(1)?.schema(),&execution)?;
                Some(layout)
            } else { None };
            let replay_build = if output == BatchOutput::Arrow
                && replayable_spill_input(build_relation) && replayable_spill_input(probe_relation) {
                Some(crate::batch_join::ProbeInput {
                    runtime: runtime.child(build_side)?, execution: execution.child(build_side), pushdowns: probe.pushdowns.clone(), output,
                })
            } else {
                None
            };
            let join = crate::batch_join::BatchJoin::new(build,probe,layout,order,projected,execution)
                .swapped(unswapped).validating_payloads(output == BatchOutput::Arrow).with_kind(*kind)
                .with_theta(theta).with_residual(residual).spilling_arrow(replay_build);
            Ok(Some((Box::new(join) as crate::columnar::BoxBatchCursor,projected)))
        }
        _ => Ok(None),
    }
}

fn replayable_spill_input(mut relation: &Relation) -> bool {
    use rtdb_plan::TemporalFilter;
    while let RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. } = relation.node() {
        if expressions.len() != input.schema().len() || !expressions.iter().zip(input.schema().fields()).all(|(named, field)| {
            matches!(named.expr, Expr::Column { id, .. } if id == field.id)
        }) { return false; }
        relation = input;
    }
    if let RelationNode::Values { rows } = relation.node() {
        return crate::columnar::ColumnBatch::supports_literal_rows(rows);
    }
    let RelationNode::Scan(scan) = relation.node() else { return false; };
    let simple = |expr: &Expr| matches!(expr, Expr::Literal(_) | Expr::Parameter { .. });
    let fixed = |filter: &TemporalFilter| match filter {
        TemporalFilter::Default | TemporalFilter::All => true,
        TemporalFilter::At(at) => simple(at),
        TemporalFilter::FromTo { from, to } | TemporalFilter::Between { from, to } => simple(from) && simple(to),
    };
    scan.predicates.is_empty() && scan.matches.is_empty() && scan.top_k.is_none() && scan.ordered_top_k.is_none()
        && fixed(&scan.valid_time) && fixed(&scan.system_time)
}

/// Opens a whole plan as a batch source where `build_batches` takes its shape, charging the root operator's state as
/// `build_pages` does; `None` where the plan runs as pages.
pub(crate) async fn build_columns(runtime: Arc<EmittedRelation>, execution: Execution) -> Result<Option<crate::columnar::BoxBatchCursor>> {
    execution.context.check_cancelled()?;
    let standalone_join = matches!(runtime.relation().node(),
        RelationNode::Join { left, right, kind, condition: Some(_) }
            if *kind != rtdb_plan::JoinKind::Cross
                && matches!(left.node(), RelationNode::Scan(_)) && matches!(right.node(), RelationNode::Scan(_)));
    if !standalone_join && batch_shape(&runtime, BatchOutput::Arrow)?.is_none() { return Ok(None); }
    Ok(build_batch_input_with_state(runtime, execution, None, BatchOutput::Arrow, true).await?.map(|(cursor, _)| cursor))
}

async fn build_materialized_columns(runtime: &Arc<EmittedRelation>, binding: &Arc<rtdb_plan::MaterializedBinding>,
    execution: Execution) -> Result<Option<(crate::columnar::BoxBatchCursor, bool)>> {
    let body = runtime.child(1)?;
    let bound = runtime.child(0)?;
    if batch_shape(&body, BatchOutput::Arrow)?.is_none() || batch_shape(&bound, BatchOutput::Arrow)?.is_none() {
        return Ok(None);
    }
    let reservation = state_reservation(runtime, &execution)?;
    let schema = bound.schema().clone();
    let Some((input, _)) = build_batch_input(bound, execution.child(0), None, BatchOutput::Arrow).await? else { return Ok(None); };
    let context = &execution.context;
    let scope = crate::materialized::BindingScope::columns(Arc::clone(binding), input, schema, execution.bindings.clone(), Arc::clone(context))?;
    let mut body_execution = execution.child(1);
    body_execution.bindings = Some(Arc::clone(&scope));
    let width = body.schema().len();
    let (body, projected) = match build_batch_input(Arc::clone(&body), body_execution.clone(), None, BatchOutput::Arrow).await? {
        Some(input) => input,
        None => (crate::columnar::batches_from_pages(build_pages(body, body_execution).await?, width, Arc::clone(&context.memory)), false),
    };
    let cursor = Box::new(crate::materialized::ColumnLetCursor { body, _binding: scope });
    let cursor = crate::columnar::retain_batch_state(cursor, reservation);
    Ok(Some((cursor, projected)))
}

pub(crate) enum OrderedInput {
    Batches(crate::columnar::BoxBatchCursor),
    Rows(BoxPageCursor),
}

impl OrderedInput {
    fn pages(self, execution: &Execution) -> BoxPageCursor {
        match self {
            Self::Rows(rows) => rows,
            Self::Batches(batches) => Box::new(crate::columnar::BatchRows::new(batches, Arc::clone(&execution.context.memory))),
        }
    }

    fn observe(self, runtime: &Arc<EmittedRelation>, execution: &Execution) -> Result<Self> {
        let reservation = state_reservation(runtime, execution)?;
        let relation = runtime.relation();
        let context = &execution.context;
        let analysis = execution.analysis.clone();
        Ok(match self {
            Self::Rows(rows) => Self::Rows(crate::trace::wrap(
                crate::analysis::wrap(crate::pages::retain_page_state(rows, reservation), analysis, &context.memory), relation, context,
            )),
            Self::Batches(batches) => Self::Batches(crate::trace::wrap_batches(
                crate::analysis::wrap_batches(crate::columnar::retain_batch_state(batches, reservation), analysis, &context.memory), relation, context,
            )),
        })
    }
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
                let Some(input) = ordered_input(runtime.child(0)?, execution.child(0), limit, offset, Some(projection)).await? else {
                    return Ok(None);
                };
                input
            }
            RelationNode::Project { expressions, .. } | RelationNode::Rename { expressions, .. } => {
                let child = runtime.child(0)?;
                let Some(positions) = passthrough(expressions, child.schema(), &projection) else { return Ok(None); };
                let Some(input) = build_ordered_columns(child, execution.child(0), positions).await? else { return Ok(None); };
                OrderedInput::Batches(input)
            }
            _ => return Ok(None),
        };
        let input = if already_in_operator { input } else { input.observe(&runtime, &execution)? };
        Ok(match input {
            OrderedInput::Batches(input) => Some(input),
            OrderedInput::Rows(_) => None,
        })
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

/// Whether `build_batches` opens this subtree as batches, and if so whether they pass through a projection.
fn batch_shape(runtime: &Arc<EmittedRelation>, output: BatchOutput) -> Result<Option<bool>> {
    stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || batch_shape_at(runtime, output))
}

fn computed_batch_sort(schema: &Schema, order_by: &[rtdb_plan::SortExpr]) -> bool {
    !order_by.is_empty() && order_by.iter().all(|key| match &key.expr {
        Expr::Column { id, .. } => schema.position(*id).is_some(),
        expr => rtdb_expr::validate_order_type(&expr.data_type()).is_ok(),
    })
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
        if rtdb_expr::validate_order_type(&ty).is_err() { return Ok(None); }
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
        RelationNode::Scan(scan) => (!crate::inspection::is_inspection(&scan.table.name.table) && crate::system_schema::definition(&scan.table.name.table).is_none()).then_some(false),
        RelationNode::Let { .. } if output == BatchOutput::Arrow => {
            if batch_shape(&runtime.child(0)?, output)?.is_none() { return Ok(None); }
            batch_shape(&runtime.child(1)?, output)?
        }
        RelationNode::Reference { .. } if output == BatchOutput::Arrow => Some(false),
        RelationNode::Values { rows } if output == BatchOutput::Arrow => crate::columnar::ColumnBatch::supports_literal_rows(rows).then_some(false),
        RelationNode::ColumnTable { expr: Expr::Parameter { .. }, .. } if output == BatchOutput::Arrow => Some(false),
        RelationNode::Filter { .. } => {
            let mut child = runtime.child(0)?;
            while matches!(child.relation().node(), RelationNode::Filter { .. }) {
                if output == BatchOutput::Pages { return Ok(None); }
                child = child.child(0)?;
            }
            match batch_shape(&child, output)? { Some(true) if output == BatchOutput::Pages => None, shape => shape }
        }
        RelationNode::Project { expressions,.. } | RelationNode::Rename { expressions,.. } => {
            let child = runtime.child(0)?;
            let columns = expressions.iter().all(|named| matches!(&named.expr, Expr::Column { id,.. } if child.schema().position(*id).is_some()));
            if !columns && output != BatchOutput::Arrow
                && !expressions.iter().all(|named| crate::columnar::projection::shaped(&named.expr)) { return Ok(None); }
            batch_shape(&child, output)?.map(|_| true)
        }
        RelationNode::Extend { .. } if output == BatchOutput::Arrow => {
            let child = runtime.child(0)?;
            batch_shape(&child, output)?.map(|_| true)
        }
        RelationNode::Set { operator, .. } if output == BatchOutput::Arrow => {
            batch_shape(&runtime.child(0)?, output)?.zip(batch_shape(&runtime.child(1)?, output)?)
                .map(|(left, right)| left || (*operator == SetOperator::Union && right))
        }
        RelationNode::Distinct { .. } if output == BatchOutput::Arrow => batch_shape(&runtime.child(0)?, output)?,
        RelationNode::Aggregate { groups, aggregates, .. } if output == BatchOutput::Arrow => {
            if !crate::aggregate::AggregateCursor::supports_columnar(runtime, groups, aggregates)? { return Ok(None); }
            batch_shape(&runtime.child(0)?, output)?.map(|_| false)
        }
        RelationNode::Mark { arguments, .. } if output == BatchOutput::Arrow && !arguments.is_empty() =>
            batch_shape(&runtime.child(0)?, output)?,
        RelationNode::Mark { condition: None, comparison, .. } if output == BatchOutput::Arrow => {
            let build = runtime.child(1)?;
            let probe = runtime.child(0)?;
            if crate::mark::batch_membership_keys(probe.schema(), build.schema(), comparison).is_none()
                || batch_shape(&build, output)?.is_none() { return Ok(None); }
            batch_shape(&probe, output)?
        }
        RelationNode::Apply { .. } if output == BatchOutput::Arrow =>
            batch_shape(&runtime.child(0)?, output)?,
        RelationNode::PatchGaps { input, .. } if output == BatchOutput::Arrow => {
            let RelationNode::Sort { order_by, .. } = input.node() else { return Ok(None); };
            let child = runtime.child(0)?.child(0)?;
            if !crate::sort::batches::BatchSort::supports(child.schema(), order_by) { return Ok(None); }
            batch_shape(&child, output)?
        }
        RelationNode::Join { kind: rtdb_plan::JoinKind::Cross, .. } if output == BatchOutput::Arrow => {
            batch_shape(&runtime.child(0)?, output)?.zip(batch_shape(&runtime.child(1)?, output)?).map(|(left, right)| left || right)
        }
        RelationNode::Join { condition: Some(_), .. } if output == BatchOutput::Arrow => {
            batch_shape(&runtime.child(0)?, output)?.zip(batch_shape(&runtime.child(1)?, output)?)
                .and_then(|(left, right)| (left == right).then_some(left))
        }
        RelationNode::Limit { .. } if output == BatchOutput::Arrow => batch_shape(&runtime.child(0)?, output)?,
        RelationNode::Unnest { expr: Expr::Column { .. }, .. } if output == BatchOutput::Arrow => batch_shape(&runtime.child(0)?, output)?,
        RelationNode::Ordinality { scope, .. } if output == BatchOutput::Arrow => {
            let child = runtime.child(0)?;
            if *scope == rtdb_plan::OrdinalityScope::Page && !matches!(child.relation().node(), RelationNode::ColumnTable { .. }) {
                return Ok(None);
            }
            batch_shape(&child, output)?
        }
        RelationNode::Window { partition_by, order_by, expressions, .. } if output == BatchOutput::Arrow => {
            let child = runtime.child(0)?;
            if !crate::window::BatchWindow::supports(child.schema(), partition_by, &[], expressions) { return Ok(None); }
            let computed = !order_by.is_empty() && !crate::sort::batches::BatchSort::supports(child.schema(), order_by);
            if computed && !computed_batch_sort(child.schema(), order_by) { return Ok(None); }
            batch_shape(&child, output)?.map(|projected| projected || computed)
        }
        RelationNode::Sort { order_by, .. } | RelationNode::SortLimit { order_by, .. } if output == BatchOutput::Arrow => {
            let child = runtime.child(0)?;
            let computed = !crate::sort::batches::BatchSort::supports(child.schema(), order_by);
            if computed && !computed_batch_sort(child.schema(), order_by) { return Ok(None); }
            batch_shape(&child, output)?.map(|projected| projected || computed)
        }
        _ => None,
    })
}

fn build_pages_constrained(runtime: Arc<EmittedRelation>, execution: Execution, constraints: crate::inspection::Constraints, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>) -> futures::future::BoxFuture<'static, Result<BoxPageCursor>> {
    let mut build = Box::pin(build_pages_at(runtime, execution, constraints, pushdowns));
    Box::pin(std::future::poll_fn(move |context| {
        stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || build.as_mut().poll(context))
    }))
}

async fn build_pages_at(runtime: Arc<EmittedRelation>, execution: Execution, constraints: crate::inspection::Constraints, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>) -> Result<BoxPageCursor> {
    let execution = execution.in_operator();
    execution.context.check_cancelled()?;
    let relation = Arc::clone(runtime.relation());
    let state_reservation = state_reservation(&runtime, &execution)?;
    let traced_relation = Arc::clone(&relation);
    let traced_context = Arc::clone(&execution.context);
    let analysis = execution.analysis.clone();
    if let RelationNode::PatchGaps { input, iid, valid_from, valid_to, document, from, to } = relation.node() {
        let input_pages = build_pages_constrained(runtime.child(0)?, execution.child(0), crate::inspection::Constraints::default(), None).await?;
        let pages = Box::new(crate::patch::PatchGaps::new(input_pages, input.schema(), [*iid, *valid_from, *valid_to, *document], from, to, execution)?);
        let pages = crate::pages::retain_page_state(pages, state_reservation);
        return Ok(crate::trace::wrap(crate::analysis::wrap(pages, analysis, &traced_context.memory), &traced_relation, &traced_context));
    }
    if let RelationNode::Project { expressions,.. } | RelationNode::Rename { expressions,.. } = relation.node()
        && !expressions.iter().all(|named| matches!(named.expr, Expr::Column { .. })) && expressions.iter().all(|named| crate::columnar::projection::shaped(&named.expr))
        && let Some((batches,_)) = build_batches(Arc::clone(&runtime),execution.clone(),pushdowns.clone()).await? {
        let pages: BoxPageCursor = Box::new(crate::columnar::BatchRows::new(batches,Arc::clone(&execution.context.memory)));
        let pages = crate::pages::retain_page_state(pages,state_reservation);
        return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
    }
    match relation.node() {
        RelationNode::Join { kind: rtdb_plan::JoinKind::Cross,.. } => {
            let left = build_pages_constrained(runtime.child(0)?,execution.child(0),constraints.clone(),pushdowns.clone()).await?;
            let right = build_pages_constrained(runtime.child(1)?,execution.child(1),constraints,pushdowns).await?;
            let pages = Box::new(crate::cross::CrossJoin::new(left,right,Arc::clone(&traced_context)));
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::Join { left,right,kind,condition } => {
            let mut constraints = constraints;if let Some(condition) = condition { constraints.require(condition); }
            let build_left = crate::join::build_left(left,right,*kind,&execution)?;
            let (build_index,probe_index) = if build_left { (0,1) } else { (1,0) };
            let build = build_pages_constrained(runtime.child(build_index)?,execution.child(build_index),constraints.clone(),pushdowns.clone()).await?;
            let probe = DeferredInput { runtime: runtime.child(probe_index)?,execution: execution.child(probe_index),constraints,pushdowns };
            let mut layout = crate::join_key::JoinLayout::new(left,right,condition.as_ref(),&execution.context.memory)?;
            layout.resolve_types(runtime.child(0)?.schema(),runtime.child(1)?.schema(),&execution)?;
            let pages = Box::new(crate::join::JoinCursor::new(build,probe,layout,build_left,*kind,execution)?);
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::Values { rows } => {
            let page = materialize_rows(rows, &execution).await?;
            let pages = crate::pages::single_page(page, Arc::clone(&execution.context));
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::LiteralPages { .. } => {
            let pages = Box::new(LiteralPageCursor { relation, next: 0, execution });
            let pages = crate::pages::retain_page_state(pages, state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages, analysis, &traced_context.memory), &traced_relation, &traced_context));
        }
        RelationNode::Scan(scan) => {
            let mut constraints = constraints;for predicate in &scan.predicates { constraints.add(predicate); }
            let predicate = scan.predicates.iter().cloned().reduce(|left,right|Expr::binary(rtdb_plan::BinaryOp::And,left,right));
            let pages: BoxPageCursor = if crate::inspection::is_inspection(&scan.table.name.table) || crate::system_schema::definition(&scan.table.name.table).is_some() {
                let pages = if crate::inspection::is_inspection(&scan.table.name.table) {
                    crate::pages::from_rows(Box::new(crate::inspection::InspectionCursor::new(scan.clone(),execution.clone(),&constraints).await?),Arc::clone(&execution.context))
                } else { crate::pages::from_rows(Box::new(crate::system::SystemCursor::new(scan.clone(),execution.clone())),Arc::clone(&execution.context)) };
                match predicate {
                    Some(predicate) => Box::new(PageFilter { input: pages,schema: runtime.schema().clone(),stages: vec![(predicate,execution.clone())],_collapsed: execution.context.memory.reserve(0)?,execution }) as BoxPageCursor,
                    None => pages,
                }
            } else {
                let batches: crate::columnar::BoxBatchCursor = Box::new(crate::scan::ScanCursor::new(scan.clone(),execution.clone(),pushdowns).await?);
                let batches = match predicate {
                    Some(predicate) => Box::new(crate::columnar::BatchFilter::new(batches,runtime.schema().clone(),predicate,execution.clone())) as crate::columnar::BoxBatchCursor,
                    None => batches,
                };
                Box::new(crate::columnar::BatchRows::new(batches,Arc::clone(&execution.context.memory)))
            };
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::Project { expressions,.. } | RelationNode::Rename { expressions,.. } => {
            let pushdowns = pushdowns.map(|filters|filters.project(expressions,&execution.context.memory)).transpose()?;
            let pages = build_pages_constrained(runtime.child(0)?,execution.child(0),constraints.through_project(expressions),pushdowns).await?;
            let pages = Box::new(PageProject { input: pages,schema: runtime.child(0)?.schema().clone(),expressions: expressions.clone(),execution });
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::Extend { expressions, .. } => {
            let child = runtime.child(0)?;
            let schema = child.schema().clone();
            let pages = build_pages_constrained(child, execution.child(0), constraints.through_extension(expressions), pushdowns).await?;
            let pages = Box::new(PageExtend { input: pages, schema, expressions: expressions.clone(), execution });
            let pages = crate::pages::retain_page_state(pages, state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages, analysis, &traced_context.memory), &traced_relation, &traced_context));
        }
        RelationNode::Filter { predicate,.. } => {
            let mut constraints = constraints;constraints.add(predicate);
            let schema = runtime.child(0)?.schema().clone();
            let mut stages = vec![(predicate.clone(),execution.clone())];
            let (mut child,mut child_execution) = (runtime.child(0)?,execution.child(0));
            // A run of stacked selections is one cursor applying each in turn, so its depth never nests the cursors.
            let mut collapsed = 0usize;
            while execution.analysis.is_none() && execution.context.tracer.is_none() {
                let RelationNode::Filter { predicate,.. } = child.relation().node() else { break };
                constraints.add(predicate);
                collapsed = collapsed.checked_add(crate::runtime_schema::local_allocation_bound(child.relation())?).ok_or_else(||invalid("operator-size","Operator state exceeds addressable memory"))?;
                stages.push((predicate.clone(),child_execution.clone()));
                let next = child.child(0)?;
                child_execution = child_execution.child(0);
                child = next;
            }
            stages.reverse();
            let collapsed = execution.context.memory.reserve(collapsed)?;
            let pages = build_pages_constrained(child,child_execution,constraints,pushdowns).await?;
            let pages = Box::new(PageFilter { input: pages,schema,stages,execution,_collapsed: collapsed });
            let pages = crate::pages::coalesce(pages,Arc::clone(&traced_context));
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::Sort { order_by,.. } => {
            let mut constraints = constraints;for order in order_by { constraints.require(&order.expr); }
            let pages = build_pages_constrained(runtime.child(0)?,execution.child(0),constraints,pushdowns).await?;
            let pages = Box::new(crate::sort::SortCursor::from_pages(pages,runtime.child(0)?.schema().clone(),order_by.clone(),execution));
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::Limit { offset,fetch,.. } => {
            let input = build_pages_constrained(runtime.child(0)?,execution.child(0),crate::inspection::Constraints::default(),None).await?;
            let offset = row_limit(offset.as_ref(),&execution).await?.unwrap_or(0);
            let remaining = row_limit(fetch.as_ref(),&execution).await?;
            let pages = Box::new(Limit { input: Some(input),offset,remaining,execution });
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::SortLimit { order_by,offset,fetch,.. } => {
            let mut constraints = constraints;for order in order_by { constraints.require(&order.expr); }
            let pushdowns = if offset.is_some() || fetch.is_some() { None } else { pushdowns };
            if !order_by.is_empty()
                && let Some((offset, remaining)) = fetch.as_ref().and_then(|fetch| ordered_window(offset.as_ref(), fetch, &execution))
                && let Ok(limit) = usize::try_from(sort_prefix_len(offset, Some(remaining)))
                && let Some(input) = ordered_input(runtime.child(0)?, execution.child(0), limit, 0, None).await?
            {
                let pages = Box::new(Limit { input: Some(input.pages(&execution)), offset, remaining: Some(remaining), execution });
                let pages = crate::pages::retain_page_state(pages, state_reservation);
                return Ok(crate::trace::wrap(crate::analysis::wrap(pages, analysis, &traced_context.memory), &traced_relation, &traced_context));
            }
            let schema = runtime.child(0)?.schema().clone();
            let batches = if !order_by.is_empty() && crate::sort::SortCursor::batch_eligible(&schema,order_by,0) {
                build_batches(runtime.child(0)?,execution.child(0),pushdowns.clone()).await?
            } else { None };
            let input = match batches {
                Some(batches) => Ok(batches),
                None => Err(build_pages_constrained(runtime.child(0)?,execution.child(0),constraints,pushdowns).await?),
            };
            let offset_value = row_limit(offset.as_ref(),&execution).await?.unwrap_or(0);
            let remaining = row_limit(fetch.as_ref(),&execution).await?;
            let row_limit = sort_prefix_len(offset_value, remaining);
            let mut pages: BoxPageCursor = match input {
                Ok((batches,projected)) if crate::sort::SortCursor::batch_eligible(&schema,order_by,row_limit) =>
                    Box::new(crate::sort::SortCursor::top_batches(batches,projected,schema,order_by.clone(),row_limit,execution.clone())),
                Ok((batches,projected)) => {
                    let pages: BoxPageCursor = Box::new(crate::columnar::BatchRows::new(batches,Arc::clone(&execution.context.memory)).admitting_dates(projected));
                    Box::new(crate::sort::SortCursor::with_row_limit(pages,schema,order_by.clone(),row_limit,execution.clone()))
                }
                Err(pages) => if order_by.is_empty() { pages } else { Box::new(crate::sort::SortCursor::with_row_limit(pages,schema,order_by.clone(),row_limit,execution.clone())) },
            };
            if offset.is_some() || fetch.is_some() { pages = Box::new(Limit { input: Some(pages),offset: offset_value,remaining,execution }); }
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::Distinct { .. } => {
            let input = build_pages_constrained(runtime.child(0)?,execution.child(0),crate::inspection::Constraints::default(),pushdowns).await?;
            let pages = Box::new(Distinct { input,seen: SeenRows::new(&execution.context,runtime.child(0)?.output())?,execution });
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::Set { operator,all,.. } => {
            let left = build_pages_constrained(runtime.child(0)?,execution.child(0),crate::inspection::Constraints::default(),pushdowns.clone()).await?;
            let right = build_pages_constrained(runtime.child(1)?,execution.child(1),crate::inspection::Constraints::default(),pushdowns).await?;
            let pages: BoxPageCursor = if *operator == SetOperator::Union {
                let input: BoxPageCursor = Box::new(Union { left: Some(left),right });
                if *all { input } else { Box::new(Distinct { input,seen: SeenRows::new(&execution.context,runtime.output())?,execution }) }
            } else { Box::new(SetCursor::new(left,right,*operator,*all,Arc::clone(&runtime),execution)?) };
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::ColumnTable { expr, .. } => {
            let pages = crate::column_table::open(&execution, expr.clone()).await?;
            let pages = crate::pages::retain_page_state(pages, state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages, analysis, &traced_context.memory), &traced_relation, &traced_context));
        }
        RelationNode::List { expr, page_rows, .. } => {
            let pages = Box::new(execution.open_list(expr.clone(),*page_rows).await?);
            let pages = crate::pages::retain_page_state(pages, state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages, analysis, &traced_context.memory), &traced_relation, &traced_context));
        }
        RelationNode::Ordinality { scope, .. } => {
            let input = build_pages_constrained(runtime.child(0)?, execution.child(0), crate::inspection::Constraints::default(), None).await?;
            let pages = Box::new(crate::list::Ordinality::new(input, *scope, Arc::clone(&execution.context)));
            let pages = crate::pages::retain_page_state(pages, state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages, analysis, &traced_context.memory), &traced_relation, &traced_context));
        }
        RelationNode::Unnest { expr,ordinality,outer,.. } => {
            let input_pages = build_pages_constrained(runtime.child(0)?,execution.child(0),crate::inspection::Constraints::default(),pushdowns).await?;
            let pages = Box::new(Unnest { input: input_pages,schema: runtime.child(0)?.schema().clone(),expr: expr.clone(),ordinality: ordinality.is_some(),outer: *outer,execution });
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::Apply { arguments,kind,.. } => {
            let input_pages = build_pages_constrained(runtime.child(0)?,execution.child(0),crate::inspection::Constraints::default(),None).await?;
            let pages = Box::new(Apply { input: input_pages,schema: runtime.child(0)?.schema().clone(),dependent: runtime.child(1)?,arguments: arguments.clone(),kind: *kind,execution });
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::Let { binding,.. } => {
            let bound = build_pages_constrained(runtime.child(0)?,execution.child(0),crate::inspection::Constraints::default(),None).await?;
            let scope = crate::materialized::BindingScope::new(Arc::clone(binding),bound,execution.bindings.clone(),Arc::clone(&execution.context))?;
            let mut body_execution = execution.child(1);body_execution.bindings = Some(Arc::clone(&scope));
            let body = build_pages_constrained(runtime.child(1)?,body_execution,constraints,pushdowns).await?;
            let pages = Box::new(crate::materialized::LetCursor { body,_binding: scope });
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::Reference { binding } => {
            let pages = execution.bindings.as_ref().ok_or_else(||invalid("missing-relation","Reference has no lexical binding"))?.open(binding).await?;
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::Mark { arguments,condition,comparison,.. } => {
            let probe = runtime.child(0)?;
            let pages = Box::new(crate::mark::Mark::new(
                DeferredInput { runtime: Arc::clone(&probe),execution: execution.child(0),constraints,pushdowns },
                &probe,runtime.child(1)?,arguments.clone(),condition.clone(),comparison.clone(),execution,
            )?);
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::Aggregate { groups,aggregates,.. } => {
            let schema = runtime.child(0)?.schema().clone();
            let columnar = crate::aggregate::AggregateCursor::reads_columns(&schema,groups,aggregates).is_some();
            let batches = if columnar { build_batches(runtime.child(0)?,execution.child(0),pushdowns.clone()).await? } else { None };
            let pages: Box<crate::aggregate::AggregateCursor> = match batches {
                Some((batches,projected)) => Box::new(crate::aggregate::AggregateCursor::from_batches(batches,projected,schema,groups.clone(),aggregates.clone(),Arc::clone(&runtime),execution)?),
                None => {
                    let input = build_pages_constrained(runtime.child(0)?,execution.child(0),crate::inspection::Constraints::default(),pushdowns).await?;
                    Box::new(crate::aggregate::AggregateCursor::new(input,schema,groups.clone(),aggregates.clone(),Arc::clone(&runtime),execution)?)
                }
            };
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::Window { partition_by,order_by,expressions,.. } => {
            let input = build_pages(runtime.child(0)?,execution.child(0)).await?;
            let pages = Box::new(crate::window::WindowCursor::new(input,runtime.child(0)?,partition_by.clone(),order_by.clone(),expressions.clone(),execution));
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis,&traced_context.memory),&traced_relation,&traced_context));
        }
        RelationNode::PatchGaps { .. } => unreachable!(),
    }
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

async fn materialize_rows(rows: &[Vec<Expr>], execution: &Execution) -> Result<Option<OwnedPage>> {
    if rows.is_empty() { return Ok(None); }
    let mut page = PageBuilder::new(rows.len(), &execution.context.memory)?;
    let input = Schema::default();
    for expressions in rows {
        let scope = execution.scope()?;
        scope.reserve(expressions.len().checked_mul(std::mem::size_of::<Value>()).ok_or_else(|| invalid("table-size", "Table row allocation overflow"))?)?;
        let mut values = Vec::with_capacity(expressions.len());
        for expr in expressions { values.push(execution.evaluate(&scope, expr, &input, &[]).await?); }
        page.push(OwnedRow::new(values, &execution.context.memory)?)?;
    }
    Ok(Some(page.finish()))
}
struct LiteralPageCursor { relation: Arc<Relation>, next: usize, execution: Execution }

#[async_trait]
impl PageCursor for LiteralPageCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.execution.context.check_cancelled()?;
        let RelationNode::LiteralPages { pages } = self.relation.node() else { return Err(invalid("page-source", "Expected literal pages")); };
        let Some(rows) = pages.get(self.next) else { return Ok(None); };
        let page = match materialize_rows(rows, &self.execution).await? {
            Some(page) => page,
            None => PageBuilder::new(0, &self.execution.context.memory)?.finish(),
        };
        self.next += 1;
        Ok(Some(page))
    }
}
fn ordered_input(
    runtime: Arc<EmittedRelation>,
    execution: Execution,
    limit: usize,
    offset: usize,
    projection: Option<Vec<usize>>,
) -> futures::future::BoxFuture<'static, Result<Option<OrderedInput>>> {
    let mut build = Box::pin(async move {
        let execution = execution.in_operator();
        let input = match runtime.relation().node() {
            RelationNode::Scan(scan) => {
                let Some(input) = crate::scan::ordered_input(scan, &execution, limit, offset, projection.as_deref()).await? else { return Ok(None); };
                input
            }
            RelationNode::Project { expressions, .. } | RelationNode::Rename { expressions, .. } => {
                let child = runtime.child(0)?;
                match projection {
                    Some(positions) => {
                        let Some(positions) = passthrough(expressions, child.schema(), &positions) else { return Ok(None); };
                        let Some(input) = ordered_input(child, execution.child(0), limit, offset, Some(positions)).await? else { return Ok(None); };
                        input
                    }
                    None => {
                        let Some(input) = ordered_input(Arc::clone(&child), execution.child(0), limit, offset, None).await? else { return Ok(None); };
                        let all = (0..expressions.len()).collect::<Vec<_>>();
                        match (input, passthrough(expressions, child.schema(), &all)) {
                            (OrderedInput::Batches(input), Some(positions)) => OrderedInput::Batches(Box::new(
                                crate::columnar::BatchProject::new(input, positions, Arc::clone(&execution.context.memory)),
                            )),
                            (input, _) => OrderedInput::Rows(Box::new(PageProject {
                                input: input.pages(&execution), schema: child.schema().clone(), expressions: expressions.clone(), execution: execution.clone(),
                            })),
                        }
                    }
                }
            }
            RelationNode::Extend { expressions, .. } if projection.is_none() => {
                let child = runtime.child(0)?;
                let Some(input) = ordered_input(Arc::clone(&child), execution.child(0), limit, offset, None).await? else { return Ok(None); };
                OrderedInput::Rows(Box::new(PageExtend {
                    input: input.pages(&execution), schema: child.schema().clone(), expressions: expressions.clone(), execution: execution.clone(),
                }))
            }
            RelationNode::Filter { .. } | RelationNode::Distinct { .. } => {
                let Some(input) = ordered_input(runtime.child(0)?, execution.child(0), limit, offset, projection).await? else { return Ok(None); };
                input
            }
            _ => return Ok(None),
        };
        input.observe(&runtime, &execution).map(Some)
    });
    Box::pin(std::future::poll_fn(move |context| {
        stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || build.as_mut().poll(context))
    }))
}

struct PageProject { input: BoxPageCursor, schema: Schema, expressions: Vec<NamedExpr>, execution: Execution }
#[async_trait]
impl PageCursor for PageProject {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.execution.context.check_cancelled()?;
        let Some(page) = self.input.next_page().await? else { return Ok(None); };
        // Projecting every input column in place copies each cell unchanged unless admitting it rewrites a date.
        let identity = self.expressions.len() == self.schema.len()
            && self.expressions.iter().enumerate().all(|(index,named)| matches!(&named.expr, Expr::Column { id, .. } if self.schema.position(*id) == Some(index)));
        if identity && page.rows().iter().all(|row| row.values.iter().all(|value| !matches!(value, Value::Date(_) | Value::List(_) | Value::Set(_) | Value::FixedSizeList(_) | Value::Struct(_) | Value::Map { .. }))) {
            return Ok(Some(page));
        }
        crate::pages::project(&page,&self.schema,self.expressions.iter().map(|expression|&expression.expr),&self.execution).await.map(Some)
    }

    fn skip_complete_pages(&mut self, rows: u64) -> Result<u64> {
        if self.expressions.iter().all(|expression| matches!(&expression.expr, Expr::Column { .. })) {
            self.input.skip_complete_pages(rows)
        } else {
            Ok(0)
        }
    }
}
struct PageExtend { input: BoxPageCursor, schema: Schema, expressions: Vec<NamedExpr>, execution: Execution }
#[async_trait]
impl PageCursor for PageExtend {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.execution.context.check_cancelled()?;
        let Some(page) = self.input.next_page().await? else { return Ok(None); };
        crate::pages::extend(&page, &self.schema, self.expressions.iter().map(|named| &named.expr), &self.execution).await.map(Some)
    }
}
/// Stacked selections, lowest first; each stage reads only the rows of a page that every stage before it kept.
struct PageFilter { input: BoxPageCursor, schema: Schema, stages: Vec<(Expr,Execution)>, execution: Execution, _collapsed: Arc<rtdb_columnar::MemoryReservation> }
#[async_trait]
impl PageCursor for PageFilter {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.execution.context.check_cancelled()?;
        'pages: while let Some(mut page) = self.input.next_page().await? {
            for (predicate,execution) in &self.stages {
                let metadata_scope = execution.scope()?;
                let invocation = execution.prepare_expression(predicate,&self.schema,&metadata_scope)?;
                let mut output = PageBuilder::new(page.len(),&execution.context.memory)?;
                let scope = execution.scope()?;
                while let Some(row) = page.next() {
                    if is_true(&execution.evaluate_invocation(&scope,&invocation,&row.values).await?)? { output.push(row)?; }
                    scope.reset()?;
                }
                if output.len() == 0 { continue 'pages; }
                page = output.finish();
            }
            return Ok(Some(page));
        }
        Ok(None)
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span); }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}
struct Limit { input: Option<BoxPageCursor>, offset: u64, remaining: Option<u64>, execution: Execution }
#[async_trait]
impl PageCursor for Limit {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        loop {
            self.execution.context.check_cancelled()?;
            if self.remaining == Some(0) && self.offset == 0 { self.input = None;return Ok(None); }
            let Some(input) = &mut self.input else { return Ok(None); };
            if self.offset != 0 {
                self.offset -= input.skip_complete_pages(self.offset)?;
                if self.remaining == Some(0) && self.offset == 0 { self.input = None; return Ok(None); }
            }
            let Some(mut page) = input.next_page().await? else { self.input = None;return Ok(None); };
            let skipped = self.offset.min(page.len() as u64) as usize;
            self.offset -= skipped as u64;
            for _ in 0..skipped { page.next(); }
            if self.remaining == Some(0) && self.offset == 0 { self.input = None;return Ok(None); }
            if page.len() == 0 { continue; }
            let count = self.remaining.unwrap_or(u64::MAX).min(page.len() as u64) as usize;
            if let Some(remaining) = &mut self.remaining { *remaining -= count as u64; }
            let mut output = PageBuilder::new(count,&self.execution.context.memory)?;
            for _ in 0..count {
                self.execution.context.check_cancelled()?;
                output.push(page.next().ok_or_else(||invalid("page-state","Limit input page ended before its row count"))?)?;
            }
            return Ok(Some(output.finish()));
        }
    }
}
struct Union { left: Option<BoxPageCursor>, right: BoxPageCursor }
#[async_trait]
impl PageCursor for Union {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        if let Some(left) = &mut self.left { while let Some(page) = left.next_page().await? { if page.len() > 0 { return Ok(Some(page)); } } }
        self.left = None;
        while let Some(page) = self.right.next_page().await? { if page.len() > 0 { return Ok(Some(page)); } }
        Ok(None)
    }
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

pub(crate) fn uri_hashes_match(left: &[Value], right: &[Value], memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<bool> {
    fn has_uri(value: &Value) -> bool {
        match value {
            Value::Uri(_) => true,
            Value::List(values) | Value::FixedSizeList(values) | Value::Set(values) => values.iter().any(has_uri),
            Value::Struct(fields) => fields.values().any(has_uri),
            Value::Map { entries, .. } => entries.iter().any(|(key,value)| has_uri(key) || has_uri(value)),
            _ => false,
        }
    }
    fn hash(values: &[Value]) -> Result<u32> {
        values.iter().try_fold(0u32, |hash,value| {
            let cell = rtdb_columnar::hash_value(value,0)? as u32;
            // Apache Arrow 19 MurmurHasher.combineHashCode, as used by IndexHasher.
            let cell = cell.wrapping_mul(0xcc9e_2d51).rotate_left(15).wrapping_mul(0x1b87_3593);
            Ok((hash ^ cell).rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64))
        })
    }
    if !left.iter().chain(right).any(has_uri) { return Ok(true); }
    let _scratch = memory.reserve(4096)?;
    Ok(hash(left)? == hash(right)?)
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
    fn temporal(value: &Value) -> bool {
        match value {
            Value::Timestamp { .. } | Value::DateDay(_) | Value::DateMilli(_) | Value::Date(_) | Value::TimestampTzRange { .. } => true,
            Value::List(values) | Value::FixedSizeList(values) | Value::Set(values) => values.iter().any(temporal),
            Value::Struct(fields) => fields.values().any(temporal),
            Value::Map { entries, .. } => entries.iter().any(|(key, value)| temporal(key) || temporal(value)),
            _ => false,
        }
    }
    values.iter().fold(0u32, |hash, value| {
        let cell = match value {
            Value::Timestamp { .. } | Value::DateDay(_) | Value::DateMilli(_) => rtdb_expr::local_micros(value, timezone)
                .and_then(|micros| rtdb_columnar::hash_value(&Value::I64(micros), 0).ok()).map_or(0x7e3a_11c5, |cell| (cell as u32) ^ 0x7e3a_11c5),
            value if temporal(value) => 0x7e3a_11c5,
            value => rtdb_columnar::hash_value(value, 0).map_or(0x5bd1_e995, |cell| cell as u32),
        };
        combine_equality_hash(hash, cell)
    })
}

pub(crate) fn combine_equality_hash(hash: u32, cell: u32) -> u32 {
    (hash ^ cell.wrapping_mul(0xcc9e_2d51).rotate_left(15)).rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64)
}

pub(crate) fn validate_distinct_types(schema: &Schema) -> Result<()> {
    for field in schema.fields() { rtdb_plan::validate_equality_types(&field.data_type, &field.data_type)?; }
    Ok(())
}
/// The distinct rows seen so far, found through an index from their hash to the rows that share it.
pub(crate) struct SeenRows { rows: Vec<OwnedRow>, index: std::collections::HashMap<u32, Vec<usize>>, validated: bool, runtime: Arc<RuntimeSchema>, context: Arc<QueryContext>, _reservation: Option<Arc<rtdb_columnar::MemoryReservation>> }
impl SeenRows {
    pub fn new(context: &Arc<QueryContext>, runtime: Arc<RuntimeSchema>) -> Result<Self> { Ok(Self { rows: Vec::new(), index: Default::default(), validated: false, runtime, context: Arc::clone(context), _reservation: None }) }
    pub fn insert(&mut self, values: &[Value]) -> Result<bool> {
        if !self.validated {
            validate_distinct_types(self.runtime.schema())?;
            self.validated = true;
        }
        let hash = equality_hash(values, &self.context.timezone);
        if let Some(candidates) = self.index.get(&hash) {
            for &candidate in candidates {
                if rows_equal(values, &self.rows[candidate].values, &self.context.timezone, &self.context.memory)? { return Ok(false); }
            }
        }
        let row = OwnedRow::copy_parts(&[values], 0, 0, &self.context.memory)?;
        if self.rows.len() == self.rows.capacity() {
            let capacity = self.rows.capacity().saturating_mul(2).max(16);
            let bytes = capacity.checked_mul(std::mem::size_of::<OwnedRow>() + std::mem::size_of::<(u32, Vec<usize>)>() + 2 * std::mem::size_of::<usize>())
                .ok_or_else(|| invalid("distinct-size", "Distinct rows exceed addressable memory"))?;
            self._reservation = Some(self.context.memory.reserve(bytes)?);
            self.rows.reserve_exact(capacity - self.rows.len());
        }
        self.index.entry(hash).or_default().push(self.rows.len());
        self.rows.push(row);
        Ok(true)
    }
}
struct Distinct { input: BoxPageCursor, seen: SeenRows, execution: Execution }
#[async_trait]
impl PageCursor for Distinct {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        while let Some(mut page) = self.input.next_page().await? {
            self.execution.context.check_cancelled()?;
            let mut output = PageBuilder::new(page.len(),&self.execution.context.memory)?;
            while let Some(row) = page.next() {
                self.execution.context.check_cancelled()?;
                if self.seen.insert(&row.values)? { output.push(row)?; }
            }
            if output.len() > 0 { return Ok(Some(output.finish())); }
        }
        Ok(None)
    }
}

struct SetCursor { left: BoxPageCursor, right: Option<BoxPageCursor>, built: Option<RightRows>, operator: SetOperator, all: bool, seen: SeenRows, runtime: Arc<EmittedRelation>, execution: Execution }
/// The right input, indexed by the hash rows equal under `rows_equal` share, and which of its rows an ALL operator consumed.
struct RightRows { rows: Vec<OwnedRow>, index: std::collections::HashMap<u32, Vec<usize>>, consumed: Vec<bool>, _reservation: Arc<rtdb_columnar::MemoryReservation> }
impl RightRows {
    fn new(rows: Vec<OwnedRow>, context: &QueryContext) -> Result<Self> {
        let bytes = rows.capacity().checked_mul(std::mem::size_of::<OwnedRow>() + std::mem::size_of::<(u32, Vec<usize>)>() + 2 * std::mem::size_of::<usize>() + 1)
            .and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| invalid("set-size", "Set rows exceed addressable memory"))?;
        let reservation = context.memory.reserve(bytes)?;
        let mut index: std::collections::HashMap<u32, Vec<usize>> = Default::default();
        for (ordinal, row) in rows.iter().enumerate() {
            index.entry(equality_hash(&row.values, &context.timezone)).or_default().push(ordinal);
        }
        let consumed = vec![false; rows.len()];
        Ok(Self { rows, index, consumed, _reservation: reservation })
    }

    fn matches(&mut self, values: &[Value], all: bool, context: &QueryContext) -> Result<bool> {
        if let Some(candidates) = self.index.get(&equality_hash(values, &context.timezone)) {
            for &ordinal in candidates {
                if all && self.consumed[ordinal] { continue; }
                if rows_equal(values, &self.rows[ordinal].values, &context.timezone, &context.memory)? {
                    if all { self.consumed[ordinal] = true; }
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
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

impl SetCursor {
    fn new(left: BoxPageCursor, right: BoxPageCursor, operator: SetOperator, all: bool, runtime: Arc<EmittedRelation>, execution: Execution) -> Result<Self> {
        Ok(Self { left, right: Some(right), built: None, operator, all, seen: SeenRows::new(&execution.context, runtime.child(0)?.output())?, runtime, execution })
    }
}
#[async_trait]
impl PageCursor for SetCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        if let Some(mut right) = self.right.take() {
            let memory = &self.execution.context.memory;
            let mut rows = Vec::new();
            while let Some(mut page) = right.next_page().await? { while let Some(row) = page.next() { self.execution.context.check_cancelled()?; rows.push(OwnedRow::copy_parts(&[&row.values], 0, 0, memory)?); } }
            self.built = Some(RightRows::new(rows, &self.execution.context)?);
        }
        while let Some(mut page) = self.left.next_page().await? {
          let mut output = PageBuilder::new(page.len(),&self.execution.context.memory)?;
          while let Some(left) = page.next() {
            self.execution.context.check_cancelled()?;
            validate_set_types(&self.runtime)?;
            if !self.all && !self.seen.insert(&left.values)? { continue; }
            let built = self.built.as_mut().ok_or_else(|| invalid("set-state", "Set input was not built"))?;
            let matched = built.matches(&left.values, self.all, &self.execution.context)?;
            if matched == (self.operator == SetOperator::Intersect) { output.push(left)?; }
          }
          if output.len() > 0 { return Ok(Some(output.finish())); }
        }
        Ok(None)
    }
}

struct RetainedList { values: rtdb_expr::EvaluatedList, _reservation: Arc<rtdb_columnar::MemoryReservation> }
impl RetainedList {
    fn new(values: rtdb_expr::EvaluatedList, memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(values.retained_bytes()?.checked_add(256).ok_or_else(|| invalid("list-size", "List descriptor size overflow"))?)?;
        Ok(Self { values, _reservation: reservation })
    }
}

struct Unnest { input: BoxPageCursor, schema: Schema, expr: Expr, ordinality: bool, outer: bool, execution: Execution }

struct BatchUnnest {
    input: crate::columnar::BoxBatchCursor,
    position: usize,
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
        let context = &self.execution.context;
        while let Some(batch) = crate::columnar::poll_nested_batch(self.input.as_mut()).await? {
            if let Some(output) = batch.expand_list(self.position, self.ordinality.as_deref(), self.outer, context)? {
                if output.len != 0 { return Ok(Some(output)); }
                continue;
            }
            let input = batch.rows_admitting(&context.memory, self.projected)?;
            drop(batch);
            let input = crate::pages::single_page(Some(input), Arc::clone(context));
            let mut unnest = Unnest { input, schema: self.schema.clone(), expr: self.expr.clone(),
                ordinality: self.ordinality.is_some(), outer: self.outer, execution: self.execution.clone() };
            if let Some(output) = unnest.next_page().await? {
                return crate::columnar::ColumnBatch::from_page(&output, &context.memory).map(Some);
            }
        }
        Ok(None)
    }
}

#[async_trait]
impl PageCursor for Unnest {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.execution.context.check_cancelled()?;
        while let Some(mut page) = self.input.next_page().await? {
            self.execution.context.check_cancelled()?;
            let metadata_scope = self.execution.scope()?;
            let invocation = self.execution.prepare_expression(&self.expr,&self.schema,&metadata_scope)?;
            let mut output = PageBuilder::new(page.len(),&self.execution.context.memory)?;
            while let Some(row) = page.next() {
                let scope = self.execution.scope()?;
                let elements = self.execution.evaluate_list_invocation(&scope,&invocation,&row.values).await?;
                let elements = RetainedList::new(elements,&self.execution.context.memory)?;
                for index in 0..elements.values.len() {
                    self.execution.context.check_cancelled()?;
                    let _cell = self.execution.context.memory.reserve(elements.values.cell_allocation_bound())?;
                    let element = elements.values.get(index).ok_or_else(|| invalid("list-index", "List cell disappeared"))?;
                    let ordinal = [if self.ordinality { Value::I32(i32::try_from(index).ok().and_then(|index|index.checked_add(1)).ok_or_else(||invalid("ordinality-overflow","List ordinal exceeds signed32 bits"))?) } else { Value::Null }];
                    let result = OwnedRow::copy_parts(&[&row.values,std::slice::from_ref(element.as_ref()),if self.ordinality { &ordinal } else { &[] }],0,0,&self.execution.context.memory)?;
                    output.push_growing(result,&self.execution.context.memory)?;
                }
                if self.outer && elements.values.is_empty() {
                    let result = OwnedRow::copy_parts(&[&row.values],0,if self.ordinality { 2 } else { 1 },&self.execution.context.memory)?;
                    output.push_growing(result,&self.execution.context.memory)?;
                }
            }
            if output.len() > 0 { return Ok(Some(output.finish())); }
        }
        Ok(None)
    }
}

fn apply_input_positions(schema: &Schema, arguments: &[(usize, Expr)], dependent: &Relation) -> Result<Vec<usize>> {
    fn correlated(relation: &Relation, schema: &Schema, required: &mut [bool]) {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || {
            for expression in relation.expressions() {
                expression.visit(&mut |expr| {
                    if let Expr::Correlated { id, .. } = expr && let Some(position) = schema.position(*id) {
                        required[position] = true;
                    }
                });
            }
            for child in relation.children() { correlated(child, schema, required); }
        });
    }
    let mut required = vec![false; schema.len()];
    for (_, argument) in arguments {
        for position in crate::columnar::expression_columns(argument, schema)? { required[position] = true; }
    }
    correlated(dependent, schema, &mut required);
    Ok(required.into_iter().enumerate().filter_map(|(position, used)| used.then_some(position)).collect())
}

struct BatchApply {
    input: crate::columnar::BoxBatchCursor,
    plan: BatchApplyPlan,
}

struct BatchApplyPlan {
    schema: Schema,
    dependent: Arc<EmittedRelation>,
    arguments: Vec<(usize, Expr)>,
    positions: Vec<usize>,
    kind: ApplyKind,
    document_field: Option<rtdb_plan::Field>,
    comparison: Option<(Expr, Schema)>,
    execution: Execution,
}

impl BatchApplyPlan {
    async fn bind(&self, batch: &crate::columnar::ColumnBatch, row: usize) -> Result<Execution> {
        let row = Arc::new(batch.expression_row(row, &self.positions, &self.execution.context.memory)?);
        self.execution.bind_arguments(&self.arguments, &self.schema, row).await
    }

    async fn dependent_batches(&self, batch: &crate::columnar::ColumnBatch, row: usize) -> Result<crate::columnar::BoxBatchCursor> {
        self.open_dependent(self.bind(batch, row).await?).await
    }

    async fn open_dependent(&self, execution: Execution) -> Result<crate::columnar::BoxBatchCursor> {
        let execution = execution.child(1);
        let runtime = Arc::clone(&self.dependent);
        let consumes_all = matches!(self.kind, ApplyKind::Inner | ApplyKind::Left | ApplyKind::PullMany);
        if (consumes_all || preserves_page_boundaries(runtime.relation()))
            && let Some(batches) = build_columns(Arc::clone(&runtime), execution.clone()).await? {
            return Ok(batches);
        }
        Ok(crate::columnar::batches_from_pages(
            build_pages(runtime, execution).await?, self.dependent.schema().len(), Arc::clone(&self.execution.context.memory),
        ))
    }

    async fn documents(&self, mut batch: crate::columnar::ColumnBatch, output: &rtdb_plan::Field) -> Result<crate::columnar::ColumnBatch> {
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
                let raw = match projection.physical_record(&schema, memory) {
                    Ok(record) => record,
                    Err(error) if error.code == "rtdb/memory-budget-exceeded" => None,
                    Err(error) => return Err(error),
                };
                let doc = if let Some((page, charge)) = raw {
                    let record: ArrayRef = Arc::new(StructArray::try_new_with_length(page.schema_ref().fields().clone(),
                        page.columns().to_vec(), None, count).map_err(|error| invalid("apply-document", error))?);
                    let field = Arc::new(arrow_schema::Field::new("doc", record.data_type().clone(), false));
                    let logical = rtdb_columnar::from_field(&field)?;
                    ColumnBatch::settled(vec![Column::Arrow { array: record, field, logical: Some(logical) }], count, charge)?
                } else {
                    let bytes = (0..count).try_fold(apply_join_bytes(count, 0)?, |bytes, row| {
                        schema.fields().iter().zip(&projection.columns).try_fold(bytes, |bytes, (field, column)| {
                            bytes.checked_add(field.name.len()).and_then(|bytes| bytes.checked_add(512))
                                .and_then(|bytes| bytes.checked_add(column.bound(row).ok()?))
                                .ok_or_else(|| invalid("row-size", "Apply document allocation overflow"))
                        })
                    })?;
                    let charge = memory.reserve(bytes)?;
                    let mut docs = Vec::with_capacity(count);
                    for row in 0..count {
                        let fields = schema.fields().iter().zip(&projection.columns)
                            .map(|(field, column)| Ok((field.name.clone(), column.value(row)?))).collect::<Result<_>>()?;
                        docs.push(Value::Struct(fields));
                    }
                    ColumnBatch::settled(vec![Column::Values(docs)], count, charge)?
                };
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
        batch.append_column(column, charge, memory)?;
        Ok(batch)
    }

    async fn joined(&self, batch: &crate::columnar::ColumnBatch) -> Result<Option<crate::columnar::ColumnBatch>> {
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
        let mut output = ColumnBatch::take_rows(&[batch], &left, &columns, memory)?;
        for position in 0..width {
            let (column, charge) = gather_nullable_column(|source| &sources[source], &right, position, memory)?;
            output.append_column(column, charge, memory)?;
        }
        Ok(Some(output))
    }
}

fn preserves_page_boundaries(relation: &Relation) -> bool {
    stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || match relation.node() {
        RelationNode::Scan(_) => true,
        RelationNode::Values { rows } => !rows.is_empty(),
        RelationNode::Project { input, .. } | RelationNode::Rename { input, .. } | RelationNode::Extend { input, .. } =>
            preserves_page_boundaries(input),
        _ => false,
    })
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
            let Some(mut batch) = crate::columnar::poll_nested_batch(self.input.as_mut()).await? else { return Ok(None); };
            batch.validate_readable(context)?;
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
                    let row = Arc::new(batch.expression_row(position, &plan.positions, memory)?);
                    let execution = plan.execution.bind_arguments(&plan.arguments, &plan.schema, Arc::clone(&row)).await?;
                    let dependent = plan.open_dependent(execution.clone()).await?;
                    let mark = crate::mark::correlated_batch_mark(&row, dependent, comparison, schema, &execution).await?;
                    marks.push(match mark { Value::Bool(value) => Some(value), _ => None });
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

async fn apply_mark(cursor: &mut BoxCursor, mut next: Option<OwnedRow>, execution: &Execution) -> Result<Option<bool>> {
    let mut mark = Some(false);
    while let Some(candidate) = next {
        execution.context.check_cancelled()?;
        match candidate.values.first() {
            Some(Value::Bool(true)) => return Ok(Some(true)),
            Some(Value::Bool(false)) => (),
            Some(Value::Null | Value::Undefined) => mark = None,
            _ => return Err(invalid("mark-type", "Mark comparison must return Boolean or null")),
        }
        next = cursor.next().await?;
    }
    Ok(mark)
}

struct Apply { input: BoxPageCursor, schema: Schema, dependent: Arc<EmittedRelation>, arguments: Vec<(usize, Expr)>, kind: ApplyKind, execution: Execution }
impl Apply {
    fn document(&self, row: &OwnedRow) -> Value {
        Value::Struct(self.dependent.relation().schema().fields().iter().zip(&row.values).map(|(field, value)| (field.name.clone(), value.clone())).collect::<BTreeMap<_, _>>().into())
    }
    fn document_bound(&self, row: &OwnedRow) -> Result<usize> {
        self.dependent.relation().schema().fields().iter().try_fold(row.bytes(), |bytes, field| bytes.checked_add(field.name.len()).and_then(|bytes| bytes.checked_add(512)).ok_or_else(|| invalid("row-size", "Apply document allocation overflow")))
    }
}
#[async_trait]
impl PageCursor for Apply {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.execution.context.check_cancelled()?;
        let Some(mut page) = self.input.next_page().await? else { return Ok(None); };
        let mut output = PageBuilder::new(page.len(),&self.execution.context.memory)?;
        while let Some(row) = page.next() {
            self.execution.context.check_cancelled()?;
            let row = Arc::new(row);
            let execution = self.execution.bind_arguments(&self.arguments,&self.schema,Arc::clone(&row)).await?;
            let mut cursor = build(Arc::clone(&self.dependent),execution.child(1)).await?;
            if matches!(self.kind, ApplyKind::Inner | ApplyKind::Left) {
                let mut matched = false;
                while let Some(dependent) = cursor.next().await? {
                    self.execution.context.check_cancelled()?;
                    matched = true;
                    output.push_growing(OwnedRow::copy_parts(&[&row.values,&dependent.values],0,0,&self.execution.context.memory)?,&self.execution.context.memory)?;
                }
                if self.kind == ApplyKind::Left && !matched {
                    output.push_growing(OwnedRow::copy_parts(&[&row.values],0,self.dependent.relation().schema().len(),&self.execution.context.memory)?,&self.execution.context.memory)?;
                }
                continue;
            }
            let first = cursor.next().await?;
            if matches!(self.kind, ApplyKind::Semi | ApplyKind::Anti) {
                if first.is_some() == (self.kind == ApplyKind::Semi) {
                    output.push_growing(OwnedRow::copy_parts(&[&row.values],0,0,&self.execution.context.memory)?,&self.execution.context.memory)?;
                }
                continue;
            }
            if self.kind == ApplyKind::Single {
                if cursor.next().await?.is_some() { return Err(crate::cardinality_violation()); }
                let result = match first {
                    Some(dependent) => OwnedRow::copy_parts(&[&row.values,&dependent.values],0,0,&self.execution.context.memory)?,
                    None => OwnedRow::copy_parts(&[&row.values],0,self.dependent.schema().len(),&self.execution.context.memory)?,
                };
                output.push_growing(result,&self.execution.context.memory)?;
                continue;
            }
            let output_scope = self.execution.scope()?;
            output_scope.reserve(row.bytes().checked_add(512).ok_or_else(|| invalid("row-size", "Apply output allocation overflow"))?)?;
            let result = match self.kind {
                ApplyKind::Exists => Value::Bool(first.is_some()),
                ApplyKind::Mark => apply_mark(&mut cursor, first, &self.execution).await?.map_or(Value::Null, Value::Bool),
                ApplyKind::Scalar | ApplyKind::Pull => {
                    if cursor.next().await?.is_some() { return Err(crate::cardinality_violation()); }
                    if let Some(row) = &first { output_scope.reserve(self.document_bound(row)?)?; }
                    first.as_ref().map_or(Value::Null, |row| if self.kind == ApplyKind::Scalar { row.values[0].clone() } else { self.document(row) })
                }
                ApplyKind::PullMany => {
                    let mut documents = Vec::new();
                    if let Some(row) = first { output_scope.reserve(self.document_bound(&row)?)?; documents.push(self.document(&row)); }
                    while let Some(row) = cursor.next().await? {
                        self.execution.context.check_cancelled()?;
                        output_scope.reserve(self.document_bound(&row)?)?;
                        documents.push(self.document(&row));
                    }
                    if documents.is_empty() { Value::Null } else { Value::List(documents) }
                }
                _ => unreachable!(),
            };
            let width = row.values.len().checked_add(1).ok_or_else(||invalid("row-size","Apply output width overflow"))?;
            let mut values = Vec::with_capacity(width);
            values.extend(row.values.iter().cloned());
            values.push(result);
            output.push_growing(OwnedRow::new(values,&self.execution.context.memory)?,&self.execution.context.memory)?;
        }
        Ok(Some(output.finish()))
    }
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
