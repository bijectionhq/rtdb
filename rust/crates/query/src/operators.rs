use std::collections::BTreeMap;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use async_trait::async_trait;
use rtdb_expr::{integer, is_true, null_eq_in_timezone};
use rtdb_plan::{ApplyKind, Expr, NamedExpr, Relation, RelationNode, Schema, SetOperator};
use rtdb_types::{LogicalType, Result, Value};

use crate::columnar::PredicateSelection;
use crate::compiled::{OperatorTemplate, Predicates, Projections, StackedFilters};
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
    build_batch_input_with_state(runtime, execution, pushdowns, output, false, false)
}

fn build_batch_input_with_state(
    runtime: Arc<EmittedRelation>, execution: Execution, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>, output: BatchOutput,
    reserve_state: bool, count_join_rows: bool,
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
        let Some((mut input, projected)) = build_batches_at(runtime, execution, pushdowns, output, count_join_rows).await? else { return Ok(None); };
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
    count_join_rows: bool,
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
            let cursor = crate::scan::ScanCursor::new(scan.clone(), execution.clone(), pushdowns).await?;
            let template = runtime.operators().template(|| operator_template(&runtime))?;
            let batches: BoxBatchCursor = match template.as_ref() {
                OperatorTemplate::Scan { filter: Some(predicates) } => cursor.filtered(predicates, execution)?,
                OperatorTemplate::Scan { filter: None } => Box::new(cursor),
                _ => return Err(template_mismatch()),
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
            let template = runtime.operators().template(|| operator_template(&runtime))?;
            let OperatorTemplate::Filter { single, stacked } = template.as_ref() else { return Err(template_mismatch()); };
            let mut input = runtime.child(0)?;
            let mut input_execution = execution.child(0);
            let mut state = execution.context.memory.reserve(0)?;
            // Each stage below the first is measured as the filter it was fused from.
            let mut stage_analysis = vec![None];
            let derived;
            let predicates = match stacked {
                Some(stacked) => {
                    for bound in stacked.bounds.iter() {
                        let lease = Arc::get_mut(&mut state).expect("Stacked filters own their reservation");
                        lease.grow_to(lease.bytes().checked_add(*bound).ok_or_else(|| invalid("operator-size", "Filter stages exceed addressable memory"))?)?;
                        stage_analysis.push(input_execution.analysis.clone());
                        input = input.child(0)?;
                        input_execution = input_execution.child(0);
                    }
                    &stacked.predicates
                }
                None => {
                    let mut stages = vec![predicate.clone()];
                    while let RelationNode::Filter { predicate, .. } = input.relation().node() {
                        let bound = crate::runtime_schema::local_allocation_bound(input.relation())?;
                        let lease = Arc::get_mut(&mut state).expect("Stacked filters own their reservation");
                        lease.grow_to(lease.bytes().checked_add(bound).ok_or_else(|| invalid("operator-size", "Filter stages exceed addressable memory"))?)?;
                        stages.push(predicate.clone());
                        stage_analysis.push(input_execution.analysis.clone());
                        input = input.child(0)?;
                        input_execution = input_execution.child(0);
                    }
                    stages.reverse();
                    derived = Predicates::new(Arc::clone(&single.schema), stages.into());
                    &derived
                }
            };
            stage_analysis.reverse();
            let Some((batches, projected)) = build_batch_input(input, input_execution, pushdowns, output).await? else { return Ok(None); };
            let context = Arc::clone(&execution.context);
            let filter = Box::new(crate::columnar::BatchFilter::admitted(batches, predicates, execution).observing_stages(stage_analysis));
            let filter = crate::columnar::retain_batch_state(filter, state);
            let filter = if output.semantic_pages() { crate::columnar::coalesce_pages(filter, context) } else { filter };
            Ok(Some((filter, projected)))
        }
        RelationNode::Project { expressions,.. } | RelationNode::Rename { expressions,.. } => {
            let template = runtime.operators().template(|| operator_template(&runtime))?;
            let OperatorTemplate::Project { positions, expressions: projections } = template.as_ref() else { return Err(template_mismatch()); };
            let child = runtime.child(0)?;
            let pushdowns = pushdowns.map(|filters|filters.project(expressions,&execution.context.memory)).transpose()?;
            let Some((batches,_)) = build_batch_input(child,execution.child(0),pushdowns,output).await? else { return Ok(None) };
            let batches: crate::columnar::BoxBatchCursor = match positions {
                Some(positions) => Box::new(crate::columnar::BatchProject::shared(batches,Arc::clone(positions),Arc::clone(&execution.context.memory))),
                None => Box::new(crate::columnar::BatchExpressions::admitted(batches,projections,execution)
                    .validating_input(projections.scalar_fallback)),
            };
            Ok(Some((batches,true)))
        }
        RelationNode::Extend { .. } => {
            let template = runtime.operators().template(|| operator_template(&runtime))?;
            let OperatorTemplate::Extend { expressions: projections } = template.as_ref() else { return Err(template_mismatch()); };
            let child = runtime.child(0)?;
            let Some((batches, _)) = build_batch_input(child, execution.child(0), pushdowns, output).await? else { return Ok(None); };
            let expressions = crate::columnar::BatchExpressions::admitted(batches, projections, execution).validating_input(projections.scalar_fallback);
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
            let Some((input, projected)) = build_batch_input(Arc::clone(&child), execution.child(0), None, output).await? else { return Ok(None); };
            if computed {
                let width = schema.len();
                let Some((input, schema, order)) = project_sort_keys(input, schema, order_by, &execution)? else { return Ok(None); };
                let positions = (0..width).chain(schema.len()..schema.len() + expressions.len()).collect();
                let memory = Arc::clone(&execution.context.memory);
                let window = crate::window::BatchWindow::new(input, schema, partition_by, order, expressions, true, execution)?;
                let window = crate::columnar::BatchProject::new(Box::new(window), positions, memory);
                return Ok(Some((Box::new(window), true)));
            }
            let input_order = if output.semantic_pages() { None } else {
                crate::window::InputOrder::bind(&child, partition_by, order_by, &execution.context.memory)
            };
            let window = crate::window::BatchWindow::with_input_order(
                input, schema, partition_by, order_by.clone(), expressions, projected, execution, input_order,
            )?;
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
            let count_join_rows = groups.is_empty() && !aggregates.is_empty()
                && matches!(child.relation().node(), RelationNode::Join { kind: rtdb_plan::JoinKind::Inner, .. })
                && aggregates.iter().all(|aggregate| aggregate.function.canonical() == rtdb_plan::AggregateFunction::CountStar
                    && aggregate.argument.is_none() && aggregate.filter.is_none() && !aggregate.distinct && aggregate.order_by.is_empty());
            let Some((input, projected)) = build_batch_input_with_state(
                child, execution.child(0), pushdowns, output, false, count_join_rows,
            ).await? else { return Ok(None); };
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
            let Some(probe_projected) = batch_shape(&probe)? else { return Ok(None); };
            if batch_shape(&build)?.is_none() { return Ok(None); }
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
            let schema = child.output();
            let dependent = runtime.child(1)?;
            let comparison = match condition {
                Some(condition) => Expr::Case { branches: vec![(condition.clone(), comparison.clone())],
                    otherwise: Box::new(Expr::Literal(Value::Bool(false))) },
                None => comparison.clone(),
            };
            let comparison_schema = schema.schema().concat(dependent.schema())?;
            let Some((input, projected)) = build_batch_input(child, execution.child(0), None, output).await? else { return Ok(None); };
            let arguments = crate::context::ApplyArguments::new(schema, arguments.clone());
            let plan = BatchApplyPlan { dependent, arguments, kind: ApplyKind::Mark,
                document_field: None, comparison: Some((comparison, comparison_schema)),
                inner_executions: apply_open_counter(&execution.context), execution };
            let apply = BatchApply { input, plan, document_reuse: None };
            Ok(Some((Box::new(apply), projected)))
        }
        RelationNode::Apply { arguments, kind: ApplyKind::LeftUnnest, .. } => {
            let child = runtime.child(0)?;
            let dependent = runtime.child(1)?;
            let expansion = BatchFilteredUnnestPlan::new(&child, &dependent, arguments)?;
            let schema = child.output();
            let Some((input, projected)) = build_batch_input(child, execution.child(0), None, output).await? else { return Ok(None); };
            let arguments = crate::context::ApplyArguments::new(schema, arguments.clone());
            let plan = BatchApplyPlan { dependent, arguments, kind: ApplyKind::Left,
                document_field: None, comparison: None,
                inner_executions: apply_open_counter(&execution.context), execution };
            let cursor: BoxBatchCursor = match expansion {
                Some(expansion) => Box::new(BatchFilteredUnnest { input, expansion, fallback: plan }),
                None => Box::new(BatchApply { input, plan, document_reuse: None }),
            };
            Ok(Some((cursor, projected)))
        }
        RelationNode::Apply { arguments, kind, .. } => {
            let child = runtime.child(0)?;
            let schema = child.output();
            let dependent = runtime.child(1)?;
            let Some((input, projected)) = build_batch_input(child, execution.child(0), None, output).await? else { return Ok(None); };
            let document_field = matches!(kind, ApplyKind::Pull | ApplyKind::PullMany)
                .then(|| runtime.schema().fields().last().cloned()).flatten();
            let document_reuse = document_field.as_ref()
                .and_then(|_| DocumentReuse::admit(dependent.relation(), schema.schema(), arguments));
            let arguments = crate::context::ApplyArguments::new(schema, arguments.clone());
            let plan = BatchApplyPlan { dependent, arguments, kind: *kind, document_field, comparison: None,
                inner_executions: apply_open_counter(&execution.context), execution };
            let apply = BatchApply { input, plan, document_reuse };
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
            let (Some(build_projected),Some(probe_projected)) = (batch_shape(&runtime.child(0)?)?,batch_shape(&runtime.child(1)?)?) else { return Ok(None) };
            let swapped = match relation.join_evidence() {
                Some(evidence) if *kind == rtdb_plan::JoinKind::Inner => !evidence.build_left,
                _ => !crate::join::build_left(left, right, *kind, &execution)?,
            };
            let (build_side,probe_side,build_relation,probe_relation) = if swapped { (1,0,right,left) } else { (0,1,left,right) };
            let mut layout = crate::join_key::JoinLayout::new(build_relation,probe_relation,condition.as_ref(),&execution.context.memory)?;
            layout.resolve_types(runtime.child(build_side)?.schema(),runtime.child(probe_side)?.schema(),&execution)?;
            let Some(order) = crate::batch_join::key_order(&layout) else { return Ok(None) };
            let (theta, residual) = match crate::batch_join::theta_tests(&layout, &execution.context.memory) {
                Some(theta) => (theta, None),
                None => (Default::default(), Some(crate::batch_join::Residual::new(&layout, &execution)?)),
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
            let join = if count_join_rows { join.counting_rows() } else { join };
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
    if batch_shape(&runtime)?.is_none() { return Ok(None); }
    Ok(build_batch_input_with_state(runtime, execution, None, output, true, false).await?.map(|(cursor, _)| cursor))
}

async fn build_materialized_columns(runtime: &Arc<EmittedRelation>, binding: &Arc<rtdb_plan::MaterializedBinding>,
    execution: Execution, output: BatchOutput) -> Result<Option<(crate::columnar::BoxBatchCursor, bool)>> {
    let body = runtime.child(1)?;
    let bound = runtime.child(0)?;
    if batch_shape(&body)?.is_none() || batch_shape(&bound)?.is_none() {
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
    let state_bound = runtime.operators().state_bound(|| {
        let relation = runtime.relation();
        let mut state_bound = crate::runtime_schema::local_allocation_bound(relation)?;
        state_bound = state_bound.checked_add(crate::runtime_schema::schema_bound(runtime.schema())?).ok_or_else(||invalid("operator-size","Operator schema copies exceed addressable memory"))?;
        for (index,_) in relation.children().iter().enumerate() { state_bound = state_bound.checked_add(crate::runtime_schema::schema_bound(runtime.child(index)?.schema())?).ok_or_else(|| invalid("operator-size", "Operator schema copies exceed addressable memory"))?; }
        Ok(state_bound)
    })?;
    execution.context.memory.reserve(state_bound)
}

/// Whether the subtree opens as batches, and if so whether they pass through a projection; the same for either batch output.
fn batch_shape(runtime: &Arc<EmittedRelation>) -> Result<Option<bool>> {
    runtime.operators().shape(|| stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || batch_shape_at(runtime)))
}

fn template_mismatch() -> rtdb_types::Anomaly { invalid("operator-template", "Operator template disagrees with its plan") }

/// What `runtime`'s node alone decides about its operator.
fn operator_template(runtime: &EmittedRelation) -> Result<OperatorTemplate> {
    let shaped = |expressions: &[NamedExpr]| expressions.iter().all(|named| crate::columnar::projection::shaped(&named.expr));
    Ok(match runtime.relation().node() {
        RelationNode::Scan(scan) => OperatorTemplate::Scan {
            filter: scan.predicates.iter().cloned().reduce(|left, right| Expr::binary(rtdb_plan::BinaryOp::And, left, right))
                .map(|predicate| Predicates::new(Arc::new(runtime.schema().clone()), vec![predicate].into())),
        },
        RelationNode::Filter { predicate, .. } => {
            let input = runtime.child(0)?;
            let schema = Arc::new(input.schema().clone());
            let single = Predicates::new(Arc::clone(&schema), vec![predicate.clone()].into());
            OperatorTemplate::Filter { single, stacked: stacked_filters(predicate, input, schema) }
        }
        RelationNode::Project { expressions, .. } | RelationNode::Rename { expressions, .. } => {
            let child = runtime.child(0)?;
            let positions = expressions.iter().map(|named| match &named.expr { Expr::Column { id, .. } => child.schema().position(*id), _ => None })
                .collect::<Option<Arc<[usize]>>>();
            let projections = Projections::new(Arc::new(child.schema().clone()), expressions.iter().map(|named| named.expr.clone()).collect(), !shaped(expressions));
            OperatorTemplate::Project { positions, expressions: projections }
        }
        RelationNode::Extend { expressions, .. } => {
            let child = runtime.child(0)?;
            let schema = child.schema();
            let projections = schema.fields().iter().map(rtdb_plan::Field::expression)
                .chain(expressions.iter().map(|named| named.expr.clone())).collect();
            OperatorTemplate::Extend { expressions: Projections::new(Arc::new(schema.clone()), projections, !shaped(expressions)) }
        }
        _ => OperatorTemplate::Other,
    })
}

/// The filters directly beneath `predicate`'s, applied with it by one operator; none where a bound cannot be decided, which
/// leaves the execution to decide it and report its error where it arises.
fn stacked_filters(predicate: &Expr, mut input: Arc<EmittedRelation>, schema: Arc<Schema>) -> Option<StackedFilters> {
    let mut stages = vec![predicate.clone()];
    let mut bounds = Vec::new();
    while let RelationNode::Filter { predicate, .. } = input.relation().node() {
        bounds.push(crate::runtime_schema::local_allocation_bound(input.relation()).ok()?);
        stages.push(predicate.clone());
        input = input.child(0).ok()?;
    }
    stages.reverse();
    Some(StackedFilters { predicates: Predicates::new(schema, stages.into()), bounds: bounds.into() })
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

fn batch_shape_at(runtime: &Arc<EmittedRelation>) -> Result<Option<bool>> {
    let relation = Arc::clone(runtime.relation());
    Ok(match relation.node() {
        RelationNode::Scan(_) => Some(false),
        RelationNode::Let { .. } => {
            if batch_shape(&runtime.child(0)?)?.is_none() { return Ok(None); }
            batch_shape(&runtime.child(1)?)?
        }
        RelationNode::Reference { .. } => Some(false),
        RelationNode::Values { .. } | RelationNode::LiteralPages { .. } | RelationNode::ArrowLiteralPages { .. } | RelationNode::List { .. } => Some(false),
        RelationNode::ColumnTable { .. } => Some(false),
        RelationNode::Filter { .. } => {
            let mut child = runtime.child(0)?;
            while matches!(child.relation().node(), RelationNode::Filter { .. }) {
                child = child.child(0)?;
            }
            batch_shape(&child)?
        }
        RelationNode::Project { .. } | RelationNode::Rename { .. } => {
            let child = runtime.child(0)?;
            batch_shape(&child)?.map(|_| true)
        }
        RelationNode::Extend { .. } => {
            let child = runtime.child(0)?;
            batch_shape(&child)?.map(|_| true)
        }
        RelationNode::Set { operator, .. } => {
            batch_shape(&runtime.child(0)?)?.zip(batch_shape(&runtime.child(1)?)?)
                .map(|(left, right)| left || (*operator == SetOperator::Union && right))
        }
        RelationNode::Distinct { .. } => batch_shape(&runtime.child(0)?)?,
        RelationNode::Aggregate { .. } => batch_shape(&runtime.child(0)?)?.map(|_| false),
        RelationNode::Mark { arguments, .. } if !arguments.is_empty() =>
            batch_shape(&runtime.child(0)?)?,
        RelationNode::Mark { .. } => {
            if batch_shape(&runtime.child(1)?)?.is_none() { return Ok(None); }
            batch_shape(&runtime.child(0)?)?
        }
        RelationNode::Apply { .. } =>
            batch_shape(&runtime.child(0)?)?,
        RelationNode::PatchGaps { .. } => batch_shape(&runtime.child(0)?)?,
        RelationNode::Join { kind: rtdb_plan::JoinKind::Cross, .. } => {
            batch_shape(&runtime.child(0)?)?.zip(batch_shape(&runtime.child(1)?)?).map(|(left, right)| left || right)
        }
        RelationNode::Join { .. } => {
            batch_shape(&runtime.child(0)?)?.zip(batch_shape(&runtime.child(1)?)?).map(|(left, right)| left || right)
        }
        RelationNode::Limit { .. } => batch_shape(&runtime.child(0)?)?,
        RelationNode::Unnest { .. } => batch_shape(&runtime.child(0)?)?,
        RelationNode::Ordinality { .. } => batch_shape(&runtime.child(0)?)?,
        RelationNode::Window { partition_by, order_by, expressions, .. } => {
            let child = runtime.child(0)?;
            if !crate::window::BatchWindow::supports(child.schema(), partition_by, &[], expressions) { return Ok(None); }
            let computed = !order_by.is_empty() && !crate::sort::batches::BatchSort::supports(child.schema(), order_by);
            batch_shape(&child)?.map(|projected| projected || computed)
        }
        RelationNode::Sort { order_by, .. } | RelationNode::SortLimit { order_by, .. } => {
            let child = runtime.child(0)?;
            let computed = !order_by.is_empty() && !crate::sort::batches::BatchSort::supports(child.schema(), order_by);
            batch_shape(&child)?.map(|projected| projected || computed)
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

/// DISTINCT over its input, holding every key it has seen in memory, as XTDB's `DistinctRelationMap` does; it fails
/// when the query budget cannot hold them rather than spilling.
struct BatchDistinct {
    input: Option<crate::columnar::BoxBatchCursor>,
    seen: crate::set_keys::BatchKeyIndex,
    schema: Arc<RuntimeSchema>,
    execution: Execution,
    projected: bool,
}

impl BatchDistinct {
    fn new(input: crate::columnar::BoxBatchCursor, schema: Arc<RuntimeSchema>, execution: Execution, projected: bool) -> Result<Self> {
        let seen = crate::set_keys::BatchKeyIndex::new(&execution.context)?;
        Ok(Self { input: Some(input), seen, schema, execution, projected })
    }
}

#[async_trait]
impl crate::columnar::BatchCursor for BatchDistinct {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        while let Some(input) = self.input.as_mut() {
            let Some(batch) = crate::columnar::poll_nested_batch(input.as_mut()).await? else {
                self.input = None;
                break;
            };
            if batch.len == 0 { continue; }
            let context = &self.execution.context;
            let memory = &context.memory;
            let selection = PredicateSelection::build(batch.len, memory, |bits| {
                self.seen.distinct(&batch, self.projected, bits, context, self.schema.schema(), |_| Ok(true))
            })?;
            if !selection.is_empty() { return batch.select_predicate(&selection, memory).map(Some); }
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
/// wall-clock time it reads as there, at any depth inside lists and structs.
pub(crate) fn equality_hash(values: &[Value], timezone: &str) -> u32 {
    values.iter().fold(0, |hash, value| combine_equality_hash(hash, equality_cell_hash(value, timezone)))
}

const TEMPORAL_HASH: u32 = 0x7e3a_11c5;

fn holds_temporal(value: &Value) -> bool {
    match value {
        Value::Timestamp { .. } | Value::DateDay(_) | Value::DateMilli(_) | Value::Date(_) | Value::TimestampTzRange { .. } => true,
        Value::Set(values) => values.iter().any(holds_temporal),
        Value::List(values) | Value::FixedSizeList(values) => match values.view() {
            rtdb_types::ListView::Values(values) => values.iter().any(holds_temporal),
            rtdb_types::ListView::Scalars(_) => values.element_type().legs().iter().any(|leg|
                matches!(leg, LogicalType::Scalar(rtdb_types::ScalarType::Timestamp { .. }
                    | rtdb_types::ScalarType::DateDay | rtdb_types::ScalarType::DateMilli))),
            rtdb_types::ListView::Union(_) => values.iter().any(|value| holds_temporal(value.as_ref())),
            _ => false,
        },
        Value::Struct(fields) => fields.values().any(holds_temporal),
        Value::Map { entries, .. } => entries.iter().any(|(key, value)| holds_temporal(key) || holds_temporal(value)),
        _ => false,
    }
}

pub(crate) fn equality_cell_hash(value: &Value, timezone: &str) -> u32 {
    match value {
        Value::Timestamp { .. } | Value::DateDay(_) | Value::DateMilli(_) => rtdb_expr::local_micros(value, timezone)
            .and_then(|micros| rtdb_columnar::hash_value(&Value::I64(micros), 0).ok()).map_or(TEMPORAL_HASH, |cell| (cell as u32) ^ TEMPORAL_HASH),
        // Lists and structs compare element by element under the session zone, so their date-times hash as above.
        Value::List(values) | Value::FixedSizeList(values) if holds_temporal(value) => values.iter()
            .fold(TEMPORAL_HASH ^ values.len() as u32, |hash, value| combine_equality_hash(hash, equality_cell_hash(value.as_ref(), timezone))),
        Value::Struct(fields) if holds_temporal(value) => fields.values()
            .fold(TEMPORAL_HASH ^ fields.len() as u32, |hash, value| combine_equality_hash(hash, equality_cell_hash(value, timezone))),
        // Ranges compare structurally; sets and maps do too, but their struct members compare regardless of field order.
        Value::TimestampTzRange { .. } => rtdb_columnar::hash_value(value, 0).map_or(TEMPORAL_HASH, |cell| cell as u32),
        Value::Set(_) | Value::Map { .. } if holds_temporal(value) => TEMPORAL_HASH ^ member_hash(value, timezone),
        value if holds_temporal(value) => TEMPORAL_HASH,
        value => rtdb_columnar::hash_value(value, 0).map_or(0x5bd1_e995, |cell| cell as u32),
    }
}

/// A hash that a set's or map's members equal under `rows_equal` share whatever order they arrive in: set members and
/// map entries add up rather than fold, and struct fields, which compare regardless of order there, add up by name.
fn member_hash(value: &Value, timezone: &str) -> u32 {
    let unordered = |tag: u32, len: usize, hashes: &mut dyn Iterator<Item = u32>| {
        hashes.fold(tag ^ len as u32, |sum, hash| sum.wrapping_add(combine_equality_hash(0, hash)))
    };
    match value {
        Value::Set(values) => unordered(0x51e7_0001, values.len(), &mut values.iter().map(|value| member_hash(value, timezone))),
        Value::Map { entries, .. } => unordered(0x3a90_0002, entries.len(), &mut entries.iter()
            .map(|(key, value)| combine_equality_hash(member_hash(key, timezone), member_hash(value, timezone)))),
        Value::Struct(fields) => unordered(0x57c7_0003, fields.len(), &mut fields.iter()
            .map(|(name, value)| combine_equality_hash(field_name_hash(name), member_hash(value, timezone)))),
        Value::List(values) | Value::FixedSizeList(values) => values.iter()
            .fold(0x11e7_0004 ^ values.len() as u32, |hash, value| combine_equality_hash(hash, member_hash(value.as_ref(), timezone))),
        value => equality_cell_hash(value, timezone),
    }
}

fn field_name_hash(name: &str) -> u32 {
    name.bytes().fold(0x811c_9dc5, |hash, byte| (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193))
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
            let selection = PredicateSelection::build(batch.len, memory, |bits| {
                let built = self.built.as_mut().ok_or_else(|| invalid("set-state", "Set input was not built"))?;
                let left_projected = self.left_projected;
                let all = self.all;
                let intersect = self.operator == SetOperator::Intersect;
                let mut retain = |row| {
                    let matched = built.matches(&batch, row, left_projected, all, context)?;
                    Ok(matched == intersect)
                };
                if all {
                    for row in 0..batch.len {
                        context.check_cancelled_at(row)?;
                        bits.append(retain(row)?);
                    }
                    Ok(())
                } else {
                    self.seen.distinct(&batch, left_projected, bits, context, self.runtime.child(0)?.schema(), retain)
                }
            })?;
            if !selection.is_empty() { return batch.select_predicate(&selection, memory).map(Some); }
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
        use crate::columnar::{BatchRow, Column, ColumnBatch, UnnestRows, pack_evaluated_column};

        let context = &self.execution.context;
        let memory = &context.memory;
        let too_large = || invalid("unnest-size", "Unnest output exceeds addressable memory");
        let scope = self.execution.scope()?;
        let invocation = self.execution.prepare_expression(&self.expr, &self.schema, &scope)?;
        let mut runs = UnnestRows::new(memory)?;
        let mut inventory = memory.reserve(256)?;
        let mut children = Vec::new();
        for row in 0..batch.len {
            context.check_cancelled_at(row)?;
            let evaluation = self.execution.scope()?;
            let input = BatchRow { batch, row, admit_dates: self.projected };
            let elements = self.execution.evaluate_list_invocation(
                &evaluation, &invocation, rtdb_expr::ExpressionRow::Borrowed(&input),
            ).await?;
            let count = elements.len();
            if count == 0 {
                if self.outer { runs.push(row, None, self.ordinality.is_some(), memory)?; }
                continue;
            }
            runs.push(row, Some((children.len() + 1, 0..count)), self.ordinality.is_some(), memory)?;
            if children.len() == children.capacity() {
                let capacity = children.capacity().checked_mul(2).ok_or_else(too_large)?.max(1);
                let bytes = capacity.checked_mul(std::mem::size_of::<ColumnBatch>())
                    .and_then(|bytes| bytes.checked_add(256)).ok_or_else(too_large)?;
                let charge = memory.reserve(bytes)?;
                children.reserve_exact(capacity - children.len());
                inventory = charge;
            }
            let (column, custody) = if let Some((field, array)) = elements.arrow_column(&evaluation)? {
                let bytes = array.get_array_memory_size().checked_add(field.size()).ok_or_else(too_large)?;
                (Column::Arrow { array, field, logical: None }, memory.reserve(bytes)?)
            } else {
                let values = elements.scalar_values().expect("non-Arrow list has evaluated cells");
                pack_evaluated_column(values, memory)?
            };
            let mut child = ColumnBatch::settled(Vec::new(), count, memory.reserve(256)?)?;
            child.append_column(column, custody, memory)?;
            children.push(child);
        }
        let output = batch.expand_evaluated_lists(runs, &children, self.ordinality.as_deref(), self.outer, context);
        drop((children, inventory));
        output
    }
}

struct BatchFilteredUnnestPlan {
    list: usize,
    scalar_arguments: Vec<usize>,
    schema: Schema,
    predicate: Expr,
    ordinality: Option<String>,
}

impl BatchFilteredUnnestPlan {
    fn new(input: &EmittedRelation, dependent: &EmittedRelation, arguments: &[(usize, Expr)]) -> Result<Option<Self>> {
        let Some((list, _, ordinality, predicate)) = crate::optimize::filtered_left_unnest_source(
            input.relation(), dependent.relation(), arguments,
        ) else { return Ok(None); };
        let mut scalar_arguments = Vec::new();
        for (_, expr) in arguments {
            let Expr::Column { id, data_type } = expr else { return Ok(None); };
            let Some(position) = input.schema().position(*id) else { return Ok(None); };
            let actual = &input.schema().fields()[position].data_type;
            if data_type.join(actual) != *data_type { return Ok(None); }
            if *id != list.id { scalar_arguments.push(position); }
        }
        for (declared, actual) in dependent.relation().schema().fields().iter().zip(dependent.schema().fields()) {
            let expected = declared.data_type.clone().maybe();
            if expected.join(&actual.data_type) != expected { return Ok(None); }
        }
        let Some(position) = input.schema().position(list.id) else { return Ok(None); };
        let schema = input.schema().concat(dependent.schema())?;
        let ordinality = ordinality.map(|field| field.name.clone());
        Ok(Some(Self { list: position, scalar_arguments, schema, predicate, ordinality }))
    }
}

struct BatchFilteredUnnest {
    input: crate::columnar::BoxBatchCursor,
    expansion: BatchFilteredUnnestPlan,
    fallback: BatchApplyPlan,
}

#[async_trait]
impl crate::columnar::BatchCursor for BatchFilteredUnnest {
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        self.fallback.trace_opens(span);
    }

    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        let execution = &self.fallback.execution;
        let context = &execution.context;
        let plan = &self.expansion;
        loop {
            context.check_cancelled()?;
            let Some(batch) = crate::columnar::poll_nested_batch(self.input.as_mut()).await? else { return Ok(None); };
            batch.validate_readable(context)?;
            if batch.len == 0 { continue; }
            if plan.scalar_arguments.iter().all(|&pos| batch.compatible_unnest_argument(pos, &plan.schema.fields()[pos].data_type))
                && let Some(output) = batch.expand_filtered_list(plan.list, &plan.schema, &plan.predicate, plan.ordinality.as_deref(), execution).await?
            {
                return Ok(Some(output));
            }
            if let Some(output) = self.fallback.joined(&Arc::new(batch)).await? {
                return Ok(Some(output));
            }
        }
    }
}

struct BatchApply {
    input: crate::columnar::BoxBatchCursor,
    plan: BatchApplyPlan,
    document_reuse: Option<DocumentReuse>,
}

#[derive(Clone, Copy)]
enum DocumentReuse { Uncorrelated, I64Column(usize) }

impl DocumentReuse {
    fn admit(mut dependent: &Relation, outer: &Schema, arguments: &[(usize, Expr)]) -> Option<Self> {
        use rtdb_plan::{BinaryOp, TemporalFilter};
        let i64_type = LogicalType::Scalar(rtdb_types::ScalarType::I64);
        let is_i64 = |ty: &LogicalType| *ty == i64_type || *ty == i64_type.clone().maybe();
        let (reuse, mut parameter) = match arguments {
            [] => (Self::Uncorrelated, None),
            [(index, Expr::Column { id, data_type })] if is_i64(data_type)
                && outer.get(*id).is_some_and(|field| field.data_type == *data_type) =>
                (Self::I64Column(outer.position(*id)?), Some(*index)),
            _ => return None,
        };
        let is_key = |predicate: &Expr, schema: &Schema, parameter: usize| {
            let Expr::Binary { op: BinaryOp::Eq, left, right } = predicate else { return false };
            [(left.as_ref(), right.as_ref()), (right.as_ref(), left.as_ref())].into_iter().any(|(key, capture)| {
                matches!(key, Expr::Column { id, data_type } if is_i64(data_type)
                    && schema.get(*id).is_some_and(|field| field.data_type == *data_type))
                    && matches!(capture, Expr::Parameter { index, data_type } if *index == parameter && is_i64(data_type))
            })
        };
        loop {
            match dependent.node() {
                RelationNode::Scan(scan) if scan.matches.is_empty() && scan.top_k.is_none() && scan.ordered_top_k.is_none()
                    && matches!(scan.valid_time, TemporalFilter::Default | TemporalFilter::All)
                    && matches!(scan.system_time, TemporalFilter::Default | TemporalFilter::All) =>
                    return match (parameter, scan.predicates.as_slice()) {
                        (None, []) => Some(reuse),
                        (Some(index), [predicate]) if is_key(predicate, dependent.schema(), index) => Some(reuse),
                        _ => None,
                    },
                RelationNode::Reference { .. } if parameter.is_none() => return Some(reuse),
                RelationNode::Filter { input, predicate } if parameter.is_some_and(|index| is_key(predicate, input.schema(), index)) => {
                    parameter = None;
                    dependent = input;
                }
                RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. }
                    if expressions.iter().all(|named| matches!(named.expr, Expr::Column { .. })) => dependent = input,
                RelationNode::Sort { input, order_by } | RelationNode::SortLimit { input, order_by, offset: None, fetch: None }
                    if order_by.iter().all(|key| matches!(key.expr, Expr::Column { .. })) => dependent = input,
                _ => return None,
            }
        }
    }

    fn key(self, batch: &crate::columnar::ColumnBatch, row: usize) -> Option<Option<i64>> {
        use arrow_array::{Array, Int64Array};
        use crate::columnar::Column;
        let Self::I64Column(position) = self else { return Some(None) };
        let column = batch.columns.get(position)?;
        if let Column::Values(values) = column {
            return match values.get(row)? {
                Value::I64(value) => Some(Some(*value)),
                Value::Null => Some(None),
                _ => None,
            };
        }
        let Some((array, field, logical, offset)) = column.arrow_cell(row)? else { return Some(None) };
        if array.data_type() == &arrow_schema::DataType::Null { return Some(None); }
        let i64_type = LogicalType::Scalar(rtdb_types::ScalarType::I64);
        if !logical.map_or_else(|| field.metadata().is_empty(), |ty| *ty == i64_type || *ty == i64_type.clone().maybe()) {
            return None;
        }
        let integers = array.as_any().downcast_ref::<Int64Array>()?;
        Some((!integers.is_null(offset)).then(|| integers.value(offset)))
    }
}

fn apply_open_counter(context: &crate::QueryContext) -> Option<AtomicU64> {
    crate::trace::operator_evidence_enabled(context).then(|| AtomicU64::new(0))
}

struct BatchApplyPlan {
    dependent: Arc<EmittedRelation>,
    arguments: crate::context::ApplyArguments,
    kind: ApplyKind,
    document_field: Option<rtdb_plan::Field>,
    comparison: Option<(Expr, Schema)>,
    inner_executions: Option<AtomicU64>,
    execution: Execution,
}

impl BatchApplyPlan {
    fn trace_opens(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        if let Some(opens) = &self.inner_executions {
            crate::trace::OperatorCounter::InnerExecutions.record(span, opens.load(Ordering::Relaxed));
        }
    }

    async fn bind(&self, batch: &Arc<crate::columnar::ColumnBatch>, row: usize) -> Result<Execution> {
        self.arguments.bind(&self.execution, Arc::clone(batch), row).await
    }

    async fn dependent_batches(&self, batch: &Arc<crate::columnar::ColumnBatch>, row: usize) -> Result<crate::columnar::BoxBatchCursor> {
        self.open_dependent(self.bind(batch, row).await?).await
    }

    async fn open_dependent(&self, execution: Execution) -> Result<crate::columnar::BoxBatchCursor> {
        if let Some(opens) = &self.inner_executions {
            let current = opens.load(Ordering::Relaxed);
            opens.store(current.saturating_add(1), Ordering::Relaxed);
        }
        let execution = execution.child(1);
        let runtime = Arc::clone(&self.dependent);
        let input = if matches!(self.kind, ApplyKind::Inner | ApplyKind::Left | ApplyKind::PullMany) {
            build_columns(runtime, execution).await?
        } else {
            build_column_pages(runtime, execution).await?
        };
        input.ok_or_else(|| invalid("apply-input", "Dependent plan cannot open as Arrow"))
    }

    async fn documents(&self, batch: Arc<crate::columnar::ColumnBatch>, output: &rtdb_plan::Field, reuse: Option<DocumentReuse>) -> Result<crate::columnar::ColumnBatch> {
        use crate::columnar::{apply::ApplyPages, poll_nested_batch};

        let context = &self.execution.context;
        let memory = &context.memory;
        let many = self.kind == ApplyKind::PullMany;
        let fields = self.dependent.schema().fields();
        let width = fields.len().checked_mul(256).ok_or_else(|| invalid("row-size", "Apply document fields overflow"))?;
        let _fields = memory.reserve(width)?;
        let order: BTreeMap<_, _> = fields.iter().enumerate().map(|(index, field)| (field.name.as_str(), index)).collect();
        let positions = order.values().copied().collect::<Vec<_>>();
        let schema = Schema::new(positions.iter().map(|&position| fields[position].clone()).collect())?;
        let mut pages = ApplyPages::new(memory)?;
        let mut list = if many {
            let bytes = batch.len.checked_mul(32).and_then(|bytes| bytes.checked_add(4096))
                .ok_or_else(|| invalid("row-size", "Apply list offsets overflow"))?;
            let reservation = memory.reserve(bytes)?;
            let mut offsets = Vec::with_capacity(batch.len + 1);
            offsets.push(0i32);
            Some((offsets, Vec::with_capacity(batch.len), reservation))
        } else {
            None
        };
        // The cache borrows completed spans from this outer page; a later page still opens its own dependent.
        const REUSE_KEYS: usize = 16;
        let reuse = reuse.filter(|_| batch.len > 1);
        let cache_charge = reuse.and_then(|_| memory.reserve(1024).ok());
        let mut completed: Vec<(Option<i64>, std::ops::Range<usize>)> =
            Vec::with_capacity(if cache_charge.is_some() { REUSE_KEYS } else { 0 });
        for row in 0..batch.len {
            context.check_cancelled_at(row)?;
            let bound = self.bind(&batch, row).await?;
            let key = reuse.filter(|_| cache_charge.is_some()).and_then(|reuse| reuse.key(&batch, row));
            let before = pages.len();
            if let Some((_, spans)) = completed.iter().find(|(previous, _)| Some(*previous) == key) {
                drop(bound);
                pages.repeat(row, spans.clone(), context)?;
            } else {
                let mut input = self.open_dependent(bound).await?;
                let first = pages.span_count();
                while let Some(page) = poll_nested_batch(input.as_mut()).await? {
                    if page.len == 0 { continue; }
                    page.validate_readable(context)?;
                    if !many && (pages.len() != before || page.len > 1) { return Err(crate::cardinality_violation()); }
                    pages.end_after(page.len)?;
                    let doc = page.apply_document(&positions, &schema, memory)?;
                    pages.push(row, Some(doc))?;
                }
                if let Some(key) = key && completed.len() < REUSE_KEYS {
                    completed.push((key, first..pages.span_count()));
                }
            }
            let found = before < pages.len();
            if let Some((offsets, valid, _)) = &mut list {
                offsets.push(pages.len() as i32);
                valid.push(found);
            } else if !found {
                pages.push(row, None)?;
            }
        }
        context.check_cancelled()?;
        let mut batch = Arc::try_unwrap(batch).map_err(|_| invalid("apply-input", "Apply document input remains borrowed"))?;
        if let Some((offsets, valid, reservation)) = list {
            batch.append_apply_lists(&pages, offsets, valid, output, reservation, memory)?;
        } else {
            let (column, charge) = pages.dependent_column(0, memory)?;
            batch.append_column(column, charge, memory)?;
        }
        Ok(batch)
    }

    async fn joined(&self, batch: &Arc<crate::columnar::ColumnBatch>) -> Result<Option<crate::columnar::ColumnBatch>> {
        use crate::columnar::{apply::ApplyPages, poll_nested_batch};
        let context = &self.execution.context;
        let mut pages = ApplyPages::new(&context.memory)?;
        let single = matches!(self.kind, ApplyKind::Single | ApplyKind::Scalar);
        for source_row in 0..batch.len {
            context.check_cancelled_at(source_row)?;
            let mut cursor = self.dependent_batches(batch, source_row).await?;
            let before = pages.len();
            while let Some(output) = poll_nested_batch(cursor.as_mut()).await? {
                if output.len == 0 { continue; }
                output.validate_readable(context)?;
                if single && (output.len > 1 || pages.len() != before) { return Err(crate::cardinality_violation()); }
                pages.push(source_row, Some(output))?;
            }
            if pages.len() == before && (self.kind == ApplyKind::Left || single) { pages.push(source_row, None)?; }
        }
        if pages.len() == 0 { return Ok(None); }
        pages.joined(batch, self.dependent.schema().len(), context).map(Some)
    }
}

#[async_trait]
impl crate::columnar::BatchCursor for BatchApply {
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        self.plan.trace_opens(span);
    }

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
            if let Some(field) = &plan.document_field { return plan.documents(batch, field, self.document_reuse).await.map(Some); }
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

    fn timestamp(value: i64, unit: rtdb_types::TimeUnit, timezone: Option<&str>) -> Value {
        Value::Timestamp { value, unit, timezone: timezone.map(str::to_owned) }
    }

    fn holder(value: Value) -> Value {
        Value::Struct(rtdb_types::StructFields::from([("at".to_owned(), Value::list(vec![value])), ("n".to_owned(), Value::I64(1))]))
    }

    #[test]
    fn nested_date_times_equal_under_the_session_zone_share_a_hash() {
        use rtdb_types::TimeUnit::{Microsecond, Millisecond};
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let micros = [holder(timestamp(86_400_000_000, Microsecond, None))];
        let equal = [
            [holder(timestamp(86_400_000, Millisecond, None))],
            [holder(timestamp(86_400_000_000, Microsecond, Some("UTC")))],
            [holder(Value::DateDay(1))],
        ];
        assert!(rows_equal(&micros, &equal[0], "UTC", &memory).unwrap());
        for other in &equal {
            if rows_equal(&micros, other, "UTC", &memory).unwrap() {
                assert_eq!(equality_hash(&micros, "UTC"), equality_hash(other, "UTC"), "{other:?}");
            }
        }
        let later = [holder(timestamp(86_400_000_001, Microsecond, None))];
        assert!(!rows_equal(&micros, &later, "UTC", &memory).unwrap());
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn sets_and_maps_of_date_times_hash_by_their_members_in_any_order() {
        use rtdb_types::TimeUnit::{Microsecond, Millisecond};
        let at = |seconds: i64| timestamp(seconds * 1_000_000, Microsecond, None);
        let set = Value::Set(vec![at(1), at(2), Value::DateDay(3)]);
        let reordered = Value::Set(vec![Value::DateDay(3), timestamp(2_000, Millisecond, None), at(1)]);
        assert_eq!(equality_hash(&[set], "UTC"), equality_hash(&[reordered], "UTC"));
        let structs = |first: bool| {
            let fields = [("at".to_owned(), at(1)), ("n".to_owned(), Value::I64(1))];
            let fields = if first { fields } else { let [a, b] = fields; [b, a] };
            Value::Set(vec![Value::Struct(rtdb_types::StructFields::from(fields))])
        };
        assert_eq!(equality_hash(&[structs(true)], "UTC"), equality_hash(&[structs(false)], "UTC"));
        let map = |entries: Vec<(Value, Value)>| Value::Map { entries, sorted: false };
        assert_eq!(
            equality_hash(&[map(vec![(Value::I64(1), at(1)), (Value::I64(2), at(2))])], "UTC"),
            equality_hash(&[map(vec![(Value::I64(2), at(2)), (Value::I64(1), at(1))])], "UTC"),
        );
        let sets: std::collections::HashSet<u32> = (0..1_000i64)
            .map(|second| equality_hash(&[Value::Set(vec![at(second), at(second + 1)])], "UTC"))
            .collect();
        assert!(sets.len() > 990, "same-size sets spread: {} distinct hashes", sets.len());
    }

    #[test]
    fn distinct_nested_date_times_spread_across_hashes() {
        let hashes: std::collections::HashSet<u32> = (0..1_000i64)
            .map(|second| equality_hash(&[holder(timestamp(second * 1_000_000, rtdb_types::TimeUnit::Microsecond, None))], "UTC"))
            .collect();
        assert!(hashes.len() > 990, "{} distinct hashes", hashes.len());
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

#[cfg(test)]
mod template_tests {
    use super::*;
    use rtdb_columnar::MemoryBudget;
    use rtdb_plan::{ColumnId, Field, UnaryOp};
    use rtdb_types::ScalarType;

    fn catalog() -> crate::CatalogSnapshot {
        let storage = Arc::new(rtdb_storage::cache::BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20));
        let database = crate::DatabaseSnapshot { partitions: vec![], storage };
        crate::CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap()
    }

    /// `$1` as `a`, extended with `a IS NULL` as `b`, under two stacked filters and a reordering projection.
    fn plan() -> Arc<Relation> {
        let field = |id: u32, name: &str, data_type: LogicalType| Field { id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false,
            unnamed: false, data_type };
        let a = field(0, "a", LogicalType::Scalar(ScalarType::I64));
        let b = field(1, "b", LogicalType::Scalar(ScalarType::Bool));
        let values = Arc::new(Relation::new(Schema::new(vec![a.clone()]).unwrap(),
            RelationNode::Values { rows: vec![vec![Expr::Parameter { index: 0, data_type: a.data_type.clone() }]] }).unwrap());
        let extended = Arc::new(values.extend(vec![NamedExpr { field: b.clone(), expr: Expr::unary(UnaryOp::IsNull, a.expression()) }]).unwrap());
        let inner = Arc::new(extended.filter(Expr::unary(UnaryOp::IsNotNull, a.expression())).unwrap());
        let outer = Arc::new(inner.filter(Expr::unary(UnaryOp::Not, b.expression())).unwrap());
        Arc::new(outer.project(vec![NamedExpr { field: b.clone(), expr: b.expression() }, NamedExpr { field: a.clone(), expr: a.expression() }]).unwrap())
    }

    fn nodes(root: &Arc<EmittedRelation>) -> Vec<Arc<EmittedRelation>> {
        let mut nodes = vec![Arc::clone(root)];
        let mut next = 0;
        while let Some(node) = nodes.get(next).cloned() {
            nodes.extend((0..2).map_while(|index| node.child(index).ok()));
            next += 1;
        }
        nodes
    }

    #[test]
    fn a_kept_compiled_plan_keeps_what_a_fresh_build_decides() {
        let relation = plan();
        let (keeper, query) = (MemoryBudget::new(1 << 24), MemoryBudget::new(1 << 24));
        let compiled = crate::compiled::CompiledQuery::kept_by(Some(Arc::clone(&keeper)));
        let integers = [LogicalType::Scalar(ScalarType::I64)];
        let mode = crate::runtime_schema::EmissionMode::Execution;
        let kept = nodes(&compiled.emit(&relation, &catalog(), &integers, &query, mode).unwrap());
        let fresh = nodes(&EmittedRelation::emit(Arc::clone(&relation), &catalog(), &integers, &query, mode).unwrap());
        assert_eq!(kept.len(), fresh.len());
        assert!(fresh.iter().any(|node| matches!(operator_template(node).unwrap(),
            OperatorTemplate::Filter { stacked: Some(ref stacked), .. } if stacked.bounds.len() == 1)));
        assert!(fresh.iter().any(|node| matches!(operator_template(node).unwrap(), OperatorTemplate::Project { positions: Some(_), .. })));
        assert!(fresh.iter().any(|node| matches!(operator_template(node).unwrap(), OperatorTemplate::Extend { .. })));
        let before = keeper.used_bytes();
        for (kept, fresh) in kept.iter().zip(&fresh) {
            let template = kept.operators().template(|| operator_template(kept)).unwrap();
            assert_eq!(*template, operator_template(fresh).unwrap());
            let again = kept.operators().template(|| panic!("a kept template is built once")).unwrap();
            assert!(Arc::ptr_eq(&template, &again));
            assert_eq!(batch_shape(kept).unwrap(), batch_shape_at(fresh).unwrap());
            assert_eq!(batch_shape(kept).unwrap(), batch_shape_at(fresh).unwrap());
        }
        assert!(keeper.used_bytes() > before);
        drop(fresh);
        assert_eq!(query.used_bytes(), 0);
        drop((kept, compiled));
        assert_eq!(keeper.used_bytes(), 0);
    }
}
