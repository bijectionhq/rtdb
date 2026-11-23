use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_expr::{integer, is_true, null_eq_in_timezone};
use rtdb_plan::{ApplyKind, Expr, NamedExpr, Relation, RelationNode, Schema, SetOperator};
use rtdb_types::{Result, Value};

use crate::context::Execution;
use crate::cursor::{BoxCursor, OwnedRow};
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

fn vector_limit(offset: Option<&Expr>, fetch: &Expr) -> bool {
    let fixed = |expr: &Expr| matches!(expr, Expr::Literal(_) | Expr::Parameter { .. });
    offset.is_none_or(fixed) && fixed(fetch)
}

/// Builds a scan, its filters and column-only projections as a batch source, or `None` where the subtree has another shape.
/// The boolean reports whether the batches passed through a projection.
pub(crate) fn build_batches(runtime: Arc<EmittedRelation>, execution: Execution, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>) -> futures::future::BoxFuture<'static, Result<Option<(crate::columnar::BoxBatchCursor, bool)>>> { Box::pin(async move {
    let relation = Arc::clone(runtime.relation());
    match relation.node() {
        RelationNode::Scan(scan) if !crate::inspection::is_inspection(&scan.table.name.table) && crate::system_schema::definition(&scan.table.name.table).is_none() => {
            let batches: crate::columnar::BoxBatchCursor = Box::new(crate::scan::ScanCursor::new(scan.clone(),execution.clone(),pushdowns).await?);
            let batches = match scan.predicates.iter().cloned().reduce(|left,right|Expr::binary(rtdb_plan::BinaryOp::And,left,right)) {
                Some(predicate) => Box::new(crate::columnar::BatchFilter::new(batches,runtime.schema().clone(),predicate,execution)) as crate::columnar::BoxBatchCursor,
                None => batches,
            };
            Ok(Some((batches,false)))
        }
        RelationNode::Filter { predicate,.. } => {
            if matches!(runtime.child(0)?.relation().node(), RelationNode::Filter { .. }) { return Ok(None); }
            let Some((batches,projected)) = build_batches(runtime.child(0)?,execution.child(0),pushdowns).await? else { return Ok(None) };
            if projected { return Ok(None); }
            Ok(Some((Box::new(crate::columnar::BatchFilter::new(batches,runtime.child(0)?.schema().clone(),predicate.clone(),execution)) as crate::columnar::BoxBatchCursor,false)))
        }
        RelationNode::Project { expressions,.. } | RelationNode::Rename { expressions,.. } => {
            let child = runtime.child(0)?;
            let positions = expressions.iter().map(|named| match &named.expr { Expr::Column { id,.. } => child.schema().position(*id), _ => None }).collect::<Option<Vec<_>>>();
            if positions.is_none() && !expressions.iter().all(|named| crate::columnar::projection::shaped(&named.expr)) { return Ok(None); }
            let schema = child.schema().clone();
            let pushdowns = pushdowns.map(|filters|filters.project(expressions,&execution.context.memory)).transpose()?;
            let Some((batches,_)) = build_batches(child,execution.child(0),pushdowns).await? else { return Ok(None) };
            let batches: crate::columnar::BoxBatchCursor = match positions {
                Some(positions) => Box::new(crate::columnar::BatchProject::new(batches,positions,Arc::clone(&execution.context.memory))),
                None => Box::new(crate::columnar::BatchExpressions::new(batches,schema,expressions.iter().map(|named|named.expr.clone()).collect(),execution)),
            };
            Ok(Some((batches,true)))
        }
        RelationNode::SortLimit { order_by, offset, fetch: Some(fetch), .. } if vector_limit(offset.as_ref(), fetch) && crate::cosine::in_projection(&relation) => {
            let child = runtime.child(0)?;
            let offset = row_limit(offset.as_ref(), &execution).await?.unwrap_or(0);
            let fetch = row_limit(Some(fetch), &execution).await?.unwrap_or(i64::MAX as u64);
            let limit = offset.saturating_add(fetch).min(i64::MAX as u64);
            if order_by.is_empty() || !crate::sort::SortCursor::batch_eligible(child.schema(), order_by, limit) { return Ok(None); }
            let schema = child.schema().clone();
            let Some((input, projected)) = build_batches(child, execution.child(0), None).await? else { return Ok(None); };
            Ok(Some((Box::new(crate::sort::ColumnTop::new(input, schema, order_by.clone(), projected, limit, offset, execution)) as crate::columnar::BoxBatchCursor, projected)))
        }
        RelationNode::Join { left,right,kind: rtdb_plan::JoinKind::Inner,condition: Some(condition) } if execution.analysis.is_none() && execution.context.tracer.is_none() => {
            let (Some(build_projected),Some(probe_projected)) = (batch_shape(&runtime.child(0)?)?,batch_shape(&runtime.child(1)?)?) else { return Ok(None) };
            if build_projected != probe_projected || !crate::join::build_left(left,right,rtdb_plan::JoinKind::Inner,&execution)? { return Ok(None); }
            // The left input is built unless it would spill and the right one fits, in which case the right one is built
            // and the output keeps its left-then-right columns: a spilled build orders its output differently anyway.
            let threshold = execution.context.options.join_build_rows;
            let swapped = crate::join::rows_at_most(left,&execution)? > threshold && crate::join::rows_at_most(right,&execution)? <= threshold;
            let (build_side,probe_side,build_relation,probe_relation) = if swapped { (1,0,right,left) } else { (0,1,left,right) };
            let mut layout = crate::join_key::JoinLayout::new(build_relation,probe_relation,Some(condition),&execution.context.memory)?;
            layout.resolve_types(runtime.child(build_side)?.schema(),runtime.child(probe_side)?.schema(),&execution)?;
            let Some(order) = crate::batch_join::key_order(&layout) else { return Ok(None) };
            let Some((build,projected)) = build_batches(runtime.child(build_side)?,execution.child(build_side),pushdowns.clone()).await? else { return Ok(None) };
            if projected != build_projected { return Ok(None); }
            let probe = crate::batch_join::ProbeInput { runtime: runtime.child(probe_side)?,execution: execution.child(probe_side),pushdowns };
            let unswapped = if swapped {
                let mut layout = crate::join_key::JoinLayout::new(left,right,Some(condition),&execution.context.memory)?;
                layout.resolve_types(runtime.child(0)?.schema(),runtime.child(1)?.schema(),&execution)?;
                Some(layout)
            } else { None };
            Ok(Some((Box::new(crate::batch_join::BatchJoin::new(build,probe,layout,order,projected,execution).swapped(unswapped)) as crate::columnar::BoxBatchCursor,projected)))
        }
        _ => Ok(None),
    }
}) }

/// Opens a whole plan as a batch source where `build_batches` takes its shape, charging the root operator's state as
/// `build_pages` does; `None` where the plan runs as pages.
pub(crate) async fn build_columns(runtime: Arc<EmittedRelation>, execution: Execution) -> Result<Option<crate::columnar::BoxBatchCursor>> {
    execution.context.check_cancelled()?;
    if execution.analysis.is_some() || execution.context.tracer.is_some() || batch_shape(&runtime)?.is_none() { return Ok(None); }
    let reservation = state_reservation(&runtime, &execution)?;
    let Some((batches, _)) = build_batches(runtime, execution, None).await? else { return Ok(None) };
    Ok(Some(crate::columnar::retain_batch_state(batches, reservation)))
}

fn state_reservation(runtime: &Arc<EmittedRelation>, execution: &Execution) -> Result<Arc<rtdb_columnar::MemoryReservation>> {
    let relation = runtime.relation();
    let mut state_bound = crate::runtime_schema::local_allocation_bound(relation)?;
    state_bound = state_bound.checked_add(crate::runtime_schema::schema_bound(runtime.schema())?).ok_or_else(||invalid("operator-size","Operator schema copies exceed addressable memory"))?;
    for (index,_) in relation.children().iter().enumerate() { state_bound = state_bound.checked_add(crate::runtime_schema::schema_bound(runtime.child(index)?.schema())?).ok_or_else(|| invalid("operator-size", "Operator schema copies exceed addressable memory"))?; }
    execution.context.memory.reserve(state_bound)
}

/// Whether `build_batches` opens this subtree as batches, and if so whether they pass through a projection.
fn batch_shape(runtime: &Arc<EmittedRelation>) -> Result<Option<bool>> {
    let relation = Arc::clone(runtime.relation());
    Ok(match relation.node() {
        RelationNode::Scan(scan) => (!crate::inspection::is_inspection(&scan.table.name.table) && crate::system_schema::definition(&scan.table.name.table).is_none()).then_some(false),
        RelationNode::Filter { .. } => {
            if matches!(runtime.child(0)?.relation().node(), RelationNode::Filter { .. }) { return Ok(None); }
            match batch_shape(&runtime.child(0)?)? { Some(false) => Some(false), _ => None }
        }
        RelationNode::Project { expressions,.. } | RelationNode::Rename { expressions,.. } => {
            let child = runtime.child(0)?;
            let columns = expressions.iter().all(|named| matches!(&named.expr, Expr::Column { id,.. } if child.schema().position(*id).is_some()));
            if !columns && !expressions.iter().all(|named| crate::columnar::projection::shaped(&named.expr)) { return Ok(None); }
            batch_shape(&child)?.map(|_| true)
        }
        RelationNode::SortLimit { offset, fetch: Some(fetch), .. } if vector_limit(offset.as_ref(), fetch) && crate::cosine::in_projection(&relation) => batch_shape(&runtime.child(0)?)?,
        _ => None,
    })
}

fn build_pages_constrained(runtime: Arc<EmittedRelation>, execution: Execution, constraints: crate::inspection::Constraints, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>) -> futures::future::BoxFuture<'static, Result<crate::pages::BoxPageCursor>> { Box::pin(async move {
    execution.context.check_cancelled()?;
    let relation = Arc::clone(runtime.relation());
    let state_reservation = state_reservation(&runtime, &execution)?;
    let traced_relation = Arc::clone(&relation);
    let traced_context = Arc::clone(&execution.context);
    let analysis = execution.analysis.clone();
    if let RelationNode::PatchGaps { input, iid, valid_from, valid_to, document, from, to } = relation.node() {
        let input_pages = build_pages_constrained(runtime.child(0)?, execution.child(0), crate::inspection::Constraints::default(), None).await?;
        let pages = Box::new(crate::patch::PatchGaps::new(input_pages, input.schema(), [*iid, *valid_from, *valid_to, *document], from, to, execution).await?);
        let pages = crate::pages::retain_page_state(pages, state_reservation);
        return Ok(crate::trace::wrap(crate::analysis::wrap(pages, analysis), &traced_relation, &traced_context));
    }
    if let RelationNode::Project { expressions,.. } | RelationNode::Rename { expressions,.. } = relation.node()
        && execution.analysis.is_none() && execution.context.tracer.is_none()
        && !expressions.iter().all(|named| matches!(named.expr, Expr::Column { .. })) && expressions.iter().all(|named| crate::columnar::projection::shaped(&named.expr))
        && let Some((batches,_)) = build_batches(Arc::clone(&runtime),execution.clone(),pushdowns.clone()).await? {
        let pages: BoxPageCursor = Box::new(crate::columnar::BatchRows::new(batches,Arc::clone(&execution.context.memory)));
        let pages = crate::pages::retain_page_state(pages,state_reservation);
        return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
    }
    match relation.node() {
        RelationNode::Join { kind: rtdb_plan::JoinKind::Cross,.. } => {
            let left = build_pages_constrained(runtime.child(0)?,execution.child(0),constraints.clone(),pushdowns.clone()).await?;
            let right = build_pages_constrained(runtime.child(1)?,execution.child(1),constraints,pushdowns).await?;
            let pages = Box::new(crate::cross::CrossJoin::new(left,right,Arc::clone(&traced_context)));
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
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
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::Values { rows } => {
            let page = materialize_rows(rows, &execution).await?;
            let pages = crate::pages::single_page(page, Arc::clone(&execution.context));
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::LiteralPages { .. } => {
            let pages = Box::new(LiteralPageCursor { relation, next: 0, execution });
            let pages = crate::pages::retain_page_state(pages, state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages, analysis), &traced_relation, &traced_context));
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
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::Project { expressions,.. } | RelationNode::Rename { expressions,.. } => {
            let pushdowns = pushdowns.map(|filters|filters.project(expressions,&execution.context.memory)).transpose()?;
            let pages = build_pages_constrained(runtime.child(0)?,execution.child(0),constraints.through_project(expressions),pushdowns).await?;
            let pages = Box::new(PageProject { input: pages,schema: runtime.child(0)?.schema().clone(),expressions: expressions.clone(),execution });
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::Extend { expressions, .. } => {
            let child = runtime.child(0)?;
            let schema = child.schema().clone();
            let pages = build_pages_constrained(child, execution.child(0), constraints.through_extension(expressions), pushdowns).await?;
            let pages = Box::new(PageExtend { input: pages, schema, expressions: expressions.clone(), execution });
            let pages = crate::pages::retain_page_state(pages, state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages, analysis), &traced_relation, &traced_context));
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
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::Sort { order_by,.. } => {
            let mut constraints = constraints;for order in order_by { constraints.require(&order.expr); }
            let pages = build_pages_constrained(runtime.child(0)?,execution.child(0),constraints,pushdowns).await?;
            let pages = Box::new(crate::sort::SortCursor::from_pages(pages,runtime.child(0)?.schema().clone(),order_by.clone(),execution));
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::Limit { offset,fetch,.. } => {
            let input = build_pages_constrained(runtime.child(0)?,execution.child(0),crate::inspection::Constraints::default(),None).await?;
            let offset = row_limit(offset.as_ref(),&execution).await?.unwrap_or(0);
            let remaining = row_limit(fetch.as_ref(),&execution).await?;
            let pages = Box::new(Limit { input: Some(input),offset,remaining,execution });
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::SortLimit { input: relation_input,order_by,offset,fetch } => {
            let mut constraints = constraints;for order in order_by { constraints.require(&order.expr); }
            let pushdowns = if offset.is_some() || fetch.is_some() { None } else { pushdowns };
            let bounds = if !order_by.is_empty() && fetch.is_some() && has_ordered_scan(relation_input) {
                Some(sort_limit_bounds(offset.as_ref(), fetch.as_ref(), &execution).await)
            } else { None };
            if let Some(Ok((offset, remaining, row_limit))) = &bounds
                && let Ok(limit) = usize::try_from(*row_limit)
                && let Some(input) = ordered_input(runtime.child(0)?, execution.child(0), limit).await?
            {
                let pages = Box::new(Limit { input: Some(input), offset: *offset, remaining: *remaining, execution });
                let pages = crate::pages::retain_page_state(pages, state_reservation);
                return Ok(crate::trace::wrap(crate::analysis::wrap(pages, analysis), &traced_relation, &traced_context));
            }
            let schema = runtime.child(0)?.schema().clone();
            let batches = if !order_by.is_empty() && execution.analysis.is_none() && execution.context.tracer.is_none() && crate::sort::SortCursor::batch_eligible(&schema,order_by,0) {
                build_batches(runtime.child(0)?,execution.child(0),pushdowns.clone()).await?
            } else { None };
            let input = match batches {
                Some(batches) => Ok(batches),
                None => Err(build_pages_constrained(runtime.child(0)?,execution.child(0),constraints,pushdowns).await?),
            };
            let (offset_value, remaining, row_limit) = match bounds {
                Some(bounds) => bounds?,
                None => sort_limit_bounds(offset.as_ref(), fetch.as_ref(), &execution).await?,
            };
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
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::Distinct { .. } => {
            let input = build_pages_constrained(runtime.child(0)?,execution.child(0),crate::inspection::Constraints::default(),pushdowns).await?;
            let pages = Box::new(Distinct { input,seen: SeenRows::new(&execution.context,runtime.child(0)?.output())?,execution });
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::Set { operator,all,.. } => {
            let left = build_pages_constrained(runtime.child(0)?,execution.child(0),crate::inspection::Constraints::default(),pushdowns.clone()).await?;
            let right = build_pages_constrained(runtime.child(1)?,execution.child(1),crate::inspection::Constraints::default(),pushdowns).await?;
            let pages: BoxPageCursor = if *operator == SetOperator::Union {
                let input: BoxPageCursor = Box::new(Union { left: Some(left),right });
                if *all { input } else { Box::new(Distinct { input,seen: SeenRows::new(&execution.context,runtime.output())?,execution }) }
            } else { Box::new(SetCursor::new(left,right,*operator,*all,Arc::clone(&runtime),execution)?) };
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::ColumnTable { expr, .. } => {
            let pages = crate::column_table::open(&execution, expr.clone()).await?;
            let pages = crate::pages::retain_page_state(pages, state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages, analysis), &traced_relation, &traced_context));
        }
        RelationNode::List { expr, page_rows, .. } => {
            let pages = Box::new(execution.open_list(expr.clone(),*page_rows).await?);
            let pages = crate::pages::retain_page_state(pages, state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages, analysis), &traced_relation, &traced_context));
        }
        RelationNode::Ordinality { scope, .. } => {
            let input = build_pages_constrained(runtime.child(0)?, execution.child(0), crate::inspection::Constraints::default(), None).await?;
            let pages = Box::new(crate::list::Ordinality::new(input, *scope, Arc::clone(&execution.context)));
            let pages = crate::pages::retain_page_state(pages, state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages, analysis), &traced_relation, &traced_context));
        }
        RelationNode::Unnest { expr,ordinality,outer,.. } => {
            let input_pages = build_pages_constrained(runtime.child(0)?,execution.child(0),crate::inspection::Constraints::default(),pushdowns).await?;
            let pages = Box::new(Unnest { input: input_pages,schema: runtime.child(0)?.schema().clone(),expr: expr.clone(),ordinality: ordinality.is_some(),outer: *outer,execution });
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::Apply { arguments,kind,.. } => {
            let input_pages = build_pages_constrained(runtime.child(0)?,execution.child(0),crate::inspection::Constraints::default(),None).await?;
            let pages = Box::new(Apply { input: input_pages,schema: runtime.child(0)?.schema().clone(),dependent: runtime.child(1)?,arguments: arguments.clone(),kind: *kind,execution });
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::Let { binding,.. } => {
            let bound = build_pages_constrained(runtime.child(0)?,execution.child(0),crate::inspection::Constraints::default(),None).await?;
            let scope = crate::materialized::BindingScope::new(Arc::clone(binding),bound,execution.bindings.clone(),Arc::clone(&execution.context))?;
            let mut body_execution = execution.child(1);body_execution.bindings = Some(Arc::clone(&scope));
            let body = build_pages_constrained(runtime.child(1)?,body_execution,constraints,pushdowns).await?;
            let pages = Box::new(crate::materialized::LetCursor { body,_binding: scope });
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::Reference { binding } => {
            let pages = execution.bindings.as_ref().ok_or_else(||invalid("missing-relation","Reference has no lexical binding"))?.open(binding).await?;
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::Mark { arguments,condition,comparison,.. } => {
            let probe = runtime.child(0)?;
            let pages = Box::new(crate::mark::Mark::new(
                DeferredInput { runtime: Arc::clone(&probe),execution: execution.child(0),constraints,pushdowns },
                &probe,runtime.child(1)?,arguments.clone(),condition.clone(),comparison.clone(),execution,
            )?);
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::Aggregate { groups,aggregates,.. } => {
            let schema = runtime.child(0)?.schema().clone();
            let columnar = execution.analysis.is_none() && execution.context.tracer.is_none() && crate::aggregate::AggregateCursor::reads_columns(&schema,groups,aggregates).is_some();
            let batches = if columnar { build_batches(runtime.child(0)?,execution.child(0),pushdowns.clone()).await? } else { None };
            let pages: Box<crate::aggregate::AggregateCursor> = match batches {
                Some((batches,projected)) => Box::new(crate::aggregate::AggregateCursor::from_batches(batches,projected,schema,groups.clone(),aggregates.clone(),Arc::clone(&runtime),execution)?),
                None => {
                    let input = build_pages_constrained(runtime.child(0)?,execution.child(0),crate::inspection::Constraints::default(),pushdowns).await?;
                    Box::new(crate::aggregate::AggregateCursor::new(input,schema,groups.clone(),aggregates.clone(),Arc::clone(&runtime),execution)?)
                }
            };
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::Window { partition_by,order_by,expressions,.. } => {
            let input = build_pages(runtime.child(0)?,execution.child(0)).await?;
            let pages = Box::new(crate::window::WindowCursor::new(input,runtime.child(0)?,partition_by.clone(),order_by.clone(),expressions.clone(),execution));
            let pages = crate::pages::retain_page_state(pages,state_reservation);
            return Ok(crate::trace::wrap(crate::analysis::wrap(pages,analysis),&traced_relation,&traced_context));
        }
        RelationNode::PatchGaps { .. } => unreachable!(),
    }
}) }

async fn row_limit(expr: Option<&Expr>, execution: &Execution) -> Result<Option<u64>> {
    let Some(expr) = expr else { return Ok(None); };
    let scope = execution.scope()?;
    let value = execution.evaluate(&scope, expr, &Schema::default(), &[]).await?;
    if let Some(value) = integer(&value).filter(|value| (0..=i128::from(i64::MAX)).contains(value)) { return Ok(Some(value as u64)); }
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
async fn sort_limit_bounds(offset: Option<&Expr>, fetch: Option<&Expr>, execution: &Execution) -> Result<(u64, Option<u64>, u64)> {
    let offset = row_limit(offset, execution).await?.unwrap_or(0);
    let remaining = row_limit(fetch, execution).await?;
    let limit = offset.saturating_add(remaining.unwrap_or(i64::MAX as u64)).min(i64::MAX as u64);
    Ok((offset, remaining, limit))
}

fn has_ordered_scan(mut relation: &Relation) -> bool {
    loop {
        match relation.node() {
            RelationNode::Scan(scan) => return scan.ordered_top_k.is_some() && scan.iids.is_none() && scan.matches.is_empty(),
            RelationNode::Project { input, .. } | RelationNode::Rename { input, .. } | RelationNode::Extend { input, .. }
            | RelationNode::Filter { input, .. } | RelationNode::Distinct { input, .. } => relation = input,
            _ => return false,
        }
    }
}

fn ordered_input(runtime: Arc<EmittedRelation>, execution: Execution, limit: usize)
    -> futures::future::BoxFuture<'static, Result<Option<BoxPageCursor>>>
{
    Box::pin(async move {
        let relation = runtime.relation();
        let input: BoxPageCursor = match relation.node() {
            RelationNode::Scan(scan) => {
                let Some(input) = crate::scan::ordered_pages(scan, &execution, limit).await? else { return Ok(None); };
                input
            }
            RelationNode::Project { expressions, .. } | RelationNode::Rename { expressions, .. } => {
                let child = runtime.child(0)?;
                let Some(input) = ordered_input(Arc::clone(&child), execution.child(0), limit).await? else { return Ok(None); };
                Box::new(PageProject { input, schema: child.schema().clone(), expressions: expressions.clone(), execution: execution.clone() })
            }
            RelationNode::Extend { expressions, .. } => {
                let child = runtime.child(0)?;
                let Some(input) = ordered_input(Arc::clone(&child), execution.child(0), limit).await? else { return Ok(None); };
                Box::new(PageExtend { input, schema: child.schema().clone(), expressions: expressions.clone(), execution: execution.clone() })
            }
            RelationNode::Filter { .. } | RelationNode::Distinct { .. } => {
                let Some(input) = ordered_input(runtime.child(0)?, execution.child(0), limit).await? else { return Ok(None); };
                input
            }
            _ => return Ok(None),
        };
        let input = crate::pages::retain_page_state(input, state_reservation(&runtime, &execution)?);
        Ok(Some(crate::trace::wrap(crate::analysis::wrap(input, execution.analysis.clone()), relation, &execution.context)))
    })
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

pub(crate) fn uri_hashes_match(left: &[Value], right: &[Value], memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<bool> {
    fn has_uri(value: &Value) -> bool {
        match value {
            Value::Uri(_) => true,
            Value::Set(values) => values.iter().any(has_uri),
            Value::List(values) | Value::FixedSizeList(values) => matches!(values.view(), rtdb_types::ListView::Values(values) if values.iter().any(has_uri)),
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
fn equality_hash(values: &[Value], timezone: &str) -> u32 {
    fn temporal(value: &Value) -> bool {
        match value {
            Value::Timestamp { .. } | Value::DateDay(_) | Value::DateMilli(_) | Value::Date(_) | Value::TimestampTzRange { .. } => true,
            Value::Set(values) => values.iter().any(temporal),
            Value::List(values) | Value::FixedSizeList(values) => matches!(values.view(), rtdb_types::ListView::Values(values) if values.iter().any(temporal)),
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
        (hash ^ cell.wrapping_mul(0xcc9e_2d51).rotate_left(15)).rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64)
    })
}
/// The distinct rows seen so far, found through an index from their hash to the rows that share it.
pub(crate) struct SeenRows { rows: Vec<OwnedRow>, index: std::collections::HashMap<u32, Vec<usize>>, validated: bool, runtime: Arc<RuntimeSchema>, context: Arc<QueryContext>, _reservation: Option<Arc<rtdb_columnar::MemoryReservation>> }
impl SeenRows {
    pub fn new(context: &Arc<QueryContext>, runtime: Arc<RuntimeSchema>) -> Result<Self> { Ok(Self { rows: Vec::new(), index: Default::default(), validated: false, runtime, context: Arc::clone(context), _reservation: None }) }
    pub fn insert(&mut self, values: &[Value]) -> Result<bool> {
        if !self.validated {
            for field in self.runtime.schema().fields() { rtdb_plan::validate_equality_types(&field.data_type, &field.data_type)?; }
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
            let bytes = rows.len().checked_mul(std::mem::size_of::<OwnedRow>() + std::mem::size_of::<(u32, Vec<usize>)>() + 2 * std::mem::size_of::<usize>() + 1)
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| invalid("set-size", "Set rows exceed addressable memory"))?;
            let reservation = memory.reserve(bytes)?;
            let mut index: std::collections::HashMap<u32, Vec<usize>> = Default::default();
            for (ordinal, row) in rows.iter().enumerate() { index.entry(equality_hash(&row.values, &self.execution.context.timezone)).or_default().push(ordinal); }
            let consumed = vec![false; rows.len()];
            self.built = Some(RightRows { rows, index, consumed, _reservation: reservation });
        }
        while let Some(mut page) = self.left.next_page().await? {
          let mut output = PageBuilder::new(page.len(),&self.execution.context.memory)?;
          while let Some(left) = page.next() {
            self.execution.context.check_cancelled()?;
            let left_types = self.runtime.child(0)?;let right_types = self.runtime.child(1)?;
            for (left, right) in left_types.schema().fields().iter().zip(right_types.schema().fields()) { rtdb_plan::validate_equality_types(&left.data_type, &right.data_type)?; }
            if !self.all && !self.seen.insert(&left.values)? { continue; }
            let built = self.built.as_mut().ok_or_else(|| invalid("set-state", "Set input was not built"))?;
            let mut matched = false;
            if let Some(candidates) = built.index.get(&equality_hash(&left.values, &self.execution.context.timezone)) {
                for &ordinal in candidates {
                    if self.all && built.consumed[ordinal] { continue; }
                    if rows_equal(&left.values, &built.rows[ordinal].values, &self.execution.context.timezone, &self.execution.context.memory)? {
                        if self.all { built.consumed[ordinal] = true; }
                        matched = true;
                        break;
                    }
                }
            }
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
                ApplyKind::Mark => {
                    let mut mark = Value::Bool(false);
                    let mut next = first;
                    while let Some(candidate) = next {
                        self.execution.context.check_cancelled()?;
                        match candidate.values.first() {
                            Some(Value::Bool(true)) => { mark = Value::Bool(true); break; }
                            Some(Value::Bool(false)) => (),
                            Some(Value::Null | Value::Undefined) => mark = Value::Null,
                            _ => return Err(invalid("mark-type","Mark comparison must return Boolean or null")),
                        }
                        next = cursor.next().await?;
                    }
                    mark
                }
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
                    if documents.is_empty() { Value::Null } else { Value::list(documents) }
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
