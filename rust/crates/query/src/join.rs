use std::sync::Arc;
use async_trait::async_trait;
use rtdb_expr::{ExpressionInvocation,is_true};
use rtdb_plan::{JoinKind,Relation,RelationNode};
use rtdb_types::{Result,Value};
use crate::{context::Execution,cursor::OwnedRow,join_key::{JoinInput,JoinLayout},join_storage::{BuildAccumulator,BuildInput,BuildTable,HashedPage,PartitionSpool},operators::DeferredInput,pages::{BoxPageCursor,OwnedPage,PageBuilder,PageCursor},pushdown::PushdownBuilder,invalid};

enum Phase {
    Build { input: BoxPageCursor,probe: DeferredInput },
    Memory { table: BuildTable,probe: BoxPageCursor,_build: BoxPageCursor },
    Disk { build: PartitionSpool,probe: PartitionSpool,table: Option<BuildTable>,build_part: usize,probe_part: usize,_build_input: BoxPageCursor,_probe_input: BoxPageCursor },
    Done,
}
pub(crate) struct JoinCursor {
    phase: Phase,plan: JoinPlan,
}
struct JoinPlan { layout: JoinLayout,build_left: bool,kind: JoinKind,execution: Execution,projected_build: bool }
impl JoinCursor {
    pub fn new(build: BoxPageCursor,probe: DeferredInput,mut layout: JoinLayout,build_left: bool,kind: JoinKind,execution: Execution) -> Result<Self> {
        if kind == JoinKind::Cross { return Err(invalid("join-kind","Cross join requires its page-pair cursor")); }
        if matches!(kind, JoinKind::Full | JoinKind::Single) || kind == JoinKind::Left && !build_left || kind == JoinKind::Right && build_left { layout.with_null_build(build_left)?; }
        Ok(Self { phase: Phase::Build { input: build,probe },plan: JoinPlan { layout,build_left,kind,execution,projected_build: false } })
    }
    pub fn with_projected_build(mut self, projected: bool) -> Self {
        self.plan.projected_build = projected;
        self
    }
}
impl JoinPlan {
    fn build_layout(&self) -> &JoinInput { if self.build_left { &self.layout.left } else { &self.layout.right } }
    fn probe_layout(&self) -> &JoinInput { if self.build_left { &self.layout.right } else { &self.layout.left } }
    fn track_build(&self) -> bool { tracks_build(self.kind, self.build_left) }
    fn preserve_probe(&self) -> bool { preserves_probe(self.kind, self.build_left) }
    fn blooms(&self) -> bool { matches!(self.kind,JoinKind::Inner | JoinKind::Semi) || self.track_build() && self.kind != JoinKind::Full }
    fn joined(&self,build: &OwnedRow,probe: &OwnedRow) -> Result<OwnedRow> {
        let (left,right) = if self.build_left { (build,probe) } else { (probe,build) };
        OwnedRow::copy_parts(&[&left.values,&right.values],0,0,&self.execution.context.memory)
    }
    fn unmatched(&self,row: &OwnedRow,left: bool) -> Result<OwnedRow> {
        let input = if left { &self.layout.left } else { &self.layout.right };
        OwnedRow::copy_parts(&[input.output(row)],if left { 0 } else { self.layout.left.original.len() },if left { self.layout.right.original.len() } else { 0 },&self.execution.context.memory)
    }
    fn output(&self,build: &OwnedRow,probe: &OwnedRow,slab: &mut crate::cursor::RowSlab) -> Result<OwnedRow> {
        let (left,right) = if self.build_left { (build,probe) } else { (probe,build) };
        OwnedRow::copy_parts_in(&[self.layout.left.output(left),self.layout.right.output(right)],0,0,slab)
    }
    fn probe_output(&self,probe: &OwnedRow) -> Result<OwnedRow> {
        OwnedRow::copy_parts(&[self.probe_layout().output(probe)],0,0,&self.execution.context.memory)
    }
    /// The joined-row positions of a comparison that is an equality between two columns, which compares their cells directly.
    fn direct_equalities(&self) -> Vec<Option<(usize,usize)>> {
        self.layout.comparisons.iter().enumerate().map(|(index,expression)| {
            let schema = if index < self.build_layout().key_count() { &self.layout.schema } else { &self.layout.theta_schema };
            match expression {
                rtdb_plan::Expr::Binary { op: rtdb_plan::BinaryOp::Eq, left, right } => match (left.as_ref(),right.as_ref()) {
                    // The evaluator rejects some declared type pairs before comparing any cells; those keep its path.
                    (rtdb_plan::Expr::Column { id: left, data_type: left_type },rtdb_plan::Expr::Column { id: right, data_type: right_type })
                        if rtdb_plan::validate_equality_types(left_type,right_type).is_ok() => Some((schema.position(*left)?,schema.position(*right)?)),
                    _ => None,
                },
                _ => None,
            }
        }).collect()
    }
    fn cell<'a>(&self,build: &'a OwnedRow,probe: &'a OwnedRow,position: usize) -> Result<&'a Value> {
        let (left,right) = if self.build_left { (build,probe) } else { (probe,build) };
        if position < left.values.len() { Ok(&left.values[position]) }
        else { right.values.get(position - left.values.len()).ok_or_else(|| invalid("join-row","Joined column is outside both rows")) }
    }
    async fn accepted(&self,comparisons: &[ExpressionInvocation<'_>],direct: &[Option<(usize,usize)>],equality: &rtdb_expr::EvaluationScope,build: &OwnedRow,probe: &OwnedRow) -> Result<bool> {
        if comparisons.is_empty() { return Ok(true); }
        let joined = if direct.iter().all(Option::is_some) { None } else { Some(self.joined(build,probe)?) };
        let mut unknown = false;
        for (index,comparison) in comparisons.iter().enumerate() {
            if self.preserve_probe() && index < self.build_layout().key_count() {
                let build_null = matches!(self.build_layout().key(build,index)?,Value::Null | Value::Undefined);
                let probe_null = matches!(self.probe_layout().key(probe,index)?,Value::Null | Value::Undefined);
                if build_null || probe_null {
                    if build_null != probe_null { return Ok(false); }
                    continue;
                }
            }
            if let Some((left,right)) = direct[index] {
                let equal = rtdb_expr::sql_eq_in_timezone(self.cell(build,probe,left)?,self.cell(build,probe,right)?,&self.execution.context.timezone,equality)?;
                equality.reset()?;
                match equal {
                    None => unknown = true,
                    Some(false) => return Ok(false),
                    Some(true) => {}
                }
                continue;
            }
            let scope = self.execution.scope()?;
            let joined = joined.as_ref().ok_or_else(|| invalid("join-row","Joined row was not built for an evaluated comparison"))?;
            let value = self.execution.evaluate_invocation(&scope,comparison,&joined.values).await?;
            if matches!(value,Value::Null | Value::Undefined) { unknown = true; }
            else if !is_true(&value)? { return Ok(false); }
        }
        Ok(!unknown)
    }
    async fn probe_page(&self,table: &mut BuildTable,probe_page: OwnedPage) -> Result<OwnedPage> {
        let metadata = self.execution.scope()?;
        metadata.reserve(self.layout.comparisons.len().checked_mul(std::mem::size_of::<ExpressionInvocation<'_>>()).ok_or_else(size_error)?)?;
        let mut comparisons = Vec::with_capacity(self.layout.comparisons.len());
        for (index,expression) in self.layout.comparisons.iter().enumerate() {
            let schema = if index < self.build_layout().key_count() { &self.layout.schema } else { &self.layout.theta_schema };
            comparisons.push(self.execution.prepare_expression(expression,schema,&metadata)?);
        }
        let direct = self.direct_equalities();
        let equality = self.execution.scope()?;
        let mut slab = crate::cursor::RowSlab::new(&self.execution.context.memory);
        let mut output = PageBuilder::new(0,&self.execution.context.memory)?;
        let _hash_scratch = self.execution.context.memory.reserve(4096)?;
        let BuildTable { rows,index,unmatched,.. } = table;
        for probe in probe_page.rows() {
            self.execution.context.check_cancelled()?;
            let mut matched = false;
            if self.kind == JoinKind::Anti {
                for build in rows.page.rows() {
                    if self.accepted(&comparisons,&direct,&equality,build,probe).await? { matched = true;break; }
                }
            } else {
                for build_index in index.candidates(self.probe_layout().hash(probe)?) {
                    let build = &rows.page.rows()[build_index];
                    if !self.accepted(&comparisons,&direct,&equality,build,probe).await? { continue; }
                    if matched && self.kind == JoinKind::Single { return Err(crate::cardinality_violation()); }
                    matched = true;
                    if let Some(unmatched) = unmatched { unmatched[build_index] = 0; }
                    if self.kind == JoinKind::Semi { break; }
                    append(&mut output,self.output(build,probe,&mut slab)?,&self.execution)?;
                }
            }
            if self.kind == JoinKind::Semi && matched || self.kind == JoinKind::Anti && !matched {
                append(&mut output,self.probe_output(probe)?,&self.execution)?;
            } else if !matched && self.preserve_probe() {
                append(&mut output,self.unmatched(probe,!self.build_left)?,&self.execution)?;
            }
        }
        Ok(output.finish())
    }
    fn unmatched_page(&self,table: &BuildTable) -> Result<Option<OwnedPage>> {
        let mut output = PageBuilder::new(0,&self.execution.context.memory)?;
        for index in table.unmatched() {
            self.execution.context.check_cancelled()?;
            append(&mut output,self.unmatched(&table.rows.page.rows()[index],self.build_left)?,&self.execution)?;
        }
        Ok((output.len() > 0).then(||output.finish()))
    }
}

fn append(page: &mut PageBuilder,row: OwnedRow,execution: &Execution) -> Result<()> {
    if page.len() == i32::MAX as usize { return Err(size_error()); }
    page.push_growing(row,&execution.context.memory)
}
fn size_error() -> rtdb_types::Anomaly { invalid("join-size","Join output or comparator metadata exceeds supported allocation bounds") }

pub(crate) fn tracks_build(kind: JoinKind, build_left: bool) -> bool {
    kind == JoinKind::Full || kind == JoinKind::Left && build_left || kind == JoinKind::Right && !build_left
}

pub(crate) fn preserves_probe(kind: JoinKind, build_left: bool) -> bool {
    matches!(kind, JoinKind::Full | JoinKind::Single)
        || kind == JoinKind::Left && !build_left || kind == JoinKind::Right && build_left
}

#[async_trait]
impl PageCursor for JoinCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        let plan = &self.plan;
        loop {
            plan.execution.context.check_cancelled()?;
            match std::mem::replace(&mut self.phase,Phase::Done) {
                Phase::Build { mut input,probe } => {
                    let mut accumulator = BuildAccumulator::new(plan.execution.context.options.join_build_rows,Arc::clone(&plan.execution.context))?;
                    while let Some(page) = input.next_page().await? {
                        let page = if plan.projected_build { page } else { plan.build_layout().project_page(page,&plan.execution).await? };
                        accumulator.append(page)?;
                    }
                    match accumulator.finish()? {
                        BuildInput::Memory(page) => {
                            let row_count = i64::try_from(page.len()).map_err(|_|size_error())?;
                            let table = BuildTable::new(HashedPage::new(page,|row|plan.build_layout().hash(row),&plan.execution.context.memory)?,plan.track_build(),&plan.execution.context.memory)?;
                            let mut filters = PushdownBuilder::new(plan.layout.condition.as_ref(),&plan.build_layout().schema,&plan.probe_layout().original,row_count,plan.blooms(),&plan.execution)?;
                            if plan.blooms() {
                                for row in table.rows.page.rows() { filters.insert(&plan.build_layout().schema,&row.values,&plan.execution).await?; }
                            }
                            let filters = filters.finish(&plan.execution.context.memory)?;
                            let probe = probe.open_pages(filters).await?;
                            self.phase = Phase::Memory { table,probe,_build: input };
                        }
                        BuildInput::Spilled(blocks) => {
                            let build = PartitionSpool::build(blocks,1,|row|plan.build_layout().hash(row),&plan.execution.context)?;
                            let mut probe = probe.open_pages(None).await?;
                            let mut accumulator = BuildAccumulator::new(plan.execution.context.options.join_build_rows,Arc::clone(&plan.execution.context))?;
                            while let Some(page) = probe.next_page().await? { accumulator.append(plan.probe_layout().project_page(page,&plan.execution).await?)?; }
                            let probe_shuffle = PartitionSpool::build(accumulator.finish_spilled()?,build.part_count(),|row|plan.probe_layout().hash(row),&plan.execution.context)?;
                            self.phase = Phase::Disk { build,probe: probe_shuffle,table: None,build_part: 0,probe_part: 0,_build_input: input,_probe_input: probe };
                        }
                    }
                }
                Phase::Memory { mut table,mut probe,_build } => {
                    let Some(page) = probe.next_page().await? else { return plan.unmatched_page(&table); };
                    let page = plan.probe_layout().project_page(page,&plan.execution).await?;
                    let output = plan.probe_page(&mut table,page).await?;
                    self.phase = Phase::Memory { table,probe,_build };
                    if output.len() > 0 { return Ok(Some(output)); }
                }
                Phase::Disk { build,probe,mut table,mut build_part,mut probe_part,_build_input,_probe_input } => {
                    if build_part == build.part_count() { return Ok(None); }
                    if table.is_none() { table = Some(BuildTable::new(build.load(build_part,&plan.execution.context)?,plan.track_build(),&plan.execution.context.memory)?); }
                    let probes_per_build = probe.part_count() / build.part_count();
                    let output = if probe_part == probes_per_build {
                        let result = plan.unmatched_page(table.as_ref().ok_or_else(||invalid("join-state","Loaded build partition is absent"))?)?;
                        table = None;build_part += 1;probe_part = 0;result
                    } else {
                        let page = probe.load(probe_part * build.part_count() + build_part,&plan.execution.context)?;
                        let result = plan.probe_page(table.as_mut().ok_or_else(||invalid("join-state","Loaded build partition is absent"))?,page.page).await?;
                        probe_part += 1;(result.len() > 0).then_some(result)
                    };
                    self.phase = Phase::Disk { build,probe,table,build_part,probe_part,_build_input,_probe_input };
                    if output.is_some() { return Ok(output); }
                }
                Phase::Done => return Ok(None),
            }
        }
    }
}

pub(crate) fn build_left(left: &Relation,right: &Relation,kind: JoinKind,execution: &Execution) -> Result<bool> {
    Ok(match kind {
        JoinKind::Left => row_count(left,execution)? < row_count(right,execution)?,
        JoinKind::Right => row_count(left,execution)? <= row_count(right,execution)?,
        JoinKind::Inner | JoinKind::Full => true,
        JoinKind::Semi | JoinKind::Anti | JoinKind::Single => false,
        JoinKind::Cross => return Err(invalid("join-kind","Cross join has no hash build side")),
    })
}
fn row_count(relation: &Relation,execution: &Execution) -> Result<i64> {
    match relation.node() {
        RelationNode::Values { rows } => i64::try_from(rows.len()).map_err(|_|size_error()),
        RelationNode::Scan(scan) => {
            let partitions = &execution.context.catalog.database(&scan.table.name.database)?.partitions;
            if partitions.is_empty() { return Ok(i64::MAX); }
            let mut rows = 0i64;
            for partition in partitions {
                let Some(table) = partition.table_catalog().metadata(&scan.table.name.table) else { return Ok(i64::MAX); };
                rows = rows.checked_add(table.row_count).ok_or_else(size_error)?;
            }
            Ok(rows)
        }
        RelationNode::Project { input,.. } | RelationNode::Extend { input,.. } | RelationNode::Rename { input,.. } | RelationNode::Filter { input,.. } => row_count(input,execution),
        RelationNode::Reference { binding } => row_count(&binding.relation,execution),
        RelationNode::Let { body,.. } => row_count(body,execution),
        _ => Ok(i64::MAX),
    }
}

#[cfg(test)]
#[path = "join_empty_page_tests.rs"]
mod empty_page_tests;

/// The rows a join input holds at most: its tables' flushed block rows and live rows, or `i64::MAX` where it can't tell.
pub(crate) fn rows_at_most(relation: &Relation,execution: &Execution) -> Result<i64> {
    match relation.node() {
        RelationNode::Scan(scan) => {
            let partitions = &execution.context.catalog.database(&scan.table.name.database)?.partitions;
            if partitions.is_empty() { return Ok(i64::MAX); }
            let mut rows = 0i64;
            for partition in partitions {
                let table = &scan.table.name.table;
                let flushed = partition.table_catalog().metadata(table).map(|meta| meta.row_count);
                let live = partition.iter_segments(table).map(|segment| i64::from(segment.row_count())).sum::<i64>();
                if flushed.is_none() && live == 0 { return Ok(i64::MAX); }
                rows = rows.checked_add(flushed.unwrap_or(0)).and_then(|rows| rows.checked_add(live)).ok_or_else(size_error)?;
            }
            Ok(rows)
        }
        RelationNode::Project { input,.. } | RelationNode::Extend { input,.. } | RelationNode::Rename { input,.. } | RelationNode::Filter { input,.. } => rows_at_most(input,execution),
        RelationNode::Values { rows } => i64::try_from(rows.len()).map_err(|_|size_error()),
        _ => Ok(i64::MAX),
    }
}
