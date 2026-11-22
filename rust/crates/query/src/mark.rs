use std::sync::Arc;
use async_trait::async_trait;
use rtdb_plan::{BinaryOp,Expr,Relation,RelationNode,Schema,TemporalFilter};
use rtdb_types::{LogicalType,Result,Value};
use crate::{context::Execution,cursor::OwnedRow,invalid,operators::{DeferredInput,build_pages},pages::{BoxPageCursor,OwnedPage,PageBuilder,PageCursor},runtime_schema::EmittedRelation,join_key::JoinLayout,join_storage::{BuildAccumulator,BuildInput,BuildTable,HashedPage,PartitionSpool,hash_values}};
use crate::columnar::{BatchCursor, BoxBatchCursor, Column, ColumnBatch, poll_nested_batch};
use crate::set_keys::BatchKeyIndex;
use crate::batch_join::ProbeInput;

enum State {
    Build { probe: DeferredInput,runtime: Arc<EmittedRelation> },
    Memory { probe: BoxPageCursor,table: BuildTable,facts: CandidateFacts,_build: BoxPageCursor },
    EveryRowDisk { probe: BoxPageCursor,build: PartitionSpool,_build: BoxPageCursor },
    KeyedDisk { probe: PartitionSpool,build: PartitionSpool,facts: CandidateFacts,table: Option<BuildTable>,build_part: usize,probe_part: usize,_build_cursor: BoxPageCursor,_probe_cursor: BoxPageCursor },
    CorrelatedUnopened { input: DeferredInput,runtime: Arc<EmittedRelation>,arguments: Vec<(usize,Expr)> },
    Correlated { input: BoxPageCursor,runtime: Arc<EmittedRelation>,arguments: Vec<(usize,Expr)> },
    Done,
}
#[derive(Clone,Copy)]
struct CandidateFacts { empty: bool,null: bool,nested: bool }
impl CandidateFacts {
    fn add(&mut self,page: &OwnedPage,schema: &Schema,candidate: usize) {
        if page.len() == 0 { return; }
        self.empty = false;
        self.null |= page.rows().iter().any(|row|matches!(row.values.get(candidate),Some(Value::Null | Value::Undefined)));
        self.nested |= schema.fields().get(candidate).is_some_and(|field|field.data_type.legs().iter().any(|ty|matches!(ty,LogicalType::List { .. } | LogicalType::Struct(_))));
    }
    fn unmatched(self,probe_null: bool) -> Value {
        if !self.empty && (self.null || probe_null) { Value::Null } else { Value::Bool(false) }
    }
}
enum MarkKeys {
    EveryRow,
    Membership { probe: usize, candidate: usize },
    Eligibility(Box<JoinLayout>),
}

struct MarkPlan { input_schema: Schema,comparison_schema: Schema,comparison: Expr,keys: MarkKeys,execution: Execution }
pub(crate) struct Mark { state: State,plan: MarkPlan }
impl Mark {
    pub fn new(input: DeferredInput,probe: &EmittedRelation,runtime: Arc<EmittedRelation>,arguments: Vec<(usize,Expr)>,condition: Option<Expr>,comparison: Expr,execution: Execution) -> Result<Self> {
        let input_schema = probe.schema().clone();
        let (keys,condition,comparison_schema) = if let Some(condition) = condition.as_ref().filter(|_| arguments.is_empty()) {
            let mut layout = JoinLayout::new(probe.relation(),runtime.relation(),Some(condition),&execution.context.memory)?;
            layout.resolve_types(probe.schema(),runtime.schema(),&execution)?;
            let condition = layout.condition.clone();
            let schema = layout.schema.clone();
            (MarkKeys::Eligibility(Box::new(layout)),condition,schema)
        } else {
            let keys = if condition.is_none() && arguments.is_empty() {
                membership_key(&input_schema,runtime.schema(),&comparison).unwrap_or(MarkKeys::EveryRow)
            } else { MarkKeys::EveryRow };
            (keys,condition,input_schema.concat(runtime.schema())?)
        };
        let comparison = if let Some(condition) = condition {
            Expr::Case { branches: vec![(condition,comparison)], otherwise: Box::new(Expr::Literal(Value::Bool(false))) }
        } else { comparison };
        let state = if arguments.is_empty() { State::Build { probe: input,runtime } }
            else { State::CorrelatedUnopened { input,runtime,arguments } };
        Ok(Self { state,plan: MarkPlan { input_schema,comparison_schema,comparison,keys,execution } })
    }
}
fn membership_key(input: &Schema,dependent: &Schema,comparison: &Expr) -> Option<MarkKeys> {
    let Expr::Binary { op: BinaryOp::Eq,left,right } = comparison else { return None; };
    for (probe,candidate) in [(left.as_ref(),right.as_ref()),(right.as_ref(),left.as_ref())] {
        if let (Expr::Column { id: probe,.. },Expr::Column { id: candidate,.. }) = (probe,candidate) {
            if let (Some(probe),Some(candidate)) = (input.position(*probe),dependent.position(*candidate)) {
                return Some(MarkKeys::Membership { probe,candidate });
            }
        }
    }
    None
}

pub(crate) fn batch_membership_keys(input: &Schema, dependent: &Schema, comparison: &Expr) -> Option<(usize, usize)> {
    use rtdb_types::ScalarType as S;
    let MarkKeys::Membership { probe, candidate } = membership_key(input, dependent, comparison)? else { return None; };
    let probe_type = &input.fields()[probe].data_type;
    let candidate_type = &dependent.fields()[candidate].data_type;
    let scalar = |ty: &LogicalType| ty.legs().into_iter().all(|leg| matches!(leg,
        LogicalType::Null | LogicalType::Scalar(S::Bool | S::I8 | S::I16 | S::I32 | S::I64 | S::F32 | S::F64 | S::Utf8 | S::Binary)));
    (scalar(probe_type) && scalar(candidate_type) && rtdb_plan::validate_equality_types(probe_type, candidate_type).is_ok())
        .then_some((probe, candidate))
}

pub(crate) fn bounded_membership_input(input: &Relation) -> bool {
    stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || match input.node() {
        RelationNode::Scan(scan) => matches!(scan.valid_time, TemporalFilter::Default | TemporalFilter::At(_)),
        RelationNode::Project { input, .. } | RelationNode::Extend { input, .. } | RelationNode::Rename { input, .. }
        | RelationNode::Filter { input, .. } => bounded_membership_input(input),
        RelationNode::Values { .. } => true,
        _ => false,
    })
}

// The build retains only its comparison column. The probe keeps its whole batch and adds a Boolean vector.
pub(crate) struct BatchMembership {
    build: Option<ProbeInput>,
    probe: MembershipProbe,
    index: BatchKeyIndex,
    keys: (usize, usize),
    probe_projected: bool,
    facts: CandidateFacts,
    execution: Execution,
}

enum MembershipProbe {
    Unopened(ProbeInput),
    Open(BoxBatchCursor),
    Done,
}

impl BatchMembership {
    pub fn new(build: ProbeInput, probe: ProbeInput,
        probe_projected: bool, keys: (usize, usize), execution: Execution) -> Result<Self> {
        let index = BatchKeyIndex::vector_hashing(&execution.context)?;
        Ok(Self { build: Some(build), probe: MembershipProbe::Unopened(probe), index, keys, probe_projected,
            facts: CandidateFacts { empty: true, null: false, nested: false }, execution })
    }

    async fn open(input: ProbeInput) -> Result<(BoxBatchCursor, bool)> {
        let batches = crate::operators::build_batch_input(Arc::clone(&input.runtime), input.execution.clone(), input.pushdowns, input.output).await?;
        if let Some(batches) = batches { return Ok(batches); }
        let memory = Arc::clone(&input.execution.context.memory);
        let width = input.runtime.schema().len();
        let pages = build_pages(input.runtime, input.execution).await?;
        Ok((crate::columnar::batches_from_pages(pages, width, memory), false))
    }

    fn is_null(column: &Column, row: usize, context: &crate::QueryContext) -> Result<bool> {
        if let Some(null) = column.null_at(row) { return Ok(null); }
        let _scratch = context.memory.reserve(column.bound(row)?)?;
        Ok(matches!(column.value(row)?, Value::Null | Value::Undefined))
    }
}

#[async_trait]
impl BatchCursor for BatchMembership {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        let context = &self.execution.context;
        let memory = &context.memory;
        let (probe_key, build_key) = self.keys;
        if let Some(build) = self.build.take() {
            let (mut build, projected) = Self::open(build).await?;
            while let Some(batch) = poll_nested_batch(build.as_mut()).await? {
                batch.validate_readable(context)?;
                if batch.len == 0 { continue; }
                self.facts.empty = false;
                for row in 0..batch.len {
                    context.check_cancelled_at(row)?;
                    self.facts.null |= Self::is_null(&batch.columns[build_key], row, context)?;
                }
                let _rows = memory.reserve(batch.len.checked_mul(std::mem::size_of::<(usize, usize)>())
                    .ok_or_else(|| invalid("mark-size", "Membership key positions exceed addressable memory"))?)?;
                let rows = (0..batch.len).map(|row| (0, row)).collect::<Vec<_>>();
                let key = batch.key_projection(&[build_key], memory)?.compact_group_keys(&rows, memory)?;
                self.index.build(key, projected, context)?;
            }
        }
        let state = std::mem::replace(&mut self.probe, MembershipProbe::Done);
        let mut probe = match state {
            MembershipProbe::Unopened(probe) => Self::open(probe).await?.0,
            MembershipProbe::Open(probe) => probe,
            MembershipProbe::Done => return Ok(None),
        };
        while let Some(mut batch) = poll_nested_batch(probe.as_mut()).await? {
            if batch.len == 0 { continue; }
            batch.validate_readable(context)?;
            let key = batch.key_projection(&[probe_key], memory)?;
            let bytes = batch.len.checked_add(256).ok_or_else(|| invalid("mark-size", "Membership marks exceed addressable memory"))?;
            let reservation = memory.reserve(bytes)?;
            let mut marks = arrow_array::builder::BooleanBuilder::with_capacity(batch.len);
            for row in 0..batch.len {
                context.check_cancelled_at(row)?;
                let null = Self::is_null(&key.columns[0], row, context)?;
                if !null && self.index.matches(&key, row, self.probe_projected, false, context)? {
                    marks.append_value(true);
                } else if !self.facts.empty && (self.facts.null || null) {
                    marks.append_null();
                } else {
                    marks.append_value(false);
                }
            }
            drop(key);
            let logical = LogicalType::Scalar(rtdb_types::ScalarType::Bool).maybe();
            let field = Arc::new(rtdb_columnar::to_field("mark", &logical)?);
            batch.append_column(Column::Arrow { array: Arc::new(marks.finish()), field, logical: Some(logical) }, reservation, memory)?;
            self.probe = MembershipProbe::Open(probe);
            return Ok(Some(batch));
        }
        Ok(None)
    }
}
impl MarkPlan {
    fn build_hash(&self,row: &OwnedRow) -> Result<u32> {
        match &self.keys {
            MarkKeys::EveryRow => Ok(0),
            MarkKeys::Membership { candidate,.. } => hash_values([row.values.get(*candidate).ok_or_else(||invalid("mark-key","Candidate key is absent"))?]),
            MarkKeys::Eligibility(layout) => layout.right.hash(row),
        }
    }
    fn probe_hash(&self,row: &OwnedRow) -> Result<u32> {
        match &self.keys {
            MarkKeys::EveryRow => Err(invalid("mark-key","Probe key is absent")),
            MarkKeys::Membership { probe,.. } => hash_values([row.values.get(*probe).ok_or_else(||invalid("mark-key","Probe key cell is absent"))?]),
            MarkKeys::Eligibility(layout) => layout.left.hash(row),
        }
    }
    fn partitioned(&self,facts: CandidateFacts) -> bool {
        match &self.keys {
            MarkKeys::EveryRow => false,
            MarkKeys::Membership { .. } => !facts.nested,
            MarkKeys::Eligibility(layout) => layout.left.key_count() > 0,
        }
    }
    async fn input_page(&self,page: OwnedPage,probe: bool) -> Result<OwnedPage> {
        match &self.keys {
            MarkKeys::Eligibility(layout) if probe => layout.left.project_page(page,&self.execution).await,
            MarkKeys::Eligibility(layout) => layout.right.project_page(page,&self.execution).await,
            _ => Ok(page),
        }
    }
    fn visible<'a>(&self,row: &'a OwnedRow) -> &'a [Value] {
        &row.values[..self.input_schema.len()]
    }
    async fn compare(&self,row: &OwnedRow,candidate: &OwnedRow,invocation: &rtdb_expr::ExpressionInvocation<'_>) -> Result<Value> {
        let combined = OwnedRow::copy_parts(&[&row.values,&candidate.values],0,0,&self.execution.context.memory)?;
        let scope = self.execution.scope()?;
        let result = self.execution.evaluate_invocation(&scope,invocation,&combined.values).await?;
        if matches!(result,Value::Bool(_) | Value::Null) { Ok(result) } else { Err(invalid("mark-type","Mark comparison must return Boolean or null")) }
    }
    async fn keyed_page(&self,page: OwnedPage,table: &BuildTable,facts: CandidateFacts) -> Result<OwnedPage> {
        let metadata = self.execution.scope()?;
        let invocation = self.execution.prepare_expression(&self.comparison,&self.comparison_schema,&metadata)?;
        let _scratch = self.execution.context.memory.reserve(4096)?;
        let mut output = PageBuilder::new(page.len(),&self.execution.context.memory)?;
        for row in page.rows() {
            self.execution.context.check_cancelled()?;
            let (mut result,search) = if let MarkKeys::Membership { probe,.. } = &self.keys {
                let null = matches!(row.values.get(*probe),Some(Value::Null | Value::Undefined));
                (facts.unmatched(null),!null)
            } else { (Value::Bool(false),true) };
            if search {
                for index in table.candidates(self.probe_hash(row)?) {
                    match self.compare(row,&table.rows.page.rows()[index],&invocation).await? {
                        Value::Bool(true) => { result = Value::Bool(true); break; }
                        Value::Null if matches!(&self.keys,MarkKeys::Eligibility(_)) => result = Value::Null,
                        Value::Bool(false) | Value::Null => (),
                        _ => unreachable!(),
                    }
                }
            }
            output.push(OwnedRow::copy_parts(&[self.visible(row),&[result]],0,0,&self.execution.context.memory)?)?;
        }
        Ok(output.finish())
    }
    fn marked_page(&self,page: &OwnedPage) -> Result<PageBuilder> {
        let mut output = PageBuilder::new(page.len(),&self.execution.context.memory)?;
        for row in page.rows() { output.push(OwnedRow::copy_parts(&[self.visible(row),&[Value::Bool(false)]],0,0,&self.execution.context.memory)?)?; }
        Ok(output)
    }
    async fn compare_all(&self,output: &mut PageBuilder,page: &OwnedPage,table: &BuildTable) -> Result<()> {
        let metadata = self.execution.scope()?;
        let invocation = self.execution.prepare_expression(&self.comparison,&self.comparison_schema,&metadata)?;
        for (row,output) in page.rows().iter().zip(output.rows_mut()) {
            self.execution.context.check_cancelled()?;
            let mut result = output.values.last().cloned().ok_or_else(||invalid("mark-state","Accumulated mark is absent"))?;
            if result == Value::Bool(true) { continue; }
            for candidate in table.rows.page.rows() {
                match self.compare(row,candidate,&invocation).await? {
                    Value::Bool(true) => { result = Value::Bool(true);break; }
                    Value::Null => result = Value::Null,
                    Value::Bool(false) => (),
                    _ => unreachable!(),
                }
            }
            output.replace_value(self.input_schema.len(),result,&self.execution.context.memory)?;
        }
        Ok(())
    }
}

#[async_trait]
impl PageCursor for Mark {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        let plan = &self.plan;
        loop {
            plan.execution.context.check_cancelled()?;
            match std::mem::replace(&mut self.state,State::Done) {
                State::Build { probe,runtime } => {
                    let mut input = build_pages(Arc::clone(&runtime),plan.execution.child(1)).await?;
                    let mut accumulator = BuildAccumulator::new(plan.execution.context.options.join_build_rows,Arc::clone(&plan.execution.context))?;
                    let mut facts = CandidateFacts { empty: true,null: false,nested: false };
                    while let Some(page) = input.next_page().await? {
                        if let MarkKeys::Membership { candidate,.. } = &plan.keys { facts.add(&page,runtime.schema(),*candidate); }
                        accumulator.append(plan.input_page(page,false).await?)?;
                    }
                    match accumulator.finish()? {
                        BuildInput::Memory(page) => {
                            let table = BuildTable::new(HashedPage::new(page,|row|plan.build_hash(row),&plan.execution.context.memory)?,false,&plan.execution.context.memory)?;
                            self.state = State::Memory { probe: probe.open_pages(None).await?,table,facts,_build: input };
                        }
                        BuildInput::Spilled(blocks) => {
                            let build = PartitionSpool::build(blocks,1,|row|plan.build_hash(row),&plan.execution.context)?;
                            let mut probe = probe.open_pages(None).await?;
                            if !plan.partitioned(facts) { self.state = State::EveryRowDisk { probe,build,_build: input }; }
                            else {
                                let mut accumulator = BuildAccumulator::new(plan.execution.context.options.join_build_rows,Arc::clone(&plan.execution.context))?;
                                while let Some(page) = probe.next_page().await? { accumulator.append(plan.input_page(page,true).await?)?; }
                                let probe_shuffle = PartitionSpool::build(accumulator.finish_spilled()?,build.part_count(),|row|plan.probe_hash(row),&plan.execution.context)?;
                                self.state = State::KeyedDisk { probe: probe_shuffle,build,facts,table: None,build_part: 0,probe_part: 0,_build_cursor: input,_probe_cursor: probe };
                            }
                        }
                    }
                }
                State::Memory { mut probe,table,facts,_build } => {
                    let Some(page) = probe.next_page().await? else { return Ok(None); };
                    if page.len() == 0 { self.state = State::Memory { probe,table,facts,_build };continue; }
                    let page = plan.input_page(page,true).await?;
                    let output = if plan.partitioned(facts) { plan.keyed_page(page,&table,facts).await? }
                        else { let mut output = plan.marked_page(&page)?;plan.compare_all(&mut output,&page,&table).await?;output.finish() };
                    self.state = State::Memory { probe,table,facts,_build };
                    if output.len() > 0 { return Ok(Some(output)); }
                }
                State::EveryRowDisk { mut probe,build,_build } => {
                    let Some(page) = probe.next_page().await? else { return Ok(None); };
                    if page.len() == 0 { self.state = State::EveryRowDisk { probe,build,_build };continue; }
                    let page = plan.input_page(page,true).await?;
                    let mut output = plan.marked_page(&page)?;
                    for part in 0..build.part_count() {
                        let table = BuildTable::new(build.load(part,&plan.execution.context)?,false,&plan.execution.context.memory)?;
                        plan.compare_all(&mut output,&page,&table).await?;
                    }
                    self.state = State::EveryRowDisk { probe,build,_build };
                    return Ok(Some(output.finish()));
                }
                State::KeyedDisk { probe,build,facts,mut table,mut build_part,mut probe_part,_build_cursor,_probe_cursor } => {
                    if build_part == build.part_count() { return Ok(None); }
                    if table.is_none() { table = Some(BuildTable::new(build.load(build_part,&plan.execution.context)?,false,&plan.execution.context.memory)?); }
                    let page = probe.load(probe_part * build.part_count() + build_part,&plan.execution.context)?.page;
                    let output = plan.keyed_page(page,table.as_ref().ok_or_else(||invalid("mark-state","Loaded build partition is absent"))?,facts).await?;
                    probe_part += 1;
                    if probe_part == probe.part_count() / build.part_count() { table = None;build_part += 1;probe_part = 0; }
                    self.state = State::KeyedDisk { probe,build,facts,table,build_part,probe_part,_build_cursor,_probe_cursor };
                    if output.len() > 0 { return Ok(Some(output)); }
                }
                State::CorrelatedUnopened { input,runtime,arguments } => {
                    self.state = State::Correlated { input: input.open_pages(None).await?,runtime,arguments };
                }
                State::Correlated { mut input,runtime,arguments } => {
                    let Some(mut page) = input.next_page().await? else { return Ok(None); };
                    let mut output = PageBuilder::new(page.len(),&plan.execution.context.memory)?;
                    while let Some(row) = page.next() {
                        let row = Arc::new(row);
                        let execution = plan.execution.bind_arguments(&arguments,&plan.input_schema,Arc::clone(&row)).await?;
                        let dependent = build_pages(Arc::clone(&runtime),execution.child(1)).await?;
                        let result = correlated_mark(&row,dependent,&plan.comparison,&plan.comparison_schema,&execution).await?;
                        output.push(OwnedRow::copy_parts(&[&row.values,&[result]],0,0,&plan.execution.context.memory)?)?;
                    }
                    self.state = State::Correlated { input,runtime,arguments };
                    return Ok(Some(output.finish()));
                }
                State::Done => return Ok(None),
            }
        }
    }
}

pub(crate) async fn correlated_mark(row: &OwnedRow,mut dependent: BoxPageCursor,comparison: &Expr,schema: &Schema,execution: &Execution) -> Result<Value> {
    let mut result = Value::Bool(false);
    while let Some(mut page) = dependent.next_page().await? {
        execution.context.check_cancelled()?;
        let metadata_scope = execution.scope()?;
        let invocation = execution.prepare_expression(comparison,schema,&metadata_scope)?;
        while let Some(candidate) = page.next() {
            execution.context.check_cancelled()?;
            let combined = OwnedRow::copy_parts(&[&row.values,&candidate.values],0,0,&execution.context.memory)?;
            let scope = execution.scope()?;
            match execution.evaluate_invocation(&scope,&invocation,&combined.values).await? {
                Value::Bool(true) => result = Value::Bool(true),
                Value::Null if result != Value::Bool(true) => result = Value::Null,
                Value::Bool(false) | Value::Null => (),
                _ => return Err(invalid("mark-type","Mark comparison must return Boolean or null")),
            }
        }
        if result == Value::Bool(true) { break; }
    }
    Ok(result)
}

pub(crate) async fn correlated_batch_mark(row: &OwnedRow, mut dependent: BoxBatchCursor, comparison: &Expr,
    schema: &Schema, execution: &Execution) -> Result<Value> {
    let context = &execution.context;
    let memory = &context.memory;
    let width = row.values.len();
    let positions = crate::columnar::expression_columns(comparison, schema)?.into_iter()
        .filter_map(|position| position.checked_sub(width)).collect::<Vec<_>>();
    let mut result = Value::Bool(false);
    while let Some(page) = poll_nested_batch(dependent.as_mut()).await? {
        page.validate_readable(context)?;
        let metadata = execution.scope()?;
        let invocation = execution.prepare_expression(comparison, schema, &metadata)?;
        for candidate in 0..page.len {
            context.check_cancelled_at(candidate)?;
            let right = page.expression_row(candidate, &positions, memory)?;
            let combined = OwnedRow::copy_parts(&[&row.values, &right.values], 0, 0, memory)?;
            let scope = execution.scope()?;
            match execution.evaluate_invocation(&scope, &invocation, &combined.values).await? {
                Value::Bool(true) => result = Value::Bool(true),
                Value::Null if result != Value::Bool(true) => result = Value::Null,
                Value::Bool(false) | Value::Null => (),
                _ => return Err(invalid("mark-type", "Mark comparison must return Boolean or null")),
            }
        }
        if result == Value::Bool(true) { break; }
    }
    Ok(result)
}

#[cfg(test)]
mod facts_tests {
    use super::*;
    use rtdb_columnar::MemoryBudget;
    use rtdb_plan::{ColumnId,Field};
    use rtdb_types::ListKind;

    #[test]
    fn nested_build_facts_start_only_after_a_nonempty_page_and_include_all_listy_families() {
        let memory = MemoryBudget::new(1 << 20);
        for ty in [LogicalType::List { kind: ListKind::List,element: Box::new(LogicalType::Null) },LogicalType::List { kind: ListKind::Map { sorted: false },element: Box::new(LogicalType::Struct(Default::default())) },Value::TimestampTzRange { lower: 0,upper: 1 }.logical_type(),LogicalType::Struct(Default::default())] {
            let schema = Schema::new(vec![Field { id: ColumnId(0),name: "k".into(),qualifier: None,qualified_only: false,unnamed: false,data_type: ty.maybe() }]).unwrap();
            let mut facts = CandidateFacts { empty: true,null: false,nested: false };
            facts.add(&PageBuilder::new(0,&memory).unwrap().finish(),&schema,0);
            assert!(facts.empty);assert!(!facts.null);assert!(!facts.nested);assert_eq!(facts.unmatched(true),Value::Bool(false));
            let mut page = PageBuilder::new(1,&memory).unwrap();page.push(OwnedRow::new(vec![Value::Null],&memory).unwrap()).unwrap();let page = page.finish();
            facts.add(&page,&schema,0);
            assert!(!facts.empty);assert!(facts.null);assert!(facts.nested);assert_eq!(facts.unmatched(false),Value::Null);
        }
        assert_eq!(memory.used_bytes(),0);
    }

    struct EmptyCallback { remaining: bool,memory: Arc<MemoryBudget> }
    #[async_trait]
    impl PageCursor for EmptyCallback {
        async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
            if std::mem::take(&mut self.remaining) { Ok(Some(PageBuilder::new(0,&self.memory)?.finish())) } else { Ok(None) }
        }
    }

    #[tokio::test]
    async fn correlated_mark_forwards_actual_empty_callback_without_opening_dependent_rows() {
        use std::collections::BTreeMap;
        use rtdb_storage::{MemoryStore,cache::BufferPool};
        use rtdb_types::{ScalarType,Timestamp};
        use tokio_util::sync::CancellationToken;
        use crate::{CatalogSnapshot,DatabaseSnapshot,QueryContext,QueryOptions,context::OuterRows,parameters::BoundParameters,runtime_schema::EmittedRelation};
        let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
        let context = QueryContext::new(catalog,MemoryBudget::new(1 << 25),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap();
        let execution = Execution { parameters: Arc::new(BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),context: Arc::clone(&context),bindings: None,analysis: None };
        let field = Field { id: ColumnId(1),name: "v".into(),qualifier: None,qualified_only: false,unnamed: false,data_type: LogicalType::Scalar(ScalarType::I64) };
        let relation = Arc::new(Relation::new(Schema::new(vec![field]).unwrap(),rtdb_plan::RelationNode::Values { rows: vec![vec![Expr::binary(BinaryOp::Divide,Expr::Literal(Value::I64(1)),Expr::Literal(Value::I64(0)))]] }).unwrap());
        let runtime = EmittedRelation::derive(Arc::clone(&relation),&context.catalog,execution.parameters.types(),&context.memory).unwrap();
        let mut cursor = Mark {
            state: State::Correlated { input: Box::new(EmptyCallback { remaining: true,memory: Arc::clone(&context.memory) }),runtime: Arc::clone(&runtime),arguments: vec![] },
            plan: MarkPlan { input_schema: Schema::default(),comparison_schema: runtime.schema().clone(),comparison: Expr::Literal(Value::Bool(true)),keys: MarkKeys::EveryRow,execution },
        };
        assert_eq!(cursor.next_page().await.unwrap().unwrap().len(),0);
        assert!(cursor.next_page().await.unwrap().is_none());
    }
}
