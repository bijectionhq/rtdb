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
    let MarkKeys::Membership { probe, candidate } = membership_key(input, dependent, comparison)? else { return None; };
    let probe_type = &input.fields()[probe].data_type;
    let candidate_type = &dependent.fields()[candidate].data_type;
    let scalar = |ty: &LogicalType| ty.legs().into_iter().all(|leg| matches!(leg, LogicalType::Null | LogicalType::Scalar(_)));
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

// The build retains only comparison keys. The probe keeps its payload and adds a Boolean vector.
pub(crate) struct BatchMembership {
    state: MembershipState,
    keys: (usize, usize),
    facts: CandidateFacts,
    spill: bool,
    execution: Execution,
}

enum MembershipState {
    Build { build: ProbeInput, probe: ProbeInput },
    Memory { probe: BoxBatchCursor, projected: bool, index: BatchKeyIndex },
    Spill(MembershipSpill),
    Done,
}

struct MembershipSpill {
    build: crate::join_spill::Partitions,
    probe: crate::join_spill::Partitions,
    index: Option<BatchKeyIndex>,
    build_partition: usize,
    probe_partition: usize,
    ready: Option<(std::collections::VecDeque<ColumnBatch>, Arc<rtdb_columnar::MemoryReservation>)>,
}

impl BatchMembership {
    pub fn new(build: ProbeInput, probe: ProbeInput, keys: (usize, usize),
        spill: bool, execution: Execution) -> Self {
        Self { state: MembershipState::Build { build, probe }, keys, spill,
            facts: CandidateFacts { empty: true, null: false, nested: false }, execution }
    }

    async fn open(input: ProbeInput) -> Result<(BoxBatchCursor, bool)> {
        crate::operators::build_batch_input(input.runtime, input.execution, input.pushdowns, input.output).await?
            .ok_or_else(|| invalid("mark-input", "Membership input cannot open as Arrow"))
    }

    fn is_null(column: &Column, row: usize, context: &crate::QueryContext) -> Result<bool> {
        if let Some(null) = column.null_at(row) { return Ok(null); }
        let _scratch = context.memory.reserve(column.bound(row)?)?;
        Ok(matches!(column.value(row)?, Value::Null | Value::Undefined))
    }

    async fn build(&mut self, build: ProbeInput, probe: ProbeInput) -> Result<MembershipState> {
        use crate::join_spill::BlockWriter;
        let context = &self.execution.context;
        let memory = &context.memory;
        let (probe_key, build_key) = self.keys;
        let schema = Schema::new(vec![build.runtime.schema().fields()[build_key].clone()])?;
        let (mut build, projected) = Self::open(build).await?;
        let mut index = BatchKeyIndex::vector_hashing(context)?;
        let mut held = Vec::new();
        let mut inventory = memory.reserve(0)?;
        let mut writer = None::<BlockWriter>;
        let mut count = 0i64;
        while let Some(batch) = poll_nested_batch(build.as_mut()).await? {
            batch.validate_readable(context)?;
            if batch.len == 0 { continue; }
            self.facts.empty = false;
            for row in 0..batch.len {
                context.check_cancelled_at(row)?;
                self.facts.null |= Self::is_null(&batch.columns[build_key], row, context)?;
            }
            let key = {
                let bytes = batch.len.checked_mul(std::mem::size_of::<(usize, usize)>()).ok_or_else(mark_size)?;
                let _rows = memory.reserve(bytes)?;
                let rows = (0..batch.len).map(|row| (0, row)).collect::<Vec<_>>();
                let key = ColumnBatch::take_rows_with_custody(&[&batch], &rows, &[build_key], true, memory)?;
                key.compact_group_keys(&rows, memory)?
            };
            if !self.spill {
                index.build(key, projected, context)?;
                continue;
            }
            if let Some(writer) = &mut writer {
                writer.append(key, context)?;
                continue;
            }
            count = count.checked_add(i64::try_from(key.len).map_err(|_| mark_size())?).ok_or_else(mark_size)?;
            if count > context.options.join_build_rows {
                let mut spill = BlockWriter::new(&schema, context)?;
                for key in held.drain(..) { spill.append(key, context)?; }
                spill.append(key, context)?;
                writer = Some(spill);
            } else {
                let bytes = (held.len() + 1).checked_mul(4 * std::mem::size_of::<ColumnBatch>()).ok_or_else(mark_size)?;
                Arc::get_mut(&mut inventory).expect("Membership owns its batch inventory").grow_to(bytes)?;
                held.push(key);
            }
        }
        drop(build);
        let Some(build) = writer else {
            for key in held { index.build(key, projected, context)?; }
            let (probe, projected) = Self::open(probe).await?;
            return Ok(MembershipState::Memory { probe, projected, index });
        };
        let build = build.finish(context)?.shuffle(1, context, |batch, row| membership_hash(batch, row, 0, context))?;
        let schema = probe.runtime.schema().clone();
        let (mut probe, _) = Self::open(probe).await?;
        let mut writer = BlockWriter::new(&schema, context)?;
        while let Some(batch) = poll_nested_batch(probe.as_mut()).await? {
            batch.validate_readable(context)?;
            writer.append(batch, context)?;
        }
        let probe = writer.finish(context)?.shuffle(build.count(), context, |batch, row| membership_hash(batch, row, probe_key, context))?;
        let disk = MembershipSpill { build, probe, index: None, build_partition: 0, probe_partition: 0, ready: None };
        Ok(MembershipState::Spill(disk))
    }

    fn mark_batch(&self, mut batch: ColumnBatch, index: &mut BatchKeyIndex, projected: bool) -> Result<ColumnBatch> {
        let context = &self.execution.context;
        let memory = &context.memory;
        let key = batch.key_projection(&[self.keys.0], memory)?;
        let bytes = batch.len.checked_add(256).ok_or_else(mark_size)?;
        let reservation = memory.reserve(bytes)?;
        let mut marks = arrow_array::builder::BooleanBuilder::with_capacity(batch.len);
        for row in 0..batch.len {
            context.check_cancelled_at(row)?;
            let null = Self::is_null(&key.columns[0], row, context)?;
            if !null && index.matches(&key, row, projected, false, context)? {
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
        Ok(batch)
    }
}

impl MembershipSpill {
    fn next(&mut self, mark: &BatchMembership) -> Result<Option<ColumnBatch>> {
        let context = &mark.execution.context;
        loop {
            context.check_cancelled()?;
            if let Some(batch) = self.ready.as_mut().and_then(|(batches, _)| batches.pop_front()) { return Ok(Some(batch)); }
            self.ready = None;
            let builds = self.build.count();
            if self.build_partition == builds { return Ok(None); }
            if self.probe_partition == self.probe.count() / builds {
                self.index = None;
                self.build_partition += 1;
                self.probe_partition = 0;
                continue;
            }
            if self.index.is_none() {
                let mut index = BatchKeyIndex::vector_hashing(context)?;
                for frame in 0..self.build.frames(self.build_partition) {
                    context.check_cancelled()?;
                    index.build(self.build.read(self.build_partition, frame)?, false, context)?;
                }
                self.index = Some(index);
            }
            let index = self.index.as_mut().expect("The build partition is loaded");
            let partition = self.probe_partition * builds + self.build_partition;
            let count = self.probe.frames(partition);
            let bytes = count.checked_mul(2 * std::mem::size_of::<ColumnBatch>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(mark_size)?;
            let reservation = context.memory.reserve(bytes)?;
            let mut ready = std::collections::VecDeque::with_capacity(count);
            for frame in 0..count {
                let batch = self.probe.read(partition, frame)?;
                ready.push_back(mark.mark_batch(batch, index, false)?);
            }
            self.probe_partition += 1;
            self.ready = Some((ready, reservation));
        }
    }
}

fn membership_hash(batch: &ColumnBatch, row: usize, key: usize, context: &crate::QueryContext) -> Result<u32> {
    crate::set_keys::Key::for_columns(batch, row, false, &[key], context)?.vector_hash()
}

fn mark_size() -> rtdb_types::Anomaly {
    invalid("mark-size", "Membership state exceeds addressable memory")
}

#[async_trait]
impl BatchCursor for BatchMembership {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        loop {
            self.execution.context.check_cancelled()?;
            match std::mem::replace(&mut self.state, MembershipState::Done) {
                MembershipState::Build { build, probe } => {
                    self.state = self.build(build, probe).await?;
                }
                MembershipState::Memory { mut probe, projected, mut index } => {
                    while let Some(batch) = poll_nested_batch(probe.as_mut()).await? {
                        if batch.len == 0 { continue; }
                        batch.validate_readable(&self.execution.context)?;
                        let output = self.mark_batch(batch, &mut index, projected)?;
                        self.state = MembershipState::Memory { probe, projected, index };
                        return Ok(Some(output));
                    }
                    return Ok(None);
                }
                MembershipState::Spill(mut disk) => {
                    let output = disk.next(self)?;
                    if output.is_some() { self.state = MembershipState::Spill(disk); }
                    return Ok(output);
                }
                MembershipState::Done => return Ok(None),
            }
        }
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

struct MarkRow<'a> {
    probe: crate::columnar::BatchRow<'a>,
    candidate: crate::columnar::BatchRow<'a>,
}

impl rtdb_expr::RowInput for MarkRow<'_> {
    fn arrow_cell(&self, index: usize) -> Option<rtdb_columnar::ArrowCell<'_>> {
        if index < self.probe.batch.columns.len() { self.probe.arrow_cell(index) }
        else { self.candidate.arrow_cell(index - self.probe.batch.columns.len()) }
    }

    fn value(&self, index: usize, scope: Option<&rtdb_expr::EvaluationScope>) -> Result<Option<Value>> {
        if index < self.probe.batch.columns.len() { self.probe.value(index, scope) }
        else { self.candidate.value(index - self.probe.batch.columns.len(), scope) }
    }
}

async fn compare_page(
    probe: &crate::columnar::BatchRow<'_>, page: &ColumnBatch, invocation: &rtdb_expr::ExpressionInvocation<'_>,
    mark: &mut Option<bool>, short_circuit: bool, vector_keys: Option<(usize, usize)>, execution: &Execution,
) -> Result<()> {
    for row in 0..page.len {
        execution.context.check_cancelled_at(row)?;
        let input = MarkRow {
            probe: crate::columnar::BatchRow { batch: probe.batch, row: probe.row, admit_dates: probe.admit_dates },
            candidate: crate::columnar::BatchRow { batch: page, row, admit_dates: true },
        };
        let direct = vector_keys.and_then(|(left, right)| {
            crate::set_keys::direct_key_equal(&probe.batch.columns[left], probe.row, &page.columns[right], row, false)
        });
        let equal = if let Some(equal) = direct {
            equal
        } else {
            let scope = execution.scope()?;
            match execution.evaluate_invocation_row(&scope, invocation, rtdb_expr::ExpressionRow::Borrowed(&input)).await? {
                Value::Bool(equal) => Some(equal),
                Value::Null => None,
                _ => return Err(invalid("mark-type", "Mark comparison must return Boolean or null")),
            }
        };
        match equal {
            Some(true) => {
                *mark = Some(true);
                if short_circuit { break; }
            }
            None if *mark != Some(true) => *mark = None,
            Some(false) | None => (),
        }
    }
    Ok(())
}

pub(crate) async fn correlated_batch_mark(batch: &ColumnBatch, row: usize, mut dependent: BoxBatchCursor, comparison: &Expr,
    schema: &Schema, execution: &Execution) -> Result<Option<bool>> {
    let mut mark = Some(false);
    let probe = crate::columnar::BatchRow { batch, row, admit_dates: true };
    while let Some(page) = poll_nested_batch(dependent.as_mut()).await? {
        page.validate_readable(&execution.context)?;
        let metadata = execution.scope()?;
        let invocation = execution.prepare_expression(comparison, schema, &metadata)?;
        compare_page(&probe, &page, &invocation, &mut mark, false, None, execution).await?;
        if mark == Some(true) { break; }
    }
    Ok(mark)
}

pub(crate) struct SharedBatchMark {
    state: SharedState,
    schema: Schema,
    comparison: Expr,
    execution: Execution,
}

enum SharedState {
    Unopened { build: ProbeInput, probe: ProbeInput },
    Open { candidates: CandidatePages, probe: BoxBatchCursor },
    Done,
}

enum CandidatePages {
    Memory(Vec<ColumnBatch>, Arc<rtdb_columnar::MemoryReservation>),
    Spill(Arc<crate::spill::SpillFile>, usize),
}

impl SharedBatchMark {
    pub fn new(build: ProbeInput, probe: ProbeInput, schema: Schema, comparison: Expr, execution: Execution) -> Self {
        Self { state: SharedState::Unopened { build, probe }, schema, comparison, execution }
    }

    async fn candidates(&mut self, build: ProbeInput) -> Result<CandidatePages> {
        let context = &self.execution.context;
        let schema = build.runtime.schema().clone();
        let (mut input, _) = BatchMembership::open(build).await?;
        let mut batches = Vec::new();
        let mut rows = 0i64;
        let mut inventory = context.memory.reserve(256)?;
        let mut spill = None::<crate::spill::SpillWriter>;
        while let Some(batch) = poll_nested_batch(input.as_mut()).await? {
            batch.validate_readable(context)?;
            if batch.len == 0 { continue; }
            rows = rows.checked_add(i64::try_from(batch.len).map_err(|_| mark_size())?).ok_or_else(mark_size)?;
            if spill.is_none() && rows > context.options.join_build_rows {
                let mut writer = context.spill.create()?;
                for held in batches.drain(..) { writer.write_columns(&held, &schema, &context.memory)?; }
                spill = Some(writer);
            }
            if let Some(writer) = &mut spill {
                writer.write_columns(&batch, &schema, &context.memory)?;
            } else {
                let bytes = (batches.len() + 1).checked_mul(4 * std::mem::size_of::<ColumnBatch>()).ok_or_else(mark_size)?;
                Arc::get_mut(&mut inventory).expect("Mark owns its batch inventory").grow_to(bytes)?;
                batches.push(batch);
            }
        }
        match spill {
            Some(writer) => Ok(CandidatePages::Spill(writer.finish()?, schema.len())),
            None => Ok(CandidatePages::Memory(batches, inventory)),
        }
    }

    fn vector_keys(&self, probe_width: usize) -> Option<(usize, usize)> {
        let Expr::Binary { op: BinaryOp::Eq, left, right } = &self.comparison else { return None; };
        let position = |expr: &Expr| {
            let Expr::Column { id, .. } = expr else { return None; };
            let index = self.schema.position(*id)?;
            crate::columnar::float_vector::logical_vector(&self.schema.fields()[index].data_type).then_some(index)
        };
        let (left, right) = (position(left)?, position(right)?);
        let (probe, build) = if left < right { (left, right) } else { (right, left) };
        (probe < probe_width && build >= probe_width).then(|| (probe, build - probe_width))
    }

    async fn apply(&mut self, mut batch: ColumnBatch, candidates: &CandidatePages) -> Result<ColumnBatch> {
        let context = &self.execution.context;
        batch.validate_readable(context)?;
        let memory = &context.memory;
        let reservation = memory.reserve(batch.len.checked_mul(2).and_then(|bytes| bytes.checked_add(256)).ok_or_else(mark_size)?)?;
        let vector_keys = self.vector_keys(batch.columns.len());
        let metadata = self.execution.scope()?;
        let invocation = self.execution.prepare_expression(&self.comparison, &self.schema, &metadata)?;
        let mut marks = arrow_array::builder::BooleanBuilder::with_capacity(batch.len);
        let mut spill = match candidates {
            CandidatePages::Spill(file, width) => Some((file.reader(Arc::clone(memory))?, *width)),
            _ => None,
        };
        for row in 0..batch.len {
            context.check_cancelled_at(row)?;
            let probe = crate::columnar::BatchRow { batch: &batch, row, admit_dates: true };
            let mut mark = Some(false);
            match candidates {
                CandidatePages::Memory(pages, _charge) => for page in pages {
                    compare_page(&probe, page, &invocation, &mut mark, true, vector_keys, &self.execution).await?;
                    if mark == Some(true) { break; }
                },
                CandidatePages::Spill(_, _) => {
                    let (reader, width) = spill.as_mut().expect("The mark opened its candidate spill");
                    reader.rewind()?;
                    while let Some(page) = reader.read_columns(*width)? {
                        compare_page(&probe, &page, &invocation, &mut mark, true, vector_keys, &self.execution).await?;
                        if mark == Some(true) { break; }
                    }
                }
            }
            marks.append_option(mark);
        }
        let ty = LogicalType::Scalar(rtdb_types::ScalarType::Bool).maybe();
        let field = Arc::new(rtdb_columnar::to_field("mark", &ty)?);
        batch.append_column(Column::Arrow { array: Arc::new(marks.finish()), field, logical: Some(ty) }, reservation, memory)?;
        Ok(batch)
    }
}

#[async_trait]
impl BatchCursor for SharedBatchMark {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        loop {
            self.execution.context.check_cancelled()?;
            match std::mem::replace(&mut self.state, SharedState::Done) {
                SharedState::Unopened { build, probe } => {
                    let candidates = self.candidates(build).await?;
                    let (probe, _) = BatchMembership::open(probe).await?;
                    self.state = SharedState::Open { candidates, probe };
                }
                SharedState::Open { candidates, mut probe } => {
                    let Some(batch) = poll_nested_batch(probe.as_mut()).await? else { return Ok(None); };
                    let output = self.apply(batch, &candidates).await?;
                    self.state = SharedState::Open { candidates, probe };
                    return Ok(Some(output));
                }
                SharedState::Done => return Ok(None),
            }
        }
    }
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

    #[test]
    fn membership_hash_reads_decorated_float_lists_under_the_cell_decode_budget() {
        use std::collections::{BTreeMap, HashMap};
        use arrow_array::{Array, Float32Array, ListArray};
        use arrow_buffer::OffsetBuffer;
        use arrow_schema::{DataType, Field};
        use rtdb_storage::{MemoryStore, cache::BufferPool};
        use rtdb_types::Timestamp;
        use tokio_util::sync::CancellationToken;

        let database = crate::DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
        let catalog = crate::CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let context = crate::QueryContext::new(catalog, MemoryBudget::new(64 * 1024),
            Timestamp(0), "UTC", crate::QueryOptions::default(), &CancellationToken::new()).unwrap();
        let values = Float32Array::from((0..8192).map(|row| (row % 2 == 0).then_some(-(row as f32))).collect::<Vec<_>>());
        let items = Arc::new(Field::new("item", DataType::Float32, true).with_metadata(HashMap::from([("source".into(), "test".into())])));
        let array = Arc::new(ListArray::new(items, OffsetBuffer::new(vec![0, 8192].into()), Arc::new(values.clone()), None));
        let field = Arc::new(Field::new("v", array.data_type().clone(), false));
        let column = Column::Arrow { array, logical: Some(rtdb_columnar::from_field(&field).unwrap()), field };
        assert!(column.bound(0).unwrap() > context.memory.limit_bytes());
        let charge = MemoryBudget::new(1 << 20).reserve(column.retained_bytes().unwrap() + 512).unwrap();
        let batch = ColumnBatch::settled(vec![column], 1, charge).unwrap();
        let value = Value::List(rtdb_types::ListValue::from_f32(values));

        assert_eq!(membership_hash(&batch, 0, 0, &context).unwrap(), hash_values([&value]).unwrap());
        let (actual, scalar) = crate::set_keys::direct_bloom_hashes(&batch.columns[0], 0).unwrap();
        assert_eq!(actual, (rtdb_columnar::hash_value(&value, 0).unwrap(), rtdb_columnar::hash_value_murmur3(&value, 0).unwrap()));
        assert!(scalar.is_none());
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
