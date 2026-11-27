use std::sync::Arc;
use async_trait::async_trait;
use rtdb_plan::{BinaryOp, Expr, Relation, RelationNode, Schema, TemporalFilter, TraceSpan, TraceValue};
use rtdb_types::{ListKind, LogicalType, Result, ScalarType, Value};
use crate::{context::Execution, invalid};
use crate::columnar::{BatchCursor, BoxBatchCursor, Column, ColumnBatch, poll_nested_batch};
use crate::set_keys::BatchKeyIndex;
use crate::batch_join::ProbeInput;
use crate::join_spill::ReplayStats;

fn membership_key(input: &Schema,dependent: &Schema,comparison: &Expr) -> Option<(usize, usize)> {
    let Expr::Binary { op: BinaryOp::Eq,left,right } = comparison else { return None; };
    for (probe,candidate) in [(left.as_ref(),right.as_ref()),(right.as_ref(),left.as_ref())] {
        if let (Expr::Column { id: probe,.. },Expr::Column { id: candidate,.. }) = (probe,candidate) {
            if let (Some(probe),Some(candidate)) = (input.position(*probe),dependent.position(*candidate)) {
                return Some((probe, candidate));
            }
        }
    }
    None
}

pub(crate) fn batch_membership_keys(input: &Schema, dependent: &Schema, comparison: &Expr) -> Option<(usize, usize)> {
    let (probe, candidate) = membership_key(input, dependent, comparison)?;
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
struct CandidateFacts { empty: bool, null: bool }

pub(crate) struct BatchMembership {
    state: MembershipState,
    keys: (usize, usize),
    facts: CandidateFacts,
    spill: bool,
    spill_replay: Option<ReplayStats>,
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
        let spill_replay = crate::trace::operator_evidence_enabled(&execution.context).then(ReplayStats::default);
        Self { state: MembershipState::Build { build, probe }, keys, spill, spill_replay,
            facts: CandidateFacts { empty: true, null: false }, execution }
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
            let key = batch.membership_build_key(build_key, memory)?;
            drop(batch);
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
        let probe_key = self.keys.0;
        let bytes = batch.len.checked_add(256).ok_or_else(mark_size)?;
        let reservation = memory.reserve(bytes)?;
        let mut marks = arrow_array::builder::BooleanBuilder::with_capacity(batch.len);
        for row in 0..batch.len {
            context.check_cancelled_at(row)?;
            match index.mark_column(&batch, probe_key, row, projected, context)? {
                Some(true) => marks.append_value(true),
                equal if !self.facts.empty && (self.facts.null || equal.is_none()) => marks.append_null(),
                _ => marks.append_value(false),
            }
        }
        let logical = LogicalType::Scalar(rtdb_types::ScalarType::Bool).maybe();
        let field = Arc::new(rtdb_columnar::to_field("mark", &logical)?);
        batch.append_column(Column::Arrow { array: Arc::new(marks.finish()), field, logical: Some(logical) }, reservation, memory)?;
        Ok(batch)
    }
}

impl MembershipSpill {
    fn next(&mut self, mark: &mut BatchMembership) -> Result<Option<ColumnBatch>> {
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
                if let Some(stats) = &mut mark.spill_replay {
                    stats.start_build_partition();
                }
                for frame in 0..self.build.frames(self.build_partition) {
                    context.check_cancelled()?;
                    let batch = self.build.read(self.build_partition, frame)?;
                    if let Some(stats) = &mut mark.spill_replay {
                        stats.build_frame(batch.len);
                    }
                    index.build(batch, false, context)?;
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
                if let Some(stats) = &mut mark.spill_replay {
                    stats.probe_frame();
                }
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

    fn trace_attributes(&self, span: &mut dyn TraceSpan) {
        if let Some(stats) = &self.spill_replay {
            stats.record(span);
        }
    }
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

fn mark_keys(schema: &Schema, comparison: &Expr, probe_width: usize) -> Option<(usize, usize)> {
    let Expr::Binary { op: BinaryOp::Eq, left, right } = comparison else { return None; };
    fn admitted(ty: &LogicalType, child: bool) -> bool {
        match ty {
            LogicalType::Maybe(value) | LogicalType::RunEndEncoded { value, .. } => admitted(value, child),
            LogicalType::Union(legs) => legs.iter().all(|leg| admitted(leg, child)),
            LogicalType::Null => true,
            LogicalType::Scalar(ScalarType::Timestamp { .. }) => !child,
            LogicalType::List { kind: ListKind::List | ListKind::FixedSizeList(_), element } => !child && admitted(element, true),
            LogicalType::Scalar(ScalarType::Utf8 | ScalarType::Binary) => true,
            LogicalType::Scalar(ScalarType::Bool | ScalarType::I8 | ScalarType::I16 | ScalarType::I32
                | ScalarType::I64 | ScalarType::F32 | ScalarType::F64) => child,
            _ => false,
        }
    }
    let position = |expr: &Expr| {
        let Expr::Column { id, .. } = expr else { return None; };
        let index = schema.position(*id)?;
        admitted(&schema.fields()[index].data_type, false).then_some(index)
    };
    let (left, right) = (position(left)?, position(right)?);
    let (probe, build) = if left < right { (left, right) } else { (right, left) };
    (probe < probe_width && build >= probe_width).then(|| (probe, build - probe_width))
}

async fn compare_page(
    probe: &crate::columnar::BatchRow<'_>, page: &ColumnBatch, invocation: &rtdb_expr::ExpressionInvocation<'_>,
    mark: &mut Option<bool>, short_circuit: bool, keys: Option<(usize, usize)>, execution: &Execution,
) -> Result<()> {
    for row in 0..page.len {
        execution.context.check_cancelled_at(row)?;
        let input = MarkRow {
            probe: crate::columnar::BatchRow { batch: probe.batch, row: probe.row, admit_dates: probe.admit_dates },
            candidate: crate::columnar::BatchRow { batch: page, row, admit_dates: true },
        };
        let direct = keys.and_then(|(left, right)| {
            crate::set_keys::direct_key_equal(probe.batch.columns.get(left)?, probe.row, page.columns.get(right)?, row, false)
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
    let keys = mark_keys(schema, comparison, batch.columns.len());
    while let Some(page) = poll_nested_batch(dependent.as_mut()).await? {
        page.validate_readable(&execution.context)?;
        let metadata = execution.scope()?;
        let invocation = execution.prepare_expression(comparison, schema, &metadata)?;
        compare_page(&probe, &page, &invocation, &mut mark, false, keys, execution).await?;
        if mark == Some(true) { break; }
    }
    Ok(mark)
}

pub(crate) struct SharedBatchMark {
    state: SharedState,
    schema: Schema,
    comparison: Expr,
    execution: Execution,
    candidate_replay_frames: Option<u64>,
}

enum SharedState {
    Unopened { build: ProbeInput, probe: ProbeInput },
    Open { candidates: CandidatePages, probe: BoxBatchCursor },
    Done,
}

enum CandidatePages {
    Memory(Vec<ColumnBatch>, Arc<rtdb_columnar::MemoryReservation>),
    Spill { file: Arc<crate::spill::SpillFile>, width: usize, multiple_pages: bool },
}

impl SharedBatchMark {
    pub fn new(build: ProbeInput, probe: ProbeInput, schema: Schema, comparison: Expr, execution: Execution) -> Self {
        let candidate_replay_frames = crate::trace::operator_evidence_enabled(&execution.context).then_some(0);
        Self { state: SharedState::Unopened { build, probe }, schema, comparison, execution, candidate_replay_frames }
    }

    async fn candidates(&mut self, build: ProbeInput) -> Result<CandidatePages> {
        let context = &self.execution.context;
        let schema = build.runtime.schema().clone();
        let (mut input, _) = BatchMembership::open(build).await?;
        let mut batches = Vec::new();
        let mut rows = 0i64;
        let mut multiple_pages = false;
        let mut inventory = context.memory.reserve(256)?;
        let mut spill = None::<crate::spill::SpillWriter>;
        while let Some(batch) = poll_nested_batch(input.as_mut()).await? {
            batch.validate_readable(context)?;
            if batch.len == 0 { continue; }
            multiple_pages |= rows > 0;
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
            Some(writer) => Ok(CandidatePages::Spill { file: writer.finish()?, width: schema.len(), multiple_pages }),
            None => Ok(CandidatePages::Memory(batches, inventory)),
        }
    }

    async fn apply(&mut self, mut batch: ColumnBatch, candidates: &CandidatePages) -> Result<ColumnBatch> {
        let context = &self.execution.context;
        batch.validate_readable(context)?;
        let memory = &context.memory;
        let reservation = memory.reserve(batch.len.checked_mul(2).and_then(|bytes| bytes.checked_add(256)).ok_or_else(mark_size)?)?;
        let keys = mark_keys(&self.schema, &self.comparison, batch.columns.len());
        let metadata = self.execution.scope()?;
        let invocation = self.execution.prepare_expression(&self.comparison, &self.schema, &metadata)?;
        let mut marks = arrow_array::builder::BooleanBuilder::with_capacity(batch.len);
        let mut spill = match candidates {
            CandidatePages::Spill { file, .. } => Some(file.reader(context)?),
            _ => None,
        };
        let mut first_page = None;
        for row in 0..batch.len {
            context.check_cancelled_at(row)?;
            let probe = crate::columnar::BatchRow { batch: &batch, row, admit_dates: true };
            let mut mark = Some(false);
            match candidates {
                CandidatePages::Memory(pages, _charge) => for page in pages {
                    compare_page(&probe, page, &invocation, &mut mark, true, keys, &self.execution).await?;
                    if mark == Some(true) { break; }
                },
                CandidatePages::Spill { width, multiple_pages, .. } => {
                    let reader = spill.as_mut().expect("The mark opened its candidate spill");
                    let replay_frames = &mut self.candidate_replay_frames;
                    if first_page.is_none() {
                        reader.rewind()?;
                        first_page = read_mark_candidate(reader, *width, replay_frames)?;
                    }
                    if let Some(page) = &first_page {
                        compare_page(&probe, page, &invocation, &mut mark, true, keys, &self.execution).await?;
                        if mark != Some(true) {
                            if *multiple_pages {
                                drop(first_page.take());
                                while let Some(page) = read_mark_candidate(reader, *width, replay_frames)? {
                                    compare_page(&probe, &page, &invocation, &mut mark, true, keys, &self.execution).await?;
                                    if mark == Some(true) { break; }
                                }
                            } else if context.cancellation.is_cancelled() || context.owner_cancellation.is_cancelled() {
                                return Err(crate::context::cancelled());
                            }
                        }
                    }
                }
            }
            marks.append_option(mark);
        }
        drop(first_page);
        let ty = LogicalType::Scalar(rtdb_types::ScalarType::Bool).maybe();
        let field = Arc::new(rtdb_columnar::to_field("mark", &ty)?);
        batch.append_column(Column::Arrow { array: Arc::new(marks.finish()), field, logical: Some(ty) }, reservation, memory)?;
        Ok(batch)
    }
}

fn read_mark_candidate(
    reader: &mut crate::spill::SpillReader, width: usize, replay_frames: &mut Option<u64>,
) -> Result<Option<ColumnBatch>> {
    let page = reader.read_columns(width)?;
    if let Some(frames) = replay_frames
        && page.is_some()
    {
        *frames = frames.saturating_add(1);
    }
    Ok(page)
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

    fn trace_attributes(&self, span: &mut dyn TraceSpan) {
        if let Some(frames) = self.candidate_replay_frames {
            span.attribute("cursor.mark_spill.candidate_replay_frames", TraceValue::Unsigned(frames));
        }
    }
}

#[cfg(test)]
mod facts_tests {
    use std::{collections::BTreeMap, sync::Arc};
    use rtdb_columnar::MemoryBudget;
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::{LogicalType, TimeUnit, Timestamp, Value};
    use tokio_util::sync::CancellationToken;

    use super::{Column, ColumnBatch, membership_hash};
    use crate::join_storage::hash_values;

    fn hash_context() -> Arc<crate::QueryContext> {
        let database = crate::DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
        let catalog = crate::CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        crate::QueryContext::new(catalog, MemoryBudget::new(64 * 1024), Timestamp(0), "UTC",
            crate::QueryOptions::default(), &CancellationToken::new()).unwrap()
    }

    #[test]
    fn membership_hash_reads_timestamp_vectors_without_scalar_scratch() {
        let context = hash_context();
        let source = MemoryBudget::new(1 << 20);
        let _full = context.memory.reserve(context.memory.available_bytes()).unwrap();
        for unit in [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            for zone in [None, Some("America/New_York")] {
                let timestamp = |value| Value::Timestamp { value, unit, timezone: zone.map(str::to_owned) };
                let logical = timestamp(0).logical_type().maybe();
                let values = [timestamp(i64::MIN), timestamp(0), Value::Null, timestamp(i64::MAX)];
                let field = Arc::new(rtdb_columnar::to_field("t", &logical).unwrap());
                let array = rtdb_columnar::build_array(&logical, &values).unwrap();
                let charge = source.reserve(array.get_array_memory_size() + 1024).unwrap();
                let column = Column::Arrow { array, field, logical: Some(logical) };
                let batch = ColumnBatch::settled(vec![column], values.len(), charge).unwrap();
                for (row, value) in values.iter().enumerate() {
                    assert_eq!(membership_hash(&batch, row, 0, &context).unwrap(), hash_values([value]).unwrap());
                }
            }
        }
    }

    #[tokio::test]
    async fn mark_compares_funded_text_and_binary_cells_below_the_decode_budget() {
        use arrow_array::{ArrayRef, BinaryArray, StringArray};
        use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, Schema};
        use rtdb_types::ScalarType;
        use crate::{columnar::BatchRow, context::{Execution, OuterRows}, parameters::BoundParameters};

        let context = hash_context();
        let execution = Execution {
            parameters: Arc::new(BoundParameters::infer(vec![], &context.memory).unwrap()),
            outer: OuterRows::empty(&context.memory).unwrap(),
            context: Arc::clone(&context), bindings: None, analysis: None,
        };
        let field = |id, scalar| Field { id: ColumnId(id), name: format!("k{id}"), qualifier: None,
            qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(scalar).maybe() };
        let left = field(0, ScalarType::Utf8);
        let right = field(1, ScalarType::Binary);
        let comparison = Expr::binary(BinaryOp::Eq, right.expression(), left.expression());
        let schema = Schema::new(vec![left.clone(), right.clone()]).unwrap();
        let keys = super::mark_keys(&schema, &comparison, 1);
        assert_eq!(keys, Some((0, 0)));

        let source = MemoryBudget::new(1 << 20);
        let batch = |field: &Field, array: ArrayRef| {
            let len = array.len();
            let arrow = Arc::new(rtdb_columnar::to_field(&field.name, &field.data_type).unwrap());
            ColumnBatch::settled(vec![Column::Arrow { array, field: arrow, logical: Some(field.data_type.clone()) }],
                len, source.reserve(1 << 18).unwrap()).unwrap()
        };
        let spelling = "été".repeat(16_384);
        let probe = batch(&left, Arc::new(StringArray::from(vec![spelling.as_str()])));
        let unequal = batch(&right, Arc::new(BinaryArray::from(vec![b"other".as_slice()])));
        let unknown = batch(&right, Arc::new(BinaryArray::from(vec![None::<&[u8]>])));
        let matched = batch(&right, Arc::new(BinaryArray::from(vec![Some(spelling.as_bytes())])));
        assert!(probe.columns[0].bound(0).unwrap() > context.memory.limit_bytes());
        assert!(matched.columns[0].bound(0).unwrap() > context.memory.limit_bytes());
        let probe = BatchRow { batch: &probe, row: 0, admit_dates: true };
        let metadata = execution.scope().unwrap();
        let invocation = execution.prepare_expression(&comparison, &schema, &metadata).unwrap();
        for short_circuit in [true, false] {
            let mut mark = Some(false);
            super::compare_page(&probe, &unequal, &invocation, &mut mark, short_circuit, keys, &execution).await.unwrap();
            assert_eq!(mark, Some(false));
            super::compare_page(&probe, &unknown, &invocation, &mut mark, short_circuit, keys, &execution).await.unwrap();
            assert_eq!(mark, None);
            super::compare_page(&probe, &matched, &invocation, &mut mark, short_circuit, keys, &execution).await.unwrap();
            assert_eq!(mark, Some(true));
        }

        let small = batch(&left, Arc::new(StringArray::from(vec!["match"])));
        let probe = BatchRow { batch: &small, row: 0, admit_dates: true };
        let missing = ColumnBatch::settled(vec![Column::Values(vec![Value::Binary(b"match".to_vec())])],
            2, source.reserve(4096).unwrap()).unwrap();
        let mut mark = Some(false);
        super::compare_page(&probe, &missing, &invocation, &mut mark, true, keys, &execution).await.unwrap();
        assert_eq!(mark, Some(true));
        let mut mark = Some(false);
        let error = super::compare_page(&probe, &missing, &invocation, &mut mark, false, keys, &execution).await.unwrap_err();
        assert_eq!(error.code, "rtdb.query/column-row");
    }

    #[test]
    fn membership_hash_reads_decorated_float_lists_under_the_cell_decode_budget() {
        use std::collections::HashMap;
        use arrow_array::{Array, Float32Array, ListArray};
        use arrow_buffer::OffsetBuffer;
        use arrow_schema::{DataType, Field};

        let context = hash_context();
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
        let crate::set_keys::BloomKey { hashes: actual, scalar, .. } = crate::set_keys::direct_bloom_hashes(&batch.columns[0], 0).unwrap();
        assert_eq!(actual, (rtdb_columnar::hash_value(&value, 0).unwrap(), rtdb_columnar::hash_value_murmur3(&value, 0).unwrap()));
        assert!(scalar.is_none());
    }
}
