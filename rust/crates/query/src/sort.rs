use std::{cmp::Ordering, sync::Arc};
use async_trait::async_trait;
use rtdb_columnar::MemoryReservation;
use rtdb_expr::{ordered_compare_in_timezone, validate_order_type};
use rtdb_plan::{Schema, SortExpr};
use rtdb_types::{LogicalType, Result, Value};
use crate::{context::Execution, cursor::{BoxCursor, OwnedRow, RowCursor}, invalid, pages::{BoxPageCursor, OwnedPage, PageBuilder, PageCursor}, spill::{SpillFile, SpillReader}};

mod records;
mod timsort;
mod legacy;
pub(crate) use records::{RecordBuffer, RecordStore};
use records::RecordCursor;

const BLOCK_ROWS: usize = 102_400;
const MERGE_FAN_IN: usize = 4;

struct SortPlan { schema: Schema, order: Vec<SortExpr>, limit: u64, execution: Execution }
pub(crate) struct SortCursor { plan: SortPlan, phase: SortPhase }
enum SortPhase { Input(BoxPageCursor), Output(SortOutput), Done }
enum OutputRows { Records(RecordCursor), Spill(SpillReader) }
struct SortOutput { rows: OutputRows, remaining: u64, page_rows: usize, empty_callback: bool }
impl SortOutput {
    fn row(&mut self, plan: &SortPlan) -> Result<Option<OwnedRow>> {
        if self.remaining == 0 { return Ok(None); }
        plan.execution.context.check_cancelled()?;
        let row = match &mut self.rows { OutputRows::Records(rows) => rows.next()?, OutputRows::Spill(rows) => rows.next()? };
        let mut row = row.ok_or_else(|| invalid("sort-state", "Sorted records ended before their row count"))?;
        if row.values.len() != plan.order.len() + plan.schema.len() { return Err(invalid("invalid-spill", "Sorted record has an invalid width")); }
        row.values.drain(..plan.order.len());
        self.remaining -= 1;
        Ok(Some(row))
    }
    fn page(&mut self, plan: &SortPlan) -> Result<Option<OwnedPage>> {
        if self.remaining == 0 && !self.empty_callback { return Ok(None); }
        self.empty_callback = false;
        let count = self.remaining.min(self.page_rows as u64) as usize;
        let mut page = PageBuilder::new(count, &plan.execution.context.memory)?;
        for _ in 0..count { page.push(self.row(plan)?.ok_or_else(|| invalid("sort-state", "Sorted page ended early"))?)?; }
        Ok(Some(page.finish()))
    }
}
impl SortCursor {
    pub fn new(input: BoxCursor, schema: Schema, order: Vec<SortExpr>, execution: Execution) -> Self {
        Self::from_pages(crate::pages::from_rows(input, Arc::clone(&execution.context)), schema, order, execution)
    }
    pub fn from_pages(input: BoxPageCursor, schema: Schema, order: Vec<SortExpr>, execution: Execution) -> Self {
        Self::with_row_limit(input, schema, order, i64::MAX as u64, execution)
    }
    pub fn with_row_limit(input: BoxPageCursor, schema: Schema, order: Vec<SortExpr>, limit: u64, execution: Execution) -> Self {
        Self { plan: SortPlan { schema, order, limit, execution }, phase: SortPhase::Input(input) }
    }
    async fn initialize(&mut self) -> Result<()> {
        if !matches!(self.phase, SortPhase::Input(_)) { return Ok(()); }
        let SortPhase::Input(input) = std::mem::replace(&mut self.phase, SortPhase::Done) else { unreachable!() };
        if let Some(output) = self.plan.initialize(input).await? { self.phase = SortPhase::Output(output); }
        Ok(())
    }
}
#[async_trait]
impl RowCursor for SortCursor {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        if let Err(error) = self.plan.execution.context.check_cancelled() { self.phase = SortPhase::Done; return Err(error); }
        self.initialize().await?;
        let SortPhase::Output(mut output) = std::mem::replace(&mut self.phase, SortPhase::Done) else { return Ok(None); };
        let row = output.row(&self.plan)?;
        if row.is_some() { self.phase = SortPhase::Output(output); }
        Ok(row)
    }
}
#[async_trait]
impl PageCursor for SortCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        if let Err(error) = self.plan.execution.context.check_cancelled() { self.phase = SortPhase::Done; return Err(error); }
        self.initialize().await?;
        let SortPhase::Output(mut output) = std::mem::replace(&mut self.phase, SortPhase::Done) else { return Ok(None); };
        let page = output.page(&self.plan)?;
        if page.is_some() { self.phase = SortPhase::Output(output); }
        Ok(page)
    }
}

struct Accumulator { records: RecordBuffer, keys: KeyTypes }
impl SortPlan {
    fn accumulator(&self) -> Result<Accumulator> {
        Ok(Accumulator { records: RecordBuffer::new(Arc::clone(&self.execution.context))?, keys: KeyTypes::declared(self)? })
    }
    async fn append(&self, accumulator: &mut Accumulator, mut page: OwnedPage) -> Result<()> {
        let mut keys = crate::pages::project(&page, &self.schema, self.order.iter().map(|order| &order.expr), &self.execution).await?;
        while let Some(row) = page.next() {
            self.execution.context.check_cancelled()?;
            let key = keys.next().ok_or_else(|| invalid("sort-state", "Projected sort keys ended before their input page"))?;
            accumulator.keys.observe(&key.values, self)?;
            accumulator.records.push(OwnedRow::copy_parts(&[&key.values, &row.values], 0, 0, &self.execution.context.memory)?)?;
        }
        Ok(())
    }
    fn indices(&self, store: &RecordStore, keys: &KeyTypes) -> Result<Indices> {
        keys.validate()?;
        let mut indices = sort_records(store,&self.execution.context,|left,right|self.compare(left,right))?;
        indices.values.truncate(usize::try_from(self.limit).unwrap_or(usize::MAX));
        Ok(indices)
    }
    fn compare(&self, left: &[Value], right: &[Value]) -> Result<Ordering> {
        for (index, order) in self.order.iter().enumerate() {
            let left = left.get(index).ok_or_else(|| invalid("invalid-spill", "Sort key is absent"))?;
            let right = right.get(index).ok_or_else(|| invalid("invalid-spill", "Sort key is absent"))?;
            let mut comparison = ordered_compare_in_timezone(left, right, order.nulls_first, &self.execution.context.timezone)?;
            if order.descending && !matches!(left, Value::Null | Value::Undefined) && !matches!(right, Value::Null | Value::Undefined) { comparison = comparison.reverse(); }
            if comparison != Ordering::Equal { return Ok(comparison); }
        }
        Ok(Ordering::Equal)
    }
    fn reduce(&self, accumulator: Accumulator) -> Result<Accumulator> {
        let Accumulator { records, keys } = accumulator;
        let store = records.finish()?;
        let indices = self.indices(&store, &keys)?;
        let selected = store.select(&indices.values, Arc::clone(&self.execution.context))?;
        let mut rows = selected.into_cursor()?;
        drop(indices);
        drop(store);
        let mut records = RecordBuffer::new(Arc::clone(&self.execution.context))?;
        while let Some(row) = rows.next()? { self.execution.context.check_cancelled()?; records.push(row)?; }
        Ok(Accumulator { records, keys })
    }
    fn write_run(&self, accumulator: Accumulator) -> Result<SortedRun> {
        let Accumulator { records, keys } = accumulator;
        let store = records.finish()?;
        let indices = self.indices(&store, &keys)?;
        let mut access = store.access()?;
        let mut writer = self.execution.context.spill.create()?;
        for index in &indices.values {
            self.execution.context.check_cancelled()?;
            writer.write(access.get(*index)?.values(), &self.execution.context.memory)?;
        }
        Ok(SortedRun { file: writer.finish()?, rows: indices.values.len() as u64, page_rows: indices.values.len(), keys })
    }
    async fn initialize(&self, mut input: BoxPageCursor) -> Result<Option<SortOutput>> {
        let mut accumulator = self.accumulator()?;
        while accumulator.records.len() <= BLOCK_ROWS {
            let Some(page) = input.next_page().await? else { break; };
            self.append(&mut accumulator, page).await?;
            if self.limit <= (BLOCK_ROWS / 2) as u64 && accumulator.records.len() > BLOCK_ROWS { accumulator = self.reduce(accumulator)?; }
        }
        if accumulator.records.len() <= BLOCK_ROWS {
            if accumulator.records.len() == 0 { return Ok(None); }
            let Accumulator { records, keys } = accumulator;
            let store = records.finish()?;
            let indices = self.indices(&store, &keys)?;
            let selected = store.select(&indices.values, Arc::clone(&self.execution.context))?;
            let rows = selected.len();
            return Ok(Some(SortOutput { rows: OutputRows::Records(selected.into_cursor()?), remaining: rows as u64, page_rows: rows, empty_callback: rows == 0 }));
        }
        let mut runs = RunList::new(self)?;
        runs.push(self.write_run(accumulator)?, self)?;
        loop {
            let mut accumulator = self.accumulator()?;
            while accumulator.records.len() <= BLOCK_ROWS {
                let Some(page) = input.next_page().await? else { break; };
                self.append(&mut accumulator, page).await?;
            }
            if accumulator.records.len() == 0 { break; }
            runs.push(self.write_run(accumulator)?, self)?;
        }
        drop(input);
        while runs.values.len() > 1 { runs = self.merge_pass(runs)?; }
        let run = runs.values.pop().ok_or_else(|| invalid("sort-state", "External sort produced no run"))?;
        Ok(Some(SortOutput { rows: OutputRows::Spill(run.file.reader(Arc::clone(&self.execution.context.memory))?), remaining: run.rows, page_rows: run.page_rows, empty_callback: false }))
    }
}
pub(crate) struct Indices { pub(crate) values: Vec<usize>, _reservation: Arc<MemoryReservation> }
pub(crate) fn sort_records(store: &RecordStore,context: &Arc<crate::QueryContext>,mut compare: impl FnMut(&[Value],&[Value]) -> Result<Ordering>) -> Result<Indices> {
    let scratch = match context.options.object_sort {
        rtdb_types::ObjectSort::TimSort => timsort::scratch_bytes(store.len())?,
        rtdb_types::ObjectSort::LegacyMergeSort => legacy::scratch_bytes(store.len())?,
    };
    let bytes = store.len().checked_mul(std::mem::size_of::<usize>()).and_then(|size|size.checked_add(scratch)).and_then(|size|size.checked_add(256)).ok_or_else(size_error)?;
    let reservation = context.memory.reserve(bytes)?;
    let mut indices: Vec<_> = (0..store.len()).collect();
    let mut access = store.access()?;
    let compare = |left,right| {
        context.check_cancelled()?;
        let left = access.get(left)?;let right = access.get(right)?;
        compare(left.values(),right.values())
    };
    match context.options.object_sort {
        rtdb_types::ObjectSort::TimSort => timsort::sort(&mut indices,compare)?,
        rtdb_types::ObjectSort::LegacyMergeSort => legacy::sort(&mut indices,compare)?,
    }
    Ok(Indices { values: indices,_reservation: reservation })
}
fn size_error() -> rtdb_types::Anomaly { invalid("sort-size", "Sort state exceeds addressable memory") }

struct KeyTypes { values: Vec<LogicalType>, _reservation: Arc<MemoryReservation> }
impl KeyTypes {
    fn declared(plan: &SortPlan) -> Result<Self> {
        let mut bound = crate::runtime_schema::schema_bound(&plan.schema)?;
        for order in &plan.order { bound = bound.checked_add(crate::runtime_schema::expression_bound(&order.expr)?).ok_or_else(size_error)?; }
        for ty in plan.execution.parameters.types() { bound = bound.checked_add(ty.retained_bytes()?).ok_or_else(size_error)?; }
        for (schema, _) in &plan.execution.outer.rows { bound = bound.checked_add(crate::runtime_schema::schema_bound(schema)?).ok_or_else(size_error)?; }
        let scope = plan.execution.scope()?;
        scope.reserve(bound.checked_mul(16).and_then(|n| n.checked_add(4096)).ok_or_else(size_error)?)?;
        let values = plan.order.iter().map(|order| plan.execution.expression_type(&scope, &order.expr, &plan.schema)).collect::<Result<Vec<_>>>()?;
        Self::new(values, plan)
    }
    fn new(values: Vec<LogicalType>, plan: &SortPlan) -> Result<Self> {
        let reservation = plan.execution.context.memory.reserve(type_bytes(&values)?)?;
        Ok(Self { values, _reservation: reservation })
    }
    fn observe(&mut self, values: &[Value], plan: &SortPlan) -> Result<()> {
        if values.len() != self.values.len() { return Err(invalid("sort-state", "Sort key metadata width changed")); }
        let bound = values.iter().try_fold(type_bytes(&self.values)?, |sum, value| sum.checked_add(value.retained_bytes()?).ok_or_else(size_error))?;
        let _temporary = plan.execution.context.memory.reserve(bound.checked_mul(16).and_then(|n| n.checked_add(4096)).ok_or_else(size_error)?)?;
        let values = self.values.iter().zip(values).map(|(ty, value)| ty.join(&value.logical_type())).collect();
        *self = Self::new(values, plan)?;
        Ok(())
    }
    fn join(&self, other: &Self, plan: &SortPlan) -> Result<Self> {
        if self.values.len() != other.values.len() { return Err(invalid("sort-state", "Sort run key metadata widths differ")); }
        let bound = type_bytes(&self.values)?.checked_add(type_bytes(&other.values)?).and_then(|n| n.checked_mul(16)).and_then(|n| n.checked_add(4096)).ok_or_else(size_error)?;
        let _temporary = plan.execution.context.memory.reserve(bound)?;
        Self::new(self.values.iter().zip(&other.values).map(|(left, right)| left.join(right)).collect(), plan)
    }
    fn validate(&self) -> Result<()> { for ty in &self.values { validate_order_type(ty)?; } Ok(()) }
}
fn type_bytes(types: &Vec<LogicalType>) -> Result<usize> {
    let base = types.capacity().checked_mul(std::mem::size_of::<LogicalType>()).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?;
    types.iter().try_fold(base, |sum, ty| sum.checked_add(ty.retained_bytes()?).ok_or_else(size_error))
}
struct SortedRun { file: Arc<SpillFile>, rows: u64, page_rows: usize, keys: KeyTypes }
struct RunList { values: Vec<SortedRun>, _reservation: Arc<MemoryReservation> }
impl RunList {
    fn new(plan: &SortPlan) -> Result<Self> { Ok(Self { values: Vec::new(), _reservation: plan.execution.context.memory.reserve(0)? }) }
    fn push(&mut self, run: SortedRun, plan: &SortPlan) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            let capacity = self.values.len().checked_mul(2).map(|n| n.max(1)).ok_or_else(size_error)?;
            let bytes = capacity.checked_mul(std::mem::size_of::<SortedRun>()).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?;
            let reservation = plan.execution.context.memory.reserve(bytes)?;
            self.values.reserve_exact(capacity - self.values.len());
            self._reservation = reservation;
        }
        self.values.push(run);
        Ok(())
    }
}
struct MergeReader { reader: SpillReader, head: Option<OwnedRow>, remaining: u64 }
impl MergeReader {
    fn new(run: &SortedRun, plan: &SortPlan) -> Result<Self> {
        let mut reader = Self { reader: run.file.reader(Arc::clone(&plan.execution.context.memory))?, head: None, remaining: run.rows };
        reader.advance()?;
        Ok(reader)
    }
    fn advance(&mut self) -> Result<()> {
        self.head = if self.remaining == 0 { None } else {
            let row = self.reader.next()?.ok_or_else(|| invalid("sort-state", "Merge input ended before its row count"))?;
            self.remaining -= 1;
            Some(row)
        };
        Ok(())
    }
}
struct MergeHeap { runs: [usize; MERGE_FAN_IN], len: usize }
impl MergeHeap {
    fn new() -> Self { Self { runs: [0; MERGE_FAN_IN], len: 0 } }
    fn push(&mut self, run: usize, mut compare: impl FnMut(usize, usize) -> Result<Ordering>) -> Result<()> {
        if self.len == self.runs.len() { return Err(invalid("sort-state", "Merge heap exceeded its fan-in")); }
        let mut slot = self.len;
        self.len += 1;
        while slot > 0 {
            let parent = (slot - 1) / 2;
            if compare(run, self.runs[parent])? != Ordering::Less { break; }
            self.runs[slot] = self.runs[parent];
            slot = parent;
        }
        self.runs[slot] = run;
        Ok(())
    }
    fn pop(&mut self, mut compare: impl FnMut(usize, usize) -> Result<Ordering>) -> Result<Option<usize>> {
        if self.len == 0 { return Ok(None); }
        let result = self.runs[0];
        self.len -= 1;
        if self.len > 0 {
            let replacement = self.runs[self.len];
            let mut slot = 0;
            while slot < self.len / 2 {
                let mut child = slot * 2 + 1;
                if child + 1 < self.len && compare(self.runs[child], self.runs[child + 1])? == Ordering::Greater { child += 1; }
                if compare(replacement, self.runs[child])? != Ordering::Greater { break; }
                self.runs[slot] = self.runs[child];
                slot = child;
            }
            self.runs[slot] = replacement;
        }
        Ok(Some(result))
    }
}
impl SortPlan {
    fn compare_heads(&self, readers: &[MergeReader], left: usize, right: usize) -> Result<Ordering> {
        self.execution.context.check_cancelled()?;
        let at = |index: usize| readers.get(index).and_then(|reader| reader.head.as_ref()).ok_or_else(|| invalid("sort-state", "Merge heap selected an absent head"));
        if left < right { self.compare(&at(left)?.values, &at(right)?.values) }
        else { self.compare(&at(right)?.values, &at(left)?.values).map(Ordering::reverse) }
    }
    fn merge_pass(&self, runs: RunList) -> Result<RunList> {
        let RunList { values, _reservation } = runs;
        let mut source = values.into_iter();
        let mut next = RunList::new(self)?;
        while source.len() > 0 {
            let count = source.len().min(MERGE_FAN_IN);
            let merged = self.merge(&source.as_slice()[..count])?;
            for _ in 0..count { drop(source.next()); }
            next.push(merged, self)?;
        }
        Ok(next)
    }
    fn merge(&self, runs: &[SortedRun]) -> Result<SortedRun> {
        let _reservation = self.execution.context.memory.reserve(runs.len().checked_mul(std::mem::size_of::<MergeReader>()).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?)?;
        let mut readers = runs.iter().map(|run| MergeReader::new(run, self)).collect::<Result<Vec<_>>>()?;
        let mut output_types = KeyTypes::declared(self)?;
        for run in runs { output_types = output_types.join(&run.keys, self)?; }
        for i in 0..runs.len() { for j in 0..i { runs[j].keys.join(&runs[i].keys, self)?.validate()?; } }
        let mut queue = MergeHeap::new();
        for i in 0..readers.len() { if readers[i].head.is_some() { queue.push(i, |left, right| self.compare_heads(&readers, left, right))?; } }
        let mut writer = self.execution.context.spill.create()?;
        let mut copied = 0;
        while copied < self.limit {
            let Some(run) = queue.pop(|left, right| self.compare_heads(&readers, left, right))? else { break; };
            self.execution.context.check_cancelled()?;
            let row = readers[run].head.take().ok_or_else(|| invalid("sort-state", "Merge winner is absent"))?;
            writer.write(&row.values, &self.execution.context.memory)?;
            drop(row);
            readers[run].advance()?;
            if readers[run].head.is_some() { queue.push(run, |left, right| self.compare_heads(&readers, left, right))?; }
            copied = copied.checked_add(1).ok_or_else(size_error)?;
        }
        Ok(SortedRun { file: writer.finish()?, rows: copied, page_rows: BLOCK_ROWS + 1, keys: output_types })
    }
}

#[cfg(test)]
mod page_layout_tests {
    use super::*;
    use std::collections::{BTreeMap,VecDeque};
    use rtdb_columnar::MemoryBudget;
    use rtdb_plan::{ColumnId,Field};
    use rtdb_storage::{MemoryStore,cache::BufferPool};
    use rtdb_types::{ScalarType,Timestamp};
    use tokio_util::sync::CancellationToken;
    use crate::{CatalogSnapshot,DatabaseSnapshot,QueryContext,QueryOptions,context::OuterRows};
    struct Input { pages: VecDeque<Vec<i64>>,memory: Arc<MemoryBudget> }
    #[async_trait]
    impl PageCursor for Input {
        async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
            let Some(rows) = self.pages.pop_front() else { return Ok(None); };
            let mut page = PageBuilder::new(rows.len(),&self.memory)?;
            for row in rows { page.push(OwnedRow::new(vec![Value::I64(row)],&self.memory)?)?; }
            Ok(Some(page.finish()))
        }
    }
    fn expression_execution() -> Execution {
        let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
        let context = QueryContext::new(catalog,MemoryBudget::new(1 << 24),Timestamp(0),"UTC",QueryOptions { batch_rows: 1,..Default::default() },&CancellationToken::new()).unwrap();
        Execution { parameters: Arc::new(crate::parameters::BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),context,bindings: None,analysis: None }
    }
    fn invalid_regex() -> rtdb_plan::Expr {
        rtdb_plan::Expr::regex_replace(rtdb_plan::Expr::Literal(Value::Utf8("source".into())),rtdb_plan::RegexReplacement {
            pattern: rtdb_plan::RegexForm::Literal(Value::Utf8("[".into())),replacement: rtdb_plan::RegexForm::Literal(Value::Utf8("x".into())),..Default::default()
        })
    }
    fn expression_sort(pages: Vec<Vec<i64>>,expressions: Vec<rtdb_plan::Expr>,execution: &Execution) -> SortCursor {
        let field = Field { id: ColumnId(0),name: "n".into(),qualifier: None,qualified_only: false,unnamed: false,data_type: LogicalType::Scalar(ScalarType::I64) };
        SortCursor::from_pages(Box::new(Input { pages: pages.into(),memory: Arc::clone(&execution.context.memory) }),Schema::new(vec![field]).unwrap(),expressions.into_iter().map(|expr|SortExpr { expr,descending: false,nulls_first: false }).collect(),execution.clone())
    }
    #[tokio::test]
    async fn computed_sort_key_columns_complete_before_later_key_metadata_preparation() {
        use rtdb_plan::{BinaryOp,Expr};
        let execution = expression_execution();let baseline = execution.context.memory.used_bytes();
        let first = Expr::binary(BinaryOp::Divide,Expr::Literal(Value::I64(1)),Expr::column(ColumnId(0),LogicalType::Scalar(ScalarType::I64)));
        let mut cursor = expression_sort(vec![vec![1,0]],vec![first,invalid_regex()],&execution);
        assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb.expression/division-by-zero");
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
    #[tokio::test]
    async fn computed_sort_key_metadata_prepares_for_empty_page_but_not_absent_input() {
        let execution = expression_execution();let baseline = execution.context.memory.used_bytes();
        let mut cursor = expression_sort(vec![vec![]],vec![invalid_regex()],&execution);
        assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb.expression/illegal-argument-exception");
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
        let mut cursor = expression_sort(vec![],vec![invalid_regex()],&execution);
        assert!(cursor.next_page().await.unwrap().is_none());
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
    #[tokio::test]
    async fn native_memory_spills_do_not_split_a_single_reference_output_page() {
        let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
        let context = QueryContext::new(catalog,MemoryBudget::new(1 << 24),Timestamp(0),"UTC",QueryOptions { batch_rows: 1,sort_memory_bytes: 1,..Default::default() },&CancellationToken::new()).unwrap();
        let execution = Execution { context: Arc::clone(&context),parameters: Arc::new(crate::parameters::BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),bindings: None,analysis: None };
        let field = Field { id: ColumnId(0),name: "n".into(),qualifier: None,qualified_only: false,unnamed: false,data_type: LogicalType::Scalar(ScalarType::I64) };
        let order = vec![SortExpr { expr: field.expression(),descending: false,nulls_first: false }];
        let input = Box::new(Input { pages: vec![vec![5,1],vec![4,2,3]].into(),memory: Arc::clone(&context.memory) });
        let mut cursor = SortCursor::from_pages(input,Schema::new(vec![field]).unwrap(),order,execution);
        let mut page = cursor.next_page().await.unwrap().unwrap();
        assert_eq!(page.len(),5);
        let mut values = Vec::new();while let Some(row) = page.next() { values.push(row.values[0].clone()); }
        assert_eq!(values,[Value::I64(1),Value::I64(2),Value::I64(3),Value::I64(4),Value::I64(5)]);
        assert!(cursor.next_page().await.unwrap().is_none());
        drop(page);drop(cursor);assert_eq!(context.memory.used_bytes(),0);assert_eq!(context.spill_bytes(),0);
    }
    #[tokio::test]
    async fn limited_sort_emits_one_capped_page_and_releases_unconsumed_spill_rows() {
        let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
        let context = QueryContext::new(catalog,MemoryBudget::new(1 << 24),Timestamp(0),"UTC",QueryOptions { batch_rows: 1,sort_memory_bytes: 1,..Default::default() },&CancellationToken::new()).unwrap();
        let execution = Execution { context: Arc::clone(&context),parameters: Arc::new(crate::parameters::BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),bindings: None,analysis: None };
        let field = Field { id: ColumnId(0),name: "n".into(),qualifier: None,qualified_only: false,unnamed: false,data_type: LogicalType::Scalar(ScalarType::I64) };
        let order = vec![SortExpr { expr: field.expression(),descending: false,nulls_first: false }];
        let input = Box::new(Input { pages: vec![vec![5,1],vec![4,2,3]].into(),memory: Arc::clone(&context.memory) });
        let mut cursor = SortCursor::with_row_limit(input,Schema::new(vec![field]).unwrap(),order,3,execution);
        let mut page = cursor.next_page().await.unwrap().unwrap();
        let mut values = Vec::new();while let Some(row) = page.next() { values.push(row.values[0].clone()); }
        assert_eq!(values,[Value::I64(1),Value::I64(2),Value::I64(3)]);
        assert!(cursor.next_page().await.unwrap().is_none());
        drop(page);drop(cursor);assert_eq!(context.memory.used_bytes(),0);assert_eq!(context.spill_bytes(),0);
    }
}

#[cfg(test)]
mod schedule_tests;

#[cfg(test)]
mod object_sort_tests;
