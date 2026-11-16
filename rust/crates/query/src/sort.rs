use std::cmp::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_expr::{ordered_compare_in_timezone, validate_order_type};
use rtdb_plan::{Schema, SortExpr};
use rtdb_types::{LogicalType, Result, Value};

use crate::context::Execution;
use crate::cursor::{BoxCursor, OwnedRow, RowCursor};
use crate::spill::{SpillFile, SpillReader};
use crate::invalid;
use crate::pages::{BoxPageCursor,OwnedPage,PageBuilder,PageCursor};

struct SortPageLayout { first: usize, remaining: usize, multiple: bool, limit: u64 }
impl Default for SortPageLayout {
    fn default() -> Self { Self { first: 0,remaining: 0,multiple: false,limit: i64::MAX as u64 } }
}
impl SortPageLayout {
    fn observe(&mut self,rows: usize) -> Result<()> {
        self.remaining = self.remaining.checked_add(rows).ok_or_else(||invalid("row-count","Sort input row count overflow"))?;
        if self.first <= 102_400 {
            self.first = self.first.checked_add(rows).ok_or_else(||invalid("row-count","Sort initial page count overflow"))?;
            if self.first > 102_400 && self.limit <= 51_200 { self.first = self.limit as usize; }
        }
        else if rows > 0 { self.multiple = true; }
        Ok(())
    }
    fn finish(&mut self) { self.remaining = self.remaining.min(usize::try_from(self.limit).unwrap_or(usize::MAX)); }
    fn next_size(&self) -> usize { self.remaining.min(if self.multiple { 102_401 } else { self.first }) }
}

pub(crate) struct SortCursor { input: Option<BoxPageCursor>, schema: Schema, order: Vec<SortExpr>, execution: Execution, reader: Option<SpillReader>, layout: SortPageLayout }
impl SortCursor {
    pub fn new(input: BoxCursor, schema: Schema, order: Vec<SortExpr>, execution: Execution) -> Self {
        Self::from_pages(crate::pages::from_rows(input,Arc::clone(&execution.context)),schema,order,execution)
    }
    pub fn from_pages(input: BoxPageCursor, schema: Schema, order: Vec<SortExpr>, execution: Execution) -> Self { Self::with_row_limit(input,schema,order,i64::MAX as u64,execution) }
    pub fn with_row_limit(input: BoxPageCursor, schema: Schema, order: Vec<SortExpr>, limit: u64, execution: Execution) -> Self { Self { input: Some(input), schema, order, execution, reader: None, layout: SortPageLayout { limit,..Default::default() } } }
    async fn initialize(&mut self, mut input: BoxPageCursor) -> Result<()> {
        let mut levels: Vec<Vec<Arc<SpillFile>>> = Vec::new();
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        let mut sequence = 0u64;
        let mut order_types: Option<(Vec<LogicalType>, Arc<rtdb_columnar::MemoryReservation>)> = None;
        while let Some(mut page) = input.next_page().await? {
          self.layout.observe(page.len())?;
          let mut keys = crate::pages::project(&page,&self.schema,self.order.iter().map(|order|&order.expr),&self.execution).await?;
          while let Some(row) = page.next() {
            self.execution.context.check_cancelled()?;
            let scope = self.execution.scope()?;
            let key = keys.next().ok_or_else(||invalid("sort-state","Projected sort keys ended before their input page"))?;
            let record = OwnedRow::copy_parts(&[&[Value::U64(sequence)],&key.values,&row.values],0,0,&self.execution.context.memory)?;
            sequence = sequence.checked_add(1).ok_or_else(|| invalid("row-count", "Sort row count overflow"))?;
            let old_bytes = order_types.as_ref().map_or(Ok(0), |(types, _)| type_bytes(types))?;
            let declared_bytes = self.order.iter().try_fold(0usize, |bytes, order| bytes.checked_add(crate::runtime_schema::expression_bound(&order.expr)?).ok_or_else(|| invalid("sort-size", "Order type size overflow")))?;
            let key_bytes = record.values[1..1 + self.order.len()].iter().try_fold(0usize, |bytes, value| {
                let zone = match value { Value::Timestamp { timezone: Some(zone), .. } => zone.len(), _ => 0 };
                bytes.checked_add(zone).and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| invalid("sort-size", "Order key type size overflow"))
            })?;
            let bound = old_bytes.checked_add(key_bytes).and_then(|bytes| bytes.checked_add(declared_bytes)).and_then(|bytes| bytes.checked_add(4096)).and_then(|bytes| bytes.checked_mul(16)).ok_or_else(|| invalid("sort-size", "Order type allocation bound overflow"))?;
            let temporary = self.execution.context.memory.reserve(bound)?;
            let types: Vec<_> = self.order.iter().enumerate().map(|(index, order)| {
                let declared = self.execution.expression_type(&scope,&order.expr,&self.schema)?;
                scope.reserve(declared.retained_bytes()?.checked_mul(16).ok_or_else(||invalid("sort-size","Order type allocation bound overflow"))?)?;
                let previous = order_types.as_ref().map_or(&declared, |(types, _)| &types[index]);
                Ok(previous.join(&key_type(&record.values[index + 1])?))
            }).collect::<Result<_>>()?;
            for ty in &types { validate_order_type(ty)?; }
            let retained = type_bytes(&types)?;
            order_types = Some((types, self.execution.context.memory.reserve(retained)?));
            drop(temporary);
            bytes = bytes.checked_add(record.bytes()).ok_or_else(|| invalid("sort-size", "Sort buffer size overflow"))?;
            rows.push(record);
            if bytes >= self.execution.context.options.sort_memory_bytes {
                self.add_run(&mut levels, self.write_run(&mut rows)?)?;
                bytes = 0;
            }
          }
        }
        drop(input);
        if !rows.is_empty() { self.add_run(&mut levels, self.write_run(&mut rows)?)?; }
        let mut pending: Vec<_> = levels.into_iter().flatten().collect();
        while pending.len() > 1 {
            let mut next = Vec::new();
            for chunk in pending.chunks(self.execution.context.options.merge_fan_in) { next.push(self.merge_runs(chunk)?); }
            pending = next;
        }
        self.reader = pending.pop().map(|file| file.reader(Arc::clone(&self.execution.context.memory))).transpose()?;
        self.layout.finish();
        Ok(())
    }
    fn read_row(&mut self) -> Result<Option<OwnedRow>> {
        self.execution.context.check_cancelled()?;
        if self.layout.remaining == 0 { self.reader = None;return Ok(None); }
        let Some(reader) = &mut self.reader else { return Ok(None); };
        let Some(mut row) = reader.next()? else { self.reader = None;return Ok(None); };
        let prefix = self.order.len() + 1;
        if row.values.len() != prefix + self.schema.len() { return Err(invalid("invalid-spill","Sorted record has an invalid width")); }
        row.values.drain(..prefix);
        self.layout.remaining = self.layout.remaining.checked_sub(1).ok_or_else(||invalid("sort-state","Sort emitted more rows than its input"))?;
        Ok(Some(row))
    }
    fn write_run(&self, rows: &mut Vec<OwnedRow>) -> Result<Arc<SpillFile>> {
        let scratch = rows.len().checked_mul(std::mem::size_of::<OwnedRow>() * 2).ok_or_else(|| invalid("sort-size", "Sort scratch size overflow"))?;
        let _reservation = self.execution.context.memory.reserve(scratch)?;
        let mut error = None;
        rows.sort_by(|left, right| {
            if error.is_some() { return Ordering::Equal; }
            match self.compare_records(&left.values, &right.values) { Ok(order) => order, Err(failure) => { error = Some(failure); Ordering::Equal } }
        });
        if let Some(error) = error { return Err(error); }
        let mut writer = self.execution.context.spill.create()?;
        for row in rows.drain(..) { self.execution.context.check_cancelled()?; writer.write(&row.values, &self.execution.context.memory)?; }
        writer.finish()
    }
    fn add_run(&self, levels: &mut Vec<Vec<Arc<SpillFile>>>, mut run: Arc<SpillFile>) -> Result<()> {
        let mut level = 0;
        loop {
            if levels.len() == level { levels.push(Vec::new()); }
            levels[level].push(run);
            if levels[level].len() < self.execution.context.options.merge_fan_in { return Ok(()); }
            let inputs = std::mem::take(&mut levels[level]);
            run = self.merge_runs(&inputs)?;
            level += 1;
        }
    }
    fn compare_records(&self, left: &[Value], right: &[Value]) -> Result<Ordering> {
        for (index, order) in self.order.iter().enumerate() {
            let (left, right) = (&left[index + 1], &right[index + 1]);
            let mut comparison = ordered_compare_in_timezone(left, right, order.nulls_first, &self.execution.context.timezone)?;
            if order.descending && !matches!(left, Value::Null | Value::Undefined) && !matches!(right, Value::Null | Value::Undefined) { comparison = comparison.reverse(); }
            if comparison != Ordering::Equal { return Ok(comparison); }
        }
        let (Some(Value::U64(left)), Some(Value::U64(right))) = (left.first(), right.first()) else { return Err(invalid("invalid-spill", "Sort record lacks an input sequence")); };
        Ok(left.cmp(right))
    }
    fn merge_runs(&self, runs: &[Arc<SpillFile>]) -> Result<Arc<SpillFile>> {
        if runs.len() == 1 { return Ok(Arc::clone(&runs[0])); }
        let mut readers = runs.iter().map(|file| file.reader(Arc::clone(&self.execution.context.memory))).collect::<Result<Vec<_>>>()?;
        let mut heads = readers.iter_mut().map(SpillReader::next).collect::<Result<Vec<_>>>()?;
        let mut writer = self.execution.context.spill.create()?;
        loop {
            self.execution.context.check_cancelled()?;
            let mut winner: Option<usize> = None;
            for (index, row) in heads.iter().enumerate() {
                let Some(row) = row else { continue; };
                if let Some(previous) = winner {
                    if self.compare_records(&row.values, &heads[previous].as_ref().ok_or_else(|| invalid("sort-state", "Merge head is absent"))?.values)? == Ordering::Less { winner = Some(index); }
                } else { winner = Some(index); }
            }
            let Some(index) = winner else { break; };
            let row = heads[index].take().ok_or_else(|| invalid("sort-state", "Merge head is absent"))?;
            writer.write(&row.values, &self.execution.context.memory)?;
            drop(row);
            heads[index] = readers[index].next()?;
        }
        writer.finish()
    }
}

fn type_bytes(types: &[LogicalType]) -> Result<usize> {
    types.iter().try_fold(256usize, |bytes, ty| bytes.checked_add(ty.retained_bytes()?).ok_or_else(|| invalid("sort-size", "Order type size overflow")))
}
fn key_type(value: &Value) -> Result<LogicalType> {
    if matches!(value, Value::List(_) | Value::Set(_) | Value::FixedSizeList(_) | Value::Map { .. } | Value::Struct(_) | Value::TimestampTzRange { .. }) {
        return Err(rtdb_types::Anomaly::incorrect("xtdb.expression/function-type-mismatch", "compare is not applicable to these vector types"));
    }
    Ok(value.logical_type())
}
#[async_trait]
impl RowCursor for SortCursor {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        self.execution.context.check_cancelled()?;
        if let Some(input) = self.input.take() { self.initialize(input).await?; }
        self.read_row()
    }
}

#[async_trait]
impl PageCursor for SortCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.execution.context.check_cancelled()?;
        if let Some(input) = self.input.take() { self.initialize(input).await?; }
        let count = self.layout.next_size();
        if count == 0 { self.reader = None;return Ok(None); }
        let mut page = PageBuilder::new(count,&self.execution.context.memory)?;
        for _ in 0..count {
            let row = self.read_row()?.ok_or_else(||invalid("sort-state","Sort spill ended before its input row count"))?;
            page.push(row)?;
        }
        Ok(Some(page.finish()))
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
    fn layout(pages: &[usize]) -> SortPageLayout { let mut layout = SortPageLayout::default();for rows in pages { layout.observe(*rows).unwrap(); }layout }
    #[test]
    fn a_single_source_accumulation_is_one_page_even_above_the_spill_threshold() {
        for pages in [&[100,200][..],&[102_400,1][..],&[100_000,50_000,0][..]] {
            let layout = layout(pages);assert_eq!(layout.next_size(),pages.iter().sum::<usize>());assert!(!layout.multiple);
        }
    }
    #[test]
    fn multiple_source_accumulations_use_reference_merged_page_size() {
        let mut first = layout(&[100_000,50_000,1]);
        assert_eq!(first.next_size(),102_401);first.remaining -= 102_401;assert_eq!(first.next_size(),47_600);
        let layout = layout(&[102_400,1,1]);assert_eq!(layout.next_size(),102_401);assert!(layout.multiple);
    }
    #[test]
    fn empty_pages_and_input_count_overflow_have_explicit_results() {
        assert_eq!(layout(&[0,0]).next_size(),0);
        let mut layout = layout(&[usize::MAX]);assert!(layout.observe(1).is_err());
    }
    #[test]
    fn small_top_bounds_reduce_each_initial_accumulation_to_one_final_page() {
        for limit in [0,1,51_200] {
            let mut layout = SortPageLayout { limit,..Default::default() };
            for rows in [100_000,50_000,100_000,50_000] { layout.observe(rows).unwrap(); }
            layout.finish();
            assert!(!layout.multiple);
            assert_eq!(layout.next_size(),limit as usize);
        }
    }
    #[test]
    fn large_top_bounds_preserve_external_merge_pages_and_cap_the_total() {
        let mut layout = SortPageLayout { limit: 150_000,..Default::default() };
        for rows in [100_000,50_000,100_000] { layout.observe(rows).unwrap(); }
        layout.finish();
        assert!(layout.multiple);
        assert_eq!(layout.next_size(),102_401);
        layout.remaining -= 102_401;
        assert_eq!(layout.next_size(),47_599);
        let mut single = SortPageLayout { limit: 51_201,..Default::default() };
        single.observe(150_000).unwrap();single.finish();
        assert!(!single.multiple);assert_eq!(single.next_size(),51_201);
    }
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
