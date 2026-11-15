use std::cmp::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_expr::compare;
use rtdb_plan::{Schema, SortExpr};
use rtdb_types::{Result, Value};

use crate::context::Execution;
use crate::cursor::{BoxCursor, OwnedRow, RowCursor};
use crate::spill::{SpillFile, SpillReader};
use crate::invalid;

pub(crate) struct SortCursor { input: Option<BoxCursor>, schema: Schema, order: Vec<SortExpr>, execution: Execution, reader: Option<SpillReader> }
impl SortCursor {
    pub fn new(input: BoxCursor, schema: Schema, order: Vec<SortExpr>, execution: Execution) -> Self { Self { input: Some(input), schema, order, execution, reader: None } }
    async fn initialize(&mut self, mut input: BoxCursor) -> Result<()> {
        let mut levels: Vec<Vec<Arc<SpillFile>>> = Vec::new();
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        let mut sequence = 0u64;
        while let Some(row) = input.next().await? {
            self.execution.context.check_cancelled()?;
            let mut record = Vec::with_capacity(self.order.len() + row.values.len() + 1);
            record.push(Value::U64(sequence));
            for order in &self.order { record.push(self.execution.evaluate(&order.expr, &self.schema, &row.values)?); }
            record.extend(row.values.iter().cloned());
            sequence = sequence.checked_add(1).ok_or_else(|| invalid("row-count", "Sort row count overflow"))?;
            let record = OwnedRow::new(record, &self.execution.context.memory)?;
            bytes = bytes.checked_add(record.bytes()).ok_or_else(|| invalid("sort-size", "Sort buffer size overflow"))?;
            rows.push(record);
            if bytes >= self.execution.context.options.sort_memory_bytes {
                self.add_run(&mut levels, self.write_run(&mut rows)?)?;
                bytes = 0;
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
        Ok(())
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
            let mut comparison = compare(left, right, order.nulls_first)?;
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
#[async_trait]
impl RowCursor for SortCursor {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        self.execution.context.check_cancelled()?;
        if let Some(input) = self.input.take() { self.initialize(input).await?; }
        let Some(reader) = &mut self.reader else { return Ok(None); };
        let Some(mut row) = reader.next()? else { self.reader = None; return Ok(None); };
        let prefix = self.order.len() + 1;
        if row.values.len() != prefix + self.schema.len() { return Err(invalid("invalid-spill", "Sorted record has an invalid width")); }
        row.values.drain(..prefix);
        Ok(Some(row))
    }
}
