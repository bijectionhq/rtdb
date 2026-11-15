use std::sync::Arc;

use async_trait::async_trait;
use rtdb_expr::is_true;
use rtdb_plan::{Expr, JoinKind, Schema};
use rtdb_types::{Result, Value};

use crate::context::Execution;
use crate::cursor::{BoxCursor, OwnedRow, RowCursor};
use crate::operators::OrdinalSet;
use crate::spill::SpillReader;
use crate::invalid;

pub(crate) struct JoinCursor {
    left: Option<BoxCursor>, right: Option<BoxCursor>, reader: Option<SpillReader>,
    current: Option<OwnedRow>, matched: bool, ordinal: u64, unmatched_phase: bool,
    matched_right: OrdinalSet, left_width: usize, right_width: usize,
    schema: Schema, kind: JoinKind, condition: Option<Expr>, execution: Execution,
}
impl JoinCursor {
    pub fn new(left: BoxCursor, right: BoxCursor, left_schema: Schema, right_schema: Schema, kind: JoinKind, condition: Option<Expr>, execution: Execution) -> Result<Self> {
        Ok(Self { left: Some(left), right: Some(right), reader: None, current: None, matched: false, ordinal: 0, unmatched_phase: false,
            matched_right: OrdinalSet::new(&execution.context)?, left_width: left_schema.len(), right_width: right_schema.len(),
            schema: left_schema.concat(&right_schema)?, kind, condition, execution })
    }
    fn next_ordinal(&mut self) -> Result<u64> {
        let ordinal = self.ordinal;
        self.ordinal = self.ordinal.checked_add(1).ok_or_else(|| invalid("row-count", "Join row count overflow"))?;
        Ok(ordinal)
    }
}
#[async_trait]
impl RowCursor for JoinCursor {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        if let Some(mut right) = self.right.take() {
            let mut writer = self.execution.context.spill.create()?;
            while let Some(row) = right.next().await? {
                self.execution.context.check_cancelled()?;
                writer.write(&row.values, &self.execution.context.memory)?;
            }
            self.reader = Some(writer.finish()?.reader(Arc::clone(&self.execution.context.memory))?);
        }
        loop {
            self.execution.context.check_cancelled()?;
            if self.unmatched_phase {
                let Some(right) = self.reader.as_mut().ok_or_else(|| invalid("join-state", "Right input is not spooled"))?.next()? else { return Ok(None); };
                let ordinal = self.next_ordinal()?;
                if self.matched_right.contains(ordinal)? { continue; }
                let values = (0..self.left_width).map(|_| Value::Null).chain(right.values.iter().cloned()).collect();
                return OwnedRow::new(values, &self.execution.context.memory).map(Some);
            }
            if self.current.is_none() {
                let next = match &mut self.left { Some(left) => left.next().await?, None => None };
                let Some(left) = next else {
                    self.left = None;
                    if !matches!(self.kind, JoinKind::Right | JoinKind::Full) { return Ok(None); }
                    self.unmatched_phase = true;
                    self.ordinal = 0;
                    self.reader.as_mut().ok_or_else(|| invalid("join-state", "Right input is not spooled"))?.rewind()?;
                    continue;
                };
                self.current = Some(left);
                self.matched = false;
                self.ordinal = 0;
                self.reader.as_mut().ok_or_else(|| invalid("join-state", "Right input is not spooled"))?.rewind()?;
            }
            let right = self.reader.as_mut().ok_or_else(|| invalid("join-state", "Right input is not spooled"))?.next()?;
            if let Some(right) = right {
                let ordinal = self.next_ordinal()?;
                let left = self.current.as_ref().ok_or_else(|| invalid("join-state", "Left input is absent"))?;
                let values: Vec<_> = left.values.iter().chain(&right.values).cloned().collect();
                let joined = OwnedRow::new(values, &self.execution.context.memory)?;
                let matched = self.condition.as_ref().map(|condition| is_true(&self.execution.evaluate(condition, &self.schema, &joined.values)?)).transpose()?.unwrap_or(true);
                if !matched { continue; }
                self.matched = true;
                if matches!(self.kind, JoinKind::Right | JoinKind::Full) { self.matched_right.insert(ordinal)?; }
                match self.kind {
                    JoinKind::Semi => return Ok(self.current.take()),
                    JoinKind::Anti => { self.current = None; continue; }
                    _ => return Ok(Some(joined)),
                }
            }
            let left = self.current.take().ok_or_else(|| invalid("join-state", "Left input is absent"))?;
            if self.matched { continue; }
            match self.kind {
                JoinKind::Anti => return Ok(Some(left)),
                JoinKind::Left | JoinKind::Full => {
                    let values = left.values.iter().cloned().chain((0..self.right_width).map(|_| Value::Null)).collect();
                    return OwnedRow::new(values, &self.execution.context.memory).map(Some);
                }
                _ => {}
            }
        }
    }
}
