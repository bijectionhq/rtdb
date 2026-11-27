use std::{alloc::Layout, sync::Arc};
use rtdb_plan::{RegexForm, RegexReplacement};
use rtdb_types::{Anomaly, MemoryReservation, Result, Value};
use crate::{Edn, EdnKind, FormAllocation};

pub(crate) fn metadata(arguments: &[Edn], allocation: FormAllocation<'_>) -> Result<RegexReplacement> {
    let field = |index| arguments.get(index).map(|value| form(value, allocation)).transpose().map(Option::unwrap_or_default);
    Ok(RegexReplacement { pattern: field(1)?, replacement: field(2)?, start: field(3)?, occurrence: field(4)?, flags: field(5)? })
}

enum Event<'a> { Form(&'a Edn), Sequence(&'a [Edn], usize), Pairs(&'a [(Edn, Edn)], usize), End }
struct Walk<'budget, 'source> { events: Vec<Event<'source>>, reservation: Option<MemoryReservation>, allocation: FormAllocation<'budget> }
impl<'source> Walk<'_, 'source> {
    fn push(&mut self, event: Event<'source>) -> Result<()> {
        if self.events.len() == self.events.capacity() {
            let capacity = self.events.capacity().checked_mul(2).ok_or_else(size)?.max(16);
            let bytes = Layout::array::<Event<'source>>(capacity).map_err(|_| size())?.size();
            let reservation = match self.allocation {
                FormAllocation::Untracked => None,
                FormAllocation::Tracked(memory) => Some(Arc::try_unwrap(memory.reserve(bytes)?).expect("new scratch reservation is unique")),
            };
            let mut next = Vec::new();
            next.try_reserve_exact(capacity).map_err(|cause| Anomaly::incorrect("rtdb/allocation-failed", cause.to_string()).with_kind(rtdb_types::AnomalyKind::ResourceExhausted))?;
            next.append(&mut self.events);
            drop(std::mem::replace(&mut self.events, next));
            self.reservation = reservation;
        }
        self.events.push(event);
        Ok(())
    }
}
fn size() -> Anomaly { Anomaly::incorrect("rtdb.plan/regex-metadata-size", "Regex metadata exceeds addressable memory").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded) }

fn form(value: &Edn, allocation: FormAllocation<'_>) -> Result<RegexForm> {
    if matches!(value.kind, EdnKind::Nil) { return Ok(RegexForm::default()); }
    let mut builder = allocation.builder();
    let mut walk = Walk { events: Vec::new(), reservation: None, allocation };
    walk.push(Event::Form(value))?;
    while let Some(event) = walk.events.pop() {
        match event {
            Event::End => builder.end()?,
            Event::Sequence(values, index) => if let Some(value) = values.get(index) { walk.push(Event::Sequence(values, index + 1))?; walk.push(Event::Form(value))?; },
            Event::Pairs(values, index) => if let Some((key, value)) = values.get(index) { walk.push(Event::Pairs(values, index + 1))?; walk.push(Event::Form(value))?; walk.push(Event::Form(key))?; },
            Event::Form(value) => match &value.kind {
                EdnKind::Nil => builder.literal(&Value::Null)?,
                EdnKind::Bool(value) => builder.literal(&Value::Bool(*value))?,
                EdnKind::Integer(value) => builder.literal(&Value::I64(*value))?,
                EdnKind::Float(value) => builder.literal(&Value::F64(*value))?,
                EdnKind::String(value) => { builder.begin_literal()?; builder.value_text(value)?; builder.end()?; }
                EdnKind::Keyword(value) => { builder.begin_literal()?; builder.value_keyword(value)?; builder.end()?; }
                EdnKind::Symbol(value) => builder.symbol(value)?, EdnKind::Character(value) => builder.character(*value)?,
                EdnKind::BigInteger(value) => builder.big_integer(value)?, EdnKind::Decimal(value) => builder.decimal(value)?,
                EdnKind::List(values) => { builder.begin_list()?; walk.push(Event::End)?; walk.push(Event::Sequence(values, 0))?; }
                EdnKind::Vector(values) => { builder.begin_vector()?; walk.push(Event::End)?; walk.push(Event::Sequence(values, 0))?; }
                EdnKind::Set(values) => { builder.begin_set()?; walk.push(Event::End)?; walk.push(Event::Sequence(values, 0))?; }
                EdnKind::Map(values) => { builder.begin_map()?; walk.push(Event::End)?; walk.push(Event::Pairs(values, 0))?; }
                EdnKind::Tagged { tag, value } => { builder.begin_tagged(tag)?; walk.push(Event::End)?; walk.push(Event::Form(value))?; }
                EdnKind::AnonymousFunction(value) => { builder.begin_anonymous()?; walk.push(Event::End)?; walk.push(Event::Form(value))?; }
            },
        }
    }
    drop(walk);
    builder.finish()
}
