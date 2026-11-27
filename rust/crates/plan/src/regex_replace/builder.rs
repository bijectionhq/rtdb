use std::sync::Arc;
use rtdb_types::{Anomaly, MemoryBudget, MemoryReservation, Result, Value};
use super::{Backing, RegexForm, RegexFormKind, RegexFormView, RegexLiteralKind, RegexLiteralView};
use super::storage::*;

#[derive(Clone, Copy)]
pub(crate) enum FormAllocation<'a> { Tracked(&'a Arc<MemoryBudget>), Untracked }
impl<'a> FormAllocation<'a> {
    pub(crate) fn builder(self) -> RegexFormBuilder<'a> { RegexFormBuilder::new(self) }
    pub(crate) fn copy(self, source: RegexFormView<'_>) -> Result<RegexForm> { let mut builder = self.builder(); builder.append_view(source)?; builder.finish() }
}

enum Lease<'a> { Untracked, Tracked { memory: &'a Arc<MemoryBudget>, reservation: Option<MemoryReservation> } }
pub(super) struct Charge<'a> { bytes: usize, lease: Lease<'a> }
impl<'a> Charge<'a> {
    fn new(mode: FormAllocation<'a>) -> Self { Self { bytes: 0, lease: match mode { FormAllocation::Untracked => Lease::Untracked, FormAllocation::Tracked(memory) => Lease::Tracked { memory, reservation: None } } } }
    fn add(&mut self, bytes: usize) -> Result<()> { self.resize(add(self.bytes, bytes)?) }
    fn resize(&mut self, bytes: usize) -> Result<()> {
        if let Lease::Tracked { memory, reservation } = &mut self.lease {
            if let Some(held) = reservation {
                if bytes >= held.bytes() { held.grow_to(bytes)?; } else { held.shrink_to(bytes)?; }
            } else if bytes != 0 { *reservation = Some(Arc::try_unwrap(memory.reserve(bytes)?).expect("new reservation is unique")); }
        }
        self.bytes = bytes;
        Ok(())
    }
    pub(super) fn copy_bytes(&mut self, value: &[u8]) -> Result<Vec<u8>> {
        self.add(array_bytes::<u8>(value.len())?)?;
        let mut copy = Vec::new();
        copy.try_reserve_exact(value.len()).map_err(allocation_error)?;
        copy.extend_from_slice(value);
        Ok(copy)
    }
    pub(super) fn copy_text(&mut self, value: &str) -> Result<String> { Ok(String::from_utf8(self.copy_bytes(value.as_bytes())?).expect("copied UTF8")) }
    fn mode(&self) -> FormAllocation<'a> { match &self.lease { Lease::Untracked => FormAllocation::Untracked, Lease::Tracked { memory, .. } => FormAllocation::Tracked(memory) } }
}

fn reserve<T>(values: &mut Vec<T>, additional: usize, charge: &mut Charge<'_>) -> Result<()> {
    let needed = add(values.len(), additional)?;
    if needed <= values.capacity() { return Ok(()); }
    let capacity = needed.max(values.capacity().checked_mul(2).ok_or_else(super::size_error)?).max(16);
    let new_bytes = array_bytes::<T>(capacity)?;
    let old_bytes = array_bytes::<T>(values.capacity())?;
    charge.add(new_bytes)?;
    let mut next = Vec::new();
    next.try_reserve_exact(capacity).map_err(allocation_error)?;
    next.append(values);
    drop(std::mem::replace(values, next));
    charge.resize(charge.bytes - old_bytes)
}

#[derive(Clone, Copy)]
enum Root { Empty, PendingLiteral, PendingNull, InlineNull, Stored }
#[derive(Clone, Copy)]
enum FrameKind {
    RawList, RawVector, RawSet, RawMap, Tagged(TextId), Anonymous, Literal,
    ValueList, ValueSet, ValueMap(bool), FixedSizeList, Struct { name: Option<TextId> },
}
struct Frame { node: NodeRef, kind: FrameKind, start: usize }
#[derive(Clone, Copy)]
enum Pending { Raw(FormId), Literal(LiteralId), Field { name: TextId, value: LiteralId, ordinal: usize } }

/// Builds one unpublished UTF8 raw root with fallible, structurally checked events.
/// A failed event discards all partial buffers; later events and `finish` report a build error.
/// Tracked buffers retain their budget through publication; untracked construction is explicit.
pub struct RegexFormBuilder<'a> {
    storage: FormStorage,
    frames: Vec<Frame>,
    pending: Vec<Pending>,
    root: Root,
    failed: bool,
    header: bool,
    retained: Charge<'a>,
    scratch: Charge<'a>,
}

pub(super) fn shape() -> Anomaly { Anomaly::incorrect("rtdb.plan/regex-form-build", "Invalid or failed raw form event stream") }

impl<'a> RegexFormBuilder<'a> {
    fn new(mode: FormAllocation<'a>) -> Self {
        Self { storage: FormStorage::default(), frames: Vec::new(), pending: Vec::new(), root: Root::Empty, failed: false, header: false, retained: Charge::new(mode), scratch: Charge::new(mode) }
    }
    /// Borrows an existing budget; no reservation or buffer is allocated until an event needs it.
    pub fn new_in(memory: &'a Arc<MemoryBudget>) -> Self { Self::new(FormAllocation::Tracked(memory)) }
    /// Creates an explicitly untracked compatibility builder without allocating.
    pub fn new_untracked() -> Self { Self::new(FormAllocation::Untracked) }
    fn event(&mut self, event: impl FnOnce(&mut Self) -> Result<()>) -> Result<()> {
        if self.failed { return Err(shape()); }
        let result = event(self);
        if result.is_err() {
            self.failed = true;
            drop(std::mem::take(&mut self.storage));
            drop(std::mem::take(&mut self.frames));
            drop(std::mem::take(&mut self.pending));
            self.retained.resize(0).expect("release is within reservation");
            self.scratch.resize(0).expect("release is within reservation");
        }
        result
    }
    fn header(&mut self) -> Result<()> { if !self.header { self.retained.add(owner_bytes()?)?; self.header = true; } Ok(()) }
    fn raw_slot(&self) -> Result<()> {
        match self.frames.last() {
            None if matches!(self.root, Root::Empty) => Ok(()),
            Some(frame) => match frame.kind {
                FrameKind::RawList | FrameKind::RawVector | FrameKind::RawSet | FrameKind::RawMap => Ok(()),
                FrameKind::Tagged(_) | FrameKind::Anonymous if self.pending.len() == frame.start => Ok(()),
                _ => Err(shape()),
            },
            None => Err(shape()),
        }
    }
    fn value_slot(&mut self) -> Result<()> {
        if matches!(self.root, Root::PendingLiteral) { self.materialize_literal()?; }
        match self.frames.last() {
            Some(frame) => match frame.kind {
                FrameKind::ValueList | FrameKind::ValueSet | FrameKind::ValueMap(_) | FrameKind::FixedSizeList => Ok(()),
                FrameKind::Literal if self.pending.len() == frame.start => Ok(()),
                FrameKind::Struct { name: Some(_) } => Ok(()),
                _ => Err(shape()),
            },
            None => Err(shape()),
        }
    }
    fn raw_row(&mut self, kind: RawKind) -> Result<FormId> {
        self.header()?;
        reserve(&mut self.storage.forms, 1, &mut self.retained)?;
        let id = FormId(self.storage.forms.len());
        self.storage.forms.push(RawRow { kind, parent: None });
        Ok(id)
    }
    fn literal_row(&mut self, kind: LiteralKind) -> Result<LiteralId> {
        reserve(&mut self.storage.literals, 1, &mut self.retained)?;
        let id = LiteralId(self.storage.literals.len());
        self.storage.literals.push(LiteralRow { kind, parent: None });
        Ok(id)
    }
    fn text(&mut self, text: &str) -> Result<TextId> {
        reserve(&mut self.storage.texts, 1, &mut self.retained)?;
        let text = self.retained.copy_text(text)?;
        let id = TextId(self.storage.texts.len());
        self.storage.texts.push(text);
        Ok(id)
    }
    fn attach(&mut self, child: NodeRef) -> Result<()> {
        if let Some(frame) = self.frames.last_mut() {
            let ordinal = self.pending.len() - frame.start;
            let pending = match (frame.kind, child) {
                (FrameKind::Struct { name: Some(name) }, NodeRef::Literal(value)) => { frame.kind = FrameKind::Struct { name: None }; Pending::Field { name, value, ordinal } }
                (_, NodeRef::Raw(value)) => Pending::Raw(value),
                (_, NodeRef::Literal(value)) => Pending::Literal(value),
            };
            self.storage.set_parent(child, Parent { node: frame.node, ordinal });
            reserve(&mut self.pending, 1, &mut self.scratch)?;
            self.pending.push(pending);
        } else {
            if !matches!(child, NodeRef::Raw(FormId(0))) { return Err(shape()); }
            self.root = Root::Stored;
        }
        Ok(())
    }
    fn push_frame(&mut self, node: NodeRef, kind: FrameKind) -> Result<()> {
        reserve(&mut self.frames, 1, &mut self.scratch)?;
        self.frames.push(Frame { node, kind, start: self.pending.len() });
        Ok(())
    }
    fn materialize_literal(&mut self) -> Result<()> {
        self.root = Root::Empty;
        let id = self.raw_row(RawKind::Draft)?;
        self.push_frame(NodeRef::Raw(id), FrameKind::Literal)
    }
    fn begin_raw(&mut self, kind: FrameKind) -> Result<()> {
        self.raw_slot()?;
        let id = self.raw_row(RawKind::Draft)?;
        self.push_frame(NodeRef::Raw(id), kind)
    }
    fn begin_value(&mut self, kind: FrameKind) -> Result<()> {
        self.value_slot()?;
        let id = self.literal_row(LiteralKind::Draft)?;
        self.push_frame(NodeRef::Literal(id), kind)
    }
    /// Begins a raw list; `end` closes it after zero or more raw children.
    pub fn begin_list(&mut self) -> Result<()> { self.event(|this| this.begin_raw(FrameKind::RawList)) }
    /// Begins a raw vector, preserving child order and duplicates.
    pub fn begin_vector(&mut self) -> Result<()> { self.event(|this| this.begin_raw(FrameKind::RawVector)) }
    /// Begins a raw set, preserving its ordered source occurrences.
    pub fn begin_set(&mut self) -> Result<()> { self.event(|this| this.begin_raw(FrameKind::RawSet)) }
    /// Begins a raw map; children must complete key/value pairs in source order.
    pub fn begin_map(&mut self) -> Result<()> { self.event(|this| this.begin_raw(FrameKind::RawMap)) }
    /// Begins an uninterpreted tag, which requires exactly one raw child.
    pub fn begin_tagged(&mut self, tag: &str) -> Result<()> { self.event(|this| { this.raw_slot()?; this.header()?; let tag = this.text(tag)?; this.begin_raw(FrameKind::Tagged(tag)) }) }
    /// Begins an anonymous form requiring exactly one raw child.
    pub fn begin_anonymous(&mut self) -> Result<()> { self.event(|this| this.begin_raw(FrameKind::Anonymous)) }
    /// Begins one physical literal payload; `end` closes it after exactly one value.
    pub fn begin_literal(&mut self) -> Result<()> {
        self.event(|this| { this.raw_slot()?; if this.frames.is_empty() { this.root = Root::PendingLiteral; Ok(()) } else { this.begin_raw(FrameKind::Literal) } })
    }
    /// Appends a legacy character scalar, including supplementary Unicode scalars.
    pub fn character(&mut self, value: char) -> Result<()> { self.event(|this| { this.raw_slot()?; let id = this.raw_row(RawKind::Character(value))?; this.attach(NodeRef::Raw(id)) }) }
    fn raw_text(&mut self, value: &str, kind: fn(TextId) -> RawKind) -> Result<()> {
        self.raw_slot()?; self.header()?;
        let text = self.text(value)?;
        let id = self.raw_row(kind(text))?;
        self.attach(NodeRef::Raw(id))
    }
    /// Copies a symbol spelling without resolving or evaluating it.
    pub fn symbol(&mut self, value: &str) -> Result<()> { self.event(|this| this.raw_text(value, RawKind::Symbol)) }
    /// Copies an uninterpreted integer spelling.
    pub fn big_integer(&mut self, value: &str) -> Result<()> { self.event(|this| this.raw_text(value, RawKind::BigInteger)) }
    /// Copies an uninterpreted decimal spelling.
    pub fn decimal(&mut self, value: &str) -> Result<()> { self.event(|this| this.raw_text(value, RawKind::Decimal)) }
    /// Copies an uninterpreted ratio spelling.
    pub fn ratio(&mut self, value: &str) -> Result<()> { self.event(|this| this.raw_text(value, RawKind::Ratio)) }
    fn value_text_kind(&mut self, value: &str, kind: fn(TextId) -> LiteralKind) -> Result<()> {
        self.value_slot()?;
        let text = self.text(value)?;
        let id = self.literal_row(kind(text))?;
        self.attach(NodeRef::Literal(id))
    }
    /// Copies one physical UTF8 string into the current literal context.
    pub fn value_text(&mut self, value: &str) -> Result<()> { self.event(|this| this.value_text_kind(value, LiteralKind::Text)) }
    /// Copies an exact keyword name without parsing or normalizing it.
    pub fn value_keyword(&mut self, value: &str) -> Result<()> { self.event(|this| this.value_text_kind(value, LiteralKind::Keyword)) }
    pub fn begin_value_list(&mut self) -> Result<()> { self.event(|this| this.begin_value(FrameKind::ValueList)) }
    /// Begins an ordered physical set without deduplication.
    pub fn begin_value_set(&mut self) -> Result<()> { self.event(|this| this.begin_value(FrameKind::ValueSet)) }
    /// Begins physical map entries; preserves the supplied sorted metadata without sorting entries.
    pub fn begin_value_map(&mut self, sorted: bool) -> Result<()> { self.event(|this| this.begin_value(FrameKind::ValueMap(sorted))) }
    pub fn begin_value_fixed_size_list(&mut self) -> Result<()> { self.event(|this| this.begin_value(FrameKind::FixedSizeList)) }
    /// Begins physical struct fields, sorted by UTF8 key with the last duplicate value retained.
    pub fn begin_value_struct(&mut self) -> Result<()> { self.event(|this| this.begin_value(FrameKind::Struct { name: None })) }
    /// Supplies a struct field name before its one physical value; duplicate values use last occurrence semantics.
    pub fn field(&mut self, name: &str) -> Result<()> {
        self.event(|this| {
            if !matches!(this.frames.last().map(|frame| frame.kind), Some(FrameKind::Struct { name: None })) { return Err(shape()); }
            let name = this.text(name)?;
            this.frames.last_mut().expect("validated struct").kind = FrameKind::Struct { name: Some(name) };
            Ok(())
        })
    }

    /// Copies one of the 34 nonrecursive physical variants; text, keyword and containers require their typed events.
    pub fn value_leaf(&mut self, value: &Value) -> Result<()> {
        self.event(|this| {
            if matches!(value, Value::Utf8(_) | Value::Keyword(_) | Value::List(_) | Value::Set(_) | Value::Map { .. } | Value::FixedSizeList(_) | Value::Struct(_)) { return Err(shape()); }
            if matches!(this.root, Root::PendingLiteral) && matches!(value, Value::Null) { this.root = Root::PendingNull; return Ok(()); }
            this.value_slot()?;
            reserve(&mut this.storage.leaves, 1, &mut this.retained)?;
            let value = Leaf34::copy(value, &mut this.retained)?;
            let leaf = LeafId(this.storage.leaves.len());
            this.storage.leaves.push(value);
            let id = this.literal_row(LiteralKind::Leaf(leaf))?;
            this.attach(NodeRef::Literal(id))
        })
    }

    /// Iteratively imports all 41 physical variants, retaining no recursive caller Value.
    /// The source stays borrowed until copying completes, including destination admission failures.
    pub fn literal(&mut self, value: &Value) -> Result<()> {
        self.event(|this| {
            this.begin_literal()?;
            if matches!(value, Value::Null) && matches!(this.root, Root::PendingLiteral) { this.value_leaf(value)?; return this.end(); }
            this.import(Action::Value(RegexLiteralView::new(value)))?;
            this.end()
        })
    }

    /// Copies a borrowed raw subtree into this root; no source backing or owning child handle is retained.
    pub fn append_view(&mut self, source: RegexFormView<'_>) -> Result<()> {
        self.event(|this| {
            if matches!(source.kind(), RegexFormKind::Literal(value) if matches!(value.kind(), RegexLiteralKind::Leaf(leaf) if matches!(leaf.value().as_ref(), Value::Null))) { return this.literal(&Value::Null); }
            this.raw_slot()?;
            this.import(Action::Raw(source))
        })
    }

    fn import(&mut self, initial: Action<'_>) -> Result<()> {
        let mut work = ImportWork { actions: Vec::new(), charge: Charge::new(self.retained.mode()) };
        work.push(initial)?;
        while let Some(action) = work.actions.pop() {
            match action {
                Action::End => self.end()?,
                Action::Raw(value) => match value.kind() {
                    RegexFormKind::Literal(value) => { self.begin_literal()?; work.push(Action::End)?; work.push(Action::Value(value))?; }
                    RegexFormKind::Character(value) => self.character(value)?,
                    RegexFormKind::Symbol(value) => self.symbol(value)?, RegexFormKind::BigInteger(value) => self.big_integer(value)?, RegexFormKind::Decimal(value) => self.decimal(value)?,
                    RegexFormKind::Ratio(value) => self.ratio(value)?,
                    RegexFormKind::List(values) => { self.begin_list()?; work.push(Action::End)?; work.push(Action::RawSequence(values, 0))?; }
                    RegexFormKind::Vector(values) => { self.begin_vector()?; work.push(Action::End)?; work.push(Action::RawSequence(values, 0))?; }
                    RegexFormKind::Set(values) => { self.begin_set()?; work.push(Action::End)?; work.push(Action::RawSequence(values, 0))?; }
                    RegexFormKind::Map(values) => { self.begin_map()?; work.push(Action::End)?; work.push(Action::RawPairs(values, 0))?; }
                    RegexFormKind::Tagged { tag, value } => { self.begin_tagged(tag)?; work.push(Action::End)?; work.push(Action::Raw(value))?; }
                    RegexFormKind::AnonymousFunction(value) => { self.begin_anonymous()?; work.push(Action::End)?; work.push(Action::Raw(value))?; }
                },
                Action::Value(value) => match value.kind() {
                    RegexLiteralKind::Text(value) => self.value_text(value)?, RegexLiteralKind::Keyword(value) => self.value_keyword(value)?,
                    RegexLiteralKind::Leaf(value) => self.value_leaf(value.value().as_ref())?,
                    RegexLiteralKind::List(values) => { self.begin_value_list()?; work.push(Action::End)?; work.push(Action::Sequence(values, 0))?; }
                    RegexLiteralKind::Set(values) => { self.begin_value_set()?; work.push(Action::End)?; work.push(Action::Sequence(values, 0))?; }
                    RegexLiteralKind::FixedSizeList(values) => { self.begin_value_fixed_size_list()?; work.push(Action::End)?; work.push(Action::Sequence(values, 0))?; }
                    RegexLiteralKind::Map { entries, sorted } => { self.begin_value_map(sorted)?; work.push(Action::End)?; work.push(Action::Pairs(entries, 0))?; }
                    RegexLiteralKind::Struct(fields) => { self.begin_value_struct()?; work.push(Action::End)?; work.push(Action::Fields(fields.iter()))?; }
                },
                Action::RawSequence(values, index) => if let Some(value) = values.get(index) { work.push(Action::RawSequence(values, index + 1))?; work.push(Action::Raw(value))?; },
                Action::RawPairs(values, index) => if let Some((key, value)) = values.get(index) { work.push(Action::RawPairs(values, index + 1))?; work.push(Action::Raw(value))?; work.push(Action::Raw(key))?; },
                Action::Sequence(values, index) => if let Some(value) = values.get(index) { work.push(Action::Sequence(values, index + 1))?; work.push(Action::Value(value))?; },
                Action::Pairs(values, index) => if let Some((key, value)) = values.get(index) { work.push(Action::Pairs(values, index + 1))?; work.push(Action::Value(value))?; work.push(Action::Value(key))?; },
                Action::Fields(mut fields) => if let Some((name, value)) = fields.next() { self.field(name)?; work.push(Action::Fields(fields))?; work.push(Action::Value(value))?; },
            }
        }
        Ok(())
    }

    /// Closes the current container or literal, validating arity and map/field completion before publication.
    pub fn end(&mut self) -> Result<()> { self.event(Self::close) }

    fn close(&mut self) -> Result<()> {
        if matches!(self.root, Root::PendingNull) { self.root = Root::InlineNull; return Ok(()); }
        let frame = self.frames.pop().ok_or_else(shape)?;
        let length = self.pending.len() - frame.start;
        match (frame.kind, frame.node) {
            (FrameKind::RawList | FrameKind::RawVector | FrameKind::RawSet, NodeRef::Raw(id)) => {
                let start = self.storage.raw_children.len();
                reserve(&mut self.storage.raw_children, length, &mut self.retained)?;
                for item in &self.pending[frame.start..] { let Pending::Raw(child) = item else { return Err(shape()); }; self.storage.raw_children.push(*child); }
                let span = Span { start, len: length };
                self.storage.forms[id.0].kind = match frame.kind { FrameKind::RawList => RawKind::List(span), FrameKind::RawVector => RawKind::Vector(span), _ => RawKind::Set(span) };
            }
            (FrameKind::RawMap, NodeRef::Raw(id)) => {
                if length % 2 != 0 { return Err(shape()); }
                let start = self.storage.raw_pairs.len();
                reserve(&mut self.storage.raw_pairs, length / 2, &mut self.retained)?;
                for pair in self.pending[frame.start..].chunks_exact(2) {
                    let [Pending::Raw(key), Pending::Raw(value)] = pair else { return Err(shape()); };
                    self.storage.raw_pairs.push((*key, *value));
                }
                self.storage.forms[id.0].kind = RawKind::Map(Span { start, len: length / 2 });
            }
            (FrameKind::Literal, NodeRef::Raw(id)) => {
                let [Pending::Literal(value)] = &self.pending[frame.start..] else { return Err(shape()); };
                self.storage.forms[id.0].kind = RawKind::Literal(*value);
            }
            (FrameKind::Tagged(_) | FrameKind::Anonymous, NodeRef::Raw(id)) => {
                let [Pending::Raw(value)] = &self.pending[frame.start..] else { return Err(shape()); };
                self.storage.forms[id.0].kind = match frame.kind { FrameKind::Tagged(tag) => RawKind::Tagged { tag, value: *value }, _ => RawKind::AnonymousFunction(*value) };
            }
            (FrameKind::ValueList | FrameKind::ValueSet | FrameKind::FixedSizeList, NodeRef::Literal(id)) => {
                let start = self.storage.literal_children.len();
                reserve(&mut self.storage.literal_children, length, &mut self.retained)?;
                for item in &self.pending[frame.start..] { let Pending::Literal(child) = item else { return Err(shape()); }; self.storage.literal_children.push(*child); }
                let span = Span { start, len: length };
                self.storage.literals[id.0].kind = match frame.kind { FrameKind::ValueList => LiteralKind::List(span), FrameKind::ValueSet => LiteralKind::Set(span), _ => LiteralKind::FixedSizeList(span) };
            }
            (FrameKind::ValueMap(sorted), NodeRef::Literal(id)) => {
                if length % 2 != 0 { return Err(shape()); }
                let start = self.storage.literal_pairs.len();
                reserve(&mut self.storage.literal_pairs, length / 2, &mut self.retained)?;
                for pair in self.pending[frame.start..].chunks_exact(2) {
                    let [Pending::Literal(key), Pending::Literal(value)] = pair else { return Err(shape()); };
                    self.storage.literal_pairs.push((*key, *value));
                }
                self.storage.literals[id.0].kind = LiteralKind::Map { entries: Span { start, len: length / 2 }, sorted };
            }
            (FrameKind::Struct { name: None }, NodeRef::Literal(id)) => {
                heap_sort_fields(&mut self.pending[frame.start..], &self.storage.texts);
                let start = self.storage.fields.len();
                reserve(&mut self.storage.fields, length, &mut self.retained)?;
                let mut at = frame.start;
                while at < self.pending.len() {
                    let Pending::Field { name, .. } = self.pending[at] else { return Err(shape()); };
                    let mut end = at + 1;
                    while end < self.pending.len() {
                        let Pending::Field { name: next, .. } = self.pending[end] else { return Err(shape()); };
                        if self.storage.texts[name.0] != self.storage.texts[next.0] { break; }
                        end += 1;
                    }
                    let Pending::Field { value, .. } = self.pending[end - 1] else { return Err(shape()); };
                    let ordinal = self.storage.fields.len() - start;
                    self.storage.set_parent(NodeRef::Literal(value), Parent { node: frame.node, ordinal });
                    self.storage.fields.push(FieldRow { name, value });
                    for index in at..end {
                        let Pending::Field { name: losing_name, value: losing_value, .. } = self.pending[index] else { return Err(shape()); };
                        if index != at { self.storage.texts[losing_name.0] = String::new(); }
                        if index + 1 != end { self.discard(NodeRef::Literal(losing_value)); }
                    }
                    at = end;
                }
                self.storage.literals[id.0].kind = LiteralKind::Struct(Span { start, len: self.storage.fields.len() - start });
            }
            _ => return Err(shape()),
        }
        self.pending.truncate(frame.start);
        self.attach(frame.node)
    }

    fn discard(&mut self, boundary: NodeRef) {
        let mut current = Some(boundary);
        while let Some(node) = current {
            current = self.storage.next(boundary, node);
            match node {
                NodeRef::Raw(id) => match self.storage.forms[id.0].kind {
                    RawKind::Symbol(text) | RawKind::BigInteger(text) | RawKind::Decimal(text) | RawKind::Ratio(text) | RawKind::Tagged { tag: text, .. } => self.storage.texts[text.0] = String::new(),
                    _ => (),
                },
                NodeRef::Literal(id) => match self.storage.literals[id.0].kind {
                    LiteralKind::Text(text) | LiteralKind::Keyword(text) => self.storage.texts[text.0] = String::new(),
                    LiteralKind::Leaf(leaf) => self.storage.leaves[leaf.0].clear(),
                    LiteralKind::Struct(span) => for field in &self.storage.fields[span.range()] { self.storage.texts[field.name.0] = String::new(); },
                    _ => (),
                },
            }
        }
    }

    pub(super) fn set_tag(&mut self, tag: &str) -> Result<()> {
        self.event(|this| {
            let Some(Frame { kind: FrameKind::Tagged(id), .. }) = this.frames.last() else { return Err(shape()); };
            let id = *id;
            let copy = this.retained.copy_text(tag)?;
            this.storage.texts[id.0] = copy;
            Ok(())
        })
    }
    pub(super) fn set_sorted(&mut self, sorted: bool) -> Result<()> {
        self.event(|this| {
            let Some(frame) = this.frames.last_mut() else { return Err(shape()); };
            if !matches!(frame.kind, FrameKind::ValueMap(_)) { return Err(shape()); }
            frame.kind = FrameKind::ValueMap(sorted);
            Ok(())
        })
    }

    /// Publishes exactly one complete root after validating its shape and retained size.
    /// Storage and its paired tracked lease remain alive together through every root clone.
    pub fn finish(mut self) -> Result<RegexForm> {
        if self.failed || !self.frames.is_empty() { return Err(shape()); }
        if matches!(self.root, Root::InlineNull) { return Ok(RegexForm::default()); }
        if !matches!(self.root, Root::Stored) || self.storage.forms.is_empty() { return Err(shape()); }
        let retained = self.storage.physical_bytes()?;
        if retained > self.retained.bytes { return Err(shape()); }
        self.retained.resize(retained)?;
        self.storage.retained = retained;
        drop(self.frames);
        drop(self.pending);
        drop(self.scratch);
        let storage = Arc::new(self.storage);
        Ok(RegexForm(match self.retained.lease {
            Lease::Untracked => Backing::Untracked(storage),
            Lease::Tracked { reservation: Some(reservation), .. } => Backing::Tracked { storage, _reservation: Arc::new(reservation) },
            Lease::Tracked { reservation: None, .. } => unreachable!("stored root was admitted"),
        }))
    }
}

enum Action<'a> {
    Raw(RegexFormView<'a>), Value(RegexLiteralView<'a>), End,
    RawSequence(super::RegexForms<'a>, usize), RawPairs(super::RegexFormPairs<'a>, usize),
    Sequence(super::RegexLiterals<'a>, usize), Pairs(super::RegexLiteralPairs<'a>, usize), Fields(super::RegexFieldIter<'a>),
}
struct ImportWork<'budget, 'source> { actions: Vec<Action<'source>>, charge: Charge<'budget> }
impl<'source> ImportWork<'_, 'source> {
    fn push(&mut self, action: Action<'source>) -> Result<()> { reserve(&mut self.actions, 1, &mut self.charge)?; self.actions.push(action); Ok(()) }
}

fn heap_sort_fields(values: &mut [Pending], texts: &[String]) {
    fn greater(a: Pending, b: Pending, texts: &[String]) -> bool {
        let (Pending::Field { name: a, ordinal: ai, .. }, Pending::Field { name: b, ordinal: bi, .. }) = (a, b) else { unreachable!("struct pending fields") };
        texts[a.0].cmp(&texts[b.0]).then(ai.cmp(&bi)).is_gt()
    }
    fn sift(values: &mut [Pending], mut at: usize, end: usize, texts: &[String]) {
        while at < end / 2 {
            let left = at * 2 + 1;
            if left >= end { break; }
            let child = if left + 1 < end && greater(values[left + 1], values[left], texts) { left + 1 } else { left };
            if !greater(values[child], values[at], texts) { break; }
            values.swap(at, child);
            at = child;
        }
    }
    for at in (0..values.len() / 2).rev() { sift(values, at, values.len(), texts); }
    for end in (1..values.len()).rev() { values.swap(0, end); sift(values, 0, end, texts); }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replacement_growth_admits_both_old_and_new_buffers_before_allocation_or_move() {
        for limit in [383,384] {
            let memory = MemoryBudget::new(limit);
            let mut charge = Charge::new(FormAllocation::Tracked(&memory));
            let mut values = Vec::<u64>::new();
            reserve(&mut values, 16, &mut charge).unwrap();
            values.extend(0..16);
            assert_eq!(memory.used_bytes(), 128);
            let result = reserve(&mut values, 1, &mut charge);
            if limit == 383 {
                assert_eq!(result.unwrap_err().code, "rtdb/memory-budget-exceeded");
                assert_eq!(values.capacity(), 16);
                assert_eq!(memory.used_bytes(), 128);
            } else {
                result.unwrap();
                assert_eq!(values.capacity(), 32);
                assert_eq!(memory.peak_bytes(), 384);
                assert_eq!(memory.used_bytes(), 256);
            }
            assert_eq!(values.as_slice(), &[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]);
            drop(values); drop(charge);
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}
