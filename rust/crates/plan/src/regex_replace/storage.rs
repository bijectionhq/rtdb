use std::{alloc::Layout, fmt, mem::size_of, sync::atomic::AtomicUsize};
use rtdb_types::{Anomaly, Result, Value};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct FormId(pub(super) usize);
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct LiteralId(pub(super) usize);
#[derive(Clone, Copy)]
pub(super) struct TextId(pub(super) usize);
#[derive(Clone, Copy)]
pub(super) struct LeafId(pub(super) usize);
#[derive(Clone, Copy)]
pub(super) struct Span { pub(super) start: usize, pub(super) len: usize }
impl Span { pub(super) fn range(self) -> std::ops::Range<usize> { self.start..self.start + self.len } }

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum NodeRef { Raw(FormId), Literal(LiteralId) }
#[derive(Clone, Copy)]
pub(super) struct Parent { pub(super) node: NodeRef, pub(super) ordinal: usize }

pub(super) struct RawRow { pub(super) kind: RawKind, pub(super) parent: Option<Parent> }
#[derive(Clone, Copy)]
pub(super) enum RawKind {
    Draft, Literal(LiteralId), Character(char), Symbol(TextId), BigInteger(TextId), Decimal(TextId), Ratio(TextId),
    List(Span), Vector(Span), Set(Span), Map(Span),
    Tagged { tag: TextId, value: FormId }, AnonymousFunction(FormId),
}
pub(super) struct LiteralRow { pub(super) kind: LiteralKind, pub(super) parent: Option<Parent> }
#[derive(Clone, Copy)]
pub(super) enum LiteralKind {
    Draft, Text(TextId), Keyword(TextId), Leaf(LeafId), List(Span), Set(Span), FixedSizeList(Span),
    Map { entries: Span, sorted: bool }, Struct(Span),
}
pub(super) struct FieldRow { pub(super) name: TextId, pub(super) value: LiteralId }
pub(super) struct Leaf34(Value);
impl Leaf34 {
    pub(super) fn value(&self) -> &Value { &self.0 }
    pub(super) fn clear(&mut self) { self.0 = Value::Null; }
    pub(super) fn copy(value: &Value, charge: &mut super::builder::Charge<'_>) -> Result<Self> {
        Ok(Self(match value {
            Value::Undefined => Value::Undefined, Value::Null => Value::Null, Value::Bool(v) => Value::Bool(*v),
            Value::I8(v) => Value::I8(*v), Value::I16(v) => Value::I16(*v), Value::I32(v) => Value::I32(*v), Value::I64(v) => Value::I64(*v),
            Value::U8(v) => Value::U8(*v), Value::U16(v) => Value::U16(*v), Value::U32(v) => Value::U32(*v), Value::U64(v) => Value::U64(*v),
            Value::F16(v) => Value::F16(*v), Value::F32(v) => Value::F32(*v), Value::F64(v) => Value::F64(*v),
            Value::Binary(v) => Value::Binary(charge.copy_bytes(v)?), Value::FixedSizeBinary(v) => Value::FixedSizeBinary(charge.copy_bytes(v)?),
            Value::Decimal(v) => Value::Decimal(rtdb_types::DecimalValue { unscaled: charge.copy_bytes(&v.unscaled)?, precision: v.precision, scale: v.scale, bit_width: v.bit_width }),
            Value::Date(v) => Value::Date(*v), Value::DateDay(v) => Value::DateDay(*v), Value::DateMilli(v) => Value::DateMilli(*v),
            Value::Time { value, unit, bit_width } => Value::Time { value: *value, unit: *unit, bit_width: *bit_width },
            Value::Timestamp { value, unit, timezone } => Value::Timestamp { value: *value, unit: *unit, timezone: timezone.as_ref().map(|v| charge.copy_text(v)).transpose()? },
            Value::Duration { value, unit } => Value::Duration { value: *value, unit: *unit },
            Value::IntervalYearMonth(v) => Value::IntervalYearMonth(*v),
            Value::IntervalDayTime { days, milliseconds } => Value::IntervalDayTime { days: *days, milliseconds: *milliseconds },
            Value::IntervalMonthDayNano { months, days, nanoseconds } => Value::IntervalMonthDayNano { months: *months, days: *days, nanoseconds: *nanoseconds },
            Value::IntervalMonthDayMicro { months, days, nanoseconds } => Value::IntervalMonthDayMicro { months: *months, days: *days, nanoseconds: *nanoseconds },
            Value::Uuid(v) => Value::Uuid(*v), Value::Uri(v) => Value::Uri(charge.copy_text(v)?), Value::Transit(v) => Value::Transit(charge.copy_bytes(v)?),
            Value::Oid(v) => Value::Oid(*v), Value::RegClass(v) => Value::RegClass(*v), Value::RegProc(v) => Value::RegProc(*v),
            Value::TimestampTzRange { lower, upper } => Value::TimestampTzRange { lower: *lower, upper: *upper },
            Value::Utf8(_) | Value::Keyword(_) | Value::List(_) | Value::Set(_) | Value::Map { .. } | Value::FixedSizeList(_) | Value::Struct(_) => return Err(super::builder::shape()),
        }))
    }
}

#[derive(Default)]
pub(super) struct FormStorage {
    pub(super) forms: Vec<RawRow>,
    pub(super) literals: Vec<LiteralRow>,
    pub(super) leaves: Vec<Leaf34>,
    pub(super) texts: Vec<String>,
    pub(super) raw_children: Vec<FormId>,
    pub(super) raw_pairs: Vec<(FormId, FormId)>,
    pub(super) literal_children: Vec<LiteralId>,
    pub(super) literal_pairs: Vec<(LiteralId, LiteralId)>,
    pub(super) fields: Vec<FieldRow>,
    pub(super) retained: usize,
}

pub(super) fn add(a: usize, b: usize) -> Result<usize> { a.checked_add(b).ok_or_else(super::size_error) }
pub(super) fn array_bytes<T>(count: usize) -> Result<usize> {
    Layout::array::<T>(count).map(|layout| layout.size()).map_err(|_| super::size_error())
}
pub(super) fn allocation_error(cause: std::collections::TryReserveError) -> Anomaly { Anomaly::incorrect("rtdb/allocation-failed", cause.to_string()).with_kind(rtdb_types::AnomalyKind::ResourceExhausted) }
pub(super) fn owner_bytes() -> Result<usize> {
    let layout = Layout::new::<[AtomicUsize; 2]>().extend(Layout::new::<FormStorage>()).map_err(|_| super::size_error())?.0.pad_to_align();
    add(layout.size(), 256)
}

impl FormStorage {
    pub(super) fn structural_eq(&self, other: &Self) -> bool {
        let boundary = NodeRef::Raw(FormId(0));
        let (mut left, mut right) = (Some(boundary), Some(boundary));
        while let (Some(a), Some(b)) = (left, right) {
            let same = match (a, b) {
                (NodeRef::Raw(a), NodeRef::Raw(b)) => match (self.forms[a.0].kind, other.forms[b.0].kind) {
                    (RawKind::Literal(_), RawKind::Literal(_)) | (RawKind::AnonymousFunction(_), RawKind::AnonymousFunction(_)) => true,
                    (RawKind::Character(a), RawKind::Character(b)) => a == b,
                    (RawKind::Symbol(a), RawKind::Symbol(b)) | (RawKind::BigInteger(a), RawKind::BigInteger(b))
                    | (RawKind::Decimal(a), RawKind::Decimal(b)) | (RawKind::Ratio(a), RawKind::Ratio(b)) => self.texts[a.0] == other.texts[b.0],
                    (RawKind::Tagged { tag: a, .. }, RawKind::Tagged { tag: b, .. }) => self.texts[a.0] == other.texts[b.0],
                    (RawKind::List(a), RawKind::List(b)) | (RawKind::Vector(a), RawKind::Vector(b)) | (RawKind::Set(a), RawKind::Set(b)) | (RawKind::Map(a), RawKind::Map(b)) => a.len == b.len,
                    _ => false,
                },
                (NodeRef::Literal(a), NodeRef::Literal(b)) => match (self.literals[a.0].kind, other.literals[b.0].kind) {
                    (LiteralKind::Text(a), LiteralKind::Text(b)) | (LiteralKind::Keyword(a), LiteralKind::Keyword(b)) => self.texts[a.0] == other.texts[b.0],
                    (LiteralKind::Leaf(a), LiteralKind::Leaf(b)) => self.leaves[a.0].0 == other.leaves[b.0].0,
                    (LiteralKind::List(a), LiteralKind::List(b)) | (LiteralKind::Set(a), LiteralKind::Set(b)) | (LiteralKind::FixedSizeList(a), LiteralKind::FixedSizeList(b)) => a.len == b.len,
                    (LiteralKind::Map { entries: a, sorted: sa }, LiteralKind::Map { entries: b, sorted: sb }) => a.len == b.len && sa == sb,
                    (LiteralKind::Struct(a), LiteralKind::Struct(b)) => a.len == b.len && self.fields[a.range()].iter().zip(&other.fields[b.range()]).all(|(a, b)| self.texts[a.name.0] == other.texts[b.name.0]),
                    _ => false,
                },
                _ => false,
            };
            if !same { return false; }
            left = self.next(boundary, a);
            right = other.next(boundary, b);
        }
        left.is_none() && right.is_none()
    }
    pub(super) fn physical_bytes(&self) -> Result<usize> {
        let mut bytes = owner_bytes()?;
        for capacity in [array_bytes::<RawRow>(self.forms.capacity())?, array_bytes::<LiteralRow>(self.literals.capacity())?,
            array_bytes::<Leaf34>(self.leaves.capacity())?, array_bytes::<String>(self.texts.capacity())?,
            array_bytes::<FormId>(self.raw_children.capacity())?, array_bytes::<(FormId, FormId)>(self.raw_pairs.capacity())?,
            array_bytes::<LiteralId>(self.literal_children.capacity())?, array_bytes::<(LiteralId, LiteralId)>(self.literal_pairs.capacity())?,
            array_bytes::<FieldRow>(self.fields.capacity())?] { bytes = add(bytes, capacity)?; }
        for text in &self.texts { bytes = add(bytes, text.capacity())?; }
        for leaf in &self.leaves {
            bytes = add(bytes, match &leaf.0 {
                Value::Binary(bytes) | Value::FixedSizeBinary(bytes) | Value::Transit(bytes) => bytes.capacity(),
                Value::Decimal(value) => value.unscaled.capacity(), Value::Uri(text) => text.capacity(),
                Value::Timestamp { timezone, .. } => timezone.as_ref().map_or(0, String::capacity),
                _ => 0,
            })?;
        }
        Ok(bytes)
    }
    pub(super) fn parent(&self, node: NodeRef) -> Option<Parent> {
        match node { NodeRef::Raw(id) => self.forms[id.0].parent, NodeRef::Literal(id) => self.literals[id.0].parent }
    }
    pub(super) fn set_parent(&mut self, node: NodeRef, parent: Parent) {
        match node { NodeRef::Raw(id) => self.forms[id.0].parent = Some(parent), NodeRef::Literal(id) => self.literals[id.0].parent = Some(parent) }
    }
    pub(super) fn child_count(&self, node: NodeRef) -> usize {
        match node {
            NodeRef::Raw(id) => match self.forms[id.0].kind {
                RawKind::Literal(_) | RawKind::Tagged { .. } | RawKind::AnonymousFunction(_) => 1,
                RawKind::List(span) | RawKind::Vector(span) | RawKind::Set(span) => span.len,
                RawKind::Map(span) => span.len * 2,
                RawKind::Draft | RawKind::Character(_) | RawKind::Symbol(_) | RawKind::BigInteger(_) | RawKind::Decimal(_) | RawKind::Ratio(_) => 0,
            },
            NodeRef::Literal(id) => match self.literals[id.0].kind {
                LiteralKind::List(span) | LiteralKind::Set(span) | LiteralKind::FixedSizeList(span) | LiteralKind::Struct(span) => span.len,
                LiteralKind::Map { entries, .. } => entries.len * 2,
                LiteralKind::Draft | LiteralKind::Text(_) | LiteralKind::Keyword(_) | LiteralKind::Leaf(_) => 0,
            },
        }
    }
    pub(super) fn child(&self, node: NodeRef, ordinal: usize) -> NodeRef {
        match node {
            NodeRef::Raw(id) => match self.forms[id.0].kind {
                RawKind::Literal(value) => NodeRef::Literal(value),
                RawKind::Tagged { value, .. } | RawKind::AnonymousFunction(value) => NodeRef::Raw(value),
                RawKind::List(span) | RawKind::Vector(span) | RawKind::Set(span) => NodeRef::Raw(self.raw_children[span.start + ordinal]),
                RawKind::Map(span) => { let pair = self.raw_pairs[span.start + ordinal / 2]; NodeRef::Raw(if ordinal % 2 == 0 { pair.0 } else { pair.1 }) }
                _ => unreachable!("leaf has no child"),
            },
            NodeRef::Literal(id) => match self.literals[id.0].kind {
                LiteralKind::List(span) | LiteralKind::Set(span) | LiteralKind::FixedSizeList(span) => NodeRef::Literal(self.literal_children[span.start + ordinal]),
                LiteralKind::Map { entries, .. } => { let pair = self.literal_pairs[entries.start + ordinal / 2]; NodeRef::Literal(if ordinal % 2 == 0 { pair.0 } else { pair.1 }) }
                LiteralKind::Struct(span) => NodeRef::Literal(self.fields[span.start + ordinal].value),
                _ => unreachable!("leaf has no child"),
            },
        }
    }
    pub(super) fn next(&self, boundary: NodeRef, mut node: NodeRef) -> Option<NodeRef> {
        if self.child_count(node) != 0 { return Some(self.child(node, 0)); }
        loop {
            if node == boundary { return None; }
            let parent = self.parent(node).expect("published child has a parent");
            if parent.ordinal + 1 < self.child_count(parent.node) { return Some(self.child(parent.node, parent.ordinal + 1)); }
            node = parent.node;
        }
    }
    pub(super) fn work_bound(&self) -> Result<usize> {
        let boundary = NodeRef::Raw(FormId(0));
        let mut current = Some(boundary);
        let mut bytes = 0;
        while let Some(node) = current {
            match node {
                NodeRef::Raw(id) => {
                    bytes = add(bytes, size_of::<RawRow>())?;
                    bytes = add(bytes, match self.forms[id.0].kind {
                        RawKind::Symbol(text) | RawKind::BigInteger(text) | RawKind::Decimal(text) | RawKind::Ratio(text) | RawKind::Tagged { tag: text, .. } => add(size_of::<String>(), self.texts[text.0].len())?,
                        RawKind::List(span) | RawKind::Vector(span) | RawKind::Set(span) => array_bytes::<FormId>(span.len)?,
                        RawKind::Map(span) => array_bytes::<(FormId, FormId)>(span.len)?,
                        _ => 0,
                    })?;
                }
                NodeRef::Literal(id) => {
                    bytes = add(bytes, size_of::<LiteralRow>())?;
                    bytes = add(bytes, match self.literals[id.0].kind {
                        LiteralKind::Text(text) | LiteralKind::Keyword(text) => add(size_of::<String>(), self.texts[text.0].len())?,
                        LiteralKind::Leaf(leaf) => leaf_bytes(&self.leaves[leaf.0].0)?,
                        LiteralKind::List(span) | LiteralKind::Set(span) | LiteralKind::FixedSizeList(span) => array_bytes::<LiteralId>(span.len)?,
                        LiteralKind::Map { entries, .. } => array_bytes::<(LiteralId, LiteralId)>(entries.len)?,
                        LiteralKind::Struct(span) => {
                            let mut own = array_bytes::<FieldRow>(span.len)?;
                            for field in &self.fields[span.range()] { own = add(own, add(size_of::<String>(), self.texts[field.name.0].len())?)?; }
                            own
                        }
                        LiteralKind::Draft => 0,
                    })?;
                }
            }
            current = self.next(boundary, node);
        }
        Ok(bytes)
    }
}

pub(super) fn leaf_bytes(value: &Value) -> Result<usize> {
    let dynamic = match value {
        Value::Binary(bytes) | Value::FixedSizeBinary(bytes) | Value::Transit(bytes) => bytes.len(),
        Value::Decimal(value) => value.unscaled.len(),
        Value::Timestamp { timezone, .. } => timezone.as_ref().map_or(0, String::len),
        Value::Uri(value) => value.len(),
        Value::Undefined | Value::Null | Value::Bool(_) | Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_)
        | Value::U8(_) | Value::U16(_) | Value::U32(_) | Value::U64(_) | Value::F16(_) | Value::F32(_) | Value::F64(_)
        | Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_) | Value::Time { .. } | Value::Duration { .. }
        | Value::IntervalYearMonth(_) | Value::IntervalDayTime { .. } | Value::IntervalMonthDayNano { .. }
        | Value::IntervalMonthDayMicro { .. } | Value::Uuid(_) | Value::Oid(_) | Value::RegClass(_) | Value::RegProc(_)
        | Value::TimestampTzRange { .. } => 0,
        Value::Utf8(_) | Value::Keyword(_) | Value::List(_) | Value::Set(_) | Value::Map { .. } | Value::FixedSizeList(_) | Value::Struct(_) => unreachable!("sealed leaf"),
    };
    add(size_of::<Value>(), dynamic)
}

#[derive(Clone, Copy)]
enum Code { Node, Payload, Part(usize), Element(usize), Pair(usize), FieldKey(usize), Inner }
#[derive(Clone, Copy)]
struct Virtual<'a> { storage: Option<&'a FormStorage>, anchor: NodeRef, code: Code }
#[derive(Clone, Copy)]
enum Atom<'a> { Text(&'a str), Char(char), Bool(bool), I8(i8), I16(i16), I32(i32), I64(i64), U8(u8), U16(u16), U32(u32), U64(u64), F32(f32), F64(f64) }
#[derive(Clone, Copy)]
enum Compound { Tuple, Struct(&'static [&'static str]), List, Map }
#[derive(Clone, Copy)]
enum Shape<'a> { Unit(&'static str), Atom(Atom<'a>), Children { kind: Compound, name: &'static str, len: usize } }
impl Shape<'_> {
    fn len(self) -> usize { match self { Self::Children { len, .. } => len, _ => 0 } }
}
fn tuple(name: &'static str) -> Shape<'static> { Shape::Children { kind: Compound::Tuple, name, len: 1 } }
fn structure(name: &'static str, fields: &'static [&'static str]) -> Shape<'static> { Shape::Children { kind: Compound::Struct(fields), name, len: fields.len() } }
fn list(len: usize) -> Shape<'static> { Shape::Children { kind: Compound::List, name: "", len } }
fn unit(value: rtdb_types::TimeUnit) -> &'static str {
    match value { rtdb_types::TimeUnit::Second => "Second", rtdb_types::TimeUnit::Millisecond => "Millisecond", rtdb_types::TimeUnit::Microsecond => "Microsecond", rtdb_types::TimeUnit::Nanosecond => "Nanosecond" }
}

impl<'a> Virtual<'a> {
    fn at(self, code: Code) -> Self { Self { code, ..self } }
    fn node(self, anchor: NodeRef) -> Self { Self { anchor, code: Code::Node, ..self } }
    fn raw(self) -> RawKind { let NodeRef::Raw(id) = self.anchor else { unreachable!("raw anchor") }; self.storage.map_or(RawKind::Literal(LiteralId(0)), |storage| storage.forms[id.0].kind) }
    fn literal(self) -> LiteralKind { let NodeRef::Literal(id) = self.anchor else { unreachable!("literal anchor") }; self.storage.map_or(LiteralKind::Leaf(LeafId(0)), |storage| storage.literals[id.0].kind) }
    fn text(self, text: TextId) -> &'a str { &self.storage.expect("stored text").texts[text.0] }
    fn leaf(self) -> &'a Value {
        static NULL: Value = Value::Null;
        let Some(storage) = self.storage else { return &NULL; };
        let LiteralKind::Leaf(leaf) = self.literal() else { unreachable!("leaf anchor") };
        &storage.leaves[leaf.0].0
    }
    fn actual_child(self, ordinal: usize) -> Self {
        self.node(match self.storage { Some(storage) => storage.child(self.anchor, ordinal), None => NodeRef::Literal(LiteralId(0)) })
    }
    fn shape(self) -> Shape<'a> {
        match self.code {
            Code::Node => match self.anchor {
                NodeRef::Raw(_) => match self.raw() {
                    RawKind::Literal(_) => tuple("Literal"), RawKind::Character(_) => tuple("Character"), RawKind::Symbol(_) => tuple("Symbol"),
                    RawKind::BigInteger(_) => tuple("BigInteger"), RawKind::Decimal(_) => tuple("Decimal"), RawKind::List(_) => tuple("List"),
                    RawKind::Vector(_) => tuple("Vector"), RawKind::Set(_) => tuple("Set"), RawKind::Map(_) => tuple("Map"),
                    RawKind::Tagged { .. } => structure("Tagged", &["tag", "value"]), RawKind::AnonymousFunction(_) => tuple("AnonymousFunction"),
                    RawKind::Ratio(_) => tuple("Ratio"),
                    RawKind::Draft => unreachable!("published raw node"),
                },
                NodeRef::Literal(_) => match self.literal() {
                    LiteralKind::Text(_) => tuple("Utf8"), LiteralKind::Keyword(_) => tuple("Keyword"), LiteralKind::List(_) => tuple("List"),
                    LiteralKind::Set(_) => tuple("Set"), LiteralKind::FixedSizeList(_) => tuple("FixedSizeList"), LiteralKind::Struct(_) => tuple("Struct"),
                    LiteralKind::Map { .. } => structure("Map", &["entries", "sorted"]), LiteralKind::Leaf(_) => leaf_shape(self.leaf()),
                    LiteralKind::Draft => unreachable!("published literal"),
                },
            },
            Code::Payload => match self.anchor {
                NodeRef::Raw(_) => match self.raw() {
                    RawKind::Character(value) => Shape::Atom(Atom::Char(value)),
                    RawKind::Symbol(text) | RawKind::BigInteger(text) | RawKind::Decimal(text) | RawKind::Ratio(text) | RawKind::Tagged { tag: text, .. } => Shape::Atom(Atom::Text(self.text(text))),
                    RawKind::List(span) | RawKind::Vector(span) | RawKind::Set(span) | RawKind::Map(span) => list(span.len),
                    _ => unreachable!("direct raw child"),
                },
                NodeRef::Literal(_) => match self.literal() {
                    LiteralKind::Text(text) => Shape::Atom(Atom::Text(self.text(text))), LiteralKind::Keyword(_) => tuple("Keyword"),
                    LiteralKind::List(span) | LiteralKind::Set(span) | LiteralKind::FixedSizeList(span) | LiteralKind::Map { entries: span, .. } => list(span.len),
                    LiteralKind::Struct(span) => Shape::Children { kind: Compound::Map, name: "", len: span.len * 2 },
                    LiteralKind::Leaf(_) => leaf_payload(self.leaf()), LiteralKind::Draft => unreachable!("draft"),
                },
            },
            Code::Part(index) => match self.literal() {
                LiteralKind::Keyword(text) => Shape::Atom(Atom::Text(self.text(text))),
                LiteralKind::Map { sorted, .. } => Shape::Atom(Atom::Bool(sorted)),
                LiteralKind::Leaf(_) => leaf_part(self.leaf(), index),
                _ => unreachable!("virtual part"),
            },
            Code::Element(index) => {
                let value = match self.leaf() { Value::Binary(bytes) | Value::FixedSizeBinary(bytes) | Value::Transit(bytes) => bytes[index], Value::Decimal(value) => value.unscaled[index], Value::Uuid(bytes) => bytes[index], _ => unreachable!("byte element") };
                Shape::Atom(Atom::U8(value))
            }
            Code::Pair(_) => Shape::Children { kind: Compound::Tuple, name: "", len: 2 },
            Code::FieldKey(index) => {
                let LiteralKind::Struct(span) = self.literal() else { unreachable!("field key") };
                let field = &self.storage.expect("stored fields").fields[span.start + index];
                Shape::Atom(Atom::Text(self.text(field.name)))
            }
            Code::Inner => match self.leaf() {
                Value::Date(value) => Shape::Atom(Atom::I64(value.date().epoch_day())),
                Value::Timestamp { timezone: Some(value), .. } => Shape::Atom(Atom::Text(value)),
                _ => unreachable!("nested bounded leaf"),
            },
        }
    }
    fn child(self, ordinal: usize) -> Self {
        match self.code {
            Code::Node => match self.anchor {
                NodeRef::Raw(_) => match self.raw() {
                    RawKind::Literal(_) | RawKind::AnonymousFunction(_) => self.actual_child(0),
                    RawKind::Tagged { .. } if ordinal == 1 => self.actual_child(0),
                    _ => self.at(Code::Payload),
                },
                NodeRef::Literal(_) => match self.literal() {
                    LiteralKind::Map { .. } if ordinal == 1 => self.at(Code::Part(1)),
                    LiteralKind::Leaf(_) if matches!(self.shape(), Shape::Children { kind: Compound::Struct(_), .. }) => self.at(Code::Part(ordinal)),
                    _ => self.at(Code::Payload),
                },
            },
            Code::Payload => match self.anchor {
                NodeRef::Raw(_) => match self.raw() { RawKind::Map(_) => self.at(Code::Pair(ordinal)), _ => self.actual_child(ordinal) },
                NodeRef::Literal(_) => match self.literal() {
                    LiteralKind::Keyword(_) => self.at(Code::Part(0)),
                    LiteralKind::Map { .. } => self.at(Code::Pair(ordinal)),
                    LiteralKind::Struct(_) => if ordinal % 2 == 0 { self.at(Code::FieldKey(ordinal / 2)) } else { self.actual_child(ordinal / 2) },
                    LiteralKind::Leaf(_) => match self.leaf() { Value::Decimal(_) | Value::Date(_) => self.at(Code::Part(ordinal)), _ => self.at(Code::Element(ordinal)) },
                    _ => self.actual_child(ordinal),
                },
            },
            Code::Pair(index) => self.actual_child(index * 2 + ordinal),
            Code::Part(_) => if matches!(self.leaf(), Value::Decimal(_)) { self.at(Code::Element(ordinal)) } else { self.at(Code::Inner) },
            _ => unreachable!("atom has no virtual child"),
        }
    }
    fn parent(self) -> Option<(Self, usize)> {
        Some(match self.code {
            Code::Node => {
                let parent = match self.storage { Some(storage) => storage.parent(self.anchor)?, None => if matches!(self.anchor, NodeRef::Raw(_)) { return None; } else { Parent { node: NodeRef::Raw(FormId(0)), ordinal: 0 } } };
                let owner = self.node(parent.node);
                match parent.node {
                    NodeRef::Raw(_) => match owner.raw() {
                        RawKind::List(_) | RawKind::Vector(_) | RawKind::Set(_) => (owner.at(Code::Payload), parent.ordinal),
                        RawKind::Map(_) => (owner.at(Code::Pair(parent.ordinal / 2)), parent.ordinal % 2),
                        RawKind::Tagged { .. } => (owner, 1), _ => (owner, 0),
                    },
                    NodeRef::Literal(_) => match owner.literal() {
                        LiteralKind::Map { .. } => (owner.at(Code::Pair(parent.ordinal / 2)), parent.ordinal % 2),
                        LiteralKind::Struct(_) => (owner.at(Code::Payload), parent.ordinal * 2 + 1),
                        _ => (owner.at(Code::Payload), parent.ordinal),
                    },
                }
            }
            Code::Payload => (self.at(Code::Node), 0),
            Code::Part(index) => {
                let nested = matches!(self.literal(), LiteralKind::Keyword(_)) || matches!(self.literal(), LiteralKind::Leaf(_)) && matches!(self.leaf(), Value::Decimal(_) | Value::Date(_));
                (self.at(if nested { Code::Payload } else { Code::Node }), index)
            }
            Code::Element(index) => (self.at(if matches!(self.leaf(), Value::Decimal(_)) { Code::Part(0) } else { Code::Payload }), index),
            Code::Pair(index) => (self.at(Code::Payload), index),
            Code::FieldKey(index) => (self.at(Code::Payload), index * 2),
            Code::Inner => (self.at(if matches!(self.leaf(), Value::Date(_)) { Code::Part(0) } else { Code::Part(2) }), 0),
        })
    }
}

fn leaf_shape(value: &Value) -> Shape<'static> {
    match value {
        Value::Undefined => Shape::Unit("Undefined"), Value::Null => Shape::Unit("Null"), Value::Bool(_) => tuple("Bool"),
        Value::I8(_) => tuple("I8"), Value::I16(_) => tuple("I16"), Value::I32(_) => tuple("I32"), Value::I64(_) => tuple("I64"),
        Value::U8(_) => tuple("U8"), Value::U16(_) => tuple("U16"), Value::U32(_) => tuple("U32"), Value::U64(_) => tuple("U64"),
        Value::F16(_) => tuple("F16"), Value::F32(_) => tuple("F32"), Value::F64(_) => tuple("F64"), Value::Binary(_) => tuple("Binary"),
        Value::FixedSizeBinary(_) => tuple("FixedSizeBinary"), Value::Decimal(_) => tuple("Decimal"), Value::Date(_) => tuple("Date"),
        Value::DateDay(_) => tuple("DateDay"), Value::DateMilli(_) => tuple("DateMilli"),
        Value::Time { .. } => structure("Time", &["value", "unit", "bit_width"]), Value::Timestamp { .. } => structure("Timestamp", &["value", "unit", "timezone"]),
        Value::Duration { .. } => structure("Duration", &["value", "unit"]), Value::IntervalYearMonth(_) => tuple("IntervalYearMonth"),
        Value::IntervalDayTime { .. } => structure("IntervalDayTime", &["days", "milliseconds"]),
        Value::IntervalMonthDayNano { .. } => structure("IntervalMonthDayNano", &["months", "days", "nanoseconds"]),
        Value::IntervalMonthDayMicro { .. } => structure("IntervalMonthDayMicro", &["months", "days", "nanoseconds"]),
        Value::Uuid(_) => tuple("Uuid"), Value::Uri(_) => tuple("Uri"), Value::Transit(_) => tuple("Transit"),
        Value::Oid(_) => tuple("Oid"), Value::RegClass(_) => tuple("RegClass"), Value::RegProc(_) => tuple("RegProc"),
        Value::TimestampTzRange { .. } => structure("TimestampTzRange", &["lower", "upper"]),
        Value::Utf8(_) | Value::Keyword(_) | Value::List(_) | Value::Set(_) | Value::Map { .. } | Value::FixedSizeList(_) | Value::Struct(_) => unreachable!("sealed leaf"),
    }
}
fn leaf_payload(value: &Value) -> Shape<'_> {
    Shape::Atom(match value {
        Value::Bool(v) => Atom::Bool(*v), Value::I8(v) => Atom::I8(*v), Value::I16(v) => Atom::I16(*v), Value::I32(v) => Atom::I32(*v), Value::I64(v) => Atom::I64(*v),
        Value::U8(v) => Atom::U8(*v), Value::U16(v) | Value::F16(v) => Atom::U16(*v), Value::U32(v) => Atom::U32(*v), Value::U64(v) => Atom::U64(*v),
        Value::F32(v) => Atom::F32(*v), Value::F64(v) => Atom::F64(*v), Value::DateDay(v) | Value::IntervalYearMonth(v) | Value::Oid(v) | Value::RegClass(v) | Value::RegProc(v) => Atom::I32(*v),
        Value::DateMilli(v) => Atom::I64(*v), Value::Uri(v) => Atom::Text(v),
        Value::Binary(v) | Value::FixedSizeBinary(v) | Value::Transit(v) => return list(v.len()), Value::Uuid(_) => return list(16),
        Value::Decimal(_) => return structure("DecimalValue", &["unscaled", "precision", "scale", "bit_width"]), Value::Date(_) => return structure("CalendarDate", &["date", "unit"]),
        _ => unreachable!("tuple leaf payload"),
    })
}
fn leaf_part(value: &Value, index: usize) -> Shape<'_> {
    Shape::Atom(match value {
        Value::Decimal(value) => match index { 0 => return list(value.unscaled.len()), 1 => Atom::I32(value.precision), 2 => Atom::I32(value.scale), _ => Atom::U16(value.bit_width) },
        Value::Date(value) => match index { 0 => return structure("CivilDate", &["epoch_day"]), _ => return Shape::Unit(match value.unit() { rtdb_types::DateUnit::Day => "Day", rtdb_types::DateUnit::Millisecond => "Millisecond" }) },
        Value::Time { value, unit: time_unit, bit_width } => match index { 0 => Atom::I64(*value), 1 => return Shape::Unit(unit(*time_unit)), _ => Atom::U8(*bit_width) },
        Value::Timestamp { value, unit: time_unit, timezone } => match index { 0 => Atom::I64(*value), 1 => return Shape::Unit(unit(*time_unit)), _ => return if timezone.is_some() { tuple("Some") } else { Shape::Unit("None") } },
        Value::Duration { value, unit: time_unit } => match index { 0 => Atom::I64(*value), _ => return Shape::Unit(unit(*time_unit)) },
        Value::IntervalDayTime { days, milliseconds } => Atom::I32(if index == 0 { *days } else { *milliseconds }),
        Value::IntervalMonthDayNano { months, days, nanoseconds } | Value::IntervalMonthDayMicro { months, days, nanoseconds } => match index { 0 => Atom::I32(*months), 1 => Atom::I32(*days), _ => Atom::I64(*nanoseconds) },
        Value::TimestampTzRange { lower, upper } => Atom::I64(if index == 0 { *lower } else { *upper }),
        _ => unreachable!("struct leaf field"),
    })
}

impl fmt::Debug for Atom<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self { Self::Text(v) => fmt::Debug::fmt(v, f), Self::Char(v) => fmt::Debug::fmt(v, f), Self::Bool(v) => fmt::Debug::fmt(v, f),
            Self::I8(v) => fmt::Debug::fmt(v, f), Self::I16(v) => fmt::Debug::fmt(v, f), Self::I32(v) => fmt::Debug::fmt(v, f), Self::I64(v) => fmt::Debug::fmt(v, f),
            Self::U8(v) => fmt::Debug::fmt(v, f), Self::U16(v) => fmt::Debug::fmt(v, f), Self::U32(v) => fmt::Debug::fmt(v, f), Self::U64(v) => fmt::Debug::fmt(v, f),
            Self::F32(v) => fmt::Debug::fmt(v, f), Self::F64(v) => fmt::Debug::fmt(v, f) }
    }
}
impl fmt::Debug for Virtual<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.shape() {
            Shape::Unit(name) => f.write_str(name), Shape::Atom(atom) => fmt::Debug::fmt(&atom, f),
            Shape::Children { kind: Compound::Tuple, name, len } => { let mut tuple = f.debug_tuple(name); for at in 0..len { tuple.field(&self.child(at)); } tuple.finish() }
            Shape::Children { kind: Compound::Struct(fields), name, .. } => { let mut object = f.debug_struct(name); for (at, field) in fields.iter().enumerate() { object.field(field, &self.child(at)); } object.finish() }
            Shape::Children { kind: Compound::List, len, .. } => { let mut list = f.debug_list(); for at in 0..len { list.entry(&self.child(at)); } list.finish() }
            Shape::Children { kind: Compound::Map, len, .. } => { let mut map = f.debug_map(); for at in (0..len).step_by(2) { map.entry(&self.child(at), &self.child(at + 1)); } map.finish() }
        }
    }
}

fn pad(f: &mut fmt::Formatter<'_>, depth: usize) -> fmt::Result { for _ in 0..depth { f.write_str("    ")?; } Ok(()) }
fn child_prefix(f: &mut fmt::Formatter<'_>, shape: Shape<'_>, ordinal: usize, depth: usize, pretty: bool) -> fmt::Result {
    let Shape::Children { kind, .. } = shape else { unreachable!("composite") };
    if matches!(kind, Compound::Map) && ordinal % 2 == 1 { return f.write_str(": "); }
    if pretty { pad(f, depth)?; } else if ordinal != 0 { f.write_str(", ")?; }
    if let Compound::Struct(fields) = kind { f.write_str(fields[ordinal])?; f.write_str(": ")?; }
    Ok(())
}
fn open(f: &mut fmt::Formatter<'_>, shape: Shape<'_>, pretty: bool) -> fmt::Result {
    let Shape::Children { kind, name, len } = shape else { unreachable!("composite") };
    f.write_str(name)?;
    f.write_str(match kind { Compound::Tuple => "(", Compound::Struct(_) => if pretty { " {" } else { " { " }, Compound::List => "[", Compound::Map => "{" })?;
    if pretty && len != 0 { f.write_str("\n")?; }
    Ok(())
}
fn close(f: &mut fmt::Formatter<'_>, shape: Shape<'_>, depth: usize, pretty: bool) -> fmt::Result {
    let Shape::Children { kind, len, .. } = shape else { unreachable!("composite") };
    if pretty && len != 0 { pad(f, depth)?; }
    f.write_str(match kind { Compound::Tuple => ")", Compound::Struct(_) => if pretty { "}" } else { " }" }, Compound::List => "]", Compound::Map => "}" })
}

pub(super) fn debug(value: &super::RegexForm, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut current = Virtual { storage: value.storage(), anchor: NodeRef::Raw(FormId(0)), code: Code::Node };
    if f.alternate() && f.fill() == '\n' { return fmt::Debug::fmt(&current, f); }
    let pretty = f.alternate();
    let mut depth = 0;
    let mut enter = true;
    loop {
        if enter {
            match current.shape() {
                Shape::Unit(name) => f.write_str(name)?, Shape::Atom(atom) => fmt::Debug::fmt(&atom, f)?,
                shape @ Shape::Children { len, .. } => {
                    open(f, shape, pretty)?;
                    if len != 0 { child_prefix(f, shape, 0, depth + 1, pretty)?; current = current.child(0); depth += 1; continue; }
                    close(f, shape, depth, pretty)?;
                }
            }
        }
        let Some((parent, ordinal)) = current.parent() else { return Ok(()); };
        depth -= 1;
        let shape = parent.shape();
        let key = matches!(shape, Shape::Children { kind: Compound::Map, .. }) && ordinal % 2 == 0;
        if pretty && !key { f.write_str(",\n")?; }
        if ordinal + 1 < shape.len() {
            child_prefix(f, shape, ordinal + 1, depth + 1, pretty)?;
            current = parent.child(ordinal + 1);
            depth += 1;
            enter = true;
        } else {
            close(f, shape, depth, pretty)?;
            current = parent;
            enter = false;
        }
    }
}
