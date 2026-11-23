use rtdb_types::StructFields;
use rtdb_types::StructFieldsIter;
use std::{iter::FusedIterator, ops::Range, slice};
use rtdb_types::Value;
use super::storage::{FormId, LiteralId, FormStorage, RawKind, LiteralKind};

#[cfg(test)]
#[path = "view_tests.rs"]
mod tests;

static NULL: Value = Value::Null;
#[derive(Clone, Copy)]
pub(super) enum RawSource<'a> { Null, Stored(&'a FormStorage, FormId) }

#[derive(Clone, Copy)]
pub struct RegexFormView<'a>(pub(super) RawSource<'a>);
#[derive(Clone, Copy)]
pub enum RegexFormKind<'a> {
    Literal(RegexLiteralView<'a>), Character(char), Symbol(&'a str), BigInteger(&'a str), Decimal(&'a str),
    List(RegexForms<'a>), Vector(RegexForms<'a>), Set(RegexForms<'a>), Map(RegexFormPairs<'a>),
    Tagged { tag: &'a str, value: RegexFormView<'a> }, AnonymousFunction(RegexFormView<'a>),
}
impl<'a> RegexFormView<'a> {
    /// Classifies one node without visiting, evaluating or copying children.
    pub fn kind(self) -> RegexFormKind<'a> {
        let RawSource::Stored(storage, id) = self.0 else { return RegexFormKind::Literal(RegexLiteralView::new(&NULL)); };
        match storage.forms[id.0].kind {
            RawKind::Literal(value) => RegexFormKind::Literal(RegexLiteralView(LiteralSource::Stored(storage, value))),
            RawKind::Character(value) => RegexFormKind::Character(value),
            RawKind::Symbol(text) => RegexFormKind::Symbol(&storage.texts[text.0]),
            RawKind::BigInteger(text) => RegexFormKind::BigInteger(&storage.texts[text.0]),
            RawKind::Decimal(text) => RegexFormKind::Decimal(&storage.texts[text.0]),
            RawKind::List(span) => RegexFormKind::List(RegexForms(storage, &storage.raw_children[span.range()])),
            RawKind::Vector(span) => RegexFormKind::Vector(RegexForms(storage, &storage.raw_children[span.range()])),
            RawKind::Set(span) => RegexFormKind::Set(RegexForms(storage, &storage.raw_children[span.range()])),
            RawKind::Map(span) => RegexFormKind::Map(RegexFormPairs(storage, &storage.raw_pairs[span.range()])),
            RawKind::Tagged { tag, value } => RegexFormKind::Tagged { tag: &storage.texts[tag.0], value: Self(RawSource::Stored(storage, value)) },
            RawKind::AnonymousFunction(value) => RegexFormKind::AnonymousFunction(Self(RawSource::Stored(storage, value))),
            RawKind::Draft => unreachable!("unpublished row"),
        }
    }
}

/// A borrowed ordered raw child sequence; no owning child handle is produced.
#[derive(Clone, Copy)]
pub struct RegexForms<'a>(&'a FormStorage, &'a [FormId]);
impl<'a> RegexForms<'a> {
    pub fn len(self) -> usize { self.1.len() }
    pub fn is_empty(self) -> bool { self.1.is_empty() }
    /// Returns None at or beyond the sequence length.
    pub fn get(self, index: usize) -> Option<RegexFormView<'a>> { self.1.get(index).map(|id| RegexFormView(RawSource::Stored(self.0, *id))) }
    /// Panics at or beyond the sequence length, like slice indexing.
    pub fn at(self, index: usize) -> RegexFormView<'a> { RegexFormView(RawSource::Stored(self.0, self.1[index])) }
    pub fn first(self) -> Option<RegexFormView<'a>> { self.get(0) }
    /// Borrows a half-open range; panics for reversed or out-of-bounds ranges.
    pub fn slice(self, range: Range<usize>) -> Self { Self(self.0, &self.1[range]) }
    pub fn split_first(self) -> Option<(RegexFormView<'a>, Self)> { self.first().map(|first| (first, self.slice(1..self.len()))) }
    pub fn iter(self) -> RegexFormIter<'a> { RegexFormIter { values: self, front: 0, back: self.len() } }
}
/// Raw map occurrences in original key/value order, including duplicates.
#[derive(Clone, Copy)]
pub struct RegexFormPairs<'a>(&'a FormStorage, &'a [(FormId, FormId)]);
impl<'a> RegexFormPairs<'a> {
    pub fn len(self) -> usize { self.1.len() }
    pub fn is_empty(self) -> bool { self.1.is_empty() }
    pub fn get(self, index: usize) -> Option<(RegexFormView<'a>, RegexFormView<'a>)> { self.1.get(index).map(|(key, value)| (RegexFormView(RawSource::Stored(self.0, *key)), RegexFormView(RawSource::Stored(self.0, *value)))) }
    pub fn iter(self) -> RegexFormPairIter<'a> { RegexFormPairIter { values: self, front: 0, back: self.len() } }
}

#[derive(Clone, Copy)]
enum LiteralSource<'a> { Legacy(&'a Value), Packed(rtdb_types::ListView<'a>, usize), Stored(&'a FormStorage, LiteralId) }
/// An immutable physical literal borrow from an external Value or canonical raw root.
#[derive(Clone, Copy)]
pub struct RegexLiteralView<'a>(LiteralSource<'a>);
#[derive(Clone, Copy)]
pub enum RegexLiteralKind<'a> {
    Text(&'a str), Keyword(&'a str), Leaf(RegexLeaf<'a>), List(RegexLiterals<'a>), Set(RegexLiterals<'a>),
    Map { entries: RegexLiteralPairs<'a>, sorted: bool }, FixedSizeList(RegexLiterals<'a>), Struct(RegexFields<'a>),
}
impl<'a> RegexLiteralView<'a> {
    /// Borrows external storage without admission or normalization.
    pub fn new(value: &'a Value) -> Self { Self(LiteralSource::Legacy(value)) }
    pub fn kind(self) -> RegexLiteralKind<'a> {
        match self.0 {
            LiteralSource::Legacy(value) => match value {
                Value::Utf8(value) => RegexLiteralKind::Text(value), Value::Keyword(value) => RegexLiteralKind::Keyword(value.as_str()),
                Value::List(values) => RegexLiteralKind::List(RegexLiterals(LiteralSequence::Fixed(values.view()))),
                Value::Set(values) => RegexLiteralKind::Set(RegexLiterals(LiteralSequence::Legacy(values))),
                Value::FixedSizeList(values) => RegexLiteralKind::FixedSizeList(RegexLiterals(LiteralSequence::Fixed(values.view()))),
                Value::Map { entries, sorted } => RegexLiteralKind::Map { entries: RegexLiteralPairs(LiteralPairs::Legacy(entries)), sorted: *sorted },
                Value::Struct(fields) => RegexLiteralKind::Struct(RegexFields(FieldSource::Legacy(fields))),
                value @ (Value::Undefined | Value::Null | Value::Bool(_) | Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_)
                    | Value::U8(_) | Value::U16(_) | Value::U32(_) | Value::U64(_) | Value::F16(_) | Value::F32(_) | Value::F64(_)
                    | Value::Binary(_) | Value::FixedSizeBinary(_) | Value::Decimal(_) | Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_)
                    | Value::Time { .. } | Value::Timestamp { .. } | Value::Duration { .. } | Value::IntervalYearMonth(_) | Value::IntervalDayTime { .. }
                    | Value::IntervalMonthDayNano { .. } | Value::IntervalMonthDayMicro { .. } | Value::Uuid(_) | Value::Uri(_) | Value::Transit(_)
                    | Value::Oid(_) | Value::RegClass(_) | Value::RegProc(_) | Value::TimestampTzRange { .. }) => RegexLiteralKind::Leaf(RegexLeaf(LeafSource::Borrowed(value))),
            },
            LiteralSource::Packed(values, index) => RegexLiteralKind::Leaf(RegexLeaf(LeafSource::Packed(values, index))),
            LiteralSource::Stored(storage, id) => match storage.literals[id.0].kind {
                LiteralKind::Text(text) => RegexLiteralKind::Text(&storage.texts[text.0]), LiteralKind::Keyword(text) => RegexLiteralKind::Keyword(&storage.texts[text.0]),
                LiteralKind::Leaf(leaf) => RegexLiteralKind::Leaf(RegexLeaf(LeafSource::Borrowed(storage.leaves[leaf.0].value()))),
                LiteralKind::List(span) => RegexLiteralKind::List(RegexLiterals(LiteralSequence::Stored(storage, &storage.literal_children[span.range()]))),
                LiteralKind::Set(span) => RegexLiteralKind::Set(RegexLiterals(LiteralSequence::Stored(storage, &storage.literal_children[span.range()]))),
                LiteralKind::FixedSizeList(span) => RegexLiteralKind::FixedSizeList(RegexLiterals(LiteralSequence::Stored(storage, &storage.literal_children[span.range()]))),
                LiteralKind::Map { entries, sorted } => RegexLiteralKind::Map { entries: RegexLiteralPairs(LiteralPairs::Stored(storage, &storage.literal_pairs[entries.range()])), sorted },
                LiteralKind::Struct(span) => RegexLiteralKind::Struct(RegexFields(FieldSource::Stored(storage, &storage.fields[span.range()]))),
                LiteralKind::Draft => unreachable!("unpublished literal"),
            },
        }
    }
}
#[derive(Clone, Copy)]
enum LeafSource<'a> { Borrowed(&'a Value), Packed(rtdb_types::ListView<'a>, usize) }

#[derive(Clone, Copy)]
pub struct RegexLeaf<'a>(LeafSource<'a>);

impl<'a> RegexLeaf<'a> {
    pub fn value(self) -> std::borrow::Cow<'a, Value> {
        match self.0 {
            LeafSource::Borrowed(value) => std::borrow::Cow::Borrowed(value),
            LeafSource::Packed(values, index) => values.get(index).expect("index in packed literal"),
        }
    }
}

#[derive(Clone, Copy)]
enum LiteralSequence<'a> { Legacy(&'a [Value]), Fixed(rtdb_types::ListView<'a>), Stored(&'a FormStorage, &'a [LiteralId]) }
/// A borrowed ordered physical sequence; no child Value is reconstructed.
#[derive(Clone, Copy)]
pub struct RegexLiterals<'a>(LiteralSequence<'a>);
impl<'a> RegexLiterals<'a> {
    pub fn len(self) -> usize { match self.0 { LiteralSequence::Legacy(values) => values.len(), LiteralSequence::Fixed(values) => values.len(), LiteralSequence::Stored(_, values) => values.len() } }
    pub fn is_empty(self) -> bool { self.len() == 0 }
    pub fn get(self, index: usize) -> Option<RegexLiteralView<'a>> {
        match self.0 {
            LiteralSequence::Legacy(values) | LiteralSequence::Fixed(rtdb_types::ListView::Values(values)) => values.get(index).map(RegexLiteralView::new),
            LiteralSequence::Fixed(values) => (index < values.len()).then_some(RegexLiteralView(LiteralSource::Packed(values, index))),
            LiteralSequence::Stored(storage, values) => values.get(index).map(|id| RegexLiteralView(LiteralSource::Stored(storage, *id))),
        }
    }
    pub fn iter(self) -> RegexLiteralIter<'a> { RegexLiteralIter { values: self, front: 0, back: self.len() } }
}
#[derive(Clone, Copy)]
enum LiteralPairs<'a> { Legacy(&'a [(Value, Value)]), Stored(&'a FormStorage, &'a [(LiteralId, LiteralId)]) }
/// Ordered physical map entries borrowing key and value.
#[derive(Clone, Copy)]
pub struct RegexLiteralPairs<'a>(LiteralPairs<'a>);
impl<'a> RegexLiteralPairs<'a> {
    pub fn len(self) -> usize { match self.0 { LiteralPairs::Legacy(values) => values.len(), LiteralPairs::Stored(_, values) => values.len() } }
    pub fn is_empty(self) -> bool { self.len() == 0 }
    pub fn get(self, index: usize) -> Option<(RegexLiteralView<'a>, RegexLiteralView<'a>)> {
        match self.0 {
            LiteralPairs::Legacy(values) => values.get(index).map(|(key, value)| (RegexLiteralView::new(key), RegexLiteralView::new(value))),
            LiteralPairs::Stored(storage, values) => values.get(index).map(|(key, value)| (RegexLiteralView(LiteralSource::Stored(storage, *key)), RegexLiteralView(LiteralSource::Stored(storage, *value)))),
        }
    }
    pub fn iter(self) -> RegexLiteralPairIter<'a> { RegexLiteralPairIter { values: self, front: 0, back: self.len() } }
}

macro_rules! cursor {
    ($name:ident, $source:ident, $item:ty) => {
        /// A borrowed cursor whose clones have independent positions.
        #[derive(Clone)]
        pub struct $name<'a> { values: $source<'a>, front: usize, back: usize }
        impl<'a> Iterator for $name<'a> {
            type Item = $item;
            fn next(&mut self) -> Option<Self::Item> { if self.front == self.back { None } else { let at = self.front; self.front += 1; self.values.get(at) } }
            fn size_hint(&self) -> (usize, Option<usize>) { let len = self.back - self.front; (len, Some(len)) }
        }
        impl DoubleEndedIterator for $name<'_> { fn next_back(&mut self) -> Option<Self::Item> { if self.front == self.back { None } else { self.back -= 1; self.values.get(self.back) } } }
        impl ExactSizeIterator for $name<'_> { fn len(&self) -> usize { self.back - self.front } }
        impl FusedIterator for $name<'_> {}
    };
}
cursor!(RegexFormIter, RegexForms, RegexFormView<'a>);
cursor!(RegexFormPairIter, RegexFormPairs, (RegexFormView<'a>, RegexFormView<'a>));
cursor!(RegexLiteralIter, RegexLiterals, RegexLiteralView<'a>);
cursor!(RegexLiteralPairIter, RegexLiteralPairs, (RegexLiteralView<'a>, RegexLiteralView<'a>));

#[derive(Clone, Copy)]
enum FieldSource<'a> { Legacy(&'a StructFields<Value>), Stored(&'a FormStorage, &'a [super::storage::FieldRow]) }
/// UTF8-sorted struct fields borrowed from their actual source.
#[derive(Clone, Copy)]
pub struct RegexFields<'a>(FieldSource<'a>);
impl<'a> RegexFields<'a> {
    pub fn len(self) -> usize { match self.0 { FieldSource::Legacy(values) => values.len(), FieldSource::Stored(_, values) => values.len() } }
    pub fn is_empty(self) -> bool { self.len() == 0 }
    pub fn iter(self) -> RegexFieldIter<'a> { RegexFieldIter(match self.0 { FieldSource::Legacy(values) => FieldCursor::Legacy(values.iter()), FieldSource::Stored(storage, values) => FieldCursor::Stored(storage, values.iter()) }) }
}
#[derive(Clone)]
enum FieldCursor<'a> { Legacy(StructFieldsIter<'a, Value>), Stored(&'a FormStorage, slice::Iter<'a, super::storage::FieldRow>) }
/// An independent sorted-field cursor retaining its original source lifetime.
#[derive(Clone)]
pub struct RegexFieldIter<'a>(FieldCursor<'a>);
impl<'a> Iterator for RegexFieldIter<'a> {
    type Item = (&'a str, RegexLiteralView<'a>);
    fn next(&mut self) -> Option<Self::Item> { match &mut self.0 { FieldCursor::Legacy(values) => values.next().map(|(key, value)| (key.as_str(), RegexLiteralView::new(value))), FieldCursor::Stored(storage, values) => values.next().map(|field| (storage.texts[field.name.0].as_str(), RegexLiteralView(LiteralSource::Stored(storage, field.value)))) } }
    fn size_hint(&self) -> (usize, Option<usize>) { match &self.0 { FieldCursor::Legacy(values) => values.size_hint(), FieldCursor::Stored(_, values) => values.size_hint() } }
}
impl DoubleEndedIterator for RegexFieldIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> { match &mut self.0 { FieldCursor::Legacy(values) => values.next_back().map(|(key, value)| (key.as_str(), RegexLiteralView::new(value))), FieldCursor::Stored(storage, values) => values.next_back().map(|field| (storage.texts[field.name.0].as_str(), RegexLiteralView(LiteralSource::Stored(storage, field.value)))) } }
}
impl ExactSizeIterator for RegexFieldIter<'_> { fn len(&self) -> usize { match &self.0 { FieldCursor::Legacy(values) => values.len(), FieldCursor::Stored(_, values) => values.len() } } }
impl FusedIterator for RegexFieldIter<'_> {}
