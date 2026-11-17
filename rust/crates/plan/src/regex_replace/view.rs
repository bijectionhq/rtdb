use std::{collections::{BTreeMap, btree_map}, iter::FusedIterator, ops::Range, slice};
use rtdb_types::Value;
use super::RegexForm;

#[cfg(test)]
#[path = "view_tests.rs"]
mod tests;

/// An internal raw form borrow for consumers in other crates.
/// Copies retain the original source lifetime and allocate no storage.
#[derive(Clone, Copy)]
pub struct RegexFormView<'a>(pub(super) &'a RegexForm);

/// Raw syntax distinctions with UTF8 text and children borrowed from the source.
#[derive(Clone, Copy)]
pub enum RegexFormKind<'a> {
    Literal(RegexLiteralView<'a>), Character(char), Symbol(&'a str), BigInteger(&'a str), Decimal(&'a str),
    List(RegexForms<'a>), Vector(RegexForms<'a>), Set(RegexForms<'a>), Map(RegexFormPairs<'a>),
    Tagged { tag: &'a str, value: RegexFormView<'a> }, AnonymousFunction(RegexFormView<'a>),
}

impl<'a> RegexFormView<'a> {
    /// Classifies one node without visiting, evaluating or copying its children.
    pub fn kind(self) -> RegexFormKind<'a> {
        match self.0 {
            RegexForm::Literal(value) => RegexFormKind::Literal(RegexLiteralView::new(value)),
            RegexForm::Character(value) => RegexFormKind::Character(*value),
            RegexForm::Symbol(value) => RegexFormKind::Symbol(value),
            RegexForm::BigInteger(value) => RegexFormKind::BigInteger(value),
            RegexForm::Decimal(value) => RegexFormKind::Decimal(value),
            RegexForm::List(values) => RegexFormKind::List(RegexForms(values)),
            RegexForm::Vector(values) => RegexFormKind::Vector(RegexForms(values)),
            RegexForm::Set(values) => RegexFormKind::Set(RegexForms(values)),
            RegexForm::Map(entries) => RegexFormKind::Map(RegexFormPairs(entries)),
            RegexForm::Tagged { tag, value } => RegexFormKind::Tagged { tag, value: value.view() },
            RegexForm::AnonymousFunction(value) => RegexFormKind::AnonymousFunction(value.view()),
        }
    }
}

/// A sequence borrowing its source, indexed by child position, without allocation.
#[derive(Clone, Copy)]
pub struct RegexForms<'a>(&'a [RegexForm]);
impl<'a> RegexForms<'a> {
    pub fn len(self) -> usize { self.0.len() }
    pub fn is_empty(self) -> bool { self.0.is_empty() }
    /// Returns `None` when `index >= len()`.
    pub fn get(self, index: usize) -> Option<RegexFormView<'a>> { self.0.get(index).map(RegexForm::view) }
    /// Panics when `index >= len()`, like slice indexing.
    pub fn at(self, index: usize) -> RegexFormView<'a> { self.0[index].view() }
    /// Returns `None` for an empty sequence.
    pub fn first(self) -> Option<RegexFormView<'a>> { self.0.first().map(RegexForm::view) }
    /// Borrows a half-open child range; panics for reversed or out-of-bounds ranges.
    pub fn slice(self, range: Range<usize>) -> Self { Self(&self.0[range]) }
    /// Returns `None` for an empty sequence; both results retain the source lifetime.
    pub fn split_first(self) -> Option<(RegexFormView<'a>, Self)> { self.0.split_first().map(|(first, rest)| (first.view(), Self(rest))) }
    /// Iterates borrowed children in source order without allocating.
    pub fn iter(self) -> RegexFormIter<'a> { RegexFormIter(self.0.iter()) }
}

/// A cursor borrowing its source; clones have independent positions.
#[derive(Clone)]
pub struct RegexFormIter<'a>(slice::Iter<'a, RegexForm>);
impl<'a> Iterator for RegexFormIter<'a> {
    type Item = RegexFormView<'a>;
    fn next(&mut self) -> Option<Self::Item> { self.0.next().map(RegexForm::view) }
    fn size_hint(&self) -> (usize, Option<usize>) { self.0.size_hint() }
}
impl DoubleEndedIterator for RegexFormIter<'_> { fn next_back(&mut self) -> Option<Self::Item> { self.0.next_back().map(RegexForm::view) } }
impl ExactSizeIterator for RegexFormIter<'_> { fn len(&self) -> usize { self.0.len() } }
impl FusedIterator for RegexFormIter<'_> {}

/// Ordered raw map entries; keys and values retain the source lifetime.
#[derive(Clone, Copy)]
pub struct RegexFormPairs<'a>(&'a [(RegexForm, RegexForm)]);
impl<'a> RegexFormPairs<'a> {
    pub fn len(self) -> usize { self.0.len() }
    pub fn is_empty(self) -> bool { self.0.is_empty() }
    /// Returns `None` when `index >= len()`.
    pub fn get(self, index: usize) -> Option<(RegexFormView<'a>, RegexFormView<'a>)> { self.0.get(index).map(|(key, value)| (key.view(), value.view())) }
    /// Iterates entries in source order without allocating.
    pub fn iter(self) -> RegexFormPairIter<'a> { RegexFormPairIter(self.0.iter()) }
}

/// A borrowed raw map cursor whose clones have independent positions.
#[derive(Clone)]
pub struct RegexFormPairIter<'a>(slice::Iter<'a, (RegexForm, RegexForm)>);
impl<'a> Iterator for RegexFormPairIter<'a> {
    type Item = (RegexFormView<'a>, RegexFormView<'a>);
    fn next(&mut self) -> Option<Self::Item> { self.0.next().map(|(key, value)| (key.view(), value.view())) }
    fn size_hint(&self) -> (usize, Option<usize>) { self.0.size_hint() }
}
impl DoubleEndedIterator for RegexFormPairIter<'_> { fn next_back(&mut self) -> Option<Self::Item> { self.0.next_back().map(|(key, value)| (key.view(), value.view())) } }
impl ExactSizeIterator for RegexFormPairIter<'_> { fn len(&self) -> usize { self.0.len() } }
impl FusedIterator for RegexFormPairIter<'_> {}

/// An immutable physical literal borrow; constructing a view performs no admission.
#[derive(Clone, Copy)]
pub struct RegexLiteralView<'a>(&'a Value);

/// Physical literal structure with UTF8 strings and keyword names kept distinct.
#[derive(Clone, Copy)]
pub enum RegexLiteralKind<'a> {
    Text(&'a str), Keyword(&'a str), Leaf(RegexLeaf<'a>),
    List(RegexLiterals<'a>), Set(RegexLiterals<'a>), Map { entries: RegexLiteralPairs<'a>, sorted: bool },
    FixedSizeList(RegexLiterals<'a>), Struct(RegexFields<'a>),
}

impl<'a> RegexLiteralView<'a> {
    /// Borrows an existing physical value without copying or validating it.
    pub fn new(value: &'a Value) -> Self { Self(value) }
    /// Classifies one value without visiting its children or normalizing its payload.
    pub fn kind(self) -> RegexLiteralKind<'a> {
        match self.0 {
            Value::Utf8(value) => RegexLiteralKind::Text(value),
            Value::Keyword(value) => RegexLiteralKind::Keyword(value.as_str()),
            Value::List(values) => RegexLiteralKind::List(RegexLiterals(values)),
            Value::Set(values) => RegexLiteralKind::Set(RegexLiterals(values)),
            Value::Map { entries, sorted } => RegexLiteralKind::Map { entries: RegexLiteralPairs(entries), sorted: *sorted },
            Value::FixedSizeList(values) => RegexLiteralKind::FixedSizeList(RegexLiterals(values)),
            Value::Struct(fields) => RegexLiteralKind::Struct(RegexFields(fields)),
            value @ (Value::Undefined | Value::Null | Value::Bool(_) | Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_)
            | Value::U8(_) | Value::U16(_) | Value::U32(_) | Value::U64(_) | Value::F16(_) | Value::F32(_) | Value::F64(_)
            | Value::Binary(_) | Value::FixedSizeBinary(_) | Value::Decimal(_) | Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_)
            | Value::Time { .. } | Value::Timestamp { .. } | Value::Duration { .. } | Value::IntervalYearMonth(_) | Value::IntervalDayTime { .. }
            | Value::IntervalMonthDayNano { .. } | Value::IntervalMonthDayMicro { .. } | Value::Uuid(_) | Value::Uri(_) | Value::Transit(_)
            | Value::Oid(_) | Value::RegClass(_) | Value::RegProc(_) | Value::TimestampTzRange { .. }) => RegexLiteralKind::Leaf(RegexLeaf(value)),
        }
    }
}

/// A sealed borrow of one of the 34 nonrecursive physical leaves.
/// UTF8 strings and keywords are excluded; buffers and metadata are borrowed unchanged.
#[derive(Clone, Copy)]
pub struct RegexLeaf<'a>(&'a Value);
impl<'a> RegexLeaf<'a> {
    pub fn value(self) -> &'a Value { self.0 }
}

/// A sequence borrowing physical literal children without allocation.
#[derive(Clone, Copy)]
pub struct RegexLiterals<'a>(&'a [Value]);
impl<'a> RegexLiterals<'a> {
    pub fn len(self) -> usize { self.0.len() }
    pub fn is_empty(self) -> bool { self.0.is_empty() }
    /// Returns `None` when `index >= len()`.
    pub fn get(self, index: usize) -> Option<RegexLiteralView<'a>> { self.0.get(index).map(RegexLiteralView::new) }
    /// Iterates children borrowed from the source without allocating.
    pub fn iter(self) -> RegexLiteralIter<'a> { RegexLiteralIter(self.0.iter()) }
}

/// A borrowed physical sequence cursor whose clones have independent positions.
#[derive(Clone)]
pub struct RegexLiteralIter<'a>(slice::Iter<'a, Value>);
impl<'a> Iterator for RegexLiteralIter<'a> {
    type Item = RegexLiteralView<'a>;
    fn next(&mut self) -> Option<Self::Item> { self.0.next().map(RegexLiteralView::new) }
    fn size_hint(&self) -> (usize, Option<usize>) { self.0.size_hint() }
}
impl DoubleEndedIterator for RegexLiteralIter<'_> { fn next_back(&mut self) -> Option<Self::Item> { self.0.next_back().map(RegexLiteralView::new) } }
impl ExactSizeIterator for RegexLiteralIter<'_> { fn len(&self) -> usize { self.0.len() } }
impl FusedIterator for RegexLiteralIter<'_> {}

/// Ordered physical map entries; no key or value is copied.
#[derive(Clone, Copy)]
pub struct RegexLiteralPairs<'a>(&'a [(Value, Value)]);
impl<'a> RegexLiteralPairs<'a> {
    pub fn len(self) -> usize { self.0.len() }
    pub fn is_empty(self) -> bool { self.0.is_empty() }
    /// Returns `None` when `index >= len()`.
    pub fn get(self, index: usize) -> Option<(RegexLiteralView<'a>, RegexLiteralView<'a>)> { self.0.get(index).map(|(key, value)| (RegexLiteralView::new(key), RegexLiteralView::new(value))) }
    /// Iterates entries borrowed from the source without allocating.
    pub fn iter(self) -> RegexLiteralPairIter<'a> { RegexLiteralPairIter(self.0.iter()) }
}

/// A borrowed physical map cursor whose clones have independent positions.
#[derive(Clone)]
pub struct RegexLiteralPairIter<'a>(slice::Iter<'a, (Value, Value)>);
impl<'a> Iterator for RegexLiteralPairIter<'a> {
    type Item = (RegexLiteralView<'a>, RegexLiteralView<'a>);
    fn next(&mut self) -> Option<Self::Item> { self.0.next().map(|(key, value)| (RegexLiteralView::new(key), RegexLiteralView::new(value))) }
    fn size_hint(&self) -> (usize, Option<usize>) { self.0.size_hint() }
}
impl DoubleEndedIterator for RegexLiteralPairIter<'_> { fn next_back(&mut self) -> Option<Self::Item> { self.0.next_back().map(|(key, value)| (RegexLiteralView::new(key), RegexLiteralView::new(value))) } }
impl ExactSizeIterator for RegexLiteralPairIter<'_> { fn len(&self) -> usize { self.0.len() } }
impl FusedIterator for RegexLiteralPairIter<'_> {}

/// Borrowed struct fields in the physical BTreeMap's key order.
#[derive(Clone, Copy)]
pub struct RegexFields<'a>(&'a BTreeMap<String, Value>);
impl<'a> RegexFields<'a> {
    pub fn len(self) -> usize { self.0.len() }
    pub fn is_empty(self) -> bool { self.0.is_empty() }
    /// Borrows field names and values without collecting keys or children.
    pub fn iter(self) -> RegexFieldIter<'a> { RegexFieldIter(self.0.iter()) }
}

/// An inline borrowed field cursor whose clones have independent positions.
#[derive(Clone)]
pub struct RegexFieldIter<'a>(btree_map::Iter<'a, String, Value>);
impl<'a> Iterator for RegexFieldIter<'a> {
    type Item = (&'a str, RegexLiteralView<'a>);
    fn next(&mut self) -> Option<Self::Item> { self.0.next().map(|(name, value)| (name.as_str(), RegexLiteralView::new(value))) }
    fn size_hint(&self) -> (usize, Option<usize>) { self.0.size_hint() }
}
impl DoubleEndedIterator for RegexFieldIter<'_> { fn next_back(&mut self) -> Option<Self::Item> { self.0.next_back().map(|(name, value)| (name.as_str(), RegexLiteralView::new(value))) } }
impl ExactSizeIterator for RegexFieldIter<'_> { fn len(&self) -> usize { self.0.len() } }
impl FusedIterator for RegexFieldIter<'_> {}
