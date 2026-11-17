use std::{alloc::Layout, collections::btree_map};
use rtdb_types::{AllocationScope, Value, java_text::JavaText};
use crate::{RegexForm, RegexReplacement};
use super::{BoundedSink, CountingSink, JsonDocument, JsonSink, JsonWriteFailure, WriteResult, leaf, text};

#[cfg(test)]
#[path = "recipe_tests.rs"]
mod tests;

enum Frame<'source> {
    Recipe { recipe: &'source RegexReplacement, next: usize },
    Form(&'source RegexForm),
    Value(&'source Value),
    Forms { values: &'source [RegexForm], next: usize },
    FormPairs { entries: &'source [(RegexForm, RegexForm)], next: usize, phase: PairPhase },
    Values { values: &'source [Value], next: usize },
    ValuePairs { entries: &'source [(Value, Value)], next: usize, phase: PairPhase, sorted: bool },
    Fields { entries: btree_map::Iter<'source, String, Value>, first: bool },
    Close(&'static [u8]),
}

#[derive(Clone, Copy)]
enum PairPhase { Key, Value, End }

#[derive(Clone, Copy)]
enum Growth { Admit, Reuse }

struct Traversal<'source, 'scope> { frames: Vec<Frame<'source>>, limit: usize, allocation: &'scope AllocationScope }
impl<'source, 'scope> Traversal<'source, 'scope> {
    fn new(allocation: &'scope AllocationScope) -> Self { Self { frames: Vec::new(), limit: 0, allocation } }

    fn push(&mut self, frame: Frame<'source>, growth: Growth) -> WriteResult<()> {
        if self.frames.len() == self.limit {
            if matches!(growth, Growth::Reuse) { return Err(JsonWriteFailure::Internal); }
            let capacity = self.limit.checked_mul(2).ok_or(JsonWriteFailure::Capacity)?.max(16);
            let bytes = Layout::array::<Frame<'source>>(capacity).map_err(|_| JsonWriteFailure::Capacity)?.size();
            self.allocation.reserve(bytes)?;
            let mut replacement = Vec::with_capacity(capacity);
            replacement.append(&mut self.frames);
            self.frames = replacement;
            self.limit = capacity;
        }
        self.frames.push(frame);
        Ok(())
    }

    fn write(&mut self, recipe: &'source RegexReplacement, sink: &mut impl JsonSink, growth: Growth) -> WriteResult<()> {
        self.frames.clear();
        sink.append(b"{")?;
        self.push(Frame::Recipe { recipe, next: 0 }, growth)?;
        while let Some(frame) = self.frames.pop() {
            match frame {
                Frame::Recipe { recipe, next } => {
                    let (name, value) = match next {
                        0 => (b"\"pattern\":".as_slice(), &recipe.pattern),
                        1 => (b"\"replacement\":".as_slice(), &recipe.replacement),
                        2 => (b"\"start\":".as_slice(), &recipe.start),
                        3 => (b"\"occurrence\":".as_slice(), &recipe.occurrence),
                        4 => (b"\"flags\":".as_slice(), &recipe.flags),
                        _ => { sink.append(b"}")?; continue; }
                    };
                    if next != 0 { sink.append(b",")?; }
                    sink.append(name)?;
                    self.push(Frame::Recipe { recipe, next: next + 1 }, growth)?;
                    self.push(Frame::Form(value), growth)?;
                }
                Frame::Form(form) => match form {
                    RegexForm::Literal(value) => {
                        sink.append(b"{\"Literal\":")?;
                        self.push(Frame::Close(b"}"), growth)?;
                        self.push(Frame::Value(value), growth)?;
                    }
                    RegexForm::Character(character) => {
                        sink.append(b"{\"Character\":")?;
                        let mut units = [0; 2];
                        text::string(JavaText::from_utf16(character.encode_utf16(&mut units)), sink)?;
                        sink.append(b"}")?;
                    }
                    RegexForm::Symbol(value) | RegexForm::BigInteger(value) | RegexForm::Decimal(value) => {
                        sink.append(match form { RegexForm::Symbol(_) => b"{\"Symbol\":".as_slice(), RegexForm::BigInteger(_) => b"{\"BigInteger\":".as_slice(), _ => b"{\"Decimal\":".as_slice() })?;
                        text::string(JavaText::from_utf8(value), sink)?;
                        sink.append(b"}")?;
                    }
                    RegexForm::List(values) | RegexForm::Vector(values) | RegexForm::Set(values) => {
                        sink.append(match form { RegexForm::List(_) => b"{\"List\":[".as_slice(), RegexForm::Vector(_) => b"{\"Vector\":[".as_slice(), _ => b"{\"Set\":[".as_slice() })?;
                        self.push(Frame::Forms { values, next: 0 }, growth)?;
                    }
                    RegexForm::Map(entries) => {
                        sink.append(b"{\"Map\":[")?;
                        self.push(Frame::FormPairs { entries, next: 0, phase: PairPhase::Key }, growth)?;
                    }
                    RegexForm::Tagged { tag, value } => {
                        sink.append(b"{\"Tagged\":{\"tag\":")?;
                        text::string(JavaText::from_utf8(tag), sink)?;
                        sink.append(b",\"value\":")?;
                        self.push(Frame::Close(b"}}"), growth)?;
                        self.push(Frame::Form(value), growth)?;
                    }
                    RegexForm::AnonymousFunction(value) => {
                        sink.append(b"{\"AnonymousFunction\":")?;
                        self.push(Frame::Close(b"}"), growth)?;
                        self.push(Frame::Form(value), growth)?;
                    }
                },
                Frame::Value(value) => match value {
                    Value::List(values) | Value::Set(values) | Value::FixedSizeList(values) => {
                        sink.append(match value { Value::List(_) => b"{\"List\":[".as_slice(), Value::Set(_) => b"{\"Set\":[".as_slice(), _ => b"{\"FixedSizeList\":[".as_slice() })?;
                        self.push(Frame::Values { values, next: 0 }, growth)?;
                    }
                    Value::Map { entries, sorted } => {
                        sink.append(b"{\"Map\":{\"entries\":[")?;
                        self.push(Frame::ValuePairs { entries, next: 0, phase: PairPhase::Key, sorted: *sorted }, growth)?;
                    }
                    Value::Struct(fields) => {
                        sink.append(b"{\"Struct\":{")?;
                        self.push(Frame::Fields { entries: fields.iter(), first: true }, growth)?;
                    }
                    Value::Keyword(keyword) => {
                        sink.append(b"{\"Keyword\":")?;
                        text::string(JavaText::from_utf8(keyword.as_str()), sink)?;
                        sink.append(b"}")?;
                    }
                    scalar @ (Value::Undefined | Value::Null | Value::Bool(_) | Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_)
                    | Value::U8(_) | Value::U16(_) | Value::U32(_) | Value::U64(_) | Value::F16(_) | Value::F32(_) | Value::F64(_)
                    | Value::Utf8(_) | Value::Binary(_) | Value::FixedSizeBinary(_) | Value::Decimal(_) | Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_)
                    | Value::Time { .. } | Value::Timestamp { .. } | Value::Duration { .. } | Value::IntervalYearMonth(_) | Value::IntervalDayTime { .. }
                    | Value::IntervalMonthDayNano { .. } | Value::IntervalMonthDayMicro { .. } | Value::Uuid(_) | Value::Uri(_) | Value::Transit(_)
                    | Value::Oid(_) | Value::RegClass(_) | Value::RegProc(_) | Value::TimestampTzRange { .. }) => leaf::serialize(leaf::LeafValue::new(scalar).ok_or(JsonWriteFailure::Internal)?, sink)?,
                },
                Frame::Forms { values, next } => {
                    if let Some(value) = values.get(next) {
                        if next != 0 { sink.append(b",")?; }
                        self.push(Frame::Forms { values, next: next + 1 }, growth)?;
                        self.push(Frame::Form(value), growth)?;
                    } else { sink.append(b"]}")?; }
                }
                Frame::FormPairs { entries, next, phase } => {
                    if let Some((key, value)) = entries.get(next) {
                        match phase {
                            PairPhase::Key => {
                                if next != 0 { sink.append(b",")?; }
                                sink.append(b"[")?;
                                self.push(Frame::FormPairs { entries, next, phase: PairPhase::Value }, growth)?;
                                self.push(Frame::Form(key), growth)?;
                            }
                            PairPhase::Value => {
                                sink.append(b",")?;
                                self.push(Frame::FormPairs { entries, next, phase: PairPhase::End }, growth)?;
                                self.push(Frame::Form(value), growth)?;
                            }
                            PairPhase::End => { sink.append(b"]")?; self.push(Frame::FormPairs { entries, next: next + 1, phase: PairPhase::Key }, growth)?; }
                        }
                    } else { sink.append(b"]}")?; }
                }
                Frame::Values { values, next } => {
                    if let Some(value) = values.get(next) {
                        if next != 0 { sink.append(b",")?; }
                        self.push(Frame::Values { values, next: next + 1 }, growth)?;
                        self.push(Frame::Value(value), growth)?;
                    } else { sink.append(b"]}")?; }
                }
                Frame::ValuePairs { entries, next, phase, sorted } => {
                    if let Some((key, value)) = entries.get(next) {
                        match phase {
                            PairPhase::Key => {
                                if next != 0 { sink.append(b",")?; }
                                sink.append(b"[")?;
                                self.push(Frame::ValuePairs { entries, next, phase: PairPhase::Value, sorted }, growth)?;
                                self.push(Frame::Value(key), growth)?;
                            }
                            PairPhase::Value => {
                                sink.append(b",")?;
                                self.push(Frame::ValuePairs { entries, next, phase: PairPhase::End, sorted }, growth)?;
                                self.push(Frame::Value(value), growth)?;
                            }
                            PairPhase::End => { sink.append(b"]")?; self.push(Frame::ValuePairs { entries, next: next + 1, phase: PairPhase::Key, sorted }, growth)?; }
                        }
                    } else { sink.append(if sorted { b"],\"sorted\":true}}" } else { b"],\"sorted\":false}}" })?; }
                }
                Frame::Fields { mut entries, first } => {
                    if let Some((name, value)) = entries.next() {
                        if !first { sink.append(b",")?; }
                        text::string(JavaText::from_utf8(name), sink)?;
                        sink.append(b":")?;
                        self.push(Frame::Fields { entries, first: false }, growth)?;
                        self.push(Frame::Value(value), growth)?;
                    } else { sink.append(b"}}")?; }
                }
                Frame::Close(bytes) => sink.append(bytes)?,
            }
        }
        Ok(())
    }
}
// Borrowed continuations cannot outlive the scope that owns their capacity charges.
impl Drop for Traversal<'_, '_> { fn drop(&mut self) { let _ = self.allocation; } }

fn encode(recipe: &RegexReplacement, allocation: &AllocationScope) -> WriteResult<JsonDocument> {
    let mut traversal = Traversal::new(allocation);
    let mut count = CountingSink::default();
    traversal.write(recipe, &mut count, Growth::Admit)?;
    let mut output = BoundedSink::new(count.length, allocation)?;
    traversal.write(recipe, &mut output, Growth::Reuse)?;
    output.finish()
}
