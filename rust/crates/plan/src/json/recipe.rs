use std::alloc::Layout;
use rtdb_types::{AllocationScope, java_text::JavaText};
use crate::{RegexReplacement, RegexFormView, RegexFormKind as FormKind, RegexForms, RegexFormPairs,
    RegexLiteralView, RegexLiteralKind as LiteralKind, RegexLiterals, RegexLiteralPairs, RegexFieldIter};
use super::{BoundedSink, CountingSink, JsonDocument, JsonSink, JsonWriteFailure, WriteResult, leaf, text};

#[cfg(test)]
#[path = "recipe_tests.rs"]
mod tests;

enum Frame<'source> {
    Recipe { recipe: &'source RegexReplacement, next: usize },
    Form(RegexFormView<'source>),
    Value(RegexLiteralView<'source>),
    Forms { values: RegexForms<'source>, next: usize },
    FormPairs { entries: RegexFormPairs<'source>, next: usize, phase: PairPhase },
    Values { values: RegexLiterals<'source>, next: usize },
    ValuePairs { entries: RegexLiteralPairs<'source>, next: usize, phase: PairPhase, sorted: bool },
    Fields { entries: RegexFieldIter<'source>, first: bool },
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
                        0 => (b"\"pattern\":".as_slice(), recipe.pattern.view()),
                        1 => (b"\"replacement\":".as_slice(), recipe.replacement.view()),
                        2 => (b"\"start\":".as_slice(), recipe.start.view()),
                        3 => (b"\"occurrence\":".as_slice(), recipe.occurrence.view()),
                        4 => (b"\"flags\":".as_slice(), recipe.flags.view()),
                        _ => { sink.append(b"}")?; continue; }
                    };
                    if next != 0 { sink.append(b",")?; }
                    sink.append(name)?;
                    self.push(Frame::Recipe { recipe, next: next + 1 }, growth)?;
                    self.push(Frame::Form(value), growth)?;
                }
                Frame::Form(form) => match form.kind() {
                    FormKind::Literal(value) => {
                        sink.append(b"{\"Literal\":")?;
                        self.push(Frame::Close(b"}"), growth)?;
                        self.push(Frame::Value(value), growth)?;
                    }
                    FormKind::Character(character) => {
                        sink.append(b"{\"Character\":")?;
                        let mut units = [0; 2];
                        text::string(JavaText::from_utf16(character.encode_utf16(&mut units)), sink)?;
                        sink.append(b"}")?;
                    }
                    FormKind::Symbol(value) | FormKind::BigInteger(value) | FormKind::Decimal(value) => {
                        sink.append(match form.kind() { FormKind::Symbol(_) => b"{\"Symbol\":".as_slice(), FormKind::BigInteger(_) => b"{\"BigInteger\":".as_slice(), _ => b"{\"Decimal\":".as_slice() })?;
                        text::string(JavaText::from_utf8(value), sink)?;
                        sink.append(b"}")?;
                    }
                    FormKind::List(values) | FormKind::Vector(values) | FormKind::Set(values) => {
                        sink.append(match form.kind() { FormKind::List(_) => b"{\"List\":[".as_slice(), FormKind::Vector(_) => b"{\"Vector\":[".as_slice(), _ => b"{\"Set\":[".as_slice() })?;
                        self.push(Frame::Forms { values, next: 0 }, growth)?;
                    }
                    FormKind::Map(entries) => {
                        sink.append(b"{\"Map\":[")?;
                        self.push(Frame::FormPairs { entries, next: 0, phase: PairPhase::Key }, growth)?;
                    }
                    FormKind::Tagged { tag, value } => {
                        sink.append(b"{\"Tagged\":{\"tag\":")?;
                        text::string(JavaText::from_utf8(tag), sink)?;
                        sink.append(b",\"value\":")?;
                        self.push(Frame::Close(b"}}"), growth)?;
                        self.push(Frame::Form(value), growth)?;
                    }
                    FormKind::AnonymousFunction(value) => {
                        sink.append(b"{\"AnonymousFunction\":")?;
                        self.push(Frame::Close(b"}"), growth)?;
                        self.push(Frame::Form(value), growth)?;
                    }
                },
                Frame::Value(value) => match value.kind() {
                    LiteralKind::List(values) | LiteralKind::Set(values) | LiteralKind::FixedSizeList(values) => {
                        sink.append(match value.kind() { LiteralKind::List(_) => b"{\"List\":[".as_slice(), LiteralKind::Set(_) => b"{\"Set\":[".as_slice(), _ => b"{\"FixedSizeList\":[".as_slice() })?;
                        self.push(Frame::Values { values, next: 0 }, growth)?;
                    }
                    LiteralKind::Map { entries, sorted } => {
                        sink.append(b"{\"Map\":{\"entries\":[")?;
                        self.push(Frame::ValuePairs { entries, next: 0, phase: PairPhase::Key, sorted }, growth)?;
                    }
                    LiteralKind::Struct(fields) => {
                        sink.append(b"{\"Struct\":{")?;
                        self.push(Frame::Fields { entries: fields.iter(), first: true }, growth)?;
                    }
                    LiteralKind::Text(text_value) => {
                        sink.append(b"{\"Utf8\":")?;
                        text::string(JavaText::from_utf8(text_value), sink)?;
                        sink.append(b"}")?;
                    }
                    LiteralKind::Keyword(keyword) => {
                        sink.append(b"{\"Keyword\":")?;
                        text::string(JavaText::from_utf8(keyword), sink)?;
                        sink.append(b"}")?;
                    }
                    LiteralKind::Leaf(proved) => leaf::serialize(leaf::LeafValue::new(proved.value()).ok_or(JsonWriteFailure::Internal)?, sink)?,
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
