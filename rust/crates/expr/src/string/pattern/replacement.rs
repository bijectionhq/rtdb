use std::alloc::Layout;
use rtdb_types::{Anomaly, Result, Value, java_text::{JavaString, JavaText}};
use crate::{EvalContext, numeric::overflow};
use super::{Pattern, compile_text, regex_error, source::work_bytes, failure::{CompileFailure, RetainedAnomaly, retain_text, retain_units}};
use super::super::{add, check_size, illegal};

#[cfg(test)] mod tests;

pub(crate) struct CompiledReplacement { pattern: Pattern, replacement: JavaString }

#[derive(Debug)]
pub(crate) enum ReplacementFailure { Java(JavaReplacementFailure), Native(NativeReplacementFailure), Resource(Anomaly) }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplacementExceptionKind { IllegalArgument, IndexOutOfBounds }

#[derive(Debug)]
pub(crate) struct JavaReplacementFailure { kind: ReplacementExceptionKind, message: JavaString, replacement: JavaString }

#[derive(Debug)]
pub(crate) struct NativeReplacementFailure { replacement: JavaString, error: RetainedAnomaly }

enum ExpansionFailure<'a> { Java(ReplacementExceptionKind, Message<'a>), Native(Anomaly) }
enum Message<'a> { Static(&'static str), Named { prefix: &'static str, name: JavaText<'a>, suffix: &'static str }, Group(usize) }
enum Part { Unit(u16), Group(usize) }
type ExpansionResult<'a> = std::result::Result<(), ExpansionFailure<'a>>;

pub(crate) fn compile_replacement(pattern: &str, replacement: &str, flags: &str, context: &EvalContext<'_>) -> Result<CompiledReplacement> {
    let replacement = postgres_backrefs(JavaText::from_utf8(replacement), context)?;
    Ok(CompiledReplacement { pattern: super::compile(pattern, flags, context)?, replacement })
}

pub(crate) fn compile_replacement_pattern(pattern: JavaText<'_>, replacement: &str, flags: &str, context: &EvalContext<'_>) -> Result<CompiledReplacement> {
    compile_replacement_text(pattern, JavaText::from_utf8(replacement), JavaText::from_utf8(flags), context).map_err(CompileFailure::legacy)
}

pub(crate) fn compile_replacement_text(pattern: JavaText<'_>, replacement: JavaText<'_>, flags: JavaText<'_>, context: &EvalContext<'_>) -> std::result::Result<CompiledReplacement, CompileFailure> {
    let replacement = postgres_backrefs(replacement, context).map_err(CompileFailure::Resource)?;
    Ok(CompiledReplacement { pattern: compile_text(pattern, flags, context)?, replacement })
}

impl CompiledReplacement {
    pub(crate) fn replace(&self, source: &str, context: &EvalContext<'_>) -> Result<Value> {
        let output = self.replace_text(JavaText::from_utf8(source), context).map_err(|failure| failure.legacy(context))?;
        utf8(output.view(), context).map(Value::Utf8)
    }

    pub(crate) fn replace_text(&self, source: JavaText<'_>, context: &EvalContext<'_>) -> std::result::Result<JavaString, ReplacementFailure> {
        let retain = |error| self.retain(ExpansionFailure::Native(error), context);
        unit_bytes(source.len_units()).map_err(&retain)?;
        let bytes = work_bytes(source).map_err(&retain)?.checked_mul(32).and_then(|bytes| bytes.checked_add(8192)).ok_or_else(overflow).map_err(&retain)?;
        context.reserve(bytes).map_err(&retain)?;
        let mut units = Vec::with_capacity(source.len_units());
        units.extend(source.units());
        let mut length = 0usize;
        self.pass(&units, |part| { length = add(length, part.len()).map_err(ExpansionFailure::Native)?; Ok(()) }).map_err(|failure| self.retain(failure, context))?;
        context.reserve(unit_bytes(length).map_err(&retain)?).map_err(&retain)?;
        let mut output = Vec::with_capacity(length);
        self.pass(&units, |part| { output.extend_from_slice(part); Ok(()) }).map_err(|failure| self.retain(failure, context))?;
        retain_units(&output, context).map_err(&retain)
    }

    fn pass<'a>(&'a self, source: &[u16], mut emit: impl FnMut(&[u16]) -> ExpansionResult<'a>) -> ExpansionResult<'a> {
        let mut end = 0usize;
        let mut position = 0usize;
        while position <= source.len() {
            let Some(captures) = self.pattern.regex.captures_utf16(source, position, end).map_err(regex_error).map_err(ExpansionFailure::Native)? else { break; };
            let start = captures[0];
            let matched_end = captures[1];
            emit(&source[end..start])?;
            replacement_parts(self.replacement.view(), &self.pattern, captures.len() / 2, |part| {
                match part {
                    Part::Unit(unit) => emit(&[unit])?,
                    Part::Group(group) if captures[group * 2] != usize::MAX => emit(&source[captures[group * 2]..captures[group * 2 + 1]])?,
                    Part::Group(_) => {},
                }
                Ok(())
            })?;
            end = matched_end;
            position = if start == end { end + 1 } else { end };
        }
        emit(&source[end..])
    }

    fn retain(&self, failure: ExpansionFailure<'_>, context: &EvalContext<'_>) -> ReplacementFailure {
        match failure {
            ExpansionFailure::Java(kind, message) => match message.retain(context) {
                Ok(message) => ReplacementFailure::Java(JavaReplacementFailure { kind, message, replacement: self.replacement.clone() }),
                Err(error) => ReplacementFailure::Resource(error),
            },
            ExpansionFailure::Native(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => ReplacementFailure::Resource(error),
            ExpansionFailure::Native(error) => match RetainedAnomaly::retain(error, context) {
                Ok(error) => ReplacementFailure::Native(NativeReplacementFailure { replacement: self.replacement.clone(), error }),
                Err(error) => ReplacementFailure::Resource(error),
            },
        }
    }
}

impl ReplacementFailure {
    fn legacy(self, context: &EvalContext<'_>) -> Anomaly {
        match self {
            Self::Java(failure) => match utf8(failure.message.view(), context) {
                Ok(message) => match failure.kind {
                    ReplacementExceptionKind::IllegalArgument => Anomaly::incorrect("xtdb.expression/illegal-argument-exception", message),
                    ReplacementExceptionKind::IndexOutOfBounds => Anomaly::fault("xtdb.error/unknown", message),
                },
                Err(error) => error,
            },
            Self::Native(failure) => failure.error.legacy(),
            Self::Resource(error) => error,
        }
    }
}

impl Message<'_> {
    fn retain(self, context: &EvalContext<'_>) -> Result<JavaString> {
        match self {
            Self::Static(message) => retain_text(JavaText::from_utf8(message), context),
            Self::Named { prefix, name, suffix } => {
                let length = prefix.len().checked_add(name.len_units()).and_then(|length| length.checked_add(suffix.len())).ok_or_else(overflow)?;
                context.reserve(unit_bytes(length)?)?;
                let mut units = Vec::with_capacity(length);
                units.extend(prefix.encode_utf16());
                units.extend(name.units());
                units.extend(suffix.encode_utf16());
                retain_units(&units, context)
            },
            Self::Group(group) => {
                context.reserve(64)?;
                retain_text(JavaText::from_utf8(&format!("No group {group}")), context)
            },
        }
    }
}

fn postgres_backrefs(source: JavaText<'_>, context: &EvalContext<'_>) -> Result<JavaString> {
    context.reserve(unit_bytes(source.len_units())?)?;
    let mut output = Vec::with_capacity(source.len_units());
    let mut units = source.units().peekable();
    while let Some(unit) = units.next() {
        if unit == 0x5c {
            if units.peek() == Some(&0x5c) { units.next(); output.extend([0x5c, 0x5c]); continue; }
            if units.peek().copied().is_some_and(ascii_digit) {
                output.push(0x24);
                while units.peek().copied().is_some_and(ascii_digit) { output.push(units.next().expect("peeked digit")); }
                continue;
            }
        }
        output.push(unit);
    }
    retain_units(&output, context)
}

fn replacement_parts<'a>(source: JavaText<'a>, pattern: &Pattern, groups: usize, mut emit: impl FnMut(Part) -> ExpansionResult<'a>) -> ExpansionResult<'a> {
    let invalid = |message| ExpansionFailure::Java(ReplacementExceptionKind::IllegalArgument, Message::Static(message));
    let mut units = source.units().enumerate().peekable();
    while let Some((_, unit)) = units.next() {
        match unit {
            0x5c => emit(Part::Unit(units.next().ok_or_else(|| invalid("character to be escaped is missing"))?.1))?,
            0x24 => {
                let (_, next) = units.next().ok_or_else(|| invalid("Illegal group reference: group index is missing"))?;
                let group = if next == 0x7b {
                    let start = units.peek().map_or(source.len_units(), |(index, _)| *index);
                    while units.peek().is_some_and(|(_, unit)| ascii_alphanumeric(*unit)) { units.next(); }
                    let end = units.peek().map_or(source.len_units(), |(index, _)| *index);
                    if start == end { return Err(invalid("named capturing group has 0 length name")); }
                    if units.peek().map(|(_, unit)| *unit) != Some(0x7d) { return Err(invalid("named capturing group is missing trailing '}'")); }
                    let name = source.slice_units(start..end).expect("scanned name range");
                    if name.units().next().is_some_and(ascii_digit) {
                        return Err(ExpansionFailure::Java(ReplacementExceptionKind::IllegalArgument, Message::Named { prefix: "capturing group name {", name, suffix: "} starts with digit character" }));
                    }
                    let group = pattern.names.iter().position(|candidate| candidate.as_ref().is_some_and(|candidate| candidate.bytes().map(u16::from).eq(name.units())))
                        .ok_or(ExpansionFailure::Java(ReplacementExceptionKind::IllegalArgument, Message::Named { prefix: "No group with name {", name, suffix: "}" }))? + 1;
                    units.next();
                    group
                } else {
                    if !ascii_digit(next) { return Err(invalid("Illegal group reference")); }
                    let mut group = usize::from(next - 0x30);
                    while let Some(digit) = units.peek().map(|(_, unit)| *unit).filter(|unit| ascii_digit(*unit)) {
                        let next = group * 10 + usize::from(digit - 0x30);
                        if next >= groups { break; }
                        group = next;
                        units.next();
                    }
                    group
                };
                if group >= groups { return Err(ExpansionFailure::Java(ReplacementExceptionKind::IndexOutOfBounds, Message::Group(group))); }
                emit(Part::Group(group))?;
            },
            _ => emit(Part::Unit(unit))?,
        }
    }
    Ok(())
}

fn unit_bytes(length: usize) -> Result<usize> {
    check_size(length)?;
    Layout::array::<u16>(length).map(|layout| layout.size()).map_err(|_| overflow())
}

fn ascii_digit(unit: u16) -> bool { (0x30..=0x39).contains(&unit) }
fn ascii_alphanumeric(unit: u16) -> bool { ascii_digit(unit) || (0x41..=0x5a).contains(&unit) || (0x61..=0x7a).contains(&unit) }

fn utf8(source: JavaText<'_>, context: &EvalContext<'_>) -> Result<String> {
    let length = check_size(source.java_utf8_len()?)?;
    let bytes = Layout::array::<u8>(length).map_err(|_| overflow())?.size();
    context.reserve(bytes)?;
    let mut output = Vec::with_capacity(length);
    output.extend(source.java_utf8_bytes());
    String::from_utf8(output).map_err(|_| illegal("Java UTF8 encoder produced invalid UTF8"))
}
