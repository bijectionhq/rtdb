use std::sync::Arc;
use rtdb_types::{Anomaly, MemoryReservation, Result, java_text::{JavaString, JavaText}};
use crate::{EvalContext, numeric::overflow};

#[derive(Debug)]
pub(crate) enum CompileFailure { Pattern(PatternFailure), Resource(Anomaly) }

#[derive(Debug)]
pub(crate) struct PatternFailure {
    pub(super) pattern: JavaString,
    pub(super) syntax: Option<JavaSyntaxFailure>,
    pub(super) adapter_point: Option<usize>,
    error: RetainedAnomaly,
}

#[derive(Debug)]
pub(super) struct RetainedAnomaly {
    error: Anomaly,
    // The diagnostic allocation must be destroyed before its retained lease.
    _reservation: Option<Arc<MemoryReservation>>,
}

#[derive(Debug)]
pub(super) struct JavaSyntaxFailure { pub description: JavaString, pub index: i32 }

pub(super) struct Syntax<'a> { pub description: Description<'a>, pub index: i32 }
pub(super) enum Description<'a> { Static(&'static str), Points { prefix: &'static str, points: &'a [u32], suffix: &'static str } }

impl CompileFailure {
    pub(super) fn retain(error: Anomaly, pattern: JavaText<'_>, adapter_point: Option<usize>, syntax: Option<Syntax<'_>>, context: &EvalContext<'_>) -> Self {
        if error.is(rtdb_types::AnomalyKind::ResourceExhausted) { return Self::Resource(error); }
        match PatternFailure::retain(error, pattern, adapter_point, syntax, context) {
            Ok(failure) => Self::Pattern(failure),
            Err(error) => Self::Resource(error),
        }
    }

    pub(super) fn legacy(self) -> Anomaly {
        match self { Self::Pattern(failure) => failure.error.legacy(), Self::Resource(error) => error }
    }
}

impl PatternFailure {
    #[cfg(test)] pub(super) fn error(&self) -> &Anomaly { self.error.error() }

    fn retain(error: Anomaly, pattern: JavaText<'_>, adapter_point: Option<usize>, syntax: Option<Syntax<'_>>, context: &EvalContext<'_>) -> Result<Self> {
        let error = RetainedAnomaly::retain(error, context)?;
        let pattern = retain_text(pattern, context)?;
        let syntax = syntax.map(|syntax| {
            let description = match syntax.description {
                Description::Static(description) => retain_text(JavaText::from_utf8(description), context)?,
                Description::Points { prefix, points, suffix } => {
                    let capacity = prefix.len().checked_add(suffix.len()).and_then(|count| count.checked_add(points.len().checked_mul(2)?)).ok_or_else(overflow)?;
                    context.reserve(capacity.checked_mul(2).ok_or_else(overflow)?)?;
                    let mut units = Vec::with_capacity(capacity);
                    units.extend(prefix.encode_utf16());
                    for &point in points {
                        if point <= 0xffff { units.push(point as u16); }
                        else { let point = point - 0x10000; units.extend([0xd800 | (point >> 10) as u16, 0xdc00 | (point & 0x3ff) as u16]); }
                    }
                    units.extend(suffix.encode_utf16());
                    retain_units(&units, context)?
                }
            };
            Ok::<_, Anomaly>(JavaSyntaxFailure { description, index: syntax.index })
        }).transpose()?;
        Ok(Self { pattern, syntax, adapter_point, error })
    }
}

impl RetainedAnomaly {
    pub(super) fn retain(error: Anomaly, context: &EvalContext<'_>) -> Result<Self> {
        let bytes = std::mem::size_of::<Anomaly>().checked_add(error.code.capacity())
            .and_then(|bytes| bytes.checked_add(error.message.as_ref().map_or(0, String::capacity)))
            .and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?;
        let reservation = context.allocation.map(|scope| scope.memory_budget().reserve(bytes)).transpose()?;
        Ok(Self { error, _reservation: reservation })
    }

    #[cfg(test)] pub(super) fn error(&self) -> &Anomaly { &self.error }
    pub(super) fn legacy(self) -> Anomaly { self.error }
}

pub(super) fn retain_text(source: JavaText<'_>, context: &EvalContext<'_>) -> Result<JavaString> {
    if let Some(source) = source.utf8_source() {
        return match context.allocation {
            Some(scope) => JavaString::copy_utf8(source, scope.memory_budget()),
            None => JavaString::copy_utf8_untracked(source),
        };
    }
    context.reserve(source.len_units().checked_mul(2).ok_or_else(overflow)?)?;
    let mut units = Vec::with_capacity(source.len_units());
    units.extend(source.units());
    retain_units(&units, context)
}

pub(super) fn retain_units(units: &[u16], context: &EvalContext<'_>) -> Result<JavaString> {
    match context.allocation {
        Some(scope) => JavaString::copy_utf16(units, scope.memory_budget()),
        None => JavaString::copy_utf16_untracked(units),
    }
}
