use std::{fmt::Write, sync::Arc};
use rtdb_types::{AllocationScope, Anomaly, MemoryBudget, MemoryReservation};
use crate::lexer::{Kind, Position};
use super::{NoViableAlternative, ParserControl, ParserSession, RuntimeError, RuntimeProblem, RuntimeResult};
use super::super::{PredictionError, PredictionMode, capacity_error};

#[cfg(test)]
#[path = "no_viable_report_tests.rs"]
mod tests;

const CODE: &str = "xtdb/sql-error";
const LISTENER_PREFIX: &str = "Errors parsing SQL statement:\n  - line ";
const ALTERNATIVE_PREFIX: &str = "no viable alternative at input ";

#[derive(Debug)]
pub(in crate::parser::prediction) struct NoViableDiagnostic<'sql> {
    cause: NoViableAlternative<'sql>,
    listener: ListenerDiagnostic,
}

#[derive(Debug)]
pub(super) struct ListenerDiagnostic {
    pub(super) anomaly: Anomaly,
    lease: Arc<MemoryReservation>,
}

impl ListenerDiagnostic {
    pub(super) fn budget(&self) -> &Arc<MemoryBudget> { self.lease.budget() }
}

impl<'sql> NoViableDiagnostic<'sql> {
    #[cfg(test)]
    pub(in crate::parser::prediction) fn cause(&self) -> NoViableAlternative<'sql> { self.cause }
    #[cfg(test)]
    pub(in crate::parser::prediction) fn anomaly(&self) -> &Anomaly { &self.listener.anomaly }
    pub(super) fn into_listener(self) -> ListenerDiagnostic { self.listener }
}

impl<'parse, 'sql, 'work> ParserSession<'parse, 'sql, '_, 'work> {
    pub(in crate::parser::prediction) fn report_no_viable_alternative(&mut self) -> RuntimeResult<'sql, ()> {
        let Some(report) = self.pending_no_viable_report() else {
            if matches!(self.control, ParserControl::PendingNoViable(_)) {
                self.control.abort();
            }
            return Err(RuntimeError::Control(RuntimeProblem::Aborted));
        };

        let (cause, mode) = (report.cause, report.mode);
        self.control = ParserControl::InErrorRecovery;
        let message = {
            let token_text = if cause.start.lexeme().kind() == Kind::Eof {
                None
            } else {
                let start = isize::try_from(cause.start.index()).map_err(|_| capacity_error())?;
                let stop = isize::try_from(cause.offending.index()).map_err(|_| capacity_error())?;
                Some(self.input.text(start, stop)?)
            };
            let input = match &token_text {
                Some(text) => text.as_str(),
                None => "<EOF>",
            };
            let capacity = message_size([ALTERNATIVE_PREFIX.len(), quoted_len(input)?])?;
            self.work.reserve(capacity).map_err(PredictionError::Admission)?;
            let mut message = message_buffer(capacity)?;
            message.push_str(ALTERNATIVE_PREFIX);
            write_quoted(&mut message, input);
            message
        };
        self.syntax_errors = self.syntax_errors.wrapping_add(1);
        let position = cause.offending.lexeme().span().start;

        match mode {
            PredictionMode::Sll => Ok(()),
            PredictionMode::Ll => Err(RuntimeError::ReportedNoViableAlternative(
                NoViableDiagnostic { cause, listener: listener_diagnostic(position, &message, self.work)? },
            )),
        }
    }
}

pub(super) fn listener_diagnostic(
    position: Position,
    parser_message: &str,
    allocation: &AllocationScope,
) -> Result<ListenerDiagnostic, PredictionError> {
    let digits = |value: usize| value.checked_ilog10().unwrap_or(0) as usize + 1;
    let capacity = message_size([
        LISTENER_PREFIX.len(),
        digits(position.line),
        digits(position.column),
        2,
        parser_message.len(),
    ])?;
    let bytes = message_size([capacity, CODE.len(), 256])?;
    let lease = allocation.memory_budget().reserve(bytes).map_err(PredictionError::Admission)?;
    let mut message = message_buffer(capacity)?;
    write!(&mut message, "{LISTENER_PREFIX}{}:{} ", position.line, position.column)
        .expect("String writing is infallible");
    message.push_str(parser_message);
    let mut code = message_buffer(CODE.len())?;
    code.push_str(CODE);
    Ok(ListenerDiagnostic { anomaly: Anomaly::incorrect(code, message), lease })
}

pub(super) fn message_size(sizes: impl IntoIterator<Item = usize>) -> Result<usize, PredictionError> {
    sizes.into_iter().try_fold(0usize, |sum, size| {
        sum.checked_add(size).filter(|total| *total <= isize::MAX as usize).ok_or_else(capacity_error)
    })
}

pub(super) fn message_buffer(capacity: usize) -> Result<String, PredictionError> {
    let mut buffer = String::new();
    buffer.try_reserve_exact(capacity).map_err(|error| {
        PredictionError::Admission(Anomaly::incorrect("rtdb/allocation-failed", error.to_string()).with_kind(rtdb_types::AnomalyKind::ResourceExhausted))
    })?;
    Ok(buffer)
}

pub(super) fn quoted_len(input: &str) -> Result<usize, PredictionError> {
    message_size([
        2,
        input.len(),
        input.bytes().filter(|&value| matches!(value, b'\r' | b'\n' | b'\t')).count(),
    ])
}

pub(super) fn write_quoted(output: &mut String, input: &str) {
    output.push('\'');
    for character in input.chars() {
        match character {
            '\r' => output.push_str("\\r"),
            '\n' => output.push_str("\\n"),
            '\t' => output.push_str("\\t"),
            character => output.push(character),
        }
    }
    output.push('\'');
}
