use std::{fmt::Write, sync::Arc};
use rtdb_types::{AllocationScope, Anomaly, MemoryReservation};
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
    anomaly: Anomaly,
    _lease: Arc<MemoryReservation>,
}

impl<'sql> NoViableDiagnostic<'sql> {
    pub(in crate::parser::prediction) fn cause(&self) -> NoViableAlternative<'sql> { self.cause }
    pub(in crate::parser::prediction) fn anomaly(&self) -> &Anomaly { &self.anomaly }
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
            let capacity = message_size([
                ALTERNATIVE_PREFIX.len(),
                2,
                input.len(),
                input.bytes().filter(|&value| matches!(value, b'\r' | b'\n' | b'\t')).count(),
            ])?;
            self.work.reserve(capacity).map_err(PredictionError::Admission)?;
            let mut message = message_buffer(capacity)?;
            message.push_str(ALTERNATIVE_PREFIX);
            message.push('\'');
            for character in input.chars() {
                match character {
                    '\r' => message.push_str("\\r"),
                    '\n' => message.push_str("\\n"),
                    '\t' => message.push_str("\\t"),
                    character => message.push(character),
                }
            }
            message.push('\'');
            message
        };
        self.syntax_errors = self.syntax_errors.wrapping_add(1);
        let position = cause.offending.lexeme().span().start;

        match mode {
            PredictionMode::Sll => Ok(()),
            PredictionMode::Ll => Err(RuntimeError::ReportedNoViableAlternative(
                listener_diagnostic(cause, position, &message, self.work)?,
            )),
        }
    }
}

fn listener_diagnostic<'sql>(
    cause: NoViableAlternative<'sql>,
    position: Position,
    parser_message: &str,
    allocation: &AllocationScope,
) -> Result<NoViableDiagnostic<'sql>, PredictionError> {
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
    Ok(NoViableDiagnostic { cause, anomaly: Anomaly::incorrect(code, message), _lease: lease })
}

fn message_size(sizes: impl IntoIterator<Item = usize>) -> Result<usize, PredictionError> {
    sizes.into_iter().try_fold(0usize, |sum, size| {
        sum.checked_add(size).filter(|total| *total <= isize::MAX as usize).ok_or_else(capacity_error)
    })
}

fn message_buffer(capacity: usize) -> Result<String, PredictionError> {
    let mut buffer = String::new();
    buffer.try_reserve_exact(capacity).map_err(|error| {
        PredictionError::Admission(Anomaly::incorrect("rtdb/allocation-failed", error.to_string()))
    })?;
    Ok(buffer)
}
