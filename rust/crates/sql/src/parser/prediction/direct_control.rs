use std::sync::Arc;
use rtdb_types::{AllocationScope, Anomaly, MemoryReservation};
use crate::lexer::{ScanError, StreamError};
use super::{ParserEnvironment, PredictionError, RuntimeError, with_bail_fallback};
use super::no_viable_report::ListenerDiagnostic;
use crate::parser::statement_syntax::StatementRoot;

/// A statement syntax report from the original grammar's listener, holding its allocation until drop.
#[derive(Debug)]
pub struct StatementSyntaxDiagnostic(ListenerDiagnostic);

impl StatementSyntaxDiagnostic {
    /// Borrows the incorrect `xtdb/sql-error` anomaly with the listener’s text and empty context.
    pub fn anomaly(&self) -> &Anomaly { &self.0.anomaly }

    /// Copies the unnormalized input into `sql` context under the listener’s existing budget.
    /// Returns an error if the size overflows or the budget declines the reservation.
    pub fn with_sql_context(self, original: &str) -> rtdb_types::Result<ContextualStatementDiagnostic> {
        let mut listener = self.0;
        let bytes = original.len().checked_add(1024)
            .ok_or_else(|| Anomaly::incorrect("rtdb.sql/allocation-size", "SQL diagnostic context exceeds addressable memory").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded))?;
        let context_charge = listener.budget().reserve(bytes)?;
        listener.anomaly.context.insert("sql".into(), original.into());
        Ok(ContextualStatementDiagnostic { listener, _context_charge: context_charge })
    }
}

/// A listener report and original SQL context whose separate reservations last until drop.
#[derive(Debug)]
pub struct ContextualStatementDiagnostic {
    listener: ListenerDiagnostic,
    _context_charge: Arc<MemoryReservation>,
}

impl ContextualStatementDiagnostic {
    /// The anomaly preserves the listener code and message and includes the copied `sql` context.
    pub fn anomaly(&self) -> &Anomaly { &self.listener.anomaly }
}

impl ParserEnvironment {
    pub(in crate::parser) fn statement_syntax_diagnostic(
        &mut self, source: &str, root: StatementRoot, work: &AllocationScope,
    ) -> rtdb_types::Result<Option<StatementSyntaxDiagnostic>> {
        let result = with_bail_fallback(self, source, work, |session, _| {
            match root {
                StatementRoot::Direct => session.direct_sql_statement()?,
                StatementRoot::Batch => session.multi_sql_statement()?,
            };
            Ok(())
        });
        match result {
            Err(RuntimeError::ReportedTokenSyntax(report)) => Ok(Some(StatementSyntaxDiagnostic(report.into_listener()))),
            Err(RuntimeError::ReportedNoViableAlternative(report)) => Ok(Some(StatementSyntaxDiagnostic(report.into_listener()))),
            Err(RuntimeError::Prediction(PredictionError::Admission(error))
                | RuntimeError::Input(StreamError::Scan(ScanError::Admission(error)))) => Err(error),
            _ => Ok(None),
        }
    }
}
