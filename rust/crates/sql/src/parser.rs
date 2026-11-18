mod grammar;
mod statement_syntax;
mod prediction;

pub use statement_syntax::{batch_syntax_diagnostic, statement_syntax_diagnostic};
pub use prediction::{ContextualStatementDiagnostic, StatementSyntaxDiagnostic};

pub(crate) fn is_unquoted_identifier(text: &str) -> bool {
    crate::lexer::keyword(text).is_none_or(|keyword| grammar::pinned_symbol(keyword.name())
        .is_some_and(prediction::is_identifier_token))
}

