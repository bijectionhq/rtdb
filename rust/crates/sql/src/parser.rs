mod grammar;
mod prediction;

pub(crate) fn is_unquoted_identifier(text: &str) -> bool {
    crate::lexer::keyword(text).is_none_or(|keyword| grammar::pinned_symbol(keyword.name())
        .is_some_and(prediction::is_identifier_token))
}
