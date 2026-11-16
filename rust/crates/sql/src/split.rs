use rtdb_plan::SourceSpan;
use rtdb_types::Result;
use sqlparser::{dialect::GenericDialect, tokenizer::{Span, Token, Tokenizer}};

pub fn split_statements(source: &str) -> Result<Vec<SourceSpan>> {
    let tokens = Tokenizer::new(&GenericDialect {}, source).tokenize_with_location()
        .map_err(|error| crate::parse::sql_error(error.to_string(), source, Span::new(error.location, error.location)))?;
    let mut start = None;
    let mut end = None;
    let mut result = Vec::new();
    for token in tokens {
        match token.token {
            Token::Whitespace(_) | Token::EOF => (),
            Token::SemiColon => { if let (Some(start), Some(end)) = (start.take(), end.take()) { result.push(crate::parse::source_span(source, Span::new(start, end))); } },
            _ => { start.get_or_insert(token.span.start); end = Some(token.span.end); },
        }
    }
    if let (Some(start), Some(end)) = (start, end) { result.push(crate::parse::source_span(source, Span::new(start, end))); }
    Ok(result)
}

#[cfg(test)]
mod tests {
    #[test]
    fn protocol_split_preserves_quoted_semicolons_and_custom_control_syntax() {
        let source = "-- setup\nBEGIN READ WRITE WITH (ASYNC = true); SELECT ';', $$a;b$$; /* ; */ COPY foo FROM STDIN WITH (FORMAT 'arrow-stream');";
        let statements = super::split_statements(source).unwrap();
        let text: Vec<_> = statements.iter().map(|span| &source[span.start..span.end]).collect();
        assert_eq!(text, ["BEGIN READ WRITE WITH (ASYNC = true)", "SELECT ';', $$a;b$$", "COPY foo FROM STDIN WITH (FORMAT 'arrow-stream')"]);
    }
    #[test]
    fn comment_only_sql_has_no_statement() { assert!(super::split_statements(" -- idle\n /* also idle */ ; ").unwrap().is_empty()); }
}
