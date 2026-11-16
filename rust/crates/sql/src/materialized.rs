use sqlparser::tokenizer::{Token, TokenWithSpan};
use rtdb_types::Result;

pub(crate) fn cte_tokens(mut tokens: Vec<TokenWithSpan>, source: &str) -> Result<Vec<TokenWithSpan>> {
    let mut index = 0;
    while index < tokens.len() {
        if !word(tokens.get(index), "WITH") { index += 1; continue; }
        let mut head = index + 1;
        if word(tokens.get(head), "RECURSIVE") { head += 1; }
        loop {
            let materialized = word(tokens.get(head), "MATERIALIZED");
            let name = head + usize::from(materialized);
            if !matches!(tokens.get(name).map(|token| &token.token), Some(Token::Word(_))) { break; }
            let mut after_name = name + 1;
            if tokens.get(after_name).is_some_and(|token| token.token == Token::LParen) {
                let Some(end) = closing(&tokens, after_name) else { break; }; after_name = end + 1;
            }
            if !word(tokens.get(after_name), "AS") { break; }
            if word(tokens.get(after_name + 1), "MATERIALIZED")
                || (word(tokens.get(after_name + 1), "NOT") && word(tokens.get(after_name + 2), "MATERIALIZED")) {
                return Err(crate::parse::sql_error("MATERIALIZED precedes the common-table name", source, tokens[after_name + 1].span));
            }
            if tokens.get(after_name + 1).is_none_or(|token| token.token != Token::LParen) { break; }
            let Some(end) = closing(&tokens, after_name + 1) else { break; };
            if materialized {
                let marker = tokens.remove(head);
                tokens.insert(after_name, marker);
            }
            if tokens.get(end + 1).is_none_or(|token| token.token != Token::Comma) { break; }
            head = end + 2;
        }
        index += 1;
    }
    Ok(tokens)
}
fn word(token: Option<&TokenWithSpan>, expected: &str) -> bool {
    matches!(token.map(|token| &token.token), Some(Token::Word(word)) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected))
}
fn closing(tokens: &[TokenWithSpan], start: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (index, token) in tokens.iter().enumerate().skip(start) {
        match token.token {
            Token::LParen => depth += 1,
            Token::RParen => { depth = depth.checked_sub(1)?; if depth == 0 { return Some(index); } }
            _ => {}
        }
    }
    None
}
