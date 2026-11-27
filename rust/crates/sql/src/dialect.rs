use sqlparser::{ast, dialect::{Dialect, GenericDialect, Precedence}, keywords::Keyword, parser::{Parser, ParserError}, tokenizer::{Token,TokenWithSpan,Word}};
use std::{cell::Cell,fmt::Write};

/// `Sql.g4`'s `nonReservedKeyword` production, with its sub-productions expanded: the grammar's keywords that may name a correlation or column.
const NON_RESERVED_KEYWORDS: &[&str] = &[
    "START", "END", "COMMITTED", "UNCOMMITTED", "TIMEZONE", "VERSION", "SYSTEM_TIME", "VALID_TIME", "SELECT", "INSERT", "UPDATE", "DELETE", "ERASE",
    "SETTING", "ROLE", "USER", "PASSWORD", "VARBINARY", "BYTEA", "URI", "OID", "COPY", "FORMAT", "ATTACH", "DETACH", "DATABASE", "LEVEL", "FILTER",
    "TABLE", "METADATA", "SYNC", "DISCARD", "PLANS", "TEMP", "TEMPORARY", "SEQUENCES", "AT", "BETWEEN", "BY", "CURRENT", "FIRST", "LAST", "NEXT",
    "OF", "POSITION", "BIGINT", "BOOLEAN", "CHAR", "DATE", "DEC", "DECIMAL", "DOUBLE", "DURATION", "FLOAT", "INT", "INTEGER", "INTERVAL", "KEYWORD",
    "NUMERIC", "OBJECT", "PERIOD", "PRECISION", "REAL", "RECORD", "REGCLASS", "REGPROC", "ROW", "SMALLINT", "TEXT", "TIME", "TIMESTAMP",
    "TIMESTAMPTZ", "TSTZRANGE", "UUID", "VARCHAR", "INT2", "INT4", "INT8", "FLOAT4", "FLOAT8", "BOOL", "BPCHAR", "NAME",
    "AVG", "MAX", "MIN", "SUM", "COUNT", "EVERY", "BOOL_AND", "BOOL_OR", "STDDEV_POP", "STDDEV_SAMP", "VAR_SAMP", "VAR_POP",
    "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "DOW", "ISODOW", "DOY", "WEEK", "QUARTER", "EPOCH",
    "RANK", "DENSE_RANK", "PERCENT_RANK", "CUME_DIST",
];

/// The grammar token an unquoted word lexes as; an alternative spelling such as INT8 lexes as its canonical keyword.
fn grammar_keyword(token: &TokenWithSpan) -> Option<&'static str> {
    match &token.token { Token::Word(word) if word.quote_style.is_none() => crate::lexer::keyword(&word.value).map(crate::lexer::Keyword::name), _ => None }
}
fn non_reserved(token: &TokenWithSpan) -> bool {
    grammar_keyword(token).is_some_and(|word| NON_RESERVED_KEYWORDS.iter().any(|keyword| word.eq_ignore_ascii_case(keyword)))
}
/// The alias candidate the parser has just consumed, when it is one of the grammar's reserved keywords.
fn reserved_word(parser: &Parser) -> Option<&'static str> {
    let token = parser.get_current_token();
    if non_reserved(token) { return None; }
    grammar_keyword(token)
}

#[derive(Debug, Default)]
pub(crate) struct SqlDialect { position_work: Cell<Option<usize>> }

const POSITION_WORK: usize = 32 * 1024 * 1024;
impl SqlDialect {
    fn precedence(precedence: Precedence) -> u8 {
        match precedence {
            Precedence::Caret => 49,Precedence::Between => 48,Precedence::Like => 46,
            _ => { let value = GenericDialect {}.prec_value(precedence);if value >= 30 { value + 20 } else { value } }
        }
    }
    fn begin_position(&self) { if self.position_work.get().is_none() { self.position_work.set(Some(POSITION_WORK)); } }
    fn spend(&self,amount: usize) -> std::result::Result<(),ParserError> {
        let Some(remaining) = self.position_work.get() else { return Ok(()); };
        match remaining.checked_sub(amount) {
            Some(next) if next > 0 => { self.position_work.set(Some(next));Ok(()) }
            _ => { self.position_work.set(Some(0));Err(position_limit()) }
        }
    }
    pub(crate) fn checked<T>(&self,result: std::result::Result<T,ParserError>) -> std::result::Result<T,ParserError> {
        if self.position_work.get() == Some(0) { Err(position_limit()) } else { result }
    }
    fn token_work(&self,token: &TokenWithSpan) -> std::result::Result<(),ParserError> {
        if self.position_work.get().is_none() { return Ok(()); }
        self.spend(std::mem::size_of::<TokenWithSpan>() * 8 + 2)?;
        write!(TokenWork(self),"{}",token.token).map_err(|_|position_limit())
    }
}
struct TokenWork<'a>(&'a SqlDialect);
impl std::fmt::Write for TokenWork<'_> {
    fn write_str(&mut self,text: &str) -> std::fmt::Result {
        let Some(amount) = text.len().checked_mul(8) else { self.0.position_work.set(Some(0));return Err(std::fmt::Error); };
        self.0.spend(amount).map_err(|_|std::fmt::Error)
    }
}
fn position_limit() -> ParserError { ParserError::ParserError("POSITION cumulative parsing work limit exceeded".into()) }

pub(crate) enum CustomStringLiteral { Duration, Uri, Keyword }

impl CustomStringLiteral {
    fn from_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("DURATION") { Some(Self::Duration) }
        else if name.eq_ignore_ascii_case("URI") { Some(Self::Uri) }
        else if name.eq_ignore_ascii_case("KEYWORD") { Some(Self::Keyword) }
        else { None }
    }

    pub(crate) fn from_type(data_type: &ast::DataType) -> Option<Self> {
        let ast::DataType::Custom(name, modifiers) = data_type else { return None; };
        let [ast::ObjectNamePart::Identifier(name)] = &name.0[..] else { return None; };
        if !modifiers.is_empty() || name.quote_style.is_some() { return None; }
        Self::from_name(&name.value)
    }
}

macro_rules! generic_flags {
    ($($name:ident),* $(,)?) => { $(fn $name(&self) -> bool { GenericDialect {}.$name() })* };
}

fn current_catalog(parser: &mut Parser, word: &Word, span: sqlparser::tokenizer::Span) -> Option<ast::Expr> {
    let name = if word.keyword == Keyword::CURRENT_CATALOG {
        parser.next_token();
        vec![word.to_ident(span)]
    } else {
        if parser.peek_nth_token_ref(1).token != Token::Period { return None; }
        let catalog = parser.peek_nth_token_ref(2);
        let Token::Word(catalog_name) = &catalog.token else { return None; };
        if catalog_name.keyword != Keyword::CURRENT_CATALOG { return None; }
        let catalog = catalog_name.to_ident(catalog.span);
        for _ in 0..3 { parser.next_token(); }
        vec![word.to_ident(span), catalog]
    };
    let args = if parser.peek_token_ref().token == Token::LParen && parser.peek_nth_token_ref(1).token == Token::RParen {
        parser.next_token();
        parser.next_token();
        ast::FunctionArguments::List(ast::FunctionArgumentList { duplicate_treatment: None, args: Vec::new(), clauses: Vec::new() })
    } else {
        ast::FunctionArguments::None
    };
    Some(ast::Expr::Function(ast::Function {
        name: name.into(), uses_odbc_syntax: false, parameters: ast::FunctionArguments::None,
        args, null_treatment: None, filter: None, over: None, within_group: Vec::new(),
    }))
}

impl Dialect for SqlDialect {
    fn dialect(&self) -> std::any::TypeId { GenericDialect {}.dialect() }
    fn is_identifier_start(&self, ch: char) -> bool { ch.is_ascii_alphabetic() || ch == '_' }
    fn is_identifier_part(&self, ch: char) -> bool { ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$') }
    fn is_delimited_identifier_start(&self, ch: char) -> bool { GenericDialect {}.is_delimited_identifier_start(ch) }
    fn preserves_doubled_identifier_delimiters(&self) -> bool { true }
    fn supports_from_pipeline(&self) -> bool { true }
    fn supports_values_in_subqueries(&self) -> bool { true }
    fn supports_records_query(&self) -> bool { true }
    fn supports_group_by_set_quantifier(&self) -> bool { true }
    fn supports_scalar_values_rows(&self) -> bool { true }
    fn supports_user_as_identifier(&self) -> bool { true }
    fn supports_expression_derived_tables(&self) -> bool { true }
    fn supports_optional_projection_items(&self) -> bool { true }
    fn supports_single_word_time_zone(&self) -> bool { true }
    fn supports_interval_field_ranges(&self) -> bool { true }
    fn supports_character_length_units(&self) -> bool { true }
    fn supports_parametric_functions(&self) -> bool { false }
    fn supports_bare_substring_start(&self) -> bool { true }
    fn supports_bare_substring_length(&self) -> bool { true }
    fn requires_consistent_substring_separators(&self) -> bool { true }
    fn requires_complete_fetch_syntax(&self) -> bool { true }
    fn supports_alias_on_wrapped_table_factor(&self) -> bool { false }
    fn unparameterized_data_type(&self, word: &Word) -> Option<ast::DataType> {
        if word.quote_style.is_some() { return None; }
        match word.keyword {
            Keyword::CHAR => Some(ast::DataType::Char(None)),
            Keyword::VARCHAR => Some(ast::DataType::Varchar(None)),
            Keyword::TEXT | Keyword::NAME => Some(ast::DataType::Text),
            _ if word.value.eq_ignore_ascii_case("BPCHAR") => Some(ast::DataType::Text),
            _ => None,
        }
    }

    fn supports_interval_identifier(&self) -> bool { true }
    fn qualifies_column_before_subscript(&self) -> bool { true }
    fn supports_table_function_syntax(&self) -> bool { false }
    fn parse_statement(&self, parser: &mut Parser) -> Option<std::result::Result<ast::Statement, ParserError>> {
        if parser.peek_keyword(Keyword::ABORT) || parser.peek_keyword(Keyword::END) {
            return Some(parser.expected_ref("an XTDB SQL statement", parser.peek_token_ref()));
        }
        None
    }
    fn is_column_alias(&self, keyword: &Keyword, parser: &mut Parser) -> bool {
        *keyword != Keyword::WINDOW && GenericDialect {}.is_column_alias(keyword, parser)
    }
    fn is_select_item_alias(&self, explicit: bool, keyword: &Keyword, parser: &mut Parser) -> bool {
        match reserved_word(parser) {
            Some(word) => explicit && (word.eq_ignore_ascii_case("CURRENT_USER") || word.eq_ignore_ascii_case("DEFAULT")),
            None => explicit || (*keyword != Keyword::DEFAULT && self.is_column_alias(keyword, parser)),
        }
    }
    fn is_table_factor_alias(&self, explicit: bool, keyword: &Keyword, parser: &mut Parser) -> bool {
        reserved_word(parser).is_none() && (explicit || self.is_table_alias(keyword, parser)
            || (non_reserved(parser.get_current_token()) && matches!(parser.peek_token_ref().token, Token::EOF | Token::RParen | Token::Comma | Token::SemiColon)))
    }

    generic_flags!(
        supports_partition_by_after_order_by,
        supports_array_join_syntax,
        supports_group_by_expr,
        supports_group_by_with_modifier,
        supports_left_associative_joins_without_parens,
        supports_connect_by,
        supports_match_recognize,
        supports_pipe_operator,
        supports_window_function_null_treatment_arg,
        supports_dictionary_syntax,
        supports_window_clause_named_window_reference,
        supports_parenthesized_set_variables,
        supports_select_wildcard_except,
        support_map_literal_syntax,
        allow_extract_custom,
        allow_extract_single_quotes,
        supports_extract_comma_syntax,
        supports_create_view_comment_syntax,
        supports_parens_around_table_factor,
        supports_create_index_with_clause,
        supports_explain_with_utility_options,
        supports_exclude_constraint,
        supports_limit_comma,
        supports_update_order_by,
        supports_from_first_select,
        supports_projection_trailing_commas,
        supports_asc_desc_in_column_definition,
        supports_try_convert,
        supports_bitwise_shift_operators,
        supports_comment_on,
        supports_load_extension,
        supports_named_fn_args_with_assignment_operator,
        supports_struct_literal,
        supports_empty_projections,
        supports_nested_comments,
        supports_user_host_grantee,
        supports_string_escape_constant,
        supports_array_typedef_with_brackets,
        supports_set_names,
        supports_comma_separated_set_assignments,
        supports_filter_during_aggregation,
        supports_select_wildcard_exclude,
        supports_data_type_signed_suffix,
        supports_interval_options,
        supports_quote_delimited_string,
        supports_select_wildcard_replace,
        supports_select_wildcard_ilike,
        supports_select_wildcard_rename,
        supports_optimize_table,
        supports_install,
        supports_detach,
        supports_prewhere,
        supports_limit_by,
        supports_settings,
        supports_constraint_keyword_without_name,
        supports_key_column_option,
        supports_comma_separated_trim,
        supports_select_item_multi_column_alias,
        supports_xml_expressions,
        supports_aliased_function_args,
    );

    fn parse_prefix(&self, parser: &mut Parser) -> Option<std::result::Result<ast::Expr, ParserError>> {
        if let Err(error) = self.token_work(parser.peek_token_ref()) { return Some(Err(error)); }
        let token = parser.peek_token();
        if token.token == Token::Tilde {
            parser.next_token();
            return Some(parser.parse_subexpr(self.prec_value(Precedence::Caret)).map(|expr| ast::Expr::UnaryOp {
                op: ast::UnaryOperator::BitwiseNot, expr: Box::new(expr),
            }));
        }
        let Token::Word(word) = &token.token else { return None; };
        if let Some(expr) = current_catalog(parser, word, token.span) { return Some(Ok(expr)); }
        if word.quote_style.is_none()
            && (word.value.eq_ignore_ascii_case("RECORD") || word.value.eq_ignore_ascii_case("OBJECT"))
            && parser.peek_nth_token_ref(1).token == Token::LParen
        {
            parser.next_token();
            parser.next_token();
            return Some(parser.parse_dictionary_fields(Token::RParen));
        }
        if word.quote_style.is_none()
            && (word.value.eq_ignore_ascii_case("NEST_ONE") || word.value.eq_ignore_ascii_case("NEST_MANY"))
            && parser.peek_nth_token_ref(1).token == Token::LParen
        {
            parser.next_token();
            parser.next_token();
            return Some((|| {
                let query = parser.parse_query()?;
                parser.expect_token(&Token::RParen)?;
                Ok(ast::Expr::Function(ast::Function {
                    name: ast::ObjectName::from(vec![word.to_ident(token.span)]),
                    uses_odbc_syntax: false,
                    parameters: ast::FunctionArguments::None,
                    args: ast::FunctionArguments::Subquery(query),
                    filter: None,
                    null_treatment: None,
                    over: None,
                    within_group: Vec::new(),
                }))
            })());
        }
        if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("NOW") {
            parser.next_token();
            return Some(parser.parse_time_functions(ast::ObjectName::from(vec![word.to_ident(token.span)])));
        }
        if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("SUBSTR") && parser.peek_nth_token(1).token == Token::LParen {
            let name = ast::ObjectName::from(vec![word.to_ident(token.span)]);
            parser.next_token();
            return Some(parser.parse_function(name));
        }
        if word.quote_style.is_none() && CustomStringLiteral::from_name(&word.value).is_some()
            && matches!(parser.peek_nth_token(1).token, Token::SingleQuotedString(_) | Token::EscapedStringLiteral(_) | Token::DollarQuotedString(_)) {
            let name = ast::ObjectName::from(vec![word.to_ident(token.span)]);
            parser.next_token();
            return Some(parser.parse_value().map(|value| ast::Expr::TypedString(ast::TypedString { data_type: ast::DataType::Custom(name, Vec::new()), value, uses_odbc_syntax: false })));
        }
        if word.quote_style.is_some() || !word.value.eq_ignore_ascii_case("POSITION") || parser.peek_nth_token(1).token != Token::LParen { return None; }
        let identifier = word.to_ident(token.span);
        parser.next_token();
        self.begin_position();
        Some(self.checked((|| {
            if let Some(expression) = self.checked(parser.maybe_parse(|parser| {
                parser.expect_token(&Token::LParen)?;
                for delimiter in position_delimiters(parser,self)?.into_iter().rev() {
                    if let Some(expression) = self.checked(parser.maybe_parse(|parser| {
                        let needle = parse_before_token(parser,delimiter,self)?;
                        parser.expect_keyword_is(Keyword::IN)?;
                        let haystack = parser.parse_expr()?;
                        let octets = if parser.parse_keyword(Keyword::USING) {
                            let units = parser.next_token();
                            match units.token {
                                Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("OCTETS") => true,
                                Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("CHARACTERS") => false,
                                _ => return Err(ParserError::ParserError("Expected CHARACTERS or OCTETS after USING".into())),
                            }
                        } else { false };
                        parser.expect_token(&Token::RParen)?;
                        Ok(ast::Expr::BinaryOp { left: Box::new(needle), op: ast::BinaryOperator::Custom(if octets { "rtdb.sql/string/octet_position" } else { "rtdb.sql/string/position" }.into()), right: Box::new(haystack) })
                    }))? { return Ok(expression); }
                }
                Err(ParserError::ParserError("Expected POSITION needle IN haystack".into()))
            }))? { return Ok(expression); }
            parser.parse_function(ast::ObjectName::from(vec![identifier]))
        })()))
    }

    fn prec_value(&self, precedence: Precedence) -> u8 {
        Self::precedence(precedence)
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<std::result::Result<u8, ParserError>> {
        if let Err(error) = self.spend(1) { return Some(Err(error)); }
        if let Some((keyword,_,_)) = ordinary_predicate(parser) {
            return Some(Ok(match keyword { Keyword::BETWEEN => 48,Keyword::IN => 47,_ => 46 }));
        }
        if let Some((_,_,flags)) = regex_predicate(parser) { return Some(Ok(if flags.is_none() { 45 } else { 44 })); }
        match parser.peek_token().token {
            Token::Arrow | Token::LongArrow | Token::HashArrow | Token::HashLongArrow => return Some(Ok(65)),
            Token::Pipe | Token::Ampersand | Token::Sharp | Token::ShiftLeft | Token::ShiftRight | Token::StringConcat => return Some(Ok(self.prec_value(Precedence::Caret))),
            Token::Caret => return Some(Err(ParserError::ParserError("Unexpected caret operator".into()))),
            _ => {},
        }
        predicate(parser).map(|(name,_)| Ok(period_precedence(&name)))
    }

    fn parse_infix(&self, parser: &mut Parser, left: &ast::Expr, precedence: u8) -> Option<std::result::Result<ast::Expr, ParserError>> {
        if let Some((keyword,negated,tokens)) = ordinary_predicate(parser) {
            if keyword == Keyword::BETWEEN {
                if !common_value(left) { return Some(Err(common_value_error())); }
                for _ in 0..tokens { parser.next_token(); }
                return Some((|| {
                    let symmetric = unquoted_word(parser,0,"SYMMETRIC");
                    if symmetric || unquoted_word(parser,0,"ASYMMETRIC") { parser.next_token(); }
                    let low = Box::new(parse_common_value(parser)?);
                    parser.expect_keyword_is(Keyword::AND)?;
                    let high = Box::new(parse_common_value(parser)?);
                    Ok(if symmetric { ast::Expr::BinaryOp { left: Box::new(left.clone()),op: ast::BinaryOperator::Custom(if negated { "rtdb.sql/between/not-symmetric" } else { "rtdb.sql/between/symmetric" }.into()),right: Box::new(ast::Expr::BinaryOp { left: low,op: ast::BinaryOperator::Custom("rtdb.sql/between/bounds".into()),right: high }) } }
                        else { ast::Expr::Between { expr: Box::new(left.clone()),low,high,negated } })
                })());
            }
            if matches!(keyword,Keyword::LIKE | Keyword::ILIKE) {
                for _ in 0..tokens { parser.next_token(); }
                return Some((|| {
                    let pattern = Box::new(parse_common_value(parser)?);
                    if parser.parse_keyword(Keyword::ESCAPE) { parse_common_value(parser)?; }
                    let expr = Box::new(left.clone());
                    Ok(if keyword == Keyword::LIKE { ast::Expr::Like { expr,pattern,negated,any: false,escape_char: None } }
                        else { ast::Expr::ILike { expr,pattern,negated,any: false,escape_char: None } })
                })());
            }
        }
        if let Some((tokens,negated,flags)) = regex_predicate(parser) {
            for _ in 0..tokens { parser.next_token(); }
            return Some((|| {
                let pattern = parse_common_value(parser)?;
                let flags = if flags.is_none() && unquoted_word(parser,0,"FLAG") {
                    parser.next_token();
                    parse_common_value(parser)?
                } else { ast::Expr::Value(ast::Value::SingleQuotedString(flags.unwrap_or("").into()).into()) };
                Ok(ast::Expr::BinaryOp {
                    left: Box::new(left.clone()),op: ast::BinaryOperator::Custom(if negated { "rtdb.sql/regex/not" } else { "rtdb.sql/regex/match" }.into()),
                    right: Box::new(ast::Expr::BinaryOp { left: Box::new(pattern),op: ast::BinaryOperator::Custom("rtdb.sql/regex/flags".into()),right: Box::new(flags) }),
                })
            })());
        }
        if parser.peek_token().token == Token::Sharp {
            parser.next_token();
            return Some(parser.parse_subexpr(precedence).map(|right| ast::Expr::BinaryOp {
                left: Box::new(left.clone()), op: ast::BinaryOperator::PGBitwiseXor, right: Box::new(right),
            }));
        }
        let (name, tokens) = predicate(parser)?;
        for _ in 0..tokens { parser.next_token(); }
        Some(parser.parse_subexpr(precedence).map(|right| ast::Expr::BinaryOp {
            left: Box::new(left.clone()), op: ast::BinaryOperator::Custom(format!("rtdb.sql/period/{name}")), right: Box::new(right),
        }))
    }
}


fn position_delimiters(parser: &Parser,dialect: &SqlDialect) -> std::result::Result<Vec<usize>,ParserError> {
    let mut delimiters = Vec::new();
    let mut depth = 0usize;
    let mut index = parser.index();
    loop {
        dialect.spend(1)?;
        match &parser.token_at(index).token {
            Token::EOF => break,
            Token::RParen if depth == 0 => break,
            Token::LParen | Token::LBracket | Token::LBrace => depth += 1,
            Token::RParen | Token::RBracket | Token::RBrace => depth = depth.saturating_sub(1),
            Token::Word(word) if depth == 0 && word.quote_style.is_none() && word.keyword == Keyword::IN => { dialect.spend(32)?;delimiters.push(index); },
            _ => {},
        }
        index += 1;
    }
    Ok(delimiters)
}

fn reset_tokens_preserving_recursion(parser: &mut Parser,tokens: Vec<TokenWithSpan>,index: usize) {
    let owner = std::mem::replace(parser,Parser::new(&GenericDialect {}));
    *parser = owner.with_tokens_with_locations(tokens);
    while parser.index() < index { parser.next_token_no_skip(); }
}

fn parse_before_token(parser: &mut Parser,delimiter: usize,dialect: &SqlDialect) -> std::result::Result<ast::Expr,ParserError> {
    let mut tokens = Vec::new();
    let mut index = 0;
    loop {
        let token = parser.token_at(index);
        dialect.token_work(token)?;
        let token = token.clone();
        let end = token.token == Token::EOF;
        tokens.push(token);
        if end { break; }
        index += 1;
    }
    let mut delimited = tokens.clone();
    delimited[delimiter].token = Token::EOF;
    let start = parser.index();
    reset_tokens_preserving_recursion(parser,delimited,start);
    let result = parser.parse_expr().and_then(|expression| {
        if parser.peek_token().token == Token::EOF { Ok(expression) }
        else { Err(ParserError::ParserError("Incomplete POSITION needle expression".into())) }
    });
    let end = if result.is_ok() { delimiter } else { parser.index() };
    reset_tokens_preserving_recursion(parser,tokens,end);
    dialect.checked(result)
}

fn period_precedence(name: &str) -> u8 {
    match name {
        "overlaps" => 43,"strictly_overlaps" => 42,"equals" => 41,"contains" => 40,"strictly_contains" => 39,
        "precedes" => 38,"strictly_precedes" => 37,"immediately_precedes" => 36,
        "succeeds" => 35,"strictly_succeeds" => 34,"immediately_succeeds" => 33,
        "lags" => 32,"strictly_lags" => 31,"immediately_lags" => 30,
        "leads" => 29,"strictly_leads" => 28,"immediately_leads" => 27,
        _ => unreachable!(),
    }
}

fn predicate(parser: &Parser) -> Option<(String, usize)> {
    let word = |index| match parser.peek_nth_token(index).token {
        Token::Word(word) if word.quote_style.is_none() => Some(word.value.to_ascii_lowercase()),
        _ => None,
    };
    let first = word(0)?;
    let (modifier, name, count) = match first.as_str() {
        "strictly" | "immediately" => (Some(first.as_str()), word(1)?, 2),
        _ => (None, first.clone(), 1),
    };
    let valid = match modifier {
        None => ["overlaps", "equals", "contains", "precedes", "succeeds", "lags", "leads"].contains(&name.as_str()),
        Some("strictly") => ["overlaps", "contains", "precedes", "succeeds", "lags", "leads"].contains(&name.as_str()),
        Some("immediately") => ["precedes", "succeeds", "lags", "leads"].contains(&name.as_str()),
        _ => false,
    };
    valid.then(|| (modifier.map_or(name.clone(), |modifier| format!("{modifier}_{name}")), count))
}


fn unquoted_word(parser: &Parser,index: usize,expected: &str) -> bool {
    matches!(parser.peek_nth_token(index).token,Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected))
}

fn regex_predicate(parser: &Parser) -> Option<(usize,bool,Option<&'static str>)> {
    match parser.peek_token().token {
        Token::Tilde => Some((1,false,Some(""))),
        Token::TildeAsterisk => Some((1,false,Some("i"))),
        Token::ExclamationMarkTilde => Some((1,true,Some(""))),
        Token::ExclamationMarkTildeAsterisk => Some((1,true,Some("i"))),
        _ if unquoted_word(parser,0,"LIKE_REGEX") => Some((1,false,None)),
        _ if unquoted_word(parser,0,"NOT") && unquoted_word(parser,1,"LIKE_REGEX") => Some((2,true,None)),
        _ => None,
    }
}

fn ordinary_predicate(parser: &Parser) -> Option<(Keyword,bool,usize)> {
    let negated = unquoted_word(parser,0,"NOT");
    let index = usize::from(negated);
    [Keyword::BETWEEN,Keyword::IN,Keyword::LIKE,Keyword::ILIKE].into_iter().find_map(|keyword| {
        matches!(parser.peek_nth_token(index).token,Token::Word(word) if word.quote_style.is_none() && word.keyword == keyword)
            .then_some((keyword,negated,index + 1))
    })
}

fn common_value_error() -> ParserError { ParserError::ParserError("Expected a common value expression; parenthesize predicates and Boolean operators".into()) }

fn parse_common_value(parser: &mut Parser) -> std::result::Result<ast::Expr,ParserError> {
    let expression = parser.parse_subexpr(SqlDialect::precedence(Precedence::Between))?;
    if !common_value(&expression) { return Err(common_value_error()); }
    Ok(expression)
}

fn common_value(expression: &ast::Expr) -> bool {
    use ast::{BinaryOperator as B,Expr as E,UnaryOperator as U};
    match expression {
        E::UnaryOp { op: U::Plus | U::Minus | U::BitwiseNot,expr } => common_value(expr),
        E::UnaryOp { .. } => false,
        E::BinaryOp { op: B::Custom(name),.. } if name.starts_with("rtdb.sql/string/") => true,
        E::BinaryOp { left,op,right } => {
            let common = matches!(op,B::Plus | B::Minus | B::Multiply | B::Divide | B::Modulo | B::StringConcat
                | B::BitwiseAnd | B::BitwiseOr | B::PGBitwiseXor | B::BitwiseXor | B::PGBitwiseShiftLeft | B::PGBitwiseShiftRight
                | B::Arrow | B::LongArrow | B::HashArrow | B::HashLongArrow);
            common && common_value(left) && common_value(right)
        }
        E::Cast { kind: ast::CastKind::DoubleColon,expr,.. } | E::CompoundFieldAccess { root: expr,.. } => common_value(expr),
        E::Between {..} | E::InList {..} | E::InSubquery {..} | E::InUnnest {..} | E::Like {..} | E::ILike {..} | E::RLike {..} | E::SimilarTo {..}
            | E::IsNull(_) | E::IsNotNull(_) | E::IsTrue(_) | E::IsNotTrue(_) | E::IsFalse(_) | E::IsNotFalse(_) | E::IsUnknown(_) | E::IsNotUnknown(_)
            | E::IsDistinctFrom(..) | E::IsNotDistinctFrom(..) | E::AnyOp {..} | E::AllOp {..} => false,
        _ => true,
    }
}

#[cfg(test)]
mod period_precedence_tests {
    use super::*;
    fn parse(source: &str) -> ast::Expr {
        let dialect = SqlDialect::default();
        let mut parser = Parser::new(&dialect).try_with_sql(source).unwrap();
        let expr = parser.parse_expr().unwrap();parser.expect_token(&Token::EOF).unwrap();expr
    }
    fn period(expr: &ast::Expr) -> (&str,&ast::Expr,&ast::Expr) {
        let ast::Expr::BinaryOp { left,op: ast::BinaryOperator::Custom(name),right } = expr else { panic!("period predicate: {expr:?}") };
        (name.strip_prefix("rtdb.sql/period/").unwrap(),left,right)
    }
    #[test]
    fn period_alternatives_follow_the_pinned_grammar_order_for_every_pair() {
        let names = ["overlaps","strictly_overlaps","equals","contains","strictly_contains","precedes","strictly_precedes","immediately_precedes","succeeds","strictly_succeeds","immediately_succeeds","lags","strictly_lags","immediately_lags","leads","strictly_leads","immediately_leads"];
        for (i,first) in names.iter().enumerate() { for (j,second) in names.iter().enumerate() {
            let source = format!("a {} b {} c",first.replace('_'," "),second.replace('_'," "));
            let expr = parse(&source);let (root,left,right) = period(&expr);
            if i <= j { assert_eq!(root,*second,"{source}");assert_eq!(period(left).0,*first,"{source}"); }
            else { assert_eq!(root,*first,"{source}");assert_eq!(period(right).0,*second,"{source}"); }
        } }
    }
    #[test]
    fn period_predicates_remain_below_ordinary_predicates_and_above_comparison() {
        assert!(matches!(parse("a = b OVERLAPS c"),ast::Expr::BinaryOp { op: ast::BinaryOperator::Eq,right,.. } if period(&right).0 == "overlaps"));
        let expr = parse("a LIKE 'a' IMMEDIATELY LEADS b");assert!(matches!(period(&expr).1,ast::Expr::Like {..}));
        let expr = parse("a OVERLAPS b LIKE 'b'");assert!(matches!(period(&expr).2,ast::Expr::Like {..}));
        let expr = parse("a | b BETWEEN c + d AND e OVERLAPS f");assert!(matches!(period(&expr).1,ast::Expr::Between {..}));
        assert!(matches!(parse("a->'b' + 1 * 2 BETWEEN 0 AND 3"),ast::Expr::Between {..}));
    }
}

#[cfg(test)]
mod position_tests {
    use super::*;
    fn parse(source: &str) -> ast::Expr {
        let dialect = SqlDialect::default();
        let mut parser = Parser::new(&dialect).try_with_sql(source).unwrap();
        let value = parser.parse_expr().unwrap();parser.expect_token(&Token::EOF).unwrap();value
    }
    fn operands(value: &ast::Expr) -> (&ast::Expr,&ast::Expr) {
        let ast::Expr::BinaryOp { left,op: ast::BinaryOperator::Custom(name),right } = value else { panic!("position: {value:?}") };
        assert!(matches!(name.as_str(),"rtdb.sql/string/position" | "rtdb.sql/string/octet_position"));
        (left,right)
    }
    #[test]
    fn position_needle_accepts_complete_predicates_and_boolean_expressions() {
        assert!(matches!(operands(&parse("POSITION(a = b IN c)")).0,ast::Expr::BinaryOp { op: ast::BinaryOperator::Eq,.. }));
        assert!(matches!(operands(&parse("POSITION(a OR b AND c IN d)")).0,ast::Expr::BinaryOp { op: ast::BinaryOperator::Or,.. }));
        assert!(matches!(operands(&parse("POSITION(a IS NULL IN d)")).0,ast::Expr::IsNull(_)));
        assert!(matches!(operands(&parse("POSITION(NOT a BETWEEN b AND c IN d)")).0,ast::Expr::UnaryOp { op: ast::UnaryOperator::Not,.. }));
    }
    #[test]
    fn position_in_predicates_use_the_last_complete_needle_and_keep_parenthesized_haystacks() {
        let value = parse("POSITION(a IN (b) IN (c))");let (needle,haystack) = operands(&value);
        assert!(matches!(needle,ast::Expr::InList {..}));assert!(matches!(haystack,ast::Expr::Nested(_)));
        let value = parse("POSITION(a IN b IN (c))");let (needle,haystack) = operands(&value);
        assert!(matches!(needle,ast::Expr::Identifier(_)));assert!(matches!(haystack,ast::Expr::InList {..}));
    }
    #[test]
    fn nested_position_case_quoted_tokens_and_function_fallback_keep_the_outer_stream() {
        for source in [
            "POSITION(CASE WHEN a IN (b) THEN 'x' ELSE 'y' END IN ('hay'))",
            "POSITION(POSITION('a' IN ('ba'))::VARCHAR IN '1')",
            "POSITION('IN' IN CASE WHEN TRUE THEN '(' ELSE ')' END)",
            "POSITION(\"IN\" /* IN ) */ IN ('hay') USING OCTETS)",
        ] { operands(&parse(source)); }
        assert!(matches!(parse("POSITION('a','ba')"),ast::Expr::Function(_)));
        let expression = parse("POSITION('a' IN 'ba') + POSITION('b' IN 'bc')");
        let ast::Expr::BinaryOp { left,op: ast::BinaryOperator::Plus,right } = expression else { panic!("addition") };
        operands(&left);operands(&right);
    }
    #[test]
    fn position_token_snapshots_preserve_the_shared_recursion_limit() {
        let source = format!("{}'x'{}","POSITION(".repeat(64)," IN 'x')".repeat(64));
        let dialect = SqlDialect::default();
        let mut parser = Parser::new(&dialect).with_recursion_limit(16).try_with_sql(&source).unwrap();
        assert_eq!(parser.parse_expr().unwrap_err(),ParserError::RecursionLimitExceeded);
        assert!((0..source.len()).any(|index|parser.token_at(index).token == Token::RParen));
    }
    #[test]
    fn failed_delimited_prefix_restores_original_tokens_and_allows_a_different_delimiter() {
        let dialect = SqlDialect::default();
        let mut parser = Parser::new(&dialect).try_with_sql("CASE WHEN a IN (b) THEN 'x' ELSE 'y' END IN 'xy'").unwrap();
        dialect.begin_position();
        let delimiters = position_delimiters(&parser,&dialect).unwrap();
        assert_eq!(delimiters.len(),2);
        assert!(parser.maybe_parse(|parser|parse_before_token(parser,delimiters[0],&dialect)).unwrap().is_none());
        let needle = parse_before_token(&mut parser,delimiters[1],&dialect).unwrap();assert!(matches!(needle,ast::Expr::Case {..}));
        assert_eq!(parser.peek_token().token,Token::make_keyword("IN"));
        parser.expect_keyword_is(Keyword::IN).unwrap();parser.parse_expr().unwrap();parser.expect_token(&Token::EOF).unwrap();
    }
    #[test]
    fn nested_position_retries_share_one_cumulative_work_limit() {
        for case_haystack in [false,true] {
            let mut source = "'x'".to_string();
            for _ in 0..16 {
                source = if case_haystack { format!("POSITION(CAST({source} AS VARCHAR) IN CASE WHEN 'x' IN ('x') THEN 'x' ELSE 'x' END)") }
                    else { format!("POSITION({source} IN 'x' IN ('x'))") };
            }
            let dialect = SqlDialect::default();
            let mut parser = Parser::new(&dialect).with_recursion_limit(512).try_with_sql(&source).unwrap();
            assert_eq!(dialect.checked(parser.parse_expr()).unwrap_err(),position_limit(),"{case_haystack}");
            assert_eq!(dialect.position_work.get(),Some(0));
        }
    }
    #[test]
    fn position_exhaustion_survives_speculation_and_function_fallback() {
        for source in ["POSITION('a' IN 'ba')","POSITION('a','ba')"] {
            let dialect = SqlDialect { position_work: Cell::new(Some(1)) };
            let mut parser = Parser::new(&dialect).try_with_sql(source).unwrap();
            let speculative = parser.maybe_parse(|parser|parser.parse_expr());
            assert_eq!(speculative.unwrap(),None);
            assert_eq!(dialect.checked(Ok(())).unwrap_err(),position_limit());
            assert_eq!(parser.parse_expr().unwrap_err(),position_limit());
        }
    }
    #[test]
    fn position_snapshot_payload_admission_leaves_the_original_stream_intact() {
        let source = format!("'{}' IN 'x'","x".repeat(1024));
        let dialect = SqlDialect { position_work: Cell::new(Some(std::mem::size_of::<TokenWithSpan>() * 8 + 64)) };
        let mut parser = Parser::new(&dialect).try_with_sql(&source).unwrap();
        let delimiter = (0..source.len()).find(|index|parser.token_at(*index).token == Token::make_keyword("IN")).unwrap();
        assert_eq!(parse_before_token(&mut parser,delimiter,&dialect).unwrap_err(),position_limit());
        assert_eq!(parser.index(),0);
        assert!(matches!(&parser.token_at(0).token,Token::SingleQuotedString(value) if value.len() == 1024));
        assert_eq!(parser.token_at(delimiter).token,Token::make_keyword("IN"));
    }
    #[test]
    fn inherited_unary_signs_stop_before_multiplication_and_admit_tighter_postfix_operators() {
        for (source,operator) in [("-a * b",ast::UnaryOperator::Minus),("+a / b",ast::UnaryOperator::Plus)] {
            let ast::Expr::BinaryOp { left,.. } = parse(source) else { panic!("multiplicative expression") };
            assert!(matches!(left.as_ref(),ast::Expr::UnaryOp { op,expr } if op == &operator && matches!(expr.as_ref(),ast::Expr::Identifier(_))));
        }
        let ast::Expr::UnaryOp { expr,.. } = parse("-a->'b'::BIGINT") else { panic!("unary") };
        assert!(matches!(expr.as_ref(),ast::Expr::BinaryOp { op: ast::BinaryOperator::Arrow,.. }));
    }
}

#[cfg(test)]
mod keyword_tier_tests {
    use super::*;
    use std::collections::BTreeSet;

    fn accepted_by(source: impl Fn(&str) -> String) -> BTreeSet<&'static str> {
        crate::lexer::spellings().filter(|(spelling, name)| spelling == name && name.chars().all(|c| c.is_ascii_alphabetic() || c == '_'))
            .map(|(name, _)| name).filter(|word| crate::parse(&source(word)).is_ok()).collect()
    }

    /// The tier's keywords the vocabulary walk enumerates: as in the reference's test, those spelt with letters and underscores.
    fn enumerable_tier() -> BTreeSet<&'static str> {
        NON_RESERVED_KEYWORDS.iter().copied().filter(|word| word.chars().all(|c| c.is_ascii_alphabetic() || c == '_')).collect()
    }

    #[test]
    fn a_correlation_name_accepts_exactly_the_non_reserved_keywords() {
        let tier = enumerable_tier();
        for (position, source) in [("with AS", "SELECT 1 AS r FROM (VALUES (1)) AS "), ("without AS", "SELECT 1 AS r FROM (VALUES (1)) ")] {
            let accepted = accepted_by(|word| format!("{source}{word}"));
            assert_eq!(tier.difference(&accepted).collect::<Vec<_>>(), Vec::<&&str>::new(), "{position}: the tier names a keyword the parser rejects");
            assert_eq!(accepted.difference(&tier).collect::<Vec<_>>(), Vec::<&&str>::new(), "{position}: the parser accepts a reserved keyword");
        }
    }

    #[test]
    fn a_column_alias_also_accepts_current_user_and_default() {
        let tier = enumerable_tier();
        let accepted = accepted_by(|word| format!("SELECT 1 AS {word}"));
        assert_eq!(tier.difference(&accepted).collect::<Vec<_>>(), Vec::<&&str>::new());
        assert_eq!(accepted.difference(&tier).copied().collect::<Vec<_>>(), ["CURRENT_USER", "DEFAULT"]);
    }

    #[test]
    fn an_alternative_spelling_takes_its_keywords_tier() {
        assert!(crate::parse("SELECT 1 AS int8").is_ok());
        assert!(crate::parse("SELECT 1 AS r FROM (VALUES (1)) AS int8").is_ok());
    }

    #[test]
    fn a_from_first_query_does_not_take_its_select_as_an_alias() {
        assert!(crate::parse("FROM (VALUES (1)) t(a) SELECT a").is_ok());
        assert!(crate::parse("SELECT 1 FROM (VALUES (1)) AS select").is_ok());
    }
}
