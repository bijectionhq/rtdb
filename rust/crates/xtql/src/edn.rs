use rtdb_plan::SourceSpan;
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Edn { pub span: SourceSpan, pub kind: EdnKind }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EdnKind {
    Nil, Bool(bool), Integer(i64), Float(f64), Decimal(String), BigInteger(String),
    String(String), Keyword(String), Symbol(String), Character(char),
    List(Vec<Edn>), Vector(Vec<Edn>), Set(Vec<Edn>), Map(Vec<(Edn, Edn)>),
    Tagged { tag: String, value: Box<Edn> },
    AnonymousFunction(Box<Edn>),
}

impl Edn {
    pub fn symbol(&self) -> Option<&str> { if let EdnKind::Symbol(value) = &self.kind { Some(value) } else { None } }
    pub fn keyword(&self) -> Option<&str> { if let EdnKind::Keyword(value) = &self.kind { Some(value) } else { None } }
    pub fn vector(&self) -> Option<&[Edn]> { if let EdnKind::Vector(values) = &self.kind { Some(values) } else { None } }
    pub fn map(&self) -> Option<&[(Edn, Edn)]> { if let EdnKind::Map(values) = &self.kind { Some(values) } else { None } }
    pub fn option(&self, key: &str) -> Option<&Edn> { self.map()?.iter().find_map(|(name, value)| (name.keyword() == Some(key)).then_some(value)) }
}

/// Reads exactly one EDN form, retaining UTF-8 byte spans and distinctions used by XTQL binding.
/// Reader evaluation is unavailable; nesting deeper than 256 forms is rejected.
pub fn read(source: &str) -> Result<Edn> {
    let mut reader = Reader { source, offset: 0, depth: 0 };
    reader.whitespace()?;
    let value = reader.form()?;
    reader.whitespace()?;
    if reader.offset != source.len() { return Err(reader.failure("Unexpected content after the query form")); }
    Ok(value)
}

struct Reader<'a> { source: &'a str, offset: usize, depth: usize }

impl<'a> Reader<'a> {
    fn failure(&self, message: impl Into<String>) -> Anomaly {
        super::error("xtql/edn-reader", message).with_context("offset", serde_json::json!(self.offset))
    }

    fn peek(&self) -> Option<char> { self.source[self.offset..].chars().next() }
    fn next(&mut self) -> Option<char> { let ch = self.peek()?; self.offset += ch.len_utf8(); Some(ch) }

    fn whitespace(&mut self) -> Result<()> {
        loop {
            while self.peek().is_some_and(|ch| ch.is_whitespace() || ch == ',') { self.next(); }
            if self.peek() == Some(';') {
                while self.next().is_some_and(|ch| ch != '\n') {}
            } else if self.source[self.offset..].starts_with("#_") {
                self.offset += 2;
                self.form()?;
            } else { return Ok(()); }
        }
    }

    fn form(&mut self) -> Result<Edn> {
        if self.depth >= 256 { return Err(self.failure("EDN nesting exceeds 256 forms")); }
        self.depth += 1;
        self.whitespace()?;
        let start = self.offset;
        let ch = self.next().ok_or_else(|| self.failure("Expected an EDN form"))?;
        let kind = match ch {
            '(' => EdnKind::List(self.sequence(')')?),
            '[' => EdnKind::Vector(self.sequence(']')?),
            '{' => self.map(None)?,
            '"' => EdnKind::String(self.string()?),
            ':' => {
                let token = self.token();
                if token.is_empty() || token.starts_with(':') { return Err(self.failure("Keyword requires an explicit name")); }
                EdnKind::Keyword(token.into())
            }
            '\\' => {
                let token = self.token();
                let ch = match token {
                    "newline" => '\n', "space" => ' ', "tab" => '\t', "return" => '\r', "backspace" => '\u{8}', "formfeed" => '\u{c}',
                    value if value.starts_with('u') && value.len() == 5 => u32::from_str_radix(&value[1..], 16).ok().and_then(char::from_u32).ok_or_else(|| self.failure("Invalid Unicode character"))?,
                    value => {
                        let mut chars = value.chars();
                        let ch = chars.next().ok_or_else(|| self.failure("Empty character literal"))?;
                        if chars.next().is_some() { return Err(self.failure("Invalid character literal")); }
                        ch
                    }
                };
                EdnKind::Character(ch)
            }
            '#' => match self.peek() {
                Some(':') => {
                    self.next();
                    let namespace = self.form()?;
                    let EdnKind::Symbol(namespace) = namespace.kind else { return Err(self.failure("Namespaced map requires an unqualified namespace symbol")); };
                    if namespace.contains('/') && namespace != "/" { return Err(self.failure("Namespaced map namespace must be unqualified")); }
                    while self.peek().is_some_and(|ch| ch.is_whitespace() || ch == ',') { self.next(); }
                    if self.next() != Some('{') { return Err(self.failure("Namespaced map requires a map literal")); }
                    self.map(Some(&namespace))?
                }
                Some('{') => {
                    self.next();
                    let values = self.sequence('}')?;
                    for (index, value) in values.iter().enumerate() {
                        if values[..index].iter().any(|existing| same_form(existing, value)) { return Err(self.failure("Duplicate EDN set element")); }
                    }
                    EdnKind::Set(values)
                }
                Some('(') => {
                    let value = self.form()?;
                    EdnKind::AnonymousFunction(Box::new(value))
                }
                Some('#') => {
                    self.next();
                    let value = match self.token() { "Inf" => f64::INFINITY, "-Inf" => f64::NEG_INFINITY, "NaN" => f64::NAN, _ => return Err(self.failure("Unknown symbolic floating-point literal")) };
                    EdnKind::Float(value)
                }
                Some('=') => return Err(self.failure("Reader evaluation is not an EDN operation")),
                _ => {
                    let tag = self.token().to_owned();
                    if tag.is_empty() { return Err(self.failure("Tagged value requires a tag name")); }
                    EdnKind::Tagged { tag, value: Box::new(self.form()?) }
                }
            },
            '\'' => {
                let symbol = Edn { span: SourceSpan { start, end: start + 1 }, kind: EdnKind::Symbol("quote".into()) };
                EdnKind::List(vec![symbol, self.form()?])
            }
            '^' => { self.form()?; let value = self.form()?; value.kind }
            ')' | ']' | '}' => return Err(self.failure("Unexpected closing delimiter")),
            _ => {
                let rest = self.token();
                let token = format!("{ch}{rest}");
                match token.as_str() {
                    "nil" => EdnKind::Nil,
                    "true" => EdnKind::Bool(true),
                    "false" => EdnKind::Bool(false),
                    token if numeric_token(token) => parse_number(token).map_err(|message| self.failure(message))?,
                    _ => EdnKind::Symbol(token),
                }
            }
        };
        self.depth -= 1;
        Ok(Edn { span: SourceSpan { start, end: self.offset }, kind })
    }

    fn map(&mut self, namespace: Option<&str>) -> Result<EdnKind> {
        let mut values = self.sequence('}')?.into_iter();
        let mut pairs: Vec<(Edn, Edn)> = Vec::new();
        while let Some(mut key) = values.next() {
            let value = values.next().ok_or_else(|| self.failure("Map requires an even number of forms"))?;
            if let Some(namespace) = namespace {
                if let EdnKind::Keyword(name) | EdnKind::Symbol(name) = &mut key.kind {
                    if name == "/" || !name.contains('/') { *name = format!("{namespace}/{name}"); }
                    else if let Some(("_", local)) = name.rsplit_once('/') { *name = local.to_owned(); }
                }
            }
            if pairs.iter().any(|(existing, _)| same_form(existing, &key)) { return Err(self.failure("Duplicate EDN map key")); }
            pairs.push((key, value));
        }
        Ok(EdnKind::Map(pairs))
    }

    fn sequence(&mut self, end: char) -> Result<Vec<Edn>> {
        let mut values = Vec::new();
        loop {
            self.whitespace()?;
            match self.peek() {
                Some(ch) if ch == end => { self.next(); return Ok(values); }
                None => return Err(self.failure(format!("Missing closing delimiter {end}"))),
                _ => values.push(self.form()?),
            }
        }
    }

    fn token(&mut self) -> &'a str {
        let start = self.offset;
        while self.peek().is_some_and(|ch| !ch.is_whitespace() && !matches!(ch, ',' | '(' | ')' | '[' | ']' | '{' | '}' | '"' | ';')) { self.next(); }
        &self.source[start..self.offset]
    }

    fn string(&mut self) -> Result<String> {
        let mut result = String::new();
        loop {
            match self.next().ok_or_else(|| self.failure("Unterminated string"))? {
                '"' => return Ok(result),
                '\\' => match self.next().ok_or_else(|| self.failure("Unterminated escape"))? {
                    'n' => result.push('\n'), 'r' => result.push('\r'), 't' => result.push('\t'),
                    'b' => result.push('\u{8}'), 'f' => result.push('\u{c}'), '"' => result.push('"'), '\\' => result.push('\\'),
                    'u' => {
                        let mut code = self.unicode_escape()?;
                        if (0xd800..=0xdbff).contains(&code) {
                            if self.next() != Some('\\') || self.next() != Some('u') { return Err(self.failure("High surrogate requires a following low surrogate")); }
                            let low = self.unicode_escape()?;
                            if !(0xdc00..=0xdfff).contains(&low) { return Err(self.failure("Invalid low surrogate")); }
                            code = 0x10000 + ((code - 0xd800) << 10) + low - 0xdc00;
                        }
                        result.push(char::from_u32(code).ok_or_else(|| self.failure("Unicode escape is not a scalar value"))?);
                    }
                    _ => return Err(self.failure("Unknown string escape")),
                },
                ch => result.push(ch),
            }
        }
    }

    fn unicode_escape(&mut self) -> Result<u32> {
        let mut code = 0;
        for _ in 0..4 { code = code * 16 + self.next().and_then(|ch| ch.to_digit(16)).ok_or_else(|| self.failure("Invalid Unicode escape"))?; }
        Ok(code)
    }
}

fn numeric_token(value: &str) -> bool {
    let value = value.strip_prefix(['+', '-']).unwrap_or(value);
    value.chars().next().and_then(rtdb_types::decimal_digit).is_some()
}

fn parse_number(value: &str) -> std::result::Result<EdnKind, &'static str> {
    if !value.is_ascii() { return Err("Invalid numeric literal"); }
    if let Some(value) = value.strip_suffix('M') {
        if rtdb_types::decimal_text_scale(value).is_err() { return Err("Invalid decimal literal"); }
        return Ok(EdnKind::Decimal(value.into()));
    }
    if let Some(value) = value.strip_suffix('N') { return Ok(EdnKind::BigInteger(integer_token(value)?.to_string())); }
    use num_traits::ToPrimitive;
    if let Ok(parsed) = integer_token(value) {
        return Ok(match parsed.to_i64() { Some(value) => EdnKind::Integer(value), None => EdnKind::BigInteger(parsed.to_string()) });
    }
    if value.contains(['.', 'e', 'E']) { return value.parse().map(EdnKind::Float).map_err(|_| "Invalid floating-point literal"); }
    Err("Invalid integer literal")
}

fn integer_token(value: &str) -> std::result::Result<num_bigint::BigInt, &'static str> {
    let (negative, unsigned) = if let Some(value) = value.strip_prefix('-') { (true, value) } else { (false, value.strip_prefix('+').unwrap_or(value)) };
    let (digits, radix) = if let Some(hex) = unsigned.strip_prefix("0x").or_else(|| unsigned.strip_prefix("0X")) { (hex, 16) }
        else if let Some((radix, digits)) = unsigned.split_once(['r', 'R']) {
            if radix.is_empty() || radix.len() > 2 || radix.starts_with('0') || !radix.bytes().all(|byte| byte.is_ascii_digit()) { return Err("Invalid integer radix"); }
            let radix: u32 = radix.parse().map_err(|_| "Invalid integer radix")?;
            if !(2..=36).contains(&radix) { return Err("Integer radix must be between 2 and 36"); }
            (digits, radix)
        }
        else if unsigned.len() > 1 && unsigned.starts_with('0') { (&unsigned[1..], 8) }
        else { (unsigned, 10) };
    if digits.is_empty() || !digits.chars().all(|ch| ch.is_ascii_alphanumeric() && ch.to_digit(radix).is_some()) { return Err("Invalid integer literal"); }
    let parsed = num_bigint::BigInt::parse_bytes(digits.as_bytes(), radix).ok_or("Invalid integer literal")?;
    Ok(if negative { -parsed } else { parsed })
}

fn same_form(left: &Edn, right: &Edn) -> bool {
    match (&left.kind, &right.kind) {
        (EdnKind::Integer(a), EdnKind::BigInteger(b)) | (EdnKind::BigInteger(b), EdnKind::Integer(a)) => b.parse::<i64>().ok() == Some(*a),
        (EdnKind::Decimal(a), EdnKind::Decimal(b)) => a.parse::<bigdecimal::BigDecimal>().ok().zip(b.parse::<bigdecimal::BigDecimal>().ok()).is_some_and(|(a, b)| a == b),
        (EdnKind::List(a) | EdnKind::Vector(a), EdnKind::List(b) | EdnKind::Vector(b)) => a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same_form(a, b)),
        (EdnKind::Set(a), EdnKind::Set(b)) => a.len() == b.len() && a.iter().all(|value| b.iter().any(|other| same_form(value, other))),
        (EdnKind::Map(a), EdnKind::Map(b)) => a.len() == b.len() && a.iter().all(|(key, value)| b.iter().any(|(other_key, other_value)| same_form(key, other_key) && same_form(value, other_value))),
        (EdnKind::Tagged { tag: a, value: av }, EdnKind::Tagged { tag: b, value: bv }) => a == b && same_form(av, bv),
        (a, b) => a == b,
    }
}
