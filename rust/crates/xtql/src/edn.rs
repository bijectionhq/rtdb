use num_bigint::BigInt;
use num_traits::{ToPrimitive, Zero};
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
                while self.next().is_some_and(|ch| ch != '\n' && ch != '\r') {}
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
            '\\' => EdnKind::Character(self.character()?),
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
                Some('?') => return Err(self.failure("Reader conditionals are not supported")),
                Some('"') => return Err(self.failure("Regex objects are not EDN literals; use a string pattern")),
                Some('<') => return Err(self.failure("Unreadable object form")),
                Some('^') => { self.next(); self.metadata()? }
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
            '^' => self.metadata()?,
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
        while self.peek().is_some_and(|ch| !ch.is_whitespace() && ch != ',' && !(reader_macro(ch) && ch != '#')) { self.next(); }
        &self.source[start..self.offset]
    }

    fn metadata(&mut self) -> Result<EdnKind> {
        self.form()?;
        Ok(self.form()?.kind)
    }

    fn character(&mut self) -> Result<char> {
        let start = self.offset;
        let first = self.next().ok_or_else(|| self.failure("Empty character literal"))?;
        self.token();
        let token = &self.source[start..self.offset];
        if token.len() == first.len_utf8() {
            return Ok(first);
        }
        let numeric = |digits: &str, radix, maximum| {
            let value = digits.chars().try_fold(0, |value, ch| {
                edn_digit(ch, radix).map(|digit| value * radix + digit)
            }).ok_or_else(|| self.failure("Invalid numeric character literal"))?;
            char::from_u32(value).filter(|_| value <= maximum).ok_or_else(|| self.failure("Character literal is out of range"))
        };
        match token {
            "newline" => Ok('\n'),
            "space" => Ok(' '),
            "tab" => Ok('\t'),
            "return" => Ok('\r'),
            "backspace" => Ok('\u{8}'),
            "formfeed" => Ok('\u{c}'),
            token if first == 'u' && token.chars().count() == 5 => numeric(&token[1..], 16, 0xffff),
            token if first == 'o' && token.chars().count() <= 4 => numeric(&token[1..], 8, 0o377),
            _ => Err(self.failure("Invalid character literal")),
        }
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
                    ch if rtdb_types::decimal_digit(ch).is_some() => {
                        let mut value = edn_digit(ch, 8).ok_or_else(|| self.failure("Invalid octal escape"))?;
                        for _ in 1..3 {
                            let Some(next) = self.peek().filter(|ch| !ch.is_whitespace() && *ch != ',' && !reader_macro(*ch)) else { break };
                            value = value * 8 + edn_digit(next, 8).ok_or_else(|| self.failure("Invalid octal escape"))?;
                            self.next();
                        }
                        let byte = u8::try_from(value).map_err(|_| self.failure("Octal escape must be in the range 0 through 377"))?;
                        result.push(char::from(byte));
                    }
                    _ => return Err(self.failure("Unknown string escape")),
                },
                ch => result.push(ch),
            }
        }
    }

    fn unicode_escape(&mut self) -> Result<u32> {
        let mut code = 0;
        for _ in 0..4 { code = code * 16 + self.next().and_then(|ch| edn_digit(ch, 16)).ok_or_else(|| self.failure("Invalid Unicode escape"))?; }
        Ok(code)
    }
}

fn reader_macro(ch: char) -> bool {
    matches!(ch, '(' | ')' | '[' | ']' | '{' | '}' | '"' | ';' | '^' | '\\' | '#')
}

fn edn_digit(ch: char, radix: u32) -> Option<u32> {
    let digit = match ch {
        'Ａ'..='Ｆ' => u32::from(ch) - u32::from('Ａ') + 10,
        'ａ'..='ｆ' => u32::from(ch) - u32::from('ａ') + 10,
        _ => ch.to_digit(16).or_else(|| rtdb_types::decimal_digit(ch).map(u32::from))?,
    };
    (digit < radix).then_some(digit)
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
    if let Ok(parsed) = integer_token(value) {
        return Ok(match parsed.to_i64() { Some(value) => EdnKind::Integer(value), None => EdnKind::BigInteger(parsed.to_string()) });
    }
    if let Some((numerator, denominator)) = value.split_once('/') {
        let unsigned = numerator.strip_prefix(['+', '-']).unwrap_or(numerator);
        if unsigned.is_empty() || denominator.is_empty() || !unsigned.bytes().chain(denominator.bytes()).all(|byte| byte.is_ascii_digit()) {
            return Err("Invalid ratio literal");
        }
        let numerator = numerator.parse::<BigInt>().map_err(|_| "Invalid ratio numerator")?;
        let denominator = denominator.parse::<BigInt>().map_err(|_| "Invalid ratio denominator")?;
        if denominator.is_zero() { return Err("Ratio denominator is zero"); }
        if !(&numerator % &denominator).is_zero() {
            return Err("Non-integral ratio literals are not supported; use a decimal or floating-point value");
        }
        let fixed_width = numerator.to_i64().is_some_and(|value| value != i64::MIN) && denominator.to_i64().is_some();
        let quotient = numerator / denominator;
        return Ok(if fixed_width {
            EdnKind::Integer(quotient.to_i64().ok_or("Ratio result exceeds signed 64 bits")?)
        } else {
            EdnKind::BigInteger(quotient.to_string())
        });
    }
    if value.contains(['.', 'e', 'E']) { return value.parse().map(EdnKind::Float).map_err(|_| "Invalid floating-point literal"); }
    Err("Invalid integer literal")
}

fn integer_token(value: &str) -> std::result::Result<BigInt, &'static str> {
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
    let parsed = BigInt::parse_bytes(digits.as_bytes(), radix).ok_or("Invalid integer literal")?;
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
