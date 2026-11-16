use crate::{EvalContext, numeric::overflow};
use fancy_regex::{Expr, Regex, RegexBuilder};
use rtdb_types::{Anomaly, Result, Value};
use super::{add, illegal, java_unicode, text};

const PATTERN_BYTES: usize = 65_536;
const STACK_BRANCHES: usize = 512;
const BACKTRACKS: usize = 100_000;
const DELEGATE_BYTES: usize = 262_144;
const DFA_BYTES: usize = 65_536;

#[derive(Clone, Copy, Default)]
struct Flags { insensitive: bool, unicode_case: bool, unicode_class: bool, multiline: bool, dotall: bool, unix: bool, comments: bool, canonical: bool }

struct Translation { pattern: String, names: Vec<Option<String>>, supplementary: bool }
struct Translator<'a> { source: &'a str, position: usize, flags: Flags, scopes: Vec<Flags>, output: String, names: Vec<Option<String>>, literals: Vec<u32>, supplementary: bool, predicate_bmp: bool }
pub(crate) struct Pattern { regex: Regex, names: Vec<Option<String>> }
pub(crate) struct LikePattern { pattern: Pattern, binary: bool }
pub(crate) struct CompiledReplacement { pattern: Pattern,replacement: String }

pub(crate) fn compile(pattern: &str, flags: &str, context: &EvalContext<'_>) -> Result<Pattern> {
    if pattern.len() > PATTERN_BYTES { return Err(limit("Pattern exceeds 64 KiB")); }
    context.reserve(pattern.len().checked_mul(16_384).map(|bytes| bytes.min(PATTERN_BYTES * 64)).and_then(|bytes| bytes.checked_mul(8)).and_then(|bytes| bytes.checked_add(524_288)).ok_or_else(overflow)?)?;
    let mut options = Flags::default();
    for flag in flags.chars() { match flag {
        'i' => { options.insensitive = true; options.unicode_case = true; },
        's' => options.dotall = true, 'm' => options.multiline = true, _ => {},
    } }
    let source = quoted_source(pattern);
    let translation = Translator { source: &source, position: 0, flags: options, scopes: Vec::new(), output: String::new(), names: Vec::new(), literals: Vec::new(), supplementary: source.chars().any(|character| character.len_utf16() == 2), predicate_bmp: true }.translate()?;
    context.reserve(translation.pattern.len().checked_mul(256).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(overflow)?)?;
    let tree = Expr::parse_java_tree(&translation.pattern).map_err(regex_error)?;
    let mut metrics = Metrics::default();
    metrics.visit(&tree.expr, 0)?;
    let compilation = metrics.delegates.checked_mul((DELEGATE_BYTES + DFA_BYTES) * 4).and_then(|bytes| bytes.checked_add(translation.pattern.len().checked_mul(256)?)).ok_or_else(overflow)?;
    let slots = metrics.nodes.checked_mul(4).and_then(|slots| slots.checked_add(64)).ok_or_else(overflow)?;
    let matching = (STACK_BRANCHES + 1).checked_mul(slots.checked_mul(32).and_then(|bytes| bytes.checked_add(64)).ok_or_else(overflow)?).ok_or_else(overflow)?;
    context.reserve(compilation.checked_add(matching).ok_or_else(overflow)?)?;
    context.reserve(BACKTRACKS * 64 + STACK_BRANCHES * 1024)?;
    let regex = RegexBuilder::new(&translation.pattern).java_utf16(true).java_supplementary(translation.supplementary).backtrack_limit(BACKTRACKS).backtrack_stack_limit(STACK_BRANCHES)
        .delegate_size_limit(DELEGATE_BYTES).delegate_dfa_size_limit(DFA_BYTES).build().map_err(regex_error)?;
    Ok(Pattern { regex, names: translation.names })
}

fn quoted_source(pattern: &str) -> String {
    let mut source = String::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();
    let mut quoted = false;
    let mut first = false;
    while let Some(character) = chars.next() {
        if !quoted {
            if character == '\\' && chars.peek() == Some(&'Q') {
                chars.next(); quoted = true; first = true; continue;
            }
            source.push(character);
            if character == '\\' { if let Some(next) = chars.next() { source.push(next); } }
        } else if character == '\\' && chars.peek() == Some(&'E') {
            chars.next(); quoted = false;
        } else if !character.is_ascii() || character.is_ascii_alphabetic() {
            source.push(character);
        } else if character.is_ascii_digit() {
            if first { source.push_str("\\x3"); }
            source.push(character);
        } else {
            source.push('\\');
            source.push(character);
        }
        first = false;
    }
    source
}

#[derive(Default)]
struct Metrics { nodes: usize, delegates: usize }
impl Metrics {
    fn visit(&mut self, expression: &Expr, depth: usize) -> Result<()> {
        if depth > 128 { return Err(limit("Pattern nesting exceeds 128 levels")); }
        self.nodes = self.nodes.checked_add(1).ok_or_else(overflow)?;
        match expression {
            Expr::Concat(children) | Expr::Alt(children) => for child in children { self.visit(child, depth + 1)?; },
            Expr::Group(child) | Expr::JavaGroup(child) | Expr::JavaAtom(child) => self.visit(child, depth + 1)?,
            Expr::JavaPossessive(child) | Expr::LookAround(child, _) | Expr::JavaLookBehind(child, _) | Expr::AtomicGroup(child) => self.visit(child, depth + 1)?,
            Expr::CanonicalClass(child) => { self.delegates = self.delegates.checked_add(1).ok_or_else(overflow)?;self.visit(child,depth + 1)?; },
            Expr::Repeat { child, lo, hi, .. } => {
                if *lo > 10_000 || (*hi != usize::MAX && *hi > 10_000) { return Err(limit("Counted repetition exceeds 10000")); }
                self.visit(child, depth + 1)?;
            },
            Expr::Delegate {..} => self.delegates = self.delegates.checked_add(1).ok_or_else(overflow)?,
            Expr::Backref { .. } | Expr::BackrefAscii { .. } | Expr::ContinueFromPreviousMatchEnd | Expr::Grapheme | Expr::GraphemeBoundary | Expr::LineEnding | Expr::Empty | Expr::Any { .. } | Expr::Assertion(_) | Expr::Literal { .. } => {},
            _ => return Err(unsupported("Regex construct is not part of Java Pattern")),
        }
        Ok(())
    }
}

impl Translator<'_> {
    fn peek(&self) -> Option<char> { self.source[self.position..].chars().next() }
    fn next(&mut self) -> Option<char> { let value = self.peek()?; self.position += value.len_utf8(); Some(value) }
    fn take(&mut self, value: &str) -> bool { if self.source[self.position..].starts_with(value) { self.position += value.len(); true } else { false } }
    fn skip_ignored(&mut self) {
        while self.flags.comments {
            match self.peek() {
                Some(character @ (' ' | '\t' | '\n' | '\x0b' | '\x0c' | '\r' | '#')) => { self.next(); self.comment(character); }
                _ => break,
            }
        }
    }
    fn pattern_peek(&mut self) -> Option<char> { self.skip_ignored(); self.peek() }
    fn pattern_next(&mut self) -> Option<char> { self.skip_ignored(); self.next() }
    fn translate(mut self) -> Result<Translation> {
        while let Some(character) = self.next() {
            if self.flags.comments && self.comment(character) { continue; }
            if matches!(character, '[' | '(' | ')' | '.' | '^' | '$' | '|' | '{' | '*' | '+' | '?') {
                self.flush_literals(matches!(character, '{' | '*' | '+' | '?'));
            }
            match character {
                '\\' => {
                    let escaped = self.escape(false, false)?;
                    if let Some(value) = class_codepoint(&escaped) { self.literals.push(value); }
                    else { self.flush_literals(false); self.output.push_str(&escaped); }
                },
                '[' => { let class = self.class(0)?; if !self.flags.canonical && !self.predicate_bmp { self.supplementary = true; } self.output.push_str(&canonical_class(class, self.flags.canonical)); },
                '(' => self.group()?,
                ')' => { self.flags = self.scopes.pop().ok_or_else(|| illegal("Unmatched closing ')'"))?; self.output.push(')'); },
                '.' => self.output.push_str(if self.flags.dotall { "[\\x00-\\x{10ffff}]" } else if self.flags.unix { "[^\\n]" } else { "[^\\n\\r\\x{85}\\x{2028}\\x{2029}]" }),
                '^' => self.output.push_str(&opaque(if !self.flags.multiline { "\\A" } else if self.flags.unix { "(?:(?<=\\n)|\\A)(?!\\z)" } else { "(?:\\A|(?<=[\\n\\x{85}\\x{2028}\\x{2029}])|(?<=\\r)(?!\\n))(?!\\z)" })),
                '$' => self.output.push_str(&opaque(end_anchor(self.flags.multiline, self.flags.unix))),
                '{' => self.repetition()?,
                '*' | '+' | '?' | '|' => self.output.push(character),
                _ => self.literals.push(character as u32),
            }
            if self.output.len() > PATTERN_BYTES * 64 { return Err(limit("Translated pattern exceeds its bound")); }
        }
        self.flush_literals(false);
        if self.output.len() > PATTERN_BYTES * 64 { return Err(limit("Translated pattern exceeds its bound")); }
        if !self.scopes.is_empty() { return Err(illegal("Unclosed group")); }
        Ok(Translation { pattern: self.output, names: self.names, supplementary: self.supplementary })
    }
    fn comment(&mut self, character: char) -> bool {
        if matches!(character, ' ' | '\t' | '\n' | '\x0b' | '\x0c' | '\r') { return true; }
        if character == '#' {
            while let Some(next) = self.peek() {
                if matches!(next, '\0' | '\n') || (!self.flags.unix && matches!(next, '\r' | '\u{85}' | '\u{2028}' | '\u{2029}')) { break; }
                self.next();
            }
            return true;
        }
        false
    }
    fn literal(&self, value: u32) -> String {
        let literal = format!("\\x{{{value:x}}}");
        let Some(value) = char::from_u32(value) else { return literal; };
        if !self.flags.insensitive { literal }
        else if self.flags.unicode_case { format!("[{}]", java_unicode::literal_single(value)) }
        else if value.is_ascii_alphabetic() { format!("[{}{}]", value.to_ascii_lowercase(), value.to_ascii_uppercase()) }
        else { literal }
    }
    fn flush_literals(&mut self, quantified: bool) {
        let last = if quantified && self.literals.len() > 1 { self.literals.pop() } else { None };
        let slice = self.literals.len() > 1 && self.flags.insensitive && self.flags.unicode_case;
        if self.literals.len() == 1 && !java_unicode::single_is_bmp(self.literals[0], self.flags.insensitive, self.flags.unicode_case) { self.supplementary = true; }
        let source_slice = self.literals.len() > 1;
        if source_slice { self.output.push_str("(?o:"); }
        for index in 0..self.literals.len() {
            let value = self.literals[index];
            let translated = if slice && char::from_u32(value).is_some() { format!("[{}]", java_unicode::folded_literal(value)) } else { self.literal(value) };
            self.output.push_str(&translated);
        }
        if source_slice { self.output.push(')'); }
        self.literals.clear();
        if let Some(value) = last {
            if !java_unicode::single_is_bmp(value, self.flags.insensitive, self.flags.unicode_case) { self.supplementary = true; }
            self.output.push_str(&self.literal(value));
        }
    }
    fn repetition(&mut self) -> Result<()> {
        if !self.peek().is_some_and(|value| value.is_ascii_digit()) { return Err(illegal("Illegal repetition")); }
        self.output.push('{');
        while let Some(character) = self.pattern_peek().filter(|value| value.is_ascii_digit() || *value == ',') {
            self.next(); self.output.push(character);
        }
        if !self.take("}") { return Err(illegal("Illegal repetition")); }
        self.output.push('}');
        Ok(())
    }
    fn group(&mut self) -> Result<()> {
        if self.scopes.len() >= 128 { return Err(limit("Pattern nesting exceeds 128 levels")); }
        let previous = self.flags;
        self.skip_ignored();
        if self.take("?") {
            for prefix in ["=", "!", ">", ":"] {
                if self.take(prefix) { self.scopes.push(previous); self.output.push_str("(?"); self.output.push_str(if prefix == ":" { "j:" } else { prefix }); return Ok(()); }
            }
            if self.take("<") {
                if let Some(character @ ('=' | '!')) = self.pattern_peek() {
                    self.next(); self.scopes.push(previous);
                    self.output.push_str(if self.source[self.position..].chars().any(|character| character.len_utf16() == 2) { "(?j<" } else { "(?<" });
                    self.output.push(character); return Ok(());
                }
                let name = self.name('>')?;
                if self.names.iter().flatten().any(|existing| *existing == name) { return Err(illegal("Named capturing group is already defined")); }
                self.capture(Some(name))?;
            } else {
                let mut enabled = true;
                loop {
                    match self.pattern_next().ok_or_else(|| illegal("Unclosed flag group"))? {
                        '-' if enabled => enabled = false,
                        ')' => return Ok(()),
                        ':' => { self.scopes.push(previous); self.output.push_str("(?j:"); return Ok(()); },
                        flag => { match flag {
                            'i' => self.flags.insensitive = enabled, 'u' => self.flags.unicode_case = enabled,
                            'U' => { self.flags.unicode_class = enabled; self.flags.unicode_case = enabled; },
                            'm' => self.flags.multiline = enabled, 's' => self.flags.dotall = enabled,
                            'd' => self.flags.unix = enabled, 'x' => self.flags.comments = enabled,
                            'c' => self.flags.canonical = enabled,
                            _ => return Err(illegal("Unknown inline modifier")),
                        } },
                    }
                }
            }
        } else { self.capture(None)?; }
        self.scopes.push(previous);
        Ok(())
    }
    fn capture(&mut self, name: Option<String>) -> Result<()> {
        if self.names.len() >= 128 { return Err(limit("Pattern exceeds 128 capture groups")); }
        self.names.push(name);
        self.output.push_str(&format!("(?<rtdb{}>", self.names.len()));
        Ok(())
    }
    fn name(&mut self, end: char) -> Result<String> {
        let mut name = String::new();
        while let Some(character) = self.pattern_next() {
            if character == end {
                if name.as_bytes().first().is_some_and(u8::is_ascii_alphabetic) { return Ok(name); }
                break;
            }
            if !character.is_ascii_alphanumeric() { break; }
            name.push(character);
        }
        Err(illegal("Invalid named capturing group"))
    }
    fn backref(&self, index: usize) -> Result<String> {
        let reference = format!("\\k<{index}>");
        Ok(if !self.flags.insensitive { reference }
            else if self.flags.unicode_case { opaque(&format!("(?i:{reference})")) }
            else { opaque(&format!("(?i-u:{reference})")) })
    }
    fn escape(&mut self, class: bool, range: bool) -> Result<String> {
        let character = self.next().ok_or_else(|| illegal("Trailing backslash"))?;
        self.predicate_bmp = true;
        let result = match character {
            'd' | 'D' | 's' | 'S' | 'w' | 'W' => {
                let inverse = character.is_ascii_uppercase();
                self.predicate_bmp = !inverse && !self.flags.unicode_class;
                let property = match character.to_ascii_lowercase() { 'd' => "Digit", 's' => "Space", 'w' => "IsWord", _ => unreachable!() };
                let body = if property == "IsWord" && !self.flags.unicode_class { "a-zA-Z_0-9".into() }
                    else { java_unicode::property_class_utf16(property, self.flags.unicode_class, false, true)? };
                format!("[{}{body}]", if inverse { "^" } else { "" })
            },
            'h' | 'H' => { self.predicate_bmp = character == 'h'; format!("[{} \\t\\x{{a0}}\\x{{1680}}\\x{{180e}}\\x{{2000}}-\\x{{200a}}\\x{{202f}}\\x{{205f}}\\x{{3000}}]", if character == 'H' { "^" } else { "" }) },
            'v' if range => "\\x0b".into(),
            'v' | 'V' => { self.predicate_bmp = character == 'v'; format!("[{}\\n\\x0b\\f\\r\\x{{85}}\\x{{2028}}\\x{{2029}}]", if character == 'V' { "^" } else { "" }) },
            'R' if !class => "\\R".into(),
            'b' | 'B' if !class => {
                let saved = self.position;
                if character == 'b' && self.pattern_next() == Some('{') && self.pattern_next() == Some('g') {
                    if self.pattern_next() != Some('}') { return Err(illegal("Illegal grapheme boundary escape")); }
                    "\\b{g}".into()
                } else {
                    self.position = saved;
                    opaque(&word_boundary(character == 'B', self.flags.unicode_class)?)
                }
            },
            'A' | 'z' if !class => format!("\\{character}"),
            'G' if !class => "\\G".into(),
            'Z' if !class => opaque(end_anchor(false, self.flags.unix)),
            'k' if !class => {
                if self.pattern_next() != Some('<') { return Err(illegal("Named backreference is missing '<'")); }
                let name = self.name('>')?;
                let index = self.names.iter().position(|candidate| candidate.as_deref() == Some(name.as_str())).ok_or_else(|| illegal("Named capturing group does not exist"))? + 1;
                self.backref(index)?
            },
            '1'..='9' if !class => {
                let mut group = character as usize - '0' as usize;
                while let Some(next) = self.pattern_peek().filter(char::is_ascii_digit) {
                    let next_group = group.checked_mul(10).and_then(|value| value.checked_add(next as usize - '0' as usize)).ok_or_else(overflow)?;
                    if next_group > self.names.len() { break; }
                    group = next_group; self.next();
                }
                self.backref(group)?
            },
            'p' | 'P' => {
                self.skip_ignored();
                let property = if self.take("{") {
                    self.skip_ignored();
                    let start = self.position;
                    loop {
                        match self.pattern_next() {
                            Some('}') => break,
                            Some(_) => {},
                            None => return Err(illegal("Unclosed character property")),
                        }
                    }
                    self.source[start..self.position - 1].to_owned()
                } else { self.pattern_next().ok_or_else(|| illegal("Missing character property"))?.to_string() };
                let body = java_unicode::property_class_utf16(&property, self.flags.unicode_class, self.flags.insensitive, true)?;
                self.predicate_bmp = character == 'p' && java_unicode::property_is_bmp(&property, self.flags.unicode_class);
                if character == 'P' { self.supplementary = true; }
                canonical_class(format!("[{}{body}]", if character == 'P' { "^" } else { "" }), self.flags.canonical && !class)
            },
            'x' | 'u' => {
                let mut value = if character == 'x' && self.pattern_peek() == Some('{') {
                    self.next();
                    if !self.pattern_peek().is_some_and(|value| value.is_ascii_hexdigit()) { return Err(illegal("Illegal hexadecimal escape")); }
                    let mut value = 0u32;
                    while let Some(digit) = self.pattern_peek().filter(|value| value.is_ascii_hexdigit()) {
                        self.next();
                        value = value.checked_mul(16).and_then(|value| value.checked_add(digit.to_digit(16)?)).filter(|value| *value <= 0x10ffff).ok_or_else(|| illegal("Hexadecimal codepoint is too big"))?;
                    }
                    if self.pattern_next() != Some('}') { return Err(illegal("Unclosed hexadecimal escape")); }
                    value
                } else { self.hex(if character == 'u' { 4 } else { 2 })? };
                if character == 'u' && (0xd800..=0xdbff).contains(&value) {
                    let saved = self.position;
                    if self.pattern_next() == Some('\\') && self.pattern_next() == Some('u') {
                        let low = self.hex(4)?;
                        if (0xdc00..=0xdfff).contains(&low) { value = 0x10000 + ((value - 0xd800) << 10) + low - 0xdc00; }
                        else { self.position = saved; }
                    } else { self.position = saved; }
                }
                format!("\\x{{{value:x}}}")
            },
            '0' => {
                let first = self.pattern_next().filter(|value| ('0'..='7').contains(value)).ok_or_else(|| illegal("Illegal octal escape"))?;
                let mut value = first as u32 - '0' as u32;
                for _ in 0..if first <= '3' { 2 } else { 1 } { if let Some(next) = self.pattern_peek().filter(|value| ('0'..='7').contains(value)) { self.next(); value = value * 8 + next as u32 - '0' as u32; } else { break; } }
                format!("\\x{{{value:x}}}")
            },
            'c' => { let value = self.pattern_next().ok_or_else(|| illegal("Illegal control escape"))? as u32 ^ 64; format!("\\x{{{value:x}}}") },
            'a' => "\\x07".into(), 'e' => "\\x1b".into(), 't' => "\\t".into(), 'n' => "\\n".into(), 'r' => "\\r".into(), 'f' => "\\f".into(),
            'X' if !class => "\\X".into(),
            'N' => {
                if self.pattern_next() != Some('{') { return Err(illegal("Illegal character name escape sequence")); }
                let start = self.position;
                while self.peek().is_some_and(|value| value != '}') { self.next(); }
                let name = &self.source[start..self.position];
                let value = java_unicode::named_character(name).ok_or_else(|| illegal("Unknown character name"))?;
                if !self.take("}") { return Err(illegal("Unclosed character name escape sequence")); }
                format!("\\x{{{value:x}}}")
            },
            value if value.is_ascii_alphabetic() || value.is_ascii_digit() => return Err(illegal("Illegal escape sequence")),
            value => format!("\\x{{{:x}}}", value as u32),
        };
        if !class && !self.predicate_bmp && !(matches!(character, 'p' | 'P') && self.flags.canonical) { self.supplementary = true; }
        if result.starts_with("\\x{") && class_character(&result).is_none() { self.predicate_bmp = false; }
        Ok(result)
    }
    fn hex(&mut self, length: usize) -> Result<u32> {
        let mut value = 0;
        for _ in 0..length {
            let digit = self.pattern_next().filter(|value| value.is_ascii_hexdigit()).ok_or_else(|| illegal("Invalid hexadecimal escape"))?;
            value = value * 16 + digit.to_digit(16).ok_or_else(overflow)?;
        }
        Ok(value)
    }
    fn class(&mut self, depth: usize) -> Result<String> {
        if depth >= 128 { return Err(limit("Character class nesting exceeds 128 levels")); }
        let mut result = String::from("[");
        let negated = self.take("^");
        if negated { result.push('^'); }
        let mut bmp = true;
        let mut first = true;
        loop {
            let character = self.next().ok_or_else(|| illegal("Unclosed character class"))?;
            if self.flags.comments && self.comment(character) { continue; }
            match character {
                ']' if !first => { result.push(']'); self.predicate_bmp = bmp && !negated; return Ok(result); },
                '[' => { result.push_str(&self.class(depth + 1)?); bmp &= self.predicate_bmp; },
                '&' if self.pattern_peek() == Some('&') => { self.next(); result.push_str("&&"); },
                value => {
                    let range = self.source[self.position..].chars().nth(1) == Some('-');
                    let atom = if value == '\\' { self.escape(true, range)? } else { format!("\\x{{{:x}}}", value as u32) };
                    if value == '\\' && class_character(&atom).is_none() { bmp &= self.predicate_bmp; }
                    if let Some(start) = class_codepoint(&atom) {
                        if self.pattern_peek() == Some('-') && !matches!(self.source[self.position + 1..].chars().next(), Some(']' | '[') | None) {
                            self.next();
                            let end = self.pattern_next().ok_or_else(overflow)?;
                            let end = if end == '\\' { class_codepoint(&self.escape(true, true)?).ok_or_else(|| illegal("Illegal character range"))? } else { end as u32 };
                            if start > end { return Err(illegal("Illegal character range")); }
                            bmp &= !self.flags.insensitive && (end < 0xd800 || start > 0xdfff && end < 0x10000);
                            result.push_str(&java_unicode::class_range_utf16(start, end, self.flags.insensitive, self.flags.unicode_case));
                        } else {
                            let bits = start < 256 && !(self.flags.insensitive && self.flags.unicode_case && matches!(start, 0xff | 0xb5 | 0x49 | 0x69 | 0x53 | 0x73 | 0x4b | 0x6b | 0xc5 | 0xe5));
                            bmp &= bits || java_unicode::single_is_bmp(start, self.flags.insensitive, self.flags.unicode_case);
                            if let Some(start) = char::from_u32(start) { result.push_str(&class_single(start, self.flags)?); }
                            else { result.push_str(&atom); }
                        }
                    } else { result.push_str(&atom); }
                },
            }
            if result.len() > PATTERN_BYTES * 64 { return Err(limit("Translated character class exceeds its bound")); }
            first = false;
        }
    }

}

fn class_character(value: &str) -> Option<char> {
    class_codepoint(value).and_then(char::from_u32)
}

fn class_codepoint(value: &str) -> Option<u32> {
    if let Some(hex) = value.strip_prefix("\\x{").and_then(|value| value.strip_suffix('}')) {
        return u32::from_str_radix(hex, 16).ok();
    }
    if value.len() == 4 && value.starts_with("\\x") { return u32::from_str_radix(&value[2..], 16).ok(); }
    match value { "\\t" => Some(9), "\\n" => Some(10), "\\r" => Some(13), "\\f" => Some(12), _ => None }
}

fn opaque(atom: &str) -> String { format!("(?o:{atom})") }

fn canonical_class(class: String, enabled: bool) -> String {
    if enabled { format!("(?n:{class})") } else { class }
}

fn class_range(start: char, end: char, flags: Flags) -> Result<String> {
    if start > end { return Err(illegal("Illegal character range")); }
    if flags.insensitive && flags.unicode_case { return Ok(java_unicode::class_range(start, end)); }
    use regex_syntax::hir::{ClassUnicode, ClassUnicodeRange};
    let mut ranges = ClassUnicode::new([ClassUnicodeRange::new(start, end)]);
    if flags.insensitive {
        for value in 'A'..='Z' {
            let lower = value.to_ascii_lowercase();
            if start <= value && value <= end { ranges.push(ClassUnicodeRange::new(lower, lower)); }
            if start <= lower && lower <= end { ranges.push(ClassUnicodeRange::new(value, value)); }
        }
    }
    let mut result = String::new();
    for range in ranges.iter() {
        result.push_str(&format!("\\x{{{:x}}}", range.start() as u32));
        if range.start() != range.end() { result.push_str(&format!("-\\x{{{:x}}}", range.end() as u32)); }
    }
    Ok(result)
}

fn class_single(value: char, flags: Flags) -> Result<String> {
    if flags.insensitive && flags.unicode_case { Ok(java_unicode::class_single(value)) }
    else { class_range(value, value, flags) }
}

fn end_anchor(multiline: bool, unix: bool) -> &'static str {
    match (multiline, unix) {
        (true, true) => "(?=\\n|\\z)",
        (false, true) => "(?=\\n?\\z)",
        (true, false) => "(?:\\z|(?=\\r|[\\x{85}\\x{2028}\\x{2029}])|(?<!\\r)(?=\\n))",
        (false, false) => "(?:\\z|(?=\\r\\n\\z)|(?<!\\r)(?=\\n\\z)|(?=[\\r\\x{85}\\x{2028}\\x{2029}]\\z))",
    }
}

fn word_boundary(inverse: bool, unicode: bool) -> Result<String> {
    let (left, right) = if unicode {
        let word = format!("[{}]", property_class("IsWord", false, false)?);
        (format!("(?j<={word})"), format!("(?={word})"))
    } else {
        let base = format!("[[{}]&&[\\x00-\\x{{ffff}}]]", property_class("javaLetterOrDigit", false, false)?);
        let marks = property_class("Mn", false, false)?;
        let mark = format!("[[{marks}]&&[\\x00-\\x{{ffff}}]]");
        (format!("(?:(?<=[a-zA-Z_0-9])|(?<={base}{mark}+))"),
         format!("(?:(?=[a-zA-Z_0-9])|(?=[{marks}])(?<={base}{mark}*))"))
    };
    Ok(if inverse { format!("(?:{left}{right}|(?!{left})(?!{right}))") }
    else { format!("(?:{left}(?!{right})|(?!{left}){right})") })
}

fn property_class(name: &str, unicode: bool, insensitive: bool) -> Result<String> {
    java_unicode::property_class(name, unicode, insensitive)
}

impl Pattern {
    pub(crate) fn search(&self,source: &str,context: &EvalContext<'_>) -> Result<bool> {
        context.reserve(source.len().checked_mul(32).and_then(|bytes| bytes.checked_add(8192)).ok_or_else(overflow)?)?;
        self.regex.is_match(source).map_err(regex_error)
    }
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8;16] = b"0123456789abcdef";
    let mut text = String::new();
    for byte in bytes { text.push(HEX[(byte >> 4) as usize] as char);text.push(HEX[(byte & 15) as usize] as char); }
    text
}

pub(crate) fn compile_like(needle: &Value,escape: Option<char>,insensitive: bool,context: &EvalContext<'_>) -> Result<LikePattern> {
    context.reserve(needle.retained_bytes()?.checked_mul(16).ok_or_else(overflow)?)?;
    let binary = matches!(needle,Value::Binary(_) | Value::FixedSizeBinary(_));
    let needle_hex;
    let needle = match needle {
        Value::Utf8(needle) => needle.as_str(),
        Value::Binary(needle) | Value::FixedSizeBinary(needle) if !insensitive => {
            needle_hex = hex(needle).replace("25","%").replace("5f","__");
            needle_hex.as_str()
        }
        _ => return Err(crate::numeric::inapplicable(if insensitive { "ilike" } else { "like" },&[needle])),
    };
    let mut regex = String::from("^");
    let mut chars = needle.chars();
    while let Some(character) = chars.next() {
        if Some(character) == escape {
            let escaped = chars.next().ok_or_else(|| Anomaly::incorrect("xtdb/invalid-like-pattern","LIKE pattern ends with its escape character"))?;
            regex.push_str(&regex::escape(&escaped.to_string()));
        } else { match character { '%' => regex.push_str(".*"),'_' => regex.push('.'),_ => regex.push_str(&regex::escape(&character.to_string())) } }
    }
    regex.push_str("\\z");
    Ok(LikePattern { pattern: compile(&regex,if insensitive { "i" } else { "" },context)?,binary })
}

impl LikePattern {
    pub(crate) fn search(&self,value: &Value,negated: bool,context: &EvalContext<'_>) -> Result<Value> {
        context.reserve(value.retained_bytes()?.checked_mul(16).ok_or_else(overflow)?)?;
        let source_hex;
        let source = match value {
            Value::Utf8(source) if !self.binary => source.as_str(),
            Value::Binary(source) | Value::FixedSizeBinary(source) if self.binary => { source_hex = hex(source);source_hex.as_str() }
            _ => return Err(crate::numeric::inapplicable("like",&[value])),
        };
        Ok(Value::Bool(self.pattern.search(source,context)? != negated))
    }
}

pub(crate) fn like(value: &Value,needle: &Value,escape: Option<char>,insensitive: bool,negated: bool,context: &EvalContext<'_>) -> Result<Value> {
    if crate::compare::nullish(value) || crate::compare::nullish(needle) { return Ok(Value::Null); }
    compile_like(needle,escape,insensitive,context)?.search(value,negated,context)
}

pub(crate) fn matches(args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    let pattern = compile(text(&args[1])?,text(&args[2])?,context)?;
    pattern.search(text(&args[0])?,context).map(Value::Bool)
}

pub(crate) fn replace(args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    let source = text(&args[0])?;
    compile_replacement(text(&args[1])?,text(&args[2])?,args.get(3).map(text).transpose()?.unwrap_or(""),context)?.replace(source,context)
}

pub(crate) fn compile_replacement(pattern: &str,replacement: &str,flags: &str,context: &EvalContext<'_>) -> Result<CompiledReplacement> {
    context.reserve(replacement.len().checked_mul(2).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?)?;
    let replacement = postgres_backrefs(replacement);
    Ok(CompiledReplacement { pattern: compile(pattern,flags,context)?,replacement })
}

impl CompiledReplacement {
    pub(crate) fn replace(&self,source: &str,context: &EvalContext<'_>) -> Result<Value> {
        replace_utf16(source,&self.pattern,&self.replacement,context)
    }
}

fn postgres_backrefs(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '\\' {
            if chars.peek() == Some(&'\\') { chars.next(); result.push_str("\\\\"); continue; }
            if chars.peek().is_some_and(char::is_ascii_digit) { result.push('$'); while chars.peek().is_some_and(char::is_ascii_digit) { result.push(chars.next().expect("peeked digit")); } continue; }
        }
        result.push(character);
    }
    result
}

enum ReplacementPart<'a> { Text(&'a str), Group(usize) }

fn replacement_tokens<'a>(value: &'a str, pattern: &Pattern, groups: usize, mut emit: impl FnMut(ReplacementPart<'a>) -> Result<()>) -> Result<()> {
    let mut cursor = 0usize;
    while cursor < value.len() {
        let character = value[cursor..].chars().next().ok_or_else(overflow)?;
        cursor += character.len_utf8();
        match character {
            '\\' => {
                let character = value[cursor..].chars().next().ok_or_else(|| illegal("character to be escaped is missing"))?;
                emit(ReplacementPart::Text(&value[cursor..cursor + character.len_utf8()]))?; cursor += character.len_utf8();
            }
            '$' if value.as_bytes().get(cursor) == Some(&b'{') => {
                cursor += 1;
                let start = cursor;
                while value.as_bytes().get(cursor).is_some_and(u8::is_ascii_alphanumeric) { cursor += 1; }
                if cursor == start { return Err(illegal("named capturing group has 0 length name")); }
                if value.as_bytes().get(cursor) != Some(&b'}') { return Err(illegal("named capturing group is missing trailing '}'")); }
                let name = &value[start..cursor]; cursor += 1;
                if name.as_bytes()[0].is_ascii_digit() { return Err(illegal("capturing group name starts with digit character")); }
                let group = pattern.names.iter().position(|candidate| candidate.as_deref() == Some(name)).ok_or_else(|| illegal(&format!("No group with name {{{name}}}")))? + 1;
                emit(ReplacementPart::Group(group))?;
            }
            '$' => {
                let digit = value.as_bytes().get(cursor).filter(|byte| byte.is_ascii_digit()).ok_or_else(|| illegal("Illegal group reference"))?;
                let mut group = usize::from(*digit - b'0'); cursor += 1;
                if group >= groups { return Err(illegal(&format!("No group {group}"))); }
                while let Some(digit) = value.as_bytes().get(cursor).filter(|byte| byte.is_ascii_digit()) {
                    let Some(next) = group.checked_mul(10).and_then(|group| group.checked_add(usize::from(*digit - b'0'))) else { break; };
                    if next >= groups { break; } group = next; cursor += 1;
                }
                emit(ReplacementPart::Group(group))?;
            }
            _ => emit(ReplacementPart::Text(&value[cursor - character.len_utf8()..cursor]))?,
        }
    }
    Ok(())
}

fn replace_utf16(source: &str, pattern: &Pattern, replacement: &str, context: &EvalContext<'_>) -> Result<Value> {
    context.reserve(source.len().checked_mul(32).and_then(|bytes| bytes.checked_add(8192)).ok_or_else(overflow)?)?;
    let source: Vec<u16> = source.encode_utf16().collect();
    let mut length = 0usize;
    replacement_utf16_pass(&source, pattern, replacement, |part| { length = add(length, part.len())?; Ok(()) })?;
    context.reserve(length.checked_mul(5).ok_or_else(overflow)?)?;
    let mut units = Vec::with_capacity(length);
    replacement_utf16_pass(&source, pattern, replacement, |part| { units.extend_from_slice(part); Ok(()) })?;
    let mut output = String::with_capacity(length.checked_mul(3).ok_or_else(overflow)?);
    for character in char::decode_utf16(units) { output.push(character.unwrap_or('?')); }
    Ok(Value::Utf8(output))
}

fn replacement_utf16_pass(source: &[u16], pattern: &Pattern, replacement: &str, mut emit: impl FnMut(&[u16]) -> Result<()>) -> Result<()> {
    let mut end = 0usize; let mut position = 0usize;
    while position <= source.len() {
        let Some(captures) = pattern.regex.captures_utf16(source, position, end).map_err(regex_error)? else { break; };
        let start = captures[0]; let matched_end = captures[1];
        emit(&source[end..start])?;
        replacement_tokens(replacement, pattern, captures.len() / 2, |part| {
            match part {
                ReplacementPart::Group(group) if captures[group * 2] != usize::MAX => emit(&source[captures[group * 2]..captures[group * 2 + 1]])?,
                ReplacementPart::Group(_) => {},
                ReplacementPart::Text(text) => for character in text.chars() { let mut units = [0; 2]; emit(character.encode_utf16(&mut units))?; },
            }
            Ok(())
        })?;
        end = matched_end;
        position = if start == end { end + 1 } else { end };
    }
    emit(&source[end..])
}

fn limit(message: &str) -> Anomaly { Anomaly::incorrect("rtdb.expr/regex-limit", message) }
fn unsupported(message: &str) -> Anomaly { Anomaly::unsupported("rtdb.expr/regex-feature", message) }
fn regex_error(error: fancy_regex::Error) -> Anomaly {
    match &error { fancy_regex::Error::RuntimeError(fancy_regex::RuntimeError::GraphemeBoundaryStart) => Anomaly::fault("xtdb.error/unknown", error.to_string()), fancy_regex::Error::RuntimeError(_) | fancy_regex::Error::CompileError(fancy_regex::CompileError::PatternSizeOverflow) => limit(&error.to_string()), fancy_regex::Error::CompileError(fancy_regex::CompileError::LookBehindNotConst) => illegal("Look-behind group does not have an obvious maximum length"), _ => illegal(&error.to_string()) }
}
