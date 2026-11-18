use rtdb_types::AllocationScope;
use crate::lexer::Kind;
use super::{IssueKind, Result, TokenType, issue, vector};
use super::token::MAX_TOKEN;

pub(super) const PINNED: &str = include_str!("Sql.interp");

pub(in crate::parser) fn pinned_symbol(name: &str) -> Option<TokenType> {
    let source = Source::read(PINNED).ok()?;
    let number = source.symbols.lines().position(|symbol| symbol == name)?;
    TokenType::read(number as i32, number).ok()
}

pub(super) struct Source {
    literals: &'static str,
    symbols: &'static str,
    rules: &'static str,
    integers: &'static str,
}

pub(super) struct Vocabulary {
    literals: Vec<Option<&'static str>>,
    symbols: Vec<&'static str>,
    pub(super) rules: Vec<&'static str>,
}

impl Source {
    pub(super) fn read(text: &'static str) -> Result<Self> {
        let text = text.strip_prefix("token literal names:\n").ok_or_else(|| issue(IssueKind::Source, 0))?;
        let (literals, text) = text.split_once("\n\ntoken symbolic names:\n").ok_or_else(|| issue(IssueKind::Source, 1))?;
        let (symbols, text) = text.split_once("\n\nrule names:\n").ok_or_else(|| issue(IssueKind::Source, 2))?;
        let (rules, text) = text.split_once("\n\n\natn:\n").ok_or_else(|| issue(IssueKind::Source, 3))?;
        let integers = text.trim().strip_prefix('[').and_then(|value| value.strip_suffix(']')).ok_or_else(|| issue(IssueKind::Source, 4))?;
        Ok(Self { literals, symbols, rules, integers })
    }

    pub(super) fn words(&self, allocation: &AllocationScope) -> Result<Vec<i32>> {
        let count = self.integers.split(',').count();
        let mut words = vector(allocation, count)?;
        for (offset, value) in self.integers.split(',').enumerate() {
            words.push(value.trim().parse().map_err(|_| issue(IssueKind::Integer, offset))?);
        }
        Ok(words)
    }

    pub(super) fn vocabulary(&self, allocation: &AllocationScope, rule_count: usize) -> Result<Vocabulary> {
        let count = usize::from(MAX_TOKEN) + 1;
        if self.literals.lines().count() != count || self.symbols.lines().count() != count || self.rules.lines().count() != rule_count {
            return Err(issue(IssueKind::Source, 5));
        }
        let mut literals = vector(allocation, count)?;
        let mut symbols = vector(allocation, count)?;
        let mut rules = vector(allocation, rule_count)?;
        for (offset, name) in self.literals.lines().enumerate() {
            if name != "null" && !(name.starts_with('\'') && name.ends_with('\'')) { return Err(issue(IssueKind::Source, offset)); }
            literals.push((name != "null").then_some(name));
        }
        for (offset, name) in self.symbols.lines().enumerate() {
            if (offset > 0 && name == "null") || name.is_empty() { return Err(issue(IssueKind::Source, offset)); }
            symbols.push(name);
        }
        for (offset, name) in self.rules.lines().enumerate() {
            if name.is_empty() { return Err(issue(IssueKind::Source, offset)); }
            rules.push(name);
        }
        Ok(Vocabulary { literals, symbols, rules })
    }
}

impl Vocabulary {
    pub(super) fn token_type(&self, kind: Kind) -> Result<TokenType> {
        let number = match kind {
            Kind::Eof => -1,
            Kind::Float => 4, Kind::Integer => 5, Kind::CharacterString => 6,
            Kind::EscapedString => 7, Kind::BinaryString => 8, Kind::Parameter => 9,
            Kind::DollarStart => 364, Kind::Identifier => 365, Kind::DelimitedIdentifier(_) => 366,
            Kind::DollarEnd => 367, Kind::DollarText => 368,
            Kind::Keyword(keyword) => return self.named(keyword.name()),
            Kind::Punctuation(punctuation) => return self.named(punctuation.name()),
        };
        TokenType::read(number, number.max(0) as usize)
    }

    fn named(&self, name: &str) -> Result<TokenType> {
        let number = self.symbols.iter().position(|candidate| *candidate == name).ok_or_else(|| issue(IssueKind::Token, 0))?;
        TokenType::read(number as i32, number)
    }

    pub(super) fn display_name(&self, token: TokenType) -> &'static str {
        if token == TokenType::EOF { return "EOF"; }
        let index = token.number() as usize;
        self.literals[index].unwrap_or(self.symbols[index])
    }
}

pub(super) struct Words<'a> { values: &'a [i32], pub(super) position: usize }

impl<'a> Words<'a> {
    pub(super) fn at(values: &'a [i32], position: usize) -> Self { Self { values, position } }

    pub(super) fn next(&mut self) -> Result<i32> {
        let value = *self.values.get(self.position).ok_or_else(|| issue(IssueKind::Extent, self.position))?;
        self.position += 1;
        Ok(value)
    }

    pub(super) fn count(&mut self) -> Result<usize> {
        let offset = self.position;
        usize::try_from(self.next()?).map_err(|_| issue(IssueKind::Extent, offset))
    }

    pub(super) fn skip(&mut self, count: usize) -> Result<()> {
        let end = self.position.checked_add(count).ok_or_else(|| issue(IssueKind::Overflow, self.position))?;
        if end > self.values.len() { return Err(issue(IssueKind::Extent, self.position)); }
        self.position = end;
        Ok(())
    }

    pub(super) fn end(&self) -> Result<()> {
        if self.position == self.values.len() { Ok(()) } else { Err(issue(IssueKind::Extent, self.position)) }
    }
}
