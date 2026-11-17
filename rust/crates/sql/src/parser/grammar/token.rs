use std::fmt::{self, Write};
use super::{Grammar, IssueKind, Result, issue};

pub(super) const MAX_TOKEN: u16 = 368;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TokenType(i16);

impl TokenType {
    pub(crate) const EOF: Self = Self(-1);

    pub(super) fn read(number: i32, offset: usize) -> Result<Self> {
        if number == -1 || (1..=i32::from(MAX_TOKEN)).contains(&number) { Ok(Self(number as i16)) }
        else { Err(issue(IssueKind::Token, offset)) }
    }

    pub(crate) fn number(self) -> i32 { i32::from(self.0) }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExpectedSymbol { Epsilon, Token(TokenType) }

impl ExpectedSymbol {
    fn bit(self) -> usize {
        match self {
            Self::Epsilon => 0,
            Self::Token(TokenType::EOF) => 1,
            Self::Token(TokenType(number)) => number as usize + 1,
        }
    }

    fn from_bit(bit: usize) -> Self {
        match bit {
            0 => Self::Epsilon,
            1 => Self::Token(TokenType::EOF),
            _ => Self::Token(TokenType((bit - 1) as i16)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TokenSet([u64; 6]);

impl TokenSet {
    pub(crate) const fn empty() -> Self { Self([0; 6]) }
    pub(super) fn singleton(symbol: ExpectedSymbol) -> Self {
        let mut set = Self::empty();
        set.insert(symbol);
        set
    }

    pub(crate) fn insert(&mut self, symbol: ExpectedSymbol) {
        let bit = symbol.bit();
        self.0[bit / 64] |= 1u64 << (bit % 64);
    }

    pub(crate) fn contains(&self, symbol: ExpectedSymbol) -> bool {
        let bit = symbol.bit();
        self.0[bit / 64] & (1u64 << (bit % 64)) != 0
    }

    pub(crate) fn union(&mut self, other: &Self) {
        for (left, right) in self.0.iter_mut().zip(other.0) { *left |= right; }
    }

    pub(crate) fn remove_epsilon(&mut self) { self.0[0] &= !1; }

    pub(crate) fn iter(&self) -> impl Iterator<Item = ExpectedSymbol> + '_ {
        (0..usize::from(MAX_TOKEN) + 2).map(ExpectedSymbol::from_bit).filter(|symbol| self.contains(*symbol))
    }

    pub(crate) fn display_len(&self, grammar: &Grammar<'_>) -> Result<usize> {
        let count = self.iter().count();
        let mut size: usize = if count == 1 { 0 } else { 2 };
        for (index, symbol) in self.iter().enumerate() {
            let extra = self.display_symbol(grammar, symbol).len() + if index == 0 { 0 } else { 2 };
            size = size.checked_add(extra).ok_or_else(|| issue(IssueKind::Overflow, index))?;
        }
        Ok(size)
    }

    pub(crate) fn write_display(&self, grammar: &Grammar<'_>, output: &mut impl Write) -> fmt::Result {
        let braces = self.iter().count() != 1;
        if braces { output.write_char('{')?; }
        for (index, symbol) in self.iter().enumerate() {
            if index != 0 { output.write_str(", ")?; }
            output.write_str(self.display_symbol(grammar, symbol))?;
        }
        if braces { output.write_char('}')?; }
        Ok(())
    }

    fn display_symbol(&self, grammar: &Grammar<'_>, symbol: ExpectedSymbol) -> &'static str {
        match symbol {
            ExpectedSymbol::Epsilon => "<EPSILON>",
            ExpectedSymbol::Token(TokenType::EOF) => "<EOF>",
            ExpectedSymbol::Token(token) => grammar.display_name(token),
        }
    }
}
