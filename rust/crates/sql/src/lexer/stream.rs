use std::fmt;
use rtdb_types::{AllocationScope, Anomaly};
use super::{Kind, Lexer, Lexeme, RecognitionError, ScanError, size_error};

#[cfg(test)]
#[path = "stream_tests.rs"]
mod tests;

const INITIAL_CAPACITY: usize = 100;

type Result<'sql, T> = std::result::Result<T, StreamError<'sql>>;

#[derive(Debug)]
pub(crate) enum StreamError<'sql> {
    Scan(ScanError<'sql>),
    Aborted,
    ConsumeEof,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct IndexedLexeme<'sql> {
    index: usize,
    lexeme: Lexeme<'sql>,
}

impl<'sql> IndexedLexeme<'sql> {
    pub(crate) fn index(self) -> usize { self.index }
    pub(crate) fn lexeme(self) -> Lexeme<'sql> { self.lexeme }
}

#[derive(Clone, Copy)]
enum Cursor { Uninitialized, At(usize) }

#[derive(Clone, Copy)]
enum Tail<'sql> {
    Open,
    Eof { index: usize },
    Recognition(RecognitionError<'sql>),
    Admission,
    PendingAdmission(Lexeme<'sql>),
}

pub(crate) struct TokenStream<'sql, 'scope> {
    lexer: Lexer<'sql, 'scope>,
    tokens: Vec<Lexeme<'sql>>,
    cursor: Cursor,
    tail: Tail<'sql>,
    allocation: &'scope AllocationScope,
}

impl<'sql, 'scope> TokenStream<'sql, 'scope> {
    pub(crate) fn new(source: &'sql str, allocation: &'scope AllocationScope) -> rtdb_types::Result<Self> {
        allocation.reserve(capacity_bytes(INITIAL_CAPACITY)?)?;
        Ok(Self {
            lexer: Lexer::new(source, allocation),
            tokens: Vec::with_capacity(INITIAL_CAPACITY),
            cursor: Cursor::Uninitialized,
            tail: Tail::Open,
            allocation,
        })
    }

    pub(crate) fn index(&self) -> Option<usize> {
        match self.cursor { Cursor::Uninitialized => None, Cursor::At(index) => Some(index) }
    }

    pub(crate) fn buffered_len(&self) -> usize { self.tokens.len() }

    pub(crate) fn buffered(&self, index: usize) -> Option<IndexedLexeme<'sql>> {
        self.tokens.get(index).copied().map(|lexeme| IndexedLexeme { index, lexeme })
    }

    pub(crate) fn lt(&mut self, offset: isize) -> Result<'sql, Option<IndexedLexeme<'sql>>> {
        let mut index = self.initialize()?;
        if offset == 0 { return Ok(None); }
        if offset < 0 { return Ok(index.checked_sub(offset.unsigned_abs()).and_then(|index| self.buffered(index))); }
        let mut remaining = offset as usize;
        while remaining > 1 {
            let next = self.next_index(index)?;
            if !self.sync(next)? { break; }
            index = next;
            remaining -= 1;
        }
        Ok(self.buffered(index))
    }

    pub(crate) fn consume(&mut self) -> Result<'sql, ()> {
        if self.aborted() { return Err(StreamError::Aborted); }
        let index = self.initialize()?;
        if self.tokens[index].kind() == Kind::Eof { return Err(StreamError::ConsumeEof); }
        let next = self.next_index(index)?;
        self.sync(next)?;
        self.cursor = Cursor::At(next);
        Ok(())
    }

    pub(crate) fn seek(&mut self, index: usize) -> Result<'sql, ()> {
        self.initialize()?;
        self.sync(index)?;
        self.cursor = Cursor::At(index.min(self.tokens.len() - 1));
        Ok(())
    }

    pub(crate) fn mark(&self) -> usize { 0 }

    pub(crate) fn release(&mut self, _marker: usize) {}

    pub(crate) fn text(&mut self, start: isize, stop: isize) -> Result<'sql, TokenText<'scope>> {
        if self.aborted() { return Err(StreamError::Aborted); }
        if start < 0 || stop < 0 { return Ok(self.empty_text()); }
        self.sync(stop as usize)?;
        let start = start as usize;
        let stop = (stop as usize).min(self.tokens.len() - 1);
        if start > stop { return Ok(self.empty_text()); }
        let capacity = self.tokens[start..=stop].iter().take_while(|token| token.kind() != Kind::Eof)
            .try_fold(0usize, |bytes, token| bytes.checked_add(token.raw().len()).ok_or_else(size_error));
        let capacity = match capacity { Ok(capacity) => capacity, Err(error) => return self.abort(error) };
        if capacity == 0 { return Ok(self.empty_text()); }
        if let Err(error) = self.allocation.reserve(capacity) { return self.abort(error); }
        let mut text = String::with_capacity(capacity);
        for token in &self.tokens[start..=stop] {
            if token.kind() == Kind::Eof { break; }
            text.push_str(token.raw());
        }
        Ok(TokenText { text, _allocation: self.allocation })
    }

    fn empty_text(&self) -> TokenText<'scope> { TokenText { text: String::new(), _allocation: self.allocation } }

    fn initialize(&mut self) -> Result<'sql, usize> {
        if let Cursor::At(index) = self.cursor { return Ok(index); }
        self.sync(0)?;
        self.cursor = Cursor::At(0);
        Ok(0)
    }

    fn sync(&mut self, index: usize) -> Result<'sql, bool> {
        while index >= self.tokens.len() {
            match self.tail {
                Tail::Eof { index: eof } => return Ok(index <= eof),
                Tail::Recognition(error) => return Err(StreamError::Scan(ScanError::Recognition(error))),
                Tail::Admission | Tail::PendingAdmission(_) => return Err(StreamError::Aborted),
                Tail::Open => (),
            }
            let lexeme = match self.lexer.next() {
                Ok(lexeme) => lexeme,
                Err(error) => {
                    self.tail = match &error {
                        ScanError::Recognition(error) => Tail::Recognition(*error),
                        ScanError::Admission(_) => Tail::Admission,
                    };
                    return Err(StreamError::Scan(error));
                }
            };
            if let Err(error) = self.grow() {
                self.tail = Tail::PendingAdmission(lexeme);
                return Err(StreamError::Scan(ScanError::Admission(error)));
            }
            let index = self.tokens.len();
            self.tokens.push(lexeme);
            if lexeme.kind() == Kind::Eof { self.tail = Tail::Eof { index }; }
        }
        Ok(true)
    }

    fn grow(&mut self) -> rtdb_types::Result<()> {
        if self.tokens.len() < self.tokens.capacity() { return Ok(()); }
        let capacity = self.tokens.capacity().checked_mul(2).ok_or_else(size_error)?;
        self.allocation.reserve(capacity_bytes(capacity)?)?;
        let mut replacement = Vec::with_capacity(capacity);
        replacement.extend_from_slice(&self.tokens);
        self.tokens = replacement;
        Ok(())
    }

    fn next_index(&mut self, index: usize) -> Result<'sql, usize> {
        match index.checked_add(1) { Some(index) => Ok(index), None => self.abort(size_error()) }
    }

    fn aborted(&self) -> bool { matches!(self.tail, Tail::Admission | Tail::PendingAdmission(_)) }

    fn abort<T>(&mut self, error: Anomaly) -> Result<'sql, T> {
        self.tail = Tail::Admission;
        Err(StreamError::Scan(ScanError::Admission(error)))
    }
}

pub(crate) struct TokenText<'scope> {
    text: String,
    _allocation: &'scope AllocationScope,
}

impl TokenText<'_> {
    pub(crate) fn as_str(&self) -> &str { &self.text }
}

impl fmt::Debug for TokenText<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { fmt::Debug::fmt(&self.text, formatter) }
}

// Drop checking keeps the scope borrow live through buffer and text destruction.
impl Drop for TokenStream<'_, '_> { fn drop(&mut self) {} }
impl Drop for TokenText<'_> { fn drop(&mut self) {} }

fn capacity_bytes(capacity: usize) -> rtdb_types::Result<usize> {
    capacity.checked_mul(std::mem::size_of::<Lexeme<'_>>()).ok_or_else(size_error)
}
