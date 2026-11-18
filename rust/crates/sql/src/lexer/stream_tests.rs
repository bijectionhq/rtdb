use rtdb_types::{AllocationScope, MemoryBudget};
use super::{INITIAL_CAPACITY, IndexedLexeme, Kind, Lexeme, ScanError, StreamError, Tail, TokenStream, TokenText};

fn initial_charge() -> usize { 4096 + 256 + INITIAL_CAPACITY * std::mem::size_of::<Lexeme<'_>>() }

fn raw<'sql>(stream: &mut TokenStream<'sql, '_>, offset: isize) -> &'sql str {
    stream.lt(offset).unwrap().unwrap().lexeme().raw()
}

fn recognition<'sql, T>(result: Result<T, StreamError<'sql>>) -> super::RecognitionError<'sql> {
    match result {
        Err(StreamError::Scan(ScanError::Recognition(error))) => error,
        _ => panic!("recognition failure"),
    }
}

fn admission<T>(result: Result<T, StreamError<'_>>) {
    match result {
        Err(StreamError::Scan(ScanError::Admission(error))) => assert_eq!(error.code, "rtdb/memory-budget-exceeded"),
        _ => panic!("allocation failure"),
    }
}

#[test]
fn constructor_admits_capacity_without_demanding_the_first_lexeme() {
    for limit in [initial_charge() - 1, initial_charge()] {
        let memory = MemoryBudget::new(limit);
        {
            let allocation = AllocationScope::new(&memory).unwrap();
            match TokenStream::new("@", &allocation) {
                Err(error) => { assert_eq!(limit, initial_charge() - 1); assert_eq!(error.code, "rtdb/memory-budget-exceeded"); }
                Ok(mut stream) => {
                    assert_eq!(limit, initial_charge());
                    assert_eq!(stream.index(), None);
                    assert_eq!(stream.buffered_len(), 0);
                    assert!(stream.buffered(0).is_none());
                    assert_eq!(stream.mark(), 0);
                    stream.release(57);
                    assert_eq!(stream.index(), None);
                    assert_eq!(recognition(stream.lt(0)).span().start.byte, 0);
                    assert_eq!(stream.index(), None);
                    assert_eq!(stream.buffered_len(), 0);
                    assert_eq!(memory.used_bytes(), initial_charge());
                }
            };
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn zero_and_negative_lookahead_initialize_before_returning_no_token() {
    for offset in [0, -1, isize::MIN] {
        let memory = MemoryBudget::new(1 << 20);
        let allocation = AllocationScope::new(&memory).unwrap();
        let mut stream = TokenStream::new("a @", &allocation).unwrap();
        assert!(stream.lt(offset).unwrap().is_none());
        assert_eq!(stream.index(), Some(0));
        assert_eq!(stream.buffered_len(), 1);
        assert_eq!(stream.buffered(0).unwrap().lexeme().raw(), "a");
        let mut invalid = TokenStream::new("@", &allocation).unwrap();
        assert_eq!(recognition(invalid.lt(offset)).span().start.byte, 0);
    }
}

#[test]
fn lookahead_and_lookbehind_preserve_the_cursor_and_buffered_ordinals() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut stream = TokenStream::new("a b c @", &allocation).unwrap();
    assert_eq!(raw(&mut stream, 3), "c");
    assert_eq!(stream.index(), Some(0));
    assert_eq!(stream.buffered_len(), 3);
    let token = stream.lt(2).unwrap().unwrap();
    assert_eq!(token.index(), 1);
    assert_eq!(token.lexeme().raw(), "b");
    stream.consume().unwrap();
    assert_eq!(stream.index(), Some(1));
    assert_eq!(raw(&mut stream, -1), "a");
    assert!(stream.lt(-2).unwrap().is_none());
    assert!(stream.lt(isize::MIN).unwrap().is_none());
    let saved = stream.index().unwrap();
    let marker = stream.mark();
    stream.seek(2).unwrap();
    stream.release(marker);
    assert_eq!(stream.index(), Some(2));
    stream.seek(saved).unwrap();
    assert_eq!(stream.index(), Some(1));
    assert_eq!(stream.buffered_len(), 3);
}

#[test]
fn consume_demands_the_following_token_before_advancing() {
    let memory = MemoryBudget::new(initial_charge());
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut stream = TokenStream::new("ROLLBACK @", &allocation).unwrap();
    assert_eq!(raw(&mut stream, 1), "ROLLBACK");
    let error = recognition(stream.consume());
    assert_eq!(error.span().start.byte, 9);
    assert_eq!(stream.index(), Some(0));
    assert_eq!(stream.buffered_len(), 1);
    let used = memory.used_bytes();
    stream.seek(0).unwrap();
    assert_eq!(raw(&mut stream, 1), "ROLLBACK");
    assert_eq!(recognition(stream.lt(2)).span(), error.span());
    assert_eq!(recognition(stream.consume()).span(), error.span());
    assert_eq!(memory.used_bytes(), used);
}

#[test]
fn failed_seek_retains_the_fetched_prefix_and_original_cursor() {
    let memory = MemoryBudget::new(initial_charge());
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut stream = TokenStream::new("a b c @", &allocation).unwrap();
    assert_eq!(raw(&mut stream, 1), "a");
    assert_eq!(recognition(stream.seek(9)).span().start.byte, 6);
    assert_eq!(stream.index(), Some(0));
    assert_eq!(stream.buffered_len(), 3);
    stream.seek(2).unwrap();
    assert_eq!(raw(&mut stream, 1), "c");
    assert_eq!(recognition(stream.seek(9)).span().start.byte, 6);
    assert_eq!(stream.index(), Some(2));
    assert_eq!(memory.used_bytes(), initial_charge());
}

#[test]
fn first_demand_failure_retains_the_cursor_committed_by_lazy_initialization() {
    for operation in 0..3 {
        let memory = MemoryBudget::new(initial_charge());
        let allocation = AllocationScope::new(&memory).unwrap();
        let mut stream = TokenStream::new("a @", &allocation).unwrap();
        let error = match operation {
            0 => recognition(stream.seek(2)),
            1 => recognition(stream.lt(3)),
            _ => recognition(stream.consume()),
        };
        assert_eq!(error.span().start.byte, 2);
        assert_eq!(stream.index(), Some(0));
        assert_eq!(stream.buffered_len(), 1);
        assert_eq!(stream.buffered(0).unwrap().lexeme().raw(), "a");
    }
}

#[test]
fn eof_is_buffered_once_and_clamps_distant_lookahead_and_seek() {
    for (source, eof) in [("", 0), ("a b", 2)] {
        let memory = MemoryBudget::new(initial_charge());
        let allocation = AllocationScope::new(&memory).unwrap();
        let mut stream = TokenStream::new(source, &allocation).unwrap();
        let token = stream.lt(isize::MAX).unwrap().unwrap();
        assert_eq!(token.index(), eof);
        assert_eq!(token.lexeme().kind(), Kind::Eof);
        assert_eq!(token.lexeme().raw(), "");
        assert_eq!(token.lexeme().span().start.byte, source.len());
        assert_eq!(stream.buffered_len(), eof + 1);
        assert_eq!(stream.index(), Some(0));
        stream.seek(usize::MAX).unwrap();
        assert_eq!(stream.index(), Some(eof));
        assert_eq!(stream.lt(isize::MAX).unwrap().unwrap().index(), eof);
        assert!(matches!(stream.consume(), Err(StreamError::ConsumeEof)));
        assert_eq!(stream.buffered_len(), eof + 1);
        assert_eq!(memory.used_bytes(), initial_charge());
    }
}

#[test]
fn text_fetches_raw_token_spellings_without_initializing_the_cursor() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let source = "a -- skip\r+ /* hidden */ 'b''c' E'\\N' $x$raw ;$x$";
    let mut stream = TokenStream::new(source, &allocation).unwrap();
    let text = stream.text(0, 6).unwrap();
    assert_eq!(text.as_str(), "a+'b''c'E'\\N'$x$raw ;$x$");
    assert_eq!(stream.index(), None);
    assert_eq!(stream.buffered_len(), 7);
    let plus = stream.buffered(1).unwrap().lexeme();
    assert_eq!(plus.span().start.byte, 10);
    assert_eq!(plus.span().start.line, 1);
    assert_eq!(plus.span().start.column, 10);
    assert_eq!(stream.buffered(3).unwrap().lexeme().body(), Some(r"\N"));
    assert_eq!(stream.buffered(4).unwrap().lexeme().kind(), Kind::DollarStart);
    assert_eq!(stream.buffered(5).unwrap().lexeme().kind(), Kind::DollarText);
    assert_eq!(stream.buffered(6).unwrap().lexeme().kind(), Kind::DollarEnd);
    assert_eq!(raw(&mut stream, 1), "a");
    assert_eq!(stream.index(), Some(0));
    assert_eq!(stream.buffered_len(), 7);
}

#[test]
fn negative_text_bounds_do_not_fetch_but_reversed_positive_bounds_do() {
    let memory = MemoryBudget::new(initial_charge());
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut stream = TokenStream::new("a @", &allocation).unwrap();
    for (start, stop) in [(-1, 100), (100, -1), (isize::MIN, isize::MAX)] {
        assert_eq!(stream.text(start, stop).unwrap().as_str(), "");
        assert_eq!(stream.buffered_len(), 0);
        assert_eq!(stream.index(), None);
    }
    assert_eq!(stream.text(3, 0).unwrap().as_str(), "");
    assert_eq!(stream.buffered_len(), 1);
    assert_eq!(stream.index(), None);
    assert_eq!(recognition(stream.text(3, 1)).span().start.byte, 2);
    assert_eq!(stream.buffered_len(), 1);
    assert_eq!(stream.index(), None);
    assert_eq!(memory.used_bytes(), initial_charge());
}

#[test]
fn text_clamps_at_eof_and_omits_its_display_text() {
    let memory = MemoryBudget::new(initial_charge() + 257);
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut stream = TokenStream::new("a b", &allocation).unwrap();
    assert_eq!(stream.text(2, isize::MAX).unwrap().as_str(), "");
    assert_eq!(stream.text(99, 100).unwrap().as_str(), "");
    assert_eq!(stream.buffered_len(), 3);
    assert_eq!(stream.index(), None);
    assert_eq!(memory.used_bytes(), initial_charge());
    assert_eq!(stream.text(1, isize::MAX).unwrap().as_str(), "b");
    assert_eq!(memory.used_bytes(), initial_charge() + 257);
    assert_eq!(stream.index(), None);
}

#[test]
fn lexical_failure_after_a_full_buffer_precedes_growth_admission() {
    let source = format!("{}@", "a ".repeat(INITIAL_CAPACITY));
    let memory = MemoryBudget::new(initial_charge());
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut stream = TokenStream::new(&source, &allocation).unwrap();
    assert_eq!(recognition(stream.lt(isize::MAX)).span().start.byte, 2 * INITIAL_CAPACITY);
    assert_eq!(stream.buffered_len(), INITIAL_CAPACITY);
    assert_eq!(stream.index(), Some(0));
    assert_eq!(memory.used_bytes(), initial_charge());
}

#[test]
fn growth_covers_both_buffers_and_is_triggered_by_a_successful_next_token() {
    let source = "a ".repeat(INITIAL_CAPACITY);
    let required = initial_charge() + 256 + 2 * INITIAL_CAPACITY * std::mem::size_of::<Lexeme<'_>>();
    for limit in [required - 1, required] {
        let memory = MemoryBudget::new(limit);
        {
            let allocation = AllocationScope::new(&memory).unwrap();
            let mut stream = TokenStream::new(&source, &allocation).unwrap();
            let result = stream.lt(INITIAL_CAPACITY as isize + 1);
            if limit == required {
                assert_eq!(result.unwrap().unwrap().lexeme().kind(), Kind::Eof);
                assert_eq!(stream.buffered_len(), INITIAL_CAPACITY + 1);
                assert_eq!(memory.used_bytes(), required);
            } else {
                admission(result);
                assert_eq!(stream.buffered_len(), INITIAL_CAPACITY);
                assert!(matches!(stream.tail, Tail::Admission));
                assert_eq!(memory.used_bytes(), initial_charge());
            }
            assert_eq!(stream.index(), Some(0));
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn failed_growth_aborts_further_scanning_even_after_budget_is_released() {
    let source = format!("{}@", "a ".repeat(INITIAL_CAPACITY + 1));
    let required = initial_charge() + 256 + 2 * INITIAL_CAPACITY * std::mem::size_of::<Lexeme<'_>>();
    let memory = MemoryBudget::new(required);
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut stream = TokenStream::new(&source, &allocation).unwrap();
    let blocker = memory.reserve(1).unwrap();
    admission(stream.lt(INITIAL_CAPACITY as isize + 1));
    assert!(matches!(stream.tail, Tail::Admission));
    assert_eq!(stream.buffered_len(), INITIAL_CAPACITY);
    drop(blocker);
    assert!(matches!(stream.lt(INITIAL_CAPACITY as isize + 1), Err(StreamError::Aborted)));
    assert!(matches!(stream.text(0, 0), Err(StreamError::Aborted)));
    assert!(matches!(stream.consume(), Err(StreamError::Aborted)));
    stream.seek(INITIAL_CAPACITY - 1).unwrap();
    assert_eq!(raw(&mut stream, 1), "a");
    assert!(matches!(stream.seek(INITIAL_CAPACITY), Err(StreamError::Aborted)));
    assert_eq!(stream.index(), Some(INITIAL_CAPACITY - 1));
    assert_eq!(memory.used_bytes(), initial_charge());
    drop(stream);
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn lexer_admission_failure_aborts_further_scanning_even_after_budget_is_released() {
    let memory = MemoryBudget::new(initial_charge() + (1 << 20));
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut stream = TokenStream::new("a /* comment */ @", &allocation).unwrap();
    let blocker = memory.reserve(1 << 20).unwrap();
    assert_eq!(raw(&mut stream, 1), "a");
    admission(stream.consume());
    assert_eq!(stream.index(), Some(0));
    assert_eq!(stream.buffered_len(), 1);
    drop(blocker);
    stream.seek(0).unwrap();
    assert_eq!(raw(&mut stream, 1), "a");
    assert!(matches!(stream.lt(2), Err(StreamError::Aborted)));
    assert!(matches!(stream.text(-1, -1), Err(StreamError::Aborted)));
    assert_eq!(memory.used_bytes(), initial_charge());
    drop(stream);
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn text_admission_failure_aborts_allocating_operations_and_preserves_cleanup_reads() {
    let memory = MemoryBudget::new(initial_charge() + 258);
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut stream = TokenStream::new("a b @", &allocation).unwrap();
    let blocker = memory.reserve(1).unwrap();
    admission(stream.text(0, 1));
    assert_eq!(stream.index(), None);
    assert_eq!(stream.buffered_len(), 2);
    drop(blocker);
    assert!(matches!(stream.text(0, 1), Err(StreamError::Aborted)));
    assert_eq!(stream.buffered(1).unwrap().lexeme().raw(), "b");
    stream.seek(0).unwrap();
    assert_eq!(raw(&mut stream, 1), "a");
    assert!(matches!(stream.consume(), Err(StreamError::Aborted)));
    assert!(matches!(stream.seek(2), Err(StreamError::Aborted)));
    assert_eq!(stream.index(), Some(0));
    assert_eq!(memory.used_bytes(), initial_charge());
    drop(stream);
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn returned_text_outlives_source_and_stream_but_retains_its_allocation_borrow() {
    fn text(allocation: &AllocationScope) -> TokenText<'_> {
        let source = String::from("a b");
        let mut stream = TokenStream::new(&source, allocation).unwrap();
        stream.text(0, 1).unwrap()
    }
    let memory = MemoryBudget::new(initial_charge() + 258);
    let allocation = AllocationScope::new(&memory).unwrap();
    let output = text(&allocation);
    assert_eq!(output.as_str(), "ab");
    assert_eq!(memory.used_bytes(), initial_charge() + 258);
    drop(output);
    assert_eq!(memory.used_bytes(), initial_charge() + 258);
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn copied_lexemes_borrow_only_source_and_release_the_buffer_owner() {
    fn token<'sql>(source: &'sql str, allocation: &AllocationScope) -> IndexedLexeme<'sql> {
        TokenStream::new(source, allocation).unwrap().lt(1).unwrap().unwrap()
    }
    let source = String::from("borrowed");
    let memory = MemoryBudget::new(initial_charge());
    let allocation = AllocationScope::new(&memory).unwrap();
    let copied = token(&source, &allocation);
    assert_eq!(memory.used_bytes(), initial_charge());
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(copied.lexeme().raw(), "borrowed");
}
