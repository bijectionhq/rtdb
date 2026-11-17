use std::{alloc::Layout, sync::Arc};
use rtdb_types::{AllocationScope, MemoryBudget, MemoryReservation};
use crate::lexer::{Kind, ScanError, StreamError};
use crate::parser::grammar::OwnedGrammar;
use crate::parser::prediction::{PredictionError, PredictionStorage};
use crate::parser::prediction::closure::{ClosureOptions, LoopEntryOption, SourceAssertions};
use super::*;
use super::super::{BailRecovery, CapturedClassPolicy, InvocationCause, InvocationException, ParserControl, ParserEnvironment, ParserState, RuleContextView, with_parser_session};

fn fixture<'sql>(
    sql: &'sql str,
    run: impl for<'parse, 'env, 'work> FnOnce(
        &mut ParserSession<'parse, 'sql, 'env, 'work>, &Arc<MemoryBudget>, &Arc<MemoryBudget>,
    ) -> RuntimeResult<'sql, ()>,
) {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    let memory = MemoryBudget::new(64 << 20);
    let retained = MemoryBudget::new(64 << 20);
    let work = AllocationScope::new(&memory).unwrap();
    let policy = CapturedClassPolicy::from_captured(ClosureOptions {
        pruning: LoopEntryOption::Enabled, assertions: SourceAssertions::Enabled,
    });
    let mut environment = ParserEnvironment::new(PredictionStorage::new(grammar, &retained), policy, &work).unwrap();
    with_parser_session(&mut environment, sql, &work, |session| {
        session.profile = ParserProfile::SllBail;
        run(session, &memory, &retained)
    }).unwrap();
    drop(environment);
    drop(work);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(retained.used_bytes(), 0);
    assert_eq!(grammar_memory.used_bytes(), 0);
}

fn complete<T>(result: PassResult<'_, '_, T>) -> T {
    match result { Ok(value) => value, Err(error) => panic!("generated success: {error:?}") }
}

fn admission<T>(result: &PassResult<'_, '_, T>) {
    assert!(matches!(result, Err(PassError::Runtime(RuntimeError::Prediction(PredictionError::Admission(error))))
        if error.code == "rtdb/memory-budget-exceeded"));
}

fn leave(memory: &Arc<MemoryBudget>, available: usize) -> Arc<MemoryReservation> {
    memory.reserve(memory.limit_bytes() - memory.used_bytes() - available).unwrap()
}

fn child_charge(capacity: usize) -> usize {
    Layout::array::<ContextChild<'_, '_>>(capacity).unwrap().size() + 256
}

fn terminals<'sql>(view: RuleContextView<'_, '_, 'sql>) -> Vec<(usize, &'sql str)> {
    view.children().iter().map(|child| match child {
        ContextChild::Terminal(token) => (token.index(), token.lexeme().raw()),
        ContextChild::Context(_) => panic!("terminal child"),
    }).collect()
}

#[test]
fn all_four_generated_alternatives_publish_fresh_labelled_contexts_and_ordered_actual_terminals() {
    for (source, class, words) in [
        ("rEaD UnCoMmItTeD trailing @", ContextClass::ReadUncommittedIsolation, &["rEaD", "UnCoMmItTeD"][..]),
        ("ReAd CoMmItTeD trailing @", ContextClass::ReadCommittedIsolation, &["ReAd", "CoMmItTeD"][..]),
        ("rEpEaTaBlE rEaD trailing @", ContextClass::RepeatableReadIsolation, &["rEpEaTaBlE", "rEaD"][..]),
        ("sErIaLiZaBlE trailing @", ContextClass::SerializableIsolation, &["sErIaLiZaBlE"][..]),
    ] {
        fixture(source, |session, _, _| {
            let key = complete(session.level_of_isolation_sll());
            assert_eq!(session.records.len(), 2);
            assert_ne!(key, InvocationKey::at(0));
            let view = session.invocation_view().context(key).unwrap();
            assert_eq!(view.key(), key);
            assert_eq!(view.rule(), 175);
            assert_eq!(view.class(), Some(class));
            assert_eq!(view.parent(), None);
            assert_eq!(view.invoking_state(), ParserState::RootSentinel);
            assert_eq!(view.start().unwrap().index(), 0);
            assert_eq!(view.stop().unwrap().index(), words.len() - 1);
            assert_eq!(terminals(view), words.iter().enumerate().map(|(index, word)| (index, *word)).collect::<Vec<_>>());
            assert_eq!(view.children().as_ptr(), session.records[key.index].children.as_ref().unwrap().as_ptr());
            assert!(session.records[0].class.is_none());
            assert!(session.records[0].children.is_none());
            assert!(session.records[0].stop.is_none());
            assert_eq!(session.records[0].start.unwrap().index(), 0);
            assert!(session.invocation_view().exception(key).is_none());
            assert_eq!(session.input_index(), Some(words.len()));
            assert_eq!(session.buffered_token(words.len()).unwrap().lexeme().raw(), "trailing");
            assert!(session.buffered_token(words.len() + 1).is_none());
            assert!(session.current().is_none());
            assert_eq!(session.state(), ParserState::RootSentinel);
            assert_eq!(session.syntax_errors, 0);
            assert_eq!(session.precedence(), 0);
            Ok(())
        });
    }
}

#[test]
fn common_isolation_replaces_the_actual_parent_tail_and_keeps_mixed_children_in_source_order() {
    fixture("ISOLATION LEVEL SERIALIZABLE trailing @", |session, _, _| {
        let parent = complete(session.common_tx_mode_isolation_sll());
        assert_eq!(session.records.len(), 4);
        let view = session.invocation_view().context(parent).unwrap();
        assert_eq!(view.class(), Some(ContextClass::IsolationLevelTxMode));
        assert_eq!(view.rule(), 164);
        assert_eq!(view.start().unwrap().index(), 0);
        assert_eq!(view.stop().unwrap().index(), 2);
        let [ContextChild::Terminal(first), ContextChild::Terminal(second), ContextChild::Context(child)] = view.children() else { panic!("ordered mixed children"); };
        assert_eq!((first.index(), first.lexeme().raw()), (0, "ISOLATION"));
        assert_eq!((second.index(), second.lexeme().raw()), (1, "LEVEL"));
        assert_eq!(*child, InvocationKey::at(3));
        let child_view = session.invocation_view().context(*child).unwrap();
        assert_eq!(child_view.class(), Some(ContextClass::SerializableIsolation));
        assert_eq!(child_view.parent(), Some(parent));
        assert_eq!(child_view.invoking_state(), ParserState::State(2884));
        assert_eq!(child_view.start().unwrap().index(), 2);
        assert_eq!(child_view.stop().unwrap().index(), 2);
        assert_eq!(terminals(child_view), [(2, "SERIALIZABLE")]);
        assert_eq!(session.records[2].parent, Some(parent));
        assert!(session.records[2].class.is_none());
        assert!(session.records[2].stop.is_none());
        assert!(session.records[2].children.is_none());
        assert_eq!(session.input_index(), Some(3));
        assert!(session.current().is_none());
        assert_eq!(session.state(), ParserState::RootSentinel);
        Ok(())
    });
}

#[test]
fn post_current_child_append_denial_runs_only_the_actual_ancestor_finally_against_that_child() {
    fixture("ISOLATION LEVEL SERIALIZABLE", |session, memory, retained| {
        complete(session.common_tx_mode_isolation_sll());
        session.seek_input(0)?;
        let before = session.records.len();
        assert!(before + 3 <= session.records.capacity());
        let retained_before = retained.used_bytes();
        let _blocker = leave(memory, child_charge(1) + child_charge(2));
        let result = session.common_tx_mode_isolation_sll();
        admission(&result);
        let parent = InvocationKey::at(before + 1);
        let child = InvocationKey::at(before + 2);
        assert_eq!(session.records.len(), before + 3);
        assert_eq!(session.current(), Some(parent));
        assert_eq!(session.state(), ParserState::State(2884));
        assert_eq!(session.input_index(), Some(2));
        assert_eq!(session.records[parent.index].class, Some(ContextClass::IsolationLevelTxMode));
        assert!(session.records[parent.index].stop.is_none());
        assert_eq!(session.records[parent.index].children.as_ref().unwrap().capacity(), 2);
        assert_eq!(terminals(session.invocation_view().context(parent).unwrap()), [(0, "ISOLATION"), (1, "LEVEL")]);
        assert_eq!(session.records[child.index].parent, Some(parent));
        assert_eq!(session.records[child.index].start.unwrap().index(), 2);
        assert_eq!(session.records[child.index].stop.unwrap().index(), 1);
        assert!(session.records[child.index].class.is_none());
        assert!(session.records[child.index].children.is_none());
        assert!(session.invocation_view().exception(child).is_none());
        assert!(session.invocation_view().exception(parent).is_none());
        assert_eq!(session.syntax_errors, 0);
        assert!(matches!(session.control, ParserControl::Failed));
        assert_eq!(retained.used_bytes(), retained_before);
        assert_eq!(memory.used_bytes(), memory.limit_bytes());
        Ok(())
    });
}

#[test]
fn child_constructor_denial_leaves_the_ancestor_current_for_its_own_finally() {
    fixture("ISOLATION LEVEL SERIALIZABLE", |session, memory, _| {
        complete(session.common_tx_mode_isolation_sll());
        while session.records.len() + 2 < session.records.capacity() { session.new_invocation(175)?; }
        assert_eq!(session.records.len() + 2, session.records.capacity());
        session.seek_input(0)?;
        let before = session.records.len();
        let _blocker = leave(memory, child_charge(1) + child_charge(2));
        let result = session.common_tx_mode_isolation_sll();
        admission(&result);
        let parent = InvocationKey::at(before + 1);
        assert_eq!(session.records.len(), before + 2);
        assert!(session.current().is_none());
        assert_eq!(session.state(), ParserState::RootSentinel);
        assert_eq!(session.input_index(), Some(2));
        assert_eq!(session.records[parent.index].stop.unwrap().index(), 1);
        assert_eq!(terminals(session.invocation_view().context(parent).unwrap()), [(0, "ISOLATION"), (1, "LEVEL")]);
        assert!(session.invocation_view().exception(parent).is_none());
        assert_eq!(session.syntax_errors, 0);
        Ok(())
    });
}

#[test]
fn label_constructor_denial_exits_the_generic_without_publishing_a_new_class() {
    fixture("SERIALIZABLE", |session, memory, retained| {
        complete(session.level_of_isolation_sll());
        while session.records.len() + 1 < session.records.capacity() { session.new_invocation(175)?; }
        let before = session.records.len();
        session.seek_input(0)?;
        let _blockers = (leave(memory, 0), leave(retained, 0));
        let result = session.level_of_isolation_sll();
        admission(&result);
        assert_eq!(session.records.len(), before + 1);
        let generic = &session.records[before];
        assert!(generic.class.is_none());
        assert_eq!(generic.start.unwrap().index(), 0);
        assert!(generic.stop.is_none());
        assert!(generic.children.is_none());
        assert!(generic.exception.is_none());
        assert!(session.current().is_none());
        assert_eq!(session.state(), ParserState::RootSentinel);
        assert_eq!(session.input_index(), Some(0));
        assert_eq!(session.syntax_errors, 0);
        Ok(())
    });
}

#[test]
fn ordinary_constructor_denial_precedes_current_state_and_token_demand() {
    fixture("SERIALIZABLE", |session, memory, _| {
        let _blocker = leave(memory, 0);
        let result = session.level_of_isolation_sll();
        admission(&result);
        assert!(session.records.is_empty());
        assert!(session.current().is_none());
        assert_eq!(session.state(), ParserState::RootSentinel);
        assert_eq!(session.input_index(), None);
        assert_eq!(session.syntax_errors, 0);
        Ok(())
    });
}

#[test]
fn root_lt1_lexer_failure_has_no_entered_body_or_invented_finally() {
    fixture("@", |session, _, _| {
        let result = session.level_of_isolation_sll();
        assert!(matches!(result, Err(PassError::Runtime(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_)))))));
        assert_eq!(session.records.len(), 1);
        assert_eq!(session.current(), Some(InvocationKey::at(0)));
        assert_eq!(session.state(), ParserState::State(350));
        assert_eq!(session.input_index(), None);
        assert!(session.records[0].start.is_none());
        assert!(session.records[0].stop.is_none());
        assert!(session.records[0].children.is_none());
        assert!(session.records[0].exception.is_none());
        assert_eq!(session.syntax_errors, 0);
        Ok(())
    });
}

#[test]
fn terminal_growth_denial_follows_actual_consume_and_finally_records_the_consumed_token() {
    fixture("READ COMMITTED", |session, memory, retained| {
        complete(session.level_of_isolation_sll());
        session.seek_input(0)?;
        let before = session.records.len();
        let _blockers = (leave(memory, child_charge(1)), leave(retained, 0));
        let result = session.level_of_isolation_sll();
        admission(&result);
        let labelled = InvocationKey::at(before + 1);
        assert_eq!(session.records.len(), before + 2);
        assert_eq!(session.records[labelled.index].class, Some(ContextClass::ReadCommittedIsolation));
        assert_eq!(terminals(session.invocation_view().context(labelled).unwrap()), [(0, "READ")]);
        assert_eq!(session.records[labelled.index].stop.unwrap().index(), 1);
        assert_eq!(session.input_index(), Some(2));
        assert_eq!(session.buffered_token(2).unwrap().lexeme().kind(), Kind::Eof);
        assert!(session.current().is_none());
        assert_eq!(session.state(), ParserState::RootSentinel);
        assert!(session.invocation_view().exception(labelled).is_none());
        assert_eq!(session.syntax_errors, 0);
        Ok(())
    });
}

#[test]
fn lexer_failure_during_the_second_match_precedes_its_terminal_attachment() {
    fixture("READ COMMITTED @", |session, _, _| {
        let result = session.level_of_isolation_sll();
        assert!(matches!(result, Err(PassError::Runtime(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_)))))));
        let labelled = InvocationKey::at(1);
        assert_eq!(session.records[labelled.index].class, Some(ContextClass::ReadCommittedIsolation));
        assert_eq!(terminals(session.invocation_view().context(labelled).unwrap()), [(0, "READ")]);
        assert_eq!(session.records[labelled.index].stop.unwrap().index(), 0);
        assert_eq!(session.input_index(), Some(1));
        assert!(session.buffered_token(2).is_none());
        assert!(session.current().is_none());
        assert_eq!(session.state(), ParserState::RootSentinel);
        assert!(session.invocation_view().exception(labelled).is_none());
        assert_eq!(session.syntax_errors, 0);
        Ok(())
    });
}

#[test]
fn adaptive_catch_reports_bails_and_finally_exits_the_live_labelled_ancestor_chain() {
    fixture("ISOLATION LEVEL READ read @", |session, _, _| {
        let result = session.common_tx_mode_isolation_sll();
        assert!(matches!(result, Err(PassError::Cancelled(_))));
        let parent = InvocationKey::at(1);
        let child = InvocationKey::at(2);
        assert_eq!(session.records.len(), 3);
        assert!(matches!(session.control, ParserControl::BailCancelled { exception, recovery: BailRecovery::Entered } if exception == child));
        assert_eq!(session.syntax_errors, 1);
        assert_eq!(session.input_index(), Some(2));
        assert!(session.buffered_token(4).is_none());
        assert!(session.current().is_none());
        assert_eq!(session.state(), ParserState::RootSentinel);
        assert_eq!(session.records[child.index].stop.unwrap().index(), 1);
        assert_eq!(session.records[parent.index].stop.unwrap().index(), 1);
        assert!(session.records[child.index].class.is_none());
        assert!(matches!(session.records[child.index].exception, Some(InvocationException::Owned(_))));
        let view = session.invocation_view();
        let original = view.exception(child).unwrap();
        let InvocationCause::NoViableAlternative(cause) = original else { panic!("adaptive cause"); };
        assert_eq!(cause.state, ParserState::State(3014));
        assert_eq!((cause.start.index(), cause.offending.index()), (2, 3));
        assert_eq!((cause.start.lexeme().raw(), cause.offending.lexeme().raw()), ("READ", "read"));
        assert!(std::ptr::eq(original, view.exception(parent).unwrap()));
        assert!(view.exception(InvocationKey::at(0)).is_none());
        assert!(matches!(session.records[parent.index].children.as_ref().unwrap().last(), Some(ContextChild::Context(key)) if *key == child));
        Ok(())
    });
}

#[test]
fn inline_bail_skips_the_recognition_catch_and_keeps_the_last_real_terminal() {
    fixture("ISOLATION LEVEL REPEATABLE wrong @", |session, _, _| {
        let result = session.common_tx_mode_isolation_sll();
        assert!(matches!(result, Err(PassError::Cancelled(_))));
        let parent = InvocationKey::at(1);
        let child = InvocationKey::at(3);
        assert_eq!(session.records[child.index].class, Some(ContextClass::RepeatableReadIsolation));
        assert_eq!(terminals(session.invocation_view().context(child).unwrap()), [(2, "REPEATABLE")]);
        assert_eq!(session.records[child.index].stop.unwrap().index(), 2);
        assert_eq!(session.records[parent.index].stop.unwrap().index(), 2);
        assert_eq!(session.input_index(), Some(3));
        assert!(session.buffered_token(4).is_none());
        assert_eq!(session.syntax_errors, 0);
        assert!(matches!(session.control, ParserControl::BailCancelled { exception, recovery: BailRecovery::Clear } if exception == child));
        let view = session.invocation_view();
        let cause = view.exception(child).unwrap();
        let InvocationCause::InputMismatch(mismatch) = cause else { panic!("inline mismatch"); };
        assert_eq!(mismatch.state, ParserState::State(3012));
        assert_eq!((mismatch.offending.index(), mismatch.offending.lexeme().raw()), (3, "wrong"));
        assert!(std::ptr::eq(cause, view.exception(parent).unwrap()));
        assert!(session.current().is_none());
        assert_eq!(session.state(), ParserState::RootSentinel);
        Ok(())
    });
}

#[test]
fn the_existing_wrapper_does_not_retry_or_repair_a_post_current_native_attachment_failure() {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    let memory = MemoryBudget::new(64 << 20);
    let retained = MemoryBudget::new(64 << 20);
    let work = AllocationScope::new(&memory).unwrap();
    let policy = CapturedClassPolicy::from_captured(ClosureOptions {
        pruning: LoopEntryOption::Enabled, assertions: SourceAssertions::Enabled,
    });
    let mut environment = ParserEnvironment::new(PredictionStorage::new(grammar, &retained), policy, &work).unwrap();
    let mut calls = 0;
    let result = super::super::with_bail_fallback(&mut environment, "ISOLATION LEVEL SERIALIZABLE", &work, |session, mode| {
        calls += 1;
        assert_eq!(mode, PredictionMode::Sll);
        complete(session.common_tx_mode_isolation_sll());
        session.seek_input(0)?;
        let before = session.records.len();
        let _blocker = leave(&memory, child_charge(1) + child_charge(2));
        let result = session.common_tx_mode_isolation_sll();
        admission(&result);
        assert_eq!(session.current(), Some(InvocationKey::at(before + 1)));
        assert_eq!(session.state(), ParserState::State(2884));
        assert_eq!(session.records[before + 2].stop.unwrap().index(), 1);
        assert!(session.records[before + 1].stop.is_none());
        result.map(|_| ())
    });
    assert_eq!(calls, 1);
    assert!(matches!(result, Err(RuntimeError::Prediction(PredictionError::Admission(error)))
        if error.code == "rtdb/memory-budget-exceeded"));
    drop(environment);
    drop(work);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(retained.used_bytes(), 0);
    assert_eq!(grammar_memory.used_bytes(), 0);
}

#[test]
fn the_private_entry_does_not_claim_other_common_branches_or_an_ll_rule_body() {
    for source in ["TIME @", "TIMEZONE @", "READ @"] {
        fixture(source, |session, _, _| {
            let result = session.common_tx_mode_isolation_sll();
            assert!(matches!(result, Err(PassError::Runtime(RuntimeError::Control(RuntimeProblem::PredictionSite)))));
            assert_eq!(session.records.len(), 1);
            assert!(session.records[0].class.is_none());
            assert!(session.records[0].exception.is_none());
            assert_eq!(session.input_index(), Some(0));
            assert!(session.buffered_token(1).is_none());
            assert!(session.current().is_none());
            assert_eq!(session.syntax_errors, 0);
            Ok(())
        });
    }
    fixture("SERIALIZABLE", |session, _, _| {
        session.profile = ParserProfile::LlDefault;
        let result = session.level_of_isolation_sll();
        assert!(matches!(result, Err(PassError::Runtime(RuntimeError::Control(RuntimeProblem::PredictionSite)))));
        assert!(session.records.is_empty());
        assert_eq!(session.input_index(), None);
        assert!(session.current().is_none());
        Ok(())
    });
}
