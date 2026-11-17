use std::{alloc::Layout, sync::Arc};
use rtdb_types::{AllocationScope, MemoryBudget, MemoryReservation};
use crate::lexer::{Kind, ScanError, StreamError};
use crate::parser::grammar::OwnedGrammar;
use crate::parser::prediction::{PredictionError, PredictionStorage};
use crate::parser::prediction::closure::{ClosureOptions, LoopEntryOption, SourceAssertions};
use super::*;
use super::super::{BailRecovery, CapturedClassPolicy, InvocationCause, InvocationException, ParserControl, ParserEnvironment, ParserState, RuleContextView, TokenDiagnostic, with_bail_fallback, with_parser_session};

fn environment_fixture<T>(
    run: impl FnOnce(&mut ParserEnvironment, &AllocationScope, &Arc<MemoryBudget>, &Arc<MemoryBudget>) -> T,
) -> (T, Arc<MemoryBudget>) {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    let memory = MemoryBudget::new(64 << 20);
    let retained = MemoryBudget::new(64 << 20);
    let work = AllocationScope::new(&memory).unwrap();
    let policy = CapturedClassPolicy::from_captured(ClosureOptions {
        pruning: LoopEntryOption::Enabled, assertions: SourceAssertions::Enabled,
    });
    let mut environment = ParserEnvironment::new(PredictionStorage::new(grammar, &retained), policy, &work).unwrap();
    let result = run(&mut environment, &work, &memory, &retained);
    drop(environment);
    drop(work);
    assert_eq!(retained.used_bytes(), 0);
    assert_eq!(grammar_memory.used_bytes(), 0);
    (result, memory)
}

fn profile_fixture<'sql>(
    sql: &'sql str,
    profile: ParserProfile,
    run: impl for<'parse, 'env, 'work> FnOnce(
        &mut ParserSession<'parse, 'sql, 'env, 'work>, &Arc<MemoryBudget>, &Arc<MemoryBudget>,
    ) -> RuntimeResult<'sql, ()>,
) {
    let (result, memory) = environment_fixture(|environment, work, memory, retained| {
        with_parser_session(environment, sql, work, |session| {
            session.profile = profile;
            run(session, memory, retained)
        })
    });
    result.unwrap();
    assert_eq!(memory.used_bytes(), 0);
}

fn fixture<'sql>(
    sql: &'sql str,
    run: impl for<'parse, 'env, 'work> FnOnce(
        &mut ParserSession<'parse, 'sql, 'env, 'work>, &Arc<MemoryBudget>, &Arc<MemoryBudget>,
    ) -> RuntimeResult<'sql, ()>,
) {
    profile_fixture(sql, ParserProfile::SllBail, run);
}

fn both_profiles<'sql>(
    sql: &'sql str,
    run: impl for<'parse, 'env, 'work> Fn(
        &mut ParserSession<'parse, 'sql, 'env, 'work>, &Arc<MemoryBudget>, &Arc<MemoryBudget>,
    ) -> RuntimeResult<'sql, ()>,
) {
    for profile in [ParserProfile::SllBail, ParserProfile::LlDefault] {
        profile_fixture(sql, profile, |session, memory, retained| run(session, memory, retained));
    }
}

fn retry_fixture<'sql, T>(
    sql: &'sql str,
    mut run: impl for<'parse, 'env, 'work> FnMut(
        &mut ParserSession<'parse, 'sql, 'env, 'work>, PredictionMode, &Arc<MemoryBudget>,
    ) -> PassResult<'parse, 'sql, T>,
) -> (RuntimeResult<'sql, T>, Arc<MemoryBudget>) {
    environment_fixture(|environment, work, _, retained| {
        with_bail_fallback(environment, sql, work, |session, mode| run(session, mode, retained))
    })
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
        both_profiles(source, |session, _, _| {
            let key = complete(session.level_of_isolation());
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
    both_profiles("ISOLATION LEVEL SERIALIZABLE trailing @", |session, _, _| {
        let parent = complete(session.common_tx_mode_isolation());
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
        complete(session.common_tx_mode_isolation());
        session.seek_input(0)?;
        let before = session.records.len();
        assert!(before + 3 <= session.records.capacity());
        let retained_before = retained.used_bytes();
        let _blocker = leave(memory, child_charge(1) + child_charge(2));
        let result = session.common_tx_mode_isolation();
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
        complete(session.common_tx_mode_isolation());
        while session.records.len() + 2 < session.records.capacity() { session.new_invocation(175)?; }
        assert_eq!(session.records.len() + 2, session.records.capacity());
        session.seek_input(0)?;
        let before = session.records.len();
        let _blocker = leave(memory, child_charge(1) + child_charge(2));
        let result = session.common_tx_mode_isolation();
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
        complete(session.level_of_isolation());
        while session.records.len() + 1 < session.records.capacity() { session.new_invocation(175)?; }
        let before = session.records.len();
        session.seek_input(0)?;
        let _blockers = (leave(memory, 0), leave(retained, 0));
        let result = session.level_of_isolation();
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
        let result = session.level_of_isolation();
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
        let result = session.level_of_isolation();
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
        complete(session.level_of_isolation());
        session.seek_input(0)?;
        let before = session.records.len();
        let _blockers = (leave(memory, child_charge(1)), leave(retained, 0));
        let result = session.level_of_isolation();
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
        let result = session.level_of_isolation();
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
        let result = session.common_tx_mode_isolation();
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
        let result = session.common_tx_mode_isolation();
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
        complete(session.common_tx_mode_isolation());
        session.seek_input(0)?;
        let before = session.records.len();
        let _blocker = leave(&memory, child_charge(1) + child_charge(2));
        let result = session.common_tx_mode_isolation();
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
fn the_private_entry_does_not_claim_other_common_branches_or_the_kernel() {
    for source in ["TIME @", "TIMEZONE @", "READ @"] {
        fixture(source, |session, _, _| {
            let result = session.common_tx_mode_isolation();
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
    profile_fixture("SERIALIZABLE", ParserProfile::Kernel, |session, _, _| {
        let result = session.level_of_isolation();
        assert!(matches!(result, Err(PassError::Runtime(RuntimeError::Control(RuntimeProblem::PredictionSite)))));
        assert!(session.records.is_empty());
        assert_eq!(session.input_index(), None);
        assert!(session.current().is_none());
        Ok(())
    });
}

fn completed_isolation<'a, 'parse, 'sql>(
    session: &'a ParserSession<'parse, 'sql, '_, '_>,
    first_record: usize,
) -> [RuleContextView<'a, 'parse, 'sql>; 2] {
    assert!(session.current().is_none());
    assert_eq!(session.state(), ParserState::RootSentinel);
    assert!(!session.matched_eof);
    assert_eq!(session.precedence(), 0);
    let view = session.invocation_view();
    let caller = view.context(InvocationKey::at(first_record + 1)).unwrap();
    assert_eq!(caller.class(), Some(ContextClass::IsolationLevelTxMode));
    assert_eq!(caller.start().unwrap().index(), 0);
    let [ContextChild::Terminal(first), ContextChild::Terminal(second), ContextChild::Context(level)] = caller.children() else {
        panic!("expected actual ordered caller children");
    };
    assert_eq!((first.index(), first.lexeme().raw()), (0, "ISOLATION"));
    assert_eq!((second.index(), second.lexeme().raw()), (1, "LEVEL"));
    let level = view.context(*level).unwrap();
    assert_eq!(level.parent(), Some(caller.key()));
    assert_eq!(level.invoking_state(), ParserState::State(2884));
    assert_eq!(level.start().unwrap().index(), 2);
    [caller, level]
}

fn token_report<'sql>(result: RuntimeResult<'sql, ()>) -> TokenDiagnostic<'sql> {
    let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("expected an LL token report"); };
    assert_eq!(report.anomaly().code, "xtdb/sql-error");
    report
}

#[test]
fn the_wrapper_returns_generated_success_without_an_ll_pass() {
    let mut calls = 0;
    let (result, memory) = retry_fixture("ISOLATION LEVEL SERIALIZABLE trailing @", |session, mode, _| {
        calls += 1;
        assert_eq!(mode, PredictionMode::Sll);
        let root = session.common_tx_mode_isolation()?;
        let [caller, level] = completed_isolation(session, 0);
        assert_eq!(root, caller.key());
        assert_eq!(terminals(level), [(2, "SERIALIZABLE")]);
        assert_eq!(session.input_index(), Some(3));
        assert!(session.buffered_token(4).is_none());
        Ok(level.class())
    });
    assert_eq!(result.unwrap(), Some(ContextClass::SerializableIsolation));
    assert_eq!(calls, 1);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn the_generated_inline_retry_keeps_one_session_and_assigns_only_the_ll_local_mismatch() {
    let mut calls = 0;
    let mut sll_owners = None;
    let mut sll_retained = None;
    let (result, memory) = retry_fixture("ISOLATION LEVEL REPEATABLE wrong other @", |session, mode, retained| {
        calls += 1;
        let owners = [
            std::ptr::from_ref(&*session).cast::<()>() as usize,
            std::ptr::from_ref(&session.input).cast::<()>() as usize,
            std::ptr::from_ref(&*session.environment).cast::<()>() as usize,
        ];
        let before = session.records.len();
        match mode {
            PredictionMode::Sll => {
                assert_eq!(calls, 1);
                assert_eq!(before, 0);
                assert_eq!(session.input_index(), None);
                sll_owners = Some(owners);
            }
            PredictionMode::Ll => {
                assert_eq!(calls, 2);
                assert_eq!(before, 4);
                assert_eq!(sll_owners, Some(owners));
                assert_eq!(sll_retained, Some(retained.used_bytes()));
                assert_eq!(session.input_index(), Some(0));
                assert_eq!(session.buffered_token(3).unwrap().lexeme().raw(), "wrong");
                assert!(session.buffered_token(4).is_none());
                assert!(session.control.is_ready());
                assert_eq!(session.syntax_errors, 0);
                let [caller, level] = completed_isolation(session, 0);
                let view = session.invocation_view();
                assert!(std::ptr::eq(view.exception(caller.key()).unwrap(), view.exception(level.key()).unwrap()));
            }
        }

        let result = session.common_tx_mode_isolation();
        let [caller, level] = completed_isolation(session, before);
        assert_eq!(level.class(), Some(ContextClass::RepeatableReadIsolation));
        assert_eq!(terminals(level), [(2, "REPEATABLE")]);
        assert_eq!(level.stop().unwrap().index(), 2);
        assert_eq!(caller.stop().unwrap().index(), 2);
        assert_eq!(session.input_index(), Some(3));
        assert!(session.buffered_token(5).is_none());
        assert_eq!(session.records.len(), before + 4);
        let view = session.invocation_view();
        let original = view.exception(level.key()).unwrap();
        let InvocationCause::InputMismatch(cause) = original else { panic!("expected inline mismatch"); };
        assert_eq!(cause.state, ParserState::State(3012));
        assert_eq!((cause.offending.index(), cause.offending.lexeme().raw()), (3, "wrong"));
        assert!(view.exception(InvocationKey::at(before + 2)).is_none());
        match mode {
            PredictionMode::Sll => {
                assert!(matches!(&result, Err(PassError::Cancelled(_))));
                assert!(matches!(session.control, ParserControl::BailCancelled { exception, recovery: BailRecovery::Clear } if exception == level.key()));
                assert!(std::ptr::eq(view.exception(caller.key()).unwrap(), original));
                assert!(session.buffered_token(4).is_none());
                assert_eq!(session.syntax_errors, 0);
                sll_retained = Some(retained.used_bytes());
            }
            PredictionMode::Ll => {
                assert!(matches!(&result, Err(PassError::Runtime(RuntimeError::ReportedTokenSyntax(_)))));
                assert!(matches!(session.control, ParserControl::InErrorRecovery));
                assert_eq!(session.buffered_token(4).unwrap().lexeme().raw(), "other");
                assert!(view.exception(caller.key()).is_none());
                assert!(!std::ptr::eq(view.exception(InvocationKey::at(3)).unwrap(), original));
                assert_eq!(session.syntax_errors, 1);
            }
        }
        result.map(|_| ())
    });
    assert_eq!(calls, 2);
    let report = token_report(result);
    let cause = report.cause().unwrap();
    assert_eq!(cause.state, ParserState::State(3012));
    assert_eq!(cause.offending.index(), 3);
    let message = "Errors parsing SQL statement:\n  - line 1:27 mismatched input 'wrong' expecting 'READ'";
    assert_eq!(report.anomaly().message.as_deref(), Some(message));
    assert_eq!(memory.used_bytes(), message.len() + "xtdb/sql-error".len() + 256);
    drop(report);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn the_generated_ll_inline_reports_leave_only_actual_terminals_and_no_caught_cause() {
    for (source, eof, message) in [
        ("ISOLATION LEVEL REPEATABLE wrong READ @", false, "1:27 extraneous input 'wrong' expecting 'READ'"),
        ("ISOLATION LEVEL REPEATABLE", true, "1:26 missing 'READ' at '<EOF>'"),
    ] {
        let mut calls = 0;
        let (result, memory) = retry_fixture(source, |session, mode, _| {
            calls += 1;
            let before = session.records.len();
            let result = session.common_tx_mode_isolation();
            let [caller, level] = completed_isolation(session, before);
            assert_eq!(level.class(), Some(ContextClass::RepeatableReadIsolation));
            assert_eq!(terminals(level), [(2, "REPEATABLE")]);
            assert_eq!(level.stop().unwrap().index(), 2);
            assert_eq!(caller.stop().unwrap().index(), 2);
            assert_eq!(session.input_index(), Some(3));
            assert!(session.buffered_token(5).is_none());
            let view = session.invocation_view();
            if mode == PredictionMode::Ll {
                assert!(matches!(&result, Err(PassError::Runtime(RuntimeError::ReportedTokenSyntax(_)))));
                assert!(matches!(session.control, ParserControl::InErrorRecovery));
                assert_eq!(session.syntax_errors, 1);
                assert!(view.exception(caller.key()).is_none());
                assert!(view.exception(level.key()).is_none());
                if eof {
                    assert_eq!(session.buffered_token(3).unwrap().lexeme().kind(), Kind::Eof);
                    assert!(session.buffered_token(4).is_none());
                } else {
                    assert_eq!(session.buffered_token(4).unwrap().lexeme().raw(), "READ");
                }
            } else {
                assert!(matches!(&result, Err(PassError::Cancelled(_))));
                assert_eq!(session.syntax_errors, 0);
                assert!(session.buffered_token(4).is_none());
                assert!(std::ptr::eq(view.exception(caller.key()).unwrap(), view.exception(level.key()).unwrap()));
            }
            result.map(|_| ())
        });
        assert_eq!(calls, 2);
        let report = token_report(result);
        assert!(report.cause().is_none());
        assert_eq!(report.offending().index(), 3);
        assert_eq!(report.offending().lexeme().kind() == Kind::Eof, eof);
        let message = format!("Errors parsing SQL statement:\n  - line {message}");
        assert_eq!(report.anomaly().message.as_deref(), Some(message.as_str()));
        drop(report);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn the_generated_ll_sync_reports_before_the_level_context_is_labelled() {
    for (source, mismatch, second, message) in [
        ("ISOLATION LEVEL wrong READ COMMITTED", false, "READ", "extraneous input"),
        ("ISOLATION LEVEL wrong other", true, "other", "mismatched input"),
    ] {
        let mut calls = 0;
        let (result, memory) = retry_fixture(source, |session, mode, _| {
            calls += 1;
            let before = session.records.len();
            let result = session.common_tx_mode_isolation();
            let [caller, level] = completed_isolation(session, before);
            assert_eq!(level.class(), None);
            assert!(level.children().is_empty());
            assert_eq!(session.records.len(), before + 3);
            assert_eq!(level.stop().unwrap().index(), 1);
            assert_eq!(caller.stop().unwrap().index(), 1);
            assert_eq!(session.input_index(), Some(2));
            assert!(session.buffered_token(4).is_none());
            assert_eq!(session.syntax_errors, 1);
            let view = session.invocation_view();
            if mode == PredictionMode::Ll {
                assert!(matches!(&result, Err(PassError::Runtime(RuntimeError::ReportedTokenSyntax(_)))));
                assert!(matches!(session.control, ParserControl::InErrorRecovery));
                assert_eq!(session.buffered_token(3).unwrap().lexeme().raw(), second);
                assert!(view.exception(caller.key()).is_none());
                if mismatch {
                    let Some(InvocationCause::InputMismatch(cause)) = view.exception(level.key()) else {
                        panic!("expected mismatch on generic level context");
                    };
                    assert_eq!(cause.state, ParserState::State(3014));
                    assert_eq!(cause.offending.index(), 2);
                } else {
                    assert!(view.exception(level.key()).is_none());
                }
            } else {
                assert!(matches!(&result, Err(PassError::Cancelled(_))));
                assert!(session.buffered_token(3).is_none());
                assert!(matches!(session.control, ParserControl::BailCancelled { exception, recovery: BailRecovery::Entered } if exception == level.key()));
                let cause = view.exception(level.key()).unwrap();
                let InvocationCause::NoViableAlternative(cause_value) = cause else { panic!("expected adaptive cause"); };
                assert_eq!(cause_value.state, ParserState::State(3014));
                assert_eq!((cause_value.start.index(), cause_value.offending.index()), (2, 2));
                assert!(std::ptr::eq(view.exception(caller.key()).unwrap(), cause));
            }
            result.map(|_| ())
        });
        assert_eq!(calls, 2);
        let report = token_report(result);
        assert_eq!(report.cause().is_some(), mismatch);
        assert_eq!((report.offending().index(), report.offending().lexeme().raw()), (2, "wrong"));
        let message = format!("Errors parsing SQL statement:\n  - line 1:16 {message} 'wrong' expecting {{'READ', 'REPEATABLE', 'SERIALIZABLE'}}");
        assert_eq!(report.anomaly().message.as_deref(), Some(message.as_str()));
        drop(report);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn the_generated_adaptive_retry_reports_the_original_invalid_second_token() {
    let mut calls = 0;
    let (result, memory) = retry_fixture("ISOLATION LEVEL READ read", |session, mode, _| {
        calls += 1;
        let before = session.records.len();
        let result = session.common_tx_mode_isolation();
        let [caller, level] = completed_isolation(session, before);
        assert_eq!(level.class(), None);
        assert!(level.children().is_empty());
        assert_eq!(session.records.len(), before + 3);
        assert_eq!(level.stop().unwrap().index(), 1);
        assert_eq!(caller.stop().unwrap().index(), 1);
        assert_eq!(session.input_index(), Some(2));
        assert!(session.buffered_token(4).is_none());
        assert_eq!(session.syntax_errors, 1);
        let view = session.invocation_view();
        let cause = view.exception(level.key()).unwrap();
        let InvocationCause::NoViableAlternative(value) = cause else { panic!("expected adaptive cause"); };
        assert_eq!(value.state, ParserState::State(3014));
        assert_eq!((value.start.index(), value.offending.index()), (2, 3));
        assert_eq!((value.start.lexeme().raw(), value.offending.lexeme().raw()), ("READ", "read"));
        if mode == PredictionMode::Ll {
            assert!(matches!(&result, Err(PassError::Runtime(RuntimeError::ReportedNoViableAlternative(_)))));
            assert!(view.exception(caller.key()).is_none());
        } else {
            assert!(matches!(&result, Err(PassError::Cancelled(_))));
            assert!(std::ptr::eq(view.exception(caller.key()).unwrap(), cause));
        }
        result.map(|_| ())
    });
    assert_eq!(calls, 2);
    let Err(RuntimeError::ReportedNoViableAlternative(report)) = result else { panic!("expected LL adaptive report"); };
    assert_eq!((report.cause().start.index(), report.cause().offending.index()), (2, 3));
    assert_eq!(report.anomaly().message.as_deref(), Some(
        "Errors parsing SQL statement:\n  - line 1:21 no viable alternative at input 'READread'",
    ));
    drop(report);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn the_wrapper_does_not_retry_a_generated_consume_lexer_failure() {
    let mut calls = 0;
    let (result, memory) = retry_fixture("ISOLATION LEVEL REPEATABLE @", |session, mode, _| {
        calls += 1;
        assert_eq!(mode, PredictionMode::Sll);
        let result = session.common_tx_mode_isolation();
        let [caller, level] = completed_isolation(session, 0);
        assert_eq!(level.class(), Some(ContextClass::RepeatableReadIsolation));
        assert!(level.children().is_empty());
        assert_eq!(level.stop().unwrap().index(), 1);
        assert_eq!(caller.stop().unwrap().index(), 1);
        assert_eq!(session.input_index(), Some(2));
        assert!(session.buffered_token(3).is_none());
        assert_eq!(session.records.len(), 4);
        assert_eq!(session.syntax_errors, 0);
        assert!(matches!(session.control, ParserControl::Failed));
        let view = session.invocation_view();
        assert!(view.exception(caller.key()).is_none());
        assert!(view.exception(level.key()).is_none());
        result.map(|_| ())
    });
    assert_eq!(calls, 1);
    let Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(error)))) = result else { panic!("expected lexer failure"); };
    assert_eq!(error.span().start.column, 27);
    assert_eq!(memory.used_bytes(), 0);
}
