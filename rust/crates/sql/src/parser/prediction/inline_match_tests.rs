use super::*;
use crate::parser::prediction::{PredictionError, runtime::TokenDiagnostic};

fn match_atom<'parse, 'sql>(
    session: &mut ParserSession<'parse, 'sql, '_, '_>,
    state: u32,
) -> PassResult<'parse, 'sql, IndexedLexeme<'sql>> {
    session.set_state(state)?;
    session.match_isolation_atom()
}

fn enter_caller<'parse, 'sql>(
    session: &mut ParserSession<'parse, 'sql, '_, '_>,
) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
    let caller = session.new_invocation(164)?;
    session.enter_rule(caller)?;
    session.set_state(2886)?;
    session.sync_required_block()?;
    assert_eq!(session.lookahead(1)?.unwrap().lexeme().raw(), "ISOLATION");
    let matched = match_atom(session, 2882)?;
    assert_eq!((matched.index(), matched.lexeme().raw()), (0, "ISOLATION"));
    Ok(caller)
}

fn enter_level<'parse, 'sql>(
    session: &mut ParserSession<'parse, 'sql, '_, '_>,
) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
    let matched = match_atom(session, 2883)?;
    assert_eq!((matched.index(), matched.lexeme().raw()), (1, "LEVEL"));
    session.set_state(2884)?;
    let level = session.new_invocation(175)?;
    session.enter_rule(level)?;
    session.set_state(3014)?;
    session.sync_required_block()?;
    Ok(level)
}

fn select_repeatable<'parse, 'sql>(
    session: &mut ParserSession<'parse, 'sql, '_, '_>,
    mode: PredictionMode,
) -> PassResult<'parse, 'sql, ()> {
    assert_eq!(session.adaptive_predict(DECISION, mode)?.number(), 3);
    assert_eq!(session.input_index(), Some(2));
    Ok(())
}

fn report<'sql, T>(result: RuntimeResult<'sql, T>) -> TokenDiagnostic<'sql> {
    let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("expected inline token report"); };
    report
}

fn direct_profile<'sql>(
    sql: &'sql str,
    profile: ParserProfile,
    run: impl for<'parse, 'env, 'work> FnOnce(&mut ParserSession<'parse, 'sql, 'env, 'work>) -> RuntimeResult<'sql, ()>,
) {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let retained = MemoryBudget::new(64 << 20);
    let memory = MemoryBudget::new(64 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    let work = AllocationScope::new(&memory).unwrap();
    let policy = CapturedClassPolicy::from_captured(ClosureOptions {
        pruning: LoopEntryOption::Enabled,
        assertions: SourceAssertions::Enabled,
    });
    let mut environment = ParserEnvironment::new(PredictionStorage::new(grammar, &retained), policy, &work).unwrap();
    with_parser_session(&mut environment, sql, &work, |session| {
        session.profile = profile;
        run(session)
    }).unwrap();
    drop(environment);
    drop(work);
    assert_eq!(grammar_memory.used_bytes(), 0);
    assert_eq!(retained.used_bytes(), 0);
    assert_eq!(memory.used_bytes(), 0);
}

fn match_valid_isolation<'parse, 'sql>(
    session: &mut ParserSession<'parse, 'sql, '_, '_>,
    mode: PredictionMode,
    alternative: usize,
    atoms: &[(u32, &str)],
) -> PassResult<'parse, 'sql, ()> {
    let caller = enter_caller(session)?;
    let level = enter_level(session)?;
    assert_eq!(session.adaptive_predict(DECISION, mode)?.number(), alternative);
    for (index, &(state, expected)) in atoms.iter().enumerate() {
        let matched = match_atom(session, state)?;
        assert_eq!((matched.index(), matched.lexeme().raw()), (index + 2, expected));
    }
    assert_eq!(session.input_index(), Some(atoms.len() + 2));
    assert!(session.buffered_token(atoms.len() + 2).is_some_and(|token| token.lexeme().kind() == Kind::Eof));
    assert!(!session.matched_eof);
    assert_eq!(session.syntax_errors, 0);
    assert!(session.invocation_view().exception(level).is_none());
    assert!(session.invocation_view().exception(caller).is_none());
    session.exit_rule()?;
    session.exit_rule()?;
    Ok(())
}

#[test]
fn every_generated_isolation_atom_returns_its_matched_occurrence_in_both_profiles() {
    for (source, alternative, atoms) in [
        ("ISOLATION LEVEL READ UNCOMMITTED", 1, &[(3007, "READ"), (3008, "UNCOMMITTED")][..]),
        ("ISOLATION LEVEL READ COMMITTED", 2, &[(3009, "READ"), (3010, "COMMITTED")][..]),
        ("ISOLATION LEVEL REPEATABLE READ", 3, &[(3011, "REPEATABLE"), (3012, "READ")][..]),
        ("ISOLATION LEVEL SERIALIZABLE", 4, &[(3013, "SERIALIZABLE")][..]),
    ] {
        let mut calls = 0;
        let (result, memory) = fixture(source, |session, mode, _, _| {
            calls += 1;
            match_valid_isolation(session, mode, alternative, atoms)
        });
        result.unwrap();
        assert_eq!(calls, 1);
        assert_eq!(memory.used_bytes(), 0);

        direct_profile(source, ParserProfile::LlDefault, |session| {
            match_valid_isolation(session, PredictionMode::Ll, alternative, atoms).unwrap();
            Ok(())
        });
    }
}

#[test]
fn the_kernel_and_wrong_atom_sites_are_rejected_before_more_input_is_demanded() {
    for (profile, state, problem) in [
        (ParserProfile::Kernel, 2882, RuntimeProblem::Aborted),
        (ParserProfile::LlDefault, 2884, RuntimeProblem::PredictionSite),
        (ParserProfile::LlDefault, 3011, RuntimeProblem::PredictionSite),
    ] {
        direct_profile("ISOLATION @", profile, |session| {
            let caller = session.new_invocation(164)?;
            session.enter_rule(caller)?;
            let result = match_atom(session, state);
            assert!(matches!(result, Err(PassError::Runtime(RuntimeError::Control(actual))) if actual == problem));
            assert_eq!(session.input_index(), Some(0));
            assert!(session.buffered_token(1).is_none());
            assert_eq!(session.syntax_errors, 0);
            assert!(session.invocation_view().exception(caller).is_none());
            session.exit_rule()
        });
    }
}

#[test]
fn deletion_and_insertion_in_the_isolation_caller_report_without_advancing() {
    for (source, spelling, next, message) in [
        ("ISOLATION wrong LEVEL @", "wrong", "LEVEL", "extraneous input 'wrong' expecting 'LEVEL'"),
        ("ISOLATION REPEATABLE READ @", "REPEATABLE", "READ", "missing 'LEVEL' at 'REPEATABLE'"),
    ] {
        let mut calls = 0;
        let (result, memory) = fixture(source, |session, mode, _, _| {
            calls += 1;
            let caller = enter_caller(session)?;
            let result = match_atom(session, 2883);
            assert_eq!(session.input_index(), Some(1));
            assert!(session.buffered_token(3).is_none());
            if mode == PredictionMode::Ll {
                assert_eq!(session.buffered_token(2).unwrap().lexeme().raw(), next);
                assert!(matches!(session.control, ParserControl::InErrorRecovery));
                assert_eq!(session.syntax_errors, 1);
                assert!(session.invocation_view().exception(caller).is_none());
            } else {
                assert!(matches!(&result, Err(PassError::Cancelled(_))));
                assert!(session.buffered_token(2).is_none());
                assert_eq!(session.syntax_errors, 0);
            }
            assert_eq!(session.current(), Some(caller));
            session.exit_rule()?;
            assert_eq!(session.records[caller.index].stop.unwrap().index(), 0);
            result
        });
        assert_eq!(calls, 2);
        let report = report(result);
        assert!(report.cause().is_none());
        assert_eq!((report.offending().index(), report.offending().lexeme().raw()), (1, spelling));
        let message = format!("Errors parsing SQL statement:\n  - line 1:10 {message}");
        assert_eq!(report.anomaly().message.as_deref(), Some(message.as_str()));
        assert_eq!(memory.used_bytes(), message.len() + "xtdb/sql-error".len() + 256);
        drop(report);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn deletion_and_eof_insertion_after_repeatable_keep_the_live_child_and_parent_clear() {
    for (source, eof, message) in [
        ("ISOLATION LEVEL REPEATABLE wrong READ @", false, "1:27 extraneous input 'wrong' expecting 'READ'"),
        ("ISOLATION LEVEL REPEATABLE", true, "1:26 missing 'READ' at '<EOF>'"),
    ] {
        let mut calls = 0;
        let (result, memory) = fixture(source, |session, mode, _, _| {
            calls += 1;
            let caller = enter_caller(session)?;
            let level = enter_level(session)?;
            select_repeatable(session, mode)?;
            assert_eq!(match_atom(session, 3011)?.lexeme().raw(), "REPEATABLE");
            let result = match_atom(session, 3012);
            assert_eq!(session.input_index(), Some(3));
            assert_eq!(session.current(), Some(level));
            assert!(!session.matched_eof);
            assert!(session.buffered_token(5).is_none());
            if mode == PredictionMode::Ll {
                assert_eq!(session.syntax_errors, 1);
                assert!(matches!(session.control, ParserControl::InErrorRecovery));
                assert!(session.invocation_view().exception(caller).is_none());
                assert!(session.invocation_view().exception(level).is_none());
                if eof {
                    assert!(session.buffered_token(3).is_some_and(|token| token.lexeme().kind() == Kind::Eof));
                    assert!(session.buffered_token(4).is_none());
                } else {
                    assert_eq!(session.buffered_token(4).unwrap().lexeme().raw(), "READ");
                }
            } else {
                assert!(matches!(&result, Err(PassError::Cancelled(_))));
                assert_eq!(session.syntax_errors, 0);
                assert!(session.buffered_token(4).is_none());
            }
            session.exit_rule()?;
            assert_eq!(session.current(), Some(caller));
            assert_eq!(session.records[level.index].stop.unwrap().index(), 2);
            session.exit_rule()?;
            assert_eq!(session.records[caller.index].stop.unwrap().index(), 2);
            result
        });
        assert_eq!(calls, 2);
        let report = report(result);
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
fn inline_mismatch_is_caught_with_the_live_local_and_the_original_offending_occurrence() {
    let mut calls = 0;
    let (result, memory) = fixture("ISOLATION LEVEL REPEATABLE wrong other @", |session, mode, _, _| {
        calls += 1;
        let caller = enter_caller(session)?;
        let level = enter_level(session)?;
        select_repeatable(session, mode)?;
        match_atom(session, 3011)?;
        let result = match_atom(session, 3012);
        let result = if mode == PredictionMode::Ll {
            let Err(PassError::Runtime(RuntimeError::InputMismatch(cause))) = result else { panic!("expected ordinary inline mismatch"); };
            assert_eq!(cause.state, ParserState::State(3012));
            assert_eq!((cause.offending.index(), cause.offending.lexeme().raw()), (3, "wrong"));
            assert_eq!(session.buffered_token(4).unwrap().lexeme().raw(), "other");
            assert!(matches!(&session.control, ParserControl::PendingInputMismatch(pending) if pending.invocation == level));
            assert_eq!(session.syntax_errors, 0);
            assert!(session.invocation_view().exception(level).is_none());
            let reported = session.catch_recognition(level);
            let view = session.invocation_view();
            let Some(InvocationCause::InputMismatch(stored)) = view.exception(level) else { panic!("expected catch assignment"); };
            assert_eq!((stored.state, stored.offending.index()), (cause.state, cause.offending.index()));
            assert!(view.exception(caller).is_none());
            assert_eq!(session.syntax_errors, 1);
            assert!(matches!(session.control, ParserControl::InErrorRecovery));
            reported
        } else {
            assert!(matches!(&result, Err(PassError::Cancelled(_))));
            assert!(session.buffered_token(4).is_none());
            result
        };
        assert_eq!(session.input_index(), Some(3));
        assert!(session.buffered_token(5).is_none());
        session.exit_rule()?;
        session.exit_rule()?;
        result
    });
    assert_eq!(calls, 2);
    let report = report(result);
    let cause = report.cause().unwrap();
    assert_eq!(cause.state, ParserState::State(3012));
    assert_eq!(cause.offending.index(), 3);
    assert_eq!(report.anomaly().message.as_deref(), Some(
        "Errors parsing SQL statement:\n  - line 1:27 mismatched input 'wrong' expecting 'READ'",
    ));
    drop(report);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn lexer_failure_preempts_recovery_at_the_first_reached_demand() {
    for (source, failure_on_match, column) in [
        ("ISOLATION LEVEL REPEATABLE @", true, 27),
        ("ISOLATION LEVEL REPEATABLE wrong @", false, 33),
    ] {
        let mut calls = 0;
        let (result, memory) = fixture(source, |session, mode, _, _| {
            calls += 1;
            let caller = enter_caller(session)?;
            let level = enter_level(session)?;
            select_repeatable(session, mode)?;
            let result = if failure_on_match {
                match_atom(session, 3011)
            } else {
                match_atom(session, 3011)?;
                match_atom(session, 3012)
            };
            assert_eq!(session.input_index(), Some(if failure_on_match { 2 } else { 3 }));
            assert_eq!(session.state(), ParserState::State(if failure_on_match { 3011 } else { 3012 }));
            assert_eq!(session.syntax_errors, 0);
            assert!(session.buffered_token(if failure_on_match { 3 } else { 4 }).is_none());
            if !failure_on_match && mode == PredictionMode::Sll {
                assert!(matches!(&result, Err(PassError::Cancelled(_))));
            } else {
                assert!(matches!(&result, Err(PassError::Runtime(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_)))))));
                assert!(matches!(session.control, ParserControl::Failed));
                assert!(session.invocation_view().exception(caller).is_none());
                assert!(session.invocation_view().exception(level).is_none());
            }
            session.exit_rule()?;
            session.exit_rule()?;
            result
        });
        assert_eq!(calls, if failure_on_match { 1 } else { 2 });
        let Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(error)))) = result else { panic!("expected native lexer failure"); };
        assert_eq!(error.span().start.column, column);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn inline_insertion_admission_distinguishes_look_parser_message_and_listener() {
    let source = "ISOLATION REPEATABLE READ @";
    let message = "missing 'LEVEL' at 'REPEATABLE'";
    for breakpoint in 0..3 {
        let mut calls = 0;
        let (result, memory) = fixture(source, |session, mode, memory, _| {
            calls += 1;
            let caller = enter_caller(session)?;
            if mode == PredictionMode::Sll {
                let result = match_atom(session, 2883);
                session.exit_rule()?;
                return result;
            }

            let probe = AllocationScope::new(memory).unwrap();
            let before = memory.used_bytes();
            let expected_bytes = {
                let grammar = session.environment.storage.grammar_view();
                let state = grammar.state(2883).unwrap();
                state.next_tokens(&probe).unwrap();
                let expected_bytes = memory.used_bytes() - before;
                let Some(TransitionView::Atom { target, .. }) = state.transitions().next() else { panic!("expected LEVEL atom"); };
                let view = session.invocation_view();
                target.next_tokens_with_context(view.invocations(&grammar), &probe).unwrap();
                expected_bytes
            };
            let all_look_bytes = memory.used_bytes() - before;
            drop(probe);
            assert!(all_look_bytes > expected_bytes);
            let available = match breakpoint {
                0 => expected_bytes,
                1 => all_look_bytes,
                2 => all_look_bytes + message.len() + 256,
                _ => unreachable!(),
            };
            let _blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes() - available).unwrap();
            let result = match_atom(session, 2883);
            assert_eq!(session.input_index(), Some(1));
            assert_eq!(session.buffered_token(2).unwrap().lexeme().raw(), "READ");
            assert!(session.buffered_token(3).is_none());
            assert_eq!(session.syntax_errors, if breakpoint == 2 { 1 } else { 0 });
            assert_eq!(matches!(session.control, ParserControl::InErrorRecovery), breakpoint != 0);
            assert!(session.invocation_view().exception(caller).is_none());
            session.exit_rule()?;
            result
        });
        assert_eq!(calls, 2);
        assert!(matches!(result, Err(RuntimeError::Prediction(PredictionError::Admission(error)))
            if error.code == "rtdb/memory-budget-exceeded"));
        assert_eq!(memory.used_bytes(), 0);
    }
}
