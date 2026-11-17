use std::{alloc::Layout, sync::Arc};
use rtdb_types::{AllocationScope, MemoryBudget, MemoryReservation};
use crate::parser::grammar::OwnedGrammar;
use crate::parser::prediction::{PredictionError, PredictionStorage};
use crate::parser::prediction::closure::{ClosureOptions, LoopEntryOption, SourceAssertions};
use super::*;
use super::super::{CapturedClassPolicy, ParserControl, ParserEnvironment, ParserState, RecursionPhase, with_parser_session};

fn fixture<'sql>(
    source: &'sql str,
    run: impl for<'parse, 'env, 'work> FnOnce(
        &mut ParserSession<'parse, 'sql, 'env, 'work>, &Arc<MemoryBudget>, &Arc<MemoryBudget>,
    ) -> RuntimeResult<'sql, ()>,
) {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    let work_memory = MemoryBudget::new(64 << 20);
    let retained = MemoryBudget::new(64 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let policy = CapturedClassPolicy::from_captured(ClosureOptions {
        pruning: LoopEntryOption::Enabled, assertions: SourceAssertions::Enabled,
    });
    let mut environment = ParserEnvironment::new(PredictionStorage::new(grammar, &retained), policy, &work).unwrap();
    with_parser_session(&mut environment, source, &work, |session| {
        session.profile = ParserProfile::SllBail;
        run(session, &work_memory, &retained)
    }).unwrap();
    drop(environment);
    drop(work);
    assert_eq!(work_memory.used_bytes(), 0);
    assert_eq!(retained.used_bytes(), 0);
    assert_eq!(grammar_memory.used_bytes(), 0);
}

fn complete<T>(result: PassResult<'_, '_, T>) -> T {
    match result { Ok(value) => value, Err(error) => panic!("generated success: {error:?}") }
}

fn child_charge(capacity: usize) -> usize {
    Layout::array::<ContextChild<'_, '_>>(capacity).unwrap().size() + 256
}

fn leave(memory: &Arc<MemoryBudget>, available: usize) -> Arc<MemoryReservation> {
    memory.reserve(memory.limit_bytes() - memory.used_bytes() - available).unwrap()
}

fn denied<T>(result: &PassResult<'_, '_, T>, capacity: usize) {
    let Err(PassError::Runtime(RuntimeError::Prediction(PredictionError::Admission(error)))) = result
        else { panic!("expected native child-vector admission"); };
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    let prefix = format!("Cannot reserve {} bytes", child_charge(capacity));
    assert!(error.message.as_deref().unwrap().starts_with(&prefix));
}

fn begin_date_before_array<'parse, 'sql>(
    session: &mut ParserSession<'parse, 'sql, '_, '_>,
    entry: &TreeDataTypeEntry<'parse>,
) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
    let mut local = entry.0.key;
    session.data_type_date_base(entry, &mut local)?;
    session.set_state(957)?;
    session.sync_required_block()?;
    assert_eq!(session.adaptive_predict(83, PredictionMode::Sll)?.number(), 1);
    Ok(local)
}

fn begin_cardinality<'parse, 'sql>(
    session: &mut ParserSession<'parse, 'sql, '_, '_>,
    entry: &TreeDataTypeEntry<'parse>,
) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
    let mut array = begin_date_before_array(session, entry)?;
    assert!(session.data_type_array_open(entry, &mut array)?);
    session.set_state(951)?;
    let children = session.records[array.index].children.as_ref().unwrap();
    assert_eq!((children.len(), children.capacity()), (2, 2));
    assert!(session.records.len() < session.records.capacity());
    Ok(array)
}

#[test]
fn generated_date_replaces_the_recursive_head_without_an_outer_attachment() {
    fixture("DaTe", |session, _, _| {
        let date = complete(session.data_type_sll());
        assert_eq!(session.records.len(), 2);
        assert_eq!(date, InvocationKey::at(1));
        assert!(matches!(session.records[0].role, InvocationRole::RecursiveEntry { phase: RecursionPhase::Closed, .. }));
        assert!(session.records[0].children.is_none());
        assert!(session.records[0].stop.is_none());
        let view = session.invocation_view().context(date).unwrap();
        assert_eq!(view.class(), Some(ContextClass::DateType));
        assert_eq!(view.parent(), None);
        assert_eq!(view.invoking_state(), ParserState::RootSentinel);
        assert_eq!(view.start().unwrap().index(), 0);
        assert_eq!(view.stop().unwrap().index(), 0);
        let [ContextChild::Terminal(terminal)] = view.children() else { panic!("DATE terminal"); };
        assert_eq!((terminal.index(), terminal.lexeme().raw()), (0, "DaTe"));
        assert_eq!(session.input_index(), Some(1));
        assert!(session.current().is_none());
        assert_eq!(session.state(), ParserState::State(957));
        assert_eq!(session.precedence(), 0);
        Ok(())
    });
}

#[test]
fn recursive_array_children_keep_the_original_saved_links_across_two_replacements() {
    fixture("DATE[2][]", |session, _, _| {
        let outer = complete(session.data_type_sll());
        assert_eq!(session.records.len(), 5);
        assert_eq!(outer, InvocationKey::at(4));
        let views = session.invocation_view();
        let outer_view = views.context(outer).unwrap();
        assert_eq!(outer_view.class(), Some(ContextClass::ArrayType));
        assert_eq!((outer_view.parent(), outer_view.invoking_state()), (None, ParserState::RootSentinel));
        assert_eq!((outer_view.start().unwrap().index(), outer_view.stop().unwrap().index()), (0, 5));
        let [ContextChild::Context(inner), ContextChild::Terminal(open), ContextChild::Terminal(close)] = outer_view.children()
            else { panic!("outer array children"); };
        assert_eq!((open.index(), close.index()), (4, 5));
        let inner_view = views.context(*inner).unwrap();
        assert_eq!(inner_view.class(), Some(ContextClass::ArrayType));
        assert_eq!((inner_view.parent(), inner_view.invoking_state()), (Some(outer), ParserState::State(52)));
        assert_eq!(inner_view.stop().unwrap().index(), 3);
        let [ContextChild::Context(date), ContextChild::Terminal(open), ContextChild::Context(cardinality), ContextChild::Terminal(close)] = inner_view.children()
            else { panic!("inner array children"); };
        assert_eq!((open.index(), close.index()), (1, 3));
        let date_view = views.context(*date).unwrap();
        assert_eq!(date_view.class(), Some(ContextClass::DateType));
        assert_eq!((date_view.parent(), date_view.invoking_state()), (Some(*inner), ParserState::State(52)));
        assert_eq!(date_view.stop().unwrap().index(), 0);
        let card = views.context(*cardinality).unwrap();
        assert_eq!(card.rule(), 31);
        assert_eq!((card.parent(), card.invoking_state()), (Some(*inner), ParserState::State(951)));
        assert_eq!((card.start().unwrap().index(), card.stop().unwrap().index()), (2, 2));
        let [ContextChild::Terminal(number)] = card.children() else { panic!("cardinality terminal"); };
        assert_eq!((number.index(), number.lexeme().raw()), (2, "2"));
        assert!(session.current().is_none());
        assert_eq!(session.state(), ParserState::State(959));
        assert_eq!(session.precedence(), 0);
        Ok(())
    });
}

#[test]
fn field_definition_names_the_actual_label_and_attaches_the_array_on_unroll() {
    fixture("x DATE[2]", |session, _, _| {
        let field = complete(session.field_definition_sll());
        let views = session.invocation_view();
        let view = views.context(field).unwrap();
        assert_eq!(view.rule(), 32);
        assert_eq!((view.start().unwrap().index(), view.stop().unwrap().index()), (0, 4));
        let [ContextChild::Context(name), ContextChild::Context(array)] = view.children() else { panic!("field children"); };
        assert_eq!(view.field_name(), Some(*name));
        let name_view = views.context(*name).unwrap();
        assert_eq!(name_view.class(), Some(ContextClass::RegularIdentifier));
        assert_eq!((name_view.parent(), name_view.invoking_state()), (Some(field), ParserState::State(982)));
        let [ContextChild::Terminal(token)] = name_view.children() else { panic!("identifier terminal"); };
        assert_eq!((token.index(), token.lexeme().raw()), (0, "x"));
        let array_view = views.context(*array).unwrap();
        assert_eq!(array_view.class(), Some(ContextClass::ArrayType));
        assert_eq!((array_view.parent(), array_view.invoking_state()), (Some(field), ParserState::State(983)));
        assert_eq!((array_view.start().unwrap().index(), array_view.stop().unwrap().index()), (1, 4));
        assert!(session.current().is_none());
        assert_eq!(session.state(), ParserState::RootSentinel);
        assert_eq!(session.precedence(), 0);
        Ok(())
    });
}

#[test]
fn recursive_finally_uses_the_child_whose_tree_entry_failed_before_its_try() {
    fixture("DATE[2]", |session, memory, _| {
        let mut entry = session.enter_data_type_tree()?;
        let array = complete(begin_cardinality(session, &entry));
        let index = session.records.len();
        let _blocker = leave(memory, 0);
        let result = session.maximum_cardinality_sll();
        denied(&result, 4);
        let child = InvocationKey::at(index);
        assert_eq!(session.current(), Some(child));
        assert_eq!(session.state(), ParserState::State(62));
        assert_eq!(session.input_index(), Some(2));
        assert!(matches!(session.records[child.index].role, InvocationRole::OrdinaryTreeAttachmentFailed));
        assert!(session.records[child.index].stop.is_none());
        assert!(session.records[child.index].children.is_none());
        assert_eq!(session.records[child.index].parent, Some(array));
        assert_eq!(session.precedence.len(), 2);

        session.unroll_data_type_tree(&mut entry)?;
        assert!(session.current().is_none());
        assert_eq!(session.precedence(), 0);
        assert_eq!(session.state(), ParserState::State(62));
        assert_eq!(session.records[child.index].invoking, ParserState::State(951));
        assert_eq!(session.records[child.index].start.unwrap().index(), 2);
        assert_eq!(session.records[child.index].stop.unwrap().index(), 1);
        assert!(session.records[child.index].parent.is_none());
        assert!(session.records[array.index].stop.is_none());
        assert_eq!(session.records[array.index].children.as_ref().unwrap().len(), 2);
        assert_eq!(session.syntax_errors, 0);
        assert!(matches!(session.control, ParserControl::Failed));
        assert!(matches!(session.unroll_data_type_tree(&mut entry), Err(RuntimeError::Control(RuntimeProblem::Recursion))));
        assert_eq!(session.precedence.len(), 1);
        Ok(())
    });
}

#[test]
fn recursive_finally_can_admit_the_smaller_actual_child_attachment_under_failed_control() {
    for (available, final_capacity, attached) in [(child_charge(2), 4, true), (0, 2, false)] {
        fixture("x DATE[2]", |session, memory, _| {
            let field = session.enter_field_definition_tree()?;
            complete(session.bind_field_name(field));
            let mut entry = session.enter_data_type_tree()?;
            let array = complete(begin_cardinality(session, &entry));
            let cardinality = InvocationKey::at(session.records.len());
            let _blocker = leave(memory, available);
            let body = session.maximum_cardinality_sll();
            denied(&body, 4);

            let finally = session.unroll_data_type_tree(&mut entry);
            assert_eq!(session.current(), Some(field));
            assert_eq!(session.state(), ParserState::State(62));
            assert_eq!(session.records[cardinality.index].parent, Some(field));
            assert_eq!(session.records[cardinality.index].invoking, ParserState::State(951));
            assert_eq!(session.records[cardinality.index].start.unwrap().index(), 3);
            assert_eq!(session.records[cardinality.index].stop.unwrap().index(), 2);
            assert!(session.records[array.index].stop.is_none());
            assert_eq!(session.records[array.index].children.as_ref().unwrap().len(), 2);
            let views = session.invocation_view();
            let view = views.context(field).unwrap();
            let name = view.field_name().unwrap();
            assert!(matches!(view.children().first(), Some(ContextChild::Context(key)) if *key == name));
            if attached {
                assert!(matches!(view.children(), [ContextChild::Context(_), ContextChild::Context(key)] if *key == cardinality));
            } else {
                assert_eq!(view.children().len(), 1);
            }

            let escaping = finally.map_err(PassError::from).and(body);
            session.exit_rule()?;
            denied(&escaping, final_capacity);
            assert_eq!(session.records[field.index].stop.unwrap().index(), 2);
            assert!(session.current().is_none());
            assert_eq!(session.state(), ParserState::RootSentinel);
            assert_eq!(session.precedence(), 0);
            assert_eq!(session.syntax_errors, 0);
            assert!(matches!(session.control, ParserControl::Failed));
            assert!(session.invocation_view().exception(cardinality).is_none());
            assert!(session.invocation_view().exception(field).is_none());
            Ok(())
        });
    }
}

#[test]
fn denied_recursive_push_attachment_keeps_the_new_current_for_finally() {
    fixture("DATE[2]", |session, memory, _| {
        let mut entry = session.enter_data_type_tree()?;
        let date = complete(begin_date_before_array(session, &entry));
        session.set_state(955)?;
        session.sync_required_block()?;
        assert_eq!(session.adaptive_predict(82, PredictionMode::Sll)?.number(), 2);
        let replacement = session.new_data_type_array(&entry)?;
        let array = replacement.key;
        let _blocker = leave(memory, 0);
        let result = session.push_data_type_array(&entry, replacement).map_err(PassError::from);
        denied(&result, 1);
        assert_eq!(session.current(), Some(array));
        assert_eq!(session.records[date.index].parent, Some(array));
        assert_eq!(session.records[date.index].invoking, ParserState::State(52));
        assert_eq!(session.records[date.index].stop.unwrap().index(), 0);
        assert_eq!(session.records[array.index].start.unwrap().index(), 0);
        assert!(session.records[array.index].children.as_ref().unwrap().is_empty());
        session.unroll_data_type_tree(&mut entry)?;
        assert_eq!(session.records[array.index].stop.unwrap().index(), 0);
        assert_eq!(session.state(), ParserState::State(955));
        assert!(session.current().is_none());
        assert_eq!(session.precedence(), 0);
        Ok(())
    });
}

#[test]
fn failed_ordinary_work_after_successful_tree_entry_cannot_skip_its_finally() {
    fixture("DATE[2]", |session, _, _| {
        let mut entry = session.enter_data_type_tree()?;
        let array = complete(begin_cardinality(session, &entry));
        let child = session.new_invocation(31)?;
        session.enter_tree_rule(child)?;
        assert!(session.set_state(u32::MAX).is_err());
        assert!(matches!(session.unroll_data_type_tree(&mut entry), Err(RuntimeError::Control(RuntimeProblem::Recursion))));
        assert_eq!(session.precedence.len(), 2);
        assert_eq!(session.current(), Some(child));
        session.exit_rule()?;
        assert_eq!(session.current(), Some(array));
        session.unroll_data_type_tree(&mut entry)?;
        assert!(session.current().is_none());
        assert_eq!(session.precedence.len(), 1);
        Ok(())
    });
}

#[test]
fn unsupported_base_and_array_spelling_stay_outside_the_private_adopter() {
    for (source, state) in [("INT @", 939), ("DATE ARRAY @", 955)] {
        fixture(source, |session, _, _| {
            let result = session.data_type_sll();
            assert!(matches!(result, Err(PassError::Runtime(RuntimeError::Control(RuntimeProblem::PredictionSite)))));
            assert!(session.current().is_none());
            assert_eq!(session.state(), ParserState::State(state));
            assert_eq!(session.precedence.len(), 1);
            assert_eq!(session.syntax_errors, 0);
            assert!(session.buffered_token(if state == 939 { 1 } else { 2 }).is_none());
            Ok(())
        });
    }
}

#[test]
fn warm_generated_field_propagates_the_real_recursive_finally_outcome() {
    let prefix = 4 * child_charge(1) + child_charge(2);
    for (during_finally, escaping_capacity, attached) in [(child_charge(2), 4, true), (0, 2, false)] {
        fixture("x DATE[2]", |session, memory, retained| {
            complete(session.field_definition_sll());
            session.seek_input(0)?;
            let before = session.records.len();
            assert_eq!(before, 7);
            assert!(before + 7 <= session.records.capacity());
            assert!(session.buffered_token(5).is_some_and(|token| token.lexeme().kind() == crate::lexer::Kind::Eof));
            let _blockers = (leave(memory, prefix + during_finally), leave(retained, 0));
            let result = session.field_definition_sll();
            denied(&result, escaping_capacity);
            assert_eq!(session.records.len(), before + 7);
            assert_eq!(memory.used_bytes(), memory.limit_bytes());

            let field = InvocationKey::at(before);
            let name = InvocationKey::at(before + 2);
            let array = InvocationKey::at(before + 5);
            let cardinality = InvocationKey::at(before + 6);
            let view = session.invocation_view();
            let field_view = view.context(field).unwrap();
            assert_eq!(field_view.field_name(), Some(name));
            assert_eq!(field_view.stop().unwrap().index(), 2);
            assert!(matches!(field_view.children().first(), Some(ContextChild::Context(key)) if *key == name));
            if attached {
                assert!(matches!(field_view.children(), [ContextChild::Context(_), ContextChild::Context(key)] if *key == cardinality));
            } else {
                assert_eq!(field_view.children().len(), 1);
            }
            let array_view = view.context(array).unwrap();
            assert_eq!(array_view.children().len(), 2);
            assert!(array_view.stop().is_none());
            let card = view.context(cardinality).unwrap();
            assert!(matches!(session.records[cardinality.index].role, InvocationRole::OrdinaryTreeAttachmentFailed));
            assert_eq!((card.start().unwrap().index(), card.stop().unwrap().index()), (3, 2));
            assert_eq!((card.parent(), card.invoking_state()), (Some(field), ParserState::State(951)));
            assert!(card.children().is_empty());
            assert!(view.exception(field).is_none());
            assert!(view.exception(cardinality).is_none());

            assert_eq!(session.input_index(), Some(3));
            assert!(session.current().is_none());
            assert_eq!(session.state(), ParserState::RootSentinel);
            assert_eq!(session.precedence.len(), 1);
            assert_eq!(session.syntax_errors, 0);
            assert!(matches!(session.control, ParserControl::Failed));
            Ok(())
        });
    }
}
