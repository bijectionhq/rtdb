use std::{mem::size_of, panic::{AssertUnwindSafe, catch_unwind}, sync::Arc};
use rtdb_types::MemoryBudget;
use crate::lexer::ScanError;
use crate::parser::grammar::OwnedGrammar;
use super::*;
use super::super::{Problem, SourceAssertionSite};
use super::super::closure::{LoopEntryOption, SourceAssertions};

fn policy() -> CapturedClassPolicy { CapturedClassPolicy::from_captured(ClosureOptions { pruning: LoopEntryOption::Enabled, assertions: SourceAssertions::Enabled }) }
fn environment(memory: &Arc<MemoryBudget>, work: &AllocationScope) -> (ParserEnvironment, Arc<MemoryBudget>) {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    (ParserEnvironment::new(PredictionStorage::new(grammar, memory), policy(), work).unwrap(), grammar_memory)
}
fn fixture<'sql, R>(sql: &'sql str, run: impl for<'parse, 'env, 'work> FnOnce(&mut ParserSession<'parse, 'sql, 'env, 'work>, &Arc<MemoryBudget>) -> RuntimeResult<'sql, R>) -> RuntimeResult<'sql, R> {
    let memory = MemoryBudget::new(64 << 20); let scope = AllocationScope::new(&memory).unwrap();
    let retained = MemoryBudget::new(64 << 20); let (mut env, grammar_memory) = environment(&retained, &scope);
    let result = with_parser_session(&mut env, sql, &scope, |session| run(session, &memory));
    drop(env); assert_eq!(retained.used_bytes(), 0); assert_eq!(grammar_memory.used_bytes(), 0);
    drop(scope); assert_eq!(memory.used_bytes(), 0); result
}
fn ordinary<'parse, 'sql>(session: &mut ParserSession<'parse, 'sql, '_, '_>, rule: u16) -> RuntimeResult<'sql, InvocationKey<'parse>> { let frame = session.new_invocation(rule)?; session.enter_rule(frame)?; Ok(frame) }
fn recursive<'parse, 'sql>(session: &mut ParserSession<'parse, 'sql, '_, '_>, rule: u16, precedence: i32) -> RuntimeResult<'sql, RecursionEntry<'parse>> { let pending = session.new_recursion_entry(rule)?; session.enter_recursion(pending, precedence) }
fn exhaust(memory: &Arc<MemoryBudget>) -> Arc<rtdb_types::MemoryReservation> { memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap() }
fn control<T>(result: &RuntimeResult<'_, T>) { assert!(matches!(result, Err(RuntimeError::Control(_)))); }

#[test]
fn captured_policy_is_explicit_and_immutable_across_sessions() {
    for pruning in [LoopEntryOption::Enabled, LoopEntryOption::Disabled] { for assertions in [SourceAssertions::Enabled, SourceAssertions::Disabled] {
        let selected = CapturedClassPolicy::from_captured(ClosureOptions { pruning, assertions });
        assert_eq!(selected.options, ClosureOptions { pruning, assertions });
    } }
    fixture("1", |session, _| { assert_eq!(session.environment.policy, policy()); ordinary(session, 32)?; session.exit_rule()?; session.clear_rule_control()?; assert_eq!(session.environment.policy, policy()); Ok(()) }).unwrap();
}
#[test]
fn environment_initializes_372_decisions_once_and_retains_history() {
    let memory=MemoryBudget::new(64<<20);let scope=AllocationScope::new(&memory).unwrap();let retained=MemoryBudget::new(64<<20);let(mut env,gm)=environment(&retained,&scope);
    let initialized=retained.used_bytes();assert!(initialized>0);
    for _ in 0..2 { with_parser_session(&mut env,"1",&scope,|s| { ordinary(s,32)?; s.set_state(983)?; let mut entry=recursive(s,26,0)?; s.with_prediction(83,|round,_,input| { let mut full=round.enter_full(input)?; let start=full.full_start()?; assert!(full.parts().0.view_set(start.key()).len()>0); Ok(()) })?;s.unroll_recursion(&mut entry)?;Ok(()) }).unwrap(); }
    assert!(retained.used_bytes()>=initialized);drop(env);assert_eq!(retained.used_bytes(),0);assert_eq!(gm.used_bytes(),0);
}
#[test]
fn environment_initialization_failure_publishes_no_owner_and_releases_store() {
    let gm=MemoryBudget::new(8<<20);let grammar=OwnedGrammar::load(&gm).unwrap();let memory=MemoryBudget::new(0);let workmem=MemoryBudget::new(8192);let work=AllocationScope::new(&workmem).unwrap();
    assert!(matches!(ParserEnvironment::new(PredictionStorage::new(grammar,&memory),policy(),&work),Err(PredictionError::Admission(_))));assert_eq!(memory.used_bytes(),0);assert_eq!(gm.used_bytes(),0);
}
#[test]
fn uninitialized_session_keeps_none_index_and_base_zero() {
    fixture("@",|s,_| {assert_eq!(s.input_index(),None);assert!(s.current().is_none());assert_eq!(s.state(),ParserState::RootSentinel);assert_eq!(s.precedence(),0);assert!(s.records.is_empty());Ok(())}).unwrap();
}
#[test]
fn ordinary_entry_and_exit_preserve_actual_start_stop_and_invoking_state() {
    fixture("1 2",|s,_| {s.set_state(955)?;let frame=ordinary(s,32)?;assert_eq!(s.state(),ParserState::State(64));assert_eq!(s.records[frame.index].invoking,ParserState::State(955));assert_eq!(s.records[frame.index].start.unwrap().index(),0);s.advance_input()?;s.exit_rule()?;assert_eq!(s.records[frame.index].stop.unwrap().index(),0);assert_eq!(s.state(),ParserState::State(955));assert!(s.current.is_none());Ok(())}).unwrap();
}
#[test]
fn ordinary_eof_stop_reads_current_eof_instead_of_previous() {
    fixture("1",|s,_| {let frame=ordinary(s,32)?;s.advance_input()?;s.note_eof_match()?;s.exit_rule()?;assert_eq!(s.records[frame.index].stop.unwrap().index(),1);Ok(())}).unwrap();
}
#[test]
fn ordinary_constructor_rejects_all_five_recursive_rules_before_append() {
    for rule in [26,33,34,76,126] {let result=fixture("1",|s,_| {let result=s.new_invocation(rule);control(&result);assert!(s.records.is_empty());result.map(|_|())});control(&result);}
}
#[test]
fn ordinary_stale_captured_state_rejects_before_source_entry() {
    let result=fixture("1",|s,_| {let frame=s.new_invocation(32)?;s.set_state(955)?;let result=s.enter_rule(frame);control(&result);assert_eq!(s.input_index(),None);assert!(s.current.is_none());assert!(s.records[frame.index].start.is_none());assert!(!s.control.is_ready());result});control(&result);
}
#[test]
fn ordinary_reentry_of_entered_or_exited_record_rejects() {
    for exited in [false,true] {let result=fixture("1",|s,_| {let frame=ordinary(s,32)?;if exited{s.exit_rule()?;}let before=s.current;let result=s.enter_rule(frame);control(&result);assert_eq!(s.current,before);result});control(&result);}
}
#[test]
fn initial_recursion_admission_failure_precedes_pending_and_all_control_changes() {
    let result=fixture("1",|s,memory| {let _lease=exhaust(memory);let result=s.new_recursion_entry(26);assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::Admission(_)))));assert!(s.records.is_empty());assert!(s.current.is_none());assert_eq!(s.precedence.len(),1);assert_eq!(s.state,ParserState::RootSentinel);result.map(|_|())});assert!(result.is_err());
}
#[test]
fn root_entry_lt1_error_keeps_pushed_current_entering_prefix_without_active_capability() {
    let result=fixture("@",|s,_| {let pending=s.new_recursion_entry(26)?;let key=pending.key;let result=s.enter_recursion(pending,7);assert!(matches!(result,Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_))))));assert_eq!(s.state,ParserState::State(52));assert_eq!(s.current,Some(key));assert_eq!(s.precedence(),7);assert!(s.records[key.index].start.is_none());assert!(matches!(s.records[key.index].role,InvocationRole::RecursiveEntry{phase:RecursionPhase::Entering,..}));result.map(|_|())});assert!(matches!(result,Err(RuntimeError::Input(_))));
}
#[test]
fn recursive_entry_grants_active_authority_after_real_start_and_keeps_signed_precedence() {
    fixture("1",|s,_| {let mut entry=recursive(s,26,-7)?;assert_eq!(s.precedence(),-7);assert!(s.records[entry.key.index].start.is_some());assert!(matches!(s.records[entry.key.index].role,InvocationRole::RecursiveEntry{phase:RecursionPhase::Active{head},..}if head==entry.key));s.unroll_recursion(&mut entry)?;assert_eq!(s.precedence(),0);Ok(())}).unwrap();
}
#[test]
fn replacement_constructor_uses_saved_pair_before_push_not_loop_call_or_live_initial() {
    fixture("1",|s,_| {let outer=ordinary(s,32)?;s.set_state(983)?;let mut entry=recursive(s,26,0)?;s.set_state(955)?;let first=s.new_recursion_replacement(&entry)?;let h1=first.key;assert_eq!(s.records[h1.index].parent,Some(outer));assert_eq!(s.records[h1.index].invoking,ParserState::State(983));s.push_recursion(&entry,first)?;assert_eq!(s.records[entry.key.index].parent,Some(h1));assert_eq!(s.records[entry.key.index].invoking,ParserState::State(52));let next=s.new_recursion_replacement(&entry)?;assert_eq!(s.records[next.key.index].parent,Some(outer));assert_eq!(s.records[next.key.index].invoking,ParserState::State(983));s.push_recursion(&entry,next)?;s.unroll_recursion(&mut entry)?;Ok(())}).unwrap();
}
#[test]
fn two_replacements_and_ordinary_child_feed_actual_current_path_to_full_start() {
    fixture("1",|s,_| {let outer=ordinary(s,32)?;s.set_state(983)?;let mut entry=recursive(s,26,0)?;let h0=entry.key;s.set_state(955)?;let first=s.new_recursion_replacement(&entry)?;let h1=first.key;s.push_recursion(&entry,first)?;s.set_state(951)?;let child=ordinary(s,31)?;assert_eq!(s.current,Some(child));s.exit_rule()?;assert_eq!(s.current,Some(h1));assert_eq!(s.state,ParserState::State(951));let next=s.new_recursion_replacement(&entry)?;let h2=next.key;s.push_recursion(&entry,next)?;assert_eq!(s.records[h0.index].parent,Some(h1));assert_eq!(s.records[h1.index].parent,Some(h2));assert_eq!(s.records[h2.index].parent,Some(outer));assert_eq!(s.records[h2.index].invoking,ParserState::State(983));s.with_prediction(83,|round,_,input| {let(_,contexts)=round.parts();let grammar=contexts.grammar();let mut path=input.invocations(grammar);assert_eq!(path.next().unwrap().number(),983);assert!(path.next().is_none());drop(path);let mut full=round.enter_full(input)?;let start=full.full_start()?;assert!(full.parts().0.view_set(start.key()).len()>0);Ok(())})?;s.unroll_recursion(&mut entry)?;Ok(())}).unwrap();
}
#[test]
fn unfinished_ordinary_child_rejects_outer_replacement_without_later_normal_resumption() {
    let result=fixture("1",|s,_| {ordinary(s,32)?;s.set_state(983)?;let mut entry=recursive(s,26,0)?;s.set_state(951)?;let child=ordinary(s,31)?;let len=s.records.len();let result=s.new_recursion_replacement(&entry);control(&result);assert_eq!(s.records.len(),len);assert_eq!(s.current,Some(child));s.exit_rule()?;s.unroll_recursion(&mut entry)?;result.map(|_|())});control(&result);
}
#[test]
fn nested_same_rule_entries_keep_distinct_witnesses_and_unroll_actual_top() {
    fixture("1",|s,_| {let mut outer=recursive(s,33,0)?;s.set_state(986)?;let mut middle=recursive(s,34,3)?;s.set_state(1137)?;let mut inner=recursive(s,33,9)?;assert_ne!(outer.key,inner.key);assert_eq!(s.precedence(),9);s.set_state(955)?;s.unroll_recursion(&mut inner)?;assert_eq!(s.current,Some(middle.key));assert_eq!(s.precedence(),3);assert_eq!(s.state,ParserState::State(955));s.unroll_recursion(&mut middle)?;assert_eq!(s.current,Some(outer.key));s.unroll_recursion(&mut outer)?;assert_eq!(s.precedence(),0);Ok(())}).unwrap();
}
#[test]
fn cross_entry_outer_unroll_rejects_without_popping_inner_and_allows_only_cleanup() {
    let result=fixture("1",|s,_| {let mut outer=recursive(s,33,0)?;s.set_state(986)?;let mut inner=recursive(s,34,3)?;let count=s.precedence.len();let result=s.unroll_recursion(&mut outer);control(&result);assert_eq!(s.precedence.len(),count);s.unroll_recursion(&mut inner)?;s.unroll_recursion(&mut outer)?;result.map(|_|())});control(&result);
}
#[test]
fn stale_fresh_replacement_rejects_after_another_push_before_link_mutation() {
    let result=fixture("1",|s,_| {let mut entry=recursive(s,26,0)?;let old=s.new_recursion_replacement(&entry)?;let new=s.new_recursion_replacement(&entry)?;s.push_recursion(&entry,new)?;let head=s.current;let result=s.push_recursion(&entry,old);control(&result);assert_eq!(s.current,head);s.unroll_recursion(&mut entry)?;result});control(&result);
}
#[test]
fn push_inherits_start_and_unroll_never_restores_recognizer_state_or_double_pops() {
    let result=fixture("1 2",|s,_| {let mut entry=recursive(s,26,2)?;s.advance_input()?;s.set_state(955)?;let next=s.new_recursion_replacement(&entry)?;let key=next.key;s.push_recursion(&entry,next)?;assert_eq!(s.records[entry.key.index].stop.unwrap().index(),0);assert_eq!(s.records[key.index].start.unwrap().index(),0);let ret=s.unroll_recursion(&mut entry)?;assert_eq!(ret,key);assert_eq!(s.state,ParserState::State(955));let count=s.precedence.len();let result=s.unroll_recursion(&mut entry);control(&result);assert_eq!(s.precedence.len(),count);result.map(|_|())});control(&result);
}
#[test]
fn parentless_root_and_nonminusone_invoking_state_remain_distinct() {
    fixture("1",|s,_| {s.set_state(955)?;let mut entry=recursive(s,26,0)?;let next=s.new_recursion_replacement(&entry)?;let key=next.key;s.push_recursion(&entry,next)?;assert!(s.records[key.index].parent.is_none());assert_eq!(s.records[key.index].invoking,ParserState::State(955));s.unroll_recursion(&mut entry)?;assert_eq!(s.state,ParserState::State(52));Ok(())}).unwrap();
}
#[test]
fn ordinary_entry_cannot_consume_inspected_recursive_initial_or_replacement_keys() {
    for replacement in [false,true] {let result=fixture("1",|s,_| {let pending=s.new_recursion_entry(26)?;let key=if replacement{let entry=s.enter_recursion(pending,0)?;s.new_recursion_replacement(&entry)?.key}else{pending.key};let state=s.state;let current=s.current;let count=s.precedence.len();let result=s.enter_rule(key);control(&result);assert_eq!(s.state,state);assert_eq!(s.current,current);assert_eq!(s.precedence.len(),count);result});control(&result);}
}
#[test]
fn ordinary_exit_cannot_bypass_recursion_unroll() {
    let result=fixture("1",|s,_| {let mut entry=recursive(s,26,0)?;let count=s.precedence.len();let result=s.exit_rule();control(&result);assert_eq!(s.precedence.len(),count);assert_eq!(s.current,Some(entry.key));s.unroll_recursion(&mut entry)?;result});control(&result);
}
#[test]
fn cached_nested_lt_reads_and_finally_succeed_after_real_tail_recognition_error() {
    let result=fixture("1 @",|s,_| {let mut entry=recursive(s,33,0)?;let original=s.lookahead(2);assert!(matches!(original,Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_))))));assert!(s.input.lt(1)?.is_some());assert!(s.input.lt(-1)?.is_none());s.unroll_recursion(&mut entry)?;assert_eq!(s.precedence(),0);original.map(|_|())});assert!(matches!(result,Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_))))));
}
#[test]
fn prediction_origin_restores_real_input_after_original_borrowed_recognition_error() {
    let result=fixture("1 @",|s,_| {let mut entry=recursive(s,26,0)?;let result=s.with_prediction(83,|_,_,input| {input.lookahead(2)?;Ok(())});assert!(matches!(result,Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_))))));assert_eq!(s.input_index(),Some(0));s.unroll_recursion(&mut entry)?;result});assert!(matches!(result,Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_))))));
}
#[test]
fn prediction_error_moves_through_all_factories_and_restores_before_rule_finally() {
    let result=fixture("1 2",|s,_| {let mut entry=recursive(s,26,0)?;let result=s.with_prediction(83,|_,_,input| {input.advance()?;Err::<(),_>(PredictionError::SourceAssertion(SourceAssertionSite::FullNoOuter).into())});assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::SourceAssertion(SourceAssertionSite::FullNoOuter)))));assert_eq!(s.input_index(),Some(0));s.unroll_recursion(&mut entry)?;assert_eq!(s.precedence(),0);result});assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::SourceAssertion(_)))));
}
#[test]
fn prediction_unwind_drops_inner_work_before_real_buffered_origin_restore() {
    fixture("1 2",|s,_| {let mut entry=recursive(s,26,0)?;let panic=catch_unwind(AssertUnwindSafe(|| {let _:RuntimeResult<'_,()>=s.with_prediction(83,|round,_,input| {let _=round.local_start()?;input.advance()?;panic!("prediction unwind")});}));assert!(panic.is_err());assert_eq!(s.input_index(),Some(0));assert!(!s.control.is_ready());s.unroll_recursion(&mut entry)?;Ok(())}).unwrap();
}
#[test]
fn real_origin_seek_disarms_and_returns_original_prediction_error() {
    fixture("1 2",|s,_| {ordinary(s,32)?;let origin=s.input.index().unwrap();let mut control=ParserControl::Ready;let mut guard=PredictionRestoration::new(&mut s.input,&mut control,origin);guard.input.consume()?;let result=guard.finish::<()>(Err(PredictionError::Artifact(Problem::LocalClosure).into()));assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::Artifact(Problem::LocalClosure)))));assert!(matches!(guard.state,RestorationState::Finished));assert_eq!(guard.input.index(),Some(origin));Ok(())}).unwrap();
}
#[test]
fn source_seek_error_overrides_original_and_guard_never_retries() {
    fixture("1 @",|s,_| {ordinary(s,32)?;let mut control=ParserControl::Ready;let mut guard=PredictionRestoration::new(&mut s.input,&mut control,100);let result=guard.finish::<()>(Err(PredictionError::Aborted.into()));assert!(matches!(result,Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_))))));assert!(matches!(guard.state,RestorationState::Finished));drop(guard);assert_eq!(s.input_index(),Some(0));Ok(())}).unwrap();
}
#[test]
fn failed_session_blocks_normal_resume_but_retains_real_cleanup_and_records() {
    let result=fixture("1 @",|s,_| {let frame=ordinary(s,32)?;let error=match s.lookahead(2){Err(error)=>error,Ok(_)=>panic!("recognition error")};assert!(matches!(&error,RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_)))));control(&s.advance_input());s.exit_rule()?;s.rewind_input()?;s.clear_rule_control()?;assert!(!s.control.is_ready());assert_eq!(s.records.len(),1);assert!(s.records[frame.index].start.is_some());control(&s.new_invocation(32));Err::<(),_>(error)});assert!(matches!(result,Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_))))));
}
#[test]
fn enlarged_primary_and_precedence_elements_are_admitted_under_actual_sizes() {
    assert!(size_of::<PrecedenceFrame<'_>>()>=size_of::<i32>()+size_of::<usize>());assert!(size_of::<InvocationRecord<'_,'_>>()>=size_of::<SavedOuterLink<'_>>());
    let result=fixture("1",|s,memory| {
        let mut entries=vec![recursive(s,33,0)?];
        while s.precedence.len()<s.precedence.capacity(){let rule=s.records[s.current.unwrap().index].rule;let(call,callee)=if rule==33{(986,34)}else{(1137,33)};s.set_state(call)?;entries.push(recursive(s,callee,3)?);}
        let current=s.current;let count=s.precedence.len();let rule=s.records[current.unwrap().index].rule;let(call,callee)=if rule==33{(986,34)}else{(1137,33)};s.set_state(call)?;let pending=s.new_recursion_entry(callee)?;
        let _lease=exhaust(memory);let result=s.enter_recursion(pending,9);assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::Admission(_)))));assert_eq!(s.current,current);assert_eq!(s.precedence.len(),count);let expected=if callee==33{66}else{68};assert_eq!(s.state,ParserState::State(expected));
        while let Some(mut entry)=entries.pop(){s.unroll_recursion(&mut entry)?;}assert_eq!(s.precedence(),0);assert_eq!(s.state,ParserState::State(expected));result.map(|_|())
    });assert!(result.is_err());
}

#[test]
fn replacement_admission_failure_retains_completed_head_for_actual_finally() {
    let result=fixture("1",|s,memory| {let mut entry=recursive(s,26,0)?;while s.records.len()<s.records.capacity(){let _=s.new_recursion_replacement(&entry)?;}let current=s.current;let count=s.records.len();let _lease=exhaust(memory);let result=s.new_recursion_replacement(&entry);assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::Admission(_)))));assert_eq!(s.current,current);assert_eq!(s.records.len(),count);assert!(s.records[entry.key.index].parent.is_none());s.unroll_recursion(&mut entry)?;result.map(|_|())});assert!(result.is_err());
}
#[test]
fn nested_entry_lt1_requires_no_fresh_admission_at_full_budget() {
    fixture("1",|s,memory| {let mut outer=recursive(s,33,0)?;s.set_state(986)?;let pending=s.new_recursion_entry(34)?;let _lease=exhaust(memory);let mut child=s.enter_recursion(pending,3)?;assert_eq!(s.input_index(),Some(0));s.unroll_recursion(&mut child)?;s.unroll_recursion(&mut outer)?;Ok(())}).unwrap();
}
#[test]
fn prediction_never_normalizes_uninitialized_index_or_invokes_callback() {
    let result=fixture("1",|s,_| {let mut called=false;let result=s.with_prediction(83,|_,_,_| {called=true;Ok(())});assert!(!called);assert!(matches!(result,Err(RuntimeError::Control(RuntimeProblem::UninitializedOrigin))));assert_eq!(s.input_index(),None);result});assert!(matches!(result,Err(RuntimeError::Control(RuntimeProblem::UninitializedOrigin))));
}

#[test]
fn every_recursive_rule_constructs_two_replacements_from_its_original_saved_root() {
    for(rule,start)in[(26,52),(33,66),(34,68),(76,152),(126,252)]{fixture("1",|s,_|{s.set_state(955)?;let mut entry=recursive(s,rule,0)?;assert_eq!(s.state(),ParserState::State(start));for _ in 0..2{let replacement=s.new_recursion_replacement(&entry)?;assert!(s.records[replacement.key.index].parent.is_none());assert_eq!(s.records[replacement.key.index].invoking,ParserState::State(955));s.push_recursion(&entry,replacement)?;}s.unroll_recursion(&mut entry)?;Ok(())}).unwrap();}
}
#[test]
fn ordinary_root_initialization_failure_keeps_source_current_and_no_invented_exit() {
    let result=fixture("@",|s,_|{let key=s.new_invocation(32)?;let result=s.enter_rule(key);assert!(matches!(result,Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_))))));assert_eq!(s.current,Some(key));assert_eq!(s.state(),ParserState::State(64));assert!(s.records[key.index].start.is_none());assert!(!s.control.is_ready());result});assert!(matches!(result,Err(RuntimeError::Input(_))));
}
#[test]
fn environment_cache_start_number_and_hit_survive_failure_and_new_sessions() {
    use super::super::closure::StartSet;
    use super::super::dfa::{Candidate,DfaTarget};
    let memory=MemoryBudget::new(64<<20);let work=AllocationScope::new(&memory).unwrap();let retained=MemoryBudget::new(64<<20);let(mut env,gm)=environment(&retained,&work);
    for pass in 0..3{let result=with_parser_session(&mut env,"1",&work,|s|{let mut entry=recursive(s,26,0)?;let result=s.with_prediction(83,|round,dfas,input|{
        if pass==1{return Err(PredictionError::SourceAssertion(SourceAssertionSite::FullNoOuter).into());}
        if let Some(key)=dfas.start(83,input.precedence())?{assert_eq!(pass,2);assert_eq!(dfas.view(83,key)?.number(),0);assert_eq!(dfas.state_count(83)?,1);let cached=round.from_cached(dfas,key)?;assert!(round.parts().0.view_set(cached.key()).len()>0);return Ok(());}
        assert_eq!(pass,0);let StartSet::Precedence(start)=round.local_start()?else{panic!("precedence")};{let(configs,_)=round.parts();dfas.replace_synthetic_prefilter(83,start.prefilter(),configs)?;}let local=round.precedence_filter(start,input.precedence())?;let(configs,contexts)=round.parts();let draft=dfas.new_draft(83,local.key(),configs)?;let DfaTarget::Interned(key)=dfas.add_state(83,Candidate::Draft(draft),configs,contexts)?else{panic!("ordinary")};dfas.set_start(83,input.precedence(),key)?;assert_eq!(dfas.view(83,key)?.number(),0);assert_eq!(dfas.state_count(83)?,1);Ok(())
    });s.unroll_recursion(&mut entry)?;result});assert_eq!(result.is_err(),pass==1);}
    drop(env);assert_eq!(retained.used_bytes(),0);assert_eq!(gm.used_bytes(),0);drop(work);assert_eq!(memory.used_bytes(),0);
}

#[test]
fn unrooted_full_work_recycles_across_sessions_without_retained_capacity_growth() {
    let memory=MemoryBudget::new(128<<20);let work=AllocationScope::new(&memory).unwrap();let retained=MemoryBudget::new(128<<20);let(mut env,gm)=environment(&retained,&work);let mut steady=None;
    for _ in 0..8{with_parser_session(&mut env,"1",&work,|s|{let mut entry=recursive(s,26,0)?;s.with_prediction(83,|round,_,input|{let mut full=round.enter_full(input)?;let start=full.full_start()?;let _=full.full_reach(start,crate::parser::grammar::TokenType::EOF)?;Ok(())})?;s.unroll_recursion(&mut entry)?;Ok(())}).unwrap();match steady{None=>steady=Some(retained.used_bytes()),Some(bytes)=>assert_eq!(retained.used_bytes(),bytes)}}
    drop(env);assert_eq!(retained.used_bytes(),0);assert_eq!(gm.used_bytes(),0);drop(work);assert_eq!(memory.used_bytes(),0);
}
#[test]
fn actual_invocation_conversion_admission_error_crosses_generic_factories_before_restoration() {
    let result=fixture("1",|s,memory|{ordinary(s,32)?;s.set_state(983)?;let mut entry=recursive(s,26,0)?;let _lease=exhaust(memory);let result=s.with_prediction(83,|round,_,input|{let mut full=round.enter_full(input)?;full.full_start().map(|_|())});assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::Admission(_)))));assert_eq!(s.input_index(),Some(0));assert!(!s.control.is_ready());s.unroll_recursion(&mut entry)?;result});assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::Admission(_)))));
}
#[test]
fn owner_local_committed_closing_prefix_cannot_pop_again() {
    let result=fixture("1",|s,_|{let mut entry=recursive(s,26,0)?;let(saved,head,_)=s.active_entry(&entry)?;s.precedence.pop();s.records[entry.key.index].role=InvocationRole::RecursiveEntry{saved,phase:RecursionPhase::Closing{head}};let count=s.precedence.len();let result=s.unroll_recursion(&mut entry);control(&result);assert_eq!(s.precedence.len(),count);assert_eq!(s.current,Some(head));result.map(|_|())});control(&result);
}

#[test]
fn borrowed_recognition_fact_outlives_session_and_renders_from_original_sql() {
    let sql=String::from("1 @");let result=fixture(&sql,|s,_|{let mut entry=recursive(s,26,0)?;let result=s.with_prediction(83,|_,_,input|{input.lookahead(2)?;Ok(())});s.unroll_recursion(&mut entry)?;result});
    let Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(error))))=result else{panic!("original SQL-borrowed recognition fact")};assert_eq!(error.span().start.byte,2);assert_eq!(error.span().end.byte,3);let memory=MemoryBudget::new(8192);let work=AllocationScope::new(&memory).unwrap();let rendered=error.render(&work).unwrap();assert!(rendered.message.as_deref().unwrap().contains("'@'"));drop(rendered);drop(work);assert_eq!(memory.used_bytes(),0);
}

#[test]
fn class_initialization_accepts_all_sixteen_exact_java_true_spellings() {
    for assertions in [SourceAssertions::Enabled, SourceAssertions::Disabled] {
        for mask in 0u8..16 {
            let mut units = [b't', b'r', b'u', b'e'].map(u16::from);
            for (index, unit) in units.iter_mut().enumerate() {
                if mask & (1 << index) != 0 { *unit -= 32; }
            }
            let mut calls = 0;
            let selected = CapturedClassPolicy::capture_parser_atn(assertions, |name| {
                calls += 1;
                assert_eq!(name, "TURN_OFF_LR_LOOP_ENTRY_BRANCH_OPT");
                Ok::<_, std::convert::Infallible>(Some(JavaText::from_utf16(&units)))
            }).unwrap();
            assert_eq!(calls, 1);
            assert_eq!(selected.options, ClosureOptions { pruning: LoopEntryOption::Disabled, assertions });
        }
    }
}

#[test]
fn missing_and_nontrue_class_inputs_keep_pruning_enabled_without_trim_or_lossy_projection() {
    let long = [u16::from(b't'); 128];
    let units: &[&[u16]] = &[
        &[0xd800], &[0xdc00], &[0xd800, 0xdc00],
        &[0x74, 0x72, 0xd800, 0x65], &[0x74, 0x72, 0xdc00, 0x65],
        &[0x74, 0x72, 0xfc, 0x65], &[0xff54, 0x72, 0x75, 0x65],
        &[0x74, 0x72, 0x75, 0x65, 0], &long,
    ];
    let values = [None].into_iter()
        .chain(["", "false", "yes", " true", "true ", "true\n", "tr ue", "TRUE\0", "truefalse"]
            .into_iter().map(|value| Some(JavaText::from_utf8(value))))
        .chain(units.iter().map(|value| Some(JavaText::from_utf16(value))));
    for value in values {
        for assertions in [SourceAssertions::Enabled, SourceAssertions::Disabled] {
            let mut calls = 0;
            let selected = CapturedClassPolicy::capture_parser_atn(assertions, |name| {
                calls += 1;
                assert_eq!(name, "TURN_OFF_LR_LOOP_ENTRY_BRANCH_OPT");
                Ok::<_, std::convert::Infallible>(value)
            }).unwrap();
            assert_eq!(calls, 1);
            assert_eq!(selected.options, ClosureOptions { pruning: LoopEntryOption::Enabled, assertions });
        }
    }
}

#[test]
fn a_class_input_provider_error_keeps_its_identity_instead_of_selecting_a_policy() {
    let failure = (17u32, 23u32);
    let mut calls = 0;
    let result = CapturedClassPolicy::capture_parser_atn(SourceAssertions::Enabled, |name| {
        calls += 1;
        assert_eq!(name, "TURN_OFF_LR_LOOP_ENTRY_BRANCH_OPT");
        Err::<Option<JavaText<'_>>, _>(&failure)
    });
    let Err(error) = result else { panic!("provider failure must survive capture"); };
    assert!(std::ptr::eq(error, &failure));
    assert_eq!(calls, 1);
}

#[test]
fn captured_class_inputs_outlive_the_provider_and_text() {
    use std::cell::Cell;
    use rtdb_types::java_text::JavaString;

    struct Provider<'a>(&'a Cell<usize>);
    impl Drop for Provider<'_> { fn drop(&mut self) { self.0.set(self.0.get() + 1); } }

    for (spelling, pruning) in [("TrUe", LoopEntryOption::Disabled), ("false", LoopEntryOption::Enabled)] {
        for assertions in [SourceAssertions::Enabled, SourceAssertions::Disabled] {
            let text_memory = MemoryBudget::new(8192);
            let released = Cell::new(0);
            let calls = Cell::new(0);
            let selected = {
                let source = JavaString::copy_utf8(spelling, &text_memory).unwrap();
                let text = source.view();
                let provider = Provider(&released);
                let calls = &calls;
                CapturedClassPolicy::capture_parser_atn(assertions, move |name| {
                    let _ = &provider;
                    calls.set(calls.get() + 1);
                    assert_eq!(name, "TURN_OFF_LR_LOOP_ENTRY_BRANCH_OPT");
                    Ok::<_, std::convert::Infallible>(Some(text))
                }).unwrap()
            };
            assert_eq!(released.get(), 1);
            assert_eq!(calls.get(), 1);
            assert_eq!(text_memory.used_bytes(), 0);
            let grammar_memory = MemoryBudget::new(8 << 20);
            let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
            let memory = MemoryBudget::new(64 << 20);
            let work = AllocationScope::new(&memory).unwrap();
            let retained = MemoryBudget::new(64 << 20);
            let mut environment = ParserEnvironment::new(PredictionStorage::new(grammar, &retained), selected, &work).unwrap();
            let initialized = retained.used_bytes();
            for _ in 0..2 {
                with_bail_fallback(&mut environment, "DATE[][]", &work, |session, mode| {
                    assert_eq!(mode, PredictionMode::Sll);
                    assert_eq!(session.environment.policy.options, ClosureOptions { pruning, assertions });
                    Ok(())
                }).unwrap();
                assert_eq!(calls.get(), 1);
                assert_eq!(environment.policy, selected);
                assert!(retained.used_bytes() >= initialized);
            }
            drop(environment);
            assert_eq!(retained.used_bytes(), 0);
            assert_eq!(grammar_memory.used_bytes(), 0);
            drop(work);
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}

#[test]
fn captured_class_assertion_status_controls_the_reached_prediction_assertion() {
    use super::super::SetMode;
    use super::super::closure::StartSet;

    for assertions in [SourceAssertions::Enabled, SourceAssertions::Disabled] {
        let selected = CapturedClassPolicy::capture_parser_atn(assertions, |name| {
            assert_eq!(name, "TURN_OFF_LR_LOOP_ENTRY_BRANCH_OPT");
            Ok::<_, std::convert::Infallible>(None)
        }).unwrap();
        let grammar_memory = MemoryBudget::new(8 << 20);
        let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
        let memory = MemoryBudget::new(64 << 20);
        let work = AllocationScope::new(&memory).unwrap();
        let retained = MemoryBudget::new(64 << 20);
        let mut environment = ParserEnvironment::new(PredictionStorage::new(grammar, &retained), selected, &work).unwrap();
        let result = with_parser_session(&mut environment, "1", &work, |session| {
            let mut entry = recursive(session, 26, 0)?;
            let result = session.with_prediction(83, |round, _, input| {
                let StartSet::Precedence(start) = round.local_start()? else { panic!("dataType precedence start"); };
                let local = round.precedence_filter(start, input.precedence())?;
                let (key, parent) = {
                    let (configs, contexts) = round.parts();
                    let grammar = contexts.grammar();
                    let parent = contexts.singleton(contexts.empty(), grammar.state(353)?)?;
                    let alternative = configs.alternative(grammar.decision(83)?, 1)?;
                    let key = configs.create(grammar.state(1)?, alternative, parent, Semantic::Unpredicated)?;
                    configs.clear(local.key())?;
                    assert_eq!(configs.view_set(local.key()).mode(), SetMode::Local);
                    configs.add_uncached(local.key(), key, contexts)?;
                    (key, parent)
                };
                let reached = round.local_reach(local, TokenType::EOF);
                if assertions == SourceAssertions::Enabled {
                    assert!(matches!(reached, Err(PredictionError::SourceAssertion(SourceAssertionSite::ReachStopEmpty))));
                } else {
                    let output = reached.as_ref().unwrap().unwrap();
                    let (configs, _) = round.parts();
                    assert_eq!(configs.view_set(output.key()).entry(0), Some(key));
                    assert_eq!(configs.view(key).context(), parent);
                }
                reached.map(|_| ()).map_err(RuntimeError::from)
            });
            assert_eq!(session.input_index(), Some(0));
            session.unroll_recursion(&mut entry)?;
            assert_eq!(session.precedence(), 0);
            result
        });
        assert_eq!(result.is_err(), assertions == SourceAssertions::Enabled);
        drop(environment);
        assert_eq!(retained.used_bytes(), 0);
        assert_eq!(grammar_memory.used_bytes(), 0);
        drop(work);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn completed_prediction_releases_its_work_while_the_parser_owns_its_context() {
    fixture("1", |session, memory| {
        let mut entry = recursive(session, 26, 0)?;
        let parser_bytes = memory.used_bytes();
        for _ in 0..2 {
            session.with_prediction(83, |round, _, input| {
                let mut full = round.enter_full(input)?;
                let start = full.full_start()?;
                assert!(full.parts().0.view_set(start.key()).len() > 0);
                assert!(memory.used_bytes() > parser_bytes);
                Ok(())
            })?;
            assert_eq!(memory.used_bytes(), parser_bytes);
        }
        assert_eq!(session.current(), Some(entry.key));
        assert_eq!(session.input_index(), Some(0));
        session.unroll_recursion(&mut entry)?;
        Ok(())
    }).unwrap();
}
