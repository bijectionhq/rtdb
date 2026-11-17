use std::sync::Arc;
use rtdb_types::MemoryBudget;
use crate::parser::grammar::OwnedGrammar;
use super::*;
use super::super::{ContextShape, MergeMode, PredictionStorage};
use super::super::runtime::{CapturedClassPolicy, ParserEnvironment, ParserSession, PredictionInput, RecursionEntry, RuntimeError, RuntimeResult, with_parser_session};
use super::super::dfa::{Candidate, DfaTarget};

fn options() -> ClosureOptions { ClosureOptions { pruning: LoopEntryOption::Enabled, assertions: SourceAssertions::Enabled } }
enum Entered<'parse> { Ordinary, Recursive(RecursionEntry<'parse>) }
fn enter<'parse,'sql>(session:&mut ParserSession<'parse,'sql,'_,'_>,rule:u16,recursive:bool,precedence:i32)->RuntimeResult<'sql,Entered<'parse>> {
    if recursive {let pending=session.new_recursion_entry(rule)?;Ok(Entered::Recursive(session.enter_recursion(pending,precedence)?))}else{let key=session.new_invocation(rule)?;session.enter_rule(key)?;Ok(Entered::Ordinary)}
}
fn exit<'parse,'sql>(session:&mut ParserSession<'parse,'sql,'_,'_>,entered:Entered<'parse>)->RuntimeResult<'sql,()> {match entered {Entered::Ordinary=>session.exit_rule(),Entered::Recursive(mut entry)=>session.unroll_recursion(&mut entry).map(|_|())}}
fn fixture<R>(decision:u16,precedence:i32,options:ClosureOptions,run:impl for<'call,'round,'cfg,'id,'access,'input,'origin,'parse,'tokenwork> FnOnce(&mut ClosureRound<'call,'round,'cfg,'id,'access,'access,Retained>,&mut PredictionDfas<'cfg,'access,'access>,&mut PredictionInput<'input,'origin,'round,'parse,'static,'tokenwork>,&Arc<MemoryBudget>,&Arc<MemoryBudget>)->RuntimeResult<'static,R>)->RuntimeResult<'static,R> {
    let gm=MemoryBudget::new(8<<20);let grammar=OwnedGrammar::load(&gm).unwrap();let(rule,recursive)={let view=grammar.view();let rule=view.decision(decision).unwrap().state().rule();(rule.number(),matches!(rule.start().kind(),StateKindView::RuleStart{left_recursive:true,..}))};
    let retained=MemoryBudget::new(128<<20);let memory=MemoryBudget::new(128<<20);let work=AllocationScope::new(&memory).unwrap();let mut env=ParserEnvironment::new(PredictionStorage::new(grammar,&retained),CapturedClassPolicy::from_captured(options),&work).unwrap();
    let result=with_parser_session(&mut env,"1 2",&work,|session| {let entered=enter(session,rule,recursive,precedence)?;let result=session.with_prediction(decision,|round,dfas,input|run(round,dfas,input,&retained,&memory));assert_eq!(session.input_index(),Some(0));let cleanup=exit(session,entered);cleanup?;result});
    drop(env);assert_eq!(retained.used_bytes(),0);assert_eq!(gm.used_bytes(),0);drop(work);assert_eq!(memory.used_bytes(),0);result
}
fn config<'cfg,'id,'g,'scope>(round:&mut ClosureRound<'_,'_,'cfg,'id,'g,'scope,Retained>,state:u32,alt:usize,context:ContextKey<'id>)->Result<ConfigKey<'cfg>> {let alt=round.configs.alternative(round.decision,alt)?;round.configs.create(round.contexts.contexts().grammar().state(state)?,alt,context,Semantic::Unpredicated)}
fn generic_set<'cfg,'id,'g,'scope>(round:&mut ClosureRound<'_,'_,'cfg,'id,'g,'scope,Retained>,keys:&[ConfigKey<'cfg>])->Result<ConfigSetKey<'cfg>> {let set=round.configs.new_set(SetMode::Full)?;for key in keys {round.add(set,*key)?;}Ok(set)}
fn predicate<'g,'scope>(grammar:&'g crate::parser::grammar::Grammar<'scope>)->(State<'g,'scope>,usize,TransitionView<'g,'scope>){for number in 0..3017{let state=grammar.state(number).unwrap();for(index,transition)in state.transitions().enumerate(){if matches!(transition,TransitionView::Precedence{..}){return(state,index,transition);}}}panic!("pinned precedence")}

#[test]
fn all_372_full_starts_use_real_parentless_rule_control_for_both_pruning_profiles() {
    for pruning in [LoopEntryOption::Enabled,LoopEntryOption::Disabled] {for decision in 0..372 {fixture(decision,0,ClosureOptions{pruning,..options()},|round,_,input,_,_|{
        assert!(matches!(round.memo,MergePresence::Absent));let mut full=round.enter_full(input)?;let start=full.full_start()?;let view=full.round.configs.view_set(start.key());assert_eq!(view.mode(),SetMode::Full);assert!(!view.readonly());assert!(view.unique().is_none());assert!(view.conflicting().is_none());assert!(!view.has_semantic());assert!(!view.dips_outer());assert!(view.len()>0);
        for index in 0..view.len(){let config=full.round.configs.view(view.entry(index).unwrap());assert_eq!(config.semantic(),Semantic::Unpredicated);assert_eq!(config.packed_outer(),0);}
        assert!(matches!(full.round.memo,MergePresence::Absent));Ok(())
    }).unwrap();}}
}
#[test]
fn every_called_rule_full_start_uses_an_actual_incoming_call_and_parent_identity() {
    let gm=MemoryBudget::new(8<<20);let grammar=OwnedGrammar::load(&gm).unwrap();let view=grammar.view();let mut routes=Vec::new();
    for number in 0..372{let rule=view.decision(number).unwrap().state().rule();let incoming=(0..3017).find_map(|n|{let state=view.state(n).unwrap();match state.transitions().next(){Some(TransitionView::Rule{rule:called,follow,..})if called.number()==rule.number()=>Some((n,state.rule().number(),matches!(state.rule().start().kind(),StateKindView::RuleStart{left_recursive:true,..}),follow.number())),_=>None}});if let Some((call,parent,parent_recursive,follow))=incoming{routes.push((number,rule.number(),matches!(rule.start().kind(),StateKindView::RuleStart{left_recursive:true,..}),call,parent,parent_recursive,follow));}else{assert!([0,1].contains(&rule.number()));}}
    drop(view);drop(grammar);
    for(decision,rule,recursive,call,parent,parent_recursive,follow)in routes{let gm=MemoryBudget::new(8<<20);let grammar=OwnedGrammar::load(&gm).unwrap();let retained=MemoryBudget::new(128<<20);let memory=MemoryBudget::new(128<<20);let work=AllocationScope::new(&memory).unwrap();let mut env=ParserEnvironment::new(PredictionStorage::new(grammar,&retained),CapturedClassPolicy::from_captured(options()),&work).unwrap();with_parser_session(&mut env,"1",&work,|session|{let outer=enter(session,parent,parent_recursive,0)?;session.set_state(call)?;let inner=enter(session,rule,recursive,0)?;session.with_prediction(decision,|round,_,input|{let grammar=round.contexts.contexts().grammar();let mut links=input.invocations(grammar);let state=links.next().unwrap();assert_eq!(state.number(),call);assert!(matches!(state.transitions().next(),Some(TransitionView::Rule{follow:target,..})if target.number()==follow));assert!(links.next().is_none());drop(links);let mut full=round.enter_full(input)?;let start=full.full_start()?;assert!(!full.round.configs.view_set(start.key()).dips_outer());Ok(())})?;exit(session,inner)?;exit(session,outer)?;Ok(())}).unwrap();drop(env);assert_eq!(retained.used_bytes(),0);assert_eq!(gm.used_bytes(),0);}
}
#[test]
fn empty_rule_stop_adds_same_full_config_without_global_falloff_or_outer_mutation() {
    fixture(65,0,options(),|round,_,input,_,_|{let empty=round.contexts.contexts().empty();let key=config(round,1,1,empty)?;let output=generic_set(round,&[])?;let mut full=round.enter_full(input)?;full.round.closure_using(key,output,true,0,false,&mut FullPolicy{input:full.input})?;assert_eq!(full.round.configs.view_set(output).entry(0),Some(key));assert_eq!(full.round.configs.view_set(output).len(),1);assert_eq!(full.round.configs.view(key).packed_outer(),0);assert!(!full.round.configs.view_set(output).dips_outer());assert_eq!(full.round.work.busy.len,0);Ok(())}).unwrap();
}
#[test]
fn full_stop_pops_normal_live_parent_with_packed_bits_preserved() {
    fixture(65,0,options(),|round,_,input,_,_|{let contexts=round.contexts.contexts();let parent=contexts.empty();let context=contexts.singleton(parent,contexts.grammar().state(356)?)?;let key=config(round,1,1,context)?;round.configs.set_suppressed(key,true)?;let packed=round.configs.view(key).packed_outer();let output=generic_set(round,&[])?;let mut full=round.enter_full(input)?;full.round.closure_using(key,output,false,0,false,&mut FullPolicy{input:full.input})?;let actual=full.round.configs.view_set(output).entry(0).unwrap();assert_ne!(actual,key);let view=full.round.configs.view(actual);assert_eq!(view.state()?.number(),356);assert_eq!(view.context(),parent);assert_eq!(view.packed_outer(),packed);assert_eq!(full.round.work.busy.len,0);Ok(())}).unwrap();
}
#[test]
fn full_array_empty_return_is_copied_to_canonical_empty_in_edge_order() {
    fixture(65,0,options(),|round,_,input,_,_|{let contexts=round.contexts.contexts();let empty=contexts.empty();let branch=contexts.singleton(empty,contexts.grammar().state(356)?)?;let context=contexts.merge_uncached(branch,empty,MergeMode::FullExact)?;assert_eq!(contexts.view(context).shape(),ContextShape::Array);let key=config(round,1,1,context)?;let output=generic_set(round,&[])?;let mut full=round.enter_full(input)?;full.round.closure_using(key,output,false,0,false,&mut FullPolicy{input:full.input})?;let set=full.round.configs.view_set(output);assert_eq!(set.len(),2);let first=full.round.configs.view(set.entry(0).unwrap());let last=full.round.configs.view(set.entry(1).unwrap());assert_eq!(first.state()?.number(),356);assert_eq!(last.state()?.number(),1);assert_eq!(last.context(),empty);assert_ne!(set.entry(1),Some(key));assert!(!set.dips_outer());Ok(())}).unwrap();
}
#[test]
fn full_precedence_evaluates_frozen_stack_at_origin_restores_current_and_keeps_unpredicated_copy() {
    for precedence in [0,100] {fixture(83,precedence,options(),|round,_,input,_,_|{input.advance()?;assert_eq!(input.index(),Some(1));let(state,index,transition)=predicate(round.contexts.contexts().grammar());let empty=round.contexts.contexts().empty();let key=config(round,state.number(),1,empty)?;let mut full=round.enter_full(input)?;let result=full.round.epsilon_target_using(key,state,index,transition,true,true,false,&mut FullPolicy{input:full.input})?;assert_eq!(full.input.index(),Some(1));if precedence==0{let result=result.unwrap();assert_ne!(result,key);assert_eq!(full.round.configs.view(result).semantic(),Semantic::Unpredicated);}else{assert!(result.is_none());}Ok(())}).unwrap();}
}
#[test]
fn full_precedence_noncollecting_or_out_of_context_copies_without_seek_or_accumulation() {
    for(collect,in_context)in[(false,true),(true,false),(false,false)]{fixture(83,100,options(),|round,_,input,_,_|{input.advance()?;let(state,index,transition)=predicate(round.contexts.contexts().grammar());let empty=round.contexts.contexts().empty();let key=config(round,state.number(),1,empty)?;let mut full=round.enter_full(input)?;let child=full.round.epsilon_target_using(key,state,index,transition,collect,in_context,false,&mut FullPolicy{input:full.input})?.unwrap();assert_eq!(full.round.configs.view(child).semantic(),Semantic::Unpredicated);assert_eq!(full.input.index(),Some(1));Ok(())}).unwrap();}
}
#[test]
fn full_reach_withholds_stop_for_every_token_and_keeps_it_when_no_longer_stop_arrives() {
    for eof in [false,true] {fixture(65,0,options(),|round,_,input,_,_|{let token=if eof{TokenType::EOF}else{round.contexts.contexts().grammar().token_type(crate::lexer::Kind::Integer)?};let empty=round.contexts.contexts().empty();let stop=config(round,1,1,empty)?;let set=generic_set(round,&[stop])?;let mut full=round.enter_full(input)?;full.phase=FullPhase::Reaching;let reached=full.full_reach(FullSet::new(set),token)?.unwrap();let set=full.round.configs.view_set(reached.key());assert_eq!(set.len(),1);assert_eq!(set.entry(0),Some(stop));assert!(matches!(full.round.memo,MergePresence::Present));Ok(())}).unwrap();}
}
#[test]
fn full_eof_new_stop_suppresses_entire_previous_stop_list_for_longest_match() {
    fixture(65,0,options(),|round,_,input,_,_|{let empty=round.contexts.contexts().empty();let old=config(round,3,1,empty)?;let pending=config(round,356,2,empty)?;let set=generic_set(round,&[old,pending])?;let mut full=round.enter_full(input)?;full.phase=FullPhase::Reaching;let reached=full.full_reach(FullSet::new(set),TokenType::EOF)?.unwrap();let view=full.round.configs.view_set(reached.key());assert!(view.len()>0);for index in 0..view.len(){let key=view.entry(index).unwrap();assert_ne!(key,old);assert_eq!(full.round.configs.view(key).state()?.number(),1);}Ok(())}).unwrap();
}
#[test]
fn full_eof_uses_busy_only_for_real_eof_successors_and_keeps_inserted_keys_immutable() {
    fixture(65,0,options(),|round,_,input,_,_|{let empty=round.contexts.contexts().empty();let a=config(round,356,1,empty)?;let b=config(round,369,2,empty)?;let output=generic_set(round,&[])?;let mut full=round.enter_full(input)?;for key in[a,b]{full.round.closure_using(key,output,false,0,true,&mut FullPolicy{input:full.input})?;}assert_eq!(full.round.work.busy.len,2);for entry in full.round.work.busy.slots.iter().flatten(){let view=full.round.configs.view(entry.config);assert!([357,370].contains(&view.state()?.number()));assert_eq!(view.context(),empty);assert_eq!(view.packed_outer(),0);assert_eq!(entry.hash,spread(full.round.configs.config_hash(entry.config,full.round.contexts.contexts())?));for i in 0..full.round.configs.view_set(output).len(){assert_ne!(Some(entry.config),full.round.configs.view_set(output).entry(i));}}Ok(())}).unwrap();
}
#[test]
fn full_busy_duplicate_comparison_at_full_budgets_needs_no_new_admission() {
    fixture(65,0,options(),|round,_,input,retained,memory|{let contexts=round.contexts.contexts();let mut a=contexts.empty();let mut b=contexts.empty();for _ in 0..128{let x=contexts.singleton(a,contexts.grammar().state(356)?)?;let y=contexts.singleton(contexts.empty(),contexts.grammar().state(369)?)?;a=contexts.merge_uncached(x,y,MergeMode::FullExact)?;let x=contexts.singleton(b,contexts.grammar().state(356)?)?;let y=contexts.singleton(contexts.empty(),contexts.grammar().state(369)?)?;b=contexts.merge_uncached(x,y,MergeMode::FullExact)?;}let left=config(round,357,1,a)?;let right=config(round,357,1,b)?;let full=round.enter_full(input)?;assert!(full.round.work.busy.add(left,full.round.configs,full.round.contexts.contexts(),full.round.work.allocation)?);let r=retained.reserve(retained.limit_bytes()-retained.used_bytes()).unwrap();let w=memory.reserve(memory.limit_bytes()-memory.used_bytes()).unwrap();let before=(retained.used_bytes(),memory.used_bytes());assert!(!full.round.work.busy.add(right,full.round.configs,full.round.contexts.contexts(),full.round.work.allocation)?);assert_eq!((retained.used_bytes(),memory.used_bytes()),before);drop((r,w));assert!(full.round.contexts.contexts().equivalent(a,b)?);Ok(())}).unwrap();
}
#[test]
fn full_handoff_preserves_absent_memo_until_first_reach() {
    fixture(83,0,options(),|round,_,input,_,_|{assert!(matches!(round.memo,MergePresence::Absent));let mut full=round.enter_full(input)?;assert!(matches!(full.round.memo,MergePresence::Absent));let start=full.full_start()?;assert!(matches!(full.round.memo,MergePresence::Absent));let _=full.full_reach(start,TokenType::EOF)?;assert!(matches!(full.round.memo,MergePresence::Present));Ok(())}).unwrap();
}
#[test]
fn full_handoff_keeps_the_present_merge_round_and_its_existing_representative() {
    fixture(65,0,options(),|round,_,input,_,_|{let start=match round.local_start()?{StartSet::Ordinary(set)=>set,StartSet::Precedence(_)=>panic!("ordinary")};let _=round.local_reach(start,TokenType::EOF)?;assert!(matches!(round.memo,MergePresence::Present));let contexts=round.contexts.contexts();let empty=contexts.empty();let a=contexts.singleton(empty,contexts.grammar().state(356)?)?;let b=contexts.singleton(empty,contexts.grammar().state(369)?)?;let before=round.contexts.merge(a,b,MergeMode::LocalWildcard)?;let mut full=round.enter_full(input)?;assert!(matches!(full.round.memo,MergePresence::Present));assert_eq!(full.round.contexts.merge(a,b,MergeMode::FullExact)?,before);let _=full.full_start()?;assert_eq!(full.round.contexts.merge(a,b,MergeMode::FullExact)?,before);Ok(())}).unwrap();
}
#[test]
fn terminal_full_round_rejects_old_local_reach_before_memo_or_admission() {
    let result=fixture(65,0,options(),|round,_,input,retained,memory|{let local=match round.local_start()?{StartSet::Ordinary(set)=>set,_=>panic!("ordinary")};drop(round.enter_full(input)?);let before=(retained.used_bytes(),memory.used_bytes());let result=round.local_reach(local,TokenType::EOF);assert!(matches!(result,Err(PredictionError::Artifact(Problem::ClosurePhase))));assert_eq!((retained.used_bytes(),memory.used_bytes()),before);assert!(matches!(round.memo,MergePresence::Absent));result.map(|_|()).map_err(RuntimeError::Prediction)});assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::Artifact(Problem::ClosurePhase)))));
}
#[test]
fn terminal_full_rejects_local_start_and_old_precedence_prefilter_in_separate_flows() {
    for filter in [false,true]{let result=fixture(83,0,options(),|round,_,input,_,_|{let start=match round.local_start()?{StartSet::Precedence(set)=>set,_=>panic!("precedence")};drop(round.enter_full(input)?);let result=if filter{round.precedence_filter(start,0).map(|_|())}else{round.local_start().map(|_|())};assert!(matches!(result,Err(PredictionError::Artifact(Problem::ClosurePhase))));result.map_err(RuntimeError::Prediction)});assert!(result.is_err());}
}
#[test]
fn terminal_full_rejects_cached_local_binding_before_dfa_key_lookup() {
    let result=fixture(65,0,options(),|round,dfas,input,_,_|{let local=match round.local_start()?{StartSet::Ordinary(set)=>set,_=>panic!("ordinary")};let(configs,contexts)=round.parts();let draft=dfas.new_draft(65,local.key(),configs)?;let target=dfas.add_state(65,Candidate::Draft(draft),configs,contexts)?;let DfaTarget::Interned(key)=target else{panic!("ordinary")};drop(round.enter_full(input)?);let result=round.from_cached(dfas,key);assert!(matches!(result,Err(PredictionError::Artifact(Problem::ClosurePhase))));result.map(|_|()).map_err(RuntimeError::Prediction)});assert!(result.is_err());
}
#[test]
fn full_start_can_run_only_once_and_full_reach_cannot_precede_it() {
    for before in [false,true]{let result=fixture(65,0,options(),|round,_,input,_,_|{let mut full=round.enter_full(input)?;let result=if before{let empty=full.round.contexts.contexts().empty();let config=config(full.round,1,1,empty)?;let set=generic_set(full.round,&[config])?;full.full_reach(FullSet::new(set),TokenType::EOF).map(|_|())}else{let _=full.full_start()?;full.full_start().map(|_|())};assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::Artifact(Problem::ClosurePhase)))));result});assert!(result.is_err());}
}
#[test]
fn second_full_handoff_is_rejected_without_returning_to_local() {
    let result=fixture(65,0,options(),|round,_,input,_,_|{drop(round.enter_full(input)?);let result=round.enter_full(input).map(|_|());assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::Artifact(Problem::ClosurePhase)))));result});assert!(result.is_err());
}
#[test]
fn full_reach_no_matching_transition_returns_none_with_default_facts() {
    fixture(65,0,options(),|round,_,input,_,_|{let empty=round.contexts.contexts().empty();let key=config(round,356,1,empty)?;let set=generic_set(round,&[key])?;let mut full=round.enter_full(input)?;full.phase=FullPhase::Reaching;let token=full.round.contexts.contexts().grammar().token_type(crate::lexer::Kind::Integer)?;assert!(full.full_reach(FullSet::new(set),token)?.is_none());Ok(())}).unwrap();
}
#[test]
fn full_reach_stop_context_assertion_precedes_skipped_append() {
    let result=fixture(65,0,options(),|round,_,input,_,_|{let contexts=round.contexts.contexts();let context=contexts.singleton(contexts.empty(),contexts.grammar().state(356)?)?;let key=config(round,1,1,context)?;let set=generic_set(round,&[key])?;let mut full=round.enter_full(input)?;full.phase=FullPhase::Reaching;let result=full.full_reach(FullSet::new(set),TokenType::EOF);assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::SourceAssertion(SourceAssertionSite::ReachStopEmpty)))));assert!(full.round.work.skipped.is_empty());assert!(full.round.aborted);assert!(full.input.ready().is_err());result.map(|_|())});assert!(result.is_err());
}
#[test]
fn full_no_outer_assertion_runs_at_completed_closure_end_under_explicit_profile() {
    for assertions in [SourceAssertions::Enabled,SourceAssertions::Disabled]{let result=fixture(65,0,ClosureOptions{assertions,..options()},|round,_,input,_,_|{let empty=round.contexts.contexts().empty();let key=config(round,1,1,empty)?;let output=generic_set(round,&[])?;round.configs.mark_outer_context(output)?;let mut full=round.enter_full(input)?;let result=full.round.closure_using(key,output,false,0,false,&mut FullPolicy{input:full.input});assert_eq!(full.round.configs.view_set(output).entry(0),Some(key));if assertions==SourceAssertions::Enabled{assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::SourceAssertion(SourceAssertionSite::FullNoOuter)))));}else{assert!(result.is_ok());}full.finish(result)});assert_eq!(result.is_err(),assertions==SourceAssertions::Enabled);}
}
#[test]
fn full_native_admission_error_preserves_phase_and_original_error_without_normal_resumption() {
    let result=fixture(65,0,options(),|round,_,input,retained,_|{let mut full=round.enter_full(input)?;let _lease=retained.reserve(retained.limit_bytes()-retained.used_bytes()).unwrap();let result=full.full_start();assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::Admission(_)))));assert!(full.round.aborted);assert_eq!(full.round.phase,ClosurePhase::Full);assert!(full.input.ready().is_err());result.map(|_|())});assert!(matches!(result,Err(RuntimeError::Prediction(PredictionError::Admission(_)))));
}

#[test]
fn full_epsilon_targets_cover_every_pinned_transition_kind_and_eof_gate() {
    fixture(65,0,options(),|round,_,input,_,_|{let mut full=round.enter_full(input)?;let grammar=full.round.contexts.contexts().grammar();let empty=full.round.contexts.contexts().empty();let mut kinds=[0usize;6];for number in 0..3017{let state=grammar.state(number)?;for(index,transition)in state.transitions().enumerate(){let key=config(full.round,number,1,empty)?;for eof in[false,true]{let result=full.round.epsilon_target_using(key,state,index,transition,false,false,eof,&mut FullPolicy{input:full.input})?;let expected=match transition{TransitionView::Atom{token,..}=>eof&&token==TokenType::EOF,TransitionView::Set{tokens,..}=>eof&&tokens.contains(ExpectedSymbol::Token(TokenType::EOF)),_=>true};assert_eq!(result.is_some(),expected);if let Some(child)=result{assert_ne!(child,key);assert_eq!(full.round.configs.view(child).state()?.number(),target(transition).number());assert_eq!(full.round.configs.view(child).semantic(),Semantic::Unpredicated);if let TransitionView::Rule{follow,..}=transition{let context=full.round.configs.view(child).context();let view=full.round.contexts.contexts().view(context);assert_eq!(view.follow(0)?.unwrap().number(),follow.number());assert_eq!(view.parent(0).flatten(),Some(empty));}}}let kind=match transition{TransitionView::Epsilon{..}=>0,TransitionView::Rule{..}=>1,TransitionView::Atom{..}=>2,TransitionView::Action{..}=>3,TransitionView::Set{..}=>4,TransitionView::Precedence{..}=>5};kinds[kind]+=1;}}assert!(kinds.iter().all(|n|*n>0));Ok(())}).unwrap();
}
#[test]
fn full_non_eof_unique_intermediate_shortcut_keeps_source_target_without_closure() {
    fixture(65,0,options(),|round,_,input,_,_|{let grammar=round.contexts.contexts().grammar();let(state,transition)=(0..3017).find_map(|n|{let state=grammar.state(n).unwrap();state.transitions().find(|t|matches!(t,TransitionView::Atom{token,target}if *token!=TokenType::EOF&&target.epsilon_only())).map(|t|(state,t))}).unwrap();let TransitionView::Atom{token,target}=transition else{panic!("atom")};let empty=round.contexts.contexts().empty();let key=config(round,state.number(),1,empty)?;let set=generic_set(round,&[key])?;let mut full=round.enter_full(input)?;full.phase=FullPhase::Reaching;let reached=full.full_reach(FullSet::new(set),token)?.unwrap();let output=full.round.configs.view_set(reached.key());assert_eq!(output.len(),1);assert_eq!(full.round.configs.view(output.entry(0).unwrap()).state()?.number(),target.number());assert_eq!(full.round.work.busy.len,0);Ok(())}).unwrap();
}
