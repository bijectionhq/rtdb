use std::sync::Arc;
use rtdb_types::{AllocationScope, MemoryBudget};
use super::{Grammar, GrammarError, OwnedGrammar, State, TokenType, TransitionView};

#[test]
fn owner_views_share_graph_identity_without_allocating_or_accepting_foreign_data() {
    let memory = MemoryBudget::new(4 << 20);
    let owner = OwnedGrammar::load(&memory).unwrap();
    let other_memory = MemoryBudget::new(4 << 20);
    let other = OwnedGrammar::load(&other_memory).unwrap();
    let retained = memory.used_bytes();
    let peak = memory.peak_bytes();
    {
        let first = owner.view();
        let second = owner.view();
        let foreign = other.view();
        for number in 0..3_017 {
            let state = first.state(number).unwrap();
            let same_owner = |linked: State<'_, '_>| {
                assert!(linked.belongs_to(&first));
                assert!(linked.belongs_to(&second));
                assert!(!linked.belongs_to(&foreign));
            };
            same_owner(state);
            same_owner(state.rule().start());
            same_owner(state.rule().stop());
            for transition in state.transitions() {
                match transition {
                    TransitionView::Rule { target, follow, rule, .. } => {
                        same_owner(target);
                        same_owner(follow);
                        same_owner(rule.start());
                    }
                    TransitionView::Epsilon { target, .. } | TransitionView::Atom { target, .. }
                        | TransitionView::Action { target, .. } | TransitionView::Set { target, .. }
                        | TransitionView::Precedence { target, .. } => same_owner(target),
                }
            }
        }
        for number in 0..372 { assert!(first.decision(number).unwrap().state().belongs_to(&second)); }
        assert_eq!(first.display_name(TokenType::EOF), "EOF");
        assert_eq!(second.rule(0).unwrap().name(), "directSqlStatement");
        assert_eq!((memory.used_bytes(), memory.peak_bytes()), (retained, peak));
    }
    assert_eq!(memory.used_bytes(), retained);
    drop(owner);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn owned_grammar_keeps_its_finite_budget_after_the_constructor_borrow_ends() {
    let weak;
    let owner = {
        let memory = MemoryBudget::new(4 << 20);
        weak = Arc::downgrade(&memory);
        OwnedGrammar::load(&memory).unwrap()
    };
    {
        let view = owner.view();
        assert_eq!(view.decision(371).unwrap().state().number(), 3_014);
        assert!(weak.upgrade().unwrap().used_bytes() > 4_096);
    }
    drop(owner);
    assert!(weak.upgrade().is_none());
}

#[test]
fn owned_and_scoped_loaders_share_the_same_admission_schedule_and_graph() {
    let scoped_memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&scoped_memory).unwrap();
    let scoped = Grammar::load(&allocation).unwrap();
    let owned_memory = MemoryBudget::new(4 << 20);
    let owner = OwnedGrammar::load(&owned_memory).unwrap();
    let owned = owner.view();
    assert_eq!(owned_memory.used_bytes(), scoped_memory.used_bytes());
    assert_eq!(owned_memory.peak_bytes(), scoped_memory.peak_bytes());
    for number in 0..3_017 {
        let left = scoped.state(number).unwrap();
        let right = owned.state(number).unwrap();
        assert_eq!((left.rule().number(), left.epsilon_only(), left.transitions().len()), (right.rule().number(), right.epsilon_only(), right.transitions().len()));
        assert!(!left.belongs_to(&owned));
        assert!(!right.belongs_to(&scoped));
    }
    drop(owned);
    drop(owner);
    assert_eq!(owned_memory.used_bytes(), 0);
    drop(scoped);
    assert!(scoped_memory.used_bytes() > 0);
    drop(allocation);
    assert_eq!(scoped_memory.used_bytes(), 0);
}

#[test]
fn owned_grammar_admission_failure_releases_its_scope_and_exact_limit_succeeds() {
    let sizing_memory = MemoryBudget::new(4 << 20);
    let owner = OwnedGrammar::load(&sizing_memory).unwrap();
    let required = sizing_memory.used_bytes();
    drop(owner);
    assert_eq!(sizing_memory.used_bytes(), 0);
    for limit in [0, 4_095, 4_096, required - 1, required] {
        let memory = MemoryBudget::new(limit);
        match OwnedGrammar::load(&memory) {
            Ok(owner) => {
                assert_eq!(limit, required);
                assert_eq!(memory.used_bytes(), required);
                drop(owner);
            }
            Err(GrammarError::Admission(error)) => {
                assert!(limit < required);
                assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            }
            Err(error) => panic!("unexpected graph failure: {error:?}"),
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn look_accepts_invocations_from_another_view_of_the_same_owned_graph() {
    let memory = MemoryBudget::new(4 << 20);
    let owner = OwnedGrammar::load(&memory).unwrap();
    let first = owner.view();
    let second = owner.view();
    let work_memory = MemoryBudget::new(4 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let stop = first.rule(2).unwrap().stop();
    let same = stop.next_tokens_with_context(std::iter::once(first.state(352).unwrap()), &work).unwrap();
    let shared = stop.next_tokens_with_context(std::iter::once(second.state(352).unwrap()), &work).unwrap();
    assert_eq!(same, shared);
    let same = stop.expected_tokens(std::iter::once(first.state(352).unwrap()), &work).unwrap();
    let shared = stop.expected_tokens(std::iter::once(second.state(352).unwrap()), &work).unwrap();
    assert_eq!(same, shared);
    let other_memory = MemoryBudget::new(4 << 20);
    let other = OwnedGrammar::load(&other_memory).unwrap();
    let foreign = other.view();
    assert!(matches!(stop.next_tokens_with_context(std::iter::once(foreign.state(352).unwrap()), &work), Err(GrammarError::ForeignOwner)));
    assert!(matches!(stop.expected_tokens(std::iter::once(foreign.state(352).unwrap()), &work), Err(GrammarError::ForeignOwner)));
}

#[test]
fn owned_graph_views_retain_the_owner_borrow_through_destruction() {
    fn view<'owner>(owner: &'owner OwnedGrammar) -> Grammar<'owner> { owner.view() }
    fn state<'view, 'owner>(view: &'view Grammar<'owner>) -> State<'view, 'owner> { view.state(0).unwrap() }
    let memory = MemoryBudget::new(4 << 20);
    let owner = OwnedGrammar::load(&memory).unwrap();
    let retained = memory.used_bytes();
    {
        let borrowed = view(&owner);
        assert_eq!(state(&borrowed).number(), 0);
        assert_eq!(memory.used_bytes(), retained);
    }
    assert_eq!(memory.used_bytes(), retained);
    assert!(std::mem::needs_drop::<OwnedGrammar>());
    assert!(std::mem::needs_drop::<Grammar<'_>>());
    drop(owner);
    assert_eq!(memory.used_bytes(), 0);
}
