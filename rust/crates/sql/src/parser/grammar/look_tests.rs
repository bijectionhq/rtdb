use std::cell::Cell;
use std::mem::size_of;
use rtdb_types::{AllocationScope, MemoryBudget};
use super::*;
use super::super::{deserialize, source};

fn fixture<'scope>(words: &[i32], allocation: &'scope AllocationScope) -> Grammar<'scope> {
    let layout = deserialize::Layout::read(words).unwrap();
    let source = source::Source::read(source::PINNED).unwrap();
    let mut vocabulary = source.vocabulary(allocation, 176).unwrap();
    vocabulary.rules.truncate(layout.rule_count());
    deserialize::load(words, layout, vocabulary, allocation).unwrap()
}

fn numbers(set: TokenSet) -> Vec<i32> {
    set.iter().map(|symbol| match symbol { ExpectedSymbol::Epsilon => -2, ExpectedSymbol::Token(token) => token.number() }).collect()
}

fn admission<T>(result: Result<T>) {
    match result {
        Err(GrammarError::Admission(error)) => assert_eq!(error.code, "rtdb/memory-budget-exceeded"),
        _ => panic!("admission failure"),
    }
}

fn foreign<T>(result: Result<T>) { assert!(matches!(result, Err(GrammarError::ForeignOwner))); }

const NULLABLE: &[i32] = &[
    4, 1, 368, 6,
    2, 0, 7, 0, 2, 1, 7, 1, 1, 0, 1, 0,
    0, 0, 2, 0, 2, 0, 0,
    4,
    0, 4, 1, 0, 0, 0,
    2, 3, 1, 0, 0, 0,
    4, 5, 3, 2, 1, 0,
    5, 1, 5, 12, 0, 0,
    0,
];

const RETURN_CHAIN: &[i32] = &[
    4, 1, 368, 4,
    2, 0, 7, 0, 2, 1, 7, 1,
    0, 0, 2, 0, 2, 0, 0,
    2,
    0, 1, 3, 2, 1, 0,
    2, 3, 1, 0, 0, 0,
    0,
];

#[test]
fn pinned_direct_statement_sets_distinguish_local_and_full_rule_ends() {
    let memory = MemoryBudget::new(8 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = Grammar::load(&allocation).unwrap();
    assert_eq!(numbers(grammar.state(354).unwrap().next_tokens(&allocation).unwrap()), [-1, 12]);
    assert_eq!(numbers(grammar.state(356).unwrap().next_tokens(&allocation).unwrap()), [-1]);
    let stop = grammar.rule(0).unwrap().stop();
    assert_eq!(numbers(stop.next_tokens(&allocation).unwrap()), [-2]);
    assert_eq!(numbers(stop.next_tokens_with_context(std::iter::empty(), &allocation).unwrap()), [-1]);
    assert_eq!(numbers(stop.expected_tokens(std::iter::empty(), &allocation).unwrap()), [-1]);
    let callee_stop = grammar.rule(2).unwrap().stop();
    let invocations = [grammar.state(352).unwrap()];
    assert_eq!(numbers(callee_stop.next_tokens_with_context(invocations.into_iter(), &allocation).unwrap()), [-1, 12]);
    assert_eq!(numbers(callee_stop.expected_tokens(invocations.into_iter(), &allocation).unwrap()), [-1, 12]);
}

#[test]
fn expected_tokens_demands_parent_invocations_only_while_epsilon_survives() {
    let memory = MemoryBudget::new(8 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = fixture(NULLABLE, &allocation);
    let foreign_grammar = Grammar::load(&allocation).unwrap();
    let foreign_state = foreign_grammar.state(3000).unwrap();
    let demanded = Cell::new(0);
    let unused = [foreign_state].into_iter().inspect(|_| demanded.set(demanded.get() + 1));
    assert_eq!(numbers(grammar.state(5).unwrap().expected_tokens(unused, &allocation).unwrap()), [12]);
    assert_eq!(demanded.get(), 0);
    let path = [grammar.state(4).unwrap(), foreign_state].into_iter().inspect(|_| demanded.set(demanded.get() + 1));
    assert_eq!(numbers(grammar.rule(1).unwrap().stop().expected_tokens(path, &allocation).unwrap()), [12]);
    assert_eq!(demanded.get(), 1);
    foreign(grammar.rule(1).unwrap().stop().expected_tokens([foreign_state].into_iter(), &allocation));
}

#[test]
fn contextful_look_converts_all_parents_outermost_first_before_traversal() {
    let memory = MemoryBudget::new(8 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = fixture(NULLABLE, &allocation);
    let foreign_grammar = Grammar::load(&allocation).unwrap();
    let foreign_state = foreign_grammar.state(3000).unwrap();
    foreign(grammar.state(5).unwrap().next_tokens_with_context([foreign_state].into_iter(), &allocation));
    let demanded = Cell::new(0);
    let path = [foreign_state, grammar.state(5).unwrap()].into_iter().inspect(|_| demanded.set(demanded.get() + 1));
    match grammar.state(5).unwrap().next_tokens_with_context(path, &allocation) {
        Err(GrammarError::Artifact(error)) => assert_eq!((error.kind, error.offset), (IssueKind::Transition, 5)),
        _ => panic!("outer invocation failure"),
    }
    assert_eq!(demanded.get(), 2);
    assert_eq!(numbers(grammar.rule(1).unwrap().stop().next_tokens(&allocation).unwrap()), [-2]);
    assert_eq!(numbers(grammar.rule(1).unwrap().stop().next_tokens_with_context([grammar.state(4).unwrap()].into_iter(), &allocation).unwrap()), [12]);
}

#[test]
fn expected_tokens_continues_through_nullable_follow_states_to_the_outer_end() {
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = fixture(RETURN_CHAIN, &allocation);
    let demanded = Cell::new(0);
    let path = std::iter::repeat_n(grammar.state(0).unwrap(), 3).inspect(|_| demanded.set(demanded.get() + 1));
    assert_eq!(numbers(grammar.rule(1).unwrap().stop().expected_tokens(path, &allocation).unwrap()), [-1]);
    assert_eq!(demanded.get(), 3);
    assert_eq!(numbers(grammar.rule(0).unwrap().start().next_tokens(&allocation).unwrap()), [-2]);
}

#[test]
fn rule_stop_temporarily_clears_the_called_rule_for_its_return_path() {
    const REPEATED_CALL: &[i32] = &[
        4, 1, 368, 7,
        2, 0, 7, 0, 2, 1, 7, 1, 1, 0, 1, 0, 1, 0,
        0, 0, 2, 0, 2, 0, 0,
        5,
        0, 4, 1, 0, 0, 0,
        2, 3, 1, 0, 0, 0,
        4, 5, 3, 2, 1, 0,
        5, 6, 3, 2, 1, 0,
        6, 1, 5, 12, 0, 0,
        0,
    ];
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = fixture(REPEATED_CALL, &allocation);
    let mut workspace = LookWorkspace::new(&grammar, &allocation).unwrap();
    assert_eq!(numbers(workspace.look(StateId(0), ReturnContext::LocalEnd).unwrap()), [12]);
    assert_eq!(workspace.called_rules, [0; 3]);
    assert!(workspace.frames.is_empty());
}

#[test]
fn returning_restores_the_rule_bit_before_left_recursive_sibling_descent() {
    const LEFT_RECURSIVE: &[i32] = &[
        4, 1, 368, 8,
        2, 0, 7, 0, 2, 1, 7, 1, 3, 1, 7, 1, 1, 1, 1, 8, 1,
        0, 1, 2, 2, 0, 2, 0, 0,
        7,
        0, 1, 3, 2, 1, 0,
        2, 4, 1, 0, 0, 0,
        4, 7, 1, 0, 0, 0,
        4, 5, 1, 0, 0, 0,
        5, 6, 3, 2, 1, 0,
        6, 7, 5, 12, 0, 0,
        7, 3, 1, 0, 0, 0,
        1, 4,
    ];
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = fixture(LEFT_RECURSIVE, &allocation);
    assert_eq!(numbers(grammar.rule(0).unwrap().start().next_tokens(&allocation).unwrap()), [-2]);
    assert_eq!(numbers(grammar.rule(1).unwrap().start().next_tokens(&allocation).unwrap()), [-2, 12]);
}

#[test]
fn completed_rule_descent_does_not_suppress_the_next_alternative() {
    const ALTERNATIVES: &[i32] = &[
        4, 1, 368, 10,
        2, 0, 7, 0, 2, 1, 7, 1, 3, 0, 9, 1, 0, 1, 0, 1, 0, 1, 0, 8, 0,
        0, 0, 2, 0, 2, 0, 0,
        9,
        0, 4, 1, 0, 0, 0,
        2, 3, 1, 0, 0, 0,
        4, 5, 1, 0, 0, 0,
        4, 6, 1, 0, 0, 0,
        5, 7, 3, 2, 1, 0,
        6, 8, 3, 2, 1, 0,
        7, 9, 5, 12, 0, 0,
        8, 9, 5, 10, 0, 0,
        9, 1, 1, 0, 0, 0,
        1, 4,
    ];
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = fixture(ALTERNATIVES, &allocation);
    let mut workspace = LookWorkspace::new(&grammar, &allocation).unwrap();
    assert_eq!(numbers(workspace.look(StateId(0), ReturnContext::LocalEnd).unwrap()), [10, 12]);
    assert_eq!(workspace.called_rules, [0; 3]);
    assert_eq!(workspace.contexts.iter().map(|node| node.follow).collect::<Vec<_>>(), [StateId(7), StateId(8)]);
}

#[test]
fn epsilon_cycles_preserve_action_precedence_and_set_label_reachability() {
    const CYCLE: &[i32] = &[
        4, 1, 368, 7,
        2, 0, 7, 0, 3, 0, 3, 8, 0, 1, 0, 1, 0, 1, 0,
        0, 0, 1, 0, 0,
        1, 2, 0, 10, 10, 12, 12,
        7,
        0, 2, 1, 0, 0, 0,
        2, 0, 1, 0, 0, 0,
        2, 3, 1, 0, 0, 0,
        3, 4, 6, 0, -1, 0,
        4, 5, 10, 7, 0, 0,
        5, 6, 7, 0, 0, 0,
        6, 1, 1, 0, 0, 0,
        1, 2,
    ];
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = fixture(CYCLE, &allocation);
    let mut workspace = LookWorkspace::new(&grammar, &allocation).unwrap();
    assert_eq!(numbers(workspace.look(StateId(0), ReturnContext::LocalEnd).unwrap()), [10, 12]);
    assert_eq!(workspace.visited_len, 5);
    assert!(workspace.frames.is_empty());
}

#[test]
fn singleton_context_equality_keeps_null_parent_identity_and_shared_ancestors() {
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = fixture(RETURN_CHAIN, &allocation);
    let mut workspace = LookWorkspace::new(&grammar, &allocation).unwrap();
    let first = workspace.context(ReturnContext::LocalEnd, StateId(1)).unwrap();
    let second = workspace.context(ReturnContext::LocalEnd, StateId(1)).unwrap();
    assert!(workspace.same_context(first, first));
    assert!(!workspace.same_context(first, second));
    assert!(!workspace.same_context(second, first));
    let child = workspace.context(first, StateId(3)).unwrap();
    let alias_child = workspace.context(first, StateId(3)).unwrap();
    let other_child = workspace.context(second, StateId(3)).unwrap();
    assert!(workspace.same_context(child, alias_child));
    assert!(!workspace.same_context(child, other_child));
    let full = workspace.context(ReturnContext::FullEnd, StateId(1)).unwrap();
    let equal_full = workspace.context(ReturnContext::FullEnd, StateId(1)).unwrap();
    assert!(workspace.same_context(full, equal_full));
    assert!(!workspace.same_context(full, first));
    assert!(!workspace.same_context(ReturnContext::LocalEnd, ReturnContext::FullEnd));
    assert!(workspace.visit(StateId(3), first).unwrap());
    assert!(!workspace.visit(StateId(3), first).unwrap());
    assert!(workspace.visit(StateId(3), second).unwrap());
    assert!(workspace.visit(StateId(3), full).unwrap());
    assert!(!workspace.visit(StateId(3), equal_full).unwrap());
    assert_eq!(workspace.visited_len, 3);
}

#[test]
fn visited_membership_ignores_called_rule_bits_and_survives_table_growth() {
    let memory = MemoryBudget::new(8 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = Grammar::load(&allocation).unwrap();
    let mut workspace = LookWorkspace::new(&grammar, &allocation).unwrap();
    for number in 0..100 { assert!(workspace.visit(StateId(number), ReturnContext::LocalEnd).unwrap()); }
    let retained = memory.used_bytes();
    workspace.set_called(RuleId(33), true);
    for number in 0..100 { assert!(!workspace.visit(StateId(number), ReturnContext::LocalEnd).unwrap()); }
    assert_eq!(workspace.visited_len, 100);
    assert_eq!(workspace.visited.len(), 256);
    assert_eq!(memory.used_bytes(), retained);
}

#[test]
fn deep_context_conversion_traversal_equality_and_drop_use_explicit_storage() {
    const DEPTH: usize = 20_000;
    let grammar_memory = MemoryBudget::new(4 << 20);
    let grammar_allocation = AllocationScope::new(&grammar_memory).unwrap();
    let grammar = fixture(RETURN_CHAIN, &grammar_allocation);
    let memory = MemoryBudget::new(64 << 20);
    let result;
    {
        let allocation = AllocationScope::new(&memory).unwrap();
        let path = std::iter::repeat_n(grammar.state(0).unwrap(), DEPTH);
        result = grammar.rule(1).unwrap().stop().next_tokens_with_context(path, &allocation).unwrap();
        let mut workspace = LookWorkspace::new(&grammar, &allocation).unwrap();
        let mut left = ReturnContext::FullEnd;
        let mut right = ReturnContext::FullEnd;
        for _ in 0..DEPTH {
            left = workspace.context(left, StateId(1)).unwrap();
            right = workspace.context(right, StateId(1)).unwrap();
        }
        assert!(workspace.same_context(left, right));
        let retained = memory.used_bytes();
        drop(workspace);
        assert_eq!(memory.used_bytes(), retained);
    }
    assert_eq!(numbers(result), [-1]);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn first_dfs_and_membership_growth_require_their_own_live_reservations() {
    let grammar_memory = MemoryBudget::new(4 << 20);
    let grammar_allocation = AllocationScope::new(&grammar_memory).unwrap();
    let grammar = fixture(RETURN_CHAIN, &grammar_allocation);
    let frames = 256 + INITIAL_CAPACITY * size_of::<Frame>();
    let slots = 256 + INITIAL_CAPACITY * size_of::<Option<Visited>>();
    for (limit, expected_used) in [(4096 + frames - 1, 4096), (4096 + frames + slots - 1, 4096 + frames), (4096 + frames + slots, 4096 + frames + slots)] {
        let memory = MemoryBudget::new(limit);
        {
            let allocation = AllocationScope::new(&memory).unwrap();
            let result = grammar.rule(0).unwrap().stop().next_tokens(&allocation);
            if limit == 4096 + frames + slots { assert_eq!(numbers(result.unwrap()), [-2]); }
            else { admission(result); }
            assert_eq!(memory.used_bytes(), expected_used);
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn duplicate_visits_do_not_grow_a_full_threshold_table_before_returning() {
    let grammar_memory = MemoryBudget::new(4 << 20);
    let grammar_allocation = AllocationScope::new(&grammar_memory).unwrap();
    let grammar = Grammar::load(&grammar_allocation).unwrap();
    let memory = MemoryBudget::new(4096 + 256 + INITIAL_CAPACITY * size_of::<Option<Visited>>());
    {
        let allocation = AllocationScope::new(&memory).unwrap();
        let mut workspace = LookWorkspace::new(&grammar, &allocation).unwrap();
        for number in 0..8 { assert!(workspace.visit(StateId(number), ReturnContext::LocalEnd).unwrap()); }
        let used = memory.used_bytes();
        assert!(!workspace.visit(StateId(0), ReturnContext::LocalEnd).unwrap());
        assert_eq!(memory.used_bytes(), used);
        admission(workspace.visit(StateId(8), ReturnContext::LocalEnd));
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn invocation_growth_failure_stops_parent_demand_and_releases_with_the_scope() {
    let grammar_memory = MemoryBudget::new(4 << 20);
    let grammar_allocation = AllocationScope::new(&grammar_memory).unwrap();
    let grammar = fixture(RETURN_CHAIN, &grammar_allocation);
    let memory = MemoryBudget::new(4096 + 256 + INITIAL_CAPACITY * size_of::<State<'_, '_>>());
    let demanded = Cell::new(0);
    {
        let allocation = AllocationScope::new(&memory).unwrap();
        let path = std::iter::repeat_n(grammar.state(0).unwrap(), 100).inspect(|_| demanded.set(demanded.get() + 1));
        admission(grammar.rule(1).unwrap().stop().next_tokens_with_context(path, &allocation));
        assert_eq!(demanded.get(), 17);
        assert_eq!(memory.used_bytes(), memory.limit_bytes());
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn expected_token_chaining_reuses_its_admitted_workspace_capacity() {
    let grammar_memory = MemoryBudget::new(4 << 20);
    let grammar_allocation = AllocationScope::new(&grammar_memory).unwrap();
    let grammar = fixture(RETURN_CHAIN, &grammar_allocation);
    let limit = 4096 + 256 + INITIAL_CAPACITY * size_of::<Frame>() + 256 + INITIAL_CAPACITY * size_of::<Option<Visited>>();
    let memory = MemoryBudget::new(limit);
    {
        let allocation = AllocationScope::new(&memory).unwrap();
        let path = std::iter::repeat_n(grammar.state(0).unwrap(), 1_000);
        assert_eq!(numbers(grammar.rule(1).unwrap().stop().expected_tokens(path, &allocation).unwrap()), [-1]);
        assert_eq!(memory.used_bytes(), limit);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn context_growth_charges_the_replacement_while_previous_storage_is_retained() {
    let grammar_memory = MemoryBudget::new(4 << 20);
    let grammar_allocation = AllocationScope::new(&grammar_memory).unwrap();
    let grammar = fixture(RETURN_CHAIN, &grammar_allocation);
    let invocations = 256 + 16 * size_of::<State<'_, '_>>() + 256 + 32 * size_of::<State<'_, '_>>();
    let contexts = 256 + 16 * size_of::<ContextNode>();
    let replacement = 256 + 32 * size_of::<ContextNode>();
    let memory = MemoryBudget::new(4096 + invocations + contexts + replacement - 1);
    let demanded = Cell::new(0);
    {
        let allocation = AllocationScope::new(&memory).unwrap();
        let path = std::iter::repeat_n(grammar.state(0).unwrap(), 17).inspect(|_| demanded.set(demanded.get() + 1));
        admission(grammar.rule(1).unwrap().stop().next_tokens_with_context(path, &allocation));
        assert_eq!(demanded.get(), 17);
        assert_eq!(memory.used_bytes(), 4096 + invocations + contexts);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn return_path_frame_growth_is_admitted_before_the_next_state_visit() {
    let grammar_memory = MemoryBudget::new(4 << 20);
    let grammar_allocation = AllocationScope::new(&grammar_memory).unwrap();
    let grammar = fixture(RETURN_CHAIN, &grammar_allocation);
    let invocations = 256 + 16 * size_of::<State<'_, '_>>() + 256 + 32 * size_of::<State<'_, '_>>();
    let contexts = 256 + 16 * size_of::<ContextNode>() + 256 + 32 * size_of::<ContextNode>();
    let frames = 256 + 16 * size_of::<Frame>();
    let slots = 256 + 16 * size_of::<Option<Visited>>() + 256 + 32 * size_of::<Option<Visited>>();
    let replacement = 256 + 32 * size_of::<Frame>();
    let admitted = 4096 + invocations + contexts + frames + slots;
    let memory = MemoryBudget::new(admitted + replacement - 1);
    {
        let allocation = AllocationScope::new(&memory).unwrap();
        let path = std::iter::repeat_n(grammar.state(0).unwrap(), 17);
        admission(grammar.rule(1).unwrap().stop().next_tokens_with_context(path, &allocation));
        assert_eq!(memory.used_bytes(), admitted);
    }
    assert_eq!(memory.used_bytes(), 0);
}
