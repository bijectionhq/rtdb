use std::mem::size_of;
use sha2::{Digest, Sha256};
use rtdb_types::{AllocationScope, MemoryBudget};
use crate::lexer::{Delimiter, Kind, Lexer};
use super::*;

fn artifact<T>(result: Result<T>, expected: IssueKind) {
    match result {
        Err(GrammarError::Artifact(error)) => assert_eq!(error.kind, expected),
        _ => panic!("artifact failure"),
    }
}

fn admission<T>(result: Result<T>) {
    match result {
        Err(GrammarError::Admission(error)) => assert_eq!(error.code, "rtdb/memory-budget-exceeded"),
        _ => panic!("allocation failure"),
    }
}

fn source_load<'scope>(words: &[i32], allocation: &'scope AllocationScope) -> Result<Grammar<'scope>> {
    let layout = deserialize::Layout::read(words)?;
    let source = source::Source::read(source::PINNED)?;
    let mut vocabulary = source.vocabulary(allocation, 176)?;
    vocabulary.rules.truncate(layout.rule_count());
    deserialize::load(words, layout, vocabulary, allocation)
}

fn allocation_stages() -> [usize; 11] {
    [
        28_406 * size_of::<i32>(),
        369 * size_of::<Option<&str>>(),
        369 * size_of::<&str>(),
        176 * size_of::<&str>(),
        3_017 * size_of::<deserialize::StateBuilder>(),
        176 * size_of::<Option<StateId>>(),
        176 * size_of::<RuleData>(),
        43 * size_of::<TokenSet>(),
        4_117 * size_of::<Transition>(),
        372 * size_of::<StateId>(),
        3_017 * size_of::<StateData>(),
    ]
}

#[test]
fn published_artifact_builds_the_complete_pinned_graph() {
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let source = source::Source::read(source::PINNED).unwrap();
    let words = source.words(&allocation).unwrap();
    assert_eq!(words.len(), 28_406);
    assert_eq!(source::PINNED.len(), 123_325);
    let provenance: serde_json::Value = serde_json::from_str(include_str!("UPSTREAM.json")).unwrap();
    assert_eq!(provenance["artifacts"]["Sql.interp"]["sha256"], "7ddd4e07bfee31ae4fb58775221992a4ec5310e0e5f1fc24d9b61af9ff9253c5");
    let digest = format!("{:x}", Sha256::digest(source::PINNED.as_bytes()));
    assert_eq!(digest, provenance["artifacts"]["Sql.interp"]["sha256"].as_str().unwrap());
    let grammar = Grammar::load(&allocation).unwrap();
    assert_eq!(grammar.states.len(), 3_017);
    assert_eq!(grammar.rules.len(), 176);
    assert_eq!(grammar.decisions.len(), 372);
    assert_eq!(grammar.sets.len(), 43);
    let mut kinds = [0usize; 13];
    let mut transitions = [0usize; 11];
    for state in &grammar.states {
        let kind = match state.kind {
            StateKind::Basic => 1, StateKind::RuleStart { .. } => 2, StateKind::BlockStart { .. } => 3,
            StateKind::PlusBlockStart { .. } => 4, StateKind::StarBlockStart { .. } => 5,
            StateKind::RuleStop => 7, StateKind::BlockEnd { .. } => 8, StateKind::StarLoopback => 9,
            StateKind::StarLoopEntry { .. } => 10, StateKind::PlusLoopback { .. } => 11, StateKind::LoopEnd { .. } => 12,
        };
        kinds[kind] += 1;
        for transition in &state.transitions {
            let kind = match transition {
                Transition::Epsilon { .. } => 1, Transition::Rule { .. } => 3, Transition::Atom { .. } => 5,
                Transition::Action { .. } => 6, Transition::Set { .. } => 7, Transition::Precedence { .. } => 10,
            };
            transitions[kind] += 1;
        }
    }
    assert_eq!(kinds, [0, 1_782, 176, 314, 5, 47, 0, 176, 366, 47, 47, 5, 52]);
    assert_eq!(transitions, [0, 2_475, 0, 602, 0, 891, 5, 56, 0, 0, 42]);
    assert_eq!(transitions.iter().sum::<usize>(), 4_071);
    let recursive: Vec<_> = (0..176).filter_map(|number| {
        let start = grammar.rule(number).unwrap().start();
        matches!(start.kind(), StateKindView::RuleStart { left_recursive: true, .. }).then_some(start.number())
    }).collect();
    assert_eq!(recursive, [52, 66, 68, 152, 252]);
    assert_eq!(grammar.rule(0).unwrap().name(), "directSqlStatement");
    for (decision, state) in [(0, 354), (1, 363), (2, 367), (34, 574), (371, 3014)] {
        let view = grammar.decision(decision).unwrap();
        assert_eq!(view.number(), decision);
        assert_eq!(view.state().number(), state);
    }
    artifact(grammar.state(3017), IssueKind::State);
    artifact(grammar.rule(176), IssueKind::Rule);
    artifact(grammar.decision(372), IssueKind::Decision);
}

#[test]
fn derived_returns_preserve_source_order_and_precedence_markers() {
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = Grammar::load(&allocation).unwrap();
    for call in [518, 522] {
        let state = grammar.state(call).unwrap();
        let TransitionView::Rule { target, rule, precedence, follow } = state.transitions().next().unwrap() else { panic!("rule transition") };
        assert_eq!((target.number(), rule.number(), precedence, follow.number()), (66, 33, 0, 575));
    }
    let stop = grammar.rule(33).unwrap().stop();
    assert_eq!(stop.number(), 67);
    let returns: Vec<_> = stop.transitions().filter_map(|transition| match transition {
        TransitionView::Epsilon { target, outermost_precedence_return } if target.number() == 575 => Some(outermost_precedence_return.unwrap().number()),
        _ => None,
    }).collect();
    assert_eq!(returns, [33]);
    let mut derived = 0;
    let mut marked = 0;
    for number in 0..176 {
        for transition in grammar.rule(number).unwrap().stop().transitions() {
            let TransitionView::Epsilon { outermost_precedence_return, .. } = transition else { panic!("derived epsilon") };
            derived += 1;
            marked += usize::from(outermost_precedence_return.is_some());
        }
    }
    assert_eq!((derived, marked), (556, 142));
    let precedence: Vec<_> = (0..372).filter_map(|number| {
        let state = grammar.decision(number).unwrap().state();
        matches!(state.kind(), StateKindView::StarLoopEntry { precedence: true, .. }).then(|| (number, state.number(), state.rule().number()))
    }).collect();
    assert_eq!(precedence, [(83, 957, 26), (99, 1131, 33), (153, 1661, 34), (220, 2084, 76), (286, 2512, 126)]);
}

#[test]
fn transformed_rule_actions_remain_distinct_epsilon_transitions() {
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = Grammar::load(&allocation).unwrap();
    for (number, destination, rule_number) in [(856, 857, 26), (985, 986, 33), (1134, 1135, 34), (2002, 2003, 76), (2454, 2455, 126)] {
        let state = grammar.state(number).unwrap();
        assert!(state.epsilon_only());
        assert_eq!(state.transitions().len(), 1);
        let TransitionView::Action { target, rule, action, context_dependent } = state.transitions().next().unwrap() else { panic!("action") };
        assert_eq!((target.number(), rule.number(), action, context_dependent), (destination, rule_number, -1, false));
    }
    let TransitionView::Atom { token, .. } = grammar.state(356).unwrap().transitions().next().unwrap() else { panic!("EOF atom") };
    assert_eq!(token, TokenType::EOF);
}

#[test]
fn completed_links_stay_on_the_grammar_owner() {
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = Grammar::load(&allocation).unwrap();
    let other = Grammar::load(&allocation).unwrap();
    for number in 0..3_017 {
        let state = grammar.state(number).unwrap();
        let same_owner = |linked: State<'_, '_>| {
            assert!(std::ptr::eq(linked.grammar, &grammar));
            assert!(!std::ptr::eq(linked.grammar, &other));
            assert_eq!(linked.rule().number(), state.rule().number());
        };
        match state.kind() {
            StateKindView::RuleStart { stop, .. } => { same_owner(stop); assert!(matches!(stop.kind(), StateKindView::RuleStop)); }
            StateKindView::BlockStart { end, .. } | StateKindView::StarBlockStart { end, .. }
                | StateKindView::PlusBlockStart { end, .. } => {
                same_owner(end);
                let StateKindView::BlockEnd { start } = end.kind() else { panic!("block end") };
                assert_eq!(start.number(), number);
            }
            StateKindView::StarLoopEntry { loopback, .. } => {
                same_owner(loopback);
                assert!(matches!(loopback.kind(), StateKindView::StarLoopback));
                let TransitionView::Epsilon { target, .. } = loopback.transitions().next().unwrap() else { panic!("loopback epsilon") };
                assert_eq!(target.number(), number);
            }
            StateKindView::LoopEnd { loopback } => same_owner(loopback),
            _ => {}
        }
        if let StateKindView::PlusBlockStart { loopback, .. } = state.kind() {
            same_owner(loopback);
            assert!(matches!(loopback.kind(), StateKindView::PlusLoopback { .. }));
            assert!(loopback.transitions().any(|transition| matches!(transition, TransitionView::Epsilon { target, .. } if target.number() == number)));
        }
    }
}

#[test]
fn raw_lexeme_kinds_map_to_every_pinned_token_without_decoding() {
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = Grammar::load(&allocation).unwrap();
    let literals = source::PINNED.split_once("token literal names:\n").unwrap().1.split_once("\n\ntoken symbolic names:\n").unwrap().0;
    let symbols = source::PINNED.split_once("token symbolic names:\n").unwrap().1.split_once("\n\nrule names:\n").unwrap().0;
    for (number, (literal, symbol)) in literals.lines().zip(symbols.lines()).enumerate().skip(10).take(354) {
        let spelling = if literal != "null" { &literal[1..literal.len() - 1] }
            else if symbol == "PG_EXPANDARRAY" { "_PG_EXPANDARRAY" } else { symbol };
        let mut lexer = Lexer::new(spelling, &allocation);
        let lexeme = lexer.next().unwrap();
        assert_eq!(lexeme.raw(), spelling);
        let token = grammar.token_type(lexeme.kind()).unwrap();
        assert_eq!(token.number(), number as i32, "{symbol}");
        assert_eq!(grammar.display_name(token), if literal == "null" { symbol } else { literal });
    }
    for (spelling, name) in [("ANALYSE", "ANALYZE"), ("LOCALDATE", "LOCAL_DATE"), ("LOCALTIME", "LOCAL_TIME"), ("LOCALTIMESTAMP", "LOCAL_TIMESTAMP")] {
        let mut lexer = Lexer::new(spelling, &allocation);
        assert_eq!(grammar.token_type(lexer.next().unwrap().kind()).unwrap().number() as usize, symbols.lines().position(|value| value == name).unwrap());
    }
    for (kind, number) in [
        (Kind::Float, 4), (Kind::Integer, 5), (Kind::CharacterString, 6), (Kind::EscapedString, 7), (Kind::BinaryString, 8),
        (Kind::Parameter, 9), (Kind::DollarStart, 364), (Kind::Identifier, 365),
        (Kind::DelimitedIdentifier(Delimiter::DoubleQuote), 366), (Kind::DelimitedIdentifier(Delimiter::Backtick), 366),
        (Kind::DollarEnd, 367), (Kind::DollarText, 368), (Kind::Eof, -1),
    ] { assert_eq!(grammar.token_type(kind).unwrap().number(), number); }
    for (number, name) in [(1, "BLOCK_COMMENT"), (2, "LINE_COMMENT"), (3, "WHITESPACE")] {
        assert_eq!(grammar.display_name(TokenType::read(number, 0).unwrap()), name);
    }
    let mut deferred = Lexer::new("E'\\N' @", &allocation);
    assert_eq!(grammar.token_type(deferred.next().unwrap().kind()).unwrap().number(), 7);
    assert_eq!(grammar.display_name(TokenType::EOF), "EOF");
}

#[test]
fn expected_sets_keep_epsilon_eof_and_high_tokens_distinct() {
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = Grammar::load(&allocation).unwrap();
    let mut set = TokenSet::empty();
    set.insert(ExpectedSymbol::Epsilon);
    for number in [-1, 12, 64, 256, 368] { set.insert(ExpectedSymbol::Token(TokenType::read(number, 0).unwrap())); }
    let mut other = TokenSet::singleton(ExpectedSymbol::Token(TokenType::EOF));
    other.union(&set);
    assert_eq!(other, set);
    assert!(set.contains(ExpectedSymbol::Epsilon));
    set.remove_epsilon();
    assert!(!set.contains(ExpectedSymbol::Epsilon));
    assert!(set.contains(ExpectedSymbol::Token(TokenType::EOF)));
    let ordered: Vec<_> = set.iter().map(|value| match value { ExpectedSymbol::Token(token) => token.number(), _ => panic!("epsilon") }).collect();
    assert_eq!(ordered, [-1, 12, 64, 256, 368]);
    for (set, expected) in [
        (TokenSet::empty(), "{}"),
        (TokenSet::singleton(ExpectedSymbol::Epsilon), "<EPSILON>"),
        (TokenSet::singleton(ExpectedSymbol::Token(TokenType::EOF)), "<EOF>"),
        ({ let mut value = TokenSet::singleton(ExpectedSymbol::Token(TokenType::EOF)); value.insert(ExpectedSymbol::Token(TokenType::read(12, 0).unwrap())); value }, "{<EOF>, ';'}"),
    ] {
        let length = set.display_len(&grammar).unwrap();
        allocation.reserve(length).unwrap();
        let mut text = String::with_capacity(length);
        set.write_display(&grammar, &mut text).unwrap();
        assert_eq!(text, expected);
        assert_eq!(text.len(), length);
    }
    artifact(TokenType::read(0, 0), IssueKind::Token);
    artifact(TokenType::read(369, 0), IssueKind::Token);
    artifact(TokenType::read(-2, 0), IssueKind::Token);
}

#[test]
fn every_construction_allocation_is_admitted_before_publication() {
    let stages = allocation_stages();
    let mut admitted = 4096;
    for (index, bytes) in stages.into_iter().enumerate() {
        let next = admitted + bytes + 256;
        for limit in [next - 1, next] {
            let memory = MemoryBudget::new(limit);
            {
                let allocation = AllocationScope::new(&memory).unwrap();
                let result = Grammar::load(&allocation);
                if index == stages.len() - 1 && limit == next {
                    let grammar = result.unwrap();
                    assert_eq!(grammar.states.len(), 3017);
                    drop(grammar);
                } else { admission(result); }
                assert_eq!(memory.used_bytes(), if limit == next { next } else { admitted });
            }
            assert_eq!(memory.used_bytes(), 0);
        }
        admitted = next;
    }
}

#[test]
fn grammar_views_borrow_both_owner_lifetimes_and_release_at_scope_drop() {
    fn retain<'g, 'scope>(grammar: &'g Grammar<'scope>) -> (State<'g, 'scope>, Rule<'g, 'scope>, Decision<'g, 'scope>) {
        (grammar.state(0).unwrap(), grammar.rule(0).unwrap(), grammar.decision(0).unwrap())
    }
    let memory = MemoryBudget::new(4 << 20);
    let display;
    {
        let allocation = AllocationScope::new(&memory).unwrap();
        let grammar = Grammar::load(&allocation).unwrap();
        let (state, rule, decision) = retain(&grammar);
        assert_eq!(state.number(), rule.start().number());
        assert_eq!(decision.state().number(), 354);
        display = grammar.display_name(TokenType::EOF);
        let used = memory.used_bytes();
        drop(grammar);
        assert_eq!(memory.used_bytes(), used);
    }
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(display, "EOF");
    assert!(std::mem::needs_drop::<Grammar<'_>>());
}

const SIMPLE: &[i32] = &[
    4, 1, 368, 3,
    2, 0, 1, 0, 7, 0,
    0, 0, 1, 0, 0, 0,
    2,
    0, 1, 1, 0, 0, 0,
    1, 2, 5, 0, 0, 1,
    0,
];

#[test]
fn malformed_artifacts_fail_without_a_partial_graph_or_owned_diagnostic() {
    let memory = MemoryBudget::new(8 << 20);
    {
        let allocation = AllocationScope::new(&memory).unwrap();
        for end in 0..SIMPLE.len() { artifact(source_load(&SIMPLE[..end], &allocation), IssueKind::Extent); }
        let grammar = source_load(SIMPLE, &allocation).unwrap();
        assert_eq!(grammar.states.len(), 3);
        let TransitionView::Atom { token, .. } = grammar.state(1).unwrap().transitions().next().unwrap() else { panic!("atom") };
        assert_eq!(token, TokenType::EOF);
        drop(grammar);
        for (offset, replacement, kind) in [
            (0, 5, IssueKind::Version), (1, 0, IssueKind::GrammarType), (2, 367, IssueKind::Token),
            (4, 6, IssueKind::State), (5, 1, IssueKind::Rule), (13, 1, IssueKind::Rule),
            (17, 3, IssueKind::State), (19, 9, IssueKind::Transition), (19, 7, IssueKind::Transition),
        ] {
            let mut words = SIMPLE.to_vec();
            words[offset] = replacement;
            artifact(source_load(&words, &allocation), kind);
        }
        let mut words = SIMPLE.to_vec();
        words.push(0);
        artifact(source_load(&words, &allocation), IssueKind::Extent);
        assert!(!std::mem::needs_drop::<ArtifactIssue>());
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn duplicate_labels_and_epsilon_targets_retain_the_first_transition_identity() {
    const DUPLICATES: &[i32] = &[
        4, 1, 368, 3,
        2, 0, 1, 0, 7, 0,
        0, 0, 1, 0, 0,
        1, 1, 0, 12, 12,
        4,
        0, 1, 6, 0, -1, 0,
        0, 1, 1, 0, 0, 0,
        1, 2, 7, 0, 0, 0,
        1, 2, 5, 12, 0, 0,
        0,
    ];
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = source_load(DUPLICATES, &allocation).unwrap();
    let start = grammar.state(0).unwrap();
    assert!(start.epsilon_only());
    assert_eq!(start.transitions().len(), 1);
    assert!(matches!(start.transitions().next().unwrap(), TransitionView::Action { action: -1, .. }));
    let token = grammar.state(1).unwrap();
    assert!(!token.epsilon_only());
    assert_eq!(token.transitions().len(), 1);
    let TransitionView::Set { tokens, .. } = token.transitions().next().unwrap() else { panic!("retained set") };
    assert!(tokens.contains(ExpectedSymbol::Token(TokenType::read(12, 0).unwrap())));
}

#[test]
fn block_links_require_a_matching_end_and_reverse_start() {
    const BLOCK: &[i32] = &[
        4, 1, 368, 4,
        2, 0, 3, 0, 2, 8, 0, 7, 0,
        0, 0, 1, 0, 0, 0,
        3,
        0, 1, 1, 0, 0, 0,
        1, 2, 1, 0, 0, 0,
        2, 3, 1, 0, 0, 0,
        1, 1,
    ];
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = source_load(BLOCK, &allocation).unwrap();
    let StateKindView::BlockStart { end, decision } = grammar.state(1).unwrap().kind() else { panic!("block") };
    assert_eq!(end.number(), 2);
    assert_eq!(decision.decision.unwrap().number(), 0);
    drop(grammar);
    let mut words = BLOCK.to_vec();
    words[8] = 3;
    artifact(source_load(&words, &allocation), IssueKind::Link);
    words = BLOCK.to_vec();
    words[6] = 5;
    let grammar = source_load(&words, &allocation).unwrap();
    assert!(matches!(grammar.state(1).unwrap().kind(), StateKindView::StarBlockStart { .. }));
    drop(grammar);
    const MISSING: &[i32] = &[
        4, 1, 368, 3, 2, 0, 8, 0, 7, 0,
        0, 0, 1, 0, 0, 0,
        2, 0, 1, 1, 0, 0, 0, 1, 2, 1, 0, 0, 0, 0,
    ];
    artifact(source_load(MISSING, &allocation), IssueKind::Link);
}
