use crate::parser::grammar::{ExpectedSymbol, OwnedGrammar, StateKindView, TransitionView};
use rtdb_types::{AllocationScope, MemoryBudget};
use super::*;

#[test]
fn ordered_nonreserved_switch_binds_each_source_case_and_child_to_the_atn() {
    let retained = MemoryBudget::new(8 << 20);
    let memory = MemoryBudget::new(8 << 20);
    let work = AllocationScope::new(&memory).unwrap();
    let owned = OwnedGrammar::load(&retained).unwrap();
    let grammar = owned.view();
    let decision = grammar.decision(65).unwrap().state();
    assert_eq!((decision.number(), decision.rule().number()), (848, NON_RESERVED_KEYWORD));
    let branches = decision.transitions();
    assert_eq!(branches.len(), NON_RESERVED_CASES.len());
    for (index, (branch, expected)) in branches.zip(NON_RESERVED_CASES).enumerate() {
        let TransitionView::Epsilon { target, .. } = branch else { panic!("keyword switch arm"); };
        assert_eq!(target.number(), FIRST_KEYWORD_ALTERNATIVE + u32::try_from(index).unwrap());
        assert!(matches!(target.kind(), StateKindView::Basic));
        let mut actions = target.transitions();
        let action = actions.next().unwrap();
        assert_eq!(actions.len(), 0);
        match (target.number(), action) {
            (797, TransitionView::Rule { rule, follow, .. }) => assert_eq!((rule.number(), follow.number()), (147, 849)),
            (798, TransitionView::Rule { rule, follow, .. }) => assert_eq!((rule.number(), follow.number()), (63, 849)),
            (799, TransitionView::Rule { rule, follow, .. }) => assert_eq!((rule.number(), follow.number()), (65, 849)),
            (800, TransitionView::Rule { rule, follow, .. }) => assert_eq!((rule.number(), follow.number()), (44, 849)),
            (_, TransitionView::Atom { token, target }) => {
                assert_eq!(expected, &[token.number()]);
                assert_eq!(target.number(), 849);
            }
            _ => panic!("unexpected keyword body"),
        }
        let first = target.next_tokens(&work).unwrap();
        let actual = first.iter().map(|symbol| match symbol {
            ExpectedSymbol::Token(token) => {
                assert_eq!(non_reserved_case(token), Some(target.number()));
                token.number()
            }
            ExpectedSymbol::Epsilon => panic!("keyword must consume"),
        }).collect::<Vec<_>>();
        assert_eq!(actual.as_slice(), expected);
    }
    for (state, count, additional) in [(848, 111, &[][..]), (759, 113, &[365, 366][..]), (507, 114, &[215, 365, 366][..])] {
        let first = grammar.state(state).unwrap().next_tokens(&work).unwrap();
        assert_eq!(first.iter().count(), count);
        assert!(first.iter().all(|symbol| match symbol {
            ExpectedSymbol::Token(token) => additional.contains(&token.number()) || non_reserved_case(token).is_some(),
            ExpectedSymbol::Epsilon => false,
        }));
    }
    drop(grammar);
    drop(owned);
    assert_eq!(retained.used_bytes(), 0);
    drop(work);
    assert_eq!(memory.used_bytes(), 0);
}
