use rtdb_types::AllocationScope;
use crate::parser::grammar::{ExpectedSymbol, State, StateKindView, TokenType, TransitionView};
use super::generated_actions::{CallAction, PredicateAction, TerminalExpectation};
use super::{
    ContextChild, ContextClass, InvocationKey, ParserSession, ParserState, PassError, PassResult, RecursionEntry,
    RuntimeError, RuntimeProblem, RuntimeResult,
};
use super::super::PredictionMode;

// Sql.java's sync sites that call adaptivePredict, including the separate loopback sites.
const ADAPTIVE: &[(u32, u16)] = &[
    (585, 36), (664, 48), (672, 50), (709, 55), (730, 59), (742, 60),
    (751, 62), (753, 62), (866, 68), (874, 69), (879, 70), (891, 72),
    (894, 73), (901, 74), (904, 75), (909, 76), (917, 77), (947, 80),
    (955, 82), (957, 83), (959, 83), (1001, 89), (1096, 93), (1106, 95),
    (1129, 98), (1131, 99), (1133, 99), (1223, 106), (1240, 108), (1257, 110),
    (1267, 111), (1329, 116), (1384, 126), (1460, 129), (1462, 130), (1484, 134),
    (1504, 137), (1522, 140), (1534, 142), (1574, 145), (1581, 146), (1634, 151),
    (1659, 152), (1661, 153), (1663, 153), (1667, 154), (1675, 156), (1680, 157),
    (1685, 158), (1692, 159), (1824, 179), (1887, 186), (1938, 193), (1965, 198),
    (1972, 199), (1974, 199), (1987, 201), (1989, 201), (1999, 202), (2001, 202),
    (2006, 203), (2008, 204), (2010, 204), (2013, 205), (2015, 206), (2020, 207),
    (2026, 208), (2037, 210), (2045, 212), (2049, 213), (2053, 214), (2055, 215),
    (2061, 216), (2082, 219), (2084, 220), (2086, 220), (2105, 222), (2121, 223),
    (2142, 225), (2147, 226), (2190, 232), (2192, 232), (2196, 233), (2209, 234),
    (2211, 234), (2280, 243), (2295, 244), (2299, 245), (2306, 246), (2310, 247),
    (2312, 247), (2314, 248), (2318, 249), (2322, 250), (2324, 250), (2327, 251),
    (2329, 252), (2333, 253), (2336, 254), (2340, 255), (2346, 256), (2349, 257),
    (2365, 259), (2384, 261), (2403, 263), (2407, 264), (2457, 274), (2460, 275),
    (2463, 276), (2466, 277), (2469, 278), (2475, 279), (2477, 279), (2479, 280),
    (2510, 285), (2512, 286), (2514, 286), (2630, 308), (2634, 309), (2643, 310),
    (2650, 311), (2654, 312), (2658, 313), (2660, 313), (2666, 314), (2678, 316),
    (2688, 318), (2702, 319), (2704, 320), (2743, 325), (2767, 329), (2801, 334),
    (2804, 335),
];

const STATEMENT_DECISION: u16 = 34;
const RETURNING_RULE: u16 = 153;
const MAX_RULE_DEPTH: usize = 128;

enum ReplayStep {
    Stop,
    Epsilon(u32),
    Decision,
    Call(CallAction),
    Terminal { expected: TerminalExpectation, target: u32 },
    Precedence(PredicateAction),
}

enum ReplayChoice {
    Predicted(usize),
    Lookahead(TokenType),
    RequiredOnce,
}

fn replay_site<'sql>() -> RuntimeError<'sql> {
    RuntimeError::Control(RuntimeProblem::PredictionSite)
}

fn replay_step<'sql>(state: State<'_, '_>) -> RuntimeResult<'sql, ReplayStep> {
    let number = state.number();
    let rule = state.rule().number();
    let kind = state.kind();
    if matches!(kind, StateKindView::RuleStop) {
        return Ok(ReplayStep::Stop);
    }
    let mut edges = state.transitions();
    if edges.len() > 1 || matches!(kind, StateKindView::StarLoopback | StateKindView::PlusBlockStart { .. }) {
        return Ok(ReplayStep::Decision);
    }
    match edges.next() {
        Some(TransitionView::Epsilon { target, outermost_precedence_return: None }) =>
            Ok(ReplayStep::Epsilon(target.number())),
        // The generated Java has an empty action on labelled recursive bases.
        Some(TransitionView::Action { target, rule: action_rule, action: -1, context_dependent: false })
            if action_rule.number() == rule => Ok(ReplayStep::Epsilon(target.number())),
        Some(TransitionView::Rule { target, rule: called, precedence, follow }) =>
            Ok(ReplayStep::Call(CallAction {
                state: number, rule, called: called.number(), start: target.number(), follow: follow.number(), precedence,
            })),
        Some(TransitionView::Atom { target, token }) =>
            Ok(ReplayStep::Terminal { expected: TerminalExpectation::Atom(token), target: target.number() }),
        Some(TransitionView::Set { target, tokens }) =>
            Ok(ReplayStep::Terminal { expected: TerminalExpectation::Set(*tokens), target: target.number() }),
        Some(TransitionView::Precedence { target, precedence }) =>
            Ok(ReplayStep::Precedence(PredicateAction { state: number, rule, target: target.number(), minimum: precedence })),
        _ => Err(replay_site()),
    }
}

fn epsilon_target<'g, 'scope>(
    state: State<'g, 'scope>, alternative: usize,
) -> Option<State<'g, 'scope>> {
    match state.transitions().nth(alternative)? {
        TransitionView::Epsilon { target, outermost_precedence_return: None } => Some(target),
        _ => None,
    }
}

fn direct_target<'g, 'scope, 'sql>(
    decision: State<'g, 'scope>, token: TokenType, work: &AllocationScope,
) -> RuntimeResult<'sql, Option<State<'g, 'scope>>> {
    let block_end = match decision.kind() {
        StateKindView::BlockStart { end, .. } => Some(end.number()),
        _ => None,
    };
    let mut bypass = None;
    for index in 0..decision.transitions().len() {
        let target = epsilon_target(decision, index).ok_or_else(replay_site)?;
        if Some(target.number()) == block_end || matches!(target.kind(), StateKindView::LoopEnd { .. }) {
            bypass = Some(target);
        } else if target.next_tokens(work)?.contains(ExpectedSymbol::Token(token)) {
            return Ok(Some(target));
        }
    }
    Ok(bypass)
}

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    pub(super) fn replay_statement_alternative(
        &mut self, local: InvocationKey<'parse>, alternative: usize, mode: PredictionMode,
    ) -> PassResult<'parse, 'sql, ()> {
        let start = {
            let grammar = self.environment.storage.grammar_view();
            let decision = grammar.decision(STATEMENT_DECISION).map_err(RuntimeError::from)?;
            epsilon_target(decision.state(), alternative - 1).map(State::number)
        };
        let start = self.finish(start.ok_or_else(replay_site))?;
        let mut local = local;
        self.replay_body(&mut local, start, None, 0, mode)
    }

    fn replay_body(
        &mut self, local: &mut InvocationKey<'parse>, mut state: u32, recursion: Option<&RecursionEntry<'parse>>,
        depth: usize, mode: PredictionMode,
    ) -> PassResult<'parse, 'sql, ()> {
        let rule = self.records[local.index].rule;
        loop {
            let step = {
                let grammar = self.environment.storage.grammar_view();
                grammar.state(state).map_err(RuntimeError::from).and_then(|state| {
                    if state.rule().number() == rule { replay_step(state) } else { Err(replay_site()) }
                })
            };
            state = match self.finish(step)? {
                ReplayStep::Stop => return Ok(()),
                ReplayStep::Epsilon(target) => target,
                ReplayStep::Decision => self.replay_decision(*local, state, rule, mode)?,
                ReplayStep::Call(call) => {
                    self.generated_call(call)?;
                    self.replay_rule(call, depth + 1, mode)?;
                    call.follow
                }
                ReplayStep::Terminal { expected, target } => {
                    self.generated_action_state(state, rule)?;
                    let token = self.match_validated_terminal(state, expected)?;
                    self.append_child(*local, ContextChild::Terminal(token))?;
                    target
                }
                ReplayStep::Precedence(action) => {
                    let entry = self.finish(recursion.ok_or_else(replay_site))?;
                    let replacement = self.new_recursion_replacement(entry)?;
                    *local = replacement.key;
                    self.push_recursion_tree(entry, replacement)?;
                    self.generated_predicate(action)?;
                    action.target
                }
            };
        }
    }

    fn replay_rule(&mut self, call: CallAction, depth: usize, mode: PredictionMode) -> PassResult<'parse, 'sql, ()> {
        if depth > MAX_RULE_DEPTH {
            return self.finish(Err(replay_site())).map_err(PassError::from);
        }
        let recursive = {
            let grammar = self.environment.storage.grammar_view();
            let start = grammar.state(call.start).map_err(RuntimeError::from)?;
            matches!(start.kind(), StateKindView::RuleStart { left_recursive: true, .. })
        };
        let parent = self.current;
        let entered: RuntimeResult<'sql, _> = (|| {
            if recursive {
                let pending = self.new_recursion_entry(call.called)?;
                let entry = self.enter_recursion(pending, call.precedence)?;
                Ok((entry.key, Some(entry)))
            } else {
                let local = self.new_invocation(call.called)?;
                self.enter_tree_rule(local)?;
                Ok((local, None))
            }
        })();
        let (mut local, entry) = match entered {
            Ok(entered) => entered,
            Err(error) => {
                // Entry can reserve tree storage after installing the child.
                if self.current != parent {
                    self.current = parent;
                    self.state = ParserState::State(call.state);
                    if recursive {
                        self.precedence.pop();
                    }
                }
                return Err(error.into());
            }
        };
        let body = (|| {
            if call.called == RETURNING_RULE {
                local = self.labelled_context(local, ContextClass::DmlReturningStatement)?;
                self.enter_outer_alt(local)?;
            }
            self.replay_body(&mut local, call.start, entry.as_ref(), depth, mode)
        })();
        let result = match body {
            Err(PassError::Runtime(RuntimeError::NoViableAlternative(_) | RuntimeError::InputMismatch(_))) =>
                self.catch_recognition(local),
            result => result,
        };
        let exit = if let Some(entry) = entry {
            self.exit_replay_recursion(&entry)
        } else {
            self.exit_rule()
        };
        match (result, exit) {
            (Err(error @ PassError::Runtime(_)), _) => Err(error),
            (result, Ok(())) => result,
            (_, Err(error)) => Err(error.into()),
        }
    }

    fn exit_replay_recursion(&mut self, entry: &RecursionEntry<'parse>) -> RuntimeResult<'sql, ()> {
        let exit = (|| {
            let (saved, _, _) = self.active_entry(entry)?;
            let child = self.unroll_entered_recursion(entry.key, saved)?;
            if let Some(parent) = saved.parent {
                self.append_child_storage(parent, ContextChild::Context(child))?;
            }
            Ok(())
        })();
        self.finish(exit)
    }

    fn replay_decision(
        &mut self, local: InvocationKey<'parse>, state: u32, rule: u16, mode: PredictionMode,
    ) -> RuntimeResult<'sql, u32> {
        let (decision, loopback, plus_loopback, required_once, precedence_entry) = {
            let grammar = self.environment.storage.grammar_view();
            let site = grammar.state(state)?;
            let loopback = matches!(site.kind(), StateKindView::StarLoopback);
            let decision = if loopback { epsilon_target(site, 0).ok_or_else(replay_site)? } else { site };
            (
                decision.number(),
                loopback,
                matches!(site.kind(), StateKindView::PlusLoopback { .. }),
                site.transitions().len() == 1 && !loopback,
                matches!(site.kind(), StateKindView::StarLoopEntry { precedence: true, .. }),
            )
        };
        if precedence_entry {
            let stop = self.input.lt(-1).map_err(RuntimeError::from);
            let stop = self.finish(stop)?;
            self.records[local.index].stop = stop;
        }
        self.sync_replay_decision(state, rule, loopback || plus_loopback)?;

        let adaptive = ADAPTIVE.binary_search_by_key(&state, |&(state, _)| state).ok()
            .map(|index| ADAPTIVE[index].1);
        let choice = if let Some(id) = adaptive {
            let matches = self.environment.storage.grammar_view().decision(id)?.state().number() == decision;
            self.finish(matches.then_some(()).ok_or_else(replay_site))?;
            ReplayChoice::Predicted(self.adaptive_predict(id, mode)?.number() - 1)
        } else {
            let (_, token) = self.lookahead_token(1)?;
            if required_once { ReplayChoice::RequiredOnce } else { ReplayChoice::Lookahead(token) }
        };
        let selected = (|| {
            let grammar = self.environment.storage.grammar_view();
            let site = grammar.state(decision)?;
            let selected = match choice {
                ReplayChoice::Predicted(index) => Some(epsilon_target(site, index).ok_or_else(replay_site)?),
                ReplayChoice::RequiredOnce => Some(epsilon_target(site, 0).ok_or_else(replay_site)?),
                ReplayChoice::Lookahead(token) => direct_target(site, token, &self.work.scratch())?,
            };
            // The generated do-while jumps to the body on a repeat, past the first sync.
            if let Some(selected) = selected
                && plus_loopback && matches!(selected.kind(), StateKindView::PlusBlockStart { .. })
            {
                return Ok(Some(epsilon_target(selected, 0).ok_or_else(replay_site)?.number()));
            }
            Ok(selected.map(State::number))
        })();
        match self.finish(selected)? {
            Some(target) => Ok(target),
            None => self.generated_no_viable(),
        }
    }
}
