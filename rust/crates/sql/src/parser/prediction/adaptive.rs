use crate::parser::grammar::{Decision, Grammar, TokenType};
use super::{Alternative, AlternativeChoice, AlternativeSet, ConfigSetKey, Configurations, PredictionContexts, PredictionError, PredictionMode, Problem, Result, Semantic, vector};
use super::closure::{ClosureRound, StartSet};
use super::dfa::{AcceptReason, Acceptance, Candidate, DfaTarget, DraftKey, InternedStateKey, PredPrediction, PredictionDfas};
use super::runtime::{PredictionInput, RuntimeError, RuntimeResult};
use super::storage::Retained;

#[cfg(test)]
#[path = "adaptive_tests.rs"]
mod tests;

pub(super) fn adaptive_predict<'cfg, 'id, 'g, 'scope, 'round, 'sql>(
    round: &mut ClosureRound<'_, 'round, 'cfg, 'id, 'g, 'scope, Retained>,
    dfas: &mut PredictionDfas<'cfg, 'g, 'scope>,
    input: &mut PredictionInput<'_, '_, 'round, '_, 'sql, '_>,
    mode: PredictionMode,
) -> RuntimeResult<'sql, Alternative> {
    let decision = input.decision();
    let origin = input.origin();
    let mut previous = local_dfa_start(round, dfas, input)?;
    let grammar = round.parts().1.grammar();
    let mut token = lookahead_type(input, grammar)?;
    loop {
        let DfaTarget::Interned(target) = local_dfa_target(round, dfas, input, previous, token, mode)? else {
            let candidate = input.no_viable_alternative()?;
            input.seek(origin)?;
            let previous_configs = round.from_cached(dfas, previous)?;
            return round.local_finishing_alternative(previous_configs, input)?
                .ok_or(RuntimeError::NoViableAlternative(candidate));
        };
        match (mode, dfas.view(decision, target)?.acceptance()) {
            (PredictionMode::Ll, Acceptance::Plain { reason: AcceptReason::Conflict, .. }) => {
                return full_prediction(round, input);
            }
            (PredictionMode::Ll, Acceptance::Predicated { reason: AcceptReason::Conflict, pairs }) => {
                let conflict_index = input.index().expect("prediction cursor");
                if conflict_index != origin {
                    input.seek(origin)?;
                }
                let alternatives = evaluate_predicate_pairs(pairs, input.precedence());
                if alternatives.count() == 1 {
                    return Ok(alternatives.first().expect("single evaluated alternative"));
                }
                if conflict_index != origin {
                    input.seek(conflict_index)?;
                }
                return full_prediction(round, input);
            }
            (_, Acceptance::Plain { prediction, .. }) => return Ok(prediction),
            (_, Acceptance::Predicated { pairs, .. }) => {
                input.seek(origin)?;
                return match evaluate_predicate_pairs(pairs, input.precedence()).first() {
                    Some(alternative) => Ok(alternative),
                    None => Err(RuntimeError::NoViableAlternative(input.no_viable_alternative()?)),
                };
            }
            (_, Acceptance::NonAccept) => {}
        }
        previous = target;
        if token != TokenType::EOF {
            input.advance()?;
            token = lookahead_type(input, grammar)?;
        }
    }
}

fn full_prediction<'cfg, 'id, 'g, 'scope, 'round, 'sql>(
    round: &mut ClosureRound<'_, 'round, 'cfg, 'id, 'g, 'scope, Retained>,
    input: &mut PredictionInput<'_, '_, 'round, '_, 'sql, '_>,
) -> RuntimeResult<'sql, Alternative> {
    let origin = input.origin();
    let mut full = round.enter_full(input)?;
    let mut previous = full.full_start()?;
    full.input().seek(origin)?;
    let grammar = full.parts().1.grammar();
    let mut token = lookahead_type(full.input(), grammar)?;
    loop {
        let Some(reach) = full.full_reach(previous, token)? else {
            let candidate = full.input().no_viable_alternative()?;
            full.input().seek(origin)?;
            return full.full_finishing_alternative(previous)?
                .ok_or(RuntimeError::NoViableAlternative(candidate));
        };
        let (configs, contexts) = full.parts();
        let mut workspace = configs.mode_workspace();
        let subsets = workspace.conflicting_subsets(configs, reach.key(), contexts)?;
        let unique = configs.view_set(reach.key()).unique_alternative();
        configs.set_unique(reach.key(), unique)?;
        if let Some(alternative) = unique {
            return Ok(alternative);
        }
        if let AlternativeChoice::One(alternative) = subsets.single_viable() {
            return Ok(alternative);
        }

        previous = reach;
        if token != TokenType::EOF {
            full.input().advance()?;
            token = lookahead_type(full.input(), grammar)?;
        }
    }
}

fn lookahead_type<'sql>(
    input: &mut PredictionInput<'_, '_, '_, '_, 'sql, '_>,
    grammar: &Grammar<'_>,
) -> RuntimeResult<'sql, TokenType> {
    let token = input.lookahead(1)?.expect("positive lookahead");
    Ok(grammar.token_type(token.lexeme().kind())?)
}

fn evaluate_predicate_pairs(pairs: &[PredPrediction], precedence: i32) -> AlternativeSet {
    let mut alternatives = AlternativeSet::default();
    for pair in pairs {
        if pair.semantic == Semantic::Unpredicated || pair.semantic.evaluate(precedence) {
            alternatives.insert(pair.alternative);
        }
    }
    alternatives
}

pub(super) fn local_dfa_start<'cfg, 'id, 'g, 'scope, 'round, 'sql>(
    round: &mut ClosureRound<'_, 'round, 'cfg, 'id, 'g, 'scope, Retained>,
    dfas: &mut PredictionDfas<'cfg, 'g, 'scope>,
    input: &PredictionInput<'_, '_, 'round, '_, 'sql, '_>,
) -> RuntimeResult<'sql, InternedStateKey<'cfg>> {
    input.ready()?;
    round.enter_local()?;
    let decision = input.decision();
    let precedence = input.precedence();
    if let Some(start) = dfas.start(decision, precedence)? {
        return Ok(start);
    }

    let start = match round.local_start()? {
        StartSet::Ordinary(start) => start,
        StartSet::Precedence(start) => {
            dfas.replace_synthetic_prefilter(decision, start.prefilter(), round.parts().0)?;
            round.precedence_filter(start, precedence)?
        }
    };
    let (configs, contexts) = round.parts();
    let draft = dfas.new_draft(decision, start.key(), configs)?;
    let DfaTarget::Interned(start) = dfas.add_state(decision, Candidate::Draft(draft), configs, contexts)? else {
        return Err(PredictionError::Artifact(Problem::DfaPhase).into());
    };
    dfas.set_start(decision, precedence, start)?;
    Ok(start)
}

pub(super) fn local_dfa_target<'cfg, 'id, 'g, 'scope, 'round, 'sql>(
    round: &mut ClosureRound<'_, 'round, 'cfg, 'id, 'g, 'scope, Retained>,
    dfas: &mut PredictionDfas<'cfg, 'g, 'scope>,
    input: &PredictionInput<'_, '_, 'round, '_, 'sql, '_>,
    previous: InternedStateKey<'cfg>,
    token: TokenType,
    mode: PredictionMode,
) -> RuntimeResult<'sql, DfaTarget<'cfg>> {
    input.ready()?;
    round.enter_local()?;
    let decision = input.decision();
    if let Some(target) = dfas.existing_edge(decision, previous, token.number())? {
        return Ok(target);
    }

    let previous_configs = round.from_cached(dfas, previous)?;
    let reach = round.local_reach(previous_configs, token)?;
    let (configs, contexts) = round.parts();
    let candidate = match reach {
        None => Candidate::Error,
        Some(reach) => {
            let selected = contexts.grammar().decision(decision)?;
            Candidate::Draft(prepare_target(dfas, configs, contexts, selected, reach.key(), mode)?)
        }
    };
    let target = dfas.add_edge(decision, Some(previous), token.number(), Some(candidate), configs, contexts)?;
    target.ok_or_else(|| PredictionError::Artifact(Problem::DfaPhase).into())
}

fn prepare_target<'cfg, 'id, 'g, 'scope>(
    dfas: &mut PredictionDfas<'cfg, 'g, 'scope>,
    configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>,
    contexts: &mut PredictionContexts<'id, 'g, 'scope, Retained>,
    decision: Decision<'g, 'scope>,
    reach: ConfigSetKey<'cfg>,
    mode: PredictionMode,
) -> Result<DraftKey<'cfg>> {
    let draft = dfas.new_draft(decision.number(), reach, configs)?;
    let (reason, alternatives) = if let Some(unique) = configs.view_set(reach).unique_alternative() {
        configs.set_unique(reach, Some(unique))?;
        let mut alternatives = AlternativeSet::default();
        alternatives.insert(unique);
        (AcceptReason::Unique, alternatives)
    } else {
        let mut workspace = configs.mode_workspace();
        if !workspace.has_sll_conflict(mode, configs, reach, contexts)? {
            return Ok(draft);
        }
        let alternatives = workspace.conflicting_subsets(configs, reach, contexts)?.union();
        let conflict = configs.publish_conflict(alternatives)?;
        configs.set_conflicting(reach, Some(conflict))?;
        (AcceptReason::Conflict, alternatives)
    };

    let prediction = alternatives.first().ok_or(PredictionError::Artifact(Problem::Alternative))?;
    let pairs = if configs.view_set(reach).has_semantic() {
        predicate_predictions(configs, contexts, decision, reach, alternatives)?
    } else {
        Vec::new()
    };
    let acceptance = if pairs.is_empty() {
        Acceptance::Plain { reason, prediction }
    } else {
        Acceptance::Predicated { reason, pairs: &pairs }
    };
    dfas.complete_acceptance(decision.number(), draft, acceptance, configs)?;
    Ok(draft)
}

fn predicate_predictions<'cfg, 'id, 'g, 'scope>(
    configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>,
    contexts: &PredictionContexts<'id, 'g, 'scope, Retained>,
    decision: Decision<'g, 'scope>,
    reach: ConfigSetKey<'cfg>,
    alternatives: AlternativeSet,
) -> Result<Vec<PredPrediction>> {
    let count = decision.state().transitions().len();
    let mut by_alternative = vector(contexts.allocation(), count)?;
    by_alternative.resize(count, None);
    let set = configs.view_set(reach);
    for index in 0..set.len() {
        let config = configs.view(set.entry(index).expect("reach occurrence"));
        let alternative = config.alternative();
        if alternatives.contains(alternative) {
            let slot = by_alternative.get_mut(alternative.number() - 1)
                .ok_or(PredictionError::Artifact(Problem::Alternative))?;
            *slot = Semantic::or(*slot, Some(config.semantic()));
        }
    }
    if !by_alternative.iter().any(|semantic| matches!(semantic, Some(Semantic::Precedence(_)))) {
        return Ok(Vec::new());
    }

    let mut pairs = vector(contexts.allocation(), alternatives.count() as usize)?;
    for (index, semantic) in by_alternative.into_iter().enumerate() {
        let alternative = configs.alternative(decision, index + 1)?;
        if alternatives.contains(alternative) {
            pairs.push(PredPrediction { semantic: semantic.unwrap_or(Semantic::Unpredicated), alternative });
        }
    }
    Ok(pairs)
}
