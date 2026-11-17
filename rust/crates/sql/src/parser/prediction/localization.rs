use super::*;
use crate::parser::prediction::{Alternative, AlternativeSet};

#[cfg(test)]
#[path = "localization_tests.rs"]
mod tests;

impl<'round, 'cfg, 'id, 'g, 'scope, M: StoreMemory>
    ClosureRound<'_, 'round, 'cfg, 'id, 'g, 'scope, M>
{
    pub(in crate::parser::prediction) fn local_finishing_alternative<'sql>(
        &mut self,
        previous: LocalSet<'round, 'cfg>,
        input: &mut PredictionInput<'_, '_, 'round, '_, 'sql, '_>,
    ) -> RuntimeResult<'sql, Option<Alternative>> {
        let result = (|| {
            self.enter_local()?;
            input.ready()?;
            if input.decision() != self.decision.number() || input.index() != Some(input.origin()) {
                return Err(RuntimeError::Control(RuntimeProblem::PredictionSite));
            }
            self.local(previous.key())?;
            Ok(self.finishing_alternative(previous.key(), input.precedence())?)
        })();
        if result.is_err() {
            self.abort();
            input.abort();
        }
        result
    }

    fn finishing_alternative(
        &mut self,
        previous: ConfigSetKey<'cfg>,
        precedence: i32,
    ) -> Result<Option<Alternative>> {
        let mode = self.configs.view_set(previous).mode();
        let succeeded = self.configs.new_set(mode)?;
        let failed = self.configs.new_set(mode)?;
        for index in 0..self.configs.view_set(previous).len() {
            let key = self.configs.view_set(previous).entry(index).expect("input occurrence");
            let predicate = self.configs.view(key).semantic();
            let destination = if predicate == Semantic::Unpredicated || predicate.evaluate(precedence) {
                succeeded
            } else {
                failed
            };
            self.configs.add_uncached(destination, key, self.contexts.contexts())?;
        }
        if let Some(alternative) = self.minimum_finished(succeeded)? {
            return Ok(Some(alternative));
        }
        self.minimum_finished(failed)
    }

    fn minimum_finished(&mut self, set: ConfigSetKey<'cfg>) -> Result<Option<Alternative>> {
        let mut choices = AlternativeSet::default();
        let set = self.configs.view_set(set);
        for index in 0..set.len() {
            let config = self.configs.view(set.entry(index).expect("split occurrence"));
            if config.outer_depth() > 0
                || (matches!(config.state()?.kind(), StateKindView::RuleStop)
                    && self.contexts.contexts().view(config.context()).has_empty_path())
            {
                choices.insert(config.alternative());
            }
        }
        Ok(choices.first())
    }
}

impl<'round, 'cfg, 'sql, M: StoreMemory>
    FullClosure<'_, '_, 'round, 'cfg, '_, '_, '_, '_, '_, '_, 'sql, '_, M>
{
    pub(in crate::parser::prediction) fn full_finishing_alternative(
        &mut self,
        previous: FullSet<'round, 'cfg>,
    ) -> RuntimeResult<'sql, Option<Alternative>> {
        let result = (|| {
            self.enter()?;
            if !matches!(self.phase, FullPhase::Reaching)
                || self.round.configs.view_set(previous.key()).mode() != SetMode::Full
            {
                return Err(RuntimeError::Prediction(PredictionError::Artifact(Problem::ClosurePhase)));
            }
            if self.input.index() != Some(self.input.origin()) {
                return Err(RuntimeError::Control(RuntimeProblem::PredictionSite));
            }
            Ok(self.round.finishing_alternative(previous.key(), self.input.precedence())?)
        })();
        self.finish(result)
    }
}
