use crate::parser::grammar::{StateKindView, TokenType, TransitionView};
use super::{
    ContextChild, ContextClass, InvocationKey, InvocationRole, ParserProfile, ParserSession, PassError,
    PassResult, RuntimeError, RuntimeProblem, RuntimeResult, TreeDataTypeEntry,
};
use super::super::PredictionMode;

#[cfg(test)]
#[path = "generated_data_type_tests.rs"]
mod tests;

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    fn data_type_atom(&mut self, state: u32) -> RuntimeResult<'sql, TokenType> {
        let selected = (|| {
            let grammar = self.environment.storage.grammar_view();
            let state = grammar.state(state)?;
            let mut edges = state.transitions();
            match (state.kind(), edges.next(), edges.len()) {
                (StateKindView::Basic, Some(TransitionView::Atom { token, .. }), 0) => Ok(token),
                _ => Err(RuntimeError::Control(RuntimeProblem::PredictionSite)),
            }
        })();
        self.finish(selected)
    }

    fn match_data_type_tree_atom(&mut self, state: u32) -> PassResult<'parse, 'sql, ()> {
        self.set_state(state)?;
        let expected = self.data_type_atom(state)?;
        let token = self.match_bail(expected)?;
        let current = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Invocation))?;
        self.append_child(current, ContextChild::Terminal(token))?;
        Ok(())
    }

    fn require_data_type_token(&mut self, atom: u32) -> RuntimeResult<'sql, ()> {
        let (_, actual) = self.lookahead_token(1)?;
        let expected = self.data_type_atom(atom)?;
        if actual != expected {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite)));
        }
        Ok(())
    }

    fn data_type_date_base(
        &mut self,
        entry: &TreeDataTypeEntry<'parse>,
        local: &mut InvocationKey<'parse>,
    ) -> PassResult<'parse, 'sql, ()> {
        self.set_state(939)?;
        self.sync_required_block()?;
        self.require_data_type_token(885)?;
        *local = self.label_date_type(entry)?;
        self.match_data_type_tree_atom(885)?;
        let stop = self.input.lt(-1).map_err(RuntimeError::from);
        let stop = self.finish(stop)?;
        self.records[local.index].stop = stop;
        Ok(())
    }

    fn data_type_array_open(
        &mut self,
        entry: &TreeDataTypeEntry<'parse>,
        local: &mut InvocationKey<'parse>,
    ) -> PassResult<'parse, 'sql, bool> {
        self.set_state(955)?;
        self.sync_required_block()?;
        if self.adaptive_predict(82, PredictionMode::Sll)?.number() != 2 {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from);
        }
        let replacement = self.new_data_type_array(entry)?;
        *local = replacement.key;
        self.push_data_type_array(entry, replacement)?;
        self.set_state(949)?;
        if self.precedence() > 1 {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from);
        }
        self.match_data_type_tree_atom(950)?;
        self.set_state(952)?;
        self.sync_required_block()?;
        let (_, actual) = self.lookahead_token(1)?;
        Ok(actual == self.data_type_atom(980)?)
    }

    fn maximum_cardinality_sll(&mut self) -> PassResult<'parse, 'sql, ()> {
        let local = self.new_invocation(31)?;
        self.enter_tree_rule(local)?;
        let result = self.match_data_type_tree_atom(980);
        self.exit_rule()?;
        result
    }

    pub(in crate::parser::prediction) fn data_type_sll(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let mut entry = self.enter_data_type_tree()?;
        let mut local = entry.0.key;
        let body = (|| {
            self.data_type_date_base(&entry, &mut local)?;
            let mut loop_state = 957;
            loop {
                self.set_state(loop_state)?;
                self.sync_required_block()?;
                match self.adaptive_predict(83, PredictionMode::Sll)?.number() {
                    2 => break,
                    1 => (),
                    _ => return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from),
                }
                if self.data_type_array_open(&entry, &mut local)? {
                    self.set_state(951)?;
                    self.maximum_cardinality_sll()?;
                }
                self.match_data_type_tree_atom(954)?;
                loop_state = 959;
            }
            Ok(local)
        })();
        let result = match body {
            Err(PassError::Runtime(RuntimeError::NoViableAlternative(_))) => self.catch_recognition(local),
            result => result,
        };
        self.unroll_data_type_tree(&mut entry)?;
        result
    }

    fn identifier_regular_sll(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let mut local = self.new_invocation(22)?;
        self.enter_tree_rule(local)?;
        let body = (|| {
            self.set_state(759)?;
            self.sync_required_block()?;
            self.require_data_type_token(754)?;
            local = self.labelled_context(local, ContextClass::RegularIdentifier)?;
            self.enter_outer_alt(local)?;
            self.set_state(756)?;
            self.sync_required_block()?;
            self.require_data_type_token(754)?;
            self.match_data_type_tree_atom(754)?;
            Ok(local)
        })();
        self.exit_rule()?;
        body
    }

    fn enter_field_definition_tree(&mut self) -> RuntimeResult<'sql, InvocationKey<'parse>> {
        let site = self.ready().and_then(|()| {
            if self.profile != ParserProfile::SllBail || self.current.is_some() {
                return Err(RuntimeError::Control(RuntimeProblem::PredictionSite));
            }
            Ok(())
        });
        self.finish(site)?;
        let local = self.new_invocation(32)?;
        self.records[local.index].role = InvocationRole::FieldDefinition { field_name: None };
        self.enter_tree_rule(local)?;
        Ok(local)
    }

    fn bind_field_name(&mut self, field: InvocationKey<'parse>) -> PassResult<'parse, 'sql, ()> {
        self.set_state(982)?;
        let field_name = self.identifier_regular_sll()?;
        self.records[field.index].role = InvocationRole::FieldDefinition { field_name: Some(field_name) };
        self.set_state(983)?;
        Ok(())
    }

    pub(in crate::parser::prediction) fn field_definition_sll(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let local = self.enter_field_definition_tree()?;
        let body = (|| {
            self.bind_field_name(local)?;
            self.data_type_sll()?;
            Ok(local)
        })();
        let result = match body {
            Err(PassError::Runtime(RuntimeError::NoViableAlternative(_))) => self.catch_recognition(local),
            result => result,
        };
        self.exit_rule()?;
        result
    }
}
