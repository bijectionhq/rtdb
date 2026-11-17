mod ast;
mod bind;
mod cast;
mod edn;
mod literal;
mod legacy_inst;
mod regex_replace;

pub use ast::{Binding, Clause, Expr, From, Query, RelationInput, SubqueryKind, Tail, TemporalFilter, parse_query, parse_query_in};
pub use bind::{CompileOptions, compile, compile_in, compile_query, compile_query_in};
pub use edn::{Edn, EdnKind, read};

use rtdb_types::Anomaly;

#[derive(Clone, Copy)]
pub(crate) enum FormAllocation<'a> { Tracked(&'a std::sync::Arc<rtdb_types::MemoryBudget>), Untracked }
impl<'a> FormAllocation<'a> {
    pub(crate) fn builder(self) -> rtdb_plan::RegexFormBuilder<'a> {
        match self { Self::Tracked(memory) => rtdb_plan::RegexFormBuilder::new_in(memory), Self::Untracked => rtdb_plan::RegexFormBuilder::new_untracked() }
    }
    pub(crate) fn copy(self, recipe: &rtdb_plan::RegexReplacement) -> rtdb_types::Result<rtdb_plan::RegexReplacement> {
        match self { Self::Tracked(memory) => recipe.copy_in(memory), Self::Untracked => recipe.copy_untracked() }
    }
}

fn error(code: &str, message: impl Into<String>) -> Anomaly { Anomaly::incorrect(code, message) }
