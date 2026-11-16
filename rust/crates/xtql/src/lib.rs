mod ast;
mod bind;
mod cast;
mod edn;
mod literal;

pub use ast::{Binding, Clause, Expr, From, Query, RelationInput, SubqueryKind, Tail, TemporalFilter, parse_query};
pub use bind::{CompileOptions, compile, compile_query};
pub use edn::{Edn, EdnKind, read};

use rtdb_types::Anomaly;

fn error(code: &str, message: impl Into<String>) -> Anomaly { Anomaly::incorrect(code, message) }
