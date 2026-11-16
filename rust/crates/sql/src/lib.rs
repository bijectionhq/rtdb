mod bind;
mod dialect;
mod period;
mod expression;
mod window;
mod inline_xtql;
mod subquery;
mod parse;
mod types;
mod mutation;
mod control;
mod split;
mod settings;
mod temporal_syntax;
mod json_access;
mod materialized;
mod wildcard;
mod returning;

pub use parse::{ParsedStatement, StatementKind, parse};
pub use mutation::{Mutation, StaticMutation, prepare_tx, prepare_patch, prepare_static};
pub use control::parse_control;
pub use split::split_statements;

use rtdb_plan::{Catalog, PreparedStatement};
use rtdb_types::{LogicalType, Result};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct PrepareOptions {
    pub search_path: Vec<String>,
    pub parameter_types: BTreeMap<usize, LogicalType>,
    /// Execution-zone metadata for SQL casts and inline XTQL; defaults to UTC.
    pub default_timezone: String,
    /// Makes GENERATE_SERIES exclude its endpoint; RANGE always excludes it.
    /// Defaults to false.
    pub generate_series_end_exclusive: bool,
}

impl Default for PrepareOptions {
    fn default() -> Self { Self { search_path: vec!["public".into()], parameter_types: BTreeMap::new(), default_timezone: "UTC".into(), generate_series_end_exclusive: false } }
}

pub fn prepare(sql: &str, catalog: &dyn Catalog, options: &PrepareOptions) -> Result<PreparedStatement> {
    let mut statements = parse(sql)?;
    if statements.len() != 1 { return Err(rtdb_types::Anomaly::incorrect("xtdb/sql-error", "A prepared statement must contain exactly one SQL statement")); }
    bind::bind(statements.remove(0), sql, catalog, options)
}
