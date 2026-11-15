mod bind;
mod expression;
mod parse;
mod types;

pub use parse::{ParsedStatement, parse};

use rtdb_plan::{Catalog, PreparedStatement};
use rtdb_types::{LogicalType, Result};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct PrepareOptions {
    pub search_path: Vec<String>,
    pub parameter_types: BTreeMap<usize, LogicalType>,
}

impl Default for PrepareOptions {
    fn default() -> Self { Self { search_path: vec!["public".into()], parameter_types: BTreeMap::new() } }
}

pub fn prepare(sql: &str, catalog: &dyn Catalog, options: &PrepareOptions) -> Result<PreparedStatement> {
    let mut statements = parse(sql)?;
    if statements.len() != 1 { return Err(rtdb_types::Anomaly::incorrect("xtdb/sql-error", "A prepared statement must contain exactly one SQL statement")); }
    bind::bind(statements.remove(0), sql, catalog, options)
}
