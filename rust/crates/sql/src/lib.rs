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
mod interval_literal;
mod literal_admission;
mod literal_syntax;
mod json_access;
mod materialized;
mod wildcard;
mod returning;
mod lexer;
mod parser;
mod literal_text;

pub use parse::{ParsedStatement, StatementKind, parse, parse_in};
pub use mutation::{Mutation, StaticMutation, prepare_tx, prepare_tx_in, prepare_patch, prepare_static, prepare_static_in};
pub use control::{parse_control, parse_control_in};
pub use split::split_statements;

use rtdb_plan::{Catalog, PreparedStatement};
use rtdb_types::{LogicalType, MemoryBudget, Result};
use std::{collections::BTreeMap, sync::Arc};

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

/// Prepares programmatic source with explicitly untracked raw metadata compatibility storage.
pub fn prepare(sql: &str, catalog: &dyn Catalog, options: &PrepareOptions) -> Result<PreparedStatement> {
    prepare_with(sql, catalog, options, bind::FormAllocation::Untracked)
}

/// Prepares source using the caller's existing budget for every reached raw metadata owner.
/// Source/catalog error order is preserved; native size, allocation and budget errors propagate.
pub fn prepare_in(sql: &str, catalog: &dyn Catalog, options: &PrepareOptions, memory: &Arc<MemoryBudget>) -> Result<PreparedStatement> {
    prepare_with(sql, catalog, options, bind::FormAllocation::Tracked(memory))
}

pub(crate) fn prepare_with(sql: &str, catalog: &dyn Catalog, options: &PrepareOptions, forms: bind::FormAllocation<'_>) -> Result<PreparedStatement> {
    let mut statements = parse::parse_with(sql, forms)?;
    if statements.len() != 1 { return Err(rtdb_types::Anomaly::incorrect("xtdb/sql-error", "A prepared statement must contain exactly one SQL statement")); }
    bind::bind(statements.remove(0), sql, catalog, options, forms)
}

#[cfg(test)]
mod owner_tests {
    use super::*;
    use std::collections::BTreeSet;
    struct ScalarCatalog;
    impl Catalog for ScalarCatalog {
        fn default_database(&self) -> &str { "xtdb" }
        fn databases(&self) -> BTreeSet<String> { BTreeSet::new() }
        fn table(&self, _: &rtdb_plan::QualifiedTable) -> Result<Option<rtdb_plan::TableBinding>> { Ok(None) }
        fn tables(&self, _: &str) -> Result<Vec<rtdb_plan::TableBinding>> { Ok(Vec::new()) }
    }
    #[test]
    fn native_source_owners_retain_their_budget_after_prepare_and_clone() {
        let sql = "SELECT regexp_replace('abc', 'a', 'x')";
        let memory = MemoryBudget::new(1 << 20);
        let prepared = prepare_in(sql, &ScalarCatalog, &PrepareOptions::default(), &memory).unwrap();
        let untracked = prepare(sql, &ScalarCatalog, &PrepareOptions::default()).unwrap();
        assert_eq!(serde_json::to_string(&prepared).unwrap(), serde_json::to_string(&untracked).unwrap());
        let retained = memory.used_bytes();
        assert!(retained > 0);
        let clone = prepared.clone(); drop(prepared);
        assert_eq!(memory.used_bytes(), retained);
        drop(clone);
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn source_without_raw_metadata_and_noncontrol_probes_do_not_acquire_an_eager_scope() {
        let memory = MemoryBudget::new(0);
        assert!(prepare_in("SELECT 1", &ScalarCatalog, &PrepareOptions::default(), &memory).is_ok());
        assert!(parse_control_in("SELECT regexp_replace('x','x','y')", &PrepareOptions::default(), &memory).unwrap().is_none());
        assert_eq!(memory.peak_bytes(), 0);
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn original_literal_checks_precede_copy_admission() {
        let memory = MemoryBudget::new(0);
        for tail in ["9223372036854775808", "1, -9223372036854775809"] {
            let sql = format!("SELECT regexp_replace('x', 'x', 'y', {tail})");
            let error = prepare_in(&sql, &ScalarCatalog, &PrepareOptions::default(), &memory).unwrap_err();
            assert_eq!(error.message.as_deref(), Some("REGEXP_REPLACE integer literal exceeds signed 64-bit range"));
        }
        let error = prepare_in("SELECT regexp_replace('x', 9223372036854775808, 'y')", &ScalarCatalog, &PrepareOptions::default(), &memory).unwrap_err();
        assert_eq!(error.message.as_deref(), Some("REGEXP_REPLACE requires literal pattern, replacement, start, count and flags"));
        assert_eq!(memory.peak_bytes(), 0);
    }
    #[test]
    fn failed_native_regex_copies_release_admitted_parts() {
        let memory = MemoryBudget::new(0);
        let error = prepare_in("SELECT regexp_replace('x', 'x', 'y')", &ScalarCatalog, &PrepareOptions::default(), &memory).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), 0);
        let baseline = MemoryBudget::new(1 << 20);
        let prepared = prepare_in("SELECT regexp_replace('x', 'x', 'y')", &ScalarCatalog, &PrepareOptions::default(), &baseline).unwrap();
        let limit = baseline.peak_bytes();
        drop(prepared);
        let memory = MemoryBudget::new(limit);
        let sql = format!("SELECT regexp_replace('x', 'x', '{}')", "y".repeat(limit + 1));
        let error = prepare_in(&sql, &ScalarCatalog, &PrepareOptions::default(), &memory).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert!(memory.peak_bytes() > 0);
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn native_control_expressions_keep_raw_leases_until_the_control_value_drops() {
        let memory = MemoryBudget::new(1 << 20);
        let control = parse_control_in("SET TIME ZONE regexp_replace('UTC','UTC','UTC')", &PrepareOptions::default(), &memory).unwrap().unwrap();
        let retained = memory.used_bytes(); assert!(retained > 0);
        let clone = control.clone(); drop(control);
        assert_eq!(memory.used_bytes(), retained);
        drop(clone);
        assert_eq!(memory.used_bytes(), 0);
    }
}
