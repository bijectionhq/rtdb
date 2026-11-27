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
mod planning_diagnostic;
mod literal_syntax;
mod json_access;
mod materialized;
mod wildcard;
mod returning;
mod lexer;
mod parser;
mod batch;

pub use parse::{DeferredStatementSyntax, ParsedStatement, StatementKind, StatementSyntax, StatementSyntaxBatch, parse, parse_in, parse_syntax_in};
pub use mutation::{Mutation, StaticMutation, StaticMutationKind, prepare_tx, prepare_tx_in, prepare_patch, prepare_static, prepare_static_in};
pub use control::{AdmittedControl, ParsedControl, parse_admitted_control_in, parse_control, parse_control_in, parse_control_syntax_in};
pub use parser::{ContextualStatementDiagnostic, StatementSyntaxDiagnostic, batch_syntax_diagnostic, statement_syntax_diagnostic};
pub use split::{SplitStatements, split_statements, split_statements_in};
pub use batch::{BatchExecution, BatchPrepare, BatchStatement, ExecutableSyntax, PrepareSyntax, StatementBatch, parse_statement_batch, parse_statement_batch_with_frontend_response};
pub use batch::{ExtendedParse, ExtendedStatement, SqlPrepareChild, SqlPrepareSyntax, parse_extended_statement};

use rtdb_plan::{Catalog, PreparedStatement};
use rtdb_types::{LogicalType, MemoryBudget, Result};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Debug, PartialEq)]
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

/// Estimates temporary SQL lexer, grammar and binding storage in bytes.
/// Returns `None` if the estimate exceeds `usize`.
pub fn statement_syntax_estimate(source_bytes: usize) -> Option<usize> {
    source_bytes.checked_mul(128).and_then(|bytes| bytes.checked_add(8192))
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
    statements.remove(0).bind(catalog, options)
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
    fn parsed_statement_binds_with_its_source_and_raw_metadata_budget() {
        let source = "SELECT regexp_replace('abc', 'a', 'x')";
        let budget = MemoryBudget::new(1 << 20);
        let statement = parse_in(source, &budget).unwrap().remove(0);
        let prepared = statement.bind(&ScalarCatalog, &PrepareOptions::default()).unwrap();
        assert_eq!(prepared.source, source);
        assert!(budget.used_bytes() > 0);
        drop(prepared);
        assert_eq!(budget.used_bytes(), 0);

        let budget = MemoryBudget::new(0);
        let statement = parse_in(source, &budget).unwrap().remove(0);
        let error = statement.bind(&ScalarCatalog, &PrepareOptions::default()).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn borrowed_inline_syntax_binds_raw_metadata_into_the_destination_budget() {
        let source = r#"XTQL $$(-> (rel [{}] []) (return {:v (regexp-replace "abc" "a" "x")}))$$"#;
        let syntax_budget = MemoryBudget::new(1 << 20);
        let parsed = parse_in(source, &syntax_budget).unwrap().remove(0);
        let syntax_bytes = syntax_budget.used_bytes();
        assert!(syntax_bytes > 0);

        let options = PrepareOptions::default();
        let denied = MemoryBudget::new(0);
        let error = parsed.bind_in(&ScalarCatalog, &options, &denied).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(denied.used_bytes(), 0);
        assert_eq!(syntax_budget.used_bytes(), syntax_bytes);

        let plan_budget = MemoryBudget::new(1 << 20);
        let prepared = parsed.bind_in(&ScalarCatalog, &options, &plan_budget).unwrap();
        assert_eq!(prepared.source, source);
        assert!(plan_budget.used_bytes() > 0);
        assert_eq!(syntax_budget.used_bytes(), syntax_bytes);
        drop(parsed);
        assert_eq!(syntax_budget.used_bytes(), 0);
        drop(prepared);
        assert_eq!(plan_budget.used_bytes(), 0);
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
    fn control_syntax_completes_before_expression_admission() {
        let source = "SET TIME ZONE REGEXP_REPLACE('UTC', 'U', 'U')";
        let budget = MemoryBudget::new(0);
        let parsed = parse_control_syntax_in(source, &budget).unwrap().unwrap();
        assert_eq!(budget.peak_bytes(), 0);
        let error = parsed.bind(&PrepareOptions::default()).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(budget.used_bytes(), 0);

        let trailing = format!("{source} TRAILING");
        let error = parse_control_in(&trailing, &PrepareOptions::default(), &budget).unwrap_err();
        assert_eq!(error.code, "xtdb/sql-error");
        assert_eq!(budget.peak_bytes(), 0);
    }

    #[test]
    fn control_syntax_checks_literal_positions_and_leaves_clock_values_to_binding() {
        let budget = MemoryBudget::new(1 << 20);
        for source in [
            "BEGIN READ WRITE, ASYNC ?, ASYNC TRUE",
            "BEGIN READ WRITE, ASYNC BOOL 'true', ASYNC TRUE",
            "SET application_name = ?",
            "SET application_name = CAST('x' AS VARCHAR)",
            "SET application_name = VARCHAR 'x'",
            "SET application_name = TIMESTAMP(3) '2020-01-01'",
            "SET application_name = TIME WITH TIME ZONE '00:00:00Z'",
        ] {
            let Err(error) = parse_control_syntax_in(source, &budget) else { panic!("accepted {source}"); };
            assert_eq!(error.code, "xtdb/sql-error", "{source}");
        }
        let source = "BEGIN READ ONLY WITH (CLOCK_TIME TIMESTAMP 'bad')";
        let parsed = parse_control_syntax_in(source, &budget).unwrap().unwrap();
        let error = parsed.bind(&PrepareOptions::default()).unwrap_err();
        assert_eq!(error.code, "xtdb/sql-error");
        assert!(error.message.unwrap().starts_with("Cannot parse timestamp: bad"));

        let source = "SET application_name = DURATION '1 second'";
        let parsed = parse_control_syntax_in(source, &budget).unwrap().unwrap();
        let error = parsed.bind(&PrepareOptions::default()).unwrap_err();
        assert_eq!(error.code, "xtdb/sql-error");
        assert_eq!(error.message.as_deref(), Some("Cannot parse duration: 1 second - failed with message Text cannot be parsed to a Duration"));
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn control_literal_syntax_accepts_xtdb_typed_strings() {
        let budget = MemoryBudget::new(0);
        for literal in [
            "DATE '2020-01-01'",
            "TIME '00:00:00'",
            "TIMESTAMP '2020-01-01T00:00:00'",
            "TIMESTAMP WITH TIME ZONE '2020-01-01T00:00:00Z'",
            "TIMESTAMP WITHOUT TIME ZONE '2020-01-01T00:00:00'",
            "DURATION 'PT1S'",
            "UUID '01234567-89ab-cdef-0123-456789abcdef'",
            "URI 'https://example.com'",
            "KEYWORD 'example'",
        ] {
            let source = format!("SET application_name = {literal}");
            assert!(parse_control_syntax_in(&source, &budget).unwrap_or_else(|error| panic!("{source}: {error:?}")).is_some());
        }
        assert_eq!(budget.peak_bytes(), 0);
    }

    #[test]
    fn transaction_options_bind_final_occurrences_in_option_order() {
        let budget = MemoryBudget::new(1 << 20);
        let source = "BEGIN READ ONLY WITH (CLOCK_TIME TIMESTAMP 'bad', CLOCK_TIME TIMESTAMP '2020-01-01T00:00:00Z')";
        let parsed = parse_control_syntax_in(source, &budget).unwrap().unwrap();
        let command = parsed.bind(&PrepareOptions::default()).unwrap();
        let rtdb_plan::ControlStatement::Begin(options) = command else { panic!("begin command"); };
        let Some(rtdb_plan::ControlAccessOptions::ReadOnly(options)) = options.access else { panic!("read-only options"); };
        assert_eq!(options.clock_time, Some(rtdb_plan::ControlValue::TimestampLiteral("2020-01-01T00:00:00Z".into())));

        for (source, first_error) in [
            ("BEGIN READ WRITE, TIMEZONE first_unknown(), METADATA second_unknown()", "first_unknown"),
            ("BEGIN READ ONLY, CLOCK_TIME TIMESTAMP 'bad', TIME ZONE first_unknown()", "first_unknown"),
        ] {
            let parsed = parse_control_syntax_in(source, &budget).unwrap().unwrap();
            let error = parsed.bind(&PrepareOptions::default()).unwrap_err();
            assert!(error.message.unwrap().contains(first_error), "{source}");
        }
        assert_eq!(budget.used_bytes(), 0);
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
