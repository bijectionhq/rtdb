use std::{collections::BTreeMap, sync::Arc};
use rtdb_plan::{Relation, RelationNode, Statement};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};

fn catalog() -> CatalogSnapshot {
    CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) })]), false).unwrap()
}
fn directions(relation: &Relation, output: &mut Vec<(bool, bool)>) {
    match relation.node() {
        RelationNode::Sort { order_by, .. } | RelationNode::Window { order_by, .. } => output.extend(order_by.iter().map(|order| (order.descending, order.nulls_first))),
        RelationNode::Aggregate { aggregates, .. } => for aggregate in aggregates { output.extend(aggregate.order_by.iter().map(|order| (order.descending, order.nulls_first))); },
        _ => (),
    }
    for child in relation.children() { directions(child, output); }
}

#[test]
fn sqlparser_sort_enum_preserves_directions_and_null_order_at_every_binding_site() {
    for sql in [
        "SELECT n FROM (VALUES (1),(2)) t(n) ORDER BY n {order}",
        "VALUES (1),(2) ORDER BY 1 {order}",
        "VALUES (1),(2) ORDER BY column1 {order}",
        "SELECT ARRAY_AGG(n ORDER BY n {order}) FROM (VALUES (1),(2)) t(n)",
        "SELECT ROW_NUMBER() OVER (ORDER BY n {order}) FROM (VALUES (1),(2)) t(n)",
    ] {
        for (order, expected) in [("", (false, false)), ("ASC", (false, false)), ("DESC", (true, true)), ("ASC NULLS FIRST", (false, true)), ("DESC NULLS LAST", (true, false))] {
            let sql = sql.replace("{order}", order);
            let Statement::Query(relation) = prepare(&sql, &catalog(), &PrepareOptions::default()).unwrap().statement else { panic!("expected query") };
            let mut actual = Vec::new(); directions(&relation, &mut actual);
            assert_eq!(actual, [expected], "{sql}");
        }
    }
}

#[test]
fn order_by_using_is_rejected_for_queries_windows_arrays_and_percentiles() {
    for sql in [
        "SELECT n FROM (VALUES (1),(2)) t(n) ORDER BY n USING <",
        "VALUES (1),(2) ORDER BY 1 USING <",
        "SELECT ARRAY_AGG(n ORDER BY n USING <) FROM (VALUES (1),(2)) t(n)",
        "SELECT ROW_NUMBER() OVER (ORDER BY n USING <) FROM (VALUES (1),(2)) t(n)",
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n USING <) FROM (VALUES (1),(2)) t(n)",
    ] { assert!(prepare(sql, &catalog(), &PrepareOptions::default()).is_err(), "{sql}"); }
}
