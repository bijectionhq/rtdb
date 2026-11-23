use super::*;
use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{ColumnId, Expr, Field, Schema};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{LogicalType, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryContext,QueryOptions,context::OuterRows,parameters::BoundParameters};

fn schema(id: u32) -> Schema { Schema::new(vec![Field { id: ColumnId(id),name: format!("c{id}"),qualifier: None,qualified_only: false,unnamed: false,data_type: LogicalType::Scalar(ScalarType::I64) }]).unwrap() }

#[test]
fn extension_retains_the_input_cardinality_used_for_outer_join_build_selection() {
    let database = DatabaseSnapshot {
        partitions: vec![],
        storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
    };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let context = QueryContext::new(
        catalog, MemoryBudget::new(1 << 25), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new(),
    ).unwrap();
    let execution = Execution {
        parameters: Arc::new(BoundParameters::infer(vec![], &context.memory).unwrap()),
        outer: OuterRows::empty(&context.memory).unwrap(), context, bindings: None, analysis: None,
    };
    let left = Arc::new(Relation::unit());
    let right = Relation::new(Schema::default(), RelationNode::Values { rows: vec![vec![], vec![]] }).unwrap();
    let extended = Arc::clone(&left).extend(vec![rtdb_plan::NamedExpr {
        field: schema(0).fields()[0].clone(), expr: Expr::Literal(Value::I64(1)),
    }]).unwrap();
    assert!(build_left(&left, &right, JoinKind::Left, &execution).unwrap());
    assert!(build_left(&extended, &right, JoinKind::Left, &execution).unwrap());
}
