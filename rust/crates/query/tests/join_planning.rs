use std::{collections::BTreeMap, sync::Arc};

use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, JoinKind, PreparedStatement, Relation, RelationNode, Schema, Statement};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn input(id: u32, rows: impl IntoIterator<Item = (i64, i64)>) -> Arc<Relation> {
    let fields = [(id, "key"), (id + 10, "tag")].map(|(id, name)| Field {
        id: ColumnId(id), name: format!("{name}_{id}"), qualifier: None,
        qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64),
    });
    let schema = Schema::new(fields.to_vec()).unwrap();
    let rows = rows.into_iter().map(|(key, tag)| vec![Expr::Literal(Value::I64(key)), Expr::Literal(Value::I64(tag))]).collect();
    Arc::new(Relation::new(schema, RelationNode::Values { rows }).unwrap())
}

#[tokio::test]
async fn a_smaller_late_build_keeps_payload_columns_and_avoids_spilling_the_intermediate() {
    let a = input(0, [(1, 101)]);
    let b = input(1, (0..12).map(|index| (1 + index % 2, 200 + index)));
    let c = input(2, [(1, 301), (2, 302)]);
    let a_b = Expr::binary(BinaryOp::Eq, a.schema().fields()[0].expression(), b.schema().fields()[0].expression());
    let b_c = Expr::binary(BinaryOp::Eq, b.schema().fields()[0].expression(), c.schema().fields()[0].expression());
    let first = Arc::new(Relation::join(a, b, JoinKind::Inner, Some(a_b)).unwrap());
    let relation = Arc::new(Relation::join(first, c, JoinKind::Inner, Some(b_c)).unwrap());

    for threshold in [2, 0] {
        let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let memory = MemoryBudget::new(1 << 26);
        let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(Arc::clone(&memory), 32, true, false).unwrap();
        let context = QueryContext::new(catalog, memory, Timestamp(0), "UTC",
            QueryOptions { join_build_rows: threshold, ..Default::default() }, &CancellationToken::new()).unwrap()
            .with_tracer(Some(tracer.clone()), None);
        let statement = PreparedStatement { statement: Statement::Query(Arc::clone(&relation)), parameter_types: BTreeMap::new(),
            source: String::new(), notices: vec![], settings: Default::default() };
        let prepared = PreparedQuery::from_prepared_in(statement, &context.catalog, &context.memory).unwrap();
        assert_eq!(prepared.schema(), relation.schema());
        assert_eq!(prepared.relation().explain_json().unwrap(), relation.explain_json().unwrap());

        let mut rows = Vec::new();
        let mut cursor = prepared.execute(Arc::clone(&context), vec![]).await.unwrap();
        while let Some(page) = cursor.next_batch().await.unwrap() {
            let batch = page.batch();
            for row in 0..batch.num_rows() {
                let values: [i64; 6] = std::array::from_fn(|column| {
                    let value = read_value(batch.schema().field(column), batch.column(column).as_ref(), row).unwrap();
                    let Value::I64(value) = value else { panic!("Expected an I64 payload"); };
                    value
                });
                rows.push(values);
            }
        }
        drop(cursor);
        tracer.close();
        rows.sort_unstable();
        assert_eq!(rows, (0..12).step_by(2).map(|offset| [1, 101, 1, 200 + offset, 1, 301]).collect::<Vec<_>>());
        assert_eq!(context.spill_peak_bytes() > 0, threshold == 0);
        assert_eq!(context.spill_bytes(), 0);

        let mut saw_late_build = false;
        while let Some(span) = receiver.next().await {
            if span.name != "query.cursor.inner-join" { continue; }
            let attributes: BTreeMap<_, _> = span.attributes().collect();
            if attributes.get("cursor.join.build_side") == Some(&"right") {
                assert_eq!(attributes["cursor.join.build_reason"], "smaller-input");
                assert_eq!(attributes["cursor.join.estimated_build_rows"], "2");
                assert_eq!(attributes["cursor.join.estimated_probe_rows"], "6");
                assert_eq!(attributes["cursor.row_count"], "6");
                saw_late_build = true;
            }
        }
        assert!(saw_late_build);
    }
}
