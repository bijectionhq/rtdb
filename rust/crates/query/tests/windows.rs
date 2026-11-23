use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{Binder, ColumnId, Expr, Relation, RelationNode, Schema, SortExpr, WindowExpr, WindowFunction};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType as T, ScalarType as S, Timestamp, Value as V};
use tokio_util::sync::CancellationToken;

fn context(spill_limit_bytes: u64) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: Vec::new(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 24), Timestamp(0), "UTC", QueryOptions { batch_rows: 1, sort_memory_bytes: 1, spill_limit_bytes, ..Default::default() }, &CancellationToken::new()).unwrap()
}

fn relation(rows: Vec<Vec<V>>, partitioned: bool) -> Arc<Relation> {
    let mut binder = Binder::default();
    let key = binder.field("key", None, T::join_all(&rows.iter().map(|row| row[0].logical_type()).collect::<Vec<_>>())).unwrap();
    let value = binder.field("value", None, T::Scalar(S::I64)).unwrap();
    let input = Arc::new(Relation::new(Schema::new(vec![key.clone(),value.clone()]).unwrap(), RelationNode::Values { rows: rows.into_iter().map(|row| row.into_iter().map(Expr::Literal).collect()).collect() }).unwrap());
    let expressions = [WindowFunction::RowNumber,WindowFunction::Lead { column:value.id,offset:1 },WindowFunction::Lag { column:value.id,offset:1 }].into_iter().enumerate().map(|(index,function)| {
        let field = binder.field(format!("window_{index}"),None,function.result_type(input.schema()).unwrap()).unwrap();
        WindowExpr { field,function }
    }).collect::<Vec<_>>();
    let schema = input.schema().concat(&Schema::new(expressions.iter().map(|expression| expression.field.clone()).collect()).unwrap()).unwrap();
    Arc::new(Relation::new(schema, RelationNode::Window { input, partition_by: if partitioned { vec![key.id] } else { Vec::new() }, order_by:vec![SortExpr { expr:value.expression(),descending:false,nulls_first:false }],expressions }).unwrap())
}

#[tokio::test]
async fn multiple_window_outputs_share_sorted_input_with_nonreflexive_group_keys() {
    let null_list = V::list(vec![V::Null]);
    let rows = vec![vec![V::Null,V::I64(3)],vec![null_list.clone(),V::I64(5)],vec![V::Null,V::I64(1)],vec![null_list.clone(),V::I64(2)]];
    let context = context(1 << 24);
    let mut cursor = PreparedQuery::new(relation(rows,true)).unwrap().execute(Arc::clone(&context),vec![]).await.unwrap();
    let mut actual = Vec::new();
    while let Some(page) = cursor.next_batch().await.unwrap() {
        let batch = page.batch();
        actual.push(batch.schema().fields().iter().zip(batch.columns()).map(|(field,array)| read_value(field,array.as_ref(),0).unwrap()).collect::<Vec<_>>());
    }
    assert_eq!(actual, [
        vec![V::Null,V::I64(1),V::I64(1),V::I64(3),V::Null],
        vec![V::Null,V::I64(3),V::I64(2),V::Null,V::I64(1)],
        vec![null_list.clone(),V::I64(5),V::I64(1),V::Null,V::Null],
        vec![null_list,V::I64(2),V::I64(1),V::Null,V::Null],
    ]);
    assert!(context.spill_peak_bytes() > 0);
    drop(cursor);
    assert_eq!(context.spill_bytes(),0);
    assert_eq!(context.memory.used_bytes(),0);
}

#[tokio::test]
async fn cancelling_or_dropping_window_cursor_releases_spills_while_output_page_stays_owned() {
    for cancel in [false,true] {
        let context = context(1 << 24);
        let prepared = PreparedQuery::new(relation((0..32).rev().map(|n| vec![V::I64(0),V::I64(n)]).collect(),false)).unwrap();
        let mut cursor = prepared.execute(Arc::clone(&context),vec![]).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert!(context.spill_peak_bytes() > 0);
        assert_eq!(context.spill_bytes(),0);
        if cancel { cursor.cancel(); assert!(cursor.next_batch().await.is_err()); }
        drop(cursor);
        assert!(!context.cancellation.is_cancelled());
        assert_eq!(context.spill_bytes(),0);
        assert!(context.memory.used_bytes() > 0);
        assert_eq!(read_value(&page.batch().schema().fields()[2],page.batch().column(2).as_ref(),0).unwrap(),V::I64(1));
        drop(page);
        assert_eq!(context.memory.used_bytes(),0);
    }
}

#[tokio::test]
async fn window_spill_failure_releases_partial_state() {
    let context = context(1);
    let mut cursor = PreparedQuery::new(relation(vec![vec![V::I64(0),V::I64(1)]],false)).unwrap().execute(Arc::clone(&context),vec![]).await.unwrap();
    assert!(cursor.next_batch().await.is_err());
    drop(cursor);
    assert_eq!(context.spill_bytes(),0);
    assert_eq!(context.memory.used_bytes(),0);
}

#[test]
fn window_plan_validates_partition_columns_nullable_results_and_offset_range() {
    let valid = relation(vec![vec![V::I64(0),V::I64(1)]],true);
    let RelationNode::Window { input,partition_by,order_by,expressions } = valid.node() else { panic!("Expected window") };
    assert!(Relation::new(valid.schema().clone(),RelationNode::Window { input:Arc::clone(input),partition_by:vec![ColumnId(u32::MAX)],order_by:order_by.clone(),expressions:expressions.clone() }).is_err());
    let mut wrong = expressions.clone();
    wrong[1].field.data_type = T::Scalar(S::I64);
    let schema = input.schema().concat(&Schema::new(wrong.iter().map(|expression| expression.field.clone()).collect()).unwrap()).unwrap();
    assert!(Relation::new(schema,RelationNode::Window { input:Arc::clone(input),partition_by:partition_by.clone(),order_by:order_by.clone(),expressions:wrong }).is_err());
    assert!(WindowFunction::Lag { column:input.schema().fields()[1].id,offset:u64::MAX }.result_type(input.schema()).is_err());
    assert!(WindowFunction::Lead { column:ColumnId(u32::MAX),offset:1 }.result_type(input.schema()).is_err());
    let encoded = serde_json::to_vec(valid.as_ref()).unwrap();
    assert_eq!(serde_json::from_slice::<Relation>(&encoded).unwrap(),*valid);
}
