use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{BinaryOp, Binder, ColumnId, Expr, Relation, RelationNode, Schema, SortExpr, WindowExpr, WindowFunction};
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

fn bounded_pages(pages: Vec<Vec<(i64, V, V)>>, descending: bool, with_lead: bool) -> Arc<Relation> {
    let mut binder = Binder::default();
    let id = binder.field("id", None, T::Scalar(S::I64)).unwrap();
    let group = binder.field("region", None, T::Scalar(S::Utf8).maybe()).unwrap();
    let balance = binder.field("balance", None, T::Scalar(S::F64).maybe()).unwrap();
    let order = SortExpr { expr: balance.expression(), descending, nulls_first: descending };
    let id_column = id.id;
    let schema = Schema::new(vec![id, group.clone(), balance]).unwrap();
    let pages = pages.into_iter().map(|page| page.into_iter().map(|(id, group, key)| {
        [V::I64(id), group, key].into_iter().map(Expr::Literal).collect()
    }).collect()).collect();
    let input = Arc::new(Relation::new(schema.clone(), RelationNode::LiteralPages { pages }).unwrap());
    let field = binder.field("rn", None, T::Scalar(S::I64)).unwrap();
    let predicate = Expr::binary(BinaryOp::LtEq, field.expression(), Expr::Literal(V::I64(2)));
    let mut expressions = vec![WindowExpr { field, function: WindowFunction::RowNumber }];
    if with_lead {
        let field = binder.field("next_id", None, T::Scalar(S::I64).maybe()).unwrap();
        expressions.push(WindowExpr { field, function: WindowFunction::Lead { column: id_column, offset: 1 } });
    }
    let schema = schema.concat(&Schema::new(expressions.iter().map(|expr| expr.field.clone()).collect()).unwrap()).unwrap();
    let window = Arc::new(Relation::new(schema, RelationNode::Window {
        input, partition_by: vec![group.id], order_by: vec![order],
        expressions,
    }).unwrap());
    Arc::new(window.filter(predicate).unwrap())
}

#[tokio::test]
async fn bounded_row_number_keeps_cross_page_peers_signed_zero_and_nan_in_sort_order() {
    let text = |text: &str| V::Utf8(text.into());
    let pages = vec![
        vec![(1, text("z"), V::F64(0.0)), (2, V::Null, V::F64(f64::NAN)), (3, text("a"), V::F64(7.0))],
        vec![(4, text("z"), V::F64(-0.0)), (5, text("z"), V::F64(-0.0)), (6, text("z"), V::F64(-0.0)),
            (7, V::Null, V::Null), (8, text("a"), V::Null)],
        vec![(9, V::Null, V::F64(f64::INFINITY)), (10, V::Null, V::F64(-f64::NAN)), (11, text("z"), V::F64(f64::NAN))],
    ];
    for (descending, ids) in [(false, [4, 5, 9, 2, 3, 8]), (true, [11, 1, 7, 2, 8, 3])] {
        for top_k_rows in [0, 2] {
            let mut context = context(0);
            let options = &mut Arc::get_mut(&mut context).unwrap().options;
            options.sort_memory_bytes = 1 << 20;
            options.top_k_rows = top_k_rows;
            let prepared = PreparedQuery::new(bounded_pages(pages.clone(), descending, false)).unwrap();
            let mut cursor = prepared.execute(Arc::clone(&context), vec![]).await.unwrap();
            let mut rows = Vec::new();
            while let Some(page) = cursor.next_batch().await.unwrap() {
                let batch = page.batch();
                let row = |column: usize| read_value(batch.schema().field(column), batch.column(column).as_ref(), 0).unwrap();
                rows.push((row(0), row(3)));
            }
            assert_eq!(rows, ids.into_iter().zip([1, 2, 1, 2, 1, 2])
                .map(|(id, rank)| (V::I64(id), V::I64(rank))).collect::<Vec<_>>());
            drop(cursor);
            assert_eq!(context.memory.used_bytes(), 0);
        }
    }
}

#[tokio::test]
async fn bounded_row_number_replays_all_input_on_an_incompatible_late_key() {
    let mut context = context(0);
    Arc::get_mut(&mut context).unwrap().options.sort_memory_bytes = 1 << 20;
    let group = V::Utf8("z".into());
    let pages = vec![
        vec![(1, group.clone(), V::F64(3.0)), (2, group.clone(), V::F64(2.0)), (3, group.clone(), V::F64(1.0))],
        vec![(4, group, V::Utf8("incompatible".into()))],
    ];
    let prepared = PreparedQuery::new(bounded_pages(pages, false, false)).unwrap();
    let mut cursor = prepared.execute(Arc::clone(&context), vec![]).await.unwrap();
    assert_eq!(cursor.next_batch().await.err().unwrap().code, "xtdb.expression/function-type-mismatch");
    drop(cursor);
    assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn bounded_row_number_retains_all_input_for_lead_in_the_same_window() {
    let mut context = context(0);
    Arc::get_mut(&mut context).unwrap().options.sort_memory_bytes = 1 << 20;
    let pages = vec![(1..=3).map(|id| (id, V::Utf8("z".into()), V::F64(id as f64))).collect()];
    let prepared = PreparedQuery::new(bounded_pages(pages, false, true)).unwrap();
    let mut cursor = prepared.execute(Arc::clone(&context), vec![]).await.unwrap();
    for next in [2, 3] {
        let page = cursor.next_batch().await.unwrap().unwrap();
        let batch = page.batch();
        assert_eq!(read_value(batch.schema().field(4), batch.column(4).as_ref(), 0).unwrap(), V::I64(next));
    }
    assert!(cursor.next_batch().await.unwrap().is_none());
    drop(cursor);
    assert_eq!(context.memory.used_bytes(), 0);
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
