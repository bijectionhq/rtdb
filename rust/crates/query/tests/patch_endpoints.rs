use std::{collections::BTreeMap,sync::Arc};
use rtdb_columnar::{MemoryBudget,read_value};
use rtdb_plan::{ColumnId,Expr,Field,Relation,RelationNode,Schema};
use rtdb_query::{CatalogSnapshot,DatabaseSnapshot,PreparedQuery,QueryContext,QueryOptions};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{CivilDate,Instant,Keyword,LogicalType as L,Result,ScalarType as S,TimeUnit as U,Timestamp,Value as V};
use tokio_util::sync::CancellationToken;

fn timestamp(value: i64) -> V { V::Timestamp { value,unit: U::Microsecond,timezone: Some("UTC".into()) } }
fn endpoint(value: i64) -> Expr { Expr::Literal(timestamp(value)) }
fn relation(from: Expr,to: Expr,empty: bool) -> Arc<Relation> {
    let mut fields = [L::Scalar(S::FixedSizeBinary(16)),timestamp(0).logical_type(),timestamp(0).logical_type().maybe(),L::Scalar(S::I64)].into_iter().enumerate().map(|(index,data_type)|Field { id: ColumnId(index as u32),name: format!("c{index}"),qualifier: None,qualified_only: false,unnamed: false,data_type }).collect::<Vec<_>>();
    let rows = if empty { vec![] } else { vec![vec![V::FixedSizeBinary(vec![1;16]),timestamp(i64::MIN),V::Null,V::I64(7)].into_iter().map(Expr::Literal).collect()] };
    let input = Arc::new(Relation::new(Schema::new(fields.clone()).unwrap(),RelationNode::Values { rows }).unwrap());
    fields[3].data_type = fields[3].data_type.clone().maybe();
    Arc::new(Relation::new(Schema::new(fields).unwrap(),RelationNode::PatchGaps { input,iid: ColumnId(0),valid_from: ColumnId(1),valid_to: ColumnId(2),document: ColumnId(3),from,to }).unwrap())
}
async fn run(from: Expr,to: Expr,arguments: Vec<V>,clock: Instant,zone: &str,empty: bool) -> Result<Vec<Vec<V>>> {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false)?;
    let context = QueryContext::new(catalog,MemoryBudget::new(1 << 27),clock,zone,QueryOptions::default(),&CancellationToken::new())?;
    let result = async {
        let mut cursor = PreparedQuery::new(relation(from,to,empty))?.execute(Arc::clone(&context),arguments).await?;
        let mut rows = Vec::new();
        while let Some(batch) = cursor.next_batch().await? {
            for row in 0..batch.batch().num_rows() { rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field,array)|read_value(field,array.as_ref(),row)).collect::<Result<_>>()?); }
        }
        Ok(rows)
    }.await;
    assert_eq!(context.memory.used_bytes(),0);
    result
}
fn parameter(index: usize) -> Expr { Expr::Parameter { index,data_type: L::Nothing } }

#[tokio::test]
async fn omitted_and_now_patch_endpoints_use_captured_nanoseconds_before_micro_narrowing() {
    let clock = Instant::new(-1,999_999_999).unwrap();
    for from in [Expr::Literal(V::Null),Expr::Literal(V::Keyword(Keyword::from_name("now"))),Expr::Call { name: "current_timestamp".into(),args: vec![],return_type: timestamp(0).logical_type() }] {
        let rows = run(from,Expr::Literal(V::Null),vec![],clock,"UTC",false).await.unwrap();
        assert_eq!(rows[0][1],timestamp(-1));assert_eq!(rows[0][2],V::Null);
    }
    let rows = run(endpoint(-10),Expr::Call { name: "current-timestamp".into(),args: vec![],return_type: timestamp(0).logical_type() },vec![],clock,"UTC",false).await.unwrap();
    assert_eq!(rows[0][2],timestamp(-1));
}

#[tokio::test]
async fn patch_parameters_use_object_time_conversion_and_do_not_default_null_values() {
    let rows = run(parameter(0),parameter(1),vec![V::Utf8("1970-01-01T12:00:00".into()),V::Timestamp { value: 1_999,unit: U::Nanosecond,timezone: Some("UTC".into()) }],Instant::default(),"UTC",false).await.unwrap();
    assert_eq!(rows[0][1],timestamp(0));assert_eq!(rows[0][2],timestamp(1));
    for value in [V::Null,V::Bool(false),V::I64(0),V::Time { value: 0,unit: U::Microsecond,bit_width: 64 }] {
        assert!(run(parameter(0),Expr::Literal(V::Null),vec![value.clone()],Instant::default(),"UTC",true).await.is_err());
        assert!(run(endpoint(0),parameter(0),vec![value],Instant::default(),"UTC",true).await.is_err());
    }
}

#[tokio::test]
async fn patch_dates_resolve_midnight_through_zone_gaps_and_reject_only_inverted_bounds() {
    let date = CivilDate::parse_iso("1919-03-31").unwrap();
    let expected = rtdb_types::parse_sql_timestamp_instant("1919-03-31T05:00Z","UTC").unwrap().as_micros().unwrap();
    let rows = run(Expr::Literal(V::DateDay(date.epoch_day() as i32)),Expr::Literal(V::Null),vec![],Instant::default(),"America/Toronto",false).await.unwrap();
    assert_eq!(rows[0][1],timestamp(expected.0));
    assert!(run(endpoint(0),endpoint(0),vec![],Timestamp(0).into(),"UTC",true).await.unwrap().is_empty());
    let error = run(endpoint(1),endpoint(0),vec![],Instant::default(),"UTC",true).await.unwrap_err();
    assert_eq!(error.code,"xtdb.indexer/invalid-valid-times");
    for (key,text) in [("valid-from","1970-01-01T00:00:00.000001Z"),("valid-to","1970-01-01T00:00:00Z")] {
        assert_eq!(rtdb_format::transit::from_json_value(&error.context[key]).unwrap(),rtdb_format::transit::TransitValue::Tagged { tag: "time/instant".into(),representation: Box::new(rtdb_format::transit::TransitValue::String(text.into())) });
    }
}
