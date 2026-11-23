use rtdb_types::StructFields;
use super::*;
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{BinaryOp,ColumnId,Field,JoinKind,RegexForm,RegexReplacement};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{LogicalType,ScalarType,Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryOptions,context::OuterRows,parameters::BoundParameters,runtime_schema::EmittedRelation};

fn execution(batch_rows: usize,threshold: i64) -> Execution {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    let context = QueryContext::new(catalog,MemoryBudget::new(1 << 27),Timestamp(0),"UTC",QueryOptions { batch_rows,join_build_rows: threshold,..Default::default() },&CancellationToken::new()).unwrap();
    Execution { parameters: Arc::new(BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),context,bindings: None,analysis: None }
}
fn field(id: u32,ty: LogicalType) -> Field { Field { id: ColumnId(id),name: format!("c{id}"),qualifier: None,qualified_only: false,unnamed: false,data_type: ty } }
fn table(id: u32,values: &[Value]) -> Arc<Relation> {
    let ty = values.iter().map(Value::logical_type).fold(LogicalType::Nothing,|left,right|left.join(&right));
    Arc::new(Relation::new(Schema::new(vec![field(id,ty)]).unwrap(),RelationNode::Values { rows: values.iter().cloned().map(|value|vec![Expr::Literal(value)]).collect() }).unwrap())
}
fn numbers(id: u32,values: &[i64]) -> Arc<Relation> { table(id,&values.iter().copied().map(Value::I64).collect::<Vec<_>>()) }
fn union(left: Arc<Relation>,right: Arc<Relation>) -> Arc<Relation> {
    Arc::new(Relation::new(left.schema().clone(),RelationNode::Set { left,right,operator: SetOperator::Union,all: true }).unwrap())
}
fn join(left: Arc<Relation>,right: Arc<Relation>,kind: JoinKind,condition: Option<Expr>) -> Arc<Relation> {
    let schema = match kind {
        JoinKind::Left => left.schema().concat(&right.schema().nullable().unwrap()).unwrap(),
        JoinKind::Right => left.schema().nullable().unwrap().concat(right.schema()).unwrap(),
        JoinKind::Full => left.schema().nullable().unwrap().concat(&right.schema().nullable().unwrap()).unwrap(),
        JoinKind::Semi | JoinKind::Anti => left.schema().clone(),
        _ => left.schema().concat(right.schema()).unwrap(),
    };
    Arc::new(Relation::new(schema,RelationNode::Join { left,right,kind,condition }).unwrap())
}
fn equal(left: &Relation,right: &Relation) -> Expr { Expr::binary(BinaryOp::Eq,left.schema().fields()[0].expression(),right.schema().fields()[0].expression()) }
async fn open(relation: Arc<Relation>,execution: &Execution) -> BoxPageCursor {
    let runtime = EmittedRelation::derive(Arc::clone(&relation),&execution.context.catalog,execution.parameters.types(),&execution.context.memory).unwrap();
    build_pages(Arc::clone(&runtime),execution.clone()).await.unwrap()
}
fn rows(page: OwnedPage) -> Vec<Vec<Value>> { page.rows().iter().map(|row|row.values.clone()).collect() }
async fn collect(mut cursor: BoxPageCursor) -> Vec<Vec<Vec<Value>>> {
    let mut pages = Vec::new();while let Some(page) = cursor.next_page().await.unwrap() { pages.push(rows(page)); }pages
}
fn inverse(column: Expr) -> Expr { Expr::binary(BinaryOp::Divide,Expr::Literal(Value::I64(1)),column) }
fn invalid_pattern() -> Expr {
    Expr::regex_replace(Expr::Literal(Value::Utf8("a".into())),RegexReplacement { pattern: raw_literal(Value::Utf8("[".into())),replacement: raw_literal(Value::Utf8("b".into())),..Default::default() })
}

#[tokio::test]
async fn memory_hash_join_finishes_each_probe_page_before_client_batching() {
    for batch_rows in [1,1024] {
        let execution = execution(batch_rows,100);let baseline = execution.context.memory.used_bytes();
        let relation = join(numbers(0,&[10,11]),union(numbers(1,&[20,21]),numbers(1,&[22])),JoinKind::Inner,None);
        let actual = collect(open(relation,&execution).await).await;
        assert_eq!(actual,[vec![vec![Value::I64(10),Value::I64(20)],vec![Value::I64(11),Value::I64(20)],vec![Value::I64(10),Value::I64(21)],vec![Value::I64(11),Value::I64(21)]],vec![vec![Value::I64(10),Value::I64(22)],vec![Value::I64(11),Value::I64(22)]]]);
        assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);assert_eq!(execution.context.spill_peak_bytes(),0);
    }
}

#[tokio::test]
async fn spill_shuffle_replays_probe_partitions_within_each_build_partition() {
    for batch_rows in [1,1024] {
        let execution = execution(batch_rows,0);let baseline = execution.context.memory.used_bytes();
        let left = union(numbers(0,&[0,1,2,3]),numbers(0,&[4,5,6,7]));
        let right = union(union(numbers(1,&[0,1]),numbers(1,&[2,3])),union(numbers(1,&[4,5]),numbers(1,&[6,7])));
        let relation = join(Arc::clone(&left),Arc::clone(&right),JoinKind::Inner,Some(equal(&left,&right)));
        let actual = collect(open(relation,&execution).await).await;
        let mut expected = Vec::new();
        for build_part in 0..4 {
            for probe_group in 0..2 {
                let selected = (0i64..8).filter(|value|crate::join_storage::hash_values([&Value::I64(*value)]).unwrap() as usize & 7 == probe_group * 4 + build_part).map(|value|vec![Value::I64(value),Value::I64(value)]).collect::<Vec<_>>();
                if !selected.is_empty() { expected.push(selected); }
            }
        }
        assert_eq!(actual,expected);assert!(execution.context.spill_peak_bytes() > 0);
        assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn complete_probe_page_errors_before_limit_can_publish_its_first_match() {
    for threshold in [0,100] {
        let execution = execution(1,threshold);let baseline = execution.context.memory.used_bytes();
        let left = numbers(0,&[1]);let right = numbers(1,&[1,0]);
        let condition = Expr::binary(BinaryOp::Eq,inverse(right.schema().fields()[0].expression()),Expr::Literal(Value::I64(1)));
        let input = join(left,right,JoinKind::Inner,Some(condition));
        let relation = Arc::new(Relation::new(input.schema().clone(),RelationNode::Limit { input,offset: None,fetch: Some(Expr::Literal(Value::I64(1))) }).unwrap());
        let mut cursor = open(relation,&execution).await;
        assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb.expression/division-by-zero");
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn computed_join_keys_finish_first_column_before_preparing_second_column() {
    let execution = execution(1,0);let baseline = execution.context.memory.used_bytes();
    let left = numbers(0,&[1,0]);let right = numbers(1,&[]);
    let first = Expr::binary(BinaryOp::Eq,inverse(left.schema().fields()[0].expression()),right.schema().fields()[0].expression());
    let second_key = Expr::Coalesce(vec![invalid_pattern(),Expr::Call { name: "str".into(),args: vec![left.schema().fields()[0].expression()],return_type: LogicalType::Scalar(ScalarType::Utf8) }]);
    let second = Expr::binary(BinaryOp::Eq,second_key,right.schema().fields()[0].expression());
    let mut cursor = open(join(left,right,JoinKind::Inner,Some(Expr::binary(BinaryOp::And,first,second))),&execution).await;
    assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb.expression/division-by-zero");
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn empty_values_probe_does_not_prepare_theta_metadata() {
    let execution = execution(1,100);
    let right = numbers(1,&[]);
    let theta = Expr::binary(BinaryOp::Eq,invalid_pattern(),Expr::Literal(Value::Utf8("x".into())));
    let mut cursor = open(join(numbers(0,&[1]),right,JoinKind::Inner,Some(theta)),&execution).await;
    assert!(cursor.next_page().await.unwrap().is_none());
}

#[tokio::test]
async fn left_outer_build_statistics_change_null_key_comparison_like_reference() {
    let execution = execution(1,100);
    let left = table(0,&[Value::Null]);let right = table(1,&[Value::Null]);
    let tied = join(Arc::clone(&left),Arc::clone(&right),JoinKind::Left,Some(equal(&left,&right)));
    assert_eq!(collect(open(tied,&execution).await).await,[vec![vec![Value::Null,Value::Null]]]);
    let left = table(0,&[Value::Null]);let right = table(1,&[Value::Null,Value::Null]);
    let flipped = join(Arc::clone(&left),Arc::clone(&right),JoinKind::Left,Some(equal(&left,&right)));
    assert_eq!(collect(open(flipped,&execution).await).await,[vec![vec![Value::Null,Value::Null]]]);
    let left = table(0,&[Value::Null,Value::Null]);let right = table(1,&[Value::Null,Value::Null]);
    let tied = join(Arc::clone(&left),Arc::clone(&right),JoinKind::Left,Some(equal(&left,&right)));
    assert_eq!(collect(open(tied,&execution).await).await,[vec![vec![Value::Null,Value::Null];4]]);
}

#[tokio::test]
async fn full_join_emits_unmatched_build_rows_as_a_separate_complete_page() {
    for threshold in [0,100] {
        let execution = execution(1,threshold);
        let left = numbers(0,&[1,2]);let right = numbers(1,&[1]);
        let mut cursor = open(join(Arc::clone(&left),Arc::clone(&right),JoinKind::Full,Some(equal(&left,&right))),&execution).await;
        let mut all = Vec::new();while let Some(page) = cursor.next_page().await.unwrap() { all.extend(rows(page)); }
        assert_eq!(all.len(),2);assert!(all.contains(&vec![Value::I64(1),Value::I64(1)]));assert!(all.contains(&vec![Value::I64(2),Value::Null]));
        drop(cursor);assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn returned_join_page_survives_cursor_drop_and_cancelled_next_pull() {
    for threshold in [0,100] {
        let execution = execution(1,threshold);let baseline = execution.context.memory.used_bytes();
        let mut cursor = open(join(numbers(0,&[1,2]),numbers(1,&[3,4]),JoinKind::Inner,None),&execution).await;
        let page = cursor.next_page().await.unwrap().unwrap();assert_eq!(page.len(),4);
        execution.context.cancellation.cancel();assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb/query-cancelled");
        drop(cursor);assert_eq!(execution.context.spill_bytes(),0);assert!(execution.context.memory.used_bytes() > baseline);
        drop(page);assert_eq!(execution.context.memory.used_bytes(),baseline);
    }
}

#[test]
fn outer_build_estimates_use_catalog_rows_and_preserve_unknown_live_only_counts() {
    use rtdb_index::{LiveIndex,Snapshot,TableCatalog,TableMeta,TrieCatalog};
    use rtdb_plan::{QualifiedTable,Scan,TableBinding,TemporalFilter};
    use rtdb_types::{BlockIndex,TableRef};
    let table_name = TableRef::new("public","docs");
    let scan = Arc::new(Relation::scan(Scan {
        table: TableBinding { name: QualifiedTable { database: "xtdb".into(),table: table_name.clone() },oid: None,columns: StructFields::from([("c0".into(),LogicalType::Scalar(ScalarType::I64))]) },
        columns: vec![field(0,LogicalType::Scalar(ScalarType::I64))],valid_time: TemporalFilter::All,system_time: TemporalFilter::All,clamp_valid_time: false,iids: None,predicates: vec![],top_k: None, ordered_top_k: None, matches: Vec::new(),
    }).unwrap());
    for (catalog_rows,expected_left) in [(None,false),(Some(1),true),(Some(2),false),(Some(10),false)] {
        let catalog = match catalog_rows {
            None => TableCatalog::default(),
            Some(rows) => TableCatalog::default().fold(&BTreeMap::from([(table_name.clone(),TableMeta { row_count: rows,..Default::default() })])).unwrap(),
        };
        let snapshot = Snapshot::new(Arc::new(TrieCatalog::new(1 << 20).unwrap()),Arc::new(catalog),Arc::new(LiveIndex::new(BlockIndex(0),None,rtdb_trie::TrieConfig::default())));
        let database = DatabaseSnapshot { partitions: vec![snapshot],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
        let context = QueryContext::new(catalog,MemoryBudget::new(1 << 24),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap();
        let execution = Execution { parameters: Arc::new(BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),context,bindings: None,analysis: None };
        let filtered = Arc::new(Relation::new(scan.schema().clone(),RelationNode::Filter { input: Arc::clone(&scan),predicate: Expr::Literal(Value::Bool(false)) }).unwrap());
        assert_eq!(crate::join::build_left(&filtered,&numbers(1,&[1,2]),JoinKind::Left,&execution).unwrap(),expected_left);
    }
}

#[tokio::test]
async fn absent_values_build_does_not_open_negative_threshold_spill() {
    let execution = execution(1,-1);let baseline = execution.context.memory.used_bytes();
    let mut cursor = open(join(numbers(0,&[]),numbers(1,&[1]),JoinKind::Inner,None),&execution).await;
    assert!(cursor.next_page().await.unwrap().is_none());assert_eq!(execution.context.spill_peak_bytes(),0);
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

fn raw_build(emit: impl FnOnce(&mut rtdb_plan::RegexFormBuilder<'_>) -> rtdb_types::Result<()>) -> RegexForm {
    let mut builder = rtdb_plan::RegexFormBuilder::new_untracked();
    emit(&mut builder).unwrap();
    builder.finish().unwrap()
}
fn raw_literal(value: rtdb_types::Value) -> RegexForm { raw_build(|builder| builder.literal(&value)) }

#[tokio::test]
async fn arrow_equijoin_matches_float_widths_and_rejects_unknown_vector_keys_across_spill() {
    let vector = |value| Value::list(vec![value]);
    let build = [vector(Value::F32(17.0)), vector(Value::Null), Value::Null];
    let probe = [vector(Value::F64(17.0)), vector(Value::F64(18.0)), vector(Value::Null), Value::Null];
    for threshold in [0, 100] {
        let execution = execution(32, threshold);
        let left = table(0, &build);
        let right = table(1, &probe);
        let condition = equal(&left, &right);
        let relation = join(left, right, JoinKind::Inner, Some(condition));
        let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], &execution.context.memory).unwrap();
        let mut cursor = crate::operators::build_columns(runtime, execution).await.unwrap().unwrap();
        let mut count = 0;
        while let Some(batch) = cursor.next_batch().await.unwrap() {
            for row in 0..batch.len {
                assert_eq!(batch.columns[0].value(row).unwrap(), build[0]);
                assert_eq!(batch.columns[1].value(row).unwrap(), probe[0]);
                count += 1;
            }
        }
        assert_eq!(count, 1);
    }
}
