use super::*;
use std::collections::BTreeMap;
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{BinaryOp,ColumnId,Expr,Field,RegexForm,RegexReplacement,Schema};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{LogicalType,ScalarType,Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryContext,QueryOptions,context::OuterRows,parameters::BoundParameters};

struct EmptyCallback { remaining: bool,context: Arc<QueryContext> }
#[async_trait]
impl PageCursor for EmptyCallback {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        if std::mem::take(&mut self.remaining) { Ok(Some(PageBuilder::new(0,&self.context.memory)?.finish())) } else { Ok(None) }
    }
}
fn schema(id: u32) -> Schema { Schema::new(vec![Field { id: ColumnId(id),name: format!("c{id}"),qualifier: None,qualified_only: false,unnamed: false,data_type: LogicalType::Scalar(ScalarType::I64) }]).unwrap() }

#[tokio::test]
async fn actual_empty_probe_callback_prepares_theta_while_absent_callback_does_not() {
    for remaining in [false,true] {
        let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
        let context = QueryContext::new(catalog,MemoryBudget::new(1 << 25),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap();
        let execution = Execution { parameters: Arc::new(BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),context: Arc::clone(&context),bindings: None,analysis: None };
        let baseline = context.memory.used_bytes();
        let replacement = Expr::regex_replace(Expr::Literal(Value::Utf8("a".into())),RegexReplacement { pattern: RegexForm::Literal(Value::Utf8("[".into())),replacement: RegexForm::Literal(Value::Utf8("b".into())),..Default::default() });
        let theta = Expr::binary(BinaryOp::Eq,replacement,Expr::Literal(Value::Utf8("x".into())));
        let layout = JoinLayout::new(&Relation::empty(schema(0)),&Relation::empty(schema(1)),Some(&theta),&context.memory).unwrap();
        let table = BuildTable::new(HashedPage::new(PageBuilder::new(0,&context.memory).unwrap().finish(),|_|Ok(0),&context.memory).unwrap(),false,&context.memory).unwrap();
        let mut cursor = JoinCursor {
            phase: Phase::Memory { table,probe: Box::new(EmptyCallback { remaining,context: Arc::clone(&context) }),_build: Box::new(EmptyCallback { remaining: false,context: Arc::clone(&context) }) },
            plan: JoinPlan { layout,build_left: true,kind: JoinKind::Inner,execution: execution.clone() },
        };
        let result = cursor.next_page().await;
        if remaining { assert_eq!(result.err().unwrap().code,"xtdb.expression/illegal-argument-exception"); }
        else { assert!(result.unwrap().is_none()); }
        drop(cursor);assert_eq!(context.memory.used_bytes(),baseline);assert_eq!(context.spill_bytes(),0);
    }
}
