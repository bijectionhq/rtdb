use std::sync::Arc;
use async_trait::async_trait;
use rtdb_plan::{BinaryOp,Expr,Schema};
#[cfg(test)]
use rtdb_plan::Relation;
use rtdb_types::{LogicalType,Result,Value};
use crate::{context::Execution,cursor::OwnedRow,invalid,operators::{DeferredInput,build_pages},pages::{BoxPageCursor,OwnedPage,PageBuilder,PageCursor},runtime_schema::EmittedRelation,join_storage::{BuildAccumulator,BuildInput,BuildTable,HashedPage,PartitionSpool,hash_values}};

enum State {
    Build { probe: DeferredInput,runtime: Arc<EmittedRelation> },
    Memory { probe: BoxPageCursor,table: BuildTable,facts: CandidateFacts,_build: BoxPageCursor },
    EveryRowDisk { probe: BoxPageCursor,build: PartitionSpool,_build: BoxPageCursor },
    KeyedDisk { probe: PartitionSpool,build: PartitionSpool,facts: CandidateFacts,table: Option<BuildTable>,build_part: usize,probe_part: usize,_build_cursor: BoxPageCursor,_probe_cursor: BoxPageCursor },
    CorrelatedUnopened { input: DeferredInput,runtime: Arc<EmittedRelation>,arguments: Vec<(usize,Expr)> },
    Correlated { input: BoxPageCursor,runtime: Arc<EmittedRelation>,arguments: Vec<(usize,Expr)> },
    Done,
}
#[derive(Clone,Copy)]
struct CandidateFacts { empty: bool,null: bool,nested: bool }
impl CandidateFacts {
    fn add(&mut self,page: &OwnedPage,schema: &Schema) {
        if page.len() == 0 { return; }
        self.empty = false;
        self.null |= page.rows().iter().any(|row|matches!(row.values.first(),Some(Value::Null | Value::Undefined)));
        self.nested |= schema.fields().first().is_some_and(|field|field.data_type.legs().iter().any(|ty|matches!(ty,LogicalType::List { .. } | LogicalType::Struct(_))));
    }
    fn unmatched(self,probe_null: bool) -> Value {
        if !self.empty && (self.null || probe_null) { Value::Null } else { Value::Bool(false) }
    }
}
struct MarkPlan { input_schema: Schema,comparison_schema: Schema,comparison: Expr,probe_key: Option<usize>,execution: Execution }
pub(crate) struct Mark { state: State,plan: MarkPlan }
impl Mark {
    pub fn new(input: DeferredInput,input_schema: Schema,runtime: Arc<EmittedRelation>,arguments: Vec<(usize,Expr)>,comparison: Expr,execution: Execution) -> Result<Self> {
        let comparison_schema = input_schema.concat(runtime.schema())?;
        let probe_key = probe_key(&input_schema,runtime.schema(),&comparison);
        let state = if arguments.is_empty() { State::Build { probe: input,runtime } }
            else { State::CorrelatedUnopened { input,runtime,arguments } };
        Ok(Self { state,plan: MarkPlan { input_schema,comparison_schema,comparison,probe_key,execution } })
    }
}
fn probe_key(input: &Schema,dependent: &Schema,comparison: &Expr) -> Option<usize> {
    let Expr::Binary { op: BinaryOp::Eq,left,right } = comparison else { return None; };
    for (probe,candidate) in [(left.as_ref(),right.as_ref()),(right.as_ref(),left.as_ref())] {
        if let (Expr::Column { id: probe,.. },Expr::Column { id: candidate,.. }) = (probe,candidate) {
            if dependent.fields().first().is_some_and(|field|field.id == *candidate) {
                if let Some(position) = input.fields().iter().position(|field|field.id == *probe) { return Some(position); }
            }
        }
    }
    None
}
impl MarkPlan {
    fn build_hash(&self,row: &OwnedRow) -> Result<u32> {
        match self.probe_key {
            None => Ok(0),
            Some(_) => hash_values([row.values.first().ok_or_else(||invalid("mark-key","Candidate key is absent"))?]),
        }
    }
    fn probe_hash(&self,row: &OwnedRow) -> Result<u32> {
        let index = self.probe_key.ok_or_else(||invalid("mark-key","Probe key is absent"))?;
        hash_values([row.values.get(index).ok_or_else(||invalid("mark-key","Probe key cell is absent"))?])
    }
    async fn compare(&self,row: &OwnedRow,candidate: &OwnedRow,invocation: &rtdb_expr::ExpressionInvocation<'_>) -> Result<Value> {
        let combined = OwnedRow::copy_parts(&[&row.values,&candidate.values],0,0,&self.execution.context.memory)?;
        let scope = self.execution.scope()?;
        let result = self.execution.evaluate_invocation(&scope,invocation,&combined.values).await?;
        if matches!(result,Value::Bool(_) | Value::Null) { Ok(result) } else { Err(invalid("mark-type","Mark comparison must return Boolean or null")) }
    }
    async fn keyed_page(&self,page: OwnedPage,table: &BuildTable,facts: CandidateFacts) -> Result<OwnedPage> {
        let metadata = self.execution.scope()?;
        let invocation = self.execution.prepare_expression(&self.comparison,&self.comparison_schema,&metadata)?;
        let _scratch = self.execution.context.memory.reserve(4096)?;
        let mut output = PageBuilder::new(page.len(),&self.execution.context.memory)?;
        for row in page.rows() {
            self.execution.context.check_cancelled()?;
            let key = row.values.get(self.probe_key.ok_or_else(||invalid("mark-key","Probe key is absent"))?).ok_or_else(||invalid("mark-key","Probe key cell is absent"))?;
            let null = matches!(key,Value::Null | Value::Undefined);
            let mut result = facts.unmatched(null);
            if !null {
                for index in table.candidates(self.probe_hash(row)?) {
                    if self.compare(row,&table.rows.page.rows()[index],&invocation).await? == Value::Bool(true) { result = Value::Bool(true);break; }
                }
            }
            output.push(OwnedRow::copy_parts(&[&row.values,&[result]],0,0,&self.execution.context.memory)?)?;
        }
        Ok(output.finish())
    }
    fn marked_page(&self,page: &OwnedPage) -> Result<PageBuilder> {
        let mut output = PageBuilder::new(page.len(),&self.execution.context.memory)?;
        for row in page.rows() { output.push(OwnedRow::copy_parts(&[&row.values,&[Value::Bool(false)]],0,0,&self.execution.context.memory)?)?; }
        Ok(output)
    }
    async fn compare_all(&self,output: &mut PageBuilder,page: &OwnedPage,table: &BuildTable) -> Result<()> {
        let metadata = self.execution.scope()?;
        let invocation = self.execution.prepare_expression(&self.comparison,&self.comparison_schema,&metadata)?;
        for (row,output) in page.rows().iter().zip(output.rows_mut()) {
            self.execution.context.check_cancelled()?;
            let mut result = output.values.last().cloned().ok_or_else(||invalid("mark-state","Accumulated mark is absent"))?;
            if result == Value::Bool(true) { continue; }
            for candidate in table.rows.page.rows() {
                match self.compare(row,candidate,&invocation).await? {
                    Value::Bool(true) => { result = Value::Bool(true);break; }
                    Value::Null => result = Value::Null,
                    Value::Bool(false) => (),
                    _ => unreachable!(),
                }
            }
            output.replace_value(row.values.len(),result,&self.execution.context.memory)?;
        }
        Ok(())
    }
}

#[async_trait]
impl PageCursor for Mark {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        let plan = &self.plan;
        loop {
            plan.execution.context.check_cancelled()?;
            match std::mem::replace(&mut self.state,State::Done) {
                State::Build { probe,runtime } => {
                    let mut input = build_pages(Arc::clone(&runtime),plan.execution.child(1)).await?;
                    let mut accumulator = BuildAccumulator::new(plan.execution.context.options.join_build_rows,Arc::clone(&plan.execution.context))?;
                    let mut facts = CandidateFacts { empty: true,null: false,nested: false };
                    while let Some(page) = input.next_page().await? { facts.add(&page,runtime.schema());accumulator.append(page)?; }
                    match accumulator.finish()? {
                        BuildInput::Memory(page) => {
                            let table = BuildTable::new(HashedPage::new(page,|row|plan.build_hash(row),&plan.execution.context.memory)?,false,&plan.execution.context.memory)?;
                            self.state = State::Memory { probe: probe.open_pages(None).await?,table,facts,_build: input };
                        }
                        BuildInput::Spilled(blocks) => {
                            let build = PartitionSpool::build(blocks,1,|row|plan.build_hash(row),&plan.execution.context)?;
                            let mut probe = probe.open_pages(None).await?;
                            if facts.nested || plan.probe_key.is_none() { self.state = State::EveryRowDisk { probe,build,_build: input }; }
                            else {
                                let mut accumulator = BuildAccumulator::new(plan.execution.context.options.join_build_rows,Arc::clone(&plan.execution.context))?;
                                while let Some(page) = probe.next_page().await? { accumulator.append(page)?; }
                                let probe_shuffle = PartitionSpool::build(accumulator.finish_spilled()?,build.part_count(),|row|plan.probe_hash(row),&plan.execution.context)?;
                                self.state = State::KeyedDisk { probe: probe_shuffle,build,facts,table: None,build_part: 0,probe_part: 0,_build_cursor: input,_probe_cursor: probe };
                            }
                        }
                    }
                }
                State::Memory { mut probe,table,facts,_build } => {
                    let Some(page) = probe.next_page().await? else { return Ok(None); };
                    let every_row = facts.nested || plan.probe_key.is_none();
                    if every_row && page.len() == 0 { self.state = State::Memory { probe,table,facts,_build };continue; }
                    let output = if every_row {
                        let mut output = plan.marked_page(&page)?;plan.compare_all(&mut output,&page,&table).await?;output.finish()
                    } else { plan.keyed_page(page,&table,facts).await? };
                    self.state = State::Memory { probe,table,facts,_build };
                    if output.len() > 0 { return Ok(Some(output)); }
                }
                State::EveryRowDisk { mut probe,build,_build } => {
                    let Some(page) = probe.next_page().await? else { return Ok(None); };
                    if page.len() == 0 { self.state = State::EveryRowDisk { probe,build,_build };continue; }
                    let mut output = plan.marked_page(&page)?;
                    for part in 0..build.part_count() {
                        let table = BuildTable::new(build.load(part,&plan.execution.context)?,false,&plan.execution.context.memory)?;
                        plan.compare_all(&mut output,&page,&table).await?;
                    }
                    self.state = State::EveryRowDisk { probe,build,_build };
                    return Ok(Some(output.finish()));
                }
                State::KeyedDisk { probe,build,facts,mut table,mut build_part,mut probe_part,_build_cursor,_probe_cursor } => {
                    if build_part == build.part_count() { return Ok(None); }
                    if table.is_none() { table = Some(BuildTable::new(build.load(build_part,&plan.execution.context)?,false,&plan.execution.context.memory)?); }
                    let page = probe.load(probe_part * build.part_count() + build_part,&plan.execution.context)?.page;
                    let output = plan.keyed_page(page,table.as_ref().ok_or_else(||invalid("mark-state","Loaded build partition is absent"))?,facts).await?;
                    probe_part += 1;
                    if probe_part == probe.part_count() / build.part_count() { table = None;build_part += 1;probe_part = 0; }
                    self.state = State::KeyedDisk { probe,build,facts,table,build_part,probe_part,_build_cursor,_probe_cursor };
                    if output.len() > 0 { return Ok(Some(output)); }
                }
                State::CorrelatedUnopened { input,runtime,arguments } => {
                    self.state = State::Correlated { input: input.open_pages(None).await?,runtime,arguments };
                }
                State::Correlated { mut input,runtime,arguments } => {
                    let Some(mut page) = input.next_page().await? else { return Ok(None); };
                    let mut output = PageBuilder::new(page.len(),&plan.execution.context.memory)?;
                    while let Some(row) = page.next() {
                        let row = Arc::new(row);
                        let execution = plan.execution.bind_arguments(&arguments,&plan.input_schema,Arc::clone(&row)).await?;
                        let dependent = build_pages(Arc::clone(&runtime),execution.clone()).await?;
                        let result = correlated_mark(&row,dependent,&plan.comparison,&plan.comparison_schema,&execution).await?;
                        output.push(OwnedRow::copy_parts(&[&row.values,&[result]],0,0,&plan.execution.context.memory)?)?;
                    }
                    self.state = State::Correlated { input,runtime,arguments };
                    return Ok(Some(output.finish()));
                }
                State::Done => return Ok(None),
            }
        }
    }
}

async fn correlated_mark(row: &OwnedRow,mut dependent: BoxPageCursor,comparison: &Expr,schema: &Schema,execution: &Execution) -> Result<Value> {
    let mut result = Value::Bool(false);
    while let Some(mut page) = dependent.next_page().await? {
        execution.context.check_cancelled()?;
        let metadata_scope = execution.scope()?;
        let invocation = execution.prepare_expression(comparison,schema,&metadata_scope)?;
        while let Some(candidate) = page.next() {
            execution.context.check_cancelled()?;
            let combined = OwnedRow::copy_parts(&[&row.values,&candidate.values],0,0,&execution.context.memory)?;
            let scope = execution.scope()?;
            match execution.evaluate_invocation(&scope,&invocation,&combined.values).await? {
                Value::Bool(true) => result = Value::Bool(true),
                Value::Null if result != Value::Bool(true) => result = Value::Null,
                Value::Bool(false) | Value::Null => (),
                _ => return Err(invalid("mark-type","Mark comparison must return Boolean or null")),
            }
        }
        if result == Value::Bool(true) { break; }
    }
    Ok(result)
}

#[cfg(test)]
mod facts_tests {
    use super::*;
    use rtdb_columnar::MemoryBudget;
    use rtdb_plan::{ColumnId,Field};
    use rtdb_types::ListKind;

    #[test]
    fn nested_build_facts_start_only_after_a_nonempty_page_and_include_all_listy_families() {
        let memory = MemoryBudget::new(1 << 20);
        for ty in [LogicalType::List { kind: ListKind::List,element: Box::new(LogicalType::Null) },LogicalType::List { kind: ListKind::Map { sorted: false },element: Box::new(LogicalType::Struct(Default::default())) },Value::TimestampTzRange { lower: 0,upper: 1 }.logical_type(),LogicalType::Struct(Default::default())] {
            let schema = Schema::new(vec![Field { id: ColumnId(0),name: "k".into(),qualifier: None,qualified_only: false,unnamed: false,data_type: ty.maybe() }]).unwrap();
            let mut facts = CandidateFacts { empty: true,null: false,nested: false };
            facts.add(&PageBuilder::new(0,&memory).unwrap().finish(),&schema);
            assert!(facts.empty);assert!(!facts.null);assert!(!facts.nested);assert_eq!(facts.unmatched(true),Value::Bool(false));
            let mut page = PageBuilder::new(1,&memory).unwrap();page.push(OwnedRow::new(vec![Value::Null],&memory).unwrap()).unwrap();let page = page.finish();
            facts.add(&page,&schema);
            assert!(!facts.empty);assert!(facts.null);assert!(facts.nested);assert_eq!(facts.unmatched(false),Value::Null);
        }
        assert_eq!(memory.used_bytes(),0);
    }

    struct EmptyCallback { remaining: bool,memory: Arc<MemoryBudget> }
    #[async_trait]
    impl PageCursor for EmptyCallback {
        async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
            if std::mem::take(&mut self.remaining) { Ok(Some(PageBuilder::new(0,&self.memory)?.finish())) } else { Ok(None) }
        }
    }

    #[tokio::test]
    async fn correlated_mark_forwards_actual_empty_callback_without_opening_dependent_rows() {
        use std::collections::BTreeMap;
        use rtdb_storage::{MemoryStore,cache::BufferPool};
        use rtdb_types::{ScalarType,Timestamp};
        use tokio_util::sync::CancellationToken;
        use crate::{CatalogSnapshot,DatabaseSnapshot,QueryContext,QueryOptions,context::OuterRows,parameters::BoundParameters,runtime_schema::EmittedRelation};
        let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
        let context = QueryContext::new(catalog,MemoryBudget::new(1 << 25),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap();
        let execution = Execution { parameters: Arc::new(BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),context: Arc::clone(&context),bindings: None,analysis: None };
        let field = Field { id: ColumnId(1),name: "v".into(),qualifier: None,qualified_only: false,unnamed: false,data_type: LogicalType::Scalar(ScalarType::I64) };
        let relation = Arc::new(Relation::new(Schema::new(vec![field]).unwrap(),rtdb_plan::RelationNode::Values { rows: vec![vec![Expr::binary(BinaryOp::Divide,Expr::Literal(Value::I64(1)),Expr::Literal(Value::I64(0)))]] }).unwrap());
        let runtime = EmittedRelation::derive(Arc::clone(&relation),&context.catalog,execution.parameters.types(),&context.memory).unwrap();
        let mut cursor = Mark {
            state: State::Correlated { input: Box::new(EmptyCallback { remaining: true,memory: Arc::clone(&context.memory) }),runtime: Arc::clone(&runtime),arguments: vec![] },
            plan: MarkPlan { input_schema: Schema::default(),comparison_schema: runtime.schema().clone(),comparison: Expr::Literal(Value::Bool(true)),probe_key: None,execution },
        };
        assert_eq!(cursor.next_page().await.unwrap().unwrap().len(),0);
        assert!(cursor.next_page().await.unwrap().is_none());
    }
}
