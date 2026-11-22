use std::sync::Arc;
use async_trait::async_trait;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{MaterializedBinding, Schema};
use rtdb_types::{Anomaly, Result, Value};
use tokio::sync::Mutex;
use crate::{
    QueryContext,
    columnar::{BatchCursor, BatchRows, BoxBatchCursor, ColumnBatch, poll_nested_batch},
    cursor::column_batch,
    invalid,
    pages::{BoxPageCursor, OwnedPage, PageBuilder, PageCursor},
    spill::{SpillFile, SpillReader},
};

pub(crate) struct BindingScope {
    binding: Arc<ReplayBinding>, parent: Option<Arc<Self>>,
}
impl BindingScope {
    pub fn new(definition: Arc<MaterializedBinding>, input: BoxPageCursor, parent: Option<Arc<Self>>, context: Arc<QueryContext>) -> Result<Arc<Self>> {
        Self::with_input(definition, Input::Rows(input), parent, context)
    }

    pub fn columns(definition: Arc<MaterializedBinding>, input: BoxBatchCursor, schema: Schema, parent: Option<Arc<Self>>, context: Arc<QueryContext>) -> Result<Arc<Self>> {
        Self::with_input(definition, Input::Columns { input, schema }, parent, context)
    }

    fn with_input(definition: Arc<MaterializedBinding>, input: Input, parent: Option<Arc<Self>>, context: Arc<QueryContext>) -> Result<Arc<Self>> {
        let reservation = context.memory.reserve(4096)?;
        Ok(Arc::new(Self { binding: Arc::new(ReplayBinding { definition, state: Mutex::new(State::Unopened(input)), context, _reservation: reservation }), parent }))
    }

    pub async fn open(&self, definition: &MaterializedBinding) -> Result<BoxPageCursor> {
        let binding = self.find(definition)?;
        let context = &binding.context;
        match binding.replay().await? {
            Replay::Rows(file) => Ok(Box::new(Reference { reader: file.reader(Arc::clone(&context.memory))?, context: Arc::clone(context) })),
            Replay::Columns(file) => {
                let batches = Box::new(ColumnReference::new(&file, context)?);
                Ok(Box::new(BatchRows::new(batches, Arc::clone(&context.memory))))
            }
        }
    }

    pub async fn open_columns(&self, definition: &MaterializedBinding) -> Result<BoxBatchCursor> {
        let binding = self.find(definition)?;
        let Replay::Columns(file) = binding.replay().await? else {
            return Err(invalid("materialized-binding", "Columnar reference requires a columnar binding"));
        };
        Ok(Box::new(ColumnReference::new(&file, &binding.context)?))
    }

    fn find(&self, definition: &MaterializedBinding) -> Result<&ReplayBinding> {
        let mut scope = Some(self);
        while let Some(current) = scope {
            if current.binding.definition.name == definition.name {
                if current.binding.definition.relation != definition.relation { return Err(invalid("materialized-binding", "Reference definition differs from its lexical binding")); }
                return Ok(&current.binding);
            }
            scope = current.parent.as_deref();
        }
        Err(invalid("missing-relation", format!("Can't find relation '{}'", definition.name)))
    }
}

enum Input {
    Rows(BoxPageCursor),
    Columns { input: BoxBatchCursor, schema: Schema },
}

#[derive(Clone)]
enum Replay { Rows(Arc<SpillFile>), Columns(Arc<SpillFile>) }

enum State { Unopened(Input), Ready(Replay), Failed(Anomaly) }
struct ReplayBinding {
    definition: Arc<MaterializedBinding>, state: Mutex<State>, context: Arc<QueryContext>, _reservation: Arc<MemoryReservation>,
}
impl ReplayBinding {
    async fn replay(&self) -> Result<Replay> {
        self.context.check_cancelled()?;
        let mut state = self.state.lock().await;
        if matches!(&*state, State::Unopened(_)) {
            let State::Unopened(input) = std::mem::replace(&mut *state, State::Failed(crate::context::cancelled())) else { unreachable!() };
            let result = self.write(input).await;
            *state = match result { Ok(file) => State::Ready(file), Err(error) => State::Failed(error) };
        }
        match &*state {
            State::Ready(file) => Ok(file.clone()),
            State::Failed(error) => Err(error.clone()),
            State::Unopened(_) => unreachable!(),
        }
    }

    async fn write(&self, input: Input) -> Result<Replay> {
        let memory = &self.context.memory;
        let mut writer = self.context.spill.create()?;
        match input {
            Input::Rows(mut input) => {
                while let Some(mut page) = input.next_page().await? {
                    self.context.check_cancelled()?;
                    let count = i64::try_from(page.len()).map_err(|_| invalid("materialized-page", "Materialized page exceeds signed row count"))?;
                    writer.write(&[Value::I64(count)], memory)?;
                    while let Some(row) = page.next() {
                        self.context.check_cancelled()?;
                        writer.write(&row.values, memory)?;
                    }
                }
                writer.finish().map(Replay::Rows)
            }
            Input::Columns { mut input, schema } => {
                let schema_bytes = crate::runtime_schema::schema_bound(&schema)?;
                while let Some(batch) = poll_nested_batch(input.as_mut()).await? {
                    self.context.check_cancelled()?;
                    batch.validate_readable(&self.context)?;
                    let batch = column_batch(&schema, memory, batch)?;
                    writer.write_arrow(&batch, schema_bytes, memory, "materialized-spill-arrow")?;
                }
                writer.finish().map(Replay::Columns)
            }
        }
    }
}
struct Reference { reader: SpillReader, context: Arc<QueryContext> }
#[async_trait]
impl PageCursor for Reference {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.context.check_cancelled()?;
        let Some(header) = self.reader.next()? else { return Ok(None); };
        let [Value::I64(count)] = header.values.as_slice() else { return Err(invalid("materialized-page", "Invalid materialized page header")); };
        let count = usize::try_from(*count).map_err(|_|invalid("materialized-page", "Invalid materialized page row count"))?;
        drop(header);
        let mut page = PageBuilder::new(count, &self.context.memory)?;
        for _ in 0..count {
            self.context.check_cancelled()?;
            page.push(self.reader.next()?.ok_or_else(||invalid("materialized-page", "Materialized page ended before its declared row count"))?)?;
        }
        Ok(Some(page.finish()))
    }
}

struct ColumnReference { reader: SpillReader, context: Arc<QueryContext> }

impl ColumnReference {
    fn new(file: &Arc<SpillFile>, context: &Arc<QueryContext>) -> Result<Self> {
        Ok(Self { reader: file.reader(Arc::clone(&context.memory))?, context: Arc::clone(context) })
    }
}

#[async_trait]
impl BatchCursor for ColumnReference {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.context.check_cancelled()?;
        self.reader.read_arrow(true, "materialized-spill-arrow")?.map(ColumnBatch::from_owned).transpose()
    }
}

pub(crate) struct LetCursor { pub body: BoxPageCursor, pub _binding: Arc<BindingScope> }
#[async_trait]
impl PageCursor for LetCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> { self.body.next_page().await }
}

pub(crate) struct ColumnLetCursor { pub body: BoxBatchCursor, pub _binding: Arc<BindingScope> }

#[async_trait]
impl BatchCursor for ColumnLetCursor {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        poll_nested_batch(self.body.as_mut()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::{BTreeMap, VecDeque}, sync::atomic::{AtomicBool, AtomicUsize, Ordering}};
    use rtdb_columnar::MemoryBudget;
    use rtdb_plan::{ColumnId, Expr, Field, Relation, RelationNode, Schema};
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::{LogicalType, ScalarType, Timestamp, Value};
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;
    use crate::{CatalogSnapshot, DatabaseSnapshot, QueryOptions, cursor::OwnedRow};

    struct GateInput { emitted: bool, started: Option<oneshot::Sender<()>>, release: oneshot::Receiver<()>, closed: Arc<AtomicBool>, memory: Arc<MemoryBudget> }
    #[async_trait]
    impl PageCursor for GateInput {
        async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
            if !self.emitted {
                self.emitted = true;
                let mut page = PageBuilder::new(1,&self.memory)?;
                page.push(OwnedRow::new(vec![Value::I64(1)],&self.memory)?)?;
                return Ok(Some(page.finish()));
            }
            if let Some(started) = self.started.take() { let _ = started.send(()); }
            let _ = (&mut self.release).await;
            Ok(None)
        }
    }
    impl Drop for GateInput { fn drop(&mut self) { self.closed.store(true, Ordering::SeqCst); } }

    struct Pages { pages: VecDeque<Vec<i64>>, polls: Arc<AtomicUsize>, memory: Arc<MemoryBudget> }
    #[async_trait]
    impl PageCursor for Pages {
        async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
            self.polls.fetch_add(1,Ordering::SeqCst);
            let Some(values) = self.pages.pop_front() else { return Ok(None); };
            let mut page = PageBuilder::new(values.len(),&self.memory)?;
            for value in values { page.push(OwnedRow::new(vec![Value::I64(value)],&self.memory)?)?; }
            Ok(Some(page.finish()))
        }
    }
    fn context(batch_rows: usize) -> Arc<QueryContext> {
        let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) })]),false).unwrap();
        QueryContext::new(catalog,MemoryBudget::new(1 << 24),Timestamp(0),"UTC",QueryOptions { batch_rows,..Default::default() },&CancellationToken::new()).unwrap()
    }
    fn definition() -> Arc<MaterializedBinding> {
        let field = Field { id: ColumnId(0),name: "n".into(),qualifier: None,qualified_only: false,unnamed: false,data_type: LogicalType::Scalar(ScalarType::I64) };
        Arc::new(MaterializedBinding { name: "c.1".into(),relation: Arc::new(Relation::new(Schema::new(vec![field]).unwrap(),RelationNode::Values { rows: vec![] }).unwrap()) })
    }

    #[tokio::test]
    async fn independent_references_replay_original_pages_including_empty_pages() {
        for batch_rows in [1,1024] {
            let context = context(batch_rows);let definition = definition();let polls = Arc::new(AtomicUsize::new(0));
            let input = Box::new(Pages { pages: VecDeque::from([vec![1,2],vec![],vec![3]]),polls: Arc::clone(&polls),memory: Arc::clone(&context.memory) });
            let scope = BindingScope::new(Arc::clone(&definition),input,None,Arc::clone(&context)).unwrap();
            let mut first = scope.open(&definition).await.unwrap();assert_eq!(polls.load(Ordering::SeqCst),4);
            let mut second = scope.open(&definition).await.unwrap();assert_eq!(polls.load(Ordering::SeqCst),4);
            drop(scope);
            for cursor in [&mut first,&mut second] {
                let mut page = cursor.next_page().await.unwrap().unwrap();assert_eq!(page.len(),2);
                assert_eq!(page.next().unwrap().values,[Value::I64(1)]);assert_eq!(page.next().unwrap().values,[Value::I64(2)]);
                assert_eq!(cursor.next_page().await.unwrap().unwrap().len(),0);
                assert_eq!(cursor.next_page().await.unwrap().unwrap().next().unwrap().values,[Value::I64(3)]);
                assert!(cursor.next_page().await.unwrap().is_none());
            }
            drop(first);drop(second);assert_eq!(context.spill_bytes(),0);assert_eq!(context.memory.used_bytes(),0);
        }
    }

    #[tokio::test]
    async fn column_references_replay_empty_and_zero_column_pages() {
        #[derive(Default)]
        struct Batches(VecDeque<ColumnBatch>);
        #[async_trait]
        impl BatchCursor for Batches {
            async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { Ok(self.0.pop_front()) }
        }

        let context = context(1);
        let schema = Schema::default();
        let relation = Arc::new(Relation::new(schema.clone(), RelationNode::Values { rows: vec![] }).unwrap());
        let definition = Arc::new(MaterializedBinding { name: "empty-fields".into(), relation });
        let input = [0, 2].into_iter().map(|count| {
            ColumnBatch::settled(vec![], count, context.memory.reserve(0).unwrap()).unwrap()
        }).collect::<VecDeque<_>>();
        let scope = BindingScope::columns(Arc::clone(&definition), Box::new(Batches(input)), schema, None, Arc::clone(&context)).unwrap();
        let mut first = scope.open_columns(&definition).await.unwrap();
        let mut second = scope.open_columns(&definition).await.unwrap();
        drop(scope);
        for cursor in [&mut first, &mut second] {
            for len in [0, 2] {
                let batch = cursor.next_batch().await.unwrap().unwrap();
                assert!(batch.columns.is_empty());
                assert_eq!(batch.len, len);
            }
            assert!(cursor.next_batch().await.unwrap().is_none());
        }
        drop((first, second));
        assert_eq!(context.spill_bytes(), 0);
        assert_eq!(context.memory.used_bytes(), 0);
    }

    #[tokio::test]
    async fn returned_replay_page_survives_cursor_and_factory_close() {
        let context = context(1);let definition = definition();
        let input = Box::new(Pages { pages: VecDeque::from([vec![1,2]]),polls: Arc::new(AtomicUsize::new(0)),memory: Arc::clone(&context.memory) });
        let scope = BindingScope::new(Arc::clone(&definition),input,None,Arc::clone(&context)).unwrap();
        let mut cursor = scope.open(&definition).await.unwrap();
        let mut page = cursor.next_page().await.unwrap().unwrap();
        drop(cursor);drop(scope);assert_eq!(context.spill_bytes(),0);assert!(context.memory.used_bytes() > 0);
        assert_eq!(page.next().unwrap().values,[Value::I64(1)]);assert_eq!(page.next().unwrap().values,[Value::I64(2)]);
        drop(page);assert_eq!(context.memory.used_bytes(),0);
    }

    #[tokio::test]
    async fn canceled_reference_rejects_the_next_page_without_retaining_a_partial_page() {
        let context = context(1);let definition = definition();
        let input = Box::new(Pages { pages: VecDeque::from([vec![1],vec![2,3]]),polls: Arc::new(AtomicUsize::new(0)),memory: Arc::clone(&context.memory) });
        let scope = BindingScope::new(Arc::clone(&definition),input,None,Arc::clone(&context)).unwrap();
        let mut cursor = scope.open(&definition).await.unwrap();drop(cursor.next_page().await.unwrap().unwrap());
        context.cancellation.cancel();assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb/query-cancelled");
        drop(cursor);drop(scope);assert_eq!(context.spill_bytes(),0);assert_eq!(context.memory.used_bytes(),0);
    }

    #[tokio::test]
    async fn incomplete_framed_spill_is_rejected_without_retaining_output_rows() {
        let context = context(1);let mut writer = context.spill.create().unwrap();
        writer.write(&[Value::I64(2)],&context.memory).unwrap();writer.write(&[Value::I64(1)],&context.memory).unwrap();
        let file = writer.finish().unwrap();
        let mut cursor = Reference { reader: file.reader(Arc::clone(&context.memory)).unwrap(),context: Arc::clone(&context) };
        assert_eq!(cursor.next_page().await.err().unwrap().code,"rtdb.query/materialized-page");
        assert_eq!(context.memory.used_bytes(),0);drop(cursor);drop(file);assert_eq!(context.spill_bytes(),0);
    }

    #[tokio::test]
    async fn dropping_materialization_future_closes_input_and_deletes_partial_spill() {
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) })]), false).unwrap();
        let context = QueryContext::new(catalog, MemoryBudget::new(1 << 24), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
        let field = Field { id: ColumnId(0), name: "n".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
        let definition = Arc::new(MaterializedBinding { name: "c.1".into(), relation: Arc::new(Relation::new(Schema::new(vec![field]).unwrap(), RelationNode::Values { rows: vec![vec![Expr::Literal(Value::I64(1))]] }).unwrap()) });
        let (started, wait_started) = oneshot::channel();
        let (_release, wait_release) = oneshot::channel();
        let closed = Arc::new(AtomicBool::new(false));
        let input = Box::new(GateInput { emitted: false, started: Some(started), release: wait_release, closed: Arc::clone(&closed), memory: Arc::clone(&context.memory) });
        let scope = BindingScope::new(Arc::clone(&definition), input, None, Arc::clone(&context)).unwrap();
        let opening = tokio::spawn(async move { scope.open(&definition).await });
        wait_started.await.unwrap();
        assert!(context.spill_bytes() > 0);
        assert!(!closed.load(Ordering::SeqCst));
        opening.abort();
        match opening.await { Err(error) => assert!(error.is_cancelled()), Ok(_) => panic!("Materialization completed through a closed gate") }
        assert!(closed.load(Ordering::SeqCst));
        assert_eq!(context.spill_bytes(), 0);
        assert_eq!(context.memory.used_bytes(), 0);
    }
}
