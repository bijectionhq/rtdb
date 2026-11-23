use std::sync::Arc;
use async_trait::async_trait;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{MaterializedBinding, Schema};
use rtdb_types::{Anomaly, Result, Value};
use tokio::sync::Mutex;
use crate::{
    QueryContext, cursor::OwnedRow,
    columnar::{BatchCursor, BatchRows, BoxBatchCursor, ColumnBatch, poll_nested_batch},
    invalid,
    pages::{BoxPageCursor, OwnedPage, PageBuilder, PageCursor},
    spill::{SpillFile, SpillReader, SpillWriter},
};

pub(crate) struct BindingScope {
    binding: Arc<ReplayBinding>, parent: Option<Arc<Self>>,
}
impl BindingScope {
    pub fn new(definition: Arc<MaterializedBinding>, input: BoxPageCursor, parent: Option<Arc<Self>>, context: Arc<QueryContext>) -> Result<Arc<Self>> {
        Self::with_input(definition, Input::Rows(input), parent, context)
    }

    pub fn columns(definition: Arc<MaterializedBinding>, input: BoxBatchCursor, schema: Schema, parent: Option<Arc<Self>>, context: Arc<QueryContext>) -> Result<Arc<Self>> {
        Self::with_input(definition, Input::Columns { input, schema: Arc::new(schema) }, parent, context)
    }

    fn with_input(definition: Arc<MaterializedBinding>, input: Input, parent: Option<Arc<Self>>, context: Arc<QueryContext>) -> Result<Arc<Self>> {
        let reservation = context.memory.reserve(4096)?;
        Ok(Arc::new(Self { binding: Arc::new(ReplayBinding { definition, state: Mutex::new(State::Unopened(input)), context, _reservation: reservation }), parent }))
    }

    pub async fn open(&self, definition: &MaterializedBinding) -> Result<BoxPageCursor> {
        let binding = self.find(definition)?;
        let context = &binding.context;
        match binding.replay().await? {
            Replay::Memory(pages) => Ok(Box::new(MemoryReference::new(pages, context, definition.relation.schema().len()))),
            Replay::Rows(file) => Ok(Box::new(Reference { reader: file.reader(Arc::clone(&context.memory))?, context: Arc::clone(context) })),
            Replay::Columns(file) => {
                let batches = Box::new(ColumnReference::new(&file, context, definition.relation.schema().len())?);
                Ok(Box::new(BatchRows::new(batches, Arc::clone(&context.memory))))
            }
        }
    }

    pub async fn open_columns(&self, definition: &MaterializedBinding) -> Result<BoxBatchCursor> {
        let binding = self.find(definition)?;
        let context = &binding.context;
        match binding.replay().await? {
            Replay::Memory(pages) => Ok(Box::new(MemoryReference::new(pages, context, definition.relation.schema().len()))),
            Replay::Columns(file) => Ok(Box::new(ColumnReference::new(&file, context, definition.relation.schema().len())?)),
            Replay::Rows(file) => {
                let pages = Box::new(Reference { reader: file.reader(Arc::clone(&context.memory))?, context: Arc::clone(context) });
                Ok(crate::columnar::batches_from_pages(pages, definition.relation.schema().len(), Arc::clone(&context.memory)))
            }
        }
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
    Columns { input: BoxBatchCursor, schema: Arc<Schema> },
}

impl Input {
    async fn next_page(&mut self, context: &QueryContext) -> Result<Option<BufferedPage>> {
        context.check_cancelled()?;
        match self {
            Self::Rows(input) => Ok(input.next_page().await?.map(BufferedPage::Rows)),
            Self::Columns { input, .. } => {
                let Some(batch) = poll_nested_batch(input.as_mut()).await? else { return Ok(None); };
                batch.validate_readable(context)?;
                Ok(Some(BufferedPage::Columns(batch)))
            }
        }
    }
}

enum BufferedPage { Rows(OwnedPage), Columns(ColumnBatch) }
impl BufferedPage {
    fn len(&self) -> usize {
        match self { Self::Rows(page) => page.len(), Self::Columns(batch) => batch.len }
    }

    fn retained_bytes(&self, context: &QueryContext) -> Result<usize> {
        match self {
            Self::Rows(page) => Ok(page.retained_bytes()),
            Self::Columns(batch) => batch.working_set_bytes(&context.memory),
        }
    }
}

struct BufferedReplay {
    pages: Vec<BufferedPage>,
    reservation: Arc<MemoryReservation>,
}

impl BufferedReplay {
    fn try_push(&mut self, page: BufferedPage) -> Result<Option<BufferedPage>> {
        if self.pages.len() == self.pages.capacity() {
            let capacity = self.pages.capacity().checked_mul(2).map(|n| n.max(1))
                .and_then(|n| n.checked_mul(std::mem::size_of::<BufferedPage>())).ok_or_else(materialized_size)?;
            let lease = Arc::get_mut(&mut self.reservation).expect("Unpublished materialization owns its page reservation");
            match lease.grow_to(capacity) {
                Ok(()) => self.pages.reserve_exact(capacity / std::mem::size_of::<BufferedPage>() - self.pages.len()),
                Err(error) if error.code == "rtdb/memory-budget-exceeded" => return Ok(Some(page)),
                Err(error) => return Err(error),
            }
        }
        self.pages.push(page);
        Ok(None)
    }
}

fn materialized_size() -> Anomaly { invalid("materialized-size", "Materialized pages exceed addressable memory") }

#[derive(Clone)]
enum Replay { Memory(Arc<BufferedReplay>), Rows(Arc<SpillFile>), Columns(Arc<SpillFile>) }

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

    async fn write(&self, mut input: Input) -> Result<Replay> {
        let memory = &self.context.memory;
        let max_bytes = (memory.available_bytes() / 4).min(64 << 20);
        let mut buffered = BufferedReplay { pages: Vec::new(), reservation: memory.reserve(0)? };
        let mut rows = 0usize;
        let mut bytes = 0usize;
        while let Some(page) = input.next_page(&self.context).await? {
            rows = rows.saturating_add(page.len());
            let retained = match page.retained_bytes(&self.context) {
                Ok(bytes) => bytes,
                Err(error) if error.code == "rtdb/memory-budget-exceeded" => usize::MAX,
                Err(error) => return Err(error),
            };
            bytes = bytes.saturating_add(retained).saturating_add(std::mem::size_of::<BufferedPage>());
            let spilled_page = if rows > self.context.options.materialized_spill_rows || bytes > max_bytes
                || bytes > memory.available_bytes() {
                Some(page)
            } else {
                buffered.try_push(page)?
            };
            if let Some(page) = spilled_page {
                return self.spill(input, buffered, page).await;
            }
        }
        Ok(Replay::Memory(Arc::new(buffered)))
    }

    async fn spill(&self, mut input: Input, buffered: BufferedReplay, page: BufferedPage) -> Result<Replay> {
        let schema = match &input { Input::Rows(_) => None, Input::Columns { schema, .. } => Some(Arc::clone(schema)) };
        let schema = schema.as_deref().map(|schema| crate::runtime_schema::schema_bound(schema).map(|bytes| (schema, bytes))).transpose()?;
        let mut writer = self.context.spill.create()?;
        for page in buffered.pages.into_iter().chain(std::iter::once(page)) {
            write_page(&mut writer, page, schema, &self.context)?;
        }
        drop(buffered.reservation);
        while let Some(page) = input.next_page(&self.context).await? {
            write_page(&mut writer, page, schema, &self.context)?;
        }
        let file = writer.finish()?;
        Ok(if schema.is_some() { Replay::Columns(file) } else { Replay::Rows(file) })
    }
}

fn write_page(writer: &mut SpillWriter, page: BufferedPage, schema: Option<(&Schema, usize)>, context: &QueryContext) -> Result<()> {
    match page {
        BufferedPage::Rows(page) => write_rows(writer, page, context),
        BufferedPage::Columns(batch) => {
            let (schema, schema_bytes) = schema.ok_or_else(|| invalid("materialized-schema", "Column materialization is missing its schema"))?;
            let memory = &context.memory;
            match batch.spill_record(schema, memory) {
                Ok(Some(record)) => {
                    let start = writer.position()?;
                    let written = writer.write(&[Value::I64(-1)], memory)
                        .and_then(|()| writer.write_arrow(&record, schema_bytes, memory, "materialized-spill-arrow"));
                    match written {
                        Ok(()) => return Ok(()),
                        Err(error) if matches!(error.code.as_str(), "rtdb/memory-budget-exceeded" | "rtdb.query/spill-limit") => {
                            writer.discard_from(start)?;
                        }
                        Err(error) => return Err(error),
                    }
                }
                Ok(None) => {}
                Err(error) if error.code == "rtdb/memory-budget-exceeded" => {}
                Err(error) => return Err(error),
            }
            write_rows(writer, batch.rows(memory)?, context)
        }
    }
}

struct MemoryReference {
    replay: Arc<BufferedReplay>,
    context: Arc<QueryContext>,
    width: usize,
    next: usize,
}

impl MemoryReference {
    fn new(replay: Arc<BufferedReplay>, context: &Arc<QueryContext>, width: usize) -> Self {
        Self { replay, context: Arc::clone(context), width, next: 0 }
    }
}

#[async_trait]
impl PageCursor for MemoryReference {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.context.check_cancelled()?;
        let Some(page) = self.replay.pages.get(self.next) else { return Ok(None); };
        let memory = &self.context.memory;
        let page = match page {
            BufferedPage::Columns(batch) => batch.rows(memory)?,
            BufferedPage::Rows(page) => {
                let mut copy = PageBuilder::new(page.len(), memory)?;
                for (index, row) in page.rows().iter().enumerate() {
                    self.context.check_cancelled_at(index)?;
                    copy.push(OwnedRow::copy_parts(&[&row.values], 0, 0, memory)?)?;
                }
                copy.finish()
            }
        };
        self.next += 1;
        Ok(Some(page))
    }
}

#[async_trait]
impl BatchCursor for MemoryReference {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.context.check_cancelled()?;
        let Some(page) = self.replay.pages.get(self.next) else { return Ok(None); };
        let memory = &self.context.memory;
        let page = match page {
            BufferedPage::Columns(batch) => batch.duplicate_for_replay(memory)?,
            BufferedPage::Rows(page) if page.len() == 0 => ColumnBatch::empty_with_width(self.width, memory)?,
            BufferedPage::Rows(page) => ColumnBatch::from_page(page, memory)?,
        };
        self.next += 1;
        Ok(Some(page))
    }
}

fn write_rows(writer: &mut SpillWriter, mut page: OwnedPage, context: &QueryContext) -> Result<()> {
    context.check_cancelled()?;
    let count = i64::try_from(page.len()).map_err(|_| invalid("materialized-page", "Materialized page exceeds signed row count"))?;
    writer.write(&[Value::I64(count)], &context.memory)?;
    while let Some(row) = page.next() {
        context.check_cancelled()?;
        writer.write(&row.values, &context.memory)?;
    }
    Ok(())
}

fn read_rows(reader: &mut SpillReader, count: i64, context: &QueryContext) -> Result<OwnedPage> {
    let count = usize::try_from(count).map_err(|_| invalid("materialized-page", "Invalid materialized page row count"))?;
    let mut page = PageBuilder::new(count, &context.memory)?;
    for _ in 0..count {
        context.check_cancelled()?;
        page.push(reader.next()?.ok_or_else(|| invalid("materialized-page", "Materialized page ended before its declared row count"))?)?;
    }
    Ok(page.finish())
}

fn page_header(reader: &mut SpillReader) -> Result<Option<i64>> {
    let Some(header) = reader.next()? else { return Ok(None); };
    let [Value::I64(count)] = header.values.as_slice() else { return Err(invalid("materialized-page", "Invalid materialized page header")); };
    Ok(Some(*count))
}

struct Reference { reader: SpillReader, context: Arc<QueryContext> }
#[async_trait]
impl PageCursor for Reference {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.context.check_cancelled()?;
        page_header(&mut self.reader)?.map(|count| read_rows(&mut self.reader, count, &self.context)).transpose()
    }
}

struct ColumnReference { reader: SpillReader, context: Arc<QueryContext>, width: usize }

impl ColumnReference {
    fn new(file: &Arc<SpillFile>, context: &Arc<QueryContext>, width: usize) -> Result<Self> {
        Ok(Self { reader: file.reader(Arc::clone(&context.memory))?, context: Arc::clone(context), width })
    }
}

#[async_trait]
impl BatchCursor for ColumnReference {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.context.check_cancelled()?;
        match page_header(&mut self.reader)? {
            Some(-1) => {
                let batch = self.reader.read_arrow(true, "materialized-spill-arrow")?
                    .ok_or_else(|| invalid("materialized-page", "Materialized Arrow page is absent"))?;
                ColumnBatch::from_owned(batch).map(Some)
            }
            Some(0) => {
                ColumnBatch::empty_with_width(self.width, &self.context.memory).map(Some)
            }
            Some(count) => {
                let page = read_rows(&mut self.reader, count, &self.context)?;
                ColumnBatch::from_page(&page, &self.context.memory).map(Some)
            }
            None => Ok(None),
        }
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
    use crate::columnar::Column;
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
        context_with_options(QueryOptions { batch_rows, ..Default::default() })
    }
    fn context_with_options(options: QueryOptions) -> Arc<QueryContext> {
        let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) })]),false).unwrap();
        QueryContext::new(catalog,MemoryBudget::new(1 << 24),Timestamp(0),"UTC",options,&CancellationToken::new()).unwrap()
    }
    fn definition() -> Arc<MaterializedBinding> {
        let field = Field { id: ColumnId(0),name: "n".into(),qualifier: None,qualified_only: false,unnamed: false,data_type: LogicalType::Scalar(ScalarType::I64) };
        Arc::new(MaterializedBinding { name: "c.1".into(),relation: Arc::new(Relation::new(Schema::new(vec![field]).unwrap(),RelationNode::Values { rows: vec![] }).unwrap()) })
    }

    #[tokio::test]
    async fn independent_references_replay_original_pages_including_empty_pages() {
        for (batch_rows, materialized_spill_rows) in [(1, 100), (1024, 2)] {
            let context = context_with_options(QueryOptions { batch_rows, materialized_spill_rows, ..Default::default() });
            let definition = definition();
            let polls = Arc::new(AtomicUsize::new(0));
            let input = Box::new(Pages { pages: VecDeque::from([vec![1,2],vec![],vec![3]]),polls: Arc::clone(&polls),memory: Arc::clone(&context.memory) });
            let scope = BindingScope::new(Arc::clone(&definition),input,None,Arc::clone(&context)).unwrap();
            let mut first = scope.open(&definition).await.unwrap();
            assert_eq!(polls.load(Ordering::SeqCst), 4);
            assert_eq!(context.spill_bytes() > 0, materialized_spill_rows < 3);
            let mut second = scope.open(&definition).await.unwrap();assert_eq!(polls.load(Ordering::SeqCst),4);
            let mut columns = scope.open_columns(&definition).await.unwrap();
            assert_eq!(columns.next_batch().await.unwrap().unwrap().len, 2);
            let empty = columns.next_batch().await.unwrap().unwrap();
            assert_eq!((empty.len, empty.columns.len()), (0, 1));
            assert_eq!(columns.next_batch().await.unwrap().unwrap().columns[0].value(0).unwrap(), Value::I64(3));
            assert!(columns.next_batch().await.unwrap().is_none());
            drop((columns, empty));
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
    async fn materialized_arrow_replay_keeps_the_record_fields_present_in_each_page() {
        use arrow_array::RecordBatch;
        use rtdb_columnar::{OwnedBatch, copy_array, to_field};
        use rtdb_types::StructFields;

        let context = context_with_options(QueryOptions { materialized_spill_rows: 0, ..Default::default() });
        let value = Value::Struct(StructFields::from([("a".into(), Value::I64(3))]));
        let actual = value.logical_type();
        let declared = LogicalType::Struct(StructFields::from([
            ("a".into(), LogicalType::Scalar(ScalarType::I64)),
            ("b".into(), LogicalType::Scalar(ScalarType::Utf8).maybe()),
        ]));
        let schema = Schema::new(vec![Field { id: ColumnId(0), name: "doc".into(), qualifier: None,
            qualified_only: false, unnamed: false, data_type: declared }]).unwrap();
        let definition = Arc::new(MaterializedBinding { name: "nested".into(),
            relation: Arc::new(Relation::new(schema.clone(), RelationNode::Values { rows: vec![] }).unwrap()) });
        let array = copy_array(&actual, std::slice::from_ref(&value)).unwrap();
        let input = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(vec![to_field("doc", &actual).unwrap()])), vec![array]).unwrap();
        let input = ColumnBatch::from_owned(OwnedBatch::new(input, &context.memory).unwrap()).unwrap();
        let input = crate::columnar::single_batch(Some(input), Arc::clone(&context));
        let scope = BindingScope::columns(Arc::clone(&definition), input, schema, None, Arc::clone(&context)).unwrap();
        let mut cursor = scope.open_columns(&definition).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert!(matches!(page.columns[0], crate::columnar::Column::Arrow { .. }));
        assert_eq!(page.columns[0].value(0).unwrap(), value);
        assert!(cursor.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_materialized_empty_page_keeps_its_columns_under_a_tight_spill_limit() {
        let context = context_with_options(QueryOptions { spill_limit_bytes: 96, ..Default::default() });
        let definition = definition();
        let schema = definition.relation.schema().clone();
        let memory = &context.memory;
        let batch = ColumnBatch::settled(vec![Column::Values(Vec::new())], 0, memory.reserve(256).unwrap()).unwrap();
        let input = crate::columnar::single_batch(Some(batch), Arc::clone(&context));
        let scope = BindingScope::columns(Arc::clone(&definition), input, schema, None, Arc::clone(&context)).unwrap();
        let mut reader = scope.open_columns(&definition).await.unwrap();
        let page = reader.next_batch().await.unwrap().unwrap();
        assert!(matches!(page.columns.as_slice(), [Column::Values(values)] if values.is_empty()));
        assert_eq!(page.len, 0);
        assert!(reader.next_batch().await.unwrap().is_none());
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
        let options = QueryOptions { materialized_spill_rows: 0, ..Default::default() };
        let context = QueryContext::new(catalog, MemoryBudget::new(1 << 24), Timestamp(0), "UTC", options, &CancellationToken::new()).unwrap();
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
