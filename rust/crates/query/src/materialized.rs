use std::sync::Arc;
use async_trait::async_trait;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{MaterializedBinding, Schema};
use rtdb_types::{Anomaly, Result};
use tokio::sync::Mutex;
use crate::{
    QueryContext,
    columnar::{BatchCursor, BoxBatchCursor, ColumnBatch, poll_nested_batch},
    invalid,
    spill::{SpillFile, SpillReader},
};

pub(crate) struct BindingScope {
    binding: Arc<ReplayBinding>, parent: Option<Arc<Self>>,
}
impl BindingScope {
    pub fn columns(definition: Arc<MaterializedBinding>, input: BoxBatchCursor, schema: Schema, parent: Option<Arc<Self>>, context: Arc<QueryContext>) -> Result<Arc<Self>> {
        let reservation = context.memory.reserve(4096)?;
        Ok(Arc::new(Self { binding: Arc::new(ReplayBinding {
            definition, state: Mutex::new(State::Unopened { input, schema }), context, _reservation: reservation,
        }), parent }))
    }

    pub async fn open_columns(&self, definition: &MaterializedBinding) -> Result<BoxBatchCursor> {
        let binding = self.find(definition)?;
        let context = &binding.context;
        match binding.replay().await? {
            Replay::Memory(replay) => Ok(Box::new(MemoryReference { replay, context: Arc::clone(context), next: 0 })),
            Replay::Spill(file) => Ok(Box::new(ColumnReference::new(&file, context, definition.relation.schema().len())?)),
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

struct BufferedReplay {
    pages: Vec<ColumnBatch>,
    reservation: Arc<MemoryReservation>,
}

impl BufferedReplay {
    fn push(&mut self, page: ColumnBatch) -> Result<()> {
        if self.pages.len() == self.pages.capacity() {
            let capacity = self.pages.capacity().checked_mul(2).map(|n| n.max(1))
                .and_then(|n| n.checked_mul(std::mem::size_of::<ColumnBatch>())).ok_or_else(materialized_size)?;
            let lease = Arc::get_mut(&mut self.reservation).expect("Unpublished materialization owns its page reservation");
            lease.grow_to(capacity)?;
            self.pages.reserve_exact(capacity / std::mem::size_of::<ColumnBatch>() - self.pages.len());
        }
        self.pages.push(page);
        Ok(())
    }
}

fn materialized_size() -> Anomaly { invalid("materialized-size", "Materialized pages exceed addressable memory") }

#[derive(Clone)]
enum Replay { Memory(Arc<BufferedReplay>), Spill(Arc<SpillFile>) }

enum State {
    Unopened { input: BoxBatchCursor, schema: Schema },
    Ready(Replay),
    Failed(Anomaly),
}
struct ReplayBinding {
    definition: Arc<MaterializedBinding>, state: Mutex<State>, context: Arc<QueryContext>, _reservation: Arc<MemoryReservation>,
}
impl ReplayBinding {
    async fn replay(&self) -> Result<Replay> {
        self.context.check_cancelled()?;
        let mut state = self.state.lock().await;
        if matches!(&*state, State::Unopened { .. }) {
            let State::Unopened { input, schema } = std::mem::replace(&mut *state, State::Failed(crate::context::cancelled())) else { unreachable!() };
            let result = self.write(input, schema).await;
            *state = match result { Ok(file) => State::Ready(file), Err(error) => State::Failed(error) };
        }
        match &*state {
            State::Ready(replay) => Ok(replay.clone()),
            State::Failed(error) => Err(error.clone()),
            State::Unopened { .. } => unreachable!(),
        }
    }

    async fn write(&self, mut input: BoxBatchCursor, schema: Schema) -> Result<Replay> {
        let memory = &self.context.memory;
        let options = &self.context.options;
        let mut buffered = BufferedReplay { pages: Vec::new(), reservation: memory.reserve(0)? };
        let mut rows = 0usize;
        let mut bytes = 0usize;
        while let Some(batch) = self.next_batch(&mut input).await? {
            rows = rows.saturating_add(batch.len);
            let retained = batch.working_set_bytes(memory)?;
            bytes = bytes.saturating_add(retained).saturating_add(std::mem::size_of::<ColumnBatch>());
            // XTDB's let spills past a row threshold alone; a budget that cannot hold the buffered pages fails the query.
            let spill = if rows > options.materialized_spill_rows || bytes > options.materialized_memory_bytes {
                Some(batch)
            } else {
                buffered.push(batch)?;
                None
            };
            if let Some(batch) = spill {
                let mut writer = self.context.spill.create()?;
                for batch in buffered.pages.into_iter().chain(std::iter::once(batch)) {
                    self.context.check_cancelled()?;
                    writer.write_columns(&batch, &schema, memory)?;
                }
                drop(buffered.reservation);
                while let Some(batch) = self.next_batch(&mut input).await? {
                    writer.write_columns(&batch, &schema, memory)?;
                }
                return writer.finish().map(Replay::Spill);
            }
        }
        Ok(Replay::Memory(Arc::new(buffered)))
    }

    async fn next_batch(&self, input: &mut BoxBatchCursor) -> Result<Option<ColumnBatch>> {
        self.context.check_cancelled()?;
        let Some(batch) = poll_nested_batch(input.as_mut()).await? else { return Ok(None); };
        self.context.check_cancelled()?;
        batch.validate_readable(&self.context)?;
        Ok(Some(batch))
    }
}

struct MemoryReference {
    replay: Arc<BufferedReplay>,
    context: Arc<QueryContext>,
    next: usize,
}

#[async_trait]
impl BatchCursor for MemoryReference {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.context.check_cancelled()?;
        let Some(batch) = self.replay.pages.get(self.next) else { return Ok(None); };
        let batch = batch.duplicate_for_replay(&self.context.memory)?;
        self.next += 1;
        Ok(Some(batch))
    }
}

struct ColumnReference { reader: SpillReader, context: Arc<QueryContext>, width: usize }

impl ColumnReference {
    fn new(file: &Arc<SpillFile>, context: &Arc<QueryContext>, width: usize) -> Result<Self> {
        Ok(Self { reader: file.reader(context)?, context: Arc::clone(context), width })
    }
}

#[async_trait]
impl BatchCursor for ColumnReference {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.context.check_cancelled()?;
        self.reader.read_columns(self.width)
    }
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
    use crate::{CatalogSnapshot, DatabaseSnapshot, QueryOptions, cursor::OwnedRow, pages::PageBuilder};

    struct GateInput { emitted: bool, started: Option<oneshot::Sender<()>>, release: oneshot::Receiver<()>, closed: Arc<AtomicBool>, memory: Arc<MemoryBudget> }
    #[async_trait]
    impl BatchCursor for GateInput {
        async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
            if !self.emitted {
                self.emitted = true;
                let mut page = PageBuilder::new(1,&self.memory)?;
                page.push(OwnedRow::new(vec![Value::I64(1)],&self.memory)?)?;
                return ColumnBatch::from_page(&page.finish(), &self.memory).map(Some);
            }
            if let Some(started) = self.started.take() { let _ = started.send(()); }
            let _ = (&mut self.release).await;
            Ok(None)
        }
    }
    impl Drop for GateInput { fn drop(&mut self) { self.closed.store(true, Ordering::SeqCst); } }

    struct Pages { pages: VecDeque<Vec<i64>>, polls: Arc<AtomicUsize>, memory: Arc<MemoryBudget> }
    #[async_trait]
    impl BatchCursor for Pages {
        async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
            self.polls.fetch_add(1,Ordering::SeqCst);
            let Some(values) = self.pages.pop_front() else { return Ok(None); };
            if values.is_empty() { return ColumnBatch::empty_with_width(1, &self.memory).map(Some); }
            let mut page = PageBuilder::new(values.len(),&self.memory)?;
            for value in values { page.push(OwnedRow::new(vec![Value::I64(value)],&self.memory)?)?; }
            ColumnBatch::from_page(&page.finish(), &self.memory).map(Some)
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
            let scope = BindingScope::columns(Arc::clone(&definition),input,definition.relation.schema().clone(),None,Arc::clone(&context)).unwrap();
            let mut first = scope.open_columns(&definition).await.unwrap();
            assert_eq!(polls.load(Ordering::SeqCst), 4);
            assert_eq!(context.spill_bytes() > 0, materialized_spill_rows < 3);
            let mut second = scope.open_columns(&definition).await.unwrap();assert_eq!(polls.load(Ordering::SeqCst),4);
            drop(scope);
            for cursor in [&mut first,&mut second] {
                let page = cursor.next_batch().await.unwrap().unwrap();assert_eq!(page.len,2);
                assert_eq!(page.columns[0].value(0).unwrap(),Value::I64(1));assert_eq!(page.columns[0].value(1).unwrap(),Value::I64(2));
                let empty = cursor.next_batch().await.unwrap().unwrap();
                assert_eq!((empty.len, empty.columns.len()), (0, 1));
                assert_eq!(cursor.next_batch().await.unwrap().unwrap().columns[0].value(0).unwrap(),Value::I64(3));
                assert!(cursor.next_batch().await.unwrap().is_none());
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
    async fn a_binding_spills_past_its_configured_bytes_and_not_for_what_else_holds_its_budget() {
        for (materialized_memory_bytes, crowded, spills) in [(1, false, true), (64 << 20, true, false)] {
            let context = context_with_options(QueryOptions { materialized_memory_bytes, ..Default::default() });
            let definition = definition();
            let _other = crowded.then(|| context.memory.reserve(context.memory.available_bytes() - (1 << 20)).unwrap());
            let pages = VecDeque::from(vec![vec![1, 2]; 64]);
            let input = Box::new(Pages { pages, polls: Arc::new(AtomicUsize::new(0)), memory: Arc::clone(&context.memory) });
            let scope = BindingScope::columns(Arc::clone(&definition), input, definition.relation.schema().clone(), None, Arc::clone(&context)).unwrap();
            let mut reader = scope.open_columns(&definition).await.unwrap();
            assert_eq!(context.spill_bytes() > 0, spills, "{materialized_memory_bytes}");
            let mut rows = 0;
            while let Some(page) = reader.next_batch().await.unwrap() { rows += page.len; }
            assert_eq!(rows, 128);
        }
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
    async fn replayed_arrow_pages_share_funding_after_both_readers_and_the_binding_close() {
        use arrow_array::{ArrayRef, Int64Array};
        use arrow_schema::{DataType, Field as ArrowField};
        use crate::columnar::SelectedColumn;

        let rows = 1024;
        for selected in [false, true] {
            let context = context(1);
            let memory = &context.memory;
            let definition = definition();
            let array: ArrayRef = Arc::new(Int64Array::from_iter_values((0..rows).map(|row| row as i64)));
            let array_bytes = array.get_array_memory_size();
            let field = Arc::new(ArrowField::new("n", DataType::Int64, false));
            let logical = Some(LogicalType::Scalar(ScalarType::I64));
            let (column, shared_bytes) = if selected {
                let owner = memory.reserve(array_bytes).unwrap();
                let picks = (0..rows).rev().map(|row| (0, row)).collect::<Vec<_>>();
                let shared_bytes = array_bytes + picks.len() * std::mem::size_of::<(usize, usize)>();
                let selection = SelectedColumn::new(
                    vec![Some((array, field, logical, Some(owner)))], Arc::new(picks),
                );
                (Column::Selected(selection), shared_bytes)
            } else {
                (Column::Arrow { array, field, logical }, array_bytes)
            };
            let mut batch = ColumnBatch::settled(vec![], rows, memory.reserve(256).unwrap()).unwrap();
            batch.append_column(column, memory.reserve(shared_bytes).unwrap(), memory).unwrap();
            let input = crate::columnar::single_batch(Some(batch), Arc::clone(&context));
            let scope = BindingScope::columns(
                Arc::clone(&definition), input, definition.relation.schema().clone(), None, Arc::clone(&context),
            ).unwrap();
            let mut first = scope.open_columns(&definition).await.unwrap();
            let mut second = scope.open_columns(&definition).await.unwrap();
            assert_eq!(context.spill_bytes(), 0);

            let pressure = memory.reserve(memory.available_bytes() - 4096).unwrap();
            let first_page = first.next_batch().await.unwrap().unwrap();
            let last_page = second.next_batch().await.unwrap().unwrap();
            assert_eq!((first_page.len, last_page.len), (rows, rows));
            assert!(first.next_batch().await.unwrap().is_none());
            assert!(second.next_batch().await.unwrap().is_none());
            drop((pressure, first, second, scope, first_page));

            assert!(memory.used_bytes() >= shared_bytes);
            assert_eq!(last_page.working_set_bytes(memory).unwrap(), memory.used_bytes());
            let expected = if selected { (rows - 1) as i64 } else { 0 };
            assert_eq!(last_page.columns[0].value(0).unwrap(), Value::I64(expected));
            drop(last_page);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn replay_admits_value_copies_and_retains_packed_children() {
        use arrow_array::Float32Array;
        use rtdb_types::{ListValue, StructFields};

        for packed in [false, true] {
            let memory = MemoryBudget::new(1 << 20);
            let source = memory.scope();
            let mut fields = StructFields::from([("text".into(), Value::Utf8("x".repeat(2048)))]);
            if packed {
                let list = ListValue::from_f32(Float32Array::from(vec![1.0; 16384]));
                fields.insert("list".into(), Value::List(list));
            }
            let value = Value::Struct(fields);
            let batch = ColumnBatch::settled(
                vec![Column::Values(vec![value.clone()])], 1, source.reserve(0).unwrap(),
            ).unwrap();
            let too_small = memory.scope_with_limit(1024);
            let error = batch.duplicate_for_replay(&too_small).err().unwrap();
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            assert_eq!(too_small.used_bytes(), 0);

            let copies = memory.scope_with_limit(8192);
            let replayed = batch.duplicate_for_replay(&copies).unwrap();
            drop(batch);
            assert_eq!(source.used_bytes() > 0, packed);
            assert!(copies.used_bytes() >= 2048);
            assert_eq!(replayed.working_set_bytes(&memory).unwrap(), memory.used_bytes());
            assert_eq!(replayed.columns[0].value(0).unwrap(), value);
            drop(value);
            drop(replayed);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn canceled_reference_rejects_the_next_page_without_retaining_a_partial_page() {
        let context = context(1);let definition = definition();
        let input = Box::new(Pages { pages: VecDeque::from([vec![1],vec![2,3]]),polls: Arc::new(AtomicUsize::new(0)),memory: Arc::clone(&context.memory) });
        let scope = BindingScope::columns(Arc::clone(&definition),input,definition.relation.schema().clone(),None,Arc::clone(&context)).unwrap();
        let mut cursor = scope.open_columns(&definition).await.unwrap();drop(cursor.next_batch().await.unwrap().unwrap());
        context.cancellation.cancel();assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb/query-cancelled");
        drop(cursor);drop(scope);assert_eq!(context.spill_bytes(),0);assert_eq!(context.memory.used_bytes(),0);
    }

    #[tokio::test]
    async fn incomplete_framed_spill_is_rejected_without_retaining_output_rows() {
        let context = context(1);let mut writer = context.spill.create().unwrap();
        writer.write_frame(&[1]).unwrap();
        let file = writer.finish().unwrap();
        let mut cursor = ColumnReference::new(&file, &context, 1).unwrap();
        assert_eq!(cursor.next_batch().await.err().unwrap().code, "rtdb.query/invalid-spill");
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
        let scope = BindingScope::columns(Arc::clone(&definition), input, definition.relation.schema().clone(), None, Arc::clone(&context)).unwrap();
        let opening = tokio::spawn(async move { scope.open_columns(&definition).await });
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
