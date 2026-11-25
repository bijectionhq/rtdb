use std::sync::Arc;
use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget,MemoryReservation};
use rtdb_types::Result;
use crate::{QueryContext, invalid};
use crate::columnar::{BatchCursor, BoxBatchCursor, ColumnBatch, poll_nested_batch};

struct LeftPages { pages: Vec<ColumnBatch>,capacity: usize,_reservation: Arc<MemoryReservation> }
impl LeftPages {
    fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { pages: Vec::new(),capacity: 0,_reservation: memory.reserve(256)? })
    }
    fn push(&mut self,page: ColumnBatch,memory: &Arc<MemoryBudget>) -> Result<()> {
        if self.pages.len() == self.capacity {
            let capacity = self.capacity.checked_mul(2).map(|capacity|capacity.max(1)).ok_or_else(size_error)?;
            let bytes = capacity.checked_mul(std::mem::size_of::<ColumnBatch>()).and_then(|bytes|bytes.checked_add(256)).ok_or_else(size_error)?;
            let reservation = memory.reserve(bytes)?;
            self.pages.reserve_exact(capacity - self.pages.len());
            self.capacity = capacity;
            self._reservation = reservation;
        }
        self.pages.push(page);
        Ok(())
    }
}

enum State {
    Pending { left: BoxBatchCursor,right: BoxBatchCursor },
    Ready { left: LeftPages,right: BoxBatchCursor,current: Option<ColumnBatch>,next_left: usize },
    Done,
}

pub(crate) struct CrossBatchJoin { state: State, context: Arc<QueryContext> }

impl CrossBatchJoin {
    pub fn new(left: BoxBatchCursor, right: BoxBatchCursor, context: Arc<QueryContext>) -> Self {
        Self { state: State::Pending { left, right }, context }
    }
}

#[async_trait]
impl BatchCursor for CrossBatchJoin {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.context.check_cancelled()?;
        if matches!(self.state,State::Pending { .. }) {
            let State::Pending { mut left,right } = std::mem::replace(&mut self.state,State::Done) else { unreachable!() };
            let mut pages = LeftPages::new(&self.context.memory)?;
            while let Some(page) = poll_nested_batch(left.as_mut()).await? {
                self.context.check_cancelled()?;
                pages.push(page,&self.context.memory)?;
            }
            self.state = State::Ready { left: pages,right,current: None,next_left: 0 };
        }
        let State::Ready { left,right,current,next_left } = &mut self.state else { return Ok(None); };
        if current.is_none() || *next_left == left.pages.len() {
            *current = None;
            *current = poll_nested_batch(right.as_mut()).await?;
            *next_left = 0;
        }
        let (Some(left),Some(right)) = (left.pages.get(*next_left),current.as_ref()) else { self.state = State::Done;return Ok(None); };
        let output = ColumnBatch::cross(left, right, product(left.len, right.len)?, &self.context)?;
        *next_left += 1;
        Ok(Some(output))
    }
}

fn product(left: usize, right: usize) -> Result<usize> {
    left.checked_mul(right).filter(|count|*count <= i32::MAX as usize).ok_or_else(size_error)
}

fn size_error() -> rtdb_types::Anomaly { invalid("cross-join-size","Cross join page exceeds its supported row or allocation size") }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cursor::OwnedRow, pages::PageBuilder};
    use std::{collections::{BTreeMap,VecDeque},sync::atomic::{AtomicBool,AtomicUsize,Ordering}};
    use rtdb_storage::{MemoryStore,cache::BufferPool};
    use rtdb_types::{Anomaly,Timestamp,Value};
    use tokio_util::sync::CancellationToken;
    use crate::{CatalogSnapshot,DatabaseSnapshot,QueryOptions};

    struct Input { pages: VecDeque<Vec<i64>>,polls: Arc<AtomicUsize>,closed: Arc<AtomicBool>,memory: Arc<MemoryBudget>,fail_at: Option<usize> }
    #[async_trait]
    impl BatchCursor for Input {
        async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
            let poll = self.polls.fetch_add(1,Ordering::SeqCst);
            if self.fail_at == Some(poll) { return Err(Anomaly::fault("test/input","Input failed")); }
            let Some(values) = self.pages.pop_front() else { return Ok(None); };
            if values.is_empty() { return ColumnBatch::empty_with_width(1, &self.memory).map(Some); }
            let mut page = PageBuilder::new(values.len(),&self.memory)?;
            for value in values { page.push(OwnedRow::new(vec![Value::I64(value)],&self.memory)?)?; }
            ColumnBatch::from_page(&page.finish(), &self.memory).map(Some)
        }
    }
    impl Drop for Input { fn drop(&mut self) { self.closed.store(true,Ordering::SeqCst); } }
    fn context(batch_rows: usize) -> Arc<QueryContext> {
        let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
        QueryContext::new(catalog,MemoryBudget::new(1 << 24),Timestamp(0),"UTC",QueryOptions { batch_rows,..Default::default() },&CancellationToken::new()).unwrap()
    }
    fn input(pages: Vec<Vec<i64>>,context: &Arc<QueryContext>) -> (Input,Arc<AtomicUsize>,Arc<AtomicBool>) {
        let polls = Arc::new(AtomicUsize::new(0));let closed = Arc::new(AtomicBool::new(false));
        (Input { pages: pages.into(),polls: Arc::clone(&polls),closed: Arc::clone(&closed),memory: Arc::clone(&context.memory),fail_at: None },polls,closed)
    }
    fn values(page: ColumnBatch) -> Vec<Vec<Value>> {
        (0..page.len).map(|row| page.columns.iter().map(|column| column.value(row).unwrap()).collect()).collect()
    }

    #[tokio::test]
    async fn cross_join_retains_left_pages_and_emits_left_major_rows_for_each_page_pair() {
        for batch_rows in [1,1024] {
            let context = context(batch_rows);
            let (left,left_polls,left_closed) = input(vec![vec![1,2],vec![3]],&context);
            let (right,right_polls,right_closed) = input(vec![vec![10,20],vec![30]],&context);
            let mut cursor = CrossBatchJoin::new(Box::new(left),Box::new(right),Arc::clone(&context));
            assert_eq!(values(cursor.next_batch().await.unwrap().unwrap()),vec![vec![Value::I64(1),Value::I64(10)],vec![Value::I64(1),Value::I64(20)],vec![Value::I64(2),Value::I64(10)],vec![Value::I64(2),Value::I64(20)]]);
            assert_eq!(left_polls.load(Ordering::SeqCst),3);assert!(left_closed.load(Ordering::SeqCst));assert_eq!(right_polls.load(Ordering::SeqCst),1);
            assert_eq!(values(cursor.next_batch().await.unwrap().unwrap()),vec![vec![Value::I64(3),Value::I64(10)],vec![Value::I64(3),Value::I64(20)]]);
            assert_eq!(right_polls.load(Ordering::SeqCst),1);
            assert_eq!(cursor.next_batch().await.unwrap().unwrap().len,2);
            assert_eq!(cursor.next_batch().await.unwrap().unwrap().len,1);
            assert!(cursor.next_batch().await.unwrap().is_none());assert!(right_closed.load(Ordering::SeqCst));
            drop(cursor);assert_eq!(context.memory.used_bytes(),0);
        }
    }

    #[tokio::test]
    async fn cross_join_materializes_arrow_columns_in_left_major_order() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use crate::columnar::Column;

        struct Batches(VecDeque<ColumnBatch>);
        #[async_trait]
        impl BatchCursor for Batches {
            async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { Ok(self.0.pop_front()) }
        }

        let context = context(1);
        let input = |pages: Vec<Vec<i64>>| {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
            Box::new(Batches(pages.into_iter().map(|page| {
                let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(page))]).unwrap();
                ColumnBatch::from_owned(rtdb_columnar::OwnedBatch::new(batch, &context.memory).unwrap()).unwrap()
            }).collect())) as BoxBatchCursor
        };
        let mut cursor = CrossBatchJoin::new(input(vec![vec![1, 2], vec![3]]), input(vec![vec![10, 20]]), Arc::clone(&context));
        for expected in [[vec![1, 1, 2, 2], vec![10, 20, 10, 20]], [vec![3, 3], vec![10, 20]]] {
            let batch = cursor.next_batch().await.unwrap().unwrap();
            for (column, expected) in batch.columns.iter().zip(expected) {
                assert!(matches!(column, Column::Selected(_)));
                let (_, field, logical, _) = column.arrow_cell(0).unwrap().unwrap();
                let (array, _charge) = crate::columnar::relayout_output(column, logical.unwrap(), field, &context.memory).unwrap().unwrap();
                assert_eq!(array.as_any().downcast_ref::<Int64Array>().unwrap().values().as_ref(), expected.as_slice());
            }
        }
        assert!(cursor.next_batch().await.unwrap().is_none());
        drop(cursor);
        assert_eq!(context.memory.used_bytes(), 0);
    }

    #[test]
    fn cross_join_keeps_source_payloads_under_a_charge_independent_of_the_product() {
        use arrow_array::{ArrayRef, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        let context = context(1);
        let input = |values: ArrayRef| {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
            let batch = RecordBatch::try_new(schema, vec![values]).unwrap();
            ColumnBatch::from_owned(rtdb_columnar::OwnedBatch::new(batch, &context.memory).unwrap()).unwrap()
        };
        let left_source: ArrayRef = Arc::new(StringArray::from_iter_values((0..256).map(|row| format!("left-{row}"))));
        let right_source: ArrayRef = Arc::new(StringArray::from_iter_values((0..256).map(|row| format!("right-{row}"))));
        let left = input(Arc::clone(&left_source));
        let right = input(Arc::clone(&right_source));
        let before = context.memory.used_bytes();
        let result = ColumnBatch::cross(&left, &right, product(left.len, right.len).unwrap(), &context).unwrap();
        assert!(context.memory.used_bytes() - before < 16 * 1024);
        drop((left, right));

        let result = result.slice(255, 2, &context.memory).unwrap();
        for (column, source, positions) in [
            (&result.columns[0], left_source, [0, 1]),
            (&result.columns[1], right_source, [255, 0]),
        ] {
            for (row, position) in positions.into_iter().enumerate() {
                let (array, _, _, offset) = column.arrow_cell(row).unwrap().unwrap();
                assert!(std::ptr::eq(array, source.as_ref()));
                assert_eq!(offset, position);
            }
        }
        assert_eq!(result.working_set_bytes(&context.memory).unwrap(), context.memory.used_bytes());
        drop(result);
        assert_eq!(context.memory.used_bytes(), 0);
    }

    #[test]
    fn cross_join_checks_the_product_before_expanding() {
        let limit = i32::MAX as usize;
        assert_eq!(product(limit, 1).unwrap(), limit);
        assert_eq!(product(limit, 2).unwrap_err().code, "rtdb.query/cross-join-size");
        assert_eq!(product(usize::MAX, 2).unwrap_err().code, "rtdb.query/cross-join-size");
    }

    #[test]
    fn cross_join_repeats_null_cells_without_their_hidden_binary_bytes() {
        use arrow_array::{BinaryArray, Int64Array, RecordBatch};
        use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
        use arrow_schema::{DataType, Field, Schema};
        use crate::columnar::Column;

        let context = context(1);
        let hidden = 1 << 20;
        let bytes = BinaryArray::new(OffsetBuffer::new(vec![0, hidden as i32].into()),
            Buffer::from_vec(vec![b'x'; hidden]), Some(NullBuffer::new_null(1)));
        let left = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("bytes", DataType::Binary, true)])), vec![Arc::new(bytes)]).unwrap();
        let right = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("number", DataType::Int64, false)])), vec![Arc::new(Int64Array::from(vec![0; 8]))]).unwrap();
        let left = ColumnBatch::from_owned(rtdb_columnar::OwnedBatch::new(left, &context.memory).unwrap()).unwrap();
        let right = ColumnBatch::from_owned(rtdb_columnar::OwnedBatch::new(right, &context.memory).unwrap()).unwrap();
        let result = ColumnBatch::cross(&left, &right, 8, &context).unwrap();
        let Column::Selected(_) = &result.columns[0] else { panic!("Expected selected null cells"); };
        drop(left);
        for row in 0..result.len {
            assert_eq!(result.columns[0].value(row).unwrap(), Value::Null);
        }
        let ty = rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Binary).maybe();
        let field = rtdb_columnar::to_field("bytes", &ty).unwrap();
        let (array, _reservation) = crate::columnar::relayout_output(&result.columns[0], &ty, &field, &context.memory).unwrap().unwrap();
        assert!(array.as_any().downcast_ref::<BinaryArray>().unwrap().value_data().is_empty());
    }

    #[tokio::test]
    async fn empty_cross_pages_are_emitted_but_absent_left_still_advances_first_right_page() {
        let context = context(1);
        let (left,_,_) = input(vec![vec![],vec![1]],&context);
        let (right,_,_) = input(vec![vec![],vec![2]],&context);
        let mut cursor = CrossBatchJoin::new(Box::new(left),Box::new(right),Arc::clone(&context));
        let mut sizes = Vec::new();while let Some(page) = cursor.next_batch().await.unwrap() { sizes.push(page.len); }
        assert_eq!(sizes,[0,0,0,1]);drop(cursor);assert_eq!(context.memory.used_bytes(),0);
        let (left,_,_) = input(vec![],&context);let (right,polls,_) = input(vec![vec![1],vec![2]],&context);
        let mut cursor = CrossBatchJoin::new(Box::new(left),Box::new(right),Arc::clone(&context));
        assert!(cursor.next_batch().await.unwrap().is_none());assert_eq!(polls.load(Ordering::SeqCst),1);
        drop(cursor);assert_eq!(context.memory.used_bytes(),0);
    }

    #[tokio::test]
    async fn later_left_page_failure_precedes_all_output_and_does_not_poll_right() {
        let context = context(1);let (mut left,_,left_closed) = input(vec![vec![1]],&context);left.fail_at = Some(1);
        let (right,polls,right_closed) = input(vec![vec![1]],&context);
        let mut cursor = CrossBatchJoin::new(Box::new(left),Box::new(right),Arc::clone(&context));
        assert_eq!(cursor.next_batch().await.err().unwrap().code,"test/input");assert_eq!(polls.load(Ordering::SeqCst),0);
        assert!(left_closed.load(Ordering::SeqCst));assert!(right_closed.load(Ordering::SeqCst));assert_eq!(context.memory.used_bytes(),0);
    }

    #[tokio::test]
    async fn canceled_cross_cursor_releases_retained_inputs_but_returned_page_keeps_its_charge() {
        let context = context(1);let (left,_,_) = input(vec![vec![1],vec![2]],&context);let (right,_,_) = input(vec![vec![3]],&context);
        let mut cursor = CrossBatchJoin::new(Box::new(left),Box::new(right),Arc::clone(&context));
        let page = cursor.next_batch().await.unwrap().unwrap();context.cancellation.cancel();
        assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb/query-cancelled");drop(cursor);assert!(context.memory.used_bytes() > 0);
        assert_eq!(values(page),vec![vec![Value::I64(1),Value::I64(3)]]);assert_eq!(context.memory.used_bytes(),0);
    }
}
