use std::{num::NonZeroUsize, sync::Arc};

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::{limit::LimitStream, metrics::{BaselineMetrics, ExecutionPlanMetricsSet}, stream::RecordBatchStreamAdapter};
use futures::StreamExt;
use rtdb_columnar::{MemoryBudget, to_field};
use rtdb_plan::Schema;
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType};

use crate::{columnar::{BoxBatchCursor, Column, rows_from_columns}, cursor::column_batch, invalid, pages::{OwnedPage, PageCursor}};

pub(crate) struct DataFusionLimit { stream: LimitStream, memory: Arc<MemoryBudget> }

impl DataFusionLimit {
    pub(crate) fn supports(schema: &Schema) -> bool {
        fn plain(data_type: &LogicalType) -> bool {
            match data_type {
                LogicalType::Maybe(inner) => plain(inner),
                LogicalType::Scalar(scalar) => matches!(scalar, ScalarType::Bool | ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64 | ScalarType::F32 | ScalarType::F64 | ScalarType::Utf8),
                _ => false,
            }
        }
        schema.fields().iter().all(|field| plain(&field.data_type))
    }

    pub(crate) fn new(mut input: BoxBatchCursor, schema: Schema, memory: Arc<MemoryBudget>, offset: usize, fetch: NonZeroUsize) -> Result<Self> {
        let fields = schema.fields().iter().map(|field| to_field(&field.name, &field.data_type)).collect::<Result<Vec<_>>>()?;
        let arrow = Arc::new(ArrowSchema::new(fields));
        let input_memory = Arc::clone(&memory);
        let input = async_stream::try_stream! {
            while let Some(batch) = input.next_batch().await.map_err(external)? {
                if batch.len != 0 {
                    let batch = column_batch(&schema, &input_memory, batch).map_err(external)?;
                    yield batch.batch().clone();
                }
            }
        };
        let metrics = BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let stream = LimitStream::new(Box::pin(RecordBatchStreamAdapter::new(arrow, input)), offset, Some(fetch.get()), metrics);
        Ok(Self { stream, memory })
    }
}

#[async_trait]
impl PageCursor for DataFusionLimit {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        let Some(batch) = self.stream.next().await.transpose().map_err(anomaly)? else { return Ok(None); };
        let schema = batch.schema();
        let columns = batch.columns().iter().zip(schema.fields()).map(|(array, field)| Column::Arrow { array: Arc::clone(array), field: Arc::clone(field), logical: None }).collect::<Vec<_>>();
        rows_from_columns(&columns, batch.num_rows(), &self.memory, false).map(Some)
    }
}

fn external(error: Anomaly) -> DataFusionError { DataFusionError::External(Box::new(error)) }

fn anomaly(error: DataFusionError) -> Anomaly {
    match error {
        DataFusionError::External(error) => error.downcast::<Anomaly>().map(|error| *error).unwrap_or_else(|error| invalid("datafusion-limit", error)),
        error => invalid("datafusion-limit", error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::VecDeque, sync::atomic::{AtomicBool, Ordering}};
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field as ArrowField};
    use rtdb_plan::{ColumnId, Field};
    use crate::columnar::{BatchCursor, ColumnBatch};

    struct ObservedInput { pages: VecDeque<ColumnBatch>, dropped: Arc<AtomicBool> }

    #[async_trait]
    impl BatchCursor for ObservedInput {
        async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { Ok(self.pages.pop_front()) }
    }

    impl Drop for ObservedInput {
        fn drop(&mut self) { self.dropped.store(true, Ordering::Relaxed); }
    }

    #[tokio::test]
    async fn the_final_slice_keeps_its_input_charge_after_limit_drops_the_source() {
        let memory = MemoryBudget::new(1 << 20);
        let data_type = LogicalType::Scalar(ScalarType::I64);
        let schema = Schema::new(vec![Field { id: ColumnId(0), name: "v".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: data_type.clone() }]).unwrap();
        let batch = |values: Vec<i64>| {
            let len = values.len();
            let field = Arc::new(ArrowField::new("v", DataType::Int64, false));
            let array = Arc::new(Int64Array::from(values));
            ColumnBatch::settled(vec![Column::Arrow { array, field, logical: Some(data_type.clone()) }], len, memory.reserve(0).unwrap()).unwrap()
        };
        let skipped = batch(vec![1, 2]);
        let retained = batch(vec![3, 4, 5]);
        let retained_bytes = retained.charged();
        let dropped = Arc::new(AtomicBool::new(false));
        let input = ObservedInput { pages: VecDeque::from([skipped, retained]), dropped: Arc::clone(&dropped) };
        let mut limit = DataFusionLimit::new(Box::new(input), schema, Arc::clone(&memory), 3, NonZeroUsize::new(1).unwrap()).unwrap();
        let output = limit.stream.next().await.unwrap().unwrap();
        assert!(dropped.load(Ordering::Relaxed));
        assert_eq!(memory.used_bytes(), retained_bytes);
        assert_eq!(output.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().as_ref(), &[4]);
        drop(output);
        assert_eq!(memory.used_bytes(), 0);
        assert!(limit.stream.next().await.is_none());
    }
}
