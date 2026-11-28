use std::{borrow::Cow, collections::BTreeSet, io::Write, sync::Arc};

use arrow_array::{ArrayRef, Float64Array, builder::BinaryBuilder};
use arrow_schema::DataType;
use rtdb_columnar::{ArrowCell, EntityIdRef, MemoryBudget, estimate_scalar_builder_bytes};
use rtdb_index::branches::RankedIndexDeclaration;
use rtdb_plan::{Binder, Catalog, Expr, QualifiedTable, Relation, Scan, Schema, TemporalFilter};
use rtdb_query::{CandidateIids, PreparedQuery, QueryContext, QueryCursor};
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, FloatComponent, Interval, LogicalType, NumericCell, Result,
    ScalarType, TableRef, TemporalBounds, TimeUnit, Timestamp, Value,
};
use rtdb_vector::{Cut, Partition, Round, Shortlist, UnitProbe, VectorDefinition};

use super::{
    HeldVectorOverlay, MAX_NEAREST_FILTERS, NearestHit, NearestQuery, NearestWindow, identity_charge,
    not_a_vector, run_vector_span,
};

pub struct NativeNearest<'a> {
    pub table: &'a TableRef,
    pub index: &'a str,
    pub declaration: &'a RankedIndexDeclaration,
    pub valid: Timestamp,
    pub filters: &'a [String],
}

impl NativeNearest<'_> {
    pub async fn query(
        &self,
        context: Arc<QueryContext>,
        query: &NearestQuery,
        overlay: &HeldVectorOverlay,
    ) -> Result<(QueryCursor, usize, bool)> {
        self.query_in(context, query, overlay, None).await
    }

    pub async fn query_mounted(&self, context: Arc<QueryContext>, query: &NearestQuery, overlay: &HeldVectorOverlay,
        definition: &VectorDefinition) -> Result<(QueryCursor, usize, bool)> {
        self.query_in(context, query, overlay, Some(definition)).await
    }

    async fn query_in(&self, context: Arc<QueryContext>, query: &NearestQuery, overlay: &HeldVectorOverlay,
        mounted: Option<&VectorDefinition>) -> Result<(QueryCursor, usize, bool)> {
        if self.filters.len() != query.filter_count() {
            return Err(native_uncovered());
        }
        let context = context.with_plaintext();
        rtdb_types::boxed(|| context.reading(self.rank(&context, query, overlay, mounted))).await
    }

    async fn rank(
        &self,
        context: &Arc<QueryContext>,
        query: &NearestQuery,
        overlay: &HeldVectorOverlay,
        mounted: Option<&VectorDefinition>,
    ) -> Result<(QueryCursor, usize, bool)> {
        let database = context
            .catalog
            .database(context.catalog.default_database())?;
        let [snapshot] = database.partitions.as_slice() else {
            return Err(native_uncovered());
        };
        let system = context.current_time.as_micros()?;
        let bounds = TemporalBounds {
            valid_time: Interval::at(self.valid)?,
            system_time: Interval::at(system)?,
        };
        let cut = Cut {
            snapshot,
            storage: &database.storage,
            memory: &context.memory,
            cancellation: &context.cancellation,
            bounds,
        };
        let (column, partition_columns) = self
            .declaration
            .definition
            .columns
            .split_first()
            .ok_or_else(native_uncovered)?;
        let partition = self.partition(partition_columns, query)?;
        let probe = UnitProbe::from_slice(&query.probe, &context.memory)?.ok_or_else(native_uncovered)?;
        let shortlist = match mounted {
            Some(definition) => Shortlist::open_mounted(context, bounds, definition, probe, query.k, partition).await?,
            None => Shortlist::open_native(context, bounds, self.table, self.index, self.declaration, probe, query.k, partition).await?,
        };
        let Some(mut shortlist) = shortlist else {
            return Err(native_uncovered());
        };
        loop {
            let Some(Round {
                candidates,
                complete,
            }) = shortlist.round(cut, None).await?
            else {
                return Err(native_uncovered());
            };
            let mut window = NearestWindow::new(&context.memory, query.k)?;
            if !candidates.is_empty() {
                let cursor = self
                    .resolve(context, column, candidates)
                    .await?;
                self.rank_rows(context, cursor, query, overlay.shadowed(0), &mut window)
                    .await?;
            }
            for (_, id, distance) in &overlay.rows {
                if let Some(distance) = (*distance)? {
                    window.admit(0, EntityIdRef::from(id), distance)?;
                }
            }
            if window.total >= query.k || complete {
                let total = window.total;
                return Ok((finish(context, window)?, total, !complete));
            }
            if !shortlist.widen() {
                return Err(native_uncovered());
            }
        }
    }

    fn partition(&self, columns: &[String], query: &NearestQuery) -> Result<Option<Partition>> {
        let values: Option<Vec<_>> = columns
            .iter()
            .map(|column| {
                let index = self.filters.iter().position(|filter| filter == column)?;
                let values = &query.filters[index];
                values.first().filter(|_| values.len() == 1)
            })
            .collect();
        values
            .map(|values| {
                Partition::of(
                    values
                        .into_iter()
                        .map(|value| Some(Cow::Owned(Value::Binary(value.clone())))),
                )
            })
            .transpose()
    }

    async fn resolve(
        &self,
        context: &Arc<QueryContext>,
        column: &str,
        iids: CandidateIids,
    ) -> Result<QueryCursor> {
        let name = QualifiedTable {
            database: context.catalog.default_database().into(),
            table: self.table.clone(),
        };
        let table = context.catalog.table(&name)?.ok_or_else(native_uncovered)?;
        let mut binder = Binder::default();
        let columns = ["_id", column]
            .into_iter()
            .chain(self.filters.iter().map(String::as_str))
            .map(|name| binder.field(name, None, table.scan_column_type(name)))
            .collect::<Result<Vec<_>>>()?;
        let relation = Relation::scan(Scan {
            table,
            columns,
            iids: None,
            predicates: Vec::new(),
            matches: Vec::new(),
            valid_time: TemporalFilter::At(Expr::Literal(Value::Timestamp {
                value: self.valid.0,
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".into()),
            })),
            system_time: TemporalFilter::Default,
            clamp_valid_time: false,
            top_k: None,
            ordered_top_k: None,
        })?;
        let context = Arc::clone(context).for_ranked_candidates(name, iids)?;
        PreparedQuery::new_in(Arc::new(relation), &context.memory)?
            .execute(context, Vec::new())
            .await
    }

    async fn rank_rows(
        &self,
        context: &Arc<QueryContext>,
        mut cursor: QueryCursor,
        query: &NearestQuery,
        shadows: Option<&BTreeSet<EntityId>>,
        window: &mut NearestWindow,
    ) -> Result<()> {
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            let schema = batch.schema_ref();
            for row in 0..batch.num_rows() {
                context.check_cancelled_at(row)?;
                let id =
                    rtdb_columnar::read_entity_id(schema.field(0), batch.column(0).as_ref(), row)?;
                if shadows
                    .is_some_and(|ids| ids.iter().any(|shadow| EntityIdRef::from(shadow) == id))
                {
                    continue;
                }
                let mut cells = [None; MAX_NEAREST_FILTERS];
                for (index, cell) in cells[..self.filters.len()].iter_mut().enumerate() {
                    let column = index + 2;
                    *cell = rtdb_columnar::binary_cell(
                        schema.field(column),
                        batch.column(column).as_ref(),
                        row,
                    )?;
                }
                if !query.admits(cells[..self.filters.len()].iter().copied()) {
                    continue;
                }
                let cell = ArrowCell {
                    field: schema.field(1),
                    array: batch.column(1).as_ref(),
                    row,
                };
                if let Some(distance) = score_f32(&context.memory, cell, query)? {
                    window.admit(0, id, distance)?;
                }
            }
        }
        Ok(())
    }
}

fn score_f32(
    memory: &Arc<MemoryBudget>,
    cell: ArrowCell<'_>,
    query: &NearestQuery,
) -> Result<Option<f64>> {
    if cell.is_null()? {
        return Ok(None);
    }
    let score = |values: rtdb_types::ListView<'_>, range: std::ops::Range<usize>| {
        query
            .distance(range.map(|index| match values.typed_float(index) {
                Some(FloatComponent::F32(value)) => f64::from(value),
                _ => f64::NAN,
            }))
            .map_err(Anomaly::from)
    };
    if let Some((values, range)) = cell.float_list_span() {
        return score(values, range);
    }
    let bytes = rtdb_columnar::estimate_read_value_bytes(cell.field, cell.array, cell.row)?;
    let _cell = memory.reserve(bytes.saturating_mul(2))?;
    if let Some((DataType::Float32, span)) = run_vector_span(cell) {
        return query.distance(span.iter().map(|value| match value {
            NumericCell::F32(value) => f64::from(value),
            _ => f64::NAN,
        })).map_err(Anomaly::from);
    }
    let value = rtdb_columnar::read_execution_value(cell.field, cell.array, cell.row)?;
    let (Value::List(values) | Value::FixedSizeList(values)) = &value else {
        return Err(not_a_vector());
    };
    let values = values.view();
    score(values, 0..values.len())
}

#[test]
fn native_nearest_scores_run_encoded_f32_and_preserves_admission_and_component_errors() {
    use rtdb_types::{ListKind, RunEndWidth};

    let logical = LogicalType::List {
        kind: ListKind::FixedSizeList(2),
        element: Box::new(LogicalType::Scalar(ScalarType::F32).maybe().run_end_encoded(RunEndWidth::I16).unwrap()),
    };
    let array = rtdb_columnar::build_array(&logical, &[
        Value::FixedSizeList(vec![Value::F32(3.0), Value::F32(0.0)].into()),
        Value::FixedSizeList(vec![Value::Null, Value::F32(1.0)].into()),
        Value::FixedSizeList(vec![Value::F32(f32::NAN), Value::F32(1.0)].into()),
    ]).unwrap();
    let field = rtdb_columnar::to_field("vector", &logical).unwrap();
    let cell = ArrowCell { field: &field, array: array.as_ref(), row: 0 };
    let query = NearestQuery::new(vec![1.0, 0.0], Vec::new(), 1).unwrap();
    let memory = MemoryBudget::new(1 << 20);
    assert_eq!(score_f32(&memory, cell, &query).unwrap(), Some(0.0));
    let error = Anomaly::from(super::InvalidComponents);
    assert_eq!(score_f32(&memory, ArrowCell { row: 1, ..cell }, &query).unwrap_err(), error);
    assert_eq!(score_f32(&memory, ArrowCell { row: 2, ..cell }, &query).unwrap_err(), error);
    let decode = rtdb_columnar::estimate_read_value_bytes(&field, array.as_ref(), 0).unwrap().saturating_mul(2);
    assert_eq!(score_f32(&MemoryBudget::new(decode), cell, &query).unwrap(), Some(0.0));
    assert_eq!(score_f32(&MemoryBudget::new(decode - 1), cell, &query).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), 0);
}

fn reply_bytes(hits: &[NearestHit]) -> usize {
    hits.iter().fold(4096usize, |bytes, hit| {
        bytes
            .saturating_add(identity_charge(EntityIdRef::from(&hit.id)))
            .saturating_add(512)
    })
}

fn reply_schema() -> Result<Schema> {
    let mut binder = Binder::default();
    Schema::new(vec![
        binder.field(
            "identity_key",
            None,
            LogicalType::Scalar(ScalarType::Binary),
        )?,
        binder.field("distance", None, LogicalType::Scalar(ScalarType::F64))?,
    ])
}

fn handle_len(hit: &NearestHit) -> usize {
    1 + EntityIdRef::from(&hit.id).handle_parts(&mut [0; 8]).1.len()
}

fn reply_column_bound(column: usize, logical: &LogicalType, hits: &[NearestHit]) -> Result<usize> {
    match column {
        0 => estimate_scalar_builder_bytes(
            logical,
            hits.iter()
                .map(|hit| Some((&ScalarType::Binary, handle_len(hit)))),
        ),
        1 => {
            estimate_scalar_builder_bytes(logical, hits.iter().map(|_| Some((&ScalarType::F64, 0))))
        }
        _ => unreachable!("The native IVF reply has two columns"),
    }
}

fn write_reply_columns(hits: &[NearestHit]) -> Vec<ArrayRef> {
    let mut keys = BinaryBuilder::with_capacity(hits.len(), hits.iter().map(handle_len).sum());
    for hit in hits {
        let mut integer = [0; 8];
        let id = EntityIdRef::from(&hit.id);
        let (tag, payload) = id.handle_parts(&mut integer);
        keys.write_all(&[tag])
            .expect("Writing to an Arrow byte buffer is infallible");
        keys.append_value(payload);
    }
    let distances = Float64Array::from_iter_values(hits.iter().map(|hit| hit.distance));
    vec![Arc::new(keys.finish()), Arc::new(distances)]
}

fn finish(context: &Arc<QueryContext>, mut window: NearestWindow) -> Result<QueryCursor> {
    let reservation = context.memory.reserve(reply_bytes(&window.hits))?;
    let schema = reply_schema()?;
    // The reply grant covers the admitted hits, while the ranking buffer was sized for k.
    let mut hits = Vec::with_capacity(window.hits.len());
    hits.append(&mut window.hits);
    QueryCursor::from_typed_rows(
        schema,
        hits,
        Arc::clone(context),
        reservation,
        reply_column_bound,
        write_reply_columns,
    )
}

#[cfg(test)]
mod reply_tests {
    use std::sync::Arc;

    use arrow_array::{Array, BinaryArray, Float64Array};
    use rtdb_columnar::{EntityIdRef, MemoryBudget};
    use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryCursor, QueryOptions};
    use rtdb_storage::sealing::PlaintextDelivery;
    use rtdb_types::{EntityId, Keyword, Result, Timestamp, Value};
    use tokio_util::sync::CancellationToken;

    use super::{NearestWindow, finish, reply_bytes, reply_schema};
    use crate::nearest::MAX_NEAREST_K;

    fn context() -> Result<Arc<QueryContext>> {
        let database = DatabaseSnapshot {
            partitions: Vec::new(),
            storage: Arc::new(rtdb_storage::cache::BufferPool::new(
                Arc::new(rtdb_storage::MemoryStore::default()),
                1 << 20,
            )),
        };
        let catalog = CatalogSnapshot::new("xtdb", [("xtdb".into(), database)].into(), false)?;
        QueryContext::new(
            catalog,
            MemoryBudget::new(1 << 20),
            Timestamp(10),
            "UTC",
            QueryOptions {
                batch_rows: 3,
                ..QueryOptions::default()
            },
            &CancellationToken::new(),
        )
    }

    fn winners() -> [(EntityId, f64); 7] {
        [
            (EntityId::Binary(vec![]), -0.0),
            (EntityId::String("é".into()), 0.0),
            (EntityId::Keyword(Keyword::from_name("ns/key")), 0.0),
            (EntityId::Integer(i64::MIN), 0.0),
            (EntityId::Integer(i64::MAX), 0.0),
            (EntityId::Uuid([0xab; 16]), 0.0),
            (EntityId::Binary(vec![0xff, 0, 4]), 0.5),
        ]
    }

    fn window(context: &Arc<QueryContext>, hits: &[(EntityId, f64)]) -> Result<NearestWindow> {
        let mut window = NearestWindow::new(&context.memory, MAX_NEAREST_K)?;
        for (id, distance) in hits.iter().rev() {
            window.admit(0, EntityIdRef::from(id), *distance)?;
        }
        Ok(window)
    }

    fn legacy_rows(hits: &[(EntityId, f64)]) -> Vec<Vec<Value>> {
        hits.iter()
            .map(|(id, distance)| vec![Value::Binary(id.handle_bytes()), Value::F64(*distance)])
            .collect()
    }

    fn legacy_cursor(context: &Arc<QueryContext>, hits: &[(EntityId, f64)]) -> Result<QueryCursor> {
        let window = window(context, hits)?;
        let reservation = context.memory.reserve(reply_bytes(&window.hits))?;
        QueryCursor::from_rows(
            reply_schema()?,
            legacy_rows(hits),
            Arc::clone(context),
            reservation,
        )
    }

    #[tokio::test]
    async fn native_ivf_reply_preserves_typed_handles_distance_bits_and_page_boundaries()
    -> Result<()> {
        let ctx = context()?;
        let legacy = context()?;
        let hits = winners();
        let mut cursor = finish(&ctx, window(&ctx, &hits)?)?;
        let mut reference = legacy_cursor(&legacy, &hits)?;
        assert_eq!(ctx.memory.used_bytes(), legacy.memory.used_bytes());
        assert_eq!(ctx.memory.peak_bytes(), legacy.memory.peak_bytes());
        for expected in hits.chunks(ctx.options.batch_rows) {
            let page = cursor.next_batch().await?.unwrap();
            let previous = reference.next_batch().await?.unwrap();
            let batch = page.batch();
            assert_eq!(batch.schema_ref(), previous.batch().schema_ref());
            assert_eq!(batch.get_array_memory_size(), previous.batch().get_array_memory_size());
            assert_eq!(batch.num_rows(), expected.len());
            assert_eq!(ctx.memory.used_bytes(), legacy.memory.used_bytes());
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            let distances = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for (row, (id, distance)) in expected.iter().enumerate() {
                assert_eq!(keys.value(row), id.handle_bytes().as_slice());
                assert_eq!(distances.value(row).to_bits(), distance.to_bits());
            }
            assert_eq!((keys.null_count(), distances.null_count()), (0, 0));
        }
        assert!(cursor.next_batch().await?.is_none());
        assert!(reference.next_batch().await?.is_none());
        assert_eq!(ctx.memory.used_bytes(), legacy.memory.used_bytes());

        let mut empty = finish(&ctx, window(&ctx, &[])?)?;
        let pressure = ctx
            .memory
            .reserve(ctx.memory.available_bytes() - size_of::<PlaintextDelivery>())?;
        assert!(empty.next_batch().await?.is_none());
        drop(pressure);
        Ok(())
    }

    #[tokio::test]
    async fn native_ivf_reply_preserves_pull_refusals_and_cancellation_order() -> Result<()> {
        let hits = winners();
        let schema = reply_schema()?;
        let rows = legacy_rows(&hits[..3]);
        let mut page_bytes = 4096;
        for (column, field) in schema.fields().iter().enumerate() {
            page_bytes += rtdb_columnar::estimate_builder_bytes_refs(
                &field.data_type,
                rows.iter().map(|row| &row[column]),
            )?;
        }
        for (available, cancelled) in [(page_bytes - 1, false), (page_bytes, false), (0, true)] {
            let ctx = context()?;
            let legacy = context()?;
            let mut cursor = finish(&ctx, window(&ctx, &hits)?)?;
            let mut reference = legacy_cursor(&legacy, &hits)?;
            let _pressure = ctx
                .memory
                .reserve(ctx.memory.available_bytes() - available)?;
            let _legacy_pressure = legacy
                .memory
                .reserve(legacy.memory.available_bytes() - available)?;
            if cancelled {
                cursor.context().cancellation.cancel();
                reference.context().cancellation.cancel();
            }
            let error = cursor.next_batch().await.unwrap_err();
            assert_eq!(error, reference.next_batch().await.unwrap_err());
            assert_eq!(
                error.code,
                if cancelled {
                    "xtdb/query-cancelled"
                } else {
                    "rtdb/memory-budget-exceeded"
                }
            );
            assert_eq!(ctx.memory.used_bytes(), legacy.memory.used_bytes());
            assert!(ctx.check_cancelled().is_ok());
        }
        Ok(())
    }
}

pub fn native_uncovered() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.vector/native-uncovered",
        "The native IVF generation cannot cover this bounded exact-cut query",
    )
}
