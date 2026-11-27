use std::{borrow::Cow, collections::BTreeSet, sync::Arc};

use arrow_schema::DataType;
use rtdb_columnar::{ArrowCell, EntityIdRef, MemoryBudget};
use rtdb_index::branches::RankedIndexDeclaration;
use rtdb_plan::{Binder, Catalog, Expr, QualifiedTable, Relation, Scan, Schema, TemporalFilter};
use rtdb_query::{CandidateIids, PreparedQuery, QueryContext, QueryCursor};
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, FloatComponent, Interval, LogicalType, NumericCell, Result,
    ScalarType, TableRef, TemporalBounds, TimeUnit, Timestamp, Value,
};
use rtdb_vector::{Cut, Partition, Round, Shortlist, UnitProbe, VectorDefinition};

use super::{
    HeldVectorOverlay, MAX_NEAREST_FILTERS, NearestQuery, NearestWindow, identity_charge,
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
    use arrow_array::{Array, FixedSizeListArray, Float32Array, Int16Array, RunArray, types::Int16Type};
    use arrow_schema::Field;

    let values = RunArray::<Int16Type>::try_new(
        &Int16Array::from(vec![1, 2, 3, 4, 5, 6]),
        &Float32Array::from(vec![Some(3.0), Some(0.0), None, Some(1.0), Some(f32::NAN), Some(1.0)]),
    ).unwrap();
    let array = FixedSizeListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)), 2, Arc::new(values), None,
    );
    let field = Field::new("vector", array.data_type().clone(), false);
    let cell = ArrowCell { field: &field, array: &array, row: 0 };
    let query = NearestQuery::new(vec![1.0, 0.0], Vec::new(), 1).unwrap();
    let memory = MemoryBudget::new(1 << 20);
    assert_eq!(score_f32(&memory, cell, &query).unwrap(), Some(0.0));
    let error = Anomaly::from(super::InvalidComponents);
    assert_eq!(score_f32(&memory, ArrowCell { row: 1, ..cell }, &query).unwrap_err(), error);
    assert_eq!(score_f32(&memory, ArrowCell { row: 2, ..cell }, &query).unwrap_err(), error);
    let decode = rtdb_columnar::estimate_read_value_bytes(&field, &array, 0).unwrap().saturating_mul(2);
    assert_eq!(score_f32(&MemoryBudget::new(decode), cell, &query).unwrap(), Some(0.0));
    assert_eq!(score_f32(&MemoryBudget::new(decode - 1), cell, &query).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), 0);
}

fn finish(context: &Arc<QueryContext>, window: NearestWindow) -> Result<QueryCursor> {
    let bytes = window.hits.iter().fold(4096usize, |bytes, hit| {
        bytes
            .saturating_add(identity_charge(EntityIdRef::from(&hit.id)))
            .saturating_add(512)
    });
    let reservation = context.memory.reserve(bytes)?;
    let mut binder = Binder::default();
    let schema = Schema::new(vec![
        binder.field(
            "identity_key",
            None,
            LogicalType::Scalar(ScalarType::Binary),
        )?,
        binder.field("distance", None, LogicalType::Scalar(ScalarType::F64))?,
    ])?;
    let rows = window
        .hits
        .iter()
        .map(|hit| {
            vec![
                Value::Binary(hit.id.handle_bytes()),
                Value::F64(hit.distance),
            ]
        })
        .collect();
    QueryCursor::from_rows(schema, rows, Arc::clone(context), reservation)
}

pub fn native_uncovered() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.vector/native-uncovered",
        "The native IVF generation cannot cover this bounded exact-cut query",
    )
}
